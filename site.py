"""
site.py
─────────────────────────────────────────────────────────────────────────────
Publishes reports to reports.sponsorapurpose.org behind Cloudflare Access
(email one-time code), one login per client.

  python site.py build     assemble site/ from reports/, portals/, assets/
  python site.py access    create/update the Cloudflare Access apps
  python site.py access --dry-run

Layout of site/:
  /<slug>/            the client's report        (client's emails + agency)
  /<slug>/request/    their campaign request portal  (same login)
  /demo/              demo report                (public, for prospects)
  /assets/            logos                      (public)
  /admin/, /          admin page, agency index   (agency only)

Who can open a client's report: clients.json "report_access", a list of
emails ("ed@pinkpaws.org") or whole domains ("@pinkpaws.org"). The agency
(AGENCY_EMAILS, same format) can open everything. Anything without its own
app falls under the site-wide agency-only app, so a page is never public by
accident.

Env: CLOUDFLARE_API_TOKEN (Access: Apps and Policies Edit), CLOUDFLARE_ACCOUNT_ID,
     REPORTS_DOMAIN (default reports.sponsorapurpose.org),
     PAGES_PROJECT (default sap-reports), AGENCY_EMAILS.
"""

import json
import os
import re
import shutil
import sys

import requests

DOMAIN  = os.environ.get("REPORTS_DOMAIN") or "reports.sponsorapurpose.org"
PROJECT = os.environ.get("PAGES_PROJECT") or "sap-reports"
TAG     = "sap-reports"  # name prefix of the Access apps this script owns
SESSION = "720h"         # clients stay signed in for 30 days

# A domain rule for these would let anyone with a free inbox in
PUBLIC_MAIL = {"gmail.com", "googlemail.com", "yahoo.com", "outlook.com", "hotmail.com", "live.com",
               "icloud.com", "me.com", "aol.com", "proton.me", "protonmail.com", "gmx.com", "msn.com"}
EMAIL = re.compile(r"^[^@\s]+@[a-z0-9-]+(\.[a-z0-9-]+)+$")
DOM   = re.compile(r"^@[a-z0-9-]+(\.[a-z0-9-]+)+$")


def load_clients():
    with open("clients.json") as f:
        return json.load(f)


def rules(entries, who):
    """Access include rules for "a@b.org" / "@b.org" entries; bad ones are an error."""
    out = []
    for e in entries or []:
        e = str(e).strip().lower()
        if DOM.match(e) and e[1:] not in PUBLIC_MAIL:
            out.append({"email_domain": {"domain": e[1:]}})
        elif EMAIL.match(e):
            out.append({"email": {"email": e}})
        else:
            raise ValueError(f"{who}: '{e}' isn't an email or an organization @domain")
    return out


# ── build ─────────────────────────────────────────────────────────────────────

def build(out="site"):
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    clients = load_clients()

    def put(src, dest):
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(src, encoding="utf-8") as f:
            html = f.read()
        # Flat links (pink-paws.html) become the client's folder (/pink-paws/)
        html = re.sub(r'href="([a-z0-9-]+)\.html"', lambda m: f'href="/{m[1]}/"', html)
        html = html.replace('src="../assets/', 'src="/assets/')
        with open(dest, "w", encoding="utf-8") as f:
            f.write(html)
        return True

    n = 0
    for c in clients:
        s = c["slug"]
        n += put(f"reports/{s}.html", f"{out}/{s}/index.html")
        if not c.get("demo"):
            put(f"portals/{s}.html", f"{out}/{s}/request/index.html")
    put("reports/admin.html", f"{out}/admin/index.html")
    put("reports/index.html", f"{out}/index.html")
    shutil.copytree("assets", f"{out}/assets")
    with open(f"{out}/_headers", "w") as f:
        f.write("/*\n  X-Robots-Tag: noindex, nofollow\n  Referrer-Policy: no-referrer\n"
                "  X-Frame-Options: DENY\n  Cache-Control: private, no-store\n"
                "/assets/*\n  Cache-Control: public, max-age=86400\n")
    print(f"✓ site/ built: {n} reports")


# ── access ────────────────────────────────────────────────────────────────────

def desired(clients, agency):
    """Every Access app the site needs, keyed by app name."""
    admin = {"name": "Agency", "decision": "allow", "include": agency}
    def app(name, domain, policies):
        return {"name": f"{TAG}: {name}", "domain": domain, "type": "self_hosted",
                "session_duration": SESSION, "app_launcher_visible": False,
                "auto_redirect_to_identity": True, "policies": policies}
    apps = [app("everything (agency only)", DOMAIN, [admin]),
            # The Pages project's own address must not be a way around the login
            app("pages.dev (agency only)", f"{PROJECT}.pages.dev", [admin]),
            app("pages.dev previews (agency only)", f"*.{PROJECT}.pages.dev", [admin]),
            app("assets (public)", f"{DOMAIN}/assets",
                [{"name": "Public", "decision": "bypass", "include": [{"everyone": {}}]}])]
    for c in clients:
        s = c["slug"]
        if c.get("demo"):
            apps.append(app(f"{s} (public demo)", f"{DOMAIN}/{s}",
                            [{"name": "Public", "decision": "bypass", "include": [{"everyone": {}}]}]))
            continue
        who = rules(c.get("report_access"), s)
        apps.append(app(s, f"{DOMAIN}/{s}",
                        [admin] + ([{"name": "Client", "decision": "allow", "include": who}] if who else [])))
    return {a["name"]: a for a in apps}


def same(cur, app):
    pol = lambda ps: [(p.get("name"), p.get("decision"), json.dumps(p.get("include"), sort_keys=True))
                      for p in sorted(ps or [], key=lambda p: p.get("precedence", 0))]
    return cur.get("domain") == app["domain"] and pol(cur.get("policies")) == pol(
        [{**p, "precedence": i + 1} for i, p in enumerate(app["policies"])])


def sync_access(dry_run=False):
    agency = rules([e for e in (os.environ.get("AGENCY_EMAILS") or "").split(",") if e.strip()], "AGENCY_EMAILS")
    if not agency:
        sys.exit("AGENCY_EMAILS is empty: set it so the agency can still open every report")
    want = desired(load_clients(), agency)
    if dry_run:
        for a in want.values():
            print(f"  {a['domain']:<50} " + " | ".join(
                f"{p['decision']}: " + ", ".join(next(iter(r.values())).get("email") or next(iter(r.values())).get("domain") or "everyone"
                                                for r in p["include"]) for p in a["policies"]))
        return

    token, account = os.environ.get("CLOUDFLARE_API_TOKEN"), os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if not token or not account:
        sys.exit("CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are required")
    base = f"https://api.cloudflare.com/client/v4/accounts/{account}/access"
    h = {"Authorization": f"Bearer {token}"}

    def cf(method, path, body=None):
        r = requests.request(method, base + path, headers=h, json=body, timeout=30)
        d = r.json() if r.content else {}
        if not r.ok or not d.get("success", True):
            raise RuntimeError(f"Cloudflare {method} {path}: {r.status_code} {d.get('errors')}")
        return d.get("result")

    # Email one-time code login has to be switched on once per account
    if not any(p.get("type") == "onetimepin" for p in cf("GET", "/identity_providers") or []):
        cf("POST", "/identity_providers", {"type": "onetimepin", "name": "Email code", "config": {}})
        print("  + turned on email one-time code login")

    have = {}
    page = 1
    while True:
        batch = cf("GET", f"/apps?per_page=100&page={page}") or []
        have.update({a["name"]: a for a in batch if a.get("name", "").startswith(TAG + ":")})
        if len(batch) < 100:
            break
        page += 1

    # Create and update before deleting, so a page is never left unprotected
    for name, app in want.items():
        body = {**app, "policies": [{**p, "precedence": i + 1} for i, p in enumerate(app["policies"])]}
        if name in have:
            if not same(have[name], app):  # skip unchanged apps: no weekly churn
                cf("PUT", f"/apps/{have[name]['id']}", body)
                print(f"  ~ {app['domain']}")
        else:
            cf("POST", "/apps", body)
            print(f"  + {app['domain']}")
    for name, app in have.items():
        if name not in want:
            cf("DELETE", f"/apps/{app['id']}")
            print(f"  - {app['domain']}")
    print(f"✓ Access in sync: {len(want)} apps")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "build":
        build()
    elif cmd == "access":
        sync_access(dry_run="--dry-run" in sys.argv)
    else:
        sys.exit(__doc__)
