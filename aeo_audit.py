"""
aeo_audit.py
─────────────────────────────────────────────────────────────────────────────
AEO (answer engine optimization) readiness audit — checks whether a client's
website is set up so ChatGPT, Claude, Perplexity, Gemini and Google's AI
answers can find, read, trust and cite it.

Free: plain HTTP fetches of the client's own site, no API keys.

CHECKS
  AI crawler access   robots.txt lets GPTBot, OAI-SearchBot, ClaudeBot,
                      PerplexityBot, Google-Extended etc. in
  llms.txt            /llms.txt exists (a plain-text guide for AI)
  Sitemap             sitemap listed in robots.txt or at /sitemap.xml
  HTTPS               site loads over https
  Organization schema JSON-LD Organization / NGO / LocalBusiness markup
  FAQ content         FAQPage schema or a FAQ section answering questions
  Readable content    homepage text is in the HTML (not only JavaScript)
  Title & description <title> and meta description present
  Contact & location  address / phone visible for local answers
  Donate path         a donate / give link AI can point people to

USAGE
  python aeo_audit.py --slug pup-profile     # print one audit
  (fetch_ai_visibility.py runs it for every client each week)
"""

import argparse
import datetime
import json
import re
import urllib.error
import urllib.request
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

UA = "Mozilla/5.0 (compatible; SAPBot/1.0; +https://sponsorapurpose.org)"

# Crawlers that decide whether a site can appear in AI answers.
AI_BOTS = [
    ("GPTBot", "ChatGPT (training)"),
    ("OAI-SearchBot", "ChatGPT search"),
    ("ChatGPT-User", "ChatGPT browsing"),
    ("ClaudeBot", "Claude"),
    ("Claude-SearchBot", "Claude search"),
    ("PerplexityBot", "Perplexity"),
    ("Google-Extended", "Gemini"),
    ("Googlebot", "Google AI Overviews / AI Mode"),
    ("Bingbot", "Bing / Copilot (ChatGPT uses Bing results)"),
]

ORG_TYPES = {"organization", "ngo", "nonprofitorganization", "localbusiness", "animalshelter",
             "museum", "legalservice", "charity", "educationalorganization", "corporation"}


def fetch(url, timeout=20, limit=1_500_000):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(limit).decode(r.headers.get_content_charset() or "utf-8", errors="replace")
            return r.status, body, r.geturl()
    except urllib.error.HTTPError as e:
        return e.code, "", url
    except Exception as e:
        return None, str(e), url


# ── robots.txt ────────────────────────────────────────────────────────────────

def parse_robots(text):
    """Return {agent_lower: [disallow paths]} — minimal robots.txt grouping."""
    groups, agents, in_rules = {}, [], False
    for raw in (text or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        key, val = [p.strip() for p in line.split(":", 1)]
        key = key.lower()
        if key == "user-agent":
            if in_rules:
                agents, in_rules = [], False
            agents.append(val.lower())
            for a in agents:
                groups.setdefault(a, [])
        elif key in ("disallow", "allow"):
            in_rules = True
            for a in agents:
                groups.setdefault(a, []).append((key, val))
    return groups


def bot_blocked(groups, bot):
    rules = groups.get(bot.lower())
    if rules is None:
        rules = groups.get("*", [])
    allow_root = any(k == "allow" and v in ("/", "/*") for k, v in rules)
    return any(k == "disallow" and v in ("/", "/*") for k, v in rules) and not allow_root


# ── HTML ──────────────────────────────────────────────────────────────────────

class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title, self.desc, self.jsonld, self.links, self.text = "", "", [], [], []
        self._in, self._skip, self._script_ld, self._buf = None, 0, False, []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "title":
            self._in = "title"
        elif tag == "meta" and (a.get("name", "").lower() == "description"
                                or a.get("property", "").lower() == "og:description"):
            self.desc = self.desc or (a.get("content") or "")
        elif tag == "script":
            if (a.get("type") or "").lower() == "application/ld+json":
                self._script_ld, self._buf = True, []
            else:
                self._skip += 1
        elif tag in ("style", "noscript", "svg"):
            self._skip += 1
        elif tag == "a" and a.get("href"):
            self.links.append(a["href"])

    def handle_endtag(self, tag):
        if tag == "title":
            self._in = None
        elif tag == "script":
            if self._script_ld:
                self.jsonld.append("".join(self._buf))
                self._script_ld = False
            elif self._skip:
                self._skip -= 1
        elif tag in ("style", "noscript", "svg") and self._skip:
            self._skip -= 1

    def handle_data(self, data):
        if self._script_ld:
            self._buf.append(data)
        elif self._in == "title":
            self.title += data
        elif not self._skip:
            self.text.append(data)


def schema_types(blocks):
    types = set()

    def walk(node):
        if isinstance(node, dict):
            t = node.get("@type")
            for x in (t if isinstance(t, list) else [t]):
                if isinstance(x, str):
                    types.add(x)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    for b in blocks:
        try:
            walk(json.loads(b))
        except Exception:
            continue
    return types


# ── AUDIT ─────────────────────────────────────────────────────────────────────

def check(key, label, status, detail, fix="", why=""):
    return {"key": key, "label": label, "status": status, "detail": detail, "fix": fix, "why": why}


def audit_site(website, fetcher=fetch):
    """Run every check. fetcher(url) -> (status, body, final_url) is injectable for tests."""
    if not website:
        return {"status": "no_website", "checks": [], "score": None,
                "checked_at": datetime.datetime.now().isoformat(timespec="seconds")}
    if "//" not in website:
        website = "https://" + website
    base = f"{urlparse(website).scheme}://{urlparse(website).netloc}"

    status, html, final = fetcher(website)
    if not status or status >= 400:
        return {"status": "unreachable", "error": f"Homepage returned {status or html[:120]}",
                "checks": [], "score": None,
                "checked_at": datetime.datetime.now().isoformat(timespec="seconds")}
    base = f"{urlparse(final).scheme}://{urlparse(final).netloc}"
    checks = []

    # AI crawler access
    r_status, robots, _ = fetcher(base + "/robots.txt")
    groups = parse_robots(robots if r_status == 200 else "")
    blocked = [(b, who) for b, who in AI_BOTS if bot_blocked(groups, b)]
    if not blocked:
        checks.append(check("crawlers", "AI crawler access", "pass",
                            "robots.txt lets every major AI crawler in." if r_status == 200
                            else "No robots.txt, so all crawlers are allowed by default.",
                            why="AI engines can only cite pages their crawlers are allowed to read."))
    else:
        names = ", ".join(f"{b} ({who})" for b, who in blocked)
        critical = any(b in ("Googlebot", "OAI-SearchBot", "PerplexityBot", "Bingbot", "Claude-SearchBot")
                       for b, _ in blocked)
        checks.append(check("crawlers", "AI crawler access", "fail" if critical else "warn",
                            f"robots.txt blocks {names}.",
                            fix="Remove the Disallow: / rules for these user-agents in robots.txt "
                                "(often added by a security or hosting plugin).",
                            why="Blocked crawlers mean that AI can't read or cite the site."))

    # llms.txt
    l_status, llms, _ = fetcher(base + "/llms.txt")
    has_llms = l_status == 200 and llms.strip() and "<html" not in llms[:500].lower()
    checks.append(check("llms", "llms.txt", "pass" if has_llms else "warn",
                        "llms.txt found." if has_llms else "No /llms.txt file.",
                        fix="" if has_llms else "Add /llms.txt: a short plain-text summary of who you are, "
                            "what you do, where you serve, and links to your key pages (adopt, donate, volunteer, programs).",
                        why="A growing number of AI tools read llms.txt to understand a site quickly."))

    # Sitemap
    sm_in_robots = bool(re.search(r"(?im)^\s*sitemap\s*:", robots or "")) if r_status == 200 else False
    sm_ok = sm_in_robots
    if not sm_ok:
        s_status, sm, _ = fetcher(base + "/sitemap.xml")
        sm_ok = s_status == 200 and ("<urlset" in sm[:2000] or "<sitemapindex" in sm[:2000])
    checks.append(check("sitemap", "XML sitemap", "pass" if sm_ok else "warn",
                        "Sitemap found." if sm_ok else "No sitemap at /sitemap.xml or in robots.txt.",
                        fix="" if sm_ok else "Publish /sitemap.xml and list it in robots.txt with a Sitemap: line.",
                        why="Sitemaps help search and AI crawlers discover every page."))

    # HTTPS
    https = final.startswith("https://")
    checks.append(check("https", "Secure site (HTTPS)", "pass" if https else "fail",
                        "Site loads over HTTPS." if https else "Site does not load over HTTPS.",
                        fix="" if https else "Turn on SSL with your host and redirect http to https.",
                        why="AI engines and Google favor secure, trustworthy sources."))

    p = PageParser()
    try:
        p.feed(html)
    except Exception:
        pass
    types = schema_types(p.jsonld)
    lower_types = {t.lower() for t in types}
    text = re.sub(r"\s+", " ", unescape(" ".join(p.text))).strip()
    words = len(text.split())

    # Organization schema
    org = lower_types & ORG_TYPES
    checks.append(check("schema_org", "Organization schema", "pass" if org else "fail",
                        f"Found {', '.join(sorted(t for t in types if t.lower() in ORG_TYPES))} markup." if org
                        else "No Organization / NonprofitOrganization structured data on the homepage.",
                        fix="" if org else "Add JSON-LD NGO (or NonprofitOrganization) schema with name, url, logo, "
                            "address, phone, sameAs (social profiles, GuideStar/Charity Navigator) and areaServed.",
                        why="Structured data tells AI exactly who you are, where you are and what you do."))

    # FAQ content
    faq_schema = "faqpage" in lower_types
    faq_link = any(re.search(r"faq|frequently", l, re.I) for l in p.links)
    faq_text = bool(re.search(r"frequently asked|\bFAQs?\b", text, re.I))
    status_faq = "pass" if faq_schema else ("warn" if (faq_link or faq_text) else "fail")
    checks.append(check("faq", "FAQ content", status_faq,
                        "FAQPage schema found." if faq_schema else
                        ("FAQ content found, but without FAQPage schema." if status_faq == "warn"
                         else "No FAQ page or FAQ section found."),
                        fix="" if faq_schema else "Publish a FAQ page answering the exact questions you track "
                            "(cost, process, eligibility, location, hours) and mark it up with FAQPage schema.",
                        why="Question-and-answer content is what AI answers quote most often."))

    # Readable content
    checks.append(check("content", "Readable homepage text",
                        "pass" if words >= 250 else ("warn" if words >= 80 else "fail"),
                        f"About {words:,} words of text in the page HTML.",
                        fix="" if words >= 250 else "Put a clear description of your mission, programs and service area "
                            "as real text on the homepage (not only in images, sliders or JavaScript widgets).",
                        why="Many AI crawlers don't run JavaScript; if the text isn't in the HTML they see an empty page."))

    # Title & description
    title, desc = p.title.strip(), p.desc.strip()
    td_status = "pass" if title and len(desc) >= 50 else ("warn" if title or desc else "fail")
    checks.append(check("meta", "Title & description", td_status,
                        f"Title: “{title[:70]}”. " + (f"Description: {len(desc)} characters." if desc else "No meta description."),
                        fix="" if td_status == "pass" else "Write a meta description (120–160 characters) that says who you "
                            "help, where, and how (e.g. adopt, donate, volunteer).",
                        why="Titles and descriptions are often what AI and Google use to summarize a source."))

    # Contact & location
    phone = bool(re.search(r"\(?\b\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b", text)) or any(l.startswith("tel:") for l in p.links)
    address = bool(re.search(r"\b\d{2,6}\s+\w+(\s\w+)*\s(St|Street|Ave|Avenue|Rd|Road|Blvd|Boulevard|Dr|Drive|Hwy|Highway|Ln|Lane|Way|Pkwy)\b", text, re.I)) \
        or "postaladdress" in lower_types
    loc_status = "pass" if phone and address else ("warn" if phone or address else "fail")
    checks.append(check("contact", "Contact & location", loc_status,
                        ("Phone and address found." if loc_status == "pass" else
                         "Found " + ("a phone number" if phone else "an address") + " only." if loc_status == "warn" else
                         "No phone number or street address found on the homepage."),
                        fix="" if loc_status == "pass" else "Show your phone and address (or service area) in the footer "
                            "and in schema, matching your Google Business Profile exactly.",
                        why="Local AI answers (\"near me\", \"in Kalispell\") rely on consistent name, address and phone."))

    # Donate path
    donate = any(re.search(r"donat|give|giving|support-us|sponsor", l, re.I) for l in p.links) \
        or bool(re.search(r"\bdonate\b", text, re.I))
    checks.append(check("donate", "Clear donate path", "pass" if donate else "warn",
                        "Donate link found." if donate else "No donate link found on the homepage.",
                        fix="" if donate else "Add a visible Donate link to a dedicated /donate page.",
                        why="When someone asks AI where to donate, it points to the clearest donation page it can find."))

    pts = {"pass": 1, "warn": 0.5, "fail": 0}
    score = round(sum(pts[c["status"]] for c in checks) / len(checks) * 100)
    return {
        "status": "ok", "url": final, "score": score, "checks": checks,
        "schema_types": sorted(types), "word_count": words,
        "title": title, "description": desc, "about_text": text[:2000],
        "checked_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="AEO readiness audit")
    ap.add_argument("--slug")
    ap.add_argument("--url")
    a = ap.parse_args()
    url = a.url
    if a.slug:
        url = next(c.get("website", "") for c in json.load(open("clients.json")) if c["slug"] == a.slug)
    print(json.dumps(audit_site(url), indent=2))
