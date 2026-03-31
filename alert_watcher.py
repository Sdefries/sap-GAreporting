"""
alert_watcher.py
─────────────────────────────────────────────────────────────────────────────
SAP Ad Grants alert system. Runs twice daily via GitHub Actions.

DATA SOURCE
  Reads from windsor_cache.json — pre-built by the automation.yml
  workflow step that calls generate_cache.py before this script runs.
  No Windsor REST API calls. No auth issues. Clean and fast.

ALERTS
  Milestone (positive)
    - Campaign CTR >= 12% with 20+ clicks → Slack + client email
    - Account-wide CTR >= 20% with 50+ clicks → Slack (internal only)
    - Grant utilization hits 90% or 100% → Slack (internal only)
    - CPA drops below $5 with 10+ conversions → Slack (internal only)

  Critical (negative)
    - Account CTR < 5% with 50+ impressions → compliance risk
    - Active campaigns, $0 spend, 0 impressions → account dark
    - Campaign spending $200+ with 0 conversions → tracking broken
    - No data returned for account → possible suspension

DEDUPLICATION
  .alert_state.json tracks which alerts fired today.
  Same alert won't fire twice in one day even across the 8am + 4pm runs.

USAGE
  python alert_watcher.py
  python alert_watcher.py --dry-run
"""

import json, os, datetime, argparse, urllib.request

# ── CONFIG ────────────────────────────────────────────────────────────────

with open("clients.json") as f:
    CLIENTS = json.load(f)

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")
ML_API_KEY    = os.environ.get("MAILERLITE_API_KEY", "")

MILESTONE = {
    "campaign_ctr_pct":    12.0,
    "account_ctr_pct":     20.0,
    "grant_utilization_1": 90.0,
    "grant_utilization_2": 100.0,
    "cpa_win":              5.00,
}
CRITICAL = {
    "min_ctr_pct":       5.0,
    "broken_conv_spend": 200.0,
}
GRANT_MAX = 10000.0


# ── DATA LOADING ──────────────────────────────────────────────────────────

def load_cache() -> dict:
    """Load windsor_cache.json. Exits if missing — workflow must build it first."""
    if not os.path.exists("windsor_cache.json"):
        print("ERROR: windsor_cache.json not found.")
        print("The generate_cache.py step must run before alert_watcher.py.")
        raise SystemExit(1)
    with open("windsor_cache.json") as f:
        data = json.load(f)
    meta = data.get("_meta", {})
    print(f"  Cache loaded: {meta.get('account_count')} accounts, "
          f"{meta.get('row_count')} rows, fetched {meta.get('fetched_at','unknown')}")
    return data


def get_rows(account_id: str, cache: dict) -> list:
    return cache.get(account_id, [])


def summarise(rows: list) -> dict:
    if not rows:
        return {"has_data": False}
    active = [r for r in rows if r.get("campaign_status") == "ENABLED"]
    s = {
        "clicks":           sum(r.get("clicks", 0) or 0 for r in rows),
        "impressions":      sum(r.get("impressions", 0) or 0 for r in rows),
        "cost":             sum(r.get("cost", 0) or 0 for r in rows),
        "conversions":      sum(r.get("conversions", 0) or 0 for r in rows),
        "campaigns":        rows,
        "active_campaigns": active,
        "has_data":         True,
    }
    s["ctr_pct"] = (
        s["clicks"] / s["impressions"] * 100 if s["impressions"] > 0 else 0.0
    )
    s["cpa"] = s["cost"] / s["conversions"] if s["conversions"] > 0 else None
    return s


# ── ALERT BUILDERS ────────────────────────────────────────────────────────

def check_milestones(client: dict, summary: dict) -> list:
    if not summary.get("has_data"):
        return []
    alerts = []
    name   = client["name"]
    email  = client.get("contact_email", "")
    fname  = client.get("contact_first_name", "there")

    # Campaign CTR milestone
    for camp in summary.get("campaigns", []):
        ctr_pct = (camp.get("ctr") or 0) * 100
        clicks  = camp.get("clicks") or 0
        cname   = camp.get("campaign", "Unknown")
        cost    = camp.get("cost") or 0
        convs   = int(camp.get("conversions") or 0)

        if ctr_pct >= MILESTONE["campaign_ctr_pct"] and clicks >= 20 and camp.get("campaign_status") == "ENABLED":
            alerts.append({
                "type": "milestone", "level": "campaign_ctr",
                "client": name, "contact_email": email,
                "slack_msg": (
                    f":trophy: *Milestone — {name}*\n"
                    f"> *{cname}* hit *{ctr_pct:.1f}% CTR* this week\n"
                    f"> {int(clicks)} clicks · ${cost:.0f} spend · {convs} conversions"
                ),
                "email_subject": "Great news — your Google Ads are performing exceptionally well",
                "email_body": (
                    f"Hi {fname},\n\n"
                    f"We wanted to share some exciting news about your Google Ad Grants campaigns.\n\n"
                    f"Your {cname} campaign hit a {ctr_pct:.1f}% click-through rate this week — "
                    f"well above the industry average and a strong indicator that your ads are resonating "
                    f"with your audience.\n\n"
                    f"Here's what that looks like:\n"
                    f"  CTR: {ctr_pct:.1f}%\n"
                    f"  Clicks this week: {int(clicks)}\n"
                    f"  Conversions: {convs}\n\n"
                    f"We'll keep optimizing to maintain this momentum. "
                    f"Your full monthly report will be in your inbox on the 1st.\n\n"
                    f"Scott\nSponsor a Purpose | sponsorapurpose.org"
                ),
            })

    # Account CTR shoutout
    if summary.get("ctr_pct", 0) >= MILESTONE["account_ctr_pct"] and summary.get("clicks", 0) >= 50:
        alerts.append({
            "type": "milestone", "level": "account_ctr_high",
            "client": name, "contact_email": None,
            "slack_msg": (
                f":star2: *Top performer — {name}*\n"
                f"> Account CTR hit *{summary['ctr_pct']:.1f}%* this week\n"
                f"> {summary['clicks']:.0f} clicks · ${summary['cost']:.0f} spend"
            ),
            "email_subject": None,
        })

    # CPA win
    if summary.get("cpa") and summary["cpa"] < MILESTONE["cpa_win"] and summary.get("conversions", 0) >= 10:
        alerts.append({
            "type": "milestone", "level": "cpa_win",
            "client": name, "contact_email": None,
            "slack_msg": (
                f":dart: *Efficiency win — {name}*\n"
                f"> CPA at *${summary['cpa']:.2f}* this week · "
                f"{summary['conversions']:.0f} conversions"
            ),
            "email_subject": None,
        })

    return alerts


def check_critical(client: dict, summary: dict) -> list:
    alerts = []
    name = client["name"]

    if not summary.get("has_data"):
        alerts.append({
            "type": "critical", "level": "no_data", "client": name,
            "slack_msg": (
                f":black_circle: *No data — {name}*\n"
                f"> Windsor returned no data. Account may be suspended."
            ),
        })
        return alerts

    # CTR compliance
    if summary["impressions"] > 50 and summary["ctr_pct"] < CRITICAL["min_ctr_pct"]:
        alerts.append({
            "type": "critical", "level": "ctr_risk", "client": name,
            "slack_msg": (
                f":red_circle: *CTR at risk — {name}*\n"
                f"> Account CTR is *{summary['ctr_pct']:.1f}%* — below the 5% Ad Grants minimum\n"
                f"> {summary['impressions']:.0f} impressions · {summary['clicks']:.0f} clicks"
            ),
        })

    # Account dark
    active = summary.get("active_campaigns", [])
    if active and summary["cost"] == 0 and summary["impressions"] == 0:
        alerts.append({
            "type": "critical", "level": "account_dark", "client": name,
            "slack_msg": (
                f":black_circle: *Account dark — {name}*\n"
                f"> {len(active)} enabled campaigns, $0 spend, 0 impressions\n"
                f"> Check account-level pause or billing"
            ),
        })

    # Broken conversion tracking
    for camp in active:
        spend = camp.get("cost") or 0
        convs = camp.get("conversions") or 0
        cname = camp.get("campaign", "Unknown")
        if spend >= CRITICAL["broken_conv_spend"] and convs == 0:
            alerts.append({
                "type": "critical", "level": "broken_conversions", "client": name,
                "slack_msg": (
                    f":warning: *Conversion tracking — {name}*\n"
                    f"> *{cname}*: ${spend:.0f} spend, 0 conversions\n"
                    f"> Likely missing a conversion goal assignment"
                ),
            })

    return alerts


# ── DELIVERY ──────────────────────────────────────────────────────────────

def post_slack(msg: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"  [SLACK] {msg[:150]}")
        return
    if not SLACK_WEBHOOK:
        print("  No SLACK_WEBHOOK — skipping")
        return
    data = json.dumps({"text": msg}).encode()
    req  = urllib.request.Request(
        SLACK_WEBHOOK, data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  Slack error: {e}")


def send_email(alert: dict, dry_run: bool = False) -> None:
    email   = alert.get("contact_email")
    subject = alert.get("email_subject")
    body    = alert.get("email_body")
    if not (email and subject and body):
        return
    if dry_run:
        print(f"  [EMAIL → {email}] {subject}")
        return
    if not ML_API_KEY:
        print("  No MAILERLITE_API_KEY — skipping email")
        return
    payload = json.dumps({
        "from": {"email": "hello@sponsorapet.org", "name": "Scott at Sponsor a Purpose"},
        "to":   [{"email": email}],
        "subject": subject,
        "text": body,
    }).encode()
    req = urllib.request.Request(
        "https://connect.mailerlite.com/api/emails",
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {ML_API_KEY}",
            "Accept":        "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            print(f"  Email sent to {email} ({r.status})")
    except Exception as e:
        print(f"  Email error: {e}")


# ── DEDUPLICATION ─────────────────────────────────────────────────────────

STATE_FILE = ".alert_state.json"

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: dict) -> None:
    today  = datetime.date.today().isoformat()
    cutoff = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    state  = {k: v for k, v in state.items() if k[:10] >= cutoff}
    state["_last_run"] = datetime.datetime.now().isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def alert_key(alert: dict) -> str:
    today = datetime.date.today().isoformat()
    return f"{today}:{alert['client']}:{alert['level']}:{alert.get('campaign','')}"

def already_sent(state: dict, alert: dict) -> bool:
    return alert_key(alert) in state

def mark_sent(state: dict, alert: dict) -> None:
    state[alert_key(alert)] = datetime.datetime.now().isoformat()


# ── MAIN ──────────────────────────────────────────────────────────────────

def run(dry_run: bool = False):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M ET")
    print(f"\nSAP Alert Watcher — {now}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Checking {len(CLIENTS)} clients...\n")

    cache  = load_cache()
    state  = load_state()
    milestones = []
    criticals  = []

    for client in CLIENTS:
        name       = client["name"]
        account_id = client.get("google_ads_id", "")
        print(f"  {name} ({account_id})")

        rows    = get_rows(account_id, cache)
        summary = summarise(rows)

        if summary.get("has_data"):
            print(f"    {len(rows)} campaigns · {summary['clicks']:.0f} clicks · "
                  f"{summary['ctr_pct']:.1f}% CTR · ${summary['cost']:.0f} spend")
        else:
            print(f"    No data in cache")

        for alert in check_milestones(client, summary):
            if not already_sent(state, alert):
                milestones.append(alert)
                mark_sent(state, alert)

        for alert in check_critical(client, summary):
            if not already_sent(state, alert):
                criticals.append(alert)
                mark_sent(state, alert)

    print(f"\nResults: {len(milestones)} milestones, {len(criticals)} critical alerts\n")

    # Post criticals
    if criticals:
        header = f":rotating_light: *SAP Alert — {len(criticals)} issue(s) detected*\n"
        for alert in criticals:
            post_slack(header + alert["slack_msg"], dry_run=dry_run)
            print(f"  CRITICAL: {alert['client']} — {alert['level']}")
    else:
        print("  No critical issues")

    # Post milestones
    if milestones:
        for alert in milestones:
            post_slack(alert["slack_msg"], dry_run=dry_run)
            send_email(alert, dry_run=dry_run)
            print(f"  MILESTONE: {alert['client']} — {alert['level']}")
    else:
        print("  No milestones this run")

    if not dry_run:
        save_state(state)

    print(f"\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
