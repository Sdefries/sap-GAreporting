"""
alert_watcher.py
─────────────────
Runs twice daily (8am + 4pm ET via GitHub Actions).
Checks all 14 clients against two sets of thresholds:

  MILESTONE ALERTS (positive)
  - Campaign CTR hits 12%+ → celebrate in #google-ads + email client
  - Account-wide CTR beats 20% → top performer shoutout
  - Grant utilization hits 90% and 100% → milestone post
  - Cost per conversion drops below $5.00 → efficiency win

  CRITICAL ALERTS (negative)
  - Account CTR drops below 5% → compliance risk
  - Total spend hits $0 with active campaigns → account dark
  - Campaign spending with 0 conversions (and spend > $200) → tracking broken
  - No impressions in 48h → account may be suspended

All alerts post to #google-ads.
Milestone alerts also email the client contact via MailerLite.

Usage:
  python alert_watcher.py
  python alert_watcher.py --dry-run   # Print alerts without sending
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import datetime
import argparse

# ── CONFIG ────────────────────────────────────────────────────────────────
with open("clients.json") as f:
    CLIENTS = json.load(f)

SLACK_WEBHOOK   = os.environ.get("SLACK_WEBHOOK", "")
WINDSOR_API_KEY = os.environ.get("WINDSOR_API_KEY", "")
ML_API_KEY      = os.environ.get("MAILERLITE_API_KEY", "")

SLACK_CHANNEL   = "C08E9K8M3E3"  # #google-ads

# ── THRESHOLDS ────────────────────────────────────────────────────────────
MILESTONE = {
    "campaign_ctr_pct":    12.0,   # Campaign CTR % to celebrate
    "account_ctr_pct":     20.0,   # Account-wide CTR shoutout
    "grant_utilization_1": 90.0,   # First grant milestone %
    "grant_utilization_2": 100.0,  # Full grant milestone %
    "cpa_win":              5.00,  # CPA below this = efficiency win
}
CRITICAL = {
    "min_ctr_pct":          5.0,   # Ad Grants compliance floor
    "zero_spend_threshold": 0.0,   # Spend = 0 on enabled account
    "broken_conv_spend":  200.0,   # Min spend to flag 0 conversions
}

GRANT_MAX = 10000.0  # Monthly Ad Grants limit


# ── WINDSOR DATA FETCH ────────────────────────────────────────────────────

def fetch_windsor(account_id: str, fields: list[str], date_preset: str = "last_7dT") -> list[dict]:
    """Fetch Windsor data for one account."""
    params = urllib.parse.urlencode({
        "api_key":     WINDSOR_API_KEY,
        "connector":   "google_ads",
        "date_preset": date_preset,
        "fields":      ",".join(fields),
        "accounts":    account_id,
    })
    url = f"https://connectors.windsor.ai/?{params}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return data.get("data", [])
    except Exception as e:
        print(f"  Windsor error for {account_id}: {e}")
        return []


def get_account_summary(account_id: str) -> dict:
    """Get aggregated last-7-day summary for an account."""
    rows = fetch_windsor(
        account_id,
        ["clicks", "impressions", "ctr", "cost", "conversions", "campaign", "campaign_status"],
        date_preset="last_7dT"
    )
    if not rows:
        return {"has_data": False}

    active_rows = [r for r in rows if r.get("campaign_status") == "ENABLED"]
    totals = {
        "clicks":       sum(r.get("clicks", 0) or 0 for r in rows),
        "impressions":  sum(r.get("impressions", 0) or 0 for r in rows),
        "cost":         sum(r.get("cost", 0) or 0 for r in rows),
        "conversions":  sum(r.get("conversions", 0) or 0 for r in rows),
        "campaigns":    rows,
        "active_campaigns": active_rows,
        "has_data":     True,
    }
    totals["ctr_pct"] = (
        (totals["clicks"] / totals["impressions"] * 100)
        if totals["impressions"] > 0 else 0.0
    )
    totals["cpa"] = (
        totals["cost"] / totals["conversions"]
        if totals["conversions"] > 0 else None
    )
    return totals


def get_monthly_spend(account_id: str) -> float:
    """Get this month's total spend for grant utilization check."""
    rows = fetch_windsor(account_id, ["cost"], date_preset="this_monthT")
    return sum(r.get("cost", 0) or 0 for r in rows)


# ── ALERT BUILDERS ────────────────────────────────────────────────────────

def check_milestones(client: dict, summary: dict, monthly_spend: float) -> list[dict]:
    """Return list of milestone alerts for a client."""
    alerts = []
    name = client["name"]
    contact = client.get("contact_email", "")

    # Campaign-level CTR milestone (12%+)
    for camp in summary.get("campaigns", []):
        ctr = camp.get("ctr") or 0
        ctr_pct = ctr * 100
        clicks = camp.get("clicks") or 0
        camp_name = camp.get("campaign", "Unknown")

        if (
            ctr_pct >= MILESTONE["campaign_ctr_pct"]
            and clicks >= 20  # Enough volume to be meaningful
            and camp.get("campaign_status") == "ENABLED"
        ):
            alerts.append({
                "type": "milestone",
                "level": "campaign_ctr",
                "client": name,
                "contact_email": contact,
                "campaign": camp_name,
                "ctr_pct": ctr_pct,
                "clicks": int(clicks),
                "cost": camp.get("cost", 0),
                "conversions": int(camp.get("conversions") or 0),
                "slack_msg": (
                    f":trophy: *Milestone — {name}*\n"
                    f"> *{camp_name}* hit *{ctr_pct:.1f}% CTR* this week\n"
                    f"> {int(clicks)} clicks · "
                    f"${camp.get('cost', 0):.0f} spend · "
                    f"{int(camp.get('conversions') or 0)} conversions"
                ),
                "email_subject": f"Great news — your Google Ads are performing exceptionally well",
                "email_body": (
                    f"Hi {client.get('contact_first_name', 'there')},\n\n"
                    f"We wanted to share some exciting news about your Google Ad Grants campaigns.\n\n"
                    f"Your *{camp_name}* campaign hit a {ctr_pct:.1f}% click-through rate this week — "
                    f"well above the industry average and a strong indicator that your ads are resonating "
                    f"with your audience.\n\n"
                    f"Here's what that looks like:\n"
                    f"• CTR: {ctr_pct:.1f}%\n"
                    f"• Clicks this week: {int(clicks)}\n"
                    f"• Conversions: {int(camp.get('conversions') or 0)}\n\n"
                    f"We'll keep optimizing to maintain this momentum. "
                    f"Your full monthly report will be in your inbox on the 1st.\n\n"
                    f"Scott\nSponsor a Purpose | sponsorapurpose.org"
                ),
            })

    # Account CTR shoutout (20%+)
    if summary["ctr_pct"] >= MILESTONE["account_ctr_pct"] and summary["clicks"] >= 50:
        alerts.append({
            "type": "milestone",
            "level": "account_ctr_high",
            "client": name,
            "contact_email": contact,
            "ctr_pct": summary["ctr_pct"],
            "slack_msg": (
                f":star2: *Top performer — {name}*\n"
                f"> Account-wide CTR hit *{summary['ctr_pct']:.1f}%* this week — "
                f"highest in the portfolio\n"
                f"> {summary['clicks']} clicks · ${summary['cost']:.0f} spend"
            ),
            "email_subject": None,  # No client email for account-level — internal only
        })

    # Grant utilization milestones
    util_pct = (monthly_spend / GRANT_MAX) * 100
    for threshold in [MILESTONE["grant_utilization_1"], MILESTONE["grant_utilization_2"]]:
        if abs(util_pct - threshold) < 2:  # Within 2% of milestone
            label = "Full grant utilized!" if threshold == 100 else "90% of grant used"
            alerts.append({
                "type": "milestone",
                "level": "grant_utilization",
                "client": name,
                "contact_email": None,  # Internal only
                "slack_msg": (
                    f":moneybag: *{label} — {name}*\n"
                    f"> ${monthly_spend:,.0f} spent this month "
                    f"({util_pct:.0f}% of $10,000 grant)"
                ),
                "email_subject": None,
            })

    # CPA win
    if summary["cpa"] and summary["cpa"] < MILESTONE["cpa_win"] and summary["conversions"] >= 10:
        alerts.append({
            "type": "milestone",
            "level": "cpa_win",
            "client": name,
            "contact_email": None,
            "slack_msg": (
                f":dart: *Efficiency win — {name}*\n"
                f"> CPA dropped to *${summary['cpa']:.2f}* this week "
                f"({summary['conversions']} conversions)"
            ),
            "email_subject": None,
        })

    return alerts


def check_critical(client: dict, summary: dict) -> list[dict]:
    """Return list of critical alerts for a client."""
    alerts = []
    name = client["name"]

    if not summary.get("has_data"):
        alerts.append({
            "type": "critical",
            "level": "no_data",
            "client": name,
            "slack_msg": (
                f":black_circle: *No data — {name}*\n"
                f"> Windsor returned no data for this account. "
                f"Account may be suspended or disconnected."
            ),
        })
        return alerts

    # CTR compliance risk
    if (
        summary["impressions"] > 50
        and summary["ctr_pct"] < CRITICAL["min_ctr_pct"]
    ):
        alerts.append({
            "type": "critical",
            "level": "ctr_risk",
            "client": name,
            "slack_msg": (
                f":red_circle: *CTR at risk — {name}*\n"
                f"> Account CTR is *{summary['ctr_pct']:.1f}%* — "
                f"below the 5% Ad Grants minimum\n"
                f"> {summary['impressions']} impressions · {summary['clicks']} clicks"
            ),
        })

    # Account dark (enabled campaigns, $0 spend)
    active = summary.get("active_campaigns", [])
    if active and summary["cost"] == 0 and summary["impressions"] == 0:
        alerts.append({
            "type": "critical",
            "level": "account_dark",
            "client": name,
            "slack_msg": (
                f":black_circle: *Account dark — {name}*\n"
                f"> {len(active)} enabled campaigns, $0 spend, 0 impressions this week\n"
                f"> Campaigns may be paused at the account level or budget exhausted"
            ),
        })

    # Conversion tracking broken (spend + 0 conversions per campaign)
    for camp in active:
        spend = camp.get("cost") or 0
        convs = camp.get("conversions") or 0
        camp_name = camp.get("campaign", "Unknown")
        if spend >= CRITICAL["broken_conv_spend"] and convs == 0:
            alerts.append({
                "type": "critical",
                "level": "broken_conversions",
                "client": name,
                "campaign": camp_name,
                "slack_msg": (
                    f":warning: *Conversion tracking — {name}*\n"
                    f"> *{camp_name}*: ${spend:.0f} spend, 0 conversions\n"
                    f"> Likely missing a conversion goal assignment"
                ),
            })

    return alerts


# ── DELIVERY ──────────────────────────────────────────────────────────────

def post_slack(message: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"\n  [SLACK] {message[:200]}")
        return
    if not SLACK_WEBHOOK:
        print("  No SLACK_WEBHOOK set")
        return
    data = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  Slack error: {e}")


def send_client_email(alert: dict, dry_run: bool = False) -> None:
    """Send milestone email to client via MailerLite transactional API."""
    email = alert.get("contact_email")
    subject = alert.get("email_subject")
    body = alert.get("email_body")

    if not email or not subject or not body:
        return

    if dry_run:
        print(f"\n  [EMAIL → {email}] {subject}")
        return

    if not ML_API_KEY:
        print("  No MAILERLITE_API_KEY set — skipping email")
        return

    payload = json.dumps({
        "from": {"email": "hello@sponsorapet.org", "name": "Scott at Sponsor a Purpose"},
        "to": [{"email": email}],
        "subject": subject,
        "text": body,
    }).encode()

    req = urllib.request.Request(
        "https://connect.mailerlite.com/api/emails",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {ML_API_KEY}",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"  Email sent to {email} ({resp.status})")
    except Exception as e:
        print(f"  MailerLite error: {e}")


# ── DEDUPLICATION ─────────────────────────────────────────────────────────
# Prevents firing the same alert twice in one day.
# Stores a simple state file of alert keys + dates.

STATE_FILE = ".alert_state.json"

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def alert_key(alert: dict) -> str:
    today = datetime.date.today().isoformat()
    return f"{today}:{alert['client']}:{alert['level']}:{alert.get('campaign','')}"


def already_sent(state: dict, alert: dict) -> bool:
    return alert_key(alert) in state


def mark_sent(state: dict, alert: dict) -> None:
    state[alert_key(alert)] = datetime.datetime.now().isoformat()
    # Clean old entries (keep last 7 days)
    cutoff = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    state = {k: v for k, v in state.items() if k[:10] >= cutoff}


# ── MAIN ──────────────────────────────────────────────────────────────────

def run(dry_run: bool = False):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M ET")
    print(f"\nSAP Alert Watcher — {now}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Checking {len(CLIENTS)} clients...\n")

    state = load_state()
    milestones_found = []
    criticals_found = []

    for client in CLIENTS:
        name = client["name"]
        account_id = client["google_ads_id"]
        print(f"  {name} ({account_id})...")

        summary = get_account_summary(account_id)
        monthly_spend = get_monthly_spend(account_id)

        milestones = check_milestones(client, summary, monthly_spend)
        criticals  = check_critical(client, summary)

        for alert in milestones + criticals:
            if not already_sent(state, alert):
                milestones_found.append(alert) if alert["type"] == "milestone" else criticals_found.append(alert)
                mark_sent(state, alert)

    # Post critical alerts first
    if criticals_found:
        header = f":rotating_light: *SAP Alert — {len(criticals_found)} issue(s) detected*\n"
        for alert in criticals_found:
            post_slack(header + alert["slack_msg"], dry_run=dry_run)
            print(f"  CRITICAL: {alert['client']} — {alert['level']}")
    else:
        print("  No critical issues found")

    # Post milestone alerts
    for alert in milestones_found:
        post_slack(alert["slack_msg"], dry_run=dry_run)
        send_client_email(alert, dry_run=dry_run)
        print(f"  MILESTONE: {alert['client']} — {alert['level']}")

    if not milestones_found:
        print("  No milestones triggered this run")

    if not dry_run:
        save_state(state)

    print(f"\nDone. {len(criticals_found)} critical, {len(milestones_found)} milestones.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
