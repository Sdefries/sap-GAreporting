"""
slack_digest.py
─────────────────────────────────────────────────────────────────────────────
Posts a Monday morning all-client performance digest to #google-ads.
Runs every Monday at 8am ET via automation.yml.

Shows the week at a glance for all 15 clients — ranked by performance.
Flags accounts needing attention at the top.
"""

import json
import os
import datetime
import urllib.request

with open("clients.json") as f:
    CLIENTS = json.load(f)

with open("google_ads_cache.json") as f:
    CACHE = json.load(f)

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")

def get_summary(account_id):
    rows = CACHE.get(account_id, [])
    if not rows:
        return None
    clicks = sum(r.get("clicks", 0) or 0 for r in rows)
    imps   = sum(r.get("impressions", 0) or 0 for r in rows)
    cost   = sum(r.get("cost", 0) or 0 for r in rows)
    convs  = sum(r.get("conversions", 0) or 0 for r in rows)
    return {
        "clicks": clicks,
        "impressions": imps,
        "cost": cost,
        "conversions": convs,
        "ctr_pct": clicks / imps * 100 if imps > 0 else 0,
        "cpa": cost / convs if convs > 0 else None,
    }

def run():
    today     = datetime.date.today()
    week_str  = today.strftime("%B %d, %Y")
    meta      = CACHE.get("_meta", {})

    results   = []
    flags     = []
    stars     = []

    for client in CLIENTS:
        name = client["name"]
        acct = client.get("google_ads_id", "")
        s    = get_summary(acct)
        if not s:
            flags.append(f":black_circle: *{name}* — no data returned")
            continue

        results.append((name, s))

        # Flags
        if s["impressions"] > 50 and s["ctr_pct"] < 5:
            flags.append(f":red_circle: *{name}* — CTR at {s['ctr_pct']:.1f}% (below 5% minimum)")
        if s["cost"] == 0 and s["impressions"] == 0:
            flags.append(f":black_circle: *{name}* — account dark, $0 spend")

        # Stars
        if s["ctr_pct"] >= 15 and s["clicks"] >= 20:
            stars.append(f":trophy: *{name}* — {s['ctr_pct']:.1f}% CTR · {s['clicks']:.0f} clicks · ${s['cost']:.0f} spend")

    # Sort by CTR
    results.sort(key=lambda x: x[1]["ctr_pct"], reverse=True)

    # Build table
    total_clicks = sum(s["clicks"] for _, s in results)
    total_spend  = sum(s["cost"] for _, s in results)
    total_convs  = sum(s["conversions"] for _, s in results)

    rows = ""
    for name, s in results:
        ctr_flag = "🟢" if s["ctr_pct"] >= 10 else "🟡" if s["ctr_pct"] >= 5 else "🔴"
        rows += f"{ctr_flag} *{name[:28]}* — {s['ctr_pct']:.1f}% CTR · {s['clicks']:.0f} clicks · ${s['cost']:.0f}\n"

    msg = f"""📊 *SAP Ad Grants — Weekly Digest*
Week of {week_str} · {len(results)} active accounts

*Portfolio totals:*
Clicks: {total_clicks:,.0f} · Spend: ${total_spend:,.0f} · Conversions: {total_convs:,.0f}

*Account performance:*
{rows}"""

    if flags:
        msg += f"\n*Needs attention ({len(flags)}):*\n" + "\n".join(flags)

    if stars:
        msg += f"\n\n*Top performers this week:*\n" + "\n".join(stars)

    msg += f"\n\n_Data via Google Ads API · Cache: {meta.get('fetched_at', 'unknown')}_"

    if not SLACK_WEBHOOK:
        print(msg)
        return

    payload = json.dumps({"text": msg}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK, data=payload,
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print("Monday digest posted to Slack")
    except Exception as e:
        print(f"Slack error: {e}")

if __name__ == "__main__":
    run()
