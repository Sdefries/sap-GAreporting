"""
plans.py
─────────────────────────────────────────────────────────────────────────────
Which report sections a client's plan includes (plans.json), used by the
report generator to lock sections and by the weekly fetchers to skip paid API
calls for features a client doesn't have.

  clients.json   "plan": "growth", "addons": ["local"]
  no "plan"      everything is included
  "locked_sections": [...] still works as an extra per-client override
"""

import json
import os

_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plans.json")
with open(_PATH) as f:
    PLANS = json.load(f)

# Every section a plan can leave out
ALL_SECTIONS = {s for p in PLANS["packages"].values() for s in p["sections"]}


def included(client):
    """Set of section keys the client's plan includes."""
    plan = client.get("plan")
    if not plan:
        return set(ALL_SECTIONS)
    if plan not in PLANS["plans"]:
        raise ValueError(f"{client.get('slug')}: unknown plan '{plan}' (see plans.json)")
    out = set()
    for pkg in PLANS["plans"][plan] + list(client.get("addons") or []):
        out |= set(PLANS["packages"].get(pkg, {}).get("sections", []))
    return out - set(client.get("locked_sections") or [])


def locked(client):
    """Section keys the client doesn't have, in a stable order."""
    have = included(client)
    return sorted(ALL_SECTIONS - have)


def has(client, section):
    return section in included(client)


def package_of(section):
    """Label of the package that includes a section, for the upgrade card."""
    for p in PLANS["packages"].values():
        if section in p["sections"]:
            return p["label"]
    return ""


# ── Size, price and check frequency ───────────────────────────────────────────

def size(client):
    """small / mid / large from clients.json "size"; None if not set yet."""
    s = client.get("size")
    return s if s in PLANS.get("sizes", {}) else None


def check_days(client):
    """How often paid checks (AI answers, keyword ranks, map, competitors) run.
    Monthly for small and mid-size nonprofits keeps their cost low."""
    s = size(client)
    return 28 if s and PLANS["sizes"][s].get("checks") == "monthly" else 6


def due(client, last_iso):
    """True when a check last run on last_iso (ISO date or datetime) is due again."""
    import datetime
    if not last_iso:
        return True
    try:
        last = datetime.date.fromisoformat(str(last_iso)[:10])
    except ValueError:
        return True
    return (datetime.date.today() - last).days >= check_days(client)


def monthly_price(client):
    """Monthly price in dollars for the client's size, plan and add-ons, or None
    when size or plan isn't set. Essentials is free for sizes listed in
    free_with_processing when the client processes donations with us."""
    s, plan = size(client), client.get("plan")
    table = (PLANS.get("prices") or {}).get(s or "")
    if not table or not plan or plan not in table["plans"]:
        return None
    price = table["plans"][plan]
    if client.get("processing") and "essentials" in table.get("free_with_processing", []):
        price -= table["plans"].get("essentials", 0)
    in_plan = set(PLANS["plans"][plan])
    price += sum(table["addons"].get(a, 0) for a in client.get("addons") or [] if a not in in_plan)
    return max(0, price)
