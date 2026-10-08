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
