"""
propose_changes.py
─────────────────────────────────────────────────────────────────────────────
Weekly optimization PROPOSALS for Ad Grants accounts. READ-ONLY by design:
this script never imports the Google Ads client and cannot write to any
account. It analyzes the cache, applies hard safeguards, has Claude review
negative-keyword candidates for mission relevance, then:

  1. Writes proposals/YYYY-MM-DD.json  (the reviewable proposal file)
  2. Posts a summary digest to Slack   (#google-ads)

Nothing is ever applied by this script. A separate apply workflow (manual
trigger only) is the sole write path, and it re-validates against fresh data.

PROPOSAL TYPES
  pause_keyword   — ENABLED keyword with Quality Score <= 2 (Ad Grants
                    compliance requires pausing), or chronic CTR-killer:
                    CTR < 2% with 200+ impressions dragging the account
                    toward the 5% suspension floor.
  add_negative    — search term with clicks and zero conversions that Claude
                    judges irrelevant to the client's mission.

HARD SAFEGUARDS (deterministic — applied before and after the AI review)
  - Never propose blocking a search term that has ANY conversions
  - Never propose a term containing the client's name or protected terms
    (clients.json optional "protected_terms" list + name tokens)
  - Never propose single-word negatives
  - Skip terms already in the client's negative list (clients.json)
  - Blast-radius caps: max 10 negatives + 5 pauses per account per run;
    proposed negatives may not account for >5% of the account's clicks
  - Proposals expire 7 days after generation (enforced at apply time)

USAGE
  python propose_changes.py             # write proposals + post Slack digest
  python propose_changes.py --dry-run   # print digest, write nothing
ENV
  ANTHROPIC_API_KEY  — Claude review of negative candidates (skipped if unset;
                       unreviewed candidates are flagged, never auto-eligible)
  SLACK_WEBHOOK      — digest destination
  STAGING=1          — prefix Slack digest with [STAGING]
"""

import json, os, re, datetime, argparse, urllib.request

with open("clients.json") as f:
    CLIENTS = json.load(f)

SLACK_WEBHOOK     = os.environ.get("SLACK_WEBHOOK", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STAGING           = os.environ.get("STAGING", "") == "1"

CAPS = {
    "max_negatives_per_account": 10,
    "max_pauses_per_account":    5,
    "max_blocked_clicks_pct":    5.0,   # negatives may not block >5% of account clicks
}
RULES = {
    "pause_quality_score":  2,      # QS <= 2 → compliance pause
    "pause_ctr_pct":        2.0,    # chronic low CTR...
    "pause_min_impressions": 200,   # ...with real impression volume
    "negative_min_clicks":  3,      # search term must be actually costing clicks
}
EXPIRES_DAYS = 7


# ── SAFEGUARD HELPERS ─────────────────────────────────────────────────────

def protected_terms(client) -> set:
    """Client name tokens + explicit protected_terms from clients.json."""
    terms = set(t.lower() for t in client.get("protected_terms", []))
    for tok in re.split(r"[^a-z0-9]+", client["name"].lower()):
        if len(tok) > 3:
            terms.add(tok)
    return terms

def existing_negatives(client) -> set:
    return set(n.lower() for n in (client.get("keywords", {}) or {}).get("negative", []))


# ── CANDIDATE GENERATION (deterministic) ──────────────────────────────────

def pause_candidates(keywords):
    """Keywords that Ad Grants compliance says should not be running."""
    out = []
    for kw in keywords:
        if kw.get("status") != "ENABLED":
            continue
        qs   = kw.get("quality_score")
        ctr  = kw.get("ctr", 0) or 0
        imps = kw.get("impressions", 0) or 0
        if qs is not None and qs <= RULES["pause_quality_score"]:
            out.append({**_kw_stats(kw), "reason": f"Quality Score {qs} — Ad Grants requires pausing QS<=2 keywords"})
        elif ctr < RULES["pause_ctr_pct"] and imps >= RULES["pause_min_impressions"]:
            out.append({**_kw_stats(kw), "reason": f"{ctr:.1f}% CTR over {imps} impressions — drags account toward the 5% compliance floor"})
    out.sort(key=lambda k: k["impressions"], reverse=True)
    return out[:CAPS["max_pauses_per_account"]]

def _kw_stats(kw):
    return {
        "keyword": kw.get("keyword", ""), "ad_group": kw.get("ad_group", ""),
        "match_type": kw.get("match_type", ""), "quality_score": kw.get("quality_score"),
        "ctr": kw.get("ctr", 0), "clicks": kw.get("clicks", 0),
        "impressions": kw.get("impressions", 0), "cost": kw.get("cost", 0),
    }

def negative_candidates(client, search_terms, account_clicks):
    """Search terms eligible for AI review, plus a log of hard-rule rejections."""
    protected = protected_terms(client)
    existing  = existing_negatives(client)
    candidates, rejected = [], []
    for st in search_terms:
        term   = (st.get("search_term") or "").strip().lower()
        clicks = st.get("clicks", 0) or 0
        convs  = st.get("conversions", 0) or 0
        if not term or clicks < RULES["negative_min_clicks"]:
            continue
        entry = {"term": term, "clicks": clicks, "impressions": st.get("impressions", 0),
                 "conversions": convs, "matched_keyword": st.get("matched_keyword", "")}
        if convs > 0:
            rejected.append({**entry, "veto": "has conversions — never blocked"})
        elif term in existing:
            continue  # already negated, nothing to propose
        elif len(term.split()) < 2:
            rejected.append({**entry, "veto": "single-word negative — over-blocks"})
        elif any(p in term for p in protected):
            rejected.append({**entry, "veto": "contains protected/brand term"})
        else:
            candidates.append(entry)
    # Blast-radius: candidates may not represent >5% of account clicks
    candidates.sort(key=lambda c: c["clicks"], reverse=True)
    budget = account_clicks * CAPS["max_blocked_clicks_pct"] / 100.0
    kept, spent = [], 0
    for c in candidates:
        if spent + c["clicks"] > budget:
            rejected.append({**c, "veto": f"blast-radius cap — negatives limited to {CAPS['max_blocked_clicks_pct']:.0f}% of account clicks"})
        else:
            kept.append(c); spent += c["clicks"]
    return kept[:CAPS["max_negatives_per_account"]], rejected


# ── CLAUDE REVIEW ─────────────────────────────────────────────────────────

def ai_review_negatives(client, candidates):
    """Ask Claude which candidate search terms are truly irrelevant to the
    client's mission. Fail-safe: on any error, candidates stay unreviewed
    (flagged, never eligible for apply)."""
    if not candidates:
        return [], []
    if not ANTHROPIC_API_KEY:
        return [], [{**c, "ai": "unreviewed — no API key"} for c in candidates]

    themes = (client.get("keywords", {}) or {}).get("include_themes", [])
    prompt = (
        f"You review Google Ad Grants search terms for a nonprofit.\n"
        f"Nonprofit: {client['name']} ({client.get('website','')})\n"
        f"Type: {client.get('org_type','nonprofit')}\n"
        f"Mission themes: {', '.join(themes) if themes else 'not specified'}\n\n"
        f"For each search term below, decide if it should be BLOCKED as a negative "
        f"keyword (verdict \"block\": clearly irrelevant to the mission — job seekers, "
        f"commercial intent unrelated to the cause, wrong geography/species/topic) or "
        f"KEPT (verdict \"keep\": plausibly a supporter, adopter, donor, or beneficiary — "
        f"when in doubt, keep).\n\n"
        f"Terms:\n" + "\n".join(f"- {c['term']} (matched: {c['matched_keyword']})" for c in candidates) +
        "\n\nRespond with ONLY a JSON array: "
        '[{"term": "...", "verdict": "block"|"keep", "reason": "<10 words"}]'
    )
    try:
        import anthropic
        resp = anthropic.Anthropic().messages.create(
            model="claude-opus-5", max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = next(b.text for b in resp.content if b.type == "text")
        verdicts = json.loads(re.search(r"\[.*\]", text, re.S).group(0))
        vmap = {v["term"].strip().lower(): v for v in verdicts}
    except Exception as e:
        print(f"  Claude review failed ({e}) — candidates left unreviewed")
        return [], [{**c, "ai": "unreviewed — review error"} for c in candidates]

    proposed, kept = [], []
    for c in candidates:
        v = vmap.get(c["term"])
        if v and v.get("verdict") == "block":
            proposed.append({**c, "ai": v.get("reason", "")})
        else:
            kept.append({**c, "ai": (v or {}).get("reason", "AI kept / no verdict")})
    return proposed, kept

# ── OUTPUT ────────────────────────────────────────────────────────────────

def post_slack(msg, dry_run):
    if dry_run or not SLACK_WEBHOOK:
        print("\n" + msg)
        return
    req = urllib.request.Request(SLACK_WEBHOOK, data=json.dumps({"text": msg}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
        print("Proposal digest posted to Slack")
    except Exception as e:
        print(f"Slack error: {e}")


def run(dry_run=False):
    today = datetime.date.today()
    print(f"\nSAP Proposal Engine — {today.isoformat()} (read-only)")

    with open("google_ads_cache.json") as f:
        cache = json.load(f)

    proposals, digest_lines, total_changes = {}, [], 0
    for client in CLIENTS:
        acct = client.get("google_ads_id", "")
        name = client["name"]
        keywords     = cache.get(f"{acct}_keywords", [])
        search_terms = cache.get(f"{acct}_search_terms", [])
        account_clicks = sum(r.get("clicks", 0) or 0 for r in cache.get(acct, []))

        pauses = pause_candidates(keywords)
        candidates, rejected = negative_candidates(client, search_terms, account_clicks)
        negatives, ai_kept = ai_review_negatives(client, candidates)

        if not (pauses or negatives):
            continue
        proposals[client["slug"]] = {
            "client": name, "account": acct,
            "pause_keywords": pauses, "add_negatives": negatives,
            "rejected_by_safeguards": rejected, "kept_by_ai": ai_kept,
        }
        total_changes += len(pauses) + len(negatives)
        line = f"*{name}:*"
        if negatives:
            tops = ", ".join(f"\"{n['term']}\" ({n['clicks']} clicks, 0 conv)" for n in negatives[:3])
            line += f" add {len(negatives)} negative(s) — {tops}{'…' if len(negatives) > 3 else ''}"
        if pauses:
            line += f"{' ·' if negatives else ''} pause {len(pauses)} keyword(s) — {pauses[0]['reason']}"
        digest_lines.append(line)
        print(f"  {name}: {len(pauses)} pauses, {len(negatives)} negatives, {len(rejected)} vetoed")

    if not proposals:
        print("No proposals this week — all accounts clean.")
        return

    doc = {
        "generated": today.isoformat(),
        "expires": (today + datetime.timedelta(days=EXPIRES_DAYS)).isoformat(),
        "status": "proposed",   # the apply workflow flips this to applied/expired
        "accounts": proposals,
    }
    path = f"proposals/{today.isoformat()}.json"
    if not dry_run:
        os.makedirs("proposals", exist_ok=True)
        with open(path, "w") as f:
            json.dump(doc, f, indent=2)
        print(f"Wrote {path}")

    rejected_n = sum(len(p["rejected_by_safeguards"]) for p in proposals.values())
    header = "[STAGING] " if STAGING else ""
    msg = (f"{header}📋 *Weekly proposals — {len(proposals)} client(s), {total_changes} change(s)*\n"
           f"_Nothing has been applied. Proposals expire {doc['expires']}._\n\n"
           + "\n".join(digest_lines)
           + f"\n\n_Vetoed by safeguards: {rejected_n} · Full detail: `{path}`_"
           + "\n→ To apply: Actions → SAP Ad Grants Automation → Run workflow → `apply` (not yet built)")
    post_slack(msg, dry_run)
    print("\nDone. (read-only — no account was modified)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    run(dry_run=p.parse_args().dry_run)
