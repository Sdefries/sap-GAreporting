"""
fetch_ai_visibility.py
─────────────────────────────────────────────────────────────────────────────
AI visibility tracker — checks whether each client is NAMED or CITED when
people ask AI assistants the questions donors, adopters and volunteers ask.

For every client prompt it asks six AI surfaces and records, per answer:
  named  — the answer mentions the org by name
  cited  — the answer links to / sources the org's website
  sites  — every domain the answer cited (so we can see who wins instead)
  competitors — named/cited flags for each tracked competitor

ENGINES                     SOURCE                                     KEY
  ai_overviews  Google AI Overviews  DataForSEO SERP (organic/live)    DATAFORSEO_LOGIN / _PASSWORD
  ai_mode       Google AI Mode       DataForSEO SERP (ai_mode/live)    DATAFORSEO_LOGIN / _PASSWORD
  chatgpt       ChatGPT              OpenAI Responses API + web search OPENAI_API_KEY
  claude        Claude               Anthropic Messages API + web search ANTHROPIC_API_KEY
  gemini        Gemini               Gemini API + Google Search grounding GEMINI_API_KEY
  perplexity    Perplexity           Perplexity Sonar API              PERPLEXITY_API_KEY

An engine with no key is skipped (shown as "Not tracked" in reports), so the
tracker can be switched on one engine at a time.

It also collects prompt ideas: "People also ask" questions from the Google
results it already pulled, plus 15 AI-suggested buyer-style prompts
(refreshed every 30 days).

CONFIG (clients.json, all optional — sensible defaults are derived):
  "ai_tracking": {
    "enabled":     true,
    "brand_names": ["Pup Profile"],              # default: client name
    "prompts":     ["Where can I adopt a rescue dog in Los Angeles?", ...],
    "engines":     ["chatgpt", "gemini", ...]     # default: all six
  },
  "competitors": [{"name": "Best Friends", "domain": "bestfriends.org"}],
  "seo_location": "Los Angeles,California,United States"   # DataForSEO location

OUTPUT
  ai_visibility_cache.json — read by generate_reports_v2.py
  Keeps the last 12 runs per client (answer text only for the latest run).

COSTS (approx, per prompt per weekly run)
  AI Overviews / AI Mode  ~$0.002–0.004 each (DataForSEO)
  ChatGPT / Gemini / Perplexity  ~$0.01–0.03 each (search-grounded answer)
  Claude  ~$0.02–0.06 (web search $10/1k searches + tokens)
  13 clients × 5 prompts × 6 engines ≈ $5–10 per weekly run

USAGE
  python fetch_ai_visibility.py                       # all clients, all engines
  python fetch_ai_visibility.py --slug pup-profile    # one client
  python fetch_ai_visibility.py --engines chatgpt,gemini
  python fetch_ai_visibility.py --dry-run             # show prompts, call nothing
  python fetch_ai_visibility.py --ideas-only          # just refresh prompt ideas
"""

import argparse
import base64
import datetime
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

CACHE_PATH   = "ai_visibility_cache.json"
MAX_RUNS     = 12
MAX_PROMPTS  = 10
EXCERPT_LEN  = 1800
IDEAS_MAX_AGE_DAYS = 30

ENGINES = ["ai_overviews", "ai_mode", "chatgpt", "claude", "gemini", "perplexity"]
ENGINE_LABELS = {
    "ai_overviews": "AI Overviews", "ai_mode": "AI Mode", "chatgpt": "ChatGPT",
    "claude": "Claude", "gemini": "Gemini", "perplexity": "Perplexity",
}

DATAFORSEO_LOGIN    = os.environ.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")
DATAFORSEO_BASE     = "https://api.dataforseo.com/v3"
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL        = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL        = os.environ.get("CLAUDE_MODEL", "claude-opus-5-5")
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL        = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
PERPLEXITY_API_KEY  = os.environ.get("PERPLEXITY_API_KEY", "")
PERPLEXITY_MODEL    = os.environ.get("PERPLEXITY_MODEL", "sonar")

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
    "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


# ── CLIENT CONFIG ─────────────────────────────────────────────────────────────

def domain_of(url):
    if not url:
        return ""
    host = urllib.parse.urlparse(url if "//" in url else "//" + url).netloc or url
    host = host.lower().split("@")[-1].split(":")[0]
    return host[4:] if host.startswith("www.") else host


def brand_names(client):
    names = (client.get("ai_tracking") or {}).get("brand_names")
    if names:
        return names
    name = re.sub(r"\s+(Grant Account|Inc\.?|LLC)$", "", client["name"], flags=re.I).strip()
    return [name]


def default_prompts(client):
    themes = (client.get("keywords") or {}).get("include_themes", [])[:5]
    locs   = [l for l in (client.get("geo") or {}).get("locations", []) if l != "United States"]
    loc    = locs[0] if locs else ""
    out = []
    for t in themes:
        out.append(f"What are the best options for {t} in {loc}?" if loc
                   else f"Which nonprofits are best known for {t}?")
    return out


def client_config(client):
    cfg = client.get("ai_tracking") or {}
    prompts = cfg.get("prompts") or default_prompts(client)
    return {
        "enabled":     cfg.get("enabled", True),
        "brand_names": brand_names(client),
        "domain":      domain_of(client.get("website", "")),
        "prompts":     prompts[:MAX_PROMPTS],
        "engines":     cfg.get("engines") or ENGINES,
        "competitors": [
            {"name": c.get("name") or c.get("domain"), "domain": domain_of(c.get("domain", ""))}
            for c in client.get("competitors", [])
        ],
        "location":    seo_location(client),
    }


def seo_location(client):
    """DataForSEO location_name, e.g. 'Kalispell,Montana,United States'."""
    if client.get("seo_location"):
        return client["seo_location"]
    locs = (client.get("geo") or {}).get("locations", [])
    if locs and "," in locs[0]:
        city, st = [p.strip() for p in locs[0].split(",", 1)]
        if st.upper() in US_STATES:
            return f"{city},{US_STATES[st.upper()]},United States"
    if locs and locs[0] in US_STATES.values():
        return f"{locs[0]},United States"
    return "United States"


# ── HTTP ──────────────────────────────────────────────────────────────────────

def post_json(url, payload, headers, timeout=90):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **headers}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        raise RuntimeError(f"HTTP {e.code}: {body}") from None


def answer(text="", sources=None, status="ok", extra=None):
    """Normalised engine result. sources: list of {url, title}."""
    seen, clean = set(), []
    for s in sources or []:
        url = (s.get("url") or "").strip()
        d = domain_of(url) or domain_of(s.get("domain", "")) or (s.get("title") or "").lower()
        if not d or (url or d) in seen:
            continue
        seen.add(url or d)
        clean.append({"url": url, "domain": d, "title": (s.get("title") or "")[:120]})
    out = {"status": status if (text or status != "ok") else "no_answer",
           "text": (text or "").strip(), "sources": clean}
    if extra:
        out.update(extra)
    return out


# ── ENGINE: DATAFORSEO (AI Overviews / AI Mode / People also ask) ─────────────

def _dfs_headers():
    token = base64.b64encode(f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _dfs_items(endpoint, payload):
    data = post_json(f"{DATAFORSEO_BASE}/{endpoint}", [payload], _dfs_headers())
    task = (data.get("tasks") or [{}])[0]
    if task.get("status_code") not in (20000, None):
        raise RuntimeError(f"DataForSEO {task.get('status_code')}: {task.get('status_message')}")
    result = (task.get("result") or [{}])[0] or {}
    return result.get("items") or []


def dfs_serp(endpoint, keyword, location, extra=None):
    payload = {"keyword": keyword, "location_name": location, "language_code": "en", **(extra or {})}
    try:
        return _dfs_items(endpoint, payload)
    except RuntimeError as e:
        # Unknown city names come back as a task error — fall back to national results
        if location != "United States" and "location" in str(e).lower():
            payload["location_name"] = "United States"
            return _dfs_items(endpoint, payload)
        raise


def _ai_block(items):
    """Pull the AI answer text + references out of SERP items."""
    texts, refs = [], []

    def walk(node, want_text=True):
        if isinstance(node, dict):
            if want_text:
                # Parent "markdown" already contains its children's text
                for key in ("markdown", "text"):
                    if isinstance(node.get(key), str):
                        texts.append(node[key])
                        want_text = key != "markdown"
                        break
            for r in node.get("references") or []:
                if isinstance(r, dict):
                    refs.append({"url": r.get("url"), "title": r.get("title") or r.get("source"),
                                 "domain": r.get("domain")})
            for k, v in node.items():
                if k != "references" and isinstance(v, (list, dict)):
                    walk(v, want_text)
        elif isinstance(node, list):
            for v in node:
                walk(v, want_text)

    blocks = [i for i in items if i.get("type") == "ai_overview"]
    walk(blocks)
    # de-dupe repeated text fragments while keeping order
    seen, uniq = set(), []
    for t in texts:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return bool(blocks), "\n".join(uniq), refs


def people_also_ask(items):
    qs = []
    for i in items:
        if i.get("type") == "people_also_ask":
            for el in i.get("items") or []:
                if el.get("title"):
                    qs.append(el["title"])
    return qs


def engine_ai_overviews(prompt, cfg):
    items = dfs_serp("serp/google/organic/live/advanced", prompt, cfg["location"],
                     {"device": "desktop", "load_async_ai_overview": True})
    present, text, refs = _ai_block(items)
    res = answer(text, refs, "ok" if present else "no_answer")
    res["_paa"] = people_also_ask(items)
    return res


def engine_ai_mode(prompt, cfg):
    items = dfs_serp("serp/google/ai_mode/live/advanced", prompt, cfg["location"])
    present, text, refs = _ai_block(items)
    return answer(text, refs, "ok" if present else "no_answer")


# ── ENGINE: CHATGPT (OpenAI Responses API with web search) ────────────────────

def engine_chatgpt(prompt, cfg):
    data = post_json("https://api.openai.com/v1/responses", {
        "model": OPENAI_MODEL,
        "input": prompt,
        "tools": [{"type": "web_search"}],
    }, {"Authorization": f"Bearer {OPENAI_API_KEY}"}, timeout=120)
    texts, sources = [], []
    for item in data.get("output") or []:
        if item.get("type") != "message":
            continue
        for part in item.get("content") or []:
            if part.get("type") == "output_text":
                texts.append(part.get("text", ""))
                for a in part.get("annotations") or []:
                    if a.get("type") == "url_citation":
                        sources.append({"url": a.get("url"), "title": a.get("title")})
    return answer("\n".join(texts), sources)


# ── ENGINE: CLAUDE (Anthropic Messages API with web search) ───────────────────

_anthropic_client = None


def engine_claude(prompt, cfg):
    global _anthropic_client
    import anthropic
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic()

    messages = [{"role": "user", "content": prompt}]
    texts, sources = [], []
    for _ in range(4):  # resume server-tool turns that pause mid-search
        resp = _anthropic_client.beta.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=16000,
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={"effort": "low"},
            tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 5}],
            messages=messages,
        )
        if resp.stop_reason == "refusal":
            return answer(status="error", extra={"error": "Claude declined to answer"})
        for block in resp.content:
            if block.type == "text":
                texts.append(block.text)
                for c in getattr(block, "citations", None) or []:
                    if getattr(c, "url", None):
                        sources.append({"url": c.url, "title": getattr(c, "title", "")})
        if resp.stop_reason != "pause_turn":
            break
        messages.append({"role": "assistant", "content": resp.content})
    return answer("".join(texts), sources)


# ── ENGINE: GEMINI (Gemini API with Google Search grounding) ──────────────────

def engine_gemini(prompt, cfg):
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    data = post_json(url, {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
    }, {"x-goog-api-key": GEMINI_API_KEY}, timeout=120)
    cand = (data.get("candidates") or [{}])[0]
    text = "".join(p.get("text", "") for p in (cand.get("content") or {}).get("parts") or [])
    sources = []
    for ch in (cand.get("groundingMetadata") or {}).get("groundingChunks") or []:
        web = ch.get("web") or {}
        # Gemini returns redirect URLs; the title carries the real domain
        sources.append({"url": "", "domain": web.get("title", ""), "title": web.get("title", "")})
    return answer(text, sources)


# ── ENGINE: PERPLEXITY (Sonar API) ────────────────────────────────────────────

def engine_perplexity(prompt, cfg):
    data = post_json("https://api.perplexity.ai/chat/completions", {
        "model": PERPLEXITY_MODEL,
        "messages": [{"role": "user", "content": prompt}],
    }, {"Authorization": f"Bearer {PERPLEXITY_API_KEY}"}, timeout=120)
    text = ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")
    sources = [{"url": r.get("url"), "title": r.get("title")} for r in data.get("search_results") or []]
    if not sources:
        sources = [{"url": u} for u in data.get("citations") or []]
    return answer(text, sources)


ENGINE_FUNCS = {
    "ai_overviews": engine_ai_overviews, "ai_mode": engine_ai_mode,
    "chatgpt": engine_chatgpt, "claude": engine_claude,
    "gemini": engine_gemini, "perplexity": engine_perplexity,
}


def engine_available(engine):
    return {
        "ai_overviews": bool(DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD),
        "ai_mode":      bool(DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD),
        "chatgpt":      bool(OPENAI_API_KEY),
        "claude":       bool(ANTHROPIC_API_KEY),
        "gemini":       bool(GEMINI_API_KEY),
        "perplexity":   bool(PERPLEXITY_API_KEY),
    }[engine]


# ── MENTION DETECTION ─────────────────────────────────────────────────────────

def mentions(text, names):
    low = (text or "").lower()
    for n in names:
        n = (n or "").strip().lower()
        if len(n) >= 3 and re.search(r"(?<![a-z0-9])" + re.escape(n) + r"(?![a-z0-9])", low):
            return True
    return False


def domain_hit(domain, res):
    if not domain:
        return False
    for s in res["sources"]:
        if s["domain"] == domain or s["domain"].endswith("." + domain):
            return True
    return domain in (res["text"] or "").lower()


def score_answer(res, cfg):
    res["named"] = mentions(res["text"], cfg["brand_names"]) or mentions(res["text"], [cfg["domain"]])
    res["cited"] = domain_hit(cfg["domain"], res)
    comps = {}
    for c in cfg["competitors"]:
        if not c["domain"]:
            continue
        names = [c["name"], c["domain"]]
        comps[c["domain"]] = {"named": mentions(res["text"], names), "cited": domain_hit(c["domain"], res)}
    res["competitors"] = comps
    res["sites"] = [s["domain"] for s in res["sources"]][:12]
    res["excerpt"] = res.pop("text")[:EXCERPT_LEN]
    res.pop("sources")
    return res


# ── PROMPT IDEAS (AI-suggested) ───────────────────────────────────────────────

def suggest_prompts(client, cfg):
    import anthropic
    themes = (client.get("keywords") or {}).get("include_themes", [])
    locs   = (client.get("geo") or {}).get("locations", [])
    brief = (
        f"Organization: {cfg['brand_names'][0]} ({client.get('org_type', 'nonprofit')})\n"
        f"Website: {client.get('website') or 'n/a'}\n"
        f"Service area: {', '.join(locs) or 'national'}\n"
        f"Programs / themes: {', '.join(themes)}\n"
        f"Prompts already tracked: {json.dumps(cfg['prompts'])}"
    )
    resp = anthropic.Anthropic().beta.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4000,
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        output_config={
            "effort": "low",
            "format": {"type": "json_schema", "schema": {
                "type": "object",
                "properties": {"prompts": {"type": "array", "items": {"type": "string"}}},
                "required": ["prompts"], "additionalProperties": False,
            }},
        },
        system=("You help a nonprofit marketing agency choose prompts to monitor in AI assistants. "
                "Write questions exactly as a real person would type them into ChatGPT or Google "
                "when looking to donate, adopt, volunteer, get help, or attend — not questions about "
                "the organization by name. Mix local and general intent. No duplicates of tracked prompts."),
        messages=[{"role": "user", "content": brief + "\n\nSuggest 15 prompts."}],
    )
    if resp.stop_reason == "refusal":
        return []
    text = next((b.text for b in resp.content if b.type == "text"), "{}")
    return [p.strip() for p in json.loads(text).get("prompts", []) if p.strip()][:15]


# ── MAIN ──────────────────────────────────────────────────────────────────────

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def fetch_client(client, cache, engines_filter=None, dry_run=False, ideas_only=False):
    cfg  = client_config(client)
    slug = client["slug"]
    if not cfg["enabled"]:
        print(f"\n  ⏭  {client['name']} — ai_tracking disabled")
        return False

    engines = [e for e in cfg["engines"] if e in ENGINES and (not engines_filter or e in engines_filter)]
    live    = [e for e in engines if engine_available(e)]
    print(f"\n  🤖 {client['name']} — {len(cfg['prompts'])} prompts · "
          f"engines: {', '.join(live) or 'none configured'} · location: {cfg['location']}")

    if dry_run:
        for p in cfg["prompts"]:
            print(f"     • {p}")
        return False

    entry = cache.get(slug) or {}
    entry.update({
        "client": client["name"], "brand_names": cfg["brand_names"], "domain": cfg["domain"],
        "competitors": cfg["competitors"], "location": cfg["location"],
    })
    paa = []

    if not ideas_only and live:
        results = {}
        for prompt in cfg["prompts"]:
            results[prompt] = {}
            for eng in engines:
                if eng not in live:
                    results[prompt][eng] = {"status": "skipped"}
                    continue
                try:
                    res = ENGINE_FUNCS[eng](prompt, cfg)
                    paa += res.pop("_paa", [])
                    res = score_answer(res, cfg)
                    flag = ("cited+named" if res["named"] and res["cited"] else
                            "cited" if res["cited"] else "named" if res["named"] else
                            "no AI answer" if res["status"] == "no_answer" else "not visible")
                    print(f"     {ENGINE_LABELS[eng]:<13} {flag:<13} {prompt[:60]}")
                except Exception as e:
                    print(f"     {ENGINE_LABELS[eng]:<13} ERROR {str(e)[:120]}")
                    res = {"status": "error", "error": str(e)[:300]}
                results[prompt][eng] = res

        runs = entry.get("runs", [])
        # Older runs keep flags only — answer text is for the latest run
        for r in runs:
            for per in r.get("results", {}).values():
                for v in per.values():
                    v.pop("excerpt", None)
        runs.append({
            "date": datetime.date.today().isoformat(),
            "checked_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "engines": engines, "live_engines": live, "prompts": cfg["prompts"], "results": results,
        })
        entry["runs"] = runs[-MAX_RUNS:]

    ideas = entry.get("prompt_ideas") or {}
    if paa:
        merged = list(dict.fromkeys(paa + [q["q"] for q in ideas.get("paa", [])]))
        ideas["paa"] = [{"q": q, "from": "Google · People also ask"} for q in merged[:25]]
    age_ok = False
    if ideas.get("ai_generated_at"):
        age = datetime.date.today() - datetime.date.fromisoformat(ideas["ai_generated_at"][:10])
        age_ok = age.days < IDEAS_MAX_AGE_DAYS
    if ANTHROPIC_API_KEY and (ideas_only or not age_ok):
        try:
            ideas["ai"] = suggest_prompts(client, cfg)
            ideas["ai_generated_at"] = datetime.date.today().isoformat()
            print(f"     ✓ {len(ideas['ai'])} AI-suggested prompts")
        except Exception as e:
            print(f"     AI prompt ideas failed: {str(e)[:120]}")
    entry["prompt_ideas"] = ideas

    cache[slug] = entry
    return True


def run(slug_filter=None, engines_filter=None, dry_run=False, ideas_only=False):
    with open("clients.json") as f:
        clients = json.load(f)
    now = datetime.datetime.now().isoformat(timespec="seconds")
    print(f"\nFetch AI visibility — {now}")
    for e in ENGINES:
        print(f"  {ENGINE_LABELS[e]:<13} {'✓ configured' if engine_available(e) else '— no key, skipped'}")

    cache = load_cache()
    fetched = 0
    for client in clients:
        if slug_filter and client["slug"] != slug_filter:
            continue
        if fetch_client(client, cache, engines_filter, dry_run, ideas_only):
            fetched += 1

    if dry_run:
        print("\n[DRY RUN] Nothing called, nothing saved")
        return
    cache["_meta"] = {"fetched_at": now, "clients_fetched": fetched}
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=1, default=str)
    print(f"\n✓ {CACHE_PATH} saved — {fetched} clients")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Track client visibility in AI answers")
    p.add_argument("--slug")
    p.add_argument("--engines", help="Comma list: " + ",".join(ENGINES))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--ideas-only", action="store_true", help="Only refresh prompt ideas")
    a = p.parse_args()
    run(a.slug, set(a.engines.split(",")) if a.engines else None, a.dry_run, a.ideas_only)
