# Search & AI Visibility

Client reports have a **Search & AI visibility** group in the left nav, modeled on Anthony's Results Driven Tracker and styled to match the Sponsor a Purpose CRM.

| Section | What it shows | Data |
|---|---|---|
| **AI tracking** | AI visibility score; a card per AI; **Visits from AI** (GA4: sessions from ChatGPT, Perplexity, Gemini, Copilot, Claude, and the pages they land on); **How AI describes you** (accuracy check); trend; prompt × AI table (click a cell to read the answer); per-AI tabs; "Who AI recommends instead" and "Sites AI trusts — get listed"; prompt ideas | `ai_visibility_cache.json` ← `fetch_ai_visibility.py`, `ga4_cache.json` ← `fetch_ga4.py` |
| **AEO action plan** | For prompts where AI recommends someone else: why, which sites to get listed on, and a page brief to publish (title, URL, outline, FAQs) | `ai_visibility_cache.json` (Claude) |
| **AEO readiness** | Website checklist (AI crawler access, llms.txt, sitemap, HTTPS, Organization schema, FAQ, readable text, title/description, contact & location, donate path) with fixes, plus **ready-to-paste llms.txt and schema** | `ai_visibility_cache.json` (no API key needed) |
| **Classic SEO** | Search visibility score, average position, top 3 / top 10, AI Overview citations, trends, tracked keyword table | `seo_cache.json` ← `fetch_seo.py` (SEO-package clients) |
| **Organic search** | SEMrush-style: every keyword the site ranks for, with volume, difficulty, estimated visits and change; position distribution; traffic value; top pages; competitors found from shared keywords; keyword opportunities; authority (backlinks, optional) | `organic_cache.json` ← `fetch_organic.py` (all clients with a website, monthly) |
| **Competitors** | You vs tracked competitors in Google rankings and AI answers | caches above |
| **Site health** | PageSpeed / Core Web Vitals, Search Console clicks and top queries | `seo_cache.json` |
| **Local map & profile** | Google Business Profile (rating, reviews, category, hours, claimed) and a Google Maps **heat map** per keyword with share of local voice and average rank | `local_cache.json` ← `fetch_local.py` (clients with `local_tracking`) |

**Live features** (test a prompt now, track prompts, generate ideas) run through a small Cloudflare Worker that keeps the API keys private. See [`worker/README.md`](../worker/README.md). Until it's set up, those buttons are hidden.

Other report changes: the **device breakdown** is now real Google Ads data, **What we did / What's next** is written from each month's data, and there's a **↓ PDF** button. `reports/index.html` is an all-clients dashboard.

Score bands everywhere: **Poor** < 20% · **Moderate** 20–50% · **Good** 50–80% · **Great** 80%+.

## How a prompt is scored

Each AI's answer to a prompt is checked for two things:

- **Named**: the answer mentions one of the client's `brand_names` (or its domain).
- **Cited**: the answer cites or links the client's website.

An answer counts as *visible* if it's named or cited. The AI visibility score is visible answers ÷ all answers (prompts × AIs). When Google shows no AI Overview / AI Mode answer, that cell reads "No AI answer" and counts as not visible.

## Setup: GitHub secrets and variables

Add these under **repo → Settings → Secrets and variables → Actions**. Each feature turns on when its key is present.

| Secret | Turns on | Where to get it |
|---|---|---|
| `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD` | AI Overviews, AI Mode, Classic SEO rankings, Organic search, Local map & profile | dataforseo.com → API access |
| `OPENAI_API_KEY` | ChatGPT | platform.openai.com |
| `ANTHROPIC_API_KEY` | Claude, AEO action plan, accuracy check, prompt ideas, starter prompts for new clients | console.anthropic.com |
| `GEMINI_API_KEY` | Gemini | aistudio.google.com |
| `PERPLEXITY_API_KEY` | Perplexity | perplexity.ai → API |
| `PAGESPEED_API_KEY` | Site speed scores (without it Google rate-limits the checks) | Google Cloud console → enable PageSpeed Insights API → API key |
| `GA_SERVICE_ACCOUNT` | Already set for GA4. Also used for Search Console: add the service account's email as a user on each client's Search Console property | — |
| `SLACK_WEBHOOK` | Already set. AI visibility alerts post here too | — |

| Variable (not secret) | Turns on |
|---|---|
| `DATAFORSEO_BACKLINKS` = `1` | Authority score (needs DataForSEO's Backlinks API subscription) |
| `LIVE_API_URL` | Live features (see worker/README.md) |

Optional model overrides (environment variables): `OPENAI_MODEL` (default `gpt-4.1-mini`), `CLAUDE_MODEL` (default `claude-opus-5-5`), `GEMINI_MODEL` (default `gemini-2.5-flash`), `PERPLEXITY_MODEL` (default `sonar`).

Claude requests use server-side refusal fallback (`fallbacks: "default"`): if Claude declines a request, it's retried on a fallback model instead of being recorded as a failed check.

## Schedule

Every **Monday 5am PST** `automation.yml` runs `fetch_ai`, `fetch_seo`, `fetch_local` and `fetch_organic`. Organic search refreshes each client about once a month. The 9am weekly report run picks up the results. Any job can also be started from **Actions → SAP Ad Grants Automation → Run workflow**.

## Slack alerts

After each AI run, `#google-ads` (the existing `SLACK_WEBHOOK`) gets a message when a client:

- drops 10+ points in AI visibility,
- stops showing up in an AI they were visible in, or
- is overtaken by a tracked competitor.

## Per-client config (`clients.json`)

```json
"ai_tracking": {
  "enabled": true,
  "brand_names": ["Pup Profile"],
  "prompts": ["Where can I adopt a rescue dog in Los Angeles?", "..."],
  "engines": ["chatgpt", "gemini"]
},
"competitors": [
  {"name": "Best Friends Animal Society", "domain": "bestfriends.org"}
],
"local_tracking": {
  "business_name": "Humane Society of Northwest Montana",
  "keywords": ["animal shelter", "adopt a dog", "humane society"],
  "grid": 5, "radius_miles": 5
},
"report_notes": {"did": "Launched the spring adoption campaign...", "next": "..."},
"seo_location": "Los Angeles,California,United States",
"organic_tracking": {"enabled": false}
```

- `ai_tracking.prompts`: up to 10. Every current client is seeded with 5. **New clients without prompts get 5 starter prompts written by Claude automatically.**
- `competitors`: powers the Competitors section and red competitor tags. The report's "Who AI recommends instead" panel suggests who to add.
- `local_tracking`: only for organizations people visit in person. It's seeded for the Humane Society of Northwest Montana and ScienceWorks. Coordinates are found automatically; add `lat`/`lng` or `place_id` if the wrong listing matches.
- `report_notes`: your team's own "What we did / What's next" text. It overrides the automatic text.
- `organic_tracking.enabled: false` skips a client in the organic report.
- Classic SEO still requires `local_seo_enrolled: true` and `seo_keywords`.

## Cost (rough, per month, all 13 clients)

| Feature | Cost |
|---|---|
| AI tracking (5 prompts × 6 AIs, weekly) | $20–40 |
| AEO action plan + accuracy check (Claude, weekly) | $5–10 |
| Organic search (monthly) | ~$1 |
| Local map (2 clients × 3 keywords × 25 points, weekly) | ~$1.50 |
| Live checks (Worker) | up to the daily limit you set |

## Privacy note

This repo is **public**, so `clients.json`, the data caches and the generated reports can all be browsed on GitHub. The reports carry `noindex` to keep them out of search results, but that doesn't make them private. For real privacy, either make the repo private (GitHub Pages from a private repo needs a paid GitHub plan) or publish `reports/` to Cloudflare Pages with Cloudflare Access in front.

## Run locally

```bash
python fetch_ai_visibility.py --dry-run                 # show prompts, call nothing
python fetch_ai_visibility.py --slug pup-profile        # one client
python fetch_local.py --dry-run
python fetch_organic.py --slug pup-profile --force
python generate_reports_v2.py --slug pup-profile
```
