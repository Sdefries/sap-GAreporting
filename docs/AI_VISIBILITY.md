# Search & AI Visibility

Client reports have **SEO**, **AEO** and **Competitors** pages in the left nav, modeled on Anthony's Results Driven Tracker and styled to match the Sponsor a Purpose CRM.

| Section | What it shows | Data |
|---|---|---|
| **Visibility overview** | Three headline scores (AI visibility, keyword visibility, authority), each with its band, change and biggest opportunity, plus the latest competitor website changes | all caches below |
| **AI tracking** | AI visibility score; a card per AI; **Visits from AI** (GA4: sessions from ChatGPT, Perplexity, Gemini, Copilot, Claude, and the pages they land on); **How AI describes you** (accuracy check); trend; prompt × AI table (click a cell to read the answer); per-AI tabs; "Who AI recommends instead" and "Sites AI trusts — get listed"; prompt ideas | `ai_visibility_cache.json` ← `fetch_ai_visibility.py`, `ga4_cache.json` ← `fetch_ga4.py` |
| **AEO action plan** | For prompts where AI recommends someone else: why, which sites to get listed on, and a page brief to publish (title, URL, outline, FAQs) | `ai_visibility_cache.json` (Claude) |
| **AEO readiness** | Website checklist (AI crawler access, llms.txt, sitemap, HTTPS, Organization schema, FAQ, readable text, title/description, contact & location, donate path) with fixes, plus **ready-to-paste llms.txt and schema** | `ai_visibility_cache.json` (no API key needed) |
| **Classic SEO** (keyword tracking) | Search visibility score, average position, top 3 / top 10, AI Overview citations, trends. Keyword table tracks Google positions to 50 with monthly search volume, best-ever position, trend line, "#1 is …" when you don't rank, filters (#1, top 3, top 10, moved up/down…), sorting, a **Competition** toggle and an **AI Overviews** tab showing which sites Google cited. "On page one with you" suggests competitors from the search results | `seo_cache.json` ← `fetch_seo.py` (SEO-package clients) |
| **Organic search** | SEMrush-style: every keyword the site ranks for, with volume, difficulty, estimated visits and change; position distribution; traffic value; top pages; competitors found from shared keywords; keyword opportunities; authority (backlinks, optional) | `organic_cache.json` ← `fetch_organic.py` (all clients with a website, monthly) |
| **Authority** | Authority score (0–100), referring domains with history, best links (authority 20+, 100+ visits a month, followed, not spam), links to win back, new vs lost, highest-authority sites, anchor text, vs competitors | `authority_cache.json` ← `fetch_authority.py` (monthly, needs `DATAFORSEO_BACKLINKS`) |
| **Competitors** | **Overview**: you vs tracked competitors in Google rankings and AI answers. **Head to head**: you vs one competitor, keyword by keyword and prompt by prompt. **Page changes**: edits to their homepage and top-level pages (title, heading, description, new text). **New pages**: pages added to their sitemap | caches above + `competitor_cache.json` ← `fetch_competitors.py` (weekly) |
| **Site health** | PageSpeed / Core Web Vitals, Search Console clicks and top queries | `seo_cache.json` |
| **Local map & profile** | Google Business Profile (rating, reviews, category, hours, claimed); a 7×7 Google Maps **heat map** per keyword (average rank, top 3 coverage, found in top 20, any business's grid); **Map pack** and **Google Maps** positions from each city served | `local_cache.json` ← `fetch_local.py` (clients with `local_tracking`) |

When the latest weekly data has gaps (an AI's checks failed, a data source is overdue, or a competitor site couldn't be loaded), the visibility overview opens with a **Last update had problems** note so nobody mistakes missing data for a drop.

**Live features** (test a prompt now, track prompts, generate ideas) run through a small Cloudflare Worker that keeps the API keys private. See [`worker/README.md`](../worker/README.md). Until it's set up, those buttons are hidden.

The report is split into **pages by product area**: Google Ads, Website (GA4), SEO (visibility overview, keyword tracking, organic search, authority, site health, local map), AEO (AI tracking, action plan, readiness), Competitors and Action plan. Pick one in the sidebar (or the menu on phones), use Previous / Next at the bottom, or link straight to a page or section, e.g. `pup-profile.html#seo` or `pup-profile.html#sec-ai`. The browser Back button works, and the PDF button still prints every page.

Other report changes: the **device breakdown** is now real Google Ads data, **What we did / What's next** is written from each month's data, and there's a **↓ PDF** button. `reports/index.html` is an all-clients dashboard.

Score bands for AI and keyword visibility: **Poor** < 20% · **Moderate** 20–50% · **Good** 50–80% · **Great** 80%+. Authority: **Poor** < 10 · **Moderate** 10–30 · **Good** 30–50 · **Great** 50+.

## Competitor website changes

`fetch_competitors.py` reads each tracked competitor's sitemap and checks its homepage and up to 14 top-level pages every week. It reports new pages, removed pages, and real content edits: a changed title, main heading or description, or at least two sentences added or removed (so dates and counters don't count). The first check of a site is a baseline; changes appear from the second week. With `ANTHROPIC_API_KEY` set, Claude adds one plain-English line per change on what it might signal. The cache stores hashes of URLs and sentences, not page copies, and keeps 4 months of changes.

## Plans, admin page and demo

**Plans.** `plans.json` defines the packages and plans:

| Plan | Includes |
|---|---|
| Essentials | Google Ads & GA4 |
| Growth | + SEO & AEO (visibility overview, AI tracking, AEO plan and readiness, keyword tracking, organic search, site health) |
| Pro | + Authority, Competitor tracking, Local map |

Set a client's plan in `clients.json` (`"plan": "growth"`, optional `"addons": ["local"]`), or on the admin page. A client with no plan sees everything. Sections outside a plan show an "isn't in your plan yet" card, their data is left out of the page, and the weekly jobs skip the paid API calls for them. Edit `plans.json` to change what each plan includes.

**Size and price.** Prices slide with the nonprofit's yearly revenue (`"size"` in `clients.json`, or the admin page). Monthly, in dollars:

| Size | Essentials | Growth | Pro | Each add-on | Checks |
|---|---|---|---|---|---|
| Under $250k a year (`small`) | 49 | 79 | 119 | 15 | Monthly |
| $250k–$1M a year (`mid`) | 99 | 149 | 229 | 25 | Monthly |
| Over $1M a year (`large`) | 400 | 750 | 1,250 | 100–125 | Weekly |

Small and mid-size clients who process donations with us (`"processing": true`) get Essentials free, so Growth costs the difference. Their paid checks (AI answers, keyword ranks, local map, competitors) run every 4 weeks instead of weekly, which keeps their API cost in line with the lower price; Google Ads, GA4 and site data still refresh weekly. A client with no size set runs weekly. Prices live in `plans.json` and only show on the admin page; nothing is charged from here.

**Admin page.** `reports/admin.html` (built with the reports). Enter the admin key to see every client, set their size, plan, add-ons and whether they process donations with us, see each client's monthly price and the total, save, and press **Rebuild reports now** to apply changes in a few minutes. It needs the Worker with `ADMIN_KEY` set (see `worker/README.md`); the page itself holds no client data.

**Demo.** The client with `"demo": true` (Maplewood Paws Rescue, a fictional rescue in Denver) is built from `demo_data.py`: realistic sample data for every section, run through the real scoring code, dated relative to today. It's at `reports/demo.html`. Change its plan on the admin page to show prospects what each plan looks like. Every fetcher, alert and digest skips demo clients, and live checks are off for it.

## How a prompt is scored

Each AI's answer to a prompt is checked for two things:

- **Named**: the answer mentions one of the client's `brand_names` (or its domain).
- **Cited**: the answer cites or links the client's website.

An answer counts as *visible* if it's named or cited. The AI visibility score is visible answers ÷ answers given. When Google shows no AI Overview / AI Mode answer for a search, that cell reads "No AI answer" and is **left out of the score** (for competitors too), because nobody can be recommended there. The summary and each AI's card say how many were left out.

## Setup: GitHub secrets and variables

Add these under **repo → Settings → Secrets and variables → Actions**. Each feature turns on when its key is present.

| Secret | Turns on | Where to get it |
|---|---|---|
| `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD` | AI Overviews, AI Mode, Classic SEO rankings, Organic search, Local map & profile | dataforseo.com → API access |
| `OPENAI_API_KEY` | ChatGPT | platform.openai.com |
| `ANTHROPIC_API_KEY` | Claude, AEO action plan, accuracy check, prompt ideas, starter prompts for new clients, notes on competitor changes | console.anthropic.com |
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

Every **Monday 5am PST** `automation.yml` runs `fetch_ai`, `fetch_seo`, `fetch_local`, `fetch_organic`, `fetch_competitors` and (when `DATAFORSEO_BACKLINKS` is `1`) `fetch_authority`. Organic search and authority refresh each client about once a month. The 9am weekly report run picks up the results. Any job can also be started from **Actions → SAP Ad Grants Automation → Run workflow**.

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
  "grid": 7, "spacing_miles": 1,
  "cities": ["Kalispell, MT", "Whitefish, MT"]
},
"report_notes": {"did": "Launched the spring adoption campaign...", "next": "..."},
"seo_location": "Los Angeles,California,United States",
"organic_tracking": {"enabled": false},
"locked_sections": ["authority", "competitors"]
```

- `ai_tracking.prompts`: up to 10. Every current client is seeded with 5. **New clients without prompts get 5 starter prompts written by Claude automatically.**
- `competitors`: clients can add, remove or one-click track up to **3** competitors from the Competitors page of their report (through the live Worker; without it they see your email). You can set more by editing this list. It powers the Competitors section (including website change monitoring) and red competitor tags. The report's "Who AI recommends instead" panel and the keyword table's "On page one with you" card suggest who to add.
- `local_tracking`: only for organizations people visit in person. It's seeded for the Humane Society of Northwest Montana and ScienceWorks. Coordinates are found automatically; add `lat`/`lng` or `place_id` if the wrong listing matches.
- `report_notes`: your team's own "What we did / What's next" text. It overrides the automatic text.
- `organic_tracking.enabled: false` skips a client in the organic report.
- `locked_sections`: sections the client's plan doesn't include. Choose from `ai`, `aeo`, `keywords`, `organic`, `authority`, `competitors`, `local`. Their data is left out of the generated report (so it can't be read from the page source), the sidebar shows a lock, and the page shows an "isn't in your plan yet" card with a button to ask about adding it.
- Classic SEO still requires `local_seo_enrolled: true` and `seo_keywords`.

## Cost (rough, per month, all 13 clients)

| Feature | Cost |
|---|---|
| AI tracking (5 prompts × 6 AIs, weekly) | $20–40 |
| AEO action plan + accuracy check (Claude, weekly) | $5–10 |
| Organic search (monthly) | ~$1 |
| Local map (2 clients × 3 keywords × 49 points, weekly) | ~$3 |
| Authority (monthly, plus the Backlinks API subscription) | $1–4 |
| Competitor changes (Claude notes only) | < $1 |
| Live checks (Worker) | up to the daily limit you set |

## Privacy note

This repo is **public**, so `clients.json`, the data caches and the generated reports can all be browsed on GitHub. The reports carry `noindex` to keep them out of search results, but that doesn't make them private. For real privacy, either make the repo private (GitHub Pages from a private repo needs a paid GitHub plan) or publish `reports/` to Cloudflare Pages with Cloudflare Access in front.

## Run locally

```bash
python fetch_ai_visibility.py --dry-run                 # show prompts, call nothing
python fetch_ai_visibility.py --slug pup-profile        # one client
python fetch_local.py --dry-run
python fetch_organic.py --slug pup-profile --force
python fetch_competitors.py --slug pup-profile
python generate_reports_v2.py --slug pup-profile
```
