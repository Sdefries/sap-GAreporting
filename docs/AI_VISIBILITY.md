# Search & AI Visibility

Client reports now have a **Search & AI visibility** group in the left nav, built from Anthony's Results Driven Tracker layout:

| Section | What it shows | Data |
|---|---|---|
| **AI tracking** | AI visibility score, one card per AI, trend over time, prompt × AI table (click a cell to read the answer), per-AI tabs with the sites each AI cited, prompt ideas | `ai_visibility_cache.json` ← `fetch_ai_visibility.py` |
| **Classic SEO** | Search visibility score (% of keywords on page 1), average position, top 3 / top 10, AI Overview citations, trend charts, keyword table | `seo_cache.json` ← `fetch_seo.py` (SEO-enrolled clients) |
| **Competitors** | You vs tracked competitors in Google rankings and in AI answers | both caches |
| **Site health** | PageSpeed / Core Web Vitals (the old "SEO" section) | `seo_cache.json` |

`reports/index.html` is now an all-clients dashboard with grant score, compliance, AI visibility and search visibility per client.

Score bands everywhere: **Poor** < 20% · **Moderate** 20–50% · **Good** 50–80% · **Great** 80%+.

## How a prompt is scored

For each prompt, each AI's answer is checked for:

- **Named**: the answer mentions one of the client's `brand_names` (or its domain)
- **Cited**: the answer cites or links the client's website

An answer counts as *visible* if it's named or cited. The AI visibility score is visible answers ÷ all answers (prompts × AIs). When Google shows no AI Overview / AI Mode answer, that cell is "No AI answer" and counts as not visible.

## Setup: GitHub secrets

Each engine runs only when its secret is set, so you can switch them on one at a time.

| Secret | Turns on | Where to get it |
|---|---|---|
| `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD` | AI Overviews, AI Mode, Classic SEO rankings | dataforseo.com → API access |
| `OPENAI_API_KEY` | ChatGPT | platform.openai.com |
| `ANTHROPIC_API_KEY` | Claude, and the monthly AI-suggested prompt ideas | console.anthropic.com |
| `GEMINI_API_KEY` | Gemini | aistudio.google.com |
| `PERPLEXITY_API_KEY` | Perplexity | perplexity.ai → API |

Optional model overrides (environment variables): `OPENAI_MODEL` (default `gpt-4.1-mini`), `CLAUDE_MODEL` (default `claude-opus-5-5`), `GEMINI_MODEL` (default `gemini-2.5-flash`), `PERPLEXITY_MODEL` (default `sonar`).

The Claude engine sends requests with server-side refusal fallback enabled (`fallbacks: "default"`), so a request Claude declines is retried on a fallback model rather than recorded as a failed check.

## Schedule

`automation.yml` runs `fetch_ai` and `fetch_seo` every **Monday 5am PST**, before the 9am weekly reports. Both can also be run from **Actions → SAP Ad Grants Automation → Run workflow** (`fetch_ai`, `fetch_seo`).

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
"seo_location": "Los Angeles,California,United States"
```

- `prompts`: up to 10. Every client is seeded with 5. When clients pick prompts in the report's **Prompt ideas** tab, the "Track selected prompts" button emails them to scott@sponsorapurpose.org; add them here.
- `engines`: optional; defaults to all six.
- `competitors`: powers the Competitors section and red competitor tags. It's empty for every client until you fill it in.
- `seo_location`: optional; derived from `geo.locations[0]` (e.g. `Kalispell, MT` → `Kalispell,Montana,United States`).
- Classic SEO still requires `local_seo_enrolled: true` and `seo_keywords`.

## Cost (rough)

13 clients × 5 prompts × 6 AIs ≈ 390 checks a week, about **$5–10 per weekly run** in total. Claude is the biggest line (web search is $10 per 1,000 searches, plus tokens). Lower it by trimming `engines` or prompts per client.

## Run locally

```bash
python fetch_ai_visibility.py --dry-run                 # show prompts, call nothing
python fetch_ai_visibility.py --slug pup-profile        # one client
python fetch_ai_visibility.py --engines chatgpt,gemini  # some engines
python generate_reports_v2.py --slug pup-profile
```
