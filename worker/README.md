# Live AEO API (Cloudflare Worker)

This makes the AI tracking section of each client report interactive:

- **Test a prompt now**: the client types any question and sees, within about a minute, whether AI Overviews, AI Mode, ChatGPT, Claude, Gemini and Perplexity mention them.
- **Track this prompt weekly / Track selected prompts**: adds prompts to that client's `ai_tracking.prompts` in `clients.json` on GitHub. The next weekly run includes them. There's a limit of 10 prompts per client.
- **✦ Generate AI ideas**: 15 fresh prompt ideas from Claude.

Reports are static pages, so the API keys live here in the Worker, never in the page. Until the Worker is set up, these buttons stay hidden and "Track selected prompts" falls back to emailing scott@sponsorapurpose.org.

## How it's protected

- **Per-client link token.** Each report carries its own token, `HMAC(REPORT_SIGNING_KEY, slug)`. A report can only check and track prompts for its own client.
- **Daily limit.** `DAILY_LIMIT` live actions per client per day (default 10) caps spend. Anyone who has a client's report link can use that client's daily quota.
- **CORS.** Only `ALLOWED_ORIGINS` (your GitHub Pages site) can call it from a browser.

## One-time setup (about 15 minutes)

You need a free Cloudflare account and Node.js on a computer.

```bash
cd worker
npm install
npx wrangler login                        # opens Cloudflare in your browser
npx wrangler kv namespace create LIMITS   # paste the printed id into wrangler.toml
```

Add the secrets. Each command prompts you to paste the value.

```bash
npx wrangler secret put REPORT_SIGNING_KEY   # any long random string; the SAME value goes in GitHub (below)
npx wrangler secret put GITHUB_TOKEN         # fine-grained token: this repo only, Contents = Read and write
npx wrangler secret put DATAFORSEO_LOGIN
npx wrangler secret put DATAFORSEO_PASSWORD
npx wrangler secret put OPENAI_API_KEY
npx wrangler secret put ANTHROPIC_API_KEY
npx wrangler secret put GEMINI_API_KEY
npx wrangler secret put PERPLEXITY_API_KEY
npx wrangler deploy                          # prints the Worker URL, e.g. https://sap-aeo.<you>.workers.dev
```

Any engine without a key shows as "Not tracked".

Then in GitHub, under **repo → Settings → Secrets and variables → Actions**:

- **Secret** `REPORT_SIGNING_KEY`: the same random string as above.
- **Variable** `LIVE_API_URL`: the Worker URL from `wrangler deploy`.

The next weekly report run turns the live buttons on.

## Settings (`wrangler.toml`)

| Setting | Default | Meaning |
|---|---|---|
| `GITHUB_REPO` | `Sdefries/sap-GAreporting` | Repo holding `clients.json` |
| `GITHUB_BRANCH` | `main` | Branch the reports are built from |
| `ALLOWED_ORIGINS` | `https://sdefries.github.io` | Comma-separated sites allowed to call the Worker |
| `DAILY_LIMIT` | `10` | Live checks + actions per client per day |

## Cost

One live check calls every configured AI once, about $0.05–0.15. At the default limit, the worst case is about $1.50 per client per day. The Cloudflare free plan is enough.

## Test

```bash
npm test     # runs the Worker against mocked APIs; no keys needed
```
