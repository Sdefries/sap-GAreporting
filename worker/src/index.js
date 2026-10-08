/**
 * SAP live AEO API — Cloudflare Worker
 * ─────────────────────────────────────────────────────────────────────────────
 * Lets clients use the AI tracking section of their report interactively,
 * without exposing API keys in the (static, GitHub Pages) report:
 *
 *   POST /check  {slug, token, prompt}     ask one prompt in every AI right now
 *   POST /ideas  {slug, token}             generate 15 prompt ideas with Claude
 *   POST /track  {slug, token, prompts[]}  add prompts to the client's weekly tracking
 *                                          (commits to clients.json on GitHub)
 *
 * AUTH   token = first 32 hex chars of HMAC-SHA256(REPORT_SIGNING_KEY, slug).
 *        generate_reports_v2.py computes the same token and puts it in each
 *        client's report, so a report can only act for its own client.
 * LIMITS DAILY_LIMIT live checks per client per day (KV namespace LIMITS).
 *
 * Scoring mirrors fetch_ai_visibility.py so live results match the weekly run.
 * Setup: see worker/README.md.
 */
import Anthropic from "@anthropic-ai/sdk";

const ENGINES = ["ai_overviews", "ai_mode", "chatgpt", "claude", "gemini", "perplexity"];
const MAX_PROMPTS = 10;
const EXCERPT_LEN = 1800;

// ── HTTP helpers ─────────────────────────────────────────────────────────────

function cors(env, req) {
  const origin = req.headers.get("Origin") || "";
  const allowed = (env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);
  return {
    "Access-Control-Allow-Origin": allowed.includes(origin) ? origin : allowed[0] || "",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    Vary: "Origin",
  };
}

function json(body, status, headers) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", ...headers } });
}

async function postJSON(url, payload, headers) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(payload) });
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

// ── Auth + limits ────────────────────────────────────────────────────────────

async function tokenFor(slug, key) {
  const k = await crypto.subtle.importKey("raw", new TextEncoder().encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", k, new TextEncoder().encode(slug)));
  return [...sig].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

async function authorized(env, slug, token) {
  if (!slug || !token || !env.REPORT_SIGNING_KEY) return false;
  const expected = await tokenFor(slug, env.REPORT_SIGNING_KEY);
  if (expected.length !== token.length) return false;
  let diff = 0;
  for (let i = 0; i < expected.length; i++) diff |= expected.charCodeAt(i) ^ token.charCodeAt(i);
  return diff === 0;
}

async function takeQuota(env, slug, cost = 1) {
  const limit = parseInt(env.DAILY_LIMIT || "10", 10);
  const key = `${slug}:${new Date().toISOString().slice(0, 10)}`;
  const used = parseInt((await env.LIMITS.get(key)) || "0", 10);
  if (used + cost > limit) return { ok: false, used, limit };
  await env.LIMITS.put(key, String(used + cost), { expirationTtl: 60 * 60 * 48 });
  return { ok: true, used: used + cost, limit };
}

// ── Client config (clients.json on GitHub) ───────────────────────────────────

async function githubFile(env, path = "clients.json") {
  const r = await fetch(`https://api.github.com/repos/${env.GITHUB_REPO}/contents/${path}?ref=${env.GITHUB_BRANCH || "main"}`, {
    headers: { Authorization: `Bearer ${env.GITHUB_TOKEN}`, "User-Agent": "sap-aeo-worker", Accept: "application/vnd.github+json" },
  });
  if (!r.ok) throw new Error(`GitHub read failed: ${r.status}`);
  const f = await r.json();
  const bytes = Uint8Array.from(atob(f.content.replace(/\n/g, "")), (c) => c.charCodeAt(0));
  return { sha: f.sha, clients: JSON.parse(new TextDecoder().decode(bytes)) };
}

function domainOf(url) {
  if (!url) return "";
  try {
    const host = new URL(url.includes("//") ? url : "https://" + url).hostname.toLowerCase();
    return host.startsWith("www.") ? host.slice(4) : host;
  } catch {
    return "";
  }
}

const US_STATES = { AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California", CO: "Colorado", CT: "Connecticut", DE: "Delaware", DC: "District of Columbia", FL: "Florida", GA: "Georgia", HI: "Hawaii", ID: "Idaho", IL: "Illinois", IN: "Indiana", IA: "Iowa", KS: "Kansas", KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland", MA: "Massachusetts", MI: "Michigan", MN: "Minnesota", MS: "Mississippi", MO: "Missouri", MT: "Montana", NE: "Nebraska", NV: "Nevada", NH: "New Hampshire", NJ: "New Jersey", NM: "New Mexico", NY: "New York", NC: "North Carolina", ND: "North Dakota", OH: "Ohio", OK: "Oklahoma", OR: "Oregon", PA: "Pennsylvania", RI: "Rhode Island", SC: "South Carolina", SD: "South Dakota", TN: "Tennessee", TX: "Texas", UT: "Utah", VT: "Vermont", VA: "Virginia", WA: "Washington", WV: "West Virginia", WI: "Wisconsin", WY: "Wyoming" };

function clientConfig(c) {
  const t = c.ai_tracking || {};
  const name = c.name.replace(/\s+(Grant Account|Inc\.?|LLC)$/i, "").trim();
  let location = c.seo_location || "United States";
  const loc = (c.geo?.locations || [])[0] || "";
  if (!c.seo_location && loc.includes(",")) {
    const i = loc.indexOf(",");
    const [city, st] = [loc.slice(0, i).trim(), loc.slice(i + 1).trim()];
    if (US_STATES[st.toUpperCase()]) location = `${city},${US_STATES[st.toUpperCase()]},United States`;
  } else if (!c.seo_location && Object.values(US_STATES).includes(loc)) location = `${loc},United States`;
  return {
    brandNames: t.brand_names?.length ? t.brand_names : [name],
    domain: domainOf(c.website),
    engines: t.engines?.length ? t.engines : ENGINES,
    prompts: t.prompts || [],
    competitors: (c.competitors || []).map((x) => ({ name: x.name || x.domain, domain: domainOf(x.domain) })).filter((x) => x.domain),
    location,
  };
}

// ── Engines (same sources as fetch_ai_visibility.py) ─────────────────────────

function answer(text = "", sources = [], status = "ok") {
  const seen = new Set();
  const clean = [];
  for (const s of sources) {
    const d = domainOf(s.url) || domainOf(s.domain) || (s.title || "").toLowerCase();
    const k = s.url || d;
    if (!d || seen.has(k)) continue;
    seen.add(k);
    clean.push(d);
  }
  return { status: text || status !== "ok" ? status : "no_answer", text: (text || "").trim(), sites: clean };
}

function aiBlock(items) {
  const texts = [];
  const refs = [];
  const walk = (node, wantText) => {
    if (Array.isArray(node)) return node.forEach((v) => walk(v, wantText));
    if (!node || typeof node !== "object") return;
    if (wantText) {
      for (const key of ["markdown", "text"]) {
        if (typeof node[key] === "string") {
          texts.push(node[key]);
          wantText = key !== "markdown";
          break;
        }
      }
    }
    for (const r of node.references || []) refs.push({ url: r.url, title: r.title || r.source, domain: r.domain });
    for (const [k, v] of Object.entries(node)) if (k !== "references" && typeof v === "object") walk(v, wantText);
  };
  const blocks = items.filter((i) => i.type === "ai_overview");
  walk(blocks, true);
  return { present: blocks.length > 0, text: [...new Set(texts)].join("\n"), refs };
}

async function dfs(env, endpoint, prompt, location, extra = {}) {
  const auth = "Basic " + btoa(`${env.DATAFORSEO_LOGIN}:${env.DATAFORSEO_PASSWORD}`);
  const run = async (loc) => {
    const d = await postJSON(`https://api.dataforseo.com/v3/${endpoint}`, [{ keyword: prompt, location_name: loc, language_code: "en", ...extra }], { Authorization: auth });
    const task = (d.tasks || [{}])[0];
    if (task.status_code !== 20000) throw new Error(`DataForSEO ${task.status_code}: ${task.status_message}`);
    return ((task.result || [{}])[0] || {}).items || [];
  };
  try {
    return await run(location);
  } catch (e) {
    if (location !== "United States" && /location/i.test(e.message)) return run("United States");
    throw e;
  }
}

const ENGINE_FUNCS = {
  async ai_overviews(env, prompt, cfg) {
    const b = aiBlock(await dfs(env, "serp/google/organic/live/advanced", prompt, cfg.location, { device: "desktop", load_async_ai_overview: true }));
    return answer(b.text, b.refs, b.present ? "ok" : "no_answer");
  },
  async ai_mode(env, prompt, cfg) {
    const b = aiBlock(await dfs(env, "serp/google/ai_mode/live/advanced", prompt, cfg.location));
    return answer(b.text, b.refs, b.present ? "ok" : "no_answer");
  },
  async chatgpt(env, prompt) {
    const d = await postJSON("https://api.openai.com/v1/responses", { model: env.OPENAI_MODEL || "gpt-4.1-mini", input: prompt, tools: [{ type: "web_search" }] }, { Authorization: `Bearer ${env.OPENAI_API_KEY}` });
    const texts = [];
    const sources = [];
    for (const item of d.output || []) {
      if (item.type !== "message") continue;
      for (const part of item.content || []) {
        if (part.type !== "output_text") continue;
        texts.push(part.text || "");
        for (const a of part.annotations || []) if (a.type === "url_citation") sources.push({ url: a.url, title: a.title });
      }
    }
    return answer(texts.join("\n"), sources);
  },
  async claude(env, prompt) {
    const client = new Anthropic({ apiKey: env.ANTHROPIC_API_KEY });
    const messages = [{ role: "user", content: prompt }];
    const texts = [];
    const sources = [];
    for (let i = 0; i < 4; i++) {
      const resp = await client.beta.messages.create({
        model: env.CLAUDE_MODEL || "claude-opus-5-5",
        max_tokens: 16000,
        betas: ["server-side-fallback-2026-07-01"],
        fallbacks: "default",
        output_config: { effort: "low" },
        tools: [{ type: "web_search_20260209", name: "web_search", max_uses: 5 }],
        messages,
      });
      if (resp.stop_reason === "refusal") return { status: "error", error: "Claude declined to answer", text: "", sites: [] };
      for (const block of resp.content) {
        if (block.type !== "text") continue;
        texts.push(block.text);
        for (const c of block.citations || []) if (c.url) sources.push({ url: c.url, title: c.title });
      }
      if (resp.stop_reason !== "pause_turn") break;
      messages.push({ role: "assistant", content: resp.content });
    }
    return answer(texts.join(""), sources);
  },
  async gemini(env, prompt) {
    const model = env.GEMINI_MODEL || "gemini-2.5-flash";
    const d = await postJSON(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`, { contents: [{ parts: [{ text: prompt }] }], tools: [{ google_search: {} }] }, { "x-goog-api-key": env.GEMINI_API_KEY });
    const cand = (d.candidates || [{}])[0];
    const text = (cand.content?.parts || []).map((p) => p.text || "").join("");
    const sources = (cand.groundingMetadata?.groundingChunks || []).map((c) => ({ url: "", domain: c.web?.title || "", title: c.web?.title || "" }));
    return answer(text, sources);
  },
  async perplexity(env, prompt) {
    const d = await postJSON("https://api.perplexity.ai/chat/completions", { model: env.PERPLEXITY_MODEL || "sonar", messages: [{ role: "user", content: prompt }] }, { Authorization: `Bearer ${env.PERPLEXITY_API_KEY}` });
    const text = d.choices?.[0]?.message?.content || "";
    let sources = (d.search_results || []).map((r) => ({ url: r.url, title: r.title }));
    if (!sources.length) sources = (d.citations || []).map((u) => ({ url: u }));
    return answer(text, sources);
  },
};

function engineAvailable(env, e) {
  return {
    ai_overviews: !!(env.DATAFORSEO_LOGIN && env.DATAFORSEO_PASSWORD),
    ai_mode: !!(env.DATAFORSEO_LOGIN && env.DATAFORSEO_PASSWORD),
    chatgpt: !!env.OPENAI_API_KEY,
    claude: !!env.ANTHROPIC_API_KEY,
    gemini: !!env.GEMINI_API_KEY,
    perplexity: !!env.PERPLEXITY_API_KEY,
  }[e];
}

// ── Scoring (mirrors fetch_ai_visibility.py) ─────────────────────────────────

function mentions(text, names) {
  const low = (text || "").toLowerCase();
  return names.some((n) => {
    n = (n || "").trim().toLowerCase();
    return n.length >= 3 && new RegExp(`(?<![a-z0-9])${n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![a-z0-9])`).test(low);
  });
}

function domainHit(domain, res) {
  if (!domain) return false;
  return res.sites.some((s) => s === domain || s.endsWith("." + domain)) || (res.text || "").toLowerCase().includes(domain);
}

function score(res, cfg) {
  const named = mentions(res.text, cfg.brandNames) || mentions(res.text, [cfg.domain]);
  const cited = domainHit(cfg.domain, res);
  const comps = {};
  for (const c of cfg.competitors) {
    const v = { named: mentions(res.text, [c.name, c.domain]), cited: domainHit(c.domain, res) };
    if (v.named || v.cited) comps[c.domain] = v;
  }
  const state = cited && named ? "cited_named" : cited ? "cited" : named ? "named" : res.status === "no_answer" ? "no_answer" : "not_visible";
  return { state, named, cited, sites: res.sites.slice(0, 12), excerpt: res.text.slice(0, EXCERPT_LEN), comps, error: "" };
}

// ── Routes ───────────────────────────────────────────────────────────────────

async function routeCheck(env, client, body) {
  const prompt = String(body.prompt || "").trim().slice(0, 300);
  if (prompt.length < 5) return [400, { error: "Type a question to check." }];
  const cfg = clientConfig(client);
  const engines = cfg.engines.filter((e) => ENGINES.includes(e));
  const quota = await takeQuota(env, client.slug);
  if (!quota.ok) return [429, { error: `You've used today's ${quota.limit} live checks. They reset tomorrow, and tracked prompts still update every week.` }];
  const cells = {};
  await Promise.all(engines.map(async (e) => {
    if (!engineAvailable(env, e)) return (cells[e] = { state: "untracked", named: false, cited: false, sites: [], excerpt: "", comps: {}, error: "" });
    try {
      const res = await ENGINE_FUNCS[e](env, prompt, cfg);
      if (res.status === "error") console.error(`${e} check error:`, res.error);
      cells[e] = res.status === "error" ? { state: "error", named: false, cited: false, sites: [], excerpt: "", comps: {}, error: "This AI didn't answer this time." } : score(res, cfg);
    } catch (err) {
      console.error(`${e} check failed:`, err.message);  // upstream detail stays in the Worker log
      cells[e] = { state: "error", named: false, cited: false, sites: [], excerpt: "", comps: {}, error: "This AI didn't answer this time." };
    }
  }));
  // "No AI answer" isn't counted, same as the weekly score
  const answered = engines.filter((e) => !["untracked", "error", "no_answer"].includes(cells[e].state));
  const visible = answered.filter((e) => ["cited_named", "cited", "named"].includes(cells[e].state));
  return [200, { prompt, cells, visible_in: visible.length, of: answered.length, checked: new Date().toISOString(), quota }];
}

async function routeIdeas(env, client) {
  if (!env.ANTHROPIC_API_KEY) return [503, { error: "Prompt ideas aren't switched on yet." }];
  const quota = await takeQuota(env, client.slug);
  if (!quota.ok) return [429, { error: `You've used today's ${quota.limit} live actions. Try again tomorrow.` }];
  const cfg = clientConfig(client);
  const brief = [
    `Organization: ${cfg.brandNames[0]} (${client.org_type || "nonprofit"})`,
    `Website: ${client.website || "n/a"}`,
    `Service area: ${(client.geo?.locations || []).join(", ") || "national"}`,
    `Programs / themes: ${(client.keywords?.include_themes || []).join(", ")}`,
    `Prompts already tracked: ${JSON.stringify(cfg.prompts)}`,
  ].join("\n");
  const resp = await new Anthropic({ apiKey: env.ANTHROPIC_API_KEY }).beta.messages.create({
    model: env.CLAUDE_MODEL || "claude-opus-5-5",
    max_tokens: 4000,
    betas: ["server-side-fallback-2026-07-01"],
    fallbacks: "default",
    output_config: {
      effort: "low",
      format: { type: "json_schema", schema: { type: "object", properties: { prompts: { type: "array", items: { type: "string" } } }, required: ["prompts"], additionalProperties: false } },
    },
    system: "You help a nonprofit marketing agency choose prompts to monitor in AI assistants. Write questions exactly as a real person would type them into ChatGPT or Google when looking to donate, adopt, volunteer, get help, or attend — not questions about the organization by name. Mix local and general intent. No duplicates of tracked prompts.",
    messages: [{ role: "user", content: brief + "\n\nSuggest 15 prompts." }],
  });
  if (resp.stop_reason === "refusal") return [502, { error: "Couldn't generate ideas this time." }];
  const text = resp.content.find((b) => b.type === "text")?.text || "{}";
  let ideas;
  try { ideas = JSON.parse(text).prompts || []; } catch { return [502, { error: "Couldn't generate ideas this time." }]; }
  return [200, { prompts: ideas.map((p) => String(p).trim()).filter(Boolean).slice(0, 15) }];
}

function toBase64(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(bin);
}

// Same layout as Python's json.dump(indent=2): non-ASCII written as \uXXXX,
// so a write from the report doesn't reformat the whole of clients.json.
function clientsJSON(clients) {
  return JSON.stringify(clients, null, 2).replace(/[\u007f-\uffff]/g, (c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0")) + "\n";
}

// Read clients.json, apply change(client) and commit it, retrying when someone
// else wrote first. change returns {error,status} to stop, {noop} to skip the
// write, or {message} to commit. Quota is only taken when there's a real write.
async function updateClient(env, slug, change, { charge = true } = {}) {
  let charged = !charge;
  for (let attempt = 0; attempt < 4; attempt++) {
    const { sha, clients } = await githubFile(env);
    const c = clients.find((x) => x.slug === slug);
    if (!c) return [404, { error: "Client not found." }];
    const out = change(c);
    if (out.error) return [out.status || 400, { error: out.error }];
    if (out.noop) return [200, out.noop];
    if (!charged) {
      const quota = await takeQuota(env, slug);
      if (!quota.ok) return [429, { error: `You've used today's ${quota.limit} live actions. Try again tomorrow.` }];
      charged = true;
    }
    const r = await fetch(`https://api.github.com/repos/${env.GITHUB_REPO}/contents/clients.json`, {
      method: "PUT",
      headers: { Authorization: `Bearer ${env.GITHUB_TOKEN}`, "User-Agent": "sap-aeo-worker", Accept: "application/vnd.github+json" },
      body: JSON.stringify({ message: out.message, content: toBase64(new TextEncoder().encode(clientsJSON(clients))), sha, branch: env.GITHUB_BRANCH || "main" }),
    });
    if (r.ok) return [200, out.result(c)];
    if (r.status !== 409 && r.status !== 422) throw new Error(`GitHub write failed: ${r.status}`);
    await new Promise((res) => setTimeout(res, 300 + Math.random() * 700));  // someone else wrote first
  }
  return [503, { error: "Busy right now, please try again." }];
}

async function routeTrack(env, body) {
  const seen = new Set();
  const wanted = (Array.isArray(body.prompts) ? body.prompts : []).slice(0, 20)
    .map((p) => String(p).replace(/\s+/g, " ").trim().slice(0, 300))
    .filter((p) => p.length >= 5 && !seen.has(p.toLowerCase()) && seen.add(p.toLowerCase()));
  if (!wanted.length) return [400, { error: "Pick at least one prompt." }];
  return updateClient(env, body.slug, (c) => {
    const current = c.ai_tracking?.prompts?.length ? c.ai_tracking.prompts : [];
    const lower = new Set(current.map((p) => p.toLowerCase()));
    const add = wanted.filter((p) => !lower.has(p.toLowerCase()));
    if (!add.length) return { noop: { added: [], prompts: current, message: "Those prompts are already tracked." } };
    if (current.length + add.length > MAX_PROMPTS) {
      return { error: `You can track up to ${MAX_PROMPTS} prompts (${current.length} tracked now). Pick ${Math.max(0, MAX_PROMPTS - current.length)} or fewer, or ask us to swap some out.` };
    }
    c.ai_tracking = c.ai_tracking || { enabled: true };
    c.ai_tracking.prompts = [...current, ...add];
    return { message: `Track ${add.length} new AI prompt${add.length > 1 ? "s" : ""} for ${c.slug} (from client report)`,
             result: (cc) => ({ added: add, prompts: cc.ai_tracking.prompts }) };
  });
}

// Sites that are platforms or listings, not peer organizations (mirrors
// DIRECTORIES in visibility_report.py)
const DIRECTORIES = new Set(["yelp.com", "reddit.com", "facebook.com", "instagram.com", "youtube.com", "wikipedia.org",
  "google.com", "tripadvisor.com", "nextdoor.com", "linkedin.com", "x.com", "twitter.com", "tiktok.com", "pinterest.com",
  "quora.com", "medium.com", "petfinder.com", "adoptapet.com", "rescueme.org", "charitynavigator.org", "guidestar.org",
  "candid.org", "greatnonprofits.org", "idealist.org", "volunteermatch.org", "eventbrite.com", "gofundme.com",
  "givebutter.com", "bbb.org", "yellowpages.com", "mapquest.com", "foursquare.com", "patch.com", "indeed.com",
  "glassdoor.com", "apple.com", "bing.com", "chatgpt.com", "openai.com", "perplexity.ai"]);
const MAX_COMPETITORS = 3; // clients can track up to 3 from their report; more can be set in clients.json

function isDirectory(d) {
  return [...DIRECTORIES].some((x) => d === x || d.endsWith("." + x)) || d.endsWith(".gov") || d.endsWith(".edu");
}

// Add or remove a tracked competitor from the client's report.
// body: {action: "add" | "remove", domain, name}. Clients can only remove
// competitors they added from the report; the agency's own picks stay.
async function routeCompetitors(env, body) {
  const action = body.action === "remove" ? "remove" : "add";
  const domain = domainOf(String(body.domain || "").trim().slice(0, 200));
  const name = String(body.name || "").replace(/\s+/g, " ").trim().slice(0, 80);
  if (!/^[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(domain)) return [400, { error: "Enter the organization's website, like bestfriends.org." }];
  if (action === "add" && isDirectory(domain)) return [400, { error: `${domain} is a listing or social site, not an organization. Add the organization's own website.` }];
  return updateClient(env, body.slug, (c) => {
    const list = Array.isArray(c.competitors) ? c.competitors : [];
    const found = list.find((x) => domainOf(x.domain) === domain);
    const result = (cc) => ({ competitors: cc.competitors });
    if (action === "add") {
      if (domain === domainOf(c.website || "")) return { error: "That's your own website." };
      if (found) return { noop: { competitors: list, message: "Already tracked." } };
      if (list.length >= MAX_COMPETITORS) return { error: `You can track up to ${MAX_COMPETITORS} competitors. Remove one first.` };
      c.competitors = [...list, { name: name || domain, domain, source: "report" }];
      return { message: `Track competitor ${domain} for ${c.slug} (from client report)`, result };
    }
    if (!found) return { noop: { competitors: list, message: "Not tracked." } };
    if (found.source !== "report") return { error: "Your account manager set this competitor. Ask them to change it.", status: 403 };
    c.competitors = list.filter((x) => x !== found);
    return { message: `Stop tracking competitor ${domain} for ${c.slug} (from client report)`, result };
  });
}

// ── Admin (agency only): set plans, rebuild reports ──────────────────────────
// Separate key (ADMIN_KEY, 16+ characters). Wrong keys are counted so the key
// can't be guessed by brute force.

async function adminAuthorized(env, key) {
  const want = env.ADMIN_KEY || "";
  if (want.length < 16 || typeof key !== "string" || key.length !== want.length) return false;
  let diff = 0;
  for (let i = 0; i < want.length; i++) diff |= want.charCodeAt(i) ^ key.charCodeAt(i);
  return diff === 0;
}

async function adminFailed(env, req) {
  const ip = req.headers.get("CF-Connecting-IP") || "unknown";
  const k = `admin-fail:${ip}:${new Date().toISOString().slice(0, 10)}`;
  const n = parseInt((await env.LIMITS.get(k)) || "0", 10) + 1;
  await env.LIMITS.put(k, String(n), { expirationTtl: 60 * 60 * 48 });
  return n;
}

async function routeAdmin(env, path, body) {
  const plans = (await githubFile(env, "plans.json")).clients;  // githubFile parses any JSON file
  if (path === "/admin/clients") {
    const { clients } = await githubFile(env);
    return [200, { plans, clients: clients.map((c) => ({ slug: c.slug, name: c.name, plan: c.plan || null,
      addons: c.addons || [], size: c.size || null, processing: !!c.processing,
      demo: !!c.demo, competitors: (c.competitors || []).length })) }];
  }
  if (path === "/admin/plan") {
    const plan = body.plan || null;
    if (plan && !plans.plans[plan]) return [400, { error: `Unknown plan "${plan}".` }];
    const addons = [...new Set((Array.isArray(body.addons) ? body.addons : []).map(String))];
    const bad = addons.filter((a) => !plans.packages[a]);
    if (bad.length) return [400, { error: `Unknown add-on: ${bad.join(", ")}.` }];
    const size = body.size || null;
    if (size && !(plans.sizes || {})[size]) return [400, { error: `Unknown size "${size}".` }];
    const processing = body.processing === true;
    return updateClient(env, body.slug, (c) => {
      if ((c.plan || null) === plan && JSON.stringify(c.addons || []) === JSON.stringify(addons)
          && (c.size || null) === size && !!c.processing === processing) return { noop: { saved: true, message: "No change." } };
      if (plan) c.plan = plan; else delete c.plan;
      if (addons.length) c.addons = addons; else delete c.addons;
      if (size) c.size = size; else delete c.size;
      if (processing) c.processing = true; else delete c.processing;
      return { message: `Set ${c.slug} to ${plan || "everything"}${addons.length ? " + " + addons.join(", ") : ""}${size ? ", " + size : ""}${processing ? ", processing" : ""} (admin)`,
               result: () => ({ saved: true }) };
    }, { charge: false });
  }
  if (path === "/admin/rebuild") {
    const r = await fetch(`https://api.github.com/repos/${env.GITHUB_REPO}/actions/workflows/automation.yml/dispatches`, {
      method: "POST",
      headers: { Authorization: `Bearer ${env.GITHUB_TOKEN}`, "User-Agent": "sap-aeo-worker", Accept: "application/vnd.github+json" },
      body: JSON.stringify({ ref: env.GITHUB_BRANCH || "main", inputs: { job: "reports" } }),
    });
    if (!r.ok) throw new Error(`Workflow dispatch failed: ${r.status}`);
    return [200, { started: true }];
  }
  return [404, { error: "Not found" }];
}

export default {
  async fetch(req, env) {
    const headers = cors(env, req);
    if (req.method === "OPTIONS") return new Response(null, { headers });
    if (req.method !== "POST") return json({ error: "Not found" }, 404, headers);
    try {
      const body = await req.json().catch(() => ({}));
      const path = new URL(req.url).pathname;
      if (path.startsWith("/admin/")) {
        if (!(await adminAuthorized(env, body.admin_key))) {
          const n = await adminFailed(env, req);
          return json({ error: n > 20 ? "Too many wrong keys today. Try again tomorrow." : "Wrong admin key." }, n > 20 ? 429 : 401, headers);
        }
        const [status, out] = await routeAdmin(env, path, body);
        return json(out, status, headers);
      }
      if (!(await authorized(env, body.slug, body.token))) return json({ error: "This report link isn't authorized for live checks." }, 401, headers);
      if (path === "/track" || path === "/competitors") {
        const [status, out] = await (path === "/track" ? routeTrack : routeCompetitors)(env, body);
        return json(out, status, headers);
      }
      const { clients } = await githubFile(env);
      const client = clients.find((x) => x.slug === body.slug);
      if (!client) return json({ error: "Client not found." }, 404, headers);
      const route = { "/check": routeCheck, "/ideas": routeIdeas }[path];
      if (!route) return json({ error: "Not found" }, 404, headers);
      const [status, out] = await route(env, client, body);
      return json(out, status, headers);
    } catch (e) {
      console.error("request failed:", e.message);
      return json({ error: "Something went wrong. Please try again in a minute." }, 500, headers);
    }
  },
};

// Exported for tests
export { tokenFor, score, mentions, clientConfig, aiBlock, answer, isDirectory, domainOf };
