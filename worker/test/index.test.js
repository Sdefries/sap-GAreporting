// Run: npm test   (mocks every outside API — no keys or network needed)
import { test } from "node:test";
import assert from "node:assert/strict";
import worker, { tokenFor } from "../src/index.js";

const clients = [{ slug: "pup-profile", name: "Pup Profile", website: "https://pupprofile.org",
  geo: { locations: ["Los Angeles, CA"] }, keywords: { include_themes: ["dog adoption"] },
  ai_tracking: { enabled: true, brand_names: ["Pup Profile"], prompts: ["Where can I adopt a dog in LA?"] },
  competitors: [{ name: "Best Friends", domain: "bestfriends.org" }] }];

function kv() { const m = new Map(); return { get: async (k) => m.get(k) ?? null, put: async (k, v) => void m.set(k, v) }; }
const b64 = (s) => Buffer.from(s).toString("base64");
let committed = null;
let dispatched = null;
const plans = JSON.parse(await (await import("node:fs/promises")).readFile(new URL("../../plans.json", import.meta.url), "utf8"));

globalThis.fetch = async (url, opts = {}) => {
  const u = String(url);
  const ok = (body) => new Response(JSON.stringify(body), { status: 200 });
  if (u.includes("api.github.com") && u.includes("plans.json")) return ok({ sha: "p", content: b64(JSON.stringify(plans)) });
  if (u.includes("api.github.com") && (!opts.method || opts.method === "GET")) return ok({ sha: "abc", content: b64(JSON.stringify(clients)) });
  if (u.includes("/dispatches")) { dispatched = JSON.parse(opts.body); return new Response(null, { status: 204 }); }
  if (u.includes("api.github.com") && opts.method === "PUT") { committed = JSON.parse(opts.body); return ok({}); }
  if (u.includes("openai.com")) return ok({ output: [{ type: "message", content: [{ type: "output_text", text: "Try Pup Profile.", annotations: [{ type: "url_citation", url: "https://www.pupprofile.org/adopt" }] }] }] });
  if (u.includes("perplexity.ai")) return ok({ choices: [{ message: { content: "Best Friends has dogs." } }], search_results: [{ url: "https://bestfriends.org/x" }] });
  throw new Error("unexpected fetch " + u);
};

const env = { ADMIN_KEY: "admin-key-0123456789", REPORT_SIGNING_KEY: "secret", GITHUB_TOKEN: "t", GITHUB_REPO: "o/r", ALLOWED_ORIGINS: "https://sdefries.github.io",
  OPENAI_API_KEY: "k", PERPLEXITY_API_KEY: "k", DAILY_LIMIT: "2", LIMITS: kv() };
const call = (path, body) => worker.fetch(new Request("https://w.dev" + path, { method: "POST", headers: { Origin: "https://sdefries.github.io" }, body: JSON.stringify(body) }), env);

test("token matches the Python report generator", async () => {
  // python: hmac.new(b"secret", b"pup-profile", hashlib.sha256).hexdigest()[:32]
  assert.equal(await tokenFor("pup-profile", "secret"), "4645b590179abfbb4879e35abcaed44a");
});

test("rejects a bad token", async () => {
  const r = await call("/check", { slug: "pup-profile", token: "nope", prompt: "Where can I adopt a dog?" });
  assert.equal(r.status, 401);
});

test("live check scores each engine and enforces the daily limit", async () => {
  const token = await tokenFor("pup-profile", "secret");
  const r = await call("/check", { slug: "pup-profile", token, prompt: "Where can I adopt a dog?" });
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("Access-Control-Allow-Origin"), "https://sdefries.github.io");
  const d = await r.json();
  assert.equal(d.cells.chatgpt.state, "cited_named");
  assert.equal(d.cells.perplexity.state, "not_visible");
  assert.deepEqual(d.cells.perplexity.comps, { "bestfriends.org": { named: true, cited: true } });
  assert.equal(d.cells.claude.state, "untracked");
  assert.equal(d.visible_in, 1);
  assert.equal(d.of, 2);
  await call("/check", { slug: "pup-profile", token, prompt: "Where can I adopt a dog?" });
  const third = await call("/check", { slug: "pup-profile", token, prompt: "Where can I adopt a dog?" });
  assert.equal(third.status, 429);
});

test("track adds new prompts to clients.json", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  const r = await call("/track", { slug: "pup-profile", token, prompts: ["Where can I adopt a dog in LA?", "Dog rescues in LA that need fosters"] });
  const d = await r.json();
  assert.equal(r.status, 200);
  assert.deepEqual(d.added, ["Dog rescues in LA that need fosters"]);
  const written = JSON.parse(Buffer.from(committed.content, "base64").toString());
  assert.equal(written[0].ai_tracking.prompts.length, 2);
  assert.equal(committed.sha, "abc");
});

test("competitors: adds one from the report", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  const r = await call("/competitors", { slug: "pup-profile", token, action: "add", name: "Wags and Walks", domain: "https://www.WagsAndWalks.org/adopt" });
  const d = await r.json();
  assert.equal(r.status, 200);
  assert.deepEqual(d.competitors.map((c) => c.domain), ["bestfriends.org", "wagsandwalks.org"]);
  const written = JSON.parse(Buffer.from(committed.content, "base64").toString());
  assert.deepEqual(written[0].competitors[1], { name: "Wags and Walks", domain: "wagsandwalks.org", source: "report" });
});

test("competitors: rejects listing sites, the client's own site and bad input", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  for (const domain of ["yelp.com", "sf.yelp.com", "pupprofile.org", "not a website"]) {
    const r = await call("/competitors", { slug: "pup-profile", token, action: "add", domain });
    assert.equal(r.status, 400, domain);
  }
});

test("competitors: clients can track at most 3", async () => {
  env.LIMITS = kv();
  const saved = clients[0].competitors;
  clients[0].competitors = [{ name: "A", domain: "a.org" }, { name: "B", domain: "b.org" }, { name: "C", domain: "c.org" }];
  try {
    const token = await tokenFor("pup-profile", "secret");
    const r = await call("/competitors", { slug: "pup-profile", token, action: "add", domain: "d.org" });
    assert.equal(r.status, 400);
    assert.match((await r.json()).error, /up to 3/);
  } finally {
    clients[0].competitors = saved;
  }
});

test("competitors: removes one the client added, not the agency's", async () => {
  env.LIMITS = kv();
  const saved = clients[0].competitors;
  clients[0].competitors = [...saved, { name: "Wags", domain: "wagsandwalks.org", source: "report" }];
  try {
    const token = await tokenFor("pup-profile", "secret");
    const agency = await call("/competitors", { slug: "pup-profile", token, action: "remove", domain: "bestfriends.org" });
    assert.equal(agency.status, 403);
    const r = await call("/competitors", { slug: "pup-profile", token, action: "remove", domain: "wagsandwalks.org" });
    assert.equal(r.status, 200);
    assert.deepEqual((await r.json()).competitors.map((c) => c.domain), ["bestfriends.org"]);
  } finally {
    clients[0].competitors = saved;
  }
});

test("track: bad input is a 400, case duplicates collapse, no-ops don't use quota", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  assert.equal((await call("/track", { slug: "pup-profile", token, prompts: "not a list" })).status, 400);
  const r = await call("/track", { slug: "pup-profile", token, prompts: ["Dog rescue near Pasadena", "dog rescue near pasadena"] });
  assert.deepEqual((await r.json()).added, ["Dog rescue near Pasadena"]);
  for (let i = 0; i < 5; i++) {
    const again = await call("/track", { slug: "pup-profile", token, prompts: ["Where can I adopt a dog in LA?"] });
    assert.equal(again.status, 200);  // already tracked: answered without spending the daily limit (2)
  }
});

test("writes clients.json with non-ASCII escaped like Python", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  await call("/track", { slug: "pup-profile", token, prompts: ["Rescates de perros en español"] });
  const raw = Buffer.from(committed.content, "base64").toString();
  assert.match(raw, /espa\\u00f1ol/);
});

test("upstream errors aren't sent to the browser", async () => {
  env.LIMITS = kv();
  const token = await tokenFor("pup-profile", "secret");
  const saved = env.OPENAI_API_KEY;
  const r = await call("/check", { slug: "pup-profile", token, prompt: "Where can I adopt a dog?" });
  const d = await r.json();
  for (const c of Object.values(d.cells)) assert.ok(!/HTTP \d/.test(c.error || ""));
  env.OPENAI_API_KEY = saved;
});

test("admin: wrong key is refused", async () => {
  env.LIMITS = kv();
  const r = await call("/admin/clients", { admin_key: "nope" });
  assert.equal(r.status, 401);
});

test("admin: lists clients with their plans", async () => {
  const r = await call("/admin/clients", { admin_key: "admin-key-0123456789" });
  const d = await r.json();
  assert.equal(r.status, 200);
  assert.ok(d.plans.plans.growth);
  assert.deepEqual(d.clients[0], { slug: "pup-profile", name: "Pup Profile", plan: null, addons: [], size: null, processing: false, demo: false, competitors: 1 });
});

test("admin: sets a plan and add-ons without using the client's daily limit", async () => {
  env.LIMITS = kv();
  const r = await call("/admin/plan", { admin_key: "admin-key-0123456789", slug: "pup-profile", plan: "growth", addons: ["local"], size: "small", processing: true });
  assert.equal(r.status, 200);
  const written = JSON.parse(Buffer.from(committed.content, "base64").toString());
  assert.equal(written[0].plan, "growth");
  assert.deepEqual(written[0].addons, ["local"]);
  assert.equal(written[0].size, "small");
  assert.equal(written[0].processing, true);
  assert.equal((await call("/admin/plan", { admin_key: "admin-key-0123456789", slug: "pup-profile", size: "huge" })).status, 400);
  const bad = await call("/admin/plan", { admin_key: "admin-key-0123456789", slug: "pup-profile", plan: "platinum" });
  assert.equal(bad.status, 400);
});

test("admin: rebuild starts the reports job", async () => {
  const r = await call("/admin/rebuild", { admin_key: "admin-key-0123456789" });
  assert.equal(r.status, 200);
  assert.deepEqual(dispatched.inputs, { job: "reports" });
});
