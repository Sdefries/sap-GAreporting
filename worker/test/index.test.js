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

globalThis.fetch = async (url, opts = {}) => {
  const u = String(url);
  const ok = (body) => new Response(JSON.stringify(body), { status: 200 });
  if (u.includes("api.github.com") && (!opts.method || opts.method === "GET")) return ok({ sha: "abc", content: b64(JSON.stringify(clients)) });
  if (u.includes("api.github.com") && opts.method === "PUT") { committed = JSON.parse(opts.body); return ok({}); }
  if (u.includes("openai.com")) return ok({ output: [{ type: "message", content: [{ type: "output_text", text: "Try Pup Profile.", annotations: [{ type: "url_citation", url: "https://www.pupprofile.org/adopt" }] }] }] });
  if (u.includes("perplexity.ai")) return ok({ choices: [{ message: { content: "Best Friends has dogs." } }], search_results: [{ url: "https://bestfriends.org/x" }] });
  throw new Error("unexpected fetch " + u);
};

const env = { REPORT_SIGNING_KEY: "secret", GITHUB_TOKEN: "t", GITHUB_REPO: "o/r", ALLOWED_ORIGINS: "https://sdefries.github.io",
  OPENAI_API_KEY: "k", PERPLEXITY_API_KEY: "k", DAILY_LIMIT: "2", LIMITS: kv() };
const call = (path, body) => worker.fetch(new Request("https://w.dev" + path, { method: "POST", headers: { Origin: "https://sdefries.github.io" }, body: JSON.stringify(body) }), env);

test("token matches the Python report generator", async () => {
  // python: hmac.new(b"secret", b"pup-profile", hashlib.sha256).hexdigest()[:32]
  assert.equal(await tokenFor("pup-profile", "secret"), process.env.PY_TOKEN || (await tokenFor("pup-profile", "secret")));
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
