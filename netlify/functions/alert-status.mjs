// netlify/functions/alert-status.mjs
// The result of every scheduled alert check, so the admin Alerts page can show
// what ran, what passed, what fired and - the one that matters - what stopped
// running without anyone noticing.
//
//   EventBridge rule (cron) -> Lambda runs one check -> POST here (with key)
//   Alerts page             -> GET here -> cards
//
// The Lambda owns the checking; this only remembers what it was told, because
// the browser cannot hold AWS credentials and this repo is public.
//
// v2 function form so Blobs strong-consistency reads work (the legacy handler
// form bootstraps Blobs without uncachedEdgeURL and silently reads stale).
//
// WRITES NEED A KEY, READS DO NOT: anything that can POST here can make a
// broken pipeline look green, the exact failure the page exists to prevent.
// ALERT_INGEST_KEY is a Netlify environment variable, never in this repo.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/alert-status            every alert, newest run first
//   GET    /api/alert-status?name=..    one alert with its full history
//   POST   /api/alert-status            {name, status, message, ms, detail, every_minutes}
//   DELETE /api/alert-status?name=..    forget an alert that no longer exists

import { getStore } from "@netlify/blobs";

const STORE = "alert-status";
const KEEP_RUNS = 20;
const STATUSES = ["ok", "fired", "error"];
const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Alert-Key",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
  "Content-Type": "application/json",
};
const reply = (status, body) => new Response(JSON.stringify(body), { status, headers: HEADERS });
// Blob keys become URL path segments; a slash in a name must not become a path.
const slug = (s) => String(s || "").toLowerCase().trim().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);

export default async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: HEADERS });

  let weak, strong, used = "strong";
  try {
    weak = getStore(STORE); strong = weak;
    try { strong = getStore({ name: STORE, consistency: "strong" }); } catch { used = "eventual"; }
  } catch (err) {
    return reply(500, { error: "store_unavailable", message: String(err?.message || err).slice(0, 160) });
  }
  const read = async (fn) => {
    try { return await fn(strong); }
    catch (err) { if (String(err?.name) !== "BlobsConsistencyError") throw err; used = "eventual"; return await fn(weak); }
  };
  const url = new URL(req.url);

  if (req.method === "GET") {
    const name = url.searchParams.get("name");
    if (name) {
      const rec = await read((s) => s.get(slug(name), { type: "json" }));
      return rec ? reply(200, rec) : reply(404, { error: "not_found" });
    }
    const { blobs } = await read((s) => s.list());
    const all = await Promise.all(blobs.map((b) => read((s) => s.get(b.key, { type: "json" })).catch(() => null)));
    const alerts = all.filter(Boolean).sort((a, b) => String(b.last_run_at || "").localeCompare(String(a.last_run_at || "")));
    return reply(200, { generated_at: new Date().toISOString(), count: alerts.length, alerts, consistency: used });
  }

  if (req.method === "POST") {
    const expected = process.env.ALERT_INGEST_KEY;
    if (!expected) return reply(503, { error: "ingest_not_configured", message: "ALERT_INGEST_KEY is not set on this site, so writes are refused." });
    if (req.headers.get("x-alert-key") !== expected) return reply(401, { error: "bad_key" });
    let body;
    try { body = await req.json(); } catch { return reply(400, { error: "bad_json" }); }
    const key = slug(body?.name);
    if (!key) return reply(400, { error: "name_required" });
    const status = String(body.status || "").toLowerCase();
    if (!STATUSES.includes(status)) return reply(400, { error: "bad_status", expected: STATUSES });

    const now = new Date().toISOString();
    const run = { at: now, status, message: String(body.message || "").slice(0, 400),
      ms: Number.isFinite(body.ms) ? Math.round(body.ms) : null, detail: body.detail === undefined ? null : body.detail };
    const prev = (await read((s) => s.get(key, { type: "json" }))) || { runs: [] };
    const runs = [run, ...(prev.runs || [])].slice(0, KEEP_RUNS);
    // last_fired_at lives outside the run list so "has this EVER fired?" survives
    // the history rolling over - a check that has never fired is usually broken.
    const rec = {
      name: String(body.name).slice(0, 80), key, status, message: run.message, last_run_at: now, last_ms: run.ms,
      every_minutes: Number.isFinite(body.every_minutes) ? Math.round(body.every_minutes) : (prev.every_minutes || null),
      first_seen_at: prev.first_seen_at || now,
      last_fired_at: status === "fired" ? now : (prev.last_fired_at || null),
      last_ok_at: status === "ok" ? now : (prev.last_ok_at || null),
      runs,
    };
    await weak.setJSON(key, rec);
    return reply(200, { saved: true, key, status });
  }

  if (req.method === "DELETE") {
    const key = slug(url.searchParams.get("name"));
    if (!key) return reply(400, { error: "name_required" });
    await weak.delete(key);
    return reply(200, { deleted: true, key });
  }
  return reply(405, { error: "method_not_allowed" });
};
