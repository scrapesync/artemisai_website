// netlify/functions/alert-status.js
// The result of every scheduled alert check, so the admin Alerts page can show
// what ran, what passed, what fired and — the one that matters — what stopped
// running without anyone noticing.
//
// HOW THE PIECES FIT
// ------------------
//   EventBridge rule (cron)  ->  Lambda runs one alert check  ->  POST here
//   Alerts page              ->  GET here  ->  cards
//
// The Lambda owns the checking. This function only remembers what it was told,
// because the browser cannot hold AWS credentials and this repo is public, so
// the page can never call EventBridge or CloudWatch directly.
//
// WHY BLOBS AND NOT THE REPO
// --------------------------
// Same reasoning as charts.js: a write per alert run would be a commit per alert
// run, each triggering a full site rebuild, on a public repo. Blobs is private
// to the site, costs nothing per write and needs no token.
//
// WHY THE WRITE PATH HAS A KEY AND THE READ PATH DOES NOT
// ------------------------------------------------------
// charts.js is deliberately open because a human has to be looking at the admin
// area to use it, and the worst case is clutter. This is different: anything
// that can POST here can make a broken pipeline look green, which is the exact
// failure the page exists to prevent. So writes need ALERT_INGEST_KEY, set as a
// Netlify environment variable and given to the Lambda. It is never in this repo.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/alert-status            every alert, newest run first
//   GET    /api/alert-status?name=..    one alert with its full history
//   POST   /api/alert-status            {name, status, message, ms, detail, every_minutes}
//   DELETE /api/alert-status?name=..    forget an alert that no longer exists

const { connectLambda, getStore } = require("@netlify/blobs");

const STORE = "alert-status";

// Enough history to see a pattern, not so much that a card takes a second to
// paint. Twenty runs of an hourly check is most of a day.
const KEEP_RUNS = 20;

// A status we do not recognise would render as an unstyled pill and quietly
// become "not red", so unknown values are rejected at the door instead.
const STATUSES = ["ok", "fired", "error"];

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Alert-Key",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
};

const reply = (statusCode, body) => ({
  statusCode,
  headers: { ...HEADERS, "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

// Blob keys become URL path segments, so a name with a slash in it would write
// to somewhere unintended rather than fail loudly.
const slug = (s) =>
  String(s || "").toLowerCase().trim().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  let store;
  try {
    connectLambda(event);
    store = getStore(STORE);
  } catch (err) {
    return reply(500, {
      error: "store_unavailable",
      message: "Netlify Blobs is not available here: " + String(err.message || err).slice(0, 160),
    });
  }

  const qs = event.queryStringParameters || {};

  /* ---------- read ---------- */
  if (event.httpMethod === "GET") {
    if (qs.name) {
      const rec = await store.get(slug(qs.name), { type: "json" });
      if (!rec) return reply(404, { error: "not_found" });
      return reply(200, rec);
    }
    const { blobs } = await store.list();
    const all = await Promise.all(
      blobs.map((b) => store.get(b.key, { type: "json" }).catch(() => null))
    );
    // A card with no runs at all is still worth showing — "never run" is a
    // finding, not an empty state — so nulls are dropped but empty ones are not.
    const alerts = all.filter(Boolean).sort((a, b) =>
      String(b.last_run_at || "").localeCompare(String(a.last_run_at || ""))
    );
    return reply(200, { generated_at: new Date().toISOString(), count: alerts.length, alerts });
  }

  /* ---------- write (Lambda only) ---------- */
  if (event.httpMethod === "POST") {
    const expected = process.env.ALERT_INGEST_KEY;
    if (!expected) {
      return reply(503, {
        error: "ingest_not_configured",
        message: "ALERT_INGEST_KEY is not set on this site, so writes are refused.",
      });
    }
    const given = event.headers["x-alert-key"] || event.headers["X-Alert-Key"];
    if (given !== expected) return reply(401, { error: "bad_key" });

    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "bad_json" }); }

    const key = slug(body.name);
    if (!key) return reply(400, { error: "name_required" });
    const status = String(body.status || "").toLowerCase();
    if (!STATUSES.includes(status)) {
      return reply(400, { error: "bad_status", expected: STATUSES });
    }

    const now = new Date().toISOString();
    const run = {
      at: now,
      status,
      message: String(body.message || "").slice(0, 400),
      ms: Number.isFinite(body.ms) ? Math.round(body.ms) : null,
      detail: body.detail === undefined ? null : body.detail,
    };

    const prev = (await store.get(key, { type: "json" })) || { runs: [] };
    const runs = [run, ...(prev.runs || [])].slice(0, KEEP_RUNS);

    // fired_at is kept separately from the run list: once the history rolls past
    // KEEP_RUNS we would otherwise lose the answer to "has this alert EVER
    // fired?", and an alert that has never once fired is usually a broken alert
    // rather than a quiet system.
    const rec = {
      name: String(body.name).slice(0, 80),
      key,
      status,
      message: run.message,
      last_run_at: now,
      last_ms: run.ms,
      // How often the EventBridge rule is meant to run, so the page can say
      // "this has not run when it should have" instead of only "last run: 3 days ago".
      every_minutes: Number.isFinite(body.every_minutes) ? Math.round(body.every_minutes)
                   : (prev.every_minutes || null),
      first_seen_at: prev.first_seen_at || now,
      last_fired_at: status === "fired" ? now : (prev.last_fired_at || null),
      last_ok_at: status === "ok" ? now : (prev.last_ok_at || null),
      runs,
    };

    await store.setJSON(key, rec);
    return reply(200, { saved: true, key, status });
  }

  /* ---------- forget ---------- */
  if (event.httpMethod === "DELETE") {
    const key = slug(qs.name);
    if (!key) return reply(400, { error: "name_required" });
    await store.delete(key);
    return reply(200, { deleted: true, key });
  }

  return reply(405, { error: "method_not_allowed" });
};
