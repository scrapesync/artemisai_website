// netlify/functions/charts.js
// The team's saved charts for the QA Dashboard's Custom tab. One person builds a
// chart in the SQL tab, publishes it with a title and a note, and everyone else
// sees it on their next visit.
//
// WHERE THIS IS STORED, AND WHY NOT IN THE REPO
// ---------------------------------------------
// Netlify Blobs. The repo already had this pattern once: save-layout.js committed
// the shared dashboard layout to qa_layout.json through the GitHub contents API.
// That works, but scrapesync/artemisai_website is a PUBLIC repo, so every save
// would publish a teammate's name, their note and their SQL to the open internet,
// and each save would cost a commit plus a full site rebuild. Blobs keeps it off
// GitHub, saves in milliseconds and needs no token.
//
// One blob per chart rather than one document holding them all: two people
// publishing at the same moment would otherwise read the same array, each append
// their own entry and the second write would silently drop the first.
//
// ATTRIBUTION IS SELF-DECLARED. The admin area's sign-in is client-side only, so
// `author` is whatever the browser sent. It is good enough for "who should I ask
// about this chart" and it is not proof of anything. The page says so too.
//
// Routes (via the /api/* redirect in netlify.toml):
// RESULTS ARE CACHED, AND THE CACHE IS SHARED
// -------------------------------------------
// Every viewer re-running every chart meant the Custom tab's cost to Redshift
// scaled with how often people looked at it, which is the wrong way round for a
// screen meant to be glanced at. Results now live in a second store, so the tab
// paints from cache and touches Redshift only when somebody asks it to. Because
// the cache is shared, one person's refresh spares the whole team rather than
// just their own browser, and everyone is looking at the same numbers.
//
// Cached results are read and written by key, never through list(), so the
// eventual-consistency lag that affects the chart listing does not apply.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/charts                  list every saved chart, newest first
//   GET    /api/charts?id=..            one chart
//   GET    /api/charts?id=..&part=cache its last cached result
//   POST   /api/charts                  {title, note, author, sql, cfg} -> saves one
//   PUT    /api/charts?id=..            {columns, rows, ms, by} -> stores a result
//   DELETE /api/charts?id=..            remove the chart and its cached result

const { connectLambda, getStore } = require("@netlify/blobs");

const STORE = "qa-custom-charts";
// A separate store, not a key prefix in the same one: the chart listing walks
// list(), and cache entries sharing that namespace would have to be filtered out
// of it forever, which is the kind of thing someone eventually forgets to do.
const CACHE_STORE = "qa-custom-chart-cache";
// A chart returning 5,000 wide rows is not worth storing; it stays live instead.
const MAX_CACHE_BYTES = 900000;

// Deliberately small ceilings. This endpoint has no authentication, because the
// admin login is client-side only, so anyone who finds the URL can write to it.
// The limits mean the worst case is clutter someone can delete, not a filled
// store or a payload that breaks the page.
const MAX_CHARTS = 200;
const LIMITS = { title: 120, note: 600, author: 60, sql: 20000, cfg: 8000 };

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
};

const reply = (statusCode, body) => ({ statusCode, headers: HEADERS, body: JSON.stringify(body) });

// Trim to a ceiling and collapse anything that is not text. Stored values are
// escaped again when the page renders them; this is about size, not safety.
function text(v, max) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim().slice(0, max);
}

function newId() {
  return "c" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

// A strong-consistency store where the runtime supports it, the plain one where
// it does not, so callers never have to care which they got.
function pair(name) {
  const weak = getStore(name);
  let strong = weak;
  try { strong = getStore({ name, consistency: "strong" }); } catch { /* stays weak */ }
  return { weak, strong };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  // Blobs reads are eventually consistent by default, and a publish that is
  // invisible for the next minute reads as a publish that failed. Strong
  // consistency fixes that, but it needs an 'uncachedEdgeURL' in the runtime
  // context and throws BlobsConsistencyError where that is absent, so it is
  // attempted and then dropped rather than assumed.
  let charts, cache;
  try {
    connectLambda(event); // hands the Blobs client this invocation's credentials
    charts = pair(STORE);
    cache = pair(CACHE_STORE);
  } catch (err) {
    return reply(500, {
      error: "store_unavailable",
      message: "Netlify Blobs is not available here: " + String(err.message || err).slice(0, 160),
    });
  }

  // Only READS go through the strong store. A write is visible to a read by key
  // immediately whatever the consistency setting, so there is nothing for strong
  // consistency to buy on the write path, and routing setJSON through it means a
  // runtime without uncachedEdgeURL fails the save outright. Writes use `.weak`.
  let usedConsistency = "strong";
  const read = async (p, fn) => {
    try { return await fn(p.strong); }
    catch (err) {
      if (String(err && err.name) !== "BlobsConsistencyError") throw err;
      usedConsistency = "eventual";
      return await fn(p.weak);
    }
  };

  /* ---------- read one, or list them all ---------- */
  if (event.httpMethod === "GET") {
    // ?id= fetches a single chart by key. Keys are read directly, which is the
    // one path that does not depend on list() having caught up.
    const q = event.queryStringParameters || {};
    const wantId = q.id;
    if (wantId && q.part === "cache") {
      try {
        const hit = await read(cache, (s) => s.get(wantId, { type: "json" }));
        return hit ? reply(200, { cache: hit }) : reply(404, { error: "no_cache", id: wantId });
      } catch (err) {
        return reply(500, { error: "cache_read_failed", message: String(err.message || err).slice(0, 200) });
      }
    }
    if (wantId) {
      try {
        const one = await read(charts, (s) => s.get(wantId, { type: "json" }));
        return one
          ? reply(200, { chart: one, consistency: usedConsistency })
          : reply(404, { error: "not_found", id: wantId, consistency: usedConsistency });
      } catch (err) {
        return reply(500, { error: "get_failed", message: String(err.message || err).slice(0, 200) });
      }
    }
    try {
      const listed = await read(charts, (s) => s.list());
      const blobs = listed.blobs || [];
      const found = await Promise.all(
        blobs.map(async (b) => {
          try { return await read(charts, (s) => s.get(b.key, { type: "json" })); }
          catch { return null; }        // a half-written or hand-deleted key is skipped
        })
      );
      const clean = found.filter(Boolean).sort((a, b) => (a.created_at < b.created_at ? 1 : -1));
      return reply(200, { charts: clean, count: clean.length, consistency: usedConsistency });
    } catch (err) {
      return reply(500, { error: "list_failed", message: String(err.message || err).slice(0, 200) });
    }
  }

  /* ---------- save ---------- */
  if (event.httpMethod === "POST") {
    let payload;
    try { payload = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "Invalid JSON" }); }

    const title = text(payload.title, LIMITS.title);
    const sql = String(payload.sql == null ? "" : payload.sql).slice(0, LIMITS.sql).trim();
    if (!title) return reply(400, { error: "no_title", message: "Give the chart a title." });
    if (!sql) return reply(400, { error: "no_sql", message: "There is no query to save." });
    if (!/^\s*(--|\/\*|select|with)/i.test(sql)) {
      return reply(400, { error: "not_a_select", message: "Only SELECT queries can be saved." });
    }

    let cfg = {};
    try {
      const raw = JSON.stringify(payload.cfg || {});
      if (raw.length > LIMITS.cfg) return reply(400, { error: "cfg_too_big" });
      cfg = JSON.parse(raw);
    } catch { cfg = {}; }

    try {
      const listed = await read(charts, (s) => s.list());
      if ((listed.blobs || []).length >= MAX_CHARTS) {
        return reply(409, {
          error: "full",
          message: `The team tab holds ${MAX_CHARTS} charts. Delete one before adding another.`,
        });
      }
      const record = {
        id: newId(),
        title,
        note: text(payload.note, LIMITS.note),
        author: text(payload.author, LIMITS.author) || "someone",
        sql,
        cfg,
        created_at: new Date().toISOString(),
      };
      await charts.weak.setJSON(record.id, record);
      return reply(201, { ok: true, chart: record, consistency: usedConsistency });
    } catch (err) {
      return reply(500, { error: "save_failed", message: String(err.message || err).slice(0, 200) });
    }
  }

  /* ---------- cache a result ---------- */
  if (event.httpMethod === "PUT") {
    const id = (event.queryStringParameters || {}).id;
    if (!id) return reply(400, { error: "no_id" });
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "Invalid JSON" }); }
    if (!Array.isArray(body.columns) || !Array.isArray(body.rows)) {
      return reply(400, { error: "bad_result", message: "Expected columns and rows." });
    }
    const record = {
      columns: body.columns,
      rows: body.rows,
      row_count: body.rows.length,
      ms: Number(body.ms) || 0,
      by: text(body.by, LIMITS.author) || "someone",
      cached_at: new Date().toISOString(),
    };
    const size = JSON.stringify(record).length;
    if (size > MAX_CACHE_BYTES) {
      // Say so rather than half-storing it: the page then keeps running this one
      // live and the reader knows why it is slower than the others.
      return reply(413, { error: "too_big", bytes: size, limit: MAX_CACHE_BYTES });
    }
    try {
      await cache.weak.setJSON(id, record);
      return reply(200, { ok: true, cached_at: record.cached_at, bytes: size });
    } catch (err) {
      return reply(500, { error: "cache_write_failed", message: String(err.message || err).slice(0, 200) });
    }
  }

  /* ---------- delete ---------- */
  if (event.httpMethod === "DELETE") {
    const id = (event.queryStringParameters || {}).id;
    if (!id) return reply(400, { error: "no_id" });
    try {
      await charts.weak.delete(id);
      // The cached result goes with it. Leaving it behind would mean a later
      // chart reusing the id inherits a stranger's numbers.
      try { await cache.weak.delete(id); } catch { /* nothing cached */ }
      return reply(200, { ok: true, deleted: id });
    } catch (err) {
      return reply(500, { error: "delete_failed", message: String(err.message || err).slice(0, 200) });
    }
  }

  return reply(405, { error: "Method not allowed" });
};
