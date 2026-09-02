// netlify/functions/admin-archive.js
// Which admin-panel cards are archived, and the shared card order. Shared, not
// per-browser.
//
// WHY SHARED AND NOT localStorage
// -------------------------------
// "This page is dead" and "this is the order the team reads the panel in" are
// facts about the tool. If they only applied to the person who set them, the
// panel would drift into a different shape for each of us, and everyone else
// would keep opening a page someone already retired.
//
// STORAGE SHAPE, AND THE BUG THE FIRST VERSION HAD
// -----------------------------------------------
// Netlify Blobs, as in charts.js. The first version kept the whole archive in
// ONE document and read-modify-wrote it. Blobs reads are eventually consistent
// by default, so the next read after a write could return the old document -
// and the next write would then overwrite the real one with stale data. That
// is exactly what happened on the live site: archive, refresh, gone.
//
// Two fixes, both borrowed from charts.js:
//   1. one blob per archived page ("a:<href>") and one for the order, so no
//      write ever depends on having read the latest state;
//   2. reads go through a strong-consistency store, falling back to eventual
//      only where the runtime cannot provide it (BlobsConsistencyError).
//
// No authentication, deliberately, matching charts.js: the admin sign-in is
// client-side only, so a key here would be theatre. Archiving is reversible
// from the same screen and nothing is deleted.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/admin-archive              { archived: [{href,title,at,by}], order: [href...] }
//   POST   /api/admin-archive              {href, title, by}   -> archive one page
//   POST   /api/admin-archive              {order: [href...], by} -> save the shared card order
//   DELETE /api/admin-archive?href=..      restore one page

const { connectLambda, getStore } = require("@netlify/blobs");

const STORE = "admin-archive-v2";
const ORDER_KEY = "order";
const MAX = 200;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
};
const reply = (statusCode, body) => ({
  statusCode, headers: { ...HEADERS, "Content-Type": "application/json" }, body: JSON.stringify(body),
});

// Only same-site page links are storable. Without this the archive doubles as a
// place to park an arbitrary attacker-supplied URL that the panel would render
// as a link.
const cleanHref = (h) => {
  const s = String(h || "").trim();
  return /^[a-zA-Z0-9._-]+\.html$/.test(s) ? s : null;
};

function pair(name) {
  const weak = getStore(name);
  let strong = weak;
  try { strong = getStore({ name, consistency: "strong" }); } catch { /* stays weak */ }
  return { weak, strong };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  let s;
  try { connectLambda(event); s = pair(STORE); }
  catch (err) {
    return reply(500, { error: "store_unavailable",
      message: "Netlify Blobs is not available here: " + String(err.message || err).slice(0, 160) });
  }

  // Reads try the strong store first. Writes always use the weak store: a
  // write is visible to a strong read immediately regardless, and routing a
  // write through the strong store fails outright where uncachedEdgeURL is
  // missing.
  const read = async (fn) => {
    try { return await fn(s.strong); }
    catch (err) {
      if (String(err && err.name) !== "BlobsConsistencyError") throw err;
      return await fn(s.weak);
    }
  };

  const snapshot = async () => {
    const { blobs } = await read((st) => st.list());
    const items = await Promise.all(blobs.map(async (b) => {
      try { return [b.key, await read((st) => st.get(b.key, { type: "json" }))]; } catch { return null; }
    }));
    const archived = [], order = [];
    for (const it of items) {
      if (!it || !it[1]) continue;
      if (it[0] === ORDER_KEY) { if (Array.isArray(it[1].order)) order.push(...it[1].order); }
      else if (it[0].startsWith("a:")) archived.push(it[1]);
    }
    archived.sort((a, b) => String(a.at || "").localeCompare(String(b.at || "")));
    return { archived, order };
  };

  if (event.httpMethod === "GET") return reply(200, await snapshot());

  if (event.httpMethod === "POST") {
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "bad_json" }); }
    const by = String(body.by || "someone").slice(0, 60);

    if (Array.isArray(body.order)) {
      const order = body.order.map(cleanHref).filter(Boolean).slice(0, MAX);
      await s.weak.setJSON(ORDER_KEY, { order, by, at: new Date().toISOString() });
      return reply(200, await snapshot());
    }

    const href = cleanHref(body.href);
    if (!href) return reply(400, { error: "bad_href", message: "Expected a same-site .html filename." });
    await s.weak.setJSON("a:" + href, {
      href,
      title: String(body.title || href).slice(0, 120),
      at: new Date().toISOString(),
      // Self-declared, like chart authorship: answers "who should I ask", proves nothing.
      by,
    });
    return reply(200, await snapshot());
  }

  if (event.httpMethod === "DELETE") {
    const href = cleanHref((event.queryStringParameters || {}).href);
    if (!href) return reply(400, { error: "bad_href" });
    await s.weak.delete("a:" + href);
    return reply(200, await snapshot());
  }

  return reply(405, { error: "method_not_allowed" });
};
