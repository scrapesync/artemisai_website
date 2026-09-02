// netlify/functions/admin-archive.js
// Which admin-panel cards are archived. Shared, not per-browser.
//
// WHY SHARED AND NOT localStorage
// -------------------------------
// The panel already saves card ORDER in localStorage, which is right: the order
// you like is yours. Archiving is the opposite kind of fact. "This page is dead"
// is a decision about the tool, and if it only applied to the person who made it
// everyone else would keep opening a page that someone already retired, and the
// panel would drift into a different shape for each of us.
//
// Storage is Netlify Blobs for the same reasons as charts.js: a public repo, and
// no reason to spend a commit and a site rebuild on hiding a card.
//
// No authentication, deliberately, matching charts.js: the admin sign-in is
// client-side only, so a key here would be theatre. Archiving is reversible from
// the same screen and nothing is deleted, so the worst case is a card someone
// has to drag back.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/admin-archive              { archived: [{href, title, at, by}] }
//   POST   /api/admin-archive              {href, title, by} -> archive one
//   DELETE /api/admin-archive?href=..      restore one

const { connectLambda, getStore } = require("@netlify/blobs");

const STORE = "admin-archive";
const KEY = "archived";

// The panel has ~22 cards. A ceiling well above that costs nothing and stops a
// loop somewhere writing until the blob is too big to read.
const MAX = 200;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
};
const reply = (statusCode, body) => ({
  statusCode,
  headers: { ...HEADERS, "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

// Only same-site page links are storable. Without this the archive doubles as a
// place to park an arbitrary attacker-supplied URL that the panel would then
// render as a link.
const cleanHref = (h) => {
  const s = String(h || "").trim();
  return /^[a-zA-Z0-9._-]+\.html$/.test(s) ? s : null;
};

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

  const read = async () => (await store.get(KEY, { type: "json" })) || { archived: [] };

  if (event.httpMethod === "GET") {
    const rec = await read();
    return reply(200, rec);
  }

  if (event.httpMethod === "POST") {
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "bad_json" }); }

    const href = cleanHref(body.href);
    if (!href) return reply(400, { error: "bad_href", message: "Expected a same-site .html filename." });

    const rec = await read();
    // Archiving something already archived is a no-op rather than an error: two
    // people dragging the same dead card is a normal thing to happen, not a fault.
    if (rec.archived.some((a) => a.href === href)) return reply(200, rec);
    if (rec.archived.length >= MAX) return reply(507, { error: "archive_full", max: MAX });

    rec.archived.push({
      href,
      title: String(body.title || href).slice(0, 120),
      at: new Date().toISOString(),
      // Self-declared, exactly like chart authorship: the sign-in is client-side,
      // so this answers "who should I ask about this" and proves nothing.
      by: String(body.by || "someone").slice(0, 60),
    });
    await store.setJSON(KEY, rec);
    return reply(200, rec);
  }

  if (event.httpMethod === "DELETE") {
    const href = cleanHref((event.queryStringParameters || {}).href);
    if (!href) return reply(400, { error: "bad_href" });
    const rec = await read();
    rec.archived = rec.archived.filter((a) => a.href !== href);
    await store.setJSON(KEY, rec);
    return reply(200, rec);
  }

  return reply(405, { error: "method_not_allowed" });
};
