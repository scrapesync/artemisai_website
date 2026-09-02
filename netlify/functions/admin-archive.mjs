// netlify/functions/admin-archive.mjs
// Which admin-panel cards are archived, plus the shared card order.
//
// WHY THIS IS A "v2" FUNCTION (export default, Request/Response)
// ------------------------------------------------------------
// The legacy exports.handler form has to bootstrap Blobs with
// connectLambda(event), which builds a context of deployID/edgeURL/siteID/
// token and nothing else. Strong-consistency reads need uncachedEdgeURL, so
// in that form they can never work and silently fall back to eventual reads -
// which on the live site meant: archive, refresh, gone. The modern runtime
// injects the full Blobs context for v2 functions. The response reports which
// consistency was actually used, so if this ever regresses it shows in the
// JSON rather than as "the archive forgot".
//
// SHAPE
// -----
// One blob per archived page ("a:<href>") and one for the order, so no write
// ever depends on having read the latest state; a stale read can only delay
// visibility, never destroy anything.
//
// No authentication, matching charts.js: the admin sign-in is client-side, a
// key here would be theatre, and every action is reversible from the screen.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/admin-archive              { archived, order, consistency }
//   POST   /api/admin-archive              {href, title, by}     -> archive one page
//   POST   /api/admin-archive              {order: [href...], by} -> save the shared order
//   DELETE /api/admin-archive?href=..      restore one page

import { getStore } from "@netlify/blobs";

const STORE = "admin-archive-v2";
const ORDER_KEY = "order";
const MAX = 200;
const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
  "Content-Type": "application/json",
};
const reply = (status, body) => new Response(JSON.stringify(body), { status, headers: HEADERS });
// Only same-site page links are storable, or the archive becomes a place to
// park an attacker-supplied URL that the panel would render as a link.
const cleanHref = (h) => { const s = String(h || "").trim(); return /^[a-zA-Z0-9._-]+\.html$/.test(s) ? s : null; };

export default async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: HEADERS });

  let weak, strong, used = "strong";
  try {
    weak = getStore(STORE);
    strong = weak;
    try { strong = getStore({ name: STORE, consistency: "strong" }); } catch { used = "eventual"; }
  } catch (err) {
    return reply(500, { error: "store_unavailable", message: String(err?.message || err).slice(0, 160) });
  }
  const read = async (fn) => {
    try { return await fn(strong); }
    catch (err) {
      if (String(err?.name) !== "BlobsConsistencyError") throw err;
      used = "eventual";
      return await fn(weak);
    }
  };
  const snapshot = async () => {
    const { blobs } = await read((s) => s.list());
    const items = await Promise.all(blobs.map(async (b) => {
      try { return [b.key, await read((s) => s.get(b.key, { type: "json" }))]; } catch { return null; }
    }));
    const archived = [], order = [];
    for (const it of items) {
      if (!it || !it[1]) continue;
      if (it[0] === ORDER_KEY) { if (Array.isArray(it[1].order)) order.push(...it[1].order); }
      else if (it[0].startsWith("a:")) archived.push(it[1]);
    }
    archived.sort((a, b) => String(a.at || "").localeCompare(String(b.at || "")));
    return { archived, order, consistency: used };
  };

  if (req.method === "GET") return reply(200, await snapshot());

  if (req.method === "POST") {
    let body;
    try { body = await req.json(); } catch { return reply(400, { error: "bad_json" }); }
    const by = String(body?.by || "someone").slice(0, 60);
    if (Array.isArray(body?.order)) {
      const order = body.order.map(cleanHref).filter(Boolean).slice(0, MAX);
      await weak.setJSON(ORDER_KEY, { order, by, at: new Date().toISOString() });
      return reply(200, await snapshot());
    }
    const href = cleanHref(body?.href);
    if (!href) return reply(400, { error: "bad_href", message: "Expected a same-site .html filename." });
    await weak.setJSON("a:" + href, { href, title: String(body?.title || href).slice(0, 120), at: new Date().toISOString(), by });
    return reply(200, await snapshot());
  }

  if (req.method === "DELETE") {
    const href = cleanHref(new URL(req.url).searchParams.get("href"));
    if (!href) return reply(400, { error: "bad_href" });
    await weak.delete("a:" + href);
    return reply(200, await snapshot());
  }
  return reply(405, { error: "method_not_allowed" });
};
