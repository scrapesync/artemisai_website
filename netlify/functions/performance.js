// netlify/functions/performance.js
// The leadership performance review: server-gated, unlike everything else on
// this site, because it holds judgements about named colleagues.
//
// WHY THIS ONE IS ACTUALLY LOCKED DOWN
// ------------------------------------
// The admin area's login is client-side theatre and the repo is public, which is
// tolerable for sprint plans and dashboards. It is not tolerable for a page that
// says how individuals are performing: the people most harmed by a leak are the
// people being reviewed. So the page itself contains no assessments, ever. The
// content lives in a private Netlify Blob and is returned only after the caller
// re-proves a username and password against public.portal_users AND that
// username is on the leadership allowlist. A stolen localStorage session gets
// nothing here.
//
// The allowlist is intended to be Asad, Jill, Alex and Hannah. Only usernames
// that exist in portal_users can pass the credential check, so names on the
// list without accounts simply cannot get in until an account is created.

const { Client } = require("pg");
const { connectLambda, getStore } = require("@netlify/blobs");

const ALLOWLIST = ["asad", "jill", "alex", "hannah"];
const STORE = "leadership";
const KEY = "team-performance";
const MAX_BYTES = 200000;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
};
const reply = (code, body) => ({ statusCode: code, headers: HEADERS, body: JSON.stringify(body) });

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };
  if (event.httpMethod !== "POST") return reply(405, { error: "POST only" });

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return reply(400, { error: "Invalid JSON" }); }

  const username = String(body.username || "").toLowerCase().trim();
  const password = String(body.password || "");
  const action = body.action === "save" ? "save" : "get";
  if (!username || !password) return reply(400, { error: "Username and password required" });

  // The allowlist check is deliberately BEFORE the database call: a non-listed
  // user learns nothing, not even whether their password was right.
  if (!ALLOWLIST.includes(username)) return reply(403, { error: "This page is restricted to leadership." });

  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439", 10),
    database: process.env.REDSHIFT_DATABASE || process.env.REDSHIFT_DB,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 8000,
    query_timeout: 12000,
  });

  try {
    await client.connect();
    const hit = await client.query(
      "SELECT username, full_name, role FROM public.portal_users WHERE username = $1 AND password = $2 AND is_active = true",
      [username, password]
    );
    if (!hit.rows.length) return reply(401, { error: "Invalid username or password" });
  } catch (err) {
    return reply(500, { error: "Could not verify credentials: " + String(err.message || err).slice(0, 120) });
  } finally {
    try { await client.end(); } catch {}
  }

  let store;
  try {
    connectLambda(event);
    store = getStore(STORE);
  } catch (err) {
    return reply(500, { error: "Store unavailable: " + String(err.message || err).slice(0, 120) });
  }

  if (action === "get") {
    try {
      const doc = await store.get(KEY, { type: "json" });
      return reply(200, { ok: true, doc: doc || null, viewer: username });
    } catch (err) {
      return reply(500, { error: "Read failed: " + String(err.message || err).slice(0, 120) });
    }
  }

  // save: the whole document, replaced atomically, stamped with who saved it.
  const doc = body.doc;
  if (!doc || typeof doc !== "object" || !Array.isArray(doc.people)) {
    return reply(400, { error: "Expected doc.people" });
  }
  doc.updated_at = new Date().toISOString();
  doc.updated_by = username;
  const raw = JSON.stringify(doc);
  if (raw.length > MAX_BYTES) return reply(413, { error: "Too large" });
  try {
    await store.setJSON(KEY, JSON.parse(raw));
    return reply(200, { ok: true, updated_at: doc.updated_at });
  } catch (err) {
    return reply(500, { error: "Save failed: " + String(err.message || err).slice(0, 120) });
  }
};
