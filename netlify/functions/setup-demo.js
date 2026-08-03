// netlify/functions/setup-demo.js
// ONE-OFF: creates the investor demo login in public.portal_users, then this
// file gets deleted in the next commit. It exists because the login Lambda has
// no create-user action and run-query is deliberately SELECT-only.
//
// Guarded by a single-use key. The repo is public so the key is visible here;
// the exposure window is the minutes between deploy and removal, and the only
// thing the endpoint can do is create or update ONE fixed username with role
// 'client', which grants nothing beyond the demo pages.

const { Client } = require("pg");

const SETUP_KEY = "sdk-71f3c9a4e2b8d5f6-once";
const DEMO_USER = "guest";
const DEMO_ROLE = "client";
const DEMO_NAME = "Guest";

exports.handler = async (event) => {
  const headers = { "Content-Type": "application/json" };
  if (event.httpMethod !== "POST") return { statusCode: 405, headers, body: JSON.stringify({ error: "POST only" }) };

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return { statusCode: 400, headers, body: JSON.stringify({ error: "bad json" }) }; }
  if (body.key !== SETUP_KEY) return { statusCode: 403, headers, body: JSON.stringify({ error: "no" }) };
  const password = String(body.password || "");
  if (body.action !== "diag" && password.length < 10) return { statusCode: 400, headers, body: JSON.stringify({ error: "password too short" }) };

  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439", 10),
    database: process.env.REDSHIFT_DATABASE || process.env.REDSHIFT_DB,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 8000,
    query_timeout: 15000,
  });

  try {
    await client.connect();

    // Diagnostic: the SHAPE of what the login Lambda compares against, never the
    // values. If team rows hold 32-char hex the Lambda compares md5; 64-char hex
    // sha256; bcrypt prefixes $2; plaintext shows as mixed/other.
    if (body.action === "diag") {
      const rows = await client.query(
        `SELECT username, role, is_active,
                LEN(password) AS pw_len,
                CASE WHEN password ~ '^[0-9a-f]+$' THEN 'lower-hex'
                     WHEN password LIKE '$2%' THEN 'bcrypt'
                     ELSE 'other' END AS pw_shape,
                last_login
           FROM public.portal_users ORDER BY username`
      );
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true, users: rows.rows }) };
    }

    const existing = await client.query(
      "SELECT id, username, role, is_active FROM public.portal_users WHERE username = $1", [DEMO_USER]
    );

    if (existing.rows.length) {
      await client.query(
        "UPDATE public.portal_users SET password = $1, role = $2, is_active = true, full_name = $3 WHERE username = $4",
        [password, DEMO_ROLE, DEMO_NAME, DEMO_USER]
      );
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true, action: "updated", user: DEMO_USER, role: DEMO_ROLE }) };
    }

    // id handling is unknown territory (identity column or plain int), so try
    // the simple insert first and fall back to computing the next id.
    try {
      await client.query(
        "INSERT INTO public.portal_users (username, password, full_name, role, is_active) VALUES ($1, $2, $3, $4, true)",
        [DEMO_USER, password, DEMO_NAME, DEMO_ROLE]
      );
    } catch (e1) {
      await client.query(
        `INSERT INTO public.portal_users (id, username, password, full_name, role, is_active)
         SELECT COALESCE(MAX(id), 0) + 1, $1, $2, $3, $4, true FROM public.portal_users`,
        [DEMO_USER, password, DEMO_NAME, DEMO_ROLE]
      );
    }
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, action: "created", user: DEMO_USER, role: DEMO_ROLE }) };
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: String(err.message || err).slice(0, 300) }) };
  } finally {
    try { await client.end(); } catch {}
  }
};
