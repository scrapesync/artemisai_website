// netlify/functions/run-query.js
// Read-only SQL runner for the QA Dashboard's chart builder.
// Accepts a single SELECT (or WITH ... SELECT), runs it against Redshift,
// returns columns + rows. Everything that is not a plain read is rejected.
//
// SECURITY MODEL (read this before changing anything):
//  1. The Redshift user in REDSHIFT_USER should be a SELECT-only account.
//     That is the real protection. Everything below is defence in depth.
//  2. Statements are validated: one statement, must start SELECT/WITH,
//     no DDL/DML keywords, no multi-statement via semicolons.
//  3. public.portal_users is blocked outright: it stores login credentials
//     in plaintext, so it must never be readable through this endpoint.
//  4. Every result is capped (LIMIT) and every query has a hard timeout.

const { Client } = require("pg");

const ROW_CAP = 5000;
const TIMEOUT_MS = 20000;

// Tables/views that must never be readable here, whatever the SQL says.
const BLOCKED = [
  "portal_users",   // plaintext credentials
  "pg_shadow",
  "pg_authid",
  "pg_user_info",
];

// Anything that writes, changes structure, or escalates.
const FORBIDDEN = [
  "insert", "update", "delete", "drop", "create", "alter", "truncate",
  "grant", "revoke", "copy", "unload", "vacuum", "analyze", "call",
  "execute", "prepare", "commit", "rollback", "begin", "set", "reset",
  "comment", "lock", "refresh", "attach", "detach", "merge",
];

function validate(sqlRaw) {
  if (!sqlRaw || typeof sqlRaw !== "string") return "No SQL provided";
  let sql = sqlRaw.trim();
  if (!sql) return "No SQL provided";
  if (sql.length > 20000) return "Query too long";

  // Strip a single trailing semicolon, then reject any remaining one
  // (that would allow a second, unvalidated statement).
  sql = sql.replace(/;\s*$/, "");
  if (sql.includes(";")) return "Only one statement is allowed (remove the semicolon)";

  // Comments can hide keywords from a naive scan, so strip them before checking.
  const stripped = sql
    .replace(/--[^\n]*/g, " ")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .toLowerCase();

  if (!/^\s*(select|with)\b/.test(stripped)) {
    return "Only SELECT queries are allowed (start with SELECT or WITH)";
  }
  for (const word of FORBIDDEN) {
    if (new RegExp(`\\b${word}\\b`).test(stripped)) {
      return `"${word.toUpperCase()}" is not allowed: this endpoint is read-only`;
    }
  }
  for (const table of BLOCKED) {
    if (stripped.includes(table)) {
      return `Table "${table}" is not readable from here`;
    }
  }
  return null; // ok
}

exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  const problem = validate(payload.sql);
  if (problem) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: problem }) };
  }

  const inner = payload.sql.trim().replace(/;\s*$/, "");
  // Wrap so the cap always applies, even if the user wrote their own LIMIT.
  const wrapped = `SELECT * FROM (${inner}) AS artemis_q LIMIT ${ROW_CAP}`;

  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439"),
    database: process.env.REDSHIFT_DATABASE || process.env.REDSHIFT_DB,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 10000,
    query_timeout: TIMEOUT_MS,
    statement_timeout: TIMEOUT_MS,
  });

  const started = Date.now();
  try {
    await client.connect();
    const res = await client.query(wrapped);
    const columns = res.fields.map((f) => f.name);
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        columns,
        rows: res.rows,
        row_count: res.rows.length,
        truncated: res.rows.length >= ROW_CAP,
        ms: Date.now() - started,
      }),
    };
  } catch (err) {
    // Send the database's own message back: for a query tool that IS the useful part.
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ error: err.message || "Query failed", ms: Date.now() - started }),
    };
  } finally {
    try { await client.end(); } catch {}
  }
};
