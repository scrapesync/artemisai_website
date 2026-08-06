// netlify/functions/mcp.js
// Remote MCP server for the ArtemisAI warehouse, at /api/mcp.
//
// Team members add this as a connector in claude.ai, Claude Desktop, or
// Claude Code, and their Claude can answer data questions by querying the
// warehouse directly. Speaks MCP Streamable HTTP in stateless JSON mode:
// every request is a self-contained JSON-RPC POST, which fits a serverless
// function exactly (no session to hold, no SSE stream to keep open).
//
// SECURITY MODEL (this repo is public; none of this depends on secrecy):
//   - Callers authenticate with a per-person key: ?key=... in the connector
//     URL, or an Authorization: Bearer header. Keys live in the
//     MCP_ANALYST_KEYS env var as "name:key,name2:key2" pairs, so one
//     person's access can be revoked without touching the others.
//   - The database user is NOT the admin user the other functions use. It is
//     the dedicated read-only artemis_analyst user (MCP_DB_USER /
//     MCP_DB_PASSWORD), which can only SELECT from the analytics tables and
//     has no grant on public.portal_users. Even a bug in this file cannot
//     read credentials or write data.
//   - On top of that: table allowlist (same 25 tables as the Data Explorer),
//     SELECT/WITH-only statement filtering, row caps, query timeout.

const { Client } = require("pg");
const { connectLambda, getStore } = require("@netlify/blobs");

const PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"];

const ALLOWED_TABLES = [
  { schema: "odl", table: "comment_sentiments", desc: "Sentiment analysis results for post comments" },
  { schema: "odl", table: "comment_sentiments_v2", desc: "Updated sentiment analysis with improved model accuracy" },
  { schema: "odl", table: "dim_comments", desc: "All Facebook post comments with text, author, and timestamps" },
  { schema: "odl", table: "dim_date", desc: "Date dimension table for time-based analysis and joins" },
  { schema: "odl", table: "dim_geographies", desc: "Geographic regions for audience location analysis" },
  { schema: "odl", table: "dim_metrics", desc: "Metric definitions and metadata for engagement tracking" },
  { schema: "odl", table: "dim_page_categories", desc: "Facebook page category classifications" },
  { schema: "odl", table: "dim_pages", desc: "Connected Facebook pages with metadata and status" },
  { schema: "odl", table: "dim_posts", desc: "All Facebook posts with text, media type, timestamps, and URLs" },
  { schema: "odl", table: "dim_reaction_types", desc: "Facebook reaction type definitions (like, love, wow, etc.)" },
  { schema: "odl", table: "fact_page_daily_demographics_insights", desc: "Daily page-level demographics: age, gender, location" },
  { schema: "odl", table: "fact_page_daily_insights", desc: "Daily page-level metrics: reach, impressions, followers, engagement" },
  { schema: "odl", table: "fact_post_daily_insights", desc: "Daily post-level metrics: reach, impressions, clicks, reactions" },
  { schema: "odl", table: "gpt_model_prediction", desc: "GPT-generated virality and engagement predictions per post" },
  { schema: "odl", table: "gpt_post_recommendation", desc: "GPT-generated content strategy recommendations per post" },
  { schema: "odl", table: "sentiments_overall", desc: "Aggregated sentiment scores across all posts and comments" },
  { schema: "public", table: "artemis_fb_connections", desc: "Connected Facebook pages from the website connect flow" },
  { schema: "public", table: "ml_comment_sentiment_results", desc: "ML pipeline sentiment classification results for comments" },
  { schema: "rdl", table: "page_daily_insights", desc: "Refined daily page metrics after transformation and cleaning" },
  { schema: "rdl", table: "page_demographics_insights", desc: "Refined page demographics data after transformation" },
  { schema: "rdl", table: "page_info", desc: "Refined page metadata: name, category, followers, verification" },
  { schema: "rdl", table: "page_posts", desc: "Refined posts data with cleaned text and normalized fields" },
  { schema: "rdl", table: "post_comments", desc: "Refined comments data with cleaned text and threading" },
  { schema: "rdl", table: "post_daily_insights", desc: "Refined daily post metrics after transformation and cleaning" },
  { schema: "rdl", table: "post_reactions", desc: "Refined post reactions data with reaction type breakdowns" },
];

const DENIED = /portal_users|pg_user|pg_shadow|svl_user|stl_query|stl_connection/i;
const MAX_ROWS = 1000;
const DEFAULT_ROWS = 200;
const CELL_CAP = 400;

const TOOLS = [
  {
    name: "list_tables",
    description: "List every ArtemisAI warehouse table available, with a description of each. Call this first to see what data exists.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "describe_table",
    description: "Get the column names and data types for one warehouse table.",
    inputSchema: {
      type: "object",
      properties: {
        schema: { type: "string", description: "Schema name, e.g. odl" },
        table: { type: "string", description: "Table name, e.g. dim_posts" },
      },
      required: ["schema", "table"],
      additionalProperties: false,
    },
  },
  {
    name: "sample_table",
    description: "Fetch a few example rows from a table to see what the data looks like.",
    inputSchema: {
      type: "object",
      properties: {
        schema: { type: "string" },
        table: { type: "string" },
        n: { type: "integer", description: "Rows to fetch, default 20, max 100" },
      },
      required: ["schema", "table"],
      additionalProperties: false,
    },
  },
  {
    name: "run_query",
    description:
      "Run a read-only SQL query (SELECT or WITH...SELECT) against the ArtemisAI Redshift warehouse. Redshift dialect (GETDATE(), LISTAGG, etc). One statement only; results capped at 1000 rows, LIMIT appended automatically if missing. Use list_tables and describe_table first.",
    inputSchema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "The SQL to run" },
        limit: { type: "integer", description: "Row cap, default 200, max 1000" },
      },
      required: ["sql"],
      additionalProperties: false,
    },
  },
];

// ---------- auth ----------

function keyTable() {
  // MCP_ANALYST_KEYS = "asad:abc123,faheem:def456"
  // A bare entry with no name ("abc123") also works and is labeled "analyst".
  const raw = process.env.MCP_ANALYST_KEYS || "";
  const map = {};
  for (const pair of raw.split(",")) {
    const i = pair.indexOf(":");
    if (i > 0) map[pair.slice(i + 1).trim()] = pair.slice(0, i).trim();
    else if (pair.trim()) map[pair.trim()] = "analyst";
  }
  return map;
}

function authenticate(event) {
  const keys = keyTable();
  const qs = (event.queryStringParameters || {}).key || "";
  const hdr = event.headers.authorization || event.headers.Authorization || "";
  const bearer = hdr.startsWith("Bearer ") ? hdr.slice(7).trim() : "";
  // Also accept the key as a path segment (/api/mcp/<key>) — some connector
  // clients handle query strings poorly, and URLs with special characters
  // in the query can trip URL validators.
  const m = (event.path || "").match(/\/mcp\/([^/?#]+)/);
  const pathKey = m ? decodeURIComponent(m[1]) : "";
  return keys[qs] || keys[bearer] || keys[pathKey] || null;
}

// ---------- db ----------

async function withDb(fn) {
  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439", 10),
    database: process.env.REDSHIFT_DATABASE || process.env.REDSHIFT_DB,
    user: process.env.MCP_DB_USER,
    password: process.env.MCP_DB_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 16000,
    query_timeout: 20000,
  });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    try { await client.end(); } catch {}
  }
}

function shapeRows(res, cap) {
  const cols = res.fields.map((f) => f.name);
  const rows = res.rows.slice(0, cap).map((r) => {
    const rec = {};
    for (const c of cols) {
      let v = r[c];
      if (v !== null && typeof v !== "number" && typeof v !== "boolean") v = String(v);
      if (typeof v === "string" && v.length > CELL_CAP) v = v.slice(0, CELL_CAP) + "…";
      rec[c] = v;
    }
    return rec;
  });
  return { cols, rows };
}

// ---------- tools ----------

async function toolListTables() {
  const lines = ["Available tables (schema.table — description):", ""];
  for (const t of ALLOWED_TABLES) lines.push(`- ${t.schema}.${t.table} — ${t.desc}`);
  lines.push("", "Use describe_table for columns, sample_table for example rows, run_query for SQL.");
  return lines.join("\n");
}

function findAllowed(schema, table) {
  const s = String(schema || "").toLowerCase().trim();
  const t = String(table || "").toLowerCase().trim();
  return ALLOWED_TABLES.find((x) => x.schema === s && x.table === t) || null;
}

async function toolDescribeTable({ schema, table }) {
  const hit = findAllowed(schema, table);
  if (!hit) return `Error: ${schema}.${table} is not in the allowed table list. Call list_tables to see what is available.`;
  return withDb(async (db) => {
    const res = await db.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
      [hit.schema, hit.table]
    );
    if (!res.rows.length) return `No columns found for ${hit.schema}.${hit.table}.`;
    const lines = [`${hit.schema}.${hit.table} — ${hit.desc}`, ""];
    for (const r of res.rows) lines.push(`  ${r.column_name}  (${r.data_type})`);
    return lines.join("\n");
  });
}

async function toolSampleTable({ schema, table, n }) {
  const hit = findAllowed(schema, table);
  if (!hit) return `Error: ${schema}.${table} is not in the allowed table list.`;
  const cap = Math.max(1, Math.min(parseInt(n || 20, 10) || 20, 100));
  return withDb(async (db) => {
    const res = await db.query(`SELECT * FROM ${hit.schema}.${hit.table} LIMIT ${cap}`);
    const { cols, rows } = shapeRows(res, cap);
    return JSON.stringify({ columns: cols, rows });
  });
}

async function toolRunQuery({ sql, limit }) {
  let q = String(sql || "").trim().replace(/;\s*$/, "").trim();
  if (q.includes(";")) return "Error: one statement per query. Remove the extra semicolons.";
  if (!/^(select|with)\b/i.test(q)) return "Error: only SELECT (or WITH ... SELECT) queries are allowed. This connection is read-only.";
  if (DENIED.test(q)) return "Error: that query touches a restricted table.";
  const cap = Math.max(1, Math.min(parseInt(limit || DEFAULT_ROWS, 10) || DEFAULT_ROWS, MAX_ROWS));
  if (!/\blimit\s+\d+\s*$/i.test(q)) q = `${q}\nLIMIT ${cap}`;
  try {
    return await withDb(async (db) => {
      const res = await db.query(q);
      const { cols, rows } = shapeRows(res, cap);
      const payload = { columns: cols, row_count: rows.length, rows };
      if (rows.length === cap) payload.note = `Results capped at ${cap} rows; refine the query or raise limit (max ${MAX_ROWS}).`;
      return JSON.stringify(payload);
    });
  } catch (err) {
    return `Query error: ${String(err.message || err).slice(0, 300)}`;
  }
}

async function callTool(name, args) {
  switch (name) {
    case "list_tables": return toolListTables();
    case "describe_table": return toolDescribeTable(args || {});
    case "sample_table": return toolSampleTable(args || {});
    case "run_query": return toolRunQuery(args || {});
    default: return null;
  }
}

// ---------- JSON-RPC over Streamable HTTP (stateless) ----------

const HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, Mcp-Session-Id, MCP-Protocol-Version",
  "Access-Control-Allow-Methods": "POST, GET, DELETE, OPTIONS",
};

const rpcResult = (id, result) => ({ jsonrpc: "2.0", id, result });
const rpcError = (id, code, message) => ({ jsonrpc: "2.0", id, error: { code, message } });

async function handleMessage(msg, viewer) {
  const { id, method, params } = msg || {};
  if (!method) return id !== undefined ? rpcError(id, -32600, "Invalid request") : null;

  // Notifications (no id) get no response body.
  if (id === undefined || id === null) return null;

  switch (method) {
    case "initialize": {
      // Echo any date-shaped protocol version the client requests: this server
      // only uses the tools surface, which is stable across MCP revisions, and
      // some clients (claude.ai among them) refuse to downgrade — answering
      // with an older version than they asked for reads as a failed connection.
      const requested = params && params.protocolVersion;
      const version = /^\d{4}-\d{2}-\d{2}$/.test(requested || "") ? requested : PROTOCOL_VERSIONS[0];
      return rpcResult(id, {
        protocolVersion: version,
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: "artemis-warehouse", title: "ArtemisAI Warehouse", version: "1.0.0" },
        instructions:
          "Read-only access to the ArtemisAI Facebook analytics warehouse (Redshift). " +
          "Start with list_tables, then describe_table, then run_query for analysis. " +
          `Signed in as: ${viewer}.`,
      });
    }
    case "ping":
      return rpcResult(id, {});
    case "tools/list":
      return rpcResult(id, { tools: TOOLS });
    case "tools/call": {
      const name = params && params.name;
      const args = (params && params.arguments) || {};
      let text;
      try {
        text = await callTool(name, args);
      } catch (err) {
        return rpcResult(id, {
          content: [{ type: "text", text: `Error: ${String(err.message || err).slice(0, 300)}` }],
          isError: true,
        });
      }
      if (text === null) return rpcError(id, -32602, `Unknown tool: ${name}`);
      const isError = typeof text === "string" && /^(Error|Query error):/.test(text);
      return rpcResult(id, { content: [{ type: "text", text }], isError });
    }
    default:
      return rpcError(id, -32601, `Method not found: ${method}`);
  }
}

// Temporary connection-debug capture: records every request (key redacted) so
// we can see exactly what a failing client sends. Dump with GET ?debug=<key>.
async function captureRequest(event, status) {
  try {
    connectLambda(event);
    const store = getStore("mcp-debug");
    const prior = (await store.get("recent", { type: "json" })) || [];
    const redactedPath = (event.path || "").replace(/\/mcp\/[^/?#]+/, "/mcp/<key>");
    prior.push({
      at: new Date().toISOString(),
      method: event.httpMethod,
      path: redactedPath,
      hasQueryKey: Boolean((event.queryStringParameters || {}).key),
      headers: {
        "user-agent": event.headers["user-agent"] || "",
        accept: event.headers.accept || "",
        "content-type": event.headers["content-type"] || "",
        "mcp-protocol-version": event.headers["mcp-protocol-version"] || "",
        "mcp-session-id": event.headers["mcp-session-id"] || "",
      },
      body: (event.body || "").slice(0, 300),
      respondedWith: status,
    });
    await store.setJSON("recent", prior.slice(-40));
  } catch {}
}

async function respond(event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  // Debug dump (requires a valid access key): GET /api/mcp?debug=<key>
  const debugKey = (event.queryStringParameters || {}).debug;
  if (event.httpMethod === "GET" && debugKey && keyTable()[debugKey]) {
    try {
      connectLambda(event);
      const store = getStore("mcp-debug");
      const log = (await store.get("recent", { type: "json" })) || [];
      return { statusCode: 200, headers: HEADERS, body: JSON.stringify(log) };
    } catch (err) {
      return { statusCode: 500, headers: HEADERS, body: JSON.stringify({ error: String(err.message || err) }) };
    }
  }
  if (event.httpMethod === "GET" || event.httpMethod === "DELETE") {
    // Stateless server: no SSE stream to open, no session to delete.
    return { statusCode: 405, headers: HEADERS, body: JSON.stringify(rpcError(null, -32000, "Method not allowed")) };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: HEADERS, body: JSON.stringify(rpcError(null, -32000, "POST only")) };
  }

  if (!process.env.MCP_DB_USER || !process.env.MCP_DB_PASSWORD || !process.env.MCP_ANALYST_KEYS) {
    return { statusCode: 500, headers: HEADERS, body: JSON.stringify(rpcError(null, -32000, "Server not configured: set MCP_DB_USER, MCP_DB_PASSWORD and MCP_ANALYST_KEYS")) };
  }

  const viewer = authenticate(event);
  if (!viewer) {
    return { statusCode: 401, headers: HEADERS, body: JSON.stringify(rpcError(null, -32000, "Unauthorized: missing or invalid access key")) };
  }

  let parsed;
  try {
    parsed = JSON.parse(event.body || "{}");
  } catch {
    return { statusCode: 400, headers: HEADERS, body: JSON.stringify(rpcError(null, -32700, "Parse error")) };
  }

  const messages = Array.isArray(parsed) ? parsed : [parsed];
  const responses = [];
  for (const m of messages) {
    const r = await handleMessage(m, viewer);
    if (r) responses.push(r);
  }

  if (!responses.length) return { statusCode: 202, headers: HEADERS, body: "" };
  const body = Array.isArray(parsed) ? responses : responses[0];
  return { statusCode: 200, headers: HEADERS, body: JSON.stringify(body) };
}

exports.handler = async (event) => {
  const res = await respond(event);
  const isDebugDump = event.httpMethod === "GET" && (event.queryStringParameters || {}).debug;
  if (event.httpMethod !== "OPTIONS" && !isDebugDump) await captureRequest(event, res.statusCode);
  return res;
};
