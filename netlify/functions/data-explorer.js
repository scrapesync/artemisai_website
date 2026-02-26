// netlify/functions/data-explorer.js
// Validates credentials against public.portal_users table you create in Redshift
// No auto-creation of tables — you run the CREATE TABLE + INSERT yourself

const { Client } = require("pg");

function createClient() {
  return new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439"),
    database: process.env.REDSHIFT_DATABASE,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 10000,
    query_timeout: 30000,
  });
}

const ALLOWED_TABLES = [
  { schema: "odl", table: "comment_sentiments", label: "Comment Sentiments", description: "Sentiment analysis results for post comments" },
  { schema: "odl", table: "comment_sentiments_v2", label: "Comment Sentiments v2", description: "Updated sentiment analysis with improved model accuracy" },
  { schema: "odl", table: "dim_comments", label: "Comments (Dimension)", description: "All Facebook post comments with text, author, and timestamps" },
  { schema: "odl", table: "dim_date", label: "Date (Dimension)", description: "Date dimension table for time-based analysis and joins" },
  { schema: "odl", table: "dim_geographies", label: "Geographies (Dimension)", description: "Geographic regions for audience location analysis" },
  { schema: "odl", table: "dim_metrics", label: "Metrics (Dimension)", description: "Metric definitions and metadata for engagement tracking" },
  { schema: "odl", table: "dim_page_categories", label: "Page Categories (Dimension)", description: "Facebook page category classifications" },
  { schema: "odl", table: "dim_pages", label: "Pages (Dimension)", description: "Connected Facebook pages with metadata and status" },
  { schema: "odl", table: "dim_posts", label: "Posts (Dimension)", description: "All Facebook posts with text, media type, timestamps, and URLs" },
  { schema: "odl", table: "dim_reaction_types", label: "Reaction Types (Dimension)", description: "Facebook reaction type definitions (like, love, wow, etc.)" },
  { schema: "odl", table: "fact_page_daily_demographics_insights", label: "Page Demographics (Daily)", description: "Daily page-level demographics: age, gender, location breakdowns" },
  { schema: "odl", table: "fact_page_daily_insights", label: "Page Insights (Daily)", description: "Daily page-level metrics: reach, impressions, followers, engagement" },
  { schema: "odl", table: "fact_post_daily_insights", label: "Post Insights (Daily)", description: "Daily post-level metrics: reach, impressions, clicks, reactions" },
  { schema: "odl", table: "gpt_model_prediction", label: "GPT Model Predictions", description: "GPT-generated virality and engagement predictions per post" },
  { schema: "odl", table: "gpt_post_recommendation", label: "GPT Post Recommendations", description: "GPT-generated content strategy recommendations per post" },
  { schema: "odl", table: "sentiments_overall", label: "Overall Sentiments", description: "Aggregated sentiment scores across all posts and comments" },
  { schema: "public", table: "artemis_fb_connections", label: "FB Connections", description: "Connected Facebook pages from the website connect flow" },
  { schema: "public", table: "ml_comment_sentiment_results", label: "ML Comment Sentiments", description: "ML pipeline sentiment classification results for comments" },
  { schema: "rdl", table: "page_daily_insights", label: "Page Daily Insights (RDL)", description: "Refined daily page metrics after transformation and cleaning" },
  { schema: "rdl", table: "page_demographics_insights", label: "Page Demographics (RDL)", description: "Refined page demographics data after transformation" },
  { schema: "rdl", table: "page_info", label: "Page Info (RDL)", description: "Refined page metadata: name, category, followers, verification" },
  { schema: "rdl", table: "page_posts", label: "Page Posts (RDL)", description: "Refined posts data with cleaned text and normalized fields" },
  { schema: "rdl", table: "post_comments", label: "Post Comments (RDL)", description: "Refined comments data with cleaned text and threading" },
  { schema: "rdl", table: "post_daily_insights", label: "Post Daily Insights (RDL)", description: "Refined daily post metrics after transformation and cleaning" },
  { schema: "rdl", table: "post_reactions", label: "Post Reactions (RDL)", description: "Refined post reactions data with reaction type breakdowns" },
];

exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };
  if (event.httpMethod !== "POST") return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };

  let payload;
  try { payload = JSON.parse(event.body); } catch { return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) }; }

  const { action } = payload;
  const client = createClient();

  try {
    await client.connect();

    // ═══════════════════════════════════════
    // LOGIN — validate against portal_users
    // ═══════════════════════════════════════
    if (action === "login") {
      const { username, password } = payload;
      if (!username || !password) return { statusCode: 400, headers, body: JSON.stringify({ error: "Username and password required" }) };

      const res = await client.query(
        `SELECT id, username, full_name, role FROM public.portal_users WHERE username = $1 AND password = $2 AND is_active = true`,
        [username.toLowerCase().trim(), password]
      );

      if (res.rows.length === 0) return { statusCode: 401, headers, body: JSON.stringify({ error: "Invalid username or password" }) };

      const user = res.rows[0];
      await client.query(`UPDATE public.portal_users SET last_login = GETDATE() WHERE id = $1`, [user.id]);

      return { statusCode: 200, headers, body: JSON.stringify({ success: true, user }) };
    }

    // ─── Auth check for all other actions ───
    const userId = payload.user_id;
    if (!userId) return { statusCode: 401, headers, body: JSON.stringify({ error: "Authentication required" }) };

    const authCheck = await client.query(`SELECT id, username, full_name, role FROM public.portal_users WHERE id = $1 AND is_active = true`, [userId]);
    if (authCheck.rows.length === 0) return { statusCode: 401, headers, body: JSON.stringify({ error: "Session invalid" }) };

    // ═══════════════════════════════════════
    // LIST TABLES
    // ═══════════════════════════════════════
    if (action === "list_tables") {
      const tables = [];
      for (const t of ALLOWED_TABLES) {
        try {
          const countRes = await client.query(`SELECT COUNT(*) as cnt FROM ${t.schema}.${t.table}`);
          const colRes = await client.query(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
            [t.schema, t.table]
          );
          tables.push({ ...t, row_count: parseInt(countRes.rows[0].cnt), columns: colRes.rows.map(c => ({ name: c.column_name, type: c.data_type })) });
        } catch {
          tables.push({ ...t, row_count: 0, columns: [], exists: false });
        }
      }
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, tables }) };
    }

    // ═══════════════════════════════════════
    // BROWSE TABLE (paginated)
    // ═══════════════════════════════════════
    if (action === "browse") {
      const { schema, table, page = 1, page_size = 50, sort_by, sort_dir = "desc" } = payload;
      const allowed = ALLOWED_TABLES.find(t => t.table === table && t.schema === schema);
      if (!allowed) return { statusCode: 403, headers, body: JSON.stringify({ error: "Table not accessible" }) };

      const offset = (page - 1) * page_size;
      const limit = Math.min(page_size, 200);

      const colRes = await client.query(
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
        [allowed.schema, allowed.table]
      );
      const columns = colRes.rows;
      const validSort = sort_by && columns.some(c => c.column_name === sort_by);
      const orderClause = validSort ? `ORDER BY "${sort_by}" ${sort_dir === "asc" ? "ASC" : "DESC"}` : `ORDER BY 1 DESC`;

      const countRes = await client.query(`SELECT COUNT(*) as cnt FROM ${allowed.schema}.${allowed.table}`);
      const total = parseInt(countRes.rows[0].cnt);
      const dataRes = await client.query(`SELECT * FROM ${allowed.schema}.${allowed.table} ${orderClause} LIMIT ${limit} OFFSET ${offset}`);

      return { statusCode: 200, headers, body: JSON.stringify({
        success: true, schema: allowed.schema, table: allowed.table, label: allowed.label, description: allowed.description,
        columns: columns.map(c => ({ name: c.column_name, type: c.data_type })), rows: dataRes.rows, total, page, page_size: limit, total_pages: Math.ceil(total / limit),
      })};
    }

    // ═══════════════════════════════════════
    // EXPORT (all rows for Excel)
    // ═══════════════════════════════════════
    if (action === "export") {
      const { schema, table } = payload;
      const allowed = ALLOWED_TABLES.find(t => t.table === table && t.schema === schema);
      if (!allowed) return { statusCode: 403, headers, body: JSON.stringify({ error: "Table not accessible" }) };

      const dataRes = await client.query(`SELECT * FROM ${allowed.schema}.${allowed.table} ORDER BY 1 DESC LIMIT 50000`);
      const colRes = await client.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
        [allowed.schema, allowed.table]
      );

      return { statusCode: 200, headers, body: JSON.stringify({
        success: true, schema: allowed.schema, table: allowed.table, label: allowed.label,
        columns: colRes.rows.map(c => c.column_name), rows: dataRes.rows, exported_at: new Date().toISOString(),
      })};
    }

    return { statusCode: 400, headers, body: JSON.stringify({ error: "Unknown action" }) };
  } catch (err) {
    console.error("Data explorer error:", err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: "Server error", detail: err.message }) };
  } finally {
    try { await client.end(); } catch {}
  }
};
