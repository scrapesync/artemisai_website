// netlify/functions/live-metrics.js
// Live refresh for the QA Dashboard: queries Redshift directly and returns the
// numbers the dashboard shows, right now, without waiting for the nightly job.
//
// WHY THIS EXISTS ALONGSIDE THE GITHUB ACTION
// -------------------------------------------
// .github/workflows/qa-dashboard-metrics.yml runs scripts/generate_qa_metrics.py
// at 01:00 UTC, commits qa_metrics.json back to the repo, and Netlify redeploys.
// That is the durable snapshot everyone sees on load, and it stays exactly as it
// is. But it cannot be the "refresh" button: a commit plus a site rebuild is
// minutes away, and it writes to the repo every time somebody presses it.
// So the button reads Redshift straight from here. Nothing is written anywhere,
// the answer comes back in seconds, and the snapshot on disk is untouched.
//
// The SQL below is deliberately the same measure as the Python generator, so a
// live number and a snapshot number mean the same thing and can sit side by side.
//
// SECTIONS (call them in parallel, each gets its own function budget):
//   ?section=core     one full pass over rdl.post_label_predictions:
//                     per-dimension agreement, routing tiers, label completeness,
//                     confidence validity, totals, confusion matrix
//   ?section=trends   30-day daily series for post agreement, post/DeepSeek
//                     confidence, comment agreement, FUSION and video confidence
//   ?section=arrival  did the weekly load land: per-table freshness, rows per day,
//                     duplicate keys, orphan rows, negative reactions
//
// Each section returns a fragment shaped exactly like qa_metrics.json, so the page
// deep-merges it over the snapshot. Anything a section could not compute is simply
// left out and the snapshot value keeps showing.
//
// ENV: REDSHIFT_HOST / REDSHIFT_PORT / REDSHIFT_DATABASE (or REDSHIFT_DB) /
//      REDSHIFT_USER / REDSHIFT_PASSWORD. A SELECT-only user is enough.

const { Client } = require("pg");

// Netlify cuts a synchronous function off at 10s, so every statement is capped
// below that and a section returns whatever it managed rather than failing whole.
const STATEMENT_MS = 7500;
const CONNECT_MS = 4000;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
};

/* ---------- small helpers ---------- */

const num = (v) => (v == null ? null : Number(v));
const iso = (d) => (d == null ? null : String(d).slice(0, 10));

// Round to one decimal, keeping null as null (null means "not measured").
function r1(v) {
  return v == null || isNaN(v) ? null : Math.round(Number(v) * 10) / 10;
}

// Percentage from a numerator/denominator pair. A zero denominator is "no
// comparisons were possible", which is null, not zero.
function pct(hit, of) {
  const d = Number(of || 0);
  return d ? r1((100 * Number(hit || 0)) / d) : null;
}

// Fill a 30-day array ending today from rows keyed by date, so the sparkline
// has one slot per day whether or not the pipeline ran that day.
function daily30(rows, keyCol, valFn) {
  const by = {};
  for (const row of rows) {
    const k = iso(row[keyCol]);
    if (k) by[k] = valFn(row);
  }
  const out = [];
  const today = new Date();
  for (let i = 29; i >= 0; i--) {
    const d = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - i));
    const k = d.toISOString().slice(0, 10);
    out.push(Object.prototype.hasOwnProperty.call(by, k) ? by[k] : null);
  }
  return out;
}

function axis30() {
  const out = [];
  const today = new Date();
  for (let i = 29; i >= 0; i--) {
    const d = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - i));
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

/* ---------- section: core ----------
   One scan of the predictions table answers most of the accuracy tab and half
   the quality tab. Doing it as a single statement keeps the whole section well
   inside the function budget. */

async function core(run) {
  const out = { totals: {}, tabs: { accuracy: {}, quality: {} } };

  const agg = await run(
    "core-aggregate",
    `SELECT
       COUNT(*)                                                             AS total,
       SUM(CASE WHEN nlp_sentiment = ds_sentiment THEN 1 ELSE 0 END)        AS hit_sentiment,
       SUM(CASE WHEN nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL THEN 1 ELSE 0 END) AS cmp_sentiment,
       SUM(CASE WHEN nlp_emotion = ds_emotion THEN 1 ELSE 0 END)            AS hit_emotion,
       SUM(CASE WHEN nlp_emotion IS NOT NULL AND ds_emotion IS NOT NULL THEN 1 ELSE 0 END)     AS cmp_emotion,
       SUM(CASE WHEN nlp_toxicity = ds_toxicity THEN 1 ELSE 0 END)          AS hit_toxicity,
       SUM(CASE WHEN nlp_toxicity IS NOT NULL AND ds_toxicity IS NOT NULL THEN 1 ELSE 0 END)   AS cmp_toxicity,
       COUNT(nlp_sentiment) AS c_sentiment,
       COUNT(nlp_emotion)   AS c_emotion,
       COUNT(nlp_topic)     AS c_topic,
       COUNT(nlp_intent)    AS c_intent,
       COUNT(nlp_toxicity)  AS c_toxicity,
       COUNT(ds_sentiment)  AS c_ds_sentiment,
       COUNT(cl_sentiment)  AS c_cl_sentiment,
       ROUND(100 * AVG(nlp_sentiment_confidence), 1) AS nlp_conf,
       ROUND(100 * AVG(ds_sentiment_confidence), 1)  AS ds_conf,
       SUM(CASE WHEN nlp_sentiment_confidence < 0 OR nlp_sentiment_confidence > 1 THEN 1 ELSE 0 END) AS bad_conf
     FROM rdl.post_label_predictions`
  );

  if (agg && agg[0]) {
    const a = agg[0];
    const total = Number(a.total || 0);

    // Overall agreement doubles as the fallback the cards use when the last
    // 30 days produced no comparisons at all.
    out.tabs.accuracy.agreement = { month: [pct(a.hit_sentiment, a.cmp_sentiment)] };
    out.tabs.accuracy.nlp_conf = { month: [r1(num(a.nlp_conf))] };
    out.tabs.accuracy.ds_conf = { month: [r1(num(a.ds_conf))] };

    out.tabs.accuracy.per_dim = [
      ["sentiment", pct(a.hit_sentiment, a.cmp_sentiment) || 0],
      ["emotion", pct(a.hit_emotion, a.cmp_emotion) || 0],
      ["toxicity", pct(a.hit_toxicity, a.cmp_toxicity) || 0],
    ];

    out.tabs.accuracy.escalation = [
      ["NLP tier", Number(a.c_sentiment || 0)],
      ["DeepSeek tier", Number(a.c_ds_sentiment || 0)],
      ["Claude tier", Number(a.c_cl_sentiment || 0)],
    ];

    const share = (n) => (total ? r1((100 * Number(n || 0)) / total) : 0);
    out.tabs.quality.completeness = [
      ["sentiment", share(a.c_sentiment)],
      ["emotion", share(a.c_emotion)],
      ["topic", share(a.c_topic)],
      ["intent", share(a.c_intent)],
      ["toxicity", share(a.c_toxicity)],
      ["ds sentiment", share(a.c_ds_sentiment)],
    ];

    out.tabs.quality.validity = [["confidence out of range", Number(a.bad_conf || 0)]];
    out.totals.posts = total;
  }

  const conf = await run(
    "confusion",
    `SELECT nlp_sentiment AS n, ds_sentiment AS d, COUNT(*) AS c
       FROM rdl.post_label_predictions
      WHERE nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL
      GROUP BY 1, 2`
  );
  if (conf) {
    const cats = ["positive", "neutral", "negative"];
    const m = {};
    for (const row of conf) m[String(row.n) + "|" + String(row.d)] = Number(row.c || 0);
    out.tabs.accuracy.confusion = {
      rows: cats,
      cols: cats,
      v: cats.map((rr) => cats.map((cc) => m[rr + "|" + cc] || 0)),
    };
  }

  const tot = await run(
    "totals",
    `SELECT (SELECT COUNT(*) FROM rdl.post_comments) AS comments,
            (SELECT COUNT(*) FROM odl.dim_pages)    AS pages`
  );
  if (tot && tot[0]) {
    out.totals.comments = Number(tot[0].comments || 0);
    out.totals.pages = Number(tot[0].pages || 0);
  }

  return out;
}

/* ---------- section: trends ----------
   The 30-day sparklines. Each query returns per-day hits and comparisons (or a
   per-day mean and a row count), so the overall figure is computed here by
   weighting, which costs nothing extra and keeps a card from reading
   "not measured" when the window happens to be quiet. */

async function trends(run) {
  const out = { window: { axis: axis30() }, tabs: { accuracy: {} } };

  const post = await run(
    "post-agreement-daily",
    `SELECT inserted_at::date AS d,
            SUM(CASE WHEN nlp_sentiment = ds_sentiment THEN 1 ELSE 0 END) AS hit,
            SUM(CASE WHEN nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL THEN 1 ELSE 0 END) AS cmp,
            AVG(nlp_sentiment_confidence) AS nlp_conf,
            AVG(ds_sentiment_confidence)  AS ds_conf,
            COUNT(*) AS n
       FROM rdl.post_label_predictions
      WHERE inserted_at >= DATEADD(day, -30, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1`
  );
  if (post) {
    // week is blanked deliberately. The card falls back day -> week -> month, so
    // leaving the snapshot's weekly roll in place would let a stale number show
    // through under a "Live" heading.
    out.tabs.accuracy.agreement = { day: daily30(post, "d", (r) => pct(r.hit, r.cmp)), week: [] };
    out.tabs.accuracy.nlp_conf = { day: daily30(post, "d", (r) => r1(100 * Number(r.nlp_conf || 0))), week: [] };
    out.tabs.accuracy.ds_conf = { day: daily30(post, "d", (r) => r1(100 * Number(r.ds_conf || 0))), week: [] };
  }

  const comment = await run(
    "comment-agreement-daily",
    `SELECT created_at::date AS d,
            SUM(CASE WHEN nlp_sentiment = ds_sentiment THEN 1 ELSE 0 END) AS hit,
            SUM(CASE WHEN nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL THEN 1 ELSE 0 END) AS cmp
       FROM rdl.comment_label_predictions
      WHERE created_at >= DATEADD(day, -30, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1`
  );
  if (comment) {
    let hit = 0, cmp = 0;
    for (const r of comment) { hit += Number(r.hit || 0); cmp += Number(r.cmp || 0); }
    out.tabs.accuracy.comment_agreement = {
      day: daily30(comment, "d", (r) => pct(r.hit, r.cmp)),
      week: [],
      month: [pct(hit, cmp)],
    };
  }

  const meanSeries = async (label, key, table, col, dateCol) => {
    const rows = await run(
      label,
      `SELECT ${dateCol}::date AS d, AVG(${col}) AS v, COUNT(*) AS n
         FROM ${table}
        WHERE ${dateCol} >= DATEADD(day, -30, CURRENT_DATE) AND ${col} IS NOT NULL
        GROUP BY 1
        ORDER BY 1`
    );
    if (!rows) return;
    let sum = 0, n = 0;
    for (const r of rows) { sum += Number(r.v || 0) * Number(r.n || 0); n += Number(r.n || 0); }
    out.tabs.accuracy[key] = {
      day: daily30(rows, "d", (r) => r1(100 * Number(r.v || 0))),
      week: [],
      month: [n ? r1((100 * sum) / n) : null],
    };
  };

  await meanSeries("fusion-conf", "fusion_conf", "rdl.fusion_predictions", "confidence", "processed_at");
  await meanSeries("video-conf", "video_conf", "rdl.video_intelligence", "scene_confidence", "processed_at");

  return out;
}

/* ---------- section: arrival ----------
   The data-quality tab. Freshness is asked table by table on purpose: one table
   the reader cannot see should not blank out the other five. */

const FRESH = [
  ["rdl.page_posts", "load_date"],
  ["rdl.post_comments", "load_date"],
  ["rdl.post_daily_insights", "load_date"],
  ["rdl.page_daily_insights", "load_date"],
  ["rdl.post_label_predictions", "inserted_at"],
  ["rdl.fusion_predictions", "processed_at"],
];

// Same rule as the nightly job: the pipeline runs weekly, so landing on any day
// of this ISO week is a pass, last week is flaky, older than that is a failure.
function freshnessStatus(dateStr) {
  const today = new Date();
  const utcToday = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  const dow = (utcToday.getUTCDay() + 6) % 7; // Monday = 0
  const weekStart = new Date(utcToday.getTime() - dow * 86400000);
  const prevWeekStart = new Date(weekStart.getTime() - 7 * 86400000);

  if (!dateStr) return ["fail", "never"];
  const d = new Date(dateStr + "T00:00:00Z");
  if (d >= weekStart) return ["pass", "this week"];
  if (d >= prevWeekStart) return ["flaky", "last week"];
  return ["fail", Math.round((utcToday - d) / 86400000) + "d ago"];
}

async function arrival(run) {
  const out = { tabs: { health: {}, quality: {} } };

  const rows = [];
  for (const [table, col] of FRESH) {
    const res = await run(
      "fresh:" + table,
      `SELECT MAX(${col})::date AS d, COUNT(*) AS n FROM ${table}`
    );
    const short = table.split(".").pop();
    if (!res || !res[0]) { rows.push([short, "error", "fail", "-", "unreadable"]); continue; }
    const d = iso(res[0].d);
    const [status, note] = freshnessStatus(d);
    rows.push([short, d || "never", status, Number(res[0].n || 0).toLocaleString("en-GB"), note]);
  }
  rows.sort((a, b) => (a[1] < b[1] ? 1 : a[1] > b[1] ? -1 : 0));
  out.tabs.health.freshness = rows;

  const loads = await run(
    "daily-loads",
    `SELECT load_date AS d, COUNT(*) AS n
       FROM rdl.page_posts
      WHERE load_date >= DATEADD(day, -30, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1`
  );
  if (loads) {
    out.window = { axis: axis30() };
    out.tabs.health.daily_loads = {
      posts: { day: daily30(loads, "d", (r) => Number(r.n || 0)).map((v) => (v == null ? 0 : v)) },
    };
  }

  const dupes = await run(
    "dupes",
    `SELECT 'dim_comments' AS t, COUNT(*) - COUNT(DISTINCT comment_sk) AS dup FROM odl.dim_comments
     UNION ALL SELECT 'dim_posts', COUNT(*) - COUNT(DISTINCT post_sk) FROM odl.dim_posts
     UNION ALL SELECT 'dim_pages', COUNT(*) - COUNT(DISTINCT page_sk) FROM odl.dim_pages`
  );
  if (dupes) out.tabs.quality.dupes = dupes.map((r) => [r.t, Number(r.dup || 0)]);

  const integrity = [];
  const orphanFacts = await run(
    "orphan-facts",
    `SELECT COUNT(*) AS n
       FROM odl.fact_post_daily_insights f
       LEFT JOIN odl.dim_posts d ON f.post_sk = d.post_sk
      WHERE d.post_sk IS NULL`
  );
  if (orphanFacts && orphanFacts[0]) integrity.push(["orphan post facts", Number(orphanFacts[0].n || 0)]);

  const orphanComments = await run(
    "orphan-comments",
    `SELECT COUNT(*) AS n
       FROM odl.dim_comments c
       LEFT JOIN odl.dim_posts p ON c.post_sk = p.post_sk
      WHERE p.post_sk IS NULL`
  );
  if (orphanComments && orphanComments[0]) integrity.push(["orphan comments", Number(orphanComments[0].n || 0)]);
  if (integrity.length) out.tabs.quality.integrity = integrity;

  const negative = await run(
    "negative-reactions",
    `SELECT COUNT(*) AS n FROM odl.dim_comments WHERE reactions_total < 0`
  );
  if (negative && negative[0]) {
    // Deliberately a second key: the core section owns quality.validity, and two
    // sections writing the same array would mean whichever finished last wins.
    out.tabs.quality.validity2 = [["negative reactions", Number(negative[0].n || 0)]];
  }

  return out;
}

const SECTIONS = { core, trends, arrival };

/* ---------- handler ---------- */

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  const name = ((event.queryStringParameters || {}).section || "core").toLowerCase();
  const section = SECTIONS[name];
  if (!section) {
    return {
      statusCode: 400,
      headers: HEADERS,
      body: JSON.stringify({ error: "Unknown section. Use core, trends or arrival." }),
    };
  }

  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439", 10),
    database: process.env.REDSHIFT_DATABASE || process.env.REDSHIFT_DB,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: CONNECT_MS,
    query_timeout: STATEMENT_MS,
    statement_timeout: STATEMENT_MS,
  });

  const started = Date.now();
  const failed = [];

  // One query failing must not take the section with it. Each statement runs on
  // its own, the error is recorded, and the caller carries on: a table the
  // reader has no grant on should cost one row, not the whole tab.
  const run = async (label, sql) => {
    try {
      const res = await client.query(sql);
      return res.rows;
    } catch (err) {
      failed.push({ query: label, message: String(err.message || err).slice(0, 200) });
      return null;
    }
  };

  try {
    await client.connect();
    const payload = await section(run);
    payload.live = true;
    payload.section = name;
    payload.queried_at = new Date().toISOString();
    payload.ms = Date.now() - started;
    if (failed.length) payload.partial = failed;

    return { statusCode: 200, headers: HEADERS, body: JSON.stringify(payload) };
  } catch (err) {
    return {
      statusCode: 502,
      headers: HEADERS,
      body: JSON.stringify({
        error: "Could not reach Redshift: " + String(err.message || err).slice(0, 200),
        section: name,
        ms: Date.now() - started,
      }),
    };
  } finally {
    try { await client.end(); } catch { /* already gone */ }
  }
};
