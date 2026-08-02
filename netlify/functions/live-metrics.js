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
// TWO SECTIONS, NOT MORE. Call them in parallel:
//   ?section=models   are the models still accurate: agreement per dimension,
//                     routing tiers, label completeness, the confusion matrix and
//                     the 30-day series for posts, comments, FUSION and video
//   ?section=data     did the data arrive: per-table freshness, rows per day,
//                     row counts, duplicate keys, orphan rows, negative reactions
//
// The first cut of this ran three sections with nine statements between them, and
// against the real cluster two of them timed out. Not because the SQL is slow
// (measured on production: every statement here lands between 0.9s and 2.9s) but
// because several connections asking at once queue behind each other in Redshift's
// WLM, and the wait, not the work, blew the budget. Hence two connections rather
// than three, and every statement that can be folded into one round trip has been.
//
// Each section returns a fragment shaped like qa_metrics.json, so the page
// deep-merges it over the snapshot. Anything a section could not compute is left
// out, the snapshot value keeps showing, and the failure is named in `partial`.
//
// ENV: REDSHIFT_HOST / REDSHIFT_PORT / REDSHIFT_DATABASE (or REDSHIFT_DB) /
//      REDSHIFT_USER / REDSHIFT_PASSWORD. A SELECT-only user is enough.

const { Client } = require("pg");

// Generous per-statement allowance, because the risk is queue wait rather than
// query cost. DEADLINE_MS stops a section issuing a statement it has no time to
// finish: better to skip one and say so than to have the platform kill the whole
// invocation and lose the answers that already came back.
const STATEMENT_MS = 12000;
const DEADLINE_MS = 19000;
const CONNECT_MS = 5000;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
};

/* ---------- small helpers ---------- */

// pg hands back DATE and TIMESTAMP columns as JS Date objects, not strings, so
// String(d).slice(0,10) would quietly produce "Fri Jul 17" and every day key
// would miss. Dates are read through their own accessors instead.
function pad(n) { return n < 10 ? "0" + n : String(n); }
function ymd(d) { return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()); }
function iso(v) {
  if (v == null) return null;
  if (v instanceof Date) return ymd(v);
  return String(v).slice(0, 10);
}

// The last n calendar days, oldest first, as yyyy-mm-dd.
function dayKeys(n) {
  const out = [], t = new Date();
  for (let i = n - 1; i >= 0; i--) {
    out.push(ymd(new Date(t.getFullYear(), t.getMonth(), t.getDate() - i)));
  }
  return out;
}

function r1(v) {
  return v == null || isNaN(v) ? null : Math.round(Number(v) * 10) / 10;
}

// Percentage from a numerator/denominator pair. A zero denominator means no
// comparisons were possible, which is null, not zero.
function pct(hit, of) {
  const d = Number(of || 0);
  return d ? r1((100 * Number(hit || 0)) / d) : null;
}

// One slot per day whether or not the pipeline ran that day.
function daily30(rows, keyCol, valFn) {
  const by = {};
  for (const row of rows) {
    const k = iso(row[keyCol]);
    if (k) by[k] = valFn(row);
  }
  return dayKeys(30).map((k) => (Object.prototype.hasOwnProperty.call(by, k) ? by[k] : null));
}

/* ---------- section: models ---------- */

async function models(run) {
  const out = { window: { axis: dayKeys(30) }, totals: {}, tabs: { accuracy: {}, quality: {} } };

  // Do the two sides of a dimension even speak the same language? The NLP models
  // and the second pass were trained separately, and toxicity turns out to use
  // non_toxic/toxic on one side and none/borderline/coded_political on the other.
  // String equality across those can only ever return 0%, which reads as "the
  // model is catastrophically wrong" when the truth is "these two cannot be
  // compared". So the vocabularies are intersected first, and a dimension with no
  // shared label reports nothing rather than a damning zero.
  const vocab = await run(
    "label-vocabularies",
    `SELECT 'sentiment' AS dim, COUNT(*) AS shared FROM (
       SELECT DISTINCT nlp_sentiment AS v FROM rdl.post_label_predictions WHERE nlp_sentiment IS NOT NULL
       INTERSECT
       SELECT DISTINCT ds_sentiment FROM rdl.post_label_predictions WHERE ds_sentiment IS NOT NULL) a
     UNION ALL
     SELECT 'emotion', COUNT(*) FROM (
       SELECT DISTINCT nlp_emotion AS v FROM rdl.post_label_predictions WHERE nlp_emotion IS NOT NULL
       INTERSECT
       SELECT DISTINCT ds_emotion FROM rdl.post_label_predictions WHERE ds_emotion IS NOT NULL) b
     UNION ALL
     SELECT 'toxicity', COUNT(*) FROM (
       SELECT DISTINCT nlp_toxicity AS v FROM rdl.post_label_predictions WHERE nlp_toxicity IS NOT NULL
       INTERSECT
       SELECT DISTINCT ds_toxicity FROM rdl.post_label_predictions WHERE ds_toxicity IS NOT NULL) c`
  );
  const shared = {};
  if (vocab) for (const r of vocab) shared[r.dim] = Number(r.shared || 0);
  // Unknown (the query failed) is treated as comparable: better to show the
  // number than to hide it because a side check did not run.
  const comparable = (dim) => shared[dim] === undefined || shared[dim] > 0;

  // One scan of the predictions table answers per-dimension agreement, the
  // routing split, label completeness and the confidence range check.
  const agg = await run(
    "model-aggregate",
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

    // The overall figure doubles as the fallback a card uses when the last
    // 30 days produced no comparisons at all.
    out.tabs.accuracy.agreement = { month: [pct(a.hit_sentiment, a.cmp_sentiment)] };
    out.tabs.accuracy.nlp_conf = { month: [r1(a.nlp_conf)] };
    out.tabs.accuracy.ds_conf = { month: [r1(a.ds_conf)] };

    // Rows are [name, value, note]. A null value with a note is the page's cue to
    // print the reason instead of a percentage.
    const dim = (name, hit, cmp) =>
      comparable(name)
        ? [name, pct(hit, cmp) || 0]
        : [name, null, "the two models use different labels here, so they cannot be compared"];
    out.tabs.accuracy.per_dim = [
      dim("sentiment", a.hit_sentiment, a.cmp_sentiment),
      dim("emotion", a.hit_emotion, a.cmp_emotion),
      dim("toxicity", a.hit_toxicity, a.cmp_toxicity),
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

  // week is blanked deliberately on every live series. The cards fall back
  // day -> week -> month, so leaving the snapshot's weekly roll in place would
  // let a stale number show through under a "Live" heading.
  const post = await run(
    "post-trends",
    `SELECT inserted_at::date AS d,
            SUM(CASE WHEN nlp_sentiment = ds_sentiment THEN 1 ELSE 0 END) AS hit,
            SUM(CASE WHEN nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL THEN 1 ELSE 0 END) AS cmp,
            AVG(nlp_sentiment_confidence) AS nlp_conf,
            AVG(ds_sentiment_confidence)  AS ds_conf
       FROM rdl.post_label_predictions
      WHERE inserted_at >= DATEADD(day, -30, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1`
  );
  if (post) {
    const A = out.tabs.accuracy;
    A.agreement = Object.assign(A.agreement || {}, { day: daily30(post, "d", (r) => pct(r.hit, r.cmp)), week: [] });
    A.nlp_conf = Object.assign(A.nlp_conf || {}, { day: daily30(post, "d", (r) => r1(100 * Number(r.nlp_conf || 0))), week: [] });
    A.ds_conf = Object.assign(A.ds_conf || {}, { day: daily30(post, "d", (r) => r1(100 * Number(r.ds_conf || 0))), week: [] });
  }

  const comment = await run(
    "comment-trends",
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

  // Two tables, one round trip: they are the same shape, so a tag column is
  // cheaper than paying the queue twice.
  const vis = await run(
    "fusion-and-video",
    `SELECT 'fusion' AS k, processed_at::date AS d, AVG(confidence) AS v, COUNT(*) AS n
       FROM rdl.fusion_predictions
      WHERE processed_at >= DATEADD(day, -30, CURRENT_DATE) AND confidence IS NOT NULL
      GROUP BY 1, 2
     UNION ALL
     SELECT 'video', processed_at::date, AVG(scene_confidence), COUNT(*)
       FROM rdl.video_intelligence
      WHERE processed_at >= DATEADD(day, -30, CURRENT_DATE) AND scene_confidence IS NOT NULL
      GROUP BY 1, 2`
  );
  if (vis) {
    for (const [tag, key] of [["fusion", "fusion_conf"], ["video", "video_conf"]]) {
      const rows = vis.filter((r) => r.k === tag);
      if (!rows.length) continue;
      let sum = 0, n = 0;
      for (const r of rows) { sum += Number(r.v || 0) * Number(r.n || 0); n += Number(r.n || 0); }
      out.tabs.accuracy[key] = {
        day: daily30(rows, "d", (r) => r1(100 * Number(r.v || 0))),
        week: [],
        month: [n ? r1((100 * sum) / n) : null],
      };
    }
  }

  return out;
}

/* ---------- section: data ---------- */

const FRESH = [
  ["page_posts", "rdl.page_posts", "load_date"],
  ["post_comments", "rdl.post_comments", "load_date"],
  ["post_daily_insights", "rdl.post_daily_insights", "load_date"],
  ["page_daily_insights", "rdl.page_daily_insights", "load_date"],
  ["post_label_predictions", "rdl.post_label_predictions", "inserted_at"],
  ["fusion_predictions", "rdl.fusion_predictions", "processed_at"],
];

// Same rule as the nightly job: the pipeline runs weekly, so landing on any day
// of this ISO week is a pass, last week is flaky, older than that is a failure.
function freshnessStatus(dateStr) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const dow = (today.getDay() + 6) % 7; // Monday = 0
  const weekStart = new Date(today.getFullYear(), today.getMonth(), today.getDate() - dow);
  const prevWeekStart = new Date(weekStart.getFullYear(), weekStart.getMonth(), weekStart.getDate() - 7);

  if (!dateStr) return ["fail", "never"];
  const p = String(dateStr).split("-").map(Number);
  const d = new Date(p[0], p[1] - 1, p[2]);
  if (d >= weekStart) return ["pass", "this week"];
  if (d >= prevWeekStart) return ["flaky", "last week"];
  return ["fail", Math.round((today - d) / 86400000) + "d ago"];
}

function freshnessRow(name, lastDate, rowCount) {
  const d = iso(lastDate);
  const [status, note] = freshnessStatus(d);
  return [name, d || "never", status, Number(rowCount || 0).toLocaleString("en-GB"), note];
}

async function data(run) {
  const out = { window: { axis: dayKeys(30) }, totals: {}, tabs: { health: {}, quality: {} } };

  // Row counts, duplicate keys and the reactions range check are all single
  // scalars, so they travel together.
  const counts = await run(
    "counts",
    `SELECT (SELECT COUNT(*) FROM rdl.post_comments)                             AS comments,
            (SELECT COUNT(*) FROM odl.dim_pages)                                 AS pages,
            (SELECT COUNT(*) FROM odl.dim_comments WHERE reactions_total < 0)    AS negative_reactions,
            (SELECT COUNT(*) - COUNT(DISTINCT comment_sk) FROM odl.dim_comments) AS dup_comments,
            (SELECT COUNT(*) - COUNT(DISTINCT post_sk) FROM odl.dim_posts)       AS dup_posts,
            (SELECT COUNT(*) - COUNT(DISTINCT page_sk) FROM odl.dim_pages)       AS dup_pages`
  );
  if (counts && counts[0]) {
    const c = counts[0];
    out.totals.comments = Number(c.comments || 0);
    out.totals.pages = Number(c.pages || 0);
    out.tabs.quality.dupes = [
      ["dim_comments", Number(c.dup_comments || 0)],
      ["dim_posts", Number(c.dup_posts || 0)],
      ["dim_pages", Number(c.dup_pages || 0)],
    ];
    // A second key on purpose: the models section owns quality.validity, and two
    // sections writing the same array would mean whichever landed last wins.
    out.tabs.quality.validity2 = [["negative reactions", Number(c.negative_reactions || 0)]];
  }

  // One union rather than six round trips. If a reader lacks a grant on any one
  // table the union fails whole, so that case falls back to asking table by table.
  const unionSql = FRESH.map(([name, table, col], i) =>
    i === 0
      ? `SELECT '${name}' AS t, MAX(${col})::date AS d, COUNT(*) AS n FROM ${table}`
      : `     UNION ALL SELECT '${name}', MAX(${col})::date, COUNT(*) FROM ${table}`
  ).join("\n");

  const fresh = await run("freshness", unionSql);
  if (fresh) {
    out.tabs.health.freshness = fresh.map((r) => freshnessRow(r.t, r.d, r.n));
  } else {
    const rows = [];
    for (const [name, table, col] of FRESH) {
      const one = await run("freshness:" + name, `SELECT MAX(${col})::date AS d, COUNT(*) AS n FROM ${table}`);
      rows.push(one && one[0] ? freshnessRow(name, one[0].d, one[0].n) : [name, "error", "fail", "-", "unreadable"]);
    }
    out.tabs.health.freshness = rows;
  }
  out.tabs.health.freshness.sort((a, b) => (a[1] < b[1] ? 1 : a[1] > b[1] ? -1 : 0));

  const loads = await run(
    "daily-loads",
    `SELECT load_date AS d, COUNT(*) AS n
       FROM rdl.page_posts
      WHERE load_date >= DATEADD(day, -30, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1`
  );
  if (loads) {
    out.tabs.health.daily_loads = {
      posts: { day: daily30(loads, "d", (r) => Number(r.n || 0)).map((v) => (v == null ? 0 : v)) },
    };
  }

  const orphans = await run(
    "orphans",
    `SELECT (SELECT COUNT(*)
               FROM odl.fact_post_daily_insights f
               LEFT JOIN odl.dim_posts d ON f.post_sk = d.post_sk
              WHERE d.post_sk IS NULL) AS orphan_facts,
            (SELECT COUNT(*)
               FROM odl.dim_comments c
               LEFT JOIN odl.dim_posts p ON c.post_sk = p.post_sk
              WHERE p.post_sk IS NULL) AS orphan_comments`
  );
  if (orphans && orphans[0]) {
    out.tabs.quality.integrity = [
      ["orphan post facts", Number(orphans[0].orphan_facts || 0)],
      ["orphan comments", Number(orphans[0].orphan_comments || 0)],
    ];
  }

  return out;
}

const SECTIONS = { models, data };

/* ---------- handler ---------- */

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  const name = ((event.queryStringParameters || {}).section || "models").toLowerCase();
  const section = SECTIONS[name];
  if (!section) {
    return {
      statusCode: 400,
      headers: HEADERS,
      body: JSON.stringify({ error: "Unknown section. Use models or data." }),
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
    const left = DEADLINE_MS - (Date.now() - started);
    if (left < 1500) {
      failed.push({ query: label, message: "skipped, the section ran out of time" });
      return null;
    }
    try {
      const res = await client.query({ text: sql, query_timeout: Math.min(STATEMENT_MS, left) });
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
