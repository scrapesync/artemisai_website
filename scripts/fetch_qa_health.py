#!/usr/bin/env python3
"""
ArtemisAI QA Health refresher.

Runs from GitHub Actions once daily (9 AM PKT). Connects to Redshift,
runs the data-freshness query against the rdl schema, evaluates
user-defined QA rules from qa_alerts.json, appends today's snapshot
to data/qa_history.json, and emails the team if anything is stale or
fired a rule.

Inputs (env / files):
  REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD,
  REDSHIFT_PORT (default 5439)              — DB connection
  SMTP_USER, SMTP_PASSWORD                  — Gmail app password
  qa_alerts.json                            — rule definitions
  team_emails.json                          — recipients

Outputs:
  data/qa_history.json                      — append today's snapshot
  email to team if anything fired

Rule types:
  stale_table       — table > N days since last load
  row_count_drop    — today's row count < (X% of 7-day avg)
  no_load_today     — any table missing today's load
"""

import os
import json
import sys
import datetime
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parent.parent
HISTORY_FILE = ROOT / 'data' / 'qa_history.json'
RULES_FILE = ROOT / 'qa_alerts.json'
TEAM_FILE = ROOT / 'team_emails.json'

# ─── Tables tracked ───────────────────────────────────────────────────
# Keep this list in sync with the SQL in run_freshness_query().
TRACKED_TABLES = [
    'page_posts',
    'page_info',
    'page_daily_insights',
    'post_daily_insights',
    'post_label_predictions',
]

# How many historical snapshots to keep in qa_history.json.
# 90 days = ~3 months of history visible in the dashboard.
MAX_HISTORY_DAYS = 90


# ─── Redshift query ───────────────────────────────────────────────────

FRESHNESS_SQL = """
SELECT
  'page_posts' AS table_name,
  COUNT(*) AS total_rows,
  MAX(load_date) AS latest_load,
  MIN(load_date) AS earliest_load,
  DATEDIFF(day, MAX(load_date), SYSDATE) AS days_since_last_load
FROM rdl.page_posts
UNION ALL
SELECT
  'page_info' AS table_name,
  COUNT(*) AS total_rows,
  MAX(load_date) AS latest_load,
  MIN(load_date) AS earliest_load,
  DATEDIFF(day, MAX(load_date), SYSDATE) AS days_since_last_load
FROM rdl.page_info
UNION ALL
SELECT
  'page_daily_insights' AS table_name,
  COUNT(*) AS total_rows,
  MAX(load_date) AS latest_load,
  MIN(load_date) AS earliest_load,
  DATEDIFF(day, MAX(load_date), SYSDATE) AS days_since_last_load
FROM rdl.page_daily_insights
UNION ALL
SELECT
  'post_daily_insights' AS table_name,
  COUNT(*) AS total_rows,
  MAX(load_date) AS latest_load,
  MIN(load_date) AS earliest_load,
  DATEDIFF(day, MAX(load_date), SYSDATE) AS days_since_last_load
FROM rdl.post_daily_insights
UNION ALL
SELECT
  'post_label_predictions' AS table_name,
  COUNT(*) AS total_rows,
  MAX(inserted_at::date) AS latest_load,
  MIN(inserted_at::date) AS earliest_load,
  DATEDIFF(day, MAX(inserted_at)::timestamp, SYSDATE) AS days_since_last_load
FROM rdl.post_label_predictions
ORDER BY days_since_last_load DESC;
"""


def _get_conn():
    """Open a Redshift connection. Caller is responsible for closing."""
    return psycopg2.connect(
        host=os.environ['REDSHIFT_HOST'],
        port=int(os.environ.get('REDSHIFT_PORT', 5439)),
        dbname=os.environ['REDSHIFT_DB'],
        user=os.environ['REDSHIFT_USER'],
        password=os.environ['REDSHIFT_PASSWORD'],
        connect_timeout=30,
    )


def _query(conn, sql):
    """Run a SQL query, return list of dicts keyed by column name."""
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def _json_safe(val):
    """Convert dates/decimals/etc to JSON-safe primitives."""
    if val is None:
        return None
    if hasattr(val, 'isoformat'):
        return val.isoformat()
    # Decimal → float
    try:
        from decimal import Decimal
        if isinstance(val, Decimal):
            return float(val)
    except ImportError:
        pass
    return val


def run_freshness_query(conn):
    """Run the freshness query, return list of dicts."""
    rows = _query(conn, FRESHNESS_SQL)
    out = []
    for d in rows:
        for k in ('latest_load', 'earliest_load'):
            d[k] = _json_safe(d[k])
        d['total_rows'] = int(d['total_rows']) if d['total_rows'] is not None else 0
        d['days_since_last_load'] = (
            int(d['days_since_last_load']) if d['days_since_last_load'] is not None else None
        )
        out.append(d)
    return out


# ─── Analysis queries ─────────────────────────────────────────────────
# All queries scope to the last 30 days of data unless noted.
# Column names match the actual rdl schema:
#   post_label_predictions:  nlp_sentiment, nlp_emotion, nlp_topic, nlp_toxicity,
#                            ds_sentiment, nlp_*_confidence, ds_*_confidence,
#                            cl_triggered, processing_time_ms, inserted_at
#   post_daily_insights:     post_impressions, post_reactions_*_total, page_id,
#                            page_name, created_time, message, load_date
#   page_posts:              id, page_id, page_name, load_date

ANALYSIS_QUERIES = {

    # ─── NLP Output ─────────────────────────────────────────────
    'sentiment_distribution_today': """
        SELECT
          COALESCE(nlp_sentiment, 'unknown') AS label,
          COUNT(*) AS count
        FROM rdl.post_label_predictions
        WHERE inserted_at::date = (SYSDATE - INTERVAL '1 day')::date
        GROUP BY 1
        ORDER BY 2 DESC;
    """,

    'sentiment_trend_30d': """
        SELECT
          inserted_at::date AS day,
          COALESCE(nlp_sentiment, 'unknown') AS label,
          COUNT(*) AS count
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE)
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """,

    'emotion_distribution_30d': """
        SELECT
          COALESCE(nlp_emotion, 'unknown') AS label,
          COUNT(*) AS count
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE)
          AND nlp_emotion IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC;
    """,

    'topic_top10_30d': """
        SELECT
          COALESCE(nlp_topic, 'unknown') AS label,
          COUNT(*) AS count
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE)
          AND nlp_topic IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10;
    """,

    # ─── Model QA ───────────────────────────────────────────────
    'agreement_nlp_vs_ds_30d': """
        SELECT
          inserted_at::date AS day,
          SUM(CASE WHEN nlp_sentiment = ds_sentiment THEN 1 ELSE 0 END) AS agree,
          COUNT(*) AS total
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE)
          AND nlp_sentiment IS NOT NULL
          AND ds_sentiment IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
    """,

    'avg_confidence_by_dimension': """
        SELECT
          'sentiment' AS dimension,
          AVG(nlp_sentiment_confidence) AS avg_conf
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE) AND nlp_sentiment_confidence IS NOT NULL
        UNION ALL SELECT 'emotion',  AVG(nlp_emotion_confidence)  FROM rdl.post_label_predictions
          WHERE inserted_at >= DATEADD(day, -30, SYSDATE) AND nlp_emotion_confidence IS NOT NULL
        UNION ALL SELECT 'topic',    AVG(nlp_topic_confidence)    FROM rdl.post_label_predictions
          WHERE inserted_at >= DATEADD(day, -30, SYSDATE) AND nlp_topic_confidence IS NOT NULL
        UNION ALL SELECT 'intent',   AVG(nlp_intent_confidence)   FROM rdl.post_label_predictions
          WHERE inserted_at >= DATEADD(day, -30, SYSDATE) AND nlp_intent_confidence IS NOT NULL
        UNION ALL SELECT 'toxicity', AVG(nlp_toxicity_confidence) FROM rdl.post_label_predictions
          WHERE inserted_at >= DATEADD(day, -30, SYSDATE) AND nlp_toxicity_confidence IS NOT NULL;
    """,

    'claude_trigger_rate_30d': """
        SELECT
          inserted_at::date AS day,
          SUM(CASE WHEN cl_triggered THEN 1 ELSE 0 END) AS triggered,
          COUNT(*) AS total
        FROM rdl.post_label_predictions
        WHERE inserted_at >= DATEADD(day, -30, SYSDATE)
        GROUP BY 1
        ORDER BY 1;
    """,

    # ─── Engagement ─────────────────────────────────────────────
    'impressions_reactions_30d': """
        SELECT
          load_date AS day,
          SUM(post_impressions) AS impressions,
          SUM(COALESCE(post_reactions_like_total, 0)
            + COALESCE(post_reactions_love_total, 0)
            + COALESCE(post_reactions_wow_total, 0)
            + COALESCE(post_reactions_haha_total, 0)
            + COALESCE(post_reactions_sorry_total, 0)
            + COALESCE(post_reactions_anger_total, 0)) AS reactions
        FROM rdl.post_daily_insights
        WHERE load_date >= (SYSDATE - INTERVAL '30 days')::date
        GROUP BY 1
        ORDER BY 1;
    """,

    'reaction_mix_today': """
        SELECT
          'like'   AS reaction, SUM(post_reactions_like_total)  AS count FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date
        UNION ALL SELECT 'love',  SUM(post_reactions_love_total)  FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date
        UNION ALL SELECT 'wow',   SUM(post_reactions_wow_total)   FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date
        UNION ALL SELECT 'haha',  SUM(post_reactions_haha_total)  FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date
        UNION ALL SELECT 'sorry', SUM(post_reactions_sorry_total) FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date
        UNION ALL SELECT 'anger', SUM(post_reactions_anger_total) FROM rdl.post_daily_insights
            WHERE load_date = (SYSDATE - INTERVAL '1 day')::date;
    """,

    'top_posts_week': """
        SELECT
          page_name,
          LEFT(COALESCE(message, ''), 120) AS message_snippet,
          post_impressions AS impressions,
          (COALESCE(post_reactions_like_total, 0)
            + COALESCE(post_reactions_love_total, 0)
            + COALESCE(post_reactions_wow_total, 0)
            + COALESCE(post_reactions_haha_total, 0)
            + COALESCE(post_reactions_sorry_total, 0)
            + COALESCE(post_reactions_anger_total, 0)) AS reactions
        FROM rdl.post_daily_insights
        WHERE created_time >= DATEADD(day, -7, SYSDATE)
          AND post_impressions IS NOT NULL
        ORDER BY post_impressions DESC NULLS LAST
        LIMIT 10;
    """,

    'sentiment_vs_impressions_today': """
        SELECT
          COALESCE(p.nlp_sentiment, 'unknown') AS sentiment,
          AVG(i.post_impressions) AS avg_impressions,
          COUNT(*) AS n
        FROM rdl.post_label_predictions p
        JOIN rdl.post_daily_insights i ON p.post_id = i.post_id
        WHERE p.inserted_at >= DATEADD(day, -7, SYSDATE)
          AND i.post_impressions IS NOT NULL
          AND p.nlp_sentiment IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC;
    """,
}


def run_analysis(conn):
    """Run all analysis queries. Each returns rows; we shape them so the
    front-end gets a predictable structure per chart. Failures on a single
    query don't kill the rest — the page just won't have that chart."""
    out = {}
    for name, sql in ANALYSIS_QUERIES.items():
        try:
            rows = _query(conn, sql)
            # JSON-safe conversion of every value
            cleaned = []
            for r in rows:
                cleaned.append({k: _json_safe(v) for k, v in r.items()})
            out[name] = cleaned
        except Exception as e:
            print(f'  WARNING: analysis query "{name}" failed: {e}')
            out[name] = {'error': str(e)}
    return out


# ─── Snapshot + history management ────────────────────────────────────

def load_history():
    if not HISTORY_FILE.exists():
        return {'snapshots': []}
    try:
        return json.loads(HISTORY_FILE.read_text())
    except Exception as e:
        print(f'  WARNING: failed to parse history file ({e}), starting fresh')
        return {'snapshots': []}


def append_snapshot(history, freshness_results):
    """Add today's snapshot. If a snapshot for today already exists
    (e.g. workflow re-run), overwrite it rather than duplicating."""
    today = datetime.date.today().isoformat()
    snapshot = {
        'date': today,
        'taken_at': datetime.datetime.utcnow().isoformat() + 'Z',
        'tables': freshness_results,
    }
    # Remove any existing entry for today, then append
    history['snapshots'] = [s for s in history['snapshots'] if s.get('date') != today]
    history['snapshots'].append(snapshot)
    # Sort by date and trim to MAX_HISTORY_DAYS
    history['snapshots'].sort(key=lambda s: s['date'])
    if len(history['snapshots']) > MAX_HISTORY_DAYS:
        history['snapshots'] = history['snapshots'][-MAX_HISTORY_DAYS:]
    return history


# ─── Alert rules ──────────────────────────────────────────────────────

def load_rules():
    if not RULES_FILE.exists():
        return []
    return json.loads(RULES_FILE.read_text())


def evaluate_rules(freshness, history, rules):
    """Check each enabled rule against current data + history.
    Returns list of fired alerts."""
    fired = []
    by_table = {r['table_name']: r for r in freshness}
    today = datetime.date.today()

    for rule in rules:
        if not rule.get('enabled', True):
            continue
        rt = rule.get('type')

        if rt == 'stale_table':
            # Fire if any tracked table is older than max_days
            max_days = rule.get('max_days', 2)
            scope = rule.get('tables', 'all')  # 'all' or list of table names
            targets = TRACKED_TABLES if scope == 'all' else scope
            for t in targets:
                row = by_table.get(t)
                if row is None:
                    continue
                d = row.get('days_since_last_load')
                if d is not None and d > max_days:
                    fired.append({
                        'rule_name': rule['name'],
                        'table': t,
                        'severity': 'high' if d > max_days + 2 else 'medium',
                        'message': f'{t} is {d} days behind (threshold: {max_days} days).',
                    })

        elif rt == 'no_load_today':
            # Fire if a table's days_since_last_load > 0 today
            scope = rule.get('tables', 'all')
            targets = TRACKED_TABLES if scope == 'all' else scope
            for t in targets:
                row = by_table.get(t)
                if row is None:
                    continue
                d = row.get('days_since_last_load')
                if d is not None and d > 0:
                    fired.append({
                        'rule_name': rule['name'],
                        'table': t,
                        'severity': 'medium',
                        'message': f'{t} was not loaded today (last load: {row.get("latest_load")}).',
                    })

        elif rt == 'row_count_drop':
            # Fire if today's total_rows < (drop_pct * 7-day avg)
            # Need history. Look at last 7 snapshots before today.
            drop_pct = rule.get('drop_pct', 0.5)  # default: 50%
            scope = rule.get('tables', 'all')
            targets = TRACKED_TABLES if scope == 'all' else scope

            prev = [s for s in history['snapshots'] if s['date'] < today.isoformat()][-7:]
            if len(prev) < 3:
                continue  # not enough history yet

            for t in targets:
                row = by_table.get(t)
                if row is None:
                    continue
                # Pull last 7 days of row counts for this table
                prev_counts = []
                for snap in prev:
                    for tbl in snap['tables']:
                        if tbl['table_name'] == t:
                            prev_counts.append(tbl['total_rows'])
                            break
                if not prev_counts:
                    continue
                avg = sum(prev_counts) / len(prev_counts)
                today_count = row['total_rows']
                if avg > 0 and today_count < avg * drop_pct:
                    fired.append({
                        'rule_name': rule['name'],
                        'table': t,
                        'severity': 'high',
                        'message': (f'{t} has {today_count:,} rows today vs '
                                    f'{avg:,.0f} avg over last {len(prev_counts)} days '
                                    f'(drop of {(1 - today_count/avg) * 100:.0f}%).'),
                    })

    return fired


# ─── Email ────────────────────────────────────────────────────────────

def render_email(fired, freshness):
    today = datetime.date.today().isoformat()
    rows = []
    if fired:
        rows.append('<h2 style="color:#E53935;margin:20px 0 8px">QA alerts fired</h2>')
        for f in fired:
            color = '#E53935' if f['severity'] == 'high' else '#F59E0B'
            rows.append(
                f'<div style="padding:12px;background:#fef2f2;border-left:3px solid {color};'
                f'margin-bottom:8px;border-radius:6px">'
                f'<strong>{f["rule_name"]}</strong> · <span style="color:#666">{f["table"]}</span>'
                f'<br><span style="color:#555;font-size:13px">{f["message"]}</span></div>'
            )

    # Always include a freshness summary table
    rows.append('<h3 style="margin:20px 0 8px">Today\'s freshness snapshot</h3>')
    rows.append('<table style="border-collapse:collapse;width:100%;font-size:13px">')
    rows.append('<tr style="background:#f3f4f6">'
                '<th style="text-align:left;padding:8px;border:1px solid #e5e7eb">Table</th>'
                '<th style="text-align:right;padding:8px;border:1px solid #e5e7eb">Rows</th>'
                '<th style="text-align:left;padding:8px;border:1px solid #e5e7eb">Last load</th>'
                '<th style="text-align:right;padding:8px;border:1px solid #e5e7eb">Days behind</th>'
                '</tr>')
    for row in freshness:
        days = row.get('days_since_last_load') or 0
        bg = '#fef2f2' if days > 2 else '#fffbeb' if days > 0 else '#ffffff'
        rows.append(
            f'<tr style="background:{bg}">'
            f'<td style="padding:8px;border:1px solid #e5e7eb">{row["table_name"]}</td>'
            f'<td style="padding:8px;border:1px solid #e5e7eb;text-align:right">{row["total_rows"]:,}</td>'
            f'<td style="padding:8px;border:1px solid #e5e7eb">{row.get("latest_load") or "—"}</td>'
            f'<td style="padding:8px;border:1px solid #e5e7eb;text-align:right">{days}</td>'
            f'</tr>'
        )
    rows.append('</table>')

    return f'''<!DOCTYPE html>
<html><body style="font-family:-apple-system,sans-serif;max-width:680px;margin:0 auto;padding:20px;color:#1f2937">
<div style="border-bottom:2px solid #00C4A0;padding-bottom:14px;margin-bottom:20px">
  <h1 style="margin:0;font-size:22px">ArtemisAI · QA Dashboard</h1>
  <p style="margin:4px 0 0;color:#666;font-size:13px">{today} · automated check</p>
</div>
{''.join(rows)}
<hr style="border:none;border-top:1px solid #e5e7eb;margin:24px 0">
<p style="color:#999;font-size:12px">Sent by GitHub Actions · scripts/fetch_qa_health.py · See <a href="https://artemisai.co.uk/qa_dashboard.html">qa_dashboard.html</a> for charts &amp; history.</p>
</body></html>'''


def send_email(recipients, subject, body):
    smtp_user = os.environ.get('SMTP_USER')
    smtp_password = os.environ.get('SMTP_PASSWORD')
    if not smtp_user or not smtp_password:
        print('  SMTP_USER / SMTP_PASSWORD not set, skipping email')
        return False
    if not recipients:
        print('  no recipients, skipping email')
        return False

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ArtemisAI QA Bot <{smtp_user}>'
    msg['To'] = ', '.join(recipients)
    msg.attach(MIMEText(body, 'html'))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
    print(f'  sent email to {len(recipients)} recipients')
    return True


def load_json(path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    print(f'[{datetime.datetime.now().isoformat()}] starting QA refresh')

    # Open ONE Redshift connection, reuse for freshness + analysis
    conn = _get_conn()
    try:
        # 1. Pull freshness data
        freshness = run_freshness_query(conn)
        print(f'  fetched freshness for {len(freshness)} tables')

        # 2. Run analysis queries (heavier — ~10-30s total on Redshift)
        analysis = run_analysis(conn)
        ok = sum(1 for v in analysis.values() if not (isinstance(v, dict) and 'error' in v))
        print(f'  ran {len(analysis)} analysis queries, {ok} succeeded')
    finally:
        conn.close()

    # 3. Load history, append today's snapshot
    history = load_history()
    history = append_snapshot(history, freshness)
    print(f'  history now has {len(history["snapshots"])} snapshots')

    # 4. Evaluate rules
    rules = load_rules()
    fired = evaluate_rules(freshness, history, rules)
    print(f'  evaluated {len(rules)} rules, {len(fired)} fired')

    # 5. Write the data file the page reads
    output = {
        'last_updated': datetime.datetime.utcnow().isoformat() + 'Z',
        'today': freshness,
        'history': history['snapshots'],
        'rules': rules,
        'fired': fired,
        'analysis': analysis,
    }
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(output, indent=2, default=str))
    print(f'  wrote {HISTORY_FILE}')

    # 6. Email if anything fired
    if fired:
        recipients = load_json(TEAM_FILE, {'emails': []}).get('emails', [])
        subject = f'[ArtemisAI QA] {len(fired)} alert{"s" if len(fired) > 1 else ""} — {datetime.date.today().isoformat()}'
        body = render_email(fired, freshness)
        send_email(recipients, subject, body)
    else:
        print('  no alerts fired, no email sent')

    print('done')


if __name__ == '__main__':
    main()
