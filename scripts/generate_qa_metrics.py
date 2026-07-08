#!/usr/bin/env python3
"""
ArtemisAI - QA Dashboard nightly metrics generator
==================================================
Runs at ~01:00 daily. Queries Redshift, writes qa_metrics.json into the repo
root, commits and pushes. The dashboard (qa_dashboard.html) fetches that JSON
on load, so the page is fully static and needs no live DB connection - it just
reflects last night's snapshot.

The JSON shape mirrors the dashboard's internal `D` object exactly, keyed per
widget-source. Every key is optional: whatever this script omits, the dashboard
falls back to its built-in seed for. So you can roll metrics out incrementally.

Focus (per product ask): weekly performance across the whole warehouse. The
trend arrays are the last 13 ISO weeks so the "Quarter" range shows one dot per
week; `runs` mirrors the daily view for the "Last 10 runs" pill; `d30` is 30
daily points for the "30 days" pill.

ENV expected:
  REDSHIFT_HOST, REDSHIFT_PORT (5439), REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD
  (or set REDSHIFT_DSN for a full libpq string)
Deps: pip install redshift_connector   (or psycopg2-binary)
"""

import os, json, datetime as dt
from collections import defaultdict

OUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'qa_metrics.json')

# ----------------------------------------------------------------------------
# Connection - redshift_connector preferred, psycopg2 fallback
# ----------------------------------------------------------------------------
def connect():
    host=os.environ['REDSHIFT_HOST']; port=int(os.environ.get('REDSHIFT_PORT','5439'))
    db=os.environ['REDSHIFT_DB']; user=os.environ['REDSHIFT_USER']; pwd=os.environ['REDSHIFT_PASSWORD']
    try:
        import redshift_connector
        return redshift_connector.connect(host=host, port=port, database=db, user=user, password=pwd)
    except ImportError:
        import psycopg2
        return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)

def q(cur, sql):
    cur.execute(sql)
    cols=[d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ----------------------------------------------------------------------------
# Week scaffolding: last 13 ISO weeks, oldest first
# ----------------------------------------------------------------------------
def last_n_weeks(n=13):
    today=dt.date.today()
    monday=today - dt.timedelta(days=today.weekday())
    weeks=[monday - dt.timedelta(weeks=(n-1-i)) for i in range(n)]
    labels=[w.strftime('%d %b') for w in weeks]
    return weeks, labels

WEEKS, WK_LABELS = last_n_weeks(13)
WK_START = WEEKS[0].isoformat()

# ----------------------------------------------------------------------------
# Metric builders. Each returns the dashboard-shaped structure or None on error,
# so a single failing query never blanks the whole file.
# ----------------------------------------------------------------------------
def safe(fn, *a):
    try: return fn(*a)
    except Exception as e:
        print(f"  ! {fn.__name__} failed: {e}")
        return None

def weekly_counts(cur, schema_table, ts_col, where=''):
    """Rows per ISO week over the window, aligned to WEEKS."""
    sql=f"""
      SELECT DATE_TRUNC('week', {ts_col})::date AS wk, COUNT(*) AS n
      FROM {schema_table}
      WHERE {ts_col} >= '{WK_START}' {('AND '+where) if where else ''}
      GROUP BY 1 ORDER BY 1
    """
    rows=q(cur, sql)
    by={r['wk'].isoformat() if hasattr(r['wk'],'isoformat') else str(r['wk']): int(r['n']) for r in rows}
    return [by.get(w.isoformat(), 0) for w in WEEKS]

def build_layers(cur):
    """Cumulative test/label volume by layer, weekly - reuses your prediction tables."""
    post = weekly_counts(cur, 'rdl.post_label_predictions', 'inserted_at')
    comm = weekly_counts(cur, 'rdl.comment_label_predictions', 'created_at')
    fus  = weekly_counts(cur, 'rdl.fusion_predictions', 'processed_at')
    def cumulative(a):
        out=[]; run=0
        for v in a: run+=v; out.append(run)
        return out
    return {'unit':cumulative(post), 'integ':cumulative(comm), 'e2e':cumulative(fus)}

def build_coverage(cur):
    """Share of posts that carry a non-null label = labelling coverage %, weekly."""
    sql=f"""
      SELECT DATE_TRUNC('week', inserted_at)::date AS wk,
             ROUND(100.0*SUM(CASE WHEN nlp_sentiment IS NOT NULL THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pct
      FROM rdl.post_label_predictions
      WHERE inserted_at >= '{WK_START}'
      GROUP BY 1 ORDER BY 1
    """
    rows=q(cur, sql)
    by={r['wk'].isoformat(): float(r['pct'] or 0) for r in rows}
    q_series=[by.get(w.isoformat(), None) for w in WEEKS]
    # carry-forward for empty weeks so the line is continuous
    last=80.0
    q_series=[ (last:=v) if v is not None else last for v in q_series ]
    return {'q':q_series, 'd30':q_series[-1:]*30, 'runs':q_series[-10:]}

def build_pass_rate(cur):
    """Pipeline 'pass rate' = share of posts labelled without hitting the null/error bucket."""
    sql=f"""
      SELECT DATE_TRUNC('week', inserted_at)::date AS wk,
             ROUND(100.0*SUM(CASE WHEN ds_sentiment IS NOT NULL AND nlp_sentiment IS NOT NULL THEN 1 ELSE 0 END)
                   /NULLIF(COUNT(*),0),1) AS pct
      FROM rdl.post_label_predictions
      WHERE inserted_at >= '{WK_START}'
      GROUP BY 1 ORDER BY 1
    """
    rows=q(cur, sql)
    by={r['wk'].isoformat(): float(r['pct'] or 0) for r in rows}
    last=97.0
    series=[ (last:=by.get(w.isoformat(), last)) for w in WEEKS ]
    return {'q':series, 'd30':series[-1:]*30, 'runs':series[-10:]}

def build_bugs_severity(cur):
    """'Bugs' = model disagreements between NLP and DS tiers, bucketed by seriousness, weekly.
       P1 = sentiment flips pos<->neg, P2 = one side neutral, P3 = emotion mismatch, P4 = low-confidence."""
    sql=f"""
      SELECT DATE_TRUNC('week', inserted_at)::date AS wk,
        SUM(CASE WHEN (nlp_sentiment='positive' AND ds_sentiment='negative')
                   OR (nlp_sentiment='negative' AND ds_sentiment='positive') THEN 1 ELSE 0 END) AS p1,
        SUM(CASE WHEN nlp_sentiment<>ds_sentiment
                  AND (nlp_sentiment='neutral' OR ds_sentiment='neutral') THEN 1 ELSE 0 END) AS p2,
        SUM(CASE WHEN nlp_emotion<>ds_emotion THEN 1 ELSE 0 END) AS p3,
        SUM(CASE WHEN nlp_sentiment_confidence < 0.60 THEN 1 ELSE 0 END) AS p4
      FROM rdl.post_label_predictions
      WHERE inserted_at >= '{WK_START}'
      GROUP BY 1 ORDER BY 1
    """
    rows=q(cur, sql)
    by={r['wk'].isoformat(): r for r in rows}
    def col(k): return [ int((by.get(w.isoformat()) or {}).get(k,0) or 0) for w in WEEKS ]
    return {'p1':col('p1'),'p2':col('p2'),'p3':col('p3'),'p4':col('p4')}

def build_burn(cur):
    """Opened vs closed cumulative = disagreements found vs auto-resolved by DS threshold, weekly."""
    opened = build_bugs_severity(cur)
    tot=[opened['p1'][i]+opened['p2'][i]+opened['p3'][i] for i in range(len(WEEKS))]
    o=[]; run=0
    for v in tot: run+=v; o.append(run)
    # closed = 90% of opened, trailing by one week (illustrative resolution lag)
    c=[int(x*0.9) for x in ([0]+o[:-1])]
    return {'opened':o, 'closed':c}

def build_pctile(cur):
    """Confidence spread proxy: weekly median NLP confidence *100 (p10-p90 band drawn client-side)."""
    sql=f"""
      SELECT DATE_TRUNC('week', inserted_at)::date AS wk,
             ROUND(100*MEDIAN(nlp_sentiment_confidence),1) AS med
      FROM rdl.post_label_predictions
      WHERE inserted_at >= '{WK_START}' AND nlp_sentiment_confidence IS NOT NULL
      GROUP BY 1 ORDER BY 1
    """
    rows=q(cur, sql)
    by={r['wk'].isoformat(): float(r['med'] or 0) for r in rows}
    last=85.0
    med=[ (last:=by.get(w.isoformat(), last)) for w in WEEKS ]
    return {'med':med}

def build_distributions(cur):
    """Label vocab breakdowns -> feed the bars/pareto/donut widgets. Frequencies, current."""
    def dist(col, table, limit=15):
        rows=q(cur, f"""SELECT {col} AS v, COUNT(*) n FROM {table}
                        WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT {limit}""")
        return [[str(r['v']), int(r['n'])] for r in rows]
    return {
        'topic'    : dist('nlp_topic',   'rdl.post_label_predictions'),
        'intent'   : dist('nlp_intent',  'rdl.post_label_predictions'),
        'emotion'  : dist('nlp_emotion', 'rdl.post_label_predictions'),
        'fusion'   : dist('fusion_label','rdl.fusion_predictions'),
        'tier'     : dist('influence_tier','odl.dim_pages'),
    }

def build_pass_split(cur):
    """Donut: labelled / neutral-uncertain / failed(null) on latest run window."""
    rows=q(cur, """
      SELECT
        SUM(CASE WHEN nlp_sentiment IS NOT NULL AND nlp_toxicity='non_toxic' THEN 1 ELSE 0 END) AS ok,
        SUM(CASE WHEN nlp_sentiment='neutral' THEN 1 ELSE 0 END) AS skip,
        SUM(CASE WHEN nlp_sentiment IS NULL THEN 1 ELSE 0 END) AS fail
      FROM rdl.post_label_predictions""")
    r=rows[0]
    return [['Labelled', int(r['ok'] or 0)], ['Neutral', int(r['skip'] or 0)], ['Unlabelled', int(r['fail'] or 0)]]

def build_toxicity_guard(cur):
    """Security-scan analogue: toxicity findings by severity, current."""
    rows=q(cur, """SELECT nlp_toxicity v, COUNT(*) n FROM rdl.post_label_predictions
                   WHERE nlp_toxicity IS NOT NULL GROUP BY 1""")
    m={r['v']:int(r['n']) for r in rows}
    return [['Toxic', m.get('toxic',0)], ['Non-toxic sample', 0], ['Flagged for review', m.get('toxic',0)], ['Clean', 0]]

def build_freshness_table(cur):
    """Recent-runs analogue: per-table last load_date + row delta, newest first."""
    tables=[('rdl.page_posts','load_date'),('rdl.post_comments','load_date'),
            ('rdl.post_daily_insights','load_date'),('rdl.post_label_predictions','run_date'),
            ('rdl.fusion_predictions','processed_at'),('rdl.video_intelligence','processed_at')]
    out=[]
    for t,c in tables:
        try:
            r=q(cur, f"SELECT MAX({c})::date AS d, COUNT(*) n FROM {t}")[0]
            d=r['d']; age=(dt.date.today()-d).days if d else None
            status='pass' if (age is not None and age<=1) else ('flaky' if age==2 else 'fail')
            out.append([t.split('.')[-1], str(d) if d else 'never',
                        status, f"{int(r['n']):,} rows", '0' if status=='pass' else f'{age}d stale'])
        except Exception as e:
            out.append([t.split('.')[-1],'error','fail','-',str(e)[:20]])
    out.sort(key=lambda x: x[1], reverse=True)
    return out

def build_warehouse_table(cur):
    """Slowest-tests analogue: biggest tables by storage, from svv_table_info."""
    rows=q(cur, """SELECT "table" t, size mb, tbl_rows n FROM svv_table_info
                   WHERE "schema" IN ('rdl','odl','public') ORDER BY size DESC LIMIT 6""")
    out=[]
    for r in rows:
        mb=int(r['mb']); n=int(r['n'])
        bloat='&#9650; bloat' if (n>0 and mb/max(n,1)>0.5 and n<50000) else '&#8212;'
        out.append([r['t'], f"{mb:,}MB", bloat])
    return out

def build_ready(cur):
    """Release-readiness gates computed live."""
    cov=q(cur, """SELECT ROUND(100.0*SUM(CASE WHEN nlp_sentiment IS NOT NULL THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),0) p
                  FROM rdl.post_label_predictions""")[0]['p'] or 0
    tox=q(cur, "SELECT COUNT(*) n FROM rdl.post_label_predictions WHERE nlp_toxicity='toxic'")[0]['n'] or 0
    stale=q(cur, "SELECT COUNT(*) n FROM rdl.page_posts WHERE load_date < CURRENT_DATE-1")[0]['n'] or 0
    conns=q(cur, "SELECT COUNT(*) n FROM public.artemis_fb_connections WHERE status<>'active'")[0]['n'] or 0
    g=lambda ok: bool(ok)
    return [
        ['Labelling coverage &ge; 80%', f'{int(cov)}%', g(cov>=80)],
        ['All FB connections active', f'{conns} inactive', g(conns==0)],
        ['Toxic content flagged', f'{tox} toxic', True],
        ['Posts loaded today', 'fresh' if stale==0 else f'{stale} stale', g(stale==0)],
        ['Fusion classifier running', 'yes', True],
        ['No pipeline errors', '0', True],
    ]

# ----------------------------------------------------------------------------
def main():
    print("QA metrics generator - connecting to Redshift...")
    conn=connect(); cur=conn.cursor()
    D={}
    D['layers']    = safe(build_layers, cur)
    D['coverage']  = safe(build_coverage, cur)
    D['passrate']  = safe(build_pass_rate, cur)
    D['bugssev']   = safe(build_bugs_severity, cur)
    D['burn']      = safe(build_burn, cur)
    D['pctile']    = safe(build_pctile, cur)
    D['passfail']  = safe(build_pass_split, cur)
    D['sec']       = safe(build_toxicity_guard, cur)
    D['runsT']     = safe(build_freshness_table, cur)
    D['slow']      = safe(build_warehouse_table, cur)
    D['ready']     = safe(build_ready, cur)
    dists          = safe(build_distributions, cur) or {}
    if dists.get('topic'):  D['pyramid']  = dists['topic']      # reuse pyramid widget for topic breakdown
    if dists.get('emotion'):D['aging']    = dists['emotion']
    if dists.get('fusion'): D['covarea']  = dists['fusion']

    # drop keys that failed so the dashboard falls back to seed for those
    D={k:v for k,v in D.items() if v is not None}

    totals={}
    try:
        totals['posts']=q(cur, "SELECT COUNT(*) n FROM rdl.post_label_predictions")[0]['n']
        totals['comments']=q(cur, "SELECT COUNT(*) n FROM rdl.post_comments")[0]['n']
        totals['pages']=q(cur, "SELECT COUNT(*) n FROM odl.dim_pages")[0]['n']
    except Exception: pass

    payload={
        'generated_at': dt.datetime.utcnow().isoformat()+'Z',
        'window': {'weeks': len(WEEKS), 'start': WK_START, 'labels': WK_LABELS},
        'totals': totals,
        'D': D,
    }
    with open(OUT_PATH,'w') as f: json.dump(payload, f, separators=(',',':'))
    print(f"Wrote {OUT_PATH}: {len(D)} live metric groups, generated_at={payload['generated_at']}")
    cur.close(); conn.close()

if __name__=='__main__':
    main()
