#!/usr/bin/env python3
"""
ArtemisAI - Data Explorer snapshot generator
============================================
Same principle as the QA dashboard: a nightly job queries Redshift and writes a
static explorer_data.json into the repo. The Data Explorer page reads that file
and stays static (no live DB per click), refreshed nightly or on demand via the
Fetch-live button.

For each whitelisted table it bakes:
  - schema (column name + type)
  - total row count
  - a preview: the most recent PREVIEW_ROWS rows
  - summaries: for categorical columns, top values; for numeric columns,
    min/max/avg + a small histogram; for date columns, the min/max span.

ENV: REDSHIFT_HOST/PORT/DB/USER/PASSWORD  (DB var accepts REDSHIFT_DB or REDSHIFT_DATABASE)
Deps: pip install redshift_connector  (psycopg2 fallback)
"""
import os, json, datetime as dt, decimal

OUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'explorer_data.json')
PREVIEW_ROWS = 200          # rows baked per table for the preview grid
SUMMARY_TOPN = 12           # top values per categorical column
MAX_SUMMARY_COLS = 8        # cap summarised columns per table to keep JSON sane

# Same 25 tables the Netlify function whitelists.
TABLES = [
  ("odl","comment_sentiments","Comment Sentiments"),
  ("odl","comment_sentiments_v2","Comment Sentiments v2"),
  ("odl","dim_comments","Comments (Dimension)"),
  ("odl","dim_date","Date (Dimension)"),
  ("odl","dim_geographies","Geographies (Dimension)"),
  ("odl","dim_metrics","Metrics (Dimension)"),
  ("odl","dim_page_categories","Page Categories (Dimension)"),
  ("odl","dim_pages","Pages (Dimension)"),
  ("odl","dim_posts","Posts (Dimension)"),
  ("odl","dim_reaction_types","Reaction Types (Dimension)"),
  ("odl","fact_page_daily_demographics_insights","Page Demographics (Daily)"),
  ("odl","fact_page_daily_insights","Page Insights (Daily)"),
  ("odl","fact_post_daily_insights","Post Insights (Daily)"),
  ("odl","gpt_model_prediction","GPT Model Predictions"),
  ("odl","gpt_post_recommendation","GPT Post Recommendations"),
  ("odl","sentiments_overall","Overall Sentiments"),
  ("public","artemis_fb_connections","FB Connections"),
  ("public","ml_comment_sentiment_results","ML Comment Sentiments"),
  ("rdl","page_daily_insights","Page Daily Insights (RDL)"),
  ("rdl","page_demographics_insights","Page Demographics (RDL)"),
  ("rdl","page_info","Page Info (RDL)"),
  ("rdl","page_posts","Page Posts (RDL)"),
  ("rdl","post_comments","Post Comments (RDL)"),
  ("rdl","post_daily_insights","Post Daily Insights (RDL)"),
  ("rdl","post_reactions","Post Reactions (RDL)"),
]

def connect():
    h=os.environ['REDSHIFT_HOST']; p=int(os.environ.get('REDSHIFT_PORT','5439'))
    d=os.environ.get('REDSHIFT_DB') or os.environ['REDSHIFT_DATABASE']
    u=os.environ['REDSHIFT_USER']; w=os.environ['REDSHIFT_PASSWORD']
    try:
        import redshift_connector
        c=redshift_connector.connect(host=h,port=p,database=d,user=u,password=w)
    except ImportError:
        import psycopg2
        c=psycopg2.connect(host=h,port=p,dbname=d,user=u,password=w)
    try: c.autocommit=True
    except Exception: pass
    return c

CONN=None
def q(cur,sql,args=None):
    cur.execute(sql,args or [])
    cols=[c[0] for c in cur.description]
    return cols,[list(r) for r in cur.fetchall()]

def rollback():
    try:
        if CONN is not None: CONN.rollback()
    except Exception: pass

def jsonable(v):
    if isinstance(v,(dt.date,dt.datetime)): return v.isoformat()
    if isinstance(v,decimal.Decimal): return float(v)
    if isinstance(v,(bytes,bytearray)): return v.decode('utf-8','replace')
    return v

# date/timestamp column to order previews by "most recent"; else first column
DATE_HINTS=['inserted_at','created_at','processed_at','load_date','run_date','processing_date','date','connected_at','created_time']

def schema_of(cur,schema,table):
    _,rows=q(cur,"""SELECT column_name,data_type FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",[schema,table])
    return [{'name':r[0],'type':r[1]} for r in rows]

def order_col(cols):
    names=[c['name'] for c in cols]
    for h in DATE_HINTS:
        if h in names: return h
    return names[0] if names else None

def is_num(t): return any(k in t for k in ['int','numeric','double','real','decimal','float','bigint','smallint'])
def is_cat(t): return any(k in t for k in ['char','text','varchar','boolean','bool'])
def is_date(t): return any(k in t for k in ['date','time'])

def summarize(cur,schema,table,cols,total):
    out=[]
    picked=0
    for c in cols:
        if picked>=MAX_SUMMARY_COLS: break
        name,typ=c['name'],c['type']
        full=f"{schema}.{table}"
        try:
            if is_cat(typ):
                _,rows=q(cur,f'SELECT "{name}" v,COUNT(*) n FROM {full} WHERE "{name}" IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT {SUMMARY_TOPN}')
                if rows:
                    out.append({'col':name,'kind':'cat','top':[[str(jsonable(r[0]))[:40],int(r[1])] for r in rows]}); picked+=1
            elif is_num(typ):
                _,mm=q(cur,f'SELECT MIN("{name}"),MAX("{name}"),AVG("{name}") FROM {full} WHERE "{name}" IS NOT NULL')
                lo,hi,av=mm[0]
                if lo is None: continue
                lo,hi,av=float(lo),float(hi),float(av or 0)
                buckets=[]
                if hi>lo:
                    step=(hi-lo)/6.0
                    _,hs=q(cur,f'SELECT LEAST(5,FLOOR(("{name}"-{lo})/{step}))::int b,COUNT(*) n FROM {full} WHERE "{name}" IS NOT NULL GROUP BY 1 ORDER BY 1')
                    bm={int(r[0]):int(r[1]) for r in hs}
                    buckets=[[f"{lo+i*step:.3g}",bm.get(i,0)] for i in range(6)]
                out.append({'col':name,'kind':'num','min':round(lo,3),'max':round(hi,3),'avg':round(av,3),'buckets':buckets}); picked+=1
            elif is_date(typ):
                _,mm=q(cur,f'SELECT MIN("{name}"),MAX("{name}") FROM {full} WHERE "{name}" IS NOT NULL')
                if mm[0][0] is not None:
                    out.append({'col':name,'kind':'date','min':str(jsonable(mm[0][0])),'max':str(jsonable(mm[0][1]))}); picked+=1
        except Exception as e:
            print(f"  ! summary {full}.{name}: {str(e)[:60]}"); rollback()
    return out

def bake_table(cur,schema,table,label):
    full=f"{schema}.{table}"
    entry={'schema':schema,'table':table,'label':label}
    cols=schema_of(cur,schema,table)
    entry['columns']=cols
    # count
    try:
        _,c=q(cur,f"SELECT COUNT(*) FROM {full}"); entry['rows']=int(c[0][0])
    except Exception as e:
        print(f"  ! count {full}: {str(e)[:60]}"); rollback(); entry['rows']=None
    # preview
    oc=order_col(cols)
    order=f'ORDER BY "{oc}" DESC' if oc else ''
    try:
        pc,pr=q(cur,f"SELECT * FROM {full} {order} LIMIT {PREVIEW_ROWS}")
        entry['preview_cols']=pc
        entry['preview']=[[jsonable(v) for v in row] for row in pr]
    except Exception as e:
        print(f"  ! preview {full}: {str(e)[:60]}"); rollback()
        entry['preview_cols']=[c['name'] for c in cols]; entry['preview']=[]
    # summaries
    entry['summaries']=summarize(cur,schema,table,cols,entry.get('rows') or 0)
    return entry

def main():
    global CONN
    print("Data Explorer snapshot - connecting...")
    conn=connect(); CONN=conn; cur=conn.cursor()
    tables=[]
    for schema,table,label in TABLES:
        print(f"  baking {schema}.{table} ...")
        try:
            tables.append(bake_table(cur,schema,table,label))
        except Exception as e:
            print(f"  ! {schema}.{table} failed: {str(e)[:80]}"); rollback()
            tables.append({'schema':schema,'table':table,'label':label,'columns':[],'rows':None,'preview_cols':[],'preview':[],'summaries':[]})
    payload={'generated_at':dt.datetime.utcnow().isoformat()+'Z',
             'preview_rows':PREVIEW_ROWS,'tables':tables}
    with open(OUT_PATH,'w') as f: json.dump(payload,f,separators=(',',':'),default=str)
    ok=sum(1 for t in tables if t.get('preview'))
    print(f"Wrote {OUT_PATH}: {len(tables)} tables ({ok} with preview data) at {payload['generated_at']}")
    cur.close(); conn.close()

if __name__=='__main__':
    main()
