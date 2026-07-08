#!/usr/bin/env python3
"""
ArtemisAI - QA Dashboard nightly metrics generator (v2, four-tab)
================================================================
Runs ~01:00 daily. Queries Redshift across four domains and writes
qa_metrics.json into the repo root. The dashboard reads it and stays static.

Tabs:
  accuracy  - NLP vs DeepSeek agreement + confidence (no human ground truth,
              so 'accuracy' = inter-model agreement and confidence health)
  health    - did loads arrive on their date, did anything fail/lag (QA/ops)
  quality   - how good is the data: completeness, dupes, integrity, validity
  analytics - everything else: content, sentiment, reactions, pages

Trend metrics are emitted at day/week/month/quarter grains plus a raw daily
series (last WINDOW_DAYS) so the client can slice any custom date range.
Every key is optional; missing keys fall back to the dashboard's seed.

ENV: REDSHIFT_HOST/PORT/DB/USER/PASSWORD
Deps: pip install redshift_connector   (psycopg2 fallback)
"""
import os, json, datetime as dt

OUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'qa_metrics.json')
WINDOW_DAYS = 120

def connect():
    h=os.environ['REDSHIFT_HOST']; p=int(os.environ.get('REDSHIFT_PORT','5439'))
    d=os.environ['REDSHIFT_DB']; u=os.environ['REDSHIFT_USER']; w=os.environ['REDSHIFT_PASSWORD']
    try:
        import redshift_connector
        return redshift_connector.connect(host=h,port=p,database=d,user=u,password=w)
    except ImportError:
        import psycopg2
        return psycopg2.connect(host=h,port=p,dbname=d,user=u,password=w)

def q(cur, sql):
    cur.execute(sql); cols=[c[0] for c in cur.description]
    return [dict(zip(cols,r)) for r in cur.fetchall()]

def safe(fn,*a):
    try: return fn(*a)
    except Exception as e:
        print(f"  ! {fn.__name__}: {e}"); return None

START = (dt.date.today()-dt.timedelta(days=WINDOW_DAYS)).isoformat()
DAYS  = [(dt.date.today()-dt.timedelta(days=WINDOW_DAYS-1-i)) for i in range(WINDOW_DAYS)]

def align_daily(rows, keycol, valcol, default=None, carry=False):
    by={}
    for r in rows:
        k=r[keycol]; k=k.isoformat() if hasattr(k,'isoformat') else str(k)[:10]
        by[k]=r[valcol]
    out=[]; last=default
    for d in DAYS:
        v=by.get(d.isoformat())
        if v is None: v=last if carry else default
        else:
            try: v=float(v)
            except (TypeError,ValueError): pass
            last=v
        out.append(v)
    return out

def roll(series, grain):
    if grain=='day': return [None if v is None else round(v,1) for v in series[-30:]]
    size={'week':7,'month':30,'quarter':91}[grain]
    out=[]; buf=[]
    for i,v in enumerate(series):
        buf.append(v if v is not None else 0)
        if len(buf)==size or i==len(series)-1:
            out.append(round(sum(buf)/len(buf),1)); buf=[]
    return out

def trend(series):
    return {'day':roll(series,'day'),'week':roll(series,'week'),
            'month':roll(series,'month'),'quarter':roll(series,'quarter'),
            'daily':[None if v is None else round(v,2) for v in series]}

# ---- ACCURACY ----
def acc_agreement(cur):
    rows=q(cur,f"""SELECT inserted_at::date d,
        ROUND(100.0*AVG(CASE WHEN nlp_sentiment=ds_sentiment THEN 1 ELSE 0 END),1) agree
      FROM rdl.post_label_predictions
      WHERE inserted_at>='{START}' AND ds_sentiment IS NOT NULL AND nlp_sentiment IS NOT NULL
      GROUP BY 1""")
    return trend(align_daily(rows,'d','agree',90,True))
def acc_confidence(cur,tier='nlp'):
    rows=q(cur,f"""SELECT inserted_at::date d, ROUND(100*AVG({tier}_sentiment_confidence),1) c
      FROM rdl.post_label_predictions
      WHERE inserted_at>='{START}' AND {tier}_sentiment_confidence IS NOT NULL GROUP BY 1""")
    return trend(align_daily(rows,'d','c',85,True))
def acc_confusion(cur):
    rows=q(cur,"""SELECT nlp_sentiment n, ds_sentiment d, COUNT(*) c
      FROM rdl.post_label_predictions
      WHERE nlp_sentiment IS NOT NULL AND ds_sentiment IS NOT NULL GROUP BY 1,2""")
    cats=['positive','neutral','negative']; m={(r['n'],r['d']):int(r['c']) for r in rows}
    return {'rows':cats,'cols':cats,'v':[[m.get((rr,cc),0) for cc in cats] for rr in cats]}
def acc_conf_buckets(cur):
    rows=q(cur,"""SELECT bucket,COUNT(*) c FROM (
        SELECT CASE WHEN nlp_sentiment_confidence<0.5 THEN '<50%'
                    WHEN nlp_sentiment_confidence<0.7 THEN '50-70%'
                    WHEN nlp_sentiment_confidence<0.85 THEN '70-85%'
                    WHEN nlp_sentiment_confidence<0.95 THEN '85-95%'
                    ELSE '95-100%' END bucket
        FROM rdl.post_label_predictions WHERE nlp_sentiment_confidence IS NOT NULL) t GROUP BY 1""")
    order=['<50%','50-70%','70-85%','85-95%','95-100%']; m={r['bucket']:int(r['c']) for r in rows}
    return [[b,m.get(b,0)] for b in order]
def acc_per_dim(cur):
    dims=[('sentiment','nlp_sentiment','ds_sentiment'),('emotion','nlp_emotion','ds_emotion'),('toxicity','nlp_toxicity','ds_toxicity')]
    out=[]
    for name,a,b in dims:
        r=q(cur,f"SELECT ROUND(100.0*AVG(CASE WHEN {a}={b} THEN 1 ELSE 0 END),1) v FROM rdl.post_label_predictions WHERE {a} IS NOT NULL AND {b} IS NOT NULL")[0]
        out.append([name,float(r['v'] or 0)])
    return out
def acc_escalation(cur):
    r=q(cur,"""SELECT SUM(CASE WHEN nlp_sentiment IS NOT NULL THEN 1 ELSE 0 END) nlp,
        SUM(CASE WHEN ds_sentiment IS NOT NULL THEN 1 ELSE 0 END) ds,
        SUM(CASE WHEN cl_sentiment IS NOT NULL THEN 1 ELSE 0 END) cl
      FROM rdl.post_label_predictions""")[0]
    return [['NLP tier',int(r['nlp'] or 0)],['DeepSeek tier',int(r['ds'] or 0)],['Claude tier',int(r['cl'] or 0)]]
def acc_proctime(cur):
    rows=q(cur,f"SELECT inserted_at::date d, ROUND(AVG(processing_time_ms)) ms FROM rdl.post_label_predictions WHERE inserted_at>='{START}' AND processing_time_ms IS NOT NULL GROUP BY 1")
    return trend(align_daily(rows,'d','ms',0,True))

# ---- HEALTH ----
FRESH_TABLES=[('rdl.page_posts','load_date'),('rdl.post_comments','load_date'),
              ('rdl.post_daily_insights','load_date'),('rdl.page_daily_insights','load_date'),
              ('rdl.post_label_predictions','inserted_at'),('rdl.fusion_predictions','processed_at')]
def hl_freshness_table(cur):
    out=[]
    for t,c in FRESH_TABLES:
        try:
            r=q(cur,f"SELECT MAX({c})::date d, COUNT(*) n FROM {t}")[0]
            d=r['d']; age=(dt.date.today()-d).days if d else None
            st='pass' if (age is not None and age<=1) else ('flaky' if age==2 else 'fail')
            out.append([t.split('.')[-1],str(d) if d else 'never',st,f"{int(r['n']):,}",'on time' if st=='pass' else f'{age}d late'])
        except Exception as e:
            out.append([t.split('.')[-1],'error','fail','-',str(e)[:18]])
    out.sort(key=lambda x:x[1],reverse=True); return out
def hl_daily_loads(cur):
    posts=align_daily(q(cur,f"SELECT load_date d,COUNT(*) n FROM rdl.page_posts WHERE load_date>='{START}' GROUP BY 1"),'d','n',0)
    comm=align_daily(q(cur,f"SELECT load_date d,COUNT(*) n FROM rdl.post_comments WHERE load_date>='{START}' GROUP BY 1"),'d','n',0)
    return {'posts':trend(posts),'comments':trend(comm)}
def hl_load_status(cur):
    rows=[]
    for t,c in FRESH_TABLES:
        rows.append(align_daily(q(cur,f"SELECT {c}::date d,1 v FROM {t} WHERE {c}>='{START}' GROUP BY 1"),'d','v',0))
    landed=[sum(1 for tb in rows if tb[i]) for i in range(len(DAYS))]
    return trend([100.0*x/len(FRESH_TABLES) for x in landed])
def hl_missing_days(cur):
    daily=align_daily(q(cur,f"SELECT load_date d,COUNT(*) n FROM rdl.page_posts WHERE load_date>='{START}' GROUP BY 1"),'d','n',0)
    gaps=[[DAYS[i].strftime('%d %b'),0] for i,v in enumerate(daily) if v==0][-12:]
    return gaps if gaps else [['no gaps',0]]
def hl_lag(cur):
    rows=q(cur,f"SELECT load_date d, ROUND(AVG(DATEDIFF(day,date,load_date)),1) lag FROM rdl.post_comments WHERE load_date>='{START}' GROUP BY 1")
    return trend(align_daily(rows,'d','lag',0,True))
def hl_rowcount_delta(cur):
    daily=align_daily(q(cur,f"SELECT load_date d,COUNT(*) n FROM rdl.post_comments WHERE load_date>='{START}' GROUP BY 1"),'d','n',0)
    return trend(daily)

# ---- QUALITY ----
def ql_completeness(cur):
    cols=['nlp_sentiment','nlp_emotion','nlp_topic','nlp_intent','nlp_toxicity','ds_sentiment']
    tot=q(cur,"SELECT COUNT(*) n FROM rdl.post_label_predictions")[0]['n'] or 1; out=[]
    for c in cols:
        nn=q(cur,f"SELECT COUNT({c}) n FROM rdl.post_label_predictions")[0]['n'] or 0
        out.append([c.replace('nlp_','').replace('ds_','ds '),round(100.0*nn/tot,1)])
    return out
def ql_dupes(cur):
    checks=[('dim_comments','comment_sk','odl'),('dim_posts','post_sk','odl'),('dim_pages','page_sk','odl')]; out=[]
    for t,k,s in checks:
        r=q(cur,f"SELECT COUNT(*) tot,COUNT(DISTINCT {k}) uniq FROM {s}.{t}")[0]
        out.append([t,int(r['tot'])-int(r['uniq'])])
    return out
def ql_integrity(cur):
    out=[]
    try:
        r=q(cur,"SELECT COUNT(*) n FROM odl.fact_post_daily_insights f LEFT JOIN odl.dim_posts d ON f.post_sk=d.post_sk WHERE d.post_sk IS NULL")[0]
        out.append(['orphan post facts',int(r['n'])])
    except Exception: pass
    try:
        r=q(cur,"SELECT COUNT(*) n FROM odl.dim_comments c LEFT JOIN odl.dim_posts p ON c.post_sk=p.post_sk WHERE p.post_sk IS NULL")[0]
        out.append(['orphan comments',int(r['n'])])
    except Exception: pass
    return out or [['no orphans',0]]
def ql_dq_score(cur):
    rows=q(cur,f"SELECT processing_date d, ROUND(100*AVG(data_quality_score),1) s FROM odl.comment_sentiments WHERE processing_date>='{START}' AND data_quality_score IS NOT NULL GROUP BY 1")
    return trend(align_daily(rows,'d','s',90,True))
def ql_highconf_share(cur):
    rows=q(cur,f"SELECT processing_date d, ROUND(100.0*AVG(CASE WHEN is_high_confidence THEN 1 ELSE 0 END),1) s FROM odl.comment_sentiments WHERE processing_date>='{START}' GROUP BY 1")
    return trend(align_daily(rows,'d','s',80,True))
def ql_validity(cur):
    out=[]
    r=q(cur,"SELECT COUNT(*) n FROM rdl.post_label_predictions WHERE nlp_sentiment_confidence<0 OR nlp_sentiment_confidence>1")[0]
    out.append(['confidence out of range',int(r['n'] or 0)])
    r=q(cur,"SELECT COUNT(*) n FROM odl.dim_comments WHERE reactions_total<0")[0]
    out.append(['negative reactions',int(r['n'] or 0)])
    return out
def ql_text_lengths(cur):
    rows=q(cur,"""SELECT bucket,COUNT(*) c FROM (
        SELECT CASE WHEN word_count<3 THEN '1-2' WHEN word_count<6 THEN '3-5'
                    WHEN word_count<11 THEN '6-10' WHEN word_count<21 THEN '11-20'
                    ELSE '20+' END bucket
        FROM odl.comment_sentiments_v2 WHERE word_count IS NOT NULL) t GROUP BY 1""")
    order=['1-2','3-5','6-10','11-20','20+']; m={r['bucket']:int(r['c']) for r in rows}
    return [[b,m.get(b,0)] for b in order]

# ---- ANALYTICS ----
def an_dist(cur,col,table,limit=15):
    rows=q(cur,f"SELECT {col} v,COUNT(*) n FROM {table} WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT {limit}")
    return [[str(r['v']),int(r['n'])] for r in rows]
def an_sentiment_trend(cur):
    rows=q(cur,f"""SELECT inserted_at::date d,
        ROUND(100.0*SUM(CASE WHEN nlp_sentiment='positive' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) pos
      FROM rdl.post_label_predictions WHERE inserted_at>='{START}' GROUP BY 1""")
    return trend(align_daily(rows,'d','pos',68,True))
def an_reactions(cur):
    r=q(cur,"""SELECT SUM(reactions_like) l,SUM(reactions_love) lo,SUM(reactions_haha) h,
        SUM(reactions_wow) w,SUM(reactions_sad) s,SUM(reactions_angry) a FROM odl.dim_comments""")[0]
    return [['Like',int(r['l'] or 0)],['Love',int(r['lo'] or 0)],['Haha',int(r['h'] or 0)],
            ['Wow',int(r['w'] or 0)],['Sad',int(r['s'] or 0)],['Angry',int(r['a'] or 0)]]
def an_emotion_scores(cur):
    r=q(cur,"""SELECT AVG(emotion_joy_score) j,AVG(emotion_sadness_score) s,AVG(emotion_anger_score) a,
        AVG(emotion_fear_score) f,AVG(emotion_surprise_score) su,AVG(emotion_disgust_score) d
       FROM odl.comment_sentiments_v2""")[0]
    return {'axes':['Joy','Sadness','Anger','Fear','Surprise','Disgust'],
            'v':[round(100*float(r[k] or 0),1) for k in ['j','s','a','f','su','d']]}

def main():
    print("Multi-tab QA metrics - connecting...")
    conn=connect(); cur=conn.cursor()
    tabs={}
    tabs['accuracy']={'agreement':safe(acc_agreement,cur),'nlp_conf':safe(acc_confidence,cur,'nlp'),
        'ds_conf':safe(acc_confidence,cur,'ds'),'confusion':safe(acc_confusion,cur),
        'conf_buckets':safe(acc_conf_buckets,cur),'per_dim':safe(acc_per_dim,cur),
        'escalation':safe(acc_escalation,cur),'proctime':safe(acc_proctime,cur)}
    tabs['health']={'freshness':safe(hl_freshness_table,cur),'daily_loads':safe(hl_daily_loads,cur),
        'load_status':safe(hl_load_status,cur),'missing_days':safe(hl_missing_days,cur),
        'lag':safe(hl_lag,cur),'rowcount_delta':safe(hl_rowcount_delta,cur)}
    tabs['quality']={'completeness':safe(ql_completeness,cur),'dupes':safe(ql_dupes,cur),
        'integrity':safe(ql_integrity,cur),'dq_score':safe(ql_dq_score,cur),
        'highconf':safe(ql_highconf_share,cur),'validity':safe(ql_validity,cur),
        'text_lengths':safe(ql_text_lengths,cur)}
    tabs['analytics']={'topic':safe(an_dist,cur,'nlp_topic','rdl.post_label_predictions'),
        'intent':safe(an_dist,cur,'nlp_intent','rdl.post_label_predictions'),
        'emotion':safe(an_dist,cur,'nlp_emotion','rdl.post_label_predictions'),
        'fusion':safe(an_dist,cur,'fusion_label','rdl.fusion_predictions'),
        'tier':safe(an_dist,cur,'influence_tier','odl.dim_pages'),
        'sentiment_trend':safe(an_sentiment_trend,cur),'reactions':safe(an_reactions,cur),
        'emotion_scores':safe(an_emotion_scores,cur)}
    for t in tabs: tabs[t]={k:v for k,v in tabs[t].items() if v is not None}
    totals={}
    try:
        totals['posts']=q(cur,"SELECT COUNT(*) n FROM rdl.post_label_predictions")[0]['n']
        totals['comments']=q(cur,"SELECT COUNT(*) n FROM rdl.post_comments")[0]['n']
        totals['pages']=q(cur,"SELECT COUNT(*) n FROM odl.dim_pages")[0]['n']
    except Exception: pass
    payload={'generated_at':dt.datetime.utcnow().isoformat()+'Z',
             'window':{'days':WINDOW_DAYS,'start':START,'axis':[d.isoformat() for d in DAYS]},
             'totals':totals,'tabs':tabs}
    with open(OUT_PATH,'w') as f: json.dump(payload,f,separators=(',',':'))
    n=sum(len(v) for v in tabs.values())
    print(f"Wrote {OUT_PATH}: {n} live metrics across 4 tabs at {payload['generated_at']}")
    cur.close(); conn.close()

if __name__=='__main__':
    main()
