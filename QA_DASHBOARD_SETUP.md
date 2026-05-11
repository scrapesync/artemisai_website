# QA Dashboard — Deployment Guide

End-to-end instructions to take the QA Dashboard from "code in the repo" to "live, refreshing every morning at 9 AM PKT, emailing the team if data is stale."

**Estimated time:** 20–30 minutes the first time.

---

## Overview

```
┌──────────────────┐  scheduled  ┌──────────────────┐  appends  ┌──────────────────┐
│ Redshift         │ ◀──────────│ GitHub Actions   │──────────▶│ data/qa_history  │
│ (rdl schema)     │  9AM PKT    │ runs Python fn   │  commits  │   .json          │
└──────────────────┘             └──────────────────┘   back    └──────────────────┘
                                          │                              │
                                          │ if anomaly                   │ browser fetches
                                          ▼                              ▼
                                 ┌──────────────────┐           ┌──────────────────┐
                                 │ Gmail SMTP       │──email──▶ │ team_emails.json │
                                 └──────────────────┘           │  recipients      │
                                                                └──────────────────┘
                                                                         │ renders
                                                                         ▼
                                                                ┌──────────────────┐
                                                                │ qa_dashboard.html│
                                                                └──────────────────┘
```

No servers, no Lambda, no API Gateway. Pure static HTML hosted on Netlify (your existing site). GitHub Actions runs the Python script once a day, commits the updated JSON back to the repo.

---

## Prereq checklist

- [ ] AWS Console access for the ArtemisAI account (someone who can manage Redshift users)
- [ ] GitHub admin access to `scrapesync/artemisai_website`
- [ ] A Gmail account with 2-Step Verification enabled (for sending alerts)
- [ ] You know the Redshift cluster endpoint and database name

---

## Step 1 — Create a read-only Redshift user

We're creating a dedicated DB user with SELECT only on the `rdl` schema. If the credentials leak the worst that can happen is someone reading data, not modifying it.

Connect to Redshift as an admin (psql, DBeaver, the Query Editor in AWS Console — whatever you use) and run:

```sql
-- Create user with a strong password
CREATE USER github_qa_reader WITH PASSWORD 'pick-a-long-random-password-here';

-- Grant connect on the database
GRANT CONNECT ON DATABASE <your_db_name> TO github_qa_reader;

-- Grant usage on the schema
GRANT USAGE ON SCHEMA rdl TO github_qa_reader;

-- Grant SELECT on all the tables the QA query reads
GRANT SELECT ON rdl.page_posts             TO github_qa_reader;
GRANT SELECT ON rdl.page_info              TO github_qa_reader;
GRANT SELECT ON rdl.page_daily_insights    TO github_qa_reader;
GRANT SELECT ON rdl.post_daily_insights    TO github_qa_reader;
GRANT SELECT ON rdl.post_label_predictions TO github_qa_reader;
```

Save the password somewhere safe — you'll add it to GitHub in Step 3.

### Verify it works

From your machine, try connecting as the new user and running the freshness query:

```bash
psql "host=<endpoint> port=5439 dbname=<db> user=github_qa_reader password=<pwd>" \
  -c "SELECT 'page_posts', COUNT(*) FROM rdl.page_posts;"
```

Should print the row count. If it errors with "permission denied", the GRANTs didn't apply — re-run them.

---

## Step 2 — Allow GitHub Actions to reach Redshift

Two ways depending on your network setup:

### Option A: Cluster publicly accessible (simplest)

If your Redshift cluster's security group already allows connections from `0.0.0.0/0` on port 5439, you're done. GitHub Actions can connect.

### Option B: VPC-private cluster

If Redshift is in a private VPC, you need to either:
- Add the GitHub Actions IP ranges to the security group (these change — see [github.com/actions/runner-images](https://github.com/actions/runner-images) for the list, refreshed monthly)
- Use a self-hosted runner inside the VPC (more setup, but no firewall rules to maintain)

For an internal QA tool, Option A is fine — the user has read-only access to one schema, the surface area is tiny.

---

## Step 3 — Gmail app password (if you haven't already for the other dashboards)

If you've already set up `SMTP_USER` and `SMTP_PASSWORD` secrets for another bot, **skip this step** — the QA dashboard reuses them.

Otherwise:

1. Go to the Gmail account that will be the sender
2. Google Account → Security → confirm **2-Step Verification** is on
3. Security → **App passwords** → create one named `ArtemisAI QA Bot`
4. Copy the 16-character password

---

## Step 4 — Add GitHub secrets

Repo → Settings → Secrets and variables → Actions → **New repository secret** for each:

| Name | Value |
| --- | --- |
| `REDSHIFT_HOST` | Cluster endpoint, e.g. `artemisai-prod.abc123.us-east-1.redshift.amazonaws.com` |
| `REDSHIFT_PORT` | `5439` (default) |
| `REDSHIFT_DB` | Database name on the cluster |
| `REDSHIFT_USER` | `github_qa_reader` |
| `REDSHIFT_PASSWORD` | The password you set in Step 1 |
| `SMTP_USER` | Gmail address (skip if already added) |
| `SMTP_PASSWORD` | Gmail app password (skip if already added) |

---

## Step 5 — Update the team email list

Edit `team_emails.json` in the repo root and commit:

```json
{
  "_doc": "Recipients for ArtemisAI bot emails (QA + future dashboards).",
  "emails": [
    "asad@artemisai.co.uk",
    "alex@artemisai.co.uk",
    "filza@artemisai.co.uk"
  ]
}
```

---

## Step 6 — Trigger the first run manually

1. Repo → **Actions** tab
2. Left sidebar → **Refresh QA health data**
3. Top right → **Run workflow** → keep `main` → **Run workflow** button

Wait ~30 seconds, refresh, click the run.

### Success looks like

```
[2026-05-11T04:00:01] starting QA refresh
  fetched freshness for 5 tables
  history now has 1 snapshots
  evaluated 3 rules, 0 fired
  wrote /home/runner/.../data/qa_history.json
  no alerts fired, no email sent
done
```

…followed by a `data: refresh qa_history.json` commit on `main`.

Open `https://artemisai.co.uk/qa_dashboard.html` — placeholder banner should be gone, real numbers showing.

### Failure modes

| Log message | Cause | Fix |
| --- | --- | --- |
| `psycopg2.OperationalError: connection ... timeout` | Security group blocking, or cluster paused | Check security group, resume cluster if paused |
| `... password authentication failed` | Wrong password or user doesn't exist | Re-run the CREATE USER SQL, update `REDSHIFT_PASSWORD` |
| `... relation "rdl.page_posts" does not exist` | Schema/table name wrong, or user lacks SELECT | Re-run the GRANT SELECT statements |
| `smtplib.SMTPAuthenticationError` | Wrong Gmail app password | Regenerate, update `SMTP_PASSWORD` |
| `Permission to ... denied` on commit step | Workflow doesn't have write perms | Repo Settings → Actions → General → Workflow permissions → "Read and write" |

---

## Step 7 — Verify the schedule

The workflow runs at **9:00 AM Pakistan Standard Time = 04:00 UTC daily**.

GitHub cron can be delayed 5–15 minutes during peak hours — first scheduled run might show up at 9:08 or so. That's normal.

---

## Day-to-day

### Add or change an alert rule

Edit `qa_alerts.json`, commit, done. Next scheduled run picks it up.

Available rule types:

```jsonc
// Any table stale beyond N days
{
  "name": "Stale check",
  "type": "stale_table",
  "tables": "all",          // or ["page_posts", "page_info"]
  "max_days": 2,
  "enabled": true
}

// Specific table didn't load today
{
  "name": "page_posts missing",
  "type": "no_load_today",
  "tables": ["page_posts"],
  "enabled": true
}

// Row count dropped significantly vs 7-day average
{
  "name": "Row drop check",
  "type": "row_count_drop",
  "tables": "all",
  "drop_pct": 0.5,          // 0.5 = fire if today < 50% of 7-day avg
  "enabled": true
}
```

Set `"enabled": false` to pause a rule without deleting.

### Add a new tracked table

Two places to edit (in `scripts/fetch_qa_health.py`):

1. Add the table name to the `TRACKED_TABLES` list at the top
2. Add a UNION ALL block to `FRESHNESS_SQL` matching the same shape as the others

Plus update the `TABLE_COLORS` map in `qa_dashboard.html` so the chart has a colour for it.

### Add or remove email recipients

Edit `team_emails.json`, commit.

### Force a refresh

Actions tab → Refresh QA health data → Run workflow.

### Disable temporarily

Actions tab → workflow → `...` menu → Disable workflow.

---

## Cost of running this

- **Redshift queries:** ~$0.0001 per run (5 small COUNT(*) queries, one connection). Negligible.
- **GitHub Actions:** Free (well within free tier even on private repos)
- **Gmail SMTP:** Free
- **Total:** $0/month

---

## Files

```
artemisai_website/
├── qa_dashboard.html                ← the page (reads data/qa_history.json)
├── qa_alerts.json                   ← rules (edit by hand)
├── team_emails.json                 ← recipients (shared with other dashboards)
├── QA_DASHBOARD_SETUP.md            ← this document
├── data/
│   └── qa_history.json              ← appended daily by the script (90-day rolling)
├── scripts/
│   └── fetch_qa_health.py           ← the Python fetcher
└── .github/
    └── workflows/
        └── qa-refresh.yml           ← cron + workflow
```

---

## Troubleshooting

**Page shows placeholder banner forever**
First scheduled run hasn't happened or it failed. Check Actions tab. Trigger manually if needed.

**Not getting emails even though page shows fired alerts**
`SMTP_USER` / `SMTP_PASSWORD` wrong, or `team_emails.json` empty. Check the run logs in Actions tab — they print whether the email was sent or skipped.

**Row counts shrinking on the chart**
That means rows were deleted from the source table. If unexpected, check who/what is deleting data. The QA dashboard doesn't delete — it only reads.

**Heatmap shows missing days for older history**
The history file accumulates from the day you first ran the workflow. The first 21 days will progressively fill up. After that you'll have a rolling 90-day view.

**One table has dramatically different scale on the chart and it's hard to read others**
The row-count chart uses a linear Y axis. If `post_daily_insights` is 100x the size of `page_info`, the small tables flatline. Open the chart, click the legend entries to toggle off the dominant table, the smaller ones will rescale.
