# Cost Estimator — Deployment Guide

Step-by-step instructions to take this from "code in the repo" to "live, refreshing 3× daily, emailing the team when something is wrong."

**Estimated time:** 30–45 minutes the first time. The 24-hour Cost Explorer activation delay is the only thing that blocks same-day setup.

---

## Overview

```
┌──────────────────┐  scheduled  ┌──────────────────┐  writes   ┌──────────────────┐
│ AWS Cost         │ ◀──────────│ GitHub Actions   │──────────▶│ data/costs.json  │
│ Explorer API     │             │ runs Python fn   │  commits  │ in the repo      │
└──────────────────┘             └──────────────────┘   back    └──────────────────┘
                                          │                              │
                                          │ if anomaly/alert             │ browser fetches
                                          ▼                              ▼
                                 ┌──────────────────┐           ┌──────────────────┐
                                 │ Gmail SMTP       │──email──▶ │ team_emails.json │
                                 │                  │           │ recipients       │
                                 └──────────────────┘           └──────────────────┘
                                                                         │ renders
                                                                         ▼
                                                                ┌──────────────────┐
                                                                │ cost_estimator   │
                                                                │ .html            │
                                                                └──────────────────┘
```

No Lambda, no API Gateway, no DynamoDB, no servers. The page is pure static HTML hosted on Netlify (your existing site). GitHub Actions does all the work and commits an updated JSON file back to the repo every 8 hours.

---

## What you need before starting

- [ ] AWS Console access for the ArtemisAI account (admin or someone who can create IAM users)
- [ ] GitHub admin access to `scrapesync/artemisai_website`
- [ ] A Gmail account you control (can be a personal one or a dedicated `cost-monitor@artemisai.co.uk`)
- [ ] 2FA enabled on that Gmail account (required for app passwords)

---

## Step 1 — Enable Cost Explorer in AWS

Cost Explorer must be activated on the AWS account before the API will return anything. **This has a 24-hour delay the first time** — do this step first so the wait runs in parallel with everything else.

1. Sign in to AWS Console as the ArtemisAI account
2. Top right → your account name → **Billing and Cost Management**
3. Left sidebar → **Cost Explorer**
4. Click **Enable Cost Explorer** if you see the welcome screen

That's it. AWS now starts ingesting cost data into the API. The first 24 hours will return empty results — that's normal.

---

## Step 2 — Create an IAM user for the GitHub Action

We're creating a dedicated read-only user, not reusing your personal credentials. If the access key leaks, the worst it can do is read your cost data (no write access, no other AWS permissions).

1. AWS Console → **IAM** → **Users** → **Create user**
2. User name: `github-actions-cost-reader`
3. **Do NOT** check "Provide user access to the AWS Management Console"
4. Click Next
5. Permissions options → **Attach policies directly**
6. Click **Create policy** (opens new tab)
7. In the new tab, click the **JSON** tab and paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ce:GetCostAndUsage",
        "ce:GetCostForecast",
        "ce:GetDimensionValues"
      ],
      "Resource": "*"
    }
  ]
}
```

8. Next → name it `CostExplorerReadOnly` → Create policy
9. Go back to the user creation tab → refresh the policy list → search `CostExplorerReadOnly` → check the box → Next → Create user

### Generate the access key

10. Click into the user → **Security credentials** tab
11. Scroll down to **Access keys** → **Create access key**
12. Use case → **Application running outside AWS** → Next → Create
13. **Copy both values immediately** (the secret is only shown once):
    - Access key ID — looks like `AKIAIOSFODNN7EXAMPLE`
    - Secret access key — looks like `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

Paste these somewhere safe for 2 minutes — you'll add them to GitHub in Step 4.

---

## Step 3 — Set up Gmail for sending alert emails

We're using SMTP because it's the simplest — no AWS SES domain verification, no third-party signup, just a Gmail account and an "app password" (a 16-character bypass token that lets the script log in without your real password).

1. Go to the Gmail account that will be the **sender** (the "From:" line on alert emails)
2. Google Account → Security → make sure **2-Step Verification** is on
3. Same Security page → search for **App passwords** → click into it
4. Sign in again if prompted
5. App name: `ArtemisAI Cost Monitor`
6. Click **Create**
7. Copy the 16-character password shown (no spaces — Google displays them with spaces for readability but it's one token)

Keep this password somewhere safe for 2 minutes. You'll never see it again.

> Alternative: if your team already has a shared Google Workspace account (e.g. `cost-monitor@artemisai.co.uk`), use that instead of a personal Gmail. The recipients see whatever address you use here as the "From:" line.

---

## Step 4 — Add secrets to GitHub

1. Open the repo: https://github.com/scrapesync/artemisai_website
2. Settings (top-right tab) → **Secrets and variables** → **Actions**
3. Click **New repository secret** for each of these 4:

| Name | Value |
| --- | --- |
| `AWS_ACCESS_KEY_ID` | The access key ID from Step 2 |
| `AWS_SECRET_ACCESS_KEY` | The secret access key from Step 2 |
| `SMTP_USER` | The Gmail address from Step 3 (e.g. `you@gmail.com`) |
| `SMTP_PASSWORD` | The 16-character app password from Step 3 (no spaces) |

GitHub stores these encrypted. They're injected into the workflow as environment variables when it runs, and never exposed in logs.

---

## Step 5 — Update the team email list

1. In the repo, open `team_emails.json`
2. Replace the placeholder list with the actual recipients
3. Commit directly to `main` (or via PR if you prefer)

```json
{
  "_doc": "Recipients for cost-monitor email alerts.",
  "emails": [
    "asad@artemisai.co.uk",
    "alex@artemisai.co.uk",
    "filza@artemisai.co.uk"
  ]
}
```

That's it for recipient management — to add/remove someone later, edit this file and commit.

---

## Step 6 — Trigger the first run manually

Don't wait for the cron (10 AM PKT). Trigger it now to verify everything works.

1. Repo → **Actions** tab
2. Left sidebar → **Refresh AWS cost data** workflow
3. Top right → **Run workflow** dropdown → keep "main" selected → **Run workflow** button

Wait 30–60 seconds. Refresh the Actions page.

### What success looks like

- Green checkmark next to the run
- Click the run → expand the "Run cost fetcher" step → you should see logs like:
  ```
  [2026-05-07T05:00:12] starting cost refresh
    fetched 30 days, MTD $XX.XX
    detected N anomalies
    evaluated 4 rules, N fired
    wrote /home/runner/work/artemisai_website/artemisai_website/data/costs.json
  ```
- The "Commit refreshed data" step shows either "data: refresh costs.json" or "No data changes — skipping commit"
- Open the page at `https://artemisai.co.uk/cost_estimator.html` — placeholder banner is gone, real numbers showing

### What failure looks like (and how to fix)

| Error in logs | What it means | Fix |
| --- | --- | --- |
| `botocore.exceptions.ClientError: ... AccessDenied` | IAM policy not attached or wrong | Re-check Step 2 policy is `CostExplorerReadOnly` and attached to the user |
| `... DataUnavailableException` | Cost Explorer not yet enabled or under 24-hr activation delay | Wait, then re-run |
| `smtplib.SMTPAuthenticationError: Username and Password not accepted` | App password wrong or 2FA not enabled | Regenerate app password at Step 3, update `SMTP_PASSWORD` secret |
| `Permission to scrapesync/artemisai_website.git denied` on the commit step | Workflow doesn't have write permission | Repo Settings → Actions → General → "Workflow permissions" → Read and write permissions |

---

## Step 7 — Verify the schedule

The workflow runs at:
- **10:00 AM PKT** = 05:00 UTC
- **6:00 PM PKT** = 13:00 UTC
- **10:00 PM PKT** = 17:00 UTC

GitHub cron has a known issue where scheduled runs can be delayed by 5–15 minutes during peak hours — that's normal, don't worry if 10:00 AM becomes 10:08 AM.

To check it's actually scheduled: Actions tab → workflow → Runs page should show the manual run from Step 6 plus future scheduled runs as they happen.

---

## Day-to-day operations

### Add or change an alert rule

1. Edit `alerts.json` in the repo
2. Commit to main
3. Next scheduled run picks it up automatically

Available rule types:

```jsonc
// Forecast end-of-month spend > $X
{ "name": "Monthly budget", "type": "monthly_budget", "budget": 650, "enabled": true }

// Today's spend > N× the 7-day rolling average
{ "name": "Daily spike", "type": "daily_spike", "multiplier": 2.0, "enabled": true }

// Single service month-to-date > $X
{ "name": "Redshift > $200", "type": "service_threshold", "service": "Redshift", "threshold": 200, "enabled": true }

// New service appearing in the bill (caught accidental services left running)
{ "name": "New service", "type": "service_appears", "service": "any", "min_cost": 0.50, "enabled": true }
```

Set `"enabled": false` to pause a rule without deleting it.

### Add or remove an email recipient

Edit `team_emails.json` and commit. Done.

### Force a refresh right now

Actions tab → Refresh AWS cost data → Run workflow. Same as Step 6.

### Disable the whole thing temporarily

Actions tab → Refresh AWS cost data → top-right `...` menu → Disable workflow. Re-enable from the same place when you want it back.

### Check what last ran

Actions tab shows every run with its result, duration, and full logs. Click into a specific run to see what the script printed and whether the commit succeeded.

---

## Costs of running this

- **Cost Explorer API:** $0.01 per request, 3 requests per day = **~$0.90/month**
- **GitHub Actions:** Free for public repos, generous free tier for private
- **Gmail SMTP:** Free
- **Total:** under $1/month for the monitoring itself

Negligible compared to what it catches.

---

## When something looks wrong

**"The page shows the placeholder banner forever"**
The first scheduled run hasn't happened yet, or it failed. Check Actions tab. Trigger manually if needed.

**"I'm not getting emails even though the page shows alerts"**
SMTP_USER or SMTP_PASSWORD is wrong, or `team_emails.json` is empty. Check the run logs in the Actions tab — they print whether the email was sent.

**"Numbers don't match my AWS Console bill"**
Cost Explorer reports unblended cost (post-credit, post-discount). The console shows the same. But there's a 24–48 hour lag in Cost Explorer data — yesterday's full bill might not appear in the API until tomorrow. Don't compare hour-by-hour, compare 3-day-old totals.

**"GitHub Action says success but the page still shows placeholder banner"**
The page caches the JSON. Hard refresh (Ctrl+Shift+R or Cmd+Shift+R). The fetch URL has `?_=Date.now()` appended to bypass caching but Netlify CDN can still cache the file itself for ~5 minutes.

**"Anomaly detected but I think it's a false positive"**
The detection is intentionally simple — z-score >2σ catches genuine spikes but also catches the first day of a new workload (e.g. starting Sprint 4 NLP labelling). Either accept the noise (one false positive a month is fine for awareness) or add a `service_threshold` rule with a higher threshold for that specific service so the spike doesn't trigger anything.

---

## Files in the repo

```
artemisai_website/
├── cost_estimator.html              ← the page (reads data/costs.json)
├── alerts.json                       ← rules (edit by hand)
├── team_emails.json                  ← recipients (edit by hand)
├── COST_ESTIMATOR_SETUP.md           ← setup doc (already in repo)
├── DEPLOYMENT_GUIDE.md               ← this document
├── data/
│   └── costs.json                    ← output of the fetcher (overwritten every run)
├── scripts/
│   └── fetch_costs.py                ← the Python fetcher
└── .github/
    └── workflows/
        └── cost-refresh.yml          ← cron schedule + workflow
```
