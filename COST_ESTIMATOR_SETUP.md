# Cost Estimator — Setup

This page is refreshed 3x daily by GitHub Actions. To enable it on a fresh
clone or new account, two sets of secrets need to be configured.

## 1. AWS IAM user (read-only Cost Explorer access)

Create a dedicated IAM user — do **not** reuse your personal credentials.

1. AWS Console → IAM → Users → **Create user** → `github-actions-cost-reader`
2. Attach this inline policy (don't give broader permissions):

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

3. Create access key (use case: "Application running outside AWS").
4. Copy the access key ID and secret — you'll paste them into GitHub next.

**Note:** Cost Explorer must be enabled on the AWS account before the API
will return data. AWS Console → Billing → Cost Explorer → Enable. There's a
24-hour activation delay the first time.

## 2. Gmail app password (for sending alert emails)

Using a dedicated Gmail account (e.g. `cost-monitor@artemisai.co.uk` or a
personal Gmail you don't mind being the sender):

1. Enable 2-Step Verification on the Google account if not already.
2. Google Account → Security → **App passwords** → generate one for "Mail".
3. Copy the 16-character password (no spaces).

This is the password the GitHub Action uses to log into SMTP and send mail.
The "From:" address shown to recipients will be the Gmail account.

## 3. Add secrets to GitHub

GitHub repo → Settings → Secrets and variables → Actions → **New repository secret**:

| Name                    | Value                                            |
| ----------------------- | ------------------------------------------------ |
| `AWS_ACCESS_KEY_ID`     | (from step 1)                                    |
| `AWS_SECRET_ACCESS_KEY` | (from step 1)                                    |
| `SMTP_USER`             | the Gmail address sending alerts                 |
| `SMTP_PASSWORD`         | the 16-char app password from step 2             |

## 4. Manage the alert rules

- **Add/edit rules:** edit `alerts.json` and commit.
- **Add/remove email recipients:** edit `team_emails.json` and commit.

Available rule `type` values:

| Type                | Required fields                | Description                                                  |
| ------------------- | ------------------------------ | ------------------------------------------------------------ |
| `monthly_budget`    | `budget`                       | Forecast end-of-month > budget                               |
| `daily_spike`       | `multiplier`                   | Today > N× the 7-day rolling average                         |
| `service_threshold` | `service`, `threshold`         | Single service month-to-date > threshold                     |
| `service_appears`   | `service` (or "any"), `min_cost` | New service appears in bill that wasn't there 14 days ago |

All rules also accept `enabled: true|false` to pause without deleting.

## 5. Run schedule

The workflow runs at:
- **10:00 AM PKT** (05:00 UTC)
- **06:00 PM PKT** (13:00 UTC)
- **10:00 PM PKT** (17:00 UTC)

Manual run: GitHub Actions tab → "Refresh AWS cost data" → Run workflow.

## 6. What the page reads

`cost_estimator.html` fetches `data/costs.json` on load. That file is written
by `scripts/fetch_costs.py` and committed back by the GitHub Action's bot
account. If the page is blank, check the Actions tab for the most recent run.
