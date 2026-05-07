#!/usr/bin/env python3
"""
ArtemisAI cost data refresher.

Runs from GitHub Actions on a cron schedule (3x daily). Pulls AWS Cost
Explorer data for the last 30 days, runs anomaly detection per service,
evaluates user-defined alert rules from alerts.json, and emails the team
if anything fired. Writes the consolidated dataset to data/costs.json
which the static HTML page reads.

Inputs (env vars / files):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  — IAM user with ce:GetCostAndUsage
  SMTP_USER, SMTP_PASSWORD                  — Gmail account + app password
  alerts.json                               — alert rule definitions
  team_emails.json                          — recipient list

Outputs:
  data/costs.json                           — page reads this
  email to team if anomaly OR alert fires

Anomaly detection (per service):
  - Z-score: today's cost > mean(last 14 days) + 2 * std(last 14 days)
  - Drift: this 7-day total / previous 7-day total > 1.30
"""

import os
import json
import sys
import datetime
import statistics
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / 'data' / 'costs.json'
ALERTS_FILE = ROOT / 'alerts.json'
TEAM_FILE = ROOT / 'team_emails.json'

# ─── Services we care about (ArtemisAI only) ─────────────────────────
# Maps Cost Explorer service names to friendly labels + brand colours.
# Cost Explorer service strings come from the SERVICE dimension and
# are stable across accounts. Anything not in this map falls into "Other".
SERVICE_MAP = {
    'Amazon Redshift':                          {'label': 'Redshift',     'color': '#FF9900'},
    'AWS Lambda':                               {'label': 'Lambda',       'color': '#7C5CBF'},
    'Amazon DynamoDB':                          {'label': 'DynamoDB',     'color': '#F59E0B'},
    'Amazon Simple Storage Service':            {'label': 'S3',           'color': '#06B6D4'},
    'Amazon CloudFront':                        {'label': 'CloudFront',   'color': '#EC4899'},
    'AmazonCloudWatch':                         {'label': 'CloudWatch',   'color': '#65676B'},
    'Amazon API Gateway':                       {'label': 'API Gateway',  'color': '#3B82F6'},
    'Amazon Athena':                            {'label': 'Athena',       'color': '#22C55E'},
    'AWS Glue':                                 {'label': 'Glue',         'color': '#A855F7'},
}
# 3rd-party APIs we track separately. These can't be pulled from Cost
# Explorer — they'd need to be set manually in alerts.json or a future
# extension that hits OpenAI/Anthropic/DeepSeek billing APIs. For now
# they appear in the JSON as fixed estimates set by the team.
THIRD_PARTY_SERVICES = ['Anthropic API', 'DeepSeek API']

# ─── AWS Cost Explorer ───────────────────────────────────────────────

def fetch_cost_data():
    """Pull last 30 days grouped by service, daily granularity."""
    end = datetime.date.today()
    start = end - datetime.timedelta(days=30)

    client = boto3.client('ce', region_name='us-east-1')  # CE is global, but client needs a region
    response = client.get_cost_and_usage(
        TimePeriod={'Start': start.isoformat(), 'End': end.isoformat()},
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
    )
    return response['ResultsByTime']


def transform_costs(results):
    """
    Convert CE response into:
      - days: list of date strings
      - per_service: {service_label: [daily_costs_in_order]}
      - daily_total: [total_each_day]
      - mtd: month-to-date total
      - last_month_total: previous calendar month total (estimate)
    """
    days = []
    per_service = {}  # label → list of daily costs
    daily_total = []

    for day in results:
        date_str = day['TimePeriod']['Start']
        days.append(date_str)
        day_total = 0.0
        # Pre-fill all known services with 0 for this day
        seen_today = {}
        for group in day['Groups']:
            svc_raw = group['Keys'][0]
            cost = float(group['Metrics']['UnblendedCost']['Amount'])
            label = SERVICE_MAP.get(svc_raw, {'label': 'Other', 'color': '#999'})['label']
            seen_today[label] = seen_today.get(label, 0.0) + cost
            day_total += cost
        # Append today's value for every tracked service
        for known_label in [v['label'] for v in SERVICE_MAP.values()] + ['Other']:
            per_service.setdefault(known_label, []).append(seen_today.get(known_label, 0.0))
        daily_total.append(day_total)

    # MTD = sum of days where day starts with current YYYY-MM
    today = datetime.date.today()
    month_prefix = today.strftime('%Y-%m')
    mtd = sum(c for d, c in zip(days, daily_total) if d.startswith(month_prefix))

    # Last full month total — approximate by summing days in the previous month
    if today.month == 1:
        prev_month_prefix = f'{today.year - 1}-12'
    else:
        prev_month_prefix = f'{today.year}-{today.month - 1:02d}'
    last_month_total = sum(c for d, c in zip(days, daily_total) if d.startswith(prev_month_prefix))

    return {
        'days': days,
        'per_service': per_service,
        'daily_total': daily_total,
        'mtd': round(mtd, 2),
        'last_month_total': round(last_month_total, 2),
    }


# ─── Anomaly detection ───────────────────────────────────────────────

def detect_anomalies(cost_data):
    """
    Two checks per service:
      1. Z-score spike: latest day > mean(prev 14 days) + 2 * std
      2. Week-over-week drift: this_week_sum / last_week_sum > 1.30

    Returns list of anomaly dicts.
    """
    anomalies = []
    days = cost_data['days']
    if len(days) < 15:
        return anomalies  # not enough history

    for service, costs in cost_data['per_service'].items():
        if not costs or sum(costs) < 0.5:  # skip services with negligible spend
            continue

        # 1. Z-score on latest day
        latest = costs[-1]
        prev_14 = costs[-15:-1]  # 14 days before today
        if len(prev_14) >= 7:
            mean_14 = statistics.mean(prev_14)
            std_14 = statistics.stdev(prev_14) if len(prev_14) > 1 else 0
            if std_14 > 0:
                z = (latest - mean_14) / std_14
                if z > 2.0 and latest > 1.0:  # spike threshold + min cost filter
                    anomalies.append({
                        'service': service,
                        'type': 'spike',
                        'severity': 'high' if z > 3 else 'medium',
                        'date': days[-1],
                        'value': round(latest, 2),
                        'baseline': round(mean_14, 2),
                        'z_score': round(z, 2),
                        'message': f'{service} spent ${latest:.2f} on {days[-1]}, '
                                   f'{z:.1f}σ above the 14-day average of ${mean_14:.2f}.',
                    })

        # 2. Week-over-week drift
        if len(costs) >= 14:
            this_week = sum(costs[-7:])
            last_week = sum(costs[-14:-7])
            if last_week > 1.0 and this_week / last_week > 1.30:
                pct = (this_week / last_week - 1) * 100
                anomalies.append({
                    'service': service,
                    'type': 'drift',
                    'severity': 'medium' if pct < 50 else 'high',
                    'date': days[-1],
                    'this_week': round(this_week, 2),
                    'last_week': round(last_week, 2),
                    'pct_change': round(pct, 1),
                    'message': f'{service} spent ${this_week:.2f} this week vs ${last_week:.2f} '
                               f'last week — up {pct:.0f}%.',
                })

    return anomalies


# ─── Alert rule evaluation ───────────────────────────────────────────

def evaluate_alerts(cost_data, alert_rules):
    """
    Check user-defined rules from alerts.json against current data.
    Each rule has a 'type' that determines the check.
    Returns list of fired alerts.
    """
    fired = []
    days = cost_data['days']
    daily_total = cost_data['daily_total']

    for rule in alert_rules:
        if not rule.get('enabled', True):
            continue
        rule_type = rule.get('type')

        if rule_type == 'monthly_budget':
            # Forecast: scale MTD to full month
            today = datetime.date.today()
            days_in_month = (today.replace(month=today.month % 12 + 1, day=1)
                             - datetime.timedelta(days=1)).day if today.month < 12 \
                             else 31
            day_of_month = today.day
            forecast = cost_data['mtd'] * days_in_month / max(day_of_month, 1)
            threshold = rule['budget']
            if forecast > threshold:
                fired.append({
                    'rule_name': rule['name'],
                    'message': f'Forecast end-of-month spend ${forecast:.2f} exceeds budget ${threshold:.2f}.',
                    'severity': 'high',
                })

        elif rule_type == 'service_threshold':
            svc = rule['service']
            threshold = rule['threshold']
            costs = cost_data['per_service'].get(svc, [])
            month_so_far = sum(costs[-datetime.date.today().day:]) if costs else 0
            if month_so_far > threshold:
                fired.append({
                    'rule_name': rule['name'],
                    'message': f'{svc} month-to-date ${month_so_far:.2f} exceeds threshold ${threshold:.2f}.',
                    'severity': 'high',
                })

        elif rule_type == 'daily_spike':
            multiplier = rule.get('multiplier', 2.0)
            if len(daily_total) >= 8:
                today_cost = daily_total[-1]
                week_avg = sum(daily_total[-8:-1]) / 7
                if today_cost > week_avg * multiplier:
                    fired.append({
                        'rule_name': rule['name'],
                        'message': f'Today spent ${today_cost:.2f}, '
                                   f'{today_cost/week_avg:.1f}× the 7-day average ${week_avg:.2f}.',
                        'severity': 'high',
                    })

        elif rule_type == 'service_appears':
            # Useful for catching new AWS services creeping into the bill
            svc = rule.get('service', 'any')
            today_idx = -1
            for label, costs in cost_data['per_service'].items():
                if svc != 'any' and label != svc:
                    continue
                # Service appeared if it has spend today but didn't in the prior 14 days
                if costs[today_idx] > rule.get('min_cost', 0.10):
                    if max(costs[-15:-1]) < rule.get('min_cost', 0.10):
                        fired.append({
                            'rule_name': rule['name'],
                            'message': f'New service detected: {label} spent ${costs[today_idx]:.2f} today.',
                            'severity': 'medium',
                        })

    return fired


# ─── Email ────────────────────────────────────────────────────────────

def send_email(recipients, subject, body_html):
    """Send via Gmail SMTP using an app password."""
    smtp_user = os.environ.get('SMTP_USER')
    smtp_password = os.environ.get('SMTP_PASSWORD')

    if not smtp_user or not smtp_password:
        print('SMTP_USER / SMTP_PASSWORD not set, skipping email')
        return False

    if not recipients:
        print('No recipients, skipping email')
        return False

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ArtemisAI Cost Monitor <{smtp_user}>'
    msg['To'] = ', '.join(recipients)
    msg.attach(MIMEText(body_html, 'html'))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
    print(f'Sent email to {len(recipients)} recipients')
    return True


def render_email(anomalies, fired_alerts, cost_data):
    """Build a readable HTML email summarising what fired."""
    today = datetime.date.today().isoformat()

    rows = []
    if fired_alerts:
        rows.append('<h2 style="color:#E53935;margin:20px 0 8px">🚨 Alert rules fired</h2>')
        for a in fired_alerts:
            rows.append(f'<div style="padding:12px;background:#fef2f2;border-left:3px solid #E53935;margin-bottom:8px;border-radius:6px">'
                        f'<strong>{a["rule_name"]}</strong><br>'
                        f'<span style="color:#666;font-size:13px">{a["message"]}</span></div>')

    if anomalies:
        rows.append('<h2 style="color:#F59E0B;margin:20px 0 8px">⚡ Anomalies detected</h2>')
        for a in anomalies:
            color = '#E53935' if a['severity'] == 'high' else '#F59E0B'
            rows.append(f'<div style="padding:12px;background:#fffbeb;border-left:3px solid {color};margin-bottom:8px;border-radius:6px">'
                        f'<strong>{a["service"]}</strong> <span style="color:#999;font-size:11px">[{a["type"]} · {a["severity"]}]</span><br>'
                        f'<span style="color:#666;font-size:13px">{a["message"]}</span></div>')

    summary = (f'<p>Month-to-date: <strong>${cost_data["mtd"]:.2f}</strong> · '
               f'Last month total: <strong>${cost_data["last_month_total"]:.2f}</strong></p>')

    return f'''<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:20px;color:#1f2937">
<div style="border-bottom:2px solid #00C4A0;padding-bottom:14px;margin-bottom:20px">
  <h1 style="margin:0;font-size:22px">ArtemisAI Cost Monitor</h1>
  <p style="margin:4px 0 0;color:#666;font-size:13px">{today} · automated check</p>
</div>
{summary}
{''.join(rows)}
<hr style="border:none;border-top:1px solid #e5e7eb;margin:24px 0">
<p style="color:#999;font-size:12px">Sent by GitHub Actions · scripts/fetch_costs.py · See <a href="https://github.com/scrapesync/artemisai_website/blob/main/cost_estimator.html">cost_estimator.html</a> for full charts.</p>
</body></html>'''


# ─── Main ─────────────────────────────────────────────────────────────

def load_json(path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


def main():
    print(f'[{datetime.datetime.now().isoformat()}] starting cost refresh')

    # 1. Pull cost data
    raw = fetch_cost_data()
    cost_data = transform_costs(raw)
    print(f'  fetched {len(cost_data["days"])} days, MTD ${cost_data["mtd"]:.2f}')

    # 2. Anomaly detection
    anomalies = detect_anomalies(cost_data)
    print(f'  detected {len(anomalies)} anomalies')

    # 3. Evaluate user rules
    rules = load_json(ALERTS_FILE, [])
    fired = evaluate_alerts(cost_data, rules)
    print(f'  evaluated {len(rules)} rules, {len(fired)} fired')

    # 4. Build the JSON the page reads
    output = {
        'last_updated': datetime.datetime.utcnow().isoformat() + 'Z',
        'days': cost_data['days'],
        'daily_total': cost_data['daily_total'],
        'per_service': cost_data['per_service'],
        'mtd': cost_data['mtd'],
        'last_month_total': cost_data['last_month_total'],
        'service_colors': {SERVICE_MAP[k]['label']: SERVICE_MAP[k]['color'] for k in SERVICE_MAP},
        'anomalies': anomalies,
        'fired_alerts': fired,
        'rules': rules,
    }
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(output, indent=2))
    print(f'  wrote {DATA_FILE}')

    # 5. Email if anything fired
    if anomalies or fired:
        recipients = load_json(TEAM_FILE, {'emails': []}).get('emails', [])
        subject_parts = []
        if fired:
            subject_parts.append(f'{len(fired)} alert{"s" if len(fired) > 1 else ""}')
        if anomalies:
            subject_parts.append(f'{len(anomalies)} anomal{"ies" if len(anomalies) > 1 else "y"}')
        subject = f'[ArtemisAI Cost] {" + ".join(subject_parts)}'
        body = render_email(anomalies, fired, cost_data)
        send_email(recipients, subject, body)
    else:
        print('  no alerts/anomalies fired, no email sent')

    print('done')


if __name__ == '__main__':
    main()
