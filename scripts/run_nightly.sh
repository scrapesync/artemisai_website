#!/usr/bin/env bash
# Nightly QA metrics refresh — invoked by cron at 01:00.
# crontab -e:
#   0 1 * * *  /path/to/repo/scripts/run_nightly.sh >> /var/log/qa_metrics.log 2>&1
set -euo pipefail
cd "$(dirname "$0")/.."
export REDSHIFT_HOST="${REDSHIFT_HOST:?set me}" REDSHIFT_DB="${REDSHIFT_DB:?}" \
       REDSHIFT_USER="${REDSHIFT_USER:?}" REDSHIFT_PASSWORD="${REDSHIFT_PASSWORD:?}"
python3 scripts/generate_qa_metrics.py
if ! git diff --quiet qa_metrics.json 2>/dev/null; then
  git add qa_metrics.json
  git commit -m "chore: nightly QA metrics snapshot $(date -u +%Y-%m-%dT%H:%MZ)"
  git push origin main
  echo "pushed new snapshot"
else
  echo "no metric change"
fi
