#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# run_local.sh — run the ETF pipeline locally on a schedule (free, no GitHub)
#
# Setup (one time):
#   chmod +x run_local.sh
#   crontab -e
#   Add this line to run every weekday at 6 PM local time:
#   0 18 * * 1-5 /full/path/to/aml-pipeline-githubaction/run_local.sh >> /full/path/to/aml-pipeline-githubaction/Log/cron.log 2>&1
#
# ─────────────────────────────────────────────────────────────────────────────

# Go to the pipeline directory (update this path)
cd "$(dirname "$0")"

# Activate your Python env if you have one (update or remove if not needed)
# source /Users/A3014443/.pyenv/versions/amlcf/bin/activate

echo "========================================"
echo "ETF Pipeline run started: $(date)"
echo "========================================"

python pipeline.py \
  --update-meta \
  --workers 3

echo "========================================"
echo "ETF Pipeline run finished: $(date)"
echo "========================================"
