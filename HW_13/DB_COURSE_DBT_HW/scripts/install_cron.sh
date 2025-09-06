#!/usr/bin/env bash
set -e

PROJECT_DIR="${1}"
if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "ERROR: Project dir '$PROJECT_DIR' not found"
  exit 1
fi
pwd
RUN_SCRIPT="$PROJECT_DIR/scripts/run_dbt_all.sh"

chmod +x "$RUN_SCRIPT"

TMP_CRON="$(mktemp)"
crontab -l 2>/dev/null | grep -v "$RUN_SCRIPT" > "$TMP_CRON" || true

{ cat <<'CRON'
CRON_TZ=Europe/Kyiv
# Run dbt build daily at 13:00 Kyiv
0 13 * * * /bin/bash -lc '"$RUN_SCRIPT"'
CRON
} >> "$TMP_CRON"

sed -i "s#"\$RUN_SCRIPT"#${RUN_SCRIPT}#g" "$TMP_CRON"

crontab "$TMP_CRON"
rm -f "$TMP_CRON"

echo "Installed cron for: $RUN_SCRIPT"
crontab -l
