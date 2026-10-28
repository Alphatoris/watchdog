#!/usr/bin/env bash
# One long job: tick every 300 s for about 5 h 50 min, then start the next run with workflow_dispatch.
# Not dependent on GitHub's cron timing. The 5-minute cron stays only as a restart: a cron run that finds
# another run in progress exits at once.
set -u
ME="${GITHUB_RUN_ID}"
guard() {
  for _ in 1 2 3 4 5 6; do
    others=$(gh run list --workflow watchdog.yml --status in_progress --json databaseId --jq "[.[] | select(.databaseId != ${ME})] | length")
    [ "${others:-0}" = "0" ] && return 0
    sleep 10
  done
  return 1
}
guard || { echo "another loop is running; exiting"; exit 0; }
START=$(date +%s)
for i in $(seq 0 69); do
  TARGET=$((START + i * 300))
  NOW=$(date +%s)
  [ "$NOW" -lt "$TARGET" ] && sleep $((TARGET - NOW))
  python3 watchdog.py --out status-out
  RC=$?
  ( cd status-out && git config user.name watchdog && git config user.email watchdog@users.noreply.github.com \
    && tail -n 300 ticks.jsonl > t && mv t ticks.jsonl && git add -A \
    && { git diff --cached --quiet || git commit -q -m tick; } \
    && { git push -q origin status || { git pull -q --rebase origin status && git push -q origin status; }; } ) || echo "publish failed"
  [ "$RC" != "0" ] && echo "tick exit $RC"
done
gh workflow run watchdog.yml --ref main
