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
# A pass every 60 s. Every fifth pass is the 5-minute tick (it counts, moves the ladder clock and publishes the feed);
# the passes between are fast: they only send what is due for a newly late item, so a new item wakes its owner within a minute.
for i in $(seq 0 349); do
  TARGET=$((START + i * 60))
  NOW=$(date +%s)
  [ "$NOW" -lt "$TARGET" ] && sleep $((TARGET - NOW))
  if [ $((i % 5)) -eq 0 ]; then
    python3 watchdog.py --out status-out
  else
    python3 watchdog.py --out status-out --fast
  fi
  RC=$?
  ( cd status-out && git config user.name watchdog && git config user.email watchdog@users.noreply.github.com \
    && tail -n 300 ticks.jsonl > t && mv t ticks.jsonl && git add -A \
    && { git diff --cached --quiet || git commit -q -m tick; } \
    && { [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/status 2>/dev/null)" ] || { git push -q origin status || { git pull -q --rebase origin status && git push -q origin status; }; }; } ) || echo "publish failed"
  [ "$RC" != "0" ] && echo "tick exit $RC"
done
gh workflow run watchdog.yml --ref main
