# watchdog

A scheduled script (GitHub Actions, every 5 minutes, no AI) that finds late work in the Alphatoris Library records and wakes whoever owns it. This repository holds only the script, its workflow, tests, and a `status` branch. It holds no project data: the public status feed has, per opaque project id, only the stage number, a fixed state word and counts of open and late items (owner decision of 28 October 2026 16:05 UTC: counts only, no names, times or free text), and the state file keys are hashes.

Secrets (repository secrets, never in files): `READ_KEY` (read-only Library key), `WAKE_URL`, `WAKE_KEY` (wake runner).

Run the tests: `python3 -m unittest -v tests/test_watchdog.py`
Dry run on fixtures: `python3 watchdog.py --out /tmp/out --fixtures <dir> --dry-run`

Unverified until live: the exact request format of the wake runner's fire endpoint (`fire_wake` in `watchdog.py`).

## PR-comment wake (fast route)
Per project, a long-lived draft "wake page" pull request in this repository (never merged; not locked, because a locked page refuses the Actions bot; the relay acts only on comments by github-actions[bot]). At ladder step 1 the script comments `Watchdog wake: <label> step 1` there with the workflow token (`pull-requests: write`). A standing thread of the project is subscribed to that PR; it wakes in seconds, replies with the single word `ack` (never project detail; the script counts only non-bot OWNER or MEMBER comments created after the wake), and messages its coordinator. The registry entry holds `wake_pr` (`owner/repo#n`, validated), and `wake_subscriber` (`session_id`, `subscribed_at`, not in the future; there is no age limit, a dead or replaced relay is caught by the acknowledgement check). After a lost wake the project keeps `wake_route` `pr_unacked` on the feed, even when nothing is late, until a non-bot owner or member comment appears on the wake page after the lost wake (a relay that re-subscribes can post `ack` to clear it). No ack within 2 ticks, a refused comment, a malformed `wake_pr` or a shared one: the item falls back to the wake runner and the hourly guard, and the feed shows `wake_route` `pr_unacked`, `pr_failed` or `hourly_guard_only`; while the fast route is healthy it shows `pr_comment`. Steps 2 and 3 never use the PR.

A project whose `public_label` is missing, malformed or shared with another is still counted and woken; the feed shows it under a neutral index (`E0`, `E1`) with `error: bad_label`, and the hourly guard lists it for the coordinator. State keys are HMAC-SHA256 keyed with `READ_KEY` (or `STATE_SALT` if set), so they cannot be guessed from a project name.
