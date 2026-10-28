# watchdog

A scheduled script (GitHub Actions, every 5 minutes, no AI) that finds late work in the Alphatoris Library records and wakes whoever owns it. This repository holds only the script, its workflow, tests, and a `status` branch. It holds no project data: the status feed has counts, stage names and times under opaque labels, and the state file keys are hashes.

Secrets (repository secrets, never in files): `READ_KEY` (read-only Library key), `WAKE_URL`, `WAKE_KEY` (wake runner).

Run the tests: `python3 -m unittest -v tests/test_watchdog.py`
Dry run on fixtures: `python3 watchdog.py --out /tmp/out --fixtures <dir> --dry-run`

Unverified until live: the exact request format of the wake runner's fire endpoint (`fire_wake` in `watchdog.py`).
