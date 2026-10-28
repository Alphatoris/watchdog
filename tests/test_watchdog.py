import json, os, sys, tempfile, unittest
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import watchdog as w

T0 = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
REG = {"delivery_lead_session_id": "session_DL", "projects": [
    {"project": "watchdog-test", "public_label": "P0", "coordinator_session_id": "session_C", "watchdog_issue": "x/y#1"}]}


class Src:
    def __init__(self, items=None, stage=None):
        self.f = {w.REGISTRY_PATH: REG,
                  "projects/watchdog-test/stages/OPEN-ITEMS.json": {"items": items or []}}
        if stage:
            self.f["projects/watchdog-test/stages/STAGE.json"] = stage

    def get(self, p):
        return self.f.get(p)


def item(**k):
    d = {"id": "oi-1", "owner": "Checker", "given": "2026-10-28T09:00:00Z", "due": "2026-10-28T09:30:00Z",
         "received": "x", "done": None, "dropped": None}
    d.update(k)
    return d


def ticks(src, n, sent, start=T0, state=None):
    state = {} if state is None else state
    for i in range(n):
        w.run_tick(src, state, start + timedelta(minutes=5 * i), lambda p: sent.append(p) or True, i + 1)
    return state


class T(unittest.TestCase):
    def test_t1_one_late_item_one_wake_no_duplicate(self):
        sent = []
        ticks(Src([item()]), 2, sent)
        self.assertEqual([(p["step"], p["target"]) for p in sent], [(1, "coordinator")])

    def test_t2_ladder_once_per_step(self):
        sent = []
        ticks(Src([item()]), 8, sent)
        self.assertEqual([(p["step"], p["target"]) for p in sent],
                         [(1, "coordinator"), (2, "delivery_lead"), (3, "owner")])

    def test_t2_step_times_are_two_ticks_apart(self):
        sent, state = [], {}
        order = []
        for i in range(8):
            before = len(sent)
            w.run_tick(Src([item()]), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(p) or True, i + 1)
            if len(sent) > before:
                order.append(i + 1)
        self.assertEqual(order, [1, 3, 5])

    def test_reserved_never_climbs(self):
        sent = []
        ticks(Src([item(reserved=True), item(id="oi-2", owner="Aamer")]), 8, sent)
        self.assertEqual(sent, [])

    def test_done_item_not_chased_and_ladder_resets(self):
        sent, state = [], {}
        ticks(Src([item()]), 1, sent, state=state)
        w.run_tick(Src([item(done="x")]), state, T0 + timedelta(minutes=5), lambda p: True, 2)
        self.assertEqual(state, {})

    def test_unreceived_after_30_minutes(self):
        late = w.late_items("p", None, {"items": [item(due="2026-10-28T12:00:00Z", received=None,
                                                       given="2026-10-28T09:00:00Z")]}, T0)
        self.assertEqual(len(late), 1)

    def test_t4_quiet_nothing_sent_feed_fresh(self):
        sent, state = [], {}
        feed = None
        for i in range(12):
            feed = w.run_tick(Src([item(due="2026-10-28T23:00:00Z")]), state, T0 + timedelta(minutes=5 * i),
                              lambda p: sent.append(p) or True, i + 1)
        self.assertEqual(sent, [])
        self.assertEqual(feed["tick_at"], "2026-10-28T10:55:00Z")

    def test_t5_failed_wake_is_retried_next_tick(self):
        calls, state = [], {}
        w.run_tick(Src([item()]), state, T0, lambda p: calls.append(1) or False, 1)
        w.run_tick(Src([item()]), state, T0 + timedelta(minutes=5), lambda p: calls.append(1) or True, 2)
        self.assertEqual(len(calls), 2)

    def test_paused_project_is_shown_and_never_chased(self):
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [dict(REG["projects"][0], paused=True)]}
        sent, state = [], {}
        r = w.run_tick(src, state, T0, lambda p: sent.append(p) or True, 1)
        self.assertEqual(sent, [])
        self.assertEqual(r["projects"]["P0"]["state"], "paused")
        self.assertEqual(r["projects"]["P0"]["late"], 0)

    def test_stage_checkpoint(self):
        s = {"stage": 2, "stage_name": "Requirements", "state": "blocked", "next_checkpoint": "2026-10-28T09:00:00Z"}
        sent = []
        ticks(Src([], s), 1, sent)
        self.assertEqual(sent[0]["item"], "stage-checkpoint")
        s["state"] = "waiting_on_aamer"
        sent = []
        ticks(Src([], s), 1, sent)
        self.assertEqual(sent, [])

    def test_feed_has_no_project_names(self):
        feed = w.run_tick(Src([item()]), {}, T0, lambda p: True, 1)
        self.assertNotIn("watchdog-test", json.dumps(feed))
        self.assertEqual(list(feed["projects"]), ["P0"])

    def test_state_keys_hold_no_names(self):
        st = {}
        w.run_tick(Src([item()]), st, T0, lambda p: True, 1)
        self.assertNotIn("watchdog-test", json.dumps(st))

    def test_b1_missing_registry_fails_the_run(self):
        class Empty:
            def get(self, p):
                return None
        with self.assertRaises(w.RegistryError):
            w.run_tick(Empty(), {}, T0, lambda p: True, 1)
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as fx:
            sys.argv = ["w", "--out", d, "--fixtures", fx, "--dry-run"]
            with self.assertRaises(SystemExit) as c:
                w.main()
            self.assertEqual(c.exception.code, 1)

    def test_b2_naive_time_is_utc_and_bad_project_is_isolated(self):
        self.assertEqual(w.parse("2026-10-28T09:00:00").tzinfo, timezone.utc)
        reg = {"projects": [{"project": "bad", "public_label": "B"}, REG["projects"][0]]}
        s = Src([item()])
        s.f[w.REGISTRY_PATH] = reg
        s.f["projects/bad/stages/OPEN-ITEMS.json"] = {"items": [{"id": "x", "due": "not a time"}]}
        sent = []
        feed = w.run_tick(s, {}, T0, lambda p: sent.append(p) or True, 1)
        self.assertEqual(feed["errors"], 1)
        self.assertEqual(feed["projects"]["B"], {"error": "ValueError"})
        self.assertEqual(len(sent), 1)           # the good project still woke

    def test_b5_payload_carries_no_session_ids(self):
        sent = []
        ticks(Src([item()]), 1, sent)
        self.assertNotIn("session", json.dumps(sent[0]))

    def test_n7_state_is_a_fixed_word(self):
        self.assertEqual(w.state_word("Blocked on X"), "blocked")
        self.assertEqual(w.state_word("secret project text"), "other")

    def test_main_dry_run_writes_files_and_lag(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as fx:
            for p, v in Src([item()]).f.items():
                os.makedirs(os.path.dirname(os.path.join(fx, p)), exist_ok=True)
                json.dump(v, open(os.path.join(fx, p), "w"))
            sys.argv = ["w", "--out", d, "--fixtures", fx, "--now", "2026-10-28T10:03:20Z", "--dry-run"]
            w.main()
            feed = json.load(open(os.path.join(d, "status/projects.json")))
            self.assertEqual(feed["lag_seconds_past_5min_mark"], 200)
            self.assertTrue(os.path.exists(os.path.join(d, "ticks.jsonl")))


if __name__ == "__main__":
    unittest.main()
