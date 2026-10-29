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

    def test_failed_wake_is_retried_next_tick(self):
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

    def test_wake_pr_payload_and_public_text(self):
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [dict(REG["projects"][0], public_label="P9", wake_pr="o/r#5", wake_subscriber={"session_id": "session_X", "subscribed_at": "2026-10-28T09:00:00Z"})]}
        sent = []
        w.run_tick(src, {}, T0, lambda p: sent.append(p) or True, 1)
        self.assertEqual((sent[0]["wake_pr"], sent[0]["label"]), ("o/r#5", "P9"))
        seen = []
        orig = w.urllib.request.urlopen
        w.urllib.request.urlopen = lambda req, timeout=0: seen.append(req) or type("R", (), {"status": 201, "__enter__": lambda s: s, "__exit__": lambda s, *a: None})()
        try:
            self.assertTrue(w.fire_wake("", "")(sent[0]))
        finally:
            w.urllib.request.urlopen = orig
        self.assertTrue(seen[0].full_url.endswith("/repos/o/r/issues/5/comments"))
        self.assertNotIn(b"watchdog-test", seen[0].data)

    def test_wake_pr_needs_a_subscriber_record_not_a_recent_one(self):
        reg = dict(REG["projects"][0], wake_pr="o/r#5")
        for sub, route in ((None, "hourly_guard_only"), ({"session_id": "s", "subscribed_at": "2026-10-27T09:00:00Z"}, "pr_comment"),
                           ({"session_id": "", "subscribed_at": "2026-10-28T09:00:00Z"}, "hourly_guard_only"),
                           ({"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"}, "pr_comment")):
            src = Src([item()])
            src.f[w.REGISTRY_PATH] = {"projects": [dict(reg, wake_subscriber=sub)]}
            sent = []
            r = w.run_tick(src, {}, T0, lambda p: sent.append(p) or True, 1)
            self.assertEqual(r["projects"]["P0"]["wake_route"], route)
            self.assertEqual("wake_pr" in sent[0], route == "pr_comment")

    def test_pr_wake_unacked_after_two_ticks_then_acked(self):
        reg = dict(REG["projects"][0], wake_pr="o/r#5", wake_subscriber={"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"})
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [reg]}
        state, sent, routes = {}, [], []
        for i, ack in enumerate((False, False, False, True)):
            r = w.run_tick(src, state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(p) or True, i + 1, lambda pr, at, a=ack: a)
            routes.append(r["projects"]["P0"]["wake_route"])
        self.assertEqual(routes, ["pr_comment", "pr_comment", "pr_unacked", "pr_unacked"])   # sticky for this item

    def _pr_src(self, **k):
        reg = dict(REG["projects"][0], wake_pr="o/r#5", wake_subscriber={"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"}, **k)
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [reg]}
        return src

    def test_b1_pr_wakes_step_one_only_and_unacked_falls_back_to_runner(self):
        sent, state = [], {}
        for i in range(8):
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual([(p["step"], "wake_pr" in p) for p in sent], [(1, True), (1, False), (2, False), (3, False)])

    def test_b2_failed_comment_falls_back_same_tick_and_shows_in_feed(self):
        sent = []
        def send(p):
            sent.append(dict(p))
            return "wake_pr" not in p                      # the comment is refused, the runner works
        state = {}
        r = w.run_tick(self._pr_src(), state, T0, send, 1)
        self.assertEqual([("wake_pr" in p) for p in sent], [True, False])
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_failed")
        sent.clear()
        r = w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5), send, 2)
        self.assertEqual(sent, [])                          # step 1 is recorded, no retry loop
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_failed")

    def test_future_subscribe_time_is_rejected(self):
        e = {"wake_pr": "o/r#5", "wake_subscriber": {"session_id": "s", "subscribed_at": "2026-10-28T11:00:00Z"}}
        self.assertFalse(w.subscriber_ok(e, T0))

    def test_shared_wake_pr_is_not_used(self):
        src = self._pr_src()
        other = dict(REG["projects"][0], project="other", public_label="P8", wake_pr="o/r#5",
                     wake_subscriber={"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"})
        src.f[w.REGISTRY_PATH]["projects"].append(other)
        sent = []
        r = w.run_tick(src, {}, T0, lambda p: sent.append(dict(p)) or True, 1)
        self.assertNotIn("wake_pr", sent[0])
        self.assertEqual(r["projects"]["P0"]["wake_route"], "hourly_guard_only")

    def test_ack_only_from_owner_or_member_not_bot(self):
        orig = w.urllib.request.urlopen
        def fake(comments):
            class R:
                def __enter__(s): return s
                def __exit__(s, *a): pass
                def read(s, *a): return json.dumps(comments).encode()
            return lambda req, timeout=0: R()
        try:
            for comments, want in (([{"user": {"type": "Bot"}, "author_association": "NONE"}], False),
                                   ([{"user": {"type": "User"}, "author_association": "NONE"}], False),
                                   ([{"user": {"type": "User"}, "author_association": "OWNER", "created_at": "2026-10-28T10:01:00Z"}], True),
                                   ([{"user": {"type": "User"}, "author_association": "OWNER", "created_at": "2026-10-28T09:00:00Z"}], False)):
                w.urllib.request.urlopen = fake(comments)
                self.assertEqual(w.pr_acked("t")("o/r#5", "2026-10-28T10:00:00Z"), want)
        finally:
            w.urllib.request.urlopen = orig

    def test_b6_malformed_wake_pr_does_not_crash_the_tick(self):
        for bad in ("Alphatoris/watchdog", "x#1", "a/b#c", 5, "a/b#1\n"):
            src = self._pr_src()
            src.f[w.REGISTRY_PATH]["projects"][0]["wake_pr"] = bad
            other = dict(REG["projects"][0], project="other", public_label="P8")
            src.f[w.REGISTRY_PATH]["projects"].append(other)
            src.f["projects/other/stages/OPEN-ITEMS.json"] = {"items": [item()]}
            sent = []
            r = w.run_tick(src, {}, T0, lambda p: sent.append(dict(p)) or True, 1)
            self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_failed")
            self.assertTrue(all("wake_pr" not in p for p in sent))
            self.assertEqual(len(sent), 2)                  # both projects still woken

    def test_n16_unacked_step_one_is_sent_again_by_the_runner(self):
        sent, state = [], {}
        for i in range(5):
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual([(p["step"], "wake_pr" in p) for p in sent], [(1, True), (1, False), (2, False), (3, False)])

    def test_feed_is_counts_only_no_names_or_free_text(self):
        stage = {"stage": 2, "stage_name": "Requirements SECRETNAME", "state": "blocked", "updated": "2026-10-28T06:30:00Z",
                 "next_checkpoint": "2026-10-28T09:00:00Z", "note": "free text SECRETNOTE"}
        src = self._pr_src()
        src.f["projects/watchdog-test/stages/STAGE.json"] = stage
        src.f["projects/watchdog-test/stages/OPEN-ITEMS.json"] = {"items": [item(what="SECRETWHAT", owner="SECRETOWNER")]}
        r = w.run_tick(src, {}, T0, lambda p: True, 1)
        text = json.dumps(r)
        for secret in ("SECRETNAME", "SECRETNOTE", "SECRETWHAT", "SECRETOWNER", "Requirements", "watchdog-test"):
            self.assertNotIn(secret, text)
        words = {"other", "blocked", "done", "in_progress", "waiting_on_aamer", "paused", "pr_comment", "pr_unacked", "pr_failed",
                 "hourly_guard_only", "project_error"}
        for lab, v in r["projects"].items():
            self.assertLessEqual(set(v), {"stage", "state", "open", "late", "wake_route", "error"})
            for k, x in v.items():
                self.assertTrue(isinstance(x, int) or x is None or x in words, (lab, k, x))
        self.assertTrue(set(r) <= {"tick_at", "projects", "errors"})

    def test_bad_label_changes_the_feed_only_never_the_wake(self):
        good = REG["projects"][0]
        cases = {"missing": {}, "malformed": {"public_label": "axiomera"}, "not a string": {"public_label": 7}}
        for name, extra in cases.items():
            bad = dict(good, project="p1", **extra)
            bad.pop("public_label", None) if name == "missing" else None
            src = Src([item()])
            src.f[w.REGISTRY_PATH] = {"projects": [bad, dict(good, project="watchdog-test")]}
            src.f["projects/p1/stages/OPEN-ITEMS.json"] = {"items": [item()]}
            sent = []
            r = w.run_tick(src, {}, T0, lambda p: sent.append(dict(p)) or True, 1)
            self.assertEqual(r["projects"]["E0"]["error"], "bad_label", name)
            self.assertEqual(r["projects"]["E0"]["late"], 1, name)      # still counted
            self.assertEqual(sorted(p["project"] for p in sent), ["p1", "watchdog-test"], name)   # and still woken
            self.assertNotIn("p1", json.dumps(r["projects"]), name)     # the name is never shown
            self.assertEqual(r["errors"], 1, name)

    def test_shared_public_label_is_flagged_for_both_and_both_woken(self):
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [REG["projects"][0], dict(REG["projects"][0], project="other")]}
        src.f["projects/other/stages/OPEN-ITEMS.json"] = {"items": [item()]}
        sent = []
        r = w.run_tick(src, {}, T0, lambda p: sent.append(dict(p)) or True, 1)
        self.assertEqual(r["errors"], 2)
        self.assertEqual(sorted(r["projects"]), ["E0", "E1"])
        self.assertEqual(len(sent), 2)

    def test_state_keys_are_keyed_with_a_secret(self):
        old = {k: os.environ.pop(k, None) for k in ("STATE_SALT", "READ_KEY")}
        try:
            os.environ["READ_KEY"] = "secret-a"
            a = w.key_of("p", "i", "d")
            os.environ["READ_KEY"] = "secret-b"
            b = w.key_of("p", "i", "d")
            self.assertNotEqual(a, b)
            self.assertNotEqual(a, __import__("hashlib").sha256(b"p:i:d").hexdigest()[:16])
        finally:
            for k, v in old.items():
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v

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
        reg = {"projects": [{"project": "bad", "public_label": "PB"}, REG["projects"][0]]}
        s = Src([item()])
        s.f[w.REGISTRY_PATH] = reg
        s.f["projects/bad/stages/OPEN-ITEMS.json"] = {"items": [{"id": "x", "due": "not a time"}]}
        sent = []
        feed = w.run_tick(s, {}, T0, lambda p: sent.append(p) or True, 1)
        self.assertEqual(feed["errors"], 1)
        self.assertEqual(feed["projects"]["PB"], {"error": "project_error"})
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

    def test_published_file_is_counts_only_apart_from_run_fields(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as fx:
            for p, v in Src([item()]).f.items():
                os.makedirs(os.path.dirname(os.path.join(fx, p)), exist_ok=True)
                json.dump(v, open(os.path.join(fx, p), "w"))
            sys.argv = ["w", "--out", d, "--fixtures", fx, "--now", "2026-10-28T10:03:20Z", "--dry-run"]
            w.main()
            feed = json.load(open(os.path.join(d, "status/projects.json")))
            self.assertEqual(set(feed), {"tick_at", "tick_no", "lag_seconds_past_5min_mark",
                                         "gap_seconds_since_last_tick", "projects", "errors"})
            for entry in feed["projects"].values():
                self.assertTrue(set(entry) <= {"stage", "state", "open", "late", "wake_route", "error"})
            for banned in ("stage_name", "stage_updated", "reserved_waiting"):
                self.assertNotIn(banned, json.dumps(feed))


if __name__ == "__main__":
    unittest.main()
