import json, os, sys, tempfile, unittest
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import watchdog as w

T0 = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
REG = {"delivery_lead_session_id": "session_DL", "projects": [
    {"project": "watchdog-test", "public_label": "P0", "coordinator_session_id": "session_C", "watchdog_issue": "x/y#1"}]}


class Src:
    def __init__(self, items=None, stage=None, log=None):
        self.log = log
        self.f = {w.REGISTRY_PATH: REG,
                  "projects/watchdog-test/stages/OPEN-ITEMS.json": {"items": items or []}}
        if stage:
            self.f["projects/watchdog-test/stages/STAGE.json"] = stage

    def get(self, p):
        if p == "ledger/status/watchdog-test.json":
            return self.log
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
                         [(1, "coordinator"), (2, "delivery_lead")])

    def test_t2_step_times_are_two_ticks_apart(self):
        sent, state = [], {}
        order = []
        for i in range(8):
            before = len(sent)
            w.run_tick(Src([item()]), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(p) or True, i + 1)
            if len(sent) > before:
                order.append(i + 1)
        self.assertEqual(order, [1, 3])

    def test_time_alone_never_reaches_the_owner(self):
        sent = []
        ticks(Src([item()]), 60, sent)
        self.assertNotIn("owner", [p["target"] for p in sent])

    def test_critical_item_goes_to_the_owner_at_once_and_first(self):
        sent = []
        ticks(Src([item(severity="critical")]), 8, sent)
        self.assertEqual([(p["step"], p["target"]) for p in sent],
                         [(3, "owner"), (1, "coordinator"), (2, "delivery_lead")])
        self.assertEqual(len([p for p in sent if p["target"] == "owner"]), 1)

    def test_critical_reserved_item_still_never_climbs(self):
        sent = []
        ticks(Src([item(severity="critical", reserved=True)]), 8, sent)
        self.assertEqual(sent, [])

    def test_dl_only_item_never_reaches_the_owner_even_if_critical(self):
        src = Src([item(id=next(iter(w.DL_ONLY)), severity="critical")])
        sent = []
        ticks(src, 8, sent)
        self.assertNotIn("owner", [p["target"] for p in sent])

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
        self.assertEqual(routes, ["pr_comment", "pr_comment", "pr_unacked", "pr_comment"])   # an ack after the lost wake clears it, even with the item still open

    def _pr_src(self, **k):
        reg = dict(REG["projects"][0], wake_pr="o/r#5", wake_subscriber={"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"}, **k)
        src = Src([item()])
        src.f[w.REGISTRY_PATH] = {"projects": [reg]}
        return src

    def test_b1_pr_wakes_step_one_only_and_unacked_falls_back_to_runner(self):
        sent, state = [], {}
        for i in range(8):
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual([(p["step"], "wake_pr" in p) for p in sent], [(1, True), (1, False), (2, False)])

    def test_many_late_items_post_one_step_one_comment_per_project_per_tick(self):
        items = [item(id=f"oi-{n}") for n in range(10)]
        src = self._pr_src()
        src.f["projects/watchdog-test/stages/OPEN-ITEMS.json"] = {"items": items}
        sent, state = [], {}
        w.run_tick(src, state, T0, lambda p: sent.append(dict(p)) or True, 1)
        self.assertEqual(len([p for p in sent if "wake_pr" in p]), 1)
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(state) - len([k for k in state if not isinstance(state[k], dict) or "steps" not in state[k]]), 10)   # every item has its own ladder state
        self.assertTrue(all(v["steps"] == [1] and v["pr_wake"]["acked"] is False for v in state.values() if "steps" in v))
        w.run_tick(src, state, T0 + timedelta(minutes=5), lambda p: sent.append(dict(p)) or True, 2)
        self.assertEqual(len(sent), 1)                      # no second comment on the next tick either

    def test_many_late_items_each_still_climb_to_step_two_and_unacked_each_fall_back(self):
        items = [item(id=f"oi-{n}") for n in range(3)]
        src = self._pr_src()
        src.f["projects/watchdog-test/stages/OPEN-ITEMS.json"] = {"items": items}
        sent, state = [], {}
        for i in range(8):
            w.run_tick(src, state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual(len([p for p in sent if p["step"] == 2]), 3)
        self.assertEqual(len([p for p in sent if p["step"] == 1 and "wake_pr" in p]), 1)
        self.assertEqual(len([p for p in sent if p["step"] == 1 and "wake_pr" not in p]), 3)   # unacked: each is sent again by the runner

    def test_item_pr_unacked_clears_when_the_relay_acks_later(self):
        sent, state = [], {}
        for i in range(4):                                  # lost wake: no ack for 2 ticks, the item and the project are marked
            r = w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_unacked")
        self.assertTrue(any(v.get("pr_down") == "pr_unacked" for v in state.values()))
        r = w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=20), lambda p: sent.append(dict(p)) or True, 5, lambda pr, at: True)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_comment")      # the late item is still open, but the relay is alive again
        self.assertEqual(r["projects"]["P0"]["late"], 1)
        self.assertFalse(any(v.get("pr_down") for v in state.values() if "steps" in v))

    def test_item_pr_unacked_stays_while_nobody_has_acked(self):
        state = {}
        for i in range(6):
            r = w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: True, i + 1, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_unacked")

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
                                   ([{"user": {"type": "User"}, "author_association": "OWNER", "created_at": "2026-10-28T09:00:00Z"}], False),
                                   # the relays' agent account (association COLLABORATOR) counts, by id and login together
                                   ([{"user": {"type": "User", "id": 350444722, "login": "alphatoris-agent"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T10:01:00Z"}], True),
                                   ([{"user": {"type": "User", "id": 350444722, "login": "alphatoris-agent"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T09:00:00Z"}], False),
                                   ([{"user": {"type": "User", "id": 1, "login": "alphatoris-agent"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T10:01:00Z"}], False),
                                   ([{"user": {"type": "User", "id": 350444722, "login": "someone-else"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T10:01:00Z"}], False),
                                   ([{"user": {"type": "User", "id": 2, "login": "other-collaborator"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T10:01:00Z"}], False),
                                   ([{"user": {"type": "Bot", "id": 350444722, "login": "alphatoris-agent"}, "author_association": "COLLABORATOR", "created_at": "2026-10-28T10:01:00Z"}], False)):
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

    def test_unacked_mark_stays_on_the_feed_until_an_ack_arrives_after_it(self):
        state, sent = {}, []
        for i in range(3):                       # wake at tick 1, no ack for 2 ticks: marked at tick 3
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        quiet = self._pr_src()
        quiet.f["projects/watchdog-test/stages/OPEN-ITEMS.json"] = {"items": []}      # the item closes: nothing late
        r = w.run_tick(quiet, state, T0 + timedelta(minutes=20), lambda p: True, 5, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_unacked")             # not "healthy" while nothing is late
        r = w.run_tick(quiet, state, T0 + timedelta(minutes=25), lambda p: True, 6, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_unacked")
        r = w.run_tick(quiet, state, T0 + timedelta(minutes=30), lambda p: True, 7, lambda pr, at: True)   # the relay answers
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_comment")
        r = w.run_tick(quiet, state, T0 + timedelta(minutes=35), lambda p: True, 8, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_comment")             # and it stays cleared
        self.assertEqual(state, {})

    def test_unacked_mark_is_dropped_when_the_wake_page_leaves_the_registry(self):
        state = {}
        for i in range(3):
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: True, i + 1, lambda pr, at: False)
        plain = Src([])
        r = w.run_tick(plain, state, T0 + timedelta(minutes=20), lambda p: True, 5, lambda pr, at: False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "hourly_guard_only")
        self.assertEqual(state, {})

    def test_n16_unacked_step_one_is_sent_again_by_the_runner(self):
        sent, state = [], {}
        for i in range(5):
            w.run_tick(self._pr_src(), state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: False)
        self.assertEqual([(p["step"], "wake_pr" in p) for p in sent], [(1, True), (1, False), (2, False)])

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
                 "hourly_guard_only", "project_error", "unknown", "ok", "ahead", "unreadable"}
        for lab, v in r["projects"].items():
            self.assertLessEqual(set(v), {"stage", "state", "open", "late", "gate", "wake_route", "error"})
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

    def test_stage_ahead_of_computed_milestone_is_late(self):
        s = {"stage": 4, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        sent = []
        ticks(Src([], s, {"accepted_milestone": "M2"}), 1, sent)
        self.assertEqual([(p["item"], p["step"], p["target"]) for p in sent], [("stage-ahead-of-evidence", 2, "delivery_lead")])
        ticks(Src([], s, {"accepted_milestone": "M2"}), 8, sent)     # a longer run never reaches the coordinator or the owner
        self.assertEqual({p["target"] for p in sent}, {"delivery_lead"})

    def test_stage_covered_by_computed_milestone_is_quiet(self):
        for shown, got in ((2, "M2"), (3, "M2"), (1, "M0")):     # working in stage n with M(n-1) is normal
            sent = []
            ticks(Src([], {"stage": shown, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}, {"accepted_milestone": got}), 1, sent)
            self.assertEqual(sent, [], (shown, got))

    def test_no_status_is_quiet_unreadable_status_is_reported_none_reached_is_ahead(self):
        s = {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        sent = []
        ticks(Src([], s, None), 1, sent)
        self.assertEqual(sent, [])
        ticks(Src([], s, {"accepted_milestone": "stage 3"}), 1, sent)
        self.assertEqual([p["item"] for p in sent], ["gate-status-unreadable"])
        sent = []
        ticks(Src([], {**s, "stage": 1}, {"accepted_milestone": None}), 1, sent)
        self.assertEqual([p["item"] for p in sent], ["stage-ahead-of-evidence"])

    def test_feed_shows_gate_unknown_when_status_is_missing(self):
        s = {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        for status, want in ((None, "unknown"), ({"accepted_milestone": "M2"}, "ok"), ({"accepted_milestone": "M0"}, "ahead"),
                             ({"accepted_milestone": "stage 3"}, "unreadable")):
            feed = w.run_tick(Src([], s, status), {}, T0, lambda p: True, 1)
            self.assertEqual(feed["projects"]["P0"]["gate"], want, status)
        self.assertNotIn("M2", json.dumps(feed))                      # words only: the milestone itself is never published

    def test_feed_gate_is_never_ok_when_either_side_is_unreadable(self):
        good = {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        cases = ((None, "garbage"), (None, {}), ({"stage": "three"}, {"x": 1}), ({"stage": "three"}, {"accepted_milestone": "M2"}),
                 ({"stage": True}, {"accepted_milestone": "M2"}), (good, "garbage"), (good, {}), (good, {"accepted_milestone": 7}),
                 (None, {"accepted_milestone": "M2"}))
        for stage, status in cases:
            feed = w.run_tick(Src([], stage, status), {}, T0, lambda p: True, 1)
            self.assertEqual(feed["projects"]["P0"]["gate"], "unreadable", (stage, status))

    def test_failed_owner_send_does_not_block_steps_one_and_two(self):
        sent, state = [], {}
        def send(p):
            sent.append((p["step"], p["target"]))
            return p["step"] != 3                       # the owner send keeps failing
        for i in range(8):
            w.run_tick(Src([item(severity="critical")]), state, T0 + timedelta(minutes=5 * i), send, i + 1)
        self.assertEqual([x for x in sent if x[0] != 3], [(1, "coordinator"), (2, "delivery_lead")])
        self.assertGreater(len([x for x in sent if x[0] == 3]), 1)     # retried each tick until it goes through

    def test_feed_gate_is_unknown_for_a_paused_project(self):
        s = {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        src = Src([], s, {"accepted_milestone": "M2"})
        reg = json.loads(json.dumps(REG))                    # REG is shared by every test: never mutate it
        reg["projects"][0]["paused"] = True
        src.f[w.REGISTRY_PATH] = reg
        feed = w.run_tick(src, {}, T0, lambda p: True, 1)
        self.assertEqual(feed["projects"]["P0"]["gate"], "unknown")

    def test_cleared_log_no_longer_counts(self):
        s = {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}
        src = Src([], s, {"accepted_milestone": "M0"})
        src.get_text = lambda p: "Cleared through stage: 3\n"
        feed = w.run_tick(src, {}, T0, lambda p: True, 1)
        self.assertEqual(feed["projects"]["P0"]["late"], 1)
        self.assertNotIn("milestone", json.dumps(feed))

    def test_status_read_error_keeps_other_late_items(self):
        class Bad(Src):
            def get(self, p):
                if p.startswith("ledger/"):
                    raise OSError("boom")
                return self.f.get(p)
        sent = []
        ticks(Bad([item()], {"stage": 3, "state": "working", "next_checkpoint": "2026-10-29T09:00:00Z"}), 1, sent)
        self.assertEqual([p["item"] for p in sent], ["oi-1"])

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
                self.assertTrue(set(entry) <= {"stage", "state", "open", "late", "gate", "wake_route", "error"})
            for banned in ("stage_name", "stage_updated", "reserved_waiting"):
                self.assertNotIn(banned, json.dumps(feed))


class DLRouting(unittest.TestCase):
    """Delivery Lead list entries that name a project in "project" (playbooks rev-058 section 17 items 3 and 6)."""
    SUB = {"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"}

    def _src(self, dl_items, target_items=None, target_paused=False):
        reg = {"delivery_lead_session_id": "session_DL", "projects": [
            {"project": "delivery-lead", "public_label": "P1", "coordinator_session_id": "session_DL", "wake_pr": "o/r#1", "wake_subscriber": self.SUB},
            {"project": "watchdog-test", "public_label": "P0", "coordinator_session_id": "session_C", "wake_pr": "o/r#5", "wake_subscriber": self.SUB,
             **({"paused": True} if target_paused else {})}]}
        src = Src(target_items or [])
        src.f[w.REGISTRY_PATH] = reg
        src.f["projects/delivery-lead/stages/OPEN-ITEMS.json"] = {"items": dl_items}      # the real path, written out: not the constant
        return src

    def _run(self, src, n=8, state=None):
        sent = []
        state = {} if state is None else state
        for i in range(n):
            w.run_tick(src, state, T0 + timedelta(minutes=5 * i), lambda p: sent.append(dict(p)) or True, i + 1, lambda pr, at: True)
        return sent, state

    def test_item_naming_a_project_wakes_that_projects_wake_page(self):
        src = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")])
        sent, _ = self._run(src, 1)
        self.assertEqual([(p["item"], p["step"], p["target"], p.get("wake_pr"), p.get("label")) for p in sent],
                         [("dl:oi-9", 1, "coordinator", "o/r#5", "P0")])        # the target's page, not the Delivery Lead's page

    def test_the_item_still_climbs_to_the_delivery_lead_and_never_to_the_owner(self):
        src = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")])
        sent, _ = self._run(src, 12)
        self.assertEqual([(p["step"], p["target"]) for p in sent], [(1, "coordinator"), (2, "delivery_lead")])
        self.assertEqual({p["item"] for p in sent}, {"dl:oi-9"})            # one ladder: the Delivery Lead's own project does not chase it again

    def test_critical_goes_to_the_owner_first_other_stops_at_step_two(self):
        base = dict(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")
        sent, _ = self._run(self._src([item(**base, severity="critical")]), 8)
        self.assertEqual([(p["step"], p["target"]) for p in sent], [(3, "owner"), (1, "coordinator"), (2, "delivery_lead")])
        sent, _ = self._run(self._src([item(**base, severity="high")]), 30)
        self.assertNotIn("owner", [p["target"] for p in sent])

    def test_paused_unknown_or_missing_project_is_not_routed(self):
        late = dict(owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")
        for dl, paused in (([item(id="oi-1", project="nowhere", **late)], False), ([item(id="oi-1", project="watchdog-test", **late)], True),
                           ([item(id="oi-1", **late)], False)):
            sent, _ = self._run(self._src(dl, target_paused=paused), 1)
            self.assertFalse(any(p["item"].startswith("dl:") or p.get("label") == "P0" for p in sent), (dl, paused))
            self.assertEqual([(p["item"], p["label"] if "label" in p else None) for p in sent], [("oi-1", "P1")])   # an ordinary Delivery Lead item, as before

    def test_reserved_item_naming_a_project_is_not_routed_and_never_climbs(self):
        sent, _ = self._run(self._src([item(id="oi-9", project="watchdog-test", owner="Aamer", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")]), 8)
        self.assertEqual(sent, [])

    def test_moving_due_forward_closes_the_old_ladder(self):
        state = {}
        old = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")])
        self._run(old, 1, state)
        self.assertEqual(len([k for k, v in state.items() if "steps" in v]), 1)
        moved = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z",
                                due="2026-10-29T09:55:00Z", received="x", chased=["2026-10-28T09:55:00Z"])])
        w.run_tick(moved, state, T0 + timedelta(minutes=5), lambda p: True, 2, lambda pr, at: True)
        self.assertEqual([k for k, v in state.items() if "steps" in v], [])

    def test_same_id_in_the_projects_own_list_does_not_collide(self):
        late = dict(owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")
        src = self._src([item(id="oi-9", project="watchdog-test", **late)], target_items=[item(id="oi-9", **late)])
        sent, state = self._run(src, 1)
        self.assertEqual(len(sent), 1)                                               # one step-1 comment for the page (PR 37)
        self.assertEqual(len([k for k, v in state.items() if "steps" in v]), 2)

    def test_the_real_registry_id_and_list_path_are_used(self):
        self.assertEqual((w.DL_PROJECT, w.DL_ITEMS_PATH), ("delivery-lead", "projects/delivery-lead/stages/OPEN-ITEMS.json"))

    def test_nothing_is_routed_when_the_delivery_lead_is_not_in_the_registry(self):
        src = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")])
        src.f[w.REGISTRY_PATH]["projects"] = [e for e in src.f[w.REGISTRY_PATH]["projects"] if e["project"] != "delivery-lead"]
        sent, _ = self._run(src, 1)
        self.assertEqual(sent, [])

    def test_entry_without_a_string_id_is_skipped_and_the_target_is_not_marked_in_error(self):
        late = dict(owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")
        for bad in ({}, {"id": None}, {"id": 7}, {"id": ""}):
            src = self._src([{**item(**late), **bad, "project": "watchdog-test"}] if "id" in bad else
                            [{k: v for k, v in item(**late).items() if k != "id"} | {"project": "watchdog-test"}])
            r = w.run_tick(src, {}, T0, lambda p: True, 1, lambda pr, at: True)
            self.assertNotIn("error", r["projects"]["P0"], bad)   # the target is untouched (a malformed id in the Delivery Lead's own list is its own project's loud error, as before)

    def test_routed_item_is_counted_for_the_delivery_lead_not_the_target(self):
        src = self._src([item(id="oi-9", project="watchdog-test", owner="Coordinator", given="2026-10-28T09:55:00Z", due="2026-10-28T09:55:00Z")])
        feed = w.run_tick(src, {}, T0, lambda p: True, 1, lambda pr, at: True)["projects"]
        self.assertEqual((feed["P1"]["open"], feed["P1"]["late"]), (1, 1))
        self.assertEqual((feed["P0"]["open"], feed["P0"]["late"]), (0, 0))


class RelayProbe(unittest.TestCase):
    """Daily probe on each relay's wake PR. Probes run on the first tick of PROBE_HOUR_UTC (06:00 UTC)."""
    PROBE_T = datetime(2026, 10, 29, 6, 0, tzinfo=timezone.utc)

    def _src(self, **k):
        reg = dict(REG["projects"][0], wake_pr="o/r#5", wake_subscriber={"session_id": "s", "subscribed_at": "2026-10-28T09:00:00Z"})
        reg.update(k)
        src = Src([])
        src.f[w.REGISTRY_PATH] = {"projects": [reg]}
        return src

    def _tick(self, src, state, t, n, sent, ack=False, send_ok=True):
        def send(p):
            sent.append(dict(p))
            return send_ok
        return w.run_tick(src, state, t, send, n, lambda pr, at: ack)

    def test_probe_sent_once_at_the_probe_hour_and_not_again_the_same_day(self):
        sent, state = [], {}
        for i in range(14):                       # 06:00 to 07:05, every 5 minutes
            self._tick(self._src(), state, self.PROBE_T + timedelta(minutes=5 * i), i + 1, sent, ack=True)
        probes = [p for p in sent if p.get("step") == "probe"]
        self.assertEqual(len(probes), 1)
        self.assertEqual((probes[0]["wake_pr"], probes[0]["label"]), ("o/r#5", "P0"))

    def test_no_probe_outside_the_probe_hour(self):
        sent, state = [], {}
        self._tick(self._src(), state, datetime(2026, 10, 29, 10, 0, tzinfo=timezone.utc), 1, sent)
        self.assertEqual([p for p in sent if p.get("step") == "probe"], [])

    def test_unacked_probe_raises_one_late_item_up_the_ladder_and_shows_pr_unacked(self):
        sent, state, routes = [], {}, []
        for i in range(8):                        # probe at tick 1, never acked, ladder runs from there
            r = self._tick(self._src(), state, self.PROBE_T + timedelta(minutes=5 * i), i + 1, sent, ack=False)
            routes.append(r["projects"]["P0"]["wake_route"])
        ladder = [(p["step"], p["target"], "wake_pr" in p) for p in sent if p.get("step") != "probe"]
        self.assertEqual(ladder, [(1, "coordinator", False), (2, "delivery_lead", False)])
        self.assertEqual(routes[-1], "pr_unacked")

    def test_unacked_probe_clears_when_the_relay_acks(self):
        sent, state, routes = [], {}, []
        for i in range(3):
            r = self._tick(self._src(), state, self.PROBE_T + timedelta(minutes=5 * i), i + 1, sent, ack=False)
            routes.append(r["projects"]["P0"]["wake_route"])
        r = self._tick(self._src(), state, self.PROBE_T + timedelta(minutes=15), 4, sent, ack=True)
        routes.append(r["projects"]["P0"]["wake_route"])
        self.assertEqual(routes, ["pr_comment", "pr_comment", "pr_unacked", "pr_comment"])
        self.assertEqual([p["step"] for p in sent if p.get("step") != "probe"], [1])   # the one ladder step, then cleared

    def test_refused_probe_shows_pr_failed(self):
        sent, state = [], {}
        r = self._tick(self._src(), state, self.PROBE_T, 1, sent, ack=False, send_ok=False)
        self.assertEqual(r["projects"]["P0"]["wake_route"], "pr_failed")

    def test_paused_or_unsubscribed_project_gets_no_probe(self):
        sent, state = [], {}
        self._tick(self._src(paused=True), state, self.PROBE_T, 1, sent)
        self._tick(self._src(wake_subscriber=None), state, self.PROBE_T, 2, sent)
        self.assertEqual([p for p in sent if p.get("step") == "probe"], [])

    def test_probe_public_text_is_label_and_probe_only(self):
        sent, state = [], {}
        self._tick(self._src(), state, self.PROBE_T, 1, sent)
        probe = [p for p in sent if p.get("step") == "probe"][0]
        self.assertEqual(set(probe), {"wake_pr", "label", "step"})


if __name__ == "__main__":
    unittest.main()
