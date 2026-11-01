#!/usr/bin/env python3
"""Coordinator watchdog. No AI. Reads the Library records, decides what is late,
wakes through the wake runner, writes a status feed. Standard library only.

Rules (design v2 sections 8 and 11): an open item past `due`, or with no `received`
30 minutes after `given`, or a stage checkpoint passed, is late. Step 1 wakes the
coordinator; step 2 the Delivery Lead after 2 more ticks (a comment on the Delivery Lead's wake page). Step 3, the owner list and
phone push, is by severity and not by time (playbooks rev-055, cycle rules section 21 item 3):
only an item marked `"severity": "critical"` reaches the owner, at once. Each step once.
Reserved items never climb the ladder.
A Delivery Lead list entry with "project" naming a registered, unpaused project is chased as a late item of that project
(state key prefixed "dl:"), so its step 1 goes to that project's wake PR (playbooks rev-058 section 17 items 3 and 6).
The public repo holds no project data: state keys are keyed hashes (HMAC), and the
feed holds, per project, only a stage number, a state word, counts of open and late
items and a wake-route word, under opaque labels. The only times in the published
file are the run's own (tick_at, tick_no, lag, gap).
"""
import argparse, base64, hashlib, hmac, json, os, re, sys, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

LIBRARY = "Alphatoris/Alphatoris-Library"
REGISTRY_PATH = "projects/alphatoris-wide/stages/REGISTRY.json"
DL_PROJECT = "delivery-lead"              # the Delivery Lead's registry id; its list is projects/<id>/stages/OPEN-ITEMS.json
DL_ITEMS_PATH = f"projects/{DL_PROJECT}/stages/OPEN-ITEMS.json"
RECEIVE_GRACE = timedelta(minutes=30)
STEP2_TICKS = 2                          # ticks after first detection
ACK_WINDOW_SECONDS = 120                 # a wake comment (step 1 on a project page, step 2 on the Delivery Lead page) not acknowledged within this long is lost; relays answer in 9 to 21 s
CLOSED_STAGE_STATES = {"done", "closed", "complete", "waiting_on_aamer", "waiting on aamer"}


def parse(t):
    if not t:
        return None
    d = datetime.fromisoformat(str(t).split(" ")[0].replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)   # a time with no zone is read as UTC


def iso(d):
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def key_of(project, item, due):
    """State key for the public state file: keyed with a secret, so a guess of project, item and due time cannot be tested."""
    secret = (os.environ.get("STATE_SALT") or os.environ.get("READ_KEY") or "").encode()
    return hmac.new(secret, f"{project}:{item}:{due}".encode(), hashlib.sha256).hexdigest()[:16]


LABEL_RE = re.compile(r"P[A-Z0-9]{1,5}")


def label_of(entry, idx, labels):
    """(label, bad). The public feed shows only public_label; a missing, malformed or shared label never stops the project
    being woken: it is shown under a neutral index and flagged (counts only, Aamer 16:05Z; never a hash of the project name)."""
    lab = entry.get("public_label")
    if isinstance(lab, str) and LABEL_RE.fullmatch(lab) and labels.count(lab) == 1:
        return lab, False
    return f"E{idx}", True


# ---- sources -------------------------------------------------------------
class GitHubSource:
    def __init__(self, token, repo=LIBRARY):
        self.token, self.repo = token, repo
        self.rate = None                              # (remaining, limit) from the last response, for the run log
        self.ref = "main"                             # replaced by main's commit sha for one pass (see pin)

    def pin(self):
        """Read every file of this pass at main's current commit sha. A read by branch name can be served from a cache up to a minute old,
        which made a new item invisible to a one-minute pass; a sha names one exact tree and is never stale. On any failure the name stays."""
        req = urllib.request.Request(f"https://api.github.com/repos/{self.repo}/git/ref/heads/main",
                                     headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
                                              "X-GitHub-Api-Version": "2022-11-28", "Cache-Control": "no-cache"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                self._note_rate(r)
                sha = json.load(r).get("object", {}).get("sha", "")
            self.ref = sha if re.fullmatch(r"[0-9a-f]{40}", str(sha)) else "main"
        except Exception as ex:
            self.ref = "main"
            print(f"could not pin main ({type(ex).__name__}): reading by branch name", file=sys.stderr)

    def _note_rate(self, r):
        try:
            self.rate = (int(r.headers.get("X-RateLimit-Remaining")), int(r.headers.get("X-RateLimit-Limit")))
        except (TypeError, ValueError):
            pass

    def get(self, path):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{self.repo}/contents/{path}?ref={self.ref}",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github.raw+json",
                     "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                self._note_rate(r)
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise


    def get_text(self, path):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{self.repo}/contents/{path}?ref={self.ref}",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github.raw+json",
                     "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                self._note_rate(r)
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise


class DirSource:
    def __init__(self, root):
        self.root = root

    def get_text(self, path):
        p = os.path.join(self.root, path)
        return open(p, encoding="utf-8").read() if os.path.exists(p) else None

    def get(self, path):
        p = os.path.join(self.root, path)
        return json.load(open(p)) if os.path.exists(p) else None


# ---- decisions -----------------------------------------------------------
NOT_RESERVED_OWNERS = ("aamer wealth intelligence", "aamer os")   # project names that start with the owner's first name


def is_reserved(it):
    """An item is reserved (Aamer's own decision: never chased) if it says reserved, or its owner names Aamer. Fail closed: any owner containing
    "aamer" is reserved, except an owner that starts with the name of a project (Aamer Wealth Intelligence, Aamer OS) unless reserved is set."""
    if it.get("reserved"):
        return True
    owner = str(it.get("owner", "")).lower().strip()
    if "aamer" not in owner:
        return False
    for name in NOT_RESERVED_OWNERS:
        # the project name must end there: "Aamer Oscar" is not "Aamer OS"
        if owner.startswith(name) and not owner[len(name):len(name) + 1].isalpha():
            return False
    return True


CHECKPOINT_ITEM = "checkpoint-missed:"          # + the item id: an item's own checkpoint passed with nobody moving it forward
CHECKPOINT_ENTRY = "registry-checkpoint-missed"  # a registry entry's next_checkpoint passed


def dl_only(item):
    """Late items that go to the Delivery Lead only: never the coordinator, never the owner."""
    return item in DL_ONLY or item == CHECKPOINT_ENTRY or str(item).startswith(CHECKPOINT_ITEM)


def late_items(project, stage, items, now, checkpoints=True):
    """Return list of (item_id, due_iso, reserved) that are late. An open item may carry an optional "checkpoint" time (the owner
    moves it forward at each check-in); once it has passed, a Delivery Lead only late item reports the miss, keyed by that time."""
    out = []
    for it in (items or {}).get("items", []):
        if it.get("done") or it.get("dropped"):
            continue
        if checkpoints and it.get("checkpoint"):
            try:
                cp = parse(it.get("checkpoint"))
            except ValueError:
                cp = None
                print("an item has a checkpoint that is not a time: ignored", file=sys.stderr)
            if cp is not None and cp < now:
                out.append((CHECKPOINT_ITEM + str(it["id"]), it["checkpoint"], is_reserved(it)))
        due = parse(it.get("due"))
        given = parse(it.get("given"))
        past_due = due is not None and due < now
        unreceived = (not it.get("received")) and given is not None and given + RECEIVE_GRACE < now
        if past_due or unreceived:
            when = it.get("due") or iso(given + RECEIVE_GRACE)
            reserved = is_reserved(it)
            out.append((it["id"], when, reserved))
    if stage:
        cp = parse(stage.get("next_checkpoint"))
        st = str(stage.get("state", "")).lower()
        if cp and cp < now and st not in CLOSED_STAGE_STATES and not stage.get("stage_ended"):
            out.append(("stage-checkpoint", stage["next_checkpoint"], False))
    return out


def route_dl_items(dl_items, entries, now):
    """Delivery Lead list entries that name a registered, unpaused project in "project" (playbooks rev-058 section 17 items 3 and 6;
    design a2). Returns {target_project_id: [item, ...]}. Such an item is chased as a late item of THAT project: step 1 to its wake PR,
    step 2 to the Delivery Lead, step 3 only if severity is critical. Reserved items, items without "project", and items naming an
    unknown or paused project (stderr note) are not routed and stay ordinary Delivery Lead items."""
    live = {e["project"]: e for e in entries if isinstance(e, dict) and e.get("project")}
    routed = {}
    if DL_PROJECT not in live:
        return routed                             # the Delivery Lead is not in the registry: nothing to route from
    for it in (dl_items or {}).get("items", []):
        tgt = it.get("project") if isinstance(it, dict) else None
        if not tgt or it.get("done") or it.get("dropped"):
            continue
        if not isinstance(it.get("id"), str) or not it["id"]:
            print("delivery lead item naming a project has no string id: not routed", file=sys.stderr)
            continue
        e = live.get(tgt)
        if e is None or e.get("paused"):
            print(f"delivery lead item names an unknown or paused project: not routed", file=sys.stderr)
            continue
        if tgt == DL_PROJECT:
            continue                              # the Delivery Lead's own project: an ordinary item
        if is_reserved(it):
            continue                              # reserved never climbs
        routed.setdefault(tgt, []).append(it)
    return routed


DL_ONLY = ("stage-ahead-of-evidence", "gate-status-unreadable")   # go to the Delivery Lead only: never the coordinator, never the owner
MILESTONE_RE = re.compile(r"M([0-6])")


def milestone_late(stage, status):
    """Evidence gate (playbooks rev-051, cycle rules section 18): report-only. STAGE.json describes current work and grants nothing;
    the gate writes the computed milestone to ledger/status/<project>.json. A stage shown more than one ahead of the computed milestone (working in stage n needs M(n-1)) is reported
    to the Delivery Lead, who coordinates the missing evidence. No status file (gate not enabled or project not configured): nothing
    to compare. A status file without a readable milestone is reported. Never blocks or edits anything."""
    if status is None or not stage:
        return []
    shown = stage.get("stage")
    if isinstance(shown, bool) or not isinstance(shown, int):
        return []
    if not isinstance(status, dict) or "accepted_milestone" not in status:
        return [("gate-status-unreadable", "", False)]
    got = status["accepted_milestone"]
    m = MILESTONE_RE.fullmatch(got) if isinstance(got, str) else None
    if got is not None and m is None:
        return [("gate-status-unreadable", "", False)]
    reached = int(m.group(1)) if m else -1    # None: not even M0
    if shown > reached + 1:                     # working in stage n needs M(n-1); further ahead has no evidence under it
        return [("stage-ahead-of-evidence", "", False)]
    return []


def gate_word(status, gate_late, stage=None):
    """Public feed word for the evidence gate: unknown (no status file), unreadable, ahead, or ok. Words only, no milestone.
    ok is only said when both sides are readable: a status file with a valid accepted_milestone and a stage that is an int."""
    if status is None:
        return "unknown"
    got = status.get("accepted_milestone") if isinstance(status, dict) else None
    shown = stage.get("stage") if isinstance(stage, dict) else None
    if not isinstance(status, dict) or "accepted_milestone" not in status \
            or (got is not None and not (isinstance(got, str) and MILESTONE_RE.fullmatch(got))) \
            or isinstance(shown, bool) or not isinstance(shown, int):
        return "unreadable"
    kinds = {l[0] for l in gate_late}
    return "unreadable" if "gate-status-unreadable" in kinds else "ahead" if "stage-ahead-of-evidence" in kinds else "ok"


STATE_WORDS = (("waiting_on_aamer", ("waiting on aamer", "waiting_on_aamer")), ("blocked", ("block",)),
               ("done", ("done", "closed", "complete")), ("in_progress", ("progress", "active", "working", "open")))


def state_word(s):
    s = str(s or "").lower()
    for word, keys in STATE_WORDS:
        if any(k in s for k in keys):
            return word
    return "other"


class RegistryError(Exception):
    pass


def run_tick(source, state, now, send, tick_no, acked=None, fast=False, sent_log=None):
    registry = source.get(REGISTRY_PATH)
    if not isinstance(registry, dict) or not isinstance(registry.get("projects"), list):
        raise RegistryError("REGISTRY.json missing or malformed")   # fails the run: never a green empty feed
    entries = list(registry["projects"])
    if sent_log is not None:                    # counts-only record of every send, so a runner resend shows up in public data
        labmap, raw_send = {}, send

        def send(payload):
            ok = raw_send(payload)
            if payload.get("step") == 3:
                return ok                       # the owner alert is never in public data: even an unlabelled entry would show that a critical item exists
            lab_ = payload.get("for_label") or payload.get("label") or labmap.get(payload.get("project"))
            step_ = payload.get("step")
            sent_log.append({"label": lab_ if isinstance(lab_, str) and LABEL_RE.fullmatch(lab_) else None,
                             "step": step_ if isinstance(step_, (int, str)) else None,
                             "route": ("dl_page" if payload.get("for_label") else "pr_comment") if payload.get("wake_pr") else "runner",
                             "runner_fired": not payload.get("wake_pr"), "ok": bool(ok)})
            return ok
    else:
        labmap = {}
    prs = [x.get("wake_pr") for x in entries if isinstance(x, dict) and x.get("wake_pr")]
    dup = {p for p in prs if prs.count(p) > 1}              # one wake PR per project: a shared one is not used
    stamp = tick_no + 1 if fast else tick_no      # a fast pass runs between ticks: what it starts is counted from the NEXT tick, so no window is shorter than designed
    feed, seen, errors = {}, set(), 0
    commented = {}                              # project id -> the step-1 PR comment already posted this tick
    commented2 = {}                             # project id -> the step-2 comment on the Delivery Lead's page already posted this tick
    try:
        routed = route_dl_items(source.get(DL_ITEMS_PATH), entries, now)   # Delivery Lead requests addressed to a project
    except Exception as ex:
        routed = {}
        print(f"delivery lead list read error: {type(ex).__name__}: {ex}", file=sys.stderr)
    routed_ids = {it["id"] for its in routed.values() for it in its if "id" in it}
    labels = [x.get("public_label") for x in entries if isinstance(x, dict)]
    for idx, e in enumerate(entries):
        try:
            lab, bad_label = label_of(e, idx, labels)
        except Exception:                        # an entry that is not even a mapping
            lab, bad_label = f"E{idx}", True
        try:
            pid = e["project"]
            labmap[pid] = lab
            stage = source.get(f"projects/{pid}/stages/STAGE.json")
            items = source.get(f"projects/{pid}/stages/OPEN-ITEMS.json")
            paused = bool(e.get("paused"))      # owner-paused project: shown, never chased
            late = [] if paused else late_items(pid, stage, items, now)
            own_dl = pid == DL_PROJECT
            dl_routed = [("dl:" + i, d, r) for i, d, r in late_items(pid, None, {"items": routed.get(pid, [])}, now, checkpoints=False)] if not paused else []
            dl_req = {"dl:" + str(i.get("id")): wake_request(i) for i in routed.get(pid, [])} if not paused else {}   # ids only
            late_ladder = [l for l in late if not (own_dl and l[0] in routed_ids)] + dl_routed   # a routed item climbs under its target, never twice
            if not paused and e.get("next_checkpoint"):          # a registry entry's own checkpoint: the entry owner moves it forward
                try:
                    cp_e = parse(e["next_checkpoint"])
                except ValueError:
                    cp_e = None
                    print("a registry entry has a next_checkpoint that is not a time: ignored", file=sys.stderr)
                if cp_e is not None and cp_e < now:
                    late.append((CHECKPOINT_ENTRY, e["next_checkpoint"], False))
                    late_ladder.append((CHECKPOINT_ENTRY, e["next_checkpoint"], False))
            gate = "unknown"                    # no status file, a paused project or a failed read: the gate is not known, not silent
            if not paused:
                try:
                    status = source.get(f"ledger/status/{pid}.json")
                    gate_late = milestone_late(stage, status)
                    late += gate_late
                    late_ladder += gate_late
                    gate = gate_word(status, gate_late, stage)
                except Exception as ex:          # a failed status read must not drop the project's other late items
                    print(f"gate status read error: {type(ex).__name__}: {ex}", file=sys.stderr)
        except Exception as ex:                  # one bad project never stops the others
            errors += 1
            feed[lab] = {"error": "project_error"}
            print(f"project error: {type(ex).__name__}: {ex}", file=sys.stderr)
            continue
        critical = {i.get("id") for i in (items or {}).get("items", []) if i.get("severity") == "critical"}
        critical |= {"dl:" + i.get("id") for i in routed.get(pid, []) if i.get("severity") == "critical"}
        openn = len([i for i in (items or {}).get("items", []) if not i.get("done") and not i.get("dropped")])
        stage_no = (stage or {}).get("stage")      # public feed (Aamer, 28 Oct 16:05 UTC): counts only, no names or free text
        feed[lab] = {"stage": stage_no if isinstance(stage_no, int) and not isinstance(stage_no, bool) else None,
                             "state": "paused" if paused else state_word((stage or {}).get("state")), "open": openn,
                             "late": len([l for l in late if not l[2]]),
                             "gate": gate,
                             "wake_route": ("pr_failed" if not valid_pr(e.get("wake_pr")) else "pr_comment" if subscriber_ok(e, now) and e.get("wake_pr") not in dup else "hourly_guard_only") if e.get("wake_pr") else "hourly_guard_only"}
        pk = key_of(pid, "route", "")            # per-project mark: a lost wake keeps the feed off "healthy" until an ack arrives after it
        mark = state.get(pk)
        if mark and not valid_pr(e.get("wake_pr")):
            del state[pk]                        # the wake page is gone from the registry: the mark has no meaning
            mark = None
        if mark and acked and not fast and acked(e["wake_pr"], mark["since"]):   # a lost-wake mark and the daily probe are judged on ticks only: a down relay costs no reads between ticks
            del state[pk]                        # the relay answered after the lost wake: it is alive again
            mark = None
        if mark:
            seen.add(pk)
            if feed[lab]["wake_route"] == "pr_comment":
                feed[lab]["wake_route"] = "pr_unacked"
        probe_late = relay_probe(pid, e, lab, state, seen, now, tick_no, send, (acked if not fast else None) or (lambda pr, at: False), feed, stamp)
        if probe_late:
            late.append(probe_late)
            late_ladder.append(probe_late)
        if bad_label:
            errors += 1                          # loud for the coordinator (guard lists projects with an error key)
            feed[lab]["error"] = "bad_label"
            print(f"bad public_label for entry {idx}: shown as {lab}", file=sys.stderr)
        if e.get("wake_pr") and not valid_pr(e.get("wake_pr")):
            print(f"bad wake_pr for {lab}: ignored", file=sys.stderr)
        for item, due, reserved in late_ladder:
            if reserved:
                continue                       # never climbs the ladder
            k = key_of(pid, item, due)
            seen.add(k)
            s = state.setdefault(k, {"first_tick": stamp, "steps": []})
            age = max(0, tick_no - s["first_tick"])
            if s.get("pr_down") == "pr_unacked" and item != "relay-probe" and pk not in state:
                del s["pr_down"]               # the project's lost-wake mark was cleared by an ack: the item's own copy must not keep the feed at pr_unacked
            if item == "relay-probe":
                s["pr_down"] = feed[lab]["wake_route"]   # the relay is the thing that is down: the ladder never uses its PR
            pw = s.get("pr_wake")              # a PR-comment wake must be acknowledged by the subscriber
            if pw and not pw["acked"] and acked:
                if acked(pw["pr"], pw["at"]):
                    pw["acked"] = True
                elif (now - parse(pw["at"])).total_seconds() >= ACK_WINDOW_SECONDS:
                    s["pr_down"] = "pr_unacked"                      # lost wake: later steps use the ladder's other route
                    state[pk] = {"since": pw["at"]}                  # and the project stays marked after this item closes
                    seen.add(pk)
                    s["steps"] = [x for x in s["steps"] if x != 1]   # and step 1 is sent again by the wake runner
                    del s["pr_wake"]
            dw = s.get("dl_wake")              # a step-2 comment on the Delivery Lead's page must be acknowledged too
            if dw and not dw["acked"] and acked:
                if acked(dw["pr"], dw["at"]):
                    dw["acked"] = True
                elif (now - parse(dw["at"])).total_seconds() >= ACK_WINDOW_SECONDS:
                    s["dl_down"] = True                              # the Delivery Lead's relay did not answer: step 2 is sent again by the wake runner
                    s["steps"] = [x for x in s["steps"] if x != 2]
                    del s["dl_wake"]
            if s.get("pr_down"):
                feed[lab]["wake_route"] = s["pr_down"]
            # step 2 follows at once when the project's own relay did not answer step 1 (the Delivery Lead must hear within about 5 minutes); otherwise it waits its 2 ticks
            steps = ((2, 0),) if dl_only(item) else ((1, 0), (2, 0 if s.get("pr_down") == "pr_unacked" else STEP2_TICKS))
            if item in critical and not dl_only(item):
                steps = ((3, 0),) + steps     # critical security, data or major failure: the owner at once; time alone never reaches him
            for step, need in steps:
                if step in s["steps"] or age < need:
                    continue
                target = {1: "coordinator", 2: "delivery_lead", 3: "owner"}[step]
                payload = {"project": pid, "item": item, "due": due, "step": step, "target": target,
                           "sent_at": iso(now)}     # no session ids: the runner reads them from the registry only
                if e.get("wake_pr") and subscriber_ok(e, now) and step == 1 and not s.get("pr_down") and e.get("wake_pr") not in dup:   # the PR wakes the coordinator only; steps 2 and 3 use the wake runner
                    payload["wake_pr"] = e["wake_pr"]   # "owner/repo#n": a subscribed session is woken by a comment
                    payload["label"] = lab
                    reqs = [dl_req[x[0]] for x in dl_routed if dl_req.get(x[0])
                            and 1 not in state.get(key_of(pid, x[0], x[1]), {}).get("steps", [])]   # each request is named once: only items whose step 1 is not yet sent
                    if reqs:
                        payload["request"] = ",".join(reqs[:8])      # whole ids only, never cut inside one
                    if pid in commented:       # one step-1 comment per project per tick: the relay wakes the coordinator once either way
                        s["steps"].append(1)
                        s["pr_wake"] = dict(commented[pid])   # same comment, so each item is still acknowledged on its own
                        break
                page = dl_step2_page(entries, dup, state, now) if step == 2 and pid != DL_PROJECT and not s.get("dl_down") else None
                if page:                                               # step 2: a bot comment on the Delivery Lead's wake page, not the runner
                    if pid in commented2:                              # one step-2 comment per project per tick
                        s["steps"].append(2)
                        s["dl_wake"] = dict(commented2[pid])
                        break
                    payload.update(wake_pr=page[0], label=page[1], for_label=lab)
                    ok = send(payload)
                    if not ok:                                         # comment refused: same step by the runner, this tick
                        for k_ in ("wake_pr", "label", "for_label"):
                            payload.pop(k_, None)
                        s["dl_down"] = True
                        ok = send(payload)
                    if ok:
                        s["steps"].append(2)
                        if payload.get("wake_pr"):
                            s["dl_wake"] = {"tick": stamp, "at": iso(now), "pr": page[0], "acked": False}
                            commented2[pid] = s["dl_wake"]
                    break
                ok = send(payload)
                if not ok and payload.get("wake_pr"):                # the comment could not be posted (for example no permission)
                    s["pr_down"] = "pr_failed"
                    feed[lab]["wake_route"] = "pr_failed"
                    payload.pop("wake_pr"); payload.pop("label", None); payload.pop("request", None)
                    ok = send(payload)                               # same step, by the existing route, in this tick
                if ok:
                    s["steps"].append(step)
                    if payload.get("wake_pr"):
                        s["pr_wake"] = {"tick": stamp, "at": iso(now), "pr": payload["wake_pr"], "acked": False}
                        commented[pid] = s["pr_wake"]
                if step != 3:
                    break                      # one step per tick; the owner send (3) is separate and never blocks steps 1 and 2
    for k in list(state):                      # closed or re-dated items drop out
        if k not in seen:
            del state[k]
    return {"tick_at": iso(now), "projects": feed, "errors": errors}


PR_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+$")


def dl_step2_page(entries, dup, state, now):
    """(wake_pr, label) of the Delivery Lead's wake page when step 2 can use it: the registry names it, it has a live subscriber,
    it is not shared, the Delivery Lead is not paused, and neither its route mark nor its daily probe says its relay is down.
    Otherwise None and step 2 uses the wake runner as before. The session id behind the runner can go stale; the page cannot."""
    e = next((x for x in entries if isinstance(x, dict) and x.get("project") == DL_PROJECT), None)
    if not e or e.get("paused") or not valid_pr(e.get("wake_pr")) or e["wake_pr"] in dup or not subscriber_ok(e, now):
        return None
    if state.get(key_of(DL_PROJECT, "route", "")) or (state.get(key_of(DL_PROJECT, "probe", "")) or {}).get("down"):
        return None
    lab = e.get("public_label")
    return (e["wake_pr"], lab) if isinstance(lab, str) and LABEL_RE.fullmatch(lab) else None


def valid_pr(x):
    return isinstance(x, str) and bool(PR_RE.fullmatch(x))
PR_ACK_TICKS = 2
# Relays now comment as the agent account, which GitHub reports as COLLABORATOR. Its id AND login must both match (the id
# cannot be spoofed by renaming, the login catches a reused id in a fixture), so no other collaborator counts.
ACK_AGENTS = ((350444722, "alphatoris-agent"),)
PROBE_HOUR_UTC = 6                       # once a day, on the first tick of this hour, each relay gets a probe
PROBE_GAP = timedelta(hours=20)


def relay_probe(pid, e, lab, state, seen, now, tick_no, send, acked, feed, stamp=None):
    """Once a day, comment a probe on a project's wake PR. A probe with no ack within PR_ACK_TICKS ticks marks the relay
    down: the feed shows pr_unacked (or pr_failed when the comment was refused), and one late item, relay-probe, goes up
    the ladder (wake runner to the coordinator, then the Delivery Lead, then the owner). It clears on the next ack.
    Returns the late item to add, or None. Public text is the label and the word probe only."""
    if not (subscriber_ok(e, now) and e.get("wake_pr") and not e.get("paused")):
        return None
    pk = key_of(pid, "probe", "")
    p = state.get(pk, {})
    if (p.get("pending") or p.get("down")) and acked(e["wake_pr"], p["at"]):
        p["pending"], p["down"] = False, None        # the relay answered the probe, or answered late: it is alive
    elif p.get("pending") and tick_no - p["tick"] >= PR_ACK_TICKS:
        p["pending"], p["down"] = False, "pr_unacked"
    elif not p.get("pending") and not p.get("down") and now.hour == PROBE_HOUR_UTC \
            and (not p.get("last") or now - parse(p["last"]) >= PROBE_GAP):
        p.update(last=iso(now), tick=tick_no if stamp is None else stamp, pending=True, at=iso(now))
        if not send({"wake_pr": e["wake_pr"], "label": lab, "step": "probe"}):
            p.update(pending=False, down="pr_failed")
    if p:
        state[pk] = p
        seen.add(pk)
    else:
        state.pop(pk, None)
    if p.get("down"):
        feed[lab]["wake_route"] = p["down"]
        return ("relay-probe", p["last"], False)
    return None


def subscriber_ok(e, now):
    """The wake PR is used only while the registry names a subscriber session with a subscribe time that is not in the
    future. There is no age limit: the script cannot see GitHub's subscription itself, so a dead or replaced relay is
    caught by the acknowledgement check (no ack within PR_ACK_TICKS falls back to the wake runner and the hourly guard
    and the feed says pr_unacked), not by the age of the record."""
    sub = e.get("wake_subscriber") or {}
    t = parse(sub.get("subscribed_at"))
    return bool(valid_pr(e.get("wake_pr")) and sub.get("session_id") and t and now >= t)   # a future time is rejected


REQ_ID_RE = re.compile(r"dl-[0-9]{1,6}")


def wake_request(item):
    """The neutral public id of a routed Delivery Lead request: the entry's "wake_id", which must look like dl-<digits>. The
    descriptive id and the text stay in the Library list (the wake page is public and the label is tied to a project there).
    None for any other value: such an entry is woken without naming a request."""
    if not isinstance(item, dict) or not isinstance(item.get("wake_id"), str) or not REQ_ID_RE.fullmatch(item["wake_id"]):
        return None
    return item["wake_id"]


def comment_wake(wake_pr, label, step, token, for_label=None, request=None):
    """Comment on a wake pull request; a subscribed session wakes in seconds. Public text: opaque labels and the step only.
    A step-2 comment on the Delivery Lead's page adds the label of the project that is late: "step 2 (for P4)"."""
    repo, num = wake_pr.rsplit("#", 1)
    tail = f" (for {for_label})" if isinstance(for_label, str) and LABEL_RE.fullmatch(for_label) else ""
    req_txt = ""
    if isinstance(request, str) and step == 1 and all(REQ_ID_RE.fullmatch(x) for x in request.split(",")):
        req_txt = (" | Delivery Lead request " + request     # request ids only; the text stays in the Library list
                   + ". Relay: after ack, message your coordinator: act now on Delivery Lead request " + request
                   + " (Library projects/delivery-lead/stages/OPEN-ITEMS.json, entry with that wake_id; no wait for Aamer); report on this page with counts and labels only")
    body = json.dumps({"body": f"Watchdog wake: {label} step {step}{tail}{req_txt}"}).encode()
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/issues/{int(num)}/comments", data=body,
                                 method="POST", headers={"Authorization": f"Bearer {token}",
                                                         "Accept": "application/vnd.github+json",
                                                         "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return 200 <= r.status < 300
    except Exception as ex:
        print(f"comment wake failed: {ex}", file=sys.stderr)
        return False


def is_ack(c, since_iso):
    """A wake page comment is an acknowledgement if it is non-bot, created after the wake, and written by the owner or a
    member or by a configured agent account (ACK_AGENTS: id and login both match)."""
    u = c.get("user") or {}
    if u.get("type") == "Bot" or c.get("created_at", "") < since_iso:
        return False
    return c.get("author_association") in ("OWNER", "MEMBER") or (u.get("id"), u.get("login")) in ACK_AGENTS


def pr_acked(token):
    """True if a person or session (not the Actions bot) commented on the wake PR after the wake; None-safe on errors."""
    def acked(wake_pr, since_iso):
        repo, num = wake_pr.rsplit("#", 1)
        req = urllib.request.Request(f"https://api.github.com/repos/{repo}/issues/{int(num)}/comments?since={since_iso}",
                                     headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return any(is_ack(c, since_iso) for c in json.load(r))
        except Exception as ex:
            print(f"ack check failed: {ex}", file=sys.stderr)
            return False
    return acked


def fire_wake(url, key):
    def send(payload):
        if payload.get("wake_pr"):
            return comment_wake(payload["wake_pr"], payload["label"], payload["step"], os.environ.get("GH_TOKEN", ""), payload.get("for_label"), payload.get("request"))
        if not url:
            print("WAKE_URL not set; not sending", file=sys.stderr)
            return False
        body = json.dumps({"text": json.dumps(payload)}).encode()
        req = urllib.request.Request(url, data=body, method="POST", headers={
            "Authorization": f"Bearer {key}", "Content-Type": "application/json",
            "anthropic-version": "2023-06-01", "anthropic-beta": "experimental-cc-routine-2026-04-01"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return 200 <= r.status < 300
        except Exception as ex:
            print(f"wake failed: {ex}", file=sys.stderr)
            return False
    return send


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="status-out", help="directory holding the status branch checkout")
    ap.add_argument("--fixtures", help="read records from a directory instead of GitHub")
    ap.add_argument("--now")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fast", action="store_true", help="between 5-minute ticks: look for new late items and send what is due, "
                    "but do not count a tick, move the ladder clock, or publish the feed")
    a = ap.parse_args()
    now = parse(a.now) if a.now else datetime.now(timezone.utc)
    src = DirSource(a.fixtures) if a.fixtures else GitHubSource(os.environ["READ_KEY"])
    if hasattr(src, "pin"):
        src.pin()
    sdir = os.path.join(a.out, "status")
    os.makedirs(sdir, exist_ok=True)
    sp = os.path.join(a.out, "state.json")
    st = json.load(open(sp)) if os.path.exists(sp) else {"tick_no": 0, "items": {}}
    before = json.dumps(st, sort_keys=True)
    if not a.fast:
        st["tick_no"] += 1                      # a fast pass keeps the 5-minute tick number: every ladder timing counts these ticks
    send = (lambda p: print("DRY-RUN wake", json.dumps(p)) or True) if a.dry_run else \
        fire_wake(os.environ.get("WAKE_URL"), os.environ.get("WAKE_KEY"))
    sent = []
    try:
        feed = run_tick(src, st["items"], now, send, st["tick_no"], pr_acked(os.environ.get("GH_TOKEN", "")), a.fast, sent_log=sent)   # acks are read only for a wake that is still waiting, so a quiet pass costs no extra calls
    except RegistryError as ex:
        print(f"FAILED: {ex}", file=sys.stderr)
        sys.exit(1)
    if getattr(src, "rate", None):
        print(f"read key rate limit: {src.rate[0]} of {src.rate[1]} left ({'fast' if a.fast else 'tick'} pass)", file=sys.stderr)
    if a.fast:
        if sent:
            st["sent_pending"] = (st.get("sent_pending") or []) + [dict(x, **{"pass": "fast"}) for x in sent]   # logged on the next tick line
        if json.dumps(st, sort_keys=True) != before:
            json.dump(st, open(sp, "w"), indent=1, sort_keys=True)   # a wake was sent or an ack seen: remember it for the next pass
        if feed["errors"]:
            print(f"fast pass: {feed['errors']} project error(s)", file=sys.stderr)
        return
    sent = st.pop("sent_pending", []) + sent   # sends of the passes since the last tick
    lag = (now - now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0)).total_seconds()
    prev = parse(st.get("last_tick_at"))
    feed["tick_no"], feed["lag_seconds_past_5min_mark"] = st["tick_no"], int(lag)
    feed["gap_seconds_since_last_tick"] = int((now - prev).total_seconds()) if prev else None
    st["last_tick_at"] = iso(now)
    json.dump(feed, open(os.path.join(sdir, "projects.json"), "w"), indent=1, sort_keys=True)
    json.dump(st, open(sp, "w"), indent=1, sort_keys=True)
    with open(os.path.join(a.out, "ticks.jsonl"), "a") as f:
        f.write(json.dumps({"tick_at": feed["tick_at"], "lag": int(lag),
                            "gap": feed["gap_seconds_since_last_tick"], "tick_no": st["tick_no"], "sent": sent}) + "\n")
    if feed["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
