#!/usr/bin/env python3
"""Coordinator watchdog. No AI. Reads the Library records, decides what is late,
wakes through the wake runner, writes a status feed. Standard library only.

Rules (design v2 sections 8 and 11): an open item past `due`, or with no `received`
30 minutes after `given`, or a stage checkpoint passed, is late. Step 1 wakes the
coordinator; step 2 the Delivery Lead after 2 more ticks. Step 3, the owner list and
phone push, is by severity and not by time (playbooks rev-053, cycle rules section 21 item 3):
only an item marked `"severity": "critical"` reaches the owner, at once. Each step once.
Reserved items never climb the ladder.
The public repo holds no project data: state keys are keyed hashes (HMAC), and the
feed holds, per project, only a stage number, a state word, counts of open and late
items and a wake-route word, under opaque labels. The only times in the published
file are the run's own (tick_at, tick_no, lag, gap).
"""
import argparse, base64, hashlib, hmac, json, os, re, sys, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

LIBRARY = "Alphatoris/Alphatoris-Library"
REGISTRY_PATH = "projects/alphatoris-wide/stages/REGISTRY.json"
DL_ITEMS_PATH = "projects/alphatoris-wide/stages/OPEN-ITEMS.json"
RECEIVE_GRACE = timedelta(minutes=30)
STEP2_TICKS = 2                          # ticks after first detection
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

    def get(self, path):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{self.repo}/contents/{path}?ref=main",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github.raw+json",
                     "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise


    def get_text(self, path):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{self.repo}/contents/{path}?ref=main",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github.raw+json",
                     "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
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
def late_items(project, stage, items, now):
    """Return list of (item_id, due_iso, reserved) that are late."""
    out = []
    for it in (items or {}).get("items", []):
        if it.get("done") or it.get("dropped"):
            continue
        due = parse(it.get("due"))
        given = parse(it.get("given"))
        past_due = due is not None and due < now
        unreceived = (not it.get("received")) and given is not None and given + RECEIVE_GRACE < now
        if past_due or unreceived:
            when = it.get("due") or iso(given + RECEIVE_GRACE)
            reserved = bool(it.get("reserved")) or "aamer" in str(it.get("owner", "")).lower()
            out.append((it["id"], when, reserved))
    if stage:
        cp = parse(stage.get("next_checkpoint"))
        st = str(stage.get("state", "")).lower()
        if cp and cp < now and st not in CLOSED_STAGE_STATES and not stage.get("stage_ended"):
            out.append(("stage-checkpoint", stage["next_checkpoint"], False))
    return out


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


def gate_word(status, gate_late):
    """Public feed word for the evidence gate: unknown (no status file), unreadable, ahead, or ok. Words only, no milestone."""
    if status is None:
        return "unknown"
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


def run_tick(source, state, now, send, tick_no, acked=None):
    registry = source.get(REGISTRY_PATH)
    if not isinstance(registry, dict) or not isinstance(registry.get("projects"), list):
        raise RegistryError("REGISTRY.json missing or malformed")   # fails the run: never a green empty feed
    entries = list(registry["projects"])
    prs = [x.get("wake_pr") for x in entries if isinstance(x, dict) and x.get("wake_pr")]
    dup = {p for p in prs if prs.count(p) > 1}              # one wake PR per project: a shared one is not used
    feed, seen, errors = {}, set(), 0
    labels = [x.get("public_label") for x in entries if isinstance(x, dict)]
    for idx, e in enumerate(entries):
        try:
            lab, bad_label = label_of(e, idx, labels)
        except Exception:                        # an entry that is not even a mapping
            lab, bad_label = f"E{idx}", True
        try:
            pid = e["project"]
            stage = source.get(f"projects/{pid}/stages/STAGE.json")
            items = source.get(f"projects/{pid}/stages/OPEN-ITEMS.json")
            paused = bool(e.get("paused"))      # owner-paused project: shown, never chased
            late = [] if paused else late_items(pid, stage, items, now)
            gate = "unknown"                    # no status file, a paused project or a failed read: the gate is not known, not silent
            if not paused:
                try:
                    status = source.get(f"ledger/status/{pid}.json")
                    gate_late = milestone_late(stage, status)
                    late += gate_late
                    gate = gate_word(status, gate_late)
                except Exception as ex:          # a failed status read must not drop the project's other late items
                    print(f"gate status read error: {type(ex).__name__}: {ex}", file=sys.stderr)
        except Exception as ex:                  # one bad project never stops the others
            errors += 1
            feed[lab] = {"error": "project_error"}
            print(f"project error: {type(ex).__name__}: {ex}", file=sys.stderr)
            continue
        critical = {i.get("id") for i in (items or {}).get("items", []) if i.get("severity") == "critical"}
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
        if mark and acked and acked(e["wake_pr"], mark["since"]):
            del state[pk]                        # the relay answered after the lost wake: it is alive again
            mark = None
        if mark:
            seen.add(pk)
            if feed[lab]["wake_route"] == "pr_comment":
                feed[lab]["wake_route"] = "pr_unacked"
        probe_late = relay_probe(pid, e, lab, state, seen, now, tick_no, send, acked or (lambda pr, at: False), feed)
        if probe_late:
            late.append(probe_late)
        if bad_label:
            errors += 1                          # loud for the coordinator (guard lists projects with an error key)
            feed[lab]["error"] = "bad_label"
            print(f"bad public_label for entry {idx}: shown as {lab}", file=sys.stderr)
        if e.get("wake_pr") and not valid_pr(e.get("wake_pr")):
            print(f"bad wake_pr for {lab}: ignored", file=sys.stderr)
        for item, due, reserved in late:
            if reserved:
                continue                       # never climbs the ladder
            k = key_of(pid, item, due)
            seen.add(k)
            s = state.setdefault(k, {"first_tick": tick_no, "steps": []})
            age = tick_no - s["first_tick"]
            if item == "relay-probe":
                s["pr_down"] = feed[lab]["wake_route"]   # the relay is the thing that is down: the ladder never uses its PR
            pw = s.get("pr_wake")              # a PR-comment wake must be acknowledged by the subscriber
            if pw and not pw["acked"] and acked:
                if acked(pw["pr"], pw["at"]):
                    pw["acked"] = True
                elif tick_no - pw["tick"] >= PR_ACK_TICKS:
                    s["pr_down"] = "pr_unacked"                      # lost wake: later steps use the ladder's other route
                    state[pk] = {"since": pw["at"]}                  # and the project stays marked after this item closes
                    seen.add(pk)
                    s["steps"] = [x for x in s["steps"] if x != 1]   # and step 1 is sent again by the wake runner
                    del s["pr_wake"]
            if s.get("pr_down"):
                feed[lab]["wake_route"] = s["pr_down"]
            steps = ((2, 0),) if item in DL_ONLY else ((1, 0), (2, STEP2_TICKS))
            if item in critical and item not in DL_ONLY:
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
                ok = send(payload)
                if not ok and payload.get("wake_pr"):                # the comment could not be posted (for example no permission)
                    s["pr_down"] = "pr_failed"
                    feed[lab]["wake_route"] = "pr_failed"
                    payload.pop("wake_pr"); payload.pop("label", None)
                    ok = send(payload)                               # same step, by the existing route, in this tick
                if ok:
                    s["steps"].append(step)
                    if payload.get("wake_pr"):
                        s["pr_wake"] = {"tick": tick_no, "at": iso(now), "pr": payload["wake_pr"], "acked": False}
                break                          # one step per tick
    for k in list(state):                      # closed or re-dated items drop out
        if k not in seen:
            del state[k]
    return {"tick_at": iso(now), "projects": feed, "errors": errors}


PR_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+$")


def valid_pr(x):
    return isinstance(x, str) and bool(PR_RE.fullmatch(x))
PR_ACK_TICKS = 2
# Relays now comment as the agent account, which GitHub reports as COLLABORATOR. Its id AND login must both match (the id
# cannot be spoofed by renaming, the login catches a reused id in a fixture), so no other collaborator counts.
ACK_AGENTS = ((350444722, "alphatoris-agent"),)
PROBE_HOUR_UTC = 6                       # once a day, on the first tick of this hour, each relay gets a probe
PROBE_GAP = timedelta(hours=20)


def relay_probe(pid, e, lab, state, seen, now, tick_no, send, acked, feed):
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
        p.update(last=iso(now), tick=tick_no, pending=True, at=iso(now))
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


def comment_wake(wake_pr, label, step, token):
    """Comment on the project's wake pull request; a subscribed session wakes in seconds. Public text: label and step only."""
    repo, num = wake_pr.rsplit("#", 1)
    body = json.dumps({"body": f"Watchdog wake: {label} step {step}"}).encode()
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
            return comment_wake(payload["wake_pr"], payload["label"], payload["step"], os.environ.get("GH_TOKEN", ""))
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
    a = ap.parse_args()
    now = parse(a.now) if a.now else datetime.now(timezone.utc)
    src = DirSource(a.fixtures) if a.fixtures else GitHubSource(os.environ["READ_KEY"])
    sdir = os.path.join(a.out, "status")
    os.makedirs(sdir, exist_ok=True)
    sp = os.path.join(a.out, "state.json")
    st = json.load(open(sp)) if os.path.exists(sp) else {"tick_no": 0, "items": {}}
    st["tick_no"] += 1
    send = (lambda p: print("DRY-RUN wake", json.dumps(p)) or True) if a.dry_run else \
        fire_wake(os.environ.get("WAKE_URL"), os.environ.get("WAKE_KEY"))
    try:
        feed = run_tick(src, st["items"], now, send, st["tick_no"], pr_acked(os.environ.get("GH_TOKEN", "")))
    except RegistryError as ex:
        print(f"FAILED: {ex}", file=sys.stderr)
        sys.exit(1)
    lag = (now - now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0)).total_seconds()
    prev = parse(st.get("last_tick_at"))
    feed["tick_no"], feed["lag_seconds_past_5min_mark"] = st["tick_no"], int(lag)
    feed["gap_seconds_since_last_tick"] = int((now - prev).total_seconds()) if prev else None
    st["last_tick_at"] = iso(now)
    json.dump(feed, open(os.path.join(sdir, "projects.json"), "w"), indent=1, sort_keys=True)
    json.dump(st, open(sp, "w"), indent=1, sort_keys=True)
    with open(os.path.join(a.out, "ticks.jsonl"), "a") as f:
        f.write(json.dumps({"tick_at": feed["tick_at"], "lag": int(lag),
                            "gap": feed["gap_seconds_since_last_tick"]}) + "\n")
    if feed["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
