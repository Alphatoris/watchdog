#!/usr/bin/env python3
"""Coordinator watchdog. No AI. Reads the Library records, decides what is late,
wakes through the wake runner, writes a status feed. Standard library only.

Rules (design v2 sections 8 and 11): an open item past `due`, or with no `received`
30 minutes after `given`, or a stage checkpoint passed, is late. Step 1 wakes the
coordinator; step 2 the Delivery Lead after 2 more ticks; step 3 the owner list and
phone push after 2 more. Each step once. Reserved items never climb the ladder.
The public repo holds no project data: state keys are hashes, the feed holds
counts, stage names and times under opaque labels.
"""
import argparse, base64, hashlib, json, os, sys, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

LIBRARY = "Alphatoris/Alphatoris-Library"
REGISTRY_PATH = "projects/alphatoris-wide/stages/REGISTRY.json"
DL_ITEMS_PATH = "projects/alphatoris-wide/stages/OPEN-ITEMS.json"
RECEIVE_GRACE = timedelta(minutes=30)
STEP2_TICKS, STEP3_TICKS = 2, 4          # ticks after first detection
CLOSED_STAGE_STATES = {"done", "closed", "complete", "waiting_on_aamer", "waiting on aamer"}


def parse(t):
    if not t:
        return None
    d = datetime.fromisoformat(str(t).split(" ")[0].replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)   # a time with no zone is read as UTC


def iso(d):
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def key_of(project, item, due):
    return hashlib.sha256(f"{project}:{item}:{due}".encode()).hexdigest()[:16]


def label_of(entry):
    return entry.get("public_label") or hashlib.sha256(entry["project"].encode()).hexdigest()[:6]


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


class DirSource:
    def __init__(self, root):
        self.root = root

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
    feed, seen, errors = {}, set(), 0
    for e in entries:
        try:
            pid = e["project"]
            stage = source.get(f"projects/{pid}/stages/STAGE.json")
            items = source.get(f"projects/{pid}/stages/OPEN-ITEMS.json")
            paused = bool(e.get("paused"))      # owner-paused project: shown, never chased
            late = [] if paused else late_items(pid, stage, items, now)
        except Exception as ex:                  # one bad project never stops the others
            errors += 1
            feed[label_of(e) if isinstance(e, dict) and "project" in e else "?"] = {"error": type(ex).__name__}
            print(f"project error: {type(ex).__name__}: {ex}", file=sys.stderr)
            continue
        openn = len([i for i in (items or {}).get("items", []) if not i.get("done") and not i.get("dropped")])
        feed[label_of(e)] = {"stage": (stage or {}).get("stage"), "stage_name": (stage or {}).get("stage_name"),
                             "state": "paused" if paused else state_word((stage or {}).get("state")), "open": openn,
                             "late": len([l for l in late if not l[2]]), "reserved_waiting": len([l for l in late if l[2]]),
                             "stage_updated": (stage or {}).get("updated"),
                             "wake_route": ("pr_comment" if subscriber_ok(e, now) else "hourly_guard_only") if e.get("wake_pr") else "hourly_guard_only"}
        for item, due, reserved in late:
            if reserved:
                continue                       # never climbs the ladder
            k = key_of(pid, item, due)
            seen.add(k)
            s = state.setdefault(k, {"first_tick": tick_no, "steps": []})
            age = tick_no - s["first_tick"]
            pw = s.get("pr_wake")              # a PR-comment wake must be acknowledged by the subscriber
            if pw and not pw["acked"] and acked:
                if acked(pw["pr"], pw["at"]):
                    pw["acked"] = True
                elif tick_no - pw["tick"] >= PR_ACK_TICKS:
                    feed[label_of(e)]["wake_route"] = "pr_unacked"   # lost wake: the hourly guard is the only route
            for step, need in ((1, 0), (2, STEP2_TICKS), (3, STEP3_TICKS)):
                if step in s["steps"] or age < need:
                    continue
                target = {1: "coordinator", 2: "delivery_lead", 3: "owner"}[step]
                payload = {"project": pid, "item": item, "due": due, "step": step, "target": target,
                           "sent_at": iso(now)}     # no session ids: the runner reads them from the registry only
                if e.get("wake_pr") and subscriber_ok(e, now):
                    payload["wake_pr"] = e["wake_pr"]   # "owner/repo#n": a subscribed session is woken by a comment
                    payload["label"] = label_of(e)
                if send(payload):
                    s["steps"].append(step)
                    if payload.get("wake_pr"):
                        s["pr_wake"] = {"tick": tick_no, "at": iso(now), "pr": payload["wake_pr"], "acked": False}
                break                          # one step per tick
    for k in list(state):                      # closed or re-dated items drop out
        if k not in seen:
            del state[k]
    return {"tick_at": iso(now), "projects": feed, "errors": errors}


SUBSCRIBER_MAX_AGE = timedelta(hours=12)
PR_ACK_TICKS = 2


def subscriber_ok(e, now):
    """The wake PR is used only while the registry names a subscriber session, subscribed recently. The script cannot see
    GitHub's subscription itself, so a missing or old record falls back to the hourly guard and the feed says so."""
    sub = e.get("wake_subscriber") or {}
    t = parse(sub.get("subscribed_at"))
    return bool(sub.get("session_id") and t and now - t <= SUBSCRIBER_MAX_AGE)


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


def pr_acked(token):
    """True if a person or session (not the Actions bot) commented on the wake PR after the wake; None-safe on errors."""
    def acked(wake_pr, since_iso):
        repo, num = wake_pr.rsplit("#", 1)
        req = urllib.request.Request(f"https://api.github.com/repos/{repo}/issues/{int(num)}/comments?since={since_iso}",
                                     headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return any(c.get("user", {}).get("type") != "Bot" for c in json.load(r))
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
