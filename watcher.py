#!/usr/bin/env python3
"""Announces BookOrbit book-request events to a Discord webhook.

Event-driven, not a poller: a trigger on book_requests calls pg_notify on insert
and on every status change, and this process holds a LISTEN open. Postgres only
delivers a notification when the transaction commits, so a request that rolls
back never produces a ping.

It connects as a read-only role. LISTEN needs no privileges at all, and the
catch-up query is a plain SELECT, so the role holds nothing but SELECT on
book_requests and users - see sql/grant-readonly.sql. The consequence is that
this process cannot install or repair its own trigger (that needs the table
owner, sql/install-trigger.sql, run once by hand). So it verifies the trigger is
present at startup and once a day, and announces its absence to Discord rather
than going quiet, which is the failure mode that would otherwise go unnoticed.

Reading the database rather than the API is also the only option on an instance
with DISABLE_LOCAL_AUTH=true, where password logins to /api/v1 return 403.

First run seeds the high-water mark and announces nothing.
"""

import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import psycopg

CHANNEL = "bookorbit_requests"
TRIGGERS = ("bo_watch_insert", "bo_watch_update")

DSN = os.environ.get("DATABASE_URL", "")
WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "")
# Optional. When set, each card's title links to <PUBLIC_URL>/requests.
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
STATE_FILE = os.environ.get("STATE_FILE", "/state/last_seen")
HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/state/heartbeat")
WEBHOOK_USERNAME = os.environ.get("WEBHOOK_USERNAME", "BookOrbit")
# The statuses worth a ping. The rest of the pipeline (searching, grabbed,
# downloading, importing) is progress noise nobody has to act on.
#
# ⚠️ `approved` has to be here or the operator's own requests are silent: a
# superuser's request is auto-approved on insert, so it never passes through
# `pending`, and with autoGrab off it parks at `approved` with failure_code
# AUTOMATION_DISABLED waiting for a release to be picked by hand. That is the
# most actionable state there is. `pending` still matters for everyone else.
STATUSES = tuple(
    s.strip()
    for s in os.environ.get(
        "STATUSES", "pending,approved,needs_review,failed,available"
    ).split(",")
    if s.strip()
)
# How often to re-check that the trigger still exists, in seconds.
TRIGGER_CHECK_INTERVAL = int(os.environ.get("TRIGGER_CHECK_INTERVAL", "86400"))

COLORS = {
    "pending": 0x5865F2,
    "available": 0x57F287,
    "failed": 0xED4245,
    "needs_review": 0xFEE75C,
    "rejected": 0x99AAB5,
    "cancelled": 0x99AAB5,
}
HEADLINES = {
    "pending": "New book request",
    "available": "Request fulfilled",
    "failed": "Request failed",
    "needs_review": "Request needs review",
    "approved": "Request approved",
    "rejected": "Request rejected",
    "cancelled": "Request cancelled",
}

ROW_SQL = """
select r.id, r.title, r.subtitle, r.authors, r.status, r.status_reason,
       r.self_serve, r.media_kind, r.cover_url, r.note, r.updated_at,
       u.username, u.name
from book_requests r
join users u on u.id = r.user_id
where r.id = %s
"""

CATCH_UP_SQL = """
select id from book_requests
where updated_at > %s and status = any(%s)
order by updated_at
"""

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("watcher")

_stop = False


def _handle_signal(signum, _frame):
    global _stop
    _stop = True
    log.info("signal %s received, shutting down", signum)


class State:
    """The high-water mark, so a restart cannot silently swallow an event.

    Postgres does not queue notifications for a listener that is not connected,
    which is the one weakness of the LISTEN approach; this is what the catch-up
    query at startup reads. It relies on BookOrbit maintaining updated_at, which
    it does in the application layer (drizzle $onUpdateFn), not via a DB default.
    """

    def __init__(self, path):
        self.path = path
        self.value = None
        try:
            with open(path) as fh:
                self.value = datetime.fromisoformat(fh.read().strip())
        except FileNotFoundError:
            pass
        except ValueError:
            log.warning("state file %s unparseable, reseeding", path)

    def save(self, when):
        if when is None:
            return
        if self.value is not None and when <= self.value:
            return
        self.value = when
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as fh:
            fh.write(when.isoformat())
        os.replace(tmp, self.path)


def heartbeat():
    """Touched on every wake of the listen loop; the healthcheck reads its age."""
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_FILE) or ".", exist_ok=True)
        with open(HEARTBEAT_FILE, "w") as fh:
            fh.write(datetime.now(timezone.utc).isoformat())
    except OSError as err:
        log.warning("could not write heartbeat: %s", err)


def post_discord(payload, attempts=5):
    body = json.dumps(payload).encode()
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(
            WEBHOOK,
            data=body,
            # ⚠️ The User-Agent is load-bearing. Discord sits behind Cloudflare,
            # which answers the default "Python-urllib/3.12" with 403 and
            # "error code: 1010" - a block, not a Discord API error, so the
            # response body carries no JSON to explain it. curl works, which is
            # what made the first manual test pass and the container fail.
            headers={
                "Content-Type": "application/json",
                "User-Agent": (
                    "bookorbit-watcher/1.0 "
                    "(+https://github.com/ramble-s/bookorbit-watcher)"
                ),
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status < 300:
                    return True
                log.warning("discord returned HTTP %s", resp.status)
        except urllib.error.HTTPError as err:
            # Discord rate limits per webhook and states how long to wait.
            if err.code == 429:
                retry = 5.0
                try:
                    retry = float(json.load(err).get("retry_after", retry))
                except Exception:
                    pass
                log.warning("discord rate limited, sleeping %.1fs", retry)
                time.sleep(min(retry, 60))
                continue
            log.warning("discord HTTP %s: %s", err.code, err.read()[:200])
        except Exception as err:  # network flap
            log.warning("discord post failed: %s", err)
        time.sleep(min(2 ** attempt, 30))
    log.error("giving up on a discord post after %s attempts", attempts)
    return False


def authors_line(authors):
    if isinstance(authors, list) and authors:
        names = [a if isinstance(a, str) else a.get("name", "") for a in authors]
        return ", ".join(n for n in names if n) or "unknown author"
    return "unknown author"


def embed_for(row):
    (
        rid, title, subtitle, authors, status, status_reason,
        self_serve, media_kind, cover_url, note, _updated, username, name,
    ) = row
    fields = [
        {"name": "Requested by", "value": name or username, "inline": True},
        {"name": "Status", "value": status, "inline": True},
    ]
    if media_kind:
        fields.append({"name": "Kind", "value": str(media_kind), "inline": True})
    if self_serve:
        fields.append({"name": "Self-serve", "value": "yes", "inline": True})
    if note:
        fields.append({"name": "Note", "value": note[:1024], "inline": False})
    if status_reason:
        fields.append({"name": "Reason", "value": status_reason[:1024], "inline": False})

    embed = {
        "title": f"{HEADLINES.get(status, 'Request updated')}: {title}",
        "description": subtitle or None,
        "color": COLORS.get(status, 0x5865F2),
        "fields": fields,
        "footer": {"text": f"{authors_line(authors)} - request #{rid}"},
    }
    if PUBLIC_URL:
        embed["url"] = f"{PUBLIC_URL}/requests"
    # cover_url is a metadata-provider URL rather than a BookOrbit route, so
    # Discord can fetch it. BookOrbit's own covers need a login and would 401.
    if cover_url and cover_url.startswith("http"):
        embed["thumbnail"] = {"url": cover_url}
    return {"username": WEBHOOK_USERNAME, "embeds": [embed]}


def announce(conn, request_id, state):
    row = conn.execute(ROW_SQL, (request_id,)).fetchone()
    if row is None:
        log.info("request %s vanished before it could be read", request_id)
        return
    status = row[4]
    if status not in STATUSES:
        log.debug("request %s status=%s not announced", request_id, status)
        state.save(row[10])
        return
    if post_discord(embed_for(row)):
        log.info("announced request %s status=%s", request_id, status)
        state.save(row[10])


def check_trigger(conn, already_warned):
    """Read-only health check on the thing this process cannot repair itself."""
    found = conn.execute(
        """
        select count(*) from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
        where c.relname = 'book_requests'
          and not t.tgisinternal
          and t.tgname = any(%s)
        """,
        (list(TRIGGERS),),
    ).fetchone()[0]
    if found == len(TRIGGERS):
        return False
    log.error("trigger missing on book_requests (found %s of %s)", found, len(TRIGGERS))
    if not already_warned:
        post_discord(
            {
                "username": WEBHOOK_USERNAME,
                "embeds": [
                    {
                        "title": "BookOrbit watcher: trigger missing",
                        "description": (
                            "The book_requests trigger is gone, so request events are "
                            "no longer being delivered. Reinstall sql/install-trigger.sql "
                            "as the table owner. The watcher is still running."
                        ),
                        "color": COLORS["failed"],
                    }
                ],
            }
        )
    return True


def catch_up(conn, state):
    if state.value is None:
        now = conn.execute("select now()").fetchone()[0]
        state.save(now)
        log.info("first run: seeded high-water mark at %s, announcing nothing", now)
        return
    rows = conn.execute(CATCH_UP_SQL, (state.value, list(STATUSES))).fetchall()
    if rows:
        log.info("catch-up: %s request(s) changed while disconnected", len(rows))
    for (request_id,) in rows:
        announce(conn, request_id, state)


def run_once(state):
    """One connection's lifetime. Returns when the connection is lost."""
    with psycopg.connect(
        DSN, autocommit=True, application_name="bookorbit-watcher"
    ) as conn:
        log.info("connected, listening on %s", CHANNEL)
        warned = check_trigger(conn, False)
        last_check = time.monotonic()
        catch_up(conn, state)
        conn.execute(f"listen {CHANNEL}")

        while not _stop:
            heartbeat()
            # ⚠️ Collect payloads, then act. The notifies() generator holds the
            # connection while it is suspended, so running a query from inside
            # the loop deadlocks: the execute waits for a lock the generator
            # only releases once it returns. The symptom is a process wedged in
            # cursor.execute while its Postgres session sits idle, never having
            # sent the query.
            #
            # ⚠️ stop_after=1, not a larger batch: the generator returns once it
            # has that many notifications OR the timeout expires, so any batch
            # above 1 delays a lone event by the full timeout. Measured at 30s
            # on the first deploy. Returning per notification keeps delivery
            # immediate; a burst just means several quick cycles.
            payloads = [
                note.payload for note in conn.notifies(timeout=30, stop_after=1)
            ]
            for payload in payloads:
                if _stop:
                    break
                try:
                    event = json.loads(payload)
                except ValueError:
                    log.warning("unparseable payload: %s", payload[:200])
                    continue
                if event.get("id") is not None:
                    announce(conn, int(event["id"]), state)
            if time.monotonic() - last_check >= TRIGGER_CHECK_INTERVAL:
                warned = check_trigger(conn, warned)
                last_check = time.monotonic()


def main():
    if not DSN:
        sys.exit("DATABASE_URL is required")
    if not WEBHOOK:
        sys.exit("DISCORD_WEBHOOK is required")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    state = State(STATE_FILE)
    backoff = 1
    while not _stop:
        try:
            run_once(state)
            backoff = 1
        except Exception as err:
            log.error("connection lost: %s", err)
            if _stop:
                break
            log.info("reconnecting in %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
    log.info("stopped")


if __name__ == "__main__":
    main()
