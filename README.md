# bookorbit-watcher

sends bookorbit book requests to a discord webhook. bookorbit has no notifications outside the app
itself ([bookorbit#1231](https://github.com/bookorbit/bookorbit/issues/1231)), so this fills the gap
until it does.

it doesn't poll. a trigger on `book_requests` fires `pg_notify` on every new request and status
change, and the watcher just listens. it also connects as a read-only role, so it can't touch your
library even if something goes wrong. the one api-based script i found logs in with a password,
which doesn't work if you've turned local auth off.

image: `ghcr.io/ramble-s/bookorbit-watcher` (amd64 + arm64)

## setup

swap `CHANGE_ME` in `sql/grant-readonly.sql` for a real password, then run both sql files against
bookorbit's database as the owner:

```sh
docker exec -i bookorbit-db psql -U bookorbit -d bookorbit < sql/grant-readonly.sql
docker exec -i bookorbit-db psql -U bookorbit -d bookorbit < sql/install-trigger.sql
```

add it to your bookorbit stack:

```yaml
bookorbit-watcher:
  image: ghcr.io/ramble-s/bookorbit-watcher:latest
  restart: unless-stopped
  environment:
    DATABASE_URL: postgresql://bookorbit_watch:${WATCH_PASSWORD}@bookorbit-db:5432/bookorbit
    DISCORD_WEBHOOK: ${DISCORD_WEBHOOK}
    PUBLIC_URL: https://bookorbit.example.com  # optional, makes the cards clickable
  volumes:
    - ./watcher-state:/state
```

two things that got me on the first deploy:
- it needs internet access to reach discord. if your database network is `internal: true`, give it a
  second network too.
- it runs as `1000:100`, so `chown 1000:100 watcher-state` first.

## what gets announced

by default: `pending`, `approved`, `needs_review`, `failed` and `available`. change it with
`STATUSES`.

if auto-grab is on, drop `approved`. it only lasts a few seconds before the book is grabbed, so it's
just noise. if auto-grab is off, keep it: your own requests skip `pending` and wait at `approved`, so
without it you won't hear about them at all.

## env vars

| var | default | what it does |
|---|---|---|
| `DATABASE_URL` | required | postgres connection string for the read-only role |
| `DISCORD_WEBHOOK` | required | where the cards go |
| `PUBLIC_URL` | none | your bookorbit url. makes each card link to its requests page |
| `STATUSES` | `pending,approved,needs_review,failed,available` | which statuses get announced |
| `WEBHOOK_USERNAME` | `BookOrbit` | name the webhook posts as |
| `TRIGGER_CHECK_INTERVAL` | `86400` | seconds between checks that the trigger still exists |
| `LOG_LEVEL` | `INFO` | set to `DEBUG` to see skipped statuses too |
| `STATE_FILE` | `/state/last_seen` | where it remembers the last request it handled |
| `HEARTBEAT_FILE` | `/state/heartbeat` | touched every 30s, the healthcheck reads it |

the watcher exits right away if either required var is missing.

## caveats

- it reads bookorbit's internal tables, so an update could break it. if the trigger ever
  disappears, the watcher posts a warning to discord instead of going quiet. tested on bookorbit
  2.10 and 3.0.
- vibe coded, provided as-is. open to issues / PRs, but no promises anything will be done with them.

MIT licensed.
