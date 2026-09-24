-- Run ONCE as the table owner (the bookorbit superuser). The watcher itself is
-- read-only and cannot install this; it only verifies the triggers exist.
--
--   docker exec -i bookorbit-db psql -U bookorbit -d bookorbit < sql/install-trigger.sql
--
-- Two triggers rather than one: Postgres rejects TG_OP in a trigger WHEN clause,
-- so the insert case and the status-change case cannot share a condition.

create or replace function bo_watch_notify() returns trigger
language plpgsql as $$
begin
  -- Only the ids travel. A notification payload is capped at 8000 bytes, and
  -- the watcher has to SELECT the row anyway to resolve the requester.
  perform pg_notify(
    'bookorbit_requests',
    json_build_object(
      'id', new.id,
      'status', new.status,
      'op', tg_op
    )::text
  );
  return new;
end $$;

drop trigger if exists bo_watch_insert on book_requests;
create trigger bo_watch_insert
  after insert on book_requests
  for each row
  execute function bo_watch_notify();

drop trigger if exists bo_watch_update on book_requests;
create trigger bo_watch_update
  after update of status on book_requests
  for each row
  when (old.status is distinct from new.status)
  execute function bo_watch_notify();
