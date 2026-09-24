-- The watcher's role. Run once as the bookorbit superuser, with a generated
-- password substituted for the placeholder; the password then lives only in the
-- stack .env, never in this repo.
--
-- This is the whole grant. No INSERT, no UPDATE, nothing on any other table, so
-- a bug or a compromise in the watcher cannot alter the catalogue. LISTEN needs
-- no privileges, which is what makes a read-only listener possible at all.
--
-- On PostgreSQL 15+ the public schema is not world-writable (verified 18.6
-- here), so a fresh role starts with nothing until granted.

create role bookorbit_watch login password 'CHANGE_ME';

grant connect on database bookorbit to bookorbit_watch;
grant usage on schema public to bookorbit_watch;
grant select on book_requests, users to bookorbit_watch;

-- Verify (expect exactly these two tables, privilege_type SELECT):
--   select table_name, privilege_type from information_schema.table_privileges
--   where grantee = 'bookorbit_watch';
