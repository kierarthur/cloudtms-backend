-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- Installs the `ws_banking_fixture` schema into a DISPOSABLE LOCAL clone.
--
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f install.sql
--
-- Nothing here is part of the release: no file under `supabase/` references this
-- schema, and `ws_banking_fixture.assert_local_only()` refuses to run in any
-- database that is not a disposable local proof database.

\set ON_ERROR_STOP on

\ir 010_fixture_library_core.sql
\ir 020_base_world.sql
\ir 030_real_owner_cancellation.sql
\ir 040_real_owner_abort_and_paye_net.sql
\ir 050_seed_settlement.sql
\ir 060_seed_transfers_operations_reservations.sql
\ir 070_states.sql

select 'ws_banking_fixture installed' as result,
       count(*) filter (where routine_row.proname like 'state_%') as state_functions,
       count(*) as total_functions
from pg_proc as routine_row
join pg_namespace as schema_row on schema_row.oid = routine_row.pronamespace
where schema_row.nspname = 'ws_banking_fixture';
