-- Repeatable CloudTMS function/view authority: weekly_source_pay_query_gate_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

\ir 05102026_1417_weekly_source_coverage_support/qualified_coverage.inc

-- Keep the existing admission contract byte-for-field compatible. The Office
-- reader consumes the same qualified facts, not a second interpretation of pay.
create or replace function private.weekly_source_pay_query_gate_v1(p_root_timesheet_id uuid)
returns jsonb language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_pay_query_facts_v1(p_root_timesheet_id)-'hours_incidents'-'manual_reviews';
$function$;
alter function private.weekly_source_pay_query_gate_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_gate_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
