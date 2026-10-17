\set ON_ERROR_STOP on
begin;
-- One-time namespace admission, before any new definition. Every overload/kind.
do $admission$
declare routine_name text;
begin
 foreach routine_name in array array[
  'daily_rate_correction_health_financial_projection_assert_v1',
  'daily_rate_correction_health_invoice_effect_v1',
  'daily_rate_correction_health_actions_assert_v1',
  'daily_rate_correction_health_review_enroll_v1',
  'daily_rate_correction_health_revision_apply_v1',
  'daily_rate_correction_health_current_placement_v1',
  'timesheet_contract_move_public_assert_v1',
  'timesheet_contract_move_actions_assert_v1',
  'timesheet_contract_move_invoice_effect_v1'
 ] loop
  if exists(select 1 from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace
   on namespace.oid=routine.pronamespace where namespace.nspname='private' and routine.proname=routine_name) then
   raise exception 'HEALTH_CLOSED_HELPER_NAMESPACE_COLLISION: %',routine_name using errcode='55000';
  end if;
 end loop;
end;
$admission$;
commit;
