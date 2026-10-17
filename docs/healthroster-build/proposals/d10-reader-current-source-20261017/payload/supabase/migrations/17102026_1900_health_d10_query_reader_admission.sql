\set ON_ERROR_STOP on
begin;
-- ONE_TIME_ADMISSION: all overloads and routine kinds, not a repeatable guard.
do $admission$
declare expected_name text;
begin
 foreach expected_name in array array[
  'weekly_source_check_only_issue_filter_keys_v1',
  'weekly_source_check_only_query_groups_v1',
  'weekly_source_check_only_candidate_reminder_subject_v1',
  'weekly_source_check_only_candidate_reminder_context_v1'
 ] loop
  if exists(select 1 from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace
   on namespace.oid=routine.pronamespace where namespace.nspname='private' and routine.proname=expected_name) then
   raise exception 'HEALTH_D10_READER_NAMESPACE_COLLISION: %',expected_name using errcode='55000';
  end if;
 end loop;
end;
$admission$;
commit;
