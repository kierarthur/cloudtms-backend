\set ON_ERROR_STOP on

do $verification$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(p.oid)
  into strict v_definition
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private'
    and p.proname='_candidate_home_summary_v1'
    and pg_catalog.pg_get_function_identity_arguments(p.oid)='p_environment text, p_account_id uuid, p_candidate_id uuid, p_daily_capability jsonb, p_now_utc timestamp with time zone';

  if position('draft_timesheet_count' in v_definition)=0
     or position('draft_expense_count' in v_definition)=0
     or position('draft_timesheet_record_ids' in v_definition)=0
     or position('draft_expense_record_ids' in v_definition)=0
     or position('contract_week_id::text' in v_definition)=0
     or position('anchor_timesheet_id::text' in v_definition)=0
     or position('target_timesheet_id::text' in v_definition)=0
     or position('candidate_submission_components' in v_definition)=0
     or position('workflow_generation=workflow.generation' in v_definition)=0
     or position('component.superseded_at_utc is null' in v_definition)=0
     or position('WORKER_DRAFT' in v_definition)=0 then
    raise exception 'CANDIDATE_HOME_DRAFT_IDENTITY_DEFINITION_MISSING';
  end if;

  if pg_catalog.has_function_privilege('service_role',
      'private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)','EXECUTE')
     or pg_catalog.has_function_privilege('anon',
      'private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)','EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',
      'private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)','EXECUTE') then
    raise exception 'CANDIDATE_HOME_PRIVATE_HELPER_EXPOSED';
  end if;
end;
$verification$;
