do $verification$
declare
  v_name text;
  v_oid oid;
  v_rls boolean;
  v_view_options text[];
  v_failed text[] := array[]::text[];
begin
  foreach v_name in array array[
    'candidate_job_titles','candidates','client_settings','clients',
    'contract_weeks','contracts','mail_outbox','settings_defaults',
    'timesheet_evidence','timesheets','timesheets_financials'
  ] loop
    select c.oid,c.relrowsecurity into v_oid,v_rls
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relname=v_name and c.relkind='r';
    if v_oid is null or v_rls is not true
       or pg_catalog.has_table_privilege('anon',v_oid,'SELECT,INSERT,UPDATE,DELETE')
       or pg_catalog.has_table_privilege('authenticated',v_oid,'SELECT,INSERT,UPDATE,DELETE')
       or not pg_catalog.has_table_privilege('service_role',v_oid,'SELECT,INSERT,UPDATE,DELETE') then
      v_failed := pg_catalog.array_append(v_failed,v_name);
    end if;
  end loop;

  select c.oid,c.reloptions into v_oid,v_view_options
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname='candidate_activity_rollup' and c.relkind='v';
  if v_oid is null
     or not ('security_invoker=true'=any(coalesce(v_view_options,array[]::text[])))
     or pg_catalog.has_table_privilege('anon',v_oid,'SELECT')
     or pg_catalog.has_table_privilege('authenticated',v_oid,'SELECT')
     or not pg_catalog.has_table_privilege('service_role',v_oid,'SELECT') then
    v_failed := pg_catalog.array_append(v_failed,'candidate_activity_rollup');
  end if;

  if pg_catalog.array_length(v_failed,1) is not null then
    raise exception 'CANDIDATE_MYTMS_RELATION_ISOLATION_FAILED:%',
      pg_catalog.array_to_string(v_failed,',');
  end if;
end
$verification$;

do $function_verification$
declare
  v_signature regprocedure;
  v_name text;
  v_failed text[] := array[]::text[];
begin
  foreach v_signature in array array[
    'public.candidate_delete_apply(uuid,uuid,text)'::regprocedure,
    'public.candidate_delete_eligibility(uuid)'::regprocedure,
    'public.candidate_list_ids(jsonb)'::regprocedure,
    'public.candidate_picker_search(text,integer,integer,boolean)'::regprocedure
  ] loop
    select p.proname into v_name from pg_catalog.pg_proc p where p.oid=v_signature::oid;
    if pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
       or pg_catalog.has_function_privilege('authenticated',v_signature,'EXECUTE')
       or not pg_catalog.has_function_privilege('service_role',v_signature,'EXECUTE') then
      v_failed := pg_catalog.array_append(v_failed,v_name);
    end if;
  end loop;

  if not exists (
    select 1 from pg_catalog.pg_proc p
    where p.oid='public.candidate_picker_search(text,integer,integer,boolean)'::regprocedure
      and p.proconfig @> array['search_path=pg_catalog, public']::text[]
  ) then
    v_failed := pg_catalog.array_append(v_failed,'candidate_picker_search_search_path');
  end if;

  if pg_catalog.array_length(v_failed,1) is not null then
    raise exception 'CANDIDATE_MYTMS_FUNCTION_ISOLATION_FAILED:%',
      pg_catalog.array_to_string(v_failed,',');
  end if;
end
$function_verification$;

-- Reassert that the dedicated Candidate authority stays browser-inaccessible.
do $authority_verification$
declare
  v_name text;
  v_oid oid;
begin
  foreach v_name in array array[
    'candidate_app_accounts','candidate_app_sessions','candidate_approval_requests',
    'candidate_auth_challenges','candidate_daily_availability_days',
    'candidate_daily_command_receipts','candidate_daily_rota_days',
    'candidate_daily_rota_generations','candidate_daily_sheet_projection_outbox',
    'candidate_notifications','candidate_submission_components','candidate_submission_workflows',
    'invoice_document_versions'
  ] loop
    select c.oid into v_oid
    from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relname=v_name and c.relkind='r' and c.relrowsecurity;
    if v_oid is null
       or pg_catalog.has_table_privilege('anon',v_oid,'SELECT,INSERT,UPDATE,DELETE')
       or pg_catalog.has_table_privilege('authenticated',v_oid,'SELECT,INSERT,UPDATE,DELETE') then
      raise exception 'CANDIDATE_AUTHORITY_ISOLATION_FAILED:%',v_name;
    end if;
  end loop;
end
$authority_verification$;
