\set ON_ERROR_STOP on

do $verification$
declare
  v_missing text[];
  v_weekly_table_count integer;
  v_rls_count integer;
  v_force_rls_count integer;
  v_bad_acl_count integer;
  v_bad_definition_count integer;
  v_c1_source_check text;
begin
  select pg_catalog.array_agg(required.signature order by required.signature)
  into v_missing
  from (
    values
      ('private.weekly_source_sha256_text_v1(text,text)'),
      ('private.weekly_source_sha256_jsonb_v1(text,jsonb)'),
      ('private.weekly_source_breaks_equivalent_v1(integer,integer)'),
      ('private.weekly_source_scope_fingerprint_v1(text,uuid,uuid,uuid,uuid,uuid)'),
      ('private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)')
  ) required(signature)
  where pg_catalog.to_regprocedure(required.signature) is null;
  if v_missing is not null then
    raise exception 'WEEKLY_SOURCE_CLASSIFIER_FUNCTIONS_MISSING: %',v_missing;
  end if;

  if not private.weekly_source_breaks_equivalent_v1(30,30)
     or not private.weekly_source_breaks_equivalent_v1(null,0)
     or private.weekly_source_breaks_equivalent_v1(30,45)
     or private.weekly_source_breaks_equivalent_v1(-1,-1) then
    raise exception 'WEEKLY_SOURCE_BREAK_EQUIVALENCE_CONTRACT_FAILED';
  end if;

  select pg_catalog.count(*)::integer,
         pg_catalog.count(*) filter(where c.relrowsecurity)::integer,
         pg_catalog.count(*) filter(where c.relforcerowsecurity)::integer
  into v_weekly_table_count,v_rls_count,v_force_rls_count
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='r'
    and (pg_catalog.left(c.relname,7)='weekly_' or c.relname='office_action_notifications');
  if v_weekly_table_count<1
     or v_rls_count<>v_weekly_table_count
     or v_force_rls_count<>v_weekly_table_count then
    raise exception 'WEEKLY_SOURCE_RLS_INCOMPLETE: tables %, rls %, force %',
      v_weekly_table_count,v_rls_count,v_force_rls_count;
  end if;

  select pg_catalog.count(*)::integer into v_bad_acl_count
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  cross join (values ('anon'),('authenticated')) browser(role_name)
  where n.nspname='public' and c.relkind='r'
    and (pg_catalog.left(c.relname,7)='weekly_' or c.relname='office_action_notifications')
    and pg_catalog.has_table_privilege(browser.role_name,c.oid,'select,insert,update,delete');
  if v_bad_acl_count<>0 then
    raise exception 'WEEKLY_SOURCE_BROWSER_TABLE_ACL_PRESENT: %',v_bad_acl_count;
  end if;

  if pg_catalog.has_function_privilege(
       'anon','private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)','execute'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)','execute'
     )
     or not pg_catalog.has_function_privilege(
       'service_role','private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)','execute'
     ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_AUTHORITY_ACL_INVALID';
  end if;

  select pg_catalog.count(*)::integer into v_bad_definition_count
  from pg_catalog.pg_proc procedure_row
  join pg_catalog.pg_namespace namespace_row on namespace_row.oid=procedure_row.pronamespace
  where namespace_row.nspname='private'
    and procedure_row.proname in (
      'weekly_source_sha256_text_v1',
      'weekly_source_sha256_jsonb_v1',
      'weekly_source_breaks_equivalent_v1',
      'weekly_source_scope_fingerprint_v1',
      'weekly_source_office_authority_v1'
    )
    and pg_catalog.pg_get_functiondef(procedure_row.oid)
      ~* '(pay_batch|banking_pay|provider_submission|settlement)';
  if v_bad_definition_count<>0 then
    raise exception 'WEEKLY_SOURCE_CLASSIFIER_FINANCE_OWNER_REFERENCE_FOUND: %',
      v_bad_definition_count;
  end if;

  select pg_catalog.pg_get_constraintdef(constraint_row.oid)
  into v_c1_source_check
  from pg_catalog.pg_constraint constraint_row
  where constraint_row.conrelid='public.weekly_exceptional_c1_publication_requests'::pg_catalog.regclass
    and constraint_row.contype='c'
    and pg_catalog.pg_get_constraintdef(constraint_row.oid) like '%source_mode%';
  if v_c1_source_check is null
     or v_c1_source_check not like '%NHSP_WEEKLY%'
     or v_c1_source_check not like '%HEALTHROSTER_WEEKLY%'
     or v_c1_source_check like '%MAGNIT%'
     or v_c1_source_check like '%GENERIC_WEEKLY%' then
    raise exception 'WEEKLY_SOURCE_C1_SOURCE_MODE_CONTRACT_INVALID: %',v_c1_source_check;
  end if;

  if pg_catalog.to_regclass('public.weekly_exceptional_pay_members') is not null
     or pg_catalog.to_regprocedure('public._ctms_weekly_source_pay_target_classify_v1(uuid)') is not null
     or pg_catalog.to_regprocedure('public.weekly_source_pay_target_timesheet_is_internal_v1(uuid)') is not null
     or pg_catalog.to_regprocedure('private.weekly_source_pay_target_effective_timesheet_v1(uuid)') is not null then
    raise exception 'WEEKLY_SOURCE_PROVISIONAL_PUBLIC_MEMBER_LAYER_PRESENT';
  end if;
end;
$verification$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_private_classifiers_v1',
  'c1_source_modes',pg_catalog.jsonb_build_array('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
) as result;
