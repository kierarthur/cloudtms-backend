-- Read-only live verification for Daily Validation compatibility and the
-- office-owned break-entry preference. This script does not create fixtures
-- or modify business data.

do $verification$
declare
  v_break_oid oid:=to_regprocedure('public.timesheet_break_entry_effective_get_v1(uuid,uuid,date)');
  v_zero_oid oid:=to_regprocedure('public.daily_zero_shifts_review_create_v1(uuid,date,date,uuid,text)');
  v_core_oid oid:=to_regprocedure('public._import_review_effective_authority_core_v1(text,uuid,uuid,date)');
  v_break_def text;
  v_zero_def text;
  v_core_def text;
  v_enum_labels text[];
begin
  if v_break_oid is null or v_zero_oid is null or v_core_oid is null then
    raise exception 'DAILY_VALIDATION_COMPATIBILITY_FUNCTION_MISSING';
  end if;

  select array_agg(e.enumlabel::text order by e.enumsortorder)
  into v_enum_labels
  from pg_type t
  join pg_namespace n on n.oid=t.typnamespace
  join pg_enum e on e.enumtypid=t.oid
  where n.nspname='public' and t.typname='timesheet_break_entry_mode_enum';
  if v_enum_labels is distinct from array['START_END_TIMES','DURATION_MINUTES']::text[] then
    raise exception 'DAILY_VALIDATION_BREAK_ENUM_INVALID';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='client_settings'
      and column_name='timesheet_break_entry_mode'
      and udt_name='timesheet_break_entry_mode_enum' and is_nullable='NO'
  ) or not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='contracts'
      and column_name='timesheet_break_entry_mode'
      and udt_name='timesheet_break_entry_mode_enum' and is_nullable='YES'
  ) then
    raise exception 'DAILY_VALIDATION_BREAK_COLUMNS_INVALID';
  end if;

  select pg_get_functiondef(v_break_oid) into v_break_def;
  select pg_get_functiondef(v_zero_oid) into v_zero_def;
  select pg_get_functiondef(v_core_oid) into v_core_def;

  if position('overrideclientsettings' in lower(v_break_def))=0
     or position('contract_client_mismatch' in lower(v_break_def))=0
     or position('cs.effective_from<=v_as_of' in lower(v_break_def))=0
     or position('p_coverage_end_date-p_coverage_start_date>365' in lower(v_zero_def))=0
     or position('v_target_count,0)>500' in lower(v_zero_def))=0
     or position('pg_advisory_xact_lock' in lower(v_zero_def))=0
     or position('ts.revoked_at is null' in lower(v_zero_def))=0
     or position('active_contract_override' in lower(v_core_def))=0 then
    raise exception 'DAILY_VALIDATION_COMPATIBILITY_DEFINITION_INVALID';
  end if;

  if not exists (
    select 1 from pg_proc p
    where p.oid=v_break_oid and p.prosecdef and p.provolatile='s'
      and pg_get_userbyid(p.proowner)='postgres'
  ) or not exists (
    select 1 from pg_proc p
    where p.oid=v_zero_oid and p.prosecdef and p.provolatile='v'
      and pg_get_userbyid(p.proowner)='postgres'
  ) then
    raise exception 'DAILY_VALIDATION_COMPATIBILITY_FUNCTION_PROPERTIES_INVALID';
  end if;

  if exists (
    select 1
    from pg_proc p
    cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
    where p.oid in (v_break_oid,v_zero_oid)
      and a.grantee=0 and a.privilege_type='EXECUTE'
  ) or has_function_privilege('anon',v_break_oid,'EXECUTE')
     or has_function_privilege('authenticated',v_break_oid,'EXECUTE')
     or has_function_privilege('anon',v_zero_oid,'EXECUTE')
     or has_function_privilege('authenticated',v_zero_oid,'EXECUTE')
     or not has_function_privilege('service_role',v_break_oid,'EXECUTE')
     or not has_function_privilege('service_role',v_zero_oid,'EXECUTE') then
    raise exception 'DAILY_VALIDATION_COMPATIBILITY_FUNCTION_ACL_INVALID';
  end if;
end
$verification$;
