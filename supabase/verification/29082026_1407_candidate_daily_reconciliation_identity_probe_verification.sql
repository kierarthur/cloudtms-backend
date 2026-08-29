\set ON_ERROR_STOP on

do $verification$
declare
  v_oid oid:=to_regprocedure('public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text)');
  v_definition text;
  v_refresh_calls integer;
begin
  if v_oid is null then
    raise exception 'candidate_daily_reconciliation_apply_atomic_v1 is missing';
  end if;

  select pg_get_functiondef(v_oid),
    (select count(*)-1 from regexp_split_to_table(pg_get_functiondef(v_oid),
      '_candidate_daily_refresh_sync_state_v1') ignored)
  into v_definition,v_refresh_calls;

  if position('v_probe_only' in v_definition)=0
     or position('probe_only' in v_definition)=0
     or position('LINKED' in v_definition)=0
     or position('NOT_ENROLLED' in v_definition)=0
     or v_refresh_calls<>1 then
    raise exception 'Candidate Daily reconciliation identity-probe definition is incomplete';
  end if;
  if v_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Candidate Daily identity probe contains illegal qualified conditional syntax';
  end if;
  if not exists(
    select 1 from pg_proc p
    where p.oid=v_oid and p.prosecdef
      and p.proconfig @> array['search_path=""']::text[]
  ) then
    raise exception 'Candidate Daily identity-probe security/search_path changed';
  end if;
  if has_function_privilege('public',v_oid,'EXECUTE') then
    raise exception 'PUBLIC can execute Candidate Daily identity probe';
  end if;
  if exists(select 1 from pg_roles where rolname='anon')
     and has_function_privilege('anon',v_oid,'EXECUTE') then
    raise exception 'anon can execute Candidate Daily identity probe';
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated')
     and has_function_privilege('authenticated',v_oid,'EXECUTE') then
    raise exception 'authenticated can execute Candidate Daily identity probe';
  end if;
  if exists(select 1 from pg_roles where rolname='service_role')
     and not has_function_privilege('service_role',v_oid,'EXECUTE') then
    raise exception 'service_role cannot execute Candidate Daily identity probe';
  end if;
end;
$verification$;
