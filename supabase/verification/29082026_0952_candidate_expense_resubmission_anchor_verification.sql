\set ON_ERROR_STOP on

do $verification$
declare
  v_oid oid:=to_regprocedure(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'
  );
  v_definition text;
begin
  if v_oid is null then
    raise exception 'candidate_workflow_transition_atomic_v1 is missing';
  end if;

  select lower(pg_get_functiondef(v_oid)) into v_definition;
  if position('when v_workflow_kind=''contract_expense''' in v_definition)=0
     or position('then v_source_workflow.anchor_timesheet_id' in v_definition)=0
     or position('else v_week.timesheet_id end' in v_definition)=0 then
    raise exception 'Candidate expense resubmission no longer preserves the worked Timesheet anchor';
  end if;
  if v_definition~'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Candidate workflow transition contains illegal qualified conditional syntax';
  end if;
  if not exists(select 1 from pg_proc where oid=v_oid and prosecdef) then
    raise exception 'Candidate workflow transition is no longer SECURITY DEFINER';
  end if;
  if has_function_privilege('public',v_oid,'EXECUTE') then
    raise exception 'PUBLIC can execute Candidate workflow transition';
  end if;
  if exists(select 1 from pg_roles where rolname='anon')
     and has_function_privilege('anon',v_oid,'EXECUTE') then
    raise exception 'anon can execute Candidate workflow transition';
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated')
     and has_function_privilege('authenticated',v_oid,'EXECUTE') then
    raise exception 'authenticated can execute Candidate workflow transition';
  end if;
  if exists(select 1 from pg_roles where rolname='service_role')
     and not has_function_privilege('service_role',v_oid,'EXECUTE') then
    raise exception 'service_role cannot execute Candidate workflow transition';
  end if;
end;
$verification$;
