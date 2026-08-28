do $verification$
declare
  v_transition_definition text;
  v_replacement_definition text;
begin
  select pg_get_functiondef(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_transition_definition;

  if position(
       'v_source_workflow.state not in (''REJECTED'',''REFUSED'')'
       in v_transition_definition
     )=0 then
    raise exception 'Candidate manager-refused resubmission authority is not installed';
  end if;

  select pg_get_functiondef(
    'private._candidate_rejection_replaced_v1(uuid)'::regprocedure
  ) into v_replacement_definition;

  if position(
       'v_rejected.state not in (''REJECTED'',''REFUSED'')'
       in v_replacement_definition
     )=0 then
    raise exception 'Candidate manager-refused replacement guard is not installed';
  end if;

  if v_transition_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\('
     or v_replacement_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Candidate manager-refused resubmission authority contains an illegal conditional-expression prefix';
  end if;
end;
$verification$;
