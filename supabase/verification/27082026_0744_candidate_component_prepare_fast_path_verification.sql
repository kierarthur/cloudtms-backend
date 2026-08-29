do $verification$
declare
  v_definition text;
begin
  select pg_get_functiondef(
    'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_definition;

  if position('private._candidate_session_context_v1' in v_definition)=0
     or position('for update' in lower(v_definition))=0
     or position('private._candidate_workflow_mutation_receipt_v1' in v_definition)=0
     or position('candidate_submission_components' in v_definition)=0
     or position('CANDIDATE_COMPONENT_TYPE_INVALID' in v_definition)=0 then
    raise exception 'Candidate component preparation fast path is incomplete';
  end if;
  if position('candidate_workflow_transition_atomic_v1' in v_definition)>0
     or position('private._candidate_policy_resolve_v1' in v_definition)>0 then
    raise exception 'Candidate component preparation still enters the all-purpose workflow path';
  end if;
  if position('pg_catalog.coalesce' in lower(v_definition))>0
     or position('pg_catalog.nullif' in lower(v_definition))>0
     or position('pg_catalog.least' in lower(v_definition))>0
     or position('pg_catalog.greatest' in lower(v_definition))>0 then
    raise exception 'Candidate component preparation contains an illegal qualified conditional expression';
  end if;

  if has_function_privilege(
       'public',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or has_function_privilege(
       'anon',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or has_function_privilege(
       'authenticated',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or not has_function_privilege(
       'service_role',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     ) then
    raise exception 'Candidate component preparation execution boundary is incorrect';
  end if;
end;
$verification$;
