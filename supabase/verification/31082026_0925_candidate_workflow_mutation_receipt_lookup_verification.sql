\set ON_ERROR_STOP on

do $verification$
declare
  v_definition text;
begin
  select pg_get_indexdef(c.oid)
  into v_definition
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public'
    and c.relname='idx_audit_candidate_workflow_mutation_request_v1';

  if v_definition is null then
    raise exception 'CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_MISSING';
  end if;
  if v_definition not like '%(correlation_id, ts_utc DESC, id DESC)%'
     or v_definition not like '%INCLUDE (object_id_text, before_json, after_json)%'
     or v_definition not like '%object_type = ''candidate_workflow_mutation_receipt''%'
  then
    raise exception 'CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_INVALID: %',v_definition;
  end if;
end;
$verification$;
