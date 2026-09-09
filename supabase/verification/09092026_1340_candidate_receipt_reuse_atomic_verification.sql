\set ON_ERROR_STOP on

do $verification$
declare
  v_definition text;
  v_index_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_definition;

  if pg_catalog.strpos(pg_catalog.lower(v_definition),'for update of source_component')=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'source_component.workflow_generation<v_workflow.generation'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'source_workflow.id<>v_workflow.id'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'live_component.source_component_id'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'live_component.workflow_generation'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'live_expense.workflow_generation'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'live_component.workflow_generation<v_workflow.generation'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'live_expense.lifecycle_state'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'ended_component.state'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'from public.timesheet_evidence live_evidence'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''manager_refused'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''office_rejected'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''withdrawn'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''candidate_evidence_bytes_already_used'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''source_content_sha256_hex'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'source_component.source_content_sha256'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_requested_source_digest'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''expected_source_content_sha256_hex'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_expected_source_digest'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''candidate_component_storage_key_invalid'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_component_kind not in (''mileage_form'',''expense_evidence'')'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_component_kind,v_expense_category,v_document_role,''pending'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_expected_source_digest,p_now_utc'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       '''abandoned'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_workflow.scope<>''weekly'''
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'v_component.source_component_id'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_definition),
       'is distinct from v_requested_source_component_id'
     )=0 then
    raise exception 'CANDIDATE_RECEIPT_REUSE_ATOMIC_AUTHORITY_MISSING';
  end if;

  if not pg_catalog.has_function_privilege(
       'service_role',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)',
       'EXECUTE'
     ) then
    raise exception 'CANDIDATE_RECEIPT_REUSE_ATOMIC_ACL_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid)
  into v_index_definition
  from pg_catalog.pg_index index_row
  join pg_catalog.pg_class index_class on index_class.oid=index_row.indexrelid
  join pg_catalog.pg_namespace index_schema on index_schema.oid=index_class.relnamespace
  where index_schema.nspname='public'
    and index_class.relname='candidate_submission_components_source_lineage_idx'
    and index_row.indisvalid
    and index_row.indisready;

  if v_index_definition is null
     or pg_catalog.strpos(
       pg_catalog.lower(v_index_definition),
       '(source_component_id, workflow_id, workflow_generation, expense_category, state)'
     )=0
     or pg_catalog.strpos(
       pg_catalog.lower(v_index_definition),
       'where (source_component_id is not null)'
     )=0 then
    raise exception 'CANDIDATE_RECEIPT_REUSE_LINEAGE_INDEX_INVALID';
  end if;
end;
$verification$;
