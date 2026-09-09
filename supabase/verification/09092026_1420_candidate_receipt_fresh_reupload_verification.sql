\set ON_ERROR_STOP on

-- Catalog and retained-data proof for the always-fresh Candidate receipt
-- recovery contract. Behaviour is exercised separately by the rollback-only
-- receipt-reuse verifier.

do $verification$
declare
  v_prepare_definition text;
  v_transition_definition text;
  v_render_definition text;
  v_guard_definition text;
  v_index_definition text;
  v_digest_index_definition text;
begin
  if exists(
    select 1
    from public.candidate_submission_components component
    where component.source_component_id is null
      and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and component.state='PENDING'
      and component.expected_source_content_sha256 is null
  ) then
    raise exception 'CANDIDATE_RECEIPT_UNBOUND_PENDING_TICKET_REMAINS';
  end if;

  select pg_catalog.pg_get_functiondef(
    'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_prepare_definition;
  select pg_catalog.pg_get_functiondef(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_transition_definition;
  select pg_catalog.pg_get_functiondef(
    'private._candidate_component_render_input_v1(uuid,integer,uuid)'::regprocedure
  ) into v_render_definition;
  select pg_catalog.pg_get_functiondef(
    'private._candidate_component_immutability_guard_v1()'::regprocedure
  ) into v_guard_definition;

  if pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       '''expected_source_content_sha256_hex''')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'v_expected_source_digest')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       '''candidate_component_digest_mismatch''')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       '''candidate_component_storage_key_invalid''')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'for update of source_component')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'v_component_kind not in (''mileage_form'',''expense_evidence'')')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'v_expected_source_digest,p_now_utc')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'used_root.source_content_sha256=v_expected_source_digest')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'candidate_submission_components_live_receipt_expected_sha256_uq')=0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'coalesce(v_source_component.storage_key')>0
     or pg_catalog.strpos(pg_catalog.lower(v_prepare_definition),
       'case when v_source_component.id is null then ''pending'' else ''immutable'' end')>0 then
    raise exception 'CANDIDATE_RECEIPT_FRESH_PREPARE_CONTRACT_INVALID';
  end if;

  if pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       'v_component.expected_source_content_sha256<>v_digest')=0
     or pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       'v_component.component_kind in (''mileage_form'',''expense_evidence'')')=0
     or pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       'v_component.storage_key')=0
     or pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       'v_component.byte_size')=0
     or pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       'v_component.media_type')=0
     or pg_catalog.strpos(pg_catalog.lower(v_transition_definition),
       '''candidate_component_media_invalid''')=0 then
    raise exception 'CANDIDATE_RECEIPT_FRESH_COMPLETE_CONTRACT_INVALID';
  end if;

  if pg_catalog.strpos(pg_catalog.lower(v_render_definition),'''abandoned''')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_component.state<>''immutable''')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source.component_kind is distinct from v_component.component_kind')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source.document_role is distinct from v_component.document_role')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source.expense_category is distinct from v_component.expense_category')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'lower(v_source.media_type) is distinct from lower(v_component.media_type)')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source.byte_size is distinct from v_component.byte_size')=0
     or pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source.source_content_sha256 is distinct from v_source_digest')=0 then
    raise exception 'CANDIDATE_RECEIPT_FRESH_RENDER_CONTRACT_INVALID';
  end if;
  if pg_catalog.strpos(pg_catalog.lower(v_render_definition),
       'v_source_digest:=coalesce(')=0 then
    raise exception 'CANDIDATE_NON_RECEIPT_LINEAGE_FALLBACK_REGRESSED';
  end if;

  if pg_catalog.strpos(pg_catalog.lower(v_guard_definition),
       'old.expected_source_content_sha256 is not null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_guard_definition),
       'old.component_kind in (''mileage_form'',''expense_evidence'')')=0
     or pg_catalog.strpos(pg_catalog.lower(v_guard_definition),
       'new.expected_source_content_sha256 is distinct from old.expected_source_content_sha256')=0
     or pg_catalog.strpos(pg_catalog.lower(v_guard_definition),
       'new.upload_idempotency_key is distinct from old.upload_idempotency_key')=0 then
    raise exception 'CANDIDATE_RECEIPT_FRESH_IMMUTABILITY_GUARD_INVALID';
  end if;

  if not exists(
    select 1
    from pg_catalog.pg_trigger trigger_row
    join pg_catalog.pg_class table_row on table_row.oid=trigger_row.tgrelid
    join pg_catalog.pg_namespace table_schema on table_schema.oid=table_row.relnamespace
    join pg_catalog.pg_proc function_row on function_row.oid=trigger_row.tgfoid
    join pg_catalog.pg_namespace function_schema on function_schema.oid=function_row.pronamespace
    where table_schema.nspname='public'
      and table_row.relname='candidate_submission_components'
      and trigger_row.tgname='candidate_submission_components_immutability_guard'
      and not trigger_row.tgisinternal
      and trigger_row.tgenabled<>'D'
      and function_schema.nspname='private'
      and function_row.proname='_candidate_component_immutability_guard_v1'
  ) then
    raise exception 'CANDIDATE_RECEIPT_FRESH_IMMUTABILITY_TRIGGER_MISSING';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid)
  into v_index_definition
  from pg_catalog.pg_index index_row
  join pg_catalog.pg_class index_class on index_class.oid=index_row.indexrelid
  join pg_catalog.pg_namespace index_schema on index_schema.oid=index_class.relnamespace
  where index_schema.nspname='public'
    and index_class.relname='candidate_submission_components_physical_storage_key_uq'
    and index_row.indisunique
    and index_row.indisvalid
    and index_row.indisready;

  if v_index_definition is null
     or pg_catalog.strpos(pg_catalog.lower(v_index_definition),'(storage_key)')=0
     or pg_catalog.strpos(pg_catalog.lower(v_index_definition),
       'storage_key is not null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_index_definition),
       'source_component_id is null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_index_definition),
       'expected_source_content_sha256 is not null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_index_definition),
       'component_kind = any')=0 then
    raise exception 'CANDIDATE_RECEIPT_PHYSICAL_STORAGE_OWNER_INDEX_INVALID';
  end if;

  if exists(
    select 1
    from public.candidate_submission_components component
    where component.storage_key is not null
      and (
        component.source_component_id is null
        or (
          component.expected_source_content_sha256 is not null
          and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        )
      )
    group by component.storage_key
    having count(*)>1
  ) then
    raise exception 'CANDIDATE_RECEIPT_PHYSICAL_STORAGE_OWNER_DUPLICATE';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid)
  into v_digest_index_definition
  from pg_catalog.pg_index index_row
  join pg_catalog.pg_class index_class on index_class.oid=index_row.indexrelid
  join pg_catalog.pg_namespace index_schema on index_schema.oid=index_class.relnamespace
  where index_schema.nspname='public'
    and index_class.relname=
      'candidate_submission_components_live_receipt_expected_sha256_uq'
    and index_row.indisunique
    and index_row.indisvalid
    and index_row.indisready;

  if v_digest_index_definition is null
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       '(expected_source_content_sha256)')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       'expected_source_content_sha256 is not null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       'source_component_id is null')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       'component_kind = any')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       'state = any')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       '''pending''::text')=0
     or pg_catalog.strpos(pg_catalog.lower(v_digest_index_definition),
       '''immutable''::text')=0 then
    raise exception 'CANDIDATE_RECEIPT_LIVE_DIGEST_RESERVATION_INDEX_INVALID';
  end if;

  if exists(
    select 1
    from public.candidate_submission_components component
    where component.expected_source_content_sha256 is not null
      and component.source_component_id is null
      and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and component.state in ('PENDING','IMMUTABLE')
    group by component.expected_source_content_sha256
    having count(*)>1
  ) then
    raise exception 'CANDIDATE_RECEIPT_LIVE_DIGEST_RESERVATION_DUPLICATE';
  end if;

  if pg_catalog.has_function_privilege(
       'public','private._candidate_component_render_input_v1(uuid,integer,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','private._candidate_component_render_input_v1(uuid,integer,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','private._candidate_component_render_input_v1(uuid,integer,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role','private._candidate_component_render_input_v1(uuid,integer,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'public','private._candidate_component_immutability_guard_v1()','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','private._candidate_component_immutability_guard_v1()','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','private._candidate_component_immutability_guard_v1()','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role','private._candidate_component_immutability_guard_v1()','EXECUTE'
     ) then
    raise exception 'CANDIDATE_RECEIPT_PRIVATE_HELPER_ACL_INVALID';
  end if;
end;
$verification$;
