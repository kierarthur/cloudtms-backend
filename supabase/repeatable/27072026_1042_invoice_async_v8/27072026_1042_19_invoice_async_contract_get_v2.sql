create or replace function public.invoice_async_contract_get_v2()
returns jsonb
language sql
stable
security definer
set search_path to 'public','private','extensions','vault','pg_temp'
as $function$
  with required(identity,procedure_identity) as (
    values
      ('private._invoice_batch_canonical_text_v2(jsonb)',
       to_regprocedure('private._invoice_batch_canonical_text_v2(jsonb)')),
      ('private._invoice_batch_hash_v2(jsonb)',
       to_regprocedure('private._invoice_batch_hash_v2(jsonb)')),
      ('private._invoice_candidate_snapshot_get_v2(text,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_get_v2(text,timestamp with time zone)')),
      ('private._invoice_candidate_snapshot_verify_v2(text,jsonb,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_verify_v2(text,jsonb,timestamp with time zone)')),
      ('private._invoice_candidate_snapshot_bump_v2(boolean,boolean,text,text,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_bump_v2(boolean,boolean,text,text,timestamp with time zone)')),
      ('private._invoice_jsonb_pick_v2(jsonb,text[])',
       to_regprocedure('private._invoice_jsonb_pick_v2(jsonb,text[])')),
      ('private._invoice_candidate_revision_trigger_v2()',
       to_regprocedure('private._invoice_candidate_revision_trigger_v2()')),
      ('private._invoice_result_page_revision_trigger_v2()',
       to_regprocedure('private._invoice_result_page_revision_trigger_v2()')),
      ('private._invoice_current_chunk_ids_v2(uuid[],integer)',
       to_regprocedure('private._invoice_current_chunk_ids_v2(uuid[],integer)')),
      ('private._invoice_batch_selection_rules_v2(jsonb)',
       to_regprocedure('private._invoice_batch_selection_rules_v2(jsonb)')),
      ('private._invoice_batch_query_validate_v2(jsonb,text)',
       to_regprocedure('private._invoice_batch_query_validate_v2(jsonb,text)')),
      ('private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz)',
       to_regprocedure('private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamp with time zone)')),
      ('private._invoice_batch_generate_classification_v2(boolean,text[],timestamptz)',
       to_regprocedure('private._invoice_batch_generate_classification_v2(boolean,text[],timestamp with time zone)')),
      ('private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)',
       to_regprocedure('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamp with time zone)')),
      ('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamp with time zone)')),
      ('private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamptz,uuid[])',
       to_regprocedure('private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamp with time zone,uuid[])')),
      ('private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamp with time zone)')),
      ('private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamptz)',
       to_regprocedure('private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamp with time zone)')),
      ('private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz)',
       to_regprocedure('private._invoice_operation_start_core_v8(jsonb,uuid,timestamp with time zone)')),
      ('private._invoice_generation_advance_core_v8(jsonb,timestamptz)',
       to_regprocedure('private._invoice_generation_advance_core_v8(jsonb,timestamp with time zone)')),
      ('private._invoice_issue_advance_core_v8(jsonb,timestamptz)',
       to_regprocedure('private._invoice_issue_advance_core_v8(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_rollup_core_v8(uuid[],timestamptz,boolean)',
       to_regprocedure('private._invoice_operation_rollup_core_v8(uuid[],timestamp with time zone,boolean)')),
      ('private._invoice_operation_get_core_v8(uuid[],uuid,text)',
       to_regprocedure('private._invoice_operation_get_core_v8(uuid[],uuid,text)')),
      ('private._invoice_batch_manifest_advance_v2(jsonb,text,timestamptz)',
       to_regprocedure('private._invoice_batch_manifest_advance_v2(jsonb,text,timestamp with time zone)')),
      ('private._invoice_dispatch_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_dispatch_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_candidate_triggers_install_v2()',
       to_regprocedure('private._invoice_candidate_triggers_install_v2()')),
      ('public.invoice_batch_generate_candidates(jsonb)',
       to_regprocedure('public.invoice_batch_generate_candidates(jsonb)')),
      ('public.invoice_batch_issue_candidates(jsonb)',
       to_regprocedure('public.invoice_batch_issue_candidates(jsonb)')),
      ('public.invoice_operation_start_batch(jsonb,uuid,timestamptz)',
       to_regprocedure('public.invoice_operation_start_batch(jsonb,uuid,timestamp with time zone)')),
      ('public.invoice_work_claim_batch(text[],text,integer,integer,timestamptz)',
       to_regprocedure('public.invoice_work_claim_batch(text[],text,integer,integer,timestamp with time zone)')),
      ('public.invoice_operation_advance_batch(jsonb,timestamptz)',
       to_regprocedure('public.invoice_operation_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_generation_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_generation_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_issue_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_issue_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_rollup_batch(uuid[],timestamptz,boolean)',
       to_regprocedure('private._invoice_operation_rollup_batch(uuid[],timestamp with time zone,boolean)')),
      ('public.invoice_operation_get(uuid[],uuid,text,jsonb)',
       to_regprocedure('public.invoice_operation_get(uuid[],uuid,text,jsonb)')),
      ('public.invoice_operation_control_batch(jsonb,uuid,timestamptz)',
       to_regprocedure('public.invoice_operation_control_batch(jsonb,uuid,timestamp with time zone)'))
  ),
  definitions as (
    select
      required.identity,
      required.procedure_identity,
      case
        when required.procedure_identity is not null
          then pg_get_functiondef(required.procedure_identity::oid)
      end definition
    from required
  ),
  manifest as (
    select
      count(*) filter (
        where procedure_identity is null
      ) missing_count,
      count(*) filter (
        where definition like '%_legacy_20260726%'
      ) forbidden_dependency_count,
      count(*) filter (
        where identity like 'private._invoice_batch_%candidate%rows_v2(%'
          and (
            definition like '%public.invoice_batch_generate_candidates(%'
            or definition like '%public.invoice_batch_issue_candidates(%'
          )
      ) public_candidate_dependency_count,
      encode(
        extensions.digest(
          convert_to(
            string_agg(
              identity||E'\n'||coalesce(definition,'MISSING'),
              E'\n--\n'
              order by identity
            ),
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      ) function_hash_manifest
    from definitions
  ),
  security_state as (
    select count(*) filter (
      where definitions.identity like 'private.%'
        and (
          has_function_privilege(
            'public',
            definitions.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'anon',
            definitions.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'authenticated',
            definitions.procedure_identity,
            'EXECUTE'
          )
        )
    ) private_exposure_count
    from definitions
    where definitions.procedure_identity is not null
  ),
  legacy_surface_state as (
    select
      count(*) filter (
      where legacy.procedure_identity is not null
        and (
          has_function_privilege(
            'public',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'anon',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'authenticated',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'service_role',
            legacy.procedure_identity,
            'EXECUTE'
          )
        )
      ) legacy_runtime_exposure_count,
      count(*) filter (
        where legacy.procedure_identity is not null
          and coalesce(p.pronargdefaults, 0) <> 0
      ) legacy_rest_overload_ambiguity_count
    from (
      values
        (to_regprocedure(
          'public.invoice_batch_generate_candidates(boolean,integer,text[],jsonb)'
        )),
        (to_regprocedure(
          'public.invoice_batch_issue_candidates(boolean,integer,jsonb)'
        ))
    ) legacy(procedure_identity)
    left join pg_proc p on p.oid = legacy.procedure_identity
  ),
  key_state as (
    select exists (
      select 1
      from private.invoice_async_snapshot_hmac_keys k
      join vault.decrypted_secrets s on s.id=k.vault_secret_id
      where k.is_current
        and k.active_from_utc<=statement_timestamp()
        and (k.active_to_utc is null or k.active_to_utc>statement_timestamp())
        and nullif(s.decrypted_secret,'') is not null
    ) snapshot_key_ready
  ),
  trigger_state as (
    select
      count(*) filter (
        where left(
                t.tgname,
                length('trg_invoice_candidate_revision_v2_')
              ) = 'trg_invoice_candidate_revision_v2_'
          and p.oid=to_regprocedure(
            'private._invoice_candidate_revision_trigger_v2()'
          )::oid
      ) candidate_trigger_count,
      count(*) filter (
        where left(
                t.tgname,
                length('trg_invoice_result_page_revision_v2_')
              ) = 'trg_invoice_result_page_revision_v2_'
          and p.oid=to_regprocedure(
            'private._invoice_result_page_revision_trigger_v2()'
          )::oid
      ) result_trigger_count
    from pg_trigger t
    join pg_proc p on p.oid=t.tgfoid
    where not t.tgisinternal
  ),
  index_state as (
    select bool_and(coalesce(i.indisvalid,false) and coalesce(i.indisready,false))
      indexes_ready
    from (
      values
        ('idx_invoice_manifest_carrier_identity_v8'),
        ('idx_invoice_batch_result_all_v8'),
        ('idx_invoice_batch_result_category_v8'),
        ('idx_invoice_operation_chunks_claim_v8'),
        ('idx_invoice_operation_control_receipt_actor_token_v8')
    ) required(index_name)
    left join pg_class c
      on c.relname=required.index_name
     and c.relnamespace='public'::regnamespace
    left join pg_index i on i.indexrelid=c.oid
  )
  select jsonb_build_object(
    'contract_version','INVOICE_ASYNC_DB_V2',
    'ready',
      manifest.missing_count=0
      and manifest.forbidden_dependency_count=0
      and manifest.public_candidate_dependency_count=0
      and security_state.private_exposure_count=0
      and legacy_surface_state.legacy_runtime_exposure_count=0
      and legacy_surface_state.legacy_rest_overload_ambiguity_count=0
      and key_state.snapshot_key_ready
      and trigger_state.candidate_trigger_count=54
      and trigger_state.result_trigger_count=3
      and coalesce(index_state.indexes_ready,false),
    'candidate_query_contract','INVOICE_BATCH_QUERY_V2',
    'candidate_response_contract','INVOICE_BATCH_CANDIDATES_V2',
    'selection_contract','INVOICE_BATCH_SELECTION_V2',
    'selection_root_contract','INVOICE_BATCH_SELECTION_ROOT_V2',
    'progress_contract','INVOICE_BATCH_PROGRESS_V2',
    'function_hash_manifest',manifest.function_hash_manifest,
    'missing_function_count',manifest.missing_count,
    'forbidden_dependency_count',manifest.forbidden_dependency_count,
    'public_candidate_dependency_count',
      manifest.public_candidate_dependency_count,
    'private_exposure_count',security_state.private_exposure_count,
    'legacy_runtime_exposure_count',
      legacy_surface_state.legacy_runtime_exposure_count,
    'legacy_rest_overload_ambiguity_count',
      legacy_surface_state.legacy_rest_overload_ambiguity_count,
    'trigger_manifest_digest',
      '39a4c76d0f0a757a2ee04e25ec05df74b1ccf5531393ecdb2b30b360bf4ba5b0',
    'snapshot_signing_ready',key_state.snapshot_key_ready,
    'candidate_trigger_count',trigger_state.candidate_trigger_count,
    'result_trigger_count',trigger_state.result_trigger_count,
    'operation_control_idempotency_ready',
      coalesce(index_state.indexes_ready,false),
    'indexes_ready',coalesce(index_state.indexes_ready,false)
  )
  from manifest
  cross join security_state
  cross join legacy_surface_state
  cross join key_state
  cross join trigger_state
  cross join index_state;
$function$;

alter function public.invoice_async_contract_get_v2() owner to postgres;
revoke all on function public.invoice_async_contract_get_v2()
  from public,anon,authenticated;
grant execute on function public.invoice_async_contract_get_v2()
  to service_role;
