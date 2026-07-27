create or replace function public.invoice_async_contract_get_v2()
returns jsonb
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
  with required(identity,procedure_identity) as (
    values
      ('private._invoice_batch_canonical_text_v2(jsonb)',
       to_regprocedure('private._invoice_batch_canonical_text_v2(jsonb)')),
      ('private._invoice_batch_hash_v2(jsonb)',
       to_regprocedure('private._invoice_batch_hash_v2(jsonb)')),
      ('private._invoice_candidate_snapshot_get_v2(text,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_get_v2(text,timestamp with time zone)')),
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
      ('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)',
       to_regprocedure('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamp with time zone)')),
      ('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamp with time zone)')),
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
  )
  select jsonb_build_object(
    'contract_version','INVOICE_ASYNC_DB_V2',
    'ready',
      manifest.missing_count=0
      and manifest.forbidden_dependency_count=0,
    'candidate_query_contract','INVOICE_BATCH_QUERY_V2',
    'candidate_response_contract','INVOICE_BATCH_CANDIDATES_V2',
    'selection_contract','INVOICE_BATCH_SELECTION_V2',
    'selection_root_contract','INVOICE_BATCH_SELECTION_ROOT_V2',
    'progress_contract','INVOICE_BATCH_PROGRESS_V2',
    'function_hash_manifest',manifest.function_hash_manifest,
    'missing_function_count',manifest.missing_count,
    'forbidden_dependency_count',manifest.forbidden_dependency_count
  )
  from manifest;
$function$;

alter function public.invoice_async_contract_get_v2() owner to postgres;
revoke all on function public.invoice_async_contract_get_v2()
  from public,anon,authenticated;
grant execute on function public.invoice_async_contract_get_v2()
  to service_role;
