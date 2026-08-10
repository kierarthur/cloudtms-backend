-- Banking Pay Workbench: publish one exact, certified bounded CURRENT source
-- into the public preview read model.
--
-- Policy X: this function is pre-draft live-truth publication only.  It copies
-- already-certified economics byte-for-byte and never calculates or mutates
-- post-draft payment evidence.

CREATE OR REPLACE FUNCTION private.pay_workbench_publish_certified_source_preview_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_economic_build_id uuid,
  p_source_build_run_id uuid,
  p_source_change_seq bigint,
  p_session_version bigint,
  p_completion_job_id uuid,
  p_refresh_scope_kind text,
  p_targeted_timesheet_ids jsonb DEFAULT '[]'::jsonb,
  p_linked_timesheet_ids jsonb DEFAULT '[]'::jsonb,
  p_publication_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_cancellation_work_item public.pay_payment_correction_work_items%ROWTYPE;
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_candidate_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_authority_kind text := UPPER(BTRIM(COALESCE(
    p_publication_options_json->>'authority_kind',
    'BOUNDED_FULL_SOURCE_BUILD'
  )));
  v_invocation_kind text := UPPER(BTRIM(COALESCE(
    p_publication_options_json->>'invocation_kind',
    'AUTO'
  )));
  v_refresh_scope_kind text := UPPER(BTRIM(COALESCE(p_refresh_scope_kind, 'CANDIDATE_FULL_LIVE')));
  v_source_count integer := 0;
  v_source_digest text := NULL;
  v_source_identity_digest text := NULL;
  v_preview_count integer := 0;
  v_preview_identity_digest text := NULL;
  v_selectable_count integer := 0;
  v_context_count integer := 0;
  v_selected_count integer := 0;
  v_upserted_count integer := 0;
  v_retired_count integer := 0;
  v_source_minus_preview integer := 0;
  v_preview_minus_source integer := 0;
  v_live_source_change_seq bigint := 0;
  v_scope_ordinal bigint := 0;
  v_explicit_selection boolean := false;
  v_selected_ids jsonb := '[]'::jsonb;
  v_section_counts jsonb := '{}'::jsonb;
  v_attestation jsonb := '{}'::jsonb;
  v_unknown_option_count integer := 0;
  v_invalid_scope_id_count integer := 0;
  v_unmatched_target_id_count integer := 0;
  v_row_contract_error_count integer := 0;
  v_already_current boolean := false;
  v_contract_version integer := CASE
    WHEN COALESCE(p_publication_options_json->>'contract_version','1') ~ '^[123]$'
      THEN (COALESCE(p_publication_options_json->>'contract_version','1'))::integer
    ELSE -1
  END;
  v_final_state text := UPPER(BTRIM(COALESCE(p_publication_options_json->>'final_state','READY')));
  v_certification_version text := NULLIF(BTRIM(COALESCE(p_publication_options_json->>'certification_version','')),'');
  v_certification_digest text := NULLIF(BTRIM(COALESCE(p_publication_options_json->>'certification_digest','')),'');
  v_projection_run public.banking_pay_workbench_candidate_delta_projection_runs%ROWTYPE;
  v_admission_seal_version integer := NULL::integer;
  v_admission_seal_digest text := NULL::text;
  v_projection_fingerprint text := NULL::text;
  v_original_economic_build_id uuid := NULL::uuid;
  v_original_source_build_run_id uuid := NULL::uuid;
  v_source_session_id uuid := NULL::uuid;
  v_clone_fence jsonb := '{}'::jsonb;
  v_min_public_ordinal bigint := NULL::bigint;
  v_max_public_ordinal bigint := NULL::bigint;
  v_semantic_proof jsonb := '{}'::jsonb;
  v_semantic_candidate_proof jsonb := '{}'::jsonb;
  v_semantic_ready boolean := false;
  v_semantic_proof_digest text := NULL::text;
  v_selection_recovery_overlay jsonb := '{}'::jsonb;
  v_ordinary_positive_selectable_count integer := 0;
  v_ordinary_positive_amount numeric := 0;
  v_recognised_deduction_count integer := 0;
  v_recognised_deduction_amount numeric := 0;
  v_usable_same_candidate_headroom numeric := 0;
  v_candidate_ready_amount numeric := 0;
  v_invalid_selectable_row_count integer := 0;
  v_cancellation_request_id uuid := NULL::uuid;
  v_cancellation_operation_id uuid := NULL::uuid;
  v_cancellation_work_item_id uuid := NULL::uuid;
  v_cancellation_reversion_run_id uuid := NULL::uuid;
  v_financial_reversion_digest text := NULL::text;
  v_cancellation_route text := UPPER(BTRIM(COALESCE(
    p_publication_options_json->>'cancellation_route','PRE_BANK_CANCEL'
  )));
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_economic_build_id IS NULL
     OR p_source_build_run_id IS NULL OR p_completion_job_id IS NULL THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH')::text;
  END IF;

  IF p_source_change_seq IS NULL OR p_source_change_seq < 0
     OR p_session_version IS NULL OR p_session_version <= 0 THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
              'reason', 'INVALID_VERSION_OR_SEQUENCE'
            )::text;
  END IF;

  IF jsonb_typeof(COALESCE(p_publication_options_json, '{}'::jsonb)) <> 'object'
     OR jsonb_typeof(COALESCE(p_targeted_timesheet_ids, '[]'::jsonb)) <> 'array'
     OR jsonb_typeof(COALESCE(p_linked_timesheet_ids, '[]'::jsonb)) <> 'array' THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
              'reason', 'JSON_ARGUMENT_SHAPE_INVALID'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_unknown_option_count
  FROM jsonb_object_keys(COALESCE(p_publication_options_json, '{}'::jsonb)) AS option_key(key)
  WHERE option_key.key NOT IN (
    'contract_version','authority_kind','invocation_kind','final_state',
    'certification_version','certification_digest','post_clone_action','post_clone_job_id',
    'source_session_id','original_economic_build_id','original_source_build_run_id','clone_job_id',
    'projection_run_id','admission_seal_version','admission_seal_digest',
    'projection_fingerprint','accepted_baseline_build_id',
    'semantic_contract_version','semantic_proof_digest',
    'cancellation_request_id','cancellation_operation_id','cancellation_work_item_id',
    'pay_batch_id','cancellation_reversion_run_id','financial_reversion_digest','source_count',
    'cancellation_route'
  );

  IF v_unknown_option_count > 0 OR v_contract_version NOT IN (1,2,3)
     OR (
       v_contract_version=1
       AND (
         EXISTS (
           SELECT 1 FROM jsonb_object_keys(COALESCE(p_publication_options_json,'{}'::jsonb)) AS v1_key(key)
           WHERE v1_key.key NOT IN ('authority_kind','invocation_kind','contract_version')
         )
         OR v_authority_kind <> 'BOUNDED_FULL_SOURCE_BUILD'
         OR v_invocation_kind NOT IN ('AUTO','INITIAL_COMPLETION','DUPLICATE_REPLAY_REPAIR')
       )
     )
     OR (
       v_contract_version=2
       AND (
         v_authority_kind NOT IN ('CERTIFIED_CLONE','TARGETED_DELTA')
         OR v_final_state NOT IN ('READY','SOURCE_EMPTY','PENDING_DELTA')
         OR v_invocation_kind NOT IN ('CLONE_OWNER_FINALISE','DELTA_FINALISE')
       )
     )
     OR (
       v_contract_version=3
       AND (
         COALESCE(p_publication_options_json->>'semantic_contract_version','')
           IS DISTINCT FROM 'READY_TO_PAY_SEMANTIC_V2'
         OR v_authority_kind NOT IN (
           'BOUNDED_FULL_SOURCE_BUILD','CERTIFIED_CLONE','TARGETED_DELTA',
           'CERTIFIED_CANCELLATION_REVERSION'
         )
         OR v_final_state NOT IN ('READY','SOURCE_EMPTY','PENDING_DELTA')
         OR (
           (v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
             AND v_invocation_kind NOT IN ('AUTO','INITIAL_COMPLETION','DUPLICATE_REPLAY_REPAIR'))
           OR (v_authority_kind='CERTIFIED_CLONE'
             AND v_invocation_kind<>'CLONE_OWNER_FINALISE')
           OR (v_authority_kind='TARGETED_DELTA'
             AND v_invocation_kind<>'DELTA_FINALISE')
           OR (v_authority_kind='CERTIFIED_CANCELLATION_REVERSION'
             AND v_invocation_kind<>'CANCELLATION_REVERSION_FINALISE')
         )
       )
     ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
              'reason', 'PUBLICATION_OPTIONS_NOT_ALLOWED'
            )::text;
  END IF;

  IF v_refresh_scope_kind NOT IN ('CANDIDATE_FULL_LIVE', 'TARGETED_TIMESHEETS') THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING',
              'reason', 'REFRESH_SCOPE_KIND_INVALID'
            )::text;
  END IF;

  -- Canonical lock order: owning job/work item -> candidate -> registry ->
  -- build -> session -> scope -> candidate state -> source -> preview.
  IF v_contract_version=3 AND v_authority_kind='CERTIFIED_CANCELLATION_REVERSION' THEN
    IF COALESCE(p_publication_options_json->>'cancellation_request_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'cancellation_operation_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR v_cancellation_route NOT IN ('PRE_BANK_CANCEL','DRAFT_OVERLAY_FAST')
       OR (
         v_cancellation_route='PRE_BANK_CANCEL'
         AND COALESCE(p_publication_options_json->>'cancellation_work_item_id','')
               IS DISTINCT FROM p_completion_job_id::text
       )
       OR (
         v_cancellation_route='DRAFT_OVERLAY_FAST'
         AND p_completion_job_id::text
               IS DISTINCT FROM COALESCE(p_publication_options_json->>'cancellation_operation_id','')
       )
       OR COALESCE(p_publication_options_json->>'pay_batch_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'cancellation_reversion_run_id','')
         IS DISTINCT FROM p_source_build_run_id::text
       OR NULLIF(BTRIM(COALESCE(p_publication_options_json->>'financial_reversion_digest','')),'') IS NULL THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
          'reason','CANCELLATION_OPTIONS_INCOMPLETE'
        )::text;
    END IF;

    v_cancellation_request_id := (p_publication_options_json->>'cancellation_request_id')::uuid;
    v_cancellation_operation_id := (p_publication_options_json->>'cancellation_operation_id')::uuid;
    v_cancellation_work_item_id := CASE WHEN v_cancellation_route='PRE_BANK_CANCEL'
      THEN p_completion_job_id ELSE NULL::uuid END;
    v_cancellation_reversion_run_id := p_source_build_run_id;
    v_financial_reversion_digest := p_publication_options_json->>'financial_reversion_digest';

    IF v_cancellation_route='PRE_BANK_CANCEL' THEN
      SELECT work_item.*
      INTO v_cancellation_work_item
      FROM public.pay_payment_correction_work_items AS work_item
      WHERE work_item.id=p_completion_job_id
      FOR UPDATE;

      IF NOT FOUND
         OR v_cancellation_work_item.correction_request_id IS DISTINCT FROM v_cancellation_request_id
         OR v_cancellation_work_item.candidate_id IS DISTINCT FROM p_candidate_id
         OR v_cancellation_work_item.pay_batch_id::text
              IS DISTINCT FROM p_publication_options_json->>'pay_batch_id'
         OR UPPER(BTRIM(COALESCE(v_cancellation_work_item.work_kind,''))) <> 'PRE_BANK_CANCEL'
         OR UPPER(BTRIM(COALESCE(v_cancellation_work_item.status,''))) <> 'APPLIED'
         OR COALESCE(v_cancellation_work_item.result_json->>'result_code','') <> 'APPLIED'
         OR NOT EXISTS (
           SELECT 1 FROM public.banking_pay_operations AS correction_operation
           WHERE correction_operation.id=v_cancellation_operation_id
             AND correction_operation.operation_type='PAYMENT_CORRECTION'
             AND correction_operation.pay_batch_id=v_cancellation_work_item.pay_batch_id
             AND correction_operation.input_json->>'correction_request_id'=v_cancellation_request_id::text
         )
         OR pg_catalog.md5((COALESCE(v_cancellation_work_item.result_json,'{}'::jsonb)
               - 'applied_at_utc' - 'processed_at_utc')::text)
              IS DISTINCT FROM v_financial_reversion_digest THEN
        RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
          USING ERRCODE='P0001',DETAIL=jsonb_build_object(
            'code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
            'reason','CANCELLATION_WORK_ITEM_NOT_EXACT'
          )::text;
      END IF;
    ELSE
      IF NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operations AS correction_operation
        JOIN public.pay_payment_correction_requests AS correction_request
          ON correction_request.id=v_cancellation_request_id
         AND correction_request.pay_batch_id=correction_operation.pay_batch_id
        JOIN public.pay_payment_correction_request_candidates AS request_candidate
          ON request_candidate.correction_request_id=correction_request.id
        JOIN public.pay_batch_candidates AS batch_candidate
          ON batch_candidate.id=request_candidate.pay_batch_candidate_id
         AND batch_candidate.pay_batch_id=correction_request.pay_batch_id
         AND batch_candidate.candidate_id=p_candidate_id
        WHERE correction_operation.id=v_cancellation_operation_id
          AND correction_operation.operation_type='PAYMENT_CORRECTION'
          AND correction_operation.pay_batch_id::text
                =p_publication_options_json->>'pay_batch_id'
          AND correction_operation.input_json->>'correction_request_id'
                =v_cancellation_request_id::text
          AND COALESCE(correction_request.plan_json->>'requested_action',
                       correction_request.selection_json->>'requested_action')='DRAFT_CANCEL'
          AND NOT EXISTS (
            SELECT 1
            FROM public.pay_payment_correction_work_items AS financial_work
            WHERE financial_work.correction_request_id=correction_request.id
          )
      ) THEN
        RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
          USING ERRCODE='P0001',DETAIL=jsonb_build_object(
            'code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
            'reason','DRAFT_OVERLAY_CANCELLATION_AUTHORITY_NOT_EXACT'
          )::text;
      END IF;
    END IF;
  ELSE
    SELECT job_row.*
    INTO v_job
    FROM public.banking_pay_workbench_jobs AS job_row
    WHERE job_row.id = p_completion_job_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_job.session_id IS DISTINCT FROM p_session_id
       OR (
         v_job.candidate_id IS DISTINCT FROM p_candidate_id
         AND NOT (v_contract_version IN (2,3) AND v_authority_kind='CERTIFIED_CLONE' AND v_job.candidate_id IS NULL)
       )
       OR UPPER(BTRIM(COALESCE(v_job.status, ''))) NOT IN ('RUNNING', 'SUCCEEDED')
       OR (
         v_contract_version IN (1,3) AND v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
         AND (
           v_job.economic_build_id IS DISTINCT FROM p_economic_build_id
           OR UPPER(BTRIM(COALESCE(v_job.job_type, ''))) NOT IN (
             'WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
             'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE','CANDIDATE_SOURCE_BUILD',
             'CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
           )
         )
       )
       OR (
         v_contract_version IN (2,3) AND v_authority_kind='CERTIFIED_CLONE'
         AND (
           UPPER(BTRIM(COALESCE(v_job.job_type,''))) NOT IN (
             'WORKBENCH_SESSION_CLONE_REBASE','SESSION_CLONE_REBASE','CLONE_REBASE',
             'WORKBENCH_CANDIDATE_DIRTY_APPLY'
           )
           OR COALESCE(p_publication_options_json->>'clone_job_id','') IS DISTINCT FROM p_completion_job_id::text
         )
       )
       OR (
         v_contract_version IN (2,3) AND v_authority_kind='TARGETED_DELTA'
         AND (
           UPPER(BTRIM(COALESCE(v_job.job_type,''))) NOT IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH','CANDIDATE_DELTA_REFRESH','DELTA_REFRESH')
           OR COALESCE(p_publication_options_json->>'projection_run_id','') IS DISTINCT FROM p_source_build_run_id::text
         )
       ) THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
          'reason', 'COMPLETION_JOB_NOT_EXACT'
        )::text;
    END IF;
  END IF;

  PERFORM 1
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING')::text;
  END IF;

  SELECT registry_row.*
  INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
  WHERE registry_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND OR v_registry.current_source_change_seq IS DISTINCT FROM p_source_change_seq THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE')::text;
  END IF;

  IF v_contract_version IN (2,3) AND v_authority_kind='CERTIFIED_CLONE' THEN
    IF COALESCE(p_publication_options_json->>'source_session_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'original_economic_build_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'original_source_build_run_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR v_certification_version IS NULL OR v_certification_digest IS NULL THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH','reason','CLONE_OPTIONS_INCOMPLETE')::text;
    END IF;
    v_source_session_id := (p_publication_options_json->>'source_session_id')::uuid;
    v_original_economic_build_id := (p_publication_options_json->>'original_economic_build_id')::uuid;
    v_original_source_build_run_id := (p_publication_options_json->>'original_source_build_run_id')::uuid;
  ELSIF v_contract_version IN (2,3) AND v_authority_kind='TARGETED_DELTA' THEN
    v_admission_seal_version := CASE WHEN COALESCE(p_publication_options_json->>'admission_seal_version','') ~ '^\d+$' THEN (p_publication_options_json->>'admission_seal_version')::integer END;
    v_admission_seal_digest := NULLIF(BTRIM(COALESCE(p_publication_options_json->>'admission_seal_digest','')),'');
    v_projection_fingerprint := NULLIF(BTRIM(COALESCE(p_publication_options_json->>'projection_fingerprint','')),'');
    IF v_admission_seal_version IS NULL OR v_admission_seal_digest IS NULL
       OR v_projection_fingerprint IS NULL
       OR COALESCE(p_publication_options_json->>'accepted_baseline_build_id','') IS DISTINCT FROM p_economic_build_id::text THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH','reason','DELTA_OPTIONS_INCOMPLETE')::text;
    END IF;
  ELSIF v_contract_version=3 AND v_authority_kind='CERTIFIED_CANCELLATION_REVERSION' THEN
    IF COALESCE(p_publication_options_json->>'source_session_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'original_economic_build_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(p_publication_options_json->>'original_source_build_run_id','')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR NULLIF(BTRIM(COALESCE(p_publication_options_json->>'semantic_proof_digest','')),'') IS NULL THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
          'reason','CANCELLATION_LINEAGE_INCOMPLETE'
        )::text;
    END IF;
    v_source_session_id := (p_publication_options_json->>'source_session_id')::uuid;
    v_original_economic_build_id := (p_publication_options_json->>'original_economic_build_id')::uuid;
    v_original_source_build_run_id := (p_publication_options_json->>'original_source_build_run_id')::uuid;
  END IF;

  SELECT build_row.*
  INTO v_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.id = p_economic_build_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_build.candidate_id IS DISTINCT FROM p_candidate_id
     OR (
       v_contract_version IN (1,3) AND v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
       AND (
         v_build.session_id IS DISTINCT FROM p_session_id
         OR v_build.session_version IS DISTINCT FROM p_session_version
         OR v_build.source_build_run_id IS DISTINCT FROM p_source_build_run_id
         OR v_build.source_change_seq IS DISTINCT FROM p_source_change_seq
         OR v_build.source_job_id IS DISTINCT FROM p_completion_job_id
       )
     )
     OR (
       v_contract_version IN (2,3) AND v_authority_kind='CERTIFIED_CLONE'
       AND (
         v_original_economic_build_id IS DISTINCT FROM p_economic_build_id
         OR v_build.source_build_run_id IS DISTINCT FROM v_original_source_build_run_id
         OR v_build.session_id IS DISTINCT FROM v_source_session_id
       )
     )
     OR (
       v_contract_version IN (2,3) AND v_authority_kind='TARGETED_DELTA'
       AND v_build.id IS DISTINCT FROM p_economic_build_id
     )
     OR (
       v_contract_version=3 AND v_authority_kind='CERTIFIED_CANCELLATION_REVERSION'
       AND (
         v_build.id IS DISTINCT FROM v_original_economic_build_id
         OR v_build.source_build_run_id IS DISTINCT FROM v_original_source_build_run_id
         OR v_build.session_id IS DISTINCT FROM v_source_session_id
         OR v_build.candidate_id IS DISTINCT FROM p_candidate_id
       )
     ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH')::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_build.status, ''))) <> 'COMPLETE'
     OR UPPER(BTRIM(COALESCE(v_build.private_stage, ''))) <> 'COMPLETE'
     OR v_build.completed_at_utc IS NULL
     OR v_build.reconciled_at_utc IS NULL
     OR v_build.failed_at_utc IS NOT NULL
     OR COALESCE(v_build.failure_json, '{}'::jsonb) <> '{}'::jsonb
     OR v_build.dependency_closure_sealed_at_utc IS NULL
     OR v_build.dependency_edge_stream_complete IS NOT TRUE
     OR v_build.edge_tag_stream_complete IS NOT TRUE
     OR NULLIF(BTRIM(COALESCE(v_build.sealed_fingerprint_digest, '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_build.canonical_digest, '')), '') IS NULL
     OR jsonb_typeof(COALESCE(v_build.attestation_json, '{}'::jsonb)) <> 'object'
     OR COALESCE(v_build.attestation_json, '{}'::jsonb) = '{}'::jsonb
     OR LOWER(BTRIM(COALESCE(v_build.attestation_json->>'effect_plan_sealed', 'false')))
          NOT IN ('true', 't', '1', 'yes', 'y', 'on')
     OR NULLIF(BTRIM(COALESCE(v_build.attestation_json->>'effect_plan_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_build.attestation_json->>'observed_finance_effect_digest', '')), '') IS NULL THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_NOT_COMPLETE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_BUILD_NOT_COMPLETE')::text;
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND
     OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.version IS DISTINCT FROM p_session_version
     OR (v_contract_version IN (1,3) AND v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
       AND v_session.source_snapshot_run_id IS DISTINCT FROM v_build.source_snapshot_run_id) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SESSION_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SESSION_STALE')::text;
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING')::text;
  END IF;
  v_scope_ordinal := v_scope.scope_ordinal;

  SELECT candidate_state.*
  INTO v_candidate_state
  FROM public.banking_pay_workbench_session_candidate_state AS candidate_state
  WHERE candidate_state.session_id = p_session_id
    AND candidate_state.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_candidate_state.session_version IS DISTINCT FROM p_session_version
     OR v_candidate_state.source_change_seq IS DISTINCT FROM p_source_change_seq THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE')::text;
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_source_change_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  IF v_live_source_change_seq IS DISTINCT FROM p_source_change_seq THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE')::text;
  END IF;

  IF v_contract_version IN (2,3) AND v_authority_kind='TARGETED_DELTA' THEN
    SELECT projection_row.*
    INTO v_projection_run
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_row
    WHERE projection_row.id=p_source_build_run_id
      AND projection_row.session_id=p_session_id
      AND projection_row.candidate_id=p_candidate_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_projection_run.session_version IS DISTINCT FROM p_session_version
       OR v_projection_run.source_change_seq IS DISTINCT FROM p_source_change_seq
       OR v_projection_run.admission_seal_version IS DISTINCT FROM v_admission_seal_version
       OR v_projection_run.admission_seal_digest IS DISTINCT FROM v_admission_seal_digest
       OR v_projection_run.projection_fingerprint IS DISTINCT FROM v_projection_fingerprint
       OR v_projection_run.admission_seal_json->>'accepted_build_id' IS DISTINCT FROM p_economic_build_id::text
       OR UPPER(BTRIM(COALESCE(v_projection_run.status,''))) NOT IN ('RUNNING','PROCESSING','WRITING','READY') THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH','reason','DELTA_AUTHORITY_NOT_EXACT')::text;
    END IF;
  ELSIF v_contract_version IN (2,3) AND v_authority_kind='CERTIFIED_CLONE' THEN
    v_clone_fence := private.pay_workbench_session_clone_bounded_certification_v1(
      v_source_session_id,
      p_session_id,
      p_candidate_id,
      jsonb_build_object(
        'certification_version',v_certification_version,
        'expected_certification_digest',v_certification_digest,
        'expected_economic_build_id',p_economic_build_id,
        'expected_source_build_run_id',v_original_source_build_run_id,
        'final_fence',true,
        'caller_holds_candidate_and_session_locks',true
      )
    );

    IF COALESCE((v_clone_fence->>'clone_eligible')::boolean,false) IS NOT TRUE
       OR COALESCE(v_clone_fence->>'certification_digest','') IS DISTINCT FROM v_certification_digest
       OR COALESCE(v_clone_fence->>'economic_build_id',v_clone_fence->>'original_economic_build_id','') IS DISTINCT FROM p_economic_build_id::text
       OR COALESCE(v_clone_fence->>'source_build_run_id',v_clone_fence->>'original_source_build_run_id','') IS DISTINCT FROM v_original_source_build_run_id::text THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH','reason','CLONE_CERTIFICATION_STALE')::text;
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_economic_builds AS newer_build
    WHERE newer_build.session_id = p_session_id
      AND newer_build.candidate_id = p_candidate_id
      AND newer_build.session_version = p_session_version
      AND newer_build.id <> p_economic_build_id
      AND newer_build.obsolete_at_utc IS NULL
      AND newer_build.failed_at_utc IS NULL
      AND (
        newer_build.source_change_seq > p_source_change_seq
        OR (
          newer_build.source_change_seq = p_source_change_seq
          AND newer_build.created_at_utc > v_build.created_at_utc
          AND UPPER(BTRIM(COALESCE(newer_build.status, ''))) NOT IN ('FAILED', 'OBSOLETE')
        )
      )
  ) OR EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS newer_job
    WHERE newer_job.session_id = p_session_id
      AND newer_job.candidate_id = p_candidate_id
      AND newer_job.id <> p_completion_job_id
      AND UPPER(BTRIM(COALESCE(newer_job.status, ''))) IN ('QUEUED', 'RUNNING')
      AND UPPER(BTRIM(COALESCE(newer_job.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      )
      AND (
        COALESCE(newer_job.payload_json->>'source_change_seq', '') !~ '^[0-9]{1,18}$'
        OR (newer_job.payload_json->>'source_change_seq')::bigint >= p_source_change_seq
      )
  ) OR EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.session_id = p_session_id
      AND (v_authority_kind<>'TARGETED_DELTA' OR projection_run.id IS DISTINCT FROM p_source_build_run_id)
      AND projection_run.candidate_id = p_candidate_id
      AND projection_run.session_version = p_session_version
      AND projection_run.source_change_seq >= p_source_change_seq
      AND UPPER(BTRIM(COALESCE(projection_run.status, ''))) IN (
        'PENDING', 'QUEUED', 'RUNNING', 'PROCESSING', 'WRITING', 'READY'
      )
  ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_NEWER_WORK_PRESENT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_NEWER_WORK_PRESENT')::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_invalid_scope_id_count
  FROM (
    SELECT value
    FROM jsonb_array_elements_text(COALESCE(p_targeted_timesheet_ids, '[]'::jsonb))
    UNION ALL
    SELECT value
    FROM jsonb_array_elements_text(COALESCE(p_linked_timesheet_ids, '[]'::jsonb))
  ) AS scope_value
  WHERE NULLIF(BTRIM(scope_value.value), '') IS NULL
     OR scope_value.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  IF v_invalid_scope_id_count > 0 THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING',
              'reason', 'INVALID_TIMESHEET_SCOPE_ID'
            )::text;
  END IF;

  IF v_refresh_scope_kind = 'TARGETED_TIMESHEETS' THEN
    WITH requested_ids AS (
      SELECT DISTINCT value::uuid AS timesheet_id
      FROM (
        SELECT value FROM jsonb_array_elements_text(COALESCE(p_targeted_timesheet_ids, '[]'::jsonb))
        UNION ALL
        SELECT value FROM jsonb_array_elements_text(COALESCE(p_linked_timesheet_ids, '[]'::jsonb))
      ) AS raw_ids
    )
    SELECT COUNT(*)::integer
    INTO v_unmatched_target_id_count
    FROM requested_ids
    WHERE NOT EXISTS (
      SELECT 1
      FROM private.banking_pay_workbench_economic_build_scope AS build_scope
      WHERE build_scope.build_id = p_economic_build_id
        AND build_scope.timesheet_id = requested_ids.timesheet_id
    )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_source_lines AS source_scope
        WHERE source_scope.session_id = p_session_id
          AND source_scope.candidate_id = p_candidate_id
          AND source_scope.session_version = p_session_version
          AND source_scope.source_build_run_id = p_source_build_run_id
          AND source_scope.source_change_seq = p_source_change_seq
          AND source_scope.status = 'CURRENT'
          AND source_scope.timesheet_id = requested_ids.timesheet_id
      );

    IF v_unmatched_target_id_count > 0
       OR (
         jsonb_array_length(COALESCE(p_targeted_timesheet_ids, '[]'::jsonb))
         + jsonb_array_length(COALESCE(p_linked_timesheet_ids, '[]'::jsonb))
       ) = 0 THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING',
                'reason', 'TARGETED_SCOPE_NOT_CERTIFIED'
              )::text;
    END IF;
  END IF;

  PERFORM source_lock.id
  FROM public.banking_pay_workbench_candidate_source_lines AS source_lock
  WHERE source_lock.session_id = p_session_id
    AND source_lock.candidate_id = p_candidate_id
    AND source_lock.session_version = p_session_version
    AND source_lock.status = 'CURRENT'
  ORDER BY source_lock.source_ordinal, source_lock.id
  FOR UPDATE;

  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_candidate_source_lines AS ambiguous_source
    WHERE ambiguous_source.session_id = p_session_id
      AND ambiguous_source.candidate_id = p_candidate_id
      AND ambiguous_source.session_version = p_session_version
      AND ambiguous_source.status = 'CURRENT'
      AND (
        ambiguous_source.source_build_run_id IS DISTINCT FROM p_source_build_run_id
        OR ambiguous_source.source_change_seq IS DISTINCT FROM p_source_change_seq
      )
  ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SET_AMBIGUOUS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SOURCE_SET_AMBIGUOUS')::text;
  END IF;

  SELECT COUNT(*)::integer,
         md5(COALESCE(string_agg(md5(source_row.source_row_json::text), '' ORDER BY source_row.source_ordinal), ''))
  INTO v_source_count, v_source_digest
  FROM public.banking_pay_workbench_candidate_source_lines AS source_row
  WHERE source_row.session_id = p_session_id
    AND source_row.candidate_id = p_candidate_id
    AND source_row.session_version = p_session_version
    AND source_row.source_build_run_id = p_source_build_run_id
    AND source_row.source_change_seq = p_source_change_seq
    AND source_row.status = 'CURRENT';

  IF v_contract_version IN (1,3) AND v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
     AND (
       v_source_count IS DISTINCT FROM v_build.canonical_count
       OR v_source_digest IS DISTINCT FROM v_build.canonical_digest) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SOURCE_DIGEST_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SOURCE_DIGEST_MISMATCH')::text;
  END IF;

  IF v_contract_version IN (2,3) AND v_authority_kind='TARGETED_DELTA' AND v_final_state='PENDING_DELTA' THEN
    v_attestation:=jsonb_build_object(
      'attestation_version',CASE WHEN v_contract_version=3 THEN 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3' ELSE 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2' END,
      'contract_version',v_contract_version,
      'semantic_contract_version',CASE WHEN v_contract_version=3 THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
      'authority_kind',v_authority_kind,
      'final_state','PENDING_DELTA',
      'parity_complete',false,
      'economic_build_id',p_economic_build_id,
      'source_build_run_id',p_source_build_run_id,
      'source_change_seq',p_source_change_seq,
      'session_version',p_session_version,
      'completion_job_id',p_completion_job_id,
      'certification_version',v_certification_version,
      'certification_digest',v_certification_digest,
      'attested_at_utc',v_now,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    );

    UPDATE public.banking_pay_workbench_session_scope AS pending_scope
    SET certified_preview_publication_required=true,
        certified_preview_publication_parity_ok=false,
        certified_preview_publication_session_version=p_session_version,
        certified_preview_publication_source_change_seq=p_source_change_seq,
        certified_preview_publication_source_build_run_id=p_source_build_run_id,
        certified_preview_publication_attestation_json=v_attestation,
        certified_preview_publication_attested_at_utc=NULL,
        status='DELTA_REFRESH_PENDING',
        dirty=true,
        updated_at_utc=v_now
    WHERE pending_scope.id=v_scope.id;

    RETURN jsonb_build_object(
      'ok',true,
      'contract_version',v_contract_version,
      'authority_kind',v_authority_kind,
      'final_state','PENDING_DELTA',
      'parity_complete',false,
      'source_row_count',v_source_count,
      'session_id',p_session_id,
      'candidate_id',p_candidate_id,
      'source_change_seq',p_source_change_seq,
      'session_version',p_session_version,
      'retry_safe',true,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  IF v_contract_version IN (2,3) AND ((v_final_state='SOURCE_EMPTY' AND v_source_count<>0)
     OR (v_final_state='READY' AND v_source_count=0)) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED'
      USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED','reason','FINAL_STATE_SOURCE_COUNT_MISMATCH')::text;
  END IF;

  PERFORM preview_lock.id
  FROM public.banking_pay_workbench_preview_rows AS preview_lock
  WHERE preview_lock.session_id = p_session_id
    AND preview_lock.candidate_id = p_candidate_id
  ORDER BY preview_lock.section, preview_lock.row_key, preview_lock.id
  FOR UPDATE;

  DROP TABLE IF EXISTS pg_temp._bpay_certified_preview_publish_rows;
  CREATE TEMPORARY TABLE pg_temp._bpay_certified_preview_publish_rows ON COMMIT DROP AS
  SELECT
    source_row.id AS source_line_id,
    source_row.source_ordinal,
    source_row.line_key,
    source_row.timesheet_id,
    source_row.source_row_json,
    source_row.economic_key_json,
    public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS resolved_section,
    public.pay_workbench_preview_line_contract_ok(
      source_row.source_row_json,
      source_row.economic_key_json,
      public.pay_workbench_preview_section_from_line_json(source_row.source_row_json)
    ) AS preview_contract_json,
    LOWER(BTRIM(COALESCE(source_row.source_row_json->>'selection_allowed', 'false')))
      IN ('true', 't', '1', 'yes', 'y', 'on') AS is_selectable,
    NULLIF(BTRIM(COALESCE(source_row.economic_key_json->>'key_type', '')), '') AS key_type,
    NULLIF(BTRIM(COALESCE(source_row.economic_key_json->>'key_value', '')), '') AS key_value,
    encode(
      extensions.digest(
        convert_to(
          jsonb_build_object(
            'identity_version', 'CERTIFIED_PREVIEW_SELECTION_IDENTITY_V1',
            'section', public.pay_workbench_preview_section_from_line_json(source_row.source_row_json),
            'row_key', source_row.line_key,
            'timesheet_id', source_row.timesheet_id,
            'economic_key_json', source_row.economic_key_json,
            'line_type', source_row.source_row_json->>'line_type',
            'finance_component_id', source_row.source_row_json->>'finance_component_id',
            'amount_ex_vat', source_row.source_row_json->>'amount_ex_vat',
            'pay_channel', source_row.source_row_json->>'pay_channel',
            'route_type', source_row.source_row_json->>'route_type',
            'draftable', source_row.source_row_json->>'draftable',
            'readiness_state', source_row.source_row_json->>'readiness_state',
            'selection_allowed', source_row.source_row_json->>'selection_allowed'
          )::text,
          'UTF8'
        ),
        'sha256'
      ),
      'hex'
    ) AS selection_identity_digest,
    existing_preview.id AS existing_preview_id,
    existing_preview.selected AS existing_selected,
    existing_preview.selection_state AS existing_selection_state,
    existing_preview.status AS existing_status,
    existing_preview.row_json->>'selection_identity_digest' AS existing_selection_identity_digest,
    existing_preview.row_json->>'selection_user_override' AS existing_selection_user_override,
    existing_preview.row_json->>'selection_origin' AS existing_selection_origin,
    existing_preview.row_json->>'selection_user_override_at_utc' AS existing_selection_user_override_at_utc
  FROM public.banking_pay_workbench_candidate_source_lines AS source_row
  LEFT JOIN public.banking_pay_workbench_preview_rows AS existing_preview
    ON existing_preview.session_id = p_session_id
   AND existing_preview.candidate_id = p_candidate_id
   AND existing_preview.section = public.pay_workbench_preview_section_from_line_json(source_row.source_row_json)
   AND existing_preview.row_key = source_row.line_key
  WHERE source_row.session_id = p_session_id
    AND source_row.candidate_id = p_candidate_id
    AND source_row.session_version = p_session_version
    AND source_row.source_build_run_id = p_source_build_run_id
    AND source_row.source_change_seq = p_source_change_seq
    AND source_row.status = 'CURRENT';

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_certified_preview_publish_rows AS prepared_row
    WHERE NULLIF(BTRIM(COALESCE(prepared_row.resolved_section, '')), '') IS NULL
       OR prepared_row.resolved_section IN ('internal_only', 'INTERNAL_ONLY')
       OR NULLIF(BTRIM(COALESCE(prepared_row.line_key, '')), '') IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM pg_temp._bpay_certified_preview_publish_rows
    GROUP BY resolved_section, line_key
    HAVING COUNT(*) <> 1
  ) OR EXISTS (
    SELECT 1
    FROM pg_temp._bpay_certified_preview_publish_rows
    GROUP BY source_ordinal
    HAVING COUNT(*) <> 1
  ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_ROW_CONTRACT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_ROW_CONTRACT_INVALID')::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_row_contract_error_count
  FROM pg_temp._bpay_certified_preview_publish_rows AS prepared_row
  WHERE (
    prepared_row.is_selectable IS TRUE
    AND (
      prepared_row.key_type IS NULL
      OR prepared_row.key_value IS NULL
      OR LOWER(BTRIM(COALESCE(
        public.pay_workbench_preview_line_contract_ok(
          prepared_row.source_row_json,
          prepared_row.economic_key_json,
          prepared_row.resolved_section
        )->>'ok',
        'false'
      ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    )
  ) OR (
    prepared_row.is_selectable IS NOT TRUE
    AND NOT (
      -- Retain the installed V1 allowance for non-economic timesheet context
      -- rows.  Other blocked/case economic rows must also satisfy the existing
      -- canonical display-row contract before this exact allow-list applies.
      (
        UPPER(BTRIM(COALESCE(prepared_row.source_row_json->>'line_type', ''))) = 'TIMESHEET_PAYMENT'
        OR LOWER(BTRIM(COALESCE(
          public.pay_workbench_preview_line_contract_ok(
            prepared_row.source_row_json,
            prepared_row.economic_key_json,
            prepared_row.resolved_section
          )->>'ok',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
      AND UPPER(BTRIM(COALESCE(prepared_row.source_row_json->>'presentation_section', ''))) IN (
        'READY_TO_PAY', 'CASES_RESOLUTIONS', 'BLOCKED_FOR_PAY', 'SNOOZED'
      )
      AND UPPER(BTRIM(COALESCE(prepared_row.source_row_json->>'presentation_role', ''))) IN (
        'PARENT', 'CONTEXT', 'DETAIL', 'SUMMARY'
      )
      AND UPPER(BTRIM(COALESCE(prepared_row.source_row_json->>'readiness_state', ''))) IN (
        'READY_TO_PAY', 'BLOCKED', 'BLOCKED_FOR_PAY', 'SNOOZED',
        'CASE_RESOLUTION', 'CASES_RESOLUTIONS'
      )
    )
  );

  IF v_row_contract_error_count > 0 THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_ROW_CONTRACT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_ROW_CONTRACT_INVALID',
              'row_count', v_row_contract_error_count
            )::text;
  END IF;

  v_explicit_selection := v_session.server_selected_preview_row_ids_provided IS TRUE;
  IF jsonb_typeof(COALESCE(v_session.server_selected_preview_row_ids, '[]'::jsonb)) <> 'array' THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SELECTION_INTENT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CERTIFIED_SOURCE_PREVIEW_SELECTION_INTENT_INVALID')::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_certified_preview_ready_rows;
  CREATE TEMPORARY TABLE pg_temp._bpay_certified_preview_ready_rows ON COMMIT DROP AS
  SELECT
    prepared_row.*,
    CASE
      WHEN prepared_row.is_selectable IS NOT TRUE THEN false
      WHEN v_contract_version = 3 THEN
        CASE
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_user_override, ''))) = 'UNSELECTED'
            THEN false
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_user_override, ''))) = 'SELECTED'
            THEN true
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_state, ''))) IN ('SELECTED', 'UNSELECTED')
            THEN prepared_row.existing_selected IS TRUE
          ELSE true
        END
      WHEN v_explicit_selection THEN
        prepared_row.existing_preview_id IS NOT NULL
        AND prepared_row.existing_status = 'READY'
        AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(v_session.server_selected_preview_row_ids) AS selected_id(value)
          WHERE selected_id.value = prepared_row.existing_preview_id::text
        )
      WHEN prepared_row.existing_preview_id IS NULL THEN true
      WHEN prepared_row.existing_status = 'READY'
        AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
        AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_state, ''))) IN ('SELECTED', 'UNSELECTED')
        THEN prepared_row.existing_selected IS TRUE
      ELSE false
    END AS effective_selected,
    CASE
      WHEN prepared_row.is_selectable IS NOT TRUE THEN 'NOT_SELECTABLE'
      WHEN v_contract_version = 3 THEN
        CASE
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_user_override, ''))) = 'UNSELECTED'
            THEN 'UNSELECTED'
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_user_override, ''))) = 'SELECTED'
            THEN 'SELECTED'
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_state, ''))) IN ('SELECTED', 'UNSELECTED')
            THEN CASE WHEN prepared_row.existing_selected IS TRUE THEN 'SELECTED' ELSE 'UNSELECTED' END
          ELSE 'SELECTED'
        END
      WHEN v_explicit_selection THEN
        CASE
          WHEN prepared_row.existing_preview_id IS NOT NULL
           AND prepared_row.existing_status = 'READY'
           AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
           AND EXISTS (
             SELECT 1
             FROM jsonb_array_elements_text(v_session.server_selected_preview_row_ids) AS selected_id(value)
             WHERE selected_id.value = prepared_row.existing_preview_id::text
           ) THEN 'SELECTED'
          ELSE 'UNSELECTED'
        END
      WHEN prepared_row.existing_preview_id IS NULL THEN 'SELECTED'
      WHEN prepared_row.existing_status = 'READY'
        AND prepared_row.existing_selection_identity_digest = prepared_row.selection_identity_digest
        AND UPPER(BTRIM(COALESCE(prepared_row.existing_selection_state, ''))) IN ('SELECTED', 'UNSELECTED')
        THEN CASE WHEN prepared_row.existing_selected IS TRUE THEN 'SELECTED' ELSE 'UNSELECTED' END
      ELSE 'UNSELECTED'
    END AS effective_selection_state
  FROM pg_temp._bpay_certified_preview_publish_rows AS prepared_row;

  -- A lost-response/duplicate completion must be a true publication no-op once
  -- the exact certified source, public identities, selection identity and scope
  -- attestation are already current.  The checks remain read-only and occur
  -- after every source/build/sequence/contract proof above has been repeated.
  SELECT
    v_scope.certified_preview_publication_required IS TRUE
    AND v_scope.certified_preview_publication_parity_ok IS TRUE
    AND v_scope.certified_preview_publication_session_version = p_session_version
    AND v_scope.certified_preview_publication_source_change_seq = p_source_change_seq
    AND v_scope.certified_preview_publication_source_build_run_id = p_source_build_run_id
    AND v_scope.certified_preview_publication_attestation_json->>'economic_build_id' = p_economic_build_id::text
    AND v_scope.certified_preview_publication_attestation_json->>'completion_job_id' = p_completion_job_id::text
    AND COALESCE((v_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean, false)
    AND v_scope.certified_preview_publication_attestation_json->>'attestation_version' = CASE
      WHEN v_contract_version=1 THEN 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
      WHEN v_contract_version=2 THEN 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
      ELSE 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
    END
    AND v_scope.certified_preview_publication_attestation_json->>'authority_kind'=v_authority_kind
    AND NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_certified_preview_ready_rows AS exact_row
      WHERE exact_row.existing_preview_id IS NULL
         OR exact_row.existing_status IS DISTINCT FROM 'READY'
         OR exact_row.existing_selection_identity_digest IS DISTINCT FROM exact_row.selection_identity_digest
         OR exact_row.existing_selected IS DISTINCT FROM exact_row.effective_selected
         OR exact_row.existing_selection_state IS DISTINCT FROM exact_row.effective_selection_state
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS current_preview
      WHERE current_preview.session_id = p_session_id
        AND current_preview.candidate_id = p_candidate_id
        AND current_preview.session_version = p_session_version
        AND current_preview.status = 'READY'
        AND NOT EXISTS (
          SELECT 1
          FROM pg_temp._bpay_certified_preview_ready_rows AS exact_row
          WHERE exact_row.resolved_section = current_preview.section
            AND exact_row.line_key = current_preview.row_key
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS current_preview
      JOIN pg_temp._bpay_certified_preview_ready_rows AS exact_row
        ON exact_row.resolved_section = current_preview.section
       AND exact_row.line_key = current_preview.row_key
      WHERE current_preview.session_id = p_session_id
        AND current_preview.candidate_id = p_candidate_id
        AND current_preview.session_version = p_session_version
        AND (
          current_preview.status IS DISTINCT FROM 'READY'
          OR current_preview.row_json->>'economic_build_id' IS DISTINCT FROM p_economic_build_id::text
          OR current_preview.row_json->>'source_build_run_id' IS DISTINCT FROM p_source_build_run_id::text
          OR current_preview.row_json->>'source_change_seq' IS DISTINCT FROM p_source_change_seq::text
          OR current_preview.row_json->>'source_line_id' IS DISTINCT FROM exact_row.source_line_id::text
          OR current_preview.row_json->>'selection_identity_digest' IS DISTINCT FROM exact_row.selection_identity_digest
          OR current_preview.row_json->'preview_contract' IS DISTINCT FROM exact_row.preview_contract_json
        )
    )
  INTO v_already_current;

  IF v_already_current IS TRUE THEN
    IF v_contract_version = 3 THEN
      v_selection_recovery_overlay := private.pay_workbench_recovery_selection_overlay_apply_v1(
        p_session_id,
        p_candidate_id,
        jsonb_build_object(
          'force_v3', true,
          'reason', 'V3_PUBLICATION_DUPLICATE_REPLAY'
        )
      );
    END IF;

    SELECT COUNT(*) FILTER (WHERE is_selectable)::integer,
           COUNT(*) FILTER (WHERE is_selectable IS NOT TRUE)::integer,
           COUNT(*) FILTER (WHERE effective_selected)::integer,
           COUNT(*)::integer
    INTO v_selectable_count, v_context_count, v_selected_count, v_preview_count
    FROM pg_temp._bpay_certified_preview_ready_rows;

    SELECT COALESCE(
      jsonb_object_agg(section_count.section, section_count.row_count ORDER BY section_count.section),
      '{}'::jsonb
    )
    INTO v_section_counts
    FROM (
      SELECT ready_row.resolved_section AS section, COUNT(*)::integer AS row_count
      FROM pg_temp._bpay_certified_preview_ready_rows AS ready_row
      GROUP BY ready_row.resolved_section
    ) AS section_count;

    RETURN jsonb_build_object(
      'ok', true,
      'parity_complete', true,
      'already_current', true,
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'economic_build_id', p_economic_build_id::text,
      'source_build_run_id', p_source_build_run_id::text,
      'source_change_seq', p_source_change_seq,
      'session_version', p_session_version,
      'source_row_count', v_preview_count,
      'preview_row_count', v_preview_count,
      'selectable_row_count', v_selectable_count,
      'context_row_count', v_context_count,
      'selected_row_count', v_selected_count,
      'upserted_row_count', 0,
      'retired_row_count', 0,
      'section_counts', v_section_counts,
      'selection_mode', CASE WHEN v_explicit_selection THEN 'EXPLICIT_INCLUDE' ELSE 'IMPLICIT_ALL' END,
      'retry_safe', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  WITH upserted AS (
    INSERT INTO public.banking_pay_workbench_preview_rows (
      session_id, candidate_id, section, row_key, row_ordinal, row_json,
      timesheet_id, key_type, key_value, selected, selection_state, status,
      session_version, created_at_utc, updated_at_utc
    )
    SELECT
      p_session_id,
      p_candidate_id,
      ready_row.resolved_section,
      ready_row.line_key,
      (v_scope_ordinal * 1000000) + ready_row.source_ordinal,
      jsonb_strip_nulls(
        ready_row.source_row_json
        || jsonb_build_object(
          'published_from_certified_source', true,
          'publication_authority_kind', v_authority_kind,
          'economic_build_id', p_economic_build_id::text,
          'source_build_run_id', p_source_build_run_id::text,
          'source_change_seq', p_source_change_seq,
          'source_ordinal', ready_row.source_ordinal,
          'source_line_id', ready_row.source_line_id::text,
          'publication_job_id', p_completion_job_id::text,
          'published_at_utc', v_now::text,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'selection_identity_digest', ready_row.selection_identity_digest,
          'preview_contract', ready_row.preview_contract_json,
          'selected', ready_row.effective_selected,
          'selection_state', ready_row.effective_selection_state,
          'selection_user_override', CASE
            WHEN v_contract_version = 3
             AND ready_row.existing_preview_id IS NOT NULL
             AND ready_row.existing_status = 'READY'
             AND ready_row.existing_selection_identity_digest = ready_row.selection_identity_digest
             AND UPPER(BTRIM(COALESCE(ready_row.existing_selection_user_override, ''))) IN ('SELECTED', 'UNSELECTED')
              THEN UPPER(BTRIM(ready_row.existing_selection_user_override))
            ELSE NULL::text
          END,
          'selection_origin', CASE
            WHEN v_contract_version = 3
             AND ready_row.existing_preview_id IS NOT NULL
             AND ready_row.existing_status = 'READY'
             AND ready_row.existing_selection_identity_digest = ready_row.selection_identity_digest
             AND UPPER(BTRIM(COALESCE(ready_row.existing_selection_user_override, ''))) IN ('SELECTED', 'UNSELECTED')
              THEN COALESCE(NULLIF(ready_row.existing_selection_origin, ''), 'USER_EXPLICIT_SELECTION')
            WHEN v_contract_version = 3 AND ready_row.is_selectable IS TRUE
              THEN 'SERVER_DEFAULT_NEW_READY_LINE'
            ELSE NULL::text
          END,
          'selection_user_override_at_utc', CASE
            WHEN v_contract_version = 3
             AND ready_row.existing_preview_id IS NOT NULL
             AND ready_row.existing_status = 'READY'
             AND ready_row.existing_selection_identity_digest = ready_row.selection_identity_digest
             AND UPPER(BTRIM(COALESCE(ready_row.existing_selection_user_override, ''))) IN ('SELECTED', 'UNSELECTED')
              THEN ready_row.existing_selection_user_override_at_utc
            ELSE NULL::text
          END
        )
      ),
      ready_row.timesheet_id,
      CASE WHEN ready_row.is_selectable THEN ready_row.key_type ELSE NULL::text END,
      CASE WHEN ready_row.is_selectable THEN ready_row.key_value ELSE NULL::text END,
      ready_row.effective_selected,
      ready_row.effective_selection_state,
      'READY',
      p_session_version,
      v_now,
      v_now
    FROM pg_temp._bpay_certified_preview_ready_rows AS ready_row
    ON CONFLICT (session_id, section, candidate_id, row_key)
    DO UPDATE SET
      row_ordinal = EXCLUDED.row_ordinal,
      row_json = EXCLUDED.row_json,
      timesheet_id = EXCLUDED.timesheet_id,
      key_type = EXCLUDED.key_type,
      key_value = EXCLUDED.key_value,
      selected = EXCLUDED.selected,
      selection_state = EXCLUDED.selection_state,
      status = EXCLUDED.status,
      session_version = EXCLUDED.session_version,
      updated_at_utc = EXCLUDED.updated_at_utc
    RETURNING id
  )
  SELECT COUNT(*)::integer INTO v_upserted_count FROM upserted;

  WITH retired AS (
    UPDATE public.banking_pay_workbench_preview_rows AS stale_preview
    SET selected = false,
        selection_state = 'SUPERSEDED',
        status = 'SUPERSEDED',
        row_json = jsonb_strip_nulls(
          COALESCE(stale_preview.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'certified_source_preview_superseded', true,
            'certified_source_preview_superseded_at_utc', v_now::text,
            'certified_source_preview_superseded_by_build_id', p_economic_build_id::text,
            'certified_source_preview_superseded_by_run_id', p_source_build_run_id::text,
            'certified_source_preview_superseded_by_change_seq', p_source_change_seq
          )
        ),
        updated_at_utc = v_now
    WHERE stale_preview.session_id = p_session_id
      AND stale_preview.candidate_id = p_candidate_id
      AND NOT EXISTS (
        SELECT 1
        FROM pg_temp._bpay_certified_preview_ready_rows AS exact_row
        WHERE exact_row.resolved_section = stale_preview.section
          AND exact_row.line_key = stale_preview.row_key
      )
      AND UPPER(BTRIM(COALESCE(stale_preview.status, ''))) <> 'SUPERSEDED'
    RETURNING stale_preview.id
  )
  SELECT COUNT(*)::integer INTO v_retired_count FROM retired;

  WITH source_identities AS (
    SELECT resolved_section AS section, line_key, source_ordinal
    FROM pg_temp._bpay_certified_preview_ready_rows
  ), preview_identities AS (
    SELECT preview_row.section,
           preview_row.row_key,
           (preview_row.row_ordinal - (v_scope_ordinal * 1000000))::bigint AS source_ordinal
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.candidate_id = p_candidate_id
      AND preview_row.session_version = p_session_version
      AND preview_row.status = 'READY'
  )
  SELECT
    (SELECT COUNT(*) FROM (SELECT * FROM source_identities EXCEPT ALL SELECT * FROM preview_identities) AS source_diff)::integer,
    (SELECT COUNT(*) FROM (SELECT * FROM preview_identities EXCEPT ALL SELECT * FROM source_identities) AS preview_diff)::integer
  INTO v_source_minus_preview, v_preview_minus_source;

  SELECT COUNT(*)::integer,
         md5(COALESCE(string_agg(resolved_section || E'\x1f' || line_key || E'\x1f' || source_ordinal::text, E'\x1e' ORDER BY source_ordinal, resolved_section, line_key), ''))
  INTO v_source_count, v_source_identity_digest
  FROM pg_temp._bpay_certified_preview_ready_rows;

  SELECT COUNT(*)::integer,
         md5(COALESCE(string_agg(
           preview_row.section || E'\x1f' || preview_row.row_key || E'\x1f'
             || (preview_row.row_ordinal - (v_scope_ordinal * 1000000))::text,
           E'\x1e'
           ORDER BY preview_row.row_ordinal, preview_row.section, preview_row.row_key
         ), ''))
  INTO v_preview_count, v_preview_identity_digest
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.candidate_id = p_candidate_id
    AND preview_row.session_version = p_session_version
    AND preview_row.status = 'READY';

  SELECT COUNT(*) FILTER (WHERE is_selectable)::integer,
         COUNT(*) FILTER (WHERE is_selectable IS NOT TRUE)::integer,
         COUNT(*) FILTER (WHERE effective_selected)::integer
  INTO v_selectable_count, v_context_count, v_selected_count
  FROM pg_temp._bpay_certified_preview_ready_rows;

  IF v_source_minus_preview <> 0
     OR v_preview_minus_source <> 0
     OR v_source_count IS DISTINCT FROM v_preview_count
     OR v_source_identity_digest IS DISTINCT FROM v_preview_identity_digest
     OR (v_contract_version IN (1,3) AND v_authority_kind='BOUNDED_FULL_SOURCE_BUILD'
       AND v_source_count IS DISTINCT FROM v_build.canonical_count) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED',
              'source_minus_preview', v_source_minus_preview,
              'preview_minus_source', v_preview_minus_source
            )::text;
  END IF;

  SELECT COALESCE(jsonb_object_agg(section_count.section, section_count.row_count ORDER BY section_count.section), '{}'::jsonb)
  INTO v_section_counts
  FROM (
    SELECT ready_row.resolved_section AS section, COUNT(*)::integer AS row_count
    FROM pg_temp._bpay_certified_preview_ready_rows AS ready_row
    GROUP BY ready_row.resolved_section
  ) AS section_count;

  IF v_contract_version = 3 THEN
    v_selection_recovery_overlay := private.pay_workbench_recovery_selection_overlay_apply_v1(
      p_session_id,
      p_candidate_id,
      jsonb_build_object(
        'force_v3', true,
        'reason', 'V3_PUBLICATION'
      )
    );
  END IF;

  SELECT COALESCE(jsonb_agg(selected_row.id::text ORDER BY selected_row.row_ordinal, selected_row.id), '[]'::jsonb)
  INTO v_selected_ids
  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = p_session_version
    AND selected_row.status = 'READY'
    AND selected_row.selected IS TRUE
    AND selected_row.selection_state = 'SELECTED';

  UPDATE public.banking_pay_workbench_sessions AS session_update
  SET server_selected_preview_row_ids = v_selected_ids,
      selected_row_count = jsonb_array_length(v_selected_ids),
      preview_row_count = (
        SELECT COUNT(*)::integer
        FROM public.banking_pay_workbench_preview_rows AS session_preview
        WHERE session_preview.session_id = p_session_id
          AND session_preview.session_version = p_session_version
          AND session_preview.status = 'READY'
      ),
      updated_at_utc = v_now
  WHERE session_update.id = p_session_id;

  SELECT pg_catalog.min(preview_row.row_ordinal),pg_catalog.max(preview_row.row_ordinal)
  INTO v_min_public_ordinal,v_max_public_ordinal
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id=p_session_id
    AND preview_row.candidate_id=p_candidate_id
    AND preview_row.session_version=p_session_version
    AND preview_row.status='READY';

  IF v_contract_version=3 THEN
    v_semantic_proof := private.pay_workbench_semantic_ready_proof_page_v1(
      p_session_id => p_session_id,
      p_candidate_ids => ARRAY[p_candidate_id],
      p_source_run_by_candidate_json => jsonb_build_object(
        p_candidate_id::text,
        p_source_build_run_id::text
      ),
      p_selected_preview_row_ids_by_candidate_json => NULL::jsonb,
      p_mode => CASE
        WHEN v_authority_kind='CERTIFIED_CANCELLATION_REVERSION' THEN 'CANCELLATION_REVERSION'
        ELSE 'PUBLICATION'
      END,
      p_options_json => '{}'::jsonb
    );

    v_semantic_candidate_proof := COALESCE(v_semantic_proof->'candidate_results'->0, '{}'::jsonb);
    v_semantic_ready := COALESCE((v_semantic_candidate_proof->>'semantic_ready')::boolean, false);
    v_semantic_proof_digest := NULLIF(BTRIM(COALESCE(v_semantic_candidate_proof->>'semantic_proof_digest','')), '');
    v_ordinary_positive_selectable_count := COALESCE((v_semantic_candidate_proof->>'ordinary_positive_selectable_count')::integer, 0);
    v_ordinary_positive_amount := COALESCE((v_semantic_candidate_proof->>'ordinary_positive_amount')::numeric, 0);
    v_recognised_deduction_count := COALESCE((v_semantic_candidate_proof->>'recognised_deduction_count')::integer, 0);
    v_recognised_deduction_amount := COALESCE((v_semantic_candidate_proof->>'recognised_deduction_amount')::numeric, 0);
    v_usable_same_candidate_headroom := COALESCE((v_semantic_candidate_proof->>'usable_same_candidate_headroom')::numeric, 0);
    v_candidate_ready_amount := COALESCE((v_semantic_candidate_proof->>'candidate_ready_amount')::numeric, 0);
    v_invalid_selectable_row_count := COALESCE((v_semantic_candidate_proof->>'invalid_selectable_row_count')::integer, -1);

    IF v_semantic_ready IS NOT TRUE
       OR v_semantic_proof_digest IS NULL
       OR v_invalid_selectable_row_count <> 0
       OR v_candidate_ready_amount < 0
       OR (
         NULLIF(BTRIM(COALESCE(p_publication_options_json->>'semantic_proof_digest','')), '') IS NOT NULL
         AND p_publication_options_json->>'semantic_proof_digest' IS DISTINCT FROM v_semantic_proof_digest
       ) THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED'
        USING ERRCODE='P0001',
              DETAIL=jsonb_build_object(
                'code','CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED',
                'candidate_id',p_candidate_id,
                'semantic_proof',v_semantic_candidate_proof
              )::text;
    END IF;
  END IF;

  IF v_contract_version=1 THEN
    v_attestation := jsonb_build_object(
      'attestation_version', 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1',
      'authority_kind', v_authority_kind,
      'parity_complete', true,
      'economic_build_id', p_economic_build_id::text,
      'source_build_run_id', p_source_build_run_id::text,
      'source_change_seq', p_source_change_seq,
      'session_version', p_session_version,
      'completion_job_id', p_completion_job_id::text,
      'refresh_scope_kind', v_refresh_scope_kind,
      'source_row_count', v_source_count,
      'preview_row_count', v_preview_count,
      'selectable_row_count', v_selectable_count,
      'context_row_count', v_context_count,
      'selected_row_count', v_selected_count,
      'source_digest', v_source_digest,
      'source_identity_digest', v_source_identity_digest,
      'preview_identity_digest', v_preview_identity_digest,
      'section_counts', v_section_counts,
      'attested_at_utc', v_now::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  ELSIF v_contract_version=2 THEN
    v_attestation := jsonb_strip_nulls(jsonb_build_object(
      'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2',
      'contract_version',2,
      'authority_kind',v_authority_kind,
      'final_state',v_final_state,
      'parity_complete',true,
      'session_id',p_session_id,
      'candidate_id',p_candidate_id,
      'economic_build_id',p_economic_build_id,
      'source_build_run_id',p_source_build_run_id,
      'original_economic_build_id',v_original_economic_build_id,
      'original_source_build_run_id',v_original_source_build_run_id,
      'source_session_id',v_source_session_id,
      'source_change_seq',p_source_change_seq,
      'session_version',p_session_version,
      'completion_job_id',p_completion_job_id,
      'refresh_scope_kind',v_refresh_scope_kind,
      'source_row_count',v_source_count,
      'preview_row_count',v_preview_count,
      'selectable_row_count',v_selectable_count,
      'context_row_count',v_context_count,
      'selected_row_count',v_selected_count,
      'source_digest',v_source_digest,
      'source_identity_digest',v_source_identity_digest,
      'preview_identity_digest',v_preview_identity_digest,
      'section_counts',v_section_counts,
      'scope_ordinal',v_scope_ordinal,
      'minimum_public_ordinal',v_min_public_ordinal,
      'maximum_public_ordinal',v_max_public_ordinal,
      'certification_version',v_certification_version,
      'certification_digest',v_certification_digest,
      'admission_seal_version',v_admission_seal_version,
      'admission_seal_digest',v_admission_seal_digest,
      'projection_fingerprint',v_projection_fingerprint,
      'attested_at_utc',v_now,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    ));
  ELSE
    v_attestation := jsonb_strip_nulls(jsonb_build_object(
      'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
      'contract_version',3,
      'semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
      'authority_kind',v_authority_kind,
      'final_state',v_final_state,
      'parity_complete',true,
      'semantic_ready',v_semantic_ready,
      'session_id',p_session_id,
      'candidate_id',p_candidate_id,
      'economic_build_id',p_economic_build_id,
      'source_build_run_id',p_source_build_run_id,
      'original_economic_build_id',v_original_economic_build_id,
      'original_source_build_run_id',v_original_source_build_run_id,
      'source_session_id',v_source_session_id,
      'source_change_seq',p_source_change_seq,
      'session_version',p_session_version,
      'completion_job_id',p_completion_job_id,
      'refresh_scope_kind',v_refresh_scope_kind,
      'source_row_count',v_source_count,
      'preview_row_count',v_preview_count,
      'selectable_row_count',v_selectable_count,
      'ordinary_positive_selectable_count',v_ordinary_positive_selectable_count,
      'ordinary_positive_amount',v_ordinary_positive_amount,
      'recognised_deduction_count',v_recognised_deduction_count,
      'recognised_deduction_amount',v_recognised_deduction_amount,
      'usable_same_candidate_headroom',v_usable_same_candidate_headroom,
      'candidate_ready_amount',v_candidate_ready_amount,
      'context_row_count',v_context_count,
      'blocked_row_count',COALESCE((v_semantic_candidate_proof->>'blocked_row_count')::integer,0),
      'invalid_selectable_row_count',v_invalid_selectable_row_count,
      'selected_row_count',v_selected_count,
      'source_digest',v_source_digest,
      'source_identity_digest',v_source_identity_digest,
      'preview_identity_digest',v_preview_identity_digest,
      'section_counts',v_section_counts
    ) || jsonb_build_object(
      'scope_ordinal',v_scope_ordinal,
      'minimum_public_ordinal',v_min_public_ordinal,
      'maximum_public_ordinal',v_max_public_ordinal,
      'certification_version',v_certification_version,
      'certification_digest',v_certification_digest,
      'admission_seal_version',v_admission_seal_version,
      'admission_seal_digest',v_admission_seal_digest,
      'projection_fingerprint',v_projection_fingerprint,
      'semantic_proof_digest',v_semantic_proof_digest,
      'selection_recovery_headroom_v1',COALESCE(v_selection_recovery_overlay,'{}'::jsonb),
      'cancellation_request_id',p_publication_options_json->>'cancellation_request_id',
      'cancellation_operation_id',p_publication_options_json->>'cancellation_operation_id',
      'cancellation_work_item_id',p_publication_options_json->>'cancellation_work_item_id',
      'pay_batch_id',p_publication_options_json->>'pay_batch_id',
      'cancellation_reversion_run_id',p_publication_options_json->>'cancellation_reversion_run_id',
      'financial_reversion_digest',p_publication_options_json->>'financial_reversion_digest',
      'attested_at_utc',v_now,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  UPDATE public.banking_pay_workbench_session_scope AS scope_update
  SET certified_preview_publication_required = true,
      certified_preview_publication_parity_ok = true,
      certified_preview_publication_session_version = p_session_version,
      certified_preview_publication_source_change_seq = p_source_change_seq,
      certified_preview_publication_source_build_run_id = p_source_build_run_id,
      certified_preview_publication_attestation_json = v_attestation,
      certified_preview_publication_attested_at_utc = v_now,
      updated_at_utc = v_now
  WHERE scope_update.id = v_scope.id;

  PERFORM public._audit_insert(
    'banking_pay_workbench_certified_source_preview',
    p_session_id::text,
    'PUBLISH_CERTIFIED_SOURCE_PREVIEW',
    NULL::jsonb,
    v_attestation || jsonb_build_object(
      'candidate_id', p_candidate_id::text,
      'upserted_row_count', v_upserted_count,
      'retired_row_count', v_retired_count,
      'selection_mode', CASE WHEN v_explicit_selection THEN 'EXPLICIT_INCLUDE' ELSE 'IMPLICIT_ALL' END
    ),
    'CERTIFIED_SOURCE_PREVIEW_PARITY_COMPLETE',
    v_session.actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'parity_complete', true,
    'contract_version',v_contract_version,
    'authority_kind',v_authority_kind,
    'final_state',CASE WHEN v_contract_version=1 THEN 'READY' ELSE v_final_state END,
    'already_current', false,
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'economic_build_id', p_economic_build_id::text,
    'source_build_run_id', p_source_build_run_id::text,
    'source_change_seq', p_source_change_seq,
    'session_version', p_session_version,
    'source_row_count', v_source_count,
    'preview_row_count', v_preview_count,
    'selectable_row_count', v_selectable_count,
    'context_row_count', v_context_count,
    'selected_row_count', v_selected_count,
    'upserted_row_count', v_upserted_count,
    'retired_row_count', v_retired_count,
    'section_counts', v_section_counts,
    'selection_mode', CASE WHEN v_explicit_selection THEN 'EXPLICIT_INCLUDE' ELSE 'IMPLICIT_ALL' END,
    'retry_safe', true,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_publish_certified_source_preview_v1(
  uuid, uuid, uuid, uuid, bigint, bigint, uuid, text, jsonb, jsonb, jsonb
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_publish_certified_source_preview_v1(
  uuid, uuid, uuid, uuid, bigint, bigint, uuid, text, jsonb, jsonb, jsonb
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_publish_certified_source_preview_v1(
  uuid, uuid, uuid, uuid, bigint, bigint, uuid, text, jsonb, jsonb, jsonb
) TO postgres;
