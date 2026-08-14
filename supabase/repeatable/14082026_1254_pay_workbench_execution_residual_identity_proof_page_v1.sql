-- CloudTMS Banking Pay: exact frozen-Q residual identity proof.
--
-- This helper is deliberately selection-bound.  It never derives cancellation
-- scope from current Workbench rows, current finance truth, or the complete
-- batch.  Q is read only from pay_payment_correction_request_candidates after
-- PREPARE_SELECTION has durably materialised it.

CREATE OR REPLACE FUNCTION private.pay_workbench_execution_residual_identity_proof_page_v1(
  p_correction_request_id uuid,
  p_correction_operation_id uuid,
  p_execution_operation_id uuid,
  p_pay_batch_id uuid,
  p_workbench_session_id uuid,
  p_workbench_session_version bigint,
  p_candidate_ids uuid[],
  p_expected_request_selection_hash text,
  p_expected_request_plan_hash text,
  p_expected_v2_chain_digest_by_candidate jsonb,
  p_expected_selected_anchor_digest_by_candidate jsonb,
  p_mode text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode,'')));
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_correction_operation public.banking_pay_operations%ROWTYPE;
  v_execution_operation public.banking_pay_operations%ROWTYPE;
  v_candidate uuid;
  v_ordinal bigint;
  v_request_candidate record;
  v_chain jsonb;
  v_attestation jsonb;
  v_selected_items jsonb;
  v_selected_anchor_digest text;
  v_affected_rows jsonb;
  v_affected_digest text;
  v_residual_digest text;
  v_rejection text;
  v_results jsonb:='[]'::jsonb;
  v_authorities jsonb:='{}'::jsonb;
  v_expected_key_count integer;
  v_request_candidate_count integer;
  v_supplied_candidate_count integer;
  v_item_count integer;
  v_distinct_item_count integer;
  v_resolved_item_count integer;
  v_allocation_count integer;
  v_ambiguous_allocation_count integer;
  v_scope_count integer;
  v_attestation_identity_count integer;
  v_anchor_count integer;
  v_context_bound_item_count integer;
  v_context_unproved_item_count integer;
  v_context_ambiguous_item_count integer;
  v_unproved_affected_item_count integer;
  v_affected_closure_ambiguity_count integer;
  v_f_count integer;
  v_a_count integer;
  v_e_count integer;
  v_c_count integer;
  v_p_count integer;
  v_f_minus_partition integer;
  v_partition_minus_f integer;
  v_e_minus_c integer;
  v_c_minus_e integer;
  v_e_minus_p integer;
  v_p_minus_e integer;
  v_c_minus_p integer;
  v_p_minus_c integer;
  v_duplicate_active_identity_count integer;
  v_lineage_mismatch_count integer;
  v_f_identity text;
  v_e_identity text;
  v_c_identity text;
  v_p_identity text;
  v_e_semantic text;
  v_c_semantic text;
  v_active_item_scope_digest text;
  v_authority_digest text;
  v_request_owned_dirty_job_id uuid;
  v_live_source_change_seq bigint;
  v_live_dirty_generation bigint;
  v_request_owned_latest_source_seq bigint;
  v_request_owned_dirty_generation bigint;
  v_request_owned_dirty_proven boolean:=false;
BEGIN
  v_supplied_candidate_count:=COALESCE(pg_catalog.cardinality(p_candidate_ids),0);

  IF p_correction_request_id IS NULL OR p_correction_operation_id IS NULL
     OR p_execution_operation_id IS NULL OR p_pay_batch_id IS NULL
     OR p_workbench_session_id IS NULL OR p_workbench_session_version IS NULL
     OR p_workbench_session_version<1 OR v_supplied_candidate_count NOT BETWEEN 1 AND 100
     OR v_mode NOT IN ('PRE_REQUEST_START','ROUTE_REPLAY')
     OR COALESCE(p_expected_request_selection_hash,'')!~'^[0-9a-f]{64}$'
     OR COALESCE(p_expected_request_plan_hash,'')!~'^[0-9a-f]{64}$'
     OR pg_catalog.jsonb_typeof(COALESCE(p_expected_v2_chain_digest_by_candidate,'null'::jsonb))<>'object'
     OR pg_catalog.jsonb_typeof(COALESCE(p_expected_selected_anchor_digest_by_candidate,'null'::jsonb))<>'object'
     OR EXISTS (SELECT 1 FROM pg_catalog.unnest(p_candidate_ids) AS supplied(id) WHERE supplied.id IS NULL)
     OR v_supplied_candidate_count<>(SELECT pg_catalog.count(DISTINCT supplied.id)
       FROM pg_catalog.unnest(p_candidate_ids) AS supplied(id)) THEN
    RAISE EXCEPTION 'EXECUTION_RESIDUAL_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_RESIDUAL_ARGUMENT_INVALID','mode',v_mode
      )::text;
  END IF;

  SELECT request_row.* INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id=p_correction_request_id;
  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,
      'rejection_reason','EXECUTION_RESIDUAL_REQUEST_NOT_FOUND');
  END IF;

  SELECT operation_row.* INTO v_correction_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_correction_operation_id;
  IF NOT FOUND OR v_correction_operation.operation_type<>'PAYMENT_CORRECTION'
     OR v_correction_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR COALESCE(v_correction_operation.input_json->>'correction_request_id','')
          IS DISTINCT FROM p_correction_request_id::text
     OR v_request.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,
      'rejection_reason','EXECUTION_RESIDUAL_CORRECTION_OPERATION_MISMATCH');
  END IF;

  IF (v_mode='PRE_REQUEST_START' AND v_request.status<>'PLANNED')
     OR v_request.selection_hash IS DISTINCT FROM p_expected_request_selection_hash
     OR v_request.plan_hash IS DISTINCT FROM p_expected_request_plan_hash THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,
      'rejection_reason',CASE
        WHEN v_mode='PRE_REQUEST_START' AND v_request.status<>'PLANNED'
          THEN 'EXECUTION_RESIDUAL_REQUEST_NOT_PLANNED'
        WHEN v_request.selection_hash IS DISTINCT FROM p_expected_request_selection_hash
          THEN 'EXECUTION_RESIDUAL_REQUEST_SELECTION_HASH_MISMATCH'
        ELSE 'EXECUTION_RESIDUAL_REQUEST_PLAN_HASH_MISMATCH' END);
  END IF;

  SELECT operation_row.* INTO v_execution_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_execution_operation_id;
  IF NOT FOUND OR v_execution_operation.operation_type<>'PAYMENT_EXECUTE'
     OR v_execution_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR v_execution_operation.status<>'COMPLETE' OR v_execution_operation.phase<>'COMPLETE' THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,
      'rejection_reason','EXECUTION_RESIDUAL_EXECUTION_OPERATION_MISMATCH');
  END IF;

  SELECT pg_catalog.count(*)::integer INTO v_request_candidate_count
  FROM public.pay_payment_correction_request_candidates AS request_candidate
  WHERE request_candidate.correction_request_id=p_correction_request_id;

  IF (v_mode='PRE_REQUEST_START' AND v_request_candidate_count<>v_supplied_candidate_count)
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.unnest(p_candidate_ids) WITH ORDINALITY AS supplied(candidate_id,ordinality)
       WHERE NOT EXISTS (
         SELECT 1
         FROM public.pay_payment_correction_request_candidates AS request_candidate
         JOIN public.pay_batch_candidates AS batch_candidate
           ON batch_candidate.id=request_candidate.pay_batch_candidate_id
          AND batch_candidate.pay_batch_id=p_pay_batch_id
         WHERE request_candidate.correction_request_id=p_correction_request_id
           AND batch_candidate.candidate_id=supplied.candidate_id
           AND (v_mode<>'PRE_REQUEST_START'
             OR request_candidate.selection_ordinal=supplied.ordinality)
       )
     ) THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,
      'rejection_reason','EXECUTION_RESIDUAL_REQUEST_CANDIDATE_SET_MISMATCH');
  END IF;

  SELECT pg_catalog.count(*)::integer INTO v_expected_key_count
  FROM pg_catalog.jsonb_object_keys(p_expected_v2_chain_digest_by_candidate);
  IF v_expected_key_count<>v_supplied_candidate_count
     OR EXISTS (
       SELECT 1 FROM pg_catalog.jsonb_each_text(p_expected_v2_chain_digest_by_candidate) AS expected(key,value)
       WHERE expected.value IS NULL
          OR expected.value!~'^[0-9a-f]{32}$'
          OR CASE
            WHEN expected.key~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN NOT ((expected.key)::uuid=ANY(p_candidate_ids))
            ELSE true
          END
     ) THEN
    RAISE EXCEPTION 'EXECUTION_RESIDUAL_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_RESIDUAL_V2_CHAIN_MAP_INVALID'
      )::text;
  END IF;

  SELECT pg_catalog.count(*)::integer INTO v_expected_key_count
  FROM pg_catalog.jsonb_object_keys(p_expected_selected_anchor_digest_by_candidate);
  IF (v_mode='PRE_REQUEST_START' AND v_expected_key_count<>0)
     OR (v_mode='ROUTE_REPLAY' AND v_expected_key_count<>v_supplied_candidate_count)
     OR EXISTS (
       SELECT 1 FROM pg_catalog.jsonb_each_text(p_expected_selected_anchor_digest_by_candidate) AS expected(key,value)
       WHERE expected.value IS NULL
          OR expected.value!~'^[0-9a-f]{64}$'
          OR CASE
            WHEN expected.key~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN NOT ((expected.key)::uuid=ANY(p_candidate_ids))
            ELSE true
          END
     ) THEN
    RAISE EXCEPTION 'EXECUTION_RESIDUAL_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_RESIDUAL_SELECTED_ANCHOR_MAP_INVALID'
      )::text;
  END IF;

  FOR v_candidate,v_ordinal IN
    SELECT supplied.candidate_id,supplied.ordinality::bigint
    FROM pg_catalog.unnest(p_candidate_ids) WITH ORDINALITY AS supplied(candidate_id,ordinality)
    ORDER BY supplied.ordinality
  LOOP
    v_rejection:=NULL;
    v_request_owned_dirty_job_id:=NULL;
    v_request_owned_latest_source_seq:=NULL;
    v_request_owned_dirty_generation:=NULL;
    v_request_owned_dirty_proven:=false;
    v_live_source_change_seq:=0;
    v_live_dirty_generation:=0;
    v_selected_anchor_digest:=NULL;
    v_affected_digest:=NULL;
    v_residual_digest:=NULL;
    v_authority_digest:=NULL;
    v_chain:=NULL;
    v_attestation:=NULL;
    v_selected_items:='[]'::jsonb;
    v_affected_rows:='[]'::jsonb;
    v_item_count:=0;
    v_distinct_item_count:=0;
    v_resolved_item_count:=0;
    v_allocation_count:=0;
    v_ambiguous_allocation_count:=0;
    v_scope_count:=0;
    v_attestation_identity_count:=0;
    v_anchor_count:=0;
    v_context_bound_item_count:=0;
    v_context_unproved_item_count:=0;
    v_context_ambiguous_item_count:=0;
    v_unproved_affected_item_count:=0;
    v_affected_closure_ambiguity_count:=0;
    v_f_count:=0;
    v_a_count:=0;
    v_e_count:=0;
    v_c_count:=0;
    v_p_count:=0;
    v_f_minus_partition:=0;
    v_partition_minus_f:=0;
    v_e_minus_c:=0;
    v_c_minus_e:=0;
    v_e_minus_p:=0;
    v_p_minus_e:=0;
    v_c_minus_p:=0;
    v_p_minus_c:=0;
    v_duplicate_active_identity_count:=0;
    v_lineage_mismatch_count:=0;
    v_f_identity:=NULL;
    v_e_identity:=NULL;
    v_c_identity:=NULL;
    v_p_identity:=NULL;
    v_e_semantic:=NULL;
    v_c_semantic:=NULL;
    SELECT request_candidate.*,batch_candidate.candidate_id
    INTO v_request_candidate
    FROM public.pay_payment_correction_request_candidates AS request_candidate
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id=request_candidate.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id=p_pay_batch_id
    WHERE request_candidate.correction_request_id=p_correction_request_id
      AND batch_candidate.candidate_id=v_candidate;

    IF NOT FOUND THEN v_rejection:='EXECUTION_RESIDUAL_REQUEST_CANDIDATE_SET_MISMATCH'; END IF;
    IF v_rejection IS NULL THEN
      v_item_count:=pg_catalog.cardinality(v_request_candidate.pay_batch_item_ids);
      SELECT pg_catalog.count(DISTINCT item_id)::integer INTO v_distinct_item_count
      FROM pg_catalog.unnest(v_request_candidate.pay_batch_item_ids) AS selected(item_id);
      IF v_item_count IS NULL OR v_item_count<1 OR v_item_count<>v_request_candidate.active_item_count
         OR v_distinct_item_count<>v_item_count THEN
        v_rejection:=CASE WHEN v_distinct_item_count<>v_item_count
          THEN 'EXECUTION_RESIDUAL_SELECTED_ITEM_DUPLICATE'
          ELSE 'EXECUTION_RESIDUAL_SELECTED_ITEM_ARRAY_INVALID' END;
      END IF;
    END IF;

    v_chain:=COALESCE(
      v_execution_operation.progress_json->'execution_unsent_overlay_chain_v2'->'candidates'->v_candidate::text,
      v_execution_operation.result_json->'execution_unsent_overlay_chain_v2'->'candidates'->v_candidate::text
    );
    IF v_rejection IS NULL AND (
      pg_catalog.jsonb_typeof(v_chain)<>'object'
      OR COALESCE(v_chain->>'contract_version','')<>'EXECUTION_UNSENT_OVERLAY_CHAIN_V2'
      OR pg_catalog.lower(pg_catalog.btrim(COALESCE(v_chain->>'closed','')))
           NOT IN ('true','t','1','yes','y','on')
      OR COALESCE(v_chain->>'execution_operation_id','')<>p_execution_operation_id::text
      OR COALESCE(v_chain->>'pay_batch_id','')<>p_pay_batch_id::text
      OR COALESCE(v_chain->>'candidate_id','')<>v_candidate::text
      OR COALESCE(v_chain->>'source_workbench_session_id','')<>p_workbench_session_id::text
      OR COALESCE(v_chain->>'source_session_version','')<>p_workbench_session_version::text
      OR COALESCE(v_chain->>'execution_commit_state','')<>'NOT_SUBMITTED'
      OR CASE WHEN COALESCE(v_chain->>'provider_attempt_count','')~'^[0-9]{1,9}$'
           THEN (v_chain->>'provider_attempt_count')::integer<>0 ELSE true END
      OR CASE WHEN COALESCE(v_chain->>'rail_transaction_count','')~'^[0-9]{1,9}$'
           THEN (v_chain->>'rail_transaction_count')::integer<>0 ELSE true END
      OR CASE WHEN COALESCE(v_chain->>'settlement_count','')~'^[0-9]{1,9}$'
           THEN (v_chain->>'settlement_count')::integer<>0 ELSE true END
      OR CASE WHEN COALESCE(v_chain->>'remittance_count','')~'^[0-9]{1,9}$'
           THEN (v_chain->>'remittance_count')::integer<>0 ELSE true END
      OR COALESCE(v_chain->>'terminal_source_change_seq','')!~'^[0-9]{1,18}$'
      OR COALESCE(v_chain->>'terminal_execution_generation','')!~'^[0-9]{1,18}$'
      OR COALESCE(v_chain->>'chain_digest','')
           IS DISTINCT FROM p_expected_v2_chain_digest_by_candidate->>v_candidate::text
    ) THEN v_rejection:='EXECUTION_RESIDUAL_V2_ORIGINAL_AUTHORITY_MISMATCH'; END IF;

    IF v_rejection IS NULL THEN
      SELECT COALESCE((SELECT change_counter.seq
          FROM public.app_change_counters AS change_counter
          WHERE change_counter.entity_key='pay_candidate:'||v_candidate::text),0),
        COALESCE((SELECT change_counter.scope_change_generation
          FROM public.app_change_counters AS change_counter
          WHERE change_counter.entity_key='pay_candidate:'||v_candidate::text),0)
      INTO v_live_source_change_seq,v_live_dirty_generation;

      SELECT dirty_job.id,
        CASE WHEN COALESCE(dirty_job.payload_json->>'latest_source_change_seq','')~'^[0-9]{1,18}$'
          THEN (dirty_job.payload_json->>'latest_source_change_seq')::bigint END,
        dirty_job.scope_change_generation
      INTO v_request_owned_dirty_job_id,v_request_owned_latest_source_seq,
        v_request_owned_dirty_generation
      FROM public.banking_pay_workbench_jobs AS dirty_job
      WHERE dirty_job.candidate_id=v_candidate
        AND dirty_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND COALESCE(dirty_job.payload_json->>'correction_dirty_causal_contract_version','')
              ='CORRECTION_OWNED_DIRTY_CAUSAL_V1'
        AND COALESCE(dirty_job.payload_json->'correction_dirty_contexts'
              ->v_candidate::text->>'correction_request_id','')=p_correction_request_id::text
        AND COALESCE(dirty_job.payload_json->'correction_dirty_contexts'
              ->v_candidate::text->>'pay_batch_id','')=p_pay_batch_id::text
        AND COALESCE(dirty_job.payload_json->>'request_owned_scope_change_tx_token','')
              =COALESCE(dirty_job.payload_json->>'scope_change_tx_token','')
        AND COALESCE(dirty_job.payload_json->>'request_owned_scope_change_tx_token','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND NOT EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb)
          ) AS reason(value)
          WHERE pg_catalog.upper(pg_catalog.btrim(reason.value)) NOT IN (
            'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
            'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
            'DIRTY_TRIGGER:PAY_BATCHES:UPDATE'
          )
        )
      ORDER BY dirty_job.created_at_utc DESC,dirty_job.id DESC
      LIMIT 1;

      v_request_owned_dirty_proven:=
        v_request_owned_dirty_job_id IS NOT NULL
        AND v_request_owned_latest_source_seq IS NOT DISTINCT FROM v_live_source_change_seq
        AND v_request_owned_dirty_generation IS NOT DISTINCT FROM v_live_dirty_generation;

      IF (v_live_source_change_seq IS DISTINCT FROM (v_chain->>'terminal_source_change_seq')::bigint
          OR v_live_dirty_generation IS DISTINCT FROM (v_chain->>'terminal_execution_generation')::bigint)
         AND v_request_owned_dirty_proven IS NOT TRUE THEN
        v_rejection:='EXECUTION_RESIDUAL_REQUEST_OWNED_DIRTY_CONTINUITY_UNPROVED';
      END IF;
    END IF;

    IF v_rejection IS NULL THEN
      WITH selected AS (
        SELECT selected_id.item_id
        FROM pg_catalog.unnest(v_request_candidate.pay_batch_item_ids) AS selected_id(item_id)
      ), item_rows AS (
        SELECT item.* FROM selected
        JOIN public.pay_batch_items AS item ON item.id=selected.item_id
        WHERE item.pay_batch_candidate_id=v_request_candidate.pay_batch_candidate_id
      ), allocation_counts AS (
        SELECT item_rows.id,pg_catalog.count(allocation.id)::integer AS allocation_count
        FROM item_rows
        LEFT JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation
          ON allocation.pay_batch_item_id=item_rows.id
        GROUP BY item_rows.id
      ), exact_rows AS (
        SELECT item_rows.*,allocation.id AS allocation_id,
          allocation.operation_id AS allocation_operation_id,
          allocation.candidate_scope_id,allocation.pay_batch_id AS allocation_batch_id,
          allocation.candidate_id AS allocation_candidate_id,
          allocation.pay_channel AS allocation_pay_channel,
          allocation.finance_case_id AS allocation_finance_case_id,
          allocation.finance_component_id AS allocation_finance_component_id,
          allocation.allocation_type,allocation.source_ref AS allocation_source_ref,
          allocation.operation_source_key AS allocation_operation_source_key,
          allocation.allocated_amount,allocation.allocation_basis_json,
          allocation.status AS allocation_status,
          scope.operation_id AS scope_operation_id,scope.workbench_session_id,
          scope.source_session_version,scope.candidate_id AS scope_candidate_id,
          scope.pay_batch_id AS scope_batch_id,scope.pay_channel AS scope_pay_channel,
          scope.scope_hash,scope.status AS scope_status,scope.allocation_basis_json AS scope_basis
        FROM item_rows
        JOIN allocation_counts ON allocation_counts.id=item_rows.id
          AND allocation_counts.allocation_count=1
        JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation
          ON allocation.pay_batch_item_id=item_rows.id
        JOIN public.banking_pay_operation_candidate_scope AS scope
          ON scope.id=allocation.candidate_scope_id
      )
      SELECT
        (SELECT pg_catalog.count(*)::integer FROM item_rows),
        (SELECT pg_catalog.count(*)::integer FROM exact_rows),
        (SELECT pg_catalog.count(*)::integer FROM allocation_counts WHERE allocation_count>1),
        (SELECT pg_catalog.count(DISTINCT candidate_scope_id)::integer FROM exact_rows),
        (SELECT pg_catalog.count(DISTINCT pg_catalog.concat_ws('|',
          scope_basis#>>'{source_publication_attestation,source_build_run_id}',
          scope_basis#>>'{source_publication_attestation,source_publication_id}',
          scope_basis#>>'{source_publication_attestation,source_change_seq}',
          scope_basis#>>'{source_publication_attestation,source_identity_digest}',
          scope_basis#>>'{source_publication_attestation,semantic_proof_digest}',
          scope_basis#>>'{source_publication_attestation,source_row_count}'
        ))::integer FROM exact_rows),
        (SELECT pg_catalog.min((scope_basis->'source_publication_attestation')::text)::jsonb FROM exact_rows),
        (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'pay_batch_item_id',id,'item_type',item_type,'pay_channel',pay_channel,
          'timesheet_id',timesheet_id,'amount_ex_vat',amount_ex_vat,
          'amount_vat',amount_vat,'amount_inc_vat',amount_inc_vat,
          'reservation_id',reservation_id,'finance_case_id',finance_case_id,
          'finance_component_id',finance_component_id,'pay_bank_transfer_id',pay_bank_transfer_id,
          'operation_source_key',operation_source_key,'source_ref',source_ref,
          'frozen_component_snapshot_json',frozen_component_snapshot_json,
          'frozen_source_basis_json',frozen_source_basis_json,
          'allocation_id',allocation_id,'allocation_operation_id',allocation_operation_id,
          'allocation_candidate_scope_id',candidate_scope_id,
          'allocation_status',allocation_status,'allocation_type',allocation_type,
          'allocation_source_ref',allocation_source_ref,'allocated_amount',allocated_amount,
          'allocation_operation_source_key',allocation_operation_source_key,
          'allocation_batch_id',allocation_batch_id,'allocation_candidate_id',allocation_candidate_id,
          'allocation_basis_json',allocation_basis_json,'candidate_scope_id',candidate_scope_id,
          'candidate_scope_pay_channel',scope_pay_channel,'candidate_scope_hash',scope_hash,
          'scope_operation_id',scope_operation_id,'scope_batch_id',scope_batch_id,
          'scope_candidate_id',scope_candidate_id,'scope_workbench_session_id',workbench_session_id,
          'scope_source_session_version',source_session_version,'scope_status',scope_status,
          'v3_source_build_run_id',scope_basis#>>'{source_publication_attestation,source_build_run_id}',
          'v3_source_publication_id',scope_basis#>>'{source_publication_attestation,source_publication_id}',
          'v3_source_change_seq',scope_basis#>>'{source_publication_attestation,source_change_seq}',
          'v3_source_identity_digest',scope_basis#>>'{source_publication_attestation,source_identity_digest}',
          'v3_semantic_proof_digest',scope_basis#>>'{source_publication_attestation,semantic_proof_digest}'
        ) ORDER BY id),'[]'::jsonb) FROM exact_rows),
        (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          id::text||':'||COALESCE(timesheet_id::text,'')||':'||
          COALESCE(pay_bank_transfer_id::text,'')||':'||COALESCE(amount_inc_vat::text,'')||':'||
          COALESCE(item_type,'')||':'||COALESCE(finance_case_id::text,'')||':'||
          COALESCE(finance_component_id::text,'')||':'||COALESCE(reservation_id::text,''),
          '|' ORDER BY id),'')) FROM item_rows)
      INTO v_resolved_item_count,v_allocation_count,v_ambiguous_allocation_count,
        v_scope_count,v_attestation_identity_count,v_attestation,v_selected_items,
        v_active_item_scope_digest;

      IF v_resolved_item_count<>v_item_count THEN v_rejection:='EXECUTION_RESIDUAL_SELECTED_ITEM_MISSING';
      ELSIF v_ambiguous_allocation_count>0 THEN v_rejection:='EXECUTION_RESIDUAL_SELECTED_ITEM_ALLOCATION_AMBIGUOUS';
      ELSIF v_allocation_count<>v_item_count THEN v_rejection:='EXECUTION_RESIDUAL_SELECTED_ITEM_ALLOCATION_MISSING';
      ELSIF v_attestation_identity_count<>1 OR v_scope_count<1
        THEN v_rejection:='EXECUTION_RESIDUAL_CANDIDATE_SCOPE_PUBLICATION_AMBIGUOUS';
      ELSIF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(v_selected_items) AS selected(value)
        WHERE selected.value->>'allocation_operation_id'<>COALESCE(v_chain->>'draft_operation_id','')
           OR selected.value->>'allocation_status'<>'ITEM_CREATED'
           OR selected.value->>'operation_source_key'
                IS DISTINCT FROM selected.value->>'allocation_operation_source_key'
           OR selected.value->>'allocation_batch_id'<>p_pay_batch_id::text
           OR selected.value->>'allocation_candidate_id'<>v_candidate::text
           OR selected.value->>'scope_operation_id'<>COALESCE(v_chain->>'draft_operation_id','')
           OR selected.value->>'scope_batch_id'<>p_pay_batch_id::text
           OR selected.value->>'scope_candidate_id'<>v_candidate::text
           OR selected.value->>'scope_workbench_session_id'<>p_workbench_session_id::text
           OR selected.value->>'scope_source_session_version'<>p_workbench_session_version::text
           OR selected.value->>'scope_status'<>'DRAFTED'
      ) THEN v_rejection:='EXECUTION_RESIDUAL_ALLOCATION_SOURCE_KEY_MISMATCH';
      ELSIF COALESCE(v_attestation->>'attestation_version','')<>'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
         OR COALESCE(v_attestation->>'semantic_contract_version','')<>'READY_TO_PAY_SEMANTIC_V2'
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(v_attestation->>'semantic_ready','')))
              NOT IN ('true','t','1','yes','y','on')
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(v_attestation->>'parity_complete','')))
              NOT IN ('true','t','1','yes','y','on')
         OR COALESCE(v_attestation->>'candidate_id','')<>v_candidate::text
         OR COALESCE(v_attestation->>'session_id','')<>p_workbench_session_id::text
         OR COALESCE(v_attestation->>'session_version','')<>p_workbench_session_version::text
         OR COALESCE(v_attestation->>'source_build_run_id','')
              !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         OR COALESCE(v_attestation->>'source_publication_id','')
              !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         OR COALESCE(v_attestation->>'source_change_seq','')!~'^[0-9]{1,18}$'
         OR COALESCE(v_attestation->>'source_row_count','')!~'^[0-9]{1,9}$'
         OR COALESCE(v_attestation->>'source_build_run_id','')<>COALESCE(v_chain->>'original_source_build_run_id','')
         OR COALESCE(v_attestation->>'source_publication_id','')<>COALESCE(v_chain->>'original_source_publication_id','')
         OR COALESCE(v_attestation->>'source_change_seq','')<>COALESCE(v_chain->>'pre_execution_source_change_seq','')
         OR COALESCE(v_attestation->>'source_identity_digest','')<>COALESCE(v_chain->>'original_source_identity_digest','')
         OR COALESCE(v_attestation->>'semantic_proof_digest','')<>COALESCE(v_chain->>'original_semantic_proof_digest','')
        THEN v_rejection:='EXECUTION_RESIDUAL_V3_ATTESTATION_INVALID';
      ELSIF v_mode='PRE_REQUEST_START'
         AND v_active_item_scope_digest IS DISTINCT FROM v_chain->>'active_item_scope_digest'
        THEN v_rejection:='EXECUTION_RESIDUAL_SELECTED_ITEM_CONTRACT_MISMATCH';
      END IF;
    END IF;

    IF v_rejection IS NULL THEN
      v_selected_anchor_digest:=private.pay_payment_correction_sha256_v1(
        pg_catalog.jsonb_build_object(
          'contract_version','EXECUTION_RESIDUAL_SELECTED_ANCHOR_V1',
          'correction_request_id',p_correction_request_id,
          'correction_operation_id',p_correction_operation_id,
          'execution_operation_id',p_execution_operation_id,'pay_batch_id',p_pay_batch_id,
          'pay_batch_candidate_id',v_request_candidate.pay_batch_candidate_id,
          'candidate_id',v_candidate,'request_selection_hash',v_request.selection_hash,
          'request_plan_hash',v_request.plan_hash,
          'candidate_scope_hash',v_request_candidate.candidate_scope_hash,
          'selected_item_count',v_item_count,'selected_items',v_selected_items
        )
      );
      IF v_mode='ROUTE_REPLAY' AND v_selected_anchor_digest
           IS DISTINCT FROM p_expected_selected_anchor_digest_by_candidate->>v_candidate::text THEN
        v_rejection:='EXECUTION_RESIDUAL_SELECTED_ANCHOR_DIGEST_MISMATCH';
      END IF;
    END IF;

    IF v_rejection IS NULL THEN
      WITH original_rows AS (
        SELECT source_row.id,source_row.source_ordinal,source_row.line_key,
          public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS section,
          source_row.timesheet_id,source_row.source_row_json,
          source_row.source_build_run_id,source_row.source_publication_id,
          source_row.source_change_seq
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.session_id=p_workbench_session_id
          AND source_row.candidate_id=v_candidate
          AND source_row.session_version=p_workbench_session_version
          AND source_row.source_change_seq=(v_attestation->>'source_change_seq')::bigint
          AND source_row.source_build_run_id=(v_attestation->>'source_build_run_id')::uuid
          AND source_row.source_publication_id=(v_attestation->>'source_publication_id')::uuid
      ), current_rows AS (
        SELECT source_row.id,source_row.source_ordinal,source_row.line_key,
          public.pay_workbench_preview_section_from_line_json(source_row.source_row_json) AS section,
          source_row.timesheet_id,source_row.source_row_json,
          source_row.source_build_run_id,source_row.source_publication_id
        FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.session_id=p_workbench_session_id
          AND source_row.candidate_id=v_candidate AND source_row.status='CURRENT'
      ), ready_rows AS (
        SELECT preview.section,preview.row_key AS line_key,
          pg_catalog.mod(preview.row_ordinal,1000000)::bigint AS source_ordinal,
          preview.row_json
        FROM public.banking_pay_workbench_preview_rows AS preview
        WHERE preview.session_id=p_workbench_session_id
          AND preview.candidate_id=v_candidate
          AND preview.session_version=p_workbench_session_version
          AND preview.status='READY'
      ), chain_transitions AS (
        SELECT transition.value AS transition_json
        FROM pg_catalog.jsonb_array_elements(COALESCE(v_chain->'transitions','[]'::jsonb))
          AS transition(value)
        WHERE pg_catalog.jsonb_typeof(transition.value)='object'
      ), chain_job_ids AS (
        SELECT DISTINCT job_id.value::uuid AS job_id
        FROM chain_transitions
        CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
          CASE WHEN pg_catalog.jsonb_typeof(chain_transitions.transition_json->'dirty_job_ids')='array'
            THEN chain_transitions.transition_json->'dirty_job_ids' ELSE '[]'::jsonb END
        ) AS job_id(value)
        WHERE pg_catalog.pg_input_is_valid(job_id.value,'uuid')
      ), chain_context_digests AS (
        SELECT DISTINCT context_digest.value AS context_digest
        FROM chain_transitions
        CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
          CASE WHEN pg_catalog.jsonb_typeof(chain_transitions.transition_json->'context_digests')='array'
            THEN chain_transitions.transition_json->'context_digests' ELSE '[]'::jsonb END
        ) AS context_digest(value)
        WHERE context_digest.value~'^[0-9a-f]{32}$'
      ), context_jobs AS (
        SELECT dirty_job.id,dirty_job.payload_json
        FROM chain_job_ids
        JOIN public.banking_pay_workbench_jobs AS dirty_job ON dirty_job.id=chain_job_ids.job_id
        WHERE dirty_job.candidate_id=v_candidate
          AND dirty_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
      ), schedule_contexts AS (
        SELECT context_jobs.id AS dirty_job_id,context_row.context_json,
          context_row.context_json->>'context_digest' AS context_digest
        FROM context_jobs
        CROSS JOIN LATERAL (
          SELECT context_jobs.payload_json->'execution_overlay_schedule_contexts'
            ->v_candidate::text AS context_json
        ) AS context_row
        WHERE COALESCE(context_jobs.payload_json->>'execution_overlay_schedule_causal_contract_version','')
                ='EXECUTION_UNSENT_SCHEDULE_CAUSAL_V2'
          AND pg_catalog.jsonb_typeof(context_row.context_json)='object'
          AND COALESCE(context_row.context_json->>'contract_version','')
                ='EXECUTION_UNSENT_SCHEDULE_CONTEXT_V2'
          AND COALESCE(context_row.context_json->>'execution_operation_id','')
                =p_execution_operation_id::text
          AND COALESCE(context_row.context_json->>'pay_batch_id','')=p_pay_batch_id::text
          AND COALESCE(context_row.context_json->>'candidate_id','')=v_candidate::text
          AND COALESCE(context_row.context_json->>'source_workbench_session_id','')
                =p_workbench_session_id::text
          AND COALESCE(context_row.context_json->>'source_session_version','')
                =p_workbench_session_version::text
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_batch_candidate_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_batch_item_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'timesheet_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'transfer_scope_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_bank_transfer_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'reservation_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'finance_case_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'finance_component_ids')='array'
          AND EXISTS (SELECT 1 FROM chain_context_digests
            WHERE chain_context_digests.context_digest=context_row.context_json->>'context_digest')
          AND context_row.context_json->>'context_digest'=pg_catalog.md5(
            COALESCE(context_row.context_json->>'execution_operation_id','')||'|'||
            COALESCE(context_row.context_json->>'pay_batch_id','')||'|'||
            COALESCE(context_row.context_json->>'candidate_id','')||'|'||
            (context_row.context_json->'pay_batch_candidate_ids')::text||'|'||
            (context_row.context_json->'pay_batch_item_ids')::text||'|'||
            (context_row.context_json->'timesheet_ids')::text||'|'||
            (context_row.context_json->'transfer_scope_ids')::text||'|'||
            (context_row.context_json->'pay_bank_transfer_ids')::text||'|'||
            (context_row.context_json->'reservation_ids')::text||'|'||
            (context_row.context_json->'finance_case_ids')::text||'|'||
            (context_row.context_json->'finance_component_ids')::text||'|'||
            COALESCE(context_row.context_json->>'source_workbench_session_id','')||'|'||
            COALESCE(context_row.context_json->>'source_snapshot_run_id','')||'|'||
            COALESCE(context_row.context_json->>'source_session_version','')||
            '|EXECUTION_UNSENT_SCHEDULE_CONTEXT_V2'
          )
      ), link_contexts AS (
        SELECT context_jobs.id AS dirty_job_id,context_row.context_json,
          context_row.context_json->>'context_digest' AS context_digest
        FROM context_jobs
        CROSS JOIN LATERAL (
          SELECT context_jobs.payload_json->'execution_overlay_contexts'
            ->v_candidate::text AS context_json
        ) AS context_row
        WHERE COALESCE(context_jobs.payload_json->>'execution_overlay_causal_contract_version','')
                ='EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'
          AND pg_catalog.jsonb_typeof(context_row.context_json)='object'
          AND COALESCE(context_row.context_json->>'contract_version','')
                ='EXECUTION_UNSENT_OVERLAY_CONTEXT_V1'
          AND COALESCE(context_row.context_json->>'execution_operation_id','')
                =p_execution_operation_id::text
          AND COALESCE(context_row.context_json->>'pay_batch_id','')=p_pay_batch_id::text
          AND COALESCE(context_row.context_json->>'candidate_id','')=v_candidate::text
          AND COALESCE(context_row.context_json->>'source_workbench_session_id','')
                =p_workbench_session_id::text
          AND COALESCE(context_row.context_json->>'source_session_version','')
                =p_workbench_session_version::text
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_batch_candidate_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_batch_item_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'timesheet_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'transfer_scope_ids')='array'
          AND pg_catalog.jsonb_typeof(context_row.context_json->'pay_bank_transfer_ids')='array'
          AND EXISTS (SELECT 1 FROM chain_context_digests
            WHERE chain_context_digests.context_digest=context_row.context_json->>'context_digest')
          AND context_row.context_json->>'context_digest'=pg_catalog.md5(
            COALESCE(context_row.context_json->>'execution_operation_id','')||'|'||
            COALESCE(context_row.context_json->>'pay_batch_id','')||'|'||
            COALESCE(context_row.context_json->>'candidate_id','')||'|'||
            (context_row.context_json->'pay_batch_candidate_ids')::text||'|'||
            (context_row.context_json->'pay_batch_item_ids')::text||'|'||
            (context_row.context_json->'timesheet_ids')::text||'|'||
            (context_row.context_json->'transfer_scope_ids')::text||'|'||
            (context_row.context_json->'pay_bank_transfer_ids')::text||'|'||
            COALESCE(context_row.context_json->>'source_workbench_session_id','')||'|'||
            COALESCE(context_row.context_json->>'source_snapshot_run_id','')||'|'||
            COALESCE(context_row.context_json->>'source_session_version','')||
            '|EXECUTION_UNSENT_OVERLAY_CONTEXT_V1'
          )
      ), selected_allocations AS (
        SELECT item.id AS pay_batch_item_id,item.pay_batch_candidate_id,item.item_type,
          item.timesheet_id AS item_timesheet_id,
          item.reservation_id,item.finance_case_id,item.finance_component_id,item.amount_inc_vat,
          item.source_ref AS item_source_ref,allocation.allocation_type,
          allocation.source_ref AS allocation_source_ref,allocation.allocation_basis_json,
          CASE WHEN COALESCE(allocation.allocation_basis_json#>>'{line,source_line_id}','')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (allocation.allocation_basis_json#>>'{line,source_line_id}')::uuid END AS source_line_id,
          CASE WHEN COALESCE(allocation.allocation_basis_json#>>'{line,timesheet_id}','')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (allocation.allocation_basis_json#>>'{line,timesheet_id}')::uuid END AS linked_timesheet_id,
          allocation.allocation_basis_json#>>'{line,economic_key}' AS allocation_economic_key
        FROM pg_catalog.unnest(v_request_candidate.pay_batch_item_ids) AS selected(item_id)
        JOIN public.pay_batch_items AS item ON item.id=selected.item_id
        JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation
          ON allocation.pay_batch_item_id=item.id
      ), context_matches AS (
        SELECT selected_allocations.pay_batch_item_id,
          schedule_contexts.context_digest AS schedule_context_digest,
          link_contexts.context_digest AS link_context_digest
        FROM selected_allocations
        JOIN schedule_contexts ON EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'pay_batch_item_ids'
          ) AS context_item(value)
          WHERE context_item.value=selected_allocations.pay_batch_item_id::text
        ) AND EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'pay_batch_candidate_ids'
          ) AS context_candidate(value)
          WHERE context_candidate.value=selected_allocations.pay_batch_candidate_id::text
        ) AND (selected_allocations.item_timesheet_id IS NULL OR EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'timesheet_ids'
          ) AS context_timesheet(value)
          WHERE context_timesheet.value=selected_allocations.item_timesheet_id::text
        )) AND (selected_allocations.reservation_id IS NULL OR EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'reservation_ids'
          ) AS context_reservation(value)
          WHERE context_reservation.value=selected_allocations.reservation_id::text
        )) AND (selected_allocations.finance_case_id IS NULL OR EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'finance_case_ids'
          ) AS context_case(value)
          WHERE context_case.value=selected_allocations.finance_case_id::text
        )) AND (selected_allocations.finance_component_id IS NULL OR EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            schedule_contexts.context_json->'finance_component_ids'
          ) AS context_component(value)
          WHERE context_component.value=selected_allocations.finance_component_id::text
        ))
        JOIN link_contexts ON EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            link_contexts.context_json->'pay_batch_item_ids'
          ) AS context_item(value)
          WHERE context_item.value=selected_allocations.pay_batch_item_id::text
        ) AND EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            link_contexts.context_json->'pay_batch_candidate_ids'
          ) AS context_candidate(value)
          WHERE context_candidate.value=selected_allocations.pay_batch_candidate_id::text
        ) AND (selected_allocations.item_timesheet_id IS NULL OR EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(
            link_contexts.context_json->'timesheet_ids'
          ) AS context_timesheet(value)
          WHERE context_timesheet.value=selected_allocations.item_timesheet_id::text
        ))
      ), context_rollup AS (
        SELECT selected_allocations.pay_batch_item_id,
          pg_catalog.count(DISTINCT context_matches.schedule_context_digest)::integer
            AS schedule_context_count,
          pg_catalog.count(DISTINCT context_matches.link_context_digest)::integer
            AS link_context_count,
          pg_catalog.min(context_matches.schedule_context_digest) AS schedule_context_digest,
          pg_catalog.min(context_matches.link_context_digest) AS link_context_digest
        FROM selected_allocations
        LEFT JOIN context_matches
          ON context_matches.pay_batch_item_id=selected_allocations.pay_batch_item_id
        GROUP BY selected_allocations.pay_batch_item_id
      ), context_bound_allocations AS (
        SELECT selected_allocations.*,context_rollup.schedule_context_digest,
          context_rollup.link_context_digest
        FROM selected_allocations
        JOIN context_rollup ON context_rollup.pay_batch_item_id=selected_allocations.pay_batch_item_id
          AND context_rollup.schedule_context_count=1 AND context_rollup.link_context_count=1
      ), anchors AS (
        SELECT original_rows.*,context_bound_allocations.pay_batch_item_id,
          context_bound_allocations.linked_timesheet_id,context_bound_allocations.item_type,
          context_bound_allocations.reservation_id,context_bound_allocations.finance_case_id,
          context_bound_allocations.finance_component_id,context_bound_allocations.amount_inc_vat,
          context_bound_allocations.item_source_ref,context_bound_allocations.allocation_type,
          context_bound_allocations.allocation_source_ref,
          context_bound_allocations.allocation_economic_key,
          context_bound_allocations.schedule_context_digest AS transition_context_digest
        FROM context_bound_allocations
        JOIN original_rows ON original_rows.id=context_bound_allocations.source_line_id
      ), recovery_anchors AS (
        SELECT anchors.*
        FROM anchors
        WHERE pg_catalog.upper(pg_catalog.btrim(COALESCE(anchors.item_type,'')))
                ='OVERPAYMENT_RECOVERY'
          AND pg_catalog.upper(pg_catalog.btrim(COALESCE(anchors.allocation_type,'')))
                ='OVERPAYMENT_RECOVERY'
          AND COALESCE(anchors.amount_inc_vat,0)<0
          AND anchors.finance_case_id IS NOT NULL
          AND anchors.finance_component_id IS NOT NULL
          AND anchors.linked_timesheet_id IS NOT NULL
          AND anchors.source_row_json->>'finance_case_id'=anchors.finance_case_id::text
          AND anchors.source_row_json->>'finance_component_id'=anchors.finance_component_id::text
          AND anchors.source_row_json->>'source_ref'=anchors.item_source_ref
          AND anchors.source_row_json->>'source_ref'=anchors.allocation_source_ref
          AND anchors.source_row_json->>'economic_key'=anchors.allocation_economic_key
      ), recovery_parent_candidates AS (
        SELECT recovery_anchors.id AS recovery_source_line_id,original_rows.*,
          recovery_anchors.transition_context_digest
        FROM recovery_anchors
        JOIN original_rows
          ON original_rows.section=recovery_anchors.section
         AND original_rows.timesheet_id=recovery_anchors.linked_timesheet_id
         AND original_rows.line_key=recovery_anchors.linked_timesheet_id::text
         AND COALESCE(original_rows.source_row_json->>'presentation_role','')='PARENT'
      ), recovery_parent_rollup AS (
        SELECT recovery_anchors.id AS recovery_source_line_id,
          pg_catalog.count(DISTINCT recovery_parent_candidates.id)::integer AS parent_count
        FROM recovery_anchors
        LEFT JOIN recovery_parent_candidates
          ON recovery_parent_candidates.recovery_source_line_id=recovery_anchors.id
        GROUP BY recovery_anchors.id
      ), affected_candidates AS (
        SELECT recovery_anchors.id,recovery_anchors.source_ordinal,recovery_anchors.line_key,
          recovery_anchors.section,recovery_anchors.timesheet_id,recovery_anchors.source_row_json,
          recovery_anchors.source_build_run_id,recovery_anchors.source_publication_id,
          recovery_anchors.source_change_seq,
          'SELECTED_FROZEN_RECOVERY_ANCHOR'::text AS closure_reason,
          recovery_anchors.transition_context_digest
        FROM recovery_anchors
        UNION ALL
        SELECT recovery_parent_candidates.id,recovery_parent_candidates.source_ordinal,
          recovery_parent_candidates.line_key,recovery_parent_candidates.section,
          recovery_parent_candidates.timesheet_id,recovery_parent_candidates.source_row_json,
          recovery_parent_candidates.source_build_run_id,
          recovery_parent_candidates.source_publication_id,
          recovery_parent_candidates.source_change_seq,
          'SELECTED_RECOVERY_TIMESHEET_PARENT'::text AS closure_reason,
          recovery_parent_candidates.transition_context_digest
        FROM recovery_parent_candidates
      ), affected_context_rollup AS (
        SELECT affected_candidates.id,
          pg_catalog.count(DISTINCT affected_candidates.transition_context_digest)::integer
            AS context_digest_count
        FROM affected_candidates GROUP BY affected_candidates.id
      ), affected AS (
        SELECT DISTINCT ON (affected_candidates.id)
          affected_candidates.id,affected_candidates.source_ordinal,
          affected_candidates.line_key,affected_candidates.section,
          affected_candidates.timesheet_id,affected_candidates.source_row_json,
          affected_candidates.source_build_run_id,affected_candidates.source_publication_id,
          affected_candidates.source_change_seq,affected_candidates.closure_reason,
          affected_candidates.transition_context_digest
        FROM affected_candidates
        JOIN affected_context_rollup ON affected_context_rollup.id=affected_candidates.id
        ORDER BY affected_candidates.id,affected_candidates.closure_reason,
          affected_candidates.transition_context_digest
      ), expected_rows AS (
        SELECT original_rows.* FROM original_rows
        WHERE NOT EXISTS (SELECT 1 FROM affected WHERE affected.id=original_rows.id)
      ), partition_rows AS (
        SELECT section,line_key,source_ordinal FROM expected_rows
        UNION ALL SELECT section,line_key,source_ordinal FROM affected
      )
      SELECT
        (SELECT pg_catalog.count(*)::integer FROM anchors),
        (SELECT pg_catalog.count(*)::integer FROM context_bound_allocations),
        (SELECT pg_catalog.count(*)::integer FROM context_rollup
          WHERE schedule_context_count<>1 OR link_context_count<>1),
        (SELECT pg_catalog.count(*)::integer FROM context_rollup
          WHERE schedule_context_count>1 OR link_context_count>1),
        (SELECT pg_catalog.count(*)::integer FROM selected_allocations
          WHERE COALESCE(selected_allocations.amount_inc_vat,0)<0
            AND NOT EXISTS (
              SELECT 1 FROM recovery_anchors
              WHERE recovery_anchors.pay_batch_item_id=selected_allocations.pay_batch_item_id
            )),
        (SELECT pg_catalog.count(*)::integer FROM recovery_parent_rollup
          WHERE recovery_parent_rollup.parent_count<>1)
          +(SELECT pg_catalog.count(*)::integer FROM affected_context_rollup
            WHERE affected_context_rollup.context_digest_count<>1),
        (SELECT pg_catalog.count(*)::integer FROM original_rows),
        (SELECT pg_catalog.count(*)::integer FROM affected),
        (SELECT pg_catalog.count(*)::integer FROM expected_rows),
        (SELECT pg_catalog.count(*)::integer FROM current_rows),
        (SELECT pg_catalog.count(*)::integer FROM ready_rows),
        (SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM original_rows EXCEPT ALL SELECT * FROM partition_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT * FROM partition_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM original_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM expected_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM current_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM current_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM expected_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM expected_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM ready_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM ready_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM expected_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM current_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM ready_rows
        ) d),(SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key,source_ordinal FROM ready_rows EXCEPT ALL SELECT section,line_key,source_ordinal FROM current_rows
        ) d),
        (SELECT pg_catalog.count(*)::integer FROM (
          SELECT section,line_key FROM current_rows GROUP BY section,line_key HAVING pg_catalog.count(*)>1
        ) d),
        (SELECT pg_catalog.count(*)::integer FROM current_rows
          WHERE source_build_run_id<>(v_attestation->>'source_build_run_id')::uuid
             OR source_publication_id<>(v_attestation->>'source_publication_id')::uuid),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(section||E'\x1f'||line_key||E'\x1f'||source_ordinal::text,E'\x1e' ORDER BY source_ordinal,section,line_key) FROM original_rows),'')),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(section||E'\x1f'||line_key||E'\x1f'||source_ordinal::text,E'\x1e' ORDER BY source_ordinal,section,line_key) FROM expected_rows),'')),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(section||E'\x1f'||line_key||E'\x1f'||source_ordinal::text,E'\x1e' ORDER BY source_ordinal,section,line_key) FROM current_rows),'')),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(section||E'\x1f'||line_key||E'\x1f'||source_ordinal::text,E'\x1e' ORDER BY source_ordinal,section,line_key) FROM ready_rows),'')),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(pg_catalog.md5(source_row_json::text),'' ORDER BY source_ordinal) FROM expected_rows),'')),
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(pg_catalog.md5(source_row_json::text),'' ORDER BY source_ordinal) FROM current_rows),'')),
        COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'source_line_id',id,'physical_section',section,'line_key',line_key,
          'source_ordinal',source_ordinal,'timesheet_id',timesheet_id,
          'source_build_run_id',source_build_run_id,'source_publication_id',source_publication_id,
          'source_change_seq',source_change_seq,'closure_reason',closure_reason,
          'transition_context_digest',transition_context_digest
        ) ORDER BY source_ordinal,section,line_key,id) FROM affected),'[]'::jsonb)
      INTO v_anchor_count,v_context_bound_item_count,v_context_unproved_item_count,
        v_context_ambiguous_item_count,v_unproved_affected_item_count,
        v_affected_closure_ambiguity_count,
        v_f_count,v_a_count,v_e_count,v_c_count,v_p_count,
        v_f_minus_partition,v_partition_minus_f,v_e_minus_c,v_c_minus_e,
        v_e_minus_p,v_p_minus_e,v_c_minus_p,v_p_minus_c,
        v_duplicate_active_identity_count,v_lineage_mismatch_count,
        v_f_identity,v_e_identity,v_c_identity,v_p_identity,
        v_e_semantic,v_c_semantic,v_affected_rows;

      IF v_anchor_count<>v_item_count THEN v_rejection:='EXECUTION_RESIDUAL_SELECTED_ITEM_ALLOCATION_MISSING';
      ELSIF v_context_ambiguous_item_count<>0
        THEN v_rejection:='EXECUTION_RESIDUAL_V2_TRANSITION_CONTEXT_AMBIGUOUS';
      ELSIF v_context_bound_item_count<>v_item_count OR v_context_unproved_item_count<>0
        THEN v_rejection:='EXECUTION_RESIDUAL_V2_TRANSITION_CONTEXT_MISSING';
      ELSIF v_unproved_affected_item_count<>0
        THEN v_rejection:='EXECUTION_RESIDUAL_AFFECTED_SCOPE_UNPROVED';
      ELSIF v_affected_closure_ambiguity_count<>0
        THEN v_rejection:='EXECUTION_RESIDUAL_AFFECTED_SCOPE_AMBIGUOUS';
      ELSIF v_f_count IS DISTINCT FROM (v_attestation->>'source_row_count')::integer
        OR v_f_identity IS DISTINCT FROM v_attestation->>'source_identity_digest'
        THEN v_rejection:='EXECUTION_RESIDUAL_ORIGINAL_SOURCE_PUBLICATION_MISMATCH';
      ELSIF v_f_minus_partition<>0 OR v_partition_minus_f<>0
        OR v_e_minus_c<>0 OR v_c_minus_e<>0 OR v_e_minus_p<>0 OR v_p_minus_e<>0
        OR v_c_minus_p<>0 OR v_p_minus_c<>0
        OR v_duplicate_active_identity_count<>0 OR v_lineage_mismatch_count<>0
        OR v_e_semantic IS DISTINCT FROM v_c_semantic
        THEN v_rejection:='EXECUTION_RESIDUAL_IDENTITY_MISMATCH';
      END IF;
    END IF;

    v_affected_digest:=private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'contract_version','EXECUTION_RESIDUAL_AFFECTED_PHYSICAL_CLOSURE_V1',
        'selected_anchor_digest',v_selected_anchor_digest,
        'execution_operation_id',p_execution_operation_id,
        'v2_chain_digest',v_chain->>'chain_digest',
        'affected_physical_rows',COALESCE(v_affected_rows,'[]'::jsonb)
      )
    );
    v_residual_digest:=private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'contract_version','EXECUTION_RESIDUAL_IDENTITY_PROOF_V1',
        'correction_request_id',p_correction_request_id,
        'correction_operation_id',p_correction_operation_id,
        'execution_operation_id',p_execution_operation_id,'pay_batch_id',p_pay_batch_id,
        'candidate_id',v_candidate,'request_selection_hash',v_request.selection_hash,
        'request_plan_hash',v_request.plan_hash,'v2_chain_digest',v_chain->>'chain_digest',
        'selected_anchor_digest',v_selected_anchor_digest,
        'affected_physical_closure_digest',v_affected_digest,
        'frozen_original_count',v_f_count,'affected_physical_count',v_a_count,
        'expected_residual_count',v_e_count,'current_source_count',v_c_count,
        'ready_preview_count',v_p_count,'frozen_original_identity_digest',v_f_identity,
        'expected_residual_identity_digest',v_e_identity,
        'current_source_identity_digest',v_c_identity,'ready_preview_identity_digest',v_p_identity,
        'expected_residual_semantic_digest',v_e_semantic,'current_source_semantic_digest',v_c_semantic,
        'f_minus_partition_count',v_f_minus_partition,'partition_minus_f_count',v_partition_minus_f,
        'expected_minus_current_count',v_e_minus_c,'current_minus_expected_count',v_c_minus_e,
        'expected_minus_preview_count',v_e_minus_p,'preview_minus_expected_count',v_p_minus_e,
        'current_minus_preview_count',v_c_minus_p,'preview_minus_current_count',v_p_minus_c,
        'duplicate_active_identity_count',v_duplicate_active_identity_count,
        'lineage_mismatch_count',v_lineage_mismatch_count,
        'context_unproved_item_count',v_context_unproved_item_count,
        'context_ambiguous_item_count',v_context_ambiguous_item_count,
        'unproved_affected_item_count',v_unproved_affected_item_count,
        'affected_closure_ambiguity_count',v_affected_closure_ambiguity_count,
        'semantic_mismatch_count',CASE WHEN v_e_semantic IS DISTINCT FROM v_c_semantic THEN 1 ELSE 0 END,
        'admitted',v_rejection IS NULL,'rejection_reason',v_rejection
      )
    );
    v_authority_digest:=private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'contract_version','CANCELLATION_REVERSION_Q_BOUND_PRE_REQUEST_START_AUTHORITY_V1',
        'boundary','AFTER_REQUEST_PREPARE_BEFORE_REQUEST_START',
        'correction_request_id',p_correction_request_id,
        'correction_operation_id',p_correction_operation_id,
        'execution_operation_id',p_execution_operation_id,'pay_batch_id',p_pay_batch_id,
        'pay_batch_candidate_id',v_request_candidate.pay_batch_candidate_id,
        'candidate_id',v_candidate,'workbench_session_id',p_workbench_session_id,
        'workbench_session_version',p_workbench_session_version,
        'request_selection_hash',v_request.selection_hash,'request_plan_hash',v_request.plan_hash,
        'v2_chain_digest',v_chain->>'chain_digest','selected_anchor_digest',v_selected_anchor_digest,
        'affected_physical_closure_digest',v_affected_digest,
        'residual_proof_digest',v_residual_digest,
        'request_owned_dirty_job_id',v_request_owned_dirty_job_id,
        'request_owned_dirty_proven',v_request_owned_dirty_proven,
        'live_source_change_seq',v_live_source_change_seq,
        'live_dirty_generation',v_live_dirty_generation
      )
    );

    v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate,'pay_batch_candidate_id',v_request_candidate.pay_batch_candidate_id,
        'admitted',v_rejection IS NULL,'rejection_reason',v_rejection,
        'v2_chain_digest',v_chain->>'chain_digest','selected_anchor_digest',v_selected_anchor_digest,
        'affected_physical_closure_digest',v_affected_digest,'residual_proof_digest',v_residual_digest,
        'authority_digest',v_authority_digest,'frozen_original_count',v_f_count,
        'request_owned_dirty_job_id',v_request_owned_dirty_job_id,
        'request_owned_dirty_proven',v_request_owned_dirty_proven,
        'affected_physical_count',v_a_count,'expected_residual_count',v_e_count,
        'current_source_count',v_c_count,'ready_preview_count',v_p_count,
        'context_unproved_item_count',v_context_unproved_item_count,
        'context_ambiguous_item_count',v_context_ambiguous_item_count,
        'unproved_affected_item_count',v_unproved_affected_item_count,
        'affected_closure_ambiguity_count',v_affected_closure_ambiguity_count,
        'expected_residual_identity_digest',v_e_identity,
        'current_source_identity_digest',v_c_identity,'ready_preview_identity_digest',v_p_identity,
        'affected_physical_rows',COALESCE(v_affected_rows,'[]'::jsonb)
      )
    ));
    v_authorities:=v_authorities||pg_catalog.jsonb_build_object(v_candidate::text,
      pg_catalog.jsonb_build_object(
        'contract_version','CANCELLATION_REVERSION_Q_BOUND_PRE_REQUEST_START_AUTHORITY_V1',
        'pay_batch_candidate_id',v_request_candidate.pay_batch_candidate_id,
        'v2_chain_digest',v_chain->>'chain_digest','selected_anchor_digest',v_selected_anchor_digest,
        'affected_physical_closure_digest',v_affected_digest,'residual_proof_digest',v_residual_digest,
        'authority_digest',v_authority_digest,'admitted',v_rejection IS NULL,
        'rejection_reason',v_rejection
      ));
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','EXECUTION_RESIDUAL_IDENTITY_PROOF_PAGE_V1',
    'mode',v_mode,'boundary','AFTER_REQUEST_PREPARE_BEFORE_REQUEST_START',
    'correction_request_id',p_correction_request_id,
    'correction_operation_id',p_correction_operation_id,
    'execution_operation_id',p_execution_operation_id,'pay_batch_id',p_pay_batch_id,
    'workbench_session_id',p_workbench_session_id,
    'workbench_session_version',p_workbench_session_version,
    'request_selection_hash',v_request.selection_hash,'request_plan_hash',v_request.plan_hash,
    'candidate_count',v_supplied_candidate_count,
    'admitted_count',(SELECT pg_catalog.count(*)::integer
      FROM pg_catalog.jsonb_array_elements(v_results) AS result(value)
      WHERE COALESCE((result.value->>'admitted')::boolean,false)),
    'all_admitted',NOT EXISTS (
      SELECT 1 FROM pg_catalog.jsonb_array_elements(v_results) AS result(value)
      WHERE COALESCE((result.value->>'admitted')::boolean,false) IS NOT TRUE
    ),
    'candidate_results',v_results,'candidate_authorities',v_authorities,
    'authority_set_digest',private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'contract_version','CANCELLATION_REVERSION_Q_BOUND_PRE_REQUEST_START_SET_V1',
        'boundary','AFTER_REQUEST_PREPARE_BEFORE_REQUEST_START',
        'correction_request_id',p_correction_request_id,
        'correction_operation_id',p_correction_operation_id,
        'execution_operation_id',p_execution_operation_id,'pay_batch_id',p_pay_batch_id,
        'request_selection_hash',v_request.selection_hash,'request_plan_hash',v_request.plan_hash,
        'candidate_authorities',v_authorities
      )
    ),
    'policy_x_authority_scope','POST_DRAFT_FROZEN_SELECTION_PLUS_EXACT_V2_UNSENT_EXECUTION_RESIDUAL'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_execution_residual_identity_proof_page_v1(
  uuid,uuid,uuid,uuid,uuid,bigint,uuid[],text,text,jsonb,jsonb,text
) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_execution_residual_identity_proof_page_v1(
  uuid,uuid,uuid,uuid,uuid,bigint,uuid[],text,text,jsonb,jsonb,text
) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_execution_residual_identity_proof_page_v1(
  uuid,uuid,uuid,uuid,uuid,bigint,uuid[],text,text,jsonb,jsonb,text
) TO postgres;
