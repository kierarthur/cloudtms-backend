-- CloudTMS Banking Pay: exact, read-only proof for an execution-owned
-- provider-unsubmitted transfer overlay.  This helper is deliberately a
-- subproof: the cancellation proof core remains the only route decision owner.

\ir 13082026_1245_pay_workbench_execution_refresh_owner_proof_page_v1.sql

CREATE OR REPLACE FUNCTION private.pay_workbench_unsent_execution_overlay_proof_page_v1(
  p_pay_batch_id uuid,
  p_candidate_ids uuid[],
  p_mode text DEFAULT 'PRE_REQUEST'::text,
  p_correction_request_id uuid DEFAULT NULL::uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL RESTRICTED
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode,'PRE_REQUEST')));
  v_results jsonb:='[]'::jsonb;
  v_count integer:=0;
  v_refresh_owner_execution_operation_id uuid:=NULL::uuid;
  v_refresh_owner_proof jsonb:=pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','EXECUTION_REFRESH_OWNER_PROOF_PAGE_V1',
    'candidate_count',0,'admitted_count',0,'candidate_results','[]'::jsonb
  );
  v_refresh_owner_bridge_observe_enabled boolean:=false;
  v_refresh_owner_bridge_publish_enabled boolean:=false;
BEGIN
  IF p_pay_batch_id IS NULL OR p_candidate_ids IS NULL
     OR pg_catalog.cardinality(p_candidate_ids)<1
     OR pg_catalog.cardinality(p_candidate_ids)>100
     OR v_mode NOT IN ('PRE_REQUEST','REQUEST_OWNED_CONTINUITY','OBSERVE_ONLY')
     OR (v_mode='REQUEST_OWNED_CONTINUITY' AND p_correction_request_id IS NULL)
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb))<>'object'
     OR p_options_json<>'{}'::jsonb THEN
    RAISE EXCEPTION 'EXECUTION_UNSENT_OVERLAY_PROOF_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_UNSENT_OVERLAY_PROOF_ARGUMENT_INVALID','mode',v_mode
      )::text;
  END IF;

  SELECT
    COALESCE((pg_catalog.to_jsonb(settings_row)
      ->>'banking_pay_execution_refresh_owner_bridge_v1_observe_enabled')::boolean,false),
    COALESCE((pg_catalog.to_jsonb(settings_row)
      ->>'banking_pay_execution_refresh_owner_bridge_v1_publish_enabled')::boolean,false)
  INTO v_refresh_owner_bridge_observe_enabled,v_refresh_owner_bridge_publish_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1
  LIMIT 1;

  v_refresh_owner_bridge_publish_enabled:=
    v_refresh_owner_bridge_observe_enabled
    AND v_refresh_owner_bridge_publish_enabled
    AND v_mode='PRE_REQUEST';

  IF v_refresh_owner_bridge_observe_enabled AND v_mode IN ('PRE_REQUEST','OBSERVE_ONLY') THEN
    SELECT operation_row.id
    INTO v_refresh_owner_execution_operation_id
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.operation_type='PAYMENT_EXECUTE'
      AND operation_row.status='COMPLETE'
      AND operation_row.phase='COMPLETE'
      AND operation_row.pay_batch_id=p_pay_batch_id
      AND EXISTS (
        SELECT 1
        FROM pg_catalog.unnest(p_candidate_ids) AS requested_candidate(candidate_id)
        WHERE COALESCE(
          operation_row.progress_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested_candidate.candidate_id::text,
          operation_row.result_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested_candidate.candidate_id::text
        ) IS NOT NULL
      )
    ORDER BY operation_row.completed_at_utc DESC NULLS LAST,operation_row.created_at_utc DESC,
             operation_row.id DESC
    LIMIT 1;

    IF v_refresh_owner_execution_operation_id IS NOT NULL THEN
      v_refresh_owner_proof:=private.pay_workbench_execution_refresh_owner_proof_page_v1(
        v_refresh_owner_execution_operation_id,p_pay_batch_id,p_candidate_ids,
        CASE WHEN v_mode='PRE_REQUEST' THEN 'PRE_REQUEST' ELSE 'OBSERVE_ONLY' END,
        '{}'::jsonb
      );
    END IF;
  END IF;

  WITH requested AS (
    SELECT candidate_scope.candidate_id,pg_catalog.min(candidate_scope.ordinality)::bigint AS ordinality
    FROM pg_catalog.unnest(p_candidate_ids) WITH ORDINALITY AS candidate_scope(candidate_id,ordinality)
    WHERE candidate_scope.candidate_id IS NOT NULL
    GROUP BY candidate_scope.candidate_id
  ), base AS (
    SELECT requested.ordinality,requested.candidate_id,
      batch_row.id AS pay_batch_id,batch_row.source_workbench_session_id AS session_id,
      batch_row.source_snapshot_run_id,batch_row.source_session_version,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.execution_commit_state,'NOT_SUBMITTED')))
        AS execution_commit_state,
      batch_candidate.id AS pay_batch_candidate_id,batch_candidate.settlement_status,
      batch_candidate.settled_at_utc,batch_candidate.remittance_sent_at_utc,
      draft_operation.id AS draft_operation_id,draft_scope.allocation_basis_json,
      draft_scope.allocation_basis_json->'source_publication_attestation' AS frozen_attestation,
      draft_scope.allocation_basis_json->>'source_publication_id' AS frozen_publication_id,
      current_scope.certified_preview_publication_source_publication_id AS current_publication_id,
      current_scope.certified_preview_publication_attestation_json AS current_attestation,
      current_scope.certified_preview_publication_parity_ok AS current_parity,
      COALESCE(change_counter.seq,0) AS live_source_change_seq,
      COALESCE(change_counter.scope_change_generation,0) AS live_dirty_generation,
      change_counter.scope_change_tx_token AS live_scope_change_tx_token,
      registry.dirty_generation AS registry_dirty_generation,
      registry.last_scope_change_tx_token AS registry_tx_token,
      execution_chain.operation_id AS execution_chain_operation_id,
      execution_chain.chain_receipt AS execution_chain_receipt,
      dirty_job.id AS dirty_job_id,dirty_job.status AS dirty_job_status,
      dirty_job.started_at_utc AS dirty_job_started_at_utc,
      dirty_job.scope_change_generation AS dirty_job_generation,
      dirty_job.payload_json AS dirty_payload,
      dirty_job.payload_json->'execution_overlay_contexts'->requested.candidate_id::text
        AS execution_context,
      refresh_owner_result.value AS refresh_owner_authority
    FROM requested
    JOIN public.pay_batches AS batch_row ON batch_row.id=p_pay_batch_id
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.pay_batch_id=batch_row.id
     AND batch_candidate.candidate_id=requested.candidate_id
    LEFT JOIN LATERAL (
      SELECT operation_row.id
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type='DRAFT_CREATE' AND operation_row.status='COMPLETE'
        AND (operation_row.pay_batch_id=batch_row.id OR EXISTS (
          SELECT 1 FROM public.banking_pay_operation_candidate_scope AS operation_scope
          WHERE operation_scope.operation_id=operation_row.id
            AND operation_scope.pay_batch_id=batch_row.id
            AND operation_scope.candidate_id=requested.candidate_id
            AND operation_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
        ))
      ORDER BY operation_row.completed_at_utc DESC NULLS LAST,operation_row.created_at_utc DESC
      LIMIT 1
    ) AS draft_operation ON true
    LEFT JOIN public.banking_pay_operation_candidate_scope AS draft_scope
      ON draft_scope.operation_id=draft_operation.id
     AND draft_scope.pay_batch_id=batch_row.id
     AND draft_scope.candidate_id=requested.candidate_id
     AND draft_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
    LEFT JOIN public.banking_pay_workbench_session_scope AS current_scope
      ON current_scope.session_id=batch_row.source_workbench_session_id
     AND current_scope.candidate_id=requested.candidate_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key='pay_candidate:'||requested.candidate_id::text
    LEFT JOIN private.banking_pay_workbench_candidate_scope_registry AS registry
      ON registry.candidate_id=requested.candidate_id
    LEFT JOIN LATERAL (
      SELECT operation_row.id AS operation_id,
        COALESCE(
          operation_row.progress_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested.candidate_id::text,
          operation_row.result_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested.candidate_id::text
        ) AS chain_receipt
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type='PAYMENT_EXECUTE'
        AND operation_row.status='COMPLETE'
        AND operation_row.pay_batch_id=batch_row.id
        AND COALESCE(
          operation_row.progress_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested.candidate_id::text,
          operation_row.result_json->'execution_unsent_overlay_chain_v2'->'candidates'
            ->requested.candidate_id::text
        ) IS NOT NULL
      ORDER BY operation_row.completed_at_utc DESC NULLS LAST,operation_row.created_at_utc DESC
      LIMIT 1
    ) AS execution_chain ON true
    LEFT JOIN LATERAL (
      SELECT candidate_job.*
      FROM public.banking_pay_workbench_jobs AS candidate_job
      WHERE candidate_job.candidate_id=requested.candidate_id
        AND candidate_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND candidate_job.status IN ('QUEUED','RUNNING','SUCCEEDED')
        AND COALESCE(candidate_job.payload_json->>'execution_overlay_causal_contract_version','')
              ='EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'
        AND COALESCE(candidate_job.payload_json->'execution_overlay_contexts'
              ->requested.candidate_id::text->>'pay_batch_id','')=batch_row.id::text
      ORDER BY GREATEST(candidate_job.created_at_utc,
               COALESCE(candidate_job.updated_at_utc,candidate_job.created_at_utc)) DESC,
               candidate_job.id DESC
      LIMIT 1
    ) AS dirty_job ON true
    LEFT JOIN LATERAL (
      SELECT result_row.value
      FROM pg_catalog.jsonb_array_elements(
        COALESCE(v_refresh_owner_proof->'candidate_results','[]'::jsonb)
      ) AS result_row(value)
      WHERE result_row.value->>'candidate_id'=requested.candidate_id::text
      LIMIT 1
    ) AS refresh_owner_result ON true
  ), execution_authority AS (
    SELECT base.*,
      COALESCE(base.execution_chain_operation_id,
        CASE WHEN COALESCE(base.execution_context->>'execution_operation_id','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (base.execution_context->>'execution_operation_id')::uuid END) AS execution_operation_id,
      COALESCE(
        CASE WHEN COALESCE(base.execution_chain_receipt->>'terminal_scope_change_tx_token','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (base.execution_chain_receipt->>'terminal_scope_change_tx_token')::uuid END,
        CASE WHEN COALESCE(base.dirty_payload->>'execution_overlay_scope_change_tx_token','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (base.dirty_payload->>'execution_overlay_scope_change_tx_token')::uuid END)
        AS execution_scope_change_tx_token,
      COALESCE(base.live_scope_change_tx_token,
        CASE WHEN COALESCE(base.dirty_payload->>'scope_change_tx_token','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (base.dirty_payload->>'scope_change_tx_token')::uuid END) AS current_scope_change_tx_token
    FROM base
  ), proof AS (
    SELECT execution_authority.*,
      execution_operation.operation_type AS execution_operation_type,
      execution_operation.status AS execution_operation_status,
      execution_operation.phase AS execution_operation_phase,
      execution_operation.pay_batch_id AS execution_operation_batch_id,
      execution_tx.state AS execution_tx_state,
      execution_tx.allocated_generation AS execution_generation,
      current_tx.state AS current_tx_state,current_tx.allocated_generation AS current_generation,
      COALESCE(scope_proof.scope_count,0) AS scope_count,
      COALESCE(scope_proof.scope_item_count,0) AS scope_item_count,
      COALESCE(scope_proof.active_item_count,0) AS active_item_count,
      COALESCE(scope_proof.scope_amount,0) AS scope_amount,
      COALESCE(scope_proof.scope_item_amount,0) AS scope_item_amount,
      COALESCE(scope_proof.bad_scope_count,0) AS bad_scope_count,
      COALESCE(scope_proof.item_difference_count,0) AS item_difference_count,
      COALESCE(scope_proof.timesheet_difference_count,0) AS timesheet_difference_count,
      COALESCE(scope_proof.transfer_difference_count,0) AS transfer_difference_count,
      scope_proof.active_item_scope_digest,
      scope_proof.execution_transfer_scope_digest,
      COALESCE(scope_proof.provider_attempt_count,0) AS provider_attempt_count,
      COALESCE(scope_proof.transfer_event_count,0) AS transfer_event_count,
      COALESCE(scope_proof.unsafe_transfer_count,0) AS unsafe_transfer_count,
      COALESCE(scope_proof.timesheet_generation_mismatch_count,0)
        AS timesheet_generation_mismatch_count,
      correction_operation.id AS correction_operation_id
    FROM execution_authority
    LEFT JOIN public.banking_pay_operations AS execution_operation
      ON execution_operation.id=execution_authority.execution_operation_id
    LEFT JOIN public.banking_pay_scope_change_transactions AS execution_tx
      ON execution_tx.tx_token=execution_authority.execution_scope_change_tx_token
    LEFT JOIN public.banking_pay_scope_change_transactions AS current_tx
      ON current_tx.tx_token=execution_authority.current_scope_change_tx_token
    LEFT JOIN LATERAL (
      SELECT operation_row.id
      FROM public.banking_pay_operations AS operation_row
      WHERE p_correction_request_id IS NOT NULL
        AND operation_row.operation_type='PAYMENT_CORRECTION'
        AND operation_row.input_json->>'correction_request_id'=p_correction_request_id::text
      ORDER BY operation_row.created_at_utc,operation_row.id LIMIT 1
    ) AS correction_operation ON true
    LEFT JOIN LATERAL (
      WITH exact_scope AS (
        SELECT transfer_scope.*
        FROM public.banking_pay_operation_transfer_scope AS transfer_scope
        WHERE transfer_scope.operation_id=execution_authority.execution_operation_id
          AND transfer_scope.pay_batch_id=p_pay_batch_id
          AND EXISTS (
            SELECT 1 FROM public.banking_pay_operation_transfer_scope_items AS membership
            WHERE membership.operation_id=transfer_scope.operation_id
              AND membership.transfer_scope_id=transfer_scope.id
              AND membership.candidate_id=execution_authority.candidate_id
          )
      ), exact_membership AS (
        SELECT membership.*
        FROM public.banking_pay_operation_transfer_scope_items AS membership
        JOIN exact_scope ON exact_scope.id=membership.transfer_scope_id
        WHERE membership.operation_id=execution_authority.execution_operation_id
          AND membership.pay_batch_id=p_pay_batch_id
          AND membership.candidate_id=execution_authority.candidate_id
      ), active_items AS (
        SELECT item.id,item.timesheet_id,item.pay_bank_transfer_id,item.amount_inc_vat,
          item.item_type,item.finance_case_id,item.finance_component_id,item.reservation_id
        FROM public.pay_batch_items AS item
        JOIN public.pay_batch_candidates AS candidate_row
          ON candidate_row.id=item.pay_batch_candidate_id
        WHERE candidate_row.pay_batch_id=p_pay_batch_id
          AND candidate_row.candidate_id=execution_authority.candidate_id
          AND COALESCE(item.is_voided,false) IS NOT TRUE
      ), item_difference AS (
        (SELECT id FROM active_items EXCEPT SELECT pay_batch_item_id FROM exact_membership)
        UNION ALL
        (SELECT pay_batch_item_id FROM exact_membership EXCEPT SELECT id FROM active_items)
      ), timesheet_difference AS (
        (SELECT timesheet_id FROM active_items WHERE timesheet_id IS NOT NULL
          EXCEPT SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(execution_authority.execution_context->'timesheet_ids','[]'::jsonb)) AS x(value))
        UNION ALL
        (SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(execution_authority.execution_context->'timesheet_ids','[]'::jsonb)) AS x(value)
          EXCEPT SELECT timesheet_id FROM active_items WHERE timesheet_id IS NOT NULL)
      ), transfer_difference AS (
        (SELECT DISTINCT pay_bank_transfer_id FROM active_items WHERE pay_bank_transfer_id IS NOT NULL
          EXCEPT SELECT id::uuid FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(execution_authority.execution_context->'pay_bank_transfer_ids','[]'::jsonb)) AS x(id))
        UNION ALL
        (SELECT id::uuid FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(execution_authority.execution_context->'pay_bank_transfer_ids','[]'::jsonb)) AS x(id)
          EXCEPT SELECT DISTINCT pay_bank_transfer_id FROM active_items WHERE pay_bank_transfer_id IS NOT NULL)
      )
      SELECT
        (SELECT pg_catalog.count(*) FROM exact_scope)::integer AS scope_count,
        (SELECT pg_catalog.count(*) FROM exact_membership)::integer AS scope_item_count,
        (SELECT pg_catalog.count(*) FROM active_items)::integer AS active_item_count,
        (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(amount),0),2) FROM exact_scope) AS scope_amount,
        (SELECT pg_catalog.round(COALESCE(pg_catalog.sum(item_amount),0),2) FROM exact_membership)
          AS scope_item_amount,
        (SELECT pg_catalog.count(*) FROM exact_scope WHERE status<>'PREPARED'
          OR provider_submit_ready IS NOT TRUE OR provider_submit_state<>'READY'
          OR provider_submit_attempt_count<>0 OR provider_submit_chunk_id IS NOT NULL
          OR provider_submit_claimed_at_utc IS NOT NULL
          OR provider_request_sending_at_utc IS NOT NULL OR provider_request_sent_at_utc IS NOT NULL
          OR provider_response_at_utc IS NOT NULL OR provider_transaction_id IS NOT NULL
          OR provider_submission_status IS NOT NULL OR provider_review_required
          OR pay_bank_transfer_id IS NULL OR prepared_item_count<=0
          OR prepared_item_count<>(SELECT pg_catalog.count(*) FROM exact_membership m
             WHERE m.transfer_scope_id=exact_scope.id)
          OR pg_catalog.round(prepared_amount_total,2)<>pg_catalog.round(amount,2)
          OR NULLIF(pg_catalog.btrim(COALESCE(prepared_scope_hash,'')),'') IS NULL
          OR NULLIF(pg_catalog.btrim(COALESCE(prepared_result_hash,'')),'') IS NULL)::integer
          AS bad_scope_count,
        (SELECT pg_catalog.count(*) FROM item_difference)::integer AS item_difference_count,
        (SELECT pg_catalog.count(*) FROM timesheet_difference)::integer AS timesheet_difference_count,
        (SELECT pg_catalog.count(*) FROM transfer_difference)::integer AS transfer_difference_count,
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(
          active_items.id::text||':'||COALESCE(active_items.timesheet_id::text,'')||':'||
          COALESCE(active_items.pay_bank_transfer_id::text,'')||':'||
          COALESCE(active_items.amount_inc_vat::text,'')||':'||
          COALESCE(active_items.item_type,'')||':'||
          COALESCE(active_items.finance_case_id::text,'')||':'||
          COALESCE(active_items.finance_component_id::text,'')||':'||
          COALESCE(active_items.reservation_id::text,'')
          ,'|' ORDER BY active_items.id) FROM active_items),'')) AS active_item_scope_digest,
        pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(
          exact_membership.transfer_scope_id::text||':'||
          exact_membership.pay_batch_item_id::text||':'||
          COALESCE(exact_scope.pay_bank_transfer_id::text,'')||':'||
          COALESCE(exact_membership.item_amount::text,'')
          ,'|' ORDER BY exact_membership.transfer_scope_id,exact_membership.pay_batch_item_id)
          FROM exact_membership
          JOIN exact_scope ON exact_scope.id=exact_membership.transfer_scope_id),''))
          AS execution_transfer_scope_digest,
        (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_provider_attempts AS attempt
          WHERE attempt.operation_id=execution_authority.execution_operation_id
             OR attempt.pay_batch_id=p_pay_batch_id)::integer AS provider_attempt_count,
        (SELECT pg_catalog.count(*) FROM public.pay_bank_transfer_events AS event_row
          WHERE event_row.pay_batch_id=p_pay_batch_id)::integer AS transfer_event_count,
        (SELECT pg_catalog.count(*) FROM public.pay_bank_transfers AS transfer_row
          WHERE transfer_row.id IN (SELECT pay_bank_transfer_id FROM exact_scope)
            AND (pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.status,'')))<>'PENDING'
              OR transfer_row.rail_tx_id IS NOT NULL OR transfer_row.completed_at_utc IS NOT NULL
              OR pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.rail_state,'')))
                   NOT IN ('','LOCAL','PENDING')
              OR pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.failed_reason,'')))
                   IN ('PROVIDER_REJECTED','PROVIDER_UNKNOWN','PROVIDER_OUTCOME_UNKNOWN','REQUEST_SENT_LOCAL')
              OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
                   transfer_row.rail_meta_json#>>'{provider_submit_diagnostic,provider_request_sent}',
                   ''
                 ))) IN ('true','t','1','yes','y','on')
              OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
                   transfer_row.rail_meta_json#>>'{provider_submit_diagnostic,provider_called}',
                   ''
                 ))) IN ('true','t','1','yes','y','on')
              OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
                   transfer_row.rail_meta_json#>>'{provider_submit_diagnostic,provider_response_present}',
                   ''
                 ))) IN ('true','t','1','yes','y','on')))::integer
          AS unsafe_transfer_count,
        (SELECT pg_catalog.count(*) FROM private.banking_pay_workbench_timesheet_scope_state AS state_row
          WHERE state_row.candidate_id=execution_authority.candidate_id
            AND state_row.timesheet_id IN (SELECT timesheet_id FROM active_items WHERE timesheet_id IS NOT NULL)
            AND (state_row.dirty_generation IS DISTINCT FROM execution_authority.live_dirty_generation
              OR state_row.last_scope_change_tx_token IS DISTINCT FROM execution_authority.current_scope_change_tx_token))::integer
          AS timesheet_generation_mismatch_count
    ) AS scope_proof ON true
  ), classified AS (
    SELECT proof.*,
      CASE
        WHEN proof.session_id IS NULL THEN 'EXECUTION_OVERLAY_NO_SOURCE_SESSION'
        WHEN proof.draft_operation_id IS NULL OR proof.allocation_basis_json IS NULL
          THEN 'EXECUTION_OVERLAY_DRAFT_SCOPE_MISMATCH'
        WHEN COALESCE(proof.frozen_attestation->>'attestation_version','')
              <>'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          OR COALESCE((proof.frozen_attestation->>'semantic_ready')::boolean,false) IS NOT TRUE
          OR COALESCE((proof.frozen_attestation->>'parity_complete')::boolean,false) IS NOT TRUE
          THEN 'EXECUTION_OVERLAY_PUBLICATION_MISMATCH'
        WHEN (
          proof.current_parity IS NOT TRUE
          OR proof.current_publication_id::text IS DISTINCT FROM proof.frozen_publication_id
          OR COALESCE(proof.current_attestation->>'economic_build_id','')
               IS DISTINCT FROM COALESCE(proof.frozen_attestation->>'economic_build_id','')
          OR COALESCE(proof.current_attestation->>'source_identity_digest','')
               IS DISTINCT FROM COALESCE(proof.frozen_attestation->>'source_identity_digest','')
          OR COALESCE(proof.current_attestation->>'semantic_proof_digest','')
               IS DISTINCT FROM COALESCE(proof.frozen_attestation->>'semantic_proof_digest','')
        ) AND NOT (
          v_refresh_owner_bridge_publish_enabled
          AND COALESCE((proof.refresh_owner_authority->>'admitted')::boolean,false)
          AND COALESCE(proof.refresh_owner_authority->>'currentness_basis','')
                ='EXACT_UNSENT_EXECUTION_REFRESH_OWNER'
        ) THEN 'EXECUTION_OVERLAY_PUBLICATION_MISMATCH'
        WHEN proof.execution_chain_receipt IS NOT NULL
          AND (COALESCE(proof.execution_chain_receipt->>'contract_version','')
                <>'EXECUTION_UNSENT_OVERLAY_CHAIN_V2'
            OR COALESCE((proof.execution_chain_receipt->>'closed')::boolean,false) IS NOT TRUE)
          THEN COALESCE(NULLIF(proof.execution_chain_receipt->>'rejection_reason',''),
            'EXECUTION_OVERLAY_CHAIN_NOT_CLOSED')
        WHEN proof.execution_chain_receipt IS NOT NULL
          AND (COALESCE(proof.execution_chain_receipt->>'execution_operation_id','')
                <>COALESCE(proof.execution_operation_id::text,'')
            OR COALESCE(proof.execution_chain_receipt->>'pay_batch_id','')<>p_pay_batch_id::text
            OR COALESCE(proof.execution_chain_receipt->>'candidate_id','')<>proof.candidate_id::text)
          THEN 'EXECUTION_OVERLAY_CHAIN_SCOPE_MISMATCH'
        WHEN proof.execution_chain_receipt IS NOT NULL
          AND (COALESCE(proof.execution_chain_receipt->>'draft_operation_id','')
                <>COALESCE(proof.draft_operation_id::text,'')
            OR COALESCE(proof.execution_chain_receipt->>'original_economic_build_id','')
                IS DISTINCT FROM COALESCE(proof.frozen_attestation->>'economic_build_id','')
            OR COALESCE(proof.execution_chain_receipt->>'original_source_build_run_id','')
                IS DISTINCT FROM COALESCE(
                  proof.frozen_attestation->>'original_source_build_run_id',
                  proof.allocation_basis_json->>'source_build_run_id','')
            OR COALESCE(proof.execution_chain_receipt->>'original_source_publication_id','')
                IS DISTINCT FROM COALESCE(proof.frozen_publication_id,'')
            OR COALESCE(proof.execution_chain_receipt->>'original_source_identity_digest','')
                IS DISTINCT FROM COALESCE(proof.allocation_basis_json->>'source_identity_digest','')
            OR COALESCE(proof.execution_chain_receipt->>'original_semantic_proof_digest','')
                IS DISTINCT FROM COALESCE(proof.allocation_basis_json->>'semantic_proof_digest',''))
          THEN 'EXECUTION_OVERLAY_CHAIN_DRAFT_AUTHORITY_MISMATCH'
        WHEN proof.execution_chain_receipt IS NOT NULL
          AND (COALESCE(proof.execution_chain_receipt->>'transition_count','') !~ '^\d+$'
            OR COALESCE(CASE
              WHEN COALESCE(proof.execution_chain_receipt->>'transition_count','') ~ '^\d+$'
                THEN (proof.execution_chain_receipt->>'transition_count')::integer
              END,0) NOT BETWEEN 1 AND 16)
          THEN 'EXECUTION_OVERLAY_CHAIN_TOO_LARGE'
        WHEN proof.execution_chain_receipt IS NULL AND proof.dirty_job_id IS NULL
          THEN 'EXECUTION_OVERLAY_DIRTY_JOB_MISSING'
        WHEN proof.execution_chain_receipt IS NULL AND proof.dirty_job_started_at_utc IS NOT NULL
          THEN 'EXECUTION_OVERLAY_DIRTY_JOB_ALREADY_STARTED'
        WHEN proof.execution_chain_receipt IS NULL
          AND (COALESCE(proof.dirty_payload->>'execution_overlay_causal_contract_version','')
              <>'EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'
          OR COALESCE(proof.execution_context->>'contract_version','')
              <>'EXECUTION_UNSENT_OVERLAY_CONTEXT_V1')
          THEN 'EXECUTION_OVERLAY_CAUSAL_CONTEXT_MISSING'
        WHEN proof.execution_chain_receipt IS NULL AND (
          COALESCE(proof.execution_context->>'pay_batch_id','')<>p_pay_batch_id::text
          OR COALESCE(proof.execution_context->>'candidate_id','')<>proof.candidate_id::text
          OR proof.execution_operation_id IS NULL
          OR COALESCE(proof.execution_context->>'context_digest','')
               IS DISTINCT FROM pg_catalog.md5(
                 proof.execution_operation_id::text||'|'||p_pay_batch_id::text||'|'||
                 proof.candidate_id::text||'|'||
                 COALESCE((proof.execution_context->'pay_batch_candidate_ids')::text,'[]')||'|'||
                 COALESCE((proof.execution_context->'pay_batch_item_ids')::text,'[]')||'|'||
                 COALESCE((proof.execution_context->'timesheet_ids')::text,'[]')||'|'||
                 COALESCE((proof.execution_context->'transfer_scope_ids')::text,'[]')||'|'||
                 COALESCE((proof.execution_context->'pay_bank_transfer_ids')::text,'[]')||'|'||
                 COALESCE(proof.execution_context->>'source_workbench_session_id','')||'|'||
                 COALESCE(proof.execution_context->>'source_snapshot_run_id','')||'|'||
                 COALESCE(proof.execution_context->>'source_session_version','')||
                 '|EXECUTION_UNSENT_OVERLAY_CONTEXT_V1'))
          THEN 'EXECUTION_OVERLAY_CAUSAL_CONTEXT_MISMATCH'
        WHEN proof.execution_operation_type<>'PAYMENT_EXECUTE'
          OR proof.execution_operation_batch_id IS DISTINCT FROM p_pay_batch_id
          OR proof.execution_operation_phase<>'COMPLETE'
          OR proof.execution_operation_status NOT IN ('PREPARED','COMPLETE')
          THEN 'EXECUTION_OVERLAY_NOT_PRESENT'
        WHEN proof.execution_tx_state<>'FINALIZED' OR proof.execution_generation IS NULL
          THEN 'EXECUTION_OVERLAY_TRANSACTION_NOT_FINALIZED'
        WHEN proof.scope_count<1 OR proof.scope_item_count<1 OR proof.bad_scope_count<>0
          OR proof.item_difference_count<>0
          OR (proof.execution_chain_receipt IS NULL AND proof.timesheet_difference_count<>0)
          OR (proof.execution_chain_receipt IS NULL AND proof.transfer_difference_count<>0)
          OR (proof.execution_chain_receipt IS NOT NULL AND
            COALESCE(proof.execution_chain_receipt->>'active_item_scope_digest','')
              IS DISTINCT FROM COALESCE(proof.active_item_scope_digest,''))
          OR (proof.execution_chain_receipt IS NOT NULL AND
            COALESCE(proof.execution_chain_receipt->>'execution_transfer_scope_digest','')
              IS DISTINCT FROM COALESCE(proof.execution_transfer_scope_digest,''))
          THEN 'EXECUTION_OVERLAY_ITEM_SCOPE_MISMATCH'
        WHEN proof.scope_item_count<>proof.active_item_count
          OR pg_catalog.round(proof.scope_amount,2)<>pg_catalog.round(proof.scope_item_amount,2)
          THEN 'EXECUTION_OVERLAY_AMOUNT_MISMATCH'
        WHEN proof.execution_commit_state<>'NOT_SUBMITTED'
          OR proof.provider_attempt_count<>0 THEN 'EXECUTION_OVERLAY_PROVIDER_BOUNDARY_CROSSED'
        WHEN proof.unsafe_transfer_count<>0 OR proof.transfer_event_count<>0
          OR pg_catalog.upper(COALESCE(proof.settlement_status,''))='SETTLED'
          OR proof.settled_at_utc IS NOT NULL OR proof.remittance_sent_at_utc IS NOT NULL
          THEN 'EXECUTION_OVERLAY_RAIL_OR_SETTLEMENT_EVIDENCE'
        WHEN v_mode='PRE_REQUEST' AND (
          proof.current_scope_change_tx_token IS DISTINCT FROM proof.execution_scope_change_tx_token
          OR proof.current_tx_state<>'FINALIZED'
          OR proof.current_generation IS DISTINCT FROM proof.live_dirty_generation
          OR proof.execution_generation IS DISTINCT FROM proof.live_dirty_generation
          OR (proof.execution_chain_receipt IS NULL
            AND proof.dirty_job_generation IS DISTINCT FROM proof.live_dirty_generation)
          OR (proof.execution_chain_receipt IS NULL
            AND COALESCE(proof.dirty_payload->>'source_change_seq','')<>proof.live_source_change_seq::text)
          OR (proof.execution_chain_receipt IS NOT NULL
            AND COALESCE(proof.execution_chain_receipt->>'terminal_execution_generation','')
                  <>proof.live_dirty_generation::text)
          OR (proof.execution_chain_receipt IS NOT NULL
            AND COALESCE(proof.execution_chain_receipt->>'terminal_source_change_seq','')
                  <>proof.live_source_change_seq::text)
          OR proof.registry_dirty_generation IS DISTINCT FROM proof.live_dirty_generation
          OR proof.registry_tx_token IS DISTINCT FROM proof.current_scope_change_tx_token
          OR proof.timesheet_generation_mismatch_count<>0)
          THEN 'EXECUTION_OVERLAY_GENERATION_MISMATCH'
        WHEN v_mode='REQUEST_OWNED_CONTINUITY' AND (
          proof.correction_operation_id IS NULL
          OR COALESCE(proof.dirty_payload->>'correction_dirty_causal_contract_version','')
               <>'CORRECTION_OWNED_DIRTY_CAUSAL_V1'
          OR COALESCE(proof.dirty_payload->'correction_dirty_contexts'
               ->proof.candidate_id::text->>'correction_request_id','')
               <>p_correction_request_id::text
          OR COALESCE(proof.dirty_payload->'correction_dirty_contexts'
               ->proof.candidate_id::text->>'correction_operation_id','')
               <>proof.correction_operation_id::text
          OR COALESCE(proof.dirty_payload->>'request_owned_scope_change_tx_token','')
               <>COALESCE(proof.dirty_payload->>'scope_change_tx_token','')
          OR proof.current_tx_state<>'FINALIZED'
          OR proof.current_generation IS DISTINCT FROM proof.live_dirty_generation
          OR proof.dirty_job_generation IS DISTINCT FROM proof.live_dirty_generation
          OR COALESCE(proof.dirty_payload->>'source_change_seq','')<>proof.live_source_change_seq::text
          OR proof.registry_dirty_generation IS DISTINCT FROM proof.live_dirty_generation
          OR proof.registry_tx_token IS DISTINCT FROM proof.current_scope_change_tx_token
          OR proof.timesheet_generation_mismatch_count<>0)
          THEN 'EXECUTION_OVERLAY_CORRECTION_CONTINUITY_MISMATCH'
        ELSE NULL
      END AS rejection_reason
    FROM proof
  ), result_rows AS (
    SELECT classified.ordinality,pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'candidate_id',classified.candidate_id,
      'pay_batch_candidate_id',classified.pay_batch_candidate_id,
      'overlay_exact',classified.rejection_reason IS NULL,
      'admitted',classified.rejection_reason IS NULL,
      'currentness_basis',CASE
        WHEN v_refresh_owner_bridge_publish_enabled
          AND COALESCE((classified.refresh_owner_authority->>'admitted')::boolean,false)
          THEN 'EXACT_UNSENT_EXECUTION_REFRESH_OWNER'
        ELSE 'EXACT_UNSENT_EXECUTION_OVERLAY' END,
      'rejection_reason',classified.rejection_reason,
      'execution_operation_id',classified.execution_operation_id,
      'execution_overlay_dirty_job_id',classified.dirty_job_id,
      'execution_overlay_scope_change_tx_token',classified.execution_scope_change_tx_token,
      'execution_overlay_scope_change_generation',classified.execution_generation,
      'source_change_seq',classified.live_source_change_seq,
      'dirty_generation',classified.live_dirty_generation,
      'execution_overlay_context_digest',classified.execution_context->>'context_digest',
      'execution_overlay_chain_contract_version',classified.execution_chain_receipt->>'contract_version',
      'execution_overlay_chain_digest',classified.execution_chain_receipt->>'chain_digest',
      'execution_overlay_chain_transition_count',classified.execution_chain_receipt->'transition_count',
      'execution_refresh_owner_bridge_observed',
        v_refresh_owner_bridge_observe_enabled
          AND COALESCE((classified.refresh_owner_authority->>'admitted')::boolean,false),
      'execution_refresh_owner_bridge_published',
        v_refresh_owner_bridge_publish_enabled
          AND COALESCE((classified.refresh_owner_authority->>'admitted')::boolean,false),
      'execution_refresh_owner_authority',classified.refresh_owner_authority,
      'authority_digest',pg_catalog.md5(
        p_pay_batch_id::text||'|'||classified.candidate_id::text||'|'||
        COALESCE(classified.execution_operation_id::text,'')||'|'||
        COALESCE(classified.dirty_job_id::text,'')||'|'||
        COALESCE(classified.execution_scope_change_tx_token::text,'')||'|'||
        COALESCE(classified.execution_generation::text,'')||'|'||
        classified.live_source_change_seq::text||'|'||classified.live_dirty_generation::text||'|'||
        COALESCE(classified.execution_context->>'context_digest','')||'|'||
        COALESCE(classified.execution_chain_receipt->>'chain_digest','')||'|'||
        COALESCE(classified.refresh_owner_authority->>'proof_digest','')||'|'||
        CASE WHEN v_refresh_owner_bridge_publish_enabled
          AND COALESCE((classified.refresh_owner_authority->>'admitted')::boolean,false)
          THEN 'EXACT_UNSENT_EXECUTION_REFRESH_OWNER'
          ELSE 'EXACT_UNSENT_EXECUTION_OVERLAY' END||'|'||
        (classified.rejection_reason IS NULL)::text||'|EXECUTION_UNSENT_OVERLAY_PROOF_PAGE_V1')
    )) AS result_json
    FROM classified
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(result_json ORDER BY ordinality),'[]'::jsonb),
         pg_catalog.count(*)::integer
  INTO v_results,v_count
  FROM result_rows;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','EXECUTION_UNSENT_OVERLAY_PROOF_PAGE_V1',
    'mode',v_mode,'pay_batch_id',p_pay_batch_id,'candidate_count',v_count,
    'admitted_count',(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(v_results) AS r(value)
      WHERE COALESCE((r.value->>'admitted')::boolean,false)),
    'candidate_results',v_results,
    'policy_x_authority_scope','POST_DRAFT_FROZEN_PAYMENT_PROOF_PLUS_EXACT_UNSENT_EXECUTION_OVERLAY'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_unsent_execution_overlay_proof_page_v1(uuid,uuid[],text,uuid,jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_unsent_execution_overlay_proof_page_v1(uuid,uuid[],text,uuid,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_unsent_execution_overlay_proof_page_v1(uuid,uuid[],text,uuid,jsonb)
  TO postgres;
