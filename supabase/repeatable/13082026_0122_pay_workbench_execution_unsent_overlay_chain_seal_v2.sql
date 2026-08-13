-- CloudTMS Banking Pay: close the exact provider-unsubmitted execution-owned
-- dirty chain before PAYMENT_EXECUTE becomes terminal.  This is evidence for
-- the existing cancellation proof owner; it performs no economic mutation.

CREATE OR REPLACE FUNCTION private.pay_workbench_execution_unsent_overlay_chain_seal_v2(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_batch public.pay_batches%ROWTYPE;
  v_candidate record;
  v_candidate_result jsonb;
  v_candidate_results jsonb:='{}'::jsonb;
  v_candidate_count integer:=0;
  v_closed_count integer:=0;
  v_transition_count integer:=0;
  v_distinct_generation_count integer:=0;
  v_link_transition_count integer:=0;
  v_schedule_transition_count integer:=0;
  v_non_final_count integer:=0;
  v_invalid_owned_event_count integer:=0;
  v_owned_source_event_count integer:=0;
  v_terminal_generation bigint:=NULL::bigint;
  v_first_generation bigint:=NULL::bigint;
  v_terminal_token uuid:=NULL::uuid;
  v_live_generation bigint:=0;
  v_live_source_seq bigint:=0;
  v_pre_execution_source_seq bigint:=NULL::bigint;
  v_live_token uuid:=NULL::uuid;
  v_registry_generation bigint:=NULL::bigint;
  v_registry_token uuid:=NULL::uuid;
  v_unowned_generation_count integer:=0;
  v_transition_rows jsonb:='[]'::jsonb;
  v_active_item_scope_digest text:=NULL::text;
  v_transfer_scope_digest text:=NULL::text;
  v_provider_attempt_count integer:=0;
  v_transfer_event_count integer:=0;
  v_unsafe_transfer_count integer:=0;
  v_settlement_count integer:=0;
  v_remittance_count integer:=0;
  v_rejection_reason text:=NULL::text;
  v_chain_digest text:=NULL::text;
  v_draft_operation_id uuid:=NULL::uuid;
  v_original_economic_build_id text:=NULL::text;
  v_original_source_build_run_id text:=NULL::text;
  v_original_source_publication_id text:=NULL::text;
  v_original_source_identity_digest text:=NULL::text;
  v_original_semantic_proof_digest text:=NULL::text;
BEGIN
  IF p_operation_id IS NULL OR p_pay_batch_id IS NULL
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb))<>'object'
     OR COALESCE(p_options_json,'{}'::jsonb)<>'{}'::jsonb THEN
    RAISE EXCEPTION 'EXECUTION_UNSENT_OVERLAY_CHAIN_SEAL_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','EXECUTION_UNSENT_OVERLAY_CHAIN_SEAL_ARGUMENT_INVALID'
      )::text;
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_operation_id;

  SELECT batch_row.* INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id=p_pay_batch_id;

  IF v_operation.id IS NULL OR v_batch.id IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type,'')))<>'PAYMENT_EXECUTE'
     OR v_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',false,'contract_version','EXECUTION_UNSENT_OVERLAY_CHAIN_V2',
      'closed',false,'rejection_reason','EXECUTION_OVERLAY_CHAIN_OPERATION_MISMATCH'
    );
  END IF;

  SELECT pg_catalog.count(DISTINCT membership.candidate_id)::integer
  INTO v_candidate_count
  FROM public.banking_pay_operation_transfer_scope_items AS membership
  WHERE membership.operation_id=p_operation_id
    AND membership.pay_batch_id=p_pay_batch_id
    AND membership.candidate_id IS NOT NULL;

  IF v_candidate_count<1 OR v_candidate_count>100 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',false,'contract_version','EXECUTION_UNSENT_OVERLAY_CHAIN_V2',
      'closed',false,'candidate_count',v_candidate_count,
      'rejection_reason',CASE WHEN v_candidate_count>100
        THEN 'EXECUTION_OVERLAY_CHAIN_CANDIDATE_LIMIT_EXCEEDED'
        ELSE 'EXECUTION_OVERLAY_CHAIN_SCOPE_MISSING' END
    );
  END IF;

  FOR v_candidate IN
    SELECT DISTINCT membership.candidate_id
    FROM public.banking_pay_operation_transfer_scope_items AS membership
    WHERE membership.operation_id=p_operation_id
      AND membership.pay_batch_id=p_pay_batch_id
      AND membership.candidate_id IS NOT NULL
    ORDER BY membership.candidate_id
  LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027));

    v_rejection_reason:=NULL::text;
    v_transition_count:=0;
    v_distinct_generation_count:=0;
    v_link_transition_count:=0;
    v_schedule_transition_count:=0;
    v_non_final_count:=0;
    v_invalid_owned_event_count:=0;
    v_owned_source_event_count:=0;
    v_terminal_generation:=NULL::bigint;
    v_first_generation:=NULL::bigint;
    v_terminal_token:=NULL::uuid;
    v_live_generation:=0;
    v_live_source_seq:=0;
    v_pre_execution_source_seq:=NULL::bigint;
    v_live_token:=NULL::uuid;
    v_registry_generation:=NULL::bigint;
    v_registry_token:=NULL::uuid;
    v_unowned_generation_count:=0;
    v_transition_rows:='[]'::jsonb;
    v_active_item_scope_digest:=NULL::text;
    v_transfer_scope_digest:=NULL::text;
    v_provider_attempt_count:=0;
    v_transfer_event_count:=0;
    v_unsafe_transfer_count:=0;
    v_settlement_count:=0;
    v_remittance_count:=0;
    v_chain_digest:=NULL::text;
    v_draft_operation_id:=NULL::uuid;
    v_original_economic_build_id:=NULL::text;
    v_original_source_build_run_id:=NULL::text;
    v_original_source_publication_id:=NULL::text;
    v_original_source_identity_digest:=NULL::text;
    v_original_semantic_proof_digest:=NULL::text;

    WITH owned_contexts AS (
      SELECT job.id AS dirty_job_id,job.status AS job_status,
        CASE WHEN COALESCE(job.payload_json->>'execution_overlay_scope_change_tx_token','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (job.payload_json->>'execution_overlay_scope_change_tx_token')::uuid END AS tx_token,
        'EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'::text AS causal_contract_version,
        job.payload_json->'execution_overlay_contexts'->v_candidate.candidate_id::text
          AS context_json
      FROM public.banking_pay_workbench_jobs AS job
      WHERE job.candidate_id=v_candidate.candidate_id
        AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND COALESCE(job.payload_json->>'execution_overlay_causal_contract_version','')
              ='EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'
        AND COALESCE(job.payload_json->'execution_overlay_contexts'
              ->v_candidate.candidate_id::text->>'execution_operation_id','')=p_operation_id::text
        AND COALESCE(job.payload_json->'execution_overlay_contexts'
              ->v_candidate.candidate_id::text->>'pay_batch_id','')=p_pay_batch_id::text

      UNION ALL

      SELECT job.id,job.status,
        CASE WHEN COALESCE(job.payload_json->>'execution_overlay_schedule_scope_change_tx_token','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (job.payload_json->>'execution_overlay_schedule_scope_change_tx_token')::uuid END,
        'EXECUTION_UNSENT_SCHEDULE_CAUSAL_V2'::text,
        job.payload_json->'execution_overlay_schedule_contexts'->v_candidate.candidate_id::text
      FROM public.banking_pay_workbench_jobs AS job
      WHERE job.candidate_id=v_candidate.candidate_id
        AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND COALESCE(job.payload_json->>'execution_overlay_schedule_causal_contract_version','')
              ='EXECUTION_UNSENT_SCHEDULE_CAUSAL_V2'
        AND COALESCE(job.payload_json->'execution_overlay_schedule_contexts'
              ->v_candidate.candidate_id::text->>'execution_operation_id','')=p_operation_id::text
        AND COALESCE(job.payload_json->'execution_overlay_schedule_contexts'
              ->v_candidate.candidate_id::text->>'pay_batch_id','')=p_pay_batch_id::text
    ), owned_tokens AS (
      SELECT context_row.tx_token,
        pg_catalog.min(context_row.dirty_job_id::text)::uuid AS representative_job_id,
        pg_catalog.jsonb_agg(DISTINCT context_row.dirty_job_id::text
          ORDER BY context_row.dirty_job_id::text) AS dirty_job_ids,
        pg_catalog.jsonb_agg(DISTINCT context_row.causal_contract_version
          ORDER BY context_row.causal_contract_version) AS causal_contract_versions,
        pg_catalog.jsonb_agg(DISTINCT context_row.context_json->>'context_digest'
          ORDER BY context_row.context_json->>'context_digest')
          FILTER (WHERE NULLIF(context_row.context_json->>'context_digest','') IS NOT NULL)
          AS context_digests,
        pg_catalog.max(CASE
          WHEN COALESCE(context_row.context_json->>'owned_source_event_count','') ~ '^[1-9][0-9]{0,8}$'
            THEN (context_row.context_json->>'owned_source_event_count')::integer
          ELSE NULL::integer
        END) AS owned_source_event_count,
        pg_catalog.bool_and(
          COALESCE(context_row.context_json->>'owned_source_event_count','') ~ '^[1-9][0-9]{0,8}$'
          AND COALESCE(context_row.context_json->>'owned_source_event_count_digest','')
              =pg_catalog.md5(
                COALESCE(context_row.context_json->>'context_digest','')||'|'||
                context_row.tx_token::text||'|'||v_candidate.candidate_id::text||'|'||
                COALESCE(context_row.context_json->>'owned_source_event_count','')||
                '|EXECUTION_OWNED_SOURCE_EVENT_COUNT_V1'
              )
        ) AS owned_source_event_count_exact
      FROM owned_contexts AS context_row
      WHERE context_row.tx_token IS NOT NULL
      GROUP BY context_row.tx_token
    ), transitions AS (
      SELECT owned_tokens.*,scope_tx.state,scope_tx.allocated_generation,
        pg_catalog.md5(
          owned_tokens.tx_token::text||'|'||COALESCE(scope_tx.allocated_generation::text,'')||'|'||
          COALESCE(owned_tokens.dirty_job_ids::text,'[]')||'|'||
          COALESCE(owned_tokens.causal_contract_versions::text,'[]')||'|'||
          COALESCE(owned_tokens.context_digests::text,'[]')||'|'||
          COALESCE(owned_tokens.owned_source_event_count::text,'')||'|'||
          COALESCE(owned_tokens.owned_source_event_count_exact::text,'')||
          '|EXECUTION_OVERLAY_TRANSITION_V2'
        ) AS transition_digest
      FROM owned_tokens
      LEFT JOIN public.banking_pay_scope_change_transactions AS scope_tx
        ON scope_tx.tx_token=owned_tokens.tx_token
    )
    SELECT pg_catalog.count(*)::integer,
      pg_catalog.count(DISTINCT allocated_generation)::integer,
      pg_catalog.count(*) FILTER (
        WHERE causal_contract_versions @> '["EXECUTION_UNSENT_OVERLAY_CAUSAL_V1"]'::jsonb
      )::integer,
      pg_catalog.count(*) FILTER (
        WHERE causal_contract_versions @> '["EXECUTION_UNSENT_SCHEDULE_CAUSAL_V2"]'::jsonb
      )::integer,
      pg_catalog.count(*) FILTER (WHERE state<>'FINALIZED' OR allocated_generation IS NULL)::integer,
      pg_catalog.count(*) FILTER (
        WHERE owned_source_event_count IS NULL OR owned_source_event_count_exact IS NOT TRUE
      )::integer,
      COALESCE(pg_catalog.sum(owned_source_event_count),0)::integer,
      pg_catalog.min(allocated_generation),pg_catalog.max(allocated_generation),
      (pg_catalog.array_agg(tx_token ORDER BY allocated_generation DESC,tx_token DESC))[1],
      COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'ordinal',ordinality,'scope_change_tx_token',tx_token,
        'allocated_generation',allocated_generation,'transaction_state',state,
        'representative_dirty_job_id',representative_job_id,'dirty_job_ids',dirty_job_ids,
        'causal_contract_versions',causal_contract_versions,
        'context_digests',COALESCE(context_digests,'[]'::jsonb),
        'owned_source_event_count',owned_source_event_count,
        'owned_source_event_count_exact',owned_source_event_count_exact,
        'transition_digest',transition_digest
      ) ORDER BY ordinality),'[]'::jsonb)
    INTO v_transition_count,v_distinct_generation_count,v_link_transition_count,
      v_schedule_transition_count,v_non_final_count,v_invalid_owned_event_count,
      v_owned_source_event_count,v_first_generation,
      v_terminal_generation,v_terminal_token,v_transition_rows
    FROM (
      SELECT transitions.*,
        pg_catalog.row_number() OVER (ORDER BY allocated_generation,tx_token)::integer AS ordinality
      FROM transitions
    ) AS ordered_transitions;

    SELECT COALESCE(counter.seq,0),COALESCE(counter.scope_change_generation,0),
      counter.scope_change_tx_token,registry.dirty_generation,registry.last_scope_change_tx_token
    INTO v_live_source_seq,v_live_generation,v_live_token,
      v_registry_generation,v_registry_token
    FROM public.app_change_counters AS counter
    LEFT JOIN private.banking_pay_workbench_candidate_scope_registry AS registry
      ON registry.candidate_id=v_candidate.candidate_id
    WHERE counter.entity_key='pay_candidate:'||v_candidate.candidate_id::text;

    SELECT draft_operation.id,
      draft_scope.allocation_basis_json#>>'{source_publication_attestation,economic_build_id}',
      COALESCE(
        draft_scope.allocation_basis_json#>>'{source_publication_attestation,original_source_build_run_id}',
        draft_scope.allocation_basis_json->>'source_build_run_id'
      ),
      draft_scope.allocation_basis_json->>'source_publication_id',
      draft_scope.allocation_basis_json->>'source_identity_digest',
      draft_scope.allocation_basis_json->>'semantic_proof_digest',
      CASE
        WHEN COALESCE(
          draft_scope.allocation_basis_json#>>'{post_draft_authority,source_change_seq}',''
        ) ~ '^[0-9]{1,18}$'
          THEN (draft_scope.allocation_basis_json#>>'{post_draft_authority,source_change_seq}')::bigint
        ELSE NULL::bigint
      END
    INTO v_draft_operation_id,v_original_economic_build_id,
      v_original_source_build_run_id,v_original_source_publication_id,
      v_original_source_identity_digest,v_original_semantic_proof_digest,
      v_pre_execution_source_seq
    FROM public.banking_pay_operations AS draft_operation
    JOIN public.banking_pay_operation_candidate_scope AS draft_scope
      ON draft_scope.operation_id=draft_operation.id
     AND draft_scope.pay_batch_id=p_pay_batch_id
     AND draft_scope.candidate_id=v_candidate.candidate_id
     AND draft_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
    WHERE draft_operation.operation_type='DRAFT_CREATE'
      AND draft_operation.status='COMPLETE'
    ORDER BY draft_operation.completed_at_utc DESC NULLS LAST,
      draft_operation.created_at_utc DESC,draft_operation.id DESC
    LIMIT 1;

    SELECT pg_catalog.count(DISTINCT candidate_job.scope_change_generation)::integer
    INTO v_unowned_generation_count
    FROM public.banking_pay_workbench_jobs AS candidate_job
    WHERE candidate_job.candidate_id=v_candidate.candidate_id
      AND candidate_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND candidate_job.scope_change_generation BETWEEN v_first_generation AND v_terminal_generation
      AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(v_transition_rows) AS transition(value)
        WHERE (transition.value->>'allocated_generation')::bigint
              =candidate_job.scope_change_generation
      );

    WITH active_items AS (
      SELECT item.id,item.timesheet_id,item.pay_bank_transfer_id,item.amount_inc_vat,
        item.item_type,item.finance_case_id,item.finance_component_id,item.reservation_id
      FROM public.pay_batch_items AS item
      JOIN public.pay_batch_candidates AS candidate_row
        ON candidate_row.id=item.pay_batch_candidate_id
      WHERE candidate_row.pay_batch_id=p_pay_batch_id
        AND candidate_row.candidate_id=v_candidate.candidate_id
        AND COALESCE(item.is_voided,false) IS NOT TRUE
    ), transfer_membership AS (
      SELECT membership.transfer_scope_id,membership.pay_batch_item_id,
        transfer_scope.pay_bank_transfer_id,membership.item_amount
      FROM public.banking_pay_operation_transfer_scope_items AS membership
      JOIN public.banking_pay_operation_transfer_scope AS transfer_scope
        ON transfer_scope.id=membership.transfer_scope_id
       AND transfer_scope.operation_id=membership.operation_id
      WHERE membership.operation_id=p_operation_id
        AND membership.pay_batch_id=p_pay_batch_id
        AND membership.candidate_id=v_candidate.candidate_id
    )
    SELECT
      pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(
        active_items.id::text||':'||COALESCE(active_items.timesheet_id::text,'')||':'||
        COALESCE(active_items.pay_bank_transfer_id::text,'')||':'||
        COALESCE(active_items.amount_inc_vat::text,'')||':'||COALESCE(active_items.item_type,'')||':'||
        COALESCE(active_items.finance_case_id::text,'')||':'||
        COALESCE(active_items.finance_component_id::text,'')||':'||
        COALESCE(active_items.reservation_id::text,'')
        ,'|' ORDER BY active_items.id) FROM active_items),'')),
      pg_catalog.md5(COALESCE((SELECT pg_catalog.string_agg(
        transfer_membership.transfer_scope_id::text||':'||
        transfer_membership.pay_batch_item_id::text||':'||
        COALESCE(transfer_membership.pay_bank_transfer_id::text,'')||':'||
        COALESCE(transfer_membership.item_amount::text,'')
        ,'|' ORDER BY transfer_membership.transfer_scope_id,transfer_membership.pay_batch_item_id)
        FROM transfer_membership),'')),
      (SELECT pg_catalog.count(*)::integer
       FROM public.banking_pay_operation_provider_attempts AS attempt
       WHERE attempt.operation_id=p_operation_id OR attempt.pay_batch_id=p_pay_batch_id),
      (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfer_events AS event_row
       WHERE event_row.pay_batch_id=p_pay_batch_id),
      (SELECT pg_catalog.count(*)::integer FROM public.pay_bank_transfers AS transfer_row
       WHERE transfer_row.id IN (SELECT pay_bank_transfer_id FROM transfer_membership)
         AND (pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.status,'')))<>'PENDING'
           OR transfer_row.rail_tx_id IS NOT NULL OR transfer_row.completed_at_utc IS NOT NULL
           OR pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.rail_state,'')))
                NOT IN ('','LOCAL','PENDING'))),
      (SELECT pg_catalog.count(*)::integer FROM public.pay_batch_candidates AS candidate_row
       WHERE candidate_row.pay_batch_id=p_pay_batch_id
         AND candidate_row.candidate_id=v_candidate.candidate_id
         AND (pg_catalog.upper(COALESCE(candidate_row.settlement_status,''))='SETTLED'
           OR candidate_row.settled_at_utc IS NOT NULL)),
      (SELECT pg_catalog.count(*)::integer FROM public.pay_batch_candidates AS candidate_row
       WHERE candidate_row.pay_batch_id=p_pay_batch_id
         AND candidate_row.candidate_id=v_candidate.candidate_id
         AND candidate_row.remittance_sent_at_utc IS NOT NULL)
    INTO v_active_item_scope_digest,v_transfer_scope_digest,
      v_provider_attempt_count,v_transfer_event_count,v_unsafe_transfer_count,
      v_settlement_count,v_remittance_count;

    v_rejection_reason:=CASE
      WHEN v_transition_count<1 THEN 'EXECUTION_OVERLAY_CHAIN_MISSING'
      WHEN v_transition_count>16 THEN 'EXECUTION_OVERLAY_CHAIN_TOO_LARGE'
      WHEN v_distinct_generation_count<>v_transition_count
        THEN 'EXECUTION_OVERLAY_CHAIN_DUPLICATE_GENERATION'
      WHEN v_link_transition_count<1 OR v_schedule_transition_count<1
        THEN 'EXECUTION_OVERLAY_CHAIN_INCOMPLETE'
      WHEN v_non_final_count<>0 THEN 'EXECUTION_OVERLAY_CHAIN_NON_FINAL_TRANSACTION'
      WHEN v_invalid_owned_event_count<>0 OR v_owned_source_event_count<v_transition_count
        THEN 'EXECUTION_OVERLAY_CHAIN_SOURCE_EVENT_PROOF_INVALID'
      WHEN v_unowned_generation_count<>0 THEN 'EXECUTION_OVERLAY_CHAIN_UNOWNED_TRANSITION'
      WHEN v_draft_operation_id IS NULL
        OR COALESCE(v_original_economic_build_id,'')
             !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(v_original_source_build_run_id,'')
             !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(v_original_source_publication_id,'')
             !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR NULLIF(pg_catalog.btrim(COALESCE(v_original_source_identity_digest,'')),'') IS NULL
        OR NULLIF(pg_catalog.btrim(COALESCE(v_original_semantic_proof_digest,'')),'') IS NULL
        OR v_pre_execution_source_seq IS NULL
        THEN 'EXECUTION_OVERLAY_DRAFT_AUTHORITY_MISSING'
      WHEN v_live_source_seq IS DISTINCT FROM
           (v_pre_execution_source_seq+v_owned_source_event_count)
        THEN 'EXECUTION_OVERLAY_CHAIN_SOURCE_SEQUENCE_MISMATCH'
      WHEN v_terminal_generation IS DISTINCT FROM v_live_generation
        OR v_registry_generation IS DISTINCT FROM v_live_generation
        THEN 'EXECUTION_OVERLAY_CHAIN_TERMINAL_GENERATION_MISMATCH'
      -- Finalization deliberately clears the staged live token fields.  The
      -- immutable terminal token remains in the FINALIZED transaction receipt.
      WHEN v_live_token IS NOT NULL OR v_registry_token IS NOT NULL
        THEN 'EXECUTION_OVERLAY_CHAIN_LIVE_TOKEN_NOT_CLEARED'
      WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.execution_commit_state,'NOT_SUBMITTED')))
            <>'NOT_SUBMITTED'
        THEN 'EXECUTION_OVERLAY_PROVIDER_ACTIVITY_PRESENT'
      WHEN v_provider_attempt_count<>0 THEN 'EXECUTION_OVERLAY_PROVIDER_ACTIVITY_PRESENT'
      WHEN v_transfer_event_count<>0 OR v_unsafe_transfer_count<>0
        THEN 'EXECUTION_OVERLAY_RAIL_ACTIVITY_PRESENT'
      WHEN v_settlement_count<>0 OR v_remittance_count<>0
        THEN 'EXECUTION_OVERLAY_SETTLEMENT_OR_REMITTANCE_PRESENT'
      ELSE NULL::text
    END;

    v_chain_digest:=pg_catalog.md5(
      p_operation_id::text||'|'||p_pay_batch_id::text||'|'||v_candidate.candidate_id::text||'|'||
      COALESCE(v_batch.source_workbench_session_id::text,'')||'|'||
      COALESCE(v_batch.source_session_version::text,'')||'|'||
      COALESCE(v_batch.source_scope_change_generation::text,'')||'|'||
      COALESCE(v_first_generation::text,'')||'|'||COALESCE(v_terminal_generation::text,'')||'|'||
      COALESCE(v_terminal_token::text,'')||'|'||v_transition_rows::text||'|'||
      COALESCE(v_active_item_scope_digest,'')||'|'||COALESCE(v_transfer_scope_digest,'')||'|'||
      COALESCE(v_draft_operation_id::text,'')||'|'||COALESCE(v_original_economic_build_id,'')||'|'||
      COALESCE(v_original_source_build_run_id,'')||'|'||
      COALESCE(v_original_source_publication_id,'')||'|'||
      COALESCE(v_original_source_identity_digest,'')||'|'||
      COALESCE(v_original_semantic_proof_digest,'')||'|'||
      COALESCE(v_pre_execution_source_seq::text,'')||'|'||
      COALESCE(v_owned_source_event_count::text,'')||'|'||
      COALESCE(v_live_source_seq::text,'')||'|'||COALESCE(v_rejection_reason,'')||
      '|EXECUTION_UNSENT_OVERLAY_CHAIN_V2'
    );

    v_candidate_result:=pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'contract_version','EXECUTION_UNSENT_OVERLAY_CHAIN_V2',
      'execution_operation_id',p_operation_id,'pay_batch_id',p_pay_batch_id,
      'candidate_id',v_candidate.candidate_id,
      'source_workbench_session_id',v_batch.source_workbench_session_id,
      'source_session_version',v_batch.source_session_version,
      'draft_operation_id',v_draft_operation_id,
      'original_economic_build_id',v_original_economic_build_id,
      'original_source_build_run_id',v_original_source_build_run_id,
      'original_source_publication_id',v_original_source_publication_id,
      'original_source_identity_digest',v_original_source_identity_digest,
      'original_semantic_proof_digest',v_original_semantic_proof_digest,
      'pre_execution_dirty_generation',v_batch.source_scope_change_generation,
      'pre_execution_source_change_seq',v_pre_execution_source_seq,
      'first_execution_generation',v_first_generation,
      'terminal_execution_generation',v_terminal_generation,
      'terminal_scope_change_tx_token',v_terminal_token,
      'terminal_source_change_seq',v_live_source_seq,
      'owned_source_event_count',v_owned_source_event_count,
      'transition_count',v_transition_count,'transitions',v_transition_rows,
      'link_transition_count',v_link_transition_count,
      'schedule_transition_count',v_schedule_transition_count,
      'active_item_scope_digest',v_active_item_scope_digest,
      'execution_transfer_scope_digest',v_transfer_scope_digest,
      'provider_attempt_count',v_provider_attempt_count,
      'rail_transaction_count',v_transfer_event_count+v_unsafe_transfer_count,
      'settlement_count',v_settlement_count,'remittance_count',v_remittance_count,
      'execution_commit_state',v_batch.execution_commit_state,
      'closed',v_rejection_reason IS NULL,'rejection_reason',v_rejection_reason,
      'chain_digest',v_chain_digest
    ));

    v_candidate_results:=v_candidate_results||
      pg_catalog.jsonb_build_object(v_candidate.candidate_id::text,v_candidate_result);
    IF v_rejection_reason IS NULL THEN v_closed_count:=v_closed_count+1; END IF;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','EXECUTION_UNSENT_OVERLAY_CHAIN_V2',
    'execution_operation_id',p_operation_id,'pay_batch_id',p_pay_batch_id,
    'candidate_count',v_candidate_count,'closed_candidate_count',v_closed_count,
    'all_closed',v_closed_count=v_candidate_count,'candidates',v_candidate_results,
    'authority_set_digest',pg_catalog.md5(
      p_operation_id::text||'|'||p_pay_batch_id::text||'|'||v_candidate_results::text||
      '|EXECUTION_UNSENT_OVERLAY_CHAIN_SET_V2'),
    'policy_x_authority_scope','POST_DRAFT_FROZEN_PAYMENT_PROOF_PLUS_EXACT_UNSENT_EXECUTION_CHAIN'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_execution_unsent_overlay_chain_seal_v2(uuid,uuid,jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_execution_unsent_overlay_chain_seal_v2(uuid,uuid,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_execution_unsent_overlay_chain_seal_v2(uuid,uuid,jsonb)
  TO postgres;
