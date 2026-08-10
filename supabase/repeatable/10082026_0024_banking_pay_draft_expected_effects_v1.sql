-- Exact transaction-local ownership contract for canonical Draft writes.
--
-- This does not suppress finance changes merely because a Draft operation is
-- active.  A writer must register the exact operation, phase, batch and
-- relation/action allow-list in its own transaction.  The financial dirty
-- trigger independently checks backend PID, transaction ID, operation scope,
-- candidate membership and relation/action before an effect can be consumed.

CREATE OR REPLACE FUNCTION private.pay_workbench_draft_expected_effects_v1(
  p_operation_id uuid,
  p_phase text,
  p_mode text,
  p_effects_json jsonb DEFAULT '[]'::jsonb,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode,'')));
  v_phase text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_phase,'')));
  v_enabled boolean := false;
  v_batch_id uuid := NULL::uuid;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_registered_count integer := 0;
  v_observed_count integer := 0;
  v_unmatched_count integer := 0;
  v_digest text := NULL::text;
BEGIN
  IF v_mode NOT IN ('REGISTER','ASSERT_COMPLETE','READ')
     OR v_phase NOT IN (
       'INSERT_ITEMS','APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
       'CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS'
     )
     OR pg_catalog.jsonb_typeof(COALESCE(p_effects_json,'[]'::jsonb)) <> 'array'
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_ARGUMENT_INVALID'
      USING ERRCODE='22023';
  END IF;

  SELECT COALESCE(settings_row.banking_pay_draft_expected_effects_v1_enabled,false)
  INTO v_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  IF COALESCE(v_enabled,false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'enabled',false,'mode',v_mode,'phase',v_phase,
      'registered_count',0,'observed_count',0,'unmatched_count',0
    );
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_OPERATION_REQUIRED'
      USING ERRCODE='22023';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_operation_id
  FOR SHARE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type,''))) <> 'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status,''))) NOT IN ('QUEUED','RUNNING','PROCESSING','CLAIMED','IN_PROGRESS')
     OR v_operation.workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_OPERATION_INVALID'
      USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
        'operation_id',p_operation_id,'phase',v_phase
      )::text;
  END IF;

  v_batch_id := CASE
    WHEN COALESCE(p_options_json->>'pay_batch_id','')
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    THEN (p_options_json->>'pay_batch_id')::uuid
    ELSE v_operation.pay_batch_id
  END;

  IF v_batch_id IS NULL OR NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.operation_id=p_operation_id
      AND scope_row.pay_batch_id=v_batch_id
      AND scope_row.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_BATCH_SCOPE_INVALID'
      USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
        'operation_id',p_operation_id,'pay_batch_id',v_batch_id,'phase',v_phase
      )::text;
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_draft_expected_effect_context_v1(
    operation_id uuid PRIMARY KEY,
    pay_batch_id uuid NOT NULL,
    workbench_session_id uuid NOT NULL,
    phase text NOT NULL,
    backend_pid integer NOT NULL,
    transaction_id bigint NOT NULL,
    registered_at_utc timestamptz NOT NULL,
    expected_effects_json jsonb NOT NULL,
    context_token text NOT NULL
  ) ON COMMIT DROP;

  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_draft_observed_effects_v1(
    operation_id uuid NOT NULL,
    pay_batch_id uuid NOT NULL,
    phase text NOT NULL,
    relation_name text NOT NULL,
    operation text NOT NULL,
    source_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    timesheet_id uuid NULL,
    before_digest text NULL,
    after_digest text NULL,
    matched boolean NOT NULL,
    observed_at_utc timestamptz NOT NULL,
    PRIMARY KEY(operation_id,phase,relation_name,operation,source_id,candidate_id)
  ) ON COMMIT DROP;

  IF v_mode='REGISTER' THEN
    IF EXISTS (
      SELECT 1
      FROM pg_catalog.jsonb_array_elements(COALESCE(p_effects_json,'[]'::jsonb)) AS effect(value)
      WHERE pg_catalog.jsonb_typeof(effect.value)<>'object'
         OR pg_catalog.upper(pg_catalog.btrim(COALESCE(effect.value->>'operation',''))) NOT IN ('INSERT','UPDATE','DELETE')
         OR pg_catalog.lower(pg_catalog.btrim(COALESCE(effect.value->>'relation_name',''))) NOT IN (
           'pay_batch_items','pay_batch_item_breakdowns','pay_batch_timesheet_snapshots',
           'pay_batch_candidates',
           'pay_advances','pay_finance_case_components','pay_finance_case_events',
           'pay_advance_reservations'
         )
    ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_DESCRIPTOR_INVALID'
        USING ERRCODE='22023';
    END IF;

    TRUNCATE pg_temp._bpay_wb_draft_expected_effect_context_v1;
    TRUNCATE pg_temp._bpay_wb_draft_observed_effects_v1;

    v_digest := pg_catalog.md5(
      p_operation_id::text||'|'||v_batch_id::text||'|'||v_phase||'|'||
      pg_catalog.pg_backend_pid()::text||'|'||pg_catalog.txid_current()::text||'|'||
      COALESCE(p_effects_json,'[]'::jsonb)::text||'|DRAFT_EXPECTED_EFFECTS_V1'
    );

    INSERT INTO pg_temp._bpay_wb_draft_expected_effect_context_v1(
      operation_id,pay_batch_id,workbench_session_id,phase,backend_pid,
      transaction_id,registered_at_utc,expected_effects_json,context_token
    ) VALUES (
      p_operation_id,v_batch_id,v_operation.workbench_session_id,v_phase,
      pg_catalog.pg_backend_pid(),pg_catalog.txid_current(),pg_catalog.clock_timestamp(),
      COALESCE(p_effects_json,'[]'::jsonb),v_digest
    );

    SELECT pg_catalog.jsonb_array_length(COALESCE(p_effects_json,'[]'::jsonb))
    INTO v_registered_count;

    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'enabled',true,'mode',v_mode,'phase',v_phase,
      'operation_id',p_operation_id,'pay_batch_id',v_batch_id,
      'registered_count',v_registered_count,'context_token',v_digest
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_wb_draft_expected_effect_context_v1 AS context_row
    WHERE context_row.operation_id=p_operation_id
      AND context_row.pay_batch_id=v_batch_id
      AND context_row.phase=v_phase
      AND context_row.backend_pid=pg_catalog.pg_backend_pid()
      AND context_row.transaction_id=pg_catalog.txid_current()
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECTS_CONTEXT_MISSING'
      USING ERRCODE='23514';
  END IF;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (WHERE observed.matched IS NOT TRUE)::integer,
         pg_catalog.md5(COALESCE(pg_catalog.string_agg(
           observed.relation_name||':'||observed.operation||':'||observed.source_id::text||':'||
           observed.candidate_id::text||':'||COALESCE(observed.before_digest,'')||':'||
           COALESCE(observed.after_digest,''),
           '|' ORDER BY observed.relation_name,observed.operation,observed.source_id,observed.candidate_id
         ),''))
  INTO v_observed_count,v_unmatched_count,v_digest
  FROM pg_temp._bpay_wb_draft_observed_effects_v1 AS observed
  WHERE observed.operation_id=p_operation_id AND observed.phase=v_phase;

  IF v_mode='ASSERT_COMPLETE' AND COALESCE(v_unmatched_count,0)>0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_EXPECTED_EFFECT_MISMATCH'
      USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
        'operation_id',p_operation_id,'pay_batch_id',v_batch_id,'phase',v_phase,
        'observed_count',v_observed_count,'unmatched_count',v_unmatched_count
      )::text;
  END IF;

  IF v_mode='ASSERT_COMPLETE' THEN
    UPDATE public.banking_pay_operations AS operation_row
    SET progress_json = COALESCE(operation_row.progress_json,'{}'::jsonb)
      || pg_catalog.jsonb_build_object(
        'draft_expected_effects_v1',
        COALESCE(operation_row.progress_json->'draft_expected_effects_v1','{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            pg_catalog.lower(v_phase),
            pg_catalog.jsonb_build_object(
              'phase',v_phase,
              'observed_count',COALESCE(v_observed_count,0),
              'unmatched_count',COALESCE(v_unmatched_count,0),
              'observed_effect_digest',v_digest,
              'completed',true,
              'completed_at_utc',pg_catalog.clock_timestamp()
            )
          )
      ),
      updated_at_utc=pg_catalog.clock_timestamp()
    WHERE operation_row.id=p_operation_id;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'enabled',true,'mode',v_mode,'phase',v_phase,
    'operation_id',p_operation_id,'pay_batch_id',v_batch_id,
    'observed_count',COALESCE(v_observed_count,0),
    'unmatched_count',COALESCE(v_unmatched_count,0),
    'observed_effect_digest',v_digest,
    'context_token',(
      SELECT context_row.context_token
      FROM pg_temp._bpay_wb_draft_expected_effect_context_v1 AS context_row
      WHERE context_row.operation_id=p_operation_id
    )
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_expected_effects_v1(uuid,text,text,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_expected_effects_v1(uuid,text,text,jsonb,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_draft_expected_effects_v1(uuid,text,text,jsonb,jsonb)
  TO postgres;


-- Final DRAFT_CREATE authority fence.  It never invents or recalculates
-- economics: it proves that the already-certified V3 source/preview remains
-- current after frozen Draft artifacts were written, and that no competing
-- Workbench owner is still pending for the bounded candidate page.
CREATE OR REPLACE FUNCTION private.pay_workbench_draft_create_adoption_finalize_v1(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_session_id uuid,
  p_candidate_ids uuid[],
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_enabled boolean:=false;
  v_candidate_ids uuid[]:=ARRAY[]::uuid[];
  v_candidate_count integer:=0;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_invalid_scope_count integer:=0;
  v_active_owner_count integer:=0;
  v_invalid_attestation_count integer:=0;
  v_missing_phase_count integer:=0;
  v_unlinked_allocation_count integer:=0;
  v_semantic_proof jsonb:='{}'::jsonb;
BEGIN
  IF p_operation_id IS NULL OR p_pay_batch_id IS NULL OR p_session_id IS NULL
     OR p_options_json IS NULL OR pg_catalog.jsonb_typeof(p_options_json)<>'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_ARGUMENT_INVALID' USING ERRCODE='22023';
  END IF;

  SELECT COALESCE(settings_row.banking_pay_draft_create_adoption_v1_enabled,false)
  INTO v_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  SELECT COALESCE(pg_catalog.array_agg(candidate.candidate_id ORDER BY candidate.candidate_id),ARRAY[]::uuid[]),
         pg_catalog.count(*)::integer
  INTO v_candidate_ids,v_candidate_count
  FROM (
    SELECT DISTINCT candidate_id
    FROM pg_catalog.unnest(COALESCE(p_candidate_ids,ARRAY[]::uuid[])) AS candidate_id
    WHERE candidate_id IS NOT NULL
  ) AS candidate;

  IF v_candidate_count<1 OR v_candidate_count>100 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_CANDIDATE_LIMIT' USING ERRCODE='22023';
  END IF;

  IF COALESCE(v_enabled,false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'enabled',false,'adopted',false,'candidate_count',v_candidate_count
    );
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type,'')))<>'DRAFT_CREATE'
     OR v_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR v_operation.workbench_session_id IS DISTINCT FROM p_session_id
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status,''))) NOT IN
       ('QUEUED','RUNNING','PROCESSING','CLAIMED','IN_PROGRESS') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_OPERATION_INVALID' USING ERRCODE='23514';
  END IF;

  -- Candidate serial authority is always acquired in UUID order.
  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(public._pay_workbench_candidate_serial_key(candidate_id),24062027)
  )
  FROM pg_catalog.unnest(v_candidate_ids) AS candidate(candidate_id)
  ORDER BY candidate.candidate_id;

  SELECT pg_catalog.count(*)::integer
  INTO v_invalid_scope_count
  FROM pg_catalog.unnest(v_candidate_ids) AS candidate(candidate_id)
  WHERE NOT EXISTS(
    SELECT 1
    FROM public.banking_pay_operation_candidate_scope AS operation_scope
    WHERE operation_scope.operation_id=p_operation_id
      AND operation_scope.pay_batch_id=p_pay_batch_id
      AND operation_scope.workbench_session_id=p_session_id
      AND operation_scope.candidate_id=candidate.candidate_id
      AND operation_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
  );

  IF v_invalid_scope_count>0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_SCOPE_INVALID' USING ERRCODE='23514';
  END IF;

  -- Every canonical Draft writer must have closed its transaction-local
  -- expected-effects phase with no unmatched economic transition.  A missing
  -- phase is not treated as an empty phase: adoption fails closed.
  SELECT pg_catalog.count(*)::integer
  INTO v_missing_phase_count
  FROM pg_catalog.unnest(ARRAY[
    'insert_items','apply_finance_adjustments','finalise_reservations',
    'create_timesheet_snapshots','build_item_breakdowns'
  ]::text[]) AS required_phase(phase)
  WHERE COALESCE((v_operation.progress_json->'draft_expected_effects_v1'
      ->required_phase.phase->>'completed')::boolean,false) IS NOT TRUE
     OR COALESCE((v_operation.progress_json->'draft_expected_effects_v1'
      ->required_phase.phase->>'unmatched_count')::integer,-1) <> 0
     OR NULLIF(v_operation.progress_json->'draft_expected_effects_v1'
      ->required_phase.phase->>'observed_effect_digest','') IS NULL;

  IF v_missing_phase_count>0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_EXPECTED_EFFECTS_INCOMPLETE'
      USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
        'operation_id',p_operation_id,'missing_or_invalid_phase_count',v_missing_phase_count
      )::text;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_unlinked_allocation_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
  JOIN public.banking_pay_operation_candidate_scope AS operation_scope
    ON operation_scope.id=allocation_row.candidate_scope_id
   AND operation_scope.operation_id=p_operation_id
   AND operation_scope.candidate_id=ANY(v_candidate_ids)
  WHERE allocation_row.operation_id=p_operation_id
    AND (
      pg_catalog.upper(pg_catalog.btrim(COALESCE(allocation_row.status,'')))<>'ITEM_CREATED'
      OR allocation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id
      OR allocation_row.pay_batch_item_id IS NULL
      OR NOT EXISTS(
        SELECT 1
        FROM public.pay_batch_items AS item
        JOIN public.pay_batch_candidates AS batch_candidate
          ON batch_candidate.id=item.pay_batch_candidate_id
        WHERE item.id=allocation_row.pay_batch_item_id
          AND batch_candidate.pay_batch_id=p_pay_batch_id
          AND batch_candidate.candidate_id=operation_scope.candidate_id
          AND COALESCE(item.is_voided,false) IS NOT TRUE
      )
    );

  IF v_unlinked_allocation_count>0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_FROZEN_ALLOCATION_INCOMPLETE'
      USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
        'operation_id',p_operation_id,'unlinked_allocation_count',v_unlinked_allocation_count
      )::text;
  END IF;

  IF EXISTS(
    SELECT 1 FROM public.pay_batch_items AS item
    JOIN public.pay_batch_candidates AS candidate ON candidate.id=item.pay_batch_candidate_id
    WHERE candidate.pay_batch_id=p_pay_batch_id
      AND candidate.candidate_id=ANY(v_candidate_ids)
      AND item.pay_bank_transfer_id IS NOT NULL
  ) OR EXISTS(
    SELECT 1 FROM public.pay_bank_transfers AS transfer
    WHERE transfer.pay_batch_id=p_pay_batch_id AND transfer.candidate_id=ANY(v_candidate_ids)
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_POST_DRAFT_AUTHORITY_PRESENT' USING ERRCODE='23514';
  END IF;

  PERFORM scope.id
  FROM public.banking_pay_workbench_session_scope AS scope
  WHERE scope.session_id=p_session_id AND scope.candidate_id=ANY(v_candidate_ids)
  ORDER BY scope.candidate_id
  FOR UPDATE;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (
           WHERE scope.pending_job_id IS NOT NULL OR scope.dirty
             OR pg_catalog.upper(pg_catalog.btrim(COALESCE(scope.status,''))) NOT IN ('MATERIALISED','READY')
         )::integer,
         pg_catalog.count(*) FILTER (
           WHERE scope.certified_preview_publication_attestation_json->>'attestation_version'
                   IS DISTINCT FROM 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
              OR scope.certified_preview_publication_attestation_json->>'contract_version' IS DISTINCT FROM '3'
              OR scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
                   IS DISTINCT FROM 'READY_TO_PAY_SEMANTIC_V2'
              OR COALESCE((scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,false) IS NOT TRUE
              OR COALESCE((scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,false) IS NOT TRUE
         )::integer
  INTO v_candidate_count,v_active_owner_count,v_invalid_attestation_count
  FROM public.banking_pay_workbench_session_scope AS scope
  WHERE scope.session_id=p_session_id AND scope.candidate_id=ANY(v_candidate_ids);

  IF v_candidate_count<>pg_catalog.cardinality(v_candidate_ids)
     OR v_active_owner_count>0 OR v_invalid_attestation_count>0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_CURRENT_AUTHORITY_NOT_PROVEN'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'scope_count',v_candidate_count,'expected_count',pg_catalog.cardinality(v_candidate_ids),
        'active_owner_count',v_active_owner_count,
        'invalid_attestation_count',v_invalid_attestation_count
      )::text;
  END IF;

  v_semantic_proof:=private.pay_workbench_semantic_ready_proof_page_v1(
    p_session_id,v_candidate_ids,'{}'::jsonb,NULL::jsonb,'OBSERVE_ONLY','{}'::jsonb
  );
  IF COALESCE((v_semantic_proof->>'semantic_ready')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_ADOPTION_SEMANTIC_PROOF_FAILED' USING ERRCODE='23514';
  END IF;

  UPDATE public.banking_pay_operations AS operation_row
  SET progress_json=COALESCE(operation_row.progress_json,'{}'::jsonb)
    || pg_catalog.jsonb_build_object(
      'draft_create_adoption_v1',pg_catalog.jsonb_build_object(
        'adopted',true,'candidate_count',pg_catalog.cardinality(v_candidate_ids),
        'semantic_ready',true,'expected_effect_phase_count',5,
        'unlinked_allocation_count',0,'completed_at_utc',pg_catalog.clock_timestamp()
      )
    ),
    updated_at_utc=pg_catalog.clock_timestamp()
  WHERE operation_row.id=p_operation_id;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'enabled',true,'adopted',true,
    'candidate_count',pg_catalog.cardinality(v_candidate_ids),
    'semantic_proof',v_semantic_proof,
    'active_owner_count',0,'expected_effect_phase_count',5,
    'unlinked_allocation_count',0,'policy_x_drift',false
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_create_adoption_finalize_v1(uuid,uuid,uuid,uuid[],jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_create_adoption_finalize_v1(uuid,uuid,uuid,uuid[],jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_draft_create_adoption_finalize_v1(uuid,uuid,uuid,uuid[],jsonb)
  TO postgres;
