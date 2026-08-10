-- Banking Pay correction-owned dirty causality V1.
--
-- This transaction-local authority distinguishes dirty evidence caused by the
-- correction lifecycle from an unrelated economic mutation.  The context is
-- never a financial bypass: triggers still invalidate normally and the exact
-- causal envelope is copied into the durable dirty job for later validation.

CREATE OR REPLACE FUNCTION private.pay_workbench_correction_dirty_context_set_v1(
  p_correction_request_id uuid,
  p_pay_batch_id uuid,
  p_candidate_ids uuid[],
  p_lifecycle_phase text,
  p_policy_x_boundary text,
  p_pre_request_authorities_json jsonb DEFAULT '{}'::jsonb,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_work_item_id uuid DEFAULT NULL::uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_phase text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_lifecycle_phase,'')));
  v_boundary text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_policy_x_boundary,'')));
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_context_count integer := 0;
BEGIN
  IF p_correction_request_id IS NULL
     OR p_pay_batch_id IS NULL
     OR v_phase NOT IN (
       'REQUEST_PREPARE','REQUEST_START','FINANCIAL_PAGE_START',
       'FINANCIAL_PAGE_APPLIED','FINANCIAL_TERMINAL'
     )
     OR v_boundary NOT IN ('POST_DRAFT_FROZEN_EVIDENCE','PRE_DRAFT_LIVE_TRUTH')
     OR pg_catalog.jsonb_typeof(COALESCE(p_pre_request_authorities_json,'{}'::jsonb)) <> 'object'
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CORRECTION_DIRTY_CONTEXT_INVALID'
      USING ERRCODE='22023', DETAIL=pg_catalog.jsonb_build_object(
        'code','PAY_WORKBENCH_CORRECTION_DIRTY_CONTEXT_INVALID',
        'lifecycle_phase',v_phase,
        'policy_x_boundary',v_boundary
      )::text;
  END IF;

  SELECT COALESCE(pg_catalog.array_agg(DISTINCT candidate_id ORDER BY candidate_id),ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM pg_catalog.unnest(COALESCE(p_candidate_ids,ARRAY[]::uuid[])) AS candidate_scope(candidate_id)
  WHERE candidate_scope.candidate_id IS NOT NULL;

  IF pg_catalog.cardinality(v_candidate_ids)=0
     OR pg_catalog.cardinality(v_candidate_ids)>1000 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CORRECTION_DIRTY_CONTEXT_SCOPE_INVALID'
      USING ERRCODE='22023', DETAIL=pg_catalog.jsonb_build_object(
        'code','PAY_WORKBENCH_CORRECTION_DIRTY_CONTEXT_SCOPE_INVALID',
        'candidate_count',pg_catalog.cardinality(v_candidate_ids)
      )::text;
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_correction_dirty_context_v1(
    contract_version text NOT NULL,
    correction_request_id uuid NOT NULL,
    correction_operation_id uuid NULL,
    correction_work_item_id uuid NULL,
    pay_batch_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    lifecycle_phase text NOT NULL,
    policy_x_boundary text NOT NULL,
    pre_request_source_change_seq bigint NOT NULL,
    pre_request_dirty_generation bigint NOT NULL,
    pre_request_fence_digest text NULL,
    context_digest text NOT NULL,
    created_at_utc timestamptz NOT NULL,
    PRIMARY KEY(candidate_id)
  ) ON COMMIT DROP;

  INSERT INTO pg_temp._bpay_wb_correction_dirty_context_v1 AS context_row(
    contract_version,correction_request_id,correction_operation_id,
    correction_work_item_id,pay_batch_id,candidate_id,lifecycle_phase,
    policy_x_boundary,pre_request_source_change_seq,
    pre_request_dirty_generation,pre_request_fence_digest,context_digest,
    created_at_utc
  )
  SELECT
    'CORRECTION_OWNED_DIRTY_CAUSAL_V1',
    p_correction_request_id,
    p_operation_id,
    p_work_item_id,
    p_pay_batch_id,
    candidate_scope.candidate_id,
    v_phase,
    v_boundary,
    COALESCE(
      CASE WHEN COALESCE(
        p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'source_change_seq',''
      ) ~ '^[0-9]{1,18}$' THEN (
        p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'source_change_seq'
      )::bigint END,
      change_counter.seq,
      0
    ),
    COALESCE(
      CASE WHEN COALESCE(
        p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'dirty_generation',''
      ) ~ '^[0-9]{1,18}$' THEN (
        p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'dirty_generation'
      )::bigint END,
      change_counter.scope_change_generation,
      0
    ),
    NULLIF(
      p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'fence_digest',''
    ),
    pg_catalog.encode(extensions.digest(
      pg_catalog.convert_to(
        'CORRECTION_OWNED_DIRTY_CAUSAL_V1'||'|'||
        p_correction_request_id::text||'|'||COALESCE(p_operation_id::text,'')||'|'||
        COALESCE(p_work_item_id::text,'')||'|'||p_pay_batch_id::text||'|'||
        candidate_scope.candidate_id::text||'|'||v_phase||'|'||v_boundary||'|'||
        COALESCE(
          CASE WHEN COALESCE(
            p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'source_change_seq',''
          ) ~ '^[0-9]{1,18}$' THEN (
            p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'source_change_seq'
          )::bigint END,
          change_counter.seq,
          0
        )::text||'|'||
        COALESCE(
          CASE WHEN COALESCE(
            p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'dirty_generation',''
          ) ~ '^[0-9]{1,18}$' THEN (
            p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'dirty_generation'
          )::bigint END,
          change_counter.scope_change_generation,
          0
        )::text||'|'||
        COALESCE(
          p_pre_request_authorities_json->candidate_scope.candidate_id::text->>'fence_digest',''
        ),
        'UTF8'
      ),
      'sha256'
    ),'hex'),
    pg_catalog.clock_timestamp()
  FROM pg_catalog.unnest(v_candidate_ids) AS candidate_scope(candidate_id)
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key='pay_candidate:'||candidate_scope.candidate_id::text
  ON CONFLICT(candidate_id) DO UPDATE
  SET contract_version=EXCLUDED.contract_version,
      correction_request_id=EXCLUDED.correction_request_id,
      correction_operation_id=EXCLUDED.correction_operation_id,
      correction_work_item_id=EXCLUDED.correction_work_item_id,
      pay_batch_id=EXCLUDED.pay_batch_id,
      lifecycle_phase=EXCLUDED.lifecycle_phase,
      policy_x_boundary=EXCLUDED.policy_x_boundary,
      pre_request_source_change_seq=EXCLUDED.pre_request_source_change_seq,
      pre_request_dirty_generation=EXCLUDED.pre_request_dirty_generation,
      pre_request_fence_digest=EXCLUDED.pre_request_fence_digest,
      context_digest=EXCLUDED.context_digest,
      created_at_utc=EXCLUDED.created_at_utc;

  GET DIAGNOSTICS v_context_count=ROW_COUNT;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,
    'contract_version','CORRECTION_OWNED_DIRTY_CAUSAL_V1',
    'correction_request_id',p_correction_request_id,
    'operation_id',p_operation_id,
    'pay_batch_id',p_pay_batch_id,
    'lifecycle_phase',v_phase,
    'policy_x_boundary',v_boundary,
    'candidate_count',v_context_count,
    'transaction_local',true
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_correction_dirty_context_set_v1(
  uuid,uuid,uuid[],text,text,jsonb,uuid,uuid,jsonb
) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_correction_dirty_context_set_v1(
  uuid,uuid,uuid[],text,text,jsonb,uuid,uuid,jsonb
) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_correction_dirty_context_set_v1(
  uuid,uuid,uuid[],text,text,jsonb,uuid,uuid,jsonb
) TO postgres;
