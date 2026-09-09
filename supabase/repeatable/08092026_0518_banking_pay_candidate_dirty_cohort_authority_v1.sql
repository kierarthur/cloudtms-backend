-- Banking Pay Candidate dirty-apply cohort authority.
--
-- Policy X boundary: this file coordinates pre-draft freshness authority only.
-- It does not merge operational job scopes/reasons, calculate economics, alter
-- eligibility, or write Draft, batch, provider, payment, settlement or
-- remittance state.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_candidate_dirty_cohort_stage_v1(
  p_job_id uuid,
  p_candidate_id uuid,
  p_now_utc timestamptz DEFAULT NULL::timestamptz
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, pg_catalog.clock_timestamp());
  v_deferral_enabled boolean := false;
  v_member record;
  v_context jsonb := '{}'::jsonb;
  v_context_digest text := NULL::text;
  v_member_deferred boolean := false;
  v_member_ids uuid[] := ARRAY[]::uuid[];
  v_admitted_member_ids uuid[] := ARRAY[]::uuid[];
  v_excluded_member_ids uuid[] := ARRAY[]::uuid[];
  v_member_count integer := 0;
  v_excluded_count integer := 0;
  v_contains_full_scope boolean := false;
  v_timesheet_root_count integer := 0;
  v_finance_case_root_count integer := 0;
  v_effective_timesheet_count integer := 0;
  v_timesheet_root_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_root_ids uuid[] := ARRAY[]::uuid[];
  v_effective_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_dependency_closure jsonb := '{}'::jsonb;
  v_family_scope jsonb := '{}'::jsonb;
  v_authority_scope text := 'TARGETED_UNION';
  v_full_fallback_reason text := NULL::text;
  v_registry_generation bigint := NULL::bigint;
  v_registry_source_seq bigint := 0;
  v_registry_pending_token uuid := NULL::uuid;
  v_live_generation bigint := NULL::bigint;
  v_live_source_seq bigint := 0;
  v_counter_pending_token uuid := NULL::uuid;
  v_common_token uuid := NULL::uuid;
  v_common_generation bigint := NULL::bigint;
  v_transaction_state text := NULL::text;
  v_transaction_generation bigint := NULL::bigint;
  v_all_members_match boolean := false;
  v_all_states_match boolean := false;
  v_all_full_markers_match boolean := false;
  v_authority_reusable boolean := false;
  v_pending_replay boolean := false;
  v_stage_token uuid := NULL::uuid;
  v_common_source_seq bigint := 0;
  v_invalidation_result jsonb := '{}'::jsonb;
  v_invalidation_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_invalidation_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_snapshot_max_updated_at timestamptz := NULL::timestamptz;
  v_snapshot_max_latest_event text := NULL::text;
BEGIN
  IF p_job_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_CURRENT_MEMBER_INVALID'
      USING ERRCODE = '22023', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_CURRENT_MEMBER_INVALID',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id
      )::text;
  END IF;

  -- The public processor already owns this lock. Reacquiring it is harmless
  -- and makes direct/internal misuse fail closed instead of creating a second
  -- Candidate queue authority.
  IF NOT pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(p_candidate_id),
      24062027
    )
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_CANDIDATE_LOCK_REQUIRED'
      USING ERRCODE = '55000', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_CANDIDATE_LOCK_REQUIRED',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id
      )::text;
  END IF;

  -- Keep the two branches separate so each partial index remains usable. The
  -- snapshot is deliberately finite-in-time, but not cardinality-capped. UUID
  -- arrays avoid every caller-controlled temporary namespace: the public entry
  -- point is SECURITY DEFINER, so even fixed-name DROP is unsafe when a caller
  -- owns a same-name temporary relation.
  SELECT COALESCE(
           pg_catalog.array_agg(snapshot_job.id ORDER BY snapshot_job.id),
           ARRAY[]::uuid[]
         )
  INTO v_member_ids
  FROM (
    SELECT candidate_job.id
    FROM public.banking_pay_workbench_jobs AS candidate_job
    WHERE candidate_job.candidate_id = p_candidate_id
      AND candidate_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND candidate_job.status IN ('QUEUED', 'RUNNING')
    UNION
    SELECT legacy_job.id
    FROM public.banking_pay_workbench_jobs AS legacy_job
    WHERE legacy_job.candidate_id IS NULL
      AND legacy_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND legacy_job.status IN ('QUEUED', 'RUNNING')
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
            legacy_job.payload_json->>'candidate_id', ''
          ))) = p_candidate_id::text
  ) AS snapshot_job;

  -- A deterministic row-lock pass converts the index snapshot into the exact
  -- admitted cohort. A row inserted after this ID snapshot is a late arrival
  -- and intentionally belongs to the next cohort.
  PERFORM 1
  FROM pg_catalog.unnest(v_member_ids) AS snapshot_member(id)
  JOIN public.banking_pay_workbench_jobs AS locked_job
    ON locked_job.id = snapshot_member.id
  ORDER BY locked_job.id
  FOR UPDATE OF locked_job;

  -- Rows can change while the deterministic lock pass waits. Recheck the
  -- finite ID snapshot under lock and carry only still-active exact members.
  SELECT COALESCE(
           pg_catalog.array_agg(current_job.id ORDER BY current_job.id),
           ARRAY[]::uuid[]
         )
  INTO v_member_ids
  FROM pg_catalog.unnest(v_member_ids) AS snapshot_member(id)
  JOIN public.banking_pay_workbench_jobs AS current_job
    ON current_job.id = snapshot_member.id
  WHERE current_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND current_job.status IN ('QUEUED', 'RUNNING')
    AND (
      current_job.candidate_id = p_candidate_id
      OR (
        current_job.candidate_id IS NULL
        AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
              current_job.payload_json->>'candidate_id', ''
            ))) = p_candidate_id::text
      )
    );

  v_admitted_member_ids := v_member_ids;

  IF NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS current_job
    WHERE current_job.id = p_job_id
      AND current_job.id = ANY(v_member_ids)
      AND current_job.status = 'RUNNING'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_CURRENT_MEMBER_INVALID'
      USING ERRCODE = '22023', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_CURRENT_MEMBER_INVALID',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id
      )::text;
  END IF;

  SELECT COALESCE(
    (pg_catalog.to_jsonb(settings_row)
      ->>'banking_pay_correction_request_dirty_deferral_v1_enabled')::boolean,
    false
  )
  INTO v_deferral_enabled
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  -- Preserve the installed correction boundary exactly. The current row has
  -- already passed this gate in the public processor; only unfinished sibling
  -- authorities are excluded. Historical/terminal correction metadata alone
  -- never makes a row permanently ineligible for a later cohort.
  FOR v_member IN
    SELECT candidate_job.id,
           candidate_job.created_at_utc,
           candidate_job.updated_at_utc,
           candidate_job.scope_change_tx_token,
           COALESCE(candidate_job.payload_json, '{}'::jsonb) AS payload_json
    FROM pg_catalog.unnest(v_member_ids) AS snapshot_member(id)
    JOIN public.banking_pay_workbench_jobs AS candidate_job
      ON candidate_job.id = snapshot_member.id
    WHERE candidate_job.id <> p_job_id
    ORDER BY candidate_job.id
  LOOP
    v_member_deferred := false;
    v_context := CASE
      WHEN pg_catalog.jsonb_typeof(
             v_member.payload_json->'correction_dirty_contexts'
           ) = 'object'
       AND pg_catalog.jsonb_typeof(
             v_member.payload_json->'correction_dirty_contexts'
               ->p_candidate_id::text
           ) = 'object'
        THEN v_member.payload_json->'correction_dirty_contexts'
               ->p_candidate_id::text
      ELSE '{}'::jsonb
    END;

    IF COALESCE(v_deferral_enabled, false)
       AND COALESCE(v_context->>'contract_version', '') =
             'CORRECTION_OWNED_DIRTY_CAUSAL_V1'
       AND COALESCE(v_context->>'candidate_id', '') = p_candidate_id::text
       AND COALESCE(v_context->>'correction_request_id', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       AND COALESCE(v_context->>'pay_batch_id', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       AND COALESCE(v_context->>'lifecycle_phase', '') IN (
         'REQUEST_PREPARE', 'REQUEST_START', 'FINANCIAL_PAGE_START',
         'FINANCIAL_PAGE_APPLIED', 'FINANCIAL_TERMINAL'
       )
       AND COALESCE(v_context->>'policy_x_boundary', '') IN (
         'POST_DRAFT_FROZEN_EVIDENCE', 'PRE_DRAFT_LIVE_TRUTH'
       )
       AND COALESCE(v_context->>'pre_request_source_change_seq', '')
             ~ '^[0-9]{1,18}$'
       AND COALESCE(v_context->>'pre_request_dirty_generation', '')
             ~ '^[0-9]{1,18}$'
       AND COALESCE(v_member.payload_json
             ->>'request_owned_scope_change_tx_token', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       AND COALESCE(v_member.payload_json->>'scope_change_tx_token', '') =
             COALESCE(v_member.payload_json
               ->>'request_owned_scope_change_tx_token', '')
       AND (
         v_member.scope_change_tx_token IS NULL
         OR v_member.scope_change_tx_token::text = COALESCE(
           v_member.payload_json->>'request_owned_scope_change_tx_token', ''
         )
       )
       AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
             v_member.payload_json->>'policy_x_dirtying_only', 'false'
           ))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
             v_member.payload_json->>'economic_truth_mutation_allowed', 'false'
           ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_context_digest := pg_catalog.encode(extensions.digest(
        pg_catalog.convert_to(
          'CORRECTION_OWNED_DIRTY_CAUSAL_V1' || '|' ||
          COALESCE(v_context->>'correction_request_id', '') || '|' ||
          COALESCE(v_context->>'correction_operation_id', '') || '|' ||
          COALESCE(v_context->>'correction_work_item_id', '') || '|' ||
          COALESCE(v_context->>'pay_batch_id', '') || '|' ||
          p_candidate_id::text || '|' ||
          COALESCE(v_context->>'lifecycle_phase', '') || '|' ||
          COALESCE(v_context->>'policy_x_boundary', '') || '|' ||
          COALESCE(v_context->>'pre_request_source_change_seq', '') || '|' ||
          COALESCE(v_context->>'pre_request_dirty_generation', '') || '|' ||
          COALESCE(v_context->>'pre_request_fence_digest', ''),
          'UTF8'
        ),
        'sha256'
      ), 'hex');

      IF v_context_digest = COALESCE(v_context->>'context_digest', '') THEN
        SELECT EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_requests AS request_row
          JOIN public.banking_pay_operations AS correction_operation
            ON correction_operation.operation_type = 'PAYMENT_CORRECTION'
           AND correction_operation.input_json->>'correction_request_id' =
                 request_row.id::text
          JOIN public.pay_batch_candidates AS batch_candidate
            ON batch_candidate.pay_batch_id = request_row.pay_batch_id
           AND batch_candidate.candidate_id = p_candidate_id
          WHERE request_row.id =
                  (v_context->>'correction_request_id')::uuid
            AND request_row.pay_batch_id = (v_context->>'pay_batch_id')::uuid
            AND correction_operation.status IN (
              'QUEUED', 'RUNNING', 'WAITING_AUTHORISATION'
            )
            AND correction_operation.phase <> 'COMPLETE'
            AND request_row.status NOT IN ('CANCELLED', 'FAILED', 'REJECTED')
            AND request_row.created_at_utc <= GREATEST(
              v_member.created_at_utc,
              COALESCE(v_member.updated_at_utc, v_member.created_at_utc)
            )
            AND (
              COALESCE(v_context->>'correction_operation_id', '') = ''
              OR correction_operation.id::text =
                    v_context->>'correction_operation_id'
            )
        ) INTO v_member_deferred;
      END IF;
    ELSIF NOT COALESCE(v_deferral_enabled, false)
       AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
             v_member.payload_json->>'trigger_table', ''
           ))) = 'pay_payment_correction_requests'
       AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
             v_member.payload_json->>'policy_x_dirtying_only', 'false'
           ))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
             v_member.payload_json->>'economic_truth_mutation_allowed', 'false'
           ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       AND (
         (
           pg_catalog.jsonb_typeof(v_member.payload_json->'reasons') = 'array'
           AND pg_catalog.jsonb_array_length(
                 v_member.payload_json->'reasons'
               ) > 0
           AND NOT EXISTS (
             SELECT 1
             FROM pg_catalog.jsonb_array_elements_text(
                    v_member.payload_json->'reasons'
                  ) AS dirty_reason(reason_text)
             WHERE pg_catalog.upper(pg_catalog.btrim(dirty_reason.reason_text))
                   NOT IN (
                     'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
                     'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
                   )
           )
         )
         OR (
           pg_catalog.jsonb_typeof(v_member.payload_json->'reasons')
                 IS DISTINCT FROM 'array'
           AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
                 v_member.payload_json->>'reason_latest',
                 v_member.payload_json->>'reason',
                 ''
               ))) IN (
                 'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
                 'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
               )
         )
       ) THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.banking_pay_operations AS correction_operation
        JOIN public.pay_payment_correction_requests AS request_row
          ON request_row.id = CASE
            WHEN COALESCE(
                   correction_operation.input_json->>'correction_request_id',
                   ''
                 ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN (correction_operation.input_json
                      ->>'correction_request_id')::uuid
            ELSE NULL::uuid
          END
        JOIN public.pay_payment_correction_request_candidates AS request_candidate
          ON request_candidate.correction_request_id = request_row.id
        JOIN public.pay_batch_candidates AS batch_candidate
          ON batch_candidate.id = request_candidate.pay_batch_candidate_id
         AND batch_candidate.candidate_id = p_candidate_id
        WHERE correction_operation.operation_type = 'PAYMENT_CORRECTION'
          AND correction_operation.status IN ('QUEUED', 'RUNNING')
          AND correction_operation.phase NOT IN ('REFRESH_WORKBENCH', 'COMPLETE')
          AND correction_operation.input_json
                ->'draft_overlay_fast_start_authorities'
                ->p_candidate_id::text->>'request_owned_dirty_job_id' =
                v_member.id::text
          AND request_row.status IN (
            'REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED',
            'EXPANDED', 'PROCESSING'
          )
          AND request_row.created_at_utc <= GREATEST(
            v_member.created_at_utc,
            COALESCE(v_member.updated_at_utc, v_member.created_at_utc)
          )
      ) INTO v_member_deferred;
    END IF;

    IF v_member_deferred THEN
      -- REQUEST_OWNED_CORRECTION_UNFINISHED remains outside this finite cohort.
      v_admitted_member_ids := pg_catalog.array_remove(
        v_admitted_member_ids,
        v_member.id
      );
      v_excluded_member_ids := pg_catalog.array_append(
        v_excluded_member_ids,
        v_member.id
      );
    END IF;
  END LOOP;

  -- The current job passed the same boundary immediately before this helper.
  IF NOT p_job_id = ANY(v_admitted_member_ids) THEN
    v_admitted_member_ids := pg_catalog.array_append(
      v_admitted_member_ids,
      p_job_id
    );
    v_excluded_member_ids := pg_catalog.array_remove(
      v_excluded_member_ids,
      p_job_id
    );
  END IF;

  v_member_count := pg_catalog.cardinality(v_admitted_member_ids);
  v_excluded_count := pg_catalog.cardinality(v_excluded_member_ids);

  SELECT max(candidate_job.updated_at_utc),
         max(candidate_job.payload_json->>'latest_event_at_utc')
  INTO v_snapshot_max_updated_at,
       v_snapshot_max_latest_event
  FROM pg_catalog.unnest(v_admitted_member_ids) AS cohort_member(id)
  JOIN public.banking_pay_workbench_jobs AS candidate_job
    ON candidate_job.id = cohort_member.id;

  IF COALESCE(v_member_count, 0) = 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_CURRENT_MEMBER_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(
           pg_catalog.array_agg(
             DISTINCT raw_scope.value_text::uuid
             ORDER BY raw_scope.value_text::uuid
           ),
           ARRAY[]::uuid[]
         )
  INTO v_timesheet_root_ids
  FROM (
    SELECT raw_target.value_text
    FROM pg_catalog.unnest(v_admitted_member_ids) AS cohort_member(id)
    JOIN public.banking_pay_workbench_jobs AS candidate_job
      ON candidate_job.id = cohort_member.id
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
      CASE WHEN pg_catalog.jsonb_typeof(
                  candidate_job.payload_json->'targeted_timesheet_ids'
                ) = 'array'
        THEN candidate_job.payload_json->'targeted_timesheet_ids'
        ELSE '[]'::jsonb END
    ) AS raw_target(value_text)
    UNION ALL
    SELECT raw_linked.value_text
    FROM pg_catalog.unnest(v_admitted_member_ids) AS cohort_member(id)
    JOIN public.banking_pay_workbench_jobs AS candidate_job
      ON candidate_job.id = cohort_member.id
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
      CASE WHEN pg_catalog.jsonb_typeof(
                  candidate_job.payload_json->'linked_timesheet_ids'
                ) = 'array'
        THEN candidate_job.payload_json->'linked_timesheet_ids'
        ELSE '[]'::jsonb END
    ) AS raw_linked(value_text)
  ) AS raw_scope(value_text)
  WHERE raw_scope.value_text
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT COALESCE(
           pg_catalog.array_agg(
             DISTINCT raw_case.value_text::uuid
             ORDER BY raw_case.value_text::uuid
           ),
           ARRAY[]::uuid[]
         )
  INTO v_finance_case_root_ids
  FROM pg_catalog.unnest(v_admitted_member_ids) AS cohort_member(id)
  JOIN public.banking_pay_workbench_jobs AS candidate_job
    ON candidate_job.id = cohort_member.id
  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
    CASE WHEN pg_catalog.jsonb_typeof(
                candidate_job.payload_json->'finance_case_ids'
              ) = 'array'
      THEN candidate_job.payload_json->'finance_case_ids'
      ELSE '[]'::jsonb END
  ) AS raw_case(value_text)
  WHERE raw_case.value_text
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(v_admitted_member_ids) AS cohort_member(id)
    JOIN public.banking_pay_workbench_jobs AS candidate_job
      ON candidate_job.id = cohort_member.id
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
          SELECT raw_target.value_text
          FROM pg_catalog.jsonb_array_elements_text(
            CASE WHEN pg_catalog.jsonb_typeof(
                        candidate_job.payload_json->'targeted_timesheet_ids'
                      ) = 'array'
              THEN candidate_job.payload_json->'targeted_timesheet_ids'
              ELSE '[]'::jsonb END
          ) AS raw_target(value_text)
          UNION ALL
          SELECT raw_linked.value_text
          FROM pg_catalog.jsonb_array_elements_text(
            CASE WHEN pg_catalog.jsonb_typeof(
                        candidate_job.payload_json->'linked_timesheet_ids'
                      ) = 'array'
              THEN candidate_job.payload_json->'linked_timesheet_ids'
              ELSE '[]'::jsonb END
          ) AS raw_linked(value_text)
          UNION ALL
          SELECT raw_case.value_text
          FROM pg_catalog.jsonb_array_elements_text(
            CASE WHEN pg_catalog.jsonb_typeof(
                        candidate_job.payload_json->'finance_case_ids'
                      ) = 'array'
              THEN candidate_job.payload_json->'finance_case_ids'
              ELSE '[]'::jsonb END
          ) AS raw_case(value_text)
        ) AS raw_scope
        WHERE raw_scope.value_text
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
  ) INTO v_contains_full_scope;

  v_timesheet_root_count := pg_catalog.cardinality(v_timesheet_root_ids);
  v_finance_case_root_count := pg_catalog.cardinality(v_finance_case_root_ids);

  IF v_contains_full_scope THEN
    v_authority_scope := 'CANDIDATE_FULL_LIVE';
    v_full_fallback_reason := 'COHORT_MEMBER_FULL_SCOPE';
  END IF;

  IF v_timesheet_root_count > 250 THEN
    v_authority_scope := 'CANDIDATE_FULL_LIVE';
    v_full_fallback_reason := 'COHORT_TIMESHEET_ROOT_CAP_EXCEEDED';
  ELSIF v_finance_case_root_count > 100 THEN
    v_authority_scope := 'CANDIDATE_FULL_LIVE';
    v_full_fallback_reason := 'COHORT_FINANCE_CASE_ROOT_CAP_EXCEEDED';
  ELSIF v_timesheet_root_count > 0 OR v_finance_case_root_count > 0 THEN
    IF pg_catalog.to_regprocedure(
         'public._pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)'
       ) IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_REFRESH_DEPENDENCY_CLOSURE_UNAVAILABLE'
        USING ERRCODE = 'P0001';
    END IF;

    v_dependency_closure := public._pay_workbench_refresh_dependency_closure_v1(
      p_candidate_id,
      v_timesheet_root_ids,
      ARRAY[]::uuid[],
      v_finance_case_root_ids,
      250,
      100
    );

    IF pg_catalog.lower(pg_catalog.btrim(COALESCE(
         v_dependency_closure->>'requires_full_candidate', 'true'
       ))) IN ('true', 't', '1', 'yes', 'y', 'on')
       OR pg_catalog.lower(pg_catalog.btrim(COALESCE(
         v_dependency_closure->>'coverage_complete', 'false'
       ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_authority_scope := 'CANDIDATE_FULL_LIVE';
      v_full_fallback_reason := COALESCE(
        NULLIF(pg_catalog.btrim(COALESCE(
          v_dependency_closure->>'fallback_reason', ''
        )), ''),
        'COHORT_DEPENDENCY_CLOSURE_INCOMPLETE'
      );
    ELSE
      SELECT COALESCE(pg_catalog.array_agg(
               DISTINCT raw_id.value_text::uuid
               ORDER BY raw_id.value_text::uuid
             ), ARRAY[]::uuid[])
      INTO v_effective_timesheet_ids
      FROM pg_catalog.jsonb_array_elements_text(COALESCE(
        v_dependency_closure->'effective_targeted_timesheet_ids',
        '[]'::jsonb
      )) AS raw_id(value_text)
      WHERE raw_id.value_text
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

      v_family_scope := public._pay_workbench_normalise_timesheet_rotation_scope_payload(
        v_effective_timesheet_ids,
        ARRAY[]::uuid[]
      );

      SELECT COALESCE(pg_catalog.array_agg(
               DISTINCT family_id.value_text::uuid
               ORDER BY family_id.value_text::uuid
             ), ARRAY[]::uuid[])
      INTO v_effective_timesheet_ids
      FROM (
        SELECT value_text
        FROM pg_catalog.jsonb_array_elements_text(COALESCE(
          v_family_scope->'targeted_timesheet_ids', '[]'::jsonb
        )) AS targeted_family(value_text)
        UNION ALL
        SELECT value_text
        FROM pg_catalog.jsonb_array_elements_text(COALESCE(
          v_family_scope->'linked_timesheet_ids', '[]'::jsonb
        )) AS linked_family(value_text)
      ) AS family_id(value_text)
      WHERE family_id.value_text
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

      IF pg_catalog.cardinality(v_effective_timesheet_ids) > 250 THEN
        v_authority_scope := 'CANDIDATE_FULL_LIVE';
        v_full_fallback_reason := 'COHORT_NORMALISED_TIMESHEET_CAP_EXCEEDED';
        v_effective_timesheet_ids := ARRAY[]::uuid[];
      ELSIF pg_catalog.cardinality(v_effective_timesheet_ids) = 0 THEN
        -- A finance-case root can legitimately resolve to no currently owned
        -- Timesheet. The existing processor treats that shape as Candidate
        -- full; preserve that fail-closed behavior instead of invoking the
        -- invalidator with an empty targeted pair set.
        v_authority_scope := 'CANDIDATE_FULL_LIVE';
        v_full_fallback_reason := 'COHORT_EMPTY_EFFECTIVE_TIMESHEET_SCOPE';
      END IF;
    END IF;
  END IF;

  IF v_authority_scope = 'CANDIDATE_FULL_LIVE'
     AND v_full_fallback_reason <> 'COHORT_MEMBER_FULL_SCOPE' THEN
    -- An incomplete/unbounded closure is represented by one stronger
    -- Candidate-full authority, never by a partial bounded subset.
    v_effective_timesheet_ids := ARRAY[]::uuid[];
  END IF;

  v_effective_timesheet_count := pg_catalog.cardinality(
    COALESCE(v_effective_timesheet_ids, ARRAY[]::uuid[])
  );

  -- Candidate registry/counter first, then bounded Timesheet state. This is
  -- the same metadata order used by the deferred finaliser after the jobs are
  -- already locked above.
  SELECT registry.dirty_generation,
         registry.current_source_change_seq,
         registry.last_scope_change_tx_token
  INTO v_registry_generation,
       v_registry_source_seq,
       v_registry_pending_token
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id = p_candidate_id
  FOR UPDATE;

  SELECT change_counter.scope_change_generation,
         change_counter.seq,
         change_counter.scope_change_tx_token
  INTO v_live_generation,
       v_live_source_seq,
       v_counter_pending_token
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text
  FOR UPDATE;

  IF v_effective_timesheet_count > 0 THEN
    PERFORM 1
    FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
    WHERE scope_state.timesheet_id = ANY(v_effective_timesheet_ids)
    ORDER BY scope_state.candidate_id, scope_state.timesheet_id
    FOR UPDATE;
  END IF;

  SELECT CASE
    WHEN COALESCE(current_job.payload_json->>'scope_change_tx_token', '')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (current_job.payload_json->>'scope_change_tx_token')::uuid
    ELSE NULL::uuid
  END
  INTO v_common_token
  FROM public.banking_pay_workbench_jobs AS current_job
  WHERE current_job.id = p_job_id;

  SELECT scope_tx.state, scope_tx.allocated_generation
  INTO v_transaction_state, v_transaction_generation
  FROM public.banking_pay_scope_change_transactions AS scope_tx
  WHERE scope_tx.tx_token = v_common_token;

  v_common_generation := CASE
    WHEN v_registry_generation IS NOT NULL
     AND v_registry_generation = v_live_generation
      THEN v_registry_generation
    ELSE NULL::bigint
  END;

  SELECT NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS candidate_job
    WHERE candidate_job.id = ANY(v_admitted_member_ids)
      AND (
        v_common_token IS NULL
        OR candidate_job.scope_change_generation IS DISTINCT FROM
             v_common_generation
        OR COALESCE(candidate_job.payload_json->>'scope_change_generation', '')
             !~ '^[0-9]{1,18}$'
        OR (candidate_job.payload_json
              ->>'scope_change_generation')::bigint IS DISTINCT FROM
             v_common_generation
        OR COALESCE(candidate_job.payload_json->>'scope_change_tx_token', '')
             <> v_common_token::text
      )
  ) INTO v_all_members_match;

  SELECT NOT EXISTS (
    SELECT 1
    FROM unnest(COALESCE(
           v_effective_timesheet_ids, ARRAY[]::uuid[]
         )) AS required_scope(timesheet_id)
    LEFT JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
      ON scope_state.timesheet_id = required_scope.timesheet_id
     AND scope_state.candidate_id = p_candidate_id
     AND scope_state.dirty_generation = v_common_generation
    WHERE scope_state.timesheet_id IS NULL
  ) INTO v_all_states_match;

  SELECT NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS candidate_job
    WHERE candidate_job.id = ANY(v_admitted_member_ids)
      AND (
        COALESCE(candidate_job.payload_json
          ->>'dirty_apply_cohort_contract_version', '') <>
            'DIRTY_APPLY_COHORT_AUTHORITY_V1'
        OR COALESCE(candidate_job.payload_json
          ->>'dirty_apply_cohort_candidate_id', '') <> p_candidate_id::text
        OR COALESCE(candidate_job.payload_json
          ->>'dirty_apply_cohort_tx_token', '') <> v_common_token::text
        OR COALESCE(candidate_job.payload_json
          ->>'dirty_apply_cohort_authority_scope', '') <>
            'CANDIDATE_FULL_LIVE'
      )
  ) INTO v_all_full_markers_match;

  v_authority_reusable :=
    v_common_token IS NOT NULL
    AND COALESCE(v_common_generation, 0) > 0
    AND v_transaction_state = 'FINALIZED'
    AND v_transaction_generation = v_common_generation
    AND v_all_members_match
    AND (
      (
        v_authority_scope = 'TARGETED_UNION'
        AND v_all_states_match
      )
      OR (
        v_authority_scope = 'CANDIDATE_FULL_LIVE'
        AND v_all_full_markers_match
      )
    );

  IF v_authority_reusable THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'action', 'REUSE_FINALIZED_AUTHORITY',
      'authority_reusable', true,
      'candidate_id', p_candidate_id,
      'scope_change_tx_token', v_common_token,
      'scope_change_generation', v_common_generation,
      'authority_scope', v_authority_scope,
      'member_count', v_member_count,
      'excluded_request_owned_count', v_excluded_count,
      'effective_timesheet_count', v_effective_timesheet_count,
      'source_change_seq', GREATEST(
        COALESCE(v_live_source_seq, 0),
        COALESCE(v_registry_source_seq, 0)
      )
    );
  END IF;

  -- A repeated call inside the same top-level worker transaction must wait for
  -- the deferred finaliser; it must not bump the source sequence again.
  SELECT
    v_common_token IS NOT NULL
    AND v_transaction_state = 'PENDING'
    AND v_registry_pending_token = v_common_token
    AND v_counter_pending_token = v_common_token
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_jobs AS candidate_job
      WHERE candidate_job.id = ANY(v_admitted_member_ids)
        AND (
          candidate_job.scope_change_tx_token IS DISTINCT FROM v_common_token
          OR candidate_job.scope_change_generation IS NOT NULL
          OR COALESCE(candidate_job.payload_json
               ->>'scope_change_tx_token', '') <> v_common_token::text
          OR COALESCE(candidate_job.payload_json
               ->>'dirty_apply_cohort_contract_version', '') <>
                 'DIRTY_APPLY_COHORT_AUTHORITY_V1'
          OR COALESCE(candidate_job.payload_json
               ->>'dirty_apply_cohort_candidate_id', '') <>
                 p_candidate_id::text
          OR COALESCE(candidate_job.payload_json
               ->>'dirty_apply_cohort_tx_token', '') <> v_common_token::text
        )
    )
  INTO v_pending_replay;

  IF v_pending_replay THEN
    UPDATE public.banking_pay_workbench_jobs AS current_job
    SET status = 'QUEUED',
        attempt_count = GREATEST(
          COALESCE(current_job.attempt_count, 0) - 1,
          0
        ),
        run_at_utc = v_now,
        started_at_utc = NULL,
        completed_at_utc = NULL,
        failed_at_utc = NULL,
        last_error_json = NULL,
        updated_at_utc = v_now
    WHERE current_job.id = p_job_id;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'action', 'WAITING_FOR_COHORT_FINALIZATION',
      'authority_reusable', false,
      'pending_finalization', true,
      'candidate_id', p_candidate_id,
      'scope_change_tx_token', v_common_token,
      'scope_change_generation', NULL,
      'authority_scope', v_authority_scope,
      'member_count', v_member_count,
      'excluded_request_owned_count', v_excluded_count,
      'effective_timesheet_count', v_effective_timesheet_count
    );
  END IF;

  v_stage_token := public.pay_workbench_scope_change_tx_token_v1();

  IF v_stage_token IS NULL OR NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_scope_change_transactions AS scope_tx
    WHERE scope_tx.tx_token = v_stage_token
      AND scope_tx.state = 'PENDING'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_TOKEN_NOT_PENDING'
      USING ERRCODE = '40001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_TOKEN_NOT_PENDING',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id,
        'scope_change_tx_token', v_stage_token
      )::text;
  END IF;

  -- One Candidate source bump per admitted cohort. If this same transaction
  -- already staged the Candidate counter under the token, reuse it.
  IF NOT EXISTS (
    SELECT 1
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key =
            'pay_candidate:' || p_candidate_id::text
      AND change_counter.scope_change_tx_token = v_stage_token
  ) THEN
    PERFORM public._change_bump('pay_candidate:' || p_candidate_id::text);
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_source_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  SELECT GREATEST(
    COALESCE(v_live_source_seq, 0),
    COALESCE(v_registry_source_seq, 0),
    COALESCE(max(GREATEST(
      COALESCE(CASE
        WHEN COALESCE(candidate_job.payload_json
               ->>'latest_source_change_seq', '') ~ '^[0-9]+$'
          THEN (candidate_job.payload_json
                  ->>'latest_source_change_seq')::bigint
        ELSE 0 END, 0),
      COALESCE(CASE
        WHEN COALESCE(candidate_job.payload_json
               ->>'source_change_seq', '') ~ '^[0-9]+$'
          THEN (candidate_job.payload_json->>'source_change_seq')::bigint
        ELSE 0 END, 0),
      COALESCE(CASE
        WHEN COALESCE(candidate_job.payload_json
               ->>'source_change_sequence', '') ~ '^[0-9]+$'
          THEN (candidate_job.payload_json
                  ->>'source_change_sequence')::bigint
        ELSE 0 END, 0)
    )), 0)
  )
  INTO v_common_source_seq
  FROM public.banking_pay_workbench_jobs AS candidate_job
  WHERE candidate_job.id = ANY(v_admitted_member_ids);

  IF v_authority_scope = 'CANDIDATE_FULL_LIVE' THEN
    v_invalidation_candidate_ids := ARRAY[p_candidate_id];
    v_invalidation_timesheet_ids := ARRAY[NULL::uuid];

    -- When a bounded union is available beside an explicit ALL member, stamp
    -- those Timesheet rows too so targeted jobs retain their ordinary exact
    -- per-row proof. ALL remains the stronger Candidate authority.
    IF v_effective_timesheet_count > 0 THEN
      v_invalidation_candidate_ids := v_invalidation_candidate_ids ||
        pg_catalog.array_fill(
          p_candidate_id,
          ARRAY[v_effective_timesheet_count]
        );
      v_invalidation_timesheet_ids := v_invalidation_timesheet_ids ||
        v_effective_timesheet_ids;
    END IF;
  ELSE
    v_invalidation_candidate_ids := pg_catalog.array_fill(
      p_candidate_id,
      ARRAY[v_effective_timesheet_count]
    );
    v_invalidation_timesheet_ids := v_effective_timesheet_ids;
  END IF;

  v_invalidation_result := private.pay_workbench_scope_invalidate_v1(
    v_invalidation_candidate_ids,
    v_invalidation_timesheet_ids,
    'DIRTY_APPLY_CANDIDATE_COHORT_REISSUE',
    v_stage_token,
    pg_catalog.jsonb_build_object(
      'skip_candidate_job_enqueue', true,
      'latest_source_change_seq', v_common_source_seq,
      'source_change_seq', v_common_source_seq,
      'source_change_sequence', v_common_source_seq,
      'dirty_apply_cohort_contract_version',
        'DIRTY_APPLY_COHORT_AUTHORITY_V1',
      'dirty_apply_cohort_candidate_id', p_candidate_id,
      'dirty_apply_cohort_tx_token', v_stage_token,
      'dirty_apply_cohort_authority_scope', v_authority_scope,
      'dirty_apply_cohort_member_count', v_member_count,
      'dirty_apply_cohort_excluded_request_owned_count', v_excluded_count,
      'dirty_apply_cohort_effective_timesheet_count',
        v_effective_timesheet_count,
      'dirty_apply_cohort_full_fallback_reason', v_full_fallback_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'economic_calculation_performed', false
    )
  );

  IF pg_catalog.lower(pg_catalog.btrim(COALESCE(
       v_invalidation_result->>'ok', 'false'
     ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
     OR COALESCE(v_invalidation_result->>'scope_change_tx_token', '') <>
          v_stage_token::text THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_INVALIDATION_NOT_STAGED'
      USING ERRCODE = '40001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_INVALIDATION_NOT_STAGED',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id,
        'scope_change_tx_token', v_stage_token,
        'invalidation_result', v_invalidation_result
      )::text;
  END IF;

  UPDATE public.banking_pay_workbench_jobs AS candidate_job
  SET status = CASE
        WHEN candidate_job.id = p_job_id THEN 'QUEUED'
        ELSE candidate_job.status
      END,
      attempt_count = CASE
        WHEN candidate_job.id = p_job_id THEN GREATEST(
          COALESCE(candidate_job.attempt_count, 0) - 1,
          0
        )
        ELSE candidate_job.attempt_count
      END,
      run_at_utc = CASE
        WHEN candidate_job.id = p_job_id THEN v_now
        ELSE candidate_job.run_at_utc
      END,
      started_at_utc = CASE
        WHEN candidate_job.id = p_job_id THEN NULL
        ELSE candidate_job.started_at_utc
      END,
      completed_at_utc = CASE
        WHEN candidate_job.id = p_job_id THEN NULL
        ELSE candidate_job.completed_at_utc
      END,
      failed_at_utc = CASE
        WHEN candidate_job.id = p_job_id THEN NULL
        ELSE candidate_job.failed_at_utc
      END,
      last_error_json = CASE
        WHEN candidate_job.id = p_job_id THEN NULL
        ELSE candidate_job.last_error_json
      END,
      payload_json = (
        COALESCE(candidate_job.payload_json, '{}'::jsonb)
          - 'scope_change_tx_token'
          - 'scope_change_generation'
          - 'bounded_scope_state_precedes_job'
      ) || pg_catalog.jsonb_build_object(
        'scope_change_tx_token', v_stage_token,
        'bounded_scope_state_precedes_job', true,
        'preinvalidated_scope_reissued', true,
        'preinvalidated_scope_reissued_at_utc', v_now,
        'preinvalidated_scope_reissue_pending_finalization', true,
        'preinvalidated_scope_original_tx_token',
          candidate_job.payload_json->>'scope_change_tx_token',
        'preinvalidated_scope_original_generation',
          candidate_job.payload_json->>'scope_change_generation',
        'preinvalidated_scope_effective_timesheet_count',
          v_effective_timesheet_count,
        'latest_source_change_seq', v_common_source_seq,
        'source_change_seq', v_common_source_seq,
        'source_change_sequence', v_common_source_seq,
        'dirty_apply_cohort_contract_version',
          'DIRTY_APPLY_COHORT_AUTHORITY_V1',
        'dirty_apply_cohort_candidate_id', p_candidate_id,
        'dirty_apply_cohort_tx_token', v_stage_token,
        'dirty_apply_cohort_leader_job_id', p_job_id,
        'dirty_apply_cohort_member_count', v_member_count,
        'dirty_apply_cohort_excluded_request_owned_count', v_excluded_count,
        'dirty_apply_cohort_authority_scope', v_authority_scope,
        'dirty_apply_cohort_contains_full_scope', v_contains_full_scope,
        'dirty_apply_cohort_effective_timesheet_count',
          v_effective_timesheet_count,
        'dirty_apply_cohort_source_change_seq', v_common_source_seq,
        'dirty_apply_cohort_full_fallback_reason', v_full_fallback_reason,
        'dirty_apply_cohort_snapshot_max_updated_at_utc',
          v_snapshot_max_updated_at,
        'dirty_apply_cohort_snapshot_max_latest_event_at_utc',
          v_snapshot_max_latest_event,
        'dirty_apply_cohort_staged_at_utc', v_now,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      scope_change_tx_token = v_stage_token,
      scope_change_generation = NULL,
      updated_at_utc = v_now
  WHERE candidate_job.id = ANY(v_admitted_member_ids);

  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS candidate_job
    WHERE candidate_job.id = ANY(v_admitted_member_ids)
      AND (
        candidate_job.scope_change_tx_token IS DISTINCT FROM v_stage_token
        OR candidate_job.scope_change_generation IS NOT NULL
        OR COALESCE(candidate_job.payload_json
             ->>'scope_change_tx_token', '') <> v_stage_token::text
        OR COALESCE(candidate_job.payload_json
             ->>'dirty_apply_cohort_tx_token', '') <> v_stage_token::text
      )
  ) OR NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id = p_candidate_id
      AND registry.last_scope_change_tx_token = v_stage_token
      AND registry.current_source_change_seq >= v_common_source_seq
  ) OR NOT EXISTS (
    SELECT 1
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key =
            'pay_candidate:' || p_candidate_id::text
      AND change_counter.scope_change_tx_token = v_stage_token
      AND change_counter.seq >= v_common_source_seq
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_POSTCONDITION_FAILED'
      USING ERRCODE = '40001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAY_WORKBENCH_DIRTY_COHORT_POSTCONDITION_FAILED',
        'job_id', p_job_id,
        'candidate_id', p_candidate_id,
        'scope_change_tx_token', v_stage_token,
        'member_count', v_member_count
      )::text;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'action', 'COHORT_REISSUED_PENDING_FINALIZATION',
    'authority_reusable', false,
    'pending_finalization', true,
    'candidate_id', p_candidate_id,
    'scope_change_tx_token', v_stage_token,
    'scope_change_generation', NULL,
    'authority_scope', v_authority_scope,
    'full_fallback_reason', v_full_fallback_reason,
    'member_count', v_member_count,
    'excluded_request_owned_count', v_excluded_count,
    'effective_timesheet_count', v_effective_timesheet_count,
    'source_change_seq', v_common_source_seq,
    'snapshot_max_updated_at_utc', v_snapshot_max_updated_at,
    'snapshot_max_latest_event_at_utc', v_snapshot_max_latest_event
  );
END;
$function$;

-- The complete public processor replacement follows. Its classification,
-- session paging, enqueue, completion and result behavior are retained from
-- the current owner; only its scope-authority reissue block delegates to the
-- Candidate cohort owner above.

-- PROCESSOR_REPLACEMENT_INSERTION_POINT
CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(p_job_id uuid, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_payload jsonb := '{}'::jsonb;
  v_candidate_id uuid;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_dependency_closure_json jsonb := '{}'::jsonb;
  v_dependency_closure_requires_full boolean := false;
  v_dependency_closure_reason text := NULL::text;
  v_family_scope_json jsonb := '{}'::jsonb;
  v_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_reported_family_timesheet_count integer := 0;
  v_canonical_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_canonical_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_effective_bounded_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preceding_scope_authority_reusable boolean := false;
  v_preinvalidated_scope_reissued boolean := false;
  v_payload_scope_change_tx_token uuid := NULL::uuid;
  v_payload_scope_change_generation bigint := NULL::bigint;
  v_effective_scope_change_tx_token uuid := NULL::uuid;
  v_effective_scope_change_generation bigint := NULL::bigint;
  v_scope_transaction_state text := NULL::text;
  v_scope_transaction_generation bigint := NULL::bigint;
  v_registry_dirty_generation bigint := NULL::bigint;
  v_live_scope_change_generation bigint := NULL::bigint;
  v_scope_state_generation_match_count integer := 0;
  v_dirty_cohort_result jsonb := '{}'::jsonb;
  v_dirty_cohort_action text := NULL::text;
  v_dirty_cohort_authority_scope text := NULL::text;
  v_dirty_cohort_full_authority_reusable boolean := false;
  v_dirty_cohort_fast_path_reusable boolean := false;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_reason text := 'DIRTY_TRIGGER:CANDIDATE';
  v_payload_seq bigint := 0;
  v_live_seq bigint := 0;
  v_processed_source_change_seq bigint := 0;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_updated integer := 0;
  v_scope_count integer := 0;
  v_source_dirty_count integer := 0;
  v_source_dirty_total integer := 0;
  v_line_work_count integer := 0;
  v_line_work_total integer := 0;
  v_preview_count integer := 0;
  v_preview_total integer := 0;
  v_session_count integer := 0;
  v_jobs_queued integer := 0;
  v_refresh_result jsonb := '{}'::jsonb;
  v_refresh_payload_json jsonb := '{}'::jsonb;
  v_jobs_coalesced_active integer := 0;
  v_jobs_coalesced_complete integer := 0;
  v_jobs_requeued_for_conflict integer := 0;
  v_preflight_result jsonb := '{}'::jsonb;
  v_preflight_action text := 'PROCEED';
  v_preflight_match_count integer := 0;
  v_dirty_marking_skipped boolean := false;
  v_is_authorise_delta_targeted boolean := false;
  v_lifecycle_context text := NULL::text;
  v_candidate_serial_state jsonb := '{}'::jsonb;
  v_candidate_serial_blocked boolean := false;
  v_scope_ensure_result jsonb := '{}'::jsonb;
  v_new_scope_baseline_required boolean := false;
  v_session_scan_cutoff_created_at timestamptz;
  v_session_scan_cutoff_id uuid;
  v_session_scan_last_created_at timestamptz;
  v_session_scan_last_id uuid;
  v_session_scan_has_more boolean := false;
  v_session_scan_started_at timestamptz;
  v_session_scan_sessions_examined bigint := 0;
  v_session_page_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_lock_acquired boolean := false;
  v_private_cursor jsonb := '{}'::jsonb;
  v_cursor_sequence bigint := NULL::bigint;
  v_reuse_selection jsonb := '{}'::jsonb;
  v_reuse_job_id uuid := NULL::uuid;
  v_reuse_source_session_id uuid := NULL::uuid;
  v_reuse_dedupe_key text := NULL::text;
  v_request_owned_dirty_request_id uuid := NULL::uuid;
  v_request_owned_dirty_operation_id uuid := NULL::uuid;
  v_request_owned_dirty_operation_phase text := NULL::text;
  v_request_owned_dirty_context jsonb := '{}'::jsonb;
  v_request_owned_dirty_context_digest text := NULL::text;
  v_request_owned_dirty_deferral_enabled boolean := false;
  v_request_owned_dirty_delay interval := interval '5 seconds';
BEGIN
  SELECT job_row.*
  INTO v_job
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.id = p_job_id
  FOR UPDATE;

  IF v_job.id IS NULL THEN
    RAISE EXCEPTION 'Banking Pay candidate dirty apply job not found: %', p_job_id
      USING ERRCODE = 'P0002';
  END IF;
  IF UPPER(BTRIM(COALESCE(v_job.job_type, ''))) <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY' THEN
    RAISE EXCEPTION 'Job % is %, not WORKBENCH_CANDIDATE_DIRTY_APPLY', p_job_id, v_job.job_type
      USING ERRCODE = '22023';
  END IF;
  IF UPPER(BTRIM(COALESCE(v_job.status, ''))) <> 'RUNNING' THEN
    RAISE EXCEPTION 'Job % must be RUNNING before processing; status=%', p_job_id, v_job.status
      USING ERRCODE = '55000';
  END IF;

  v_payload := COALESCE(v_job.payload_json, '{}'::jsonb);
  v_preinvalidated_scope_reissued := lower(BTRIM(COALESCE(
    v_payload->>'preinvalidated_scope_reissued','false'
  ))) IN ('true','t','1','yes','y','on');
  v_candidate_id := COALESCE(
    v_job.candidate_id,
    CASE WHEN COALESCE(v_payload->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (v_payload->>'candidate_id')::uuid END
  );
  IF v_candidate_id IS NULL THEN
    RAISE EXCEPTION 'Candidate dirty apply job % has no candidate_id', p_job_id
      USING ERRCODE = '22023';
  END IF;

  v_candidate_lock_acquired := pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_candidate_id),
      24062027
    )
  );

  IF v_candidate_lock_acquired THEN
    v_candidate_serial_state := public._pay_workbench_candidate_serial_active_state(
      p_job_id,
      v_candidate_id,
      v_job.job_type,
      v_payload,
      v_now
    );
  END IF;
  v_candidate_serial_blocked := NOT v_candidate_lock_acquired;

  IF v_candidate_serial_blocked IS TRUE THEN
    UPDATE public.banking_pay_workbench_jobs AS delayed_job
    SET status = 'QUEUED',
        attempt_count = GREATEST(COALESCE(delayed_job.attempt_count, 0) - 1, 0),
        run_at_utc = GREATEST(COALESCE(delayed_job.run_at_utc, v_now), v_now + interval '5 seconds'),
        started_at_utc = NULL,
        updated_at_utc = v_now,
        payload_json = public._pay_workbench_dirty_payload_merge(
          COALESCE(delayed_job.payload_json, '{}'::jsonb),
          jsonb_strip_nulls(jsonb_build_object(
            'candidate_serial_key', public._pay_workbench_candidate_serial_key(v_candidate_id),
            'candidate_serial_candidate_id', v_candidate_id::text,
            'candidate_serial_blocked_by_job_id', v_candidate_serial_state->>'blocked_job_id',
            'candidate_serial_blocked_by_chain_job_id', v_candidate_serial_state->>'blocked_chain_job_id',
            'candidate_serial_blocked_by_projection_run_id', v_candidate_serial_state->>'projection_run_id',
            'candidate_serial_wait_reason', COALESCE(v_candidate_serial_state->>'reason', CASE WHEN NOT v_candidate_lock_acquired THEN 'CANDIDATE_SERIAL_LOCK_BUSY' ELSE 'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED' END),
            'candidate_serial_delayed_at_utc', v_now::text,
            'dirty_apply_row_marking_applied', false,
            'dirty_marking_skipped', true,
            'session_progress_dirtying_skipped', true,
            'classifier_work_skipped_by_candidate_serial', true,
            'source_build_enqueue_skipped_by_candidate_serial', true,
            'line_work_enqueue_skipped_by_candidate_serial', true,
            'preview_materialise_enqueue_skipped_by_candidate_serial', true,
            'rerun_required', true,
            'has_more', true,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ))
        )
    WHERE delayed_job.id = p_job_id;

    PERFORM public._pay_workbench_candidate_serial_audit(
      'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED',
      p_job_id,
      v_candidate_id,
      COALESCE(v_candidate_serial_state, '{}'::jsonb) || jsonb_build_object(
        'job_type', v_job.job_type,
        'dirty_apply_row_marking_applied', false,
        'dirty_marking_skipped', true,
        'delayed_until_utc', (v_now + interval '5 seconds')::text
      ),
      'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED',
      NULL::uuid
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', p_job_id::text,
      'candidate_id', v_candidate_id::text,
      'candidate_serial_delayed', true,
      'has_more', true,
      'rerun_required', true,
      'next_cursor_json', COALESCE(v_job.private_cursor_json, '{}'::jsonb),
      'reason', COALESCE(v_candidate_serial_state->>'reason', CASE WHEN NOT v_candidate_lock_acquired THEN 'CANDIDATE_SERIAL_LOCK_BUSY' ELSE 'CANDIDATE_SERIAL_DIRTY_APPLY_DELAYED' END),
      'dirty_apply_row_marking_applied', false,
      'dirty_marking_skipped', true,
      'session_progress_dirtying_skipped', true,
      'classifier_work_skipped_by_candidate_serial', true,
      'source_build_enqueue_skipped_by_candidate_serial', true,
      'line_work_enqueue_skipped_by_candidate_serial', true,
      'preview_materialise_enqueue_skipped_by_candidate_serial', true,
      'elapsed_ms', EXTRACT(MILLISECONDS FROM clock_timestamp() - v_started_at)
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_payload->'targeted_timesheet_ids') = 'array' THEN v_payload->'targeted_timesheet_ids' ELSE '[]'::jsonb END) AS raw_value(value_text)
  WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_linked_timesheet_ids
  FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_payload->'linked_timesheet_ids') = 'array' THEN v_payload->'linked_timesheet_ids' ELSE '[]'::jsonb END) AS raw_value(value_text)
  WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_payload->'finance_case_ids') = 'array'
        THEN v_payload->'finance_case_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_value(value_text)
  WHERE raw_value.value_text
    ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_linked_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    IF to_regprocedure(
         'public._pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)'
       ) IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_REFRESH_DEPENDENCY_CLOSURE_UNAVAILABLE'
        USING ERRCODE = 'P0001';
    END IF;

    EXECUTE
      'SELECT public._pay_workbench_refresh_dependency_closure_v1($1,$2,$3,$4,$5,$6)'
    INTO v_dependency_closure_json
    USING
      v_candidate_id,
      v_targeted_timesheet_ids,
      v_linked_timesheet_ids,
      v_finance_case_ids,
      250,
      100;

    v_dependency_closure_requires_full :=
      LOWER(BTRIM(COALESCE(
        v_dependency_closure_json->>'requires_full_candidate',
        'true'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR LOWER(BTRIM(COALESCE(
        v_dependency_closure_json->>'coverage_complete',
        'false'
      ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on');
    v_dependency_closure_reason := NULLIF(BTRIM(COALESCE(
      v_dependency_closure_json->>'fallback_reason',
      ''
    )), '');

    IF v_dependency_closure_requires_full THEN
      v_targeted_timesheet_ids := ARRAY[]::uuid[];
      v_linked_timesheet_ids := ARRAY[]::uuid[];
      v_finance_case_ids := ARRAY[]::uuid[];
      v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    ELSE
      SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
      INTO v_targeted_timesheet_ids
      FROM jsonb_array_elements_text(
        COALESCE(v_dependency_closure_json->'effective_targeted_timesheet_ids', '[]'::jsonb)
      ) AS effective_timesheet(value)
      WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

      v_linked_timesheet_ids := ARRAY[]::uuid[];

      SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid), ARRAY[]::uuid[])
      INTO v_finance_case_ids
      FROM jsonb_array_elements_text(
        COALESCE(v_dependency_closure_json->'effective_finance_case_ids', '[]'::jsonb)
      ) AS effective_finance_case(value)
      WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
    END IF;
  END IF;

  IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0
     OR COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    v_family_scope_json := public._pay_workbench_normalise_timesheet_rotation_scope_payload(v_targeted_timesheet_ids, v_linked_timesheet_ids);

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_family_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'family_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_canonical_targeted_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'targeted_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT value_text::uuid ORDER BY value_text::uuid), ARRAY[]::uuid[])
    INTO v_canonical_linked_timesheet_ids
    FROM jsonb_array_elements_text(COALESCE(v_family_scope_json->'linked_timesheet_ids', '[]'::jsonb)) AS raw_value(value_text)
    WHERE raw_value.value_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT all_ids.timesheet_id ORDER BY all_ids.timesheet_id), ARRAY[]::uuid[])
    INTO v_all_timesheet_ids
    FROM (
      SELECT unnest(v_targeted_timesheet_ids) AS timesheet_id
      UNION ALL
      SELECT unnest(v_linked_timesheet_ids) AS timesheet_id
      UNION ALL
      SELECT unnest(v_family_timesheet_ids) AS timesheet_id
    ) AS all_ids
    WHERE all_ids.timesheet_id IS NOT NULL;
    v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
  ELSE
    v_linked_timesheet_ids := ARRAY[]::uuid[];
    v_family_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_targeted_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_linked_timesheet_ids := ARRAY[]::uuid[];
    v_all_timesheet_ids := ARRAY[]::uuid[];
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
  END IF;

  SELECT COALESCE(array_agg(DISTINCT bounded_id ORDER BY bounded_id), ARRAY[]::uuid[])
  INTO v_effective_bounded_timesheet_ids
  FROM (
    SELECT unnest(v_canonical_targeted_timesheet_ids) AS bounded_id
    UNION ALL
    SELECT unnest(v_canonical_linked_timesheet_ids) AS bounded_id
  ) AS bounded_scope
  WHERE bounded_id IS NOT NULL;

  v_reason := COALESCE(NULLIF(BTRIM(COALESCE(v_payload->>'reason_latest', v_payload->>'reason', '')), ''), 'DIRTY_TRIGGER:CANDIDATE');
  v_payload_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_payload->>'latest_source_change_seq', '') ~ '^\d+$' THEN (v_payload->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_seq', '') ~ '^\d+$' THEN (v_payload->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_payload->>'source_change_sequence', '') ~ '^\d+$' THEN (v_payload->>'source_change_sequence')::bigint END, 0)
  );

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_live_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || v_candidate_id::text;

  v_processed_source_change_seq := GREATEST(COALESCE(v_payload_seq, 0), COALESCE(v_live_seq, 0));

  -- A correction request transition is orchestration evidence, not economic
  -- truth.  Validate the transaction-stamped causal envelope before consulting
  -- the durable correction lifecycle.  The latest scope token must be the
  -- request-owned token; any later unrelated invalidation changes that token
  -- and immediately falls through to the ordinary classifier.
  SELECT COALESCE(
    (pg_catalog.to_jsonb(settings_row)
      ->>'banking_pay_correction_request_dirty_deferral_v1_enabled')::boolean,
    false
  )
  INTO v_request_owned_dirty_deferral_enabled
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  v_request_owned_dirty_context:=CASE
    WHEN pg_catalog.jsonb_typeof(v_payload->'correction_dirty_contexts')='object'
      AND pg_catalog.jsonb_typeof(
        v_payload->'correction_dirty_contexts'->v_candidate_id::text
      )='object'
      THEN v_payload->'correction_dirty_contexts'->v_candidate_id::text
    ELSE '{}'::jsonb
  END;

  IF COALESCE(v_request_owned_dirty_deferral_enabled,false)
     AND COALESCE(v_request_owned_dirty_context->>'contract_version','')
           ='CORRECTION_OWNED_DIRTY_CAUSAL_V1'
     AND COALESCE(v_request_owned_dirty_context->>'candidate_id','')=v_candidate_id::text
     AND COALESCE(v_request_owned_dirty_context->>'correction_request_id','')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND COALESCE(v_request_owned_dirty_context->>'pay_batch_id','')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND COALESCE(v_request_owned_dirty_context->>'lifecycle_phase','') IN (
       'REQUEST_PREPARE','REQUEST_START','FINANCIAL_PAGE_START',
       'FINANCIAL_PAGE_APPLIED','FINANCIAL_TERMINAL'
     )
     AND COALESCE(v_request_owned_dirty_context->>'policy_x_boundary','') IN (
       'POST_DRAFT_FROZEN_EVIDENCE','PRE_DRAFT_LIVE_TRUTH'
     )
     AND COALESCE(v_request_owned_dirty_context->>'pre_request_source_change_seq','')
           ~ '^[0-9]{1,18}$'
     AND COALESCE(v_request_owned_dirty_context->>'pre_request_dirty_generation','')
           ~ '^[0-9]{1,18}$'
     AND COALESCE(v_payload->>'request_owned_scope_change_tx_token','')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND COALESCE(v_payload->>'scope_change_tx_token','')
           =COALESCE(v_payload->>'request_owned_scope_change_tx_token','')
     AND (
       v_job.scope_change_tx_token IS NULL
       OR v_job.scope_change_tx_token::text
            =COALESCE(v_payload->>'request_owned_scope_change_tx_token','')
     )
     AND LOWER(BTRIM(COALESCE(v_payload->>'policy_x_dirtying_only','false')))
           IN ('true','t','1','yes','y','on')
     AND LOWER(BTRIM(COALESCE(v_payload->>'economic_truth_mutation_allowed','false')))
           NOT IN ('true','t','1','yes','y','on') THEN
    v_request_owned_dirty_context_digest:=pg_catalog.encode(extensions.digest(
      pg_catalog.convert_to(
        'CORRECTION_OWNED_DIRTY_CAUSAL_V1'||'|'||
        COALESCE(v_request_owned_dirty_context->>'correction_request_id','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'correction_operation_id','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'correction_work_item_id','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'pay_batch_id','')||'|'||
        v_candidate_id::text||'|'||
        COALESCE(v_request_owned_dirty_context->>'lifecycle_phase','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'policy_x_boundary','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'pre_request_source_change_seq','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'pre_request_dirty_generation','')||'|'||
        COALESCE(v_request_owned_dirty_context->>'pre_request_fence_digest',''),
        'UTF8'
      ),
      'sha256'
    ),'hex');

    IF v_request_owned_dirty_context_digest
         =COALESCE(v_request_owned_dirty_context->>'context_digest','') THEN
      SELECT request_row.id,correction_operation.id,correction_operation.phase,
             CASE
               WHEN correction_operation.status='WAITING_AUTHORISATION'
                 OR correction_operation.phase='AWAITING_REAUTHENTICATION'
                 OR request_row.status IN ('PLANNING','PLANNED','REQUESTED','AWAITING_AUTHORISATION')
                 THEN interval '30 seconds'
               ELSE interval '5 seconds'
             END
      INTO v_request_owned_dirty_request_id,
           v_request_owned_dirty_operation_id,
           v_request_owned_dirty_operation_phase,
           v_request_owned_dirty_delay
      FROM public.pay_payment_correction_requests AS request_row
      JOIN public.banking_pay_operations AS correction_operation
        ON correction_operation.operation_type='PAYMENT_CORRECTION'
       AND correction_operation.input_json->>'correction_request_id'=request_row.id::text
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id=request_row.pay_batch_id
       AND batch_candidate.candidate_id=v_candidate_id
      WHERE request_row.id=(v_request_owned_dirty_context->>'correction_request_id')::uuid
        AND request_row.pay_batch_id=(v_request_owned_dirty_context->>'pay_batch_id')::uuid
        AND correction_operation.status IN ('QUEUED','RUNNING','WAITING_AUTHORISATION')
        AND correction_operation.phase<>'COMPLETE'
        AND request_row.status NOT IN ('CANCELLED','FAILED','REJECTED')
        -- Candidate dirty jobs are deliberately deduplicated and may be reused.
        -- Their immutable created_at can therefore predate the correction even
        -- when the exact request-owned event was merged later.  The causal
        -- digest and transaction token above prove ownership; updated_at proves
        -- that this durable job received the request event after it existed.
        AND request_row.created_at_utc<=GREATEST(
          v_job.created_at_utc,
          COALESCE(v_job.updated_at_utc,v_job.created_at_utc)
        )
        AND (
          COALESCE(v_request_owned_dirty_context->>'correction_operation_id','')=''
          OR correction_operation.id::text
               =v_request_owned_dirty_context->>'correction_operation_id'
        )
      ORDER BY correction_operation.created_at_utc DESC
      LIMIT 1;
    END IF;
  ELSE
    -- Compatibility fallback while the new setting is disabled.  This is the
    -- prior retrospective proof and is deliberately retained for rollback.
    IF LOWER(BTRIM(COALESCE(v_payload->>'trigger_table','')))
          ='pay_payment_correction_requests'
       AND LOWER(BTRIM(COALESCE(v_payload->>'policy_x_dirtying_only','false')))
             IN ('true','t','1','yes','y','on')
       AND LOWER(BTRIM(COALESCE(v_payload->>'economic_truth_mutation_allowed','false')))
             NOT IN ('true','t','1','yes','y','on')
       AND (
         (
           jsonb_typeof(v_payload->'reasons')='array'
           AND jsonb_array_length(v_payload->'reasons')>0
           AND NOT EXISTS (
             SELECT 1
             FROM jsonb_array_elements_text(v_payload->'reasons') AS dirty_reason(reason_text)
             WHERE UPPER(BTRIM(dirty_reason.reason_text)) NOT IN (
               'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
               'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
             )
           )
         )
         OR (
           jsonb_typeof(v_payload->'reasons') IS DISTINCT FROM 'array'
           AND UPPER(BTRIM(COALESCE(v_payload->>'reason_latest',v_payload->>'reason',''))) IN (
             'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
             'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
           )
         )
       ) THEN
      SELECT request_row.id,correction_operation.id,correction_operation.phase
      INTO v_request_owned_dirty_request_id,
           v_request_owned_dirty_operation_id,
           v_request_owned_dirty_operation_phase
      FROM public.banking_pay_operations AS correction_operation
      JOIN public.pay_payment_correction_requests AS request_row
        ON request_row.id=CASE
          WHEN COALESCE(correction_operation.input_json->>'correction_request_id','')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (correction_operation.input_json->>'correction_request_id')::uuid
          ELSE NULL::uuid END
      JOIN public.pay_payment_correction_request_candidates AS request_candidate
        ON request_candidate.correction_request_id=request_row.id
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=request_candidate.pay_batch_candidate_id
       AND batch_candidate.candidate_id=v_candidate_id
      WHERE correction_operation.operation_type='PAYMENT_CORRECTION'
        AND correction_operation.status IN ('QUEUED','RUNNING')
        AND correction_operation.phase NOT IN ('REFRESH_WORKBENCH','COMPLETE')
        AND correction_operation.input_json->'draft_overlay_fast_start_authorities'
              ->v_candidate_id::text->>'request_owned_dirty_job_id'=p_job_id::text
        AND request_row.status IN (
          'REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING'
        )
        AND request_row.created_at_utc<=GREATEST(
          v_job.created_at_utc,
          COALESCE(v_job.updated_at_utc,v_job.created_at_utc)
        )
      ORDER BY request_row.created_at_utc DESC,correction_operation.created_at_utc DESC
      LIMIT 1;
    END IF;
  END IF;

  IF v_request_owned_dirty_request_id IS NOT NULL THEN
    UPDATE public.banking_pay_workbench_jobs AS delayed_job
    SET status = 'QUEUED',
        attempt_count = GREATEST(COALESCE(delayed_job.attempt_count, 0) - 1, 0),
        run_at_utc = GREATEST(COALESCE(delayed_job.run_at_utc, v_now), v_now + v_request_owned_dirty_delay),
        started_at_utc = NULL,
        updated_at_utc = v_now,
        payload_json = public._pay_workbench_dirty_payload_merge(
          COALESCE(delayed_job.payload_json, '{}'::jsonb),
          jsonb_build_object(
            'request_owned_dirty_classification', 'REQUEST_OWNED_POLICY_X_DIRTY',
            'waiting_for_correction_financial_boundary',
              COALESCE(v_request_owned_dirty_operation_phase,'')<>'REFRESH_WORKBENCH',
            'waiting_for_correction_route_election',
              COALESCE(v_request_owned_dirty_operation_phase,'')='REFRESH_WORKBENCH'
              OR COALESCE(v_request_owned_dirty_context->>'lifecycle_phase','') IN (
                'FINANCIAL_PAGE_APPLIED','FINANCIAL_TERMINAL'
              ),
            'correction_request_id', v_request_owned_dirty_request_id::text,
            'correction_operation_id', v_request_owned_dirty_operation_id::text,
            'correction_operation_phase', v_request_owned_dirty_operation_phase,
            'correction_dirty_context_digest', v_request_owned_dirty_context->>'context_digest',
            'request_owned_scope_change_tx_token', v_payload->>'request_owned_scope_change_tx_token',
            'request_boundary_delayed_at_utc', v_now::text,
            'dirty_apply_row_marking_applied', false,
            'dirty_marking_skipped', true,
            'session_progress_dirtying_skipped', true,
            'classifier_work_skipped_by_request_boundary', true,
            'source_build_enqueue_skipped_by_request_boundary', true,
            'rerun_required', true,
            'has_more', true,
            'policy_x_authority_scope', 'POST_DRAFT_FROZEN_BATCH_EVIDENCE'
          )
        )
    WHERE delayed_job.id = p_job_id;

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', p_job_id::text,
      'candidate_id', v_candidate_id::text,
      'candidate_serial_delayed', true,
      'request_boundary_delayed', true,
      'has_more', true,
      'rerun_required', true,
      'reason', CASE
        WHEN COALESCE(v_request_owned_dirty_operation_phase,'')='REFRESH_WORKBENCH'
          OR COALESCE(v_request_owned_dirty_context->>'lifecycle_phase','') IN (
            'FINANCIAL_PAGE_APPLIED','FINANCIAL_TERMINAL'
          )
          THEN 'WAITING_FOR_CORRECTION_ROUTE_ELECTION'
        ELSE 'REQUEST_OWNED_POLICY_X_DIRTY_WAITING_FOR_FINANCIAL_BOUNDARY'
      END,
      'correction_request_id', v_request_owned_dirty_request_id::text,
      'correction_operation_id', v_request_owned_dirty_operation_id::text,
      'correction_operation_phase', v_request_owned_dirty_operation_phase,
      'correction_dirty_context_digest', v_request_owned_dirty_context->>'context_digest',
      'request_owned_scope_change_tx_token', v_payload->>'request_owned_scope_change_tx_token',
      'dirty_apply_row_marking_applied', false,
      'dirty_marking_skipped', true,
      'session_progress_dirtying_skipped', true,
      'classifier_work_skipped_by_request_boundary', true,
      'source_build_enqueue_skipped_by_request_boundary', true,
      'elapsed_ms', EXTRACT(MILLISECONDS FROM clock_timestamp() - v_started_at)
    );
  END IF;

  IF v_job.private_cursor_kind IS NOT NULL
     AND v_job.private_cursor_kind <> 'DIRTY_SESSION_SCAN_V1' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_SESSION_CURSOR_KIND_INVALID'
      USING ERRCODE='P0001';
  END IF;

  v_private_cursor := COALESCE(v_job.private_cursor_json, '{}'::jsonb);
  BEGIN
    v_cursor_sequence := CASE
      WHEN COALESCE(v_private_cursor->>'scan_source_change_seq','') ~ '^\d+$'
        THEN (v_private_cursor->>'scan_source_change_seq')::bigint
      ELSE NULL::bigint
    END;
    v_session_scan_cutoff_created_at := NULLIF(BTRIM(COALESCE(v_private_cursor->>'upper_created_at_utc','')),'')::timestamptz;
    v_session_scan_cutoff_id := NULLIF(BTRIM(COALESCE(v_private_cursor->>'upper_session_id','')),'')::uuid;
    v_session_scan_last_created_at := NULLIF(BTRIM(COALESCE(v_private_cursor->>'last_created_at_utc','')),'')::timestamptz;
    v_session_scan_last_id := NULLIF(BTRIM(COALESCE(v_private_cursor->>'last_session_id','')),'')::uuid;
    v_session_scan_started_at := COALESCE(
      NULLIF(BTRIM(COALESCE(v_private_cursor->>'scan_started_at_utc','')),'')::timestamptz,
      v_now
    );
    v_session_scan_sessions_examined := COALESCE(
      CASE WHEN COALESCE(v_private_cursor->>'sessions_examined','') ~ '^\d+$'
        THEN (v_private_cursor->>'sessions_examined')::bigint END,
      0
    );
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_SESSION_CURSOR_INVALID' USING ERRCODE='P0001';
  END;

  IF v_job.private_cursor_kind IS NULL
     OR v_job.private_stage_version IS DISTINCT FROM 1
     OR v_cursor_sequence IS DISTINCT FROM v_processed_source_change_seq THEN
    SELECT session_candidate.created_at_utc, session_candidate.id
    INTO v_session_scan_cutoff_created_at, v_session_scan_cutoff_id
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.status='OPEN'
      AND session_candidate.discarded_at_utc IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=session_candidate.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (session_candidate.pay_date,session_candidate.created_at_utc,session_candidate.id)
      )
    ORDER BY session_candidate.created_at_utc DESC,session_candidate.id DESC
    LIMIT 1;

    v_session_scan_last_created_at := NULL::timestamptz;
    v_session_scan_last_id := NULL::uuid;
    v_session_scan_started_at := v_now;
    v_session_scan_sessions_examined := 0;

    UPDATE public.banking_pay_workbench_jobs AS cursor_job
    SET private_cursor_kind='DIRTY_SESSION_SCAN_V1',
        private_stage_version=1,
        private_cursor_json=jsonb_strip_nulls(jsonb_build_object(
          'scan_source_change_seq',v_processed_source_change_seq,
          'upper_created_at_utc',v_session_scan_cutoff_created_at,
          'upper_session_id',v_session_scan_cutoff_id,
          'last_created_at_utc',NULL,
          'last_session_id',NULL,
          'scan_started_at_utc',v_session_scan_started_at,
          'sessions_examined',0
        )),
        updated_at_utc=v_now
    WHERE cursor_job.id=p_job_id;
  END IF;
  v_lifecycle_context := LOWER(BTRIM(COALESCE(v_payload->>'lifecycle_mutation_context', v_payload->>'mutation_context', v_payload->>'lifecycle_context', '')));
  v_is_authorise_delta_targeted := v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
    AND COALESCE(array_length(v_canonical_targeted_timesheet_ids, 1), 0) > 0
    AND COALESCE(array_length(v_finance_case_ids, 1), 0) = 0
    AND lower(BTRIM(COALESCE(v_payload->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) NOT IN ('true','t','1','yes','y','on')
    AND (
      lower(BTRIM(COALESCE(v_payload->>'authorise_boundary_changed', v_payload->>'timesheet_authorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
      OR lower(BTRIM(COALESCE(v_payload->>'unauthorise_boundary_changed', v_payload->>'timesheet_unauthorise_boundary_changed', 'false'))) IN ('true','t','1','yes','y','on')
      OR v_lifecycle_context IN ('timesheet_authorise', 'authorise_timesheet', 'timesheet_unauthorise', 'unauthorise_timesheet')
      OR LOWER(COALESCE(v_reason, '')) LIKE '%authorise%'
      OR LOWER(COALESCE(v_reason, '')) LIKE '%unauthorise%'
    );

  -- The trigger invalidates the exact row known at write time.  Dependency and
  -- rotation closure can legitimately discover a wider Timesheet family by
  -- the time this worker runs, and a newer event can also supersede the
  -- original generation.  Prove the *final* bounded scope here, once under the
  -- Candidate serial lock and before scanning any open session.  If the old
  -- proof is incomplete or stale, issue one fresh central invalidation for the
  -- complete effective scope and reuse that one proof for every session.
  v_payload_scope_change_tx_token := CASE
    WHEN COALESCE(v_payload->>'scope_change_tx_token','')
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_payload->>'scope_change_tx_token')::uuid
    ELSE NULL::uuid
  END;
  v_payload_scope_change_generation := CASE
    WHEN COALESCE(v_payload->>'scope_change_generation','') ~ '^[0-9]{1,18}$'
      THEN (v_payload->>'scope_change_generation')::bigint
    ELSE NULL::bigint
  END;

  SELECT scope_tx.state,scope_tx.allocated_generation
  INTO v_scope_transaction_state,v_scope_transaction_generation
  FROM public.banking_pay_scope_change_transactions AS scope_tx
  WHERE scope_tx.tx_token=v_payload_scope_change_tx_token;

  SELECT registry.dirty_generation,
         change_counter.scope_change_generation
  INTO v_registry_dirty_generation,v_live_scope_change_generation
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  LEFT JOIN public.app_change_counters AS change_counter
    ON change_counter.entity_key='pay_candidate:'||v_candidate_id::text
  WHERE registry.candidate_id=v_candidate_id;

  SELECT count(*)::integer
  INTO v_scope_state_generation_match_count
  FROM unnest(v_effective_bounded_timesheet_ids) AS requested(timesheet_id)
  JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
    ON scope_state.timesheet_id=requested.timesheet_id
   AND scope_state.candidate_id=v_candidate_id
   AND scope_state.dirty_generation=v_payload_scope_change_generation;

  -- A bounded-union cohort keeps the original per-Timesheet proof below. A
  -- cap/ownership fallback is a stronger Candidate-full authority and is
  -- accepted only when this exact postgres-owned helper stamped the same
  -- Candidate/token and the ordinary finalized generation fences all match.
  v_dirty_cohort_full_authority_reusable :=
    COALESCE(v_payload->>'dirty_apply_cohort_authority_scope', '') =
          'CANDIDATE_FULL_LIVE'
    AND COALESCE(v_payload->>'dirty_apply_cohort_contract_version', '') =
          'DIRTY_APPLY_COHORT_AUTHORITY_V1'
    AND COALESCE(v_payload->>'dirty_apply_cohort_candidate_id', '') =
          v_candidate_id::text
    AND COALESCE(v_payload->>'dirty_apply_cohort_tx_token', '') =
          v_payload_scope_change_tx_token::text;

  v_preceding_scope_authority_reusable :=
    lower(BTRIM(COALESCE(v_payload->>'bounded_scope_state_precedes_job','false')))
      IN ('true','t','1','yes','y','on')
    AND v_payload_scope_change_tx_token IS NOT NULL
    AND COALESCE(v_payload_scope_change_generation,0)>0
    AND v_scope_transaction_state='FINALIZED'
    AND v_scope_transaction_generation=v_payload_scope_change_generation
    AND v_registry_dirty_generation=v_payload_scope_change_generation
    AND v_live_scope_change_generation=v_payload_scope_change_generation
    AND (
      (
        cardinality(v_effective_bounded_timesheet_ids)>0
        AND COALESCE(v_payload->>'dirty_apply_cohort_authority_scope', '') =
              'TARGETED_UNION'
        AND v_scope_state_generation_match_count=
              cardinality(v_effective_bounded_timesheet_ids)
      )
      OR v_dirty_cohort_full_authority_reusable
    );

  -- A member whose own exact proof still matches the finalized cohort can
  -- finish without rescanning every active sibling. If a later event has a
  -- different token/generation, that job fails this fast path and forms the
  -- next cohort with whatever active members remain.
  v_dirty_cohort_fast_path_reusable :=
    v_preceding_scope_authority_reusable
    AND COALESCE(v_payload->>'dirty_apply_cohort_contract_version', '') =
          'DIRTY_APPLY_COHORT_AUTHORITY_V1'
    AND COALESCE(v_payload->>'dirty_apply_cohort_candidate_id', '') =
          v_candidate_id::text
    AND COALESCE(v_payload->>'dirty_apply_cohort_tx_token', '') =
          v_payload_scope_change_tx_token::text
    AND (
      (
        cardinality(v_effective_bounded_timesheet_ids)>0
        AND COALESCE(v_payload->>'dirty_apply_cohort_authority_scope', '') =
              'TARGETED_UNION'
      )
      OR v_dirty_cohort_full_authority_reusable
    );

  IF v_dirty_cohort_fast_path_reusable THEN
    v_dirty_cohort_action := 'FAST_PATH_FINALIZED_COHORT';
    v_dirty_cohort_authority_scope :=
      v_payload->>'dirty_apply_cohort_authority_scope';
    v_effective_scope_change_tx_token := v_payload_scope_change_tx_token;
    v_effective_scope_change_generation :=
      v_payload_scope_change_generation;
  ELSE
    -- The current proof is absent/stale, or this row predates cohort metadata.
    -- The cohort owner either returns one already-current shared authority or
    -- stages every admitted same-Candidate sibling under one pending token.
    v_dirty_cohort_result :=
      private.pay_workbench_candidate_dirty_cohort_stage_v1(
        p_job_id,
        v_candidate_id,
        v_now
      );
    v_dirty_cohort_action := COALESCE(
      v_dirty_cohort_result->>'action',
      ''
    );
    v_dirty_cohort_authority_scope := NULLIF(BTRIM(COALESCE(
      v_dirty_cohort_result->>'authority_scope',
      ''
    )), '');

    IF v_dirty_cohort_action IN (
      'COHORT_REISSUED_PENDING_FINALIZATION',
      'WAITING_FOR_COHORT_FINALIZATION'
    ) THEN
      v_preinvalidated_scope_reissued := true;
      v_effective_scope_change_tx_token := CASE
        WHEN COALESCE(v_dirty_cohort_result->>'scope_change_tx_token', '')
               ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (v_dirty_cohort_result->>'scope_change_tx_token')::uuid
        ELSE NULL::uuid
      END;

      RETURN jsonb_build_object(
        'ok', true,
        'job_id', p_job_id::text,
        'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        'candidate_id', v_candidate_id::text,
        'refresh_scope_kind', v_refresh_scope_kind,
        'targeted_timesheet_count',
          COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
        'family_timesheet_count',
          COALESCE(array_length(v_family_timesheet_ids, 1), 0),
        'preinvalidated_scope_reissued', true,
        'preinvalidated_scope_reissue_pending_finalization', true,
        'effective_scope_change_tx_token',
          v_effective_scope_change_tx_token::text,
        'effective_scope_change_generation', NULL,
        'dirty_apply_cohort_action', v_dirty_cohort_action,
        'dirty_apply_cohort_authority_scope',
          v_dirty_cohort_authority_scope,
        'dirty_apply_cohort_member_count', COALESCE(
          (v_dirty_cohort_result->>'member_count')::integer,
          0
        ),
        'dirty_apply_cohort_excluded_request_owned_count', COALESCE(
          (v_dirty_cohort_result
            ->>'excluded_request_owned_count')::integer,
          0
        ),
        'dirty_apply_row_marking_applied', false,
        'dirty_marking_skipped', true,
        'session_progress_dirtying_skipped', true,
        'more_due', true,
        'has_more', true,
        'rerun_required', true,
        'next_cursor_json',
          COALESCE(v_job.private_cursor_json, '{}'::jsonb),
        'made_progress',
          v_dirty_cohort_action =
            'COHORT_REISSUED_PENDING_FINALIZATION',
        'dirty_apply_complete', false,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (
          clock_timestamp() - v_started_at
        )) * 1000)::numeric, 2)
      );
    ELSIF v_dirty_cohort_action <> 'REUSE_FINALIZED_AUTHORITY' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_POSTCONDITION_FAILED'
        USING ERRCODE = '40001', DETAIL = jsonb_build_object(
          'code', 'PAY_WORKBENCH_DIRTY_COHORT_POSTCONDITION_FAILED',
          'job_id', p_job_id,
          'candidate_id', v_candidate_id,
          'cohort_result', v_dirty_cohort_result
        )::text;
    END IF;

    v_effective_scope_change_tx_token := CASE
      WHEN COALESCE(v_dirty_cohort_result->>'scope_change_tx_token', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (v_dirty_cohort_result->>'scope_change_tx_token')::uuid
      ELSE NULL::uuid
    END;
    v_effective_scope_change_generation := CASE
      WHEN COALESCE(
             v_dirty_cohort_result->>'scope_change_generation',
             ''
           ) ~ '^[0-9]{1,18}$'
        THEN (v_dirty_cohort_result->>'scope_change_generation')::bigint
      ELSE NULL::bigint
    END;

    IF v_effective_scope_change_tx_token IS DISTINCT FROM
         v_payload_scope_change_tx_token
       OR v_effective_scope_change_generation IS DISTINCT FROM
         v_payload_scope_change_generation THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_REUSE_PROOF_MISMATCH'
        USING ERRCODE = '40001', DETAIL = jsonb_build_object(
          'code', 'PAY_WORKBENCH_DIRTY_COHORT_REUSE_PROOF_MISMATCH',
          'job_id', p_job_id,
          'candidate_id', v_candidate_id,
          'cohort_result', v_dirty_cohort_result
        )::text;
    END IF;
  END IF;

  -- ALL dominates a mixed cohort, and any dependency/cap fallback must really
  -- take the established Candidate-full refresh path. The durable per-job
  -- scopes/reasons remain unchanged for audit and replay compatibility.
  IF v_dirty_cohort_authority_scope = 'CANDIDATE_FULL_LIVE' THEN
    v_dependency_closure_requires_full := true;
    v_dependency_closure_reason := COALESCE(
      NULLIF(BTRIM(COALESCE(
        v_dirty_cohort_result->>'full_fallback_reason',
        v_payload->>'dirty_apply_cohort_full_fallback_reason',
        ''
      )), ''),
      'DIRTY_APPLY_COHORT_FULL_AUTHORITY'
    );
    v_dependency_closure_json := jsonb_build_object(
      'ok', true,
      'coverage_complete', false,
      'requires_full_candidate', true,
      'fallback_reason', v_dependency_closure_reason,
      'effective_targeted_timesheet_ids', '[]'::jsonb,
      'effective_linked_timesheet_ids', '[]'::jsonb,
      'effective_finance_case_ids', '[]'::jsonb,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'economic_calculation_performed', false
    );
    v_targeted_timesheet_ids := ARRAY[]::uuid[];
    v_linked_timesheet_ids := ARRAY[]::uuid[];
    v_finance_case_ids := ARRAY[]::uuid[];
    v_family_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_targeted_timesheet_ids := ARRAY[]::uuid[];
    v_canonical_linked_timesheet_ids := ARRAY[]::uuid[];
    v_all_timesheet_ids := ARRAY[]::uuid[];
    v_effective_bounded_timesheet_ids := ARRAY[]::uuid[];
    v_family_scope_json := '{}'::jsonb;
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    v_is_authorise_delta_targeted := false;
    -- The downstream scope merge unions array keys from its existing payload.
    -- Strip only the local working copy so a previously targeted durable job
    -- cannot leak stale IDs into this stronger Candidate-full refresh request.
    v_payload := v_payload
      - 'targeted_timesheet_ids'
      - 'linked_timesheet_ids'
      - 'finance_case_ids';
  END IF;

  -- Candidate-full execution deliberately clears the local arrays so no later
  -- step can accidentally fall back to the targeted path.  Preserve the
  -- already certified cohort's bounded Timesheet count in the response,
  -- however: family_timesheet_count is an established diagnostic/result field
  -- and clearing the working arrays must not rewrite its meaning to zero.
  v_reported_family_timesheet_count := COALESCE(
    array_length(v_family_timesheet_ids, 1),
    0
  );
  IF v_dirty_cohort_authority_scope = 'CANDIDATE_FULL_LIVE' THEN
    IF COALESCE(
         v_payload->>'dirty_apply_cohort_effective_timesheet_count',
         ''
       ) ~ '^[0-9]{1,9}$' THEN
      v_reported_family_timesheet_count :=
        (v_payload->>'dirty_apply_cohort_effective_timesheet_count')::integer;
    ELSIF COALESCE(
            v_dirty_cohort_result->>'effective_timesheet_count',
            ''
          ) ~ '^[0-9]{1,9}$' THEN
      v_reported_family_timesheet_count :=
        (v_dirty_cohort_result->>'effective_timesheet_count')::integer;
    END IF;
  END IF;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'dirty_worker_apply_start', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0), 'family_timesheet_count', v_reported_family_timesheet_count, 'latest_source_change_seq', v_payload_seq, 'processed_source_change_seq', v_processed_source_change_seq));

  SELECT COALESCE(array_agg(page_row.id ORDER BY page_row.created_at_utc,page_row.id),ARRAY[]::uuid[])
  INTO v_session_page_ids
  FROM (
    SELECT session_candidate.id,session_candidate.created_at_utc
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.status='OPEN'
      AND session_candidate.discarded_at_utc IS NULL
      AND v_session_scan_cutoff_created_at IS NOT NULL
      AND (session_candidate.created_at_utc,session_candidate.id)<=
          (v_session_scan_cutoff_created_at,v_session_scan_cutoff_id)
      AND (
        v_session_scan_last_created_at IS NULL
        OR (session_candidate.created_at_utc,session_candidate.id)>
           (v_session_scan_last_created_at,v_session_scan_last_id)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=session_candidate.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (session_candidate.pay_date,session_candidate.created_at_utc,session_candidate.id)
      )
    ORDER BY session_candidate.created_at_utc,session_candidate.id
    LIMIT GREATEST(1, COALESCE(p_limit, 100))
  ) AS page_row;

  PERFORM session_lock.id
  FROM public.banking_pay_workbench_sessions AS session_lock
  WHERE session_lock.id=ANY(v_session_page_ids)
  ORDER BY session_lock.id
  FOR UPDATE;

  FOR v_session_row IN
    SELECT session_candidate.*
    FROM public.banking_pay_workbench_sessions AS session_candidate
    WHERE session_candidate.id=ANY(v_session_page_ids)
    ORDER BY session_candidate.created_at_utc,session_candidate.id
  LOOP
    v_session_scan_last_created_at:=v_session_row.created_at_utc;
    v_session_scan_last_id:=v_session_row.id;
    v_session_scan_sessions_examined:=v_session_scan_sessions_examined+1;
    IF NOT EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_session_scope AS existing_scope
      WHERE existing_scope.session_id=v_session_row.id
        AND existing_scope.candidate_id=v_candidate_id
    ) THEN
      v_scope_ensure_result:=private.pay_workbench_session_candidate_scope_ensure_v1(
        v_session_row.id,v_candidate_id,p_job_id,v_processed_source_change_seq,v_reason
      );
      IF COALESCE((v_scope_ensure_result->>'eligible')::boolean,false) IS NOT TRUE THEN
        CONTINUE;
      END IF;
      v_new_scope_baseline_required:=COALESCE((v_scope_ensure_result->>'inserted')::boolean,false);
    ELSE
      v_new_scope_baseline_required:=false;
    END IF;
    IF v_new_scope_baseline_required IS TRUE THEN
      v_reuse_selection := private.pay_workbench_candidate_reuse_source_select_v1(
        v_session_row.id,
        v_candidate_id,
        v_processed_source_change_seq,
        jsonb_build_object(
          'source_job_id',p_job_id::text,
          'direct_candidate_id',v_candidate_id::text,
          'target_session_id',v_session_row.id::text
        )
      );

      IF coalesce((v_reuse_selection->>'reuse_available')::boolean,false) IS TRUE
         AND coalesce(v_reuse_selection->>'selected_source_session_id','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_reuse_source_session_id := (v_reuse_selection->>'selected_source_session_id')::uuid;
        v_reuse_dedupe_key := 'workbench:certified-reuse-v2:'
          ||v_session_row.id::text||':'||v_candidate_id::text||':'||v_processed_source_change_seq::text;

        INSERT INTO public.banking_pay_workbench_jobs AS reuse_job(
          id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,
          dedupe_key,snapshot_run_id,session_id,candidate_id,payload_json,
          created_at_utc,updated_at_utc,started_at_utc,completed_at_utc,
          failed_at_utc,last_error_json
        ) VALUES (
          gen_random_uuid(),'WORKBENCH_SESSION_CLONE_REBASE','QUEUED',42,v_now,0,8,
          v_reuse_dedupe_key,v_session_row.source_snapshot_run_id,v_session_row.id,v_candidate_id,
          jsonb_strip_nulls(jsonb_build_object(
            'job_type','WORKBENCH_SESSION_CLONE_REBASE',
            'source_session_id',v_reuse_source_session_id::text,
            'target_session_id',v_session_row.id::text,
            'direct_candidate_id',v_candidate_id::text,
            'source_change_seq',v_processed_source_change_seq,
            'source_job_id',p_job_id::text,
            'source_selection_authorised',true,
            'allow_session_rebase',true,
            'rebase_simple_rows_only',true,
            'clone_mode','CERTIFIED_ONLY',
            'cursor_json','{}'::jsonb,
            'limit',1,
            'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
          )),
          v_now,v_now,NULL,NULL,NULL,NULL
        )
        ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED','RUNNING')
        DO UPDATE SET
          run_at_utc=LEAST(reuse_job.run_at_utc,EXCLUDED.run_at_utc),
          payload_json=coalesce(reuse_job.payload_json,'{}'::jsonb)||EXCLUDED.payload_json,
          updated_at_utc=v_now
        RETURNING reuse_job.id INTO v_reuse_job_id;

        UPDATE public.banking_pay_workbench_session_scope AS reuse_scope
        SET status='PENDING',dirty=true,pending_job_id=v_reuse_job_id,
            error_json=NULL::jsonb,
            certified_preview_publication_required=true,
            certified_preview_publication_parity_ok=false,
            certified_preview_publication_session_version=NULL,
            certified_preview_publication_source_change_seq=NULL,
            certified_preview_publication_source_build_run_id=NULL,
            certified_preview_publication_source_publication_id=NULL,
            certified_preview_publication_attestation_json='{}'::jsonb,
            certified_preview_publication_attested_at_utc=NULL,
            updated_at_utc=v_now
        WHERE reuse_scope.session_id=v_session_row.id
          AND reuse_scope.candidate_id=v_candidate_id;

        v_jobs_queued:=v_jobs_queued+1;
        v_session_count:=v_session_count+1;
        v_refresh_result:=jsonb_build_object(
          'ok',true,
          'job_id',v_reuse_job_id::text,
          'job_type','WORKBENCH_SESSION_CLONE_REBASE',
          'scope_status','PENDING',
          'source_build_required',false,
          'certified_reuse_pending',true
        );
        CONTINUE;
      END IF;
    END IF;


    IF v_is_authorise_delta_targeted AND NOT v_new_scope_baseline_required THEN
      SELECT public.pay_workbench_authorise_delta_hotkey_preflight(
        p_session_id => v_session_row.id,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_canonical_targeted_timesheet_ids,
        p_linked_timesheet_ids => v_canonical_linked_timesheet_ids,
        p_payload_json => public._pay_workbench_merge_targeted_scope_payload(
          v_payload,
          jsonb_build_object(
            'session_id', v_session_row.id::text,
            'source_session_id', v_session_row.id::text,
            'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
            'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
            'session_version', COALESCE(v_session_row.version, 0),
            'session_signature', v_session_row.session_signature,
            'refresh_scope_kind', 'TARGETED_TIMESHEETS',
            'projection_mode', 'DELTA',
            'projection_class', 'TIMESHEET_LIFECYCLE',
            'targeted_timesheet_ids', COALESCE(v_family_scope_json->'targeted_timesheet_ids', to_jsonb(v_canonical_targeted_timesheet_ids)),
            'linked_timesheet_ids', COALESCE(v_family_scope_json->'linked_timesheet_ids', to_jsonb(v_canonical_linked_timesheet_ids)),
            'queue_identity_targeted_timesheet_ids', COALESCE(v_family_scope_json->'queue_identity_targeted_timesheet_ids', v_family_scope_json->'family_timesheet_ids', to_jsonb(v_canonical_targeted_timesheet_ids)),
            'queue_identity_linked_timesheet_ids', COALESCE(v_family_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
            'requested_timesheet_ids', COALESCE(v_family_scope_json->'requested_timesheet_ids', to_jsonb(v_targeted_timesheet_ids)),
            'targeted_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids)),
            'linked_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_linked_timesheet_ids', to_jsonb(v_linked_timesheet_ids)),
            'source_change_seq', v_processed_source_change_seq,
            'latest_source_change_seq', v_processed_source_change_seq,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        p_reason => v_reason,
        p_actor_user_id => v_session_row.actor_user_id,
        p_source_change_seq => v_processed_source_change_seq
      ) INTO v_preflight_result;

      v_preflight_action := COALESCE(v_preflight_result->>'action', 'PROCEED');

      IF v_preflight_action IN ('REUSED_QUEUED_SAME_FAMILY_JOB', 'UPDATED_WAITING_AFTER_RUNNING_JOB') THEN
        v_preflight_match_count := v_preflight_match_count + 1;
        v_dirty_marking_skipped := true;
        v_jobs_queued := v_jobs_queued + CASE WHEN NULLIF(BTRIM(COALESCE(v_preflight_result->>'job_id', '')), '') IS NOT NULL THEN 1 ELSE 0 END;
        v_session_count := v_session_count + 1;

        UPDATE public.banking_pay_workbench_jobs AS job_update
        SET payload_json = public._pay_workbench_dirty_payload_merge(
              COALESCE(job_update.payload_json, '{}'::jsonb),
              jsonb_build_object(
                'processed_source_change_seq', v_processed_source_change_seq,
                'processed_at_utc', v_now::text,
                'processed_candidate_id', v_candidate_id::text,
                'source_change_seq', v_processed_source_change_seq,
                'source_change_sequence', v_processed_source_change_seq,
                'latest_source_change_seq', v_processed_source_change_seq,
                'rerun_required', false,
                'has_more', false,
                'cursor_json', '{}'::jsonb,
                'next_cursor_json', '{}'::jsonb,
                'dirty_apply_row_marking_applied', false,
                'dirty_marking_skipped', true,
                'session_progress_dirtying_skipped', true,
                'classifier_work_skipped', true,
                'source_rows_marking_skipped', true,
                'line_work_marking_skipped', true,
                'preview_marking_skipped', true,
                'early_preflight_action', v_preflight_action,
                'early_preflight_result', COALESCE(v_preflight_result, '{}'::jsonb),
                'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_preflight_result->>'job_id', '')), ''),
                'actual_refresh_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
                'actual_refresh_scope_status', 'DELTA_REFRESH_PENDING',
                'source_build_required', false,
                'delta_refresh_required', true,
                'line_work_action', 'EARLY_PREFLIGHT_SKIPPED_DIRTY_MARKING',
                'early_preflight_completed_cleanly', true,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              )
            ),
            updated_at_utc = v_now
        WHERE job_update.id = p_job_id;

        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'early_preflight_return_before_dirty_marking', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'session_id', v_session_row.id::text, 'action', v_preflight_action, 'normalised_delta_family_key', v_preflight_result->>'normalised_delta_family_key', 'dirty_marking_skipped', true));

        CONTINUE;
      END IF;
    END IF;

    -- Resolve the current economic owner before making public source/preview
    -- rows dirty.  This allows delayed finance/batch/candidate dirty events to
    -- be absorbed by an already-active or already-published full authority.
    v_refresh_payload_json := public._pay_workbench_merge_targeted_scope_payload(
      v_payload,
      jsonb_build_object(
        'session_id', v_session_row.id::text,
        'source_session_id', v_session_row.id::text,
        'source_snapshot_run_id', v_session_row.source_snapshot_run_id::text,
        'snapshot_run_id', v_session_row.source_snapshot_run_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'pay_channel_scope', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_session_row.filters_json->>'pay_channel_scope', v_session_row.filters_json#>>'{filters,pay_channel_scope}', ''))), ''), 'ALL'),
        'refresh_scope_kind', v_refresh_scope_kind,
        'targeted_timesheet_ids', COALESCE(to_jsonb(v_canonical_targeted_timesheet_ids), '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(to_jsonb(v_canonical_linked_timesheet_ids), '[]'::jsonb),
        'finance_case_ids', COALESCE(to_jsonb(v_finance_case_ids), '[]'::jsonb),
        'dependency_closure', COALESCE(v_dependency_closure_json, '{}'::jsonb),
        'dependency_closure_fallback_reason', v_dependency_closure_reason,
        'requested_timesheet_ids', COALESCE(v_family_scope_json->'requested_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb)),
        'targeted_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb)),
        'linked_timesheet_ids_requested', COALESCE(v_family_scope_json->'requested_linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb)),
        'family_timesheet_ids', COALESCE(v_family_scope_json->'family_timesheet_ids', COALESCE(to_jsonb(v_family_timesheet_ids), '[]'::jsonb)),
        'source_rows_marked_dirty_count', 0,
        'line_work_marked_pending_count', 0,
        'preview_rows_marked_dirty_count', 0,
        'source_change_seq', v_processed_source_change_seq,
        'latest_source_change_seq', v_processed_source_change_seq,
        'force_legacy', false,
        'force_broad_legacy', false,
        'new_scope_baseline_required',v_new_scope_baseline_required,
        'source_dirty_job_id', p_job_id::text,
        'enqueue_origin', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );

    SELECT public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id => v_session_row.source_snapshot_run_id,
      p_candidate_id => v_candidate_id,
      p_reason => v_reason,
      p_actor_user_id => v_session_row.actor_user_id,
      p_payload_json => v_refresh_payload_json
    )
    INTO v_refresh_result;

    IF lower(BTRIM(COALESCE(v_refresh_result->>'coalesced', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_dirty_marking_skipped := true;
      v_session_count := v_session_count + 1;
      IF UPPER(BTRIM(COALESCE(v_refresh_result->>'owner_resolution', ''))) = 'COMPLETE_CURRENT_AUTHORITY' THEN
        v_jobs_coalesced_complete := v_jobs_coalesced_complete + 1;
      ELSE
        v_jobs_coalesced_active := v_jobs_coalesced_active + 1;
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS coalesced_dirty_job
      SET payload_json = public._pay_workbench_dirty_payload_merge(
            COALESCE(coalesced_dirty_job.payload_json, '{}'::jsonb),
            jsonb_build_object(
              'processed_source_change_seq', v_processed_source_change_seq,
              'processed_at_utc', v_now::text,
              'processed_candidate_id', v_candidate_id::text,
              'dirty_apply_row_marking_applied', false,
              'dirty_marking_skipped', true,
              'coalesced_to_current_refresh_authority', true,
              'coalesced_owner_resolution', v_refresh_result->>'owner_resolution',
              'coalesced_owner_build_id', v_refresh_result->>'owner_build_id',
              'coalesced_owner_root_job_id', v_refresh_result->>'owner_root_job_id',
              'refresh_enqueue_result', v_refresh_result,
              'rerun_required', false,
              'has_more', false,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ),
          updated_at_utc = v_now
      WHERE coalesced_dirty_job.id = p_job_id;

      CONTINUE;
    END IF;

    UPDATE public.banking_pay_workbench_session_scope AS scope_row
    SET status = COALESCE(NULLIF(BTRIM(v_refresh_result->>'scope_status'), ''), 'SOURCE_BUILD_PENDING'),
        dirty = true,
        error_json = NULL::jsonb,
        certified_preview_publication_required = true,
        certified_preview_publication_parity_ok = false,
        certified_preview_publication_session_version = NULL,
        certified_preview_publication_source_change_seq = NULL,
        certified_preview_publication_source_build_run_id = NULL,
        certified_preview_publication_source_publication_id = NULL,
        certified_preview_publication_attestation_json = '{}'::jsonb,
        certified_preview_publication_attested_at_utc = NULL,
        updated_at_utc = v_now
    WHERE scope_row.session_id = v_session_row.id
      AND scope_row.candidate_id = v_candidate_id;
    GET DIAGNOSTICS v_scope_updated = ROW_COUNT;
    v_scope_count := v_scope_count + COALESCE(v_scope_updated, 0);

    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line
    SET status = 'DIRTY',
        source_row_json = jsonb_strip_nulls(
          COALESCE(source_line.source_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_trigger_table', COALESCE(v_payload->>'trigger_table', 'dirty_apply_worker'),
            'dirty_trigger_operation', COALESCE(v_payload->>'trigger_op', 'APPLY'),
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE source_line.session_id = v_session_row.id
      AND source_line.candidate_id = v_candidate_id
      AND source_line.status = 'CURRENT'
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR source_line.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(source_line.source_row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (source_line.source_row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_source_dirty_count = ROW_COUNT;
    v_source_dirty_total := v_source_dirty_total + COALESCE(v_source_dirty_count, 0);

    UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
    SET status = 'PENDING',
        result_row_json = NULL::jsonb,
        error_json = NULL::jsonb,
        work_payload_json = jsonb_strip_nulls(
          COALESCE(line_work.work_payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE line_work.session_id = v_session_row.id
      AND line_work.candidate_id = v_candidate_id
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR line_work.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(line_work.result_row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (line_work.result_row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_line_work_count = ROW_COUNT;
    v_line_work_total := v_line_work_total + COALESCE(v_line_work_count, 0);

    UPDATE public.banking_pay_workbench_preview_rows AS preview_row
    SET status = 'DIRTY',
        selected = false,
        selection_state = 'DIRTY',
        row_json = jsonb_strip_nulls(
          COALESCE(preview_row.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'source_change_seq', v_processed_source_change_seq,
            'dirty_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        updated_at_utc = v_now
    WHERE preview_row.session_id = v_session_row.id
      AND preview_row.candidate_id = v_candidate_id
      AND (
        v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
        OR preview_row.timesheet_id = ANY(v_all_timesheet_ids)
        OR (
          NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND (preview_row.row_json->>'finance_case_id')::uuid
                = ANY(COALESCE(v_finance_case_ids, ARRAY[]::uuid[]))
        )
      );
    GET DIAGNOSTICS v_preview_count = ROW_COUNT;
    v_preview_total := v_preview_total + COALESCE(v_preview_count, 0);

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET progress_state = 'DIRTY',
        scope_pending_count = GREATEST(COALESCE(session_update.scope_pending_count, 0), 1),
        selected_row_count = GREATEST(COALESCE(session_update.selected_row_count, 0) - COALESCE(v_preview_count, 0), 0),
        candidate_sample_rows_json = jsonb_build_array(jsonb_build_object('candidate_id', v_candidate_id::text, 'status', 'SOURCE_BUILD_PENDING', 'reason', v_reason)),
        progress_json = jsonb_strip_nulls(
          COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_dirty_candidate_id', v_candidate_id::text,
            'last_dirty_reason', v_reason,
            'last_dirty_at_utc', v_now::text,
            'last_dirty_job_id', p_job_id::text,
            'last_dirty_source_change_seq', v_processed_source_change_seq,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          )
        ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = v_session_row.id;
    v_session_count := v_session_count + 1;

    UPDATE public.banking_pay_workbench_jobs AS refresh_job
    SET payload_json = jsonb_strip_nulls(
          COALESCE(refresh_job.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'source_rows_marked_dirty_count', COALESCE(v_source_dirty_count, 0),
            'line_work_marked_pending_count', COALESCE(v_line_work_count, 0),
            'preview_rows_marked_dirty_count', COALESCE(v_preview_count, 0),
            'dirty_apply_marking_completed_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE COALESCE(v_refresh_result->>'job_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND refresh_job.id = (v_refresh_result->>'job_id')::uuid;

    v_jobs_queued := v_jobs_queued
      + CASE
          WHEN lower(BTRIM(COALESCE(v_refresh_result->>'new_owner_created', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1
          WHEN NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), '') IS NOT NULL
               AND lower(BTRIM(COALESCE(v_refresh_result->>'reused', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN 1
          WHEN COALESCE(v_refresh_result->>'jobs_queued', '') ~ '^-?[0-9]+$' THEN (v_refresh_result->>'jobs_queued')::integer
          ELSE 0
        END;
  END LOOP;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS remaining_session
    WHERE remaining_session.status='OPEN' AND remaining_session.discarded_at_utc IS NULL
      AND v_session_scan_cutoff_created_at IS NOT NULL
      AND (remaining_session.created_at_utc,remaining_session.id)<=
          (v_session_scan_cutoff_created_at,v_session_scan_cutoff_id)
      AND (v_session_scan_last_created_at IS NULL OR
        (remaining_session.created_at_utc,remaining_session.id)>
          (v_session_scan_last_created_at,v_session_scan_last_id))
      AND NOT EXISTS (
        SELECT 1 FROM public.banking_pay_workbench_sessions AS newer_session
        WHERE newer_session.actor_user_id=remaining_session.actor_user_id
          AND newer_session.status='OPEN' AND newer_session.discarded_at_utc IS NULL
          AND (newer_session.pay_date,newer_session.created_at_utc,newer_session.id)>
              (remaining_session.pay_date,remaining_session.created_at_utc,remaining_session.id)
      )
  ) INTO v_session_scan_has_more;

  UPDATE public.banking_pay_workbench_jobs AS job_update
  SET payload_json = public._pay_workbench_dirty_payload_merge(
        COALESCE(job_update.payload_json, '{}'::jsonb),
        jsonb_build_object(
          'processed_source_change_seq', v_processed_source_change_seq,
          'processed_at_utc', v_now::text,
          'processed_candidate_id', v_candidate_id::text,
          'dirty_scope_count', v_scope_count,
          'dirty_line_count', v_line_work_total,
          'dirty_preview_count', v_preview_total,
          'dirty_apply_row_marking_applied', (v_scope_count + v_source_dirty_total + v_line_work_total + v_preview_total) > 0,
          'dirty_marking_skipped', v_dirty_marking_skipped,
          'jobs_coalesced_active', v_jobs_coalesced_active,
          'jobs_coalesced_complete', v_jobs_coalesced_complete,
          'jobs_requeued_for_conflict', v_jobs_requeued_for_conflict,
          'early_preflight_checked', v_is_authorise_delta_targeted,
          'early_preflight_action', COALESCE(v_preflight_action, 'PROCEED'),
          'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), ''),
          'actual_refresh_job_type', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_type', v_refresh_result->>'canonical_job_type', '')), ''),
          'actual_refresh_scope_status', NULLIF(BTRIM(COALESCE(v_refresh_result->>'scope_status', '')), ''),
          'source_build_required', lower(BTRIM(COALESCE(v_refresh_result->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'delta_refresh_required', lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'line_work_action', CASE
            WHEN lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              THEN 'DIRTY_ROW_MARKING_ONLY_DELTA_REFRESH'
            ELSE 'SOURCE_BUILD_OR_LEGACY_REFRESH'
          END,
          'refresh_enqueue_result', COALESCE(v_refresh_result, '{}'::jsonb)
          ,'preinvalidated_scope_reissued',v_preinvalidated_scope_reissued
          ,'effective_scope_change_tx_token',v_effective_scope_change_tx_token::text
          ,'effective_scope_change_generation',v_effective_scope_change_generation
          ,'session_scan_cutoff_created_at_utc',v_session_scan_cutoff_created_at::text
          ,'session_scan_cutoff_id',v_session_scan_cutoff_id::text
          ,'session_scan_last_created_at_utc',CASE WHEN v_session_scan_has_more THEN v_session_scan_last_created_at::text ELSE NULL END
          ,'session_scan_last_id',CASE WHEN v_session_scan_has_more THEN v_session_scan_last_id::text ELSE NULL END
          ,'rerun_required',v_session_scan_has_more
          ,'has_more',v_session_scan_has_more
        )
      ),
      private_cursor_kind=CASE WHEN v_session_scan_has_more THEN 'DIRTY_SESSION_SCAN_V1' ELSE NULL::text END,
      private_stage_version=CASE WHEN v_session_scan_has_more THEN 1 ELSE NULL::integer END,
      private_cursor_json=CASE
        WHEN v_session_scan_has_more THEN jsonb_strip_nulls(jsonb_build_object(
          'scan_source_change_seq',v_processed_source_change_seq,
          'upper_created_at_utc',v_session_scan_cutoff_created_at,
          'upper_session_id',v_session_scan_cutoff_id,
          'last_created_at_utc',v_session_scan_last_created_at,
          'last_session_id',v_session_scan_last_id,
          'scan_started_at_utc',v_session_scan_started_at,
          'sessions_examined',v_session_scan_sessions_examined
        ))
        ELSE '{}'::jsonb
      END,
      updated_at_utc = v_now
  WHERE job_update.id = p_job_id;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'dirty_worker_apply_done', 'job_id', p_job_id::text, 'candidate_id', v_candidate_id::text, 'dirty_scope_count', v_scope_count, 'dirty_source_line_count', v_source_dirty_total, 'dirty_line_count', v_line_work_total, 'dirty_preview_count', v_preview_total, 'jobs_queued', v_jobs_queued, 'processed_source_change_seq', v_processed_source_change_seq, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    'candidate_id', v_candidate_id::text,
    'refresh_scope_kind', v_refresh_scope_kind,
    'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
    'family_timesheet_count', v_reported_family_timesheet_count,
    'finance_case_count', COALESCE(array_length(v_finance_case_ids, 1), 0),
    'dependency_closure', COALESCE(v_dependency_closure_json, '{}'::jsonb),
    'dirty_scope_count', v_scope_count,
    'dirty_source_line_count', v_source_dirty_total,
    'dirty_line_count', v_line_work_total,
    'dirty_preview_count', v_preview_total,
    'sessions_touched', v_session_count,
    'jobs_queued', v_jobs_queued,
    'jobs_coalesced_active', v_jobs_coalesced_active,
    'jobs_coalesced_complete', v_jobs_coalesced_complete,
    'jobs_requeued_for_conflict', v_jobs_requeued_for_conflict,
    'processed_source_change_seq', v_processed_source_change_seq,
    'dirty_apply_row_marking_applied', (v_scope_count + v_source_dirty_total + v_line_work_total + v_preview_total) > 0,
    'dirty_marking_skipped', v_dirty_marking_skipped,
    'early_preflight_checked', v_is_authorise_delta_targeted,
    'early_preflight_action', COALESCE(v_preflight_action, 'PROCEED'),
    'actual_refresh_job_id', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_id', '')), ''),
    'actual_refresh_job_type', NULLIF(BTRIM(COALESCE(v_refresh_result->>'job_type', v_refresh_result->>'canonical_job_type', '')), ''),
    'actual_refresh_scope_status', NULLIF(BTRIM(COALESCE(v_refresh_result->>'scope_status', '')), ''),
    'source_build_required', lower(BTRIM(COALESCE(v_refresh_result->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'delta_refresh_required', lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'line_work_action', CASE
      WHEN lower(BTRIM(COALESCE(v_refresh_result->>'delta_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        THEN 'DIRTY_ROW_MARKING_ONLY_DELTA_REFRESH'
      ELSE 'SOURCE_BUILD_OR_LEGACY_REFRESH'
    END,
    'refresh_enqueue_result', COALESCE(v_refresh_result, '{}'::jsonb),
    'preinvalidated_scope_reissued', v_preinvalidated_scope_reissued,
    'effective_scope_change_tx_token', v_effective_scope_change_tx_token::text,
    'effective_scope_change_generation', v_effective_scope_change_generation,
    'more_due', v_session_scan_has_more,
    'has_more', v_session_scan_has_more,
    'made_progress', true,
    'dirty_apply_complete', NOT v_session_scan_has_more,
    'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_candidate_dirty_cohort_stage_v1(
  uuid, uuid, timestamptz
) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_candidate_dirty_cohort_stage_v1(
  uuid, uuid, timestamptz
) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_candidate_dirty_cohort_stage_v1(
  uuid, uuid, timestamptz
) TO postgres;

ALTER FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(
  uuid, integer
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(
  uuid, integer
) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(
  uuid, integer
) TO postgres, service_role;

NOTIFY pgrst, 'reload schema';

commit;
