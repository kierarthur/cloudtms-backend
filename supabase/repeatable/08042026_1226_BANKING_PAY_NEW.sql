BEGIN;

DROP FUNCTION IF EXISTS public.pay_preview(
  date,
  date,
  uuid,
  uuid,
  uuid,
  jsonb
);

COMMIT;

CREATE OR REPLACE FUNCTION public.pay_workbench_snapshot_ensure_run(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_today_uk date := (now() AT TIME ZONE 'Europe/London')::date;
  v_pay_eligibility_months_back integer := 6;
  v_pay_eligibility_weeks_ahead integer := 2;
  v_pay_week_start date;
  v_eligibility_from_date date;
  v_eligibility_to_date date;
  v_snapshot_run_id uuid;
  v_status text;
  v_is_active boolean;
BEGIN
  IF p_pay_date IS NULL THEN
    RAISE EXCEPTION 'pay_date is required';
  END IF;

  IF p_week_ending_cutoff IS NULL THEN
    RAISE EXCEPTION 'week_ending_cutoff is required';
  END IF;

  v_pay_week_start := public._pay_week_start_monday(p_pay_date);

  BEGIN
    SELECT
      sd.pay_eligibility_months_back,
      sd.pay_eligibility_weeks_ahead
    INTO
      v_pay_eligibility_months_back,
      v_pay_eligibility_weeks_ahead
    FROM public.settings_defaults sd
    ORDER BY sd.id ASC
    LIMIT 1;
  EXCEPTION
    WHEN undefined_column THEN
      v_pay_eligibility_months_back := 6;
      v_pay_eligibility_weeks_ahead := 2;
  END;

  v_pay_eligibility_months_back := GREATEST(0, LEAST(COALESCE(v_pay_eligibility_months_back, 6), 120));
  v_pay_eligibility_weeks_ahead := GREATEST(0, LEAST(COALESCE(v_pay_eligibility_weeks_ahead, 2), 52));

  v_eligibility_from_date := (v_today_uk - (v_pay_eligibility_months_back::text || ' months')::interval)::date;
  v_eligibility_to_date := (v_today_uk + (v_pay_eligibility_weeks_ahead::text || ' weeks')::interval)::date;

  SELECT
    sr.id,
    sr.status,
    sr.is_active
  INTO
    v_snapshot_run_id,
    v_status,
    v_is_active
  FROM public.banking_pay_snapshot_runs sr
  WHERE sr.pay_date = p_pay_date
    AND sr.week_ending_cutoff = p_week_ending_cutoff
    AND sr.is_active = true
  ORDER BY sr.created_at_utc DESC, sr.id DESC
  LIMIT 1;

  IF v_snapshot_run_id IS NULL THEN
    BEGIN
      INSERT INTO public.banking_pay_snapshot_runs (
        pay_date,
        week_ending_cutoff,
        pay_week_start,
        eligibility_from_date,
        eligibility_to_date,
        status,
        is_active,
        summary_json,
        paye_guardrails_json,
        created_at_utc,
        updated_at_utc,
        ready_at_utc,
        failed_at_utc,
        last_error_json
      )
      VALUES (
        p_pay_date,
        p_week_ending_cutoff,
        v_pay_week_start,
        v_eligibility_from_date,
        v_eligibility_to_date,
        'OPEN',
        true,
        '{}'::jsonb,
        '{}'::jsonb,
        v_now,
        v_now,
        NULL,
        NULL,
        NULL
      )
      RETURNING
        public.banking_pay_snapshot_runs.id,
        public.banking_pay_snapshot_runs.status,
        public.banking_pay_snapshot_runs.is_active
      INTO
        v_snapshot_run_id,
        v_status,
        v_is_active;
    EXCEPTION
      WHEN unique_violation THEN
        SELECT
          sr.id,
          sr.status,
          sr.is_active
        INTO
          v_snapshot_run_id,
          v_status,
          v_is_active
        FROM public.banking_pay_snapshot_runs sr
        WHERE sr.pay_date = p_pay_date
          AND sr.week_ending_cutoff = p_week_ending_cutoff
          AND sr.is_active = true
        ORDER BY sr.created_at_utc DESC, sr.id DESC
        LIMIT 1;
      WHEN exclusion_violation THEN
        SELECT
          sr.id,
          sr.status,
          sr.is_active
        INTO
          v_snapshot_run_id,
          v_status,
          v_is_active
        FROM public.banking_pay_snapshot_runs sr
        WHERE sr.pay_date = p_pay_date
          AND sr.week_ending_cutoff = p_week_ending_cutoff
          AND sr.is_active = true
        ORDER BY sr.created_at_utc DESC, sr.id DESC
        LIMIT 1;
    END;
  END IF;

  IF v_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'Unable to create or reuse active banking pay snapshot run for pay_date % and cutoff %', p_pay_date, p_week_ending_cutoff;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'snapshot_run_id', v_snapshot_run_id::text,
    'pay_date', p_pay_date::text,
    'week_ending_cutoff', p_week_ending_cutoff::text,
    'pay_week_start', v_pay_week_start::text,
    'eligibility_from_date', v_eligibility_from_date::text,
    'eligibility_to_date', v_eligibility_to_date::text,
    'status', v_status,
    'is_active', v_is_active,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
    'created_or_reused_at_utc', v_now
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_workbench_snapshot_enqueue_scope(
  p_snapshot_run_id uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_client_id uuid DEFAULT NULL::uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_run_pay_date date;
  v_run_week_ending_cutoff date;
  v_run_pay_week_start date;
  v_run_eligibility_from_date date;
  v_run_eligibility_to_date date;
  v_candidate_id uuid;
  v_enqueued_count integer := 0;
  v_candidate_ids jsonb := '[]'::jsonb;
BEGIN
  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  SELECT
    sr.pay_date,
    sr.week_ending_cutoff,
    sr.pay_week_start,
    sr.eligibility_from_date,
    sr.eligibility_to_date
  INTO
    v_run_pay_date,
    v_run_week_ending_cutoff,
    v_run_pay_week_start,
    v_run_eligibility_from_date,
    v_run_eligibility_to_date
  FROM public.banking_pay_snapshot_runs sr
  WHERE sr.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  FOR v_candidate_id IN
    WITH timesheet_scope AS (
      SELECT DISTINCT
        tf.candidate_id
      FROM public.timesheets_financials tf
      JOIN public.timesheets ts
        ON ts.timesheet_id = tf.timesheet_id
      JOIN public.candidates c
        ON c.id = tf.candidate_id
      LEFT JOIN public.timesheet_pay_state tps
        ON tps.timesheet_id = tf.timesheet_id
      WHERE tf.is_current = true
        AND COALESCE(tf.pay_on_hold, false) = false
        AND UPPER(COALESCE(tf.processing_status::text, '')) NOT IN ('UNASSIGNED', 'CLIENT_UNRESOLVED', 'RATE_MISSING', 'PAY_CHANNEL_MISSING')
        AND UPPER(COALESCE(c.pay_method, '')) IN ('PAYE', 'UMBRELLA')
        AND (
          (
            ts.authorised_at_server IS NOT NULL
            AND ts.week_ending_date::date >= v_run_eligibility_from_date
            AND ts.week_ending_date::date <= v_run_eligibility_to_date
            AND ts.week_ending_date::date <= v_run_week_ending_cutoff
          )
          OR (
            tps.last_settled_snapshot_json IS NOT NULL
            AND ts.week_ending_date::date >= v_run_eligibility_from_date
            AND ts.week_ending_date::date <= v_run_week_ending_cutoff
          )
        )
        AND (p_candidate_id IS NULL OR tf.candidate_id = p_candidate_id)
        AND (p_client_id IS NULL OR tf.client_id = p_client_id)
    ),
    finance_scope AS (
      SELECT DISTINCT
        vfcr.candidate_id
      FROM public.v_finance_cases_register vfcr
      WHERE vfcr.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'OVERPAYMENT'::public.pay_finance_case_type_enum,
        'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
        AND UPPER(COALESCE(vfcr.status::text, '')) = 'ACTIVE'
        AND COALESCE(vfcr.outstanding_amount, 0) > 0
        AND NOT (vfcr.active_snooze_id IS NOT NULL AND vfcr.active_snooze_until_date IS NULL)
        AND (
          (
            vfcr.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
            AND (
              UPPER(COALESCE(vfcr.payout_status::text, '')) <> 'PAID'
              OR vfcr.next_due_week_start IS NULL
              OR vfcr.next_due_week_start <= v_run_pay_week_start
            )
          )
          OR (
            vfcr.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
            AND (
              vfcr.next_due_week_start IS NULL
              OR vfcr.next_due_week_start <= v_run_pay_week_start
            )
          )
          OR (
            vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
            AND (
              vfcr.next_due_week_start IS NULL
              OR vfcr.next_due_week_start <= v_run_pay_week_start
            )
          )
          OR vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        )
        AND (p_candidate_id IS NULL OR vfcr.candidate_id = p_candidate_id)
        AND (p_client_id IS NULL OR vfcr.client_id = p_client_id)
    ),
    scope_candidates AS (
      SELECT ts_scope.candidate_id
      FROM timesheet_scope ts_scope
      UNION
      SELECT fin_scope.candidate_id
      FROM finance_scope fin_scope
    ),
    candidate_change_state AS (
      SELECT
        sc.candidate_id,
        COALESCE(acc.seq, 0) AS live_change_seq,
        snap.status AS snapshot_status,
        COALESCE(snap.source_change_seq, 0) AS snapshot_change_seq
      FROM scope_candidates sc
      LEFT JOIN public.app_change_counters acc
        ON acc.entity_key = 'pay_candidate:' || sc.candidate_id::text
      LEFT JOIN public.banking_pay_snapshot_candidate_state snap
        ON snap.snapshot_run_id = p_snapshot_run_id
       AND snap.candidate_id = sc.candidate_id
    )
    SELECT
      ccs.candidate_id
    FROM candidate_change_state ccs
    WHERE ccs.snapshot_status IS DISTINCT FROM 'READY'
       OR ccs.snapshot_change_seq < ccs.live_change_seq
    ORDER BY ccs.candidate_id
  LOOP
    PERFORM public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id => p_snapshot_run_id,
      p_candidate_id => v_candidate_id,
      p_reason => 'SNAPSHOT_SCOPE_WARMUP',
      p_actor_user_id => p_actor_user_id,
      p_payload_json => jsonb_build_object(
        'scope_pay_date', v_run_pay_date::text,
        'scope_week_ending_cutoff', v_run_week_ending_cutoff::text,
        'scope_candidate_filter_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
        'scope_client_filter_id', CASE WHEN p_client_id IS NULL THEN NULL ELSE p_client_id::text END
      )
    );

    v_enqueued_count := v_enqueued_count + 1;
    v_candidate_ids := v_candidate_ids || jsonb_build_array(v_candidate_id::text);
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'snapshot_run_id', p_snapshot_run_id::text,
    'pay_date', v_run_pay_date::text,
    'week_ending_cutoff', v_run_week_ending_cutoff::text,
    'pay_week_start', v_run_pay_week_start::text,
    'eligibility_from_date', v_run_eligibility_from_date::text,
    'eligibility_to_date', v_run_eligibility_to_date::text,
    'candidate_filter_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'client_filter_id', CASE WHEN p_client_id IS NULL THEN NULL ELSE p_client_id::text END,
    'enqueued_count', v_enqueued_count,
    'candidate_ids', v_candidate_ids
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(
  p_snapshot_run_id uuid,
  p_candidate_id uuid,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_live_change_seq bigint := 0;
  v_dedupe_key text;
  v_job_id uuid;
BEGIN
  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.banking_pay_snapshot_runs sr
  WHERE sr.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  PERFORM 1
  FROM public.candidates c
  WHERE c.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  SELECT COALESCE(acc.seq, 0)
  INTO v_live_change_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_dedupe_key := 'SNAPSHOT_CANDIDATE_REFRESH:' || p_snapshot_run_id::text || ':' || p_candidate_id::text || ':s' || v_live_change_seq::text;

  INSERT INTO public.banking_pay_workbench_jobs (
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    candidate_id,
    payload_json,
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    'SNAPSHOT_CANDIDATE_REFRESH',
    'QUEUED',
    50,
    v_now,
    0,
    8,
    v_dedupe_key,
    p_snapshot_run_id,
    NULL,
    p_candidate_id,
    COALESCE(p_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'reason', p_reason,
        'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', v_live_change_seq,
        'job_type', 'SNAPSHOT_CANDIDATE_REFRESH'
      ),
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
      updated_at_utc = v_now
  RETURNING public.banking_pay_workbench_jobs.id
  INTO v_job_id;

  INSERT INTO public.banking_pay_snapshot_candidate_state (
    snapshot_run_id,
    candidate_id,
    status,
    candidate_fragment_json,
    summary_fragment_json,
    paye_candidate_json,
    non_paye_payee_json,
    payees_json,
    case_resolution_states_json,
    canonical_preview_lines_json,
    source_change_seq,
    created_at_utc,
    updated_at_utc,
    last_refreshed_at_utc,
    last_error_json
  )
  VALUES (
    p_snapshot_run_id,
    p_candidate_id,
    'PENDING',
    '{}'::jsonb,
    '{}'::jsonb,
    NULL,
    NULL,
    '[]'::jsonb,
    '[]'::jsonb,
    '[]'::jsonb,
    v_live_change_seq,
    v_now,
    v_now,
    NULL,
    NULL
  )
  ON CONFLICT (snapshot_run_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = GREATEST(public.banking_pay_snapshot_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
      updated_at_utc = v_now,
      last_error_json = NULL;

  UPDATE public.banking_pay_workbench_session_candidate_state scs
  SET status = 'PENDING',
      source_change_seq = GREATEST(scs.source_change_seq, v_live_change_seq),
      pending_job_id = v_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL
  FROM public.banking_pay_workbench_sessions ws
  WHERE ws.id = scs.session_id
    AND ws.status = 'OPEN'
    AND ws.source_snapshot_run_id = p_snapshot_run_id
    AND scs.candidate_id = p_candidate_id
    AND ws.scope_candidate_ids @> ARRAY[p_candidate_id]::uuid[];

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
    'snapshot_run_id', p_snapshot_run_id::text,
    'candidate_id', p_candidate_id::text,
    'source_change_seq', v_live_change_seq,
    'dedupe_key', v_dedupe_key,
    'reason', p_reason
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_session_candidate_refresh(
  p_session_id uuid,
  p_candidate_id uuid,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_version bigint;
  v_source_snapshot_run_id uuid;
  v_session_status text;
  v_source_change_seq bigint := 0;
  v_dedupe_key text;
  v_job_id uuid;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  SELECT
    ws.version,
    ws.source_snapshot_run_id,
    ws.status
  INTO
    v_session_version,
    v_source_snapshot_run_id,
    v_session_status
  FROM public.banking_pay_workbench_sessions ws
  WHERE ws.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  IF v_session_status <> 'OPEN' THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not OPEN', p_session_id;
  END IF;

  PERFORM 1
  FROM public.banking_pay_workbench_sessions ws
  WHERE ws.id = p_session_id
    AND ws.scope_candidate_ids @> ARRAY[p_candidate_id]::uuid[];

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %', p_candidate_id, p_session_id;
  END IF;

  SELECT COALESCE(snap.source_change_seq, 0)
  INTO v_source_change_seq
  FROM public.banking_pay_snapshot_candidate_state snap
  WHERE snap.snapshot_run_id = v_source_snapshot_run_id
    AND snap.candidate_id = p_candidate_id;

  IF v_source_change_seq = 0 THEN
    SELECT COALESCE(acc.seq, 0)
    INTO v_source_change_seq
    FROM public.app_change_counters acc
    WHERE acc.entity_key = 'pay_candidate:' || p_candidate_id::text;
  END IF;

  v_dedupe_key := 'SESSION_CANDIDATE_RECOMPUTE:' || p_session_id::text || ':' || p_candidate_id::text || ':v' || v_session_version::text || ':s' || v_source_change_seq::text;

  INSERT INTO public.banking_pay_workbench_jobs (
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    candidate_id,
    payload_json,
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    'SESSION_CANDIDATE_RECOMPUTE',
    'QUEUED',
    10,
    v_now,
    0,
    8,
    v_dedupe_key,
    v_source_snapshot_run_id,
    p_session_id,
    p_candidate_id,
    COALESCE(p_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'reason', p_reason,
        'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', v_session_version,
        'source_snapshot_run_id', CASE WHEN v_source_snapshot_run_id IS NULL THEN NULL ELSE v_source_snapshot_run_id::text END,
        'source_change_seq', v_source_change_seq,
        'job_type', 'SESSION_CANDIDATE_RECOMPUTE'
      ),
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
      updated_at_utc = v_now
  RETURNING public.banking_pay_workbench_jobs.id
  INTO v_job_id;

  INSERT INTO public.banking_pay_workbench_session_candidate_state (
    session_id,
    candidate_id,
    status,
    effective_candidate_fragment_json,
    effective_summary_fragment_json,
    effective_paye_candidate_json,
    effective_non_paye_payee_json,
    effective_payees_json,
    effective_case_resolution_states_json,
    effective_canonical_preview_lines_json,
    source_change_seq,
    session_version,
    pending_job_id,
    created_at_utc,
    updated_at_utc,
    last_recomputed_at_utc,
    last_error_json
  )
  VALUES (
    p_session_id,
    p_candidate_id,
    'PENDING',
    '{}'::jsonb,
    '{}'::jsonb,
    NULL,
    NULL,
    '[]'::jsonb,
    '[]'::jsonb,
    '[]'::jsonb,
    v_source_change_seq,
    v_session_version,
    v_job_id,
    v_now,
    v_now,
    NULL,
    NULL
  )
  ON CONFLICT (session_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = GREATEST(public.banking_pay_workbench_session_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
      session_version = EXCLUDED.session_version,
      pending_job_id = v_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL;

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', 'SESSION_CANDIDATE_RECOMPUTE',
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'session_version', v_session_version,
    'source_snapshot_run_id', CASE WHEN v_source_snapshot_run_id IS NULL THEN NULL ELSE v_source_snapshot_run_id::text END,
    'source_change_seq', v_source_change_seq,
    'dedupe_key', v_dedupe_key,
    'reason', p_reason
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_payee_readiness_ensure(
  p_candidate_id uuid,
  p_payees_json jsonb,
  p_snapshot_run_id uuid DEFAULT NULL::uuid,
  p_session_id uuid DEFAULT NULL::uuid,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_source_change_seq bigint := 0;
  v_session_version bigint := NULL;
  v_resolved_snapshot_run_id uuid := p_snapshot_run_id;
  v_dedupe_key text;
  v_job_id uuid;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.candidates c
  WHERE c.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  IF p_payees_json IS NULL THEN
    RAISE EXCEPTION 'payees_json is required';
  END IF;

  IF jsonb_typeof(p_payees_json) <> 'array' THEN
    RAISE EXCEPTION 'payees_json must be a JSON array';
  END IF;

  IF p_session_id IS NOT NULL THEN
    SELECT
      ws.source_snapshot_run_id,
      ws.version
    INTO
      v_resolved_snapshot_run_id,
      v_session_version
    FROM public.banking_pay_workbench_sessions ws
    WHERE ws.id = p_session_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
    END IF;
  END IF;

  IF v_resolved_snapshot_run_id IS NOT NULL THEN
    SELECT COALESCE(snap.source_change_seq, 0)
    INTO v_source_change_seq
    FROM public.banking_pay_snapshot_candidate_state snap
    WHERE snap.snapshot_run_id = v_resolved_snapshot_run_id
      AND snap.candidate_id = p_candidate_id;
  END IF;

  IF v_source_change_seq = 0 THEN
    SELECT COALESCE(acc.seq, 0)
    INTO v_source_change_seq
    FROM public.app_change_counters acc
    WHERE acc.entity_key = 'pay_candidate:' || p_candidate_id::text;
  END IF;

  v_dedupe_key := 'PAYEE_READINESS_ENSURE:'
    || COALESCE(v_resolved_snapshot_run_id::text, 'no-run')
    || ':'
    || COALESCE(p_session_id::text, 'no-session')
    || ':'
    || p_candidate_id::text
    || ':s'
    || v_source_change_seq::text
    || ':'
    || md5(COALESCE(p_payees_json::text, '[]'));

  INSERT INTO public.banking_pay_workbench_jobs (
    job_type,
    status,
    priority,
    run_at_utc,
    attempt_count,
    max_attempts,
    dedupe_key,
    snapshot_run_id,
    session_id,
    candidate_id,
    payload_json,
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    'PAYEE_READINESS_ENSURE',
    'QUEUED',
    100,
    v_now,
    0,
    8,
    v_dedupe_key,
    v_resolved_snapshot_run_id,
    p_session_id,
    p_candidate_id,
    COALESCE(p_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'reason', p_reason,
        'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'candidate_id', p_candidate_id::text,
        'snapshot_run_id', CASE WHEN v_resolved_snapshot_run_id IS NULL THEN NULL ELSE v_resolved_snapshot_run_id::text END,
        'session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
        'session_version', v_session_version,
        'source_change_seq', v_source_change_seq,
        'payees_json', p_payees_json,
        'job_type', 'PAYEE_READINESS_ENSURE'
      ),
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
      updated_at_utc = v_now
  RETURNING public.banking_pay_workbench_jobs.id
  INTO v_job_id;

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', 'PAYEE_READINESS_ENSURE',
    'snapshot_run_id', CASE WHEN v_resolved_snapshot_run_id IS NULL THEN NULL ELSE v_resolved_snapshot_run_id::text END,
    'session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
    'candidate_id', p_candidate_id::text,
    'session_version', v_session_version,
    'source_change_seq', v_source_change_seq,
    'dedupe_key', v_dedupe_key,
    'reason', p_reason
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_claim_due_jobs(
  p_limit integer DEFAULT 25,
  p_now_utc timestamptz DEFAULT NULL::timestamptz
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_limit integer := GREATEST(1, LEAST(COALESCE(p_limit, 25), 500));
  v_now timestamptz := now();
  v_cutoff timestamptz := COALESCE(p_now_utc, v_now);
  v_claimed jsonb := '[]'::jsonb;
  v_claimed_count integer := 0;
BEGIN
  WITH claim AS (
    SELECT
      j.id,
      j.job_type,
      j.priority,
      j.run_at_utc,
      j.attempt_count,
      j.max_attempts,
      j.snapshot_run_id,
      j.session_id,
      j.candidate_id,
      j.payload_json,
      j.created_at_utc
    FROM public.banking_pay_workbench_jobs j
    WHERE j.status = 'QUEUED'
      AND j.run_at_utc <= v_cutoff
    ORDER BY j.priority ASC, j.run_at_utc ASC, j.created_at_utc ASC, j.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT v_limit
  ),
  upd AS (
    UPDATE public.banking_pay_workbench_jobs j2
    SET status = 'RUNNING',
        attempt_count = COALESCE(j2.attempt_count, 0) + 1,
        started_at_utc = COALESCE(j2.started_at_utc, v_now),
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM claim c
    WHERE j2.id = c.id
    RETURNING
      j2.id,
      j2.job_type,
      j2.priority,
      j2.run_at_utc,
      j2.attempt_count,
      j2.max_attempts,
      j2.snapshot_run_id,
      j2.session_id,
      j2.candidate_id,
      j2.payload_json,
      j2.started_at_utc,
      j2.created_at_utc
  )
  SELECT
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'job_id', u.id::text,
          'job_type', u.job_type,
          'priority', u.priority,
          'run_at_utc', u.run_at_utc,
          'attempt_count', u.attempt_count,
          'max_attempts', u.max_attempts,
          'snapshot_run_id', CASE WHEN u.snapshot_run_id IS NULL THEN NULL ELSE u.snapshot_run_id::text END,
          'session_id', CASE WHEN u.session_id IS NULL THEN NULL ELSE u.session_id::text END,
          'candidate_id', CASE WHEN u.candidate_id IS NULL THEN NULL ELSE u.candidate_id::text END,
          'payload_json', u.payload_json,
          'started_at_utc', u.started_at_utc,
          'created_at_utc', u.created_at_utc
        )
        ORDER BY u.priority ASC, u.run_at_utc ASC, u.created_at_utc ASC, u.id ASC
      ),
      '[]'::jsonb
    ),
    COUNT(*)::integer
  INTO
    v_claimed,
    v_claimed_count
  FROM upd u;

  RETURN jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'limit', v_limit,
    'claimed_count', v_claimed_count,
    'claimed', v_claimed
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_complete_job(
  p_job_id uuid,
  p_result_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_status text;
BEGIN
  IF p_job_id IS NULL THEN
    RAISE EXCEPTION 'job_id is required';
  END IF;

  UPDATE public.banking_pay_workbench_jobs j
  SET status = 'SUCCEEDED',
      updated_at_utc = v_now,
      completed_at_utc = v_now,
      failed_at_utc = NULL,
      last_error_json = NULL,
      payload_json = COALESCE(j.payload_json, '{}'::jsonb) || jsonb_build_object('result_json', COALESCE(p_result_json, '{}'::jsonb))
  WHERE j.id = p_job_id
  RETURNING j.status
  INTO v_status;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_jobs row % not found', p_job_id;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'status', 'SUCCEEDED',
    'completed_at_utc', v_now,
    'result_json', COALESCE(p_result_json, '{}'::jsonb)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_fail_job(
  p_job_id uuid,
  p_error_json jsonb,
  p_retry_after_seconds integer DEFAULT NULL::integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_attempt_count integer;
  v_max_attempts integer;
  v_retry_after_seconds integer;
  v_next_run_at_utc timestamptz;
  v_new_status text;
BEGIN
  IF p_job_id IS NULL THEN
    RAISE EXCEPTION 'job_id is required';
  END IF;

  IF p_error_json IS NULL THEN
    RAISE EXCEPTION 'error_json is required';
  END IF;

  SELECT
    j.attempt_count,
    j.max_attempts
  INTO
    v_attempt_count,
    v_max_attempts
  FROM public.banking_pay_workbench_jobs j
  WHERE j.id = p_job_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_jobs row % not found', p_job_id;
  END IF;

  v_retry_after_seconds := GREATEST(
    5,
    LEAST(
      COALESCE(p_retry_after_seconds, GREATEST(30, LEAST(COALESCE(v_attempt_count, 1) * 60, 3600))),
      86400
    )
  );

  IF COALESCE(v_attempt_count, 0) >= COALESCE(v_max_attempts, 8) THEN
    v_new_status := 'DEAD';
    v_next_run_at_utc := NULL;
  ELSE
    v_new_status := 'QUEUED';
    v_next_run_at_utc := v_now + make_interval(secs => v_retry_after_seconds);
  END IF;

  UPDATE public.banking_pay_workbench_jobs j
  SET status = v_new_status,
      run_at_utc = COALESCE(v_next_run_at_utc, j.run_at_utc),
      updated_at_utc = v_now,
      started_at_utc = CASE WHEN v_new_status = 'QUEUED' THEN NULL ELSE j.started_at_utc END,
      completed_at_utc = NULL,
      failed_at_utc = v_now,
      last_error_json = p_error_json,
      payload_json = COALESCE(j.payload_json, '{}'::jsonb) || jsonb_build_object('last_failure_json', p_error_json)
  WHERE j.id = p_job_id;

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'status', v_new_status,
    'attempt_count', v_attempt_count,
    'max_attempts', v_max_attempts,
    'retry_after_seconds', CASE WHEN v_new_status = 'QUEUED' THEN v_retry_after_seconds ELSE NULL END,
    'next_run_at_utc', v_next_run_at_utc,
    'failed_at_utc', v_now,
    'error_json', p_error_json
  );
END;
$function$;
CREATE OR REPLACE FUNCTION public.pay_preview_build_context(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_client_id uuid DEFAULT NULL::uuid,
  p_preview_decisions_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_today_uk date := (now() AT TIME ZONE 'Europe/London')::date;
  v_pay_week_start date;
  v_pay_eligibility_months_back integer := 6;
  v_pay_eligibility_weeks_ahead integer := 2;
  v_eligibility_from_date date;
  v_eligibility_to_date date;
  v_vat_rate_pct numeric;
  v_erni_pct numeric;
  v_rail_provider_default text;
  v_rail_env_default text;
  v_rail_supports_scheduling boolean;
  v_rail_supports_name_check boolean;
  v_rail_supports_auto_execute boolean;
  v_default_schedule_umbrella_local text;
  v_default_schedule_paye_local text;
  v_funds_warning_hours_json jsonb;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_paye_guardrails jsonb := '{}'::jsonb;
  v_scope_candidate_ids jsonb := '[]'::jsonb;
  v_scope_candidate_count integer := 0;
  v_preview_decisions_root jsonb := '{}'::jsonb;
  v_preview_case_resolutions jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_date IS NULL THEN
    RAISE EXCEPTION 'pay_date is required';
  END IF;

  IF p_week_ending_cutoff IS NULL THEN
    RAISE EXCEPTION 'week_ending_cutoff is required';
  END IF;

  IF to_regclass('public.settings_finance_windows') IS NULL THEN
    RAISE EXCEPTION 'settings_finance_windows missing';
  END IF;

  v_pay_week_start := public._pay_week_start_monday(p_pay_date);

  SELECT
    public.settings_finance_windows.vat_rate_pct,
    public.settings_finance_windows.erni_pct
  INTO
    v_vat_rate_pct,
    v_erni_pct
  FROM public.settings_finance_windows
  WHERE p_pay_date >= public.settings_finance_windows.date_from
    AND p_pay_date <= COALESCE(public.settings_finance_windows.date_to, 'infinity'::date)
  ORDER BY public.settings_finance_windows.date_from DESC, public.settings_finance_windows.id DESC
  LIMIT 1;

  IF v_vat_rate_pct IS NULL OR v_erni_pct IS NULL THEN
    RAISE EXCEPTION 'No finance window found for pay_date %', p_pay_date;
  END IF;

  SELECT
    public.settings_defaults.rail_provider_default,
    public.settings_defaults.rail_env_default,
    public.settings_defaults.rail_supports_scheduling,
    public.settings_defaults.rail_supports_name_check,
    public.settings_defaults.rail_supports_auto_execute,
    public.settings_defaults.default_schedule_umbrella_local,
    public.settings_defaults.default_schedule_paye_local,
    public.settings_defaults.funds_warning_hours_json,
    public.settings_defaults.pay_eligibility_months_back,
    public.settings_defaults.pay_eligibility_weeks_ahead
  INTO
    v_rail_provider_default,
    v_rail_env_default,
    v_rail_supports_scheduling,
    v_rail_supports_name_check,
    v_rail_supports_auto_execute,
    v_default_schedule_umbrella_local,
    v_default_schedule_paye_local,
    v_funds_warning_hours_json,
    v_pay_eligibility_months_back,
    v_pay_eligibility_weeks_ahead
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id ASC
  LIMIT 1;

  IF v_rail_provider_default IS NULL OR v_rail_env_default IS NULL THEN
    RAISE EXCEPTION 'settings_defaults missing or not populated';
  END IF;

  v_pay_eligibility_months_back := GREATEST(0, LEAST(COALESCE(v_pay_eligibility_months_back, 6), 120));
  v_pay_eligibility_weeks_ahead := GREATEST(0, LEAST(COALESCE(v_pay_eligibility_weeks_ahead, 2), 52));

  v_eligibility_from_date := (v_today_uk - (v_pay_eligibility_months_back::text || ' months')::interval)::date;
  v_eligibility_to_date := (v_today_uk + (v_pay_eligibility_weeks_ahead::text || ' weeks')::interval)::date;

  v_need_name_check := (COALESCE(v_rail_supports_name_check, false) = true)
                       AND (UPPER(BTRIM(COALESCE(v_rail_provider_default, ''))) <> 'CSV');
  v_requires_payee_map := (UPPER(BTRIM(COALESCE(v_rail_provider_default, ''))) <> 'CSV');

  v_preview_decisions_root := CASE
    WHEN jsonb_typeof(COALESCE(p_preview_decisions_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_preview_decisions_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_preview_case_resolutions := CASE
    WHEN jsonb_typeof(v_preview_decisions_root->'case_resolutions') = 'object'
      THEN COALESCE(v_preview_decisions_root->'case_resolutions', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_paye_guardrails := public.pay_paye_guardrails(
    p_pay_date => p_pay_date,
    p_ignore_pay_batch_id => NULL::uuid,
    p_actor_user_id => p_actor_user_id
  );

  WITH force_include AS (
    SELECT DISTINCT
      public.timesheet_payment_overrides.timesheet_id
    FROM public.timesheet_payment_overrides
    WHERE public.timesheet_payment_overrides.cleared_at_utc IS NULL
      AND public.timesheet_payment_overrides.consumed_at_utc IS NULL
      AND public.timesheet_payment_overrides.consumed_by_pay_batch_id IS NULL
      AND UPPER(COALESCE(public.timesheet_payment_overrides.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
      AND public.timesheet_payment_overrides.timesheet_id IS NOT NULL
  ),
  timesheet_scope AS (
    SELECT DISTINCT
      public.timesheets_financials.candidate_id
    FROM public.timesheets_financials
    JOIN public.timesheets
      ON public.timesheets.timesheet_id = public.timesheets_financials.timesheet_id
    JOIN public.candidates
      ON public.candidates.id = public.timesheets_financials.candidate_id
    LEFT JOIN public.timesheet_pay_state
      ON public.timesheet_pay_state.timesheet_id = public.timesheets_financials.timesheet_id
    LEFT JOIN force_include
      ON force_include.timesheet_id = public.timesheets_financials.timesheet_id
    WHERE public.timesheets_financials.is_current = true
      AND COALESCE(public.timesheets_financials.pay_on_hold, false) = false
      AND COALESCE(public.timesheets_financials.has_rate_issue, false) = false
      AND COALESCE(public.timesheets_financials.has_pay_channel_issue, false) = false
      AND UPPER(COALESCE(public.timesheets_financials.processing_status::text, '')) NOT IN ('UNASSIGNED', 'CLIENT_UNRESOLVED', 'RATE_MISSING', 'PAY_CHANNEL_MISSING')
      AND UPPER(COALESCE(public.candidates.pay_method, '')) IN ('PAYE', 'UMBRELLA')
      AND (
        (
          public.timesheets.authorised_at_server IS NOT NULL
          AND public.timesheets.week_ending_date::date >= v_eligibility_from_date
          AND public.timesheets.week_ending_date::date <= v_eligibility_to_date
          AND public.timesheets.week_ending_date::date <= p_week_ending_cutoff
        )
        OR force_include.timesheet_id IS NOT NULL
        OR (
          public.timesheet_pay_state.last_settled_snapshot_json IS NOT NULL
          AND public.timesheets.week_ending_date::date >= v_eligibility_from_date
          AND public.timesheets.week_ending_date::date <= p_week_ending_cutoff
        )
      )
      AND (p_candidate_id IS NULL OR public.timesheets_financials.candidate_id = p_candidate_id)
      AND (p_client_id IS NULL OR public.timesheets_financials.client_id = p_client_id)
  ),
  finance_scope AS (
    SELECT DISTINCT
      public.v_finance_cases_register.candidate_id
    FROM public.v_finance_cases_register
    JOIN public.candidates
      ON public.candidates.id = public.v_finance_cases_register.candidate_id
    WHERE public.v_finance_cases_register.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'OVERPAYMENT'::public.pay_finance_case_type_enum,
        'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND UPPER(COALESCE(public.v_finance_cases_register.status::text, '')) = 'ACTIVE'
      AND COALESCE(public.v_finance_cases_register.outstanding_amount, 0) > 0
      AND UPPER(COALESCE(public.candidates.pay_method, '')) IN ('PAYE', 'UMBRELLA')
      AND NOT (
        public.v_finance_cases_register.active_snooze_id IS NOT NULL
        AND public.v_finance_cases_register.active_snooze_until_date IS NULL
      )
      AND (
        (
          public.v_finance_cases_register.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
          AND (
            UPPER(COALESCE(public.v_finance_cases_register.payout_status::text, '')) <> 'PAID'
            OR public.v_finance_cases_register.next_due_week_start IS NULL
            OR public.v_finance_cases_register.next_due_week_start <= v_pay_week_start
          )
        )
        OR (
          public.v_finance_cases_register.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
          AND (
            public.v_finance_cases_register.next_due_week_start IS NULL
            OR public.v_finance_cases_register.next_due_week_start <= v_pay_week_start
          )
        )
        OR (
          public.v_finance_cases_register.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
          AND (
            public.v_finance_cases_register.next_due_week_start IS NULL
            OR public.v_finance_cases_register.next_due_week_start <= v_pay_week_start
          )
        )
        OR public.v_finance_cases_register.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      AND (p_candidate_id IS NULL OR public.v_finance_cases_register.candidate_id = p_candidate_id)
      AND (p_client_id IS NULL OR public.v_finance_cases_register.client_id = p_client_id)
  ),
  scope_candidates AS (
    SELECT timesheet_scope.candidate_id
    FROM timesheet_scope
    UNION
    SELECT finance_scope.candidate_id
    FROM finance_scope
  )
  SELECT
    COALESCE(jsonb_agg(to_jsonb(scope_candidates.candidate_id::text) ORDER BY scope_candidates.candidate_id), '[]'::jsonb),
    COUNT(*)::integer
  INTO
    v_scope_candidate_ids,
    v_scope_candidate_count
  FROM scope_candidates;

  RETURN jsonb_build_object(
    'pay_date', p_pay_date::text,
    'pay_week_start', v_pay_week_start::text,
    'week_ending_cutoff_date', p_week_ending_cutoff::text,
    'eligibility', jsonb_build_object(
      'today_uk', v_today_uk::text,
      'from_date', v_eligibility_from_date::text,
      'to_date', v_eligibility_to_date::text,
      'months_back', v_pay_eligibility_months_back,
      'weeks_ahead', v_pay_eligibility_weeks_ahead
    ),
    'filters', jsonb_build_object(
      'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'client_id', CASE WHEN p_client_id IS NULL THEN NULL ELSE p_client_id::text END
    ),
    'finance', jsonb_build_object(
      'vat_rate_pct', v_vat_rate_pct,
      'erni_pct', v_erni_pct
    ),
    'settings', jsonb_build_object(
      'rail', jsonb_build_object(
        'provider_default', v_rail_provider_default,
        'env_default', v_rail_env_default,
        'supports_scheduling', v_rail_supports_scheduling,
        'supports_name_check', v_rail_supports_name_check,
        'supports_auto_execute', v_rail_supports_auto_execute,
        'need_name_check', v_need_name_check,
        'requires_payee_map', v_requires_payee_map
      ),
      'schedule_defaults', jsonb_build_object(
        'umbrella_local', v_default_schedule_umbrella_local,
        'paye_local', v_default_schedule_paye_local
      ),
      'funds_warning_hours_json', COALESCE(v_funds_warning_hours_json, '[]'::jsonb)
    ),
    'paye_guardrails', COALESCE(v_paye_guardrails, '{}'::jsonb),
    'scope_candidate_ids', COALESCE(v_scope_candidate_ids, '[]'::jsonb),
    'scope_candidate_count', v_scope_candidate_count,
    'preview_decisions_json', v_preview_decisions_root,
    'preview_case_resolutions', v_preview_case_resolutions
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_preview_build_candidate_rollup(
  p_context_json jsonb,
  p_candidate_effective_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_context_json jsonb := COALESCE(p_context_json, '{}'::jsonb);
  v_candidate_effective_root jsonb := COALESCE(p_candidate_effective_json, '{}'::jsonb);
  v_candidate_row_source jsonb := '{}'::jsonb;
  v_candidate_id text := '';
  v_display_name text := '';
  v_tms_ref text := '';
  v_current_pay_method text := '';
  v_case_states_input jsonb := '[]'::jsonb;
  v_lines_input jsonb := '[]'::jsonb;
  v_payees_input jsonb := '[]'::jsonb;
  v_itemisation_input jsonb := '[]'::jsonb;
  v_baseline_component_rows jsonb := '[]'::jsonb;
  v_explicit_blocked_input jsonb := '[]'::jsonb;
  v_explicit_do_not_pay_input jsonb := '[]'::jsonb;
  v_explicit_snoozed_input jsonb := '[]'::jsonb;
  v_normalized_case_states jsonb := '[]'::jsonb;
  v_normalized_lines jsonb := '[]'::jsonb;
  v_normalized_payees jsonb := '[]'::jsonb;
  v_normalized_itemisation jsonb := '[]'::jsonb;
  v_blocked_items jsonb := '[]'::jsonb;
  v_do_not_pay_items jsonb := '[]'::jsonb;
  v_snoozed_items jsonb := '[]'::jsonb;
  v_primary_payee_json jsonb := NULL;
  v_primary_line_json jsonb := NULL;
  v_primary_payee_entity_kind text := '';
  v_primary_payee_entity_id text := '';
  v_primary_bank_details_hash text := '';
  v_primary_payee_bank_hash text := '';
  v_primary_bank_details_hash_snapshot text := '';
  v_primary_snapshot_bank_details_hash text := '';
  v_primary_name_check_status text := '';
  v_primary_name_check_has_override boolean := false;
  v_primary_payee_map_present boolean := false;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_candidate_blockers jsonb := '[]'::jsonb;
  v_has_blocked_case boolean := false;
  v_case_resolution_state_count integer := 0;
  v_blocked_case_state_count integer := 0;
  v_canonical_preview_line_count integer := 0;
  v_ready_preview_line_count integer := 0;
  v_blocked_preview_line_count integer := 0;
  v_do_not_pay_line_count integer := 0;
  v_snoozed_line_count integer := 0;
  v_payees_count integer := 0;
  v_total_amount_ex_vat numeric := 0;
  v_total_amount_vat numeric := 0;
  v_total_amount_inc_vat numeric := 0;
  v_draftable_amount_ex_vat numeric := 0;
  v_draftable_amount_vat numeric := 0;
  v_draftable_amount_inc_vat numeric := 0;
  v_has_any_delta boolean := false;
  v_has_review_required_blocker boolean := false;
  v_is_ready_for_draft boolean := false;
  v_is_review_required boolean := false;
  v_summary_fragment jsonb := '{}'::jsonb;
  v_candidate_row_base jsonb := '{}'::jsonb;
  v_candidate_row jsonb := '{}'::jsonb;
BEGIN
  IF jsonb_typeof(v_context_json) <> 'object' THEN
    RAISE EXCEPTION 'p_context_json must be a JSON object';
  END IF;

  IF jsonb_typeof(v_candidate_effective_root) <> 'object' THEN
    RAISE EXCEPTION 'p_candidate_effective_json must be a JSON object';
  END IF;

  v_candidate_row_source := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'candidate_row') = 'object'
      THEN COALESCE(v_candidate_effective_root->'candidate_row', '{}'::jsonb)
    ELSE v_candidate_effective_root
  END;

  v_candidate_id := BTRIM(COALESCE(
    v_candidate_row_source->>'candidate_id',
    v_candidate_effective_root->>'candidate_id',
    ''
  ));

  IF v_candidate_id = '' THEN
    RAISE EXCEPTION 'candidate_id is required on p_candidate_effective_json';
  END IF;

  v_display_name := BTRIM(COALESCE(
    v_candidate_row_source->>'display_name',
    v_candidate_row_source->>'candidate_name',
    v_candidate_row_source->>'candidate_display_name',
    v_candidate_effective_root->>'display_name',
    v_candidate_effective_root->>'candidate_name',
    v_candidate_effective_root->>'candidate_display_name',
    ''
  ));

  v_tms_ref := BTRIM(COALESCE(
    v_candidate_row_source->>'tms_ref',
    v_candidate_row_source->>'candidate_tms_ref',
    v_candidate_effective_root->>'tms_ref',
    v_candidate_effective_root->>'candidate_tms_ref',
    ''
  ));

  v_case_states_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'case_resolution_states') = 'array'
      THEN COALESCE(v_candidate_effective_root->'case_resolution_states', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'case_resolution_states') = 'array'
      THEN COALESCE(v_candidate_row_source->'case_resolution_states', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_lines_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'canonical_preview_lines') = 'array'
      THEN COALESCE(v_candidate_effective_root->'canonical_preview_lines', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'canonical_preview_lines') = 'array'
      THEN COALESCE(v_candidate_row_source->'canonical_preview_lines', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_payees_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'payees') = 'array'
      THEN COALESCE(v_candidate_effective_root->'payees', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'payees') = 'array'
      THEN COALESCE(v_candidate_row_source->'payees', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_itemisation_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'itemisation') = 'array'
      THEN COALESCE(v_candidate_effective_root->'itemisation', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'itemisation') = 'array'
      THEN COALESCE(v_candidate_row_source->'itemisation', '[]'::jsonb)
    ELSE v_lines_input
  END;

  v_explicit_blocked_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'blocked_items') = 'array'
      THEN COALESCE(v_candidate_effective_root->'blocked_items', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'blocked_items') = 'array'
      THEN COALESCE(v_candidate_row_source->'blocked_items', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_explicit_do_not_pay_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'do_not_pay_items') = 'array'
      THEN COALESCE(v_candidate_effective_root->'do_not_pay_items', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'do_not_pay_items') = 'array'
      THEN COALESCE(v_candidate_row_source->'do_not_pay_items', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_explicit_snoozed_input := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'snoozed_items') = 'array'
      THEN COALESCE(v_candidate_effective_root->'snoozed_items', '[]'::jsonb)
    WHEN jsonb_typeof(v_candidate_row_source->'snoozed_items') = 'array'
      THEN COALESCE(v_candidate_row_source->'snoozed_items', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  v_baseline_component_rows := CASE
    WHEN jsonb_typeof(v_candidate_effective_root->'baseline_component_rows') = 'array'
      THEN COALESCE(v_candidate_effective_root->'baseline_component_rows', '[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  SELECT
    COALESCE(
      jsonb_agg(
        CASE
          WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
            THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
          ELSE elem.value
        END
        ORDER BY elem.ord
      ),
      '[]'::jsonb
    )
  INTO v_normalized_case_states
  FROM jsonb_array_elements(v_case_states_input) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        CASE
          WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
            THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
          ELSE elem.value
        END
        ORDER BY elem.ord
      ),
      '[]'::jsonb
    )
  INTO v_normalized_lines
  FROM jsonb_array_elements(v_lines_input) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        CASE
          WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
            THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
          ELSE elem.value
        END
        ORDER BY elem.ord
      ),
      '[]'::jsonb
    )
  INTO v_normalized_payees
  FROM jsonb_array_elements(v_payees_input) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        CASE
          WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
            THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
          ELSE elem.value
        END
        ORDER BY elem.ord
      ),
      '[]'::jsonb
    )
  INTO v_normalized_itemisation
  FROM jsonb_array_elements(v_itemisation_input) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object';

  IF jsonb_typeof(v_explicit_blocked_input) = 'array' AND jsonb_array_length(v_explicit_blocked_input) > 0 THEN
    SELECT
      COALESCE(
        jsonb_agg(
          CASE
            WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
              THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
            ELSE elem.value
          END
          ORDER BY elem.ord
        ),
        '[]'::jsonb
      )
    INTO v_blocked_items
    FROM jsonb_array_elements(v_explicit_blocked_input) WITH ORDINALITY AS elem(value, ord)
    WHERE jsonb_typeof(elem.value) = 'object';
  ELSE
    SELECT
      COALESCE(
        jsonb_agg(elem.value ORDER BY elem.ord),
        '[]'::jsonb
      )
    INTO v_blocked_items
    FROM jsonb_array_elements(v_normalized_lines) WITH ORDINALITY AS elem(value, ord)
    WHERE (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
    );
  END IF;

  IF jsonb_typeof(v_explicit_do_not_pay_input) = 'array' AND jsonb_array_length(v_explicit_do_not_pay_input) > 0 THEN
    SELECT
      COALESCE(
        jsonb_agg(
          CASE
            WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
              THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
            ELSE elem.value
          END
          ORDER BY elem.ord
        ),
        '[]'::jsonb
      )
    INTO v_do_not_pay_items
    FROM jsonb_array_elements(v_explicit_do_not_pay_input) WITH ORDINALITY AS elem(value, ord)
    WHERE jsonb_typeof(elem.value) = 'object';
  ELSE
    SELECT
      COALESCE(
        jsonb_agg(elem.value ORDER BY elem.ord),
        '[]'::jsonb
      )
    INTO v_do_not_pay_items
    FROM jsonb_array_elements(v_normalized_lines) WITH ORDINALITY AS elem(value, ord)
    WHERE (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
    );
  END IF;

  IF jsonb_typeof(v_explicit_snoozed_input) = 'array' AND jsonb_array_length(v_explicit_snoozed_input) > 0 THEN
    SELECT
      COALESCE(
        jsonb_agg(
          CASE
            WHEN jsonb_typeof(elem.value) = 'object' AND COALESCE(NULLIF(BTRIM(COALESCE(elem.value->>'candidate_id', '')), ''), '') = ''
              THEN elem.value || jsonb_build_object('candidate_id', v_candidate_id)
            ELSE elem.value
          END
          ORDER BY elem.ord
        ),
        '[]'::jsonb
      )
    INTO v_snoozed_items
    FROM jsonb_array_elements(v_explicit_snoozed_input) WITH ORDINALITY AS elem(value, ord)
    WHERE jsonb_typeof(elem.value) = 'object';
  ELSE
    SELECT
      COALESCE(
        jsonb_agg(elem.value ORDER BY elem.ord),
        '[]'::jsonb
      )
    INTO v_snoozed_items
    FROM jsonb_array_elements(v_normalized_lines) WITH ORDINALITY AS elem(value, ord)
    WHERE (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );
  END IF;

  SELECT elem.value
  INTO v_primary_payee_json
  FROM jsonb_array_elements(v_normalized_payees) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object'
  ORDER BY elem.ord ASC
  LIMIT 1;

  SELECT elem.value
  INTO v_primary_line_json
  FROM jsonb_array_elements(v_normalized_lines) WITH ORDINALITY AS elem(value, ord)
  WHERE jsonb_typeof(elem.value) = 'object'
  ORDER BY elem.ord ASC
  LIMIT 1;

  v_current_pay_method := UPPER(BTRIM(COALESCE(
    v_candidate_row_source->>'current_pay_method',
    v_candidate_row_source->>'pay_method',
    v_candidate_row_source->>'pay_channel',
    v_candidate_effective_root->>'current_pay_method',
    v_candidate_effective_root->>'pay_method',
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'pay_channel' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'current_pay_method' END,
    ''
  )));

  IF v_display_name = '' THEN
    v_display_name := BTRIM(COALESCE(
      CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'display_name' END,
      CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'candidate_name' END,
      CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'display_name' END,
      CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'candidate_name' END,
      ''
    ));
  END IF;

  IF v_tms_ref = '' THEN
    v_tms_ref := BTRIM(COALESCE(
      CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'tms_ref' END,
      CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'candidate_tms_ref' END,
      CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'tms_ref' END,
      CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'candidate_tms_ref' END,
      ''
    ));
  END IF;

  v_primary_payee_entity_kind := UPPER(BTRIM(COALESCE(
    v_candidate_row_source->>'payee_entity_kind',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'payee_entity_kind' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'entity_kind' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'payee_entity_kind' END,
    ''
  )));

  v_primary_payee_entity_id := BTRIM(COALESCE(
    v_candidate_row_source->>'payee_entity_id',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'payee_entity_id' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'entity_id' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'payee_entity_id' END,
    ''
  ));

  v_primary_bank_details_hash := BTRIM(COALESCE(
    v_candidate_row_source->>'bank_details_hash',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'bank_details_hash' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'bank_details_hash' END,
    ''
  ));

  v_primary_payee_bank_hash := BTRIM(COALESCE(
    v_candidate_row_source->>'payee_bank_hash',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'payee_bank_hash' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'payee_bank_hash' END,
    v_primary_bank_details_hash,
    ''
  ));

  v_primary_bank_details_hash_snapshot := BTRIM(COALESCE(
    v_candidate_row_source->>'bank_details_hash_snapshot',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'bank_details_hash_snapshot' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'bank_details_hash_snapshot' END,
    ''
  ));

  v_primary_snapshot_bank_details_hash := BTRIM(COALESCE(
    v_candidate_row_source->>'snapshot_bank_details_hash',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'snapshot_bank_details_hash' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'snapshot_bank_details_hash' END,
    v_primary_bank_details_hash_snapshot,
    ''
  ));

  v_primary_name_check_status := UPPER(BTRIM(COALESCE(
    v_candidate_row_source->>'name_check_status',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'name_check_status' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json #>> '{name_check,status}' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'name_check_status' END,
    ''
  )));

  v_primary_name_check_has_override := COALESCE(LOWER(BTRIM(COALESCE(
    v_candidate_row_source->>'name_check_has_override',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'name_check_has_override' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json #>> '{name_check,has_override}' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'name_check_has_override' END,
    'false'
  ))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  v_primary_payee_map_present := COALESCE(LOWER(BTRIM(COALESCE(
    v_candidate_row_source->>'payee_map_present',
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json->>'payee_map_present' END,
    CASE WHEN v_primary_payee_json IS NULL THEN NULL ELSE v_primary_payee_json #>> '{payee_map,present}' END,
    CASE WHEN v_primary_line_json IS NULL THEN NULL ELSE v_primary_line_json->>'payee_map_present' END,
    'false'
  ))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  v_need_name_check := COALESCE(LOWER(BTRIM(COALESCE(v_context_json #>> '{settings,rail,need_name_check}', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');
  v_requires_payee_map := COALESCE(LOWER(BTRIM(COALESCE(v_context_json #>> '{settings,rail,requires_payee_map}', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_normalized_case_states) AS elem(value)
    WHERE COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
  )
  INTO v_has_blocked_case;

  SELECT
    COALESCE(jsonb_agg(to_jsonb(blocker_distinct.code) ORDER BY blocker_distinct.code), '[]'::jsonb)
  INTO v_candidate_blockers
  FROM (
    SELECT DISTINCT blocker_codes.code
    FROM (
      SELECT UPPER(BTRIM(explicit_code.value)) AS code
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_candidate_row_source->'blockers') = 'array' THEN COALESCE(v_candidate_row_source->'blockers', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS explicit_code(value)

      UNION ALL

      SELECT UPPER(BTRIM(line_code.value)) AS code
      FROM jsonb_array_elements(v_normalized_lines) AS line_elem(value)
      CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(line_elem.value->'blockers') = 'array' THEN COALESCE(line_elem.value->'blockers', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS line_code(value)

      UNION ALL

      SELECT UPPER(BTRIM(payee_code.value)) AS code
      FROM jsonb_array_elements(v_normalized_payees) AS payee_elem(value)
      CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(payee_elem.value->'blockers') = 'array' THEN COALESCE(payee_elem.value->'blockers', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS payee_code(value)

      UNION ALL

      SELECT 'BLOCKED_FINANCE_CASE' AS code
      WHERE v_has_blocked_case = true

      UNION ALL

      SELECT 'BLOCKED_BANK_DETAILS' AS code
      WHERE v_current_pay_method <> ''
        AND COALESCE(v_primary_bank_details_hash, '') = ''
        AND (COALESCE(v_primary_payee_entity_kind, '') <> '' OR COALESCE(v_primary_payee_entity_id, '') <> '')

      UNION ALL

      SELECT 'BLOCKED_NO_PAYEE_MAP' AS code
      WHERE v_requires_payee_map = true
        AND v_current_pay_method <> ''
        AND COALESCE(v_primary_bank_details_hash, '') <> ''
        AND v_primary_payee_map_present = false

      UNION ALL

      SELECT 'BLOCKED_NAME_CHECK' AS code
      WHERE v_need_name_check = true
        AND v_current_pay_method <> ''
        AND COALESCE(v_primary_bank_details_hash, '') <> ''
        AND v_primary_name_check_has_override = false
        AND COALESCE(v_primary_name_check_status, '') <> 'PASS'
    ) AS blocker_codes
    WHERE COALESCE(blocker_codes.code, '') <> ''
  ) AS blocker_distinct;

  SELECT COUNT(*)::integer
  INTO v_case_resolution_state_count
  FROM jsonb_array_elements(v_normalized_case_states) AS elem(value);

  SELECT COUNT(*)::integer
  INTO v_blocked_case_state_count
  FROM jsonb_array_elements(v_normalized_case_states) AS elem(value)
  WHERE COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COUNT(*)::integer
  INTO v_canonical_preview_line_count
  FROM jsonb_array_elements(v_normalized_lines) AS elem(value);

  SELECT COUNT(*)::integer
  INTO v_ready_preview_line_count
  FROM jsonb_array_elements(v_normalized_lines) AS elem(value)
  WHERE NOT (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );

  v_blocked_preview_line_count := jsonb_array_length(v_blocked_items);
  v_do_not_pay_line_count := jsonb_array_length(v_do_not_pay_items);
  v_snoozed_line_count := jsonb_array_length(v_snoozed_items);

  SELECT COUNT(*)::integer
  INTO v_payees_count
  FROM jsonb_array_elements(v_normalized_payees) AS elem(value);

  SELECT
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_ex_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_inc_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_inc_vat')::numeric ELSE 0 END), 0)
  INTO
    v_total_amount_ex_vat,
    v_total_amount_vat,
    v_total_amount_inc_vat
  FROM jsonb_array_elements(v_normalized_lines) AS elem(value);

  SELECT
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_ex_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_inc_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_inc_vat')::numeric ELSE 0 END), 0)
  INTO
    v_draftable_amount_ex_vat,
    v_draftable_amount_vat,
    v_draftable_amount_inc_vat
  FROM jsonb_array_elements(v_normalized_lines) AS elem(value)
  WHERE NOT (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );

  v_has_any_delta := (v_canonical_preview_line_count > 0) OR (v_case_resolution_state_count > 0) OR (jsonb_array_length(v_normalized_itemisation) > 0);
  v_has_review_required_blocker := (jsonb_array_length(v_candidate_blockers) > 0)
                                   OR (v_blocked_case_state_count > 0)
                                   OR (v_blocked_preview_line_count > 0)
                                   OR (v_do_not_pay_line_count > 0);
  v_is_ready_for_draft := v_has_any_delta AND (v_ready_preview_line_count > 0) AND NOT v_has_review_required_blocker;
  v_is_review_required := v_has_any_delta AND v_has_review_required_blocker;

  v_summary_fragment := COALESCE(
    CASE
      WHEN jsonb_typeof(v_candidate_effective_root->'summary_fragment') = 'object' THEN v_candidate_effective_root->'summary_fragment'
      WHEN jsonb_typeof(v_candidate_row_source->'summary_fragment') = 'object' THEN v_candidate_row_source->'summary_fragment'
      ELSE '{}'::jsonb
    END,
    '{}'::jsonb
  ) || jsonb_build_object(
    'candidate_count', 1,
    'paye_candidates_count', CASE WHEN v_current_pay_method = 'PAYE' THEN 1 ELSE 0 END,
    'non_paye_payees_count', CASE WHEN v_current_pay_method = 'PAYE' THEN 0 ELSE 1 END,
    'ready_candidates_count', CASE WHEN v_is_ready_for_draft THEN 1 ELSE 0 END,
    'blocked_candidates_count', CASE WHEN v_is_review_required THEN 1 ELSE 0 END,
    'case_resolution_state_count', v_case_resolution_state_count,
    'blocked_case_state_count', v_blocked_case_state_count,
    'canonical_preview_line_count', v_canonical_preview_line_count,
    'ready_preview_line_count', v_ready_preview_line_count,
    'blocked_preview_line_count', v_blocked_preview_line_count,
    'do_not_pay_line_count', v_do_not_pay_line_count,
    'snoozed_line_count', v_snoozed_line_count,
    'payees_count', v_payees_count,
    'total_amount_ex_vat', v_total_amount_ex_vat,
    'total_amount_vat', v_total_amount_vat,
    'total_amount_inc_vat', v_total_amount_inc_vat,
    'draftable_amount_ex_vat', v_draftable_amount_ex_vat,
    'draftable_amount_vat', v_draftable_amount_vat,
    'draftable_amount_inc_vat', v_draftable_amount_inc_vat,
    'has_any_delta', v_has_any_delta,
    'has_review_required_blocker', v_has_review_required_blocker,
    'is_ready_for_draft', v_is_ready_for_draft,
    'is_review_required', v_is_review_required
  );

  v_candidate_row_base := (v_candidate_row_source
    - 'case_resolution_states'
    - 'canonical_preview_lines'
    - 'payees'
    - 'itemisation'
    - 'blocked_items'
    - 'do_not_pay_items'
    - 'snoozed_items'
    - 'summary_fragment'
    - 'paye_candidate'
    - 'non_paye_payee');

  v_candidate_row := v_candidate_row_base || jsonb_build_object(
    'candidate_id', v_candidate_id,
    'display_name', v_display_name,
    'candidate_name', v_display_name,
    'tms_ref', v_tms_ref,
    'current_pay_method', v_current_pay_method,
    'is_ready_for_draft', v_is_ready_for_draft,
    'is_review_required', v_is_review_required,
    'ready_to_pay', v_is_ready_for_draft,
    'blockers', v_candidate_blockers,
    'payee_entity_kind', NULLIF(v_primary_payee_entity_kind, ''),
    'payee_entity_id', NULLIF(v_primary_payee_entity_id, ''),
    'bank_details_hash', NULLIF(v_primary_bank_details_hash, ''),
    'payee_bank_hash', NULLIF(v_primary_payee_bank_hash, ''),
    'bank_details_hash_snapshot', NULLIF(v_primary_bank_details_hash_snapshot, ''),
    'snapshot_bank_details_hash', NULLIF(v_primary_snapshot_bank_details_hash, ''),
    'name_check_status', NULLIF(v_primary_name_check_status, ''),
    'name_check_has_override', v_primary_name_check_has_override,
    'payee_map_present', v_primary_payee_map_present,
    'case_resolution_states', v_normalized_case_states,
    'case_resolution_state_count', v_case_resolution_state_count,
    'blocked_case_state_count', v_blocked_case_state_count,
    'canonical_preview_line_count', v_canonical_preview_line_count,
    'ready_preview_line_count', v_ready_preview_line_count,
    'blocked_preview_line_count', v_blocked_preview_line_count,
    'do_not_pay_line_count', v_do_not_pay_line_count,
    'snoozed_line_count', v_snoozed_line_count,
    'payees_count', v_payees_count,
    'total_amount_ex_vat', v_total_amount_ex_vat,
    'total_amount_vat', v_total_amount_vat,
    'total_amount_inc_vat', v_total_amount_inc_vat,
    'draftable_amount_ex_vat', v_draftable_amount_ex_vat,
    'draftable_amount_vat', v_draftable_amount_vat,
    'draftable_amount_inc_vat', v_draftable_amount_inc_vat,
    'itemisation', v_normalized_itemisation
  );

  RETURN jsonb_build_object(
    'candidate_id', v_candidate_id,
    'current_pay_method', v_current_pay_method,
    'summary_fragment', v_summary_fragment,
    'case_resolution_states', v_normalized_case_states,
    'canonical_preview_lines', v_normalized_lines,
    'payees', v_normalized_payees,
    'itemisation', v_normalized_itemisation,
    'blocked_items', v_blocked_items,
    'do_not_pay_items', v_do_not_pay_items,
    'snoozed_items', v_snoozed_items,
    'baseline_component_rows', v_baseline_component_rows,
    'paye_candidate', CASE WHEN v_current_pay_method = 'PAYE' THEN v_candidate_row ELSE NULL END,
    'non_paye_payee', CASE WHEN v_current_pay_method = 'PAYE' THEN NULL ELSE v_candidate_row END
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_preview_assemble_payload(
  p_context_json jsonb,
  p_candidate_rollups_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_context_json jsonb := COALESCE(p_context_json, '{}'::jsonb);
  v_candidate_rollups_root jsonb := COALESCE(p_candidate_rollups_json, '[]'::jsonb);
  v_rollup jsonb;
  v_paye_candidates_raw jsonb := '[]'::jsonb;
  v_non_paye_payees_raw jsonb := '[]'::jsonb;
  v_case_resolution_states_raw jsonb := '[]'::jsonb;
  v_canonical_preview_lines_raw jsonb := '[]'::jsonb;
  v_payees_raw jsonb := '[]'::jsonb;
  v_itemisation_raw jsonb := '[]'::jsonb;
  v_blocked_items_raw jsonb := '[]'::jsonb;
  v_do_not_pay_items_raw jsonb := '[]'::jsonb;
  v_snoozed_items_raw jsonb := '[]'::jsonb;
  v_baseline_component_rows_raw jsonb := '[]'::jsonb;
  v_paye_candidates jsonb := '[]'::jsonb;
  v_non_paye_payees jsonb := '[]'::jsonb;
  v_case_resolution_states jsonb := '[]'::jsonb;
  v_canonical_preview_lines jsonb := '[]'::jsonb;
  v_payees jsonb := '[]'::jsonb;
  v_itemisation jsonb := '[]'::jsonb;
  v_blocked_items jsonb := '[]'::jsonb;
  v_do_not_pay_items jsonb := '[]'::jsonb;
  v_snoozed_items jsonb := '[]'::jsonb;
  v_baseline_component_rows jsonb := '[]'::jsonb;
  v_paye_summary_breakdown jsonb := '{}'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_candidate_count integer := 0;
  v_paye_candidates_count integer := 0;
  v_non_paye_payees_count integer := 0;
  v_ready_candidates_count integer := 0;
  v_blocked_candidates_count integer := 0;
  v_case_resolution_state_count integer := 0;
  v_blocked_case_state_count integer := 0;
  v_canonical_preview_line_count integer := 0;
  v_ready_preview_line_count integer := 0;
  v_blocked_preview_line_count integer := 0;
  v_do_not_pay_line_count integer := 0;
  v_snoozed_line_count integer := 0;
  v_payees_count integer := 0;
  v_total_amount_ex_vat numeric := 0;
  v_total_amount_vat numeric := 0;
  v_total_amount_inc_vat numeric := 0;
  v_draftable_amount_ex_vat numeric := 0;
  v_draftable_amount_vat numeric := 0;
  v_draftable_amount_inc_vat numeric := 0;
  v_gross_side_additions_ex_vat numeric := 0;
  v_gross_side_deductions_ex_vat numeric := 0;
  v_net_side_additions_ex_vat numeric := 0;
  v_net_side_deductions_ex_vat numeric := 0;
  v_payees_need_name_check integer := 0;
  v_payees_need_payee_map integer := 0;
  v_payees_missing_bank_details integer := 0;
BEGIN
  IF jsonb_typeof(v_context_json) <> 'object' THEN
    RAISE EXCEPTION 'p_context_json must be a JSON object';
  END IF;

  IF jsonb_typeof(v_candidate_rollups_root) <> 'array' THEN
    RAISE EXCEPTION 'p_candidate_rollups_json must be a JSON array';
  END IF;

  FOR v_rollup IN
    SELECT elem.value
    FROM jsonb_array_elements(v_candidate_rollups_root) AS elem(value)
    WHERE jsonb_typeof(elem.value) = 'object'
  LOOP
    IF jsonb_typeof(v_rollup->'paye_candidate') = 'object' THEN
      v_paye_candidates_raw := v_paye_candidates_raw || jsonb_build_array(v_rollup->'paye_candidate');
    END IF;

    IF jsonb_typeof(v_rollup->'non_paye_payee') = 'object' THEN
      v_non_paye_payees_raw := v_non_paye_payees_raw || jsonb_build_array(v_rollup->'non_paye_payee');
    END IF;

    IF jsonb_typeof(v_rollup->'case_resolution_states') = 'array' THEN
      v_case_resolution_states_raw := v_case_resolution_states_raw || COALESCE(v_rollup->'case_resolution_states', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'canonical_preview_lines') = 'array' THEN
      v_canonical_preview_lines_raw := v_canonical_preview_lines_raw || COALESCE(v_rollup->'canonical_preview_lines', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'payees') = 'array' THEN
      v_payees_raw := v_payees_raw || COALESCE(v_rollup->'payees', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'itemisation') = 'array' THEN
      v_itemisation_raw := v_itemisation_raw || COALESCE(v_rollup->'itemisation', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'blocked_items') = 'array' THEN
      v_blocked_items_raw := v_blocked_items_raw || COALESCE(v_rollup->'blocked_items', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'do_not_pay_items') = 'array' THEN
      v_do_not_pay_items_raw := v_do_not_pay_items_raw || COALESCE(v_rollup->'do_not_pay_items', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'snoozed_items') = 'array' THEN
      v_snoozed_items_raw := v_snoozed_items_raw || COALESCE(v_rollup->'snoozed_items', '[]'::jsonb);
    END IF;

    IF jsonb_typeof(v_rollup->'baseline_component_rows') = 'array' THEN
      v_baseline_component_rows_raw := v_baseline_component_rows_raw || COALESCE(v_rollup->'baseline_component_rows', '[]'::jsonb);
    END IF;
  END LOOP;

  SELECT
    COALESCE(
      jsonb_agg(elem.value ORDER BY BTRIM(COALESCE(elem.value->>'display_name', elem.value->>'candidate_name', '')), BTRIM(COALESCE(elem.value->>'tms_ref', '')), BTRIM(COALESCE(elem.value->>'candidate_id', ''))),
      '[]'::jsonb
    )
  INTO v_paye_candidates
  FROM jsonb_array_elements(v_paye_candidates_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(elem.value ORDER BY BTRIM(COALESCE(elem.value->>'display_name', elem.value->>'candidate_name', '')), BTRIM(COALESCE(elem.value->>'tms_ref', '')), BTRIM(COALESCE(elem.value->>'candidate_id', ''))),
      '[]'::jsonb
    )
  INTO v_non_paye_payees
  FROM jsonb_array_elements(v_non_paye_payees_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(elem.value ORDER BY BTRIM(COALESCE(elem.value->>'candidate_id', '')), BTRIM(COALESCE(elem.value->>'case_key', '')), BTRIM(COALESCE(elem.value->>'finance_case_id', '')), BTRIM(COALESCE(elem.value->>'timesheet_id', ''))),
      '[]'::jsonb
    )
  INTO v_case_resolution_states
  FROM jsonb_array_elements(v_case_resolution_states_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(elem.value ORDER BY BTRIM(COALESCE(elem.value->>'candidate_id', '')), BTRIM(COALESCE(elem.value->>'display_name', '')), BTRIM(COALESCE(elem.value->>'line_type', '')), BTRIM(COALESCE(elem.value->>'preview_row_id', elem.value->>'line_id', elem.value->>'row_id', elem.value->>'id', ''))),
      '[]'::jsonb
    )
  INTO v_canonical_preview_lines
  FROM jsonb_array_elements(v_canonical_preview_lines_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        elem.value
        ORDER BY
          BTRIM(COALESCE(elem.value->>'candidate_id', '')),
          BTRIM(COALESCE(elem.value->>'timesheet_id', '')),
          BTRIM(COALESCE(elem.value->>'finance_case_id', '')),
          BTRIM(COALESCE(elem.value->>'segment_id', elem.value->>'segment_stable_key', '')),
          BTRIM(COALESCE(elem.value->>'line_id', elem.value->>'preview_row_id', elem.value->>'row_id', elem.value->>'id', '')),
          BTRIM(COALESCE(elem.value->>'line_type', elem.value->>'item_type', elem.value->>'category', elem.value->>'component_kind', elem.value->>'component_type', ''))
      ),
      '[]'::jsonb
    )
  INTO v_itemisation
  FROM jsonb_array_elements(v_itemisation_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        elem.value
        ORDER BY
          BTRIM(COALESCE(elem.value->>'candidate_id', '')),
          BTRIM(COALESCE(elem.value->>'timesheet_id', elem.value->>'finance_case_id', '')),
          BTRIM(COALESCE(elem.value->>'segment_id', elem.value->>'segment_stable_key', elem.value->>'line_id', elem.value->>'preview_row_id', elem.value->>'row_id', elem.value->>'id', '')),
          BTRIM(COALESCE(elem.value->>'line_type', elem.value->>'category', elem.value->>'component_kind', elem.value->>'component_type', ''))
      ),
      '[]'::jsonb
    )
  INTO v_blocked_items
  FROM jsonb_array_elements(v_blocked_items_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        elem.value
        ORDER BY
          BTRIM(COALESCE(elem.value->>'candidate_id', '')),
          BTRIM(COALESCE(elem.value->>'timesheet_id', elem.value->>'finance_case_id', '')),
          BTRIM(COALESCE(elem.value->>'segment_id', elem.value->>'segment_stable_key', elem.value->>'line_id', elem.value->>'preview_row_id', elem.value->>'row_id', elem.value->>'id', '')),
          BTRIM(COALESCE(elem.value->>'line_type', elem.value->>'category', elem.value->>'component_kind', elem.value->>'component_type', ''))
      ),
      '[]'::jsonb
    )
  INTO v_do_not_pay_items
  FROM jsonb_array_elements(v_do_not_pay_items_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        elem.value
        ORDER BY
          BTRIM(COALESCE(elem.value->>'candidate_id', '')),
          BTRIM(COALESCE(elem.value->>'timesheet_id', elem.value->>'finance_case_id', '')),
          BTRIM(COALESCE(elem.value->>'segment_id', elem.value->>'segment_stable_key', elem.value->>'line_id', elem.value->>'preview_row_id', elem.value->>'row_id', elem.value->>'id', '')),
          BTRIM(COALESCE(elem.value->>'line_type', elem.value->>'category', elem.value->>'component_kind', elem.value->>'component_type', ''))
      ),
      '[]'::jsonb
    )
  INTO v_snoozed_items
  FROM jsonb_array_elements(v_snoozed_items_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(
      jsonb_agg(
        elem.value
        ORDER BY
          BTRIM(COALESCE(elem.value->>'candidate_id', '')),
          BTRIM(COALESCE(elem.value->>'case_key', '')),
          BTRIM(COALESCE(elem.value->>'timesheet_id', '')),
          BTRIM(COALESCE(elem.value->>'finance_case_id', '')),
          BTRIM(COALESCE(elem.value->>'finance_component_id', '')),
          BTRIM(COALESCE(elem.value->>'component_scope', '')),
          BTRIM(COALESCE(elem.value->>'source_family_key', '')),
          BTRIM(COALESCE(elem.value->>'component_key_type', '')),
          BTRIM(COALESCE(elem.value->>'component_key_value', '')),
          BTRIM(COALESCE(elem.value->>'component_fingerprint', '')),
          BTRIM(COALESCE(elem.value->>'source_basis_fingerprint', ''))
      ),
      '[]'::jsonb
    )
  INTO v_baseline_component_rows
  FROM jsonb_array_elements(v_baseline_component_rows_raw) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  WITH payee_elements AS (
    SELECT
      elem.value AS payee_json,
      elem.ordinality AS payee_ordinality,
      UPPER(BTRIM(COALESCE(elem.value->>'payee_entity_kind', elem.value->>'entity_kind', ''))) AS payee_entity_kind,
      BTRIM(COALESCE(elem.value->>'payee_entity_id', elem.value->>'entity_id', '')) AS payee_entity_id,
      BTRIM(COALESCE(elem.value->>'bank_details_hash', '')) AS bank_details_hash
    FROM jsonb_array_elements(v_payees_raw) WITH ORDINALITY AS elem(value, ordinality)
    WHERE jsonb_typeof(elem.value) = 'object'
  ), payee_ranked AS (
    SELECT
      payee_elements.payee_json,
      payee_elements.payee_ordinality,
      payee_elements.payee_entity_kind,
      payee_elements.payee_entity_id,
      payee_elements.bank_details_hash,
      row_number() OVER (
        PARTITION BY payee_elements.payee_entity_kind, payee_elements.payee_entity_id, payee_elements.bank_details_hash
        ORDER BY payee_elements.payee_ordinality ASC
      ) AS rn
    FROM payee_elements
  )
  SELECT
    COALESCE(
      jsonb_agg(payee_ranked.payee_json ORDER BY payee_ranked.payee_entity_kind, payee_ranked.payee_entity_id, payee_ranked.bank_details_hash, payee_ranked.payee_ordinality),
      '[]'::jsonb
    )
  INTO v_payees
  FROM payee_ranked
  WHERE payee_ranked.rn = 1;

  SELECT COUNT(*)::integer
  INTO v_paye_candidates_count
  FROM jsonb_array_elements(v_paye_candidates) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT COUNT(*)::integer
  INTO v_non_paye_payees_count
  FROM jsonb_array_elements(v_non_paye_payees) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  v_candidate_count := v_paye_candidates_count + v_non_paye_payees_count;

  SELECT COUNT(*)::integer
  INTO v_ready_candidates_count
  FROM (
    SELECT elem.value
    FROM jsonb_array_elements(v_paye_candidates) AS elem(value)
    WHERE jsonb_typeof(elem.value) = 'object'
    UNION ALL
    SELECT elem.value
    FROM jsonb_array_elements(v_non_paye_payees) AS elem(value)
    WHERE jsonb_typeof(elem.value) = 'object'
  ) AS candidate_rows(value)
  WHERE COALESCE(LOWER(BTRIM(COALESCE(candidate_rows.value->>'is_ready_for_draft', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COUNT(*)::integer
  INTO v_blocked_candidates_count
  FROM (
    SELECT elem.value
    FROM jsonb_array_elements(v_paye_candidates) AS elem(value)
    WHERE jsonb_typeof(elem.value) = 'object'
    UNION ALL
    SELECT elem.value
    FROM jsonb_array_elements(v_non_paye_payees) AS elem(value)
    WHERE jsonb_typeof(elem.value) = 'object'
  ) AS candidate_rows(value)
  WHERE COALESCE(LOWER(BTRIM(COALESCE(candidate_rows.value->>'is_review_required', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COUNT(*)::integer
  INTO v_case_resolution_state_count
  FROM jsonb_array_elements(v_case_resolution_states) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT COUNT(*)::integer
  INTO v_blocked_case_state_count
  FROM jsonb_array_elements(v_case_resolution_states) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COUNT(*)::integer
  INTO v_canonical_preview_line_count
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT COUNT(*)::integer
  INTO v_ready_preview_line_count
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND NOT (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );

  v_blocked_preview_line_count := jsonb_array_length(v_blocked_items);
  v_do_not_pay_line_count := jsonb_array_length(v_do_not_pay_items);
  v_snoozed_line_count := jsonb_array_length(v_snoozed_items);

  SELECT COUNT(*)::integer
  INTO v_payees_count
  FROM jsonb_array_elements(v_payees) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_ex_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_inc_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_inc_vat')::numeric ELSE 0 END), 0)
  INTO
    v_total_amount_ex_vat,
    v_total_amount_vat,
    v_total_amount_inc_vat
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  SELECT
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_ex_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_vat')::numeric ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN BTRIM(COALESCE(elem.value->>'amount_inc_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (elem.value->>'amount_inc_vat')::numeric ELSE 0 END), 0)
  INTO
    v_draftable_amount_ex_vat,
    v_draftable_amount_vat,
    v_draftable_amount_inc_vat
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND NOT (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
      OR COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );

  SELECT COUNT(*)::integer
  INTO v_payees_need_name_check
  FROM jsonb_array_elements(v_payees) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(elem.value->'blockers') = 'array' THEN COALESCE(elem.value->'blockers', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        ) AS blocker(value)
        WHERE UPPER(BTRIM(blocker.value)) = 'BLOCKED_NAME_CHECK'
      )
    );

  SELECT COUNT(*)::integer
  INTO v_payees_need_payee_map
  FROM jsonb_array_elements(v_payees) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(elem.value->'blockers') = 'array' THEN COALESCE(elem.value->'blockers', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        ) AS blocker(value)
        WHERE UPPER(BTRIM(blocker.value)) = 'BLOCKED_NO_PAYEE_MAP'
      )
    );

  SELECT COUNT(*)::integer
  INTO v_payees_missing_bank_details
  FROM jsonb_array_elements(v_payees) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      COALESCE(BTRIM(COALESCE(elem.value->>'bank_details_hash', '')), '') = ''
      OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(elem.value->'blockers') = 'array' THEN COALESCE(elem.value->'blockers', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        ) AS blocker(value)
        WHERE UPPER(BTRIM(blocker.value)) = 'BLOCKED_BANK_DETAILS'
      )
    );

  SELECT
    ROUND(COALESCE(SUM(
      CASE
        WHEN UPPER(BTRIM(COALESCE(elem.value->>'pay_channel', ''))) = 'PAYE'
          AND UPPER(BTRIM(COALESCE(elem.value->>'paye_treatment', ''))) = 'GROSS_ADD'
          AND COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_excluded_from_allocation', 'false'))), 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          AND BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN GREATEST((elem.value->>'amount_ex_vat')::numeric, 0)
        ELSE 0
      END
    ), 0), 2),
    ROUND(COALESCE(SUM(
      CASE
        WHEN UPPER(BTRIM(COALESCE(elem.value->>'pay_channel', ''))) = 'PAYE'
          AND UPPER(BTRIM(COALESCE(elem.value->>'paye_treatment', ''))) = 'GROSS_DEDUCT'
          AND COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_excluded_from_allocation', 'false'))), 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          AND BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ABS((elem.value->>'amount_ex_vat')::numeric)
        ELSE 0
      END
    ), 0), 2),
    ROUND(COALESCE(SUM(
      CASE
        WHEN UPPER(BTRIM(COALESCE(elem.value->>'pay_channel', ''))) = 'PAYE'
          AND UPPER(BTRIM(COALESCE(elem.value->>'paye_treatment', ''))) = 'NET_ADD'
          AND COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_excluded_from_allocation', 'false'))), 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          AND BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN GREATEST((elem.value->>'amount_ex_vat')::numeric, 0)
        ELSE 0
      END
    ), 0), 2),
    ROUND(COALESCE(SUM(
      CASE
        WHEN UPPER(BTRIM(COALESCE(elem.value->>'pay_channel', ''))) = 'PAYE'
          AND UPPER(BTRIM(COALESCE(elem.value->>'paye_treatment', ''))) = 'NET_DEDUCT'
          AND COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_excluded_from_allocation', 'false'))), 'false') NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          AND BTRIM(COALESCE(elem.value->>'amount_ex_vat', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ABS((elem.value->>'amount_ex_vat')::numeric)
        ELSE 0
      END
    ), 0), 2)
  INTO
    v_gross_side_additions_ex_vat,
    v_gross_side_deductions_ex_vat,
    v_net_side_additions_ex_vat,
    v_net_side_deductions_ex_vat
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object';

  v_paye_summary_breakdown := jsonb_build_object(
    'gross_side_additions_ex_vat', v_gross_side_additions_ex_vat,
    'gross_side_deductions_ex_vat', v_gross_side_deductions_ex_vat,
    'net_side_additions_ex_vat', v_net_side_additions_ex_vat,
    'net_side_deductions_ex_vat', v_net_side_deductions_ex_vat
  );

  v_summary := jsonb_build_object(
    'readiness', jsonb_build_object(
      'payees_total', v_payees_count,
      'payees_need_name_check', v_payees_need_name_check,
      'payees_need_payee_map', v_payees_need_payee_map,
      'payees_missing_bank_details', v_payees_missing_bank_details
    ),
    'candidates', jsonb_build_object(
      'ready_count', v_ready_candidates_count,
      'review_required_count', v_blocked_candidates_count,
      'total_candidates', v_candidate_count
    ),
    'candidate_count', v_candidate_count,
    'paye_candidates_count', v_paye_candidates_count,
    'non_paye_payees_count', v_non_paye_payees_count,
    'ready_candidates_count', v_ready_candidates_count,
    'blocked_candidates_count', v_blocked_candidates_count,
    'case_resolution_state_count', v_case_resolution_state_count,
    'blocked_case_state_count', v_blocked_case_state_count,
    'canonical_preview_line_count', v_canonical_preview_line_count,
    'ready_preview_line_count', v_ready_preview_line_count,
    'blocked_preview_line_count', v_blocked_preview_line_count,
    'do_not_pay_line_count', v_do_not_pay_line_count,
    'snoozed_line_count', v_snoozed_line_count,
    'payees_count', v_payees_count,
    'total_amount_ex_vat', v_total_amount_ex_vat,
    'total_amount_vat', v_total_amount_vat,
    'total_amount_inc_vat', v_total_amount_inc_vat,
    'draftable_amount_ex_vat', v_draftable_amount_ex_vat,
    'draftable_amount_vat', v_draftable_amount_vat,
    'draftable_amount_inc_vat', v_draftable_amount_inc_vat,
    'paye_breakdown', v_paye_summary_breakdown
  );

  RETURN jsonb_build_object(
    'pay_date', v_context_json->'pay_date',
    'pay_week_start', v_context_json->'pay_week_start',
    'week_ending_cutoff_date', v_context_json->'week_ending_cutoff_date',
    'eligibility', COALESCE(v_context_json->'eligibility', '{}'::jsonb),
    'filters', COALESCE(v_context_json->'filters', '{}'::jsonb),
    'finance', COALESCE(v_context_json->'finance', '{}'::jsonb),
    'settings', COALESCE(v_context_json->'settings', '{}'::jsonb),
    'summary', v_summary,
    'paye_guardrails', COALESCE(v_context_json->'paye_guardrails', '{}'::jsonb),
    'paye_summary_breakdown', v_paye_summary_breakdown,
    'payees', v_payees,
    'canonical_preview_lines', v_canonical_preview_lines,
    'case_resolution_states', v_case_resolution_states,
    'itemisation', v_itemisation,
    'paye_candidates', v_paye_candidates,
    'non_paye_payees', v_non_paye_payees,
    'blocked_items', v_blocked_items,
    'do_not_pay_items', v_do_not_pay_items,
    'snoozed_items', v_snoozed_items,
    'baseline_component_rows', v_baseline_component_rows
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_preview_apply_candidate_overlay(
  p_candidate_baseline_json jsonb,
  p_case_resolutions_json jsonb DEFAULT NULL::jsonb,
  p_overrides_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_candidate_baseline_root jsonb := coalesce(p_candidate_baseline_json, '{}'::jsonb);
  v_case_resolutions_input jsonb := coalesce(p_case_resolutions_json, '{}'::jsonb);
  v_overrides_input jsonb := coalesce(p_overrides_json, '{}'::jsonb);
  v_candidate_id text := '';
  v_candidate_row jsonb := '{}'::jsonb;
  v_summary_fragment jsonb := '{}'::jsonb;
  v_case_states jsonb := '[]'::jsonb;
  v_lines jsonb := '[]'::jsonb;
  v_payees jsonb := '[]'::jsonb;
  v_itemisation jsonb := '[]'::jsonb;
  v_blocked_items jsonb := '[]'::jsonb;
  v_do_not_pay_items jsonb := '[]'::jsonb;
  v_snoozed_items jsonb := '[]'::jsonb;
  v_baseline_component_rows jsonb := '[]'::jsonb;
  v_case_resolutions jsonb := '{}'::jsonb;
  v_exclude_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_map jsonb := '{}'::jsonb;
  v_bucket_applied_component_rows jsonb := '[]'::jsonb;
  v_final_component_rows jsonb := '[]'::jsonb;
  v_updated_case_states jsonb := '[]'::jsonb;
  v_case_state_map jsonb := '{}'::jsonb;
  v_updated_lines jsonb := '[]'::jsonb;
  v_updated_itemisation jsonb := '[]'::jsonb;
  v_case_resolution_entry record;
  v_case_resolution_json jsonb := '{}'::jsonb;
  v_resolution_case_key text := '';
  v_resolution_family text := '';
  v_resolve_all_linked_timesheets boolean := false;
  v_component_row jsonb := '{}'::jsonb;
  v_updated_component_row jsonb := '{}'::jsonb;
  v_component_scope text := '';
  v_component_case_key text := '';
  v_component_timesheet_id text := '';
  v_component_source_family_key text := '';
  v_component_key_type text := '';
  v_component_key_value text := '';
  v_component_bucket_code text := '';
  v_component_source_basis_fingerprint text := '';
  v_component_fingerprint text := '';
  v_component_source_units numeric := null::numeric;
  v_component_source_rate numeric := null::numeric;
  v_component_source_charge_rate numeric := null::numeric;
  v_case_state jsonb := '{}'::jsonb;
  v_line jsonb := '{}'::jsonb;
  v_case_state_case_key text := '';
  v_case_state_case_scope text := '';
  v_case_state_timesheet_id text := '';
  v_case_state_components jsonb := '[]'::jsonb;
  v_case_state_updated_components jsonb := '[]'::jsonb;
  v_case_state_updated jsonb := '{}'::jsonb;
  v_case_state_updated_components_count integer := 0;
  v_case_state_open_taxable_count integer := 0;
  v_case_state_open_reimbursement_count integer := 0;
  v_case_state_unresolved_taxable_count integer := 0;
  v_case_state_needs_resolution boolean := false;
  v_case_state_satisfied_now boolean := true;
  v_case_state_existing_blocked_reasons_count integer := 0;
  v_case_state_total_preview_amount_ex_vat numeric := 0;
  v_case_state_safe_amount_ex_vat numeric := 0;
  v_case_state_blocked_amount_ex_vat numeric := 0;
  v_case_state_unresolved_taxable_amount_ex_vat numeric := 0;
  v_case_state_taxable_manual_debt_resolution jsonb := null::jsonb;
  v_case_state_summary jsonb := '{}'::jsonb;
  v_template_base_line jsonb := null::jsonb;
  v_template_ready_line jsonb := null::jsonb;
  v_template_blocked_line jsonb := null::jsonb;
  v_template_do_not_pay_line jsonb := null::jsonb;
  v_case_ready_amount_ex_vat numeric := 0;
  v_case_blocked_amount_ex_vat numeric := 0;
  v_case_total_amount_ex_vat numeric := 0;
  v_case_is_excluded boolean := false;
  v_new_line jsonb := '{}'::jsonb;
  v_line_id_base text := '';
  v_line_preview_row_id text := '';
  v_item jsonb := '{}'::jsonb;
  v_component_match_found boolean := false;
  v_component_case_applies boolean := false;
  v_component_direct_case_match boolean := false;
  v_component_linked_case_match boolean := false;
  v_component_linked_ids jsonb := '[]'::jsonb;
  v_bucket_resolution_json jsonb := '{}'::jsonb;
  v_bucket_source_family_key text := '';
  v_bucket_component_key_type text := '';
  v_bucket_component_key_value text := '';
  v_bucket_source_basis_fingerprint text := '';
  v_bucket_bucket_code text := '';
  v_bucket_resolution_mode text := '';
  v_bucket_target_rate numeric := null::numeric;
  v_bucket_source_units numeric := null::numeric;
  v_bucket_source_rate numeric := null::numeric;
  v_bucket_source_charge_rate numeric := null::numeric;
  v_bucket_target_pay_ex_vat numeric := null::numeric;
  v_bucket_target_charge_ex_vat numeric := null::numeric;
  v_bucket_target_margin_ex_vat numeric := null::numeric;
  v_bucket_margin_delta_ex_vat numeric := null::numeric;
  v_nonbucket_resolution_entry record;
  v_nonbucket_resolution_mode text := '';
  v_nonbucket_target_amount_ex_vat numeric := null::numeric;
  v_nonbucket_case_row_count integer := 0;
  v_nonbucket_case_total_basis numeric := 0;
  v_nonbucket_case_allocated_so_far numeric := 0;
  v_nonbucket_case_row_index integer := 0;
  v_nonbucket_case_row_basis numeric := 0;
  v_nonbucket_case_row_target_amount_ex_vat numeric := 0;
  v_nonbucket_case_row_key text := '';
  v_nonbucket_case_row_match_key text := '';
  v_nonbucket_allocations jsonb := '{}'::jsonb;
  v_nonbucket_case_payload jsonb := '{}'::jsonb;
  v_nonbucket_case_row_allocations jsonb := '{}'::jsonb;
  v_component_lookup_key text := '';
  v_component_source_charge_ex_vat numeric := null::numeric;
  v_component_source_pay_ex_vat numeric := null::numeric;
  v_component_target_pay_ex_vat numeric := null::numeric;
  v_component_target_charge_ex_vat numeric := null::numeric;
  v_component_target_margin_ex_vat numeric := null::numeric;
  v_component_margin_delta_ex_vat numeric := null::numeric;
  v_component_preview_amount_ex_vat numeric := null::numeric;
  v_component_ready_preview_amount_ex_vat numeric := null::numeric;
  v_component_blocked_preview_amount_ex_vat numeric := null::numeric;
  v_component_requires_resolution boolean := false;
  v_component_is_actionable_resolution_row boolean := false;
  v_line_case_key text := '';
  v_line_case_scope text := '';
  v_line_timesheet_id text := '';
  v_line_presentation_section text := '';
  v_line_existing_blocked_reason_codes jsonb := '[]'::jsonb;
  v_line_existing_preview_row_id text := '';
  v_line_existing_line_id text := '';
  v_line_existing_presentation_parent_line_id text := '';
  v_excluded_timesheet_entry jsonb;
  v_excluded_timesheet_id text := '';
  v_case_state_component jsonb := '{}'::jsonb;
  v_component_requires_resolution_text text := '';
  v_component_preview_amount_text text := '';
  v_component_ready_amount_text text := '';
  v_component_blocked_amount_text text := '';
  v_case_state_exists boolean := false;
  v_case_state_from_map jsonb := '{}'::jsonb;
  v_case_state_summary_source jsonb := '{}'::jsonb;
  v_case_state_existing_due_amount_ex_vat numeric := 0;
  v_line_amount_ex_vat numeric := 0;
  v_do_not_pay_reason_codes jsonb := '[]'::jsonb;
  v_case_line_count integer := 0;
  v_case_ready_line_present boolean := false;
  v_case_blocked_line_present boolean := false;
  v_case_first_line jsonb := null::jsonb;
  v_case_component_row jsonb := '{}'::jsonb;
  v_component_order integer := 0;
  v_case_component_order integer := 0;
  v_component_sort_key text := '';
  v_case_scope_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_is_component_match boolean := false;
  v_line_blocked_reason_codes jsonb := '[]'::jsonb;
  v_existing_case_state_summary jsonb := '{}'::jsonb;
  v_nonbucket_resolution_applied boolean := false;
  v_nonbucket_case_allowed boolean := false;
  v_bucket_resolution_applied boolean := false;
begin
  if jsonb_typeof(v_candidate_baseline_root) <> 'object' then
    raise exception 'p_candidate_baseline_json must be a JSON object';
  end if;

  if jsonb_typeof(v_case_resolutions_input) <> 'object' then
    v_case_resolutions_input := '{}'::jsonb;
  end if;

  if jsonb_typeof(v_overrides_input) <> 'object' then
    v_overrides_input := '{}'::jsonb;
  end if;

  v_candidate_id := btrim(coalesce(
    v_candidate_baseline_root->>'candidate_id',
    v_candidate_baseline_root #>> '{candidate_row,candidate_id}',
    ''
  ));

  if v_candidate_id = '' then
    raise exception 'candidate_id is required on p_candidate_baseline_json';
  end if;

  v_candidate_row := case when jsonb_typeof(v_candidate_baseline_root->'candidate_row') = 'object' then coalesce(v_candidate_baseline_root->'candidate_row', '{}'::jsonb) else '{}'::jsonb end;
  v_summary_fragment := case when jsonb_typeof(v_candidate_baseline_root->'summary_fragment') = 'object' then coalesce(v_candidate_baseline_root->'summary_fragment', '{}'::jsonb) else '{}'::jsonb end;
  v_case_states := case when jsonb_typeof(v_candidate_baseline_root->'case_resolution_states') = 'array' then coalesce(v_candidate_baseline_root->'case_resolution_states', '[]'::jsonb) else '[]'::jsonb end;
  v_lines := case when jsonb_typeof(v_candidate_baseline_root->'canonical_preview_lines') = 'array' then coalesce(v_candidate_baseline_root->'canonical_preview_lines', '[]'::jsonb) else '[]'::jsonb end;
  v_payees := case when jsonb_typeof(v_candidate_baseline_root->'payees') = 'array' then coalesce(v_candidate_baseline_root->'payees', '[]'::jsonb) else '[]'::jsonb end;
  v_itemisation := case when jsonb_typeof(v_candidate_baseline_root->'itemisation') = 'array' then coalesce(v_candidate_baseline_root->'itemisation', '[]'::jsonb) else '[]'::jsonb end;
  v_blocked_items := case when jsonb_typeof(v_candidate_baseline_root->'blocked_items') = 'array' then coalesce(v_candidate_baseline_root->'blocked_items', '[]'::jsonb) else '[]'::jsonb end;
  v_do_not_pay_items := case when jsonb_typeof(v_candidate_baseline_root->'do_not_pay_items') = 'array' then coalesce(v_candidate_baseline_root->'do_not_pay_items', '[]'::jsonb) else '[]'::jsonb end;
  v_snoozed_items := case when jsonb_typeof(v_candidate_baseline_root->'snoozed_items') = 'array' then coalesce(v_candidate_baseline_root->'snoozed_items', '[]'::jsonb) else '[]'::jsonb end;
  v_baseline_component_rows := case when jsonb_typeof(v_candidate_baseline_root->'baseline_component_rows') = 'array' then coalesce(v_candidate_baseline_root->'baseline_component_rows', '[]'::jsonb) else '[]'::jsonb end;

  v_case_resolutions := case
    when jsonb_typeof(v_case_resolutions_input->'case_resolutions') = 'object' then coalesce(v_case_resolutions_input->'case_resolutions', '{}'::jsonb)
    when jsonb_typeof(v_case_resolutions_input) = 'object' then coalesce(v_case_resolutions_input, '{}'::jsonb)
    else '{}'::jsonb
  end;

  v_exclude_timesheet_ids := case
    when jsonb_typeof(v_overrides_input->'exclude_timesheet_ids') = 'array' then coalesce(v_overrides_input->'exclude_timesheet_ids', '[]'::jsonb)
    when jsonb_typeof(v_overrides_input->'timesheet_exclusions') = 'array' then coalesce(v_overrides_input->'timesheet_exclusions', '[]'::jsonb)
    else '[]'::jsonb
  end;

  for v_case_state in
    select elem.value
    from jsonb_array_elements(v_case_states) as elem(value)
    where jsonb_typeof(elem.value) = 'object'
  loop
    v_case_state_case_key := btrim(coalesce(v_case_state->>'case_key', ''));
    if v_case_state_case_key = '' then
      continue;
    end if;

    if upper(btrim(coalesce(v_case_state->>'case_scope', ''))) = 'TIMESHEET' then
      v_case_scope_linked_timesheet_ids := case
        when jsonb_typeof(v_case_state #> '{linked_resolution_scope_json,linked_timesheet_ids}') = 'array'
          then coalesce(v_case_state #> '{linked_resolution_scope_json,linked_timesheet_ids}', '[]'::jsonb)
        when nullif(btrim(coalesce(v_case_state->>'timesheet_id', '')), '') is not null
          then jsonb_build_array(v_case_state->>'timesheet_id')
        else '[]'::jsonb
      end;

      v_linked_timesheet_map := v_linked_timesheet_map || jsonb_build_object(v_case_state_case_key, v_case_scope_linked_timesheet_ids);
    end if;
  end loop;

  for v_component_row in
    select elem.value
    from jsonb_array_elements(v_baseline_component_rows) as elem(value)
    where jsonb_typeof(elem.value) = 'object'
  loop
    v_updated_component_row := v_component_row;
    v_bucket_resolution_applied := false;

    v_component_scope := upper(btrim(coalesce(v_component_row->>'component_scope', '')));
    v_component_case_key := btrim(coalesce(v_component_row->>'case_key', ''));
    v_component_timesheet_id := btrim(coalesce(v_component_row->>'timesheet_id', ''));
    v_component_source_family_key := btrim(coalesce(v_component_row->>'source_family_key', ''));
    v_component_key_type := upper(btrim(coalesce(v_component_row->>'component_key_type', '')));
    v_component_key_value := btrim(coalesce(v_component_row->>'component_key_value', ''));
    v_component_bucket_code := upper(btrim(coalesce(v_component_row->>'bucket_code', '')));
    v_component_source_basis_fingerprint := btrim(coalesce(v_component_row->>'source_basis_fingerprint', ''));
    v_component_fingerprint := btrim(coalesce(v_component_row->>'component_fingerprint', ''));

    v_component_source_units := case when coalesce(v_component_row->>'source_units', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_units')::numeric, 6) else null::numeric end;
    v_component_source_rate := case when coalesce(v_component_row->>'source_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_rate')::numeric, 6) else null::numeric end;
    v_component_source_charge_rate := case when coalesce(v_component_row->>'source_charge_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_charge_rate')::numeric, 6) else null::numeric end;
    v_component_source_charge_ex_vat := case when coalesce(v_component_row->>'source_charge_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_charge_ex_vat')::numeric, 2) else null::numeric end;
    v_component_source_pay_ex_vat := case when coalesce(v_component_row->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_pay_ex_vat')::numeric, 2) else null::numeric end;

    for v_case_resolution_entry in
      select preview_resolution.key as case_key, preview_resolution.value as resolution_json
      from jsonb_each(v_case_resolutions) as preview_resolution(key, value)
      where jsonb_typeof(preview_resolution.value) = 'object'
    loop
      v_case_resolution_json := v_case_resolution_entry.resolution_json;
      v_resolution_case_key := btrim(coalesce(v_case_resolution_json->>'case_key', v_case_resolution_entry.case_key, ''));
      v_resolution_family := upper(btrim(coalesce(v_case_resolution_json->>'resolution_family', '')));
      v_resolve_all_linked_timesheets := coalesce((v_case_resolution_json->>'resolve_all_linked_timesheets')::boolean, false);

      if v_resolution_family <> 'BUCKETED' then
        continue;
      end if;

      v_component_direct_case_match := (v_resolution_case_key <> '' and v_resolution_case_key = v_component_case_key);
      v_component_linked_case_match := false;

      if v_component_direct_case_match = false and v_resolve_all_linked_timesheets = true and v_component_scope = 'TIMESHEET' and v_component_timesheet_id <> '' then
        v_component_linked_ids := coalesce(v_linked_timesheet_map->v_resolution_case_key, '[]'::jsonb);
        if exists (
          select 1
          from jsonb_array_elements_text(v_component_linked_ids) as linked_id(value)
          where btrim(linked_id.value) = v_component_timesheet_id
        ) then
          v_component_linked_case_match := true;
        end if;
      end if;

      v_component_case_applies := (v_component_direct_case_match or v_component_linked_case_match);

      if v_component_case_applies = false then
        continue;
      end if;

      for v_bucket_resolution_json in
        select bucket_elem.value
        from jsonb_array_elements(coalesce(v_case_resolution_json->'bucket_resolutions', '[]'::jsonb)) as bucket_elem(value)
        where jsonb_typeof(bucket_elem.value) = 'object'
      loop
        v_bucket_source_family_key := btrim(coalesce(v_bucket_resolution_json->>'source_family_key', ''));
        v_bucket_component_key_type := upper(btrim(coalesce(v_bucket_resolution_json->>'component_key_type', '')));
        v_bucket_component_key_value := btrim(coalesce(v_bucket_resolution_json->>'component_key_value', ''));
        v_bucket_source_basis_fingerprint := btrim(coalesce(v_bucket_resolution_json->>'source_basis_fingerprint', ''));
        v_bucket_bucket_code := upper(btrim(coalesce(v_bucket_resolution_json->>'bucket_code', '')));
        v_bucket_resolution_mode := upper(btrim(coalesce(v_bucket_resolution_json->>'resolution_mode', '')));
        v_bucket_target_rate := case when coalesce(v_bucket_resolution_json->>'target_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_bucket_resolution_json->>'target_rate')::numeric, 6) else null::numeric end;
        v_bucket_source_units := case when coalesce(v_bucket_resolution_json->>'source_units', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_bucket_resolution_json->>'source_units')::numeric, 6) else null::numeric end;
        v_bucket_source_rate := case when coalesce(v_bucket_resolution_json->>'source_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_bucket_resolution_json->>'source_rate')::numeric, 6) else null::numeric end;
        v_bucket_source_charge_rate := case when coalesce(v_bucket_resolution_json->>'source_charge_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_bucket_resolution_json->>'source_charge_rate')::numeric, 6) else null::numeric end;

        v_is_component_match := false;

        if v_bucket_source_family_key = v_component_source_family_key
           and v_bucket_component_key_type = v_component_key_type
           and v_bucket_component_key_value = v_component_key_value then
          if v_bucket_source_basis_fingerprint <> '' then
            v_is_component_match := (v_bucket_source_basis_fingerprint = v_component_source_basis_fingerprint);
          else
            v_is_component_match := true;

            if v_bucket_bucket_code <> '' then
              v_is_component_match := v_is_component_match and (v_bucket_bucket_code = v_component_bucket_code);
            end if;

            if v_bucket_source_units is not null then
              v_is_component_match := v_is_component_match and (v_component_source_units is not null and round(v_component_source_units, 6) = v_bucket_source_units);
            end if;

            if v_bucket_source_rate is not null then
              v_is_component_match := v_is_component_match and (v_component_source_rate is not null and round(v_component_source_rate, 6) = v_bucket_source_rate);
            end if;

            if v_bucket_source_charge_rate is not null then
              v_is_component_match := v_is_component_match and (v_component_source_charge_rate is not null and round(v_component_source_charge_rate, 6) = v_bucket_source_charge_rate);
            end if;
          end if;
        end if;

        if v_is_component_match = false then
          continue;
        end if;

        if v_bucket_target_rate is null or v_component_source_units is null or v_component_source_units = 0 then
          continue;
        end if;

        v_bucket_target_pay_ex_vat := round(v_bucket_target_rate * v_component_source_units, 2);
        v_bucket_target_charge_ex_vat := v_component_source_charge_ex_vat;
        v_bucket_target_margin_ex_vat := case when v_bucket_target_charge_ex_vat is null then null::numeric else round(v_bucket_target_charge_ex_vat - v_bucket_target_pay_ex_vat, 2) end;
        v_bucket_margin_delta_ex_vat := case when v_component_source_pay_ex_vat is null then null::numeric else round(v_component_source_pay_ex_vat - v_bucket_target_pay_ex_vat, 2) end;

        v_updated_component_row := (v_updated_component_row
          - 'approved_resolution_mode'
          - 'approved_target_rate'
          - 'requires_resolution'
          - 'case_resolution_satisfied_now_component'
          - 'suggestion_provenance'
          - 'is_fresh_suggested_resolution'
          - 'preview_component_amount_ex_vat'
          - 'ready_preview_amount_ex_vat'
          - 'blocked_preview_amount_ex_vat'
          - 'target_pay_ex_vat'
          - 'target_charge_ex_vat'
          - 'target_margin_ex_vat'
          - 'margin_delta_ex_vat'
          - 'resolution_state'
          - 'target_rate')
          || jsonb_build_object(
            'approved_resolution_mode', v_bucket_resolution_mode,
            'approved_target_rate', round(v_bucket_target_rate, 6),
            'requires_resolution', false,
            'case_resolution_satisfied_now_component', true,
            'suggestion_provenance', 'PREVIEW_CASE_RESOLUTION',
            'is_fresh_suggested_resolution', false,
            'preview_component_amount_ex_vat', v_bucket_target_pay_ex_vat,
            'ready_preview_amount_ex_vat', v_bucket_target_pay_ex_vat,
            'blocked_preview_amount_ex_vat', 0,
            'target_pay_ex_vat', v_bucket_target_pay_ex_vat,
            'target_charge_ex_vat', v_bucket_target_charge_ex_vat,
            'target_margin_ex_vat', v_bucket_target_margin_ex_vat,
            'margin_delta_ex_vat', v_bucket_margin_delta_ex_vat,
            'resolution_state', 'RESOLVED',
            'target_rate', round(v_bucket_target_rate, 6)
          );

        v_bucket_resolution_applied := true;
        exit;
      end loop;

      if v_bucket_resolution_applied = true then
        exit;
      end if;
    end loop;

    v_bucket_applied_component_rows := v_bucket_applied_component_rows || jsonb_build_array(v_updated_component_row);
  end loop;

  v_nonbucket_allocations := '{}'::jsonb;

  for v_nonbucket_resolution_entry in
    select preview_resolution.key as case_key, preview_resolution.value as resolution_json
    from jsonb_each(v_case_resolutions) as preview_resolution(key, value)
    where jsonb_typeof(preview_resolution.value) = 'object'
  loop
    v_case_resolution_json := v_nonbucket_resolution_entry.resolution_json;
    v_resolution_case_key := btrim(coalesce(v_case_resolution_json->>'case_key', v_nonbucket_resolution_entry.case_key, ''));
    v_resolution_family := upper(btrim(coalesce(v_case_resolution_json->>'resolution_family', '')));

    if v_resolution_family <> 'NON_BUCKET' or v_resolution_case_key = '' then
      continue;
    end if;

    select exists (
      select 1
      from jsonb_array_elements(v_case_states) as case_state_elem(value)
      where jsonb_typeof(case_state_elem.value) = 'object'
        and btrim(coalesce(case_state_elem.value->>'case_key', '')) = v_resolution_case_key
        and upper(btrim(coalesce(case_state_elem.value->>'case_scope', ''))) = 'FINANCE_CASE'
        and upper(btrim(coalesce(case_state_elem.value->>'case_type', ''))) = 'MANUAL_DEBT_ADJUSTMENT'
    ) into v_nonbucket_case_allowed;

    if v_nonbucket_case_allowed = false then
      continue;
    end if;

    v_nonbucket_resolution_mode := upper(btrim(coalesce(v_case_resolution_json->>'resolution_mode', '')));
    v_nonbucket_target_amount_ex_vat := case when coalesce(v_case_resolution_json->>'target_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_case_resolution_json->>'target_amount_ex_vat')::numeric, 2) else null::numeric end;

    if v_nonbucket_target_amount_ex_vat is null then
      continue;
    end if;

    v_nonbucket_case_row_count := 0;
    v_nonbucket_case_total_basis := 0;

    for v_component_row in
      select elem.value
      from jsonb_array_elements(v_bucket_applied_component_rows) as elem(value)
      where jsonb_typeof(elem.value) = 'object'
    loop
      if btrim(coalesce(v_component_row->>'case_key', '')) = v_resolution_case_key
         and upper(btrim(coalesce(v_component_row->>'component_scope', ''))) = 'FINANCE_CASE'
         and upper(btrim(coalesce(v_component_row->>'classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE' then
        v_nonbucket_case_row_count := v_nonbucket_case_row_count + 1;
        v_nonbucket_case_row_basis := case
          when coalesce(v_component_row->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'remaining_source_amount')::numeric, 2)
          when coalesce(v_component_row->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_amount')::numeric, 2)
          when coalesce(v_component_row->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'component_amount_ex_vat')::numeric, 2)
          else 0::numeric
        end;
        v_nonbucket_case_total_basis := v_nonbucket_case_total_basis + greatest(v_nonbucket_case_row_basis, 0);
      end if;
    end loop;

    if v_nonbucket_case_row_count <= 0 then
      continue;
    end if;

    v_nonbucket_case_row_index := 0;
    v_nonbucket_case_allocated_so_far := 0;
    v_nonbucket_case_row_allocations := '{}'::jsonb;

    for v_component_row in
      select elem.value
      from jsonb_array_elements(v_bucket_applied_component_rows) as elem(value)
      where jsonb_typeof(elem.value) = 'object'
    loop
      if btrim(coalesce(v_component_row->>'case_key', '')) = v_resolution_case_key
         and upper(btrim(coalesce(v_component_row->>'component_scope', ''))) = 'FINANCE_CASE'
         and upper(btrim(coalesce(v_component_row->>'classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE' then
        v_nonbucket_case_row_index := v_nonbucket_case_row_index + 1;
        v_nonbucket_case_row_basis := case
          when coalesce(v_component_row->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'remaining_source_amount')::numeric, 2)
          when coalesce(v_component_row->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_amount')::numeric, 2)
          when coalesce(v_component_row->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'component_amount_ex_vat')::numeric, 2)
          else 0::numeric
        end;

        if v_nonbucket_case_row_index < v_nonbucket_case_row_count and v_nonbucket_case_total_basis > 0 then
          v_nonbucket_case_row_target_amount_ex_vat := round(v_nonbucket_target_amount_ex_vat * (greatest(v_nonbucket_case_row_basis, 0) / v_nonbucket_case_total_basis), 2);
        elsif v_nonbucket_case_row_index < v_nonbucket_case_row_count then
          v_nonbucket_case_row_target_amount_ex_vat := 0;
        else
          v_nonbucket_case_row_target_amount_ex_vat := round(v_nonbucket_target_amount_ex_vat - v_nonbucket_case_allocated_so_far, 2);
        end if;

        v_nonbucket_case_allocated_so_far := v_nonbucket_case_allocated_so_far + v_nonbucket_case_row_target_amount_ex_vat;
        v_nonbucket_case_row_key := btrim(coalesce(v_component_row->>'component_fingerprint', ''));
        if v_nonbucket_case_row_key = '' then
          v_nonbucket_case_row_key := btrim(coalesce(v_component_row->>'finance_component_id', ''));
        end if;
        if v_nonbucket_case_row_key = '' then
          v_nonbucket_case_row_key := btrim(coalesce(v_component_row->>'component_key_type', '')) || '|' || btrim(coalesce(v_component_row->>'component_key_value', '')) || '|' || v_nonbucket_case_row_index::text;
        end if;

        v_nonbucket_case_row_allocations := v_nonbucket_case_row_allocations || jsonb_build_object(v_nonbucket_case_row_key, jsonb_build_object(
          'resolution_mode', v_nonbucket_resolution_mode,
          'target_amount_ex_vat', v_nonbucket_case_row_target_amount_ex_vat
        ));
      end if;
    end loop;

    v_nonbucket_allocations := v_nonbucket_allocations || jsonb_build_object(v_resolution_case_key, v_nonbucket_case_row_allocations);
  end loop;

  for v_component_row in
    select elem.value
    from jsonb_array_elements(v_bucket_applied_component_rows) as elem(value)
    where jsonb_typeof(elem.value) = 'object'
  loop
    v_updated_component_row := v_component_row;
    v_nonbucket_resolution_applied := false;
    v_resolution_case_key := btrim(coalesce(v_component_row->>'case_key', ''));

    if jsonb_typeof(v_nonbucket_allocations->v_resolution_case_key) = 'object' then
      v_component_lookup_key := btrim(coalesce(v_component_row->>'component_fingerprint', ''));
      if v_component_lookup_key = '' then
        v_component_lookup_key := btrim(coalesce(v_component_row->>'finance_component_id', ''));
      end if;

      if v_component_lookup_key <> '' and jsonb_typeof((v_nonbucket_allocations->v_resolution_case_key)->v_component_lookup_key) = 'object' then
        v_nonbucket_case_payload := (v_nonbucket_allocations->v_resolution_case_key)->v_component_lookup_key;
        v_nonbucket_resolution_mode := upper(btrim(coalesce(v_nonbucket_case_payload->>'resolution_mode', '')));
        v_nonbucket_target_amount_ex_vat := case when coalesce(v_nonbucket_case_payload->>'target_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_nonbucket_case_payload->>'target_amount_ex_vat')::numeric, 2) else null::numeric end;

        if v_nonbucket_target_amount_ex_vat is not null then
          v_component_target_charge_ex_vat := case when coalesce(v_component_row->>'source_charge_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_charge_ex_vat')::numeric, 2) else null::numeric end;
          v_component_target_margin_ex_vat := case when v_component_target_charge_ex_vat is null then null::numeric else round(v_component_target_charge_ex_vat - v_nonbucket_target_amount_ex_vat, 2) end;
          v_component_margin_delta_ex_vat := case when coalesce(v_component_row->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_component_row->>'source_pay_ex_vat')::numeric - v_nonbucket_target_amount_ex_vat, 2) else null::numeric end;

          v_updated_component_row := (v_updated_component_row
            - 'approved_nonbucket_resolution_mode'
            - 'approved_nonbucket_target_amount_ex_vat'
            - 'requires_resolution'
            - 'case_resolution_satisfied_now_component'
            - 'suggestion_provenance'
            - 'is_fresh_suggested_resolution'
            - 'preview_component_amount_ex_vat'
            - 'ready_preview_amount_ex_vat'
            - 'blocked_preview_amount_ex_vat'
            - 'target_pay_ex_vat'
            - 'target_charge_ex_vat'
            - 'target_margin_ex_vat'
            - 'margin_delta_ex_vat'
            - 'resolution_state')
            || jsonb_build_object(
              'approved_nonbucket_resolution_mode', v_nonbucket_resolution_mode,
              'approved_nonbucket_target_amount_ex_vat', v_nonbucket_target_amount_ex_vat,
              'requires_resolution', false,
              'case_resolution_satisfied_now_component', true,
              'suggestion_provenance', 'PREVIEW_CASE_RESOLUTION',
              'is_fresh_suggested_resolution', false,
              'preview_component_amount_ex_vat', v_nonbucket_target_amount_ex_vat,
              'ready_preview_amount_ex_vat', v_nonbucket_target_amount_ex_vat,
              'blocked_preview_amount_ex_vat', 0,
              'target_pay_ex_vat', v_nonbucket_target_amount_ex_vat,
              'target_charge_ex_vat', v_component_target_charge_ex_vat,
              'target_margin_ex_vat', v_component_target_margin_ex_vat,
              'margin_delta_ex_vat', v_component_margin_delta_ex_vat,
              'resolution_state', 'RESOLVED'
            );

          v_nonbucket_resolution_applied := true;
        end if;
      end if;
    end if;

    v_final_component_rows := v_final_component_rows || jsonb_build_array(v_updated_component_row);
  end loop;

  v_updated_case_states := '[]'::jsonb;
  v_case_state_map := '{}'::jsonb;

  for v_case_state in
    select elem.value
    from jsonb_array_elements(v_case_states) as elem(value)
    where jsonb_typeof(elem.value) = 'object'
  loop
    v_case_state_case_key := btrim(coalesce(v_case_state->>'case_key', ''));
    v_case_state_case_scope := upper(btrim(coalesce(v_case_state->>'case_scope', '')));
    v_case_state_timesheet_id := btrim(coalesce(v_case_state->>'timesheet_id', ''));
    v_case_state_updated_components := '[]'::jsonb;
    v_case_state_updated_components_count := 0;
    v_case_state_open_taxable_count := 0;
    v_case_state_open_reimbursement_count := 0;
    v_case_state_unresolved_taxable_count := 0;
    v_case_state_total_preview_amount_ex_vat := 0;
    v_case_state_safe_amount_ex_vat := 0;
    v_case_state_blocked_amount_ex_vat := 0;
    v_case_state_unresolved_taxable_amount_ex_vat := 0;

    for v_case_component_row in
      select elem.value
      from jsonb_array_elements(v_final_component_rows) as elem(value)
      where jsonb_typeof(elem.value) = 'object'
    loop
      if btrim(coalesce(v_case_component_row->>'case_key', '')) = v_case_state_case_key then
        v_case_state_updated_components := v_case_state_updated_components || jsonb_build_array(v_case_component_row);
        v_case_state_updated_components_count := v_case_state_updated_components_count + 1;

        if upper(btrim(coalesce(v_case_component_row->>'classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE' then
          v_case_state_open_taxable_count := v_case_state_open_taxable_count + 1;
        elsif upper(btrim(coalesce(v_case_component_row->>'classification', ''))) = 'REIMBURSEMENT_GROSS_FIXED' then
          v_case_state_open_reimbursement_count := v_case_state_open_reimbursement_count + 1;
        end if;

        if coalesce(v_case_component_row->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
          v_case_state_total_preview_amount_ex_vat := v_case_state_total_preview_amount_ex_vat + round((v_case_component_row->>'preview_component_amount_ex_vat')::numeric, 2);
        elsif coalesce(v_case_component_row->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
          v_case_state_total_preview_amount_ex_vat := v_case_state_total_preview_amount_ex_vat + round((v_case_component_row->>'component_amount_ex_vat')::numeric, 2);
        end if;

        if coalesce(v_case_component_row->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
          v_case_state_safe_amount_ex_vat := v_case_state_safe_amount_ex_vat + round((v_case_component_row->>'ready_preview_amount_ex_vat')::numeric, 2);
        elsif coalesce(v_case_component_row->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
          v_case_state_safe_amount_ex_vat := v_case_state_safe_amount_ex_vat + round((v_case_component_row->>'preview_component_amount_ex_vat')::numeric, 2);
        elsif coalesce(v_case_component_row->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
          v_case_state_safe_amount_ex_vat := v_case_state_safe_amount_ex_vat + round((v_case_component_row->>'component_amount_ex_vat')::numeric, 2);
        end if;

        v_component_requires_resolution := coalesce((v_case_component_row->>'requires_resolution')::boolean, false);
        if v_component_requires_resolution then
          v_case_state_unresolved_taxable_count := v_case_state_unresolved_taxable_count + 1;
          if coalesce(v_case_component_row->>'blocked_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
            v_case_state_unresolved_taxable_amount_ex_vat := v_case_state_unresolved_taxable_amount_ex_vat + round((v_case_component_row->>'blocked_preview_amount_ex_vat')::numeric, 2);
          elsif coalesce(v_case_component_row->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
            v_case_state_unresolved_taxable_amount_ex_vat := v_case_state_unresolved_taxable_amount_ex_vat + round((v_case_component_row->>'preview_component_amount_ex_vat')::numeric, 2);
          elsif coalesce(v_case_component_row->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
            v_case_state_unresolved_taxable_amount_ex_vat := v_case_state_unresolved_taxable_amount_ex_vat + round((v_case_component_row->>'component_amount_ex_vat')::numeric, 2);
          end if;
        end if;
      end if;
    end loop;

    v_case_state_needs_resolution := (v_case_state_unresolved_taxable_count > 0);
    v_case_state_satisfied_now := not v_case_state_needs_resolution;
    if v_case_state_needs_resolution then
      v_case_state_blocked_amount_ex_vat := round(v_case_state_total_preview_amount_ex_vat, 2);
      v_case_state_safe_amount_ex_vat := 0;
    else
      v_case_state_blocked_amount_ex_vat := 0;
      v_case_state_safe_amount_ex_vat := round(v_case_state_safe_amount_ex_vat, 2);
    end if;

    v_case_is_excluded := false;
    if v_case_state_case_scope = 'TIMESHEET' and v_case_state_timesheet_id <> '' then
      if exists (
        select 1
        from jsonb_array_elements(v_exclude_timesheet_ids) as excluded(value)
        where btrim(trim(both '"' from excluded.value::text)) = v_case_state_timesheet_id
      ) then
        v_case_is_excluded := true;
      end if;
    end if;

    if v_case_is_excluded then
      v_case_state_needs_resolution := false;
      v_case_state_satisfied_now := true;
      v_case_state_unresolved_taxable_count := 0;
      v_case_state_unresolved_taxable_amount_ex_vat := 0;
      v_case_state_blocked_amount_ex_vat := 0;
      v_case_state_safe_amount_ex_vat := round(v_case_state_total_preview_amount_ex_vat, 2);
    end if;

    select count(*)::int
    into v_case_state_existing_blocked_reasons_count
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(v_case_state->'blocked_reason_codes') = 'array' then coalesce(v_case_state->'blocked_reason_codes', '[]'::jsonb)
        else '[]'::jsonb
      end
    ) as blocker(value);

    v_existing_case_state_summary := case when jsonb_typeof(v_case_state->'case_resolution_summary') = 'object' then coalesce(v_case_state->'case_resolution_summary', '{}'::jsonb) else '{}'::jsonb end;

    v_case_state_summary := (v_existing_case_state_summary
      - 'case_needs_resolution'
      - 'case_resolution_satisfied_now'
      - 'safe_amount_ex_vat'
      - 'blocked_case_amount_ex_vat'
      - 'unresolved_taxable_amount_ex_vat'
      - 'open_taxable_count'
      - 'open_reimbursement_count'
      - 'unresolved_taxable_count'
      - 'is_blocked'
      - 'excluded_from_pay')
      || jsonb_build_object(
        'case_needs_resolution', v_case_state_needs_resolution,
        'case_resolution_satisfied_now', v_case_state_satisfied_now,
        'safe_amount_ex_vat', round(v_case_state_safe_amount_ex_vat, 2),
        'blocked_case_amount_ex_vat', round(v_case_state_blocked_amount_ex_vat, 2),
        'unresolved_taxable_amount_ex_vat', round(v_case_state_unresolved_taxable_amount_ex_vat, 2),
        'open_taxable_count', v_case_state_open_taxable_count,
        'open_reimbursement_count', v_case_state_open_reimbursement_count,
        'unresolved_taxable_count', v_case_state_unresolved_taxable_count,
        'is_blocked', (case when v_case_is_excluded then false else (v_case_state_needs_resolution or v_case_state_existing_blocked_reasons_count > 0) end),
        'excluded_from_pay', v_case_is_excluded
      );

    v_case_state_taxable_manual_debt_resolution := case when jsonb_typeof(v_case_state->'taxable_manual_debt_resolution') = 'object' then coalesce(v_case_state->'taxable_manual_debt_resolution', '{}'::jsonb) else null::jsonb end;
    if v_case_state_taxable_manual_debt_resolution is not null and jsonb_typeof(v_case_state_taxable_manual_debt_resolution) = 'object' then
      if exists (
        select 1
        from jsonb_array_elements(v_case_state_updated_components) as comp(value)
        where jsonb_typeof(comp.value) = 'object'
          and coalesce(comp.value->>'approved_nonbucket_resolution_mode', '') <> ''
      ) then
        select upper(btrim(coalesce(comp.value->>'approved_nonbucket_resolution_mode', ''))),
               case when coalesce(comp.value->>'approved_nonbucket_target_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((comp.value->>'approved_nonbucket_target_amount_ex_vat')::numeric, 2) else null::numeric end
        into v_nonbucket_resolution_mode, v_nonbucket_target_amount_ex_vat
        from jsonb_array_elements(v_case_state_updated_components) as comp(value)
        where jsonb_typeof(comp.value) = 'object'
          and coalesce(comp.value->>'approved_nonbucket_resolution_mode', '') <> ''
        limit 1;

        v_case_state_taxable_manual_debt_resolution := v_case_state_taxable_manual_debt_resolution
          || jsonb_build_object(
            'approved_resolution_mode', v_nonbucket_resolution_mode,
            'approved_target_amount_ex_vat', v_nonbucket_target_amount_ex_vat,
            'case_resolution_satisfied_now', v_case_state_satisfied_now
          );
      end if;
    end if;

    v_case_state_updated := (v_case_state
      - 'components'
      - 'case_needs_resolution'
      - 'case_resolution_satisfied_now'
      - 'is_blocked'
      - 'open_taxable_count'
      - 'open_reimbursement_count'
      - 'unresolved_taxable_count'
      - 'safe_amount_ex_vat'
      - 'blocked_case_amount_ex_vat'
      - 'unresolved_taxable_amount_ex_vat'
      - 'case_resolution_summary'
      - 'taxable_manual_debt_resolution'
      - 'excluded_from_pay')
      || jsonb_build_object(
        'components', coalesce(v_case_state_updated_components, '[]'::jsonb),
        'case_needs_resolution', v_case_state_needs_resolution,
        'case_resolution_satisfied_now', v_case_state_satisfied_now,
        'is_blocked', (case when v_case_is_excluded then false else (v_case_state_needs_resolution or v_case_state_existing_blocked_reasons_count > 0) end),
        'open_taxable_count', v_case_state_open_taxable_count,
        'open_reimbursement_count', v_case_state_open_reimbursement_count,
        'unresolved_taxable_count', v_case_state_unresolved_taxable_count,
        'safe_amount_ex_vat', round(v_case_state_safe_amount_ex_vat, 2),
        'blocked_case_amount_ex_vat', round(v_case_state_blocked_amount_ex_vat, 2),
        'unresolved_taxable_amount_ex_vat', round(v_case_state_unresolved_taxable_amount_ex_vat, 2),
        'case_resolution_summary', v_case_state_summary,
        'excluded_from_pay', v_case_is_excluded
      );

    if v_case_state_taxable_manual_debt_resolution is not null and jsonb_typeof(v_case_state_taxable_manual_debt_resolution) = 'object' then
      v_case_state_updated := v_case_state_updated || jsonb_build_object('taxable_manual_debt_resolution', v_case_state_taxable_manual_debt_resolution);
    end if;

    if coalesce(v_case_state_updated->>'due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
      v_case_state_updated := v_case_state_updated || jsonb_build_object('due_amount_ex_vat', round(v_case_state_total_preview_amount_ex_vat, 2));
    end if;

    if coalesce(v_case_state_updated->>'payment_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then
      v_case_state_updated := v_case_state_updated || jsonb_build_object('payment_amount_ex_vat', round(v_case_state_total_preview_amount_ex_vat, 2));
    end if;

    v_updated_case_states := v_updated_case_states || jsonb_build_array(v_case_state_updated);
    if v_case_state_case_key <> '' then
      v_case_state_map := v_case_state_map || jsonb_build_object(v_case_state_case_key, v_case_state_updated);
    end if;
  end loop;

  v_updated_lines := '[]'::jsonb;

  for v_case_state in
    select elem.value
    from jsonb_array_elements(v_updated_case_states) as elem(value)
    where jsonb_typeof(elem.value) = 'object'
  loop
    v_case_state_case_key := btrim(coalesce(v_case_state->>'case_key', ''));
    v_case_state_case_scope := upper(btrim(coalesce(v_case_state->>'case_scope', '')));
    v_case_state_timesheet_id := btrim(coalesce(v_case_state->>'timesheet_id', ''));
    v_case_ready_amount_ex_vat := case when coalesce(v_case_state->>'safe_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_case_state->>'safe_amount_ex_vat')::numeric, 2) else 0::numeric end;
    v_case_blocked_amount_ex_vat := case when coalesce(v_case_state->>'blocked_case_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((v_case_state->>'blocked_case_amount_ex_vat')::numeric, 2) else 0::numeric end;
    v_case_total_amount_ex_vat := round(v_case_ready_amount_ex_vat + v_case_blocked_amount_ex_vat, 2);

    v_case_is_excluded := false;
    if v_case_state_case_scope = 'TIMESHEET' and v_case_state_timesheet_id <> '' then
      if exists (
        select 1
        from jsonb_array_elements(v_exclude_timesheet_ids) as excluded(value)
        where btrim(trim(both '"' from excluded.value::text)) = v_case_state_timesheet_id
      ) then
        v_case_is_excluded := true;
      end if;
    end if;

    v_template_base_line := null::jsonb;
    v_template_ready_line := null::jsonb;
    v_template_blocked_line := null::jsonb;
    v_template_do_not_pay_line := null::jsonb;

    for v_line in
      select elem.value
      from jsonb_array_elements(v_lines) as elem(value)
      where jsonb_typeof(elem.value) = 'object'
    loop
      if btrim(coalesce(v_line->>'case_key', '')) = v_case_state_case_key then
        if v_template_base_line is null then
          v_template_base_line := v_line;
        end if;

        if upper(btrim(coalesce(v_line->>'presentation_section', ''))) = 'READY_TO_PAY' and v_template_ready_line is null then
          v_template_ready_line := v_line;
        end if;

        if upper(btrim(coalesce(v_line->>'presentation_section', ''))) = 'BLOCKED_FOR_PAY' and v_template_blocked_line is null then
          v_template_blocked_line := v_line;
        end if;

        if upper(btrim(coalesce(v_line->>'presentation_section', ''))) = 'DO_NOT_PAY' and v_template_do_not_pay_line is null then
          v_template_do_not_pay_line := v_line;
        end if;
      end if;
    end loop;

    if v_template_base_line is null then
      continue;
    end if;

    if v_template_ready_line is null then
      v_template_ready_line := v_template_base_line;
    end if;
    if v_template_blocked_line is null then
      v_template_blocked_line := v_template_base_line;
    end if;
    if v_template_do_not_pay_line is null then
      v_template_do_not_pay_line := v_template_base_line;
    end if;

    if v_case_is_excluded = true and round(v_case_total_amount_ex_vat, 2) <> 0 then
      v_line_existing_line_id := btrim(coalesce(v_template_do_not_pay_line->>'line_id', v_case_state_case_key));
      if v_line_existing_line_id = '' then
        v_line_existing_line_id := v_case_state_case_key;
      end if;
      if right(v_line_existing_line_id, 11) <> ':do_not_pay' then
        v_line_existing_line_id := v_line_existing_line_id || ':do_not_pay';
      end if;

      v_do_not_pay_reason_codes := case
        when jsonb_typeof(v_template_do_not_pay_line->'blocked_reason_codes') = 'array' then coalesce(v_template_do_not_pay_line->'blocked_reason_codes', '[]'::jsonb)
        else '[]'::jsonb
      end;
      if not exists (
        select 1
        from jsonb_array_elements_text(v_do_not_pay_reason_codes) as blocker(value)
        where upper(btrim(blocker.value)) = 'DO_NOT_PAY'
      ) then
        v_do_not_pay_reason_codes := v_do_not_pay_reason_codes || jsonb_build_array('DO_NOT_PAY');
      end if;

      v_new_line := (v_template_do_not_pay_line
        - 'preview_row_id'
        - 'line_id'
        - 'presentation_line_id'
        - 'presentation_section'
        - 'presentation_reason'
        - 'readiness_state'
        - 'draftable'
        - 'is_ready_for_draft'
        - 'is_do_not_pay'
        - 'is_excluded_from_allocation'
        - 'blocked_reason_codes'
        - 'amount_ex_vat'
        - 'amount_display'
        - 'case_resolution_summary'
        - 'case_components'
        - 'taxable_manual_debt_resolution')
        || jsonb_build_object(
          'preview_row_id', v_line_existing_line_id,
          'line_id', v_line_existing_line_id,
          'presentation_line_id', v_line_existing_line_id,
          'presentation_section', 'DO_NOT_PAY',
          'presentation_reason', 'SESSION_TIMESHEET_EXCLUSION',
          'readiness_state', 'DO_NOT_PAY',
          'draftable', false,
          'is_ready_for_draft', false,
          'is_do_not_pay', true,
          'is_excluded_from_allocation', true,
          'blocked_reason_codes', v_do_not_pay_reason_codes,
          'amount_ex_vat', round(v_case_total_amount_ex_vat, 2),
          'amount_display', round(v_case_total_amount_ex_vat, 2),
          'case_resolution_summary', coalesce(v_case_state->'case_resolution_summary', '{}'::jsonb),
          'case_components', coalesce(v_case_state->'components', '[]'::jsonb)
        );

      if jsonb_typeof(v_case_state->'taxable_manual_debt_resolution') = 'object' then
        v_new_line := v_new_line || jsonb_build_object('taxable_manual_debt_resolution', v_case_state->'taxable_manual_debt_resolution');
      end if;

      v_updated_lines := v_updated_lines || jsonb_build_array(v_new_line);
    else
      if round(v_case_ready_amount_ex_vat, 2) <> 0 then
        v_line_existing_line_id := btrim(coalesce(v_template_ready_line->>'line_id', v_case_state_case_key));
        if v_line_existing_line_id = '' then
          v_line_existing_line_id := v_case_state_case_key;
        end if;

        v_new_line := (v_template_ready_line
          - 'preview_row_id'
          - 'line_id'
          - 'presentation_line_id'
          - 'presentation_section'
          - 'presentation_reason'
          - 'readiness_state'
          - 'draftable'
          - 'is_ready_for_draft'
          - 'is_do_not_pay'
          - 'is_excluded_from_allocation'
          - 'blocked_reason_codes'
          - 'amount_ex_vat'
          - 'amount_display'
          - 'case_resolution_summary'
          - 'case_components'
          - 'taxable_manual_debt_resolution')
          || jsonb_build_object(
            'preview_row_id', v_line_existing_line_id,
            'line_id', v_line_existing_line_id,
            'presentation_line_id', v_line_existing_line_id,
            'presentation_section', 'READY_TO_PAY',
            'presentation_reason', 'READY_TO_PAY',
            'readiness_state', 'READY_TO_PAY',
            'draftable', true,
            'is_ready_for_draft', true,
            'is_do_not_pay', false,
            'is_excluded_from_allocation', false,
            'blocked_reason_codes', '[]'::jsonb,
            'amount_ex_vat', round(v_case_ready_amount_ex_vat, 2),
            'amount_display', round(v_case_ready_amount_ex_vat, 2),
            'case_resolution_summary', coalesce(v_case_state->'case_resolution_summary', '{}'::jsonb),
            'case_components', coalesce(v_case_state->'components', '[]'::jsonb)
          );

        if jsonb_typeof(v_case_state->'taxable_manual_debt_resolution') = 'object' then
          v_new_line := v_new_line || jsonb_build_object('taxable_manual_debt_resolution', v_case_state->'taxable_manual_debt_resolution');
        end if;

        v_updated_lines := v_updated_lines || jsonb_build_array(v_new_line);
      end if;

      if round(v_case_blocked_amount_ex_vat, 2) <> 0 then
        v_line_existing_line_id := btrim(coalesce(v_template_blocked_line->>'line_id', v_case_state_case_key));
        if v_line_existing_line_id = '' then
          v_line_existing_line_id := v_case_state_case_key || ':blocked';
        end if;
        if v_line_existing_line_id = btrim(coalesce(v_template_ready_line->>'line_id', '')) then
          v_line_existing_line_id := v_line_existing_line_id || ':blocked';
        end if;

        v_line_blocked_reason_codes := case
          when jsonb_typeof(v_template_blocked_line->'blocked_reason_codes') = 'array' then coalesce(v_template_blocked_line->'blocked_reason_codes', '[]'::jsonb)
          else '[]'::jsonb
        end;
        if not exists (
          select 1
          from jsonb_array_elements_text(v_line_blocked_reason_codes) as blocker(value)
          where upper(btrim(blocker.value)) = 'RESOLUTION_REQUIRED'
        ) then
          v_line_blocked_reason_codes := v_line_blocked_reason_codes || jsonb_build_array('RESOLUTION_REQUIRED');
        end if;

        v_new_line := (v_template_blocked_line
          - 'preview_row_id'
          - 'line_id'
          - 'presentation_line_id'
          - 'presentation_section'
          - 'presentation_reason'
          - 'readiness_state'
          - 'draftable'
          - 'is_ready_for_draft'
          - 'is_do_not_pay'
          - 'is_excluded_from_allocation'
          - 'blocked_reason_codes'
          - 'amount_ex_vat'
          - 'amount_display'
          - 'case_resolution_summary'
          - 'case_components'
          - 'taxable_manual_debt_resolution')
          || jsonb_build_object(
            'preview_row_id', v_line_existing_line_id,
            'line_id', v_line_existing_line_id,
            'presentation_line_id', v_line_existing_line_id,
            'presentation_section', 'BLOCKED_FOR_PAY',
            'presentation_reason', 'CASE_BLOCKED',
            'readiness_state', 'BLOCKED_FOR_PAY',
            'draftable', false,
            'is_ready_for_draft', false,
            'is_do_not_pay', false,
            'is_excluded_from_allocation', false,
            'blocked_reason_codes', v_line_blocked_reason_codes,
            'amount_ex_vat', round(v_case_blocked_amount_ex_vat, 2),
            'amount_display', round(v_case_blocked_amount_ex_vat, 2),
            'case_resolution_summary', coalesce(v_case_state->'case_resolution_summary', '{}'::jsonb),
            'case_components', coalesce(v_case_state->'components', '[]'::jsonb)
          );

        if jsonb_typeof(v_case_state->'taxable_manual_debt_resolution') = 'object' then
          v_new_line := v_new_line || jsonb_build_object('taxable_manual_debt_resolution', v_case_state->'taxable_manual_debt_resolution');
        end if;

        v_updated_lines := v_updated_lines || jsonb_build_array(v_new_line);
      end if;
    end if;
  end loop;

  if jsonb_array_length(v_itemisation) > 0 then
    for v_item in
      select elem.value
      from jsonb_array_elements(v_itemisation) as elem(value)
      where jsonb_typeof(elem.value) = 'object'
    loop
      v_line_case_key := btrim(coalesce(v_item->>'case_key', ''));
      if v_line_case_key <> '' and jsonb_typeof(v_case_state_map->v_line_case_key) = 'object' then
        v_case_state_from_map := v_case_state_map->v_line_case_key;
        v_item := (v_item
          - 'case_resolution_summary'
          - 'components'
          - 'taxable_manual_debt_resolution')
          || jsonb_build_object(
            'case_resolution_summary', coalesce(v_case_state_from_map->'case_resolution_summary', '{}'::jsonb),
            'components', coalesce(v_case_state_from_map->'components', '[]'::jsonb)
          );

        if jsonb_typeof(v_case_state_from_map->'taxable_manual_debt_resolution') = 'object' then
          v_item := v_item || jsonb_build_object('taxable_manual_debt_resolution', v_case_state_from_map->'taxable_manual_debt_resolution');
        end if;

        if upper(btrim(coalesce(v_case_state_from_map->>'case_scope', ''))) = 'TIMESHEET' then
          v_case_state_timesheet_id := btrim(coalesce(v_case_state_from_map->>'timesheet_id', ''));
          if v_case_state_timesheet_id <> '' and exists (
            select 1
            from jsonb_array_elements(v_exclude_timesheet_ids) as excluded(value)
            where btrim(trim(both '"' from excluded.value::text)) = v_case_state_timesheet_id
          ) then
            v_item := v_item || jsonb_build_object('excluded_from_pay', true, 'is_do_not_pay', true);
          end if;
        end if;
      end if;
      v_updated_itemisation := v_updated_itemisation || jsonb_build_array(v_item);
    end loop;
  else
    v_updated_itemisation := v_itemisation;
  end if;

  select coalesce(jsonb_agg(elem.value order by coalesce(elem.value->>'candidate_id', ''), coalesce(elem.value->>'display_name', ''), coalesce(elem.value->>'line_type', ''), coalesce(elem.value->>'line_id', '')), '[]'::jsonb)
  into v_updated_lines
  from jsonb_array_elements(v_updated_lines) as elem(value)
  where jsonb_typeof(elem.value) = 'object'
    and (
      (coalesce(elem.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' and round((elem.value->>'amount_ex_vat')::numeric, 2) <> 0)
      or upper(btrim(coalesce(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
    );

  select coalesce(jsonb_agg(elem.value order by coalesce(elem.value->>'candidate_id', ''), coalesce(elem.value->>'timesheet_id', ''), coalesce(elem.value->>'line_id', '')), '[]'::jsonb)
  into v_blocked_items
  from jsonb_array_elements(v_updated_lines) as elem(value)
  where jsonb_typeof(elem.value) = 'object'
    and upper(btrim(coalesce(elem.value->>'presentation_section', ''))) = 'BLOCKED_FOR_PAY';

  select coalesce(jsonb_agg(elem.value order by coalesce(elem.value->>'candidate_id', ''), coalesce(elem.value->>'timesheet_id', ''), coalesce(elem.value->>'line_id', '')), '[]'::jsonb)
  into v_do_not_pay_items
  from jsonb_array_elements(v_updated_lines) as elem(value)
  where jsonb_typeof(elem.value) = 'object'
    and (
      upper(btrim(coalesce(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
      or coalesce((elem.value->>'is_do_not_pay')::boolean, false) = true
    );

  v_candidate_row := (coalesce(v_candidate_row, '{}'::jsonb)
    - 'case_resolution_states'
    - 'canonical_preview_lines'
    - 'itemisation'
    - 'blocked_items'
    - 'do_not_pay_items'
    - 'snoozed_items')
    || jsonb_build_object(
      'candidate_id', v_candidate_id,
      'case_resolution_states', coalesce(v_updated_case_states, '[]'::jsonb),
      'itemisation', coalesce(v_updated_itemisation, '[]'::jsonb),
      'blocked_items', coalesce(v_blocked_items, '[]'::jsonb),
      'do_not_pay_items', coalesce(v_do_not_pay_items, '[]'::jsonb),
      'snoozed_items', coalesce(v_snoozed_items, '[]'::jsonb)
    );

  return jsonb_build_object(
    'candidate_id', v_candidate_id,
    'candidate_row', coalesce(v_candidate_row, '{}'::jsonb),
    'summary_fragment', coalesce(v_summary_fragment, '{}'::jsonb),
    'case_resolution_states', coalesce(v_updated_case_states, '[]'::jsonb),
    'canonical_preview_lines', coalesce(v_updated_lines, '[]'::jsonb),
    'payees', coalesce(v_payees, '[]'::jsonb),
    'itemisation', coalesce(v_updated_itemisation, '[]'::jsonb),
    'blocked_items', coalesce(v_blocked_items, '[]'::jsonb),
    'do_not_pay_items', coalesce(v_do_not_pay_items, '[]'::jsonb),
    'snoozed_items', coalesce(v_snoozed_items, '[]'::jsonb),
    'baseline_component_rows', coalesce(v_final_component_rows, '[]'::jsonb)
  );
end;
$function$;
CREATE OR REPLACE FUNCTION public.pay_preview(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_client_id uuid DEFAULT NULL::uuid,
  p_preview_decisions_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_context_json jsonb := '{}'::jsonb;
  v_scope_candidate_ids jsonb := '[]'::jsonb;
  v_candidate_rollups_json jsonb := '[]'::jsonb;
  v_scope_candidate_id_text text := '';
  v_candidate_baseline_json jsonb := '{}'::jsonb;
  v_candidate_effective_json jsonb := '{}'::jsonb;
  v_candidate_rollup_json jsonb := '{}'::jsonb;
begin
  v_context_json := public.pay_preview_build_context(
    p_pay_date => p_pay_date,
    p_week_ending_cutoff => p_week_ending_cutoff,
    p_actor_user_id => p_actor_user_id,
    p_candidate_id => p_candidate_id,
    p_client_id => p_client_id,
    p_preview_decisions_json => p_preview_decisions_json
  );

  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'pay_preview_build_context(...) must return a JSON object';
  end if;

  v_scope_candidate_ids := case
    when jsonb_typeof(v_context_json->'scope_candidate_ids') = 'array' then coalesce(v_context_json->'scope_candidate_ids', '[]'::jsonb)
    else '[]'::jsonb
  end;

  for v_scope_candidate_id_text in
    select btrim(scope_candidate.value)
    from jsonb_array_elements_text(v_scope_candidate_ids) as scope_candidate(value)
    where btrim(scope_candidate.value) <> ''
    order by btrim(scope_candidate.value)
  loop
    v_candidate_baseline_json := public.pay_preview_build_candidate_baseline(
      p_context_json => v_context_json,
      p_candidate_id => v_scope_candidate_id_text::uuid
    );

    v_candidate_effective_json := public.pay_preview_apply_candidate_overlay(
      p_candidate_baseline_json => v_candidate_baseline_json,
      p_case_resolutions_json => coalesce(v_context_json->'preview_case_resolutions', '{}'::jsonb),
      p_overrides_json => coalesce(v_context_json->'preview_decisions_json', '{}'::jsonb)
    );

    v_candidate_rollup_json := public.pay_preview_build_candidate_rollup(
      p_context_json => v_context_json,
      p_candidate_effective_json => v_candidate_effective_json
    );

    v_candidate_rollups_json := v_candidate_rollups_json || jsonb_build_array(v_candidate_rollup_json);
  end loop;

  return public.pay_preview_assemble_payload(
    p_context_json => v_context_json,
    p_candidate_rollups_json => v_candidate_rollups_json
  );
end;
$function$;
