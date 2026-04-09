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
    'blocked_items', v_blocked_items,
    'do_not_pay_items', v_do_not_pay_items,
    'snoozed_items', v_snoozed_items,
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
  v_paye_candidates jsonb := '[]'::jsonb;
  v_non_paye_payees jsonb := '[]'::jsonb;
  v_case_resolution_states jsonb := '[]'::jsonb;
  v_canonical_preview_lines jsonb := '[]'::jsonb;
  v_payees jsonb := '[]'::jsonb;
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

  SELECT COUNT(*)::integer
  INTO v_blocked_preview_line_count
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_blocked', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
    );

  SELECT COUNT(*)::integer
  INTO v_do_not_pay_line_count
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_do_not_pay', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'DO_NOT_PAY'
    );

  SELECT COUNT(*)::integer
  INTO v_snoozed_line_count
  FROM jsonb_array_elements(v_canonical_preview_lines) AS elem(value)
  WHERE jsonb_typeof(elem.value) = 'object'
    AND (
      COALESCE(LOWER(BTRIM(COALESCE(elem.value->>'is_snoozed', 'false'))), 'false') IN ('true', 't', '1', 'yes', 'y', 'on')
      OR UPPER(BTRIM(COALESCE(elem.value->>'presentation_section', ''))) = 'SNOOZED'
    );

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
    'draftable_amount_inc_vat', v_draftable_amount_inc_vat
  );

  RETURN jsonb_build_object(
    'summary', v_summary,
    'paye_candidates', v_paye_candidates,
    'non_paye_payees', v_non_paye_payees,
    'case_resolution_states', v_case_resolution_states,
    'canonical_preview_lines', v_canonical_preview_lines,
    'paye_guardrails', COALESCE(v_context_json->'paye_guardrails', '{}'::jsonb),
    'payees', v_payees
  );
END;
$function$;
CREATE OR REPLACE FUNCTION public.pay_preview_build_candidate_baseline(
  p_context_json jsonb,
  p_candidate_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_context_json jsonb := coalesce(p_context_json, '{}'::jsonb);
  v_candidate_id uuid := p_candidate_id;
  v_pay_date date;
  v_week_ending_cutoff date;
  v_client_id uuid := null::uuid;
  v_actor_user_id uuid := null::uuid;
  v_week_start date;
  v_today_uk date;
  v_pay_eligibility_months_back int := 6;
  v_pay_eligibility_weeks_ahead int := 2;
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
  v_funds_warning_hours_json jsonb := '[]'::jsonb;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_paye jsonb := '[]'::jsonb;
  v_nonpaye jsonb := '[]'::jsonb;
  v_blocked jsonb := '[]'::jsonb;
  v_do_not_pay jsonb := '[]'::jsonb;
  v_snoozed jsonb := '[]'::jsonb;
  v_payees jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_paye_guardrails jsonb := '{}'::jsonb;
  v_canonical_preview_lines jsonb := '[]'::jsonb;
  v_paye_summary_breakdown jsonb := '{}'::jsonb;
  v_case_resolution_states jsonb := '[]'::jsonb;
  v_candidate_row jsonb := '{}'::jsonb;
  v_itemisation jsonb := '[]'::jsonb;
  v_baseline_component_rows jsonb := '[]'::jsonb;
begin
  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'p_context_json must be a JSON object';
  end if;

  if v_candidate_id is null then
    raise exception 'candidate_id is required';
  end if;

  v_pay_date := nullif(btrim(coalesce(v_context_json->>'pay_date', '')), '')::date;
  v_week_ending_cutoff := nullif(btrim(coalesce(v_context_json->>'week_ending_cutoff_date', '')), '')::date;
  v_client_id := nullif(btrim(coalesce(v_context_json #>> '{filters,client_id}', '')), '')::uuid;
  v_week_start := nullif(btrim(coalesce(v_context_json->>'pay_week_start', '')), '')::date;
  v_today_uk := coalesce(nullif(btrim(coalesce(v_context_json #>> '{eligibility,today_uk}', '')), '')::date, (now() at time zone 'Europe/London')::date);
  v_eligibility_from_date := nullif(btrim(coalesce(v_context_json #>> '{eligibility,from_date}', '')), '')::date;
  v_eligibility_to_date := nullif(btrim(coalesce(v_context_json #>> '{eligibility,to_date}', '')), '')::date;
  v_pay_eligibility_months_back := coalesce(nullif(btrim(coalesce(v_context_json #>> '{eligibility,months_back}', '')), '')::int, 6);
  v_pay_eligibility_weeks_ahead := coalesce(nullif(btrim(coalesce(v_context_json #>> '{eligibility,weeks_ahead}', '')), '')::int, 2);
  v_vat_rate_pct := nullif(btrim(coalesce(v_context_json #>> '{finance,vat_rate_pct}', '')), '')::numeric;
  v_erni_pct := nullif(btrim(coalesce(v_context_json #>> '{finance,erni_pct}', '')), '')::numeric;
  v_rail_provider_default := nullif(btrim(coalesce(v_context_json #>> '{settings,rail,provider_default}', '')), '');
  v_rail_env_default := nullif(btrim(coalesce(v_context_json #>> '{settings,rail,env_default}', '')), '');
  v_rail_supports_scheduling := coalesce((v_context_json #>> '{settings,rail,supports_scheduling}')::boolean, false);
  v_rail_supports_name_check := coalesce((v_context_json #>> '{settings,rail,supports_name_check}')::boolean, false);
  v_rail_supports_auto_execute := coalesce((v_context_json #>> '{settings,rail,supports_auto_execute}')::boolean, false);
  v_default_schedule_umbrella_local := nullif(btrim(coalesce(v_context_json #>> '{settings,schedule_defaults,umbrella_local}', '')), '');
  v_default_schedule_paye_local := nullif(btrim(coalesce(v_context_json #>> '{settings,schedule_defaults,paye_local}', '')), '');
  v_funds_warning_hours_json := case when jsonb_typeof(v_context_json #> '{settings,funds_warning_hours_json}') = 'array' then coalesce(v_context_json #> '{settings,funds_warning_hours_json}', '[]'::jsonb) else '[]'::jsonb end;
  v_need_name_check := coalesce((v_context_json #>> '{settings,rail,need_name_check}')::boolean, false);
  v_requires_payee_map := coalesce((v_context_json #>> '{settings,rail,requires_payee_map}')::boolean, false);
  v_paye_guardrails := case when jsonb_typeof(v_context_json->'paye_guardrails') = 'object' then coalesce(v_context_json->'paye_guardrails', '{}'::jsonb) else '{}'::jsonb end;

  if v_pay_date is null then
    raise exception 'pay_date missing from p_context_json';
  end if;

  if v_week_ending_cutoff is null then
    raise exception 'week_ending_cutoff_date missing from p_context_json';
  end if;

  if v_week_start is null then
    raise exception 'pay_week_start missing from p_context_json';
  end if;

  if v_eligibility_from_date is null or v_eligibility_to_date is null then
    raise exception 'eligibility window missing from p_context_json';
  end if;

  if v_vat_rate_pct is null or v_erni_pct is null then
    raise exception 'finance window values missing from p_context_json';
  end if;

  if v_rail_provider_default is null or v_rail_env_default is null then
    raise exception 'rail defaults missing from p_context_json';
  end if;

with active_snoozes as (
    select
      s.id as snooze_id,
      s.candidate_id,
      s.timesheet_id,
      s.booking_id,
      s.segment_id,
      s.segment_stable_key,
      s.source_ref,
      upper(coalesce(s.snooze_kind,'')) as snooze_kind,
      s.snooze_until_date,
      s.note
    from public.pay_item_snoozes s
    where s.cleared_at_utc is null
      and (
        s.snooze_until_date is null
        or s.snooze_until_date >= v_pay_date
      )
  ),
  active_timesheet_payment_snoozes as (
    select
      s.candidate_id,
      s.timesheet_id,
      s.booking_id,
      s.snooze_id,
      s.snooze_until_date,
      s.note
    from active_snoozes s
    where s.source_ref is null
      and s.segment_id is null
      and s.segment_stable_key is null
      and s.snooze_kind = 'TIMESHEET_PAYMENT'
  ),
  active_segment_snoozes as (
    select
      s.candidate_id,
      s.timesheet_id,
      s.booking_id,
      s.segment_id,
      coalesce(s.segment_stable_key, s.segment_id) as segment_stable_key,
      s.snooze_kind,
      s.snooze_id,
      s.snooze_until_date,
      s.note
    from active_snoozes s
    where s.source_ref is null
      and (s.segment_stable_key is not null or s.segment_id is not null)
      and s.snooze_kind in ('DO_NOT_PAY','BLOCKED_TIMESHEET')
  ),
  active_timesheet_payment_overrides as (
    select
      tpo.timesheet_id,
      tpo.candidate_id,
      tpo.id as override_id,
      tpo.reason as override_reason,
      tpo.created_at_utc
    from public.timesheet_payment_overrides tpo
    where tpo.cleared_at_utc is null
      and tpo.consumed_at_utc is null
      and tpo.consumed_by_pay_batch_id is null
      and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
  ),
  force_include as (
    select distinct
      tpo.timesheet_id
    from public.timesheet_payment_overrides tpo
    where tpo.cleared_at_utc is null
      and tpo.consumed_at_utc is null
      and tpo.consumed_by_pay_batch_id is null
      and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
      and tpo.timesheet_id is not null
  ),
  reserved_batch_items as (
    -- Items are considered "reserved" if they belong to an ACTIVE (non-cancelled, non-settled) batch.
    -- These amounts must be SUBTRACTED numerically (Policy X), not used as an existence-only suppressor.
    select
      pbi.id as pay_batch_item_id,
      pbc_r.pay_batch_id as pay_batch_id,
      pbi.timesheet_id as timesheet_id,
      pbi.segment_key as segment_key,
      -- Normalise segment_id (some legacy items store it in source_ref 'seg:<id>')
      nullif(
        btrim(coalesce(
          case
            when nullif(btrim(coalesce(pbi.segment_key,'')), '') is not null then pbi.segment_key
            when nullif(btrim(coalesce(pbi.source_ref,'')), '') like 'seg:%' then split_part(pbi.source_ref, ':', 2)
            else null
          end,
          ''
        )),
        ''
      ) as segment_id_norm,
      pbi.source_ref as source_ref,
      pbi.item_type as item_type,
      round(coalesce(pbi.amount_ex_vat,0), 2) as amount_ex_vat
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc_r
      on pbc_r.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb_r
      on pb_r.id = pbc_r.pay_batch_id
    where pbi.timesheet_id is not null
      and upper(coalesce(pbi.pay_channel,'')) in ('PAYE','UMBRELLA')
      and pbi.item_type <> 'DEBT_CREATED'
      and upper(coalesce(pb_r.status::text,'')) in (
        'DRAFT',
        'DRAFT_CREATED',
        'READY',
        'WAITING_BANK_CONFIRM',
        'PARTIAL',
        'FAILED',
        'BLOCKED_FUNDS',
        'SCHEDULED',
        'EXECUTING',
        'AWAITING_AUTHORISATION',
        'AUTHORISED_FOR_PAYMENT'
      )
  ),
  reserved_by_source_ref as (
    select
      rbi.timesheet_id,
      rbi.source_ref,
      round(sum(rbi.amount_ex_vat),2) as reserved_amount_ex_vat
    from reserved_batch_items rbi
    where rbi.source_ref is not null
      and btrim(coalesce(rbi.source_ref,'')) <> ''
    group by rbi.timesheet_id, rbi.source_ref
  ),
  reserved_total_by_timesheet as (
    select
      rbi.timesheet_id,
      round(sum(rbi.amount_ex_vat),2) as reserved_total_ex_vat
    from reserved_batch_items rbi
    group by rbi.timesheet_id
  ),
  reserved_segment_key_map as (
    select
      rbi.timesheet_id,
      rbi.segment_id_norm as segment_id_norm,
      case
        when nullif(btrim(coalesce(seg->>'date','')), '') is not null then 'TS_DAY'::text
        else 'TS_TOTAL'::text
      end as component_key_type,
      case
        when nullif(btrim(coalesce(seg->>'date','')), '') is not null
          then nullif(btrim(coalesce(seg->>'date','')), '')
        else 'TOTAL'
      end as component_key_value
    from reserved_batch_items rbi
    join public.pay_batch_timesheet_snapshots pbts
      on pbts.pay_batch_id = rbi.pay_batch_id
     and pbts.timesheet_id = rbi.timesheet_id
    join lateral (
      select s as seg
      from jsonb_array_elements(coalesce(pbts.target_snapshot_json->'segments','[]'::jsonb)) s
      where s is not null
        and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = rbi.segment_id_norm
      limit 1
    ) ss on true
    where rbi.item_type = 'SEGMENT_DELTA'
      and rbi.segment_id_norm is not null
  ),
  reserved_segment_sums as (
    select
      rskm.timesheet_id,
      rskm.component_key_type,
      rskm.component_key_value,
      round(sum(rbi.amount_ex_vat),2) as reserved_amount_ex_vat
    from reserved_segment_key_map rskm
    join reserved_batch_items rbi
      on rbi.timesheet_id = rskm.timesheet_id
     and rbi.item_type = 'SEGMENT_DELTA'
     and rbi.segment_id_norm = rskm.segment_id_norm
    where rskm.component_key_value is not null
      and btrim(coalesce(rskm.component_key_value,'')) <> ''
    group by rskm.timesheet_id, rskm.component_key_type, rskm.component_key_value
  ),
  reserved_preview_segment_ords as (
    select
      rbi.timesheet_id,
      case
        when coalesce(split_part(coalesce(rbi.source_ref,''), ':', 3), '') ~ '^\d+$'
          then split_part(rbi.source_ref, ':', 3)::int
        else null
      end as preview_seg_ord,
      round(sum(rbi.amount_ex_vat),2) as reserved_amount_ex_vat
    from reserved_batch_items rbi
    where rbi.item_type = 'ADJUSTMENT_DELTA'
      and rbi.source_ref is not null
      and btrim(coalesce(rbi.source_ref,'')) like 'preview_seg:%'
    group by
      rbi.timesheet_id,
      case
        when coalesce(split_part(coalesce(rbi.source_ref,''), ':', 3), '') ~ '^\d+$'
          then split_part(rbi.source_ref, ':', 3)::int
        else null
      end
  ),
  reserved_additional_by_code as (
    select
      rbi.timesheet_id,
      nullif(btrim(coalesce(bd.bucket_code,'')), '') as code,
      round(sum(coalesce(bd.amount_ex_vat,0)),2) as reserved_amount_ex_vat
    from reserved_batch_items rbi
    join public.pay_batch_item_breakdowns bd
      on bd.pay_batch_item_id = rbi.pay_batch_item_id
    where bd.line_kind = 'ADDITIONAL_UNIT'
      and bd.bucket_code is not null
      and btrim(coalesce(bd.bucket_code,'')) <> ''
    group by rbi.timesheet_id, nullif(btrim(coalesce(bd.bucket_code,'')), '')
  ),
eligible_tsfin as (
    select
      tf.id as tsfin_id,
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      ts.booking_id as ts_booking_id,
      coalesce(tf.role, con.role, ts.job_title_norm) as ts_role,
      coalesce(tf.band, con.band, ts.band) as ts_band,

      ts.week_ending_date as ts_week_ending_date,
      cl.name as ts_client_name,

      upper(coalesce(tf.pay_method,'')) as ts_pay_method,
      upper(coalesce(c.pay_method,'')) as cand_pay_method,

      c.tms_ref as cand_tms_ref,
      c.display_name as cand_display_name,

      c.umbrella_id as cand_umbrella_id,

      -- ✅ Bank readiness (candidate)
      c.bank_details_hash as cand_bank_hash,

      ts.reference_number,
      case when coalesce(con.overrideclientsettings,false) = true then coalesce(con.require_reference_to_pay,false) else coalesce(cs.pay_reference_required,false) end as require_reference_to_pay,

      -- Timesheet advance: force-include even if unauthorised/outside window
      (fi.timesheet_id is not null) as is_forced_advance,

      tf.invoice_breakdown_json,

      tf.total_hours,
      tf.total_pay_ex_vat,
      tf.total_charge_ex_vat,

      tf.hours_day,
      tf.hours_night,
      tf.hours_sat,
      tf.hours_sun,
      tf.hours_bh,
      tf.pay_day,
      tf.pay_night,
      tf.pay_sat,
      tf.pay_sun,
      tf.pay_bh,
      tf.charge_day,
      tf.charge_night,
      tf.charge_sat,
      tf.charge_sun,
      tf.charge_bh,

      -- ✅ Use TSFIN canonical totals for additional units
      tf.additional_pay_ex_vat,
      tf.additional_charge_ex_vat,
      tf.additional_units_json,

      tf.expenses_pay_ex_vat,
      tf.expenses_charge_ex_vat,
      tf.travel_pay_ex_vat,
      tf.travel_charge_ex_vat,
      tf.accommodation_pay_ex_vat,
      tf.accommodation_charge_ex_vat,
      tf.other_pay_ex_vat,
      tf.other_charge_ex_vat,
      tf.mileage_pay_ex_vat,
      tf.mileage_charge_ex_vat,

      -- Baseline snapshot (settled)
      tps.last_settled_snapshot_json,
      tps.last_settled_signature,
      coalesce(
        tps.last_settled_signature,
        md5(coalesce(tps.last_settled_snapshot_json::text, '{}'))
      ) as effective_last_settled_signature,

      ts.authorised_at_server,
      tf.pay_on_hold,
      tf.has_rate_issue,
      tf.has_pay_channel_issue,
      tf.processing_status
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
    join public.clients cl
      on cl.id = tf.client_id
    left join public.contracts con
      on con.id = ts.contract_id
    left join public.client_settings cs
      on cs.client_id = tf.client_id
    join public.candidates c
      on c.id = tf.candidate_id
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = tf.timesheet_id
    left join force_include fi
      on fi.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and coalesce(tf.pay_on_hold,false) = false
      and coalesce(tf.has_rate_issue,false) = false
      and coalesce(tf.has_pay_channel_issue,false) = false
      and upper(coalesce(tf.processing_status::text,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')

      -- ✅ Eligibility: authorised within window + cutoff OR forced include OR baseline exists (recovery cases)
      and (
        (
          ts.authorised_at_server is not null
          and ts.week_ending_date::date >= v_eligibility_from_date
          and ts.week_ending_date::date <= v_eligibility_to_date
          and ts.week_ending_date::date <= v_week_ending_cutoff
        )
        or fi.timesheet_id is not null
        or (
          tps.last_settled_snapshot_json is not null
          and ts.week_ending_date::date >= v_eligibility_from_date
          and ts.week_ending_date::date <= v_week_ending_cutoff
        )
      )

      -- ✅ Optional filters (default ALL/ALL when NULL)
      and (v_candidate_id is null or tf.candidate_id = v_candidate_id)
      and (v_client_id is null or tf.client_id = v_client_id)
  ),
  debted_overpayment_cases as (
    select
      pa.candidate_id,
      pa.linked_timesheet_id as timesheet_id,
      pa.baseline_signature
    from public.pay_advances pa
    where pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
      and pa.status in ('ACTIVE'::public.pay_advance_status_enum, 'PAID_OFF'::public.pay_advance_status_enum)
      and pa.linked_timesheet_id is not null
    group by
      pa.candidate_id,
      pa.linked_timesheet_id,
      pa.baseline_signature
  ),
umb_map as (
    select
      u.id as umbrella_id,
      coalesce(u.enabled,false) as umb_enabled,
      coalesce(u.vat_chargeable,false) as vat_chargeable,
      -- ✅ Bank readiness (umbrella)
      u.bank_details_hash as umb_bank_hash
    from public.umbrellas u
  ),
  adj as (
    select
      a.id as adj_id,
      a.timesheet_id,
      a.candidate_id,
      round(coalesce(a.delta_pay_ex_vat,0),2) as delta_pay_ex_vat
    from public.ts_pay_adjustments a
    where a.as_advance = false
      and a.timesheet_id is not null
  ),
  ts_current as (
    select
      e.candidate_id,
      e.timesheet_id,
      e.tsfin_id,
      e.client_id,
      e.ts_booking_id,
      e.ts_role,
      e.ts_band,
      e.ts_week_ending_date,
      e.ts_client_name,
      e.ts_pay_method,
      e.cand_pay_method,
      e.cand_tms_ref,
      e.cand_display_name,
      e.cand_umbrella_id,
      e.cand_bank_hash,
      e.reference_number,
      e.require_reference_to_pay,
      coalesce(e.is_forced_advance,false) as is_forced_advance,

      coalesce(um.vat_chargeable,false) as umb_vat_chargeable,
      coalesce(um.umb_enabled,false) as umb_enabled,
      um.umb_bank_hash,

      -- ✅ segments retain stable segment identity for preview/snooze matching
      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json) = 'object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(e.invoice_breakdown_json->'segments') = 'array'
        then (
          select coalesce(jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
              'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
              'charge_amount', round(coalesce(nullif(seg->>'charge_amount','')::numeric, nullif(seg->>'charge_ex_vat','')::numeric,0),2),
              'units', coalesce(nullif(seg->>'units','')::numeric, nullif(seg->>'hours','')::numeric),
              'hours', coalesce(nullif(seg->>'hours','')::numeric, nullif(seg->>'units','')::numeric),
              'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric, 0),
              'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric, 0),
              'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric, 0),
              'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric, 0),
              'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric, 0),
              'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean, false),
              'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), ''),
              'date', nullif(btrim(coalesce(seg->>'date','')), ''),
              'segment_key', nullif(btrim(coalesce(seg->>'segment_key','')), ''),
              'segment_stable_key', coalesce(
                nullif(btrim(coalesce(seg->>'segment_stable_key','')), ''),
                nullif(btrim(coalesce(seg->>'segment_id','')), ''),
                nullif(btrim(coalesce(seg->>'segment_key','')), ''),
                nullif(btrim(coalesce(seg->>'date','')), ''),
                nullif(btrim(coalesce(seg->>'ref_num','')), '')
              ),
              'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')), ''),
              'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')), ''),
              'start', case
                when nullif(btrim(coalesce(seg->>'start','')), '') is not null then nullif(btrim(coalesce(seg->>'start','')), '')
                when nullif(btrim(coalesce(seg->>'start_utc','')), '') is not null then to_char(((seg->>'start_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
                else null
              end,
              'end', case
                when nullif(btrim(coalesce(seg->>'end','')), '') is not null then nullif(btrim(coalesce(seg->>'end','')), '')
                when nullif(btrim(coalesce(seg->>'end_utc','')), '') is not null then to_char(((seg->>'end_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
                else null
              end,
              'break_start', nullif(btrim(coalesce(seg->>'break_start','')), ''),
              'break_end', nullif(btrim(coalesce(seg->>'break_end','')), ''),
              'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric, nullif(seg->>'break_minutes','')::numeric),
              'breaks', case when jsonb_typeof(seg->'breaks') = 'array' then seg->'breaks' else '[]'::jsonb end,
              'client_name', e.ts_client_name,
              'role', e.ts_role,
              'band', e.ts_band
            )
          ), '[]'::jsonb)
          from jsonb_array_elements(e.invoice_breakdown_json->'segments') seg
          where seg is not null and jsonb_typeof(seg)='object'
        )
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || e.timesheet_id::text),
            'pay_amount', round(coalesce(e.total_pay_ex_vat,0),2),
            'charge_amount', round(coalesce(e.total_charge_ex_vat,0),2),
            'units', e.total_hours,
            'hours', e.total_hours,
            'hours_day', coalesce(e.hours_day, 0),
            'hours_night', coalesce(e.hours_night, 0),
            'hours_sat', coalesce(e.hours_sat, 0),
            'hours_sun', coalesce(e.hours_sun, 0),
            'hours_bh', coalesce(e.hours_bh, 0),
            'exclude_from_pay', false,
            'ref_num', nullif(btrim(coalesce(e.reference_number,'')), ''),
            'date', null,
            'segment_key', ('ts:' || e.timesheet_id::text),
            'segment_stable_key', ('timesheet:' || coalesce(e.ts_booking_id, e.timesheet_id::text)),
            'start_utc', null,
            'end_utc', null,
            'start', null,
            'end', null,
            'break_start', null,
            'break_end', null,
            'break_mins', null,
            'breaks', '[]'::jsonb,
            'client_name', e.ts_client_name,
            'role', e.ts_role,
            'band', e.ts_band
          )
        )
      end as current_segments_json,

      round(coalesce(e.total_hours,0),2) as total_hours,
      round(coalesce(e.total_pay_ex_vat,0),2) as total_pay_ex_vat,
      round(coalesce(e.total_charge_ex_vat,0),2) as total_charge_ex_vat,
      round(coalesce(e.hours_day,0),6) as hours_day,
      round(coalesce(e.hours_night,0),6) as hours_night,
      round(coalesce(e.hours_sat,0),6) as hours_sat,
      round(coalesce(e.hours_sun,0),6) as hours_sun,
      round(coalesce(e.hours_bh,0),6) as hours_bh,
      case when e.pay_day is null then null else round(e.pay_day,6) end as pay_day,
      case when e.pay_night is null then null else round(e.pay_night,6) end as pay_night,
      case when e.pay_sat is null then null else round(e.pay_sat,6) end as pay_sat,
      case when e.pay_sun is null then null else round(e.pay_sun,6) end as pay_sun,
      case when e.pay_bh is null then null else round(e.pay_bh,6) end as pay_bh,
      case when e.charge_day is null then null else round(e.charge_day,6) end as charge_day,
      case when e.charge_night is null then null else round(e.charge_night,6) end as charge_night,
      case when e.charge_sat is null then null else round(e.charge_sat,6) end as charge_sat,
      case when e.charge_sun is null then null else round(e.charge_sun,6) end as charge_sun,
      case when e.charge_bh is null then null else round(e.charge_bh,6) end as charge_bh,
      coalesce(e.additional_units_json, '{}'::jsonb) as current_additional_units_json,

      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(e.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
        else round(coalesce(e.additional_pay_ex_vat,0),2)
      end as current_additional_pay_ex_vat,
      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(e.invoice_breakdown_json #>> '{additional,charge_ex_vat}','')::numeric,0),2)
        else round(coalesce(e.additional_charge_ex_vat,0),2)
      end as current_additional_charge_ex_vat,
      round(coalesce(e.expenses_pay_ex_vat,0),2) as current_expenses_pay_ex_vat,
      round(coalesce(e.expenses_charge_ex_vat,0),2) as current_expenses_charge_ex_vat,
      round(coalesce(e.travel_pay_ex_vat,0),2) as current_travel_pay_ex_vat,
      round(coalesce(e.travel_charge_ex_vat,0),2) as current_travel_charge_ex_vat,
      round(coalesce(e.accommodation_pay_ex_vat,0),2) as current_accommodation_pay_ex_vat,
      round(coalesce(e.accommodation_charge_ex_vat,0),2) as current_accommodation_charge_ex_vat,
      round(coalesce(e.other_pay_ex_vat,0),2) as current_other_pay_ex_vat,
      round(coalesce(e.other_charge_ex_vat,0),2) as current_other_charge_ex_vat,
      round(coalesce(e.mileage_pay_ex_vat,0),2) as current_mileage_pay_ex_vat,
      round(coalesce(e.mileage_charge_ex_vat,0),2) as current_mileage_charge_ex_vat,

      coalesce(
        (
          select jsonb_agg(
            jsonb_build_object(
              'id', a.adj_id::text,
              'delta_pay_ex_vat', a.delta_pay_ex_vat
            )
          )
          from adj a
          where a.timesheet_id = e.timesheet_id
        ),
        '[]'::jsonb
      ) as current_adjustments_json,

      e.last_settled_snapshot_json,
      e.effective_last_settled_signature as baseline_signature,
      (doc.timesheet_id is not null) as has_active_overpayment_case
    from eligible_tsfin e
    left join umb_map um
      on um.umbrella_id = e.cand_umbrella_id
    left join debted_overpayment_cases doc
      on doc.candidate_id = e.candidate_id
     and doc.timesheet_id = e.timesheet_id
     and coalesce(doc.baseline_signature, '') = coalesce(e.effective_last_settled_signature, '')
  ),
  ts_baseline as (
    select
      t.candidate_id,
      t.timesheet_id,
      t.client_id,
      t.ts_booking_id,
      t.ts_role,
      t.ts_band,
      t.ts_week_ending_date,
      t.ts_client_name,
      t.ts_pay_method,
      t.cand_pay_method,
      t.cand_tms_ref,
      t.cand_display_name,
      t.cand_umbrella_id,
      t.umb_enabled,
      t.umb_vat_chargeable,
      t.require_reference_to_pay,
      t.is_forced_advance,

      -- ✅ bank readiness propagation
      t.cand_bank_hash,
      t.umb_bank_hash,

      coalesce(t.last_settled_snapshot_json, '{}'::jsonb) as base_json,
      t.baseline_signature,
      coalesce(t.has_active_overpayment_case,false) as has_active_overpayment_case,

      coalesce(t.current_segments_json, '[]'::jsonb) as current_segments_json,
      coalesce(t.current_adjustments_json, '[]'::jsonb) as current_adjustments_json,

      t.total_hours,
      t.total_pay_ex_vat,
      t.total_charge_ex_vat,
      t.hours_day,
      t.hours_night,
      t.hours_sat,
      t.hours_sun,
      t.hours_bh,
      t.pay_day,
      t.pay_night,
      t.pay_sat,
      t.pay_sun,
      t.pay_bh,
      t.charge_day,
      t.charge_night,
      t.charge_sat,
      t.charge_sun,
      t.charge_bh,
      t.hours_day as cur_hours_day,
      t.hours_night as cur_hours_night,
      t.hours_sat as cur_hours_sat,
      t.hours_sun as cur_hours_sun,
      t.hours_bh as cur_hours_bh,
      t.pay_day as cur_pay_day,
      t.pay_night as cur_pay_night,
      t.pay_sat as cur_pay_sat,
      t.pay_sun as cur_pay_sun,
      t.pay_bh as cur_pay_bh,
      t.charge_day as cur_charge_day,
      t.charge_night as cur_charge_night,
      t.charge_sat as cur_charge_sat,
      t.charge_sun as cur_charge_sun,
      t.charge_bh as cur_charge_bh,
      case
        when coalesce(t.last_settled_snapshot_json->>'hours_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,day}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'hours_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,day}')::numeric), 6)
        else 0::numeric
      end as bas_hours_day,
      case
        when coalesce(t.last_settled_snapshot_json->>'hours_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,night}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'hours_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,night}')::numeric), 6)
        else 0::numeric
      end as bas_hours_night,
      case
        when coalesce(t.last_settled_snapshot_json->>'hours_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sat}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'hours_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sat}')::numeric), 6)
        else 0::numeric
      end as bas_hours_sat,
      case
        when coalesce(t.last_settled_snapshot_json->>'hours_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sun}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'hours_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sun}')::numeric), 6)
        else 0::numeric
      end as bas_hours_sun,
      case
        when coalesce(t.last_settled_snapshot_json->>'hours_bh', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,bh}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'hours_bh')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,bh}')::numeric), 6)
        else 0::numeric
      end as bas_hours_bh,
      case
        when coalesce(t.last_settled_snapshot_json->>'pay_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,day}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'pay_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,day}')::numeric), 6)
        else null::numeric
      end as bas_pay_day,
      case
        when coalesce(t.last_settled_snapshot_json->>'pay_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,night}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'pay_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,night}')::numeric), 6)
        else null::numeric
      end as bas_pay_night,
      case
        when coalesce(t.last_settled_snapshot_json->>'pay_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sat}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'pay_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sat}')::numeric), 6)
        else null::numeric
      end as bas_pay_sat,
      case
        when coalesce(t.last_settled_snapshot_json->>'pay_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sun}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'pay_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sun}')::numeric), 6)
        else null::numeric
      end as bas_pay_sun,
      case
        when coalesce(t.last_settled_snapshot_json->>'pay_bh', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,bh}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'pay_bh')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,bh}')::numeric), 6)
        else null::numeric
      end as bas_pay_bh,
      case
        when coalesce(t.last_settled_snapshot_json->>'charge_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,day}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'charge_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,day}')::numeric), 6)
        else null::numeric
      end as bas_charge_day,
      case
        when coalesce(t.last_settled_snapshot_json->>'charge_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,night}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'charge_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,night}')::numeric), 6)
        else null::numeric
      end as bas_charge_night,
      case
        when coalesce(t.last_settled_snapshot_json->>'charge_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sat}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'charge_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sat}')::numeric), 6)
        else null::numeric
      end as bas_charge_sat,
      case
        when coalesce(t.last_settled_snapshot_json->>'charge_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sun}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'charge_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sun}')::numeric), 6)
        else null::numeric
      end as bas_charge_sun,
      case
        when coalesce(t.last_settled_snapshot_json->>'charge_bh', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,bh}', '') ~ '^-?\d+(\.\d+)?$'
          then round(coalesce((t.last_settled_snapshot_json->>'charge_bh')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,bh}')::numeric), 6)
        else null::numeric
      end as bas_charge_bh,
      t.current_additional_pay_ex_vat,
      t.current_additional_charge_ex_vat,
      t.current_additional_units_json,
      t.current_expenses_pay_ex_vat,
      t.current_expenses_charge_ex_vat,
      t.current_travel_pay_ex_vat,
      t.current_travel_charge_ex_vat,
      t.current_accommodation_pay_ex_vat,
      t.current_accommodation_charge_ex_vat,
      t.current_other_pay_ex_vat,
      t.current_other_charge_ex_vat,
      t.current_mileage_pay_ex_vat,
      t.current_mileage_charge_ex_vat
    from ts_current t
  ),
  segment_status as (
    -- Economic-key segment reconciliation:
    -- segment identity is retained only for UI continuity and snooze matching.
    -- Outstanding and reservation subtraction stay in the economic entitlement keyspace
    -- (TS_DAY => work_date, TS_TOTAL => TOTAL).
    with
    cur_segments as (
      select
        b.timesheet_id,
        b.candidate_id,
        cur_seg.seg_ord as source_seg_ord,
        nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')),'') as segment_id,
        nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')),'') as ref_num,
        nullif(btrim(coalesce(cur_seg.seg->>'date','')),'') as work_date,
        coalesce(nullif(cur_seg.seg->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        round(coalesce(nullif(cur_seg.seg->>'pay_amount','')::numeric,0),2) as pay_amount_ex_vat,
        round(coalesce(nullif(cur_seg.seg->>'charge_amount','')::numeric, nullif(cur_seg.seg->>'charge_ex_vat','')::numeric,0),2) as charge_amount_ex_vat,
        case
          when nullif(cur_seg.seg->>'units','') is not null then nullif(cur_seg.seg->>'units','')::numeric
          when nullif(cur_seg.seg->>'hours','') is not null then nullif(cur_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(cur_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (cur_seg.seg->>'end_utc')::timestamptz
                    - (cur_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(cur_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end as source_units,
        null::numeric as source_rate,
        case
          when nullif(cur_seg.seg->>'charge_rate','') is not null then round(nullif(cur_seg.seg->>'charge_rate','')::numeric, 6)
          when nullif(cur_seg.seg->>'charge_unit_rate','') is not null then round(nullif(cur_seg.seg->>'charge_unit_rate','')::numeric, 6)
          when (case
          when nullif(cur_seg.seg->>'units','') is not null then nullif(cur_seg.seg->>'units','')::numeric
          when nullif(cur_seg.seg->>'hours','') is not null then nullif(cur_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(cur_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (cur_seg.seg->>'end_utc')::timestamptz
                    - (cur_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(cur_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end) is not null
           and (case
          when nullif(cur_seg.seg->>'units','') is not null then nullif(cur_seg.seg->>'units','')::numeric
          when nullif(cur_seg.seg->>'hours','') is not null then nullif(cur_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(cur_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (cur_seg.seg->>'end_utc')::timestamptz
                    - (cur_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(cur_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end) <> 0
          then round(
            round(coalesce(nullif(cur_seg.seg->>'charge_amount','')::numeric, nullif(cur_seg.seg->>'charge_ex_vat','')::numeric,0),2) / (case
          when nullif(cur_seg.seg->>'units','') is not null then nullif(cur_seg.seg->>'units','')::numeric
          when nullif(cur_seg.seg->>'hours','') is not null then nullif(cur_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(cur_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (cur_seg.seg->>'end_utc')::timestamptz
                    - (cur_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(cur_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(cur_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(cur_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(cur_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(cur_seg.seg->>'break_mins','')::numeric,
                    nullif(cur_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end),
            6
          )
          else null::numeric
        end as source_charge_rate,
        coalesce(
          nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')),''),
          nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')),''),
          nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')),''),
          nullif(btrim(coalesce(cur_seg.seg->>'date','')),''),
          nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')),'')
        ) as segment_stable_key,
        case
          when nullif(btrim(coalesce(cur_seg.seg->>'date','')), '') is not null then 'TS_DAY'::text
          else 'TS_TOTAL'::text
        end as component_key_type,
        case
          when nullif(btrim(coalesce(cur_seg.seg->>'date','')), '') is not null
            then nullif(btrim(coalesce(cur_seg.seg->>'date','')), '')
          else 'TOTAL'
        end as component_key_value
      from ts_baseline b
      join lateral jsonb_array_elements(coalesce(b.current_segments_json,'[]'::jsonb)) with ordinality as cur_seg(seg, seg_ord) on true
      where cur_seg.seg is not null
        and jsonb_typeof(cur_seg.seg) = 'object'
    ),
    bas_segments as (
      select
        b.timesheet_id,
        b.candidate_id,
        bas_seg.seg_ord as source_seg_ord,
        nullif(btrim(coalesce(bas_seg.seg->>'segment_id','')),'') as segment_id,
        nullif(btrim(coalesce(bas_seg.seg->>'ref_num','')),'') as ref_num,
        nullif(btrim(coalesce(bas_seg.seg->>'date','')),'') as work_date,
        coalesce(nullif(bas_seg.seg->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        round(coalesce(nullif(bas_seg.seg->>'pay_amount','')::numeric,0),2) as pay_amount_ex_vat,
        round(coalesce(nullif(bas_seg.seg->>'charge_amount','')::numeric, nullif(bas_seg.seg->>'charge_ex_vat','')::numeric,0),2) as charge_amount_ex_vat,
        case
          when nullif(bas_seg.seg->>'units','') is not null then nullif(bas_seg.seg->>'units','')::numeric
          when nullif(bas_seg.seg->>'hours','') is not null then nullif(bas_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(bas_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (bas_seg.seg->>'end_utc')::timestamptz
                    - (bas_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(bas_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end as source_units,
        null::numeric as source_rate,
        case
          when nullif(bas_seg.seg->>'charge_rate','') is not null then round(nullif(bas_seg.seg->>'charge_rate','')::numeric, 6)
          when nullif(bas_seg.seg->>'charge_unit_rate','') is not null then round(nullif(bas_seg.seg->>'charge_unit_rate','')::numeric, 6)
          when (case
          when nullif(bas_seg.seg->>'units','') is not null then nullif(bas_seg.seg->>'units','')::numeric
          when nullif(bas_seg.seg->>'hours','') is not null then nullif(bas_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(bas_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (bas_seg.seg->>'end_utc')::timestamptz
                    - (bas_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(bas_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end) is not null
           and (case
          when nullif(bas_seg.seg->>'units','') is not null then nullif(bas_seg.seg->>'units','')::numeric
          when nullif(bas_seg.seg->>'hours','') is not null then nullif(bas_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(bas_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (bas_seg.seg->>'end_utc')::timestamptz
                    - (bas_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(bas_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end) <> 0
          then round(
            round(coalesce(nullif(bas_seg.seg->>'charge_amount','')::numeric, nullif(bas_seg.seg->>'charge_ex_vat','')::numeric,0),2) / (case
          when nullif(bas_seg.seg->>'units','') is not null then nullif(bas_seg.seg->>'units','')::numeric
          when nullif(bas_seg.seg->>'hours','') is not null then nullif(bas_seg.seg->>'hours','')::numeric
          when nullif(btrim(coalesce(bas_seg.seg->>'start_utc','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end_utc','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (bas_seg.seg->>'end_utc')::timestamptz
                    - (bas_seg.seg->>'start_utc')::timestamptz
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          when nullif(btrim(coalesce(bas_seg.seg->>'start','')), '') is not null
           and nullif(btrim(coalesce(bas_seg.seg->>'end','')), '') is not null
          then round(
            greatest(
              (
                extract(
                  epoch from (
                    (
                      timestamp '2000-01-01'
                      + nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time
                      + case
                          when nullif(btrim(coalesce(bas_seg.seg->>'end','')), '')::time < nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                          then interval '1 day'
                          else interval '0 day'
                        end
                    )
                    - (
                        timestamp '2000-01-01'
                        + nullif(btrim(coalesce(bas_seg.seg->>'start','')), '')::time
                      )
                  )
                ) / 3600.0
              )
              - (
                  coalesce(
                    nullif(bas_seg.seg->>'break_mins','')::numeric,
                    nullif(bas_seg.seg->>'break_minutes','')::numeric,
                    0
                  ) / 60.0
                ),
              0
            ),
            6
          )
          else null::numeric
        end),
            6
          )
          else null::numeric
        end as source_charge_rate,
        coalesce(
          nullif(btrim(coalesce(bas_seg.seg->>'segment_stable_key','')),''),
          nullif(btrim(coalesce(bas_seg.seg->>'segment_id','')),''),
          nullif(btrim(coalesce(bas_seg.seg->>'segment_key','')),''),
          nullif(btrim(coalesce(bas_seg.seg->>'date','')),''),
          nullif(btrim(coalesce(bas_seg.seg->>'ref_num','')),'')
        ) as segment_stable_key,
        case
          when nullif(btrim(coalesce(bas_seg.seg->>'date','')), '') is not null then 'TS_DAY'::text
          else 'TS_TOTAL'::text
        end as component_key_type,
        case
          when nullif(btrim(coalesce(bas_seg.seg->>'date','')), '') is not null
            then nullif(btrim(coalesce(bas_seg.seg->>'date','')), '')
          else 'TOTAL'
        end as component_key_value
      from ts_baseline b
      join lateral jsonb_array_elements(coalesce(b.base_json->'segments','[]'::jsonb)) with ordinality as bas_seg(seg, seg_ord) on true
      where bas_seg.seg is not null
        and jsonb_typeof(bas_seg.seg) = 'object'
    ),
    cur_ranked as (
      select
        cs.timesheet_id,
        cs.candidate_id,
        cs.source_seg_ord,
        cs.segment_id,
        cs.ref_num,
        cs.work_date,
        cs.exclude_from_pay,
        cs.pay_amount_ex_vat,
        cs.charge_amount_ex_vat,
        cs.source_units,
        cs.source_rate,
        cs.source_charge_rate,
        cs.segment_stable_key,
        cs.component_key_type,
        cs.component_key_value,
        row_number() over (
          partition by cs.timesheet_id, cs.candidate_id, cs.component_key_type, cs.component_key_value
          order by cs.source_seg_ord nulls last, cs.segment_id nulls last, cs.ref_num nulls last
        ) as bucket_row_ord
      from cur_segments cs
    ),
    bas_ranked as (
      select
        bs.timesheet_id,
        bs.candidate_id,
        bs.source_seg_ord,
        bs.segment_id,
        bs.ref_num,
        bs.work_date,
        bs.exclude_from_pay,
        bs.pay_amount_ex_vat,
        bs.charge_amount_ex_vat,
        bs.source_units,
        bs.source_rate,
        bs.source_charge_rate,
        bs.segment_stable_key,
        bs.component_key_type,
        bs.component_key_value,
        row_number() over (
          partition by bs.timesheet_id, bs.candidate_id, bs.component_key_type, bs.component_key_value
          order by bs.source_seg_ord nulls last, bs.segment_id nulls last, bs.ref_num nulls last
        ) as bucket_row_ord
      from bas_segments bs
    ),
    ids as (
      select distinct
        cr.timesheet_id,
        cr.candidate_id,
        cr.component_key_type,
        cr.component_key_value,
        cr.bucket_row_ord
      from cur_ranked cr
      union
      select distinct
        br.timesheet_id,
        br.candidate_id,
        br.component_key_type,
        br.component_key_value,
        br.bucket_row_ord
      from bas_ranked br
    ),
    agg as (
      select
        i.timesheet_id,
        i.candidate_id,
        i.component_key_type,
        i.component_key_value,
        i.bucket_row_ord,
        max(cr.segment_id) as cur_segment_id,
        max(br.segment_id) as bas_segment_id,
        max(coalesce(cr.ref_num, br.ref_num)) as ref_num,
        max(coalesce(cr.work_date, br.work_date)) as work_date,
        max(coalesce(cr.segment_stable_key, br.segment_stable_key)) as segment_stable_key,
        max(cr.source_seg_ord) as cur_source_seg_ord,
        max(br.source_seg_ord) as bas_source_seg_ord,
        bool_or(coalesce(cr.exclude_from_pay,false)) as cur_excluded,
        max(cr.source_units) as cur_source_units,
        max(cr.source_rate) as cur_source_rate,
        max(cr.source_charge_rate) as cur_source_charge_rate,
        round(sum(case when cr.bucket_row_ord = i.bucket_row_ord then (case when coalesce(cr.exclude_from_pay,false) then 0 else coalesce(cr.pay_amount_ex_vat,0) end) else 0 end), 2) as cur_payable_ex_vat,
        round(sum(case when br.bucket_row_ord = i.bucket_row_ord then (case when coalesce(br.exclude_from_pay,false) then 0 else coalesce(br.pay_amount_ex_vat,0) end) else 0 end), 2) as bas_payable_ex_vat,
        round(sum(case when cr.bucket_row_ord = i.bucket_row_ord then (case when coalesce(cr.exclude_from_pay,false) then 0 else coalesce(cr.charge_amount_ex_vat,0) end) else 0 end), 2) as cur_charge_ex_vat,
        round(sum(case when br.bucket_row_ord = i.bucket_row_ord then (case when coalesce(br.exclude_from_pay,false) then 0 else coalesce(br.charge_amount_ex_vat,0) end) else 0 end), 2) as bas_charge_ex_vat
      from ids i
      left join cur_ranked cr
        on cr.timesheet_id = i.timesheet_id
       and cr.candidate_id = i.candidate_id
       and cr.component_key_type = i.component_key_type
       and cr.component_key_value = i.component_key_value
       and cr.bucket_row_ord = i.bucket_row_ord
      left join bas_ranked br
        on br.timesheet_id = i.timesheet_id
       and br.candidate_id = i.candidate_id
       and br.component_key_type = i.component_key_type
       and br.component_key_value = i.component_key_value
       and br.bucket_row_ord = i.bucket_row_ord
      group by i.timesheet_id, i.candidate_id, i.component_key_type, i.component_key_value, i.bucket_row_ord
    ),
    calc as (
      select
        b.candidate_id,
        b.timesheet_id,
        b.ts_booking_id as booking_id,
        coalesce(a.cur_segment_id, a.bas_segment_id) as segment_id,
        coalesce(a.cur_segment_id, a.bas_segment_id) as segment_key,
        a.segment_stable_key,
        a.component_key_type,
        a.component_key_value,
        a.bucket_row_ord,
        coalesce(a.cur_source_seg_ord, a.bas_source_seg_ord) as segment_sort_ord,
        a.work_date,
        a.ref_num,
        a.cur_source_units as source_units,
        a.cur_source_rate as source_rate,
        a.cur_source_charge_rate as source_charge_rate,
        a.cur_excluded,
        coalesce(b.has_active_overpayment_case,false) as has_active_overpayment_case,
        b.require_reference_to_pay,
        coalesce(b.is_forced_advance,false) as is_forced_advance,
        round(
          coalesce(a.cur_payable_ex_vat,0)
          - coalesce(a.bas_payable_ex_vat,0),
          2
        ) as raw_delta_before_reservation_ex,
        round(
          coalesce(a.cur_charge_ex_vat,0)
          - coalesce(a.bas_charge_ex_vat,0),
          2
        ) as raw_delta_charge_ex_vat,
        round(
          case
            when (
              coalesce(b.has_active_overpayment_case,false) = true
              and round(
                coalesce(a.cur_payable_ex_vat,0)
                - coalesce(a.bas_payable_ex_vat,0),
                2
              ) < 0
            )
            then 0
            when (
              b.require_reference_to_pay = true
              and coalesce(b.is_forced_advance,false) = false
              and a.cur_excluded = false
              and (a.ref_num is null or btrim(coalesce(a.ref_num,'')) = '')
              and round(
                coalesce(a.cur_payable_ex_vat,0)
                - coalesce(a.bas_payable_ex_vat,0),
                2
              ) > 0
            )
            then 0
            else round(
              coalesce(a.cur_payable_ex_vat,0)
              - coalesce(a.bas_payable_ex_vat,0),
              2
            )
          end,
          2
        ) as preview_base_eff_delta_ex,
        round(
          case
            when (
              coalesce(b.has_active_overpayment_case,false) = true
              and round(
                coalesce(a.cur_payable_ex_vat,0)
                - coalesce(a.bas_payable_ex_vat,0),
                2
              ) < 0
            )
            then 0
            when round(
              coalesce(a.cur_payable_ex_vat,0)
              - coalesce(a.bas_payable_ex_vat,0),
              2
            ) > 0
            then round(
              coalesce(a.cur_payable_ex_vat,0)
              - coalesce(a.bas_payable_ex_vat,0),
              2
            )
            else 0
          end,
          2
        ) as allocatable_delta_ex,
        coalesce(rss.reserved_amount_ex_vat,0) as reserved_bucket_amount_ex_vat
      from ts_baseline b
      join agg a
        on a.timesheet_id = b.timesheet_id
       and a.candidate_id = b.candidate_id
      left join reserved_segment_sums rss
        on rss.timesheet_id = b.timesheet_id
       and rss.component_key_type = a.component_key_type
       and rss.component_key_value = a.component_key_value
    ),
    alloc as (
      select
        c.candidate_id,
        c.timesheet_id,
        c.booking_id,
        c.segment_id,
        c.segment_key,
        c.segment_stable_key,
        c.component_key_type,
        c.component_key_value,
        c.bucket_row_ord,
        c.segment_sort_ord,
        c.work_date,
        c.ref_num,
        c.source_units,
        c.source_rate,
        c.source_charge_rate,
        c.cur_excluded,
        c.has_active_overpayment_case,
        c.require_reference_to_pay,
        c.is_forced_advance,
        c.raw_delta_before_reservation_ex,
        c.raw_delta_charge_ex_vat,
        c.preview_base_eff_delta_ex,
        c.reserved_bucket_amount_ex_vat,
        round(
          case
            when coalesce(c.allocatable_delta_ex,0) <= 0 then 0
            else greatest(
              least(
                coalesce(c.reserved_bucket_amount_ex_vat,0)
                - coalesce(
                    sum(coalesce(c.allocatable_delta_ex,0)) over (
                      partition by c.timesheet_id, c.component_key_type, c.component_key_value
                      order by c.segment_sort_ord nulls last, c.bucket_row_ord, c.segment_id nulls last
                      rows between unbounded preceding and 1 preceding
                    ),
                    0
                  ),
                c.allocatable_delta_ex
              ),
              0
            )
          end,
          2
        ) as allocated_reserved_amount_ex_vat
      from calc c
    )
    select
      a.candidate_id,
      a.timesheet_id,
      a.booking_id,
      a.segment_id,
      a.segment_key,
      a.segment_stable_key,
      a.component_key_type,
      a.component_key_value,
      a.bucket_row_ord,
      a.segment_sort_ord,
      a.work_date,
      a.ref_num,
      a.source_units,
      a.source_rate,
      a.source_charge_rate,
      a.raw_delta_before_reservation_ex,
      a.raw_delta_charge_ex_vat,
      a.preview_base_eff_delta_ex,
      round(
        case
          when (
            a.has_active_overpayment_case = true
            and round(
              a.raw_delta_before_reservation_ex
              - a.allocated_reserved_amount_ex_vat,
              2
            ) < 0
          )
          then 0
          else round(
            a.raw_delta_before_reservation_ex
            - a.allocated_reserved_amount_ex_vat,
            2
          )
        end,
        2
      ) as delta_pay_ex_vat,
      round(
        case
          when (
            a.has_active_overpayment_case = true
            and round(
              a.raw_delta_before_reservation_ex
              - a.allocated_reserved_amount_ex_vat,
              2
            ) < 0
          )
          then 0
          when (
            a.require_reference_to_pay = true
            and a.is_forced_advance = false
            and a.cur_excluded = false
            and (a.ref_num is null or btrim(coalesce(a.ref_num,'')) = '')
            and round(
              a.raw_delta_before_reservation_ex
              - a.allocated_reserved_amount_ex_vat,
              2
            ) > 0
          )
          then 0
          else round(
            a.raw_delta_before_reservation_ex
            - a.allocated_reserved_amount_ex_vat,
            2
          )
        end,
        2
      ) as eff_delta_ex,
      case
        when round(a.raw_delta_before_reservation_ex, 2) = 0 then round(a.raw_delta_charge_ex_vat, 2)
        else round(
          a.raw_delta_charge_ex_vat
          * (
              (
                case
                  when (
                    a.has_active_overpayment_case = true
                    and round(
                      a.raw_delta_before_reservation_ex
                      - a.allocated_reserved_amount_ex_vat,
                      2
                    ) < 0
                  )
                  then 0
                  when (
                    a.require_reference_to_pay = true
                    and a.is_forced_advance = false
                    and a.cur_excluded = false
                    and (a.ref_num is null or btrim(coalesce(a.ref_num,'')) = '')
                    and round(
                      a.raw_delta_before_reservation_ex
                      - a.allocated_reserved_amount_ex_vat,
                      2
                    ) > 0
                  )
                  then 0
                  else round(
                    a.raw_delta_before_reservation_ex
                    - a.allocated_reserved_amount_ex_vat,
                    2
                  )
                end
              ) / nullif(round(a.raw_delta_before_reservation_ex, 2),0)
            ),
          2
        )
      end as eff_delta_charge_ex_vat,
      (
        coalesce(a.cur_excluded,false) = true
        and round(
          a.raw_delta_before_reservation_ex
          - a.allocated_reserved_amount_ex_vat,
          2
        ) <> 0
      ) as is_do_not_pay,
      (
        a.require_reference_to_pay = true
        and a.is_forced_advance = false
        and a.cur_excluded = false
        and (a.ref_num is null or btrim(coalesce(a.ref_num,'')) = '')
        and round(
          a.raw_delta_before_reservation_ex
          - a.allocated_reserved_amount_ex_vat,
          2
        ) > 0
      ) as is_ref_missing,
      (
        a.require_reference_to_pay = true
        and a.is_forced_advance = false
        and a.cur_excluded = false
        and (a.ref_num is null or btrim(coalesce(a.ref_num,'')) = '')
        and round(
          a.raw_delta_before_reservation_ex
          - a.allocated_reserved_amount_ex_vat,
          2
        ) > 0
      ) as is_blocked,
      null::uuid as snooze_id,
      null::date as snooze_until_date,
      null::text as note
    from alloc a
  ),
  blocked_items_all as (
    select
      ss.candidate_id,
      ss.timesheet_id,
      ss.booking_id,
      ss.segment_id,
      ss.segment_stable_key,
      ss.ref_num,
      ss.delta_pay_ex_vat as blocked_delta_ex,
      sn.snooze_id,
      sn.snooze_until_date,
      sn.note
    from segment_status ss
    left join active_segment_snoozes sn
      on sn.candidate_id = ss.candidate_id
     and sn.snooze_kind = 'BLOCKED_TIMESHEET'
     and (
       (sn.booking_id is not null and ss.booking_id is not null and sn.booking_id = ss.booking_id and sn.segment_stable_key is not distinct from ss.segment_stable_key)
       or (sn.booking_id is null and sn.timesheet_id is not distinct from ss.timesheet_id and sn.segment_id is not distinct from ss.segment_id)
     )
    where ss.is_blocked = true
  ),
blocked_items as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.booking_id,
      b.segment_id,
      b.segment_stable_key,
      b.ref_num,
      b.blocked_delta_ex,
      b.snooze_id
    from blocked_items_all b
    where b.snooze_id is null
  ),
  blocked_items_snoozed as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.booking_id,
      b.segment_id,
      b.segment_stable_key,
      b.ref_num,
      b.blocked_delta_ex,
      b.snooze_id,
      b.snooze_until_date,
      b.note
    from blocked_items_all b
    where b.snooze_id is not null
      and b.snooze_until_date is not null
  ),
  do_not_pay_all as (
    select
      ss.candidate_id,
      ss.timesheet_id,
      ss.booking_id,
      ss.segment_id,
      ss.segment_stable_key,
      ss.ref_num as ref_num,
      ss.delta_pay_ex_vat as raw_delta_ex,
      sn.snooze_id,
      sn.snooze_until_date,
      sn.note
    from segment_status ss
    left join active_segment_snoozes sn
      on sn.candidate_id = ss.candidate_id
     and sn.snooze_kind = 'DO_NOT_PAY'
     and (
       (sn.booking_id is not null and ss.booking_id is not null and sn.booking_id = ss.booking_id and sn.segment_stable_key is not distinct from ss.segment_stable_key)
       or (sn.booking_id is null and sn.timesheet_id is not distinct from ss.timesheet_id and sn.segment_id is not distinct from ss.segment_id)
     )
    where ss.is_do_not_pay = true
  ),
  do_not_pay_items as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.booking_id,
      d.segment_id,
      d.segment_stable_key,
      d.ref_num,
      d.raw_delta_ex,
      d.snooze_id
    from do_not_pay_all d
    where d.snooze_id is null
       or coalesce(d.raw_delta_ex,0) <> 0
  ),
  do_not_pay_items_snoozed as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.booking_id,
      d.segment_id,
      d.segment_stable_key,
      d.ref_num,
      d.raw_delta_ex,
      d.snooze_id,
      d.snooze_until_date,
      d.note
    from do_not_pay_all d
    where d.snooze_id is not null
      and d.snooze_until_date is not null
      and coalesce(d.raw_delta_ex,0) = 0
  ),
  ts_deltas as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.client_id,

      b.ts_week_ending_date,
      b.ts_client_name,
      b.ts_pay_method,
      b.cand_pay_method,

      b.cand_tms_ref,
      b.cand_display_name,
      b.cand_umbrella_id,

      b.umb_enabled,
      b.umb_vat_chargeable,

      b.cand_bank_hash,
      b.umb_bank_hash,

      -- SEGMENTS: reservation math stays in the economic entitlement keyspace,
      -- while stable identifiers are retained only for UI continuity.
      -- Ordinality must be based on the original preview rows (before preview-row reservation subtraction),
      -- otherwise preview_seg:<timesheet_id>:<ord> draft rows will not line up with the rows they reserved.
      coalesce((
        with ss_rows as (
          select
            ss.segment_id,
            ss.segment_key,
            ss.segment_stable_key,
            ss.component_key_type,
            ss.component_key_value,
            ss.work_date,
            ss.ref_num,
            ss.preview_base_eff_delta_ex,
            ss.eff_delta_ex,
            ss.eff_delta_charge_ex_vat,
            ss.source_units,
            ss.source_rate,
            ss.source_charge_rate,
            ss.bucket_row_ord,
            ss.segment_sort_ord,
            row_number() over (
              partition by ss.timesheet_id
              order by ss.segment_sort_ord nulls last, ss.bucket_row_ord, ss.component_key_type, ss.component_key_value, ss.segment_id nulls last
            ) as seg_ord
          from segment_status ss
          where ss.timesheet_id = b.timesheet_id
            and ss.preview_base_eff_delta_ex <> 0
        ),
        ss_effective as (
          select
            ssr.segment_id,
            ssr.segment_key,
            ssr.segment_stable_key,
            ssr.component_key_type,
            ssr.component_key_value,
            ssr.work_date,
            ssr.ref_num,
            ssr.eff_delta_charge_ex_vat,
            ssr.source_units,
            ssr.source_rate,
            ssr.source_charge_rate,
            ssr.bucket_row_ord,
            ssr.segment_sort_ord,
            round(
              ssr.eff_delta_ex - coalesce(rpso.reserved_amount_ex_vat,0),
              2
            ) as eff_delta_ex_after_reserved
          from ss_rows ssr
          left join reserved_preview_segment_ords rpso
            on rpso.timesheet_id = b.timesheet_id
           and rpso.preview_seg_ord = ssr.seg_ord
        )
        select jsonb_agg(
          jsonb_build_object(
            'segment_id', sse.segment_id,
            'segment_key', sse.segment_key,
            'segment_stable_key', sse.segment_stable_key,
            'component_key_type', sse.component_key_type,
            'component_key_value', sse.component_key_value,
            'work_date', sse.work_date,
            'ref_num', sse.ref_num,
            'delta_pay_ex_vat', sse.eff_delta_ex_after_reserved,
            'delta_charge_ex_vat', sse.eff_delta_charge_ex_vat,
            'source_units', sse.source_units
          )
          order by sse.segment_sort_ord nulls last, sse.bucket_row_ord, sse.component_key_type, sse.component_key_value, sse.segment_id nulls last
        )
        from ss_effective sse
        where sse.eff_delta_ex_after_reserved <> 0
      ), '[]'::jsonb) as segment_deltas_json,

      -- ADDITIONAL (total): outstanding = current - baseline - reserved
      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_additional_pay_ex_vat,0)
              - coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'additional'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_additional_pay_ex_vat,0)
            - coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'additional'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_additional_pay_ex_vat,
      round(coalesce(b.current_additional_charge_ex_vat,0) - coalesce(nullif(b.base_json->>'additional_charge_ex_vat','')::numeric,0),2) as delta_additional_charge_ex_vat,

      -- ADDITIONAL (per code): best-effort from additional_units_json + baseline snapshot + reserved breakdowns
      coalesce((
        with
        cur as (
          select
            nullif(btrim(coalesce(e.key,'')), '') as code,
            round(
              coalesce(
                nullif(e.value->>'pay_ex_vat','')::numeric,
                nullif(e.value->>'amount_ex_vat','')::numeric,
                nullif(e.value->>'pay_amount_ex_vat','')::numeric,
                nullif(e.value->>'pay_amount','')::numeric,
                (
                  coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric, 0)
                  * coalesce(nullif(e.value->>'rate','')::numeric, 0)
                ),
                0
              ),
              2
            ) as amount_ex_vat,
            round(coalesce(nullif(e.value->>'charge_ex_vat','')::numeric, nullif(e.value->>'charge_amount_ex_vat','')::numeric, nullif(e.value->>'charge_amount','')::numeric, (coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric, 0) * coalesce(nullif(e.value->>'charge_rate','')::numeric, 0)), 0),2) as charge_amount_ex_vat,
            coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric) as source_units,
            coalesce(nullif(e.value->>'rate','')::numeric, nullif(e.value->>'pay_rate','')::numeric) as source_rate,
            coalesce(nullif(e.value->>'charge_rate','')::numeric, nullif(e.value->>'charge_unit_rate','')::numeric) as source_charge_rate
          from jsonb_each(case when jsonb_typeof(b.current_additional_units_json) = 'object' then b.current_additional_units_json else '{}'::jsonb end) e
          where jsonb_typeof(coalesce(b.current_additional_units_json,'{}'::jsonb)) = 'object'
            and nullif(btrim(coalesce(e.key,'')), '') is not null
        ),
        cur_arr as (
          select
            nullif(btrim(coalesce(elem->>'code','')),'') as code,
            round(
              coalesce(
                nullif(elem->>'pay_ex_vat','')::numeric,
                nullif(elem->>'amount_ex_vat','')::numeric,
                nullif(elem->>'pay_amount_ex_vat','')::numeric,
                nullif(elem->>'pay_amount','')::numeric,
                (
                  coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric, 0)
                  * coalesce(nullif(elem->>'rate','')::numeric, 0)
                ),
                0
              ),
              2
            ) as amount_ex_vat,
            round(coalesce(nullif(elem->>'charge_ex_vat','')::numeric, nullif(elem->>'charge_amount_ex_vat','')::numeric, nullif(elem->>'charge_amount','')::numeric, (coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric, 0) * coalesce(nullif(elem->>'charge_rate','')::numeric, 0)), 0),2) as charge_amount_ex_vat,
            coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric) as source_units,
            coalesce(nullif(elem->>'rate','')::numeric, nullif(elem->>'pay_rate','')::numeric) as source_rate,
            coalesce(nullif(elem->>'charge_rate','')::numeric, nullif(elem->>'charge_unit_rate','')::numeric) as source_charge_rate
          from jsonb_array_elements(case when jsonb_typeof(b.current_additional_units_json) = 'array' then b.current_additional_units_json else '[]'::jsonb end) elem
          where jsonb_typeof(coalesce(b.current_additional_units_json,'[]'::jsonb)) = 'array'
            and nullif(btrim(coalesce(elem->>'code','')),'') is not null
        ),
        cur_all as (
          select * from cur
          union all
          select * from cur_arr
        ),
        bas as (
          select
            nullif(btrim(coalesce(e.key,'')), '') as code,
            round(
              coalesce(
                nullif(e.value->>'pay_ex_vat','')::numeric,
                nullif(e.value->>'amount_ex_vat','')::numeric,
                nullif(e.value->>'pay_amount_ex_vat','')::numeric,
                nullif(e.value->>'pay_amount','')::numeric,
                (
                  coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric, 0)
                  * coalesce(nullif(e.value->>'rate','')::numeric, 0)
                ),
                0
              ),
              2
            ) as amount_ex_vat,
            round(coalesce(nullif(e.value->>'charge_ex_vat','')::numeric, nullif(e.value->>'charge_amount_ex_vat','')::numeric, nullif(e.value->>'charge_amount','')::numeric, (coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric, 0) * coalesce(nullif(e.value->>'charge_rate','')::numeric, 0)), 0),2) as charge_amount_ex_vat,
            coalesce(nullif(e.value->>'units','')::numeric, nullif(e.value->>'units_week','')::numeric) as source_units,
            coalesce(nullif(e.value->>'rate','')::numeric, nullif(e.value->>'pay_rate','')::numeric) as source_rate,
            coalesce(nullif(e.value->>'charge_rate','')::numeric, nullif(e.value->>'charge_unit_rate','')::numeric) as source_charge_rate
          from jsonb_each(case when jsonb_typeof(b.base_json->'additional_units_json') = 'object' then b.base_json->'additional_units_json' else '{}'::jsonb end) e
          where jsonb_typeof(coalesce(b.base_json->'additional_units_json','{}'::jsonb)) = 'object'
            and nullif(btrim(coalesce(e.key,'')), '') is not null
        ),
        bas_arr as (
          select
            nullif(btrim(coalesce(elem->>'code','')),'') as code,
            round(
              coalesce(
                nullif(elem->>'pay_ex_vat','')::numeric,
                nullif(elem->>'amount_ex_vat','')::numeric,
                nullif(elem->>'pay_amount_ex_vat','')::numeric,
                nullif(elem->>'pay_amount','')::numeric,
                (
                  coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric, 0)
                  * coalesce(nullif(elem->>'rate','')::numeric, 0)
                ),
                0
              ),
              2
            ) as amount_ex_vat,
            round(coalesce(nullif(elem->>'charge_ex_vat','')::numeric, nullif(elem->>'charge_amount_ex_vat','')::numeric, nullif(elem->>'charge_amount','')::numeric, (coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric, 0) * coalesce(nullif(elem->>'charge_rate','')::numeric, 0)), 0),2) as charge_amount_ex_vat,
            coalesce(nullif(elem->>'units','')::numeric, nullif(elem->>'units_week','')::numeric) as source_units,
            coalesce(nullif(elem->>'rate','')::numeric, nullif(elem->>'pay_rate','')::numeric) as source_rate,
            coalesce(nullif(elem->>'charge_rate','')::numeric, nullif(elem->>'charge_unit_rate','')::numeric) as source_charge_rate
          from jsonb_array_elements(case when jsonb_typeof(b.base_json->'additional_units_json') = 'array' then b.base_json->'additional_units_json' else '[]'::jsonb end) elem
          where jsonb_typeof(coalesce(b.base_json->'additional_units_json','[]'::jsonb)) = 'array'
            and nullif(btrim(coalesce(elem->>'code','')),'') is not null
        ),
        bas_all as (
          select * from bas
          union all
          select * from bas_arr
        ),
        ids as (
          select distinct
            x.code
          from (
            select code from cur_all where code is not null
            union
            select code from bas_all where code is not null
            union
            select rab.code from reserved_additional_by_code rab where rab.timesheet_id = b.timesheet_id and rab.code is not null
          ) x
        ),
        rows as (
          select
            i.code,
            round(
              coalesce((select sum(ca.amount_ex_vat) from cur_all ca where ca.code = i.code),0)
              - coalesce((select sum(ba.amount_ex_vat) from bas_all ba where ba.code = i.code),0),
              2
            ) as raw_delta_amount_ex_vat,
            round(
              coalesce((select sum(ca.charge_amount_ex_vat) from cur_all ca where ca.code = i.code),0)
              - coalesce((select sum(ba.charge_amount_ex_vat) from bas_all ba where ba.code = i.code),0),
              2
            ) as raw_delta_charge_ex_vat,
            coalesce((select max(ca.source_units) from cur_all ca where ca.code = i.code),(select max(ba.source_units) from bas_all ba where ba.code = i.code)) as source_units,
            coalesce((select max(ca.source_rate) from cur_all ca where ca.code = i.code),(select max(ba.source_rate) from bas_all ba where ba.code = i.code)) as source_rate,
            coalesce((select max(ca.source_charge_rate) from cur_all ca where ca.code = i.code),(select max(ba.source_charge_rate) from bas_all ba where ba.code = i.code)) as source_charge_rate,
            case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce((select sum(ca.amount_ex_vat) from cur_all ca where ca.code = i.code),0)
                  - coalesce((select sum(ba.amount_ex_vat) from bas_all ba where ba.code = i.code),0)
                  - coalesce((select rab.reserved_amount_ex_vat from reserved_additional_by_code rab where rab.timesheet_id = b.timesheet_id and rab.code = i.code limit 1),0),
                  2
                ) < 0
              )
              then 0::numeric
              else round(
                coalesce((select sum(ca.amount_ex_vat) from cur_all ca where ca.code = i.code),0)
                - coalesce((select sum(ba.amount_ex_vat) from bas_all ba where ba.code = i.code),0)
                - coalesce((select rab.reserved_amount_ex_vat from reserved_additional_by_code rab where rab.timesheet_id = b.timesheet_id and rab.code = i.code limit 1),0),
                2
              )
            end as delta_amount_ex_vat
          from ids i
        )
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'code', r.code,
              'delta_pay_ex_vat', r.delta_amount_ex_vat,
              'delta_charge_ex_vat', case when coalesce(r.raw_delta_amount_ex_vat,0) = 0 then r.raw_delta_charge_ex_vat else round(r.raw_delta_charge_ex_vat * (r.delta_amount_ex_vat / nullif(r.raw_delta_amount_ex_vat,0)), 2) end,
              'source_units', r.source_units,
              'source_rate', r.source_rate,
              'source_charge_rate', r.source_charge_rate
            )
            order by r.code
          ) filter (where r.delta_amount_ex_vat <> 0),
          '[]'::jsonb
        )
        from rows r
      ), '[]'::jsonb) as additional_unit_deltas_json,

      -- EXPENSES/TRAVEL/etc (outstanding = current - baseline - reserved)
      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_expenses_pay_ex_vat,0)
              - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'expenses'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_expenses_pay_ex_vat,0)
            - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'expenses'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_expenses_pay_ex_vat,
      round(coalesce(b.current_expenses_charge_ex_vat,0) - coalesce(nullif(b.base_json #>> '{expenses,expenses_charge_ex_vat}','')::numeric,0),2) as delta_expenses_charge_ex_vat,

      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_travel_pay_ex_vat,0)
              - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'travel'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_travel_pay_ex_vat,0)
            - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'travel'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_travel_pay_ex_vat,
      round(coalesce(b.current_travel_charge_ex_vat,0) - coalesce(nullif(b.base_json #>> '{expenses,travel_charge_ex_vat}','')::numeric,0),2) as delta_travel_charge_ex_vat,

      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_accommodation_pay_ex_vat,0)
              - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'accommodation'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_accommodation_pay_ex_vat,0)
            - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'accommodation'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_accommodation_pay_ex_vat,
      round(coalesce(b.current_accommodation_charge_ex_vat,0) - coalesce(nullif(b.base_json #>> '{expenses,accommodation_charge_ex_vat}','')::numeric,0),2) as delta_accommodation_charge_ex_vat,

      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_other_pay_ex_vat,0)
              - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'other'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_other_pay_ex_vat,0)
            - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'other'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_other_pay_ex_vat,
      round(coalesce(b.current_other_charge_ex_vat,0) - coalesce(nullif(b.base_json #>> '{expenses,other_charge_ex_vat}','')::numeric,0),2) as delta_other_charge_ex_vat,

      round(
        case
          when (
            coalesce(b.has_active_overpayment_case,false) = true
            and round(
              coalesce(b.current_mileage_pay_ex_vat,0)
              - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = 'mileage'
                  limit 1
                ), 0),
              2
            ) < 0
          )
          then 0
          else round(
            coalesce(b.current_mileage_pay_ex_vat,0)
            - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0)
            - coalesce((
                select r.reserved_amount_ex_vat
                from reserved_by_source_ref r
                where r.timesheet_id = b.timesheet_id
                  and r.source_ref = 'mileage'
                limit 1
              ), 0),
            2
          )
        end,
        2
      ) as delta_mileage_pay_ex_vat,
      round(coalesce(b.current_mileage_charge_ex_vat,0) - coalesce(nullif(b.base_json #>> '{expenses,mileage_charge_ex_vat}','')::numeric,0),2) as delta_mileage_charge_ex_vat,

      -- ADJUSTMENTS: outstanding = current - baseline - reserved (source_ref='adj:<id>')
      coalesce((
        with
        cur as (
          select
            a.adj_id,
            round(coalesce(a.delta_pay_ex_vat,0),2) as amt
          from adj a
          where a.timesheet_id = b.timesheet_id
        ),
        bas as (
          select
            nullif(btrim(coalesce(x->>'adj_id','')),'')::uuid as adj_id,
            round(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0),2) as amt
          from jsonb_array_elements(coalesce(b.base_json->'adjustments','[]'::jsonb)) x
          where nullif(btrim(coalesce(x->>'adj_id','')),'') is not null
        ),
        ids as (
          select distinct adj_id from cur
          union
          select distinct adj_id from bas
        ),
        rows_base as (
          select
            i.adj_id,
            round(
              coalesce((select c.amt from cur c where c.adj_id = i.adj_id limit 1),0)
              - coalesce((select x.amt from bas x where x.adj_id = i.adj_id limit 1),0)
              - coalesce((
                  select r.reserved_amount_ex_vat
                  from reserved_by_source_ref r
                  where r.timesheet_id = b.timesheet_id
                    and r.source_ref = ('adj:'||i.adj_id::text)
                  limit 1
                ), 0),
              2
            ) as delta_amt
          from ids i
        ),
        preview_reserved_total as (
          select
            round(
              coalesce(sum(abs(rpso.reserved_amount_ex_vat)),0),
              2
            ) as reserved_abs
          from reserved_preview_segment_ords rpso
          where rpso.timesheet_id = b.timesheet_id
            and coalesce(rpso.reserved_amount_ex_vat,0) < 0
        ),
        rows_negative as (
          select
            rb.adj_id,
            rb.delta_amt,
            row_number() over (order by rb.adj_id::text) as neg_ord,
            coalesce(
              sum(abs(rb.delta_amt)) over (
                order by rb.adj_id::text
                rows between unbounded preceding and 1 preceding
              ),
              0
            ) as prev_neg_abs
          from rows_base rb
          where rb.delta_amt < 0
        ),
        rows_negative_applied as (
          select
            rn.adj_id,
            round(
              rn.delta_amt
              + greatest(
                  least(
                    coalesce(prt.reserved_abs,0) - rn.prev_neg_abs,
                    abs(rn.delta_amt)
                  ),
                  0
                ),
              2
            ) as delta_amt
          from rows_negative rn
          cross join preview_reserved_total prt
        ),
        rows as (
          select
            rb.adj_id,
            case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and coalesce(rna.delta_amt, rb.delta_amt) < 0
              )
              then 0::numeric
              else coalesce(rna.delta_amt, rb.delta_amt)
            end as delta_amt
          from rows_base rb
          left join rows_negative_applied rna
            on rna.adj_id = rb.adj_id
        )
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'adj_id', r.adj_id,
              'delta_pay_ex_vat', r.delta_amt
            )
            order by r.adj_id::text
          ) filter (where r.delta_amt <> 0),
          '[]'::jsonb
        )
        from rows r
      ), '[]'::jsonb) as adjustment_deltas_json,

      -- Reservation integrity (preview-only signal): reserved > remaining previewable truth
      (
        coalesce((
          select rtb.reserved_total_ex_vat
          from reserved_total_by_timesheet rtb
          where rtb.timesheet_id = b.timesheet_id
          limit 1
        ), 0)
        >
        (
          coalesce((
            select round(
              sum(
                case
                  when coalesce(ss.raw_delta_before_reservation_ex,0) < 0
                       and coalesce(b.has_active_overpayment_case,false) = true
                    then 0
                  else coalesce(ss.raw_delta_before_reservation_ex,0)
                end
              ),
              2
            )
            from segment_status ss
            where ss.timesheet_id = b.timesheet_id
          ), 0)
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_additional_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_additional_pay_ex_vat,0)
                - coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_expenses_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_expenses_pay_ex_vat,0)
                - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_travel_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_travel_pay_ex_vat,0)
                - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_accommodation_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_accommodation_pay_ex_vat,0)
                - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_other_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_other_pay_ex_vat,0)
                - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce(b.current_mileage_pay_ex_vat,0)
                  - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce(b.current_mileage_pay_ex_vat,0)
                - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),
                2
              )
            end
          + case
              when (
                coalesce(b.has_active_overpayment_case,false) = true
                and round(
                  coalesce((
                    select round(sum(coalesce(a.delta_pay_ex_vat,0)),2)
                    from adj a
                    where a.timesheet_id = b.timesheet_id
                  ),0)
                  - coalesce((
                    select round(sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)),2)
                    from jsonb_array_elements(coalesce(b.base_json->'adjustments','[]'::jsonb)) x
                  ),0),
                  2
                ) < 0
              )
              then 0
              else round(
                coalesce((
                  select round(sum(coalesce(a.delta_pay_ex_vat,0)),2)
                  from adj a
                  where a.timesheet_id = b.timesheet_id
                ),0)
                - coalesce((
                  select round(sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)),2)
                  from jsonb_array_elements(coalesce(b.base_json->'adjustments','[]'::jsonb)) x
                ),0),
                2
              )
            end
        ) + 0.01
      ) as reservation_overrun_detected
    from ts_baseline b
  ),
ts_itemised as (
    select
      d2.*,
      d2.total_ex as payment_amount_ex_vat,
      case
        when d2.ts_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(d2.total_ex, v_vat_rate_pct, d2.umb_vat_chargeable)->>'inc')::numeric
        else d2.total_ex
      end as payment_amount_inc_vat,
      case
        when d2.ts_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(d2.total_ex, v_vat_rate_pct, d2.umb_vat_chargeable)->>'inc')::numeric
        else d2.total_ex
      end as payment_amount
    from (
      select
        d1.*,
        round(
          coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d1.segment_deltas_json) x),0)
          + coalesce(d1.delta_additional_pay_ex_vat,0)
          + coalesce(d1.delta_expenses_pay_ex_vat,0)
          + coalesce(d1.delta_travel_pay_ex_vat,0)
          + coalesce(d1.delta_accommodation_pay_ex_vat,0)
          + coalesce(d1.delta_other_pay_ex_vat,0)
          + coalesce(d1.delta_mileage_pay_ex_vat,0)
          + coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d1.adjustment_deltas_json) x),0),
          2
        ) as total_ex
      from ts_deltas d1
    ) d2
  ),
  worked_time_current_segment_rows as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.client_id,
      b.ts_week_ending_date,
      b.ts_client_name,
      b.ts_pay_method,
      b.cand_pay_method,
      b.cand_tms_ref,
      b.cand_display_name,
      b.cand_umbrella_id,
      b.umb_enabled,
      b.umb_vat_chargeable,
      b.cand_bank_hash,
      b.umb_bank_hash,
      cur_seg.seg_ord as segment_sort_ord,
      nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), '') as segment_id,
      nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), '') as segment_key,
      coalesce(
        nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')), ''),
        nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
        nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), ''),
        nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''),
        nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), '')
      ) as segment_stable_key,
      nullif(btrim(coalesce(cur_seg.seg->>'date','')), '') as work_date,
      nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), '') as ref_num,
      case
        when nullif(btrim(coalesce(cur_seg.seg->>'date','')), '') is not null then 'TS_DAY'::text
        else 'TS_TOTAL'::text
      end as component_key_type,
      case
        when nullif(btrim(coalesce(cur_seg.seg->>'date','')), '') is not null
          then nullif(btrim(coalesce(cur_seg.seg->>'date','')), '')
        else 'TOTAL'
      end as component_key_value,
      bucket.bucket_code,
      bucket.bucket_sort_ord,
      round(bucket.source_units, 6) as source_units,
      case bucket.bucket_code
        when 'DAY' then b.cur_pay_day
        when 'NIGHT' then b.cur_pay_night
        when 'SAT' then b.cur_pay_sat
        when 'SUN' then b.cur_pay_sun
        when 'BH' then b.cur_pay_bh
        else null::numeric
      end as source_rate,
      case bucket.bucket_code
        when 'DAY' then b.cur_charge_day
        when 'NIGHT' then b.cur_charge_night
        when 'SAT' then b.cur_charge_sat
        when 'SUN' then b.cur_charge_sun
        when 'BH' then b.cur_charge_bh
        else null::numeric
      end as source_charge_rate
    from ts_baseline b
    join lateral jsonb_array_elements(coalesce(b.current_segments_json, '[]'::jsonb)) with ordinality as cur_seg(seg, seg_ord) on true
    join lateral (
      values
        ('DAY'::text, 1::int, coalesce(nullif(cur_seg.seg->>'hours_day','')::numeric, 0)),
        ('NIGHT'::text, 2::int, coalesce(nullif(cur_seg.seg->>'hours_night','')::numeric, 0)),
        ('SAT'::text, 3::int, coalesce(nullif(cur_seg.seg->>'hours_sat','')::numeric, 0)),
        ('SUN'::text, 4::int, coalesce(nullif(cur_seg.seg->>'hours_sun','')::numeric, 0)),
        ('BH'::text, 5::int, coalesce(nullif(cur_seg.seg->>'hours_bh','')::numeric, 0))
    ) as bucket(bucket_code, bucket_sort_ord, source_units) on true
    where cur_seg.seg is not null
      and jsonb_typeof(cur_seg.seg) = 'object'
      and coalesce(bucket.source_units, 0) <> 0
  ),
  worked_time_baseline_segment_rows as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.client_id,
      b.ts_week_ending_date,
      b.ts_client_name,
      b.ts_pay_method,
      b.cand_pay_method,
      b.cand_tms_ref,
      b.cand_display_name,
      b.cand_umbrella_id,
      b.umb_enabled,
      b.umb_vat_chargeable,
      b.cand_bank_hash,
      b.umb_bank_hash,
      bas_seg.seg_ord as segment_sort_ord,
      nullif(btrim(coalesce(bas_seg.seg->>'segment_id','')), '') as segment_id,
      nullif(btrim(coalesce(bas_seg.seg->>'segment_key','')), '') as segment_key,
      coalesce(
        nullif(btrim(coalesce(bas_seg.seg->>'segment_stable_key','')), ''),
        nullif(btrim(coalesce(bas_seg.seg->>'segment_id','')), ''),
        nullif(btrim(coalesce(bas_seg.seg->>'segment_key','')), ''),
        nullif(btrim(coalesce(bas_seg.seg->>'date','')), ''),
        nullif(btrim(coalesce(bas_seg.seg->>'ref_num','')), '')
      ) as segment_stable_key,
      nullif(btrim(coalesce(bas_seg.seg->>'date','')), '') as work_date,
      nullif(btrim(coalesce(bas_seg.seg->>'ref_num','')), '') as ref_num,
      case
        when nullif(btrim(coalesce(bas_seg.seg->>'date','')), '') is not null then 'TS_DAY'::text
        else 'TS_TOTAL'::text
      end as component_key_type,
      case
        when nullif(btrim(coalesce(bas_seg.seg->>'date','')), '') is not null
          then nullif(btrim(coalesce(bas_seg.seg->>'date','')), '')
        else 'TOTAL'
      end as component_key_value,
      bucket.bucket_code,
      bucket.bucket_sort_ord,
      round(bucket.source_units, 6) as source_units,
      case bucket.bucket_code
        when 'DAY' then b.bas_pay_day
        when 'NIGHT' then b.bas_pay_night
        when 'SAT' then b.bas_pay_sat
        when 'SUN' then b.bas_pay_sun
        when 'BH' then b.bas_pay_bh
        else null::numeric
      end as source_rate,
      case bucket.bucket_code
        when 'DAY' then b.bas_charge_day
        when 'NIGHT' then b.bas_charge_night
        when 'SAT' then b.bas_charge_sat
        when 'SUN' then b.bas_charge_sun
        when 'BH' then b.bas_charge_bh
        else null::numeric
      end as source_charge_rate
    from ts_baseline b
    join lateral jsonb_array_elements(
      case
        when jsonb_typeof(b.base_json->'segments') = 'array'
         and jsonb_array_length(coalesce(b.base_json->'segments', '[]'::jsonb)) > 0
          then coalesce(b.base_json->'segments', '[]'::jsonb)
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || b.timesheet_id::text),
            'segment_key', ('ts:' || b.timesheet_id::text),
            'segment_stable_key', ('timesheet:' || coalesce(b.ts_booking_id, b.timesheet_id::text)),
            'date', null,
            'ref_num', null,
            'hours_day', b.bas_hours_day,
            'hours_night', b.bas_hours_night,
            'hours_sat', b.bas_hours_sat,
            'hours_sun', b.bas_hours_sun,
            'hours_bh', b.bas_hours_bh
          )
        )
      end
    ) with ordinality as bas_seg(seg, seg_ord) on true
    join lateral (
      values
        ('DAY'::text, 1::int, coalesce(nullif(bas_seg.seg->>'hours_day','')::numeric, 0)),
        ('NIGHT'::text, 2::int, coalesce(nullif(bas_seg.seg->>'hours_night','')::numeric, 0)),
        ('SAT'::text, 3::int, coalesce(nullif(bas_seg.seg->>'hours_sat','')::numeric, 0)),
        ('SUN'::text, 4::int, coalesce(nullif(bas_seg.seg->>'hours_sun','')::numeric, 0)),
        ('BH'::text, 5::int, coalesce(nullif(bas_seg.seg->>'hours_bh','')::numeric, 0))
    ) as bucket(bucket_code, bucket_sort_ord, source_units) on true
    where bas_seg.seg is not null
      and jsonb_typeof(bas_seg.seg) = 'object'
      and coalesce(bucket.source_units, 0) <> 0
  ),
  worked_time_current_ranked as (
    select
      wcsr.*,
      row_number() over (
        partition by wcsr.timesheet_id, wcsr.candidate_id, wcsr.component_key_type, wcsr.component_key_value, wcsr.bucket_code
        order by wcsr.segment_sort_ord nulls last, wcsr.segment_id nulls last, wcsr.ref_num nulls last
      ) as bucket_row_ord
    from worked_time_current_segment_rows wcsr
  ),
  worked_time_baseline_ranked as (
    select
      wbsr.*,
      row_number() over (
        partition by wbsr.timesheet_id, wbsr.candidate_id, wbsr.component_key_type, wbsr.component_key_value, wbsr.bucket_code
        order by wbsr.segment_sort_ord nulls last, wbsr.segment_id nulls last, wbsr.ref_num nulls last
      ) as bucket_row_ord
    from worked_time_baseline_segment_rows wbsr
  ),
  worked_time_bucket_ids as (
    select distinct
      wcr.timesheet_id,
      wcr.candidate_id,
      wcr.component_key_type,
      wcr.component_key_value,
      wcr.bucket_code,
      wcr.bucket_sort_ord,
      wcr.bucket_row_ord
    from worked_time_current_ranked wcr
    union
    select distinct
      wbr.timesheet_id,
      wbr.candidate_id,
      wbr.component_key_type,
      wbr.component_key_value,
      wbr.bucket_code,
      wbr.bucket_sort_ord,
      wbr.bucket_row_ord
    from worked_time_baseline_ranked wbr
  ),
  worked_time_bucket_agg as (
    select
      wbi.timesheet_id,
      wbi.candidate_id,
      wbi.component_key_type,
      wbi.component_key_value,
      wbi.bucket_code,
      max(wbi.bucket_sort_ord) as bucket_sort_ord,
      wbi.bucket_row_ord,
      max(wcr.segment_id) as cur_segment_id,
      max(wbr.segment_id) as bas_segment_id,
      max(coalesce(wcr.segment_key, wbr.segment_key)) as segment_key,
      max(coalesce(wcr.segment_stable_key, wbr.segment_stable_key)) as segment_stable_key,
      max(coalesce(wcr.ref_num, wbr.ref_num)) as ref_num,
      max(coalesce(wcr.work_date, wbr.work_date)) as work_date,
      max(wcr.segment_sort_ord) as cur_segment_sort_ord,
      max(wbr.segment_sort_ord) as bas_segment_sort_ord,
      max(wcr.source_rate) as cur_source_rate,
      max(wcr.source_charge_rate) as cur_source_charge_rate,
      round(sum(case when wcr.bucket_row_ord = wbi.bucket_row_ord then coalesce(wcr.source_units,0) else 0 end), 6) as cur_source_units,
      round(sum(case when wbr.bucket_row_ord = wbi.bucket_row_ord then coalesce(wbr.source_units,0) else 0 end), 6) as bas_source_units,
      round(sum(
        case
          when wcr.bucket_row_ord = wbi.bucket_row_ord and wcr.source_rate is not null
            then coalesce(wcr.source_units,0) * wcr.source_rate
          else 0
        end
      ), 2) as cur_pay_amount_ex_vat,
      round(sum(
        case
          when wbr.bucket_row_ord = wbi.bucket_row_ord and wbr.source_rate is not null
            then coalesce(wbr.source_units,0) * wbr.source_rate
          else 0
        end
      ), 2) as bas_pay_amount_ex_vat,
      round(sum(
        case
          when wcr.bucket_row_ord = wbi.bucket_row_ord and wcr.source_charge_rate is not null
            then coalesce(wcr.source_units,0) * wcr.source_charge_rate
          else 0
        end
      ), 2) as cur_charge_amount_ex_vat,
      round(sum(
        case
          when wbr.bucket_row_ord = wbi.bucket_row_ord and wbr.source_charge_rate is not null
            then coalesce(wbr.source_units,0) * wbr.source_charge_rate
          else 0
        end
      ), 2) as bas_charge_amount_ex_vat
    from worked_time_bucket_ids wbi
    left join worked_time_current_ranked wcr
      on wcr.timesheet_id = wbi.timesheet_id
     and wcr.candidate_id = wbi.candidate_id
     and wcr.component_key_type = wbi.component_key_type
     and wcr.component_key_value = wbi.component_key_value
     and wcr.bucket_code = wbi.bucket_code
     and wcr.bucket_row_ord = wbi.bucket_row_ord
    left join worked_time_baseline_ranked wbr
      on wbr.timesheet_id = wbi.timesheet_id
     and wbr.candidate_id = wbi.candidate_id
     and wbr.component_key_type = wbi.component_key_type
     and wbr.component_key_value = wbi.component_key_value
     and wbr.bucket_code = wbi.bucket_code
     and wbr.bucket_row_ord = wbi.bucket_row_ord
    group by
      wbi.timesheet_id,
      wbi.candidate_id,
      wbi.component_key_type,
      wbi.component_key_value,
      wbi.bucket_code,
      wbi.bucket_row_ord
  ),
  worked_time_bucket_calc as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.client_id,
      b.ts_week_ending_date,
      b.ts_client_name,
      b.ts_pay_method,
      b.cand_pay_method,
      b.cand_tms_ref,
      b.cand_display_name,
      b.cand_umbrella_id,
      b.umb_enabled,
      b.umb_vat_chargeable,
      b.cand_bank_hash,
      b.umb_bank_hash,
      b.ts_booking_id as booking_id,
      coalesce(wba.cur_segment_id, wba.bas_segment_id) as segment_id,
      coalesce(wba.segment_key, coalesce(wba.cur_segment_id, wba.bas_segment_id)) as segment_key,
      wba.segment_stable_key,
      wba.component_key_type,
      wba.component_key_value,
      wba.bucket_code,
      wba.bucket_sort_ord,
      wba.bucket_row_ord,
      coalesce(wba.cur_segment_sort_ord, wba.bas_segment_sort_ord) as segment_sort_ord,
      wba.work_date,
      wba.ref_num,
      wba.cur_source_rate as source_rate,
      wba.cur_source_charge_rate as source_charge_rate,
      round(coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0), 6) as raw_delta_source_units,
      coalesce(b.has_active_overpayment_case,false) as has_active_overpayment_case,
      b.require_reference_to_pay,
      coalesce(b.is_forced_advance,false) as is_forced_advance,
      round(
        case
          when wba.cur_source_rate is null then 0
          else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
        end,
        2
      ) as raw_delta_before_reservation_ex,
      round(
        case
          when wba.cur_source_charge_rate is null then 0
          else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_charge_rate, 2)
        end,
        2
      ) as raw_delta_charge_ex_vat,
      round(
        case
          when coalesce(b.has_active_overpayment_case,false) = true
           and round(
             case
               when wba.cur_source_rate is null then 0
               else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
             end,
             2
           ) < 0
            then 0
          when b.require_reference_to_pay = true
           and coalesce(b.is_forced_advance,false) = false
           and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
           and round(
             case
               when wba.cur_source_rate is null then 0
               else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
             end,
             2
           ) > 0
            then 0
          else round(
            case
              when wba.cur_source_rate is null then 0
              else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
            end,
            2
          )
        end,
        2
      ) as preview_base_eff_delta_ex,
      round(
        case
          when coalesce(b.has_active_overpayment_case,false) = true
           and round(
             case
               when wba.cur_source_rate is null then 0
               else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
             end,
             2
           ) < 0
            then 0
          when round(
            case
              when wba.cur_source_rate is null then 0
              else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
            end,
            2
          ) > 0
            then round(
              case
                when wba.cur_source_rate is null then 0
                else round((coalesce(wba.cur_source_units,0) - coalesce(wba.bas_source_units,0)) * wba.cur_source_rate, 2)
              end,
              2
            )
          else 0
        end,
        2
      ) as allocatable_delta_ex,
      coalesce(rss.reserved_amount_ex_vat, 0) as reserved_key_amount_ex_vat
    from ts_baseline b
    join worked_time_bucket_agg wba
      on wba.timesheet_id = b.timesheet_id
     and wba.candidate_id = b.candidate_id
    left join reserved_segment_sums rss
      on rss.timesheet_id = b.timesheet_id
     and rss.component_key_type = wba.component_key_type
     and rss.component_key_value = wba.component_key_value
  ),
  worked_time_bucket_alloc as (
    select
      wbc.*,
      round(
        case
          when coalesce(wbc.allocatable_delta_ex,0) <= 0 then 0
          else greatest(
            least(
              coalesce(wbc.reserved_key_amount_ex_vat,0)
              - coalesce(
                  sum(coalesce(wbc.allocatable_delta_ex,0)) over (
                    partition by wbc.timesheet_id, wbc.component_key_type, wbc.component_key_value
                    order by wbc.segment_sort_ord nulls last, wbc.bucket_sort_ord, wbc.bucket_row_ord, wbc.segment_id nulls last
                    rows between unbounded preceding and 1 preceding
                  ),
                  0
                ),
              wbc.allocatable_delta_ex
            ),
            0
          )
        end,
        2
      ) as allocated_reserved_amount_ex_vat
    from worked_time_bucket_calc wbc
  ),
  worked_time_bucket_effective as (
    select
      wba.candidate_id,
      wba.timesheet_id,
      wba.client_id,
      wba.ts_week_ending_date,
      wba.ts_client_name,
      wba.ts_pay_method,
      wba.cand_pay_method,
      wba.cand_tms_ref,
      wba.cand_display_name,
      wba.cand_umbrella_id,
      wba.umb_enabled,
      wba.umb_vat_chargeable,
      wba.cand_bank_hash,
      wba.umb_bank_hash,
      wba.booking_id,
      wba.segment_id,
      wba.segment_key,
      wba.segment_stable_key,
      wba.component_key_type,
      wba.component_key_value,
      wba.bucket_code,
      wba.bucket_sort_ord,
      wba.bucket_row_ord,
      wba.segment_sort_ord,
      wba.work_date,
      wba.ref_num,
      case
        when wba.source_rate is null or wba.source_rate = 0 then null::numeric
        when round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6) <= 0 then null::numeric
        when round(
          round(greatest(coalesce(wba.raw_delta_source_units,0), 0) * wba.source_rate, 2),
          2
        ) <> round(
          case
            when wba.has_active_overpayment_case = true
             and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) < 0
              then 0
            when wba.require_reference_to_pay = true
             and wba.is_forced_advance = false
             and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
             and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) > 0
              then 0
            else round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2)
          end,
          2
        ) then null::numeric
        else wba.source_rate
      end as source_rate,
      case
        when wba.source_rate is null or wba.source_rate = 0 then null::numeric
        when round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6) <= 0 then null::numeric
        when wba.source_charge_rate is not null then wba.source_charge_rate
        when round(coalesce(wba.raw_delta_charge_ex_vat,0), 2) = 0 then null::numeric
        else round(
          round(coalesce(wba.raw_delta_charge_ex_vat,0), 2)
          / nullif(round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6), 0),
          6
        )
      end as source_charge_rate,
      round(
        case
          when wba.has_active_overpayment_case = true
           and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) < 0
            then 0
          when wba.require_reference_to_pay = true
           and wba.is_forced_advance = false
           and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
           and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) > 0
            then 0
          else round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2)
        end,
        2
      ) as component_amount_ex_vat,
      round(
        case
          when wba.source_charge_rate is not null
           and wba.source_rate is not null
           and wba.source_rate <> 0
           and round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6) > 0
           and round(
             round(greatest(coalesce(wba.raw_delta_source_units,0), 0) * wba.source_rate, 2),
             2
           ) = round(
             case
               when wba.has_active_overpayment_case = true
                and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) < 0
                 then 0
               when wba.require_reference_to_pay = true
                and wba.is_forced_advance = false
                and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
                and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) > 0
                 then 0
               else round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2)
             end,
             2
           )
            then round(greatest(coalesce(wba.raw_delta_source_units,0), 0) * wba.source_charge_rate, 2)
          when round(coalesce(wba.raw_delta_before_reservation_ex,0), 2) = 0 then round(coalesce(wba.raw_delta_charge_ex_vat,0), 2)
          else round(
            coalesce(wba.raw_delta_charge_ex_vat,0)
            * (
                (
                  case
                    when wba.has_active_overpayment_case = true
                     and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) < 0
                      then 0
                    when wba.require_reference_to_pay = true
                     and wba.is_forced_advance = false
                     and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
                     and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) > 0
                      then 0
                    else round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2)
                  end
                ) / nullif(round(coalesce(wba.raw_delta_before_reservation_ex,0), 2), 0)
              ),
            2
          )
        end,
        2
      ) as source_charge_ex_vat,
      case
        when wba.source_rate is null or wba.source_rate = 0 then null::numeric
        when round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6) <= 0 then null::numeric
        when round(
          round(greatest(coalesce(wba.raw_delta_source_units,0), 0) * wba.source_rate, 2),
          2
        ) <> round(
          case
            when wba.has_active_overpayment_case = true
             and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) < 0
              then 0
            when wba.require_reference_to_pay = true
             and wba.is_forced_advance = false
             and (wba.ref_num is null or btrim(coalesce(wba.ref_num,'')) = '')
             and round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2) > 0
              then 0
            else round(coalesce(wba.raw_delta_before_reservation_ex,0) - coalesce(wba.allocated_reserved_amount_ex_vat,0), 2)
          end,
          2
        ) then null::numeric
        else round(greatest(coalesce(wba.raw_delta_source_units,0), 0), 6)
      end as source_units
    from worked_time_bucket_alloc wba
  ),
  worked_time_bucket_component_rows as (
    select
      wtbe.candidate_id,
      wtbe.timesheet_id,
      wtbe.client_id,
      wtbe.ts_week_ending_date,
      wtbe.ts_client_name,
      wtbe.ts_pay_method,
      wtbe.cand_pay_method,
      wtbe.cand_tms_ref,
      wtbe.cand_display_name,
      wtbe.cand_umbrella_id,
      wtbe.umb_enabled,
      wtbe.umb_vat_chargeable,
      wtbe.cand_bank_hash,
      wtbe.umb_bank_hash,
      ('timesheet:' || wtbe.timesheet_id::text) as source_family_key,
      wtbe.component_key_type,
      wtbe.component_key_value,
      'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum as classification,
      round(coalesce(wtbe.component_amount_ex_vat,0), 2) as component_amount_ex_vat,
      round(coalesce(wtbe.source_charge_ex_vat,0), 2) as source_charge_ex_vat,
      case when wtbe.source_units is null then null else round(wtbe.source_units, 6) end as source_units,
      case when wtbe.source_rate is null then null else round(wtbe.source_rate, 6) end as source_rate,
      case when wtbe.source_charge_rate is null then null else round(wtbe.source_charge_rate, 6) end as source_charge_rate,
      upper(coalesce(wtbe.ts_pay_method, '')) as source_pay_method,
      upper(coalesce(wtbe.cand_pay_method, '')) as current_target_pay_method,
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', wtbe.timesheet_id::text,
          'segment_id', wtbe.segment_id,
          'segment_key', wtbe.segment_key,
          'segment_stable_key', wtbe.segment_stable_key,
          'work_date', wtbe.work_date,
          'ref_num', wtbe.ref_num,
          'bucket_code', wtbe.bucket_code,
          'source_units', case when wtbe.source_units is null then null else round(wtbe.source_units, 6) end,
          'source_rate', case when wtbe.source_rate is null then null else round(wtbe.source_rate, 6) end,
          'source_charge_rate', case when wtbe.source_charge_rate is null then null else round(wtbe.source_charge_rate, 6) end,
          'source_charge_ex_vat', round(coalesce(wtbe.source_charge_ex_vat,0), 2),
          'source_pay_ex_vat', round(coalesce(wtbe.component_amount_ex_vat,0), 2)
        )
      ) as source_basis_json,
      public.pay_finance_component_fingerprint(
        ('timesheet:' || wtbe.timesheet_id::text),
        wtbe.component_key_type,
        wtbe.component_key_value,
        'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum,
        upper(coalesce(wtbe.ts_pay_method, '')),
        upper(coalesce(wtbe.cand_pay_method, '')),
        jsonb_strip_nulls(
          jsonb_build_object(
            'timesheet_id', wtbe.timesheet_id::text,
            'segment_id', wtbe.segment_id,
            'segment_key', wtbe.segment_key,
            'segment_stable_key', wtbe.segment_stable_key,
            'work_date', wtbe.work_date,
            'ref_num', wtbe.ref_num,
            'bucket_code', wtbe.bucket_code
          )
        ),
        round(coalesce(wtbe.component_amount_ex_vat,0), 2),
        v_erni_pct,
        jsonb_build_object('candidate_pay_method', upper(coalesce(wtbe.cand_pay_method, '')))
      ) as component_fingerprint
    from worked_time_bucket_effective wtbe
    where round(coalesce(wtbe.component_amount_ex_vat,0), 2) <> 0
  ),
  worked_time_key_totals as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.client_id,
      d.ts_week_ending_date,
      d.ts_client_name,
      d.ts_pay_method,
      d.cand_pay_method,
      d.cand_tms_ref,
      d.cand_display_name,
      d.cand_umbrella_id,
      d.umb_enabled,
      d.umb_vat_chargeable,
      d.cand_bank_hash,
      d.umb_bank_hash,
      case
        when nullif(btrim(coalesce(seg->>'work_date','')), '') is not null then 'TS_DAY'::text
        else 'TS_TOTAL'::text
      end as component_key_type,
      case
        when nullif(btrim(coalesce(seg->>'work_date','')), '') is not null
          then nullif(btrim(coalesce(seg->>'work_date','')), '')
        else 'TOTAL'
      end as component_key_value,
      round(sum(coalesce(nullif(seg->>'delta_pay_ex_vat','')::numeric, 0)), 2) as total_pay_ex_vat,
      round(sum(coalesce(nullif(seg->>'delta_charge_ex_vat','')::numeric, 0)), 2) as total_charge_ex_vat
    from ts_itemised d
    cross join lateral jsonb_array_elements(coalesce(d.segment_deltas_json, '[]'::jsonb)) seg
    group by
      d.candidate_id,
      d.timesheet_id,
      d.client_id,
      d.ts_week_ending_date,
      d.ts_client_name,
      d.ts_pay_method,
      d.cand_pay_method,
      d.cand_tms_ref,
      d.cand_display_name,
      d.cand_umbrella_id,
      d.umb_enabled,
      d.umb_vat_chargeable,
      d.cand_bank_hash,
      d.umb_bank_hash,
      case
        when nullif(btrim(coalesce(seg->>'work_date','')), '') is not null then 'TS_DAY'::text
        else 'TS_TOTAL'::text
      end,
      case
        when nullif(btrim(coalesce(seg->>'work_date','')), '') is not null
          then nullif(btrim(coalesce(seg->>'work_date','')), '')
        else 'TOTAL'
      end
    having round(sum(coalesce(nullif(seg->>'delta_pay_ex_vat','')::numeric, 0)), 2) <> 0
  ),
  worked_time_bucket_component_sums as (
    select
      wtcr.candidate_id,
      wtcr.timesheet_id,
      wtcr.component_key_type,
      wtcr.component_key_value,
      round(sum(coalesce(wtcr.component_amount_ex_vat,0)), 2) as total_bucket_pay_ex_vat,
      round(sum(coalesce(wtcr.source_charge_ex_vat,0)), 2) as total_bucket_charge_ex_vat
    from worked_time_bucket_component_rows wtcr
    group by
      wtcr.candidate_id,
      wtcr.timesheet_id,
      wtcr.component_key_type,
      wtcr.component_key_value
  ),
  worked_time_amount_fallback_rows as (
    select
      wkt.candidate_id,
      wkt.timesheet_id,
      wkt.client_id,
      wkt.ts_week_ending_date,
      wkt.ts_client_name,
      wkt.ts_pay_method,
      wkt.cand_pay_method,
      wkt.cand_tms_ref,
      wkt.cand_display_name,
      wkt.cand_umbrella_id,
      wkt.umb_enabled,
      wkt.umb_vat_chargeable,
      wkt.cand_bank_hash,
      wkt.umb_bank_hash,
      ('timesheet:' || wkt.timesheet_id::text) as source_family_key,
      wkt.component_key_type,
      wkt.component_key_value,
      'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum as classification,
      round(coalesce(wkt.total_pay_ex_vat,0) - coalesce(wbcs.total_bucket_pay_ex_vat,0), 2) as component_amount_ex_vat,
      round(coalesce(wkt.total_charge_ex_vat,0) - coalesce(wbcs.total_bucket_charge_ex_vat,0), 2) as source_charge_ex_vat,
      null::numeric as source_units,
      null::numeric as source_rate,
      null::numeric as source_charge_rate,
      upper(coalesce(wkt.ts_pay_method, '')) as source_pay_method,
      upper(coalesce(wkt.cand_pay_method, '')) as current_target_pay_method,
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', wkt.timesheet_id::text,
          'work_date', case when wkt.component_key_type = 'TS_DAY' then wkt.component_key_value else null end,
          'component_fallback', 'WORKED_TIME_AMOUNT',
          'source_charge_ex_vat', round(coalesce(wkt.total_charge_ex_vat,0) - coalesce(wbcs.total_bucket_charge_ex_vat,0), 2),
          'source_pay_ex_vat', round(coalesce(wkt.total_pay_ex_vat,0) - coalesce(wbcs.total_bucket_pay_ex_vat,0), 2)
        )
      ) as source_basis_json,
      public.pay_finance_component_fingerprint(
        ('timesheet:' || wkt.timesheet_id::text),
        wkt.component_key_type,
        wkt.component_key_value,
        'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum,
        upper(coalesce(wkt.ts_pay_method, '')),
        upper(coalesce(wkt.cand_pay_method, '')),
        jsonb_strip_nulls(
          jsonb_build_object(
            'timesheet_id', wkt.timesheet_id::text,
            'work_date', case when wkt.component_key_type = 'TS_DAY' then wkt.component_key_value else null end,
            'component_fallback', 'WORKED_TIME_AMOUNT'
          )
        ),
        round(coalesce(wkt.total_pay_ex_vat,0) - coalesce(wbcs.total_bucket_pay_ex_vat,0), 2),
        v_erni_pct,
        jsonb_build_object('candidate_pay_method', upper(coalesce(wkt.cand_pay_method, '')))
      ) as component_fingerprint
    from worked_time_key_totals wkt
    left join worked_time_bucket_component_sums wbcs
      on wbcs.candidate_id = wkt.candidate_id
     and wbcs.timesheet_id = wkt.timesheet_id
     and wbcs.component_key_type = wkt.component_key_type
     and wbcs.component_key_value = wkt.component_key_value
    where round(coalesce(wkt.total_pay_ex_vat,0) - coalesce(wbcs.total_bucket_pay_ex_vat,0), 2) <> 0
  ),
  timesheet_component_rows as (
    select
      wt.candidate_id,
      wt.timesheet_id,
      wt.client_id,
      wt.ts_week_ending_date,
      wt.ts_client_name,
      wt.ts_pay_method,
      wt.cand_pay_method,
      wt.cand_tms_ref,
      wt.cand_display_name,
      wt.cand_umbrella_id,
      wt.umb_enabled,
      wt.umb_vat_chargeable,
      wt.cand_bank_hash,
      wt.umb_bank_hash,
      wt.source_family_key,
      wt.component_key_type,
      wt.component_key_value,
      wt.classification,
      wt.component_amount_ex_vat,
      wt.source_charge_ex_vat,
      wt.source_units,
      wt.source_rate,
      wt.source_charge_rate,
      wt.source_pay_method,
      wt.current_target_pay_method,
      wt.source_basis_json,
      wt.component_fingerprint
    from worked_time_bucket_component_rows wt

    union all

    select
      wf.candidate_id,
      wf.timesheet_id,
      wf.client_id,
      wf.ts_week_ending_date,
      wf.ts_client_name,
      wf.ts_pay_method,
      wf.cand_pay_method,
      wf.cand_tms_ref,
      wf.cand_display_name,
      wf.cand_umbrella_id,
      wf.umb_enabled,
      wf.umb_vat_chargeable,
      wf.cand_bank_hash,
      wf.umb_bank_hash,
      wf.source_family_key,
      wf.component_key_type,
      wf.component_key_value,
      wf.classification,
      wf.component_amount_ex_vat,
      wf.source_charge_ex_vat,
      wf.source_units,
      wf.source_rate,
      wf.source_charge_rate,
      wf.source_pay_method,
      wf.current_target_pay_method,
      wf.source_basis_json,
      wf.component_fingerprint
    from worked_time_amount_fallback_rows wf
    union all

    select
      d.candidate_id,
      d.timesheet_id,
      d.client_id,
      d.ts_week_ending_date,
      d.ts_client_name,
      d.ts_pay_method,
      d.cand_pay_method,
      d.cand_tms_ref,
      d.cand_display_name,
      d.cand_umbrella_id,
      d.umb_enabled,
      d.umb_vat_chargeable,
      d.cand_bank_hash,
      d.umb_bank_hash,
      ('timesheet:' || d.timesheet_id::text) as source_family_key,
      'ADDITIONAL_CODE'::text as component_key_type,
      coalesce(nullif(btrim(coalesce(au->>'code','')), ''), 'TOTAL') as component_key_value,
      'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum as classification,
      round(coalesce(nullif(au->>'delta_pay_ex_vat','')::numeric, 0), 2) as component_amount_ex_vat,
      round(coalesce(nullif(au->>'delta_charge_ex_vat','')::numeric, 0), 2) as source_charge_ex_vat,
      nullif(au->>'source_units','')::numeric as source_units,
      nullif(au->>'source_rate','')::numeric as source_rate,
      nullif(au->>'source_charge_rate','')::numeric as source_charge_rate,
      upper(coalesce(d.ts_pay_method, '')) as source_pay_method,
      upper(coalesce(d.cand_pay_method, '')) as current_target_pay_method,
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', d.timesheet_id::text,
          'additional_code', coalesce(nullif(btrim(coalesce(au->>'code','')), ''), 'TOTAL'),
          'source_units', nullif(au->>'source_units','')::numeric,
          'source_rate', nullif(au->>'source_rate','')::numeric,
          'source_charge_rate', nullif(au->>'source_charge_rate','')::numeric,
          'source_charge_ex_vat', round(coalesce(nullif(au->>'delta_charge_ex_vat','')::numeric, 0), 2),
          'source_pay_ex_vat', round(coalesce(nullif(au->>'delta_pay_ex_vat','')::numeric, 0), 2)
        )
      ) as source_basis_json,
      public.pay_finance_component_fingerprint(
        ('timesheet:' || d.timesheet_id::text),
        'ADDITIONAL_CODE',
        coalesce(nullif(btrim(coalesce(au->>'code','')), ''), 'TOTAL'),
        'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum,
        upper(coalesce(d.ts_pay_method, '')),
        upper(coalesce(d.cand_pay_method, '')),
        jsonb_strip_nulls(
          jsonb_build_object(
            'timesheet_id', d.timesheet_id::text,
            'additional_code', coalesce(nullif(btrim(coalesce(au->>'code','')), ''), 'TOTAL')
          )
        ),
        round(coalesce(nullif(au->>'delta_pay_ex_vat','')::numeric, 0), 2),
        v_erni_pct,
        jsonb_build_object('candidate_pay_method', upper(coalesce(d.cand_pay_method, '')))
      ) as component_fingerprint
    from ts_itemised d
    cross join lateral jsonb_array_elements(
      case
        when jsonb_array_length(coalesce(d.additional_unit_deltas_json, '[]'::jsonb)) > 0 then coalesce(d.additional_unit_deltas_json, '[]'::jsonb)
        when round(coalesce(d.delta_additional_pay_ex_vat, 0), 2) <> 0 then jsonb_build_array(jsonb_build_object('code', 'TOTAL', 'delta_pay_ex_vat', d.delta_additional_pay_ex_vat))
        else '[]'::jsonb
      end
    ) au
    where round(coalesce(nullif(au->>'delta_pay_ex_vat','')::numeric, 0), 2) <> 0

    union all

    select
      d.candidate_id,
      d.timesheet_id,
      d.client_id,
      d.ts_week_ending_date,
      d.ts_client_name,
      d.ts_pay_method,
      d.cand_pay_method,
      d.cand_tms_ref,
      d.cand_display_name,
      d.cand_umbrella_id,
      d.umb_enabled,
      d.umb_vat_chargeable,
      d.cand_bank_hash,
      d.umb_bank_hash,
      ('timesheet:' || d.timesheet_id::text) as source_family_key,
      'ADJUSTMENT_CODE'::text as component_key_type,
      coalesce(nullif(btrim(coalesce(adj->>'adj_id','')), ''), 'TOTAL') as component_key_value,
      'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum as classification,
      round(coalesce(nullif(adj->>'delta_pay_ex_vat','')::numeric, 0), 2) as component_amount_ex_vat,
      null::numeric as source_charge_ex_vat,
      null::numeric as source_units,
      null::numeric as source_rate,
      null::numeric as source_charge_rate,
      upper(coalesce(d.ts_pay_method, '')) as source_pay_method,
      upper(coalesce(d.cand_pay_method, '')) as current_target_pay_method,
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', d.timesheet_id::text,
          'adjustment_id', nullif(btrim(coalesce(adj->>'adj_id','')), ''),
          'source_pay_ex_vat', round(coalesce(nullif(adj->>'delta_pay_ex_vat','')::numeric, 0), 2)
        )
      ) as source_basis_json,
      public.pay_finance_component_fingerprint(
        ('timesheet:' || d.timesheet_id::text),
        'ADJUSTMENT_CODE',
        coalesce(nullif(btrim(coalesce(adj->>'adj_id','')), ''), 'TOTAL'),
        'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum,
        upper(coalesce(d.ts_pay_method, '')),
        upper(coalesce(d.cand_pay_method, '')),
        jsonb_strip_nulls(
          jsonb_build_object(
            'timesheet_id', d.timesheet_id::text,
            'adjustment_id', nullif(btrim(coalesce(adj->>'adj_id','')), '')
          )
        ),
        round(coalesce(nullif(adj->>'delta_pay_ex_vat','')::numeric, 0), 2),
        v_erni_pct,
        jsonb_build_object('candidate_pay_method', upper(coalesce(d.cand_pay_method, '')))
      ) as component_fingerprint
    from ts_itemised d
    cross join lateral jsonb_array_elements(coalesce(d.adjustment_deltas_json, '[]'::jsonb)) adj
    where round(coalesce(nullif(adj->>'delta_pay_ex_vat','')::numeric, 0), 2) <> 0

    union all

    select
      d.candidate_id,
      d.timesheet_id,
      d.client_id,
      d.ts_week_ending_date,
      d.ts_client_name,
      d.ts_pay_method,
      d.cand_pay_method,
      d.cand_tms_ref,
      d.cand_display_name,
      d.cand_umbrella_id,
      d.umb_enabled,
      d.umb_vat_chargeable,
      d.cand_bank_hash,
      d.umb_bank_hash,
      ('timesheet:' || d.timesheet_id::text) as source_family_key,
      'EXPENSE_CODE'::text as component_key_type,
      x.component_key_value,
      'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum as classification,
      x.component_amount_ex_vat,
      case x.component_key_value
        when 'EXPENSES' then round(coalesce(d.delta_expenses_charge_ex_vat,0),2)
        when 'TRAVEL' then round(coalesce(d.delta_travel_charge_ex_vat,0),2)
        when 'ACCOMMODATION' then round(coalesce(d.delta_accommodation_charge_ex_vat,0),2)
        when 'OTHER' then round(coalesce(d.delta_other_charge_ex_vat,0),2)
        when 'MILEAGE' then round(coalesce(d.delta_mileage_charge_ex_vat,0),2)
        else null::numeric
      end as source_charge_ex_vat,
      null::numeric as source_units,
      null::numeric as source_rate,
      null::numeric as source_charge_rate,
      upper(coalesce(d.ts_pay_method, '')) as source_pay_method,
      upper(coalesce(d.cand_pay_method, '')) as current_target_pay_method,
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', d.timesheet_id::text,
          'expense_code', x.component_key_value,
          'source_charge_ex_vat', case x.component_key_value
            when 'EXPENSES' then round(coalesce(d.delta_expenses_charge_ex_vat,0),2)
            when 'TRAVEL' then round(coalesce(d.delta_travel_charge_ex_vat,0),2)
            when 'ACCOMMODATION' then round(coalesce(d.delta_accommodation_charge_ex_vat,0),2)
            when 'OTHER' then round(coalesce(d.delta_other_charge_ex_vat,0),2)
            when 'MILEAGE' then round(coalesce(d.delta_mileage_charge_ex_vat,0),2)
            else null::numeric
          end,
          'source_pay_ex_vat', x.component_amount_ex_vat
        )
      ) as source_basis_json,
      public.pay_finance_component_fingerprint(
        ('timesheet:' || d.timesheet_id::text),
        'EXPENSE_CODE',
        x.component_key_value,
        'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
        upper(coalesce(d.ts_pay_method, '')),
        upper(coalesce(d.cand_pay_method, '')),
        jsonb_strip_nulls(
          jsonb_build_object(
            'timesheet_id', d.timesheet_id::text,
            'expense_code', x.component_key_value
          )
        ),
        x.component_amount_ex_vat,
        NULL::numeric,
        jsonb_build_object('candidate_pay_method', upper(coalesce(d.cand_pay_method, '')))
      ) as component_fingerprint
    from ts_itemised d
    cross join lateral (
      values
        ('EXPENSES', round(coalesce(d.delta_expenses_pay_ex_vat, 0), 2)),
        ('TRAVEL', round(coalesce(d.delta_travel_pay_ex_vat, 0), 2)),
        ('ACCOMMODATION', round(coalesce(d.delta_accommodation_pay_ex_vat, 0), 2)),
        ('OTHER', round(coalesce(d.delta_other_pay_ex_vat, 0), 2)),
        ('MILEAGE', round(coalesce(d.delta_mileage_pay_ex_vat, 0), 2))
    ) as x(component_key_value, component_amount_ex_vat)
    where x.component_amount_ex_vat <> 0
  ),
  finance_case_baseline_scope as (
    select
      vfcr.*
    from public.v_finance_cases_register vfcr
    where vfcr.case_type in (
      'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
      'OVERPAYMENT'::public.pay_finance_case_type_enum,
      'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
      'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
    )
      and upper(coalesce(vfcr.status::text,'')) = 'ACTIVE'
      and coalesce(vfcr.outstanding_amount,0) > 0
      and not (vfcr.active_snooze_id is not null and vfcr.active_snooze_until_date is null)
      and (
        (
          vfcr.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
          and (
            upper(coalesce(vfcr.payout_status::text,'')) <> 'PAID'
            or vfcr.next_due_week_start is null
            or vfcr.next_due_week_start <= v_week_start
          )
        )
        or (
          vfcr.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
          and (
            vfcr.next_due_week_start is null
            or vfcr.next_due_week_start <= v_week_start
          )
        )
        or (
          vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
          and (
            vfcr.next_due_week_start is null
            or vfcr.next_due_week_start <= v_week_start
          )
        )
        or vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
      and (v_candidate_id is null or vfcr.candidate_id = v_candidate_id)
      and (v_client_id is null or vfcr.client_id = v_client_id)
  ),
  timesheet_component_match_rows as (
    select
      tcr.candidate_id,
      tcr.timesheet_id,
      tcr.client_id,
      tcr.ts_week_ending_date,
      tcr.ts_client_name,
      tcr.ts_pay_method,
      tcr.cand_pay_method,
      tcr.cand_tms_ref,
      tcr.cand_display_name,
      tcr.cand_umbrella_id,
      tcr.umb_enabled,
      tcr.umb_vat_chargeable,
      tcr.cand_bank_hash,
      tcr.umb_bank_hash,
      tcr.source_family_key,
      tcr.component_key_type,
      tcr.component_key_value,
      tcr.classification,
      tcr.component_amount_ex_vat,
      tcr.source_charge_ex_vat,
      tcr.source_units,
      tcr.source_rate,
      tcr.source_charge_rate,
      tcr.source_pay_method,
      tcr.current_target_pay_method,
      tcr.source_basis_json,
      tcr.component_fingerprint,
      mdc.finance_case_id as matched_finance_case_id,
      mdc.finance_component_id as matched_finance_component_id,
      mdc.saved_target_pay_method as matched_saved_target_pay_method,
      mdc.saved_resolution_mode as matched_saved_resolution_mode,
      mdc.saved_resolution_payload_json as matched_saved_resolution_payload_json,
      mdc.saved_resolution_result_json as matched_saved_resolution_result_json,
      mdc.resolution_fingerprint as matched_resolution_fingerprint,
      mdc.is_resolution_stale as matched_is_resolution_stale,
      mdc.stale_reason as matched_stale_reason
    from timesheet_component_rows tcr
    left join lateral (
      select
        pfc.finance_case_id,
        pfc.id as finance_component_id,
        pfc.saved_target_pay_method,
        pfc.saved_resolution_mode,
        pfc.saved_resolution_payload_json,
        pfc.saved_resolution_result_json,
        pfc.resolution_fingerprint,
        pfc.is_resolution_stale,
        pfc.stale_reason
      from public.pay_finance_case_components pfc
      join finance_case_baseline_scope vfcr_m
        on vfcr_m.finance_case_id = pfc.finance_case_id
       and vfcr_m.candidate_id = tcr.candidate_id
       and vfcr_m.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
      where pfc.closed_at_utc is null
        and coalesce(pfc.remaining_source_amount, 0) > 0
        and pfc.candidate_id = tcr.candidate_id
        and pfc.linked_timesheet_id = tcr.timesheet_id
        and pfc.source_family_key = tcr.source_family_key
        and pfc.component_key_type = tcr.component_key_type
        and pfc.component_key_value = tcr.component_key_value
        and pfc.classification = tcr.classification
        and coalesce(pfc.source_basis_json, '{}'::jsonb) = coalesce(tcr.source_basis_json, '{}'::jsonb)
      order by pfc.updated_at_utc desc, pfc.created_at_utc desc, pfc.id desc
      limit 1
    ) mdc on true
  ),
  transient_timesheet_component_rows as (
    select
      tmr.candidate_id,
      tmr.timesheet_id,
      tmr.client_id,
      tmr.ts_week_ending_date,
      tmr.ts_client_name,
      tmr.ts_pay_method,
      tmr.cand_pay_method,
      tmr.cand_tms_ref,
      tmr.cand_display_name,
      tmr.cand_umbrella_id,
      tmr.umb_enabled,
      tmr.umb_vat_chargeable,
      tmr.cand_bank_hash,
      tmr.umb_bank_hash,
      tmr.source_family_key,
      tmr.component_key_type,
      tmr.component_key_value,
      tmr.classification,
      tmr.component_amount_ex_vat,
      tmr.source_charge_ex_vat,
      tmr.source_units,
      tmr.source_rate,
      tmr.source_charge_rate,
      tmr.source_pay_method,
      tmr.current_target_pay_method,
      tmr.source_basis_json,
      tmr.component_fingerprint
    from timesheet_component_match_rows tmr
    where tmr.matched_finance_component_id is null
  ),
  transient_timesheet_component_review_rows as (
    select
      ttr.candidate_id,
      ttr.timesheet_id,
      ttr.client_id,
      ttr.ts_week_ending_date,
      ttr.ts_client_name,
      ttr.ts_pay_method,
      ttr.cand_pay_method,
      ttr.cand_tms_ref,
      ttr.cand_display_name,
      ttr.cand_umbrella_id,
      ttr.umb_enabled,
      ttr.umb_vat_chargeable,
      ttr.cand_bank_hash,
      ttr.umb_bank_hash,
      ttr.source_family_key,
      ttr.component_key_type,
      ttr.component_key_value,
      ttr.classification,
      ttr.component_amount_ex_vat,
      ttr.source_charge_ex_vat,
      ttr.source_units,
      ttr.source_rate,
      ttr.source_charge_rate,
      ttr.source_pay_method,
      ttr.current_target_pay_method,
      ttr.source_basis_json,
      ttr.component_fingerprint,
      (
        ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and ttr.source_pay_method in ('PAYE','UMBRELLA')
        and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
        and ttr.current_target_pay_method <> ''
        and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
      ) as has_suggested_resolution,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'NO_SUGGESTION_AVAILABLE'
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then 'NO_SUGGESTION_AVAILABLE'
        else 'FRESH_SUGGESTION'
      end as suggestion_provenance,
      (
        ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and ttr.source_pay_method in ('PAYE','UMBRELLA')
        and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
        and ttr.current_target_pay_method <> ''
        and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
      ) as is_fresh_suggested_resolution,
      false as is_reusable_saved_resolution,
      false as is_stale_saved_resolution,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
         and ttr.source_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method <> ''
         and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
        then jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS',
            'target_pay_method', ttr.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(ttr.umb_vat_chargeable,false),
            'target_units', case when ttr.source_units is not null then round(ttr.source_units,6) else null end,
            'suggested_target_rate', case when ttr.source_units is not null and ttr.source_units <> 0 then round(round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2) / ttr.source_units, 2) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
          )
        )
        else null::jsonb
      end as suggested_resolution_payload_json,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
         and ttr.source_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method <> ''
         and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
        then jsonb_strip_nulls(
          jsonb_build_object(
            'target_pay_method', ttr.current_target_pay_method,
            'target_amount_ex_vat', round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),
            'target_amount_vat', round(coalesce((ttrs.target_amounts_json->>'vat')::numeric,0),2),
            'target_amount_inc_vat', round(coalesce((ttrs.target_amounts_json->>'inc')::numeric,0),2),
            'basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'applied_basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(ttr.umb_vat_chargeable,false),
            'target_units', case when ttr.source_units is not null then round(ttr.source_units,6) else null end,
            'replacement_rate', case when ttr.source_units is not null and ttr.source_units <> 0 then round(round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2) / ttr.source_units, 2) else null end,
            'target_amount_ex_vat_per_source_ex_vat', case when coalesce(ttr.component_amount_ex_vat,0) <> 0 then round(round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2) / ttr.component_amount_ex_vat, 10) else null end,
            'target_amount_vat_per_source_ex_vat', case when coalesce(ttr.component_amount_ex_vat,0) <> 0 then round(round(coalesce((ttrs.target_amounts_json->>'vat')::numeric,0),2) / ttr.component_amount_ex_vat, 10) else null end,
            'target_amount_inc_vat_per_source_ex_vat', case when coalesce(ttr.component_amount_ex_vat,0) <> 0 then round(round(coalesce((ttrs.target_amounts_json->>'inc')::numeric,0),2) / ttr.component_amount_ex_vat, 10) else null end,
            'target_units_per_source_ex_vat', case when ttr.source_units is not null and coalesce(ttr.component_amount_ex_vat,0) <> 0 then round(ttr.source_units / ttr.component_amount_ex_vat, 10) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'source_pay_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'source_charge_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end,
            'source_margin_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat - ttr.component_amount_ex_vat,2) end,
            'target_pay_ex_vat', round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),
            'target_charge_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end,
            'target_margin_ex_vat', case when ttr.source_charge_ex_vat is null or ttrs.target_amounts_json is null then null else round(ttr.source_charge_ex_vat - round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),2) end,
            'margin_delta_ex_vat', case when ttr.source_charge_ex_vat is null or ttrs.target_amounts_json is null then null else round(ttr.component_amount_ex_vat - round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),2) end
          )
        )
        else null::jsonb
      end as suggested_resolution_result_json,
      round(coalesce(ttr.component_amount_ex_vat,0),2) as source_pay_ex_vat,
      case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end as source_charge_component_ex_vat,
      case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat - ttr.component_amount_ex_vat,2) end as source_margin_ex_vat,
      case when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,'')) and ttrs.target_amounts_json is not null then round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2) else null end as target_pay_ex_vat,
      case when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,'')) and ttr.source_charge_ex_vat is not null then round(ttr.source_charge_ex_vat,2) else null end as target_charge_ex_vat,
      case when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,'')) and ttr.source_charge_ex_vat is not null and ttrs.target_amounts_json is not null then round(ttr.source_charge_ex_vat - round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),2) else null end as target_margin_ex_vat,
      case when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,'')) and ttr.source_charge_ex_vat is not null and ttrs.target_amounts_json is not null then round(ttr.component_amount_ex_vat - round(coalesce((ttrs.target_amounts_json->>'ex')::numeric,0),2),2) else null end as margin_delta_ex_vat,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'Fixed reimbursements are not channel-converted and do not participate in suggested-rates review.'
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then 'No suggested rates are required because this taxable component already aligns with the current target pay method.'
        else 'This suggestion converts the taxable component to a target-side equivalent while leaving fixed reimbursements unchanged.'
      end as suggestion_explanation_text
    from transient_timesheet_component_rows ttr
    left join lateral (
      select
        case
          when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
          when ttr.source_pay_method = 'PAYE' and ttr.current_target_pay_method = 'UMBRELLA' then public._pay_convert_paye_to_umbrella(ttr.component_amount_ex_vat, v_erni_pct, v_vat_rate_pct, coalesce(ttr.umb_vat_chargeable,false))
          when ttr.source_pay_method = 'UMBRELLA' and ttr.current_target_pay_method = 'PAYE' then jsonb_build_object('ex', public._pay_convert_umbrella_to_paye_ex(ttr.component_amount_ex_vat, v_erni_pct), 'vat', 0, 'inc', public._pay_convert_umbrella_to_paye_ex(ttr.component_amount_ex_vat, v_erni_pct))
          when ttr.current_target_pay_method = 'PAYE' then jsonb_build_object('ex', round(coalesce(ttr.component_amount_ex_vat,0),2), 'vat', 0, 'inc', round(coalesce(ttr.component_amount_ex_vat,0),2))
          when ttr.current_target_pay_method = 'UMBRELLA' then public._pay_umbrella_vat_calc(ttr.component_amount_ex_vat, v_vat_rate_pct, coalesce(ttr.umb_vat_chargeable,false))
          else null::jsonb
        end as target_amounts_json
    ) ttrs on true
  ),
  timesheet_case_actionable_basis as (
    select distinct
      ttrr.timesheet_id,
      case
        when ttrr.component_key_type in ('TS_DAY','TS_TOTAL')
          then ('SEGMENT_BUCKET:' || coalesce(nullif(btrim(coalesce(ttrr.source_basis_json->>'bucket_code','')), ''), 'UNSPECIFIED'))
        when ttrr.component_key_type = 'ADDITIONAL_CODE' then ('ADDITIONAL_CODE:' || coalesce(ttrr.component_key_value,''))
        else (coalesce(ttrr.component_key_type,'') || ':' || coalesce(ttrr.component_key_value,''))
      end as basis_family_key,
      upper(coalesce(ttrr.source_pay_method,'')) as source_pay_method,
      round(coalesce(ttrr.source_rate,0),6) as source_rate,
      round(coalesce(ttrr.source_charge_rate,0),6) as source_charge_rate
    from transient_timesheet_component_review_rows ttrr
    where ttrr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      and upper(coalesce(ttrr.source_pay_method,'')) is distinct from upper(coalesce(ttrr.current_target_pay_method,''))
      and ttrr.source_units is not null
      and coalesce(ttrr.source_units,0) <> 0
      and ttrr.source_rate is not null
      and ttrr.source_charge_rate is not null
      and ttrr.component_key_type <> 'ADJUSTMENT_CODE'
  ),
  timesheet_live_scope as (
    select
      tb.candidate_id,
      tb.timesheet_id,
      tb.client_id,
      public.timesheets.contract_id,
      case
        when public.timesheets.contract_id is not null then 'CONTRACT'
        else 'DAILY_SAME_BASIS'
      end as linked_scope_type,
      md5(
        coalesce(
          (
            select jsonb_agg(
              jsonb_build_object(
                'basis_family_key', tcab.basis_family_key,
                'source_pay_method', tcab.source_pay_method,
                'source_rate', tcab.source_rate,
                'source_charge_rate', tcab.source_charge_rate
              )
              order by tcab.basis_family_key, tcab.source_pay_method, tcab.source_rate, tcab.source_charge_rate
            )::text
            from timesheet_case_actionable_basis tcab
            where tcab.timesheet_id = tb.timesheet_id
          ),
          '[]'
        )
      ) as actionable_basis_signature
    from ts_baseline tb
    join public.timesheets
      on public.timesheets.timesheet_id = tb.timesheet_id
  ),
  timesheet_linked_scope_counts as (
    select
      seed_scope.timesheet_id as seed_timesheet_id,
      count(*)::int as linked_timesheet_count,
      coalesce(jsonb_agg(target_scope.timesheet_id::text order by target_scope.timesheet_id::text), '[]'::jsonb) as linked_timesheet_ids_json
    from timesheet_live_scope seed_scope
    join timesheet_live_scope target_scope
      on target_scope.candidate_id = seed_scope.candidate_id
     and (
       (
         seed_scope.contract_id is not null
         and target_scope.contract_id is not distinct from seed_scope.contract_id
       )
       or (
         seed_scope.contract_id is null
         and target_scope.contract_id is null
         and target_scope.client_id is not distinct from seed_scope.client_id
         and target_scope.actionable_basis_signature is not distinct from seed_scope.actionable_basis_signature
       )
     )
    group by seed_scope.timesheet_id
  ),
  transient_timesheet_component_review_rows_effective as (
    select
      ttr.candidate_id,
      ttr.timesheet_id,
      ttr.client_id,
      ttr.ts_week_ending_date,
      ttr.ts_client_name,
      ttr.ts_pay_method,
      ttr.cand_pay_method,
      ttr.cand_tms_ref,
      ttr.cand_display_name,
      ttr.cand_umbrella_id,
      ttr.umb_enabled,
      ttr.umb_vat_chargeable,
      ttr.cand_bank_hash,
      ttr.umb_bank_hash,
      ttr.source_family_key,
      ttr.component_key_type,
      ttr.component_key_value,
      ttr.classification,
      round(coalesce(ttr.component_amount_ex_vat,0),2) as component_amount_ex_vat,
      round(coalesce(ttr.source_charge_ex_vat,0),2) as source_charge_ex_vat,
      ttr.source_units,
      ttr.source_rate,
      ttr.source_charge_rate,
      ttr.source_pay_method,
      ttr.current_target_pay_method,
      ttr.source_basis_json,
      ttr.component_fingerprint,
      ttrx.is_actionable_bucket_resolution as is_actionable_resolution_row,
      ttrx.is_fixed_taxable_conversion as is_fixed_no_action_taxable_row,
      null::text as approved_resolution_mode,
      null::numeric as approved_target_rate,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then false
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then false
        when ttrx.is_actionable_bucket_resolution = true then true
        else false
      end as requires_resolution,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then true
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then true
        when ttrx.is_actionable_bucket_resolution = true then false
        else true
      end as case_resolution_satisfied_now_component,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
         and ttr.source_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
         and ttr.current_target_pay_method <> ''
         and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
        then true
        else false
      end as has_suggested_resolution,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'NO_SUGGESTION_AVAILABLE'
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then 'NO_SUGGESTION_AVAILABLE'
        when ttrx.is_actionable_bucket_resolution = true then 'FRESH_SUGGESTION'
        else 'NO_ACTION_FIXED_CONVERSION'
      end as suggestion_provenance,
      (ttrx.is_actionable_bucket_resolution = true) as is_fresh_suggested_resolution,
      false as is_reusable_saved_resolution,
      false as is_stale_saved_resolution,
      case
        when ttrx.is_actionable_bucket_resolution = true then jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS',
            'target_pay_method', ttr.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(ttr.umb_vat_chargeable,false),
            'target_units', case when ttr.source_units is not null then round(ttr.source_units,6) else null end,
            'suggested_target_rate', case when tts.suggested_target_rate is null then null else round(tts.suggested_target_rate,2) end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
          )
        )
        else null::jsonb
      end as suggested_resolution_payload_json,
      case
        when ttrx.is_actionable_bucket_resolution = true then jsonb_strip_nulls(
          jsonb_build_object(
            'target_pay_method', ttr.current_target_pay_method,
            'target_amount_ex_vat', round(coalesce(nullif(ttam.suggested_target_amounts_json->>'ex','')::numeric, tts.suggested_target_pay_ex_vat, 0),2),
            'target_amount_vat', round(coalesce(nullif(ttam.suggested_target_amounts_json->>'vat','')::numeric, 0),2),
            'target_amount_inc_vat', round(coalesce(nullif(ttam.suggested_target_amounts_json->>'inc','')::numeric, coalesce(tts.suggested_target_pay_ex_vat,0), 0),2),
            'basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'applied_basis_source_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(ttr.umb_vat_chargeable,false),
            'target_units', case when ttr.source_units is not null then round(ttr.source_units,6) else null end,
            'replacement_rate', case when tts.suggested_target_rate is null then null else round(tts.suggested_target_rate,2) end,
            'target_amount_ex_vat_per_source_ex_vat', case when round(coalesce(ttr.component_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(ttam.suggested_target_amounts_json->>'ex','')::numeric, tts.suggested_target_pay_ex_vat, 0),2) / round(coalesce(ttr.component_amount_ex_vat,0),2), 10) else null end,
            'target_amount_vat_per_source_ex_vat', case when round(coalesce(ttr.component_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(ttam.suggested_target_amounts_json->>'vat','')::numeric,0),2) / round(coalesce(ttr.component_amount_ex_vat,0),2), 10) else null end,
            'target_amount_inc_vat_per_source_ex_vat', case when round(coalesce(ttr.component_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(ttam.suggested_target_amounts_json->>'inc','')::numeric, coalesce(tts.suggested_target_pay_ex_vat,0), 0),2) / round(coalesce(ttr.component_amount_ex_vat,0),2), 10) else null end,
            'target_units_per_source_ex_vat', case when ttr.source_units is not null and round(coalesce(ttr.component_amount_ex_vat,0),2) <> 0 then round(ttr.source_units / round(coalesce(ttr.component_amount_ex_vat,0),2), 10) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'source_pay_ex_vat', round(coalesce(ttr.component_amount_ex_vat,0),2),
            'source_charge_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end,
            'source_margin_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat - ttr.component_amount_ex_vat,2) end,
            'target_pay_ex_vat', round(coalesce(nullif(ttam.suggested_target_amounts_json->>'ex','')::numeric, tts.suggested_target_pay_ex_vat, 0),2),
            'target_charge_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end,
            'target_margin_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat - round(coalesce(nullif(ttam.suggested_target_amounts_json->>'ex','')::numeric, tts.suggested_target_pay_ex_vat, 0),2),2) end,
            'margin_delta_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.component_amount_ex_vat - round(coalesce(nullif(ttam.suggested_target_amounts_json->>'ex','')::numeric, tts.suggested_target_pay_ex_vat, 0),2),2) end
          )
        )
        else null::jsonb
      end as suggested_resolution_result_json,
      round(coalesce(ttr.component_amount_ex_vat,0),2) as source_pay_ex_vat,
      case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat,2) end as source_charge_component_ex_vat,
      case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat - ttr.component_amount_ex_vat,2) end as source_margin_ex_vat,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(ttr.component_amount_ex_vat,0),2)
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
        else round(coalesce(tts.suggested_target_pay_ex_vat, ttr.component_amount_ex_vat, 0),2)
      end as target_pay_ex_vat,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and ttr.source_charge_ex_vat is not null then round(ttr.source_charge_ex_vat,2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and ttr.source_charge_ex_vat is not null
          then round(
            ttr.source_charge_ex_vat
            - case
                when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
                else round(coalesce(tts.suggested_target_pay_ex_vat, ttr.component_amount_ex_vat, 0),2)
              end,
            2
          )
        else null::numeric
      end as target_margin_ex_vat,
      case
        when ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and ttr.source_charge_ex_vat is not null
          then round(
            (
              ttr.source_charge_ex_vat
              - case
                  when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
                  else round(coalesce(tts.suggested_target_pay_ex_vat, ttr.component_amount_ex_vat, 0),2)
                end
            ) - (ttr.source_charge_ex_vat - ttr.component_amount_ex_vat),
            2
          )
        else null::numeric
      end as margin_delta_ex_vat,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'Fixed reimbursements are not channel-converted and do not participate in suggested-rates review.'
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then 'No suggested rates are required because this taxable component already aligns with the current target pay method.'
        when ttrx.is_fixed_taxable_conversion = true then 'This taxable row does not expose a per-unit rate edit. It remains visible as a fixed no-action row and is converted deterministically onto the current target pay method.'
        else 'This suggestion converts the taxable component to a target-side equivalent while keeping units fixed, charge fixed, and margin constant except for unavoidable penny balancing.'
      end as suggestion_explanation_text,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(ttr.component_amount_ex_vat,0),2)
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
        when ttrx.is_actionable_bucket_resolution = true then round(coalesce(ttr.component_amount_ex_vat,0),2)
        else round(coalesce(tts.suggested_target_pay_ex_vat, ttr.component_amount_ex_vat, 0),2)
      end as preview_component_amount_ex_vat,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(ttr.component_amount_ex_vat,0),2)
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
        when ttrx.is_actionable_bucket_resolution = true then 0::numeric
        else round(coalesce(tts.suggested_target_pay_ex_vat, ttr.component_amount_ex_vat, 0),2)
      end as ready_preview_amount_ex_vat,
      case
        when ttrx.is_actionable_bucket_resolution = true then round(coalesce(ttr.component_amount_ex_vat,0),2)
        else 0::numeric
      end as blocked_preview_amount_ex_vat,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'FIXED'
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then 'NOT_REQUIRED'
        when ttrx.is_actionable_bucket_resolution = true then 'REQUIRED'
        else 'FIXED'
      end as resolution_state,
      case
        when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::numeric
        when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then null::numeric
        when ttrx.is_actionable_bucket_resolution = true then round(tts.suggested_target_rate,2)
        else null::numeric
      end as target_rate
    from transient_timesheet_component_review_rows ttr
    left join lateral (
      select
        (
          ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          and ttr.source_pay_method in ('PAYE','UMBRELLA')
          and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
          and ttr.current_target_pay_method <> ''
          and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
          and round(coalesce(ttr.component_amount_ex_vat,0),2) > 0
          and ttr.source_units is not null
          and coalesce(ttr.source_units,0) <> 0
          and ttr.source_rate is not null
          and ttr.source_charge_rate is not null
          and ttr.component_key_type <> 'ADJUSTMENT_CODE'
        ) as is_actionable_bucket_resolution,
        (
          ttr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          and ttr.source_pay_method in ('PAYE','UMBRELLA')
          and ttr.current_target_pay_method in ('PAYE','UMBRELLA')
          and ttr.current_target_pay_method <> ''
          and upper(coalesce(ttr.source_pay_method,'')) is distinct from upper(coalesce(ttr.current_target_pay_method,''))
          and not (
            ttr.source_units is not null
            and coalesce(ttr.source_units,0) <> 0
            and ttr.source_rate is not null
            and ttr.source_charge_rate is not null
            and ttr.component_key_type <> 'ADJUSTMENT_CODE'
          )
        ) as is_fixed_taxable_conversion
    ) ttrx on true
    left join lateral (
      select
        case
          when ttr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(ttr.component_amount_ex_vat,0),2)
          when upper(coalesce(ttr.source_pay_method,'')) is not distinct from upper(coalesce(ttr.current_target_pay_method,'')) then round(coalesce(ttr.component_amount_ex_vat,0),2)
          when ttr.source_pay_method = 'PAYE' and ttr.current_target_pay_method = 'UMBRELLA' then round((public._pay_convert_paye_to_umbrella(round(coalesce(ttr.component_amount_ex_vat,0),2), v_erni_pct, v_vat_rate_pct, coalesce(ttr.umb_vat_chargeable,false))->>'ex')::numeric,2)
          when ttr.source_pay_method = 'UMBRELLA' and ttr.current_target_pay_method = 'PAYE' then round(public._pay_convert_umbrella_to_paye_ex(round(coalesce(ttr.component_amount_ex_vat,0),2), v_erni_pct),2)
          when ttr.current_target_pay_method = 'UMBRELLA' then round((public._pay_umbrella_vat_calc(round(coalesce(ttr.component_amount_ex_vat,0),2), v_vat_rate_pct, coalesce(ttr.umb_vat_chargeable,false))->>'ex')::numeric,2)
          else round(coalesce(ttr.component_amount_ex_vat,0),2)
        end as target_ex_before_rate
    ) ttb on true
    left join lateral (
      select
        case
          when ttrx.is_actionable_bucket_resolution = true and ttr.source_units is not null and ttr.source_units <> 0
            then round(ttb.target_ex_before_rate / ttr.source_units, 2)
          else null::numeric
        end as suggested_target_rate,
        case
          when ttrx.is_actionable_bucket_resolution = true and ttr.source_units is not null and ttr.source_units <> 0
            then round(round(ttb.target_ex_before_rate / ttr.source_units, 2) * ttr.source_units, 2)
          else round(ttb.target_ex_before_rate, 2)
        end as suggested_target_pay_ex_vat
    ) tts on true
    left join lateral (
      select
        case
          when tts.suggested_target_pay_ex_vat is null then null::jsonb
          when upper(coalesce(ttr.current_target_pay_method,'')) = 'UMBRELLA' then public._pay_umbrella_vat_calc(round(coalesce(tts.suggested_target_pay_ex_vat,0),2), v_vat_rate_pct, coalesce(ttr.umb_vat_chargeable,false))
          else jsonb_build_object('ex', round(coalesce(tts.suggested_target_pay_ex_vat,0),2), 'vat', 0, 'inc', round(coalesce(tts.suggested_target_pay_ex_vat,0),2))
        end as suggested_target_amounts_json
    ) ttam on true
  ),
  timesheet_case_rollup as (
    select
      tcr.candidate_id,
      tcr.timesheet_id,
      max(tcr.client_id::text)::uuid as client_id,
      max(tcr.ts_week_ending_date) as ts_week_ending_date,
      max(tcr.ts_client_name) as ts_client_name,
      max(tcr.ts_pay_method) as ts_pay_method,
      max(tcr.cand_pay_method) as cand_pay_method,
      max(tcr.cand_tms_ref) as cand_tms_ref,
      max(tcr.cand_display_name) as cand_display_name,
      max(tcr.cand_umbrella_id::text)::uuid as cand_umbrella_id,
      bool_or(tcr.umb_enabled) as umb_enabled,
      bool_or(tcr.umb_vat_chargeable) as umb_vat_chargeable,
      bool_or(tcr.cand_bank_hash is not null and btrim(tcr.cand_bank_hash) <> '') as candidate_has_bank_details,
      max(tcr.cand_bank_hash) as candidate_bank_hash,
      bool_or(tcr.umb_bank_hash is not null and btrim(tcr.umb_bank_hash) <> '') as umbrella_has_bank_details,
      max(tcr.umb_bank_hash) as umbrella_bank_hash,
      'BUCKETED'::text as resolution_family,
      (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0) as case_needs_resolution,
      (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) = 0) as case_resolution_satisfied_now,
      'Suggested Rate'::text as resolution_action_label,
      jsonb_strip_nulls(
        jsonb_build_object(
          'resolve_all_linked_timesheets_default', true,
          'linked_scope_type', coalesce(max(tls.linked_scope_type), 'SELF_ONLY'),
          'linked_timesheet_count', coalesce(max(tslc.linked_timesheet_count), 1),
          'linked_timesheet_ids', coalesce(min(tslc.linked_timesheet_ids_json::text)::jsonb, jsonb_build_array(tcr.timesheet_id::text)),
          'contract_id', case when max(tls.contract_id::text) is null then null else max(tls.contract_id::text) end,
          'client_id', case when max(tls.client_id::text) is null then null else max(tls.client_id::text) end,
          'seed_timesheet_id', tcr.timesheet_id::text,
          'confirmation_text', (
            'This will resolve ' || coalesce(max(tslc.linked_timesheet_count), 1)::text || ' timesheets for this candidate. Are you sure you wish to continue?'
          )
        )
      ) as linked_resolution_scope_json,
      round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)), 2) as case_total_amount_ex,
      round(
        case
          when count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0 then 0::numeric
          else sum(coalesce(tcr.ready_preview_amount_ex_vat, tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0))
        end,
        2
      ) as safe_amount_ex,
      round(sum(case when coalesce(tcr.requires_resolution, false) = true then coalesce(tcr.blocked_preview_amount_ex_vat, tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0) else 0 end), 2) as unresolved_taxable_amount_ex,
      count(*) filter (where tcr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum) as open_taxable_count,
      count(*) filter (where tcr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) as open_reimbursement_count,
      count(*) filter (where coalesce(tcr.requires_resolution, false) = true) as unresolved_taxable_count,
      0::integer as stale_count,
      (count(*) filter (where tcr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum) > 0 and count(*) filter (where tcr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) > 0) as is_mixed_case,
      (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0) as is_blocked,
      round(sum(case when tcr.component_key_type in ('TS_DAY','TS_TOTAL') then coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0) else 0 end), 2) as segments_total_ex,
      max(coalesce(td.delta_additional_pay_ex_vat, 0)) as delta_additional_pay_ex_vat,
      max(coalesce(td.delta_expenses_pay_ex_vat, 0)) as delta_expenses_pay_ex_vat,
      max(coalesce(td.delta_travel_pay_ex_vat, 0)) as delta_travel_pay_ex_vat,
      max(coalesce(td.delta_accommodation_pay_ex_vat, 0)) as delta_accommodation_pay_ex_vat,
      max(coalesce(td.delta_other_pay_ex_vat, 0)) as delta_other_pay_ex_vat,
      max(coalesce(td.delta_mileage_pay_ex_vat, 0)) as delta_mileage_pay_ex_vat,
      coalesce(min(td.segment_deltas_json::text)::jsonb, '[]'::jsonb) as segment_deltas_json,
      coalesce(min(td.adjustment_deltas_json::text)::jsonb, '[]'::jsonb) as adjustment_deltas_json,
      coalesce(min(td.additional_unit_deltas_json::text)::jsonb, '[]'::jsonb) as additional_unit_deltas_json,
      bool_or(coalesce(td.reservation_overrun_detected,false)) as reservation_overrun_detected,
      round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)),2) as payment_amount_ex_vat,
      round(
        case
          when max(tcr.cand_pay_method) = 'UMBRELLA' then (public._pay_umbrella_vat_calc(round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)),2), v_vat_rate_pct, bool_or(tcr.umb_vat_chargeable))->>'inc')::numeric
          else round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)),2)
        end,
        2
      ) as payment_amount_inc_vat,
      round(
        case
          when max(tcr.cand_pay_method) = 'UMBRELLA' then (public._pay_umbrella_vat_calc(round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)),2), v_vat_rate_pct, bool_or(tcr.umb_vat_chargeable))->>'inc')::numeric
          else round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)),2)
        end,
        2
      ) as payment_amount,
      jsonb_build_object(
        'case_key', ('timesheet:' || max(tcr.timesheet_id::text)),
        'case_type', 'TIMESHEET_PAYMENT',
        'resolution_family', 'BUCKETED',
        'case_needs_resolution', (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0),
        'case_resolution_satisfied_now', (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) = 0),
        'resolution_action_label', 'Suggested Rate',
        'linked_resolution_scope_json', jsonb_strip_nulls(
          jsonb_build_object(
            'resolve_all_linked_timesheets_default', true,
            'linked_scope_type', coalesce(max(tls.linked_scope_type), 'SELF_ONLY'),
            'linked_timesheet_count', coalesce(max(tslc.linked_timesheet_count), 1),
            'linked_timesheet_ids', coalesce(min(tslc.linked_timesheet_ids_json::text)::jsonb, jsonb_build_array(tcr.timesheet_id::text)),
            'contract_id', case when max(tls.contract_id::text) is null then null else max(tls.contract_id::text) end,
            'client_id', case when max(tls.client_id::text) is null then null else max(tls.client_id::text) end,
            'seed_timesheet_id', tcr.timesheet_id::text,
            'confirmation_text', ('This will resolve ' || coalesce(max(tslc.linked_timesheet_count), 1)::text || ' timesheets for this candidate. Are you sure you wish to continue?')
          )
        ),
        'is_mixed_case', (count(*) filter (where tcr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum) > 0 and count(*) filter (where tcr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) > 0),
        'open_taxable_count', count(*) filter (where tcr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum),
        'open_reimbursement_count', count(*) filter (where tcr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum),
        'unresolved_taxable_count', count(*) filter (where coalesce(tcr.requires_resolution, false) = true),
        'stale_count', 0,
        'is_blocked', (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0),
        'safe_amount_ex_vat', case when (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0) then 0::numeric else round(sum(coalesce(tcr.ready_preview_amount_ex_vat, tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)), 2) end,
        'blocked_case_amount_ex_vat', case when (count(*) filter (where coalesce(tcr.requires_resolution, false) = true) > 0) then round(sum(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0)), 2) else 0::numeric end,
        'unresolved_taxable_amount_ex_vat', round(sum(case when coalesce(tcr.requires_resolution, false) = true then coalesce(tcr.blocked_preview_amount_ex_vat, tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0) else 0 end), 2)
      ) as case_resolution_summary_json,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'finance_component_id', null,
            'source_family_key', tcr.source_family_key,
            'component_key_type', tcr.component_key_type,
            'component_key_value', tcr.component_key_value,
            'label',
              case
                when tcr.component_key_type in ('TS_DAY','TS_TOTAL')
                  then coalesce(nullif(btrim(coalesce(tcr.source_basis_json->>'bucket_code','')), ''), tcr.component_key_value, tcr.component_key_type)
                when tcr.component_key_type = 'ADDITIONAL_CODE'
                  then coalesce(nullif(btrim(coalesce(tcr.source_basis_json->>'additional_code','')), ''), tcr.component_key_value)
                when tcr.component_key_type = 'ADJUSTMENT_CODE'
                  then coalesce(nullif(btrim(coalesce(tcr.source_basis_json->>'adjustment_id','')), ''), tcr.component_key_value)
                when tcr.component_key_type = 'EXPENSE_CODE'
                  then coalesce(nullif(btrim(coalesce(tcr.source_basis_json->>'expense_code','')), ''), tcr.component_key_value)
                else tcr.component_key_value
              end,
            'bucket_code', nullif(btrim(coalesce(tcr.source_basis_json->>'bucket_code','')), ''),
            'source_basis_fingerprint', md5(coalesce(tcr.source_basis_json::text, '{}'::text)),
            'is_rate_bearing',
              (
                tcr.source_units is not null
                and coalesce(tcr.source_units,0) <> 0
                and tcr.source_rate is not null
                and tcer.effective_source_charge_rate is not null
                and tcr.component_key_type <> 'ADJUSTMENT_CODE'
              ),
            'classification', tcr.classification::text,
            'source_pay_method', tcr.source_pay_method,
            'current_target_pay_method', tcr.current_target_pay_method,
            'component_amount_ex_vat', round(coalesce(tcr.component_amount_ex_vat,0),2),
            'preview_component_amount_ex_vat', round(coalesce(tcr.preview_component_amount_ex_vat, tcr.component_amount_ex_vat, 0),2),
            'ready_preview_amount_ex_vat', round(coalesce(tcr.ready_preview_amount_ex_vat, 0),2),
            'blocked_preview_amount_ex_vat', round(coalesce(tcr.blocked_preview_amount_ex_vat, 0),2),
            'source_basis_json', jsonb_strip_nulls(
              tcr.source_basis_json
              || jsonb_build_object(
                'source_rate', case when tcr.source_rate is null then null else round(tcr.source_rate,2) end,
                'source_charge_rate',
                  case
                    when tcer.effective_source_charge_rate is null then null
                    else round(tcer.effective_source_charge_rate, 2)
                  end
              )
            ),
            'saved_target_pay_method', null,
            'saved_resolution_mode', case when tcr.approved_resolution_mode is null then null else tcr.approved_resolution_mode end,
            'saved_resolution_payload_json', null,
            'saved_resolution_result_json', null,
            'has_suggested_resolution', tcr.has_suggested_resolution,
            'suggestion_provenance', tcr.suggestion_provenance,
            'is_fresh_suggested_resolution', tcr.is_fresh_suggested_resolution,
            'is_reusable_saved_resolution', tcr.is_reusable_saved_resolution,
            'is_stale_saved_resolution', tcr.is_stale_saved_resolution,
            'suggested_resolution_payload_json', tcr.suggested_resolution_payload_json,
            'suggested_resolution_result_json', tcr.suggested_resolution_result_json,
            'source_units', tcr.source_units,
            'target_units', case when ttrg.target_units is null then tcr.source_units else ttrg.target_units end,
            'source_rate', case when tcr.source_rate is null then null else round(tcr.source_rate,2) end,
            'source_charge_rate',
              case
                when tcer.effective_source_charge_rate is null then null
                else round(tcer.effective_source_charge_rate, 2)
              end,
            'target_rate', case when tcr.target_rate is null then null else round(tcr.target_rate,2) end,
            'source_pay_ex_vat', tcr.source_pay_ex_vat,
            'source_charge_ex_vat', tcr.source_charge_component_ex_vat,
            'source_margin_ex_vat', tcr.source_margin_ex_vat,
            'target_pay_ex_vat', tcr.target_pay_ex_vat,
            'target_charge_ex_vat', tcr.target_charge_ex_vat,
            'target_margin_ex_vat', tcr.target_margin_ex_vat,
            'margin_delta_ex_vat', tcr.margin_delta_ex_vat,
            'suggestion_explanation_text', tcr.suggestion_explanation_text,
            'component_fingerprint', tcr.component_fingerprint,
            'is_resolution_stale', false,
            'stale_reason', null,
            'requires_resolution', coalesce(tcr.requires_resolution, false),
            'resolution_state', tcr.resolution_state,
            'is_actionable_resolution_row', coalesce(tcr.is_actionable_resolution_row, false),
            'is_fixed_no_action_taxable_row', coalesce(tcr.is_fixed_no_action_taxable_row, false)
          )
          order by
            case
              when tcr.component_key_type in ('TS_DAY','TS_TOTAL') then 1
              when tcr.component_key_type = 'ADDITIONAL_CODE' then 2
              when tcr.component_key_type = 'ADJUSTMENT_CODE' then 3
              when tcr.component_key_type = 'EXPENSE_CODE' then 4
              else 9
            end,
            tcr.component_key_value,
            coalesce(nullif(btrim(coalesce(tcr.source_basis_json->>'bucket_code','')), ''), '')
        ),
        '[]'::jsonb
      ) as case_components_json
    from transient_timesheet_component_review_rows_effective tcr
    left join ts_deltas td
      on td.timesheet_id = tcr.timesheet_id
     and td.candidate_id = tcr.candidate_id
    left join timesheet_live_scope tls
      on tls.timesheet_id = tcr.timesheet_id
    left join timesheet_linked_scope_counts tslc
      on tslc.seed_timesheet_id = tcr.timesheet_id
    left join lateral (
      select
        case
          when tcr.source_charge_rate is not null then tcr.source_charge_rate
          when coalesce(tcr.source_basis_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$'
            then (tcr.source_basis_json->>'source_charge_rate')::numeric
          when coalesce(tcr.source_units,0) <> 0 and tcr.source_charge_component_ex_vat is not null
            then (tcr.source_charge_component_ex_vat / tcr.source_units)
          else null::numeric
        end as effective_source_charge_rate
    ) tcer on true
    left join lateral (
      select
        case
          when tcr.target_rate is not null and tcr.source_units is not null then round(tcr.source_units,6)
          else tcr.source_units
        end as target_units
    ) ttrg on true
    group by tcr.candidate_id, tcr.timesheet_id
  ),
  finance_candidate_seed as (
    select
      vfcr.candidate_id,
      c.tms_ref as cand_tms_ref,
      c.display_name as cand_display_name,
      upper(coalesce(c.pay_method,'')) as cand_pay_method,
      c.umbrella_id as cand_umbrella_id,
      coalesce(u.enabled,false) as umb_enabled,
      coalesce(u.vat_chargeable,false) as umb_vat_chargeable,
      (c.bank_details_hash is not null and btrim(c.bank_details_hash) <> '') as candidate_has_bank_details,
      c.bank_details_hash as candidate_bank_hash,
      (u.bank_details_hash is not null and btrim(u.bank_details_hash) <> '') as umbrella_has_bank_details,
      u.bank_details_hash as umbrella_bank_hash
    from finance_case_baseline_scope vfcr
    join public.candidates c
      on c.id = vfcr.candidate_id
    left join public.umbrellas u
      on u.id = c.umbrella_id
  ),
  candidate_base as (
    select
      x.candidate_id,
      max(x.cand_tms_ref) as cand_tms_ref,
      max(x.cand_display_name) as cand_display_name,
      max(x.cand_pay_method) as cand_pay_method,
      max(x.cand_umbrella_id::text)::uuid as cand_umbrella_id,
      bool_or(x.umb_enabled) as umb_enabled,
      bool_or(x.umb_vat_chargeable) as umb_vat_chargeable,
      bool_or(x.candidate_has_bank_details) as candidate_has_bank_details,
      max(x.candidate_bank_hash) as candidate_bank_hash,
      bool_or(x.umbrella_has_bank_details) as umbrella_has_bank_details,
      max(x.umbrella_bank_hash) as umbrella_bank_hash
    from (
      select
        d.candidate_id,
        d.cand_tms_ref,
        d.cand_display_name,
        d.cand_pay_method,
        d.cand_umbrella_id,
        d.umb_enabled,
        d.umb_vat_chargeable,
        (d.cand_bank_hash is not null and btrim(d.cand_bank_hash) <> '') as candidate_has_bank_details,
        d.cand_bank_hash as candidate_bank_hash,
        (d.umb_bank_hash is not null and btrim(d.umb_bank_hash) <> '') as umbrella_has_bank_details,
        d.umb_bank_hash as umbrella_bank_hash
      from ts_deltas d
      union all
      select
        fcs.candidate_id,
        fcs.cand_tms_ref,
        fcs.cand_display_name,
        fcs.cand_pay_method,
        fcs.cand_umbrella_id,
        fcs.umb_enabled,
        fcs.umb_vat_chargeable,
        fcs.candidate_has_bank_details,
        fcs.candidate_bank_hash,
        fcs.umbrella_has_bank_details,
        fcs.umbrella_bank_hash
      from finance_candidate_seed fcs
    ) x
    group by x.candidate_id
  ),
  timesheet_candidate_rollup as (
    select
      tcr.candidate_id,
      bool_or(coalesce(tcr.unresolved_taxable_count, 0) > 0) as has_mismatch,
      round(sum(case when coalesce(tcr.is_blocked, false) = false then coalesce(tcr.case_total_amount_ex, 0) else 0 end), 2) as non_mismatch_total_ex,
      round(sum(case when coalesce(tcr.unresolved_taxable_count, 0) > 0 and tcr.ts_pay_method = 'PAYE' then coalesce(tcr.unresolved_taxable_amount_ex, 0) else 0 end), 2) as mismatch_source_paye_ex,
      round(sum(case when coalesce(tcr.unresolved_taxable_count, 0) > 0 and tcr.ts_pay_method = 'UMBRELLA' then coalesce(tcr.unresolved_taxable_amount_ex, 0) else 0 end), 2) as mismatch_source_umbrella_ex,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', tcr.timesheet_id::text,
            'week_ending_date', case when tcr.ts_week_ending_date is null then null else tcr.ts_week_ending_date::text end,
            'client_id', case when tcr.client_id is null then null else tcr.client_id::text end,
            'client_name', tcr.ts_client_name,
            'payment_amount_ex_vat', tcr.payment_amount_ex_vat,
            'payment_amount_inc_vat', tcr.payment_amount_inc_vat,
            'payment_amount', tcr.payment_amount,
            'source_pay_method', tcr.ts_pay_method,
            'candidate_pay_method', tcr.cand_pay_method,
            'segment_deltas', tcr.segment_deltas_json,
            'adjustment_deltas', tcr.adjustment_deltas_json,
            'delta_additional_pay_ex_vat', tcr.delta_additional_pay_ex_vat,
            'additional_unit_deltas', tcr.additional_unit_deltas_json,
            'reservation_overrun_detected', tcr.reservation_overrun_detected,
            'delta_expenses_pay_ex_vat', tcr.delta_expenses_pay_ex_vat,
            'delta_travel_pay_ex_vat', tcr.delta_travel_pay_ex_vat,
            'delta_accommodation_pay_ex_vat', tcr.delta_accommodation_pay_ex_vat,
            'delta_other_pay_ex_vat', tcr.delta_other_pay_ex_vat,
            'delta_mileage_pay_ex_vat', tcr.delta_mileage_pay_ex_vat,
            'case_key', ('timesheet:' || tcr.timesheet_id::text),
            'case_resolution_summary', coalesce(tcr.case_resolution_summary_json, '{}'::jsonb),
            'components', coalesce(tcr.case_components_json, '[]'::jsonb)
          )
          order by tcr.ts_week_ending_date, tcr.ts_client_name, tcr.timesheet_id
        ) filter (where round(coalesce(tcr.case_total_amount_ex,0),2) <> 0),
        '[]'::jsonb
      ) as timesheets_itemisation
    from timesheet_case_rollup tcr
    group by tcr.candidate_id
  ),
  candidate_rollup as (
    select
      cb.candidate_id,
      cb.cand_tms_ref,
      cb.cand_display_name,
      cb.cand_pay_method,
      cb.cand_umbrella_id,
      cb.umb_enabled,
      cb.umb_vat_chargeable,
      cb.candidate_has_bank_details,
      cb.candidate_bank_hash,
      cb.umbrella_has_bank_details,
      cb.umbrella_bank_hash,
      coalesce(tcrr.has_mismatch, false) as has_mismatch,
      coalesce(tcrr.non_mismatch_total_ex, 0) as non_mismatch_total_ex,
      coalesce(tcrr.mismatch_source_paye_ex, 0) as mismatch_source_paye_ex,
      coalesce(tcrr.mismatch_source_umbrella_ex, 0) as mismatch_source_umbrella_ex,
      coalesce(tcrr.timesheets_itemisation, '[]'::jsonb) as timesheets_itemisation
    from candidate_base cb
    left join timesheet_candidate_rollup tcrr
      on tcrr.candidate_id = cb.candidate_id
  ),
  blocked_counts as (
    select bi.candidate_id, count(*)::int as blocked_count
    from blocked_items bi
    group by bi.candidate_id
  ),
  do_not_pay_counts as (
    select di.candidate_id, count(*)::int as do_not_pay_count
    from do_not_pay_items di
    group by di.candidate_id
  ),
  -- ✅ Loan catch-up (Option A): include ALL schedule entries week_start <= v_week_start and amount < 0
  loan_due as (
    select
      pa.candidate_id,
      round(
        sum(
          abs(coalesce(nullif(e->>'amount','')::numeric,0))
        ),
        2
      ) as loan_due_total,
      jsonb_agg(
        jsonb_build_object(
          'advance_id', pa.id::text,
          'week_start', (nullif(e->>'week_start','')::date)::text,
          'due_amount', round(abs(coalesce(nullif(e->>'amount','')::numeric,0)),2),
          'reason', pa.reason::text,
          -- legacy alias (kept to avoid breaking any existing consumer)
          'scheduled_amount', round(abs(coalesce(nullif(e->>'amount','')::numeric,0)),2)
        )
        order by (nullif(e->>'week_start','')::date) asc nulls last, pa.created_at asc, pa.id
      ) as loan_due_entries
    from public.pay_advances pa
    join lateral jsonb_array_elements(coalesce(pa.schedule_json,'[]'::jsonb)) e on true
    where pa.status::text = 'ACTIVE'
      and nullif(e->>'week_start','') is not null
      and (nullif(e->>'week_start','')::date) <= v_week_start
      and coalesce(nullif(e->>'amount','')::numeric,0) < 0
    group by pa.candidate_id
  ),
  overpayment_balances as (
    select
      pa.candidate_id,
      round(sum(coalesce(pa.outstanding_amount,0)),2) as overpayment_balance_remaining
    from public.pay_advances pa
    where upper(coalesce(pa.advance_kind::text,'')) = 'OVERPAYMENT'
      and upper(coalesce(pa.status::text,'')) = 'ACTIVE'
    group by pa.candidate_id
  ),
  loan_due_this_week as (
    select
      pa.candidate_id,
      round(sum(
        case
          when upper(coalesce(pa.advance_kind::text,'')) = 'LOAN'
            and upper(coalesce(pa.status::text,'')) = 'ACTIVE'
            and upper(coalesce(pa.payout_status::text,'')) = 'PAID'
            and coalesce(pa.outstanding_amount,0) > 0
            and (pa.start_week_start is null or pa.start_week_start <= v_week_start)
          then least(coalesce(pa.weekly_due,0), coalesce(pa.outstanding_amount,0))
          else 0
        end
      ),2) as loan_due_this_week
    from public.pay_advances pa
    group by pa.candidate_id
  ),
  loan_repaid_wtd as (
    select
      pbc.candidate_id,
      round(sum(abs(coalesce(pbi.amount_ex_vat,0))),2) as loan_repaid_wtd
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb
      on pb.id = pbc.pay_batch_id
    where pbi.item_type = 'LOAN_REPAYMENT'
      and pb.pay_date::date >= v_week_start
      and pb.pay_date::date < v_pay_date
      and upper(coalesce(pb.status::text,'')) <> 'CANCELLED'
    group by pbc.candidate_id
  ),
  paid_wtd_before as (
    select
      cr.candidate_id,
      public._pay_candidate_arranged_pay_wtd_before(
        v_candidate_id => cr.candidate_id,
        p_week_start => v_week_start,
        v_pay_date => v_pay_date,
        p_before_created_at_utc => null::timestamptz,
        p_before_pay_batch_id => null::uuid
      )::numeric(12,2) as paid_wtd_before
    from candidate_rollup cr
  ),
  cand_enriched as (
    select
      cr.candidate_id,
      cr.cand_tms_ref,
      cr.cand_display_name,
      cr.cand_pay_method,
      cr.cand_umbrella_id,
      cr.umb_enabled,
      cr.umb_vat_chargeable,
      cr.has_mismatch,
      cr.non_mismatch_total_ex,
      cr.mismatch_source_paye_ex,
      cr.mismatch_source_umbrella_ex,
      cr.timesheets_itemisation,
      coalesce(bc.blocked_count,0) as blocked_count,
      coalesce(dpc.do_not_pay_count,0) as do_not_pay_count,

      -- ✅ bank readiness
      cr.candidate_has_bank_details,
      cr.candidate_bank_hash,
      cr.umbrella_has_bank_details,
      cr.umbrella_bank_hash,

      -- ✅ loan catch-up (legacy schedule)
      coalesce(ld.loan_due_total,0) as loan_due_total,
      coalesce(ld.loan_due_entries,'[]'::jsonb) as loan_due_entries,

      -- ✅ Policy B: overpayment + loan state (week-to-date)
      coalesce(ob.overpayment_balance_remaining,0) as overpayment_balance_remaining,
      coalesce(ldtw.loan_due_this_week,0) as loan_due_this_week,
      coalesce(lrw.loan_repaid_wtd,0) as loan_repaid_wtd,
      coalesce(cand.min_take_home_wtd,0) as min_take_home_wtd,
      round(
        case
          when cr.cand_pay_method = 'UMBRELLA' then
            least(
              greatest(coalesce(ldtw.loan_due_this_week,0) - coalesce(lrw.loan_repaid_wtd,0),0),
              greatest(
                least(
                  -- earnings_after_recovery
                  (greatest(cr.non_mismatch_total_ex,0) - least(coalesce(ob.overpayment_balance_remaining,0), greatest(cr.non_mismatch_total_ex,0))),
                  -- above-floor cap
                  (coalesce(pwb.paid_wtd_before,0) + (greatest(cr.non_mismatch_total_ex,0) - least(coalesce(ob.overpayment_balance_remaining,0), greatest(cr.non_mismatch_total_ex,0)))) - coalesce(cand.min_take_home_wtd,0)
                ),
                0
              )
            )
          else null
        end,
        2
      ) as max_possible_loan_take_this_run,
      case when cr.cand_pay_method = 'PAYE' then 'NET_REQUIRED' else 'NOT_REQUIRED' end as paye_net_status
    from candidate_rollup cr
    left join blocked_counts bc on bc.candidate_id = cr.candidate_id
    left join do_not_pay_counts dpc on dpc.candidate_id = cr.candidate_id
    left join loan_due ld on ld.candidate_id = cr.candidate_id
    left join overpayment_balances ob on ob.candidate_id = cr.candidate_id
    left join loan_due_this_week ldtw on ldtw.candidate_id = cr.candidate_id
    left join loan_repaid_wtd lrw on lrw.candidate_id = cr.candidate_id
    left join paid_wtd_before pwb on pwb.candidate_id = cr.candidate_id
    left join public.candidates cand on cand.id = cr.candidate_id
  ),
  -- ✅ NEW: Payees section (candidate + umbrella) + readiness derived from bank_name_checks + bank_payee_map
  payee_baseline_rows as (
    select
      'CANDIDATE'::text as payee_entity_kind,
      ce.candidate_id as payee_entity_id,
      nullif(btrim(coalesce(ce.candidate_bank_hash,'')), '') as bank_details_hash
    from cand_enriched ce
    union all
    select
      'UMBRELLA'::text as payee_entity_kind,
      ce.cand_umbrella_id as payee_entity_id,
      nullif(btrim(coalesce(ce.umbrella_bank_hash,'')), '') as bank_details_hash
    from cand_enriched ce
    where ce.cand_umbrella_id is not null
    union all
    select
      'CANDIDATE'::text as payee_entity_kind,
      fcb.candidate_id as payee_entity_id,
      nullif(btrim(coalesce(obd.bank_details_hash,'')), '') as bank_details_hash
    from finance_case_baseline_scope fcb
    join public.pay_finance_case_oneoff_payout_bank_details obd
      on obd.finance_case_id = fcb.finance_case_id
    where fcb.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
    union all
    select
      case
        when fcb.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then 'UMBRELLA'
        else 'CANDIDATE'
      end as payee_entity_kind,
      case
        when fcb.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then c_fc.umbrella_id
        else fcb.candidate_id
      end as payee_entity_id,
      case
        when fcb.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then nullif(btrim(coalesce(obd.bank_details_hash,'')), '')
        when fcb.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then nullif(btrim(coalesce(u_fc.bank_details_hash,'')), '')
        else nullif(btrim(coalesce(c_fc.bank_details_hash,'')), '')
      end as bank_details_hash
    from finance_case_baseline_scope fcb
    join public.candidates c_fc
      on c_fc.id = fcb.candidate_id
    left join public.umbrellas u_fc
      on u_fc.id = c_fc.umbrella_id
    left join public.pay_finance_case_oneoff_payout_bank_details obd
      on obd.finance_case_id = fcb.finance_case_id
  ),
  payees_src as (
    select
      pbr.payee_entity_kind,
      pbr.payee_entity_id,
      pbr.bank_details_hash
    from payee_baseline_rows pbr
    where pbr.payee_entity_id is not null
  ),
  payees as (
    select
      upper(btrim(coalesce(ps.payee_entity_kind,''))) as payee_entity_kind,
      ps.payee_entity_id as payee_entity_id,
      ps.bank_details_hash as bank_details_hash
    from payees_src ps
    where ps.payee_entity_id is not null
    group by 1,2,3
  ),
  payees_enriched as (
    select
      p.payee_entity_kind,
      p.payee_entity_id,
      p.bank_details_hash,

      b.payee_name,
      b.account_holder,
      b.bank_name,
      b.sort_code,
      b.account_number,
      b.account_type,

      coalesce(bnc.status, 'UNVERIFIED') as name_check_status,
      bnc.checked_at_utc as name_check_checked_at_utc,
      bnc.override_reason as name_check_override_reason,
      bnc.override_by_user_id as name_check_override_by_user_id,
      bnc.override_at_utc as name_check_override_at_utc,
      bnc.override_hash as name_check_override_hash,
      (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash) as name_check_has_override,

      bpm.payee_id as payee_map_payee_id,
      bpm.payee_account_id as payee_map_payee_account_id,
      bpm.meta_json as payee_map_meta_json,
      (bpm.payee_id is not null) as payee_map_present,

      b.is_missing_bank_details as is_missing_bank_details,

      (
        v_need_name_check = true
        and b.is_missing_bank_details = false
        and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
        and not (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash)
      ) as is_name_check_blocked,

      (
        v_requires_payee_map = true
        and b.is_missing_bank_details = false
        and (bpm.payee_id is null)
      ) as is_payee_map_blocked
    from payees p
    left join public.candidates c_pay
      on p.payee_entity_kind = 'CANDIDATE'
     and c_pay.id = p.payee_entity_id
    left join public.umbrellas u_pay
      on p.payee_entity_kind = 'UMBRELLA'
     and u_pay.id = p.payee_entity_id
    left join lateral (
      select
        vfcr_oneoff.finance_case_id,
        obd.beneficiary_name,
        obd.sort_code,
        obd.account_number,
        obd.bank_details_hash
      from finance_case_baseline_scope vfcr_oneoff
      join public.pay_finance_case_oneoff_payout_bank_details obd
        on obd.finance_case_id = vfcr_oneoff.finance_case_id
      where p.payee_entity_kind = 'CANDIDATE'
        and vfcr_oneoff.candidate_id = p.payee_entity_id
        and vfcr_oneoff.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
        and obd.bank_details_hash is not distinct from p.bank_details_hash
      order by vfcr_oneoff.finance_case_id
      limit 1
    ) obd_pay on true

    left join lateral (
      select
        case
          when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.beneficiary_name
          when p.payee_entity_kind = 'CANDIDATE' then c_pay.display_name
          else u_pay.name
        end as payee_name,
        case
          when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.beneficiary_name
          when p.payee_entity_kind = 'CANDIDATE' then c_pay.account_holder
          else u_pay.name
        end as account_holder,
        case
          when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then null::text
          else coalesce(c_pay.bank_name, u_pay.bank_name)
        end as bank_name,
        case
          when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.sort_code
          else coalesce(c_pay.sort_code, u_pay.sort_code)
        end as sort_code,
        case
          when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.account_number
          else coalesce(c_pay.account_number, u_pay.account_number)
        end as account_number,
        case
          when p.payee_entity_kind = 'CANDIDATE' then 'personal'
          else 'business'
        end as account_type,
        (
          p.bank_details_hash is null
          or btrim(p.bank_details_hash) = ''
          or nullif(btrim(coalesce(
                case
                  when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.beneficiary_name
                  when p.payee_entity_kind = 'CANDIDATE' then c_pay.account_holder
                  else u_pay.name
                end,
                ''
              )), '') is null
          or nullif(btrim(coalesce(
                case
                  when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.sort_code
                  else coalesce(c_pay.sort_code, u_pay.sort_code)
                end,
                ''
              )), '') is null
          or nullif(btrim(coalesce(
                case
                  when p.payee_entity_kind = 'CANDIDATE' and obd_pay.finance_case_id is not null then obd_pay.account_number
                  else coalesce(c_pay.account_number, u_pay.account_number)
                end,
                ''
              )), '') is null
        ) as is_missing_bank_details
    ) b on true

    left join public.bank_name_checks bnc
      on bnc.rail_provider = v_rail_provider_default
     and bnc.rail_env = v_rail_env_default
     and bnc.entity_kind = p.payee_entity_kind
     and bnc.entity_id = p.payee_entity_id
     and bnc.bank_details_hash is not distinct from p.bank_details_hash
    left join public.bank_payee_map bpm
      on bpm.rail_provider = v_rail_provider_default
     and bpm.rail_env = v_rail_env_default
     and bpm.entity_kind = p.payee_entity_kind
     and bpm.entity_id = p.payee_entity_id
     and bpm.bank_details_hash is not distinct from p.bank_details_hash
  ),
  payees_json as (
    select
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'payee_entity_kind', pe.payee_entity_kind,
            'payee_entity_id', pe.payee_entity_id::text,
            'bank_details_hash', pe.bank_details_hash,

            -- ✅ Bank fields required to call the rail
            'payee_name', pe.payee_name,
            'account_holder', pe.account_holder,
            'bank_name', pe.bank_name,
            'sort_code', pe.sort_code,
            'account_number', pe.account_number,
            'account_type', pe.account_type,

            -- ✅ Name-check details (status + timestamps + override metadata)
            'name_check', jsonb_build_object(
              'status', pe.name_check_status,
              'checked_at_utc', pe.name_check_checked_at_utc,
              'has_override', (pe.name_check_has_override = true),
              'override_reason', pe.name_check_override_reason,
              'override_by_user_id', case when pe.name_check_override_by_user_id is null then null else pe.name_check_override_by_user_id::text end,
              'override_at_utc', pe.name_check_override_at_utc,
              'override_hash', pe.name_check_override_hash
            ),

            -- ✅ Payee map details (presence + IDs)
            'payee_map', jsonb_build_object(
              'present', (pe.payee_map_present = true),
              'payee_id', pe.payee_map_payee_id,
              'payee_account_id', pe.payee_map_payee_account_id
            ),

            'blockers',
              (
                (case when pe.is_missing_bank_details then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
                ||
                (case when pe.is_name_check_blocked then jsonb_build_array('BLOCKED_NAME_CHECK') else '[]'::jsonb end)
                ||
                (case when pe.is_payee_map_blocked then jsonb_build_array('BLOCKED_NO_PAYEE_MAP') else '[]'::jsonb end)
              )
          )
          order by pe.payee_entity_kind, pe.payee_entity_id::text
        ),
        '[]'::jsonb
      ) as payees
    from payees_enriched pe
  ),
  cand_payee0 as (
    select
      ce.*,
      case when ce.cand_pay_method = 'PAYE' then 'CANDIDATE' else 'UMBRELLA' end as payee_entity_kind,
      case when ce.cand_pay_method = 'PAYE' then ce.candidate_id else ce.cand_umbrella_id end as payee_entity_id,
      case when ce.cand_pay_method = 'PAYE' then nullif(btrim(coalesce(ce.candidate_bank_hash,'')), '') else nullif(btrim(coalesce(ce.umbrella_bank_hash,'')), '') end as payee_bank_hash
    from cand_enriched ce
  ),
  cand_payee as (
    select
      cp.*,

      coalesce(pe.name_check_status, 'UNVERIFIED') as payee_name_check_status,
      coalesce(pe.name_check_has_override, false) as payee_name_check_has_override,
      coalesce(pe.payee_map_present, false) as payee_map_present,

      (
        (case when (cp.payee_entity_id is null or cp.payee_bank_hash is null or btrim(cp.payee_bank_hash) = '') then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
        ||
        (case
           when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_need_name_check = true and coalesce(pe.name_check_status,'UNVERIFIED') <> 'PASS' and not coalesce(pe.name_check_has_override,false))
           then jsonb_build_array('BLOCKED_NAME_CHECK')
           else '[]'::jsonb
         end)
        ||
        (case
           when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_requires_payee_map = true and not coalesce(pe.payee_map_present,false))
           then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
           else '[]'::jsonb
         end)
      ) as blockers,

      (
        jsonb_array_length(
          (
            (case when (cp.payee_entity_id is null or cp.payee_bank_hash is null or btrim(cp.payee_bank_hash) = '') then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
            ||
            (case
               when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_need_name_check = true and coalesce(pe.name_check_status,'UNVERIFIED') <> 'PASS' and not coalesce(pe.name_check_has_override,false))
               then jsonb_build_array('BLOCKED_NAME_CHECK')
               else '[]'::jsonb
             end)
            ||
            (case
               when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_requires_payee_map = true and not coalesce(pe.payee_map_present,false))
               then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
               else '[]'::jsonb
             end)
          )
        ) = 0
      ) as is_ready_for_draft
    from cand_payee0 cp
    left join payees_enriched pe
      on pe.payee_entity_kind = cp.payee_entity_kind
     and pe.payee_entity_id = cp.payee_entity_id
     and pe.bank_details_hash is not distinct from cp.payee_bank_hash
  ),
  timesheet_case_rollup_effective as (
    select
      tcr.candidate_id,
      tcr.timesheet_id,
      tcr.client_id,
      tcr.ts_week_ending_date,
      tcr.ts_client_name,
      tcr.ts_pay_method,
      tcr.cand_pay_method,
      tcr.cand_tms_ref,
      tcr.cand_display_name,
      tcr.cand_umbrella_id,
      tcr.umb_enabled,
      tcr.umb_vat_chargeable,
      tcr.candidate_has_bank_details,
      tcr.candidate_bank_hash,
      tcr.umbrella_has_bank_details,
      tcr.umbrella_bank_hash,
      tcr.resolution_family,
      tcr.case_needs_resolution,
      tcr.case_resolution_satisfied_now,
      tcr.resolution_action_label,
      tcr.linked_resolution_scope_json,
      tcr.case_total_amount_ex,
      case
        when (coalesce(tcr.is_blocked, false) or not coalesce(cp.is_ready_for_draft, false))
        then 0::numeric
        else tcr.safe_amount_ex
      end as safe_amount_ex,
      tcr.unresolved_taxable_amount_ex,
      tcr.open_taxable_count,
      tcr.open_reimbursement_count,
      tcr.unresolved_taxable_count,
      tcr.stale_count,
      tcr.is_mixed_case,
      (coalesce(tcr.is_blocked, false) or not coalesce(cp.is_ready_for_draft, false)) as is_blocked,
      tcr.segments_total_ex,
      tcr.delta_additional_pay_ex_vat,
      tcr.delta_expenses_pay_ex_vat,
      tcr.delta_travel_pay_ex_vat,
      tcr.delta_accommodation_pay_ex_vat,
      tcr.delta_other_pay_ex_vat,
      tcr.delta_mileage_pay_ex_vat,
      tcr.segment_deltas_json,
      tcr.adjustment_deltas_json,
      tcr.additional_unit_deltas_json,
      tcr.reservation_overrun_detected,
      tcr.payment_amount_ex_vat,
      tcr.payment_amount_inc_vat,
      tcr.payment_amount,
      (
        coalesce(tcr.case_resolution_summary_json, '{}'::jsonb)
        || jsonb_build_object(
          'is_blocked', (coalesce(tcr.is_blocked, false) or not coalesce(cp.is_ready_for_draft, false)),
          'resolution_family', tcr.resolution_family,
          'case_needs_resolution', tcr.case_needs_resolution,
          'case_resolution_satisfied_now', (tcr.case_resolution_satisfied_now and coalesce(cp.is_ready_for_draft, false)),
          'resolution_action_label', tcr.resolution_action_label,
          'linked_resolution_scope_json', tcr.linked_resolution_scope_json,
          'safe_amount_ex_vat', case
            when (coalesce(tcr.is_blocked, false) or not coalesce(cp.is_ready_for_draft, false))
            then 0::numeric
            else coalesce(tcr.case_total_amount_ex, 0)
          end,
          'blocked_case_amount_ex_vat', case
            when (coalesce(tcr.is_blocked, false) or not coalesce(cp.is_ready_for_draft, false))
            then coalesce(tcr.case_total_amount_ex, 0)
            else 0::numeric
          end,
          'blocked_reason_codes', case
            when coalesce(tcr.is_blocked, false) = true then jsonb_build_array('BLOCKED_TAXABLE_RESOLUTION')
            else '[]'::jsonb
          end
        )
      ) as case_resolution_summary_json,
      tcr.case_components_json
    from timesheet_case_rollup tcr
    left join cand_payee cp
      on cp.candidate_id = tcr.candidate_id
  ),
  summary_json as (
    select
      jsonb_build_object(
        'readiness', jsonb_build_object(
          'payees_total', pr.payees_total,
          'payees_need_name_check', pr.payees_need_name_check,
          'payees_need_payee_map', pr.payees_need_payee_map,
          'payees_missing_bank_details', pr.payees_missing_bank_details
        ),
        'candidates', jsonb_build_object(
          'ready_count', cr.ready_count,
          'review_required_count', cr.review_required_count,
          'total_candidates', cr.total_candidates
        )
      ) as summary
    from (
      select
        count(*)::int as payees_total,
        sum(case when pe.is_missing_bank_details then 1 else 0 end)::int as payees_missing_bank_details,
        sum(case when pe.is_name_check_blocked then 1 else 0 end)::int as payees_need_name_check,
        sum(case when pe.is_payee_map_blocked then 1 else 0 end)::int as payees_need_payee_map
      from payees_enriched pe
    ) pr
    cross join (
      select
        count(*)::int as total_candidates,
        sum(
          case when
            (
              (coalesce(cp.non_mismatch_total_ex,0) <> 0
               or coalesce(cp.mismatch_source_paye_ex,0) <> 0
               or coalesce(cp.mismatch_source_umbrella_ex,0) <> 0)
              and cp.is_ready_for_draft = true
              and coalesce(cp.blocked_count,0) = 0
              and coalesce(cp.do_not_pay_count,0) = 0
              and cp.has_mismatch = false
            )
          then 1 else 0 end
        )::int as ready_count,
        sum(
          case when
            (
              (coalesce(cp.non_mismatch_total_ex,0) <> 0
               or coalesce(cp.mismatch_source_paye_ex,0) <> 0
               or coalesce(cp.mismatch_source_umbrella_ex,0) <> 0)
              and (
                cp.has_mismatch = true
                or jsonb_array_length(cp.blockers) > 0
                or coalesce(cp.blocked_count,0) > 0
                or coalesce(cp.do_not_pay_count,0) > 0
              )
            )
          then 1 else 0 end
        )::int as review_required_count
      from cand_payee cp
    ) cr
  ),
  finance_case_repaid_wtd as (
    select
      nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as finance_case_id,
      round(sum(abs(coalesce(pbi.amount_ex_vat,0))),2) as repaid_wtd_ex
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb
      on pb.id = pbc.pay_batch_id
    where pbi.is_voided = false
      and pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
      and pbi.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY')
      and pbi.repayment_week_start = v_week_start
      and upper(coalesce(pb.status::text,'')) <> 'CANCELLED'
    group by nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
  ),
  finance_case_recovery_rows_base as (
    select
      vfcr.finance_case_id,
      vfcr.candidate_id,
      vfcr.case_type,
      vfcr.taxability,
      upper(coalesce(cr.cand_pay_method,'')) as candidate_pay_method,
      round(greatest(coalesce(cr.non_mismatch_total_ex,0),0),2)::numeric(12,2) as run_earnings_headroom_ex,
      round(
        greatest(
          coalesce(pwb.paid_wtd_before,0)
          + greatest(coalesce(cr.non_mismatch_total_ex,0),0),
          0
        ),
        2
      )::numeric(12,2) as run_take_home_before,
      round(greatest(coalesce(c.min_take_home_wtd,0),0),2)::numeric(12,2) as default_take_home_floor,
      vfcr.payout_status,
      vfcr.created_at,
      case
        when vfcr.minimum_earnings_threshold is null then null::numeric(12,2)
        else round(greatest(vfcr.minimum_earnings_threshold,0),2)::numeric(12,2)
      end as minimum_earnings_threshold,
      case
        when vfcr.take_home_floor_override is null then null::numeric(12,2)
        else round(greatest(vfcr.take_home_floor_override,0),2)::numeric(12,2)
      end as take_home_floor_override,
      round(
        greatest(
          case
            when vfcr.case_type = 'PAYMENT_ADVANCE'
             and upper(coalesce(vfcr.payout_status::text,'')) = 'PAID'
            then least(coalesce(vfcr.weekly_due,0), coalesce(vfcr.outstanding_amount,0))
                 - coalesce(fcrw.repaid_wtd_ex,0)
                 - greatest(coalesce(vfcr.active_reserved_amount,0) - coalesce(fcrw.repaid_wtd_ex,0), 0)
            when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT'
            then least(coalesce(vfcr.weekly_due,0), coalesce(vfcr.outstanding_amount,0))
                 - coalesce(fcrw.repaid_wtd_ex,0)
                 - greatest(coalesce(vfcr.active_reserved_amount,0) - coalesce(fcrw.repaid_wtd_ex,0), 0)
            when vfcr.case_type = 'OVERPAYMENT'
            then greatest(coalesce(vfcr.outstanding_amount,0) - coalesce(vfcr.active_reserved_amount,0), 0)
            else 0::numeric
          end,
          0::numeric
        ),
        2
      )::numeric(12,2) as nominal_due_amount
    from finance_case_baseline_scope vfcr
    join candidate_rollup cr
      on cr.candidate_id = vfcr.candidate_id
    join public.candidates c
      on c.id = vfcr.candidate_id
    left join paid_wtd_before pwb
      on pwb.candidate_id = vfcr.candidate_id
    left join finance_case_repaid_wtd fcrw
      on fcrw.finance_case_id = vfcr.finance_case_id
    where vfcr.case_type in ('PAYMENT_ADVANCE','MANUAL_DEBT_ADJUSTMENT','OVERPAYMENT')
      and (
        (vfcr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(vfcr.payout_status::text,'')) = 'PAID')
        or vfcr.case_type in ('MANUAL_DEBT_ADJUSTMENT','OVERPAYMENT')
      )
  ),
  manual_debt_recovery_rows as (
    select
      fcrrb.candidate_id,
      fcrrb.finance_case_id,
      fcrrb.case_type,
      fcrrb.payout_status,
      fcrrb.nominal_due_amount,
      fcrrb.minimum_earnings_threshold,
      fcrrb.take_home_floor_override,
      fcrrb.run_earnings_headroom_ex,
      fcrrb.run_take_home_before,
      fcrrb.default_take_home_floor,
      row_number() over (
        partition by fcrrb.candidate_id
        order by fcrrb.created_at, fcrrb.finance_case_id
      )::integer as sort_order
    from finance_case_recovery_rows_base fcrrb
    where fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT'
      and fcrrb.nominal_due_amount > 0
  ),
  manual_debt_recovery_allocations as (
    select
      mdra.candidate_id,
      mdra_alloc.finance_case_id,
      round(coalesce(mdra_alloc.protected_recoverable_amount,0),2)::numeric(12,2) as protected_recoverable_amount
    from (
      select
        mdrr.candidate_id,
        max(mdrr.run_earnings_headroom_ex) as run_earnings_headroom_ex,
        max(mdrr.run_take_home_before) as run_take_home_before,
        max(mdrr.default_take_home_floor) as default_take_home_floor,
        jsonb_agg(
          jsonb_build_object(
            'sort_order', mdrr.sort_order,
            'finance_case_id', mdrr.finance_case_id::text,
            'case_type', mdrr.case_type::text,
            'payout_status', case when mdrr.payout_status is null then null else mdrr.payout_status::text end,
            'nominal_due_amount', mdrr.nominal_due_amount,
            'minimum_earnings_threshold', mdrr.minimum_earnings_threshold,
            'take_home_floor_override', mdrr.take_home_floor_override
          )
          order by mdrr.sort_order, mdrr.finance_case_id
        ) as recovery_rows_json
      from manual_debt_recovery_rows mdrr
      group by mdrr.candidate_id
    ) mdra
    cross join lateral public._pay_finance_protected_recovery_allocate(
      p_recovery_rows => mdra.recovery_rows_json,
      p_run_earnings_headroom => mdra.run_earnings_headroom_ex,
      p_run_take_home_headroom => mdra.run_take_home_before,
      p_default_take_home_floor => mdra.default_take_home_floor
    ) mdra_alloc
  ),
  manual_debt_recovery_totals as (
    select
      mdra.candidate_id,
      round(sum(mdra.protected_recoverable_amount),2)::numeric(12,2) as protected_recoverable_total
    from manual_debt_recovery_allocations mdra
    group by mdra.candidate_id
  ),
  overpayment_recovery_rows as (
    select
      fcrrb.candidate_id,
      fcrrb.candidate_pay_method,
      fcrrb.finance_case_id,
      fcrrb.case_type,
      fcrrb.payout_status,
      fcrrb.nominal_due_amount,
      fcrrb.minimum_earnings_threshold,
      fcrrb.take_home_floor_override,
      fcrrb.run_earnings_headroom_ex,
      fcrrb.run_take_home_before,
      fcrrb.default_take_home_floor,
      row_number() over (
        partition by fcrrb.candidate_id
        order by fcrrb.created_at, fcrrb.finance_case_id
      )::integer as sort_order
    from finance_case_recovery_rows_base fcrrb
    where fcrrb.case_type = 'OVERPAYMENT'
      and fcrrb.nominal_due_amount > 0
  ),
  overpayment_recovery_allocations as (
    select
      opra.candidate_id,
      opra_alloc.finance_case_id,
      round(coalesce(opra_alloc.protected_recoverable_amount,0),2)::numeric(12,2) as protected_recoverable_amount
    from (
      select
        oprr.candidate_id,
        max(
          case
            when oprr.candidate_pay_method = 'UMBRELLA'
              then round(
                greatest(
                  oprr.run_earnings_headroom_ex - coalesce(mdrt.protected_recoverable_total,0),
                  0
                ),
                2
              )::numeric(12,2)
            else oprr.run_earnings_headroom_ex
          end
        ) as run_earnings_headroom_ex,
        max(
          case
            when oprr.candidate_pay_method = 'UMBRELLA'
              then round(
                greatest(
                  oprr.run_take_home_before - coalesce(mdrt.protected_recoverable_total,0),
                  0
                ),
                2
              )::numeric(12,2)
            else oprr.run_take_home_before
          end
        ) as run_take_home_before,
        max(oprr.default_take_home_floor) as default_take_home_floor,
        jsonb_agg(
          jsonb_build_object(
            'sort_order', oprr.sort_order,
            'finance_case_id', oprr.finance_case_id::text,
            'case_type', 'MANUAL_DEBT_ADJUSTMENT',
            'payout_status', null,
            'nominal_due_amount', oprr.nominal_due_amount,
            'minimum_earnings_threshold', oprr.minimum_earnings_threshold,
            'take_home_floor_override', oprr.take_home_floor_override
          )
          order by oprr.sort_order, oprr.finance_case_id
        ) as recovery_rows_json
      from overpayment_recovery_rows oprr
      left join manual_debt_recovery_totals mdrt
        on mdrt.candidate_id = oprr.candidate_id
      group by oprr.candidate_id
    ) opra
    cross join lateral public._pay_finance_protected_recovery_allocate(
      p_recovery_rows => opra.recovery_rows_json,
      p_run_earnings_headroom => opra.run_earnings_headroom_ex,
      p_run_take_home_headroom => opra.run_take_home_before,
      p_default_take_home_floor => opra.default_take_home_floor
    ) opra_alloc
  ),
  overpayment_recovery_totals as (
    select
      opra.candidate_id,
      round(sum(opra.protected_recoverable_amount),2)::numeric(12,2) as protected_recoverable_total
    from overpayment_recovery_allocations opra
    group by opra.candidate_id
  ),
  payment_advance_recovery_rows as (
    select
      fcrrb.candidate_id,
      fcrrb.candidate_pay_method,
      fcrrb.finance_case_id,
      fcrrb.case_type,
      fcrrb.payout_status,
      fcrrb.nominal_due_amount,
      fcrrb.minimum_earnings_threshold,
      fcrrb.take_home_floor_override,
      fcrrb.run_earnings_headroom_ex,
      fcrrb.run_take_home_before,
      row_number() over (
        partition by fcrrb.candidate_id
        order by fcrrb.created_at, fcrrb.finance_case_id
      )::integer as sort_order
    from finance_case_recovery_rows_base fcrrb
    where fcrrb.case_type = 'PAYMENT_ADVANCE'
      and upper(coalesce(fcrrb.payout_status::text,'')) = 'PAID'
      and fcrrb.nominal_due_amount > 0
  ),
  payment_advance_recovery_allocations as (
    select
      para.candidate_id,
      para_alloc.finance_case_id,
      round(coalesce(para_alloc.protected_recoverable_amount,0),2)::numeric(12,2) as protected_recoverable_amount
    from (
      select
        parr.candidate_id,
        max(
          case
            when parr.candidate_pay_method = 'UMBRELLA'
              then round(
                greatest(
                  parr.run_earnings_headroom_ex - coalesce(mdrt.protected_recoverable_total,0) - coalesce(oprt.protected_recoverable_total,0),
                  0
                ),
                2
              )::numeric(12,2)
            else parr.run_earnings_headroom_ex
          end
        ) as run_earnings_headroom_ex,
        max(
          case
            when parr.candidate_pay_method = 'UMBRELLA'
              then round(
                greatest(
                  parr.run_take_home_before - coalesce(mdrt.protected_recoverable_total,0) - coalesce(oprt.protected_recoverable_total,0),
                  0
                ),
                2
              )::numeric(12,2)
            else parr.run_take_home_before
          end
        ) as run_take_home_before,
        jsonb_agg(
          jsonb_build_object(
            'sort_order', parr.sort_order,
            'finance_case_id', parr.finance_case_id::text,
            'case_type', parr.case_type::text,
            'payout_status', case when parr.payout_status is null then null else parr.payout_status::text end,
            'nominal_due_amount', parr.nominal_due_amount,
            'minimum_earnings_threshold', parr.minimum_earnings_threshold,
            'take_home_floor_override', parr.take_home_floor_override
          )
          order by parr.sort_order, parr.finance_case_id
        ) as recovery_rows_json
      from payment_advance_recovery_rows parr
      left join manual_debt_recovery_totals mdrt
        on mdrt.candidate_id = parr.candidate_id
      left join overpayment_recovery_totals oprt
        on oprt.candidate_id = parr.candidate_id
      group by parr.candidate_id
    ) para
    cross join lateral public._pay_finance_protected_recovery_allocate(
      p_recovery_rows => para.recovery_rows_json,
      p_run_earnings_headroom => para.run_earnings_headroom_ex,
      p_run_take_home_headroom => para.run_take_home_before,
      p_default_take_home_floor => null::numeric
    ) para_alloc
  ),
  finance_case_protected_allocations as (
    select
      mdra.finance_case_id,
      mdra.protected_recoverable_amount
    from manual_debt_recovery_allocations mdra

    union all

    select
      opra.finance_case_id,
      opra.protected_recoverable_amount
    from overpayment_recovery_allocations opra

    union all

    select
      para.finance_case_id,
      para.protected_recoverable_amount
    from payment_advance_recovery_allocations para
  ),
  finance_case_payee_readiness as (
    select
      f0.finance_case_id,
      f0.payee_entity_kind,
      f0.payee_entity_id,
      f0.bank_details_hash,
      f0.beneficiary_name,
      f0.sort_code,
      f0.account_number,
      case
        when f0.account_number is null or btrim(coalesce(f0.account_number,'')) = '' then null
        else lpad(right(f0.account_number, 4), greatest(length(f0.account_number), 4), '*')
      end as masked_bank_account,
      coalesce(bnc.status, 'UNVERIFIED') as name_check_status,
      (bnc.override_reason is not null and bnc.override_hash = f0.bank_details_hash) as name_check_has_override,
      (bpm.payee_id is not null) as payee_map_present,
      (
        f0.payee_entity_id is null
        or f0.bank_details_hash is null
        or btrim(coalesce(f0.bank_details_hash,'')) = ''
        or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
        or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
        or nullif(btrim(coalesce(f0.account_number,'')), '') is null
      ) as is_missing_bank_details,
      (
        v_need_name_check = true
        and not (
          f0.payee_entity_id is null
          or f0.bank_details_hash is null
          or btrim(coalesce(f0.bank_details_hash,'')) = ''
          or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
          or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
          or nullif(btrim(coalesce(f0.account_number,'')), '') is null
        )
        and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
        and not (bnc.override_reason is not null and bnc.override_hash = f0.bank_details_hash)
      ) as is_name_check_blocked,
      (
        v_requires_payee_map = true
        and not (
          f0.payee_entity_id is null
          or f0.bank_details_hash is null
          or btrim(coalesce(f0.bank_details_hash,'')) = ''
          or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
          or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
          or nullif(btrim(coalesce(f0.account_number,'')), '') is null
        )
        and bpm.payee_id is null
      ) as is_payee_map_blocked,
      (
        (case
          when (
            f0.payee_entity_id is null
            or f0.bank_details_hash is null
            or btrim(coalesce(f0.bank_details_hash,'')) = ''
            or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
            or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
            or nullif(btrim(coalesce(f0.account_number,'')), '') is null
          )
          then jsonb_build_array('BLOCKED_BANK_DETAILS')
          else '[]'::jsonb
        end)
        ||
        (case
          when (
            v_need_name_check = true
            and not (
              f0.payee_entity_id is null
              or f0.bank_details_hash is null
              or btrim(coalesce(f0.bank_details_hash,'')) = ''
              or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
              or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
              or nullif(btrim(coalesce(f0.account_number,'')), '') is null
            )
            and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
            and not (bnc.override_reason is not null and bnc.override_hash = f0.bank_details_hash)
          )
          then jsonb_build_array('BLOCKED_NAME_CHECK')
          else '[]'::jsonb
        end)
        ||
        (case
          when (
            v_requires_payee_map = true
            and not (
              f0.payee_entity_id is null
              or f0.bank_details_hash is null
              or btrim(coalesce(f0.bank_details_hash,'')) = ''
              or nullif(btrim(coalesce(f0.beneficiary_name,'')), '') is null
              or nullif(btrim(coalesce(f0.sort_code,'')), '') is null
              or nullif(btrim(coalesce(f0.account_number,'')), '') is null
            )
            and bpm.payee_id is null
          )
          then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
          else '[]'::jsonb
        end)
      ) as blocked_reason_codes
    from (
      select
        vfcr.finance_case_id,
        case
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then 'UMBRELLA'
          else 'CANDIDATE'
        end as payee_entity_kind,
        case
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then c.umbrella_id
          else vfcr.candidate_id
        end as payee_entity_id,
        case
          when vfcr.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then obd.bank_details_hash
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then u.bank_details_hash
          else c.bank_details_hash
        end as bank_details_hash,
        case
          when vfcr.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then obd.beneficiary_name
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then u.name
          else coalesce(c.account_holder, c.display_name)
        end as beneficiary_name,
        case
          when vfcr.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then obd.sort_code
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then u.sort_code
          else c.sort_code
        end as sort_code,
        case
          when vfcr.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then obd.account_number
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then u.account_number
          else c.account_number
        end as account_number
      from finance_case_baseline_scope vfcr
      join public.candidates c
        on c.id = vfcr.candidate_id
      left join public.umbrellas u
        on u.id = c.umbrella_id
      left join public.pay_finance_case_oneoff_payout_bank_details obd
        on obd.finance_case_id = vfcr.finance_case_id
      where vfcr.finance_case_id is not null
    ) f0
    left join public.bank_name_checks bnc
      on bnc.rail_provider = v_rail_provider_default
     and bnc.rail_env = v_rail_env_default
     and bnc.entity_kind = f0.payee_entity_kind
     and bnc.entity_id = f0.payee_entity_id
     and bnc.bank_details_hash is not distinct from f0.bank_details_hash
    left join public.bank_payee_map bpm
      on bpm.rail_provider = v_rail_provider_default
     and bpm.rail_env = v_rail_env_default
     and bpm.entity_kind = f0.payee_entity_kind
     and bpm.entity_id = f0.payee_entity_id
     and bpm.bank_details_hash is not distinct from f0.bank_details_hash
  ),
  finance_case_component_rows as (
    select
      vfcr.finance_case_id,
      vfcr.candidate_id,
      vfcr.case_type,
      vfcr.taxability,
      pfc.id as finance_component_id,
      pfc.source_family_key,
      pfc.component_key_type,
      pfc.component_key_value,
      pfc.classification,
      upper(coalesce(pfc.source_pay_method, '')) as source_pay_method,
      upper(coalesce(cp.cand_pay_method, '')) as current_target_pay_method,
      cp.umb_vat_chargeable,
      pfc.source_basis_json,
      round(coalesce(pfc.source_amount, 0), 2) as source_amount,
      round(coalesce(pfc.remaining_source_amount, 0), 2) as remaining_source_amount,
      pfc.saved_target_pay_method,
      pfc.saved_resolution_mode,
      pfc.saved_resolution_payload_json,
      pfc.saved_resolution_result_json,
      pfc.resolution_fingerprint,
      pfc.is_resolution_stale,
      pfc.stale_reason,
      public.pay_finance_component_fingerprint(
        pfc.source_family_key,
        pfc.component_key_type,
        pfc.component_key_value,
        pfc.classification,
        upper(coalesce(pfc.source_pay_method, '')),
        upper(coalesce(cp.cand_pay_method, '')),
        coalesce(pfc.source_basis_json, '{}'::jsonb),
        round(coalesce(pfc.source_amount, 0), 2),
        case
          when coalesce(pfc.saved_resolution_payload_json->>'relevant_erni_pct', pfc.saved_resolution_result_json->>'relevant_erni_pct', '') ~ '^-?\d+(\.\d+)?$'
            then coalesce(pfc.saved_resolution_payload_json->>'relevant_erni_pct', pfc.saved_resolution_result_json->>'relevant_erni_pct')::numeric
          else v_erni_pct
        end,
        coalesce(pfc.saved_resolution_payload_json, pfc.saved_resolution_result_json, '{}'::jsonb)
      ) as current_component_fingerprint
    from finance_case_baseline_scope vfcr
    join cand_payee cp
      on cp.candidate_id = vfcr.candidate_id
    join public.pay_finance_case_components pfc
      on pfc.finance_case_id = vfcr.finance_case_id
     and pfc.closed_at_utc is null
     and coalesce(pfc.remaining_source_amount, 0) > 0
  ),
  finance_case_component_review_rows as (
    select
      fccr.finance_case_id,
      fccr.candidate_id,
      fccr.case_type,
      fccr.taxability,
      fccr.finance_component_id,
      fccr.source_family_key,
      fccr.component_key_type,
      fccr.component_key_value,
      fccr.classification,
      fccr.source_pay_method,
      fccr.current_target_pay_method,
      fccr.umb_vat_chargeable,
      fccr.source_basis_json,
      fccr.source_amount,
      fccr.remaining_source_amount,
      nullif(fccr.source_basis_json->>'source_units','')::numeric as source_units,
      nullif(fccr.source_basis_json->>'source_rate','')::numeric as source_rate,
      nullif(fccr.source_basis_json->>'source_charge_rate','')::numeric as source_charge_rate,
      coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) as source_charge_ex_vat,
      fccr.saved_target_pay_method,
      fccr.saved_resolution_mode,
      fccr.saved_resolution_payload_json,
      fccr.saved_resolution_result_json,
      fccr.resolution_fingerprint,
      fccr.is_resolution_stale,
      fccr.stale_reason,
      fccr.current_component_fingerprint,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and fccr.source_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method <> ''
      ) as has_suggested_resolution,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'NO_SUGGESTION_AVAILABLE'
        when fccr.saved_resolution_mode is not null and coalesce(fccr.is_resolution_stale,false) = false and upper(coalesce(fccr.saved_target_pay_method,'')) = upper(coalesce(fccr.current_target_pay_method,'')) and (fccr.resolution_fingerprint is null or fccr.resolution_fingerprint is not distinct from fccr.current_component_fingerprint) then 'REUSABLE_SAVED_RESOLUTION'
        when fccr.saved_resolution_mode is not null then 'STALE_SAVED_RESOLUTION'
        else 'FRESH_SUGGESTION'
      end as suggestion_provenance,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and fccr.source_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method <> ''
        and fccr.saved_resolution_mode is null
      ) as is_fresh_suggested_resolution,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and fccr.saved_resolution_mode is not null
        and coalesce(fccr.is_resolution_stale,false) = false
        and upper(coalesce(fccr.saved_target_pay_method,'')) = upper(coalesce(fccr.current_target_pay_method,''))
        and (fccr.resolution_fingerprint is null or fccr.resolution_fingerprint is not distinct from fccr.current_component_fingerprint)
      ) as is_reusable_saved_resolution,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and fccr.saved_resolution_mode is not null
        and (
          coalesce(fccr.is_resolution_stale,false) = true
          or upper(coalesce(fccr.saved_target_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
          or (fccr.resolution_fingerprint is not null and fccr.resolution_fingerprint is distinct from fccr.current_component_fingerprint)
        )
      ) as is_stale_saved_resolution,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
        when fccr.saved_resolution_mode is not null and coalesce(fccr.is_resolution_stale,false) = false and upper(coalesce(fccr.saved_target_pay_method,'')) = upper(coalesce(fccr.current_target_pay_method,'')) and (fccr.resolution_fingerprint is null or fccr.resolution_fingerprint is not distinct from fccr.current_component_fingerprint)
          then fccr.saved_resolution_payload_json
        else jsonb_strip_nulls(jsonb_build_object(
          'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS',
          'target_pay_method', fccr.current_target_pay_method,
          'applied_basis_source_amount_ex_vat', round(coalesce(fccr.source_amount,0),2),
          'relevant_erni_pct', round(v_erni_pct,6),
          'vat_rate_pct', round(v_vat_rate_pct,6),
          'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
          'target_units', case when nullif(fccr.source_basis_json->>'source_units','') is not null then round(nullif(fccr.source_basis_json->>'source_units','')::numeric,6) else null end,
          'suggested_target_rate', case when nullif(fccr.source_basis_json->>'source_units','') is not null and nullif(fccr.source_basis_json->>'source_units','')::numeric <> 0 then round(round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2) / (nullif(fccr.source_basis_json->>'source_units','')::numeric), 2) else null end,
          'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
        ))
      end as suggested_resolution_payload_json,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
        when fccr.saved_resolution_mode is not null and coalesce(fccr.is_resolution_stale,false) = false and upper(coalesce(fccr.saved_target_pay_method,'')) = upper(coalesce(fccr.current_target_pay_method,'')) and (fccr.resolution_fingerprint is null or fccr.resolution_fingerprint is not distinct from fccr.current_component_fingerprint)
          then fccr.saved_resolution_result_json
        else jsonb_strip_nulls(jsonb_build_object(
          'target_pay_method', fccr.current_target_pay_method,
          'target_amount_ex_vat', round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2),
          'target_amount_vat', round(coalesce((fcsr.target_amounts_json->>'vat')::numeric,0),2),
          'target_amount_inc_vat', round(coalesce((fcsr.target_amounts_json->>'inc')::numeric,0),2),
          'basis_source_amount_ex_vat', round(coalesce(fccr.source_amount,0),2),
          'applied_basis_source_amount_ex_vat', round(coalesce(fccr.source_amount,0),2),
          'relevant_erni_pct', round(v_erni_pct,6),
          'vat_rate_pct', round(v_vat_rate_pct,6),
          'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
          'target_units', case when nullif(fccr.source_basis_json->>'source_units','') is not null then round(nullif(fccr.source_basis_json->>'source_units','')::numeric,6) else null end,
          'replacement_rate', case when nullif(fccr.source_basis_json->>'source_units','') is not null and nullif(fccr.source_basis_json->>'source_units','')::numeric <> 0 then round(round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2) / (nullif(fccr.source_basis_json->>'source_units','')::numeric), 2) else null end,
          'target_amount_ex_vat_per_source_ex_vat', case when coalesce(fccr.source_amount,0) <> 0 then round(round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2) / fccr.source_amount, 10) else null end,
          'target_amount_vat_per_source_ex_vat', case when coalesce(fccr.source_amount,0) <> 0 then round(round(coalesce((fcsr.target_amounts_json->>'vat')::numeric,0),2) / fccr.source_amount, 10) else null end,
          'target_amount_inc_vat_per_source_ex_vat', case when coalesce(fccr.source_amount,0) <> 0 then round(round(coalesce((fcsr.target_amounts_json->>'inc')::numeric,0),2) / fccr.source_amount, 10) else null end,
          'target_units_per_source_ex_vat', case when nullif(fccr.source_basis_json->>'source_units','') is not null and coalesce(fccr.source_amount,0) <> 0 then round((nullif(fccr.source_basis_json->>'source_units','')::numeric) / fccr.source_amount, 10) else null end,
          'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
          'source_pay_ex_vat', round(coalesce(fccr.source_amount,0),2),
          'source_charge_ex_vat', case when coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) is null then null else round(coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric),2) end,
          'source_margin_ex_vat', case when coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) is null then null else round(coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) - fccr.source_amount,2) end,
          'target_pay_ex_vat', round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2),
          'target_charge_ex_vat', case when coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) is null then null else round(coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric),2) end,
          'target_margin_ex_vat', case when coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) is null or fcsr.target_amounts_json is null then null else round(coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) - round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2),2) end,
          'margin_delta_ex_vat', case when coalesce(nullif(fccr.source_basis_json->>'source_charge_ex_vat','')::numeric, nullif(fccr.saved_resolution_result_json->>'source_charge_ex_vat','')::numeric) is null or fcsr.target_amounts_json is null then null else round(fccr.source_amount - round(coalesce((fcsr.target_amounts_json->>'ex')::numeric,0),2),2) end
        ))
      end as suggested_resolution_result_json,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'Fixed reimbursements are not channel-converted and do not participate in suggested-rates review.'
        when fccr.saved_resolution_mode is not null and coalesce(fccr.is_resolution_stale,false) = false and upper(coalesce(fccr.saved_target_pay_method,'')) = upper(coalesce(fccr.current_target_pay_method,'')) and (fccr.resolution_fingerprint is null or fccr.resolution_fingerprint is not distinct from fccr.current_component_fingerprint) then 'This component already has a reusable saved resolution for the current target pay method.'
        when fccr.saved_resolution_mode is not null then 'A stale saved resolution exists for this component. The suggested rates below reflect the current target pay method.'
        when fccr.source_pay_method <> fccr.current_target_pay_method then 'This suggestion converts the taxable component to a target-side equivalent while leaving fixed reimbursements unchanged.'
        else 'This suggestion preserves equivalent basis using the current target pay method.'
      end as suggestion_explanation_text
    from finance_case_component_rows fccr
    left join lateral (
      select
        case
          when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
          when fccr.source_pay_method = 'PAYE' and fccr.current_target_pay_method = 'UMBRELLA' then public._pay_convert_paye_to_umbrella(fccr.source_amount, v_erni_pct, v_vat_rate_pct, coalesce(fccr.umb_vat_chargeable,false))
          when fccr.source_pay_method = 'UMBRELLA' and fccr.current_target_pay_method = 'PAYE' then jsonb_build_object('ex', public._pay_convert_umbrella_to_paye_ex(fccr.source_amount, v_erni_pct), 'vat', 0, 'inc', public._pay_convert_umbrella_to_paye_ex(fccr.source_amount, v_erni_pct))
          when fccr.current_target_pay_method = 'PAYE' then jsonb_build_object('ex', round(coalesce(fccr.source_amount,0),2), 'vat', 0, 'inc', round(coalesce(fccr.source_amount,0),2))
          when fccr.current_target_pay_method = 'UMBRELLA' then public._pay_umbrella_vat_calc(fccr.source_amount, v_vat_rate_pct, coalesce(fccr.umb_vat_chargeable,false))
          else null::jsonb
        end as target_amounts_json
    ) fcsr on true
  ),
  finance_case_component_review_rows_effective as (
    select
      fccr.finance_case_id,
      fccr.candidate_id,
      fccr.case_type,
      fccr.taxability,
      fccr.finance_component_id,
      fccr.source_family_key,
      fccr.component_key_type,
      fccr.component_key_value,
      fccr.classification,
      fccr.source_pay_method,
      fccr.current_target_pay_method,
      fccr.umb_vat_chargeable,
      fccr.source_basis_json,
      fccr.source_amount,
      fccr.remaining_source_amount,
      fccr.source_units,
      fccr.source_rate,
      fccr.source_charge_rate,
      fccr.source_charge_ex_vat,
      fccr.saved_target_pay_method,
      fccr.saved_resolution_mode,
      fccr.saved_resolution_payload_json,
      fccr.saved_resolution_result_json,
      fccr.resolution_fingerprint,
      fccr.is_resolution_stale,
      fccr.stale_reason,
      fccr.current_component_fingerprint,
      fctx.is_actionable_bucket_resolution as is_actionable_resolution_row,
      fctx.is_fixed_taxable_conversion as is_fixed_no_action_taxable_row,
      null::text as approved_resolution_mode,
      null::numeric as approved_target_rate,
      null::text as approved_nonbucket_resolution_mode,
      null::numeric as approved_nonbucket_target_amount_ex_vat,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then false
        when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then false
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then not (coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false)
        when fctx.is_actionable_bucket_resolution = true and coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then false
        when fctx.is_actionable_bucket_resolution = true then true
        else false
      end as requires_resolution,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then true
        when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then true
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then (coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false)
        when fctx.is_actionable_bucket_resolution = true and coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then true
        when fctx.is_actionable_bucket_resolution = true then false
        else true
      end as case_resolution_satisfied_now_component,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and fccr.source_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method in ('PAYE','UMBRELLA')
        and fccr.current_target_pay_method <> ''
      ) as has_suggested_resolution,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'NO_SUGGESTION_AVAILABLE'
        when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then 'NO_SUGGESTION_AVAILABLE'
        when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then 'REUSABLE_SAVED_RESOLUTION'
        when coalesce(fccr.is_stale_saved_resolution,false) = true then 'STALE_SAVED_RESOLUTION'
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'FRESH_SUGGESTION'
        when fctx.is_actionable_bucket_resolution = true then 'FRESH_SUGGESTION'
        else 'NO_ACTION_FIXED_CONVERSION'
      end as suggestion_provenance,
      (
        fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        and upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
        and coalesce(fccr.is_reusable_saved_resolution,false) = false
        and coalesce(fccr.is_stale_saved_resolution,false) = false
        and (
          fctx.is_actionable_bucket_resolution = true
          or fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT'
        )
      ) as is_fresh_suggested_resolution,
      coalesce(fccr.is_reusable_saved_resolution,false) as is_reusable_saved_resolution,
      coalesce(fccr.is_stale_saved_resolution,false) as is_stale_saved_resolution,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
        when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then fccr.saved_resolution_payload_json
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS',
            'target_pay_method', fccr.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
            'target_units', case when fccr.source_units is not null then round(fccr.source_units,6) else null end,
            'suggested_target_rate', case when fctx.is_actionable_bucket_resolution = true and fbase.suggested_target_rate is not null then round(fbase.suggested_target_rate,2) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
          )
        )
        when fctx.is_actionable_bucket_resolution = true or fctx.is_fixed_taxable_conversion = true or upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,'')) then jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS',
            'target_pay_method', fccr.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
            'target_units', case when fccr.source_units is not null then round(fccr.source_units,6) else null end,
            'suggested_target_rate', case when fctx.is_actionable_bucket_resolution = true and fbase.suggested_target_rate is not null then round(fbase.suggested_target_rate,2) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
          )
        )
        else fccr.suggested_resolution_payload_json
      end as suggested_resolution_payload_json,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then null::jsonb
        when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then fccr.saved_resolution_result_json
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then jsonb_strip_nulls(
          jsonb_build_object(
            'target_pay_method', fccr.current_target_pay_method,
            'target_amount_ex_vat', round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),
            'target_amount_vat', round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'vat','')::numeric, 0),2),
            'target_amount_inc_vat', round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'inc','')::numeric, coalesce(fbase.suggested_target_pay_ex_vat, 0), 0),2),
            'basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'applied_basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
            'target_units', case when fccr.source_units is not null then round(fccr.source_units,6) else null end,
            'replacement_rate', case when fctx.is_actionable_bucket_resolution = true and fbase.suggested_target_rate is not null then round(fbase.suggested_target_rate,2) else null end,
            'target_amount_ex_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_amount_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'vat','')::numeric,0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_amount_inc_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'inc','')::numeric, coalesce(fbase.suggested_target_pay_ex_vat, 0), 0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_units_per_source_ex_vat', case when fccr.source_units is not null and round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(fccr.source_units / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'source_pay_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'source_charge_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat,2) end,
            'source_margin_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat - fbasis.basis_source_amount_ex_vat,2) end,
            'target_pay_ex_vat', round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),
            'target_charge_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat,2) end,
            'target_margin_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat - round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),2) end,
            'margin_delta_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.basis_source_amount_ex_vat - round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),2) end
          )
        )
        when fctx.is_actionable_bucket_resolution = true or fctx.is_fixed_taxable_conversion = true or upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,'')) then jsonb_strip_nulls(
          jsonb_build_object(
            'target_pay_method', fccr.current_target_pay_method,
            'target_amount_ex_vat', round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),
            'target_amount_vat', round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'vat','')::numeric, 0),2),
            'target_amount_inc_vat', round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'inc','')::numeric, coalesce(fbase.suggested_target_pay_ex_vat, 0), 0),2),
            'basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'applied_basis_source_amount_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'relevant_erni_pct', round(v_erni_pct,6),
            'vat_rate_pct', round(v_vat_rate_pct,6),
            'umbrella_vat_chargeable', coalesce(fccr.umb_vat_chargeable,false),
            'target_units', case when fccr.source_units is not null then round(fccr.source_units,6) else null end,
            'replacement_rate', case when fctx.is_actionable_bucket_resolution = true and fbase.suggested_target_rate is not null then round(fbase.suggested_target_rate,2) else null end,
            'target_amount_ex_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_amount_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'vat','')::numeric,0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_amount_inc_vat_per_source_ex_vat', case when round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(round(coalesce(nullif(fbase_amounts.suggested_target_amounts_json->>'inc','')::numeric, coalesce(fbase.suggested_target_pay_ex_vat, 0), 0),2) / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'target_units_per_source_ex_vat', case when fccr.source_units is not null and round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) <> 0 then round(fccr.source_units / round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), 10) else null end,
            'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'source_pay_ex_vat', round(coalesce(fbasis.basis_source_amount_ex_vat,0),2),
            'source_charge_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat,2) end,
            'source_margin_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat - fbasis.basis_source_amount_ex_vat,2) end,
            'target_pay_ex_vat', round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),
            'target_charge_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat,2) end,
            'target_margin_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat - round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),2) end,
            'margin_delta_ex_vat', case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.basis_source_amount_ex_vat - round(coalesce(fbase.suggested_target_pay_ex_vat, 0),2),2) end
          )
        )
        else fccr.suggested_resolution_result_json
      end as suggested_resolution_result_json,
      round(coalesce(fbasis.basis_source_amount_ex_vat,0),2) as source_pay_ex_vat,
      case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat,2) end as source_charge_component_ex_vat,
      case when fbasis.source_charge_basis_ex_vat is null then null else round(fbasis.source_charge_basis_ex_vat - fbasis.basis_source_amount_ex_vat,2) end as source_margin_ex_vat,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
        when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
        when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false and fsaved.reusable_saved_target_pay_ex_vat is not null then round(fsaved.reusable_saved_target_pay_ex_vat,2)
        else round(coalesce(fbase.suggested_target_pay_ex_vat, fbasis.basis_source_amount_ex_vat, 0),2)
      end as target_pay_ex_vat,
      case
        when fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and fbasis.source_charge_basis_ex_vat is not null then round(fbasis.source_charge_basis_ex_vat,2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and fbasis.source_charge_basis_ex_vat is not null
          then round(
            fbasis.source_charge_basis_ex_vat - case
              when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
              when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false and fsaved.reusable_saved_target_pay_ex_vat is not null then round(fsaved.reusable_saved_target_pay_ex_vat,2)
              else round(coalesce(fbase.suggested_target_pay_ex_vat, fbasis.basis_source_amount_ex_vat, 0),2)
            end,
            2
          )
        else null::numeric
      end as target_margin_ex_vat,
      case
        when fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and fbasis.source_charge_basis_ex_vat is not null
          then round(
            fbasis.basis_source_amount_ex_vat - case
              when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
              when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false and fsaved.reusable_saved_target_pay_ex_vat is not null then round(fsaved.reusable_saved_target_pay_ex_vat,2)
              else round(coalesce(fbase.suggested_target_pay_ex_vat, fbasis.basis_source_amount_ex_vat, 0),2)
            end,
            2
          )
        else null::numeric
      end as margin_delta_ex_vat,
      case
        when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'Fixed reimbursements are not channel-converted and do not participate in suggested-rates review.'
        when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then 'No suggested rates are required because this taxable component already aligns with the current target pay method.'
        when coalesce(fccr.is_reusable_saved_resolution,false) = true and coalesce(fccr.is_stale_saved_resolution,false) = false then 'This component already has a reusable saved resolution for the current target pay method.'
        when coalesce(fccr.is_stale_saved_resolution,false) = true then 'A stale saved resolution exists for this component. The suggested rates below reflect the current target pay method.'
        when fccr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'This non-bucket finance case resolves as one suggested/editable gross total. The total remaining source amount is converted onto the current target pay method.'
        when fctx.is_fixed_taxable_conversion = true then 'This taxable row does not expose a per-unit rate edit. It remains visible as a fixed no-action row and is converted deterministically onto the current target pay method.'
        else 'This suggestion converts the taxable component to a target-side equivalent while keeping units fixed, charge fixed, and margin constant except for unavoidable penny balancing.'
      end as suggestion_explanation_text
    from finance_case_component_review_rows fccr
    left join lateral (
      select
        (
          fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          and fccr.source_pay_method in ('PAYE','UMBRELLA')
          and fccr.current_target_pay_method in ('PAYE','UMBRELLA')
          and fccr.current_target_pay_method <> ''
          and upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
          and fccr.source_units is not null
          and coalesce(fccr.source_units,0) <> 0
          and fccr.source_rate is not null
          and fccr.source_charge_rate is not null
          and fccr.component_key_type <> 'ADJUSTMENT_CODE'
          and fccr.case_type not = 'MANUAL_DEBT_ADJUSTMENT'
        ) as is_actionable_bucket_resolution,
        (
          fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          and fccr.source_pay_method in ('PAYE','UMBRELLA')
          and fccr.current_target_pay_method in ('PAYE','UMBRELLA')
          and fccr.current_target_pay_method <> ''
          and upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
          and not (
            fccr.source_units is not null
            and coalesce(fccr.source_units,0) <> 0
            and fccr.source_rate is not null
            and fccr.source_charge_rate is not null
            and fccr.component_key_type <> 'ADJUSTMENT_CODE'
            and fccr.case_type not = 'MANUAL_DEBT_ADJUSTMENT'
          )
        ) as is_fixed_taxable_conversion
    ) fctx on true
    left join lateral (
      select
        round(coalesce(fccr.remaining_source_amount, fccr.source_amount, 0), 2) as basis_source_amount_ex_vat,
        case
          when fccr.source_charge_ex_vat is null then null::numeric
          when round(coalesce(fccr.source_amount,0),2) = 0 then round(coalesce(fccr.source_charge_ex_vat,0),2)
          else round(round(coalesce(fccr.source_charge_ex_vat,0),2) * (round(coalesce(fccr.remaining_source_amount, fccr.source_amount, 0),2) / nullif(round(coalesce(fccr.source_amount,0),2),0)), 2)
        end as source_charge_basis_ex_vat
    ) fbasis on true
    left join lateral (
      select
        case
          when fccr.classification <> 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
          when upper(coalesce(fccr.source_pay_method,'')) is not distinct from upper(coalesce(fccr.current_target_pay_method,'')) then round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
          when fccr.source_pay_method = 'PAYE' and fccr.current_target_pay_method = 'UMBRELLA' then round((public._pay_convert_paye_to_umbrella(round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), v_erni_pct, v_vat_rate_pct, coalesce(fccr.umb_vat_chargeable,false))->>'ex')::numeric,2)
          when fccr.source_pay_method = 'UMBRELLA' and fccr.current_target_pay_method = 'PAYE' then round(public._pay_convert_umbrella_to_paye_ex(round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), v_erni_pct),2)
          when fccr.current_target_pay_method = 'UMBRELLA' then round((public._pay_umbrella_vat_calc(round(coalesce(fbasis.basis_source_amount_ex_vat,0),2), v_vat_rate_pct, coalesce(fccr.umb_vat_chargeable,false))->>'ex')::numeric,2)
          else round(coalesce(fbasis.basis_source_amount_ex_vat,0),2)
        end as target_ex_before_rate
    ) fbase_pre on true
    left join lateral (
      select
        case
          when fctx.is_actionable_bucket_resolution = true and fccr.source_units is not null and fccr.source_units <> 0
            then round(fbase_pre.target_ex_before_rate / fccr.source_units, 2)
          else null::numeric
        end as suggested_target_rate,
        case
          when fctx.is_actionable_bucket_resolution = true and fccr.source_units is not null and fccr.source_units <> 0
            then round(round(fbase_pre.target_ex_before_rate / fccr.source_units, 2) * fccr.source_units, 2)
          else round(fbase_pre.target_ex_before_rate, 2)
        end as suggested_target_pay_ex_vat
    ) fbase on true
    left join lateral (
      select
        case
          when fbase.suggested_target_pay_ex_vat is null then null::jsonb
          when upper(coalesce(fccr.current_target_pay_method,'')) = 'UMBRELLA' then public._pay_umbrella_vat_calc(round(coalesce(fbase.suggested_target_pay_ex_vat,0),2), v_vat_rate_pct, coalesce(fccr.umb_vat_chargeable,false))
          else jsonb_build_object('ex', round(coalesce(fbase.suggested_target_pay_ex_vat,0),2), 'vat', 0, 'inc', round(coalesce(fbase.suggested_target_pay_ex_vat,0),2))
        end as suggested_target_amounts_json
    ) fbase_amounts on true
    left join lateral (
      select
        case
          when coalesce(fccr.is_reusable_saved_resolution,false) = true
           and coalesce(fccr.is_stale_saved_resolution,false) = false
           and coalesce(fccr.saved_resolution_result_json->>'target_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          then round((fccr.saved_resolution_result_json->>'target_amount_ex_vat')::numeric, 2)
          else null::numeric
        end as reusable_saved_target_pay_ex_vat
    ) fsaved on true
  ),
  finance_case_due_source_amounts as (
    select
      vfcr.finance_case_id,
      vfcr.candidate_id,
      vfcr.case_type,
      round(
        greatest(
          case
            when vfcr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(vfcr.payout_status::text,'')) = 'PAID' then coalesce(
              fcpa.protected_recoverable_amount,
              least(coalesce(vfcr.weekly_due,0), coalesce(vfcr.outstanding_amount,0))
              - coalesce(fcrw.repaid_wtd_ex,0)
              - greatest(coalesce(vfcr.active_reserved_amount,0) - coalesce(fcrw.repaid_wtd_ex,0), 0)
            )
            when vfcr.case_type = 'PAYMENT_ADVANCE' then case
              when vfcr.lifecycle_status_display in ('Paid','Cancelled') then 0::numeric
              else coalesce(vfcr.original_amount,0) - coalesce(vfcr.active_reserved_amount,0)
            end
            when vfcr.case_type = 'OVERPAYMENT' then coalesce(
              fcpa.protected_recoverable_amount,
              greatest(coalesce(vfcr.outstanding_amount,0) - coalesce(vfcr.active_reserved_amount,0), 0)
            )
            when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then coalesce(
              fcpa.protected_recoverable_amount,
              least(coalesce(vfcr.weekly_due,0), coalesce(vfcr.outstanding_amount,0))
              - coalesce(fcrw.repaid_wtd_ex,0)
              - greatest(coalesce(vfcr.active_reserved_amount,0) - coalesce(fcrw.repaid_wtd_ex,0), 0)
            )
            when vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then case
              when vfcr.lifecycle_status_display in ('Paid','Cancelled') then 0::numeric
              else coalesce(vfcr.original_amount,0) - coalesce(vfcr.active_reserved_amount,0)
            end
            else 0::numeric
          end,
          0::numeric
        ),
        2
      ) as due_source_amount_ex_vat
    from finance_case_baseline_scope vfcr
    left join finance_case_repaid_wtd fcrw
      on fcrw.finance_case_id = vfcr.finance_case_id
    left join finance_case_protected_allocations fcpa
      on fcpa.finance_case_id = vfcr.finance_case_id
    where vfcr.finance_case_id is not null
  ),
  finance_case_component_due_source_base as (
    select
      fce.finance_case_id,
      fce.candidate_id,
      fce.case_type,
      fce.taxability,
      fce.finance_component_id,
      round(coalesce(fce.remaining_source_amount,0),2) as remaining_source_amount,
      round(coalesce(fcds.due_source_amount_ex_vat,0),2) as due_source_amount_ex_vat,
      round(sum(coalesce(fce.remaining_source_amount,0)) over (partition by fce.finance_case_id),2) as total_remaining_source_amount,
      row_number() over (partition by fce.finance_case_id order by fce.finance_component_id) as component_ord,
      count(*) over (partition by fce.finance_case_id) as component_count
    from finance_case_component_review_rows_effective fce
    join finance_case_due_source_amounts fcds
      on fcds.finance_case_id = fce.finance_case_id
  ),
  finance_case_component_due_source_shares as (
    select
      fcdsb.*,
      round(
        case
          when coalesce(fcdsb.due_source_amount_ex_vat,0) = 0 then 0::numeric
          when coalesce(fcdsb.total_remaining_source_amount,0) = 0 then 0::numeric
          when fcdsb.component_count = 1 then fcdsb.due_source_amount_ex_vat
          when fcdsb.component_ord < fcdsb.component_count then (fcdsb.due_source_amount_ex_vat * fcdsb.remaining_source_amount / nullif(fcdsb.total_remaining_source_amount,0))
          else 0::numeric
        end,
        2
      ) as preliminary_source_due_amount_ex_vat
    from finance_case_component_due_source_base fcdsb
  ),
  finance_case_component_due_source_allocations as (
    select
      fcdss.finance_case_id,
      fcdss.finance_component_id,
      round(
        case
          when fcdss.component_count = 1 then fcdss.due_source_amount_ex_vat
          when fcdss.component_ord < fcdss.component_count then fcdss.preliminary_source_due_amount_ex_vat
          else fcdss.due_source_amount_ex_vat - coalesce(sum(fcdss.preliminary_source_due_amount_ex_vat) over (partition by fcdss.finance_case_id order by fcdss.component_ord rows between unbounded preceding and 1 preceding), 0)
        end,
        2
      ) as allocated_source_due_amount_ex_vat
    from finance_case_component_due_source_shares fcdss
  ),
  finance_case_component_due_preview_base as (
    select
      fce.finance_case_id,
      fce.finance_component_id,
      fcda.allocated_source_due_amount_ex_vat,
      row_number() over (partition by fce.finance_case_id order by fce.finance_component_id) as component_ord,
      count(*) over (partition by fce.finance_case_id) as component_count,
      round(
        case
          when coalesce(fcda.allocated_source_due_amount_ex_vat,0) = 0 then 0::numeric
          when fce.case_type = 'MANUAL_DEBT_ADJUSTMENT' and coalesce(fce.approved_nonbucket_target_amount_ex_vat,0) <> 0 and round(coalesce(fce.remaining_source_amount,0),2) <> 0 then fcda.allocated_source_due_amount_ex_vat * (round(coalesce(fce.target_pay_ex_vat,0),2) / nullif(round(coalesce(fce.remaining_source_amount,0),2),0))
          when fce.case_type = 'OVERPAYMENT' and round(coalesce(fce.remaining_source_amount,0),2) <> 0 then fcda.allocated_source_due_amount_ex_vat * (round(coalesce(fce.target_pay_ex_vat,0),2) / nullif(round(coalesce(fce.remaining_source_amount,0),2),0))
          when round(coalesce(fce.remaining_source_amount,0),2) <> 0 and round(coalesce(fce.target_pay_ex_vat,0),2) <> 0 then fcda.allocated_source_due_amount_ex_vat * (round(coalesce(fce.target_pay_ex_vat,0),2) / nullif(round(coalesce(fce.remaining_source_amount,0),2),0))
          else fcda.allocated_source_due_amount_ex_vat
        end,
        2
      ) as preliminary_preview_due_amount_ex_vat
    from finance_case_component_review_rows_effective fce
    join finance_case_component_due_source_allocations fcda
      on fcda.finance_case_id = fce.finance_case_id
     and fcda.finance_component_id = fce.finance_component_id
  ),
  finance_case_component_due_preview_allocations as (
    select
      fcdpb.finance_case_id,
      fcdpb.finance_component_id,
      round(fcdpb.preliminary_preview_due_amount_ex_vat, 2) as allocated_preview_due_amount_ex_vat
    from finance_case_component_due_preview_base fcdpb
  ),
  finance_case_taxable_manual_debt_resolution as (
    with manual_debt_shape as (
      select
        vfcr.finance_case_id,
        vfcr.candidate_id,
        vfcr.next_due_week_start,
        vfcr.weekly_due,
        vfcr.weeks_total,
        vfcr.outstanding_amount,
        vfcr.schedule_json,
        count(fccr.finance_component_id) filter (
          where fccr.finance_component_id is not null
        )::int as open_component_count,
        count(fccr.finance_component_id) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        )::int as qualifying_component_count,
        (max(fccr.finance_component_id::text) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ))::uuid as finance_component_id,
        max(fccr.source_pay_method) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as source_pay_method,
        max(fccr.current_target_pay_method) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as target_pay_method,
        bool_or(coalesce(fccr.umb_vat_chargeable,false)) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as umb_vat_chargeable,
        max(fccr.source_amount) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as source_amount,
        max(fccr.remaining_source_amount) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as remaining_source_amount,
        bool_or(coalesce(fccr.is_reusable_saved_resolution,false)) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as is_reusable_saved_resolution,
        bool_or(coalesce(fccr.is_stale_saved_resolution,false)) filter (
          where fccr.finance_component_id is not null
            and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            and fccr.component_key_type = 'CASE_TOTAL'
            and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
        ) as is_stale_saved_resolution,
        bool_or(
          fccr.finance_component_id is not null
          and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          and fccr.component_key_type = 'CASE_TOTAL'
          and upper(coalesce(fccr.component_key_value,'')) = 'TOTAL'
          and (
            upper(coalesce(fccr.source_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
            or fccr.is_resolution_stale = true
            or upper(coalesce(fccr.saved_target_pay_method,'')) is distinct from upper(coalesce(fccr.current_target_pay_method,''))
            or (
              fccr.resolution_fingerprint is not null
              and fccr.resolution_fingerprint is distinct from fccr.current_component_fingerprint
            )
          )
        ) as requires_resolution
      from finance_case_baseline_scope vfcr
      left join finance_case_component_review_rows_effective fccr
        on fccr.finance_case_id = vfcr.finance_case_id
      where vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT'
      group by
        vfcr.finance_case_id,
        vfcr.candidate_id,
        vfcr.next_due_week_start,
        vfcr.weekly_due,
        vfcr.weeks_total,
        vfcr.outstanding_amount,
        vfcr.schedule_json
    )
    select
      mds.finance_case_id,
      (
        mds.open_component_count = 1
        and mds.qualifying_component_count = 1
        and mds.finance_component_id is not null
        and mds.source_pay_method in ('PAYE','UMBRELLA')
        and mds.target_pay_method in ('PAYE','UMBRELLA')
        and mds.target_pay_method <> ''
      ) as has_dedicated_resolution_payload,
      (
        mds.open_component_count = 1
        and mds.qualifying_component_count = 1
        and mds.finance_component_id is not null
        and mds.source_pay_method in ('PAYE','UMBRELLA')
        and mds.target_pay_method in ('PAYE','UMBRELLA')
        and mds.target_pay_method <> ''
        and coalesce(mds.requires_resolution,false) = true
      ) as use_dedicated_blocker,
      case
        when (
          mds.open_component_count = 1
          and mds.qualifying_component_count = 1
          and mds.finance_component_id is not null
          and mds.source_pay_method in ('PAYE','UMBRELLA')
          and mds.target_pay_method in ('PAYE','UMBRELLA')
          and mds.target_pay_method <> ''
        )
        then jsonb_strip_nulls(
          jsonb_build_object(
            'resolution_kind', 'TAXABLE_MANUAL_DEBT_CHANNEL_CHANGE',
            'finance_component_id', mds.finance_component_id::text,
            'source_pay_method', mds.source_pay_method,
            'target_pay_method', mds.target_pay_method,
            'remaining_source_amount', round(coalesce(mds.remaining_source_amount,0),2),
            'suggested_target_amount_ex_vat', round(coalesce(nullif(mdca.target_amounts_json->>'ex','')::numeric,0),2),
            'suggested_target_amount_vat', round(coalesce(nullif(mdca.target_amounts_json->>'vat','')::numeric,0),2),
            'suggested_target_amount_inc_vat', round(coalesce(nullif(mdca.target_amounts_json->>'inc','')::numeric,0),2),
            'remaining_weeks', mdwr.remaining_weeks,
            'current_weekly_due', case when mds.weekly_due is null then null else round(mds.weekly_due,2) end,
            'suggested_weekly_due_by_remaining_weeks', case when coalesce(mdwr.remaining_weeks,0) > 0 then round(round(coalesce(nullif(mdca.target_amounts_json->>'ex','')::numeric,0),2) / mdwr.remaining_weeks, 2) else null end,
            'next_due_week_start', case when mds.next_due_week_start is null then null else mds.next_due_week_start::text end,
            'is_reusable_saved_resolution', coalesce(mds.is_reusable_saved_resolution,false),
            'is_stale_saved_resolution', coalesce(mds.is_stale_saved_resolution,false),
            'suggestion_explanation_text', case
              when coalesce(mds.is_reusable_saved_resolution,false) = true and coalesce(mds.requires_resolution,false) = false then 'This taxable manual debt adjustment already has a reusable whole-remaining-balance restructure for the current pay method.'
              when coalesce(mds.is_stale_saved_resolution,false) = true then 'A saved whole-remaining-balance taxable debt restructure exists but is stale for the current pay method. The suggested plan recalculates the remaining outstanding balance onto the current pay method.'
              else 'This taxable manual debt adjustment must be restructured as a whole remaining balance onto the current pay method before it can move to Ready to Pay.'
            end
          )
        )
        else null::jsonb
      end as taxable_manual_debt_resolution_json
    from manual_debt_shape mds
    left join lateral (
      select
        case
          when count(*) > 0 then count(*)::integer
          when coalesce(mds.weekly_due,0) > 0 and coalesce(mds.outstanding_amount,0) > 0 then ceil(mds.outstanding_amount / mds.weekly_due)::integer
          when coalesce(mds.weeks_total,0) > 0 then mds.weeks_total
          else null::integer
        end as remaining_weeks
      from jsonb_array_elements(coalesce(mds.schedule_json,'[]'::jsonb)) mdse(schedule_entry)
      where jsonb_typeof(mdse.schedule_entry) = 'object'
        and nullif(btrim(coalesce(mdse.schedule_entry->>'week_start','')), '') is not null
        and (mds.next_due_week_start is null or (mdse.schedule_entry->>'week_start')::date >= mds.next_due_week_start)
        and coalesce(nullif(mdse.schedule_entry->>'amount','')::numeric,0) < 0
    ) mdwr on true
    left join lateral (
      select
        case
          when mds.source_pay_method = 'PAYE' and mds.target_pay_method = 'UMBRELLA' then public._pay_convert_paye_to_umbrella(round(coalesce(mds.remaining_source_amount,0),2), v_erni_pct, v_vat_rate_pct, coalesce(mds.umb_vat_chargeable,false))
          when mds.source_pay_method = 'UMBRELLA' and mds.target_pay_method = 'PAYE' then jsonb_build_object('ex', public._pay_convert_umbrella_to_paye_ex(round(coalesce(mds.remaining_source_amount,0),2), v_erni_pct), 'vat', 0, 'inc', public._pay_convert_umbrella_to_paye_ex(round(coalesce(mds.remaining_source_amount,0),2), v_erni_pct))
          when mds.target_pay_method = 'PAYE' then jsonb_build_object('ex', round(coalesce(mds.remaining_source_amount,0),2), 'vat', 0, 'inc', round(coalesce(mds.remaining_source_amount,0),2))
          when mds.target_pay_method = 'UMBRELLA' then public._pay_umbrella_vat_calc(round(coalesce(mds.remaining_source_amount,0),2), v_vat_rate_pct, coalesce(mds.umb_vat_chargeable,false))
          else null::jsonb
        end as target_amounts_json
    ) mdca on true
  ),

  finance_case_resolution_rollup as (
    with grouped as (
      select
        vfcr.finance_case_id,
        vfcr.case_type,
        vfcr.advance_kind,
        vfcr.reason,
        vfcr.candidate_id,
        cp.cand_tms_ref,
        cp.cand_display_name,
        cp.cand_pay_method as candidate_pay_method,
        fpr.payee_entity_kind,
        fpr.payee_entity_id,
        vfcr.client_id,
        vfcr.client_name,
        vfcr.linked_timesheet_id,
        vfcr.linked_shift_date,
        vfcr.adjustment_comment,
        vfcr.next_due_week_start,
        vfcr.active_snooze_id,
        vfcr.active_snooze_kind,
        vfcr.active_snooze_until_date,
        vfcr.active_snooze_note,
        vfcr.taxability,
        vfcr.routing_kind,
        vfcr.oneoff_bank_details_present,
        vfcr.oneoff_bank_details_required,
        vfcr.is_candidate_directed_oneoff_payout,
        vfcr.appears_on_umbrella_remittance,
        vfcr.generates_candidate_payment_advice,
        vfcr.snooze_allowed,
        vfcr.lifecycle_status_display,
        fpr.bank_details_hash as payee_bank_hash,
        fpr.beneficiary_name,
        fpr.masked_bank_account,
        case
          when vfcr.case_type in ('OVERPAYMENT','MANUAL_DEBT_ADJUSTMENT') then true
          when vfcr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(vfcr.payout_status::text,'')) = 'PAID' then true
          else false
        end as is_recovery_case,
        case
          when vfcr.case_type in ('OVERPAYMENT','MANUAL_DEBT_ADJUSTMENT') then '[]'::jsonb
          when vfcr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(vfcr.payout_status::text,'')) = 'PAID' then '[]'::jsonb
          else coalesce(fpr.blocked_reason_codes, '[]'::jsonb)
        end as payee_blocked_reason_codes,
        case
          when vfcr.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum then 'one-off specified bank account'
          when vfcr.routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum then 'umbrella company'
          else 'normal PAYE route'
        end as destination_label,
        round(coalesce(max(fcds.due_source_amount_ex_vat),0),2) as due_source_amount_ex_vat,
        round(coalesce(sum(fcdpa.allocated_preview_due_amount_ex_vat), max(fcds.due_source_amount_ex_vat), 0),2) as due_amount_ex_vat,
        case
          when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'NON_BUCKET'::text
          else 'BUCKETED'::text
        end as resolution_family,
        case
          when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'Suggested Gross Total'::text
          else 'Suggested Rate'::text
        end as resolution_action_label,
        coalesce(count(fccr.finance_component_id) filter (
          where fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        ), 0)::int as open_taxable_count,
        coalesce(count(fccr.finance_component_id) filter (
          where fccr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        ), 0)::int as open_reimbursement_count,
        coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true), 0)::int as unresolved_taxable_count,
        coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.is_stale_saved_resolution,false) = true), 0)::int as stale_count,
        (coalesce(count(fccr.finance_component_id) filter (
          where fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        ), 0) > 0
         and
         coalesce(count(fccr.finance_component_id) filter (
          where fccr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        ), 0) > 0) as is_mixed_case,
        (coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true),0) > 0) as case_needs_resolution,
        (coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true),0) = 0) as case_resolution_satisfied_now,
        coalesce(
          (jsonb_agg(
            jsonb_build_object(
              'resolution_kind', 'NON_BUCKET_GROSS_TOTAL',
              'resolution_family', 'NON_BUCKET',
              'resolution_action_label', 'Suggested Gross Total',
              'source_amount_ex_vat', round(coalesce(fccr.remaining_source_amount,0),2),
              'suggested_target_amount_ex_vat', round(coalesce(fccr.target_pay_ex_vat,0),2),
              'approved_target_amount_ex_vat', case when fccr.approved_nonbucket_target_amount_ex_vat is null then null else round(fccr.approved_nonbucket_target_amount_ex_vat,2) end,
              'suggestion_explanation_text', fccr.suggestion_explanation_text
            )
          ) filter (where vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' and fccr.finance_component_id is not null))->0,
          null::jsonb
        ) as non_bucket_resolution_json,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'finance_component_id', fccr.finance_component_id::text,
              'source_family_key', fccr.source_family_key,
              'component_key_type', fccr.component_key_type,
              'component_key_value', fccr.component_key_value,
              'classification', fccr.classification::text,
              'source_pay_method', fccr.source_pay_method,
              'current_target_pay_method', fccr.current_target_pay_method,
              'source_amount', round(coalesce(fccr.source_amount,0),2),
              'remaining_source_amount', round(coalesce(fccr.remaining_source_amount,0),2),
              'source_basis_json', jsonb_strip_nulls(fccr.source_basis_json || jsonb_build_object('source_rate', case when fccr.source_rate is null then null else round(fccr.source_rate,2) end, 'source_charge_rate', case when fccr.source_charge_rate is null then null else round(fccr.source_charge_rate,2) end)),
              'saved_target_pay_method', fccr.saved_target_pay_method,
              'saved_resolution_mode', case when fccr.saved_resolution_mode is null then null else fccr.saved_resolution_mode::text end,
              'saved_resolution_payload_json', fccr.saved_resolution_payload_json,
              'saved_resolution_result_json', fccr.saved_resolution_result_json,
              'has_suggested_resolution', fccr.has_suggested_resolution,
              'suggestion_provenance', fccr.suggestion_provenance,
              'is_fresh_suggested_resolution', fccr.is_fresh_suggested_resolution,
              'is_reusable_saved_resolution', fccr.is_reusable_saved_resolution,
              'is_stale_saved_resolution', fccr.is_stale_saved_resolution,
              'suggested_resolution_payload_json', fccr.suggested_resolution_payload_json,
              'suggested_resolution_result_json', fccr.suggested_resolution_result_json,
              'source_units', fccr.source_units,
              'target_units', case when nullif(fccr.suggested_resolution_result_json->>'target_units','') is not null then (fccr.suggested_resolution_result_json->>'target_units')::numeric else fccr.source_units end,
              'source_rate', case when fccr.source_rate is null then null else round(fccr.source_rate,2) end,
              'target_rate', case when nullif(fccr.suggested_resolution_result_json->>'replacement_rate','') is not null then round((fccr.suggested_resolution_result_json->>'replacement_rate')::numeric,2) when nullif(fccr.suggested_resolution_payload_json->>'suggested_target_rate','') is not null then round((fccr.suggested_resolution_payload_json->>'suggested_target_rate')::numeric,2) else null end,
              'source_pay_ex_vat', round(coalesce(fccr.source_pay_ex_vat,0),2),
              'source_charge_ex_vat', fccr.source_charge_component_ex_vat,
              'source_margin_ex_vat', fccr.source_margin_ex_vat,
              'target_pay_ex_vat', round(coalesce(fccr.target_pay_ex_vat,0),2),
              'target_charge_ex_vat', fccr.target_charge_ex_vat,
              'target_margin_ex_vat', fccr.target_margin_ex_vat,
              'margin_delta_ex_vat', fccr.margin_delta_ex_vat,
              'suggestion_explanation_text', fccr.suggestion_explanation_text,
              'component_fingerprint', fccr.current_component_fingerprint,
              'is_resolution_stale', coalesce(fccr.is_stale_saved_resolution,false),
              'stale_reason', fccr.stale_reason,
              'requires_resolution', coalesce(fccr.requires_resolution,false),
              'resolution_state', case
                when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' and coalesce(fccr.requires_resolution,false) = true then 'REQUIRED'
                when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'RESOLVED'
                else case when fccr.classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum then 'FIXED' when coalesce(fccr.requires_resolution,false) = true then 'REQUIRED' else 'RESOLVED' end
              end,
              'is_actionable_resolution_row', coalesce(fccr.is_actionable_resolution_row,false),
              'is_fixed_no_action_taxable_row', coalesce(fccr.is_fixed_no_action_taxable_row,false),
              'allocated_source_due_amount_ex_vat', round(coalesce(fcda.allocated_source_due_amount_ex_vat,0),2),
              'preview_due_amount_ex_vat', round(coalesce(fcdpa.allocated_preview_due_amount_ex_vat, coalesce(fcda.allocated_source_due_amount_ex_vat,0)),2)
            )
            order by fccr.classification::text, fccr.component_key_type, fccr.component_key_value
          ) filter (where fccr.finance_component_id is not null),
          '[]'::jsonb
        ) as case_components_json
      from finance_case_baseline_scope vfcr
      join cand_payee cp
        on cp.candidate_id = vfcr.candidate_id
      left join finance_case_component_review_rows_effective fccr
        on fccr.finance_case_id = vfcr.finance_case_id
      left join finance_case_payee_readiness fpr
        on fpr.finance_case_id = vfcr.finance_case_id
      left join finance_case_due_source_amounts fcds
        on fcds.finance_case_id = vfcr.finance_case_id
      left join finance_case_component_due_source_allocations fcda
        on fcda.finance_case_id = fccr.finance_case_id
       and fcda.finance_component_id = fccr.finance_component_id
      left join finance_case_component_due_preview_allocations fcdpa
        on fcdpa.finance_case_id = fccr.finance_case_id
       and fcdpa.finance_component_id = fccr.finance_component_id
      where vfcr.finance_case_id is not null
      group by
        vfcr.finance_case_id,
        vfcr.case_type,
        vfcr.advance_kind,
        vfcr.reason,
        vfcr.candidate_id,
        cp.cand_tms_ref,
        cp.cand_display_name,
        cp.cand_pay_method,
        fpr.payee_entity_kind,
        fpr.payee_entity_id,
        fpr.bank_details_hash,
        fpr.beneficiary_name,
        fpr.masked_bank_account,
        fpr.blocked_reason_codes,
        vfcr.client_id,
        vfcr.client_name,
        vfcr.linked_timesheet_id,
        vfcr.linked_shift_date,
        vfcr.adjustment_comment,
        vfcr.next_due_week_start,
        vfcr.active_snooze_id,
        vfcr.active_snooze_kind,
        vfcr.active_snooze_until_date,
        vfcr.active_snooze_note,
        vfcr.taxability,
        vfcr.routing_kind,
        vfcr.oneoff_bank_details_present,
        vfcr.oneoff_bank_details_required,
        vfcr.is_candidate_directed_oneoff_payout,
        vfcr.appears_on_umbrella_remittance,
        vfcr.generates_candidate_payment_advice,
        vfcr.snooze_allowed,
        vfcr.lifecycle_status_display,
        vfcr.payout_status
    )
    select
      g.finance_case_id,
      g.case_type,
      g.advance_kind,
      g.reason,
      g.candidate_id,
      g.cand_tms_ref,
      g.cand_display_name,
      g.candidate_pay_method,
      g.payee_entity_kind,
      g.payee_entity_id,
      (jsonb_array_length(coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)) = 0) as candidate_ready_for_draft,
      g.client_id,
      g.client_name,
      g.linked_timesheet_id,
      g.linked_shift_date,
      g.adjustment_comment,
      g.next_due_week_start,
      g.active_snooze_id,
      g.active_snooze_kind,
      g.active_snooze_until_date,
      g.active_snooze_note,
      g.taxability,
      g.routing_kind,
      g.oneoff_bank_details_present,
      g.oneoff_bank_details_required,
      g.is_candidate_directed_oneoff_payout,
      g.appears_on_umbrella_remittance,
      g.generates_candidate_payment_advice,
      g.snooze_allowed,
      g.lifecycle_status_display,
      g.payee_bank_hash,
      g.beneficiary_name,
      g.masked_bank_account,
      g.destination_label,
      g.due_amount_ex_vat,
      g.open_taxable_count,
      g.open_reimbursement_count,
      g.unresolved_taxable_count,
      g.stale_count,
      g.is_mixed_case,
      g.resolution_family,
      g.case_needs_resolution,
      g.case_resolution_satisfied_now,
      g.resolution_action_label,
      null::jsonb as linked_resolution_scope_json,
      (
        g.case_needs_resolution = true
        or jsonb_array_length(coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)) > 0
      ) as is_blocked,
      (
        coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)
        ||
        (case
          when g.resolution_family = 'NON_BUCKET' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_NON_BUCKET_RESOLUTION')
          when g.resolution_family = 'BUCKETED' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_TAXABLE_RESOLUTION')
          else '[]'::jsonb
        end)
      ) as blocked_reason_codes,
      jsonb_strip_nulls(
        jsonb_build_object(
          'case_key', ('finance:' || g.finance_case_id::text),
          'case_type', g.case_type::text,
          'resolution_family', g.resolution_family,
          'case_needs_resolution', g.case_needs_resolution,
          'case_resolution_satisfied_now', g.case_resolution_satisfied_now,
          'resolution_action_label', g.resolution_action_label,
          'linked_resolution_scope_json', null,
          'taxability', case when g.taxability is null then null else g.taxability::text end,
          'routing_kind', case when g.routing_kind is null then null else g.routing_kind::text end,
          'destination_label', g.destination_label,
          'is_mixed_case', g.is_mixed_case,
          'open_taxable_count', g.open_taxable_count,
          'open_reimbursement_count', g.open_reimbursement_count,
          'unresolved_taxable_count', g.unresolved_taxable_count,
          'stale_count', g.stale_count,
          'is_blocked', (
            g.case_needs_resolution = true
            or jsonb_array_length(coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)) > 0
          ),
          'due_amount_ex_vat', g.due_amount_ex_vat,
          'blocked_reason_codes', (
            coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)
            ||
            (case
              when g.resolution_family = 'NON_BUCKET' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_NON_BUCKET_RESOLUTION')
              when g.resolution_family = 'BUCKETED' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_TAXABLE_RESOLUTION')
              else '[]'::jsonb
            end)
          ),
          'non_bucket_resolution', g.non_bucket_resolution_json,
          'taxable_manual_debt_resolution', case
            when g.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
             and g.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
             and coalesce(fctmdr.has_dedicated_resolution_payload, false) = true
            then fctmdr.taxable_manual_debt_resolution_json
            else null::jsonb
          end
        )
      ) as case_resolution_summary_json,
      case
        when g.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
         and g.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
         and coalesce(fctmdr.has_dedicated_resolution_payload, false) = true
        then fctmdr.taxable_manual_debt_resolution_json
        else null::jsonb
      end as taxable_manual_debt_resolution_json,
      g.case_components_json
    from grouped g
    left join finance_case_taxable_manual_debt_resolution fctmdr
      on fctmdr.finance_case_id = g.finance_case_id
  ),
  canonical_timesheet_lines as (
    select
      tcr.candidate_id,
      tcr.timesheet_id,
      tb.ts_booking_id as booking_id,
      tb.ts_role,
      tb.ts_band,
      tcr.client_id,
      tcr.ts_client_name as client_name,
      tcr.ts_week_ending_date as week_ending_date,
      tcr.ts_pay_method as source_pay_method,
      tcr.umb_vat_chargeable,
      cp.cand_pay_method as candidate_pay_method,
      cp.cand_tms_ref,
      cp.cand_display_name,
      cp.payee_entity_kind,
      cp.payee_entity_id,
      (cp.is_ready_for_draft and coalesce(tcr.is_blocked, false) = false) as is_ready_for_draft,
      ato.override_id,
      ato.override_reason,
      ats.snooze_id,
      ats.snooze_until_date,
      ats.note as snooze_note,
      round(coalesce(tcr.payment_amount_ex_vat,0),2) as amount_ex_vat,
      round(coalesce(tcr.payment_amount_inc_vat, tcr.payment_amount, tcr.payment_amount_ex_vat, 0),2) as amount_display,
      coalesce(tcr.is_blocked, false) as case_is_blocked,
      coalesce(tcr.case_resolution_summary_json, '{}'::jsonb) as case_resolution_summary_json,
      coalesce(tcr.case_components_json, '[]'::jsonb) as case_components_json,
      coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'segment_id', nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
            'segment_key', nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), ''),
            'segment_stable_key', coalesce(
              nullif(btrim(coalesce(delta_seg.seg->>'segment_stable_key','')), ''),
              nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')), ''),
              nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
              nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), ''),
              nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''),
              nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), '')
            ),
            'date', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''), nullif(btrim(coalesce(delta_seg.seg->>'work_date','')), '')),
            'client_name', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'client_name','')), ''), tcr.ts_client_name),
            'role', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'role','')), ''), tb.ts_role),
            'band', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'band','')), ''), tb.ts_band),
            'start', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'start','')), ''), nullif(btrim(coalesce(cur_seg.seg->>'start_hhmm','')), '')),
            'finish', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'end','')), ''), nullif(btrim(coalesce(cur_seg.seg->>'end_hhmm','')), '')),
            'start_utc', nullif(btrim(coalesce(cur_seg.seg->>'start_utc','')), ''),
            'end_utc', nullif(btrim(coalesce(cur_seg.seg->>'end_utc','')), ''),
            'break_start', nullif(btrim(coalesce(cur_seg.seg->>'break_start','')), ''),
            'break_end', nullif(btrim(coalesce(cur_seg.seg->>'break_end','')), ''),
            'break_mins', coalesce(nullif(cur_seg.seg->>'break_mins','')::numeric, nullif(cur_seg.seg->>'break_minutes','')::numeric),
            'breaks', coalesce(cur_seg.seg->'breaks', '[]'::jsonb),
            'ref_num', coalesce(nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), ''), nullif(btrim(coalesce(delta_seg.seg->>'ref_num','')), '')),
            'pay_amount_ex_vat', round(coalesce(nullif(delta_seg.seg->>'delta_pay_ex_vat','')::numeric,0),2),
            'snooze_identity', jsonb_build_object(
              'identity_type', 'TIMESHEET_SEGMENT',
              'timesheet_id', tcr.timesheet_id::text,
              'booking_id', tb.ts_booking_id,
              'segment_id', nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
              'segment_stable_key', coalesce(
                nullif(btrim(coalesce(delta_seg.seg->>'segment_stable_key','')), ''),
                nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')), ''),
                nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
                nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), ''),
                nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''),
                nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), '')
              ),
              'source_ref', null
            ),
            'snooze_state', case
              when ass.snooze_id is null then jsonb_build_object('state','NONE')
              when ass.snooze_until_date is null then jsonb_build_object(
                'state', 'INDEFINITE_SNOOZED',
                'snooze_id', ass.snooze_id::text,
                'snooze_until_date', null,
                'note', ass.note,
                'snooze_kind', ass.snooze_kind
              )
              else jsonb_build_object(
                'state', 'DATED_SNOOZED',
                'snooze_id', ass.snooze_id::text,
                'snooze_until_date', ass.snooze_until_date::text,
                'note', ass.note,
                'snooze_kind', ass.snooze_kind
              )
            end
          )
          order by
            coalesce(nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''), nullif(btrim(coalesce(delta_seg.seg->>'work_date','')), '')) nulls last,
            coalesce(nullif(btrim(coalesce(cur_seg.seg->>'start','')), ''), nullif(btrim(coalesce(cur_seg.seg->>'start_hhmm','')), '')) nulls last,
            coalesce(nullif(btrim(coalesce(delta_seg.seg->>'segment_stable_key','')), ''), nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')), '')) nulls last,
            nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), '') nulls last
        )
        from jsonb_array_elements(coalesce(tcr.segment_deltas_json, '[]'::jsonb)) as delta_seg(seg)
        left join lateral (
          select seg.value as seg
          from jsonb_array_elements(coalesce(tb.current_segments_json, '[]'::jsonb)) as seg(value)
          where coalesce(
                  nullif(btrim(coalesce(seg.value->>'segment_stable_key','')), ''),
                  nullif(btrim(coalesce(seg.value->>'segment_id','')), ''),
                  nullif(btrim(coalesce(seg.value->>'segment_key','')), ''),
                  nullif(btrim(coalesce(seg.value->>'date','')), ''),
                  nullif(btrim(coalesce(seg.value->>'ref_num','')), '')
                ) is not distinct from coalesce(
                  nullif(btrim(coalesce(delta_seg.seg->>'segment_stable_key','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'segment_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'segment_key','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'work_date','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'ref_num','')), '')
                )
          order by 1
          limit 1
        ) cur_seg on true
        left join active_segment_snoozes ass
          on ass.candidate_id = tcr.candidate_id
         and (
           (ass.booking_id is not null and ass.booking_id = tb.ts_booking_id and ass.segment_stable_key is not distinct from coalesce(
             nullif(btrim(coalesce(delta_seg.seg->>'segment_stable_key','')), ''),
             nullif(btrim(coalesce(cur_seg.seg->>'segment_stable_key','')), ''),
             nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''),
             nullif(btrim(coalesce(cur_seg.seg->>'segment_key','')), ''),
             nullif(btrim(coalesce(cur_seg.seg->>'date','')), ''),
             nullif(btrim(coalesce(cur_seg.seg->>'ref_num','')), '')
           ))
           or (ass.booking_id is null and ass.timesheet_id = tcr.timesheet_id and ass.segment_id is not distinct from nullif(btrim(coalesce(cur_seg.seg->>'segment_id','')), ''))
         )
      ), '[]'::jsonb) as segment_rows_json
    from timesheet_case_rollup_effective tcr
    join cand_payee cp
      on cp.candidate_id = tcr.candidate_id
    join ts_baseline tb
      on tb.timesheet_id = tcr.timesheet_id
     and tb.candidate_id = tcr.candidate_id
    left join active_timesheet_payment_overrides ato
      on ato.timesheet_id = tcr.timesheet_id
     and ato.candidate_id = tcr.candidate_id
    left join active_timesheet_payment_snoozes ats
      on ats.candidate_id = tcr.candidate_id
     and (
       (ats.booking_id is not null and ats.booking_id = tb.ts_booking_id)
       or (ats.booking_id is null and ats.timesheet_id = tcr.timesheet_id)
     )
    where round(coalesce(tcr.payment_amount_ex_vat,0),2) <> 0
      and not (ats.snooze_id is not null and ats.snooze_until_date is null)
  ),
  timesheet_active_segment_snooze_meta as (
    select
      ctl.candidate_id,
      ctl.timesheet_id,
      ctl.booking_id,
      count(*)::int as active_segment_snooze_count,
      count(*) filter (where ass.snooze_until_date is not null)::int as active_segment_dated_snooze_count,
      count(*) filter (where ass.snooze_until_date is null)::int as active_segment_indefinite_snooze_count
    from canonical_timesheet_lines ctl
    join active_segment_snoozes ass
      on ass.candidate_id = ctl.candidate_id
     and (
       (ass.booking_id is not null and ctl.booking_id is not null and ass.booking_id = ctl.booking_id)
       or (ass.booking_id is null and ass.timesheet_id = ctl.timesheet_id)
     )
    group by ctl.candidate_id, ctl.timesheet_id, ctl.booking_id
  ),
  canonical_timesheet_segment_rows as (
    select
      ctl.candidate_id,
      ctl.timesheet_id,
      ctl.booking_id,
      cur_seg_norm.seg_ord,
      cur_seg_norm.segment_id,
      cur_seg_norm.segment_key,
      cur_seg_norm.segment_stable_key,
      cur_seg_norm.segment_date,
      cur_seg_norm.client_name,
      cur_seg_norm.role,
      cur_seg_norm.band,
      cur_seg_norm.start_hhmm,
      cur_seg_norm.finish_hhmm,
      cur_seg_norm.start_utc,
      cur_seg_norm.end_utc,
      cur_seg_norm.break_start,
      cur_seg_norm.break_end,
      cur_seg_norm.break_mins,
      cur_seg_norm.breaks,
      coalesce(cur_seg_norm.ref_num, ss_match.ref_num) as ref_num,
      round(
        case
          when ass_match.snooze_id is not null then coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, ss_match.eff_delta_ex, 0)
          when coalesce(ss_match.is_blocked, false) = true or coalesce(ss_match.is_do_not_pay, false) = true then coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, 0)
          else coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0)
        end,
        2
      ) as presentation_amount_ex_vat,
      round(coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, 0), 2) as raw_delta_ex_vat,
      round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) as effective_delta_ex_vat,
      coalesce(ss_match.is_blocked, false) as status_is_blocked,
      coalesce(ss_match.is_do_not_pay, false) as status_is_do_not_pay,
      ass_match.snooze_id as segment_snooze_id,
      ass_match.snooze_until_date as segment_snooze_until_date,
      ass_match.note as segment_snooze_note,
      ass_match.snooze_kind as segment_snooze_kind,
      case
        when ass_match.snooze_id is not null and ass_match.snooze_until_date is null then 'HIDDEN_INDEFINITE'
        when ass_match.snooze_id is not null then 'BLOCKED_VISIBLE'
        when coalesce(ss_match.is_blocked, false) = true or coalesce(ss_match.is_do_not_pay, false) = true then 'BLOCKED_VISIBLE'
        when round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0 then 'READY'
        else 'IGNORED'
      end as presentation_segment_state,
      jsonb_build_object(
        'timesheet_id', ctl.timesheet_id::text,
        'booking_id', ctl.booking_id,
        'segment_id', cur_seg_norm.segment_id,
        'segment_key', cur_seg_norm.segment_key,
        'segment_stable_key', cur_seg_norm.segment_stable_key,
        'date', cur_seg_norm.segment_date,
        'client_name', cur_seg_norm.client_name,
        'role', cur_seg_norm.role,
        'band', cur_seg_norm.band,
        'start', cur_seg_norm.start_hhmm,
        'finish', cur_seg_norm.finish_hhmm,
        'start_utc', cur_seg_norm.start_utc,
        'end_utc', cur_seg_norm.end_utc,
        'break_start', cur_seg_norm.break_start,
        'break_end', cur_seg_norm.break_end,
        'break_mins', cur_seg_norm.break_mins,
        'breaks', cur_seg_norm.breaks,
        'ref_num', coalesce(cur_seg_norm.ref_num, ss_match.ref_num),
        'pay_amount_ex_vat', round(
          case
            when ass_match.snooze_id is not null then coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, ss_match.eff_delta_ex, 0)
            when coalesce(ss_match.is_blocked, false) = true or coalesce(ss_match.is_do_not_pay, false) = true then coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, 0)
            else coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0)
          end,
          2
        ),
        'raw_delta_ex_vat', round(coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, 0), 2),
        'effective_delta_ex_vat', round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2),
        'is_blocked', coalesce(ss_match.is_blocked, false),
        'is_do_not_pay', coalesce(ss_match.is_do_not_pay, false),
        'presentation_segment_state', case
          when ass_match.snooze_id is not null and ass_match.snooze_until_date is null then 'HIDDEN_INDEFINITE'
          when ass_match.snooze_id is not null then 'BLOCKED_VISIBLE'
          when coalesce(ss_match.is_blocked, false) = true or coalesce(ss_match.is_do_not_pay, false) = true then 'BLOCKED_VISIBLE'
          when round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0 then 'READY'
          else 'IGNORED'
        end,
        'is_ready_segment', (
          ass_match.snooze_id is null
          and coalesce(ss_match.is_blocked, false) = false
          and coalesce(ss_match.is_do_not_pay, false) = false
          and round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0
        ),
        'is_blocked_visible_segment', (
          (ass_match.snooze_id is not null and ass_match.snooze_until_date is not null)
          or coalesce(ss_match.is_blocked, false) = true
          or coalesce(ss_match.is_do_not_pay, false) = true
        ),
        'is_hidden_indefinite_segment', (ass_match.snooze_id is not null and ass_match.snooze_until_date is null),
        'snooze_identity', jsonb_build_object(
          'identity_type', 'TIMESHEET_SEGMENT',
          'timesheet_id', ctl.timesheet_id::text,
          'booking_id', ctl.booking_id,
          'segment_id', cur_seg_norm.segment_id,
          'segment_stable_key', cur_seg_norm.segment_stable_key,
          'source_ref', null
        ),
        'snooze_state', case
          when ass_match.snooze_id is null then jsonb_build_object('state', 'NONE')
          when ass_match.snooze_until_date is null then jsonb_build_object(
            'state', 'INDEFINITE_SNOOZED',
            'snooze_id', ass_match.snooze_id::text,
            'snooze_until_date', null,
            'note', ass_match.note,
            'snooze_kind', ass_match.snooze_kind
          )
          else jsonb_build_object(
            'state', 'DATED_SNOOZED',
            'snooze_id', ass_match.snooze_id::text,
            'snooze_until_date', ass_match.snooze_until_date::text,
            'note', ass_match.note,
            'snooze_kind', ass_match.snooze_kind
          )
        end
      ) as segment_base_json
    from canonical_timesheet_lines ctl
    join ts_baseline tb
      on tb.timesheet_id = ctl.timesheet_id
     and tb.candidate_id = ctl.candidate_id
    cross join lateral jsonb_array_elements(coalesce(tb.current_segments_json, '[]'::jsonb)) with ordinality as cur_seg(seg_json, seg_ord)
    cross join lateral (
      select
        cur_seg.seg_ord,
        nullif(btrim(coalesce(cur_seg.seg_json->>'segment_id','')), '') as segment_id,
        nullif(btrim(coalesce(cur_seg.seg_json->>'segment_key','')), '') as segment_key,
        coalesce(
          nullif(btrim(coalesce(cur_seg.seg_json->>'segment_stable_key','')), ''),
          nullif(btrim(coalesce(cur_seg.seg_json->>'segment_id','')), ''),
          nullif(btrim(coalesce(cur_seg.seg_json->>'segment_key','')), ''),
          nullif(btrim(coalesce(cur_seg.seg_json->>'date','')), ''),
          nullif(btrim(coalesce(cur_seg.seg_json->>'ref_num','')), '')
        ) as segment_stable_key,
        nullif(btrim(coalesce(cur_seg.seg_json->>'date','')), '') as segment_date,
        coalesce(nullif(btrim(coalesce(cur_seg.seg_json->>'client_name','')), ''), ctl.client_name) as client_name,
        coalesce(nullif(btrim(coalesce(cur_seg.seg_json->>'role','')), ''), ctl.ts_role) as role,
        coalesce(nullif(btrim(coalesce(cur_seg.seg_json->>'band','')), ''), ctl.ts_band) as band,
        coalesce(nullif(btrim(coalesce(cur_seg.seg_json->>'start','')), ''), nullif(btrim(coalesce(cur_seg.seg_json->>'start_hhmm','')), '')) as start_hhmm,
        coalesce(nullif(btrim(coalesce(cur_seg.seg_json->>'end','')), ''), nullif(btrim(coalesce(cur_seg.seg_json->>'end_hhmm','')), '')) as finish_hhmm,
        nullif(btrim(coalesce(cur_seg.seg_json->>'start_utc','')), '') as start_utc,
        nullif(btrim(coalesce(cur_seg.seg_json->>'end_utc','')), '') as end_utc,
        nullif(btrim(coalesce(cur_seg.seg_json->>'break_start','')), '') as break_start,
        nullif(btrim(coalesce(cur_seg.seg_json->>'break_end','')), '') as break_end,
        coalesce(nullif(cur_seg.seg_json->>'break_mins','')::numeric, nullif(cur_seg.seg_json->>'break_minutes','')::numeric) as break_mins,
        case when jsonb_typeof(cur_seg.seg_json->'breaks') = 'array' then cur_seg.seg_json->'breaks' else '[]'::jsonb end as breaks,
        nullif(btrim(coalesce(cur_seg.seg_json->>'ref_num','')), '') as ref_num
    ) cur_seg_norm
    left join lateral (
      select
        ss.segment_id,
        ss.segment_stable_key,
        ss.ref_num,
        ss.work_date,
        ss.delta_pay_ex_vat,
        ss.eff_delta_ex,
        ss.is_blocked,
        ss.is_do_not_pay
      from segment_status ss
      where ss.candidate_id = ctl.candidate_id
        and ss.timesheet_id = ctl.timesheet_id
        and (
          (cur_seg_norm.segment_stable_key is not null and ss.segment_stable_key is not distinct from cur_seg_norm.segment_stable_key)
          or (
            cur_seg_norm.segment_stable_key is null
            and cur_seg_norm.segment_id is not null
            and ss.segment_id is not distinct from cur_seg_norm.segment_id
          )
        )
      order by
        case
          when cur_seg_norm.segment_stable_key is not null
           and ss.segment_stable_key is not distinct from cur_seg_norm.segment_stable_key
          then 0 else 1
        end,
        case
          when cur_seg_norm.segment_id is not null
           and ss.segment_id is not distinct from cur_seg_norm.segment_id
          then 0 else 1
        end,
        ss.segment_stable_key nulls last,
        ss.segment_id nulls last
      limit 1
    ) ss_match on true
    left join lateral (
      select
        round(sum(coalesce(ttre.preview_component_amount_ex_vat, 0)), 2) as preview_component_amount_ex_vat,
        round(sum(coalesce(ttre.ready_preview_amount_ex_vat, ttre.preview_component_amount_ex_vat, 0)), 2) as ready_preview_amount_ex_vat
      from transient_timesheet_component_review_rows_effective ttre
      where ttre.candidate_id = ctl.candidate_id
        and ttre.timesheet_id = ctl.timesheet_id
        and ttre.component_key_type in ('TS_DAY','TS_TOTAL')
        and (
          (
            cur_seg_norm.segment_stable_key is not null
            and coalesce(
              nullif(btrim(coalesce(ttre.source_basis_json->>'segment_stable_key','')), ''),
              nullif(btrim(coalesce(ttre.source_basis_json->>'segment_id','')), ''),
              nullif(btrim(coalesce(ttre.source_basis_json->>'segment_key','')), ''),
              nullif(btrim(coalesce(ttre.source_basis_json->>'work_date','')), ''),
              nullif(btrim(coalesce(ttre.source_basis_json->>'ref_num','')), '')
            ) is not distinct from cur_seg_norm.segment_stable_key
          )
          or (
            cur_seg_norm.segment_stable_key is null
            and cur_seg_norm.segment_id is not null
            and nullif(btrim(coalesce(ttre.source_basis_json->>'segment_id','')), '') is not distinct from cur_seg_norm.segment_id
          )
        )
    ) ttre_match on true
    left join lateral (
      select
        ass.snooze_id,
        ass.snooze_until_date,
        ass.note,
        ass.snooze_kind,
        ass.segment_id,
        ass.segment_stable_key
      from active_segment_snoozes ass
      where ass.candidate_id = ctl.candidate_id
        and (
          (
            ass.booking_id is not null
            and ctl.booking_id is not null
            and ass.booking_id = ctl.booking_id
            and (
              (cur_seg_norm.segment_stable_key is not null and ass.segment_stable_key is not distinct from cur_seg_norm.segment_stable_key)
              or (
                cur_seg_norm.segment_stable_key is null
                and cur_seg_norm.segment_id is not null
                and ass.segment_id is not distinct from cur_seg_norm.segment_id
              )
            )
          )
          or (
            ass.booking_id is null
            and ass.timesheet_id = ctl.timesheet_id
            and (
              (cur_seg_norm.segment_stable_key is not null and ass.segment_stable_key is not distinct from cur_seg_norm.segment_stable_key)
              or (
                cur_seg_norm.segment_stable_key is null
                and cur_seg_norm.segment_id is not null
                and ass.segment_id is not distinct from cur_seg_norm.segment_id
              )
            )
          )
        )
      order by
        case when ass.segment_stable_key is not null then 0 else 1 end,
        ass.snooze_id
      limit 1
    ) ass_match on true
    where (
      ass_match.snooze_id is not null
      or coalesce(ss_match.is_blocked, false) = true
      or coalesce(ss_match.is_do_not_pay, false) = true
      or round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0
      or round(coalesce(ttre_match.preview_component_amount_ex_vat, ss_match.delta_pay_ex_vat, 0), 2) <> 0
    )
  ),
  canonical_timesheet_segment_rollup as (
    select
      ctl.candidate_id,
      ctl.timesheet_id,
      ctl.booking_id,
      coalesce(tasm.active_segment_snooze_count, 0) as active_segment_snooze_count,
      coalesce(tasm.active_segment_dated_snooze_count, 0) as active_segment_dated_snooze_count,
      coalesce(tasm.active_segment_indefinite_snooze_count, 0) as active_segment_indefinite_snooze_count,
      count(*) filter (where ctsr.presentation_segment_state in ('READY', 'BLOCKED_VISIBLE', 'HIDDEN_INDEFINITE'))::int as total_segment_count,
      count(*) filter (where ctsr.presentation_segment_state = 'READY')::int as ready_segment_count,
      count(*) filter (where ctsr.presentation_segment_state = 'BLOCKED_VISIBLE')::int as blocked_visible_segment_count,
      count(*) filter (where ctsr.presentation_segment_state = 'HIDDEN_INDEFINITE')::int as hidden_indefinite_segment_count,
      round(coalesce(sum(case when ctsr.presentation_segment_state = 'READY' then ctsr.presentation_amount_ex_vat else 0 end), 0), 2) as ready_segment_amount_ex_vat,
      round(coalesce(sum(case when ctsr.presentation_segment_state = 'BLOCKED_VISIBLE' then ctsr.presentation_amount_ex_vat else 0 end), 0), 2) as blocked_visible_segment_amount_ex_vat,
      round(coalesce(sum(case when ctsr.presentation_segment_state = 'HIDDEN_INDEFINITE' then ctsr.presentation_amount_ex_vat else 0 end), 0), 2) as hidden_indefinite_segment_amount_ex_vat,
      coalesce(
        jsonb_agg(
          ctsr.segment_base_json || jsonb_build_object(
            'presentation_section', 'READY_TO_PAY',
            'presentation_role', 'CHILD',
            'presentation_parent_line_id', ctl.timesheet_id::text,
            'has_active_timesheet_snooze', (ctl.snooze_id is not null),
            'has_active_segment_snoozes', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'active_segment_snooze_count', coalesce(tasm.active_segment_snooze_count, 0),
            'active_segment_dated_snooze_count', coalesce(tasm.active_segment_dated_snooze_count, 0),
            'active_segment_indefinite_snooze_count', coalesce(tasm.active_segment_indefinite_snooze_count, 0),
            'whole_timesheet_snooze_action_blocked', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'whole_timesheet_snooze_action_block_reason', case when coalesce(tasm.active_segment_snooze_count, 0) > 0 then 'ACTIVE_SEGMENT_SNOOZES_EXIST' else null end,
            'segment_snooze_action_blocked', (ctl.snooze_id is not null),
            'segment_snooze_action_block_reason', case when ctl.snooze_id is not null then 'WHOLE_TIMESHEET_SNOOZE_ACTIVE' else null end
          )
          order by ctsr.seg_ord
        ) filter (where ctsr.presentation_segment_state = 'READY'),
        '[]'::jsonb
      ) as ready_segment_rows_json,
      coalesce(
        jsonb_agg(
          ctsr.segment_base_json || jsonb_build_object(
            'presentation_section', 'BLOCKED_FOR_PAY',
            'presentation_role', 'CHILD',
            'presentation_parent_line_id', ctl.timesheet_id::text,
            'has_active_timesheet_snooze', (ctl.snooze_id is not null),
            'has_active_segment_snoozes', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'active_segment_snooze_count', coalesce(tasm.active_segment_snooze_count, 0),
            'active_segment_dated_snooze_count', coalesce(tasm.active_segment_dated_snooze_count, 0),
            'active_segment_indefinite_snooze_count', coalesce(tasm.active_segment_indefinite_snooze_count, 0),
            'whole_timesheet_snooze_action_blocked', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'whole_timesheet_snooze_action_block_reason', case when coalesce(tasm.active_segment_snooze_count, 0) > 0 then 'ACTIVE_SEGMENT_SNOOZES_EXIST' else null end,
            'segment_snooze_action_blocked', (ctl.snooze_id is not null),
            'segment_snooze_action_block_reason', case when ctl.snooze_id is not null then 'WHOLE_TIMESHEET_SNOOZE_ACTIVE' else null end
          )
          order by ctsr.seg_ord
        ) filter (where ctsr.presentation_segment_state = 'BLOCKED_VISIBLE'),
        '[]'::jsonb
      ) as blocked_visible_segment_rows_json,
      coalesce(
        jsonb_agg(
          ctsr.segment_base_json || jsonb_build_object(
            'presentation_section', 'BLOCKED_FOR_PAY',
            'presentation_role', 'CHILD',
            'presentation_parent_line_id', ctl.timesheet_id::text,
            'has_active_timesheet_snooze', (ctl.snooze_id is not null),
            'has_active_segment_snoozes', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'active_segment_snooze_count', coalesce(tasm.active_segment_snooze_count, 0),
            'active_segment_dated_snooze_count', coalesce(tasm.active_segment_dated_snooze_count, 0),
            'active_segment_indefinite_snooze_count', coalesce(tasm.active_segment_indefinite_snooze_count, 0),
            'whole_timesheet_snooze_action_blocked', (coalesce(tasm.active_segment_snooze_count, 0) > 0),
            'whole_timesheet_snooze_action_block_reason', case when coalesce(tasm.active_segment_snooze_count, 0) > 0 then 'ACTIVE_SEGMENT_SNOOZES_EXIST' else null end,
            'segment_snooze_action_blocked', (ctl.snooze_id is not null),
            'segment_snooze_action_block_reason', case when ctl.snooze_id is not null then 'WHOLE_TIMESHEET_SNOOZE_ACTIVE' else null end
          )
          order by ctsr.seg_ord
        ) filter (where ctsr.presentation_segment_state in ('READY', 'BLOCKED_VISIBLE')),
        '[]'::jsonb
      ) as visible_segment_rows_json
    from canonical_timesheet_lines ctl
    left join timesheet_active_segment_snooze_meta tasm
      on tasm.candidate_id = ctl.candidate_id
     and tasm.timesheet_id = ctl.timesheet_id
     and tasm.booking_id is not distinct from ctl.booking_id
    left join canonical_timesheet_segment_rows ctsr
      on ctsr.candidate_id = ctl.candidate_id
     and ctsr.timesheet_id = ctl.timesheet_id
    group by
      ctl.candidate_id,
      ctl.timesheet_id,
      ctl.booking_id,
      ctl.snooze_id,
      tasm.active_segment_snooze_count,
      tasm.active_segment_dated_snooze_count,
      tasm.active_segment_indefinite_snooze_count
  ),
  canonical_timesheet_presentation_seed as (
    select
      ctl.candidate_id,
      ctl.timesheet_id,
      ctl.booking_id,
      ctl.ts_role,
      ctl.ts_band,
      ctl.client_id,
      ctl.client_name,
      ctl.week_ending_date,
      ctl.source_pay_method,
      ctl.umb_vat_chargeable,
      ctl.candidate_pay_method,
      ctl.cand_tms_ref,
      ctl.cand_display_name,
      ctl.payee_entity_kind,
      ctl.payee_entity_id,
      ctl.is_ready_for_draft,
      ctl.override_id,
      ctl.override_reason,
      ctl.snooze_id,
      ctl.snooze_until_date,
      ctl.snooze_note,
      ctl.amount_ex_vat,
      ctl.amount_display,
      ctl.case_is_blocked,
      ctl.case_resolution_summary_json,
      ctl.case_components_json,
      coalesce(ctsr.total_segment_count, 0) as total_segment_count,
      coalesce(ctsr.ready_segment_count, 0) as ready_segment_count,
      coalesce(ctsr.blocked_visible_segment_count, 0) as blocked_visible_segment_count,
      coalesce(ctsr.hidden_indefinite_segment_count, 0) as hidden_indefinite_segment_count,
      coalesce(ctsr.active_segment_snooze_count, 0) as active_segment_snooze_count,
      coalesce(ctsr.active_segment_dated_snooze_count, 0) as active_segment_dated_snooze_count,
      coalesce(ctsr.active_segment_indefinite_snooze_count, 0) as active_segment_indefinite_snooze_count,
      coalesce(ctsr.ready_segment_amount_ex_vat, 0) as ready_segment_amount_ex_vat,
      coalesce(ctsr.blocked_visible_segment_amount_ex_vat, 0) as blocked_visible_segment_amount_ex_vat,
      coalesce(ctsr.hidden_indefinite_segment_amount_ex_vat, 0) as hidden_indefinite_segment_amount_ex_vat,
      coalesce(ctsr.ready_segment_rows_json, '[]'::jsonb) as ready_segment_rows_json,
      coalesce(ctsr.blocked_visible_segment_rows_json, '[]'::jsonb) as blocked_visible_segment_rows_json,
      coalesce(ctsr.visible_segment_rows_json, '[]'::jsonb) as visible_segment_rows_json,
      round(
        coalesce(ctl.amount_ex_vat, 0)
        - coalesce(ctsr.ready_segment_amount_ex_vat, 0)
        - coalesce(ctsr.blocked_visible_segment_amount_ex_vat, 0)
        - coalesce(ctsr.hidden_indefinite_segment_amount_ex_vat, 0),
        2
      ) as non_segment_amount_ex_vat,
      (ctl.snooze_id is not null) as has_active_timesheet_snooze,
      (coalesce(ctsr.active_segment_snooze_count, 0) > 0) as has_active_segment_snoozes
    from canonical_timesheet_lines ctl
    left join canonical_timesheet_segment_rollup ctsr
      on ctsr.candidate_id = ctl.candidate_id
     and ctsr.timesheet_id = ctl.timesheet_id
     and ctsr.booking_id is not distinct from ctl.booking_id
  ),
  canonical_timesheet_presentation_state as (
    select
      ctps.*,
      round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) as ready_section_amount_ex_vat,
      round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0), 2) as blocked_section_amount_ex_vat,
      round(
        case
          when ctps.source_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2), v_vat_rate_pct, ctps.umb_vat_chargeable)->>'inc')::numeric
          else round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2)
        end,
        2
      ) as ready_section_amount_display,
      round(
        case
          when ctps.source_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0), 2), v_vat_rate_pct, ctps.umb_vat_chargeable)->>'inc')::numeric
          else round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0), 2)
        end,
        2
      ) as blocked_section_amount_display,
      (
        round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
        or coalesce(ctps.ready_segment_count, 0) > 0
      ) as has_ready_presentation,
      (
        ctps.has_active_timesheet_snooze = true
        or ctps.case_is_blocked = true
        or coalesce(ctps.blocked_visible_segment_count, 0) > 0
        or (
          ctps.is_ready_for_draft = false
          and (
            round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
            or coalesce(ctps.ready_segment_count, 0) > 0
          )
        )
      ) as has_blocked_presentation,
      (
        ctps.has_active_timesheet_snooze = false
        and ctps.case_is_blocked = false
        and (
          round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
          or coalesce(ctps.ready_segment_count, 0) > 0
        )
        and coalesce(ctps.blocked_visible_segment_count, 0) > 0
      ) as is_partially_ready,
      (
        ctps.has_active_timesheet_snooze = false
        and ctps.case_is_blocked = false
        and (
          round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
          or coalesce(ctps.ready_segment_count, 0) > 0
        )
        and coalesce(ctps.blocked_visible_segment_count, 0) > 0
      ) as is_partially_blocked
    from canonical_timesheet_presentation_seed ctps
  ),
  canonical_timesheet_presentation_rows as (
    select
      ctpp.candidate_id,
      (
        jsonb_build_object(
          'line_id', case
            when ctpp.is_partially_ready then (ctpp.timesheet_id::text || ':01:ready')
            else ctpp.timesheet_id::text
          end,
          'candidate_id', ctpp.candidate_id::text,
          'tms_ref', ctpp.cand_tms_ref,
          'display_name', ctpp.cand_display_name,
          'line_type', 'TIMESHEET_PAYMENT',
          'finance_case_id', null,
          'case_key', ('timesheet:' || ctpp.timesheet_id::text),
          'case_type', 'TIMESHEET_PAYMENT',
          'case_is_blocked', ctpp.case_is_blocked,
          'case_resolution_summary', ctpp.case_resolution_summary_json,
          'case_components', ctpp.case_components_json,
          'timesheet_id', ctpp.timesheet_id::text,
          'booking_id', ctpp.booking_id,
          'client_id', case when ctpp.client_id is null then null else ctpp.client_id::text end,
          'client_name', ctpp.client_name,
          'week_ending_date', case when ctpp.week_ending_date is null then null else ctpp.week_ending_date::text end,
          'role', ctpp.ts_role,
          'band', ctpp.ts_band,
          'linked_shift_date', null,
          'pay_channel', ctpp.candidate_pay_method,
          'paye_treatment', case when ctpp.candidate_pay_method = 'PAYE' then 'GROSS_ADD' else 'NONE' end,
          'route_type', 'NORMAL_PAYMENT',
          'adjustment_comment', null
        )
        || jsonb_build_object(
          'amount_ex_vat', ctpp.ready_section_amount_ex_vat,
          'amount_display', ctpp.ready_section_amount_display,
          'is_advanced', (ctpp.override_id is not null),
          'advanced_override_id', case when ctpp.override_id is null then null else ctpp.override_id::text end,
          'advanced_reason', ctpp.override_reason,
          'is_excluded_from_allocation', false,
          'is_ready_for_draft', ctpp.is_ready_for_draft,
          'segment_rows', ctpp.ready_segment_rows_json,
          'segment_count', jsonb_array_length(coalesce(ctpp.ready_segment_rows_json, '[]'::jsonb))
        )
        || jsonb_build_object(
          'presentation_section', 'READY_TO_PAY',
          'presentation_role', 'PARENT',
          'presentation_line_id', case
            when ctpp.is_partially_ready then (ctpp.timesheet_id::text || ':01:ready')
            else ctpp.timesheet_id::text
          end,
          'presentation_parent_line_id', ctpp.timesheet_id::text,
          'real_business_timesheet_id', ctpp.timesheet_id::text,
          'total_segment_count', ctpp.total_segment_count,
          'ready_segment_count', ctpp.ready_segment_count,
          'blocked_visible_segment_count', ctpp.blocked_visible_segment_count,
          'hidden_indefinite_segment_count', ctpp.hidden_indefinite_segment_count,
          'is_partially_ready', ctpp.is_partially_ready,
          'is_partially_blocked', ctpp.is_partially_blocked,
          'section_amount_ex_vat', ctpp.ready_section_amount_ex_vat,
          'section_amount_display', ctpp.ready_section_amount_display,
          'section_segment_rows', ctpp.ready_segment_rows_json,
          'section_segment_count', jsonb_array_length(coalesce(ctpp.ready_segment_rows_json, '[]'::jsonb)),
          'section_non_segment_amount_ex_vat', ctpp.non_segment_amount_ex_vat
        )
        || jsonb_build_object(
          'has_active_timesheet_snooze', ctpp.has_active_timesheet_snooze,
          'has_active_segment_snoozes', ctpp.has_active_segment_snoozes,
          'active_segment_snooze_count', ctpp.active_segment_snooze_count,
          'active_segment_dated_snooze_count', ctpp.active_segment_dated_snooze_count,
          'active_segment_indefinite_snooze_count', ctpp.active_segment_indefinite_snooze_count,
          'whole_timesheet_snooze_action_blocked', ctpp.has_active_segment_snoozes,
          'whole_timesheet_snooze_action_block_reason', case when ctpp.has_active_segment_snoozes then 'ACTIVE_SEGMENT_SNOOZES_EXIST' else null end,
          'segment_snooze_action_blocked', ctpp.has_active_timesheet_snooze,
          'segment_snooze_action_block_reason', case when ctpp.has_active_timesheet_snooze then 'WHOLE_TIMESHEET_SNOOZE_ACTIVE' else null end,
          'presentation_reason', case
            when ctpp.is_partially_ready then 'PARTIAL_READY_TO_PAY'
            when ctpp.hidden_indefinite_segment_count > 0 then 'READY_WITH_HIDDEN_INDEFINITE_SEGMENTS'
            else 'READY_TO_PAY'
          end,
          'presentation_advisory_text', case
            when ctpp.is_partially_ready then 'Some segments are blocked'
            when ctpp.hidden_indefinite_segment_count > 0 then 'Some segments are snoozed indefinitely'
            else null
          end
        )
        || jsonb_build_object(
          'snooze_identity', jsonb_build_object(
            'identity_type', 'TIMESHEET',
            'timesheet_id', ctpp.timesheet_id::text,
            'booking_id', ctpp.booking_id,
            'segment_id', null,
            'segment_stable_key', null,
            'source_ref', null
          ),
          'snooze_state', case
            when ctpp.snooze_id is null then jsonb_build_object('state', 'NONE')
            else jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', ctpp.snooze_id::text,
              'snooze_until_date', ctpp.snooze_until_date::text,
              'note', ctpp.snooze_note
            )
          end
        )
      ) as line_json,
      ctpp.candidate_pay_method as pay_channel,
      case when ctpp.candidate_pay_method = 'PAYE' then 'GROSS_ADD' else 'NONE' end as paye_treatment,
      ctpp.ready_section_amount_ex_vat as amount_ex_vat,
      false as is_excluded_from_allocation
    from canonical_timesheet_presentation_state ctpp
    where ctpp.has_active_timesheet_snooze = false
      and ctpp.case_is_blocked = false
      and ctpp.has_ready_presentation = true
      and ctpp.is_ready_for_draft = true

    union all

    select
      ctpp.candidate_id,
      (
        jsonb_build_object(
          'line_id', case
            when ctpp.has_active_timesheet_snooze = false
             and ctpp.case_is_blocked = false
             and ctpp.has_ready_presentation = true
             and ctpp.blocked_visible_segment_count > 0
            then (ctpp.timesheet_id::text || ':02:blocked')
            else ctpp.timesheet_id::text
          end,
          'candidate_id', ctpp.candidate_id::text,
          'tms_ref', ctpp.cand_tms_ref,
          'display_name', ctpp.cand_display_name,
          'line_type', 'TIMESHEET_PAYMENT',
          'finance_case_id', null,
          'case_key', ('timesheet:' || ctpp.timesheet_id::text),
          'case_type', 'TIMESHEET_PAYMENT',
          'case_is_blocked', ctpp.case_is_blocked,
          'case_resolution_summary', ctpp.case_resolution_summary_json,
          'case_components', ctpp.case_components_json,
          'timesheet_id', ctpp.timesheet_id::text,
          'booking_id', ctpp.booking_id,
          'client_id', case when ctpp.client_id is null then null else ctpp.client_id::text end,
          'client_name', ctpp.client_name,
          'week_ending_date', case when ctpp.week_ending_date is null then null else ctpp.week_ending_date::text end,
          'role', ctpp.ts_role,
          'band', ctpp.ts_band,
          'linked_shift_date', null,
          'pay_channel', ctpp.candidate_pay_method,
          'paye_treatment', case when ctpp.candidate_pay_method = 'PAYE' then 'GROSS_ADD' else 'NONE' end,
          'route_type', 'NORMAL_PAYMENT',
          'adjustment_comment', null
        )
        || jsonb_build_object(
          'amount_ex_vat', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.amount_ex_vat
            else ctpp.blocked_section_amount_ex_vat
          end,
          'amount_display', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.amount_display
            else ctpp.blocked_section_amount_display
          end,
          'is_advanced', (ctpp.override_id is not null),
          'advanced_override_id', case when ctpp.override_id is null then null else ctpp.override_id::text end,
          'advanced_reason', ctpp.override_reason,
          'blocked_reason_codes', (
            (case
              when ctpp.has_active_timesheet_snooze = true then jsonb_build_array('BLOCKED_DATED_SNOOZE')
              else '[]'::jsonb
            end)
            ||
            (case
              when ctpp.case_is_blocked = true then jsonb_build_array('BLOCKED_TAXABLE_RESOLUTION')
              else '[]'::jsonb
            end)
          ),
          'is_excluded_from_allocation', (ctpp.has_active_timesheet_snooze = true),
          'is_ready_for_draft', case
            when ctpp.has_active_timesheet_snooze = true then ctpp.is_ready_for_draft
            when ctpp.case_is_blocked = true then ctpp.is_ready_for_draft
            else false
          end,
          'segment_rows', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.visible_segment_rows_json
            else ctpp.blocked_visible_segment_rows_json
          end,
          'segment_count', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then jsonb_array_length(coalesce(ctpp.visible_segment_rows_json, '[]'::jsonb))
            else jsonb_array_length(coalesce(ctpp.blocked_visible_segment_rows_json, '[]'::jsonb))
          end
        )
        || jsonb_build_object(
          'presentation_section', 'BLOCKED_FOR_PAY',
          'presentation_role', 'PARENT',
          'presentation_line_id', case
            when ctpp.has_active_timesheet_snooze = false
             and ctpp.case_is_blocked = false
             and ctpp.has_ready_presentation = true
             and ctpp.blocked_visible_segment_count > 0
            then (ctpp.timesheet_id::text || ':02:blocked')
            else ctpp.timesheet_id::text
          end,
          'presentation_parent_line_id', ctpp.timesheet_id::text,
          'real_business_timesheet_id', ctpp.timesheet_id::text,
          'total_segment_count', ctpp.total_segment_count,
          'ready_segment_count', ctpp.ready_segment_count,
          'blocked_visible_segment_count', ctpp.blocked_visible_segment_count,
          'hidden_indefinite_segment_count', ctpp.hidden_indefinite_segment_count,
          'is_partially_ready', ctpp.is_partially_ready,
          'is_partially_blocked', ctpp.is_partially_blocked,
          'section_amount_ex_vat', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.amount_ex_vat
            else ctpp.blocked_section_amount_ex_vat
          end,
          'section_amount_display', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.amount_display
            else ctpp.blocked_section_amount_display
          end,
          'section_segment_rows', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.visible_segment_rows_json
            else ctpp.blocked_visible_segment_rows_json
          end,
          'section_segment_count', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then jsonb_array_length(coalesce(ctpp.visible_segment_rows_json, '[]'::jsonb))
            else jsonb_array_length(coalesce(ctpp.blocked_visible_segment_rows_json, '[]'::jsonb))
          end,
          'section_non_segment_amount_ex_vat', case
            when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.non_segment_amount_ex_vat
            else 0
          end
        )
        || jsonb_build_object(
          'has_active_timesheet_snooze', ctpp.has_active_timesheet_snooze,
          'has_active_segment_snoozes', ctpp.has_active_segment_snoozes,
          'active_segment_snooze_count', ctpp.active_segment_snooze_count,
          'active_segment_dated_snooze_count', ctpp.active_segment_dated_snooze_count,
          'active_segment_indefinite_snooze_count', ctpp.active_segment_indefinite_snooze_count,
          'whole_timesheet_snooze_action_blocked', ctpp.has_active_segment_snoozes,
          'whole_timesheet_snooze_action_block_reason', case when ctpp.has_active_segment_snoozes then 'ACTIVE_SEGMENT_SNOOZES_EXIST' else null end,
          'segment_snooze_action_blocked', ctpp.has_active_timesheet_snooze,
          'segment_snooze_action_block_reason', case when ctpp.has_active_timesheet_snooze then 'WHOLE_TIMESHEET_SNOOZE_ACTIVE' else null end,
          'presentation_reason', case
            when ctpp.has_active_timesheet_snooze = true then 'WHOLE_TIMESHEET_SNOOZED'
            when ctpp.case_is_blocked = true then 'CASE_BLOCKED'
            when ctpp.is_partially_blocked then 'PARTIAL_BLOCKED_FOR_PAY'
            else 'BLOCKED_FOR_PAY'
          end,
          'presentation_advisory_text', case
            when ctpp.is_partially_blocked then 'Some segments are ready to pay'
            else null
          end
        )
        || jsonb_build_object(
          'snooze_identity', jsonb_build_object(
            'identity_type', 'TIMESHEET',
            'timesheet_id', ctpp.timesheet_id::text,
            'booking_id', ctpp.booking_id,
            'segment_id', null,
            'segment_stable_key', null,
            'source_ref', null
          ),
          'snooze_state', case
            when ctpp.snooze_id is null then jsonb_build_object('state', 'NONE')
            else jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', ctpp.snooze_id::text,
              'snooze_until_date', ctpp.snooze_until_date::text,
              'note', ctpp.snooze_note
            )
          end
        )
      ) as line_json,
      ctpp.candidate_pay_method as pay_channel,
      case when ctpp.candidate_pay_method = 'PAYE' then 'GROSS_ADD' else 'NONE' end as paye_treatment,
      case
        when ctpp.has_active_timesheet_snooze = true or ctpp.case_is_blocked = true or ctpp.is_ready_for_draft = false then ctpp.amount_ex_vat
        else ctpp.blocked_section_amount_ex_vat
      end as amount_ex_vat,
      (ctpp.has_active_timesheet_snooze = true) as is_excluded_from_allocation
    from canonical_timesheet_presentation_state ctpp
    where ctpp.has_blocked_presentation = true
      and (
        ctpp.has_active_timesheet_snooze = true
        or ctpp.case_is_blocked = true
        or ctpp.is_ready_for_draft = false
        or ctpp.blocked_visible_segment_count > 0
      )
  ),
  finance_case_lines as (
    select
      fcrr.candidate_id,
      fcrr.finance_case_id,
      fcrr.client_id,
      fcrr.client_name,
      fcrr.candidate_pay_method,
      fcrr.cand_tms_ref,
      fcrr.cand_display_name,
      fcrr.payee_entity_kind,
      fcrr.payee_entity_id,
      fcrr.candidate_ready_for_draft,
      fcrr.case_type,
      fcrr.taxability,
      fcrr.routing_kind,
      fcrr.destination_label,
      fcrr.beneficiary_name,
      fcrr.masked_bank_account,
      fcrr.payee_bank_hash,
      fcrr.adjustment_comment,
      fcrr.linked_timesheet_id,
      fcrr.linked_shift_date,
      fcrr.next_due_week_start,
      fcrr.active_snooze_id,
      fcrr.active_snooze_kind,
      fcrr.active_snooze_until_date,
      fcrr.active_snooze_note,
      fcrr.due_amount_ex_vat,
      fcrr.is_blocked as case_is_blocked,
      (
        coalesce(fcrr.blocked_reason_codes, '[]'::jsonb)
        ||
        (case
          when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null then jsonb_build_array('BLOCKED_DATED_SNOOZE')
          else '[]'::jsonb
        end)
      ) as blocked_reason_codes,
      fcrr.case_resolution_summary_json,
      fcrr.taxable_manual_debt_resolution_json,
      fcrr.case_components_json,
      fcrr.oneoff_bank_details_present,
      fcrr.is_candidate_directed_oneoff_payout,
      fcrr.appears_on_umbrella_remittance,
      fcrr.generates_candidate_payment_advice,
      fcrr.snooze_allowed,
      fcrr.lifecycle_status_display,
      case
        when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID' then 'LOAN_PAYOUT'
        when fcrr.case_type = 'PAYMENT_ADVANCE' then 'PAYMENT_ADVANCE_REPAYMENT'
        when fcrr.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
        when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
        when fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
        else fcrr.case_type::text
      end as line_type,
      case
        when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID' then 'Loan payment'
        when fcrr.case_type = 'PAYMENT_ADVANCE' then 'Loan repayment'
        when fcrr.case_type = 'OVERPAYMENT' then 'Overpayment recovery'
        when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'Manual credit adjustment payment'
        when fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'Manual debt adjustment deduction'
        else replace(fcrr.case_type::text, '_', ' ')
      end as item_type_label,
      case
        when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID' then 'PAYMENT'
        when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'PAYMENT'
        else 'DEDUCTION'
      end as item_direction,
      case
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'PAYMENT_ADVANCE'
         and upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID'
        then 'NET_ADD'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'PAYMENT_ADVANCE'
        then 'NET_DEDUCT'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'OVERPAYMENT'
         and fcrr.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
        then 'GROSS_DEDUCT'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'OVERPAYMENT'
         and fcrr.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
        then 'NET_DEDUCT'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'
         and fcrr.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
        then 'GROSS_ADD'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'
         and fcrr.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
        then 'NET_ADD'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT'
         and fcrr.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
        then 'GROSS_DEDUCT'
        when fcrr.candidate_pay_method = 'PAYE'
         and fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT'
         and fcrr.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
        then 'NET_DEDUCT'
        else 'NONE'
      end as paye_treatment,
      case
        when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID' then round(coalesce(fcrr.due_amount_ex_vat,0),2)
        when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then round(coalesce(fcrr.due_amount_ex_vat,0),2)
        else round(-coalesce(fcrr.due_amount_ex_vat,0),2)
      end as signed_amount_ex_vat,
      case
        when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null then 'BLOCKED_FOR_PAY'
        when fcrr.is_blocked then 'BLOCKED_FOR_PAY'
        else 'READY_TO_PAY'
      end as readiness_state,
      (
        fcrr.is_blocked = false
        and not (fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null)
        and round(coalesce(fcrr.due_amount_ex_vat,0),2) > 0
      ) as draftable
    from finance_case_resolution_rollup fcrr
    where round(coalesce(fcrr.due_amount_ex_vat,0),2) > 0
  ),
  timesheet_canonical_preview_lines as (
    select
      ctpr.candidate_id,
      (
        ctpr.line_json
        || jsonb_build_object(
          'preview_row_id', coalesce(nullif(btrim(coalesce(ctpr.line_json->>'line_id','')), ''), md5(ctpr.line_json::text)),
          'readiness_state', case
            when upper(coalesce(ctpr.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY'
              or coalesce(nullif(ctpr.line_json->>'is_ready_for_draft','')::boolean, false) = false
            then 'BLOCKED_FOR_PAY'
            else 'READY_TO_PAY'
          end,
          'draftable', (
            upper(coalesce(ctpr.line_json->>'presentation_section','')) = 'READY_TO_PAY'
            and coalesce(nullif(ctpr.line_json->>'is_excluded_from_allocation','')::boolean, false) = false
            and coalesce(nullif(ctpr.line_json->>'is_ready_for_draft','')::boolean, false) = true
          )
        )
      ) as line_json,
      ctpr.pay_channel,
      ctpr.paye_treatment,
      ctpr.amount_ex_vat,
      ctpr.is_excluded_from_allocation
    from canonical_timesheet_presentation_rows ctpr
  ),
  canonical_preview_lines as (
    select
      tcpl.candidate_id,
      tcpl.line_json,
      tcpl.pay_channel,
      tcpl.paye_treatment,
      tcpl.amount_ex_vat,
      tcpl.is_excluded_from_allocation
    from timesheet_canonical_preview_lines tcpl

    union all

    select
      fcl.candidate_id,
      (
        jsonb_build_object(
          'preview_row_id', ('finance:' || fcl.finance_case_id::text || ':' || lower(fcl.line_type)),
          'line_id', ('finance:' || fcl.finance_case_id::text || ':' || lower(fcl.line_type)),
          'candidate_id', fcl.candidate_id::text,
          'tms_ref', fcl.cand_tms_ref,
          'display_name', fcl.cand_display_name,
          'line_type', fcl.line_type,
          'item_type_label', fcl.item_type_label,
          'item_direction', fcl.item_direction,
          'finance_case_id', fcl.finance_case_id::text,
          'case_key', ('finance:' || fcl.finance_case_id::text),
          'case_type', fcl.case_type::text,
          'case_is_blocked', fcl.case_is_blocked
        )
        || jsonb_build_object(
          'case_resolution_summary', fcl.case_resolution_summary_json,
          'case_components', fcl.case_components_json,
          'timesheet_id', case when fcl.linked_timesheet_id is null then null else fcl.linked_timesheet_id::text end,
          'client_id', case when fcl.client_id is null then null else fcl.client_id::text end,
          'client_name', fcl.client_name,
          'week_ending_date', null,
          'linked_shift_date', case when fcl.linked_shift_date is null then null else fcl.linked_shift_date::text end,
          'pay_channel', fcl.candidate_pay_method,
          'paye_treatment', fcl.paye_treatment,
          'route_type', case when fcl.routing_kind is null then 'NORMAL_PAYMENT' else fcl.routing_kind::text end,
          'routing_kind', case when fcl.routing_kind is null then null else fcl.routing_kind::text end,
          'destination_label', fcl.destination_label,
          'taxability', case when fcl.taxability is null then null else fcl.taxability::text end
        )
        || jsonb_strip_nulls(
          jsonb_build_object(
            'taxable_manual_debt_resolution', fcl.taxable_manual_debt_resolution_json
          )
        )
        || jsonb_build_object(
          'beneficiary_name', fcl.beneficiary_name,
          'masked_bank_account', fcl.masked_bank_account,
          'bank_details_hash', fcl.payee_bank_hash,
          'blocked_reason_codes', fcl.blocked_reason_codes,
          'readiness_state', fcl.readiness_state,
          'draftable', fcl.draftable,
          'snooze_allowed', fcl.snooze_allowed,
          'oneoff_bank_details_present', fcl.oneoff_bank_details_present,
          'is_candidate_directed_oneoff_payout', fcl.is_candidate_directed_oneoff_payout,
          'appears_on_umbrella_remittance', fcl.appears_on_umbrella_remittance,
          'generates_candidate_payment_advice', fcl.generates_candidate_payment_advice,
          'adjustment_comment', fcl.adjustment_comment,
          'amount_ex_vat', fcl.signed_amount_ex_vat,
          'amount_display', fcl.signed_amount_ex_vat,
          'is_advanced', false,
          'advanced_override_id', null,
          'advanced_reason', null
        )
        || jsonb_build_object(
          'is_excluded_from_allocation', (fcl.active_snooze_id is not null and fcl.active_snooze_until_date is not null),
          'is_ready_for_draft', fcl.draftable,
          'presentation_section', case
            when fcl.active_snooze_id is not null and fcl.active_snooze_until_date is not null then 'BLOCKED_FOR_PAY'
            when fcl.case_is_blocked then 'BLOCKED_FOR_PAY'
            else 'READY_TO_PAY'
          end,
          'presentation_role', 'PARENT',
          'presentation_line_id', ('finance:' || fcl.finance_case_id::text || ':' || lower(fcl.line_type)),
          'presentation_parent_line_id', ('finance:' || fcl.finance_case_id::text),
          'presentation_reason', case
            when fcl.active_snooze_id is not null and fcl.active_snooze_until_date is not null then 'DATED_SNOOZE'
            when fcl.case_is_blocked then 'CASE_BLOCKED'
            else 'READY_TO_PAY'
          end,
          'source_ref', ('advance:' || fcl.finance_case_id::text),
          'snooze_kind', case
            when fcl.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcl.lifecycle_status_display,'')) = 'PAID' then 'PAYMENT_ADVANCE_REPAYMENT'
            when fcl.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
            else ''
          end
        )
        || jsonb_build_object(
          'snooze_identity', jsonb_build_object(
            'identity_type', 'FINANCE_CASE',
            'timesheet_id', null,
            'booking_id', null,
            'segment_id', null,
            'segment_stable_key', null,
            'source_ref', ('advance:' || fcl.finance_case_id::text)
          ),
          'snooze_state', case
            when fcl.active_snooze_id is null then jsonb_build_object('state','NONE')
            when fcl.active_snooze_until_date is null then jsonb_build_object(
              'state', 'INDEFINITE_SNOOZED',
              'snooze_id', fcl.active_snooze_id::text,
              'snooze_until_date', null,
              'note', fcl.active_snooze_note
            )
            else jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', fcl.active_snooze_id::text,
              'snooze_until_date', fcl.active_snooze_until_date::text,
              'note', fcl.active_snooze_note
            )
          end
        )
      ) as line_json,
      fcl.candidate_pay_method as pay_channel,
      fcl.paye_treatment,
      fcl.signed_amount_ex_vat as amount_ex_vat,
      (fcl.active_snooze_id is not null and fcl.active_snooze_until_date is not null) as is_excluded_from_allocation
    from finance_case_lines fcl
  ),
  candidate_preview_line_rollup as (
    select
      cpl.candidate_id,
      count(*) filter (
        where coalesce(nullif(cpl.line_json->>'draftable','')::boolean, false) = true
      )::int as ready_preview_line_count,
      count(*) filter (
        where upper(coalesce(cpl.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY'
      )::int as blocked_preview_line_count,
      bool_or(
        coalesce(nullif(cpl.line_json->>'draftable','')::boolean, false) = true
      ) as has_ready_preview_line
    from canonical_preview_lines cpl
    group by cpl.candidate_id
  ),
  candidate_preview_timesheet_rollup as (
    select
      ctpp.candidate_id,
      round(
        coalesce(sum(case when ctpp.has_active_timesheet_snooze = false and ctpp.case_is_blocked = false and ctpp.has_ready_presentation = true and ctpp.is_ready_for_draft = true then ctpp.ready_section_amount_ex_vat else 0 end), 0),
        2
      ) as ready_timesheet_total_ex_vat,
      count(*) filter (
        where ctpp.has_blocked_presentation = true
      )::int as blocked_timesheet_preview_count,
      count(*) filter (
        where ctpp.has_active_timesheet_snooze = false
          and ctpp.case_is_blocked = false
          and ctpp.has_ready_presentation = true
          and ctpp.is_ready_for_draft = true
      )::int as ready_timesheet_preview_count,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', ctpp.timesheet_id::text,
            'week_ending_date', case when ctpp.week_ending_date is null then null else ctpp.week_ending_date::text end,
            'client_id', case when ctpp.client_id is null then null else ctpp.client_id::text end,
            'client_name', ctpp.client_name,
            'payment_amount_ex_vat', ctpp.ready_section_amount_ex_vat,
            'payment_amount_inc_vat', ctpp.ready_section_amount_display,
            'payment_amount', ctpp.ready_section_amount_display,
            'source_pay_method', ctpp.source_pay_method,
            'candidate_pay_method', ctpp.candidate_pay_method,
            'segment_deltas', (
              select coalesce(
                jsonb_agg(
                  jsonb_build_object(
                    'segment_id', rs->>'segment_id',
                    'segment_key', rs->>'segment_key',
                    'segment_stable_key', rs->>'segment_stable_key',
                    'work_date', rs->>'date',
                    'ref_num', rs->>'ref_num',
                    'delta_pay_ex_vat', round(coalesce(nullif(rs->>'pay_amount_ex_vat','')::numeric, 0), 2),
                    'raw_delta_ex_vat', round(coalesce(nullif(rs->>'raw_delta_ex_vat','')::numeric, 0), 2),
                    'effective_delta_ex_vat', round(coalesce(nullif(rs->>'effective_delta_ex_vat','')::numeric, 0), 2)
                  )
                  order by
                    nullif(btrim(coalesce(rs->>'date','')), '') nulls last,
                    nullif(btrim(coalesce(rs->>'start','')), '') nulls last,
                    nullif(btrim(coalesce(rs->>'segment_stable_key','')), '') nulls last,
                    nullif(btrim(coalesce(rs->>'segment_id','')), '') nulls last
                ),
                '[]'::jsonb
              )
              from jsonb_array_elements(coalesce(ctpp.ready_segment_rows_json, '[]'::jsonb)) rs
            ),
            'adjustment_deltas', coalesce(tcr.adjustment_deltas_json, '[]'::jsonb),
            'delta_additional_pay_ex_vat', coalesce(tcr.delta_additional_pay_ex_vat, 0),
            'additional_unit_deltas', coalesce(tcr.additional_unit_deltas_json, '[]'::jsonb),
            'reservation_overrun_detected', coalesce(tcr.reservation_overrun_detected, false),
            'delta_expenses_pay_ex_vat', coalesce(tcr.delta_expenses_pay_ex_vat, 0),
            'delta_travel_pay_ex_vat', coalesce(tcr.delta_travel_pay_ex_vat, 0),
            'delta_accommodation_pay_ex_vat', coalesce(tcr.delta_accommodation_pay_ex_vat, 0),
            'delta_other_pay_ex_vat', coalesce(tcr.delta_other_pay_ex_vat, 0),
            'delta_mileage_pay_ex_vat', coalesce(tcr.delta_mileage_pay_ex_vat, 0),
            'case_key', ('timesheet:' || ctpp.timesheet_id::text),
            'case_resolution_summary', coalesce(ctpp.case_resolution_summary_json, '{}'::jsonb),
            'components', coalesce(ctpp.case_components_json, '[]'::jsonb),
            'presentation_section', 'READY_TO_PAY',
            'presentation_role', 'PARENT',
            'total_segment_count', ctpp.total_segment_count,
            'ready_segment_count', ctpp.ready_segment_count,
            'blocked_visible_segment_count', ctpp.blocked_visible_segment_count,
            'hidden_indefinite_segment_count', ctpp.hidden_indefinite_segment_count,
            'is_partially_ready', ctpp.is_partially_ready,
            'is_partially_blocked', ctpp.is_partially_blocked
          )
          order by ctpp.week_ending_date, ctpp.client_name, ctpp.timesheet_id
        ) filter (where ctpp.has_active_timesheet_snooze = false and ctpp.case_is_blocked = false and ctpp.has_ready_presentation = true and ctpp.is_ready_for_draft = true and round(coalesce(ctpp.ready_section_amount_ex_vat,0),2) <> 0),
        '[]'::jsonb
      ) as ready_timesheets_itemisation
    from canonical_timesheet_presentation_state ctpp
    left join timesheet_case_rollup_effective tcr
      on tcr.timesheet_id = ctpp.timesheet_id
     and tcr.candidate_id = ctpp.candidate_id
    group by ctpp.candidate_id
  ),
  timesheet_case_states_flat as (
    select
      tcr.candidate_id,
      tcr.cand_display_name as sort_candidate_display,
      tcr.cand_tms_ref as sort_candidate_tms_ref,
      1 as sort_case_order,
      ('timesheet:' || tcr.timesheet_id::text) as case_key,
      true as is_blocked,
      jsonb_build_object(
        'case_key', ('timesheet:' || tcr.timesheet_id::text),
        'case_scope', 'TIMESHEET_PAYMENT',
        'finance_case_id', null,
        'case_type', 'TIMESHEET_PAYMENT',
        'timesheet_id', tcr.timesheet_id::text,
        'candidate_id', tcr.candidate_id::text,
        'client_id', case when tcr.client_id is null then null else tcr.client_id::text end,
        'client_name', tcr.ts_client_name,
        'week_ending_date', case when tcr.ts_week_ending_date is null then null else tcr.ts_week_ending_date::text end,
        'source_pay_method', tcr.ts_pay_method,
        'candidate_pay_method', tcr.cand_pay_method,
        'resolution_family', tcr.resolution_family,
        'case_needs_resolution', tcr.case_needs_resolution,
        'case_resolution_satisfied_now', tcr.case_resolution_satisfied_now,
        'resolution_action_label', tcr.resolution_action_label,
        'linked_resolution_scope_json', tcr.linked_resolution_scope_json,
        'is_blocked', true,
        'is_mixed_case', tcr.is_mixed_case,
        'open_taxable_count', tcr.open_taxable_count,
        'open_reimbursement_count', tcr.open_reimbursement_count,
        'unresolved_taxable_count', tcr.unresolved_taxable_count,
        'stale_count', tcr.stale_count,
        'safe_amount_ex_vat', tcr.safe_amount_ex,
        'unresolved_taxable_amount_ex_vat', tcr.unresolved_taxable_amount_ex,
        'case_resolution_summary', tcr.case_resolution_summary_json,
        'components', tcr.case_components_json
      ) as case_json
    from timesheet_case_rollup_effective tcr
    where coalesce(tcr.case_needs_resolution, false) = true
      and coalesce(tcr.case_resolution_satisfied_now, false) = false
  ),
  finance_case_states_flat as (
    select
      fcrr.candidate_id,
      fcrr.cand_display_name as sort_candidate_display,
      fcrr.cand_tms_ref as sort_candidate_tms_ref,
      2 as sort_case_order,
      ('finance:' || fcrr.finance_case_id::text) as case_key,
      true as is_blocked,
      jsonb_build_object(
        'case_key', ('finance:' || fcrr.finance_case_id::text),
        'case_scope', 'FINANCE_CASE',
        'finance_case_id', fcrr.finance_case_id::text,
        'timesheet_id', case when fcrr.linked_timesheet_id is null then null else fcrr.linked_timesheet_id::text end,
        'candidate_id', fcrr.candidate_id::text,
        'client_id', case when fcrr.client_id is null then null else fcrr.client_id::text end,
        'client_name', fcrr.client_name,
        'linked_shift_date', case when fcrr.linked_shift_date is null then null else fcrr.linked_shift_date::text end,
        'next_due_week_start', case when fcrr.next_due_week_start is null then null else fcrr.next_due_week_start::text end,
        'case_type', fcrr.case_type::text,
        'candidate_pay_method', fcrr.candidate_pay_method,
        'taxability', case when fcrr.taxability is null then null else fcrr.taxability::text end,
        'routing_kind', case when fcrr.routing_kind is null then null else fcrr.routing_kind::text end,
        'destination_label', fcrr.destination_label,
        'resolution_family', fcrr.resolution_family,
        'case_needs_resolution', fcrr.case_needs_resolution,
        'case_resolution_satisfied_now', fcrr.case_resolution_satisfied_now,
        'resolution_action_label', fcrr.resolution_action_label,
        'linked_resolution_scope_json', fcrr.linked_resolution_scope_json,
        'blocked_reason_codes', fcrr.blocked_reason_codes,
        'snooze_allowed', fcrr.snooze_allowed,
        'is_blocked', true,
        'is_mixed_case', fcrr.is_mixed_case,
        'open_taxable_count', fcrr.open_taxable_count,
        'open_reimbursement_count', fcrr.open_reimbursement_count,
        'unresolved_taxable_count', fcrr.unresolved_taxable_count,
        'stale_count', fcrr.stale_count,
        'due_amount_ex_vat', fcrr.due_amount_ex_vat,
        'case_resolution_summary', fcrr.case_resolution_summary_json,
        'components', fcrr.case_components_json
      ) || jsonb_strip_nulls(
        jsonb_build_object(
          'taxable_manual_debt_resolution', fcrr.taxable_manual_debt_resolution_json
        )
      ) as case_json
    from finance_case_resolution_rollup fcrr
    where fcrr.due_amount_ex_vat > 0
      and coalesce(fcrr.case_needs_resolution, false) = true
      and coalesce(fcrr.case_resolution_satisfied_now, false) = false
  ),
  candidate_case_states_flat as (
    select
      tcsf.candidate_id,
      tcsf.sort_candidate_display,
      tcsf.sort_candidate_tms_ref,
      tcsf.sort_case_order,
      tcsf.case_key,
      tcsf.is_blocked,
      tcsf.case_json
    from timesheet_case_states_flat tcsf

    union all

    select
      fcsf.candidate_id,
      fcsf.sort_candidate_display,
      fcsf.sort_candidate_tms_ref,
      fcsf.sort_case_order,
      fcsf.case_key,
      fcsf.is_blocked,
      fcsf.case_json
    from finance_case_states_flat fcsf
  ),
  candidate_case_states as (
    select
      ccsf.candidate_id,
      count(*)::int as total_case_count,
      sum(case when ccsf.is_blocked then 1 else 0 end)::int as blocked_case_count,
      sum(case when ccsf.is_blocked then 0 else 1 end)::int as safe_case_count,
      coalesce(jsonb_agg(ccsf.case_json order by ccsf.sort_case_order, ccsf.case_key), '[]'::jsonb) as case_resolution_states
    from candidate_case_states_flat ccsf
    group by ccsf.candidate_id
  ),
  case_resolution_states_json as (
    select coalesce(jsonb_agg(ccsf.case_json order by ccsf.sort_candidate_display nulls last, ccsf.sort_candidate_tms_ref nulls last, ccsf.sort_case_order, ccsf.case_key), '[]'::jsonb) as payload
    from candidate_case_states_flat ccsf
  ),
  finance_candidate_totals as (
    select
      fcrr.candidate_id,
      round(sum(case when fcrr.due_amount_ex_vat > 0 then fcrr.due_amount_ex_vat else 0 end), 2) as finance_due_total_ex_vat,
      round(sum(case when fcrr.due_amount_ex_vat > 0 and fcrr.is_blocked = false then fcrr.due_amount_ex_vat else 0 end), 2) as finance_safe_due_total_ex_vat,
      round(sum(case when fcrr.due_amount_ex_vat > 0 and fcrr.is_blocked = true then fcrr.due_amount_ex_vat else 0 end), 2) as finance_blocked_due_total_ex_vat,
      count(*) filter (where fcrr.due_amount_ex_vat > 0) as finance_due_case_count,
      count(*) filter (where fcrr.due_amount_ex_vat > 0 and fcrr.is_blocked = false) as finance_safe_case_count,
      count(*) filter (where fcrr.due_amount_ex_vat > 0 and fcrr.is_blocked = true) as finance_blocked_case_count
    from finance_case_resolution_rollup fcrr
    where fcrr.due_amount_ex_vat > 0
    group by fcrr.candidate_id
  ),
  candidate_finance_itemisation as (
    select
      fcl.candidate_id,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'finance_case_id', fcl.finance_case_id::text,
            'case_key', ('finance:' || fcl.finance_case_id::text),
            'case_type', fcl.case_type::text,
            'line_type', fcl.line_type,
            'item_type_label', fcl.item_type_label,
            'item_direction', fcl.item_direction,
            'client_id', case when fcl.client_id is null then null else fcl.client_id::text end,
            'client_name', fcl.client_name,
            'linked_shift_date', case when fcl.linked_shift_date is null then null else fcl.linked_shift_date::text end,
            'next_due_week_start', case when fcl.next_due_week_start is null then null else fcl.next_due_week_start::text end,
            'taxability', case when fcl.taxability is null then null else fcl.taxability::text end,
            'routing_kind', case when fcl.routing_kind is null then null else fcl.routing_kind::text end,
            'destination_label', fcl.destination_label,
            'amount_ex_vat', fcl.signed_amount_ex_vat,
            'amount_display', fcl.signed_amount_ex_vat,
            'paye_treatment', fcl.paye_treatment,
            'presentation_section', fcl.readiness_state,
            'case_resolution_summary', fcl.case_resolution_summary_json,
            'components', fcl.case_components_json
          ) || jsonb_strip_nulls(
            jsonb_build_object(
              'taxable_manual_debt_resolution', fcl.taxable_manual_debt_resolution_json
            )
          )
          order by fcl.case_type::text, fcl.finance_case_id
        ),
        '[]'::jsonb
      ) as finance_itemisation
    from finance_case_lines fcl
    group by fcl.candidate_id
  ),
  paye_summary_breakdown_json as (
    select jsonb_build_object(
      'gross_side_additions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'GROSS_ADD' and cpl.is_excluded_from_allocation = false then greatest(cpl.amount_ex_vat,0) else 0 end),0),2),
      'gross_side_deductions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'GROSS_DEDUCT' and cpl.is_excluded_from_allocation = false then abs(cpl.amount_ex_vat) else 0 end),0),2),
      'net_side_additions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'NET_ADD' and cpl.is_excluded_from_allocation = false then greatest(cpl.amount_ex_vat,0) else 0 end),0),2),
      'net_side_deductions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'NET_DEDUCT' and cpl.is_excluded_from_allocation = false then abs(cpl.amount_ex_vat) else 0 end),0),2)
    ) as payload
    from canonical_preview_lines cpl
  ),
  timesheet_baseline_component_rows as (
    select
      1 as sort_scope,
      ('timesheet:' || ttr.timesheet_id::text) as sort_case_key,
      coalesce(ttr.component_fingerprint, '') as sort_component_fingerprint,
      coalesce(ttr.component_key_type, '') as sort_component_key_type,
      coalesce(ttr.component_key_value, '') as sort_component_key_value,
      jsonb_strip_nulls(
        jsonb_build_object(
          'component_scope', 'TIMESHEET',
          'candidate_id', ttr.candidate_id::text,
          'case_key', ('timesheet:' || ttr.timesheet_id::text),
          'timesheet_id', ttr.timesheet_id::text,
          'finance_case_id', null,
          'finance_component_id', null,
          'source_family_key', ttr.source_family_key,
          'component_key_type', ttr.component_key_type,
          'component_key_value', ttr.component_key_value,
          'bucket_code', nullif(btrim(coalesce(ttr.source_basis_json->>'bucket_code', '')), ''),
          'source_basis_json', coalesce(ttr.source_basis_json, '{}'::jsonb),
          'source_basis_fingerprint', case when coalesce(ttr.source_basis_json, '{}'::jsonb) <> '{}'::jsonb then md5(ttr.source_basis_json::text) else null::text end,
          'component_fingerprint', ttr.component_fingerprint,
          'classification', ttr.classification::text,
          'source_pay_method', ttr.source_pay_method,
          'current_target_pay_method', ttr.current_target_pay_method,
          'source_units', case when ttr.source_units is null then null else round(ttr.source_units, 6) end,
          'source_rate', case when ttr.source_rate is null then null else round(ttr.source_rate, 6) end,
          'source_charge_rate', case when ttr.source_charge_rate is null then null else round(ttr.source_charge_rate, 6) end,
          'component_amount_ex_vat', round(coalesce(ttr.component_amount_ex_vat, 0), 2),
          'source_charge_ex_vat', case when ttr.source_charge_ex_vat is null then null else round(ttr.source_charge_ex_vat, 2) end,
          'source_pay_ex_vat', case when ttr.source_pay_ex_vat is null then null else round(ttr.source_pay_ex_vat, 2) end,
          'source_margin_ex_vat', case when ttr.source_margin_ex_vat is null then null else round(ttr.source_margin_ex_vat, 2) end,
          'target_pay_ex_vat', case when ttr.target_pay_ex_vat is null then null else round(ttr.target_pay_ex_vat, 2) end,
          'target_charge_ex_vat', case when ttr.target_charge_ex_vat is null then null else round(ttr.target_charge_ex_vat, 2) end,
          'target_margin_ex_vat', case when ttr.target_margin_ex_vat is null then null else round(ttr.target_margin_ex_vat, 2) end,
          'requires_resolution', coalesce(ttr.requires_resolution, false),
          'case_resolution_satisfied_now_component', coalesce(ttr.case_resolution_satisfied_now_component, false),
          'has_suggested_resolution', coalesce(ttr.has_suggested_resolution, false),
          'suggestion_provenance', ttr.suggestion_provenance,
          'approved_resolution_mode', case when ttr.approved_resolution_mode is null then null else ttr.approved_resolution_mode::text end,
          'approved_target_rate', case when ttr.approved_target_rate is null then null else round(ttr.approved_target_rate, 6) end,
          'suggested_resolution_payload_json', ttr.suggested_resolution_payload_json,
          'suggested_resolution_result_json', ttr.suggested_resolution_result_json,
          'is_actionable_resolution_row', coalesce(ttr.is_actionable_resolution_row, false),
          'is_fixed_no_action_taxable_row', coalesce(ttr.is_fixed_no_action_taxable_row, false)
        )
      ) as row_json
    from transient_timesheet_component_review_rows_effective ttr
    where ttr.candidate_id = v_candidate_id
  ),
  finance_baseline_component_rows as (
    select
      2 as sort_scope,
      ('finance:' || fcr.finance_case_id::text) as sort_case_key,
      coalesce(fcr.current_component_fingerprint, '') as sort_component_fingerprint,
      coalesce(fcr.component_key_type, '') as sort_component_key_type,
      coalesce(fcr.component_key_value, '') as sort_component_key_value,
      jsonb_strip_nulls(
        jsonb_build_object(
          'component_scope', 'FINANCE_CASE',
          'candidate_id', fcr.candidate_id::text,
          'case_key', ('finance:' || fcr.finance_case_id::text),
          'timesheet_id', null,
          'finance_case_id', fcr.finance_case_id::text,
          'finance_component_id', case when fcr.finance_component_id is null then null else fcr.finance_component_id::text end,
          'case_type', fcr.case_type::text,
          'taxability', case when fcr.taxability is null then null else fcr.taxability::text end,
          'source_family_key', fcr.source_family_key,
          'component_key_type', fcr.component_key_type,
          'component_key_value', fcr.component_key_value,
          'bucket_code', nullif(btrim(coalesce(fcr.source_basis_json->>'bucket_code', '')), ''),
          'source_basis_json', coalesce(fcr.source_basis_json, '{}'::jsonb),
          'source_basis_fingerprint', case when coalesce(fcr.source_basis_json, '{}'::jsonb) <> '{}'::jsonb then md5(fcr.source_basis_json::text) else null::text end,
          'component_fingerprint', fcr.current_component_fingerprint,
          'classification', fcr.classification::text,
          'source_pay_method', fcr.source_pay_method,
          'current_target_pay_method', fcr.current_target_pay_method,
          'saved_target_pay_method', fcr.saved_target_pay_method,
          'source_amount', round(coalesce(fcr.source_amount, 0), 2),
          'remaining_source_amount', round(coalesce(fcr.remaining_source_amount, 0), 2),
          'source_units', case when fcr.source_units is null then null else round(fcr.source_units, 6) end,
          'source_rate', case when fcr.source_rate is null then null else round(fcr.source_rate, 6) end,
          'source_charge_rate', case when fcr.source_charge_rate is null then null else round(fcr.source_charge_rate, 6) end,
          'source_charge_ex_vat', case when fcr.source_charge_component_ex_vat is null then null else round(fcr.source_charge_component_ex_vat, 2) end,
          'source_pay_ex_vat', case when fcr.source_pay_ex_vat is null then null else round(fcr.source_pay_ex_vat, 2) end,
          'source_margin_ex_vat', case when fcr.source_margin_ex_vat is null then null else round(fcr.source_margin_ex_vat, 2) end,
          'target_pay_ex_vat', case when fcr.target_pay_ex_vat is null then null else round(fcr.target_pay_ex_vat, 2) end,
          'target_charge_ex_vat', case when fcr.target_charge_ex_vat is null then null else round(fcr.target_charge_ex_vat, 2) end,
          'target_margin_ex_vat', case when fcr.target_margin_ex_vat is null then null else round(fcr.target_margin_ex_vat, 2) end,
          'approved_resolution_mode', case when fcr.approved_resolution_mode is null then null else fcr.approved_resolution_mode::text end,
          'approved_target_rate', case when fcr.approved_target_rate is null then null else round(fcr.approved_target_rate, 6) end,
          'approved_nonbucket_resolution_mode', case when fcr.approved_nonbucket_resolution_mode is null then null else fcr.approved_nonbucket_resolution_mode::text end,
          'approved_nonbucket_target_amount_ex_vat', case when fcr.approved_nonbucket_target_amount_ex_vat is null then null else round(fcr.approved_nonbucket_target_amount_ex_vat, 2) end,
          'saved_resolution_mode', case when fcr.saved_resolution_mode is null then null else fcr.saved_resolution_mode::text end,
          'saved_resolution_payload_json', fcr.saved_resolution_payload_json,
          'saved_resolution_result_json', fcr.saved_resolution_result_json,
          'resolution_fingerprint', fcr.resolution_fingerprint,
          'is_resolution_stale', coalesce(fcr.is_resolution_stale, false),
          'stale_reason', fcr.stale_reason,
          'requires_resolution', coalesce(fcr.requires_resolution, false),
          'case_resolution_satisfied_now_component', coalesce(fcr.case_resolution_satisfied_now_component, false),
          'suggested_resolution_payload_json', fcr.suggested_resolution_payload_json,
          'suggested_resolution_result_json', fcr.suggested_resolution_result_json,
          'is_actionable_resolution_row', coalesce(fcr.is_actionable_resolution_row, false),
          'is_fixed_no_action_taxable_row', coalesce(fcr.is_fixed_no_action_taxable_row, false)
        )
      ) as row_json
    from finance_case_component_review_rows_effective fcr
    where fcr.candidate_id = v_candidate_id
  ),
  baseline_component_rows_json as (
    select
      coalesce(
        jsonb_agg(u.row_json order by u.sort_scope, u.sort_case_key, u.sort_component_fingerprint, u.sort_component_key_type, u.sort_component_key_value),
        '[]'::jsonb
      ) as payload
    from (
      select * from timesheet_baseline_component_rows
      union all
      select * from finance_baseline_component_rows
    ) u
  )


  select
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', ce.candidate_id::text,
            'tms_ref', ce.cand_tms_ref,
            'display_name', ce.cand_display_name,
            'current_pay_method', ce.cand_pay_method,
            'umbrella_id', case when ce.cand_umbrella_id is null then null else ce.cand_umbrella_id::text end,
            'umbrella_enabled', ce.umb_enabled,
            'umbrella_vat_chargeable', ce.umb_vat_chargeable,

            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', null,
            'umbrella_bank_hash', null,

            'payee_entity_kind', ce.payee_entity_kind,
            'payee_entity_id', case when ce.payee_entity_id is null then null else ce.payee_entity_id::text end,
            'payee_bank_hash', ce.payee_bank_hash,
            'payee_map_present', ce.payee_map_present,
            'name_check_status', ce.payee_name_check_status,
            'name_check_has_override', ce.payee_name_check_has_override,
            'blockers', ce.blockers,

            'blocked_count', ce.blocked_count,
            'do_not_pay_count', ce.do_not_pay_count,
            'blocked_case_count', coalesce(ccs.blocked_case_count, 0),
            'safe_case_count', coalesce(ccs.safe_case_count, 0),
            'preview_blocked_timesheet_count', coalesce(cptr.blocked_timesheet_preview_count, 0),
            'preview_ready_timesheet_count', coalesce(cptr.ready_timesheet_preview_count, 0),
            'case_resolution_states', coalesce(ccs.case_resolution_states, '[]'::jsonb),
            'gross_preview_ex_vat_non_mismatch', coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),
            'finance_due_total_ex_vat', coalesce(fct.finance_due_total_ex_vat,0),
            'finance_safe_due_total_ex_vat', coalesce(fct.finance_safe_due_total_ex_vat,0),
            'finance_blocked_due_total_ex_vat', coalesce(fct.finance_blocked_due_total_ex_vat,0),
            'mismatch', jsonb_build_object(
              'has_mismatch', ce.has_mismatch,
              'source_paye_ex_vat', ce.mismatch_source_paye_ex,
              'source_umbrella_ex_vat', ce.mismatch_source_umbrella_ex,
              'if_settle_via_paye_ex_vat',
                round(
                  ce.mismatch_source_paye_ex
                  + public._pay_convert_umbrella_to_paye_ex(ce.mismatch_source_umbrella_ex, v_erni_pct),
                  2
                ),
              'if_settle_via_umbrella',
                public._pay_convert_paye_to_umbrella(ce.mismatch_source_paye_ex, v_erni_pct, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
                ||
                public._pay_umbrella_vat_calc(ce.mismatch_source_umbrella_ex, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
            ),
            'overpayment_balance_remaining', ce.overpayment_balance_remaining,
            'loan_due_this_week', ce.loan_due_this_week,
            'loan_repaid_wtd', ce.loan_repaid_wtd,
            'min_take_home_wtd', ce.min_take_home_wtd,
            'max_possible_loan_take_this_run',
              round(
                case
                  when ce.cand_pay_method = 'UMBRELLA' then
                    least(
                      greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                      greatest(
                        least(
                          (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                          (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                        ),
                        0
                      )
                    )
                  else null
                end,
                2
              ),
            'paye_net_status', ce.paye_net_status,
            'loan', jsonb_build_object(
              'pay_week_start', v_week_start::text,
              'loan_due_total', ce.loan_due_total,
              'loan_due_entries', ce.loan_due_entries,
              'loan_due_this_week', ce.loan_due_this_week,
              'loan_repaid_wtd', ce.loan_repaid_wtd,
              'min_take_home_wtd', ce.min_take_home_wtd,
              'max_possible_loan_take_this_run',
                round(
                  case
                    when ce.cand_pay_method = 'UMBRELLA' then
                      least(
                        greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                        greatest(
                          least(
                            (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                            (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                          ),
                          0
                        )
                      )
                    else null
                  end,
                  2
                ),
              'paye_net_status', ce.paye_net_status,
              'cap_fields', jsonb_build_object(
                'min_take_home', ce.min_take_home_wtd,
                'max_deduction',
                  round(
                    case
                      when ce.cand_pay_method = 'UMBRELLA' then
                        least(
                          greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                          greatest(
                            least(
                              (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                              (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                            ),
                            0
                          )
                        )
                      else null
                    end,
                    2
                  )
              )
            ),
            'computed_net_bank_amount_non_mismatch', null,
            'itemisation', (coalesce(cptr.ready_timesheets_itemisation, ce.timesheets_itemisation, '[]'::jsonb) || coalesce(cfi.finance_itemisation, '[]'::jsonb))
          )
          order by ce.cand_display_name nulls last, ce.cand_tms_ref nulls last, ce.candidate_id
        )
        from cand_payee ce
        left join candidate_case_states ccs
          on ccs.candidate_id = ce.candidate_id
        left join finance_candidate_totals fct
          on fct.candidate_id = ce.candidate_id
        left join candidate_preview_timesheet_rollup cptr
          on cptr.candidate_id = ce.candidate_id
        left join candidate_finance_itemisation cfi
          on cfi.candidate_id = ce.candidate_id
        left join candidate_preview_line_rollup cplr
          on cplr.candidate_id = ce.candidate_id
        where ce.cand_pay_method = 'PAYE'
      ),
      '[]'::jsonb
    ),
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', ce.candidate_id::text,
            'tms_ref', ce.cand_tms_ref,
            'display_name', ce.cand_display_name,
            'current_pay_method', ce.cand_pay_method,
            'umbrella_id', case when ce.cand_umbrella_id is null then null else ce.cand_umbrella_id::text end,
            'umbrella_enabled', ce.umb_enabled,
            'umbrella_vat_chargeable', ce.umb_vat_chargeable,

            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_has_bank_details else null end,
            'umbrella_bank_hash', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_bank_hash else null end,

            'payee_entity_kind', ce.payee_entity_kind,
            'payee_entity_id', case when ce.payee_entity_id is null then null else ce.payee_entity_id::text end,
            'payee_bank_hash', ce.payee_bank_hash,
            'payee_map_present', ce.payee_map_present,
            'name_check_status', ce.payee_name_check_status,
            'name_check_has_override', ce.payee_name_check_has_override,
            'blockers', ce.blockers,

            'blocked_count', ce.blocked_count,
            'do_not_pay_count', ce.do_not_pay_count,
            'blocked_case_count', coalesce(ccs.blocked_case_count, 0),
            'safe_case_count', coalesce(ccs.safe_case_count, 0),
            'preview_blocked_timesheet_count', coalesce(cptr.blocked_timesheet_preview_count, 0),
            'preview_ready_timesheet_count', coalesce(cptr.ready_timesheet_preview_count, 0),
            'case_resolution_states', coalesce(ccs.case_resolution_states, '[]'::jsonb),
            'gross_preview_ex_vat_non_mismatch', coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),
            'finance_due_total_ex_vat', coalesce(fct.finance_due_total_ex_vat,0),
            'finance_safe_due_total_ex_vat', coalesce(fct.finance_safe_due_total_ex_vat,0),
            'finance_blocked_due_total_ex_vat', coalesce(fct.finance_blocked_due_total_ex_vat,0),
            'mismatch', jsonb_build_object(
              'has_mismatch', ce.has_mismatch,
              'source_paye_ex_vat', ce.mismatch_source_paye_ex,
              'source_umbrella_ex_vat', ce.mismatch_source_umbrella_ex,
              'if_settle_via_paye_ex_vat',
                round(
                  ce.mismatch_source_paye_ex
                  + public._pay_convert_umbrella_to_paye_ex(ce.mismatch_source_umbrella_ex, v_erni_pct),
                  2
                ),
              'if_settle_via_umbrella',
                public._pay_convert_paye_to_umbrella(ce.mismatch_source_paye_ex, v_erni_pct, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
                ||
                public._pay_umbrella_vat_calc(ce.mismatch_source_umbrella_ex, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
            ),
            'overpayment_balance_remaining', ce.overpayment_balance_remaining,
            'loan_due_this_week', ce.loan_due_this_week,
            'loan_repaid_wtd', ce.loan_repaid_wtd,
            'min_take_home_wtd', ce.min_take_home_wtd,
            'max_possible_loan_take_this_run',
              round(
                case
                  when ce.cand_pay_method = 'UMBRELLA' then
                    least(
                      greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                      greatest(
                        least(
                          (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                          (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                        ),
                        0
                      )
                    )
                  else null
                end,
                2
              ),
            'paye_net_status', ce.paye_net_status,
            'loan', jsonb_build_object(
              'pay_week_start', v_week_start::text,
              'loan_due_total', ce.loan_due_total,
              'loan_due_entries', ce.loan_due_entries,
              'loan_due_this_week', ce.loan_due_this_week,
              'loan_repaid_wtd', ce.loan_repaid_wtd,
              'min_take_home_wtd', ce.min_take_home_wtd,
              'max_possible_loan_take_this_run',
                round(
                  case
                    when ce.cand_pay_method = 'UMBRELLA' then
                      least(
                        greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                        greatest(
                          least(
                            (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                            (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                          ),
                          0
                        )
                      )
                    else null
                  end,
                  2
                ),
              'paye_net_status', ce.paye_net_status,
              'cap_fields', jsonb_build_object(
                'min_take_home', ce.min_take_home_wtd,
                'max_deduction',
                  round(
                    case
                      when ce.cand_pay_method = 'UMBRELLA' then
                        least(
                          greatest(coalesce(ce.loan_due_this_week,0) - coalesce(ce.loan_repaid_wtd,0),0),
                          greatest(
                            least(
                              (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0))),
                              (coalesce((select pwb.paid_wtd_before from paid_wtd_before pwb where pwb.candidate_id = ce.candidate_id limit 1),0) + (greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0) - least(coalesce(ce.overpayment_balance_remaining,0), greatest(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0),0)))) - coalesce(ce.min_take_home_wtd,0)
                            ),
                            0
                          )
                        )
                      else null
                    end,
                    2
                  )
              )
            ),
            'computed_net_bank_amount_non_mismatch',
              (public._pay_umbrella_vat_calc(coalesce(cptr.ready_timesheet_total_ex_vat, ce.non_mismatch_total_ex, 0), v_vat_rate_pct, ce.umb_vat_chargeable)->>'inc')::numeric,
            'itemisation', (coalesce(cptr.ready_timesheets_itemisation, ce.timesheets_itemisation, '[]'::jsonb) || coalesce(cfi.finance_itemisation, '[]'::jsonb))
          )
          order by ce.cand_display_name nulls last, ce.cand_tms_ref nulls last, ce.candidate_id
        )
        from cand_payee ce
        left join candidate_case_states ccs
          on ccs.candidate_id = ce.candidate_id
        left join finance_candidate_totals fct
          on fct.candidate_id = ce.candidate_id
        left join candidate_preview_timesheet_rollup cptr
          on cptr.candidate_id = ce.candidate_id
        left join candidate_finance_itemisation cfi
          on cfi.candidate_id = ce.candidate_id
        left join candidate_preview_line_rollup cplr
          on cplr.candidate_id = ce.candidate_id
        where ce.cand_pay_method <> 'PAYE'
      ),
      '[]'::jsonb
    ),
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', bi.candidate_id::text,
            'timesheet_id', bi.timesheet_id::text,
            'segment_id', bi.segment_id,
            'ref_num', bi.ref_num,
            'reason', 'MISSING_REF_NUM',
            'blocked_delta_ex_vat', bi.blocked_delta_ex,
            'line_type', 'BLOCKED_TIMESHEET',
            'finance_case_id', null,
            'paye_treatment', null,
            'route_type', 'NORMAL_PAYMENT',
            'adjustment_comment', null,
            'snooze_identity', jsonb_build_object(
              'identity_type', 'TIMESHEET_SEGMENT',
              'timesheet_id', bi.timesheet_id::text,
              'booking_id', bi.booking_id,
              'segment_id', bi.segment_id,
              'segment_stable_key', bi.segment_stable_key,
              'source_ref', null
            ),
            'snooze_state', jsonb_build_object('state','NONE')
          )
          order by bi.candidate_id, bi.timesheet_id, bi.segment_id
        )
        from blocked_items bi
      ),
      '[]'::jsonb
    ),
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', di.candidate_id::text,
            'timesheet_id', di.timesheet_id::text,
            'segment_id', di.segment_id,
            'ref_num', di.ref_num,
            'raw_delta_ex_vat', di.raw_delta_ex,
            'line_type', 'DO_NOT_PAY',
            'finance_case_id', null,
            'paye_treatment', null,
            'route_type', 'NORMAL_PAYMENT',
            'adjustment_comment', null,
            'snooze_identity', jsonb_build_object(
              'identity_type', 'TIMESHEET_SEGMENT',
              'timesheet_id', di.timesheet_id::text,
              'booking_id', di.booking_id,
              'segment_id', di.segment_id,
              'segment_stable_key', di.segment_stable_key,
              'source_ref', null
            ),
            'snooze_state', jsonb_build_object('state','NONE')
          )
          order by di.candidate_id, di.timesheet_id, di.segment_id
        )
        from do_not_pay_items di
      ),
      '[]'::jsonb
    ),
    coalesce(
      (
        select jsonb_agg(x order by x->>'candidate_id', coalesce(x->>'timesheet_id',''), coalesce(x->>'finance_case_id',''), coalesce(x->>'segment_id',''))
        from (
          select jsonb_build_object(
            'kind', case when ctsr.segment_snooze_kind = 'DO_NOT_PAY' then 'DO_NOT_PAY' else 'BLOCKED' end,
            'candidate_id', ctsr.candidate_id::text,
            'timesheet_id', ctsr.timesheet_id::text,
            'segment_id', ctsr.segment_id,
            'ref_num', ctsr.ref_num,
            'amount_ex_vat', ctsr.presentation_amount_ex_vat,
            'raw_delta_ex_vat', ctsr.raw_delta_ex_vat,
            'effective_delta_ex_vat', ctsr.effective_delta_ex_vat,
            'blocked_delta_ex_vat', ctsr.presentation_amount_ex_vat,
            'line_type', case when ctsr.segment_snooze_kind = 'DO_NOT_PAY' then 'DO_NOT_PAY' else 'BLOCKED_TIMESHEET' end,
            'finance_case_id', null,
            'paye_treatment', null,
            'route_type', 'NORMAL_PAYMENT',
            'adjustment_comment', null,
            'snooze_identity', jsonb_build_object(
              'identity_type', 'TIMESHEET_SEGMENT',
              'timesheet_id', ctsr.timesheet_id::text,
              'booking_id', ctsr.booking_id,
              'segment_id', ctsr.segment_id,
              'segment_stable_key', ctsr.segment_stable_key,
              'source_ref', null
            ),
            'snooze_state', jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', ctsr.segment_snooze_id::text,
              'snooze_until_date', ctsr.segment_snooze_until_date::text,
              'note', ctsr.segment_snooze_note,
              'snooze_kind', ctsr.segment_snooze_kind
            ),
            'snooze_id', ctsr.segment_snooze_id::text,
            'snooze_until_date', ctsr.segment_snooze_until_date::text,
            'note', ctsr.segment_snooze_note
          ) as x
          from canonical_timesheet_segment_rows ctsr
          where ctsr.segment_snooze_id is not null
            and ctsr.segment_snooze_until_date is not null

          union all

          select jsonb_build_object(
            'kind', 'TIMESHEET_PAYMENT',
            'candidate_id', ctl.candidate_id::text,
            'timesheet_id', ctl.timesheet_id::text,
            'segment_id', null,
            'ref_num', null,
            'amount_ex_vat', ctl.amount_ex_vat,
            'raw_delta_ex_vat', ctl.amount_ex_vat,
            'effective_delta_ex_vat', ctl.amount_ex_vat,
            'blocked_delta_ex_vat', ctl.amount_ex_vat,
            'line_type', 'TIMESHEET_PAYMENT',
            'finance_case_id', null,
            'paye_treatment', case when ctl.candidate_pay_method = 'PAYE' then 'GROSS_ADD' else 'NONE' end,
            'route_type', 'NORMAL_PAYMENT',
            'adjustment_comment', null,
            'snooze_identity', jsonb_build_object(
              'identity_type', 'TIMESHEET',
              'timesheet_id', ctl.timesheet_id::text,
              'booking_id', ctl.booking_id,
              'segment_id', null,
              'segment_stable_key', null,
              'source_ref', null
            ),
            'snooze_state', jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', ctl.snooze_id::text,
              'snooze_until_date', ctl.snooze_until_date::text,
              'note', ctl.snooze_note
            ),
            'snooze_id', ctl.snooze_id::text,
            'snooze_until_date', ctl.snooze_until_date::text,
            'note', ctl.snooze_note
          ) as x
          from canonical_timesheet_lines ctl
          where ctl.snooze_id is not null
            and ctl.snooze_until_date is not null

          union all

          select jsonb_build_object(
            'kind', fcl.case_type::text,
            'candidate_id', fcl.candidate_id::text,
            'timesheet_id', case when fcl.linked_timesheet_id is null then null else fcl.linked_timesheet_id::text end,
            'segment_id', null,
            'ref_num', null,
            'amount_ex_vat', fcl.signed_amount_ex_vat,
            'raw_delta_ex_vat', fcl.signed_amount_ex_vat,
            'effective_delta_ex_vat', fcl.signed_amount_ex_vat,
            'blocked_delta_ex_vat', fcl.signed_amount_ex_vat,
            'line_type', fcl.line_type,
            'finance_case_id', fcl.finance_case_id::text,
            'paye_treatment', fcl.paye_treatment,
            'route_type', 'NORMAL_PAYMENT',
            'adjustment_comment', fcl.adjustment_comment,
            'snooze_identity', jsonb_build_object(
              'identity_type', 'FINANCE_CASE',
              'timesheet_id', null,
              'booking_id', null,
              'segment_id', null,
              'segment_stable_key', null,
              'source_ref', ('advance:' || fcl.finance_case_id::text)
            ),
            'snooze_state', jsonb_build_object(
              'state', 'DATED_SNOOZED',
              'snooze_id', fcl.active_snooze_id::text,
              'snooze_until_date', fcl.active_snooze_until_date::text,
              'note', fcl.active_snooze_note
            ),
            'snooze_id', fcl.active_snooze_id::text,
            'snooze_until_date', fcl.active_snooze_until_date::text,
            'note', fcl.active_snooze_note
          ) as x
          from finance_case_lines fcl
          where fcl.active_snooze_id is not null
            and fcl.active_snooze_until_date is not null
            and fcl.due_amount_ex_vat > 0
        ) u
      ),
      '[]'::jsonb
    ),
    coalesce((select pj.payees from payees_json pj), '[]'::jsonb),
    '{}'::jsonb,
    coalesce((select jsonb_agg(cpl.line_json order by cpl.candidate_id, cpl.line_json->>'display_name', cpl.line_json->>'line_type', cpl.line_json->>'line_id') from canonical_preview_lines cpl), '[]'::jsonb),
    coalesce((select psbj.payload from paye_summary_breakdown_json psbj), '{}'::jsonb),
    coalesce((select crsj.payload from case_resolution_states_json crsj), '[]'::jsonb),
    coalesce((select bcrj.payload from baseline_component_rows_json bcrj), '[]'::jsonb)
  into v_paye, v_nonpaye, v_blocked, v_do_not_pay, v_snoozed, v_payees, v_summary, v_canonical_preview_lines, v_paye_summary_breakdown, v_case_resolution_states, v_baseline_component_rows;

  if jsonb_typeof(v_paye) = 'array' and jsonb_array_length(v_paye) > 0 then
    v_candidate_row := coalesce(v_paye->0, '{}'::jsonb);
  elsif jsonb_typeof(v_nonpaye) = 'array' and jsonb_array_length(v_nonpaye) > 0 then
    v_candidate_row := coalesce(v_nonpaye->0, '{}'::jsonb);
  else
    v_candidate_row := jsonb_build_object(
      'candidate_id', v_candidate_id::text,
      'display_name', null,
      'tms_ref', null,
      'current_pay_method', null,
      'case_resolution_states', coalesce(v_case_resolution_states, '[]'::jsonb),
      'itemisation', '[]'::jsonb
    );
  end if;

  v_itemisation := case
    when jsonb_typeof(v_candidate_row->'itemisation') = 'array' then coalesce(v_candidate_row->'itemisation', '[]'::jsonb)
    else '[]'::jsonb
  end;

  select jsonb_build_object(
    'candidate_count', 1,
    'case_resolution_state_count', case when jsonb_typeof(coalesce(v_case_resolution_states, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_case_resolution_states, '[]'::jsonb)) else 0 end,
    'canonical_preview_line_count', case when jsonb_typeof(coalesce(v_canonical_preview_lines, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_canonical_preview_lines, '[]'::jsonb)) else 0 end,
    'blocked_preview_line_count', case when jsonb_typeof(coalesce(v_blocked, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_blocked, '[]'::jsonb)) else 0 end,
    'do_not_pay_line_count', case when jsonb_typeof(coalesce(v_do_not_pay, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_do_not_pay, '[]'::jsonb)) else 0 end,
    'snoozed_line_count', case when jsonb_typeof(coalesce(v_snoozed, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_snoozed, '[]'::jsonb)) else 0 end,
    'payees_count', case when jsonb_typeof(coalesce(v_payees, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_payees, '[]'::jsonb)) else 0 end,
    'itemisation_count', case when jsonb_typeof(coalesce(v_itemisation, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_itemisation, '[]'::jsonb)) else 0 end,
    'baseline_component_row_count', case when jsonb_typeof(coalesce(v_baseline_component_rows, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_baseline_component_rows, '[]'::jsonb)) else 0 end,
    'total_amount_ex_vat', coalesce((select round(sum(case when coalesce(elem.value->>'amount_ex_vat','') ~ '^-?[0-9]+(\.[0-9]+)?$' then (elem.value->>'amount_ex_vat')::numeric else 0::numeric end), 2) from jsonb_array_elements(coalesce(v_canonical_preview_lines, '[]'::jsonb)) as elem(value)), 0::numeric),
    'total_amount_display', coalesce((select round(sum(case when coalesce(elem.value->>'amount_display','') ~ '^-?[0-9]+(\.[0-9]+)?$' then (elem.value->>'amount_display')::numeric else 0::numeric end), 2) from jsonb_array_elements(coalesce(v_canonical_preview_lines, '[]'::jsonb)) as elem(value)), 0::numeric),
    'paye_breakdown', coalesce(v_paye_summary_breakdown, '{}'::jsonb)
  )
  into v_summary;

  return jsonb_build_object(
    'candidate_id', v_candidate_id::text,
    'candidate_row', coalesce(v_candidate_row, '{}'::jsonb),
    'summary_fragment', coalesce(v_summary, '{}'::jsonb),
    'case_resolution_states', coalesce(v_case_resolution_states, '[]'::jsonb),
    'canonical_preview_lines', coalesce(v_canonical_preview_lines, '[]'::jsonb),
    'payees', coalesce(v_payees, '[]'::jsonb),
    'itemisation', coalesce(v_itemisation, '[]'::jsonb),
    'blocked_items', coalesce(v_blocked, '[]'::jsonb),
    'do_not_pay_items', coalesce(v_do_not_pay, '[]'::jsonb),
    'snoozed_items', coalesce(v_snoozed, '[]'::jsonb),
    'baseline_component_rows', coalesce(v_baseline_component_rows, '[]'::jsonb)
  );
end;
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
