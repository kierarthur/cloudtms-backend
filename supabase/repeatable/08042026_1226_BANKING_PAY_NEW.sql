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
