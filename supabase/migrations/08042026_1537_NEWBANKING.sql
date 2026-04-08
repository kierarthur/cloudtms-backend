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
    'blocked_candidates_count', CASE WHEN v_is_ready_for_draft THEN 0 ELSE 1 END,
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

  v_blocked_candidates_count := GREATEST(v_candidate_count - v_ready_candidates_count, 0);

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
