-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 622cef344ab3.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_sync_overpayments_from_preview(p_pay_date date, p_week_ending_cutoff date, p_actor_user_id uuid, p_pay_channel_scope text, p_candidate_ids uuid[], p_mismatch_choices jsonb DEFAULT '{}'::jsonb, p_client_filter_single uuid DEFAULT NULL::uuid, p_force_include_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_exclude_timesheet_ids uuid[] DEFAULT NULL::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_scope text := upper(btrim(coalesce(p_pay_channel_scope, '')));
  v_timesheet_case_count int := 0;
  v_cases_inserted int := 0;
  v_cases_touched int := 0;
  v_cases_amended int := 0;
  v_cases_reopened int := 0;
  v_cases_cleared int := 0;
  v_case_update_count int := 0;
  v_overpayment_case_count int := 0;
  v_underpayment_case_count int := 0;
  v_case_candidates_json jsonb := '[]'::jsonb;
  v_overpayment_json jsonb := '[]'::jsonb;
  v_underpayment_json jsonb := '[]'::jsonb;
  v_preview_decisions_json jsonb := '{}'::jsonb;
  v_preview_context_json jsonb := '{}'::jsonb;
  v_preview_baseline_json jsonb := '{}'::jsonb;
  v_preview_candidate_row_json jsonb := '{}'::jsonb;
  v_preview_scope_context_json jsonb := '{}'::jsonb;
  v_preview_scope_candidate_ids_json jsonb := '[]'::jsonb;
  v_preview_candidate_loop_id uuid := NULL::uuid;
  v_preview_candidate_count integer := 0;
  v_requested_candidate_count integer := 0;
  v_missing_candidate_count integer := 0;
  v_missing_candidate_ids_json jsonb := '[]'::jsonb;
  v_authoritative_timesheet_scope_requested boolean := false;
  v_authoritative_timesheet_scope boolean := false;
  v_explicit_empty_timesheet_scope boolean := false;
  v_preview_scope_strategy text := 'CANDIDATE_FULL_LIVE';
  v_sync_authority_token text := NULL::text;
  v_current_sync_authority_token text := NULL::text;
  v_workbench_session_id_text text := NULL::text;
  v_source_build_run_id_text text := NULL::text;
  v_session_version_text text := NULL::text;
  v_workbench_session_id uuid := NULL::uuid;
  v_source_build_run_id uuid := NULL::uuid;
  v_workbench_session_version bigint := NULL::bigint;
  v_authoritative_candidate_id uuid := NULL::uuid;
  v_existing_active_reserved_amount numeric(12,2) := 0;
  v_effective_case_amount_ex numeric(12,2);
  v_active_reservation_protection_applied boolean := false;

  v_target_case_row record;
  v_existing_case_row record;
  v_case_before_json jsonb;
  v_case_after_json jsonb;
  v_components_sync_result jsonb;
  v_component_case_outstanding_amount numeric(12,2) := 0;
  v_component_open_remaining_total numeric(12,2) := 0;
  v_component_case_mismatch_amount numeric(12,2) := 0;
  v_existing_recovered_amount numeric(12,2);
  v_new_outstanding_amount numeric(12,2);
  v_target_amount_is_authoritative_outstanding boolean := false;
  v_selected_finance_case_id uuid;
  v_selected_event_type text;
  v_selected_reason text;
  v_selected_note text;
  v_open_case_candidate record;
  v_target_case_amount_ex numeric(12,2);
  v_case_taxability public.pay_finance_taxability_enum := NULL;
  v_case_routing_kind public.pay_finance_routing_kind_enum := NULL;
  v_synced_case_taxability public.pay_finance_taxability_enum := NULL;
  v_synced_case_routing_kind public.pay_finance_routing_kind_enum := NULL;
  v_synced_before_taxability public.pay_finance_taxability_enum := NULL;
  v_synced_before_routing_kind public.pay_finance_routing_kind_enum := NULL;
  v_case_metadata_component_count integer := 0;
  v_synced_open_component_count integer := 0;
  v_taxability_event_before_json jsonb;
  v_taxability_event_after_json jsonb;
  v_authoritative_negative_component_count integer := 0;
  v_authoritative_negative_component_digest text := NULL::text;
  v_expected_negative_component_digest text := NULL::text;
  v_authoritative_settled_baseline_digest text := NULL::text;
  v_expected_settled_baseline_digest text := NULL::text;
begin
  declare v_correction_candidate uuid; v_residuals jsonb;
  begin
    foreach v_correction_candidate in array coalesce(p_candidate_ids,array[]::uuid[]) loop
      v_residuals:=public._ctms_candidate_correction_residuals_v1(null::uuid,v_correction_candidate,null::uuid,'PAY_SYNC_OVERPAYMENTS');
      if exists(select 1 from jsonb_array_elements(v_residuals) r where coalesce((r->>'draftable')::boolean,false) is not true) then
        raise exception 'CORRECTION_RESIDUAL_NOT_READY_FOR_OVERPAYMENT_SYNC' using errcode='P0001',detail=v_residuals::text;
      end if;
    end loop;
  end;
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW_START',
    jsonb_build_object(
      'pay_date', p_pay_date,
      'week_ending_cutoff', p_week_ending_cutoff,
      'pay_channel_scope', p_pay_channel_scope,
      'candidate_count', COALESCE(array_length(p_candidate_ids, 1), 0),
      'force_include_timesheet_count', COALESCE(array_length(p_force_include_timesheet_ids, 1), 0),
      'exclude_timesheet_count', COALESCE(array_length(p_exclude_timesheet_ids, 1), 0)
    ),
    'pay_sync_overpayments',
    COALESCE(p_pay_date::text, 'NO_PAY_DATE'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  if p_pay_date is null then
    raise exception 'pay_date is required';
  end if;

  if p_week_ending_cutoff is null then
    raise exception 'week_ending_cutoff is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id is required';
  end if;

  if v_scope not in ('PAYE', 'UMBRELLA') then
    raise exception 'Invalid pay_channel_scope (expected PAYE or UMBRELLA)';
  end if;

  v_requested_candidate_count := COALESCE(array_length(p_candidate_ids, 1), 0);

  v_authoritative_timesheet_scope_requested := LOWER(BTRIM(COALESCE(
    p_mismatch_choices->>'overpayment_sync_authoritative_timesheet_scope',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_sync_authority_token := NULLIF(BTRIM(COALESCE(
    p_mismatch_choices->>'overpayment_sync_authority_token',
    ''
  )), '');
  v_current_sync_authority_token := NULLIF(BTRIM(COALESCE(
    current_setting('cloudtms.pay_workbench_overpayment_sync_token', true),
    ''
  )), '');

  IF COALESCE(v_authoritative_timesheet_scope_requested, false)
     AND (
       p_force_include_timesheet_ids IS NULL
       OR v_sync_authority_token IS NULL
       OR v_current_sync_authority_token IS NULL
       OR v_sync_authority_token IS DISTINCT FROM v_current_sync_authority_token
     ) THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_AUTHORITY_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_AUTHORITY_INVALID',
              'message', 'Authoritative Timesheet scope is internal Workbench authority and requires the matching transaction-local source-build token.',
              'force_include_argument_supplied', p_force_include_timesheet_ids IS NOT NULL,
              'authority_token_supplied', v_sync_authority_token IS NOT NULL,
              'transaction_authority_present', v_current_sync_authority_token IS NOT NULL
            )::text;
  END IF;

  v_authoritative_timesheet_scope := COALESCE(v_authoritative_timesheet_scope_requested, false)
    AND p_force_include_timesheet_ids IS NOT NULL
    AND v_sync_authority_token IS NOT NULL
    AND v_sync_authority_token = v_current_sync_authority_token;

  v_explicit_empty_timesheet_scope := COALESCE(v_authoritative_timesheet_scope, false)
    AND COALESCE(array_length(p_force_include_timesheet_ids, 1), 0) = 0;

  v_preview_scope_strategy := CASE
    WHEN COALESCE(v_authoritative_timesheet_scope, false)
     AND COALESCE(array_length(p_force_include_timesheet_ids, 1), 0) > 0
      THEN 'TARGETED_TIMESHEETS'
    WHEN COALESCE(v_authoritative_timesheet_scope, false)
      THEN 'AUTHORITATIVE_EMPTY'
    ELSE 'CANDIDATE_FULL_LIVE'
  END;

  v_preview_decisions_json := (
      CASE
        WHEN jsonb_typeof(COALESCE(p_mismatch_choices, '{}'::jsonb)) = 'object'
          THEN COALESCE(p_mismatch_choices, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
      - 'refresh_scope_kind'
      - 'targeted_timesheet_ids'
      - 'targeted_timesheet_ids_requested'
      - 'linked_timesheet_ids'
      - 'linked_timesheet_ids_requested'
      - 'source_build_allow_full_fallback'
      - 'overpayment_sync_authority_token'
      - 'overpayment_sync_authoritative_timesheet_scope'
      - 'authoritative_timesheet_scope'
      - 'exact_timesheet_scope'
      - 'workbench_session_id'
      - 'source_build_run_id'
      - 'source_change_seq'
      - 'session_version'
      - 'overpayment_sync_scope_digest'
      - 'overpayment_sync_negative_component_digest'
      - 'overpayment_sync_settled_baseline_digest'
      - 'policy_x_authority_scope'
    )
    || jsonb_build_object(
      'overpayment_sync_mode', true,
      'emit_raw_overpayment_components', true,
      'preview_context_mode', CASE WHEN v_requested_candidate_count = 1 THEN 'FULL_COMPAT' ELSE 'PAGE' END,
      'scope_limit', LEAST(GREATEST(COALESCE(v_requested_candidate_count, 1), 1), 100),
      'pay_channel_scope', v_scope,
      'candidate_ids', COALESCE(to_jsonb(p_candidate_ids), '[]'::jsonb),
      'force_include_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
      'exclude_timesheet_ids', COALESCE(to_jsonb(p_exclude_timesheet_ids), '[]'::jsonb),
      'overpayment_sync_preview_scope_strategy', v_preview_scope_strategy
    )
    || CASE
      WHEN v_preview_scope_strategy = 'TARGETED_TIMESHEETS' THEN
        jsonb_build_object(
          'refresh_scope_kind', 'TARGETED_TIMESHEETS',
          'targeted_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
          'targeted_timesheet_ids_requested', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
          'linked_timesheet_ids', '[]'::jsonb,
          'linked_timesheet_ids_requested', '[]'::jsonb,
          'source_build_allow_full_fallback', false,
          'overpayment_sync_explicit_empty_scope', false
        )
      ELSE
        jsonb_build_object(
          'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
          'targeted_timesheet_ids', '[]'::jsonb,
          'targeted_timesheet_ids_requested', '[]'::jsonb,
          'linked_timesheet_ids', '[]'::jsonb,
          'linked_timesheet_ids_requested', '[]'::jsonb,
          'overpayment_sync_explicit_empty_scope', COALESCE(v_explicit_empty_timesheet_scope, false)
        )
    END
    || CASE
      WHEN COALESCE(v_authoritative_timesheet_scope, false) THEN
        jsonb_build_object(
          'workbench_source_build_mode', true,
          'source_build_mode', true,
          'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'overpayment_sync_authority_token', v_sync_authority_token
        )
      ELSE '{}'::jsonb
    END;


  IF EXISTS (
    SELECT 1
    FROM unnest(COALESCE(p_candidate_ids, ARRAY[]::uuid[])) AS requested_candidate(candidate_id)
    WHERE requested_candidate.candidate_id IS NULL
  ) THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_ID_INVALID',
              'message', 'candidate_ids must not contain null values.'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM unnest(COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[])) AS included_timesheet(timesheet_id)
    WHERE included_timesheet.timesheet_id IS NULL
  ) THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_FORCE_INCLUDE_TIMESHEET_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_FORCE_INCLUDE_TIMESHEET_ID_INVALID',
              'message', 'force_include_timesheet_ids must not contain null values.'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM unnest(COALESCE(p_exclude_timesheet_ids, ARRAY[]::uuid[])) AS excluded_timesheet(timesheet_id)
    WHERE excluded_timesheet.timesheet_id IS NULL
  ) THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_EXCLUDE_TIMESHEET_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_EXCLUDE_TIMESHEET_ID_INVALID',
              'message', 'exclude_timesheet_ids must not contain null values.'
            )::text;
  END IF;

  IF v_requested_candidate_count > 100 THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_SCOPE_TOO_LARGE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_SCOPE_TOO_LARGE',
              'candidate_count', v_requested_candidate_count,
              'limit', 100,
              'message', 'Supply at most 100 explicit candidate IDs per bounded reconciliation call.'
            )::text;
  END IF;

  IF COALESCE(v_authoritative_timesheet_scope, false) THEN
    IF v_requested_candidate_count IS DISTINCT FROM 1 THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_SINGLE_CANDIDATE_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_SINGLE_CANDIDATE_REQUIRED',
                'candidate_count', v_requested_candidate_count
              )::text;
    END IF;

    v_workbench_session_id_text := NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'workbench_session_id', '')), '');
    v_source_build_run_id_text := NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'source_build_run_id', '')), '');
    v_session_version_text := NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'session_version', '')), '');

    IF v_workbench_session_id_text IS NULL
       OR v_workbench_session_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR v_source_build_run_id_text IS NULL
       OR v_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR v_session_version_text IS NULL
       OR v_session_version_text !~ '^[0-9]{1,18}$'
       OR NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_scope_digest', '')), '') IS NULL
       OR NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_negative_component_digest', '')), '') IS NULL
       OR NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_settled_baseline_digest', '')), '') IS NULL
       OR UPPER(BTRIM(COALESCE(p_mismatch_choices->>'policy_x_authority_scope', ''))) <> 'PRE_DRAFT_LIVE_TRUTH' THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_METADATA_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_METADATA_INVALID',
                'workbench_session_id', v_workbench_session_id_text,
                'source_build_run_id', v_source_build_run_id_text,
                'session_version', v_session_version_text,
                'policy_x_authority_scope', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'policy_x_authority_scope', '')), '')
              )::text;
    END IF;

    v_workbench_session_id := v_workbench_session_id_text::uuid;
    v_source_build_run_id := v_source_build_run_id_text::uuid;
    v_workbench_session_version := v_session_version_text::bigint;

    SELECT requested_candidate.candidate_id
    INTO v_authoritative_candidate_id
    FROM unnest(p_candidate_ids) AS requested_candidate(candidate_id)
    WHERE requested_candidate.candidate_id IS NOT NULL
    LIMIT 1;

    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS authoritative_session
    JOIN public.banking_pay_workbench_session_scope AS authoritative_scope
      ON authoritative_scope.session_id = authoritative_session.id
     AND authoritative_scope.candidate_id = v_authoritative_candidate_id
    JOIN public.candidates AS authoritative_candidate
      ON authoritative_candidate.id = authoritative_scope.candidate_id
    WHERE authoritative_session.id = v_workbench_session_id
      AND UPPER(BTRIM(COALESCE(authoritative_session.status, ''))) = 'OPEN'
      AND authoritative_session.discarded_at_utc IS NULL
      AND COALESCE(authoritative_session.version, 1) = v_workbench_session_version
      AND authoritative_session.actor_user_id = p_actor_user_id
      AND authoritative_session.pay_date = p_pay_date
      AND authoritative_session.week_ending_cutoff = p_week_ending_cutoff
      AND UPPER(BTRIM(COALESCE(authoritative_candidate.pay_method, ''))) = v_scope;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_SESSION_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_SESSION_INVALID',
                'workbench_session_id', v_workbench_session_id::text,
                'source_build_run_id', v_source_build_run_id::text,
                'session_version', v_workbench_session_version,
                'candidate_id', v_authoritative_candidate_id::text,
                'pay_channel_scope', v_scope
              )::text;
    END IF;

    IF EXISTS (
      WITH candidate_authority_seed_timesheets AS (
        SELECT authoritative_tsfin.timesheet_id
        FROM public.timesheets_financials AS authoritative_tsfin
        WHERE authoritative_tsfin.candidate_id = v_authoritative_candidate_id
          AND authoritative_tsfin.timesheet_id IS NOT NULL

        UNION

        SELECT authoritative_case.linked_timesheet_id
        FROM public.pay_advances AS authoritative_case
        WHERE authoritative_case.candidate_id = v_authoritative_candidate_id
          AND authoritative_case.linked_timesheet_id IS NOT NULL

        UNION

        SELECT authoritative_component.linked_timesheet_id
        FROM public.pay_finance_case_components AS authoritative_component
        WHERE authoritative_component.candidate_id = v_authoritative_candidate_id
          AND authoritative_component.linked_timesheet_id IS NOT NULL

        UNION

        SELECT authoritative_override.timesheet_id
        FROM public.timesheet_payment_overrides AS authoritative_override
        WHERE authoritative_override.candidate_id = v_authoritative_candidate_id
          AND authoritative_override.timesheet_id IS NOT NULL

        UNION

        SELECT authoritative_adjustment.timesheet_id
        FROM public.ts_pay_adjustments AS authoritative_adjustment
        WHERE authoritative_adjustment.candidate_id = v_authoritative_candidate_id
          AND authoritative_adjustment.timesheet_id IS NOT NULL

        UNION

        SELECT authoritative_snooze.timesheet_id
        FROM public.pay_item_snoozes AS authoritative_snooze
        WHERE authoritative_snooze.candidate_id = v_authoritative_candidate_id
          AND authoritative_snooze.timesheet_id IS NOT NULL
          AND authoritative_snooze.cleared_at_utc IS NULL
          AND authoritative_snooze.cancelled_at_utc IS NULL

        UNION

        SELECT authoritative_batch_item.timesheet_id
        FROM public.pay_batch_candidates AS authoritative_batch_candidate
        JOIN public.pay_batch_items AS authoritative_batch_item
          ON authoritative_batch_item.pay_batch_candidate_id = authoritative_batch_candidate.id
        WHERE authoritative_batch_candidate.candidate_id = v_authoritative_candidate_id
          AND authoritative_batch_item.timesheet_id IS NOT NULL
          AND COALESCE(authoritative_batch_item.is_voided, false) IS NOT TRUE

        UNION

        SELECT authoritative_correction.timesheet_id
        FROM public.pay_payment_correction_items AS authoritative_correction
        WHERE authoritative_correction.candidate_id = v_authoritative_candidate_id
          AND authoritative_correction.timesheet_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(authoritative_correction.status, ''))) = 'APPLIED'
      ), candidate_authority_seed_array AS (
        SELECT COALESCE(
          ARRAY_AGG(
            DISTINCT candidate_authority_seed_timesheets.timesheet_id
            ORDER BY candidate_authority_seed_timesheets.timesheet_id
          ),
          ARRAY[]::uuid[]
        ) AS timesheet_ids
        FROM candidate_authority_seed_timesheets
      ), candidate_authority_timesheets AS (
        SELECT candidate_authority_seed_timesheets.timesheet_id
        FROM candidate_authority_seed_timesheets

        UNION

        SELECT authority_rotation.canonical_timesheet_id
        FROM candidate_authority_seed_array
        JOIN public._pay_timesheet_rotation_scope(
          candidate_authority_seed_array.timesheet_ids
        ) AS authority_rotation
          ON true
        WHERE authority_rotation.canonical_timesheet_id IS NOT NULL

        UNION

        SELECT authority_rotation.family_timesheet_id
        FROM candidate_authority_seed_array
        JOIN public._pay_timesheet_rotation_scope(
          candidate_authority_seed_array.timesheet_ids
        ) AS authority_rotation
          ON true
        WHERE authority_rotation.family_timesheet_id IS NOT NULL
      )
      SELECT 1
      FROM unnest(COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[])) AS authoritative_timesheet(timesheet_id)
      WHERE NOT EXISTS (
        SELECT 1
        FROM candidate_authority_timesheets AS candidate_authority
        WHERE candidate_authority.timesheet_id = authoritative_timesheet.timesheet_id
      )
    ) THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_CANDIDATE_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_SCOPE_CANDIDATE_MISMATCH',
                'candidate_id', v_authoritative_candidate_id::text,
                'scope_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb)
              )::text;
    END IF;
  END IF;

  IF COALESCE(v_explicit_empty_timesheet_scope, false) THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW_RESULT',
      jsonb_build_object(
        'pay_channel_scope', v_scope,
        'authoritative_timesheet_scope', true,
        'explicit_empty_timesheet_scope', true,
        'preview_scope_strategy', v_preview_scope_strategy,
        'candidate_count', v_requested_candidate_count,
        'cases_inserted', 0,
        'cases_touched', 0,
        'cases_amended', 0,
        'cases_reopened', 0,
        'cases_cleared', 0
      ),
      'pay_sync_overpayments',
      COALESCE(p_pay_date::text, 'NO_PAY_DATE'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'pay_channel_scope', v_scope,
      'authoritative_timesheet_scope', true,
      'explicit_empty_timesheet_scope', true,
      'preview_scope_strategy', v_preview_scope_strategy,
      'scope_timesheet_ids', '[]'::jsonb,
      'requested_candidate_count', v_requested_candidate_count,
      'preview_candidate_count', 0,
      'preview_candidate_coverage_complete', true,
      'negative_preview_timesheets_count', 0,
      'negative_preview_timesheets', '[]'::jsonb,
      'underpayment_case_count', 0,
      'underpayment_cases', '[]'::jsonb,
      'timesheet_finance_case_candidates_count', 0,
      'timesheet_finance_case_candidates', '[]'::jsonb,
      'cases_inserted', 0,
      'cases_touched', 0,
      'cases_amended', 0,
      'cases_reopened', 0,
      'cases_cleared', 0
    );
  END IF;

  create temporary table if not exists pg_temp.tmp_sync_timesheet_case_candidates (
    candidate_id uuid not null,
    timesheet_id uuid not null,
    client_id uuid null,
    linked_shift_date date null,
    corrected_amount_ex numeric(12,2) not null,
    baseline_signature text null,
    candidate_pay_method text not null,
    case_is_blocked boolean not null,
    needs_lifecycle_tracking boolean not null default false,
    overpayment_amount_ex numeric(12,2) not null,
    underpayment_amount_ex numeric(12,2) not null,
    desired_case_type public.pay_finance_case_type_enum null,
    desired_advance_kind public.pay_advance_kind_enum null,
    desired_reason public.pay_advance_reason_enum null,
    source_original_paid_amount numeric(12,2) null,
    source_corrected_paid_amount numeric(12,2) null,
    components_sync_json jsonb not null default '[]'::jsonb,
    primary key (candidate_id, timesheet_id)
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_sync_case_links (
    candidate_id uuid not null,
    timesheet_id uuid not null,
    finance_case_id uuid not null,
    desired_case_type public.pay_finance_case_type_enum not null,
    source_original_paid_amount numeric(12,2) not null,
    source_corrected_paid_amount numeric(12,2) not null,
    case_amount_ex numeric(12,2) not null,
    linked_shift_date date null,
    baseline_signature text null,
    components_sync_json jsonb not null,
    primary key (candidate_id, timesheet_id)
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_sync_case_clears (
    finance_case_id uuid not null primary key,
    candidate_id uuid not null,
    timesheet_id uuid not null,
    old_case_type public.pay_finance_case_type_enum not null,
    old_original_amount numeric(12,2) not null,
    old_outstanding_amount numeric(12,2) not null,
    old_source_original_paid_amount numeric(12,2) null,
    old_source_corrected_paid_amount numeric(12,2) null,
    old_linked_shift_date date null,
    old_baseline_signature text null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_sync_preview_candidates (
    candidate_id uuid not null primary key,
    candidate_json jsonb not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_sync_authoritative_negative_components (
    timesheet_id uuid not null,
    key_type text not null,
    key_value text not null,
    truth_ex_vat numeric(12,2) not null,
    baseline_ex_vat numeric(12,2) not null,
    reserved_ex_vat numeric(12,2) not null,
    outstanding_ex_vat numeric(12,2) not null,
    baseline_signature text null,
    primary key (timesheet_id, key_type, key_value)
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_sync_raw_negative_timesheet_rows (
    candidate_id uuid not null,
    timesheet_id uuid not null,
    client_id uuid null,
    candidate_pay_method text not null,
    case_is_blocked boolean not null,
    case_components_json jsonb not null default '[]'::jsonb,
    primary key (candidate_id, timesheet_id)
  ) on commit drop;

  truncate table pg_temp.tmp_sync_timesheet_case_candidates;
  truncate table pg_temp.tmp_sync_case_links;
  truncate table pg_temp.tmp_sync_case_clears;
  truncate table pg_temp.tmp_sync_preview_candidates;
  truncate table pg_temp.tmp_sync_authoritative_negative_components;
  truncate table pg_temp.tmp_sync_raw_negative_timesheet_rows;

  IF COALESCE(v_authoritative_timesheet_scope, false) THEN
    INSERT INTO pg_temp.tmp_sync_authoritative_negative_components (
      timesheet_id,
      key_type,
      key_value,
      truth_ex_vat,
      baseline_ex_vat,
      reserved_ex_vat,
      outstanding_ex_vat,
      baseline_signature
    )
    WITH live_entitlement_components AS (
      SELECT
        entitlement_component.timesheet_id,
        UPPER(BTRIM(entitlement_component.key_type)) AS key_type,
        BTRIM(entitlement_component.key_value) AS key_value,
        ROUND(COALESCE(entitlement_component.truth_ex_vat, 0), 2)::numeric(12,2) AS truth_ex_vat,
        ROUND(COALESCE(entitlement_component.baseline_ex_vat, 0), 2)::numeric(12,2) AS baseline_ex_vat
      FROM public._pay_current_timesheet_entitlement_components(
        COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[])
      ) AS entitlement_component
      WHERE NULLIF(BTRIM(COALESCE(entitlement_component.key_type, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(entitlement_component.key_value, '')), '') IS NOT NULL
    ), active_entitlement_reservations AS (
      SELECT
        reserved_component.timesheet_id,
        UPPER(BTRIM(reserved_component.key_type)) AS key_type,
        BTRIM(reserved_component.key_value) AS key_value,
        ROUND(COALESCE(reserved_component.amount_ex_vat, 0), 2)::numeric(12,2) AS reserved_ex_vat
      FROM public._pay_reserved_components(
        COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[]),
        NULL::uuid
      ) AS reserved_component
      WHERE NULLIF(BTRIM(COALESCE(reserved_component.key_type, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(reserved_component.key_value, '')), '') IS NOT NULL
    ), live_economic_keys AS (
      SELECT
        live_component.timesheet_id,
        live_component.key_type,
        live_component.key_value
      FROM live_entitlement_components AS live_component

      UNION

      SELECT
        reserved_component.timesheet_id,
        reserved_component.key_type,
        reserved_component.key_value
      FROM active_entitlement_reservations AS reserved_component
    ), raw_outstanding_components AS (
      SELECT
        live_key.timesheet_id,
        live_key.key_type,
        live_key.key_value,
        ROUND(COALESCE(live_component.truth_ex_vat, 0), 2)::numeric(12,2) AS truth_ex_vat,
        ROUND(COALESCE(live_component.baseline_ex_vat, 0), 2)::numeric(12,2) AS baseline_ex_vat,
        ROUND(COALESCE(reserved_component.reserved_ex_vat, 0), 2)::numeric(12,2) AS reserved_ex_vat,
        ROUND(
          COALESCE(live_component.truth_ex_vat, 0)
          - COALESCE(live_component.baseline_ex_vat, 0)
          - COALESCE(reserved_component.reserved_ex_vat, 0),
          2
        )::numeric(12,2) AS outstanding_ex_vat
      FROM live_economic_keys AS live_key
      LEFT JOIN live_entitlement_components AS live_component
        ON live_component.timesheet_id = live_key.timesheet_id
       AND live_component.key_type = live_key.key_type
       AND live_component.key_value = live_key.key_value
      LEFT JOIN active_entitlement_reservations AS reserved_component
        ON reserved_component.timesheet_id = live_key.timesheet_id
       AND reserved_component.key_type = live_key.key_type
       AND reserved_component.key_value = live_key.key_value
    )
    SELECT
      raw_component.timesheet_id,
      raw_component.key_type,
      raw_component.key_value,
      raw_component.truth_ex_vat,
      raw_component.baseline_ex_vat,
      raw_component.reserved_ex_vat,
      raw_component.outstanding_ex_vat,
      CASE
        WHEN COALESCE(active_settled_basis.active_settled_component_count, 0) > 0
          THEN active_settled_basis.active_settled_signature
        ELSE COALESCE(
          timesheet_pay_state.last_settled_signature,
          md5(COALESCE(timesheet_pay_state.last_settled_snapshot_json::text, '{}'))
        )
      END AS baseline_signature
    FROM raw_outstanding_components AS raw_component
    LEFT JOIN public.timesheet_pay_state AS timesheet_pay_state
      ON timesheet_pay_state.timesheet_id = raw_component.timesheet_id
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*)::integer AS active_settled_component_count,
        md5(COALESCE(jsonb_agg(
          jsonb_build_object(
            'key_type', active_settled_component.key_type,
            'key_value', active_settled_component.key_value,
            'amount_ex_vat', ROUND(COALESCE(active_settled_component.amount_ex_vat, 0), 2),
            'amount_inc_vat', ROUND(COALESCE(active_settled_component.amount_inc_vat, 0), 2)
          ) ORDER BY active_settled_component.key_type, active_settled_component.key_value
        )::text, '[]')) AS active_settled_signature
      FROM public._pay_active_settled_components(
        ARRAY[raw_component.timesheet_id]::uuid[]
      ) AS active_settled_component
    ) AS active_settled_basis ON true
    WHERE raw_component.outstanding_ex_vat < 0;

    SELECT
      COUNT(*)::integer,
      md5(COALESCE(jsonb_agg(
        jsonb_build_object(
          'timesheet_id', negative_component.timesheet_id::text,
          'key_type', negative_component.key_type,
          'key_value', negative_component.key_value,
          'truth_ex_vat', negative_component.truth_ex_vat,
          'baseline_ex_vat', negative_component.baseline_ex_vat,
          'reserved_ex_vat', negative_component.reserved_ex_vat,
          'outstanding_ex_vat', negative_component.outstanding_ex_vat
        ) ORDER BY negative_component.timesheet_id, negative_component.key_type, negative_component.key_value
      )::text, '[]'))
    INTO
      v_authoritative_negative_component_count,
      v_authoritative_negative_component_digest
    FROM pg_temp.tmp_sync_authoritative_negative_components AS negative_component;

    SELECT md5(COALESCE(jsonb_agg(
      jsonb_build_object(
        'timesheet_id', settled_component.timesheet_id::text,
        'key_type', settled_component.key_type,
        'key_value', settled_component.key_value,
        'amount_ex_vat', ROUND(COALESCE(settled_component.amount_ex_vat, 0), 2),
        'amount_inc_vat', ROUND(COALESCE(settled_component.amount_inc_vat, 0), 2)
      ) ORDER BY settled_component.timesheet_id, settled_component.key_type, settled_component.key_value
    )::text, '[]'))
    INTO v_authoritative_settled_baseline_digest
    FROM public._pay_active_settled_components(
      COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[])
    ) AS settled_component;

    v_expected_negative_component_digest := NULLIF(BTRIM(COALESCE(
      p_mismatch_choices->>'overpayment_sync_negative_component_digest',
      ''
    )), '');
    v_expected_settled_baseline_digest := NULLIF(BTRIM(COALESCE(
      p_mismatch_choices->>'overpayment_sync_settled_baseline_digest',
      ''
    )), '');

    IF v_authoritative_negative_component_digest IS DISTINCT FROM v_expected_negative_component_digest
       OR v_authoritative_settled_baseline_digest IS DISTINCT FROM v_expected_settled_baseline_digest THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_COMPONENT_DIGEST_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_COMPONENT_DIGEST_MISMATCH',
                'candidate_id', v_authoritative_candidate_id::text,
                'negative_component_count', COALESCE(v_authoritative_negative_component_count, 0),
                'expected_negative_component_digest', v_expected_negative_component_digest,
                'actual_negative_component_digest', v_authoritative_negative_component_digest,
                'expected_settled_baseline_digest', v_expected_settled_baseline_digest,
                'actual_settled_baseline_digest', v_authoritative_settled_baseline_digest,
                'message', 'The Workbench negative-component or settled-baseline authority changed before finance-case synchronisation.'
              )::text;
    END IF;
  END IF;
  IF v_requested_candidate_count > 0 THEN
    FOR v_preview_candidate_loop_id IN
      SELECT DISTINCT requested_candidate.candidate_id
      FROM unnest(COALESCE(p_candidate_ids, ARRAY[]::uuid[])) AS requested_candidate(candidate_id)
      WHERE requested_candidate.candidate_id IS NOT NULL
      ORDER BY requested_candidate.candidate_id
    LOOP
      v_preview_context_json := public.pay_preview_build_context(
        p_pay_date => p_pay_date,
        p_week_ending_cutoff => p_week_ending_cutoff,
        p_actor_user_id => p_actor_user_id,
        p_candidate_id => v_preview_candidate_loop_id,
        p_client_id => p_client_filter_single,
        p_preview_decisions_json => v_preview_decisions_json
      )
      || jsonb_build_object(
        'overpayment_sync_mode', true,
        'emit_raw_overpayment_components', true,
        'pay_channel_scope', v_scope,
        'candidate_ids', COALESCE(to_jsonb(p_candidate_ids), '[]'::jsonb),
        'force_include_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
        'exclude_timesheet_ids', COALESCE(to_jsonb(p_exclude_timesheet_ids), '[]'::jsonb)
      )
      || CASE
        WHEN COALESCE(v_authoritative_timesheet_scope, false) THEN
          jsonb_build_object(
            'workbench_source_build_mode', true,
            'source_build_mode', true,
            'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          )
        ELSE '{}'::jsonb
      END;

      v_preview_baseline_json := public.pay_preview_candidate_build_summary_fragment(
        p_context_json => v_preview_context_json,
        p_candidate_id => v_preview_candidate_loop_id
      );

      IF jsonb_typeof(COALESCE(v_preview_baseline_json, '{}'::jsonb)) <> 'object' THEN
        RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_PREVIEW_BASELINE_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_SYNC_OVERPAYMENTS_PREVIEW_BASELINE_INVALID',
                  'candidate_id', v_preview_candidate_loop_id::text,
                  'pay_channel_scope', v_scope
                )::text;
      END IF;

      IF COALESCE(v_authoritative_timesheet_scope, false)
         AND COALESCE(v_authoritative_negative_component_count, 0) > 0
         AND to_regclass('pg_temp.timesheet_case_rollup') IS NULL THEN
        PERFORM public.pay_preview_candidate_build_case_component_rows(
          p_context_json => v_preview_context_json,
          p_candidate_id => v_preview_candidate_loop_id
        );
      END IF;

      IF COALESCE(v_authoritative_timesheet_scope, false)
         AND to_regclass('pg_temp.timesheet_case_rollup') IS NOT NULL THEN
        IF EXISTS (
          WITH preview_component_totals AS (
            SELECT
              raw_case.timesheet_id,
              UPPER(BTRIM(COALESCE(raw_component.value->>'component_key_type', ''))) AS key_type,
              BTRIM(COALESCE(raw_component.value->>'component_key_value', '')) AS key_value,
              COUNT(*)::integer AS preview_component_count,
              ROUND(
                SUM((raw_component.value->>'component_amount_ex_vat')::numeric),
                2
              )::numeric(12,2) AS preview_truth_ex_vat
            FROM pg_temp.timesheet_case_rollup AS raw_case
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(raw_case.case_components_json) = 'array'
                  THEN COALESCE(raw_case.case_components_json, '[]'::jsonb)
                ELSE '[]'::jsonb
              END
            ) AS raw_component(value)
            WHERE raw_case.candidate_id = v_preview_candidate_loop_id
              AND raw_case.timesheet_id = ANY(COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[]))
              AND NOT (raw_case.timesheet_id = ANY(COALESCE(p_exclude_timesheet_ids, ARRAY[]::uuid[])))
              AND COALESCE(raw_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            GROUP BY
              raw_case.timesheet_id,
              UPPER(BTRIM(COALESCE(raw_component.value->>'component_key_type', ''))),
              BTRIM(COALESCE(raw_component.value->>'component_key_value', ''))
          )
          SELECT 1
          FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
          LEFT JOIN preview_component_totals AS preview_total
            ON preview_total.timesheet_id = authoritative_component.timesheet_id
           AND preview_total.key_type = authoritative_component.key_type
           AND preview_total.key_value = authoritative_component.key_value
          WHERE (
                  preview_total.preview_component_count IS NULL
                  AND ABS(ROUND(authoritative_component.truth_ex_vat, 2)) > 0.01
                )
             OR (
                  preview_total.preview_component_count IS NOT NULL
                  AND ABS(ROUND(
                    preview_total.preview_truth_ex_vat
                    - authoritative_component.truth_ex_vat,
                    2
                  )) > 0.01
                )
        ) THEN
          RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_NEGATIVE_COMPONENT_METADATA_MISMATCH'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_NEGATIVE_COMPONENT_METADATA_MISMATCH',
                    'candidate_id', v_preview_candidate_loop_id::text,
                    'negative_component_count', COALESCE(v_authoritative_negative_component_count, 0),
                    'negative_component_digest', v_authoritative_negative_component_digest,
                    'message', 'Canonical negative component keys were not represented by preview metadata whose current component totals match authoritative live truth.'
                  )::text;
        END IF;

        INSERT INTO pg_temp.tmp_sync_raw_negative_timesheet_rows (
          candidate_id,
          timesheet_id,
          client_id,
          candidate_pay_method,
          case_is_blocked,
          case_components_json
        )
        SELECT
          raw_case.candidate_id,
          raw_case.timesheet_id,
          raw_case.client_id,
          COALESCE(NULLIF(UPPER(BTRIM(raw_case.cand_pay_method)), ''), v_scope),
          COALESCE(raw_case.is_blocked, false),
          COALESCE((
            WITH matching_preview_components AS (
              SELECT
                authoritative_component.timesheet_id,
                authoritative_component.key_type,
                authoritative_component.key_value,
                authoritative_component.truth_ex_vat,
                authoritative_component.baseline_ex_vat,
                authoritative_component.reserved_ex_vat,
                authoritative_component.outstanding_ex_vat,
                raw_component.value AS component_json,
                raw_component.ordinality::integer AS component_ordinality,
                ROUND(
                  (raw_component.value->>'component_amount_ex_vat')::numeric,
                  2
                )::numeric(12,2) AS preview_truth_component_ex_vat,
                COUNT(*) OVER (
                  PARTITION BY
                    authoritative_component.timesheet_id,
                    authoritative_component.key_type,
                    authoritative_component.key_value
                )::integer AS physical_component_count,
                ROUND(
                  SUM(ABS((raw_component.value->>'component_amount_ex_vat')::numeric)) OVER (
                    PARTITION BY
                      authoritative_component.timesheet_id,
                      authoritative_component.key_type,
                      authoritative_component.key_value
                  ),
                  2
                )::numeric(12,2) AS preview_truth_weight_total_ex_vat,
                ROW_NUMBER() OVER (
                  PARTITION BY
                    authoritative_component.timesheet_id,
                    authoritative_component.key_type,
                    authoritative_component.key_value
                  ORDER BY
                    COALESCE(raw_component.value->'source_basis_json', '{}'::jsonb)::text,
                    raw_component.ordinality
                )::integer AS allocation_rank
              FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
              CROSS JOIN LATERAL jsonb_array_elements(
                CASE
                  WHEN jsonb_typeof(raw_case.case_components_json) = 'array'
                    THEN COALESCE(raw_case.case_components_json, '[]'::jsonb)
                  ELSE '[]'::jsonb
                END
              ) WITH ORDINALITY AS raw_component(value, ordinality)
              WHERE authoritative_component.timesheet_id = raw_case.timesheet_id
                AND authoritative_component.key_type = UPPER(BTRIM(COALESCE(raw_component.value->>'component_key_type', '')))
                AND authoritative_component.key_value = BTRIM(COALESCE(raw_component.value->>'component_key_value', ''))
                AND COALESCE(raw_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            ), preliminary_allocations AS (
              SELECT
                matching_component.*,
                CASE
                  WHEN matching_component.physical_component_count = 1
                    THEN matching_component.outstanding_ex_vat
                  WHEN COALESCE(matching_component.preview_truth_weight_total_ex_vat, 0) <= 0.01
                    THEN CASE
                      WHEN matching_component.allocation_rank = 1
                        THEN matching_component.outstanding_ex_vat
                      ELSE 0::numeric
                    END
                  ELSE ROUND(
                    matching_component.outstanding_ex_vat
                    * ABS(matching_component.preview_truth_component_ex_vat)
                    / matching_component.preview_truth_weight_total_ex_vat,
                    2
                  )
                END::numeric(12,2) AS preliminary_outstanding_allocation
              FROM matching_preview_components AS matching_component
            ), final_allocations AS (
              SELECT
                preliminary_allocation.*,
                CASE
                  WHEN preliminary_allocation.allocation_rank = preliminary_allocation.physical_component_count
                    THEN ROUND(
                      preliminary_allocation.outstanding_ex_vat
                      - COALESCE(
                          SUM(preliminary_allocation.preliminary_outstanding_allocation) OVER (
                            PARTITION BY
                              preliminary_allocation.timesheet_id,
                              preliminary_allocation.key_type,
                              preliminary_allocation.key_value
                            ORDER BY preliminary_allocation.allocation_rank
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                          ),
                          0
                        ),
                      2
                    )
                  ELSE preliminary_allocation.preliminary_outstanding_allocation
                END::numeric(12,2) AS authoritative_outstanding_allocation
              FROM preliminary_allocations AS preliminary_allocation
            )
            SELECT jsonb_agg(
              allocated_component.component_json
              || jsonb_build_object(
                'component_amount_ex_vat', allocated_component.authoritative_outstanding_allocation,
                'authoritative_truth_ex_vat', allocated_component.truth_ex_vat,
                'authoritative_baseline_ex_vat', allocated_component.baseline_ex_vat,
                'authoritative_reserved_ex_vat', allocated_component.reserved_ex_vat,
                'authoritative_outstanding_ex_vat', allocated_component.outstanding_ex_vat,
                'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH'
              )
              ORDER BY
                allocated_component.key_type,
                allocated_component.key_value,
                COALESCE(allocated_component.component_json->'source_basis_json', '{}'::jsonb)::text,
                allocated_component.component_ordinality
            )
            FROM final_allocations AS allocated_component
          ), '[]'::jsonb)
          || COALESCE((
            SELECT jsonb_agg(
              jsonb_build_object(
                'component_key_type', authoritative_component.key_type,
                'component_key_value', authoritative_component.key_value,
                'component_amount_ex_vat', authoritative_component.outstanding_ex_vat,
                'authoritative_truth_ex_vat', authoritative_component.truth_ex_vat,
                'authoritative_baseline_ex_vat', authoritative_component.baseline_ex_vat,
                'authoritative_reserved_ex_vat', authoritative_component.reserved_ex_vat,
                'authoritative_outstanding_ex_vat', authoritative_component.outstanding_ex_vat,
                'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH',
                'source_pay_method', COALESCE(NULLIF(UPPER(BTRIM(raw_case.cand_pay_method)), ''), v_scope),
                'source_basis_json', jsonb_build_object(
                  'linked_timesheet_id', authoritative_component.timesheet_id::text,
                  'component_key_type', authoritative_component.key_type,
                  'component_key_value', authoritative_component.key_value,
                  'source_pay_method', COALESCE(NULLIF(UPPER(BTRIM(raw_case.cand_pay_method)), ''), v_scope),
                  'component_amount_ex_vat', ABS(authoritative_component.outstanding_ex_vat),
                  'authority_helper', '_pay_current_timesheet_entitlement_components'
                )
              )
              ORDER BY authoritative_component.key_type, authoritative_component.key_value
            )
            FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
            WHERE authoritative_component.timesheet_id = raw_case.timesheet_id
              AND ABS(ROUND(authoritative_component.truth_ex_vat, 2)) <= 0.01
              AND NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                  CASE
                    WHEN jsonb_typeof(raw_case.case_components_json) = 'array'
                      THEN COALESCE(raw_case.case_components_json, '[]'::jsonb)
                    ELSE '[]'::jsonb
                  END
                ) AS preview_component(value)
                WHERE authoritative_component.key_type = UPPER(BTRIM(COALESCE(preview_component.value->>'component_key_type', '')))
                  AND authoritative_component.key_value = BTRIM(COALESCE(preview_component.value->>'component_key_value', ''))
                  AND COALESCE(preview_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              )
          ), '[]'::jsonb)
        FROM pg_temp.timesheet_case_rollup AS raw_case
        WHERE raw_case.candidate_id = v_preview_candidate_loop_id
          AND raw_case.timesheet_id = ANY(COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[]))
          AND NOT (raw_case.timesheet_id = ANY(COALESCE(p_exclude_timesheet_ids, ARRAY[]::uuid[])))
          AND EXISTS (
            SELECT 1
            FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
            WHERE authoritative_component.timesheet_id = raw_case.timesheet_id
          )
        ON CONFLICT (candidate_id, timesheet_id) DO UPDATE
        SET
          client_id = EXCLUDED.client_id,
          candidate_pay_method = EXCLUDED.candidate_pay_method,
          case_is_blocked = EXCLUDED.case_is_blocked,
          case_components_json = EXCLUDED.case_components_json;

        INSERT INTO pg_temp.tmp_sync_raw_negative_timesheet_rows (
          candidate_id,
          timesheet_id,
          client_id,
          candidate_pay_method,
          case_is_blocked,
          case_components_json
        )
        SELECT
          v_preview_candidate_loop_id,
          authoritative_component.timesheet_id,
          financial_metadata.client_id,
          COALESCE(financial_metadata.candidate_pay_method, v_scope),
          false,
          jsonb_agg(
            jsonb_build_object(
              'component_key_type', authoritative_component.key_type,
              'component_key_value', authoritative_component.key_value,
              'component_amount_ex_vat', authoritative_component.outstanding_ex_vat,
              'authoritative_truth_ex_vat', authoritative_component.truth_ex_vat,
              'authoritative_baseline_ex_vat', authoritative_component.baseline_ex_vat,
              'authoritative_reserved_ex_vat', authoritative_component.reserved_ex_vat,
              'authoritative_outstanding_ex_vat', authoritative_component.outstanding_ex_vat,
              'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH',
              'source_pay_method', COALESCE(financial_metadata.candidate_pay_method, v_scope),
              'source_basis_json', jsonb_build_object(
                'linked_timesheet_id', authoritative_component.timesheet_id::text,
                'component_key_type', authoritative_component.key_type,
                'component_key_value', authoritative_component.key_value,
                'source_pay_method', COALESCE(financial_metadata.candidate_pay_method, v_scope),
                'component_amount_ex_vat', ABS(authoritative_component.outstanding_ex_vat),
                'authority_helper', '_pay_current_timesheet_entitlement_components'
              )
            )
            ORDER BY authoritative_component.key_type, authoritative_component.key_value
          )
        FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(
              MIN(financial_row.client_id::text) FILTER (WHERE COALESCE(financial_row.is_current, false)),
              MIN(financial_row.client_id::text)
            )::uuid AS client_id,
            COALESCE(
              MIN(NULLIF(UPPER(BTRIM(financial_row.pay_method)), '')) FILTER (WHERE COALESCE(financial_row.is_current, false)),
              MIN(NULLIF(UPPER(BTRIM(financial_row.pay_method)), ''))
            ) AS candidate_pay_method
          FROM public.timesheets_financials AS financial_row
          WHERE financial_row.timesheet_id = authoritative_component.timesheet_id
            AND financial_row.candidate_id = v_preview_candidate_loop_id
        ) AS financial_metadata ON true
        WHERE ABS(ROUND(authoritative_component.truth_ex_vat, 2)) <= 0.01
          AND NOT EXISTS (
            SELECT 1
            FROM pg_temp.timesheet_case_rollup AS raw_case
            WHERE raw_case.candidate_id = v_preview_candidate_loop_id
              AND raw_case.timesheet_id = authoritative_component.timesheet_id
          )
        GROUP BY
          authoritative_component.timesheet_id,
          financial_metadata.client_id,
          financial_metadata.candidate_pay_method
        ON CONFLICT (candidate_id, timesheet_id) DO UPDATE
        SET
          client_id = EXCLUDED.client_id,
          candidate_pay_method = EXCLUDED.candidate_pay_method,
          case_is_blocked = EXCLUDED.case_is_blocked,
          case_components_json = EXCLUDED.case_components_json;

        IF EXISTS (
          WITH allocated_component_totals AS (
            SELECT
              raw_timesheet.timesheet_id,
              UPPER(BTRIM(COALESCE(component.value->>'component_key_type', ''))) AS key_type,
              BTRIM(COALESCE(component.value->>'component_key_value', '')) AS key_value,
              COUNT(*)::integer AS allocated_component_count,
              ROUND(SUM((component.value->>'component_amount_ex_vat')::numeric), 2)::numeric(12,2) AS allocated_outstanding_ex_vat
            FROM pg_temp.tmp_sync_raw_negative_timesheet_rows AS raw_timesheet
            CROSS JOIN LATERAL jsonb_array_elements(raw_timesheet.case_components_json) AS component(value)
            WHERE raw_timesheet.candidate_id = v_preview_candidate_loop_id
              AND COALESCE(component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            GROUP BY
              raw_timesheet.timesheet_id,
              UPPER(BTRIM(COALESCE(component.value->>'component_key_type', ''))),
              BTRIM(COALESCE(component.value->>'component_key_value', ''))
          )
          SELECT 1
          FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
          LEFT JOIN allocated_component_totals AS allocated_total
            ON allocated_total.timesheet_id = authoritative_component.timesheet_id
           AND allocated_total.key_type = authoritative_component.key_type
           AND allocated_total.key_value = authoritative_component.key_value
          WHERE allocated_total.allocated_component_count IS NULL
             OR ABS(ROUND(
                  COALESCE(allocated_total.allocated_outstanding_ex_vat, 0)
                  - authoritative_component.outstanding_ex_vat,
                  2
                )) > 0.01
        ) THEN
          RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_NEGATIVE_COMPONENT_METADATA_MISMATCH'
            USING ERRCODE = 'P0001',
                  DETAIL = jsonb_build_object(
                    'code', 'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_NEGATIVE_COMPONENT_METADATA_MISMATCH',
                    'candidate_id', v_preview_candidate_loop_id::text,
                    'negative_component_count', COALESCE(v_authoritative_negative_component_count, 0),
                    'negative_component_digest', v_authoritative_negative_component_digest,
                    'message', 'Canonical negative components were not represented exactly by the validated pre-draft component allocations.'
                  )::text;
        END IF;
      END IF;

      v_preview_candidate_row_json := CASE
        WHEN jsonb_typeof(v_preview_baseline_json->'candidate_row') = 'object'
          THEN COALESCE(v_preview_baseline_json->'candidate_row', '{}'::jsonb)
        ELSE '{}'::jsonb
      END;

      IF NULLIF(BTRIM(COALESCE(
           v_preview_candidate_row_json->>'candidate_id',
           v_preview_baseline_json->>'candidate_id',
           ''
         )), '') IS DISTINCT FROM v_preview_candidate_loop_id::text THEN
        RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE',
                  'candidate_id', v_preview_candidate_loop_id::text,
                  'pay_channel_scope', v_scope,
                  'baseline_candidate_id', NULLIF(BTRIM(COALESCE(
                    v_preview_candidate_row_json->>'candidate_id',
                    v_preview_baseline_json->>'candidate_id',
                    ''
                  )), '')
                )::text;
      END IF;

      IF UPPER(BTRIM(COALESCE(v_preview_candidate_row_json->>'current_pay_method', ''))) IS DISTINCT FROM v_scope THEN
        RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_PREVIEW_PAY_CHANNEL_MISMATCH'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_SYNC_OVERPAYMENTS_PREVIEW_PAY_CHANNEL_MISMATCH',
                  'candidate_id', v_preview_candidate_loop_id::text,
                  'requested_pay_channel_scope', v_scope,
                  'current_pay_method', NULLIF(BTRIM(COALESCE(v_preview_candidate_row_json->>'current_pay_method', '')), '')
                )::text;
      END IF;

      v_preview_candidate_row_json := v_preview_candidate_row_json
        || jsonb_build_object(
          'candidate_id', v_preview_candidate_loop_id::text,
          'itemisation', CASE
            WHEN jsonb_typeof(v_preview_candidate_row_json->'itemisation') = 'array'
              THEN COALESCE(v_preview_candidate_row_json->'itemisation', '[]'::jsonb)
            WHEN jsonb_typeof(v_preview_baseline_json->'itemisation') = 'array'
              THEN COALESCE(v_preview_baseline_json->'itemisation', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        );

      INSERT INTO pg_temp.tmp_sync_preview_candidates (
        candidate_id,
        candidate_json
      )
      VALUES (
        v_preview_candidate_loop_id,
        v_preview_candidate_row_json
      )
      ON CONFLICT (candidate_id) DO UPDATE
      SET candidate_json = EXCLUDED.candidate_json;
    END LOOP;
  ELSE
    v_preview_scope_context_json := public.pay_preview_build_context(
      p_pay_date => p_pay_date,
      p_week_ending_cutoff => p_week_ending_cutoff,
      p_actor_user_id => p_actor_user_id,
      p_candidate_id => NULL::uuid,
      p_client_id => p_client_filter_single,
      p_preview_decisions_json => v_preview_decisions_json
        || jsonb_build_object(
          'preview_context_mode', 'PAGE',
          'scope_limit', 100
        )
    );

    v_preview_scope_candidate_ids_json := CASE
      WHEN jsonb_typeof(v_preview_scope_context_json->'candidate_ids') = 'array'
        THEN COALESCE(v_preview_scope_context_json->'candidate_ids', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    IF LOWER(BTRIM(COALESCE(v_preview_scope_context_json->>'has_more', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_SCOPE_REQUIRES_EXPLICIT_IDS'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_CANDIDATE_SCOPE_REQUIRES_EXPLICIT_IDS',
                'message', 'The bounded candidate page is incomplete. Supply explicit candidate_ids rather than accepting a partial all-candidate reconciliation.',
                'pay_channel_scope', v_scope,
                'page_count', COALESCE(v_preview_scope_context_json->>'page_count', '0'),
                'limit', COALESCE(v_preview_scope_context_json->>'limit', '100'),
                'next_cursor_json', COALESCE(v_preview_scope_context_json->'next_cursor_json', 'null'::jsonb)
              )::text;
    END IF;

    FOR v_preview_candidate_loop_id IN
      SELECT DISTINCT scoped_candidate.value::uuid
      FROM jsonb_array_elements_text(COALESCE(v_preview_scope_candidate_ids_json, '[]'::jsonb)) AS scoped_candidate(value)
      WHERE scoped_candidate.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ORDER BY scoped_candidate.value::uuid
    LOOP
      v_preview_context_json := public.pay_preview_build_context(
        p_pay_date => p_pay_date,
        p_week_ending_cutoff => p_week_ending_cutoff,
        p_actor_user_id => p_actor_user_id,
        p_candidate_id => v_preview_candidate_loop_id,
        p_client_id => p_client_filter_single,
        p_preview_decisions_json => v_preview_decisions_json
      )
      || jsonb_build_object(
        'overpayment_sync_mode', true,
        'emit_raw_overpayment_components', true,
        'pay_channel_scope', v_scope,
        'force_include_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
        'exclude_timesheet_ids', COALESCE(to_jsonb(p_exclude_timesheet_ids), '[]'::jsonb)
      );

      v_preview_baseline_json := public.pay_preview_candidate_build_summary_fragment(
        p_context_json => v_preview_context_json,
        p_candidate_id => v_preview_candidate_loop_id
      );

      v_preview_candidate_row_json := CASE
        WHEN jsonb_typeof(v_preview_baseline_json->'candidate_row') = 'object'
          THEN COALESCE(v_preview_baseline_json->'candidate_row', '{}'::jsonb)
        ELSE '{}'::jsonb
      END;

      IF NULLIF(BTRIM(COALESCE(
           v_preview_candidate_row_json->>'candidate_id',
           v_preview_baseline_json->>'candidate_id',
           ''
         )), '') IS DISTINCT FROM v_preview_candidate_loop_id::text THEN
        RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE',
                  'candidate_id', v_preview_candidate_loop_id::text,
                  'pay_channel_scope', v_scope
                )::text;
      END IF;

      IF UPPER(BTRIM(COALESCE(v_preview_candidate_row_json->>'current_pay_method', ''))) = v_scope THEN
        v_preview_candidate_row_json := v_preview_candidate_row_json
          || jsonb_build_object(
            'candidate_id', v_preview_candidate_loop_id::text,
            'itemisation', CASE
              WHEN jsonb_typeof(v_preview_candidate_row_json->'itemisation') = 'array'
                THEN COALESCE(v_preview_candidate_row_json->'itemisation', '[]'::jsonb)
              WHEN jsonb_typeof(v_preview_baseline_json->'itemisation') = 'array'
                THEN COALESCE(v_preview_baseline_json->'itemisation', '[]'::jsonb)
              ELSE '[]'::jsonb
            END
          );

        INSERT INTO pg_temp.tmp_sync_preview_candidates (
          candidate_id,
          candidate_json
        )
        VALUES (
          v_preview_candidate_loop_id,
          v_preview_candidate_row_json
        )
        ON CONFLICT (candidate_id) DO UPDATE
        SET candidate_json = EXCLUDED.candidate_json;
      END IF;
    END LOOP;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_preview_candidate_count
  FROM pg_temp.tmp_sync_preview_candidates AS preview_candidate;

  WITH requested_candidates AS (
    SELECT DISTINCT requested_candidate.candidate_id
    FROM unnest(COALESCE(p_candidate_ids, ARRAY[]::uuid[])) AS requested_candidate(candidate_id)
    WHERE requested_candidate.candidate_id IS NOT NULL
  ), missing_candidates AS (
    SELECT requested_candidates.candidate_id
    FROM requested_candidates
    WHERE NOT EXISTS (
      SELECT 1
      FROM pg_temp.tmp_sync_preview_candidates AS preview_candidate
      WHERE preview_candidate.candidate_id = requested_candidates.candidate_id
    )
  )
  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(missing_candidates.candidate_id::text ORDER BY missing_candidates.candidate_id), '[]'::jsonb)
  INTO v_missing_candidate_count,
       v_missing_candidate_ids_json
  FROM missing_candidates;

  IF COALESCE(v_missing_candidate_count, 0) > 0 THEN
    RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_SYNC_OVERPAYMENTS_PREVIEW_CANDIDATE_COVERAGE_INCOMPLETE',
              'pay_channel_scope', v_scope,
              'requested_candidate_count', v_requested_candidate_count,
              'preview_candidate_count', v_preview_candidate_count,
              'missing_candidate_ids', COALESCE(v_missing_candidate_ids_json, '[]'::jsonb)
            )::text;
  END IF;

  insert into pg_temp.tmp_sync_timesheet_case_candidates (
    candidate_id,
    timesheet_id,
    client_id,
    linked_shift_date,
    corrected_amount_ex,
    baseline_signature,
    candidate_pay_method,
    case_is_blocked,
    needs_lifecycle_tracking,
    overpayment_amount_ex,
    underpayment_amount_ex,
    desired_case_type,
    desired_advance_kind,
    desired_reason,
    source_original_paid_amount,
    source_corrected_paid_amount,
    components_sync_json
  )
  with candidate_rows as (
    select
      preview_candidate.candidate_id,
      upper(btrim(coalesce(preview_candidate.candidate_json->>'current_pay_method', ''))) as candidate_pay_method,
      case
        when jsonb_typeof(preview_candidate.candidate_json->'itemisation') = 'array'
          then coalesce(preview_candidate.candidate_json->'itemisation', '[]'::jsonb)
        else '[]'::jsonb
      end as itemisation_json
    from pg_temp.tmp_sync_preview_candidates as preview_candidate
    where upper(btrim(coalesce(preview_candidate.candidate_json->>'current_pay_method', ''))) = v_scope
      and (
        coalesce(array_length(p_candidate_ids, 1), 0) = 0
        or preview_candidate.candidate_id = any(p_candidate_ids)
      )
  ),
  itemisation_timesheet_rows as (
    select
      cr.candidate_id,
      cr.candidate_pay_method,
      itm.value as item_json,
      nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid as timesheet_id,
      nullif(btrim(coalesce(itm.value->>'client_id', '')), '')::uuid as client_id,
      round(coalesce(nullif(itm.value->>'amount_ex_vat', '')::numeric, 0), 2)::numeric(12,2) as corrected_amount_ex,
      coalesce(itm.value->>'case_is_blocked', 'false')::boolean as case_is_blocked,
      coalesce(itm.value->'case_components', '[]'::jsonb) as case_components_json
    from candidate_rows cr
    cross join lateral jsonb_array_elements(coalesce(cr.itemisation_json, '[]'::jsonb)) as itm(value)
    where coalesce(itm.value->>'line_type', '') = 'TIMESHEET_PAYMENT'
      and nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '') is not null
      and (
        coalesce(array_length(p_force_include_timesheet_ids, 1), 0) = 0
        or nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid = any(p_force_include_timesheet_ids)
        or p_force_include_timesheet_ids is null
      )
      and not (
        coalesce(array_length(p_exclude_timesheet_ids, 1), 0) > 0
        and nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid = any(p_exclude_timesheet_ids)
      )
  ),
  authoritative_live_truth_totals AS (
    SELECT
      entitlement_component.timesheet_id,
      ROUND(COALESCE(SUM(COALESCE(entitlement_component.truth_ex_vat, 0)), 0), 2)::numeric(12,2) AS corrected_amount_ex
    FROM public._pay_current_timesheet_entitlement_components(
      CASE
        WHEN COALESCE(v_authoritative_timesheet_scope, false)
          THEN COALESCE(p_force_include_timesheet_ids, ARRAY[]::uuid[])
        ELSE ARRAY[]::uuid[]
      END
    ) AS entitlement_component
    GROUP BY entitlement_component.timesheet_id
  ),
  authoritative_negative_timesheet_rows AS (
    SELECT
      raw_timesheet.candidate_id,
      raw_timesheet.candidate_pay_method,
      jsonb_build_object(
        'line_type', 'TIMESHEET_PAYMENT',
        'timesheet_id', raw_timesheet.timesheet_id::text,
        'client_id', CASE WHEN raw_timesheet.client_id IS NULL THEN NULL ELSE raw_timesheet.client_id::text END,
        'amount_ex_vat', ROUND(COALESCE(live_truth.corrected_amount_ex, 0), 2),
        'case_is_blocked', raw_timesheet.case_is_blocked,
        'case_components', raw_timesheet.case_components_json,
        'source_authority', 'AUTHORITATIVE_NEGATIVE_COMPONENT_FALLBACK'
      ) AS item_json,
      raw_timesheet.timesheet_id,
      raw_timesheet.client_id,
      ROUND(COALESCE(live_truth.corrected_amount_ex, 0), 2)::numeric(12,2) AS corrected_amount_ex,
      raw_timesheet.case_is_blocked,
      raw_timesheet.case_components_json
    FROM pg_temp.tmp_sync_raw_negative_timesheet_rows AS raw_timesheet
    LEFT JOIN authoritative_live_truth_totals AS live_truth
      ON live_truth.timesheet_id = raw_timesheet.timesheet_id
    WHERE COALESCE(v_authoritative_timesheet_scope, false)
      AND raw_timesheet.candidate_id = v_authoritative_candidate_id
      AND EXISTS (
        SELECT 1
        FROM pg_temp.tmp_sync_authoritative_negative_components AS authoritative_component
        WHERE authoritative_component.timesheet_id = raw_timesheet.timesheet_id
      )
  ),
  timesheet_item_rows AS (
    SELECT
      itemisation_row.candidate_id,
      itemisation_row.candidate_pay_method,
      itemisation_row.item_json,
      itemisation_row.timesheet_id,
      itemisation_row.client_id,
      itemisation_row.corrected_amount_ex,
      itemisation_row.case_is_blocked,
      itemisation_row.case_components_json
    FROM itemisation_timesheet_rows AS itemisation_row
    WHERE NOT EXISTS (
      SELECT 1
      FROM authoritative_negative_timesheet_rows AS authoritative_row
      WHERE authoritative_row.candidate_id = itemisation_row.candidate_id
        AND authoritative_row.timesheet_id = itemisation_row.timesheet_id
    )

    UNION ALL

    SELECT
      authoritative_row.candidate_id,
      authoritative_row.candidate_pay_method,
      authoritative_row.item_json,
      authoritative_row.timesheet_id,
      authoritative_row.client_id,
      authoritative_row.corrected_amount_ex,
      authoritative_row.case_is_blocked,
      authoritative_row.case_components_json
    FROM authoritative_negative_timesheet_rows AS authoritative_row
  ),
  active_settled_basis AS (
    SELECT
      distinct_timesheet_rows.timesheet_id,
      active_components.active_settled_component_count,
      active_components.active_settled_amount_ex,
      active_components.active_settled_signature
    FROM (
      SELECT DISTINCT timesheet_item_rows.timesheet_id
      FROM timesheet_item_rows
      WHERE timesheet_item_rows.timesheet_id IS NOT NULL
    ) AS distinct_timesheet_rows
    LEFT JOIN LATERAL (
      SELECT
        count(*)::integer AS active_settled_component_count,
        round(COALESCE(sum(COALESCE(active_settled_rows.amount_ex_vat, 0)), 0), 2)::numeric(12,2) AS active_settled_amount_ex,
        md5(COALESCE(jsonb_agg(
          jsonb_build_object(
            'key_type', active_settled_rows.key_type,
            'key_value', active_settled_rows.key_value,
            'amount_ex_vat', round(COALESCE(active_settled_rows.amount_ex_vat, 0), 2),
            'amount_inc_vat', round(COALESCE(active_settled_rows.amount_inc_vat, 0), 2)
          )
          ORDER BY active_settled_rows.key_type, active_settled_rows.key_value
        )::text, '[]')) AS active_settled_signature
      FROM public._pay_active_settled_components(ARRAY[distinct_timesheet_rows.timesheet_id]::uuid[]) AS active_settled_rows
    ) AS active_components
      ON true
  ),
  timesheet_item_with_baseline as (
    select
      tir.candidate_id,
      tir.timesheet_id,
      tir.client_id,
      coalesce(ts.worked_start_iso::date, ts.scheduled_start_iso::date, ts.week_ending_date) as linked_shift_date,
      tir.corrected_amount_ex,
      case
        when COALESCE(active_settled_basis.active_settled_component_count, 0) > 0
          then active_settled_basis.active_settled_signature
        else coalesce(
          tps.last_settled_signature,
          md5(coalesce(tps.last_settled_snapshot_json::text, '{}'))
        )
      end as baseline_signature,
      tir.candidate_pay_method,
      tir.case_is_blocked,
      tir.case_components_json
    from timesheet_item_rows tir
    join public.timesheets ts
      on ts.timesheet_id = tir.timesheet_id
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = tir.timesheet_id
    left join active_settled_basis
      on active_settled_basis.timesheet_id = tir.timesheet_id
  ),
  timesheet_exploded_components as (
    select
      tiwb.candidate_id,
      tiwb.timesheet_id,
      tiwb.client_id,
      tiwb.linked_shift_date,
      tiwb.corrected_amount_ex,
      tiwb.baseline_signature,
      tiwb.candidate_pay_method,
      tiwb.case_is_blocked,
      comp.value as component_json,
      comp.ordinality::integer as component_order,
      UPPER(NULLIF(BTRIM(COALESCE(comp.value->>'component_key_type', '')), '')) AS component_key_type,
      NULLIF(BTRIM(COALESCE(comp.value->>'component_key_value', '')), '') AS component_key_value,
      round(coalesce(nullif(comp.value->>'component_amount_ex_vat', '')::numeric, 0), 2)::numeric(12,2) as component_amount_ex
    from timesheet_item_with_baseline tiwb
    cross join lateral jsonb_array_elements(coalesce(tiwb.case_components_json, '[]'::jsonb)) with ordinality as comp(value, ordinality)
  ),
  finance_component_movements_raw as (
    select
      pa.candidate_id,
      pa.linked_timesheet_id as timesheet_id,
      upper(btrim(coalesce(pfc.component_key_type, pbi.frozen_component_key_type, ''))) as component_key_type,
      nullif(btrim(coalesce(pfc.component_key_value, pbi.frozen_component_key_value, '')), '') as component_key_value,
      upper(btrim(coalesce(pbi.item_type, ''))) as item_type,
      round(abs(coalesce(
        par.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(pbi.id),
        pbi.frozen_source_amount,
        pbi.amount_ex_vat,
        pbi.amount_inc_vat,
        par.reserved_amount,
        0
      )), 2)::numeric(12,2) as movement_amount_ex,
      true as is_settled_movement,
      false as is_active_reservation
    from public.pay_advances pa
    join public.pay_batch_items pbi
      on pbi.finance_case_id = pa.id
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    left join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
    left join public.pay_advance_reservations par
      on par.pay_batch_item_id = pbi.id
    left join public.pay_finance_case_components pfc
      on pfc.id = coalesce(par.finance_component_id, pbi.finance_component_id)
    where pa.case_type in ('OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
      and pa.linked_timesheet_id is not null
      and coalesce(pbi.is_voided, false) = false
      and upper(btrim(coalesce(pbi.item_type, ''))) in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
      and (
        upper(btrim(coalesce(pbc.settlement_status, ''))) = 'SETTLED'
        or pbc.settled_at_utc is not null
        or upper(btrim(coalesce(pbt.status, ''))) = 'COMPLETED'
        or pbt.completed_at_utc is not null
        or upper(btrim(coalesce(par.status, ''))) = 'SETTLED'
        or par.settled_at_utc is not null
      )
      and not exists (
        select 1
        from public.pay_payment_correction_items pci
        where pci.pay_batch_item_id = pbi.id
          and pci.status = 'APPLIED'
          and pci.correction_item_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      )

    union all

    select
      pa.candidate_id,
      pa.linked_timesheet_id as timesheet_id,
      upper(btrim(coalesce(pfc.component_key_type, pbi.frozen_component_key_type, ''))) as component_key_type,
      nullif(btrim(coalesce(pfc.component_key_value, pbi.frozen_component_key_value, '')), '') as component_key_value,
      upper(btrim(coalesce(
        pbi.item_type,
        case
          when pa.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 'OVERPAYMENT_RECOVERY'
          when pa.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum then 'UNDERPAYMENT_PAYMENT'
          else null
        end,
        ''
      ))) as item_type,
      round(abs(coalesce(
        par.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(pbi.id),
        pbi.frozen_source_amount,
        pbi.amount_ex_vat,
        pbi.amount_inc_vat,
        par.reserved_amount,
        0
      )), 2)::numeric(12,2) as movement_amount_ex,
      false as is_settled_movement,
      true as is_active_reservation
    from public.pay_advance_reservations par
    join public.pay_advances pa
      on pa.id = par.finance_case_id
    left join public.pay_batch_items pbi
      on pbi.id = par.pay_batch_item_id
    left join public.pay_finance_case_components pfc
      on pfc.id = coalesce(par.finance_component_id, pbi.finance_component_id)
    where pa.case_type in ('OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
      and pa.linked_timesheet_id is not null
      and upper(btrim(coalesce(par.status, ''))) in ('RESERVED', 'COMMITTED')
      and par.released_at_utc is null
      and par.settled_at_utc is null
      and (pbi.id is null or coalesce(pbi.is_voided, false) = false)
      and upper(btrim(coalesce(
        pbi.item_type,
        case
          when pa.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 'OVERPAYMENT_RECOVERY'
          when pa.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum then 'UNDERPAYMENT_PAYMENT'
          else null
        end,
        ''
      ))) in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
  ),
  finance_component_movements as (
    select
      fcmr.candidate_id,
      fcmr.timesheet_id,
      fcmr.component_key_type,
      fcmr.component_key_value,
      round(coalesce(sum(fcmr.movement_amount_ex) filter (where fcmr.item_type = 'OVERPAYMENT_RECOVERY' and fcmr.is_settled_movement), 0), 2)::numeric(12,2) as settled_overpayment_recovery_ex,
      round(coalesce(sum(fcmr.movement_amount_ex) filter (where fcmr.item_type = 'OVERPAYMENT_RECOVERY' and fcmr.is_active_reservation), 0), 2)::numeric(12,2) as reserved_overpayment_recovery_ex,
      round(coalesce(sum(fcmr.movement_amount_ex) filter (where fcmr.item_type = 'UNDERPAYMENT_PAYMENT' and fcmr.is_settled_movement), 0), 2)::numeric(12,2) as settled_underpayment_payment_ex,
      round(coalesce(sum(fcmr.movement_amount_ex) filter (where fcmr.item_type = 'UNDERPAYMENT_PAYMENT' and fcmr.is_active_reservation), 0), 2)::numeric(12,2) as reserved_underpayment_payment_ex,
      true as has_finance_movement
    from finance_component_movements_raw fcmr
    where fcmr.component_key_type is not null
      and fcmr.component_key_value is not null
    group by fcmr.candidate_id, fcmr.timesheet_id, fcmr.component_key_type, fcmr.component_key_value
  ),
  timesheet_exploded_with_signed_balance as (
    select
      tec.*,
      coalesce(fcm.settled_overpayment_recovery_ex, 0) as settled_overpayment_recovery_ex,
      coalesce(fcm.reserved_overpayment_recovery_ex, 0) as reserved_overpayment_recovery_ex,
      coalesce(fcm.settled_underpayment_payment_ex, 0) as settled_underpayment_payment_ex,
      coalesce(fcm.reserved_underpayment_payment_ex, 0) as reserved_underpayment_payment_ex,
      coalesce(fcm.has_finance_movement, false) as has_finance_movement,
      case
        /* The authoritative pre-draft component amount is already the canonical
           live-truth minus settled-baseline minus active-reservation balance.
           Reapplying finance movements here would double count them. */
        when upper(btrim(coalesce(
               tec.component_json->>'overpayment_component_authority',
               ''
             ))) = 'PRE_DRAFT_LIVE_TRUTH'
          then round(coalesce(tec.component_amount_ex, 0), 2)::numeric(12,2)
        else round(
          coalesce(tec.component_amount_ex, 0)
          + coalesce(fcm.settled_overpayment_recovery_ex, 0)
          + coalesce(fcm.reserved_overpayment_recovery_ex, 0)
          - coalesce(fcm.settled_underpayment_payment_ex, 0)
          - coalesce(fcm.reserved_underpayment_payment_ex, 0),
          2
        )::numeric(12,2)
      end as signed_component_amount_ex
    from timesheet_exploded_components tec
    left join finance_component_movements fcm
      on fcm.candidate_id = tec.candidate_id
     and fcm.timesheet_id = tec.timesheet_id
     and fcm.component_key_type = tec.component_key_type
     and fcm.component_key_value = tec.component_key_value
  ),
  timesheet_case_rollup as (
    select
      tec.candidate_id,
      tec.timesheet_id,
      min(tec.client_id::text)::uuid as client_id,
      min(tec.linked_shift_date) as linked_shift_date,
      min(tec.corrected_amount_ex) as corrected_amount_ex,
      min(tec.baseline_signature) as baseline_signature,
      min(tec.candidate_pay_method) as candidate_pay_method,
      bool_or(tec.case_is_blocked) as case_is_blocked,
      bool_or(coalesce(tec.has_finance_movement, false)) as has_finance_movement,
      round(coalesce(sum(case when tec.signed_component_amount_ex < 0 and tec.component_key_type is not null and tec.component_key_value is not null then abs(tec.signed_component_amount_ex) else 0 end), 0), 2)::numeric(12,2) as overpayment_amount_ex,
      round(coalesce(sum(case when tec.signed_component_amount_ex > 0 and tec.component_key_type is not null and tec.component_key_value is not null then tec.signed_component_amount_ex else 0 end), 0), 2)::numeric(12,2) as underpayment_amount_ex,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'candidate_id', tec.candidate_id::text,
            'client_id', case when tec.client_id is null then null else tec.client_id::text end,
            'linked_timesheet_id', tec.timesheet_id::text,
            'source_family_key', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_family_key', '')), ''), 'timesheet:' || tec.timesheet_id::text),
            'component_key_type', tec.component_key_type,
            'component_key_value', tec.component_key_value,
            'classification', CASE
              WHEN upper(btrim(coalesce(tec.component_json->>'classification', ''))) IN ('TAXABLE_CHANNEL_SENSITIVE','REIMBURSEMENT_GROSS_FIXED','NET_PAY_FIXED_RECOVERY')
                THEN upper(btrim(coalesce(tec.component_json->>'classification', '')))
              WHEN tec.component_key_type = 'EXPENSE_CODE'
                THEN 'REIMBURSEMENT_GROSS_FIXED'
              ELSE 'TAXABLE_CHANNEL_SENSITIVE'
            END,
            'source_pay_method', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_pay_method', '')), ''), tec.candidate_pay_method),
            'current_target_pay_method', tec.candidate_pay_method,
            'source_basis_json', CASE
              WHEN jsonb_typeof(tec.component_json->'source_basis_json') = 'object'
                   AND coalesce(tec.component_json->'source_basis_json', '{}'::jsonb) <> '{}'::jsonb
                THEN tec.component_json->'source_basis_json'
              ELSE jsonb_strip_nulls(jsonb_build_object(
                'linked_timesheet_id', tec.timesheet_id::text,
                'source_family_key', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_family_key', '')), ''), 'timesheet:' || tec.timesheet_id::text),
                'component_key_type', tec.component_key_type,
                'component_key_value', tec.component_key_value,
                'source_pay_method', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_pay_method', '')), ''), tec.candidate_pay_method),
                'component_amount_ex_vat', abs(tec.signed_component_amount_ex)
              ))
            END,
            'source_amount', abs(tec.signed_component_amount_ex),
            'overpayment_component_authority', NULLIF(UPPER(BTRIM(COALESCE(
              tec.component_json->>'overpayment_component_authority',
              ''
            ))), ''),
            'remaining_source_amount', CASE
              WHEN upper(btrim(coalesce(
                     tec.component_json->>'overpayment_component_authority',
                     ''
                   ))) = 'PRE_DRAFT_LIVE_TRUTH'
                THEN abs(tec.signed_component_amount_ex)
              ELSE NULL
            END,
            'allocation_priority_group', case when tec.component_json->>'classification' = 'TAXABLE_CHANNEL_SENSITIVE' then 0 else 1 end,
            'allocation_priority_order', tec.component_order
          )
          order by coalesce(tec.component_json->>'classification',''), coalesce(tec.component_json->>'component_key_type',''), coalesce(tec.component_json->>'component_key_value','')
        ) filter (where tec.signed_component_amount_ex < 0 and tec.component_key_type is not null and tec.component_key_value is not null),
        '[]'::jsonb
      ) as overpayment_components_json,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'candidate_id', tec.candidate_id::text,
            'client_id', case when tec.client_id is null then null else tec.client_id::text end,
            'linked_timesheet_id', tec.timesheet_id::text,
            'source_family_key', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_family_key', '')), ''), 'timesheet:' || tec.timesheet_id::text),
            'component_key_type', tec.component_key_type,
            'component_key_value', tec.component_key_value,
            'classification', CASE
              WHEN upper(btrim(coalesce(tec.component_json->>'classification', ''))) IN ('TAXABLE_CHANNEL_SENSITIVE','REIMBURSEMENT_GROSS_FIXED','NET_PAY_FIXED_RECOVERY')
                THEN upper(btrim(coalesce(tec.component_json->>'classification', '')))
              WHEN tec.component_key_type = 'EXPENSE_CODE'
                THEN 'REIMBURSEMENT_GROSS_FIXED'
              ELSE 'TAXABLE_CHANNEL_SENSITIVE'
            END,
            'source_pay_method', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_pay_method', '')), ''), tec.candidate_pay_method),
            'current_target_pay_method', tec.candidate_pay_method,
            'source_basis_json', CASE
              WHEN jsonb_typeof(tec.component_json->'source_basis_json') = 'object'
                   AND coalesce(tec.component_json->'source_basis_json', '{}'::jsonb) <> '{}'::jsonb
                THEN tec.component_json->'source_basis_json'
              ELSE jsonb_strip_nulls(jsonb_build_object(
                'linked_timesheet_id', tec.timesheet_id::text,
                'source_family_key', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_family_key', '')), ''), 'timesheet:' || tec.timesheet_id::text),
                'component_key_type', tec.component_key_type,
                'component_key_value', tec.component_key_value,
                'source_pay_method', coalesce(nullif(btrim(coalesce(tec.component_json->>'source_pay_method', '')), ''), tec.candidate_pay_method),
                'component_amount_ex_vat', abs(tec.signed_component_amount_ex)
              ))
            END,
            'source_amount', abs(tec.signed_component_amount_ex),
            'overpayment_component_authority', NULLIF(UPPER(BTRIM(COALESCE(
              tec.component_json->>'overpayment_component_authority',
              ''
            ))), ''),
            'remaining_source_amount', CASE
              WHEN upper(btrim(coalesce(
                     tec.component_json->>'overpayment_component_authority',
                     ''
                   ))) = 'PRE_DRAFT_LIVE_TRUTH'
                THEN abs(tec.signed_component_amount_ex)
              ELSE NULL
            END,
            'allocation_priority_group', case when tec.component_json->>'classification' = 'TAXABLE_CHANNEL_SENSITIVE' then 0 else 1 end,
            'allocation_priority_order', tec.component_order
          )
          order by coalesce(tec.component_json->>'classification',''), coalesce(tec.component_json->>'component_key_type',''), coalesce(tec.component_json->>'component_key_value','')
        ) filter (where tec.signed_component_amount_ex > 0 and tec.component_key_type is not null and tec.component_key_value is not null),
        '[]'::jsonb
      ) as underpayment_components_json
    from timesheet_exploded_with_signed_balance tec
    group by tec.candidate_id, tec.timesheet_id
  ),
  active_linked_underpayment_cases as (
    select
      pa.candidate_id,
      pa.linked_timesheet_id as timesheet_id,
      true as has_existing_active_underpayment_case
    from public.pay_advances pa
    where pa.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum
      and pa.linked_timesheet_id is not null
      and upper(coalesce(pa.status::text, '')) = 'ACTIVE'
      and round(coalesce(pa.outstanding_amount, 0), 2) > 0
    group by pa.candidate_id, pa.linked_timesheet_id
  ),
  timesheet_case_candidates as (
    select
      tcr.candidate_id,
      tcr.timesheet_id,
      tcr.client_id,
      tcr.linked_shift_date,
      tcr.corrected_amount_ex,
      tcr.baseline_signature,
      tcr.candidate_pay_method,
      tcr.case_is_blocked,
      (
        tcr.overpayment_amount_ex > 0
        or (
          tcr.underpayment_amount_ex > 0
          and (
            tcr.case_is_blocked = true
            or coalesce(aluc.has_existing_active_underpayment_case, false) = true
            or coalesce(tcr.has_finance_movement, false) = true
          )
        )
      ) as needs_lifecycle_tracking,
      tcr.overpayment_amount_ex,
      tcr.underpayment_amount_ex,
      case
        when tcr.overpayment_amount_ex > 0 then 'OVERPAYMENT'::public.pay_finance_case_type_enum
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then 'UNDERPAYMENT'::public.pay_finance_case_type_enum
        else null::public.pay_finance_case_type_enum
      end as desired_case_type,
      case
        when tcr.overpayment_amount_ex > 0 then 'OVERPAYMENT'::public.pay_advance_kind_enum
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then 'UNDERPAYMENT'::public.pay_advance_kind_enum
        else null::public.pay_advance_kind_enum
      end as desired_advance_kind,
      case
        when tcr.overpayment_amount_ex > 0 then 'OVERPAYMENT'::public.pay_advance_reason_enum
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then 'UNDERPAYMENT'::public.pay_advance_reason_enum
        else null::public.pay_advance_reason_enum
      end as desired_reason,
      case
        when tcr.overpayment_amount_ex > 0 then round(tcr.corrected_amount_ex + tcr.overpayment_amount_ex, 2)::numeric(12,2)
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then round(tcr.corrected_amount_ex - tcr.underpayment_amount_ex, 2)::numeric(12,2)
        else null::numeric(12,2)
      end as source_original_paid_amount,
      case
        when tcr.overpayment_amount_ex > 0 then round(tcr.corrected_amount_ex, 2)::numeric(12,2)
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then round(tcr.corrected_amount_ex, 2)::numeric(12,2)
        else null::numeric(12,2)
      end as source_corrected_paid_amount,
      case
        when tcr.overpayment_amount_ex > 0 then tcr.overpayment_components_json
        when tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         ) then tcr.underpayment_components_json
        else '[]'::jsonb
      end as components_sync_json,
      1 as candidate_priority
    from timesheet_case_rollup tcr
    left join active_linked_underpayment_cases aluc
      on aluc.candidate_id = tcr.candidate_id
     and aluc.timesheet_id = tcr.timesheet_id
    where tcr.overpayment_amount_ex > 0
       or (
         tcr.underpayment_amount_ex > 0
         and (
           tcr.case_is_blocked = true
           or coalesce(aluc.has_existing_active_underpayment_case, false) = true
           or coalesce(tcr.has_finance_movement, false) = true
         )
       )
  ),
  finance_case_item_rows as (
    select
      cr.candidate_id,
      cr.candidate_pay_method,
      itm.value as item_json,
      nullif(btrim(coalesce(itm.value->>'finance_case_id', '')), '')::uuid as finance_case_id,
      nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid as timesheet_id,
      nullif(btrim(coalesce(itm.value->>'client_id', '')), '')::uuid as client_id,
      nullif(btrim(coalesce(itm.value->>'linked_shift_date', '')), '')::date as linked_shift_date,
      upper(btrim(coalesce(itm.value->>'case_type', ''))) as case_type_text,
      coalesce(itm.value->>'case_is_blocked', 'false')::boolean as case_is_blocked,
      round(abs(coalesce(nullif(itm.value->>'amount_ex_vat', '')::numeric, 0)), 2)::numeric(12,2) as line_amount_ex,
      coalesce(itm.value->'case_components', '[]'::jsonb) as case_components_json
    from candidate_rows cr
    cross join lateral jsonb_array_elements(coalesce(cr.itemisation_json, '[]'::jsonb)) as itm(value)
    where coalesce(itm.value->>'line_type', '') in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
      and nullif(btrim(coalesce(itm.value->>'finance_case_id', '')), '') is not null
      and nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '') is not null
      and (
        coalesce(array_length(p_force_include_timesheet_ids, 1), 0) = 0
        or nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid = any(p_force_include_timesheet_ids)
        or p_force_include_timesheet_ids is null
      )
      and not (
        coalesce(array_length(p_exclude_timesheet_ids, 1), 0) > 0
        and nullif(btrim(coalesce(itm.value->>'timesheet_id', '')), '')::uuid = any(p_exclude_timesheet_ids)
      )
  ),
  finance_case_candidates_raw as (
    select
      fcir.candidate_id,
      fcir.timesheet_id,
      coalesce(fcir.client_id, pa.client_id) as client_id,
      coalesce(fcir.linked_shift_date, pa.linked_shift_date) as linked_shift_date,
      round(coalesce(pa.source_corrected_paid_amount, 0), 2)::numeric(12,2) as corrected_amount_ex,
      pa.baseline_signature,
      fcir.candidate_pay_method,
      fcir.case_is_blocked,
      true as needs_lifecycle_tracking,
      case when pa.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then round(coalesce(pa.original_amount, 0), 2)::numeric(12,2) else 0::numeric(12,2) end as overpayment_amount_ex,
      case when pa.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum then round(coalesce(pa.original_amount, 0), 2)::numeric(12,2) else 0::numeric(12,2) end as underpayment_amount_ex,
      pa.case_type as desired_case_type,
      pa.advance_kind as desired_advance_kind,
      pa.reason as desired_reason,
      round(coalesce(pa.source_original_paid_amount, 0), 2)::numeric(12,2) as source_original_paid_amount,
      round(coalesce(pa.source_corrected_paid_amount, 0), 2)::numeric(12,2) as source_corrected_paid_amount,
      coalesce(fcir.case_components_json, '[]'::jsonb) as components_sync_json,
      2 as candidate_priority
    from finance_case_item_rows fcir
    join public.pay_advances pa
      on pa.id = fcir.finance_case_id
    where pa.case_type in ('OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
      and pa.linked_timesheet_id = fcir.timesheet_id
      and upper(coalesce(pa.status::text, '')) = 'ACTIVE'
      and round(coalesce(pa.outstanding_amount, 0), 2) > 0
  ),
  combined_case_candidates as (
    select
      tcc.candidate_id,
      tcc.timesheet_id,
      tcc.client_id,
      tcc.linked_shift_date,
      tcc.corrected_amount_ex,
      tcc.baseline_signature,
      tcc.candidate_pay_method,
      tcc.case_is_blocked,
      tcc.needs_lifecycle_tracking,
      tcc.overpayment_amount_ex,
      tcc.underpayment_amount_ex,
      tcc.desired_case_type,
      tcc.desired_advance_kind,
      tcc.desired_reason,
      tcc.source_original_paid_amount,
      tcc.source_corrected_paid_amount,
      tcc.components_sync_json,
      tcc.candidate_priority
    from timesheet_case_candidates tcc

    union all

    select
      fccr.candidate_id,
      fccr.timesheet_id,
      fccr.client_id,
      fccr.linked_shift_date,
      fccr.corrected_amount_ex,
      fccr.baseline_signature,
      fccr.candidate_pay_method,
      fccr.case_is_blocked,
      fccr.needs_lifecycle_tracking,
      fccr.overpayment_amount_ex,
      fccr.underpayment_amount_ex,
      fccr.desired_case_type,
      fccr.desired_advance_kind,
      fccr.desired_reason,
      fccr.source_original_paid_amount,
      fccr.source_corrected_paid_amount,
      fccr.components_sync_json,
      fccr.candidate_priority
    from finance_case_candidates_raw fccr
  ),
  deduped_case_candidates as (
    select
      ccc.candidate_id,
      ccc.timesheet_id,
      ccc.client_id,
      ccc.linked_shift_date,
      ccc.corrected_amount_ex,
      ccc.baseline_signature,
      ccc.candidate_pay_method,
      ccc.case_is_blocked,
      ccc.needs_lifecycle_tracking,
      ccc.overpayment_amount_ex,
      ccc.underpayment_amount_ex,
      ccc.desired_case_type,
      ccc.desired_advance_kind,
      ccc.desired_reason,
      ccc.source_original_paid_amount,
      ccc.source_corrected_paid_amount,
      ccc.components_sync_json
    from (
      select
        ccc_inner.*,
        row_number() over (
          partition by ccc_inner.candidate_id, ccc_inner.timesheet_id
          order by
            ccc_inner.candidate_priority asc,
            case when ccc_inner.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 0 else 1 end,
            case when ccc_inner.needs_lifecycle_tracking then 0 else 1 end
        ) as rn
      from combined_case_candidates ccc_inner
    ) ccc
    where ccc.rn = 1
  )
  select
    dcc.candidate_id,
    dcc.timesheet_id,
    dcc.client_id,
    dcc.linked_shift_date,
    dcc.corrected_amount_ex,
    dcc.baseline_signature,
    dcc.candidate_pay_method,
    dcc.case_is_blocked,
    dcc.needs_lifecycle_tracking,
    dcc.overpayment_amount_ex,
    dcc.underpayment_amount_ex,
    dcc.desired_case_type,
    dcc.desired_advance_kind,
    dcc.desired_reason,
    dcc.source_original_paid_amount,
    dcc.source_corrected_paid_amount,
    dcc.components_sync_json
  from deduped_case_candidates dcc
  where dcc.desired_case_type is not null;

  select count(*)::int into v_timesheet_case_count from pg_temp.tmp_sync_timesheet_case_candidates;
  select count(*)::int into v_overpayment_case_count from pg_temp.tmp_sync_timesheet_case_candidates where desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum;
  select count(*)::int into v_underpayment_case_count from pg_temp.tmp_sync_timesheet_case_candidates where desired_case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'candidate_id', t.candidate_id::text,
        'timesheet_id', t.timesheet_id::text,
        'client_id', case when t.client_id is null then null else t.client_id::text end,
        'linked_shift_date', case when t.linked_shift_date is null then null else t.linked_shift_date::text end,
        'baseline_signature', t.baseline_signature,
        'corrected_amount_ex', t.corrected_amount_ex,
        'case_is_blocked', t.case_is_blocked,
        'desired_case_type', case when t.desired_case_type is null then null else t.desired_case_type::text end,
        'needs_lifecycle_tracking', t.needs_lifecycle_tracking,
        'overpayment_amount_ex', t.overpayment_amount_ex,
        'underpayment_amount_ex', t.underpayment_amount_ex
      )
      order by t.candidate_id::text, t.timesheet_id::text
    ),
    '[]'::jsonb
  ) into v_case_candidates_json
  from pg_temp.tmp_sync_timesheet_case_candidates t;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'candidate_id', t.candidate_id::text,
        'timesheet_id', t.timesheet_id::text,
        'case_type', t.desired_case_type::text,
        'amount_ex', t.overpayment_amount_ex
      )
      order by t.candidate_id::text, t.timesheet_id::text
    ),
    '[]'::jsonb
  ) into v_overpayment_json
  from pg_temp.tmp_sync_timesheet_case_candidates t
  where t.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'candidate_id', t.candidate_id::text,
        'timesheet_id', t.timesheet_id::text,
        'case_type', t.desired_case_type::text,
        'amount_ex', t.underpayment_amount_ex
      )
      order by t.candidate_id::text, t.timesheet_id::text
    ),
    '[]'::jsonb
  ) into v_underpayment_json
  from pg_temp.tmp_sync_timesheet_case_candidates t
  where t.desired_case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum;

  for v_target_case_row in
    select
      t.*
    from pg_temp.tmp_sync_timesheet_case_candidates t
    order by t.candidate_id, t.timesheet_id
  loop
    v_selected_event_type := NULL;
    v_selected_reason := NULL;
    v_selected_note := NULL;
    v_case_update_count := 0;

    if v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then
      v_target_case_amount_ex := round(v_target_case_row.overpayment_amount_ex, 2)::numeric(12,2);
    elsif v_target_case_row.desired_case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum then
      v_target_case_amount_ex := round(v_target_case_row.underpayment_amount_ex, 2)::numeric(12,2);
    else
      v_target_case_amount_ex := null;
    end if;

    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(v_target_case_row.components_sync_json) = 'array'
            THEN v_target_case_row.components_sync_json
          ELSE '[]'::jsonb
        END
      ) AS authoritative_component(value)
      WHERE UPPER(BTRIM(COALESCE(
        authoritative_component.value->>'overpayment_component_authority',
        ''
      ))) = 'PRE_DRAFT_LIVE_TRUTH'
    )
    INTO v_target_amount_is_authoritative_outstanding;

    v_case_taxability := NULL;
    v_case_routing_kind := NULL;
    v_case_metadata_component_count := 0;
    IF v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum THEN
      SELECT
        COUNT(*)::integer,
        CASE
          WHEN COUNT(*) = 0 THEN NULL::public.pay_finance_taxability_enum
          WHEN BOOL_AND(COALESCE(normalized_component.classification = 'TAXABLE_CHANNEL_SENSITIVE', false)) THEN 'TAXABLE'::public.pay_finance_taxability_enum
          WHEN BOOL_AND(COALESCE(normalized_component.classification IN ('REIMBURSEMENT_GROSS_FIXED','NET_PAY_FIXED_RECOVERY'), false)) THEN 'NON_TAXABLE'::public.pay_finance_taxability_enum
          ELSE NULL::public.pay_finance_taxability_enum
        END,
        CASE
          WHEN COUNT(*) = 0 THEN NULL::public.pay_finance_routing_kind_enum
          WHEN upper(coalesce(v_target_case_row.candidate_pay_method, '')) = 'PAYE' THEN 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum
          WHEN upper(coalesce(v_target_case_row.candidate_pay_method, '')) = 'UMBRELLA' THEN 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum
          ELSE NULL::public.pay_finance_routing_kind_enum
        END
      INTO
        v_case_metadata_component_count,
        v_case_taxability,
        v_case_routing_kind
      FROM (
        SELECT
          upper(nullif(btrim(coalesce(component_element.value->>'classification', '')), '')) AS classification,
          upper(nullif(btrim(coalesce(
            component_element.value->>'source_pay_method',
            component_element.value->>'current_target_pay_method',
            v_target_case_row.candidate_pay_method,
            ''
          )), '')) AS source_pay_method
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(v_target_case_row.components_sync_json) = 'array' THEN v_target_case_row.components_sync_json
            ELSE '[]'::jsonb
          END
        ) AS component_element(value)
      ) AS normalized_component;
    END IF;

    select
      pa.id as finance_case_id,
      pa.case_type as old_case_type,
      pa.status as old_status,
      round(coalesce(pa.original_amount, 0), 2)::numeric(12,2) as old_original_amount,
      round(coalesce(pa.outstanding_amount, 0), 2)::numeric(12,2) as old_outstanding_amount,
      round(coalesce(pa.source_original_paid_amount, 0), 2)::numeric(12,2) as old_source_original_paid_amount,
      round(coalesce(pa.source_corrected_paid_amount, 0), 2)::numeric(12,2) as old_source_corrected_paid_amount,
      pa.linked_shift_date as old_linked_shift_date,
      pa.baseline_signature as old_baseline_signature,
      pa.taxability as old_taxability,
      pa.routing_kind as old_routing_kind,
      pa.written_off_at_utc as old_written_off_at_utc,
      pa.cleared_at_utc as old_cleared_at_utc,
      exists (
        select 1
        from public.pay_finance_case_events sync_safety_events
        where sync_safety_events.finance_case_id = pa.id
          and (
            upper(btrim(coalesce(sync_safety_events.event_type, ''))) like '%WRITE%OFF%'
            or upper(btrim(coalesce(sync_safety_events.event_type, ''))) in (
              'MANUAL_ECONOMIC_OVERRIDE',
              'MANUAL_WRITE_OFF',
              'CASE_MANUAL_ECONOMIC_OVERRIDE',
              'COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
            )
            or upper(btrim(coalesce(sync_safety_events.reason, ''))) in (
              'MANUAL_ECONOMIC_OVERRIDE',
              'WRITE_OFF',
              'COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
            )
          )
      ) as old_has_manual_or_restructure_event,
      exists (
        select 1
        from public.pay_finance_case_events automatic_clear_event
        where automatic_clear_event.finance_case_id = pa.id
          and (
            (
              upper(btrim(coalesce(automatic_clear_event.event_type, ''))) = 'CASE_CLEARED'
              and lower(btrim(coalesce(automatic_clear_event.reason, ''))) = 'rail_settlement'
            )
            or (
              upper(btrim(coalesce(automatic_clear_event.event_type, ''))) = 'CLEARED'
              and upper(btrim(coalesce(automatic_clear_event.reason, ''))) = 'PREVIEW_FINANCE_SYNC'
            )
          )
      ) as old_has_automatic_reconcilable_clear,
      round(coalesce((
        select sum(abs(coalesce(
          settled_reservation.reserved_source_amount,
          public._pay_batch_item_source_reservation_amount_ex_vat(settled_item.id),
          settled_item.frozen_source_amount,
          settled_item.amount_ex_vat,
          settled_item.amount_inc_vat,
          settled_reservation.reserved_amount,
          0
        )))
        from public.pay_batch_items as settled_item
        join public.pay_batch_candidates as settled_candidate
          on settled_candidate.id = settled_item.pay_batch_candidate_id
        join public.pay_batches as settled_batch
          on settled_batch.id = settled_candidate.pay_batch_id
        left join public.pay_bank_transfers as settled_transfer
          on settled_transfer.id = settled_item.pay_bank_transfer_id
        left join public.pay_advance_reservations as settled_reservation
          on settled_reservation.pay_batch_item_id = settled_item.id
        where coalesce(settled_item.is_voided, false) is not true
          and coalesce(
            settled_reservation.finance_case_id,
            settled_item.finance_case_id,
            case
              when upper(btrim(split_part(coalesce(settled_item.source_ref, ''), ':', 1))) = 'ADVANCE'
               and nullif(btrim(split_part(coalesce(settled_item.source_ref, ''), ':', 2)), '')
                   ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              then nullif(btrim(split_part(coalesce(settled_item.source_ref, ''), ':', 2)), '')::uuid
              else null::uuid
            end
          ) = pa.id
          and (
            (
              v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
              and upper(btrim(coalesce(settled_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
            )
            or (
              v_target_case_row.desired_case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum
              and upper(btrim(coalesce(settled_item.item_type, ''))) = 'UNDERPAYMENT_PAYMENT'
            )
          )
          and not exists (
            select 1
            from public.pay_payment_correction_items as settled_correction
            where settled_correction.pay_batch_item_id = settled_item.id
              and settled_correction.status = 'APPLIED'
              and settled_correction.correction_item_kind in (
                'PRE_BANK_CANCEL',
                'NO_MONEY_UNWIND',
                'SETTLED_REVERSAL'
              )
          )
          and (
            upper(btrim(coalesce(settled_candidate.settlement_status, ''))) = 'SETTLED'
            or settled_candidate.settled_at_utc is not null
            or upper(btrim(coalesce(settled_transfer.status, ''))) = 'COMPLETED'
            or settled_transfer.completed_at_utc is not null
            or upper(btrim(coalesce(settled_reservation.status, ''))) = 'SETTLED'
            or settled_reservation.settled_at_utc is not null
          )
      ), 0), 2)::numeric(12,2) as old_active_recovered_amount
    into v_existing_case_row
    from public.pay_advances pa
    where pa.candidate_id = v_target_case_row.candidate_id
      and pa.linked_timesheet_id = v_target_case_row.timesheet_id
      and pa.case_type = v_target_case_row.desired_case_type
    order by
      case when upper(coalesce(pa.status::text,'')) = 'ACTIVE' then 0 else 1 end,
      pa.updated_at desc,
      pa.created_at desc,
      pa.id desc
    limit 1
    for update;

    v_existing_recovered_amount := greatest(
      coalesce(v_existing_case_row.old_active_recovered_amount, 0),
      0
    );

    v_existing_active_reserved_amount := 0::numeric(12,2);

    IF v_existing_case_row.finance_case_id IS NOT NULL THEN
      SELECT ROUND(COALESCE(SUM(ABS(COALESCE(
        advance_reservation.reserved_source_amount,
        advance_reservation.reserved_amount,
        batch_item.frozen_source_amount,
        batch_item.amount_ex_vat,
        batch_item.amount_inc_vat,
        0
      ))), 0), 2)::numeric(12,2)
      INTO v_existing_active_reserved_amount
      FROM public.pay_advance_reservations AS advance_reservation
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = advance_reservation.pay_batch_item_id
      WHERE advance_reservation.finance_case_id = v_existing_case_row.finance_case_id
        AND UPPER(BTRIM(COALESCE(advance_reservation.status, ''))) IN ('RESERVED', 'COMMITTED')
        AND advance_reservation.released_at_utc IS NULL
        AND (batch_item.id IS NULL OR COALESCE(batch_item.is_voided, false) IS NOT TRUE);
    END IF;

    if v_existing_case_row.finance_case_id is not null
       and (
         v_existing_case_row.old_written_off_at_utc is not null
         or (
           v_existing_case_row.old_cleared_at_utc is not null
           and not (
             upper(coalesce(v_existing_case_row.old_status::text, '')) = 'PAID_OFF'
             and coalesce(v_existing_case_row.old_has_automatic_reconcilable_clear, false)
             and coalesce(v_existing_case_row.old_has_manual_or_restructure_event, false) is not true
             and round(
               coalesce(v_target_case_amount_ex, 0)
               - coalesce(v_existing_recovered_amount, 0),
               2
             ) > 0
           )
         )
         or upper(coalesce(v_existing_case_row.old_status::text,'')) not in ('ACTIVE','PAID_OFF')
         or coalesce(v_existing_case_row.old_has_manual_or_restructure_event, false)
       ) then
      insert into public.pay_finance_case_events (
        finance_case_id,
        event_type,
        event_at_utc,
        actor_user_id,
        pay_batch_id,
        reservation_id,
        before_json,
        after_json,
        reason,
        note
      )
      values (
        v_existing_case_row.finance_case_id,
        'SYNC_SKIPPED',
        now(),
        p_actor_user_id,
        null,
        null,
        jsonb_build_object(
          'case_type', v_existing_case_row.old_case_type::text,
          'status', v_existing_case_row.old_status::text,
          'original_amount', v_existing_case_row.old_original_amount,
          'outstanding_amount', v_existing_case_row.old_outstanding_amount,
          'source_original_paid_amount', v_existing_case_row.old_source_original_paid_amount,
          'source_corrected_paid_amount', v_existing_case_row.old_source_corrected_paid_amount,
          'linked_shift_date', case when v_existing_case_row.old_linked_shift_date is null then null else v_existing_case_row.old_linked_shift_date::text end,
          'baseline_signature', v_existing_case_row.old_baseline_signature,
          'written_off_at_utc', v_existing_case_row.old_written_off_at_utc,
          'cleared_at_utc', v_existing_case_row.old_cleared_at_utc,
          'has_manual_or_restructure_event', v_existing_case_row.old_has_manual_or_restructure_event,
          'has_automatic_reconcilable_clear', v_existing_case_row.old_has_automatic_reconcilable_clear,
          'existing_recovered_amount', v_existing_recovered_amount
        ),
        jsonb_build_object(
          'sync_skipped', true,
          'component_sync_skipped', true,
          'component_authority_preserved', true,
          'target_case_type', case when v_target_case_row.desired_case_type is null then null else v_target_case_row.desired_case_type::text end,
          'target_original_amount', v_target_case_amount_ex,
          'target_source_original_paid_amount', v_target_case_row.source_original_paid_amount,
          'target_source_corrected_paid_amount', v_target_case_row.source_corrected_paid_amount,
          'target_baseline_signature', v_target_case_row.baseline_signature,
          'target_pay_channel_scope', v_scope,
          'workbench_session_id', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'workbench_session_id', '')), ''),
          'source_build_run_id', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'source_build_run_id', '')), ''),
          'session_version', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'session_version', '')), ''),
          'overpayment_sync_scope_digest', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_scope_digest', '')), ''),
          'overpayment_sync_negative_component_digest', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_negative_component_digest', '')), ''),
          'overpayment_sync_settled_baseline_digest', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'overpayment_sync_settled_baseline_digest', '')), ''),
          'policy_x_authority_scope', NULLIF(BTRIM(COALESCE(p_mismatch_choices->>'policy_x_authority_scope', '')), '')
        ),
        'PREVIEW_FINANCE_SYNC_SKIPPED_PROTECTED_CASE',
        'Skipped case and component synchronisation because the existing case is written-off, cleared, closed, paused, manually resolved, or restructured.'
      );

      /* A protected case must not be inserted into tmp_sync_case_links with an
         empty component payload.  Every row in tmp_sync_case_links is processed
         later by pay_finance_components_sync_from_preview; an empty payload is
         authoritative removal input and can close open components.  The case
         clear pass already excludes protected cases by lifecycle state/event,
         so omitting the link preserves the complete protected case/component
         authority without allowing automatic sync to rewrite it. */
      continue;
    end if;

    IF v_existing_case_row.finance_case_id IS NOT NULL
       AND v_existing_case_row.old_case_type IS DISTINCT FROM v_target_case_row.desired_case_type
       AND COALESCE(v_existing_active_reserved_amount, 0) > 0 THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_ACTIVE_RESERVATION_DIRECTION_CONFLICT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_ACTIVE_RESERVATION_DIRECTION_CONFLICT',
                'finance_case_id', v_existing_case_row.finance_case_id::text,
                'candidate_id', v_target_case_row.candidate_id::text,
                'timesheet_id', v_target_case_row.timesheet_id::text,
                'existing_case_type', v_existing_case_row.old_case_type::text,
                'target_case_type', v_target_case_row.desired_case_type::text,
                'active_reserved_amount', COALESCE(v_existing_active_reserved_amount, 0),
                'message', 'The finance-case direction cannot change while a frozen recovery or payment reservation remains active. Settle or cancel the active batch before reconciling the opposite direction.'
              )::text;
    END IF;

    IF COALESCE(v_target_amount_is_authoritative_outstanding, false) THEN
      /* PRE_DRAFT_LIVE_TRUTH supplies the canonical amount that remains after
         settled baseline movements and active reservations.  Keep historic
         consumption in the case lifetime total, but do not subtract it from
         the authoritative remaining amount a second time. */
      v_active_reservation_protection_applied := COALESCE(v_existing_active_reserved_amount, 0) > 0
        AND COALESCE(v_target_case_amount_ex, 0) < COALESCE(v_existing_active_reserved_amount, 0);
      v_new_outstanding_amount := GREATEST(
        COALESCE(v_target_case_amount_ex, 0),
        COALESCE(v_existing_active_reserved_amount, 0),
        0
      )::numeric(12,2);
      v_effective_case_amount_ex := ROUND(
        COALESCE(v_existing_recovered_amount, 0) + v_new_outstanding_amount,
        2
      )::numeric(12,2);
    ELSE
      v_active_reservation_protection_applied := COALESCE(v_existing_active_reserved_amount, 0) > 0
        AND COALESCE(v_target_case_amount_ex, 0) < ROUND(COALESCE(v_existing_recovered_amount, 0) + COALESCE(v_existing_active_reserved_amount, 0), 2);
      v_effective_case_amount_ex := GREATEST(
        COALESCE(v_target_case_amount_ex, 0),
        ROUND(COALESCE(v_existing_recovered_amount, 0) + COALESCE(v_existing_active_reserved_amount, 0), 2)
      )::numeric(12,2);
      v_new_outstanding_amount := greatest(v_effective_case_amount_ex - v_existing_recovered_amount, COALESCE(v_existing_active_reserved_amount, 0), 0)::numeric(12,2);
    END IF;

    if v_existing_case_row.finance_case_id is null then
      insert into public.pay_advances (
        candidate_id,
        client_id,
        case_type,
        advance_kind,
        reason,
        linked_timesheet_id,
        linked_shift_date,
        baseline_signature,
        source_original_paid_amount,
        source_corrected_paid_amount,
        original_amount,
        outstanding_amount,
        status,
        created_by,
        updated_at,
        created_at,
        cleared_at_utc,
        cleared_by_user_id,
        write_off_reason,
        written_off_at_utc,
        written_off_by_user_id,
        taxability,
        routing_kind
      )
      values (
        v_target_case_row.candidate_id,
        v_target_case_row.client_id,
        v_target_case_row.desired_case_type,
        v_target_case_row.desired_advance_kind,
        v_target_case_row.desired_reason,
        v_target_case_row.timesheet_id,
        v_target_case_row.linked_shift_date,
        v_target_case_row.baseline_signature,
        v_target_case_row.source_original_paid_amount,
        v_target_case_row.source_corrected_paid_amount,
        v_effective_case_amount_ex,
        v_new_outstanding_amount,
        'ACTIVE'::public.pay_advance_status_enum,
        p_actor_user_id,
        now(),
        now(),
        null,
        null,
        null,
        null,
        null,
        v_case_taxability,
        v_case_routing_kind
      )
      returning id into v_selected_finance_case_id;

      v_cases_inserted := v_cases_inserted + 1;
      v_selected_event_type := 'CREATED';
      v_selected_reason := 'PREVIEW_FINANCE_SYNC';
      v_selected_note := case when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 'Created overpayment finance case from component-aware preview sync' else 'Created lifecycle-tracked underpayment finance case from component-aware preview sync' end;
      v_case_before_json := null;
      v_case_after_json := jsonb_build_object(
        'case_type', v_target_case_row.desired_case_type::text,
        'candidate_id', v_target_case_row.candidate_id::text,
        'linked_timesheet_id', v_target_case_row.timesheet_id::text,
        'linked_shift_date', case when v_target_case_row.linked_shift_date is null then null else v_target_case_row.linked_shift_date::text end,
        'baseline_signature', v_target_case_row.baseline_signature,
        'original_amount', v_effective_case_amount_ex,
        'outstanding_amount', v_new_outstanding_amount,
        'source_original_paid_amount', v_target_case_row.source_original_paid_amount,
        'source_corrected_paid_amount', v_target_case_row.source_corrected_paid_amount,
        'taxability', CASE WHEN v_case_taxability IS NULL THEN NULL ELSE v_case_taxability::text END,
        'routing_kind', CASE WHEN v_case_routing_kind IS NULL THEN NULL ELSE v_case_routing_kind::text END,
        'status', 'ACTIVE'
      )
      || CASE
        WHEN v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
         AND v_case_metadata_component_count > 0
         AND (v_case_taxability IS NOT NULL OR v_case_routing_kind IS NOT NULL) THEN jsonb_build_object(
          'before_taxability', null,
          'after_taxability', CASE WHEN v_case_taxability IS NULL THEN NULL ELSE v_case_taxability::text END,
          'before_routing_kind', null,
          'after_routing_kind', CASE WHEN v_case_routing_kind IS NULL THEN NULL ELSE v_case_routing_kind::text END,
          'inferred_from_open_components', true
        )
        ELSE '{}'::jsonb
      END;
    else
      v_case_before_json := jsonb_build_object(
        'case_type', v_existing_case_row.old_case_type::text,
        'status', v_existing_case_row.old_status::text,
        'original_amount', v_existing_case_row.old_original_amount,
        'outstanding_amount', v_existing_case_row.old_outstanding_amount,
        'source_original_paid_amount', v_existing_case_row.old_source_original_paid_amount,
        'source_corrected_paid_amount', v_existing_case_row.old_source_corrected_paid_amount,
        'taxability', CASE WHEN v_existing_case_row.old_taxability IS NULL THEN NULL ELSE v_existing_case_row.old_taxability::text END,
        'routing_kind', CASE WHEN v_existing_case_row.old_routing_kind IS NULL THEN NULL ELSE v_existing_case_row.old_routing_kind::text END,
        'linked_shift_date', case when v_existing_case_row.old_linked_shift_date is null then null else v_existing_case_row.old_linked_shift_date::text end,
        'baseline_signature', v_existing_case_row.old_baseline_signature
      )
      || CASE
        WHEN v_existing_case_row.old_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum THEN jsonb_build_object(
          'before_taxability', CASE WHEN v_existing_case_row.old_taxability IS NULL THEN NULL ELSE v_existing_case_row.old_taxability::text END,
          'before_routing_kind', CASE WHEN v_existing_case_row.old_routing_kind IS NULL THEN NULL ELSE v_existing_case_row.old_routing_kind::text END
        )
        ELSE '{}'::jsonb
      END;

      update public.pay_advances pa
      set
        client_id = v_target_case_row.client_id,
        case_type = v_target_case_row.desired_case_type,
        advance_kind = v_target_case_row.desired_advance_kind,
        reason = v_target_case_row.desired_reason,
        linked_shift_date = v_target_case_row.linked_shift_date,
        baseline_signature = v_target_case_row.baseline_signature,
        source_original_paid_amount = v_target_case_row.source_original_paid_amount,
        source_corrected_paid_amount = v_target_case_row.source_corrected_paid_amount,
        original_amount = v_effective_case_amount_ex,
        outstanding_amount = v_new_outstanding_amount,
        status = case when v_new_outstanding_amount > 0 then 'ACTIVE'::public.pay_advance_status_enum else 'PAID_OFF'::public.pay_advance_status_enum end,
        cleared_at_utc = case when v_new_outstanding_amount > 0 then null else coalesce(pa.cleared_at_utc, now()) end,
        cleared_by_user_id = case when v_new_outstanding_amount > 0 then null else coalesce(pa.cleared_by_user_id, p_actor_user_id) end,
        taxability = case
          when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum and v_case_metadata_component_count > 0 then v_case_taxability
          else pa.taxability
        end,
        routing_kind = case
          when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum and v_case_metadata_component_count > 0 then v_case_routing_kind
          else pa.routing_kind
        end,
        updated_at = now()
      where pa.id = v_existing_case_row.finance_case_id
        and row(
          pa.client_id,
          pa.case_type,
          pa.advance_kind,
          pa.reason,
          pa.linked_shift_date,
          pa.baseline_signature,
          pa.source_original_paid_amount,
          pa.source_corrected_paid_amount,
          pa.original_amount,
          pa.outstanding_amount,
          pa.status,
          pa.cleared_at_utc,
          pa.cleared_by_user_id,
          pa.taxability,
          pa.routing_kind
        ) is distinct from row(
          v_target_case_row.client_id,
          v_target_case_row.desired_case_type,
          v_target_case_row.desired_advance_kind,
          v_target_case_row.desired_reason,
          v_target_case_row.linked_shift_date,
          v_target_case_row.baseline_signature,
          v_target_case_row.source_original_paid_amount,
          v_target_case_row.source_corrected_paid_amount,
          v_effective_case_amount_ex,
          v_new_outstanding_amount,
          case when v_new_outstanding_amount > 0 then 'ACTIVE'::public.pay_advance_status_enum else 'PAID_OFF'::public.pay_advance_status_enum end,
          case when v_new_outstanding_amount > 0 then null else coalesce(pa.cleared_at_utc, now()) end,
          case when v_new_outstanding_amount > 0 then null else coalesce(pa.cleared_by_user_id, p_actor_user_id) end,
          case
            when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum and v_case_metadata_component_count > 0 then v_case_taxability
            else pa.taxability
          end,
          case
            when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum and v_case_metadata_component_count > 0 then v_case_routing_kind
            else pa.routing_kind
          end
        );

      get diagnostics v_case_update_count = row_count;

      v_selected_finance_case_id := v_existing_case_row.finance_case_id;
      v_cases_touched := v_cases_touched + 1;

      if v_case_update_count > 0
         and upper(coalesce(v_existing_case_row.old_status::text,'')) = 'PAID_OFF'
         and v_new_outstanding_amount > 0 then
        v_cases_reopened := v_cases_reopened + 1;
        v_selected_event_type := 'REOPENED';
        v_selected_reason := 'PREVIEW_FINANCE_SYNC';
        v_selected_note := case when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 'Reopened overpayment finance case from component-aware preview sync' else 'Reopened lifecycle-tracked underpayment finance case from component-aware preview sync' end;
      elsif v_case_update_count > 0 then
        v_cases_amended := v_cases_amended + 1;
        v_selected_event_type := 'AMENDED';
        v_selected_reason := 'PREVIEW_FINANCE_SYNC';
        v_selected_note := case when v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then 'Amended overpayment finance case from component-aware preview sync' else 'Amended lifecycle-tracked underpayment finance case from component-aware preview sync' end;
      end if;

      v_case_after_json := jsonb_build_object(
        'case_type', v_target_case_row.desired_case_type::text,
        'status', case when v_new_outstanding_amount > 0 then 'ACTIVE' else 'PAID_OFF' end,
        'original_amount', v_effective_case_amount_ex,
        'outstanding_amount', v_new_outstanding_amount,
        'source_original_paid_amount', v_target_case_row.source_original_paid_amount,
        'source_corrected_paid_amount', v_target_case_row.source_corrected_paid_amount,
        'taxability', CASE
          WHEN v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum AND v_case_metadata_component_count > 0 THEN CASE WHEN v_case_taxability IS NULL THEN NULL ELSE v_case_taxability::text END
          ELSE CASE WHEN v_existing_case_row.old_taxability IS NULL THEN NULL ELSE v_existing_case_row.old_taxability::text END
        END,
        'routing_kind', CASE
          WHEN v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum AND v_case_metadata_component_count > 0 THEN CASE WHEN v_case_routing_kind IS NULL THEN NULL ELSE v_case_routing_kind::text END
          ELSE CASE WHEN v_existing_case_row.old_routing_kind IS NULL THEN NULL ELSE v_existing_case_row.old_routing_kind::text END
        END,
        'linked_shift_date', case when v_target_case_row.linked_shift_date is null then null else v_target_case_row.linked_shift_date::text end,
        'baseline_signature', v_target_case_row.baseline_signature
      )
      || CASE
        WHEN v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
         AND v_case_metadata_component_count > 0
         AND (
           v_existing_case_row.old_taxability IS DISTINCT FROM v_case_taxability
           OR v_existing_case_row.old_routing_kind IS DISTINCT FROM v_case_routing_kind
         ) THEN jsonb_build_object(
          'before_taxability', CASE WHEN v_existing_case_row.old_taxability IS NULL THEN NULL ELSE v_existing_case_row.old_taxability::text END,
          'after_taxability', CASE WHEN v_case_taxability IS NULL THEN NULL ELSE v_case_taxability::text END,
          'before_routing_kind', CASE WHEN v_existing_case_row.old_routing_kind IS NULL THEN NULL ELSE v_existing_case_row.old_routing_kind::text END,
          'after_routing_kind', CASE WHEN v_case_routing_kind IS NULL THEN NULL ELSE v_case_routing_kind::text END,
          'inferred_from_open_components', true
        )
        ELSE '{}'::jsonb
      END;
    end if;

    insert into pg_temp.tmp_sync_case_links (
      candidate_id,
      timesheet_id,
      finance_case_id,
      desired_case_type,
      source_original_paid_amount,
      source_corrected_paid_amount,
      case_amount_ex,
      linked_shift_date,
      baseline_signature,
      components_sync_json
    )
    values (
      v_target_case_row.candidate_id,
      v_target_case_row.timesheet_id,
      v_selected_finance_case_id,
      v_target_case_row.desired_case_type,
      v_target_case_row.source_original_paid_amount,
      v_target_case_row.source_corrected_paid_amount,
      COALESCE(v_effective_case_amount_ex, v_target_case_amount_ex),
      v_target_case_row.linked_shift_date,
      v_target_case_row.baseline_signature,
      v_target_case_row.components_sync_json
    )
    on conflict (candidate_id, timesheet_id) do update
    set
      finance_case_id = excluded.finance_case_id,
      desired_case_type = excluded.desired_case_type,
      source_original_paid_amount = excluded.source_original_paid_amount,
      source_corrected_paid_amount = excluded.source_corrected_paid_amount,
      case_amount_ex = excluded.case_amount_ex,
      linked_shift_date = excluded.linked_shift_date,
      baseline_signature = excluded.baseline_signature,
      components_sync_json = excluded.components_sync_json;

    if v_selected_event_type is not null then
    insert into public.pay_finance_case_events (
      finance_case_id,
      event_type,
      event_at_utc,
      actor_user_id,
      pay_batch_id,
      reservation_id,
      before_json,
      after_json,
      reason,
      note
    )
    values (
      v_selected_finance_case_id,
      v_selected_event_type,
      now(),
      p_actor_user_id,
      null,
      null,
      v_case_before_json,
      v_case_after_json,
      v_selected_reason,
      v_selected_note
    );
    end if;

    IF COALESCE(v_active_reservation_protection_applied, false)
       AND NOT EXISTS (
         SELECT 1
         FROM public.pay_finance_case_events AS protection_event
         WHERE protection_event.finance_case_id = v_selected_finance_case_id
           AND protection_event.event_type = 'SYNC_SHRINK_DEFERRED_ACTIVE_RESERVATION'
           AND protection_event.reason = 'PREVIEW_FINANCE_SYNC_ACTIVE_RESERVATION_PROTECTION'
           AND protection_event.before_json IS NOT DISTINCT FROM v_case_before_json
           AND protection_event.after_json = jsonb_build_object(
             'target_case_amount_ex', v_target_case_amount_ex,
             'effective_case_amount_ex', v_effective_case_amount_ex,
             'active_reserved_recovery_amount', v_existing_active_reserved_amount,
             'existing_recovered_amount', v_existing_recovered_amount,
             'outstanding_amount', v_new_outstanding_amount
           )
       ) THEN
      insert into public.pay_finance_case_events (
        finance_case_id,
        event_type,
        event_at_utc,
        actor_user_id,
        pay_batch_id,
        reservation_id,
        before_json,
        after_json,
        reason,
        note
      )
      values (
        v_selected_finance_case_id,
        'SYNC_SHRINK_DEFERRED_ACTIVE_RESERVATION',
        now(),
        p_actor_user_id,
        null,
        null,
        v_case_before_json,
        jsonb_build_object(
          'target_case_amount_ex', v_target_case_amount_ex,
          'effective_case_amount_ex', v_effective_case_amount_ex,
          'active_reserved_recovery_amount', v_existing_active_reserved_amount,
          'existing_recovered_amount', v_existing_recovered_amount,
          'outstanding_amount', v_new_outstanding_amount
        ),
        'PREVIEW_FINANCE_SYNC_ACTIVE_RESERVATION_PROTECTION',
        'Deferred overpayment/underpayment case shrink because an active recovery reservation exists.'
      );
    END IF;
  end loop;

  for v_open_case_candidate in
    select
      pa.id as finance_case_id,
      pa.candidate_id,
      pa.linked_timesheet_id as timesheet_id,
      pa.case_type as old_case_type,
      round(coalesce(pa.original_amount,0),2)::numeric(12,2) as old_original_amount,
      round(coalesce(pa.outstanding_amount,0),2)::numeric(12,2) as old_outstanding_amount,
      round(coalesce(pa.source_original_paid_amount,0),2)::numeric(12,2) as old_source_original_paid_amount,
      round(coalesce(pa.source_corrected_paid_amount,0),2)::numeric(12,2) as old_source_corrected_paid_amount,
      pa.linked_shift_date as old_linked_shift_date,
      pa.baseline_signature as old_baseline_signature
    from public.pay_advances pa
    where pa.case_type in ('OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
      and pa.linked_timesheet_id is not null
      and (
        coalesce(array_length(p_candidate_ids, 1), 0) = 0
        or pa.candidate_id = any(p_candidate_ids)
      )
      and (
        p_client_filter_single is null
        or pa.client_id = p_client_filter_single
      )
      and (
        coalesce(array_length(p_force_include_timesheet_ids, 1), 0) = 0
        or pa.linked_timesheet_id = any(p_force_include_timesheet_ids)
        or p_force_include_timesheet_ids is null
      )
      and not (
        coalesce(array_length(p_exclude_timesheet_ids, 1), 0) > 0
        and pa.linked_timesheet_id = any(p_exclude_timesheet_ids)
      )
      and not exists (
        select 1
        from pg_temp.tmp_sync_case_links l
        where l.finance_case_id = pa.id
      )
      and exists (
        select 1
        from public.candidates c
        where c.id = pa.candidate_id
          and upper(coalesce(c.pay_method,'')) = v_scope
      )
      and upper(coalesce(pa.status::text, '')) = 'ACTIVE'
      and pa.written_off_at_utc is null
      and pa.cleared_at_utc is null
      and not exists (
        select 1
        from public.pay_finance_case_events sync_clear_safety_events
        where sync_clear_safety_events.finance_case_id = pa.id
          and (
            upper(btrim(coalesce(sync_clear_safety_events.event_type, ''))) like '%WRITE%OFF%'
            or upper(btrim(coalesce(sync_clear_safety_events.event_type, ''))) in (
              'MANUAL_ECONOMIC_OVERRIDE',
              'MANUAL_WRITE_OFF',
              'CASE_MANUAL_ECONOMIC_OVERRIDE',
              'COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
            )
            or upper(btrim(coalesce(sync_clear_safety_events.reason, ''))) in (
              'MANUAL_ECONOMIC_OVERRIDE',
              'WRITE_OFF',
              'COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
            )
          )
      )
  loop
    SELECT ROUND(COALESCE(SUM(ABS(COALESCE(advance_reservation.reserved_source_amount, advance_reservation.reserved_amount, batch_item.frozen_source_amount, batch_item.amount_ex_vat, batch_item.amount_inc_vat, 0))), 0), 2)::numeric(12,2)
    INTO v_existing_active_reserved_amount
    FROM public.pay_advance_reservations AS advance_reservation
    LEFT JOIN public.pay_batch_items AS batch_item
      ON batch_item.id = advance_reservation.pay_batch_item_id
    WHERE advance_reservation.finance_case_id = v_open_case_candidate.finance_case_id
      AND UPPER(BTRIM(COALESCE(advance_reservation.status, ''))) IN ('RESERVED', 'COMMITTED')
      AND advance_reservation.released_at_utc IS NULL
      AND (batch_item.id IS NULL OR COALESCE(batch_item.is_voided, false) IS NOT TRUE);

    IF COALESCE(v_existing_active_reserved_amount, 0) > 0 THEN
      IF NOT EXISTS (
        SELECT 1
        FROM public.pay_finance_case_events AS protection_event
        WHERE protection_event.finance_case_id = v_open_case_candidate.finance_case_id
          AND protection_event.event_type = 'SYNC_CLEAR_DEFERRED_ACTIVE_RESERVATION'
          AND protection_event.reason = 'PREVIEW_FINANCE_SYNC_ACTIVE_RESERVATION_PROTECTION'
          AND protection_event.before_json = jsonb_build_object(
            'case_type', v_open_case_candidate.old_case_type::text,
            'status', 'ACTIVE',
            'original_amount', v_open_case_candidate.old_original_amount,
            'outstanding_amount', v_open_case_candidate.old_outstanding_amount
          )
          AND protection_event.after_json = jsonb_build_object(
            'clear_deferred', true,
            'active_reserved_recovery_amount', v_existing_active_reserved_amount
          )
      ) THEN
        insert into public.pay_finance_case_events (
          finance_case_id,
          event_type,
          event_at_utc,
          actor_user_id,
          pay_batch_id,
          reservation_id,
          before_json,
          after_json,
          reason,
          note
        )
        values (
          v_open_case_candidate.finance_case_id,
          'SYNC_CLEAR_DEFERRED_ACTIVE_RESERVATION',
          now(),
          p_actor_user_id,
          null,
          null,
          jsonb_build_object(
            'case_type', v_open_case_candidate.old_case_type::text,
            'status', 'ACTIVE',
            'original_amount', v_open_case_candidate.old_original_amount,
            'outstanding_amount', v_open_case_candidate.old_outstanding_amount
          ),
          jsonb_build_object(
            'clear_deferred', true,
            'active_reserved_recovery_amount', v_existing_active_reserved_amount
          ),
          'PREVIEW_FINANCE_SYNC_ACTIVE_RESERVATION_PROTECTION',
          'Deferred finance case clear because an active recovery reservation exists.'
        );
      END IF;

      CONTINUE;
    END IF;

    insert into pg_temp.tmp_sync_case_clears (
      finance_case_id,
      candidate_id,
      timesheet_id,
      old_case_type,
      old_original_amount,
      old_outstanding_amount,
      old_source_original_paid_amount,
      old_source_corrected_paid_amount,
      old_linked_shift_date,
      old_baseline_signature
    )
    values (
      v_open_case_candidate.finance_case_id,
      v_open_case_candidate.candidate_id,
      v_open_case_candidate.timesheet_id,
      v_open_case_candidate.old_case_type,
      v_open_case_candidate.old_original_amount,
      v_open_case_candidate.old_outstanding_amount,
      v_open_case_candidate.old_source_original_paid_amount,
      v_open_case_candidate.old_source_corrected_paid_amount,
      v_open_case_candidate.old_linked_shift_date,
      v_open_case_candidate.old_baseline_signature
    )
    on conflict (finance_case_id) do nothing;
  end loop;

  for v_existing_case_row in
    select *
    from pg_temp.tmp_sync_case_clears
    order by candidate_id, timesheet_id, finance_case_id
  loop
    update public.pay_advances pa
    set
      status = 'PAID_OFF'::public.pay_advance_status_enum,
      outstanding_amount = 0,
      cleared_at_utc = coalesce(pa.cleared_at_utc, now()),
      cleared_by_user_id = coalesce(pa.cleared_by_user_id, p_actor_user_id),
      updated_at = now()
    where pa.id = v_existing_case_row.finance_case_id;

    insert into public.pay_finance_case_events (
      finance_case_id,
      event_type,
      event_at_utc,
      actor_user_id,
      pay_batch_id,
      reservation_id,
      before_json,
      after_json,
      reason,
      note
    )
    values (
      v_existing_case_row.finance_case_id,
      'CLEARED',
      now(),
      p_actor_user_id,
      null,
      null,
      jsonb_build_object(
        'case_type', v_existing_case_row.old_case_type::text,
        'status', 'ACTIVE',
        'original_amount', v_existing_case_row.old_original_amount,
        'outstanding_amount', v_existing_case_row.old_outstanding_amount,
        'source_original_paid_amount', v_existing_case_row.old_source_original_paid_amount,
        'source_corrected_paid_amount', v_existing_case_row.old_source_corrected_paid_amount,
        'linked_shift_date', case when v_existing_case_row.old_linked_shift_date is null then null else v_existing_case_row.old_linked_shift_date::text end,
        'baseline_signature', v_existing_case_row.old_baseline_signature
      ),
      jsonb_build_object(
        'case_type', v_existing_case_row.old_case_type::text,
        'status', 'PAID_OFF',
        'original_amount', v_existing_case_row.old_original_amount,
        'outstanding_amount', 0,
        'source_original_paid_amount', v_existing_case_row.old_source_original_paid_amount,
        'source_corrected_paid_amount', v_existing_case_row.old_source_corrected_paid_amount,
        'linked_shift_date', case when v_existing_case_row.old_linked_shift_date is null then null else v_existing_case_row.old_linked_shift_date::text end,
        'baseline_signature', v_existing_case_row.old_baseline_signature
      ),
      'PREVIEW_FINANCE_SYNC',
      'Cleared finance case because the current preview no longer requires a persistent lifecycle-tracked overpayment/underpayment header'
    );

    v_components_sync_result := public.pay_finance_components_sync_from_preview(
      v_existing_case_row.finance_case_id,
      '[]'::jsonb,
      p_actor_user_id
    );

    IF jsonb_typeof(COALESCE(v_components_sync_result, '{}'::jsonb)) <> 'object'
       OR LOWER(BTRIM(COALESCE(v_components_sync_result->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_COMPONENT_SYNC_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_COMPONENT_SYNC_FAILED',
                'finance_case_id', v_existing_case_row.finance_case_id::text,
                'candidate_id', v_existing_case_row.candidate_id::text,
                'timesheet_id', v_existing_case_row.timesheet_id::text,
                'operation', 'CLEAR',
                'component_sync_result', COALESCE(v_components_sync_result, '{}'::jsonb)
              )::text;
    END IF;

    v_cases_cleared := v_cases_cleared + 1;
  end loop;

  for v_target_case_row in
    select l.*
    from pg_temp.tmp_sync_case_links l
    order by l.candidate_id, l.timesheet_id
  loop
    v_components_sync_result := public.pay_finance_components_sync_from_preview(
      v_target_case_row.finance_case_id,
      v_target_case_row.components_sync_json,
      p_actor_user_id
    );

    IF jsonb_typeof(COALESCE(v_components_sync_result, '{}'::jsonb)) <> 'object'
       OR LOWER(BTRIM(COALESCE(v_components_sync_result->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_COMPONENT_SYNC_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_COMPONENT_SYNC_FAILED',
                'finance_case_id', v_target_case_row.finance_case_id::text,
                'candidate_id', v_target_case_row.candidate_id::text,
                'timesheet_id', v_target_case_row.timesheet_id::text,
                'desired_case_type', v_target_case_row.desired_case_type::text,
                'operation', 'RECONCILE',
                'component_sync_result', COALESCE(v_components_sync_result, '{}'::jsonb)
              )::text;
    END IF;

    SELECT
      round(coalesce(pa.outstanding_amount, 0), 2)::numeric(12,2),
      round(coalesce(sum(coalesce(pfc.remaining_source_amount, 0)) FILTER (WHERE pfc.closed_at_utc IS NULL), 0), 2)::numeric(12,2)
    INTO
      v_component_case_outstanding_amount,
      v_component_open_remaining_total
    FROM public.pay_advances AS pa
    LEFT JOIN public.pay_finance_case_components AS pfc
      ON pfc.finance_case_id = pa.id
    WHERE pa.id = v_target_case_row.finance_case_id
    GROUP BY pa.outstanding_amount;

    v_component_case_mismatch_amount := round(
      coalesce(v_component_case_outstanding_amount, 0) - coalesce(v_component_open_remaining_total, 0),
      2
    )::numeric(12,2);

    IF abs(coalesce(v_component_case_mismatch_amount, 0)) > 0.01 THEN
      RAISE EXCEPTION 'PAY_SYNC_OVERPAYMENTS_COMPONENT_CASE_RECONCILIATION_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SYNC_OVERPAYMENTS_COMPONENT_CASE_RECONCILIATION_FAILED',
                'finance_case_id', v_target_case_row.finance_case_id::text,
                'candidate_id', v_target_case_row.candidate_id::text,
                'timesheet_id', v_target_case_row.timesheet_id::text,
                'desired_case_type', v_target_case_row.desired_case_type::text,
                'case_outstanding_amount', coalesce(v_component_case_outstanding_amount, 0),
                'open_component_remaining_total', coalesce(v_component_open_remaining_total, 0),
                'mismatch_amount', coalesce(v_component_case_mismatch_amount, 0),
                'component_sync_result', COALESCE(v_components_sync_result, '{}'::jsonb)
              )::text;
    END IF;

    if v_target_case_row.desired_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then
      v_synced_open_component_count := 0;
      v_synced_case_taxability := NULL;
      v_synced_case_routing_kind := NULL;

      select
        pa.taxability,
        pa.routing_kind
      into
        v_synced_before_taxability,
        v_synced_before_routing_kind
      from public.pay_advances pa
      where pa.id = v_target_case_row.finance_case_id
      limit 1;

      select
        count(*)::integer,
        case
          when count(*) = 0 then null::public.pay_finance_taxability_enum
          when bool_and(pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum) then 'TAXABLE'::public.pay_finance_taxability_enum
          when bool_and(pfc.classification in (
            'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
            'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
          )) then 'NON_TAXABLE'::public.pay_finance_taxability_enum
          else null::public.pay_finance_taxability_enum
        end,
        case
          when count(*) = 0 then null::public.pay_finance_routing_kind_enum
          when v_scope = 'PAYE' then 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum
          when v_scope = 'UMBRELLA' then 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum
          else null::public.pay_finance_routing_kind_enum
        end
      into
        v_synced_open_component_count,
        v_synced_case_taxability,
        v_synced_case_routing_kind
      from public.pay_finance_case_components pfc
      where pfc.finance_case_id = v_target_case_row.finance_case_id
        and pfc.closed_at_utc is null
        and round(coalesce(pfc.remaining_source_amount, 0), 2) > 0;

      if v_synced_open_component_count > 0
         and (v_synced_before_taxability is distinct from v_synced_case_taxability
              or v_synced_before_routing_kind is distinct from v_synced_case_routing_kind) then
        v_taxability_event_before_json := jsonb_build_object(
          'case_type', 'OVERPAYMENT',
          'taxability', case when v_synced_before_taxability is null then null else v_synced_before_taxability::text end,
          'routing_kind', case when v_synced_before_routing_kind is null then null else v_synced_before_routing_kind::text end
        );

        v_taxability_event_after_json := jsonb_build_object(
          'case_type', 'OVERPAYMENT',
          'taxability', case when v_synced_case_taxability is null then null else v_synced_case_taxability::text end,
          'routing_kind', case when v_synced_case_routing_kind is null then null else v_synced_case_routing_kind::text end,
          'before_taxability', case when v_synced_before_taxability is null then null else v_synced_before_taxability::text end,
          'after_taxability', case when v_synced_case_taxability is null then null else v_synced_case_taxability::text end,
          'before_routing_kind', case when v_synced_before_routing_kind is null then null else v_synced_before_routing_kind::text end,
          'after_routing_kind', case when v_synced_case_routing_kind is null then null else v_synced_case_routing_kind::text end,
          'inferred_from_open_components', true
        );

        update public.pay_advances pa
        set
          taxability = v_synced_case_taxability,
          routing_kind = v_synced_case_routing_kind,
          updated_at = now()
        where pa.id = v_target_case_row.finance_case_id;

        insert into public.pay_finance_case_events (
          finance_case_id,
          event_type,
          event_at_utc,
          actor_user_id,
          pay_batch_id,
          reservation_id,
          before_json,
          after_json,
          reason,
          note
        )
        values (
          v_target_case_row.finance_case_id,
          case
            when v_synced_before_taxability is distinct from v_synced_case_taxability
             and v_synced_before_routing_kind is distinct from v_synced_case_routing_kind then 'HEADER_METADATA_INFERRED'
            when v_synced_before_taxability is distinct from v_synced_case_taxability then 'TAXABILITY_INFERRED'
            else 'ROUTING_KIND_INFERRED'
          end,
          now(),
          p_actor_user_id,
          null,
          null,
          v_taxability_event_before_json,
          v_taxability_event_after_json,
          'PREVIEW_FINANCE_SYNC',
          'Inferred overpayment taxability/routing metadata from open finance case components after component sync'
        );
      end if;
    end if;
  end loop;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW_RESULT',
    jsonb_build_object(
      'pay_channel_scope', v_scope,
      'requested_candidate_count', v_requested_candidate_count,
      'preview_candidate_count', v_preview_candidate_count,
      'preview_candidate_coverage_complete', COALESCE(v_missing_candidate_count, 0) = 0,
      'authoritative_timesheet_scope', COALESCE(v_authoritative_timesheet_scope, false),
      'explicit_empty_timesheet_scope', COALESCE(v_explicit_empty_timesheet_scope, false),
      'preview_scope_strategy', v_preview_scope_strategy,
      'overpayment_case_count', v_overpayment_case_count,
      'underpayment_case_count', v_underpayment_case_count,
      'cases_inserted', v_cases_inserted,
      'cases_touched', v_cases_touched,
      'cases_amended', v_cases_amended,
      'cases_reopened', v_cases_reopened,
      'cases_cleared', v_cases_cleared
    ),
    'pay_sync_overpayments',
    COALESCE(p_pay_date::text, 'NO_PAY_DATE'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  return jsonb_build_object(
    'ok', true,
    'pay_channel_scope', v_scope,
    'requested_candidate_count', v_requested_candidate_count,
    'preview_candidate_count', v_preview_candidate_count,
    'preview_candidate_coverage_complete', COALESCE(v_missing_candidate_count, 0) = 0,
    'missing_candidate_ids', COALESCE(v_missing_candidate_ids_json, '[]'::jsonb),
    'authoritative_timesheet_scope', COALESCE(v_authoritative_timesheet_scope, false),
    'explicit_empty_timesheet_scope', COALESCE(v_explicit_empty_timesheet_scope, false),
    'preview_scope_strategy', v_preview_scope_strategy,
    'scope_timesheet_ids', COALESCE(to_jsonb(p_force_include_timesheet_ids), '[]'::jsonb),
    'negative_preview_timesheets_count', v_overpayment_case_count,
    'negative_preview_timesheets', v_overpayment_json,
    'underpayment_case_count', v_underpayment_case_count,
    'underpayment_cases', v_underpayment_json,
    'timesheet_finance_case_candidates_count', v_timesheet_case_count,
    'timesheet_finance_case_candidates', v_case_candidates_json,
    'cases_inserted', v_cases_inserted,
    'cases_touched', v_cases_touched,
    'cases_amended', v_cases_amended,
    'cases_reopened', v_cases_reopened,
    'cases_cleared', v_cases_cleared
  );
exception
  when others then
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW_ERROR',
      jsonb_build_object(
        'pay_date', p_pay_date,
        'week_ending_cutoff', p_week_ending_cutoff,
        'pay_channel_scope', p_pay_channel_scope,
        'candidate_count', COALESCE(array_length(p_candidate_ids, 1), 0),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_sync_overpayments',
      COALESCE(p_pay_date::text, 'NO_PAY_DATE'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );
    RAISE;
end;
$function$;
