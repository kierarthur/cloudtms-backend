-- Focused authority for semantic Ready-to-Pay candidate summaries.

CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_summary_fragment(
  p_context_json jsonb,
  p_candidate_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_context_matches boolean := false;
  v_context_row_count integer := 0;
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
  v_rail_supports_scheduling boolean := false;
  v_rail_supports_name_check boolean := false;
  v_rail_supports_auto_execute boolean := false;
  v_default_schedule_umbrella_local text;
  v_default_schedule_paye_local text;
  v_funds_warning_hours_json jsonb := '[]'::jsonb;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_paye_guardrails jsonb := '{}'::jsonb;
  v_paye jsonb := '[]'::jsonb;
  v_nonpaye jsonb := '[]'::jsonb;
  v_blocked jsonb := '[]'::jsonb;
  v_do_not_pay jsonb := '[]'::jsonb;
  v_snoozed jsonb := '[]'::jsonb;
  v_payees jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_canonical_preview_lines jsonb := '[]'::jsonb;
  v_paye_summary_breakdown jsonb := '{}'::jsonb;
  v_case_resolution_states jsonb := '[]'::jsonb;
  v_candidate_row jsonb := '{}'::jsonb;
  v_itemisation jsonb := '[]'::jsonb;
  v_baseline_component_rows jsonb := '[]'::jsonb;
  v_hidden_recovery_template_lines jsonb := '[]'::jsonb;
  v_filtered_canonical_preview_lines jsonb := '[]'::jsonb;
  v_carry_forward_itemisation jsonb := '[]'::jsonb;
  v_carry_forward_total_ex_vat numeric := 0;
  v_carry_forward_total_vat numeric := 0;
  v_carry_forward_total_inc_vat numeric := 0;
  v_workbench_session_id uuid := NULL::uuid;
  v_workbench_session_id_text text := NULL::text;
  v_preview_row_count integer := 0;
  v_draftable_row_count integer := 0;
  v_blocked_row_count integer := 0;
  v_do_not_pay_row_count integer := 0;
  v_case_resolution_row_count integer := 0;
  v_pending_line_count integer := 0;
  v_ready_line_count integer := 0;
  v_materialised_line_count integer := 0;
  v_error_line_count integer := 0;
  v_total_amount_ex_vat numeric := 0;
  v_draftable_amount_ex_vat numeric := 0;
  v_preview_section_counts jsonb := '{}'::jsonb;
  v_semantic_ready_publication_enabled boolean := false;
begin
  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'p_context_json must be a JSON object';
  end if;

  if v_candidate_id is null then
    raise exception 'candidate_id is required';
  end if;

  SELECT COALESCE(settings_row.banking_pay_workbench_semantic_ready_publication_v3_enabled,false)
  INTO v_semantic_ready_publication_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  v_workbench_session_id_text := NULLIF(BTRIM(COALESCE(
    v_context_json->>'workbench_session_id',
    v_context_json->>'session_id',
    v_context_json#>>'{workbench,session_id}',
    v_context_json#>>'{line_work,session_id}',
    ''
  )), '');

  IF v_workbench_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_workbench_session_id := v_workbench_session_id_text::uuid;
  END IF;

  IF v_workbench_session_id IS NOT NULL THEN
    PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_workbench_session_id
      AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_PREVIEW_SUMMARY_FRAGMENT_SESSION_NOT_OPEN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PREVIEW_SUMMARY_FRAGMENT_SESSION_NOT_OPEN',
                'session_id', v_workbench_session_id::text,
                'candidate_id', v_candidate_id::text
              )::text;
    END IF;

    SELECT
      COUNT(*)::integer,
      COUNT(*) FILTER (
        WHERE COALESCE(preview_row.selected, false) = true
          AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) NOT IN ('NOT_SELECTABLE', 'EXCLUDED')
          AND (
            COALESCE(v_semantic_ready_publication_enabled,false) IS NOT TRUE
            OR (
              LOWER(BTRIM(COALESCE(preview_row.row_json->>'draftable','false'))) IN ('true','t','1','yes','y','on')
              AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'selection_allowed','false'))) IN ('true','t','1','yes','y','on')
              AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_excluded_from_allocation','false'))) NOT IN ('true','t','1','yes','y','on')
            )
          )
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(preview_row.row_json->>'presentation_section', ''))) IN ('BLOCKED', 'BLOCKED_FOR_PAY')
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(preview_row.row_json->>'presentation_section', ''))) = 'DO_NOT_PAY'
           OR LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_do_not_pay', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(preview_row.row_json->>'presentation_section', ''))) = 'CASES_RESOLUTIONS'
      )::integer,
      COALESCE(SUM(CASE
        WHEN COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND (
           COALESCE(v_semantic_ready_publication_enabled,false) IS NOT TRUE
           OR (
             LOWER(BTRIM(COALESCE(preview_row.row_json->>'draftable','false'))) IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'selection_allowed','false'))) IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_excluded_from_allocation','false'))) NOT IN ('true','t','1','yes','y','on')
           )
         )
        THEN (preview_row.row_json->>'amount_ex_vat')::numeric
        ELSE 0::numeric
      END), 0::numeric),
      COALESCE(SUM(CASE
        WHEN COALESCE(preview_row.selected, false) = true
         AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) NOT IN ('NOT_SELECTABLE', 'EXCLUDED')
         AND COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND (
           COALESCE(v_semantic_ready_publication_enabled,false) IS NOT TRUE
           OR (
             LOWER(BTRIM(COALESCE(preview_row.row_json->>'draftable','false'))) IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'selection_allowed','false'))) IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_excluded_from_allocation','false'))) NOT IN ('true','t','1','yes','y','on')
           )
         )
        THEN (preview_row.row_json->>'amount_ex_vat')::numeric
        ELSE 0::numeric
      END), 0::numeric)
    INTO
      v_preview_row_count,
      v_draftable_row_count,
      v_blocked_row_count,
      v_do_not_pay_row_count,
      v_case_resolution_row_count,
      v_total_amount_ex_vat,
      v_draftable_amount_ex_vat
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = v_workbench_session_id
      AND preview_row.candidate_id = v_candidate_id;

    SELECT
      COALESCE(
        jsonb_object_agg(section_counts.section_key, section_counts.section_count ORDER BY section_counts.section_key),
        '{}'::jsonb
      )
    INTO v_preview_section_counts
    FROM (
      SELECT
        COALESCE(NULLIF(BTRIM(COALESCE(preview_row.section, '')), ''), 'canonical_preview_lines') AS section_key,
        COUNT(*)::integer AS section_count
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = v_workbench_session_id
        AND preview_row.candidate_id = v_candidate_id
      GROUP BY COALESCE(NULLIF(BTRIM(COALESCE(preview_row.section, '')), ''), 'canonical_preview_lines')
    ) AS section_counts;

    SELECT
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'PENDING')::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'READY')::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'MATERIALISED')::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'ERROR')::integer
    INTO
      v_pending_line_count,
      v_ready_line_count,
      v_materialised_line_count,
      v_error_line_count
    FROM public.banking_pay_workbench_candidate_line_work AS line_work
    WHERE line_work.session_id = v_workbench_session_id
      AND line_work.candidate_id = v_candidate_id;

    v_summary := jsonb_build_object(
      'candidate_count', 1,
      'canonical_preview_line_count', COALESCE(v_preview_row_count, 0),
      'ready_preview_line_count', COALESCE(v_draftable_row_count, 0),
      'blocked_preview_line_count', COALESCE(v_blocked_row_count, 0),
      'do_not_pay_line_count', COALESCE(v_do_not_pay_row_count, 0),
      'case_resolution_state_count', COALESCE(v_case_resolution_row_count, 0),
      'pending_line_work_count', COALESCE(v_pending_line_count, 0),
      'ready_line_work_count', COALESCE(v_ready_line_count, 0),
      'materialised_line_work_count', COALESCE(v_materialised_line_count, 0),
      'error_line_work_count', COALESCE(v_error_line_count, 0),
      'preview_section_counts', COALESCE(v_preview_section_counts, '{}'::jsonb),
      'total_amount_ex_vat', round(COALESCE(v_total_amount_ex_vat, 0), 2),
      'draftable_amount_ex_vat', round(COALESCE(v_draftable_amount_ex_vat, 0), 2),
      'semantic_ready_publication_enabled',COALESCE(v_semantic_ready_publication_enabled,false),
      'is_ready_for_draft', (COALESCE(v_draftable_row_count, 0) > 0 AND COALESCE(v_error_line_count, 0) = 0),
      'is_review_required', (COALESCE(v_blocked_row_count, 0) > 0 OR COALESCE(v_do_not_pay_row_count, 0) > 0 OR COALESCE(v_case_resolution_row_count, 0) > 0 OR COALESCE(v_error_line_count, 0) > 0),
      'row_backed', true
    );

    v_candidate_row := jsonb_build_object(
      'candidate_id', v_candidate_id::text,
      'row_backed', true,
      'is_ready_for_draft', (COALESCE(v_draftable_row_count, 0) > 0 AND COALESCE(v_error_line_count, 0) = 0),
      'is_review_required', (COALESCE(v_blocked_row_count, 0) > 0 OR COALESCE(v_do_not_pay_row_count, 0) > 0 OR COALESCE(v_case_resolution_row_count, 0) > 0 OR COALESCE(v_error_line_count, 0) > 0),
      'preview_row_count', COALESCE(v_preview_row_count, 0),
      'draftable_preview_row_count', COALESCE(v_draftable_row_count, 0),
      'pending_line_work_count', COALESCE(v_pending_line_count, 0),
      'ready_line_work_count', COALESCE(v_ready_line_count, 0),
      'materialised_line_work_count', COALESCE(v_materialised_line_count, 0),
      'error_line_work_count', COALESCE(v_error_line_count, 0)
    );

    RETURN jsonb_build_object(
      'candidate_id', v_candidate_id::text,
      'workbench_session_id', v_workbench_session_id::text,
      'row_backed_summary_fragment', true,
      'candidate_row', v_candidate_row,
      'summary_fragment', v_summary,
      'case_resolution_states', '[]'::jsonb,
      'canonical_preview_lines', '[]'::jsonb,
      'hidden_recovery_template_lines', '[]'::jsonb,
      'hidden_recovery_template_line_count', 0,
      'payees', '[]'::jsonb,
      'itemisation', '[]'::jsonb,
      'blocked_items', '[]'::jsonb,
      'do_not_pay_items', '[]'::jsonb,
      'snoozed_items', '[]'::jsonb,
      'baseline_component_rows', '[]'::jsonb,
      'paye_candidates', '[]'::jsonb,
      'non_paye_candidates', '[]'::jsonb,
      'paye_summary_breakdown', '{}'::jsonb
    );
  END IF;


  if to_regclass('pg_temp.pay_preview_candidate_context') is not null then
    select count(*)::int
    into v_context_row_count
    from pg_temp.pay_preview_candidate_context ctx;
  else
    v_context_row_count := 0;
  end if;

  if v_context_row_count > 0 then
    select (ctx.candidate_id is not distinct from v_candidate_id and ctx.context_json = v_context_json)
    into v_context_matches
    from pg_temp.pay_preview_candidate_context ctx
    limit 1;
  else
    v_context_matches := false;
  end if;

  if to_regclass('pg_temp.pay_preview_candidate_context') is null or coalesce(v_context_matches, false) = false then
    perform public.pay_preview_candidate_collect_scope(v_context_json, v_candidate_id);
  end if;

  if to_regclass('pg_temp.canonical_preview_lines') is null then
    perform public.pay_preview_candidate_build_canonical_lines(v_context_json, v_candidate_id);
  end if;

  select
    ctx.candidate_id,
    ctx.pay_date,
    ctx.week_ending_cutoff,
    ctx.client_id,
    ctx.actor_user_id,
    ctx.week_start,
    ctx.today_uk,
    ctx.pay_eligibility_months_back,
    ctx.pay_eligibility_weeks_ahead,
    ctx.eligibility_from_date,
    ctx.eligibility_to_date,
    ctx.vat_rate_pct,
    ctx.erni_pct,
    ctx.rail_provider_default,
    ctx.rail_env_default,
    ctx.rail_supports_scheduling,
    ctx.rail_supports_name_check,
    ctx.rail_supports_auto_execute,
    ctx.default_schedule_umbrella_local,
    ctx.default_schedule_paye_local,
    ctx.funds_warning_hours_json,
    ctx.need_name_check,
    ctx.requires_payee_map,
    ctx.paye_guardrails
  into
    v_candidate_id,
    v_pay_date,
    v_week_ending_cutoff,
    v_client_id,
    v_actor_user_id,
    v_week_start,
    v_today_uk,
    v_pay_eligibility_months_back,
    v_pay_eligibility_weeks_ahead,
    v_eligibility_from_date,
    v_eligibility_to_date,
    v_vat_rate_pct,
    v_erni_pct,
    v_rail_provider_default,
    v_rail_env_default,
    v_rail_supports_scheduling,
    v_rail_supports_name_check,
    v_rail_supports_auto_execute,
    v_default_schedule_umbrella_local,
    v_default_schedule_paye_local,
    v_funds_warning_hours_json,
    v_need_name_check,
    v_requires_payee_map,
    v_paye_guardrails
  from pg_temp.pay_preview_candidate_context ctx
  limit 1;

  drop table if exists pg_temp.summary_json, pg_temp.timesheet_case_states_flat, pg_temp.finance_case_states_flat, pg_temp.candidate_case_states_flat, pg_temp.candidate_case_states, pg_temp.case_resolution_states_json, pg_temp.finance_candidate_totals, pg_temp.candidate_finance_itemisation, pg_temp.candidate_timesheet_itemisation_merged, pg_temp.paye_summary_breakdown_json, pg_temp.timesheet_baseline_component_rows, pg_temp.finance_baseline_component_rows, pg_temp.baseline_component_rows_json;

  create temporary table summary_json on commit drop as
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
  
  ;

  create temporary table timesheet_case_states_flat on commit drop as
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
  
  ;

  create temporary table finance_case_states_flat on commit drop as
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
  
  ;

  create temporary table candidate_case_states_flat on commit drop as
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
  
  ;

  create temporary table candidate_case_states on commit drop as
        select
          ccsf.candidate_id,
          count(*)::int as total_case_count,
          sum(case when ccsf.is_blocked then 1 else 0 end)::int as blocked_case_count,
          sum(case when ccsf.is_blocked then 0 else 1 end)::int as safe_case_count,
          coalesce(jsonb_agg(ccsf.case_json order by ccsf.sort_case_order, ccsf.case_key), '[]'::jsonb) as case_resolution_states
        from candidate_case_states_flat ccsf
        group by ccsf.candidate_id
  
  ;

  create temporary table case_resolution_states_json on commit drop as
        select coalesce(jsonb_agg(ccsf.case_json order by ccsf.sort_candidate_display nulls last, ccsf.sort_candidate_tms_ref nulls last, ccsf.sort_case_order, ccsf.case_key), '[]'::jsonb) as payload
        from candidate_case_states_flat ccsf
  
  ;

  create temporary table finance_candidate_totals on commit drop as
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
  
  ;

  create temporary table candidate_finance_itemisation on commit drop as
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
                'nominal_due_amount_ex_vat', round(coalesce(fcl.nominal_due_amount_ex_vat, 0), 2),
                'recoverable_this_pay_run_ex_vat', round(greatest(coalesce(fcl.due_amount_ex_vat, 0), 0), 2),
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
  
  ;

  create temporary table candidate_timesheet_itemisation_merged on commit drop as
        select
          ce.candidate_id,
          (
            coalesce(
              (
                select
                  jsonb_agg(
                    case
                      when ready_match.ready_item is null then base_items.base_item
                      else (base_items.base_item || ready_match.ready_item)
                    end
                    order by base_items.base_ord
                  )
                from (
                  select
                    base_entry.base_ord,
                    base_entry.base_item,
                    coalesce(
                      nullif(btrim(coalesce(base_entry.base_item->>'case_key', '')), ''),
                      case
                        when nullif(btrim(coalesce(base_entry.base_item->>'timesheet_id', '')), '') is null then null
                        else ('timesheet:' || nullif(btrim(coalesce(base_entry.base_item->>'timesheet_id', '')), ''))
                      end
                    ) as match_key
                  from jsonb_array_elements(coalesce(ce.timesheets_itemisation, '[]'::jsonb)) with ordinality as base_entry(base_item, base_ord)
                ) base_items
                left join lateral (
                  select
                    ready_items.ready_item
                  from (
                    select
                      ready_entry.ready_item,
                      ready_entry.ready_ord,
                      coalesce(
                        nullif(btrim(coalesce(ready_entry.ready_item->>'case_key', '')), ''),
                        case
                          when nullif(btrim(coalesce(ready_entry.ready_item->>'timesheet_id', '')), '') is null then null
                          else ('timesheet:' || nullif(btrim(coalesce(ready_entry.ready_item->>'timesheet_id', '')), ''))
                        end
                      ) as match_key
                    from jsonb_array_elements(coalesce(cptr.ready_timesheets_itemisation, '[]'::jsonb)) with ordinality as ready_entry(ready_item, ready_ord)
                  ) ready_items
                  where ready_items.match_key is not null
                    and ready_items.match_key = base_items.match_key
                  order by ready_items.ready_ord
                  limit 1
                ) ready_match on true
              ),
              '[]'::jsonb
            )
            ||
            coalesce(
              (
                select
                  jsonb_agg(ready_items.ready_item order by ready_items.ready_ord)
                from (
                  select
                    ready_entry.ready_item,
                    ready_entry.ready_ord,
                    coalesce(
                      nullif(btrim(coalesce(ready_entry.ready_item->>'case_key', '')), ''),
                      case
                        when nullif(btrim(coalesce(ready_entry.ready_item->>'timesheet_id', '')), '') is null then null
                        else ('timesheet:' || nullif(btrim(coalesce(ready_entry.ready_item->>'timesheet_id', '')), ''))
                      end
                    ) as match_key
                  from jsonb_array_elements(coalesce(cptr.ready_timesheets_itemisation, '[]'::jsonb)) with ordinality as ready_entry(ready_item, ready_ord)
                ) ready_items
                where not exists (
                  select 1
                  from (
                    select
                      coalesce(
                        nullif(btrim(coalesce(base_entry.base_item->>'case_key', '')), ''),
                        case
                          when nullif(btrim(coalesce(base_entry.base_item->>'timesheet_id', '')), '') is null then null
                          else ('timesheet:' || nullif(btrim(coalesce(base_entry.base_item->>'timesheet_id', '')), ''))
                        end
                      ) as match_key
                    from jsonb_array_elements(coalesce(ce.timesheets_itemisation, '[]'::jsonb)) with ordinality as base_entry(base_item, base_ord)
                  ) base_keys
                  where base_keys.match_key is not null
                    and base_keys.match_key = ready_items.match_key
                )
              ),
              '[]'::jsonb
            )
          ) as merged_timesheets_itemisation
        from cand_payee ce
        left join candidate_preview_timesheet_rollup cptr
          on cptr.candidate_id = ce.candidate_id

  ;

  create temporary table paye_summary_breakdown_json on commit drop as
        select jsonb_build_object(
          'gross_side_additions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'GROSS_ADD' and cpl.is_excluded_from_allocation = false then greatest(cpl.amount_ex_vat,0) else 0 end),0),2),
          'gross_side_deductions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'GROSS_DEDUCT' and cpl.is_excluded_from_allocation = false then abs(cpl.amount_ex_vat) else 0 end),0),2),
          'net_side_additions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'NET_ADD' and cpl.is_excluded_from_allocation = false then greatest(cpl.amount_ex_vat,0) else 0 end),0),2),
          'net_side_deductions_ex_vat', round(coalesce(sum(case when cpl.pay_channel = 'PAYE' and cpl.paye_treatment = 'NET_DEDUCT' and cpl.is_excluded_from_allocation = false then abs(cpl.amount_ex_vat) else 0 end),0),2)
        ) as payload
        from canonical_preview_lines cpl
  
  ;

  create temporary table timesheet_baseline_component_rows on commit drop as
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
  
  ;

  create temporary table finance_baseline_component_rows on commit drop as
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
  
  ;

  create temporary table baseline_component_rows_json on commit drop as
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
  
  ;

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
            'itemisation', (coalesce(ctim.merged_timesheets_itemisation, ce.timesheets_itemisation, '[]'::jsonb) || coalesce(cfi.finance_itemisation, '[]'::jsonb))
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
        left join candidate_timesheet_itemisation_merged ctim
          on ctim.candidate_id = ce.candidate_id
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
            'itemisation', (coalesce(ctim.merged_timesheets_itemisation, ce.timesheets_itemisation, '[]'::jsonb) || coalesce(cfi.finance_itemisation, '[]'::jsonb))
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
        left join candidate_timesheet_itemisation_merged ctim
          on ctim.candidate_id = ce.candidate_id
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

  with raw_visible_manual_debt_lines as (
    select
      row_number() over () as line_sort_key,
      visible_line.value as line_value
    from jsonb_array_elements(coalesce(v_canonical_preview_lines, '[]'::jsonb)) as visible_line(value)
    where jsonb_typeof(visible_line.value) = 'object'
      and upper(coalesce(visible_line.value->>'line_type', '')) = 'MANUAL_DEBT_RECOVERY'
      and upper(coalesce(visible_line.value->>'pay_channel', '')) = 'PAYE'
      and upper(coalesce(visible_line.value->>'paye_treatment', '')) = 'NET_DEDUCT'
      and coalesce(nullif(btrim(coalesce(visible_line.value->>'finance_case_id', '')), ''), '') <> ''
  ),
  visible_manual_debt_components as (
    select
      raw_visible_manual_debt_lines.line_sort_key,
      raw_visible_manual_debt_lines.line_value,
      case
        when jsonb_typeof(raw_visible_manual_debt_lines.line_value->'case_components') = 'array'
             and jsonb_array_length(coalesce(raw_visible_manual_debt_lines.line_value->'case_components', '[]'::jsonb)) > 0
          then raw_visible_manual_debt_lines.line_value->'case_components'->0
        else '{}'::jsonb
      end as component_value
    from raw_visible_manual_debt_lines
  ),
  built_hidden_template_lines as (
    select
      visible_manual_debt_components.line_sort_key,
      coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'candidate_id', '')), ''), v_candidate_id::text) as candidate_id_text,
      coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'finance_case_id', '')), ''), '') as finance_case_id_text,
      coalesce(nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'finance_component_id', '')), ''), '') as finance_component_id_text,
      coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'source_ref', '')), ''), '') as source_ref_text,
      jsonb_strip_nulls(
        jsonb_build_object(
          'candidate_id', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'candidate_id', '')), ''), v_candidate_id::text),
          'finance_case_id', nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'finance_case_id', '')), ''),
          'finance_component_id', nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'finance_component_id', '')), ''),
          'recovery_family', 'MANUAL_DEBT_RECOVERY',
          'source_ref', coalesce(
            nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'source_ref', '')), ''),
            case
              when coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'finance_case_id', '')), ''), '') = '' then null
              else 'advance:' || nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'finance_case_id', '')), '')
            end
          ),
          'pay_channel', 'PAYE',
          'paye_treatment', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'paye_treatment', '')), ''), 'NET_DEDUCT'),
          'frozen_component_key_type', nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'component_key_type', '')), ''),
          'frozen_component_key_value', nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'component_key_value', '')), ''),
          'frozen_component_classification', nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'classification', '')), ''),
          'frozen_component_snapshot_json', jsonb_strip_nulls(
            jsonb_build_object(
              'finance_case_id', nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'finance_case_id', '')), ''),
              'case_components', case
                when jsonb_typeof(visible_manual_debt_components.line_value->'case_components') = 'array' then coalesce(visible_manual_debt_components.line_value->'case_components', '[]'::jsonb)
                else '[]'::jsonb
              end,
              'case_resolution_summary', case
                when jsonb_typeof(visible_manual_debt_components.line_value->'case_resolution_summary') = 'object' then visible_manual_debt_components.line_value->'case_resolution_summary'
                else null::jsonb
              end
            )
          ),
          'frozen_source_basis_json', jsonb_strip_nulls(
            coalesce(
              case
                when jsonb_typeof(visible_manual_debt_components.component_value->'source_basis_json') = 'object' then visible_manual_debt_components.component_value->'source_basis_json'
                else '{}'::jsonb
              end,
              '{}'::jsonb
            )
            || jsonb_build_object(
              'recovery_family', 'MANUAL_DEBT_RECOVERY',
              'pay_channel', 'PAYE',
              'frozen_source_pay_method', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'source_pay_method', '')), ''), 'PAYE'),
              'frozen_target_pay_method', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'current_target_pay_method', '')), ''), 'PAYE'),
              'outstanding_amount', to_jsonb(coalesce(
                case when coalesce(visible_manual_debt_components.component_value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->>'remaining_source_amount')::numeric else null::numeric end,
                case when coalesce(visible_manual_debt_components.component_value->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->>'source_amount')::numeric else null::numeric end
              )),
              'nominal_due_amount_ex_vat', to_jsonb(abs(coalesce(
                case when coalesce(visible_manual_debt_components.line_value->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.line_value->>'nominal_due_amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(visible_manual_debt_components.line_value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.line_value->>'amount_ex_vat')::numeric else 0::numeric end,
                0::numeric
              ))),
              'current_due_amount_ex_vat', to_jsonb(abs(coalesce(case when coalesce(visible_manual_debt_components.line_value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.line_value->>'amount_ex_vat')::numeric else 0::numeric end, 0::numeric))),
              'next_due_week_start', coalesce(
                nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'next_due_week_start', '')), ''),
                nullif(btrim(coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'next_due_week_start', '')), '')
              ),
              'sort_order', visible_manual_debt_components.line_sort_key
            )
          ),
          'frozen_source_pay_method', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'source_pay_method', '')), ''), 'PAYE'),
          'frozen_target_pay_method', coalesce(nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'current_target_pay_method', '')), ''), 'PAYE'),
          'frozen_resolution_mode', coalesce(
            nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'saved_resolution_mode', '')), ''),
            nullif(btrim(coalesce(visible_manual_debt_components.component_value->>'resolution_mode', '')), '')
          ),
          'frozen_resolution_payload_json', case
            when jsonb_typeof(visible_manual_debt_components.line_value->'case_resolution_summary') = 'object'
                 and jsonb_typeof(visible_manual_debt_components.line_value->'case_resolution_summary'->'non_bucket_resolution') = 'object'
              then visible_manual_debt_components.line_value->'case_resolution_summary'->'non_bucket_resolution'
            when jsonb_typeof(visible_manual_debt_components.component_value->'saved_resolution_payload_json') = 'object'
              then visible_manual_debt_components.component_value->'saved_resolution_payload_json'
            when jsonb_typeof(visible_manual_debt_components.component_value->'suggested_resolution_payload_json') = 'object'
              then visible_manual_debt_components.component_value->'suggested_resolution_payload_json'
            else null::jsonb
          end,
          'frozen_resolution_result_json', case
            when jsonb_typeof(visible_manual_debt_components.line_value->'case_resolution_summary') = 'object'
              then visible_manual_debt_components.line_value->'case_resolution_summary'
            when jsonb_typeof(visible_manual_debt_components.component_value->'saved_resolution_result_json') = 'object'
              then visible_manual_debt_components.component_value->'saved_resolution_result_json'
            when jsonb_typeof(visible_manual_debt_components.component_value->'suggested_resolution_result_json') = 'object'
              then visible_manual_debt_components.component_value->'suggested_resolution_result_json'
            else null::jsonb
          end,
          'frozen_source_amount', round(coalesce(
            case when coalesce(visible_manual_debt_components.component_value->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->>'source_amount')::numeric else null::numeric end,
            0::numeric
          ), 2),
          'frozen_outstanding_amount', round(coalesce(
            case when coalesce(visible_manual_debt_components.component_value->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->>'remaining_source_amount')::numeric else null::numeric end,
            case when coalesce(visible_manual_debt_components.component_value->>'source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->>'source_amount')::numeric else null::numeric end,
            0::numeric
          ), 2),
          'weekly_due', round(coalesce(
            case when coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'weekly_due', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->'source_basis_json'->>'weekly_due')::numeric else null::numeric end,
            abs(coalesce(case when coalesce(visible_manual_debt_components.line_value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.line_value->>'amount_ex_vat')::numeric else 0::numeric end, 0::numeric)),
            0::numeric
          ), 2),
          'minimum_earnings_threshold', case when coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'minimum_earnings_threshold', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->'source_basis_json'->>'minimum_earnings_threshold')::numeric else null::numeric end,
          'take_home_floor_override', case when coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'take_home_floor_override', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->'source_basis_json'->>'take_home_floor_override')::numeric else null::numeric end,
          'default_take_home_floor', coalesce(
            case when coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'default_take_home_floor', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (visible_manual_debt_components.component_value->'source_basis_json'->>'default_take_home_floor')::numeric else null::numeric end,
            0::numeric
          ),
          'next_due_week_start', coalesce(
            nullif(btrim(coalesce(visible_manual_debt_components.line_value->>'next_due_week_start', '')), ''),
            nullif(btrim(coalesce(visible_manual_debt_components.component_value->'source_basis_json'->>'next_due_week_start', '')), '')
          ),
          'sort_order', visible_manual_debt_components.line_sort_key
        )
      ) as hidden_line
    from visible_manual_debt_components
  ),
  dedup_hidden_template_lines as (
    select distinct on (
      built_hidden_template_lines.candidate_id_text,
      built_hidden_template_lines.finance_case_id_text,
      built_hidden_template_lines.finance_component_id_text,
      built_hidden_template_lines.source_ref_text
    )
      built_hidden_template_lines.line_sort_key,
      built_hidden_template_lines.hidden_line
    from built_hidden_template_lines
    order by
      built_hidden_template_lines.candidate_id_text,
      built_hidden_template_lines.finance_case_id_text,
      built_hidden_template_lines.finance_component_id_text,
      built_hidden_template_lines.source_ref_text,
      built_hidden_template_lines.line_sort_key
  )
  select coalesce(
           jsonb_agg(dedup_hidden_template_lines.hidden_line order by dedup_hidden_template_lines.line_sort_key),
           '[]'::jsonb
         )
  into v_hidden_recovery_template_lines
  from dedup_hidden_template_lines;

  with raw_canonical_preview_lines as (
    select
      row_number() over () as line_sort_key,
      raw_line.value as line_value
    from jsonb_array_elements(coalesce(v_canonical_preview_lines, '[]'::jsonb)) as raw_line(value)
    where jsonb_typeof(raw_line.value) = 'object'
  )
  select coalesce(
           jsonb_agg(filtered_canonical_preview_lines.line_value order by filtered_canonical_preview_lines.line_sort_key),
           '[]'::jsonb
         )
  into v_filtered_canonical_preview_lines
  from (
    select
      raw_canonical_preview_lines.line_sort_key,
      raw_canonical_preview_lines.line_value
    from raw_canonical_preview_lines
    where (
      upper(coalesce(raw_canonical_preview_lines.line_value->>'line_type', '')) <> 'MANUAL_DEBT_RECOVERY'
      or upper(coalesce(raw_canonical_preview_lines.line_value->>'pay_channel', '')) <> 'PAYE'
      or upper(coalesce(raw_canonical_preview_lines.line_value->>'paye_treatment', '')) <> 'NET_DEDUCT'
      or coalesce(nullif(btrim(coalesce(raw_canonical_preview_lines.line_value->>'finance_case_id', '')), ''), '') = ''
      or exists (
        select 1
        from jsonb_array_elements(coalesce(v_hidden_recovery_template_lines, '[]'::jsonb)) as hidden_template_line(hidden_value)
        where jsonb_typeof(hidden_template_line.hidden_value) = 'object'
          and upper(coalesce(hidden_template_line.hidden_value->>'recovery_family', '')) = 'MANUAL_DEBT_RECOVERY'
          and upper(coalesce(nullif(btrim(coalesce(hidden_template_line.hidden_value->>'pay_channel', '')), ''), 'PAYE')) = 'PAYE'
          and coalesce(nullif(btrim(coalesce(hidden_template_line.hidden_value->>'finance_case_id', '')), ''), '') =
              coalesce(nullif(btrim(coalesce(raw_canonical_preview_lines.line_value->>'finance_case_id', '')), ''), '')
      )
    )
  ) as filtered_canonical_preview_lines;

  v_canonical_preview_lines := coalesce(v_filtered_canonical_preview_lines, '[]'::jsonb);

  if jsonb_typeof(v_paye) = 'array'
 and jsonb_array_length(v_paye) > 0 then
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

  select
    coalesce(
      jsonb_agg(
        jsonb_strip_nulls(
          jsonb_build_object(
            'kind', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
            'candidate_id', coalesce(nullif(btrim(coalesce(carry_forward_line.value->>'candidate_id', '')), ''), v_candidate_id::text),
            'manual_adjustment_carry_forward_id', nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), ''),
            'source_ref', coalesce(
              nullif(btrim(coalesce(carry_forward_line.value->>'source_ref', '')), ''),
              case
                when nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), '') is null then null
                else 'carry_forward:' || nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), '')
              end
            ),
            'operation_source_key', coalesce(
              nullif(btrim(coalesce(carry_forward_line.value->>'operation_source_key', '')), ''),
              case
                when nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), '') is null then null
                else 'carry_forward:' || nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), '')
              end
            ),
            'line_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
            'description', coalesce(
              nullif(btrim(coalesce(carry_forward_line.value->>'description', '')), ''),
              nullif(btrim(coalesce(carry_forward_line.value->>'adjustment_comment', '')), ''),
              'Manual adjustment carry-forward'
            ),
            'adjustment_comment', coalesce(
              nullif(btrim(coalesce(carry_forward_line.value->>'adjustment_comment', '')), ''),
              nullif(btrim(coalesce(carry_forward_line.value->>'description', '')), ''),
              'Manual adjustment carry-forward'
            ),
            'payment_amount_ex_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat')::numeric, 2)
              else 0::numeric
            end,
            'payment_amount_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat')::numeric, 2)
              else 0::numeric
            end,
            'payment_amount_inc_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display')::numeric, 2)
              else 0::numeric
            end,
            'amount_ex_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat')::numeric, 2)
              else 0::numeric
            end,
            'amount_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat')::numeric, 2)
              else 0::numeric
            end,
            'amount_inc_vat', case
              when coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display')::numeric, 2)
              else 0::numeric
            end,
            'amount_display', case
              when coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then round(coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display')::numeric, 2)
              else 0::numeric
            end,
            'pay_channel', upper(btrim(coalesce(carry_forward_line.value->>'pay_channel', ''))),
            'paye_treatment', nullif(btrim(coalesce(carry_forward_line.value->>'paye_treatment', '')), ''),
            'tax_treatment_json', case
              when jsonb_typeof(carry_forward_line.value->'tax_treatment_json') = 'object' then carry_forward_line.value->'tax_treatment_json'
              else '{}'::jsonb
            end,
            'taxability', nullif(btrim(coalesce(carry_forward_line.value->>'taxability', carry_forward_line.value#>>'{tax_treatment_json,taxability}', '')), ''),
            'adjustment_direction', nullif(btrim(coalesce(carry_forward_line.value->>'adjustment_direction', '')), ''),
            'source_snapshot_json', case
              when jsonb_typeof(carry_forward_line.value->'source_snapshot_json') = 'object' then carry_forward_line.value->'source_snapshot_json'
              else '{}'::jsonb
            end
          )
        )
        order by coalesce(carry_forward_line.value->>'preview_row_id', carry_forward_line.value->>'manual_adjustment_carry_forward_id')
      ),
      '[]'::jsonb
    ),
    coalesce(sum(
      case
        when coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then round(coalesce(carry_forward_line.value->>'payment_amount_ex_vat', carry_forward_line.value->>'amount_ex_vat')::numeric, 2)
        else 0::numeric
      end
    ), 0::numeric),
    coalesce(sum(
      case
        when coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then round(coalesce(carry_forward_line.value->>'payment_amount_vat', carry_forward_line.value->>'amount_vat')::numeric, 2)
        else 0::numeric
      end
    ), 0::numeric),
    coalesce(sum(
      case
        when coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then round(coalesce(carry_forward_line.value->>'payment_amount_inc_vat', carry_forward_line.value->>'amount_inc_vat', carry_forward_line.value->>'amount_display')::numeric, 2)
        else 0::numeric
      end
    ), 0::numeric)
  into
    v_carry_forward_itemisation,
    v_carry_forward_total_ex_vat,
    v_carry_forward_total_vat,
    v_carry_forward_total_inc_vat
  from jsonb_array_elements(coalesce(v_canonical_preview_lines, '[]'::jsonb)) as carry_forward_line(value)
  where jsonb_typeof(carry_forward_line.value) = 'object'
    and upper(coalesce(carry_forward_line.value->>'line_type', '')) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_itemisation, '[]'::jsonb)) as existing_item(value)
      where nullif(btrim(coalesce(existing_item.value->>'manual_adjustment_carry_forward_id', '')), '') is not null
        and nullif(btrim(coalesce(existing_item.value->>'manual_adjustment_carry_forward_id', '')), '') =
            nullif(btrim(coalesce(carry_forward_line.value->>'manual_adjustment_carry_forward_id', '')), '')
    );

  v_itemisation := coalesce(v_itemisation, '[]'::jsonb) || coalesce(v_carry_forward_itemisation, '[]'::jsonb);

  v_candidate_row := coalesce(v_candidate_row, '{}'::jsonb)
    || jsonb_build_object(
      'itemisation', coalesce(v_itemisation, '[]'::jsonb),
      'manual_adjustment_carry_forward_total_ex_vat', round(coalesce(v_carry_forward_total_ex_vat, 0), 2),
      'manual_adjustment_carry_forward_total_vat', round(coalesce(v_carry_forward_total_vat, 0), 2),
      'manual_adjustment_carry_forward_total_inc_vat', round(coalesce(v_carry_forward_total_inc_vat, 0), 2)
    )
    || case
      when coalesce(v_candidate_row->>'gross_preview_ex_vat_non_mismatch', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then jsonb_build_object(
          'gross_preview_ex_vat_non_mismatch',
          round((v_candidate_row->>'gross_preview_ex_vat_non_mismatch')::numeric + coalesce(v_carry_forward_total_ex_vat, 0), 2)
        )
      else '{}'::jsonb
    end
    || case
      when coalesce(v_candidate_row->>'computed_net_bank_amount_non_mismatch', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then jsonb_build_object(
          'computed_net_bank_amount_non_mismatch',
          round((v_candidate_row->>'computed_net_bank_amount_non_mismatch')::numeric + coalesce(v_carry_forward_total_inc_vat, 0), 2)
        )
      else '{}'::jsonb
    end;

  IF upper(coalesce(v_candidate_row->>'current_pay_method', '')) = 'PAYE' THEN
    SELECT coalesce(
             jsonb_agg(
               CASE
                 WHEN coalesce(candidate_row.value->>'candidate_id', '') = v_candidate_id::text THEN v_candidate_row
                 ELSE candidate_row.value
               END
               ORDER BY candidate_row.ord
             ),
             '[]'::jsonb
           )
    INTO v_paye
    FROM jsonb_array_elements(coalesce(v_paye, '[]'::jsonb)) WITH ORDINALITY AS candidate_row(value, ord);

    IF NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(coalesce(v_paye, '[]'::jsonb)) AS candidate_row(value)
      WHERE coalesce(candidate_row.value->>'candidate_id', '') = v_candidate_id::text
    ) THEN
      v_paye := coalesce(v_paye, '[]'::jsonb) || jsonb_build_array(v_candidate_row);
    END IF;
  ELSE
    SELECT coalesce(
             jsonb_agg(
               CASE
                 WHEN coalesce(candidate_row.value->>'candidate_id', '') = v_candidate_id::text THEN v_candidate_row
                 ELSE candidate_row.value
               END
               ORDER BY candidate_row.ord
             ),
             '[]'::jsonb
           )
    INTO v_nonpaye
    FROM jsonb_array_elements(coalesce(v_nonpaye, '[]'::jsonb)) WITH ORDINALITY AS candidate_row(value, ord);

    IF NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(coalesce(v_nonpaye, '[]'::jsonb)) AS candidate_row(value)
      WHERE coalesce(candidate_row.value->>'candidate_id', '') = v_candidate_id::text
    ) THEN
      v_nonpaye := coalesce(v_nonpaye, '[]'::jsonb) || jsonb_build_array(v_candidate_row);
    END IF;
  END IF;

  select jsonb_build_object(
    'candidate_count', 1,
    'case_resolution_state_count', case when jsonb_typeof(coalesce(v_case_resolution_states, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_case_resolution_states, '[]'::jsonb)) else 0 end,
    'canonical_preview_line_count', case when jsonb_typeof(coalesce(v_canonical_preview_lines, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_canonical_preview_lines, '[]'::jsonb)) else 0 end,
    'manual_adjustment_carry_forward_line_count', case when jsonb_typeof(coalesce(v_carry_forward_itemisation, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_carry_forward_itemisation, '[]'::jsonb)) else 0 end,
    'manual_adjustment_carry_forward_total_ex_vat', round(coalesce(v_carry_forward_total_ex_vat, 0), 2),
    'manual_adjustment_carry_forward_total_vat', round(coalesce(v_carry_forward_total_vat, 0), 2),
    'manual_adjustment_carry_forward_total_inc_vat', round(coalesce(v_carry_forward_total_inc_vat, 0), 2),
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
    'hidden_recovery_template_lines', coalesce(v_hidden_recovery_template_lines, '[]'::jsonb),
    'manual_adjustment_carry_forward_itemisation', coalesce(v_carry_forward_itemisation, '[]'::jsonb),
    'manual_adjustment_carry_forward_line_count', case when jsonb_typeof(coalesce(v_carry_forward_itemisation, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_carry_forward_itemisation, '[]'::jsonb)) else 0 end,
    'manual_adjustment_carry_forward_total_ex_vat', round(coalesce(v_carry_forward_total_ex_vat, 0), 2),
    'manual_adjustment_carry_forward_total_vat', round(coalesce(v_carry_forward_total_vat, 0), 2),
    'manual_adjustment_carry_forward_total_inc_vat', round(coalesce(v_carry_forward_total_inc_vat, 0), 2),
    'hidden_recovery_template_line_count', case when jsonb_typeof(coalesce(v_hidden_recovery_template_lines, '[]'::jsonb)) = 'array' then jsonb_array_length(coalesce(v_hidden_recovery_template_lines, '[]'::jsonb)) else 0 end,
    'payees', coalesce(v_payees, '[]'::jsonb),
    'itemisation', coalesce(v_itemisation, '[]'::jsonb),
    'blocked_items', coalesce(v_blocked, '[]'::jsonb),
    'do_not_pay_items', coalesce(v_do_not_pay, '[]'::jsonb),
    'snoozed_items', coalesce(v_snoozed, '[]'::jsonb),
    'baseline_component_rows', coalesce(v_baseline_component_rows, '[]'::jsonb),
    'paye_candidates', coalesce(v_paye, '[]'::jsonb),
    'non_paye_candidates', coalesce(v_nonpaye, '[]'::jsonb),
    'paye_summary_breakdown', coalesce(v_paye_summary_breakdown, '{}'::jsonb)
  );
end;
$function$;


DROP FUNCTION IF EXISTS public.pay_preview_candidate_collect_scope(jsonb, uuid);
