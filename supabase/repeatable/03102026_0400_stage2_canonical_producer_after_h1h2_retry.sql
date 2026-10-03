-- Exact adjacent Stage 2 preview producer family reassertion after a resumed H1/H2 closure.
-- Generated from supabase/repeatable/26092026_0203_banking_pay_stage2_workbench_draft_v1.sql; function SHA-256 b0845aabbf78ec5cf8d19772083879edbcc880eb04e3504838bf83048b565f38.
-- Replaces only these three current functions; no payment/provider action is executed.
\set ON_ERROR_STOP on
begin;
CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines(p_context_json jsonb, p_candidate_id uuid)
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

  v_workbench_session_id uuid := NULL::uuid;
  v_workbench_session_id_text text := NULL::text;
  v_line_materialise_limit integer := 100;
  v_line_materialise_result jsonb := '{}'::jsonb;
  v_workbench_line_source_mode text := NULL::text;
  v_workbench_classifier_only boolean := false;
  v_source_build_mode boolean := false;
  v_source_build_context_required boolean := false;
  v_collect_called_inside_canonical boolean := false;
  v_collect_recollect_attempted boolean := false;
  v_collect_recollect_blocked boolean := false;
  v_canonical_started_at timestamptz := clock_timestamp();
  v_canonical_elapsed_ms numeric := 0;
  v_semantic_ready_observe_enabled boolean := false;
  v_semantic_ready_publication_enabled boolean := false;
  v_allocation_segment_failure jsonb := '{}'::jsonb;
  -- A28 reconciliation slicing (final slice only; bound to the current Workbench build token)
  v_a28_slice jsonb := CASE WHEN COALESCE(current_setting('cloudtms.pay_workbench_reconcile_slice_v1', true), '') ~ '^\s*\{'
    THEN current_setting('cloudtms.pay_workbench_reconcile_slice_v1', true)::jsonb ELSE NULL::jsonb END;
  v_a28_slice_applies boolean := false;
  v_a28_slice_candidate_id uuid := NULL::uuid;
begin
  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'p_context_json must be a JSON object';
  end if;

  if v_candidate_id is null then
    raise exception 'candidate_id is required';
  end if;

  SELECT
    COALESCE(settings_row.banking_pay_workbench_semantic_ready_observe_v2_enabled,false),
    COALESCE(settings_row.banking_pay_workbench_semantic_ready_publication_v3_enabled,false)
  INTO v_semantic_ready_observe_enabled,v_semantic_ready_publication_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;


  v_source_build_mode := (
    LOWER(BTRIM(COALESCE(
      v_context_json->>'source_build_mode',
      v_context_json->>'workbench_source_build_mode',
      v_context_json #>> '{source_build,enabled}',
      v_context_json #>> '{workbench,source_build_mode}',
      ''
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR UPPER(BTRIM(COALESCE(
      v_context_json->>'job_type',
      v_context_json #>> '{job,type}',
      v_context_json #>> '{source_build,job_type}',
      v_context_json #>> '{workbench,job_type}',
      ''
    ))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
  );

  v_source_build_context_required := COALESCE(v_source_build_mode, false);


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

  IF COALESCE(v_context_json->>'line_work_limit', v_context_json#>>'{line_work,limit}', v_context_json->>'limit', '') ~ '^[0-9]+$' THEN
    v_line_materialise_limit := LEAST(GREATEST(COALESCE(v_context_json->>'line_work_limit', v_context_json#>>'{line_work,limit}', v_context_json->>'limit')::integer, 1), 100);
  ELSE
    v_line_materialise_limit := 100;
  END IF;

  v_workbench_line_source_mode := UPPER(NULLIF(BTRIM(COALESCE(
    v_context_json->>'workbench_line_source_mode',
    v_context_json->>'line_source_mode',
    v_context_json#>>'{line_work,source_mode}',
    v_context_json#>>'{workbench,line_source_mode}',
    ''
  )), ''));

  v_workbench_classifier_only := COALESCE(v_workbench_line_source_mode, '') IN (
    'CLASSIFY_ONLY',
    'CLASSIFIER_ONLY',
    'LINE_SOURCE_CLASSIFY_ONLY',
    'CANONICAL_CLASSIFY_ONLY'
  );

  IF v_workbench_session_id IS NOT NULL AND COALESCE(v_workbench_classifier_only, false) IS NOT TRUE AND COALESCE(v_source_build_mode, false) IS NOT TRUE THEN
    v_line_materialise_result := public.pay_workbench_preview_rows_materialise_chunk(
      p_session_id => v_workbench_session_id,
      p_candidate_id => v_candidate_id,
      p_cursor_json => COALESCE(
        v_context_json->'line_materialise_cursor',
        v_context_json->'line_work_cursor',
        v_context_json#>'{line_work,cursor}',
        NULL::jsonb
      ),
      p_limit => v_line_materialise_limit
    );

    RETURN jsonb_build_object(
      'candidate_id', v_candidate_id::text,
      'session_id', v_workbench_session_id::text,
      'row_backed_line_work', true,
      'canonical_display_rows_written', true,
      'canonical_preview_line_count', COALESCE(NULLIF(BTRIM(COALESCE(v_line_materialise_result->>'materialised_count', '')), '')::integer, 0),
      'ready_preview_line_count', COALESCE(NULLIF(BTRIM(COALESCE(v_line_materialise_result->>'materialised_count', '')), '')::integer, 0),
      'blocked_preview_line_count', 0,
      'hidden_recovery_template_line_count', 0,
      'manual_adjustment_carry_forward_line_count', 0,
      'next_cursor', v_line_materialise_result->'next_cursor',
      'has_more', COALESCE(NULLIF(v_line_materialise_result->>'has_more', '')::boolean, false),
      'materialise_result', COALESCE(v_line_materialise_result, '{}'::jsonb)
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
    v_collect_recollect_attempted := true;

    IF COALESCE(v_source_build_context_required, false) = true THEN
      v_collect_recollect_blocked := true;
      RAISE EXCEPTION 'SOURCE_BUILD_CANONICAL_REQUIRES_PRECOLLECTED_CONTEXT'
        USING DETAIL = 'WORKBENCH_CANDIDATE_SOURCE_BUILD canonical mode refuses to call pay_preview_candidate_collect_scope implicitly. Call pay_preview_candidate_collect_scope first with the bounded source-build payload.';
    END IF;

    v_collect_called_inside_canonical := true;
    perform public.pay_preview_candidate_collect_scope(v_context_json, v_candidate_id);
  end if;

  if to_regclass('pg_temp.finance_case_resolution_rollup') is null then
    perform public.pay_preview_candidate_build_finance_case_baseline(v_context_json, v_candidate_id);
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

  drop table if exists pg_temp.canonical_timesheet_lines, pg_temp.timesheet_active_segment_snooze_meta, pg_temp.canonical_timesheet_segment_rows, pg_temp.canonical_timesheet_segment_rollup, pg_temp.canonical_timesheet_presentation_seed, pg_temp.canonical_timesheet_presentation_state, pg_temp.canonical_timesheet_presentation_rows, pg_temp.finance_case_lines, pg_temp.hidden_recovery_template_lines, pg_temp.manual_adjustment_carry_forward_lines, pg_temp.timesheet_allocation_component_lines, pg_temp.semantic_finance_case_lines, pg_temp.timesheet_canonical_preview_lines, pg_temp.canonical_preview_lines, pg_temp.candidate_preview_line_rollup, pg_temp.candidate_preview_timesheet_rollup;

  create temporary table canonical_timesheet_lines on commit drop as
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
          cp.payee_bank_hash,
          cp.payee_name_check_status,
          cp.payee_name_check_has_override,
          cp.payee_map_present,
          coalesce(cp.blockers, '[]'::jsonb) as payee_blockers,
          (jsonb_array_length(coalesce(cp.blockers, '[]'::jsonb)) > 0) as has_payee_readiness_block,
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
          coalesce(expense_components.expense_aware_case_components_json, tcr.case_components_json, '[]'::jsonb) as case_components_json,
          coalesce(expense_components.ready_expense_amount_ex_vat, 0) as ready_expense_amount_ex_vat,
          coalesce(expense_components.blocked_expense_amount_ex_vat, 0) as blocked_expense_amount_ex_vat,
          coalesce(expense_components.hidden_expense_amount_ex_vat, 0) as hidden_expense_amount_ex_vat,
          coalesce(expense_components.ready_expense_count, 0) as ready_expense_count,
          coalesce(expense_components.blocked_expense_count, 0) as blocked_expense_count,
          coalesce(expense_components.hidden_expense_count, 0) as hidden_expense_count,
          coalesce(expense_components.active_expense_snooze_count, 0) as active_expense_snooze_count,
          coalesce(expense_components.stale_expense_identity_count, 0) as stale_expense_identity_count,
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
                'nhsp_shift_id', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'nhsp_shift_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'nhsp_shift_id','')), '')
                ),
                'shift_id', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'shift_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'shift_id','')), ''),
                  nullif(btrim(coalesce(cur_seg.seg->>'nhsp_shift_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'nhsp_shift_id','')), '')
                ),
                'external_row_key', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'external_row_key','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'external_row_key','')), '')
                ),
                'hr_request_id', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'hr_request_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'hr_request_id','')), '')
                ),
                'request_id', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'request_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'request_id','')), ''),
                  nullif(btrim(coalesce(cur_seg.seg->>'hr_request_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'hr_request_id','')), '')
                ),
                'source_system', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'source_system','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'source_system','')), '')
                ),
                'latest_import_id', coalesce(
                  nullif(btrim(coalesce(cur_seg.seg->>'latest_import_id','')), ''),
                  nullif(btrim(coalesce(delta_seg.seg->>'latest_import_id','')), '')
                ),
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
        left join lateral (
          with component_rows as (
            select
              component_element.value as component_json,
              component_element.ordinality::integer as component_ordinal,
              upper(nullif(btrim(coalesce(component_element.value->>'component_key_type', '')), '')) as component_key_type,
              upper(nullif(btrim(coalesce(component_element.value->>'component_key_value', component_element.value#>>'{source_basis_json,expense_code}', '')), '')) as expense_code,
              case
                when jsonb_typeof(component_element.value->'source_basis_json') = 'object'
                  then coalesce(component_element.value->'source_basis_json', '{}'::jsonb)
                else '{}'::jsonb
              end as source_basis_json,
              lower(coalesce(
                nullif(btrim(coalesce(component_element.value->>'source_basis_fingerprint', '')), ''),
                md5(coalesce(component_element.value->'source_basis_json', '{}'::jsonb)::text)
              )) as source_basis_fingerprint,
              round(coalesce(
                case when coalesce(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (component_element.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (component_element.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (component_element.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (component_element.value->>'target_pay_ex_vat')::numeric else null::numeric end,
                0::numeric
              ), 2) as component_amount_ex_vat
            from jsonb_array_elements(
              case
                when jsonb_typeof(tcr.case_components_json) = 'array' then coalesce(tcr.case_components_json, '[]'::jsonb)
                else '[]'::jsonb
              end
            ) with ordinality as component_element(value, ordinality)
          ), expense_identity_rows as (
            select
              component_rows.*,
              (
                component_rows.component_key_type = 'EXPENSE_CODE'
                and component_rows.expense_code in ('EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE')
                and component_rows.source_basis_fingerprint ~ '^[0-9a-f]{32}$'
              ) as is_expense_component,
              case
                when component_rows.component_key_type = 'EXPENSE_CODE'
                 and component_rows.expense_code in ('EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE')
                 and component_rows.source_basis_fingerprint ~ '^[0-9a-f]{32}$'
                then lower(
                  'timesheet-expense:' || tcr.timesheet_id::text || ':' ||
                  component_rows.expense_code || ':' || component_rows.source_basis_fingerprint
                )
                else null::text
              end as expense_source_ref,
              case component_rows.expense_code
                when 'EXPENSES' then 'Expenses'
                when 'TRAVEL' then 'Travel'
                when 'ACCOMMODATION' then 'Accommodation'
                when 'OTHER' then 'Other'
                when 'MILEAGE' then 'Mileage'
                else null::text
              end as expense_label
            from component_rows
          ), enriched_rows as (
            select
              expense_identity_rows.*,
              exact_snooze.snooze_id as exact_snooze_id,
              exact_snooze.snooze_until_date as exact_snooze_until_date,
              exact_snooze.note as exact_snooze_note,
              exact_snooze.snooze_kind as exact_snooze_kind,
              stale_snooze.snooze_id as stale_snooze_id,
              stale_snooze.source_ref as stale_source_ref,
              stale_snooze.snooze_until_date as stale_snooze_until_date,
              stale_snooze.note as stale_snooze_note
            from expense_identity_rows
            left join lateral (
              select
                active_snooze.snooze_id,
                active_snooze.snooze_until_date,
                active_snooze.note,
                active_snooze.snooze_kind
              from active_snoozes as active_snooze
              where expense_identity_rows.is_expense_component
                and active_snooze.candidate_id = tcr.candidate_id
                and active_snooze.source_ref is not null
                and lower(active_snooze.source_ref) = expense_identity_rows.expense_source_ref
                and active_snooze.snooze_kind = 'DO_NOT_PAY'
              order by active_snooze.snooze_id
              limit 1
            ) as exact_snooze on true
            left join lateral (
              select
                active_snooze.snooze_id,
                active_snooze.source_ref,
                active_snooze.snooze_until_date,
                active_snooze.note
              from active_snoozes as active_snooze
              where expense_identity_rows.is_expense_component
                and exact_snooze.snooze_id is null
                and active_snooze.candidate_id = tcr.candidate_id
                and active_snooze.timesheet_id = tcr.timesheet_id
                and active_snooze.source_ref is not null
                and lower(active_snooze.source_ref) like lower(
                  'timesheet-expense:' || tcr.timesheet_id::text || ':' ||
                  expense_identity_rows.expense_code || ':%'
                )
                and lower(active_snooze.source_ref) is distinct from expense_identity_rows.expense_source_ref
                and active_snooze.snooze_kind = 'DO_NOT_PAY'
              order by active_snooze.snooze_id
              limit 1
            ) as stale_snooze on true
          ), state_rows as (
            select
              enriched_rows.*,
              case
                when not enriched_rows.is_expense_component then 'NOT_EXPENSE'
                when enriched_rows.exact_snooze_id is not null and enriched_rows.exact_snooze_until_date is null then 'HIDDEN_INDEFINITE'
                when enriched_rows.exact_snooze_id is not null then 'BLOCKED_DATED'
                when enriched_rows.stale_snooze_id is not null then 'BLOCKED_STALE_IDENTITY'
                else 'READY'
              end as expense_presentation_state
            from enriched_rows
          )
          select
            coalesce(
              jsonb_agg(
                case
                  when state_rows.is_expense_component then
                    state_rows.component_json
                    || jsonb_build_object(
                      'expense_code', state_rows.expense_code,
                      'expense_label', state_rows.expense_label,
                      'expense_item_type', case when state_rows.expense_code = 'MILEAGE' then 'MILEAGE_DELTA' else 'EXPENSE_DELTA' end,
                      'source_ref', state_rows.expense_source_ref,
                      'source_basis_fingerprint', state_rows.source_basis_fingerprint,
                      'expense_source_basis_fingerprint', state_rows.source_basis_fingerprint,
                      'expense_source_basis_json', state_rows.source_basis_json
                    )
                    || jsonb_build_object(
                      'presentation_section', case
                        when state_rows.expense_presentation_state = 'READY' then 'READY_TO_PAY'
                        when state_rows.expense_presentation_state in ('BLOCKED_DATED', 'BLOCKED_STALE_IDENTITY') then 'BLOCKED_FOR_PAY'
                        else 'INTERNAL_ONLY'
                      end,
                      'expense_presentation_state', state_rows.expense_presentation_state,
                      'draftable', (state_rows.expense_presentation_state = 'READY'),
                      'is_ready_for_draft', (state_rows.expense_presentation_state = 'READY'),
                      'is_excluded_from_allocation', (state_rows.expense_presentation_state <> 'READY'),
                      'selection_allowed', (state_rows.expense_presentation_state = 'READY'),
                      'expense_identity_stale', (state_rows.expense_presentation_state = 'BLOCKED_STALE_IDENTITY')
                    )
                    || jsonb_build_object(
                      'snooze_identity', jsonb_build_object(
                        'identity_type', 'TIMESHEET_EXPENSE',
                        'timesheet_id', tcr.timesheet_id::text,
                        'booking_id', tb.ts_booking_id,
                        'segment_id', null,
                        'segment_stable_key', null,
                        'source_ref', state_rows.expense_source_ref,
                        'expense_code', state_rows.expense_code,
                        'source_basis_fingerprint', state_rows.source_basis_fingerprint
                      ),
                      'snooze_state', case
                        when state_rows.exact_snooze_id is null and state_rows.stale_snooze_id is null
                          then jsonb_build_object('state', 'NONE')
                        when state_rows.exact_snooze_id is not null and state_rows.exact_snooze_until_date is null
                          then jsonb_build_object(
                            'state', 'INDEFINITE_SNOOZED',
                            'snooze_id', state_rows.exact_snooze_id::text,
                            'snooze_until_date', null,
                            'note', state_rows.exact_snooze_note,
                            'snooze_kind', state_rows.exact_snooze_kind
                          )
                        when state_rows.exact_snooze_id is not null
                          then jsonb_build_object(
                            'state', 'DATED_SNOOZED',
                            'snooze_id', state_rows.exact_snooze_id::text,
                            'snooze_until_date', state_rows.exact_snooze_until_date::text,
                            'note', state_rows.exact_snooze_note,
                            'snooze_kind', state_rows.exact_snooze_kind
                          )
                        else jsonb_build_object(
                          'state', 'STALE_SOURCE_IDENTITY',
                          'snooze_id', state_rows.stale_snooze_id::text,
                          'snooze_until_date', case when state_rows.stale_snooze_until_date is null then null else state_rows.stale_snooze_until_date::text end,
                          'note', state_rows.stale_snooze_note,
                          'stale_source_ref', state_rows.stale_source_ref,
                          'refresh_required', true
                        )
                      end
                    )
                  else state_rows.component_json
                end
                order by state_rows.component_ordinal
              ),
              '[]'::jsonb
            ) as expense_aware_case_components_json,
            round(coalesce(sum(state_rows.component_amount_ex_vat) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state = 'READY'
            ), 0), 2) as ready_expense_amount_ex_vat,
            round(coalesce(sum(state_rows.component_amount_ex_vat) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state in ('BLOCKED_DATED', 'BLOCKED_STALE_IDENTITY')
            ), 0), 2) as blocked_expense_amount_ex_vat,
            round(coalesce(sum(state_rows.component_amount_ex_vat) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state = 'HIDDEN_INDEFINITE'
            ), 0), 2) as hidden_expense_amount_ex_vat,
            count(*) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state = 'READY'
            )::integer as ready_expense_count,
            count(*) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state in ('BLOCKED_DATED', 'BLOCKED_STALE_IDENTITY')
            )::integer as blocked_expense_count,
            count(*) filter (
              where state_rows.is_expense_component and state_rows.expense_presentation_state = 'HIDDEN_INDEFINITE'
            )::integer as hidden_expense_count,
            count(*) filter (
              where state_rows.is_expense_component and state_rows.exact_snooze_id is not null
            )::integer as active_expense_snooze_count,
            count(*) filter (
              where state_rows.is_expense_component and state_rows.stale_snooze_id is not null
            )::integer as stale_expense_identity_count
          from state_rows
        ) as expense_components on true
        -- Protected presentation authority is built for every authoritative
        -- target. Public eligibility is applied only when final lines are
        -- emitted below, so zero-net and indefinitely snoozed targets remain
        -- attestable without becoming public economic rows.

  ;

  create temporary table timesheet_active_segment_snooze_meta on commit drop as
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

  ;

  create temporary table canonical_timesheet_segment_rows on commit drop as
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
            when coalesce(ss_match.is_do_not_pay, false) = true then 'BLOCKED_VISIBLE'
            when coalesce(ss_match.is_blocked, false) = true
             and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
            then 'BLOCKED_VISIBLE'
            when round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0
             and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
            then 'READY'
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
            'nhsp_shift_id', cur_seg_norm.nhsp_shift_id,
            'shift_id', cur_seg_norm.shift_id,
            'external_row_key', cur_seg_norm.external_row_key,
            'hr_request_id', cur_seg_norm.hr_request_id,
            'request_id', cur_seg_norm.request_id,
            'source_system', cur_seg_norm.source_system,
            'latest_import_id', cur_seg_norm.latest_import_id,
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
              when coalesce(ss_match.is_do_not_pay, false) = true then 'BLOCKED_VISIBLE'
              when coalesce(ss_match.is_blocked, false) = true
               and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
              then 'BLOCKED_VISIBLE'
              when round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0
               and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
              then 'READY'
              else 'IGNORED'
            end,
            'is_ready_segment', (
              ass_match.snooze_id is null
              and coalesce(ss_match.is_blocked, false) = false
              and coalesce(ss_match.is_do_not_pay, false) = false
              and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
              and round(coalesce(ttre_match.ready_preview_amount_ex_vat, ttre_match.preview_component_amount_ex_vat, ss_match.eff_delta_ex, 0), 2) <> 0
            ),
            'is_blocked_visible_segment', (
              (ass_match.snooze_id is not null and ass_match.snooze_until_date is not null)
              or (
                coalesce(ss_match.is_blocked, false) = true
                and not (coalesce(ttre_match.requires_resolution, false) = true and coalesce(ttre_match.is_actionable_resolution_row, false) = true)
              )
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
            nullif(btrim(coalesce(cur_seg.seg_json->>'ref_num','')), '') as ref_num,
            nullif(btrim(coalesce(cur_seg.seg_json->>'nhsp_shift_id','')), '') as nhsp_shift_id,
            coalesce(
              nullif(btrim(coalesce(cur_seg.seg_json->>'shift_id','')), ''),
              nullif(btrim(coalesce(cur_seg.seg_json->>'nhsp_shift_id','')), '')
            ) as shift_id,
            nullif(btrim(coalesce(cur_seg.seg_json->>'external_row_key','')), '') as external_row_key,
            nullif(btrim(coalesce(cur_seg.seg_json->>'hr_request_id','')), '') as hr_request_id,
            coalesce(
              nullif(btrim(coalesce(cur_seg.seg_json->>'request_id','')), ''),
              nullif(btrim(coalesce(cur_seg.seg_json->>'hr_request_id','')), '')
            ) as request_id,
            nullif(btrim(coalesce(cur_seg.seg_json->>'source_system','')), '') as source_system,
            nullif(btrim(coalesce(cur_seg.seg_json->>'latest_import_id','')), '') as latest_import_id
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
            round(sum(coalesce(ttre.ready_preview_amount_ex_vat, ttre.preview_component_amount_ex_vat, 0)), 2) as ready_preview_amount_ex_vat,
            bool_or(coalesce(ttre.requires_resolution, false)) as requires_resolution,
            bool_or(coalesce(ttre.is_actionable_resolution_row, false)) as is_actionable_resolution_row
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

  ;

  create temporary table canonical_timesheet_segment_rollup on commit drop as
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

  ;

  create temporary table canonical_timesheet_presentation_seed on commit drop as
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
          ctl.payee_bank_hash,
          ctl.payee_name_check_status,
          ctl.payee_name_check_has_override,
          ctl.payee_map_present,
          coalesce(ctl.payee_blockers, '[]'::jsonb) as payee_blockers,
          coalesce(ctl.has_payee_readiness_block, false) as has_payee_readiness_block,
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
          coalesce(ctl.ready_expense_amount_ex_vat, 0) as ready_expense_amount_ex_vat,
          coalesce(ctl.blocked_expense_amount_ex_vat, 0) as blocked_expense_amount_ex_vat,
          coalesce(ctl.hidden_expense_amount_ex_vat, 0) as hidden_expense_amount_ex_vat,
          coalesce(ctl.ready_expense_count, 0) as ready_expense_count,
          coalesce(ctl.blocked_expense_count, 0) as blocked_expense_count,
          coalesce(ctl.hidden_expense_count, 0) as hidden_expense_count,
          coalesce(ctl.active_expense_snooze_count, 0) as active_expense_snooze_count,
          coalesce(ctl.stale_expense_identity_count, 0) as stale_expense_identity_count,
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
          (
            coalesce(ctsr.total_segment_count, 0) > 0
            and (
              lower(btrim(coalesce(ctl.case_resolution_summary_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
              or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_applied', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
              or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_active', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
              or (
                coalesce(ctl.case_resolution_summary_json->>'resolved_rate_component_count', '') ~ '^[0-9]+$'
                and (ctl.case_resolution_summary_json->>'resolved_rate_component_count')::integer > 0
              )
            )
          ) as resolved_segment_rows_replace_source_total,
          case
            when coalesce(ctsr.total_segment_count, 0) > 0
             and (
               lower(btrim(coalesce(ctl.case_resolution_summary_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
               or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_applied', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
               or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_active', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
               or (
                 coalesce(ctl.case_resolution_summary_json->>'resolved_rate_component_count', '') ~ '^[0-9]+$'
                 and (ctl.case_resolution_summary_json->>'resolved_rate_component_count')::integer > 0
               )
             ) then round(coalesce(ctl.ready_expense_amount_ex_vat, 0), 2)
            else round(
              coalesce(ctl.amount_ex_vat, 0)
              - coalesce(ctsr.ready_segment_amount_ex_vat, 0)
              - coalesce(ctsr.blocked_visible_segment_amount_ex_vat, 0)
              - coalesce(ctsr.hidden_indefinite_segment_amount_ex_vat, 0)
              - coalesce(ctl.blocked_expense_amount_ex_vat, 0)
              - coalesce(ctl.hidden_expense_amount_ex_vat, 0),
              2
            )
          end as non_segment_amount_ex_vat,
          (ctl.snooze_id is not null) as has_active_timesheet_snooze,
          (coalesce(ctsr.active_segment_snooze_count, 0) > 0) as has_active_segment_snoozes
        from canonical_timesheet_lines ctl
        left join canonical_timesheet_segment_rollup ctsr
          on ctsr.candidate_id = ctl.candidate_id
         and ctsr.timesheet_id = ctl.timesheet_id
         and ctsr.booking_id is not distinct from ctl.booking_id

  ;

  create temporary table canonical_timesheet_presentation_state on commit drop as
        select
          ctps.*,
          coalesce(nullif(ctps.case_resolution_summary_json->>'case_needs_resolution','')::boolean, false) as case_needs_resolution,
          exists (
            select 1
            from jsonb_array_elements(coalesce(ctps.case_components_json, '[]'::jsonb)) as case_component(value)
            where coalesce(nullif(case_component.value->>'requires_resolution','')::boolean, false) = true
              and coalesce(nullif(case_component.value->>'is_actionable_resolution_row','')::boolean, false) = true
          ) as has_actionable_resolution_component,
          round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) as ready_section_amount_ex_vat,
          round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0) + coalesce(ctps.blocked_expense_amount_ex_vat, 0), 2) as blocked_section_amount_ex_vat,
          round(
            coalesce(
              nullif(ctps.case_resolution_summary_json->>'blocked_case_amount_ex_vat', '')::numeric,
              nullif(ctps.case_resolution_summary_json->>'unresolved_taxable_amount_ex_vat', '')::numeric,
              nullif(ctps.case_resolution_summary_json->>'safe_amount_ex_vat', '')::numeric,
              0::numeric
            ),
            2
          ) as case_resolution_section_amount_ex_vat,
          round(
            case
              when ctps.source_pay_method = 'UMBRELLA' then
                (public._pay_umbrella_vat_calc(
                  round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2),
                  v_vat_rate_pct,
                  ctps.umb_vat_chargeable
                )->>'inc')::numeric
              else round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2)
            end,
            2
          ) as ready_section_amount_display,
          round(
            case
              when ctps.source_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0) + coalesce(ctps.blocked_expense_amount_ex_vat, 0), 2), v_vat_rate_pct, ctps.umb_vat_chargeable)->>'inc')::numeric
              else round(coalesce(ctps.blocked_visible_segment_amount_ex_vat, 0) + coalesce(ctps.blocked_expense_amount_ex_vat, 0), 2)
            end,
            2
          ) as blocked_section_amount_display,
          round(
            case
              when ctps.source_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(
                round(
                  coalesce(
                    nullif(ctps.case_resolution_summary_json->>'blocked_case_amount_ex_vat', '')::numeric,
                    nullif(ctps.case_resolution_summary_json->>'unresolved_taxable_amount_ex_vat', '')::numeric,
                    nullif(ctps.case_resolution_summary_json->>'safe_amount_ex_vat', '')::numeric,
                    0::numeric
                  ),
                  2
                ),
                v_vat_rate_pct,
                ctps.umb_vat_chargeable
              )->>'inc')::numeric
              else round(
                coalesce(
                  nullif(ctps.case_resolution_summary_json->>'blocked_case_amount_ex_vat', '')::numeric,
                  nullif(ctps.case_resolution_summary_json->>'unresolved_taxable_amount_ex_vat', '')::numeric,
                  nullif(ctps.case_resolution_summary_json->>'safe_amount_ex_vat', '')::numeric,
                  0::numeric
                ),
                2
              )
            end,
            2
          ) as case_resolution_section_amount_display,
          (
            round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
            or coalesce(ctps.ready_segment_count, 0) > 0
          ) as has_ready_presentation,
          (
            ctps.has_active_timesheet_snooze = false
            and ctps.is_ready_for_draft = false
            and coalesce(nullif(ctps.case_resolution_summary_json->>'case_needs_resolution','')::boolean, false) = false
            and (
              ctps.case_is_blocked = false
              or coalesce(ctps.has_payee_readiness_block, false) = true
            )
            and (
              round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
              or coalesce(ctps.ready_segment_count, 0) > 0
            )
          ) as has_non_resolution_readiness_block,
          (
            ctps.has_active_timesheet_snooze = true
            or coalesce(ctps.blocked_visible_segment_count, 0) > 0
            or coalesce(ctps.blocked_expense_count, 0) > 0
            or (
              ctps.has_active_timesheet_snooze = false
              and ctps.is_ready_for_draft = false
              and coalesce(nullif(ctps.case_resolution_summary_json->>'case_needs_resolution','')::boolean, false) = false
              and (
                ctps.case_is_blocked = false
                or coalesce(ctps.has_payee_readiness_block, false) = true
              )
              and (
                round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
                or coalesce(ctps.ready_segment_count, 0) > 0
              )
            )
          ) as has_blocked_presentation,
          (
            ctps.has_active_timesheet_snooze = false
            and ctps.case_is_blocked = true
            and coalesce(nullif(ctps.case_resolution_summary_json->>'case_needs_resolution','')::boolean, false) = true
            and (
              round(
                coalesce(
                  nullif(ctps.case_resolution_summary_json->>'blocked_case_amount_ex_vat', '')::numeric,
                  nullif(ctps.case_resolution_summary_json->>'unresolved_taxable_amount_ex_vat', '')::numeric,
                  nullif(ctps.case_resolution_summary_json->>'safe_amount_ex_vat', '')::numeric,
                  0::numeric
                ),
                2
              ) <> 0
              or exists (
                select 1
                from jsonb_array_elements(coalesce(ctps.case_components_json, '[]'::jsonb)) as actionable_component(value)
                where coalesce(nullif(actionable_component.value->>'requires_resolution','')::boolean, false) = true
                  and coalesce(nullif(actionable_component.value->>'is_actionable_resolution_row','')::boolean, false) = true
              )
            )
            and coalesce(ctps.blocked_visible_segment_count, 0) = 0
            and not (
              ctps.has_active_timesheet_snooze = false
              and ctps.is_ready_for_draft = false
              and coalesce(nullif(ctps.case_resolution_summary_json->>'case_needs_resolution','')::boolean, false) = false
              and (
                ctps.case_is_blocked = false
                or coalesce(ctps.has_payee_readiness_block, false) = true
              )
              and (
                round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
                or coalesce(ctps.ready_segment_count, 0) > 0
              )
            )
          ) as has_case_resolution_presentation,
          (
            ctps.has_active_timesheet_snooze = false
            and ctps.case_is_blocked = false
            and (
              round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
              or coalesce(ctps.ready_segment_count, 0) > 0
            )
            and (coalesce(ctps.blocked_visible_segment_count, 0) > 0 or coalesce(ctps.blocked_expense_count, 0) > 0)
          ) as is_partially_ready,
          (
            ctps.has_active_timesheet_snooze = false
            and ctps.case_is_blocked = false
            and (
              round(coalesce(ctps.ready_segment_amount_ex_vat, 0) + coalesce(ctps.non_segment_amount_ex_vat, 0), 2) <> 0
              or coalesce(ctps.ready_segment_count, 0) > 0
            )
            and (coalesce(ctps.blocked_visible_segment_count, 0) > 0 or coalesce(ctps.blocked_expense_count, 0) > 0)
          ) as is_partially_blocked
        from canonical_timesheet_presentation_seed ctps

  ;

  create temporary table canonical_timesheet_presentation_rows on commit drop as
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
              'section_non_segment_amount_ex_vat', ctpp.non_segment_amount_ex_vat,
              'resolved_segment_rows_replace_source_total', coalesce(ctpp.resolved_segment_rows_replace_source_total, false)
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
                when ctpp.is_partially_ready and ctpp.blocked_visible_segment_count > 0 and ctpp.blocked_expense_count > 0 then 'Some segments and expenses are blocked'
                when ctpp.is_partially_ready and ctpp.blocked_expense_count > 0 then 'Some expenses are blocked'
                when ctpp.is_partially_ready then 'Some segments are blocked'
                when ctpp.hidden_indefinite_segment_count > 0 and ctpp.hidden_expense_count > 0 then 'Some segments and expenses are snoozed indefinitely'
                when ctpp.hidden_expense_count > 0 then 'Some expenses are snoozed indefinitely'
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
          and round(coalesce(ctpp.amount_ex_vat,0),2) <> 0
        union all

        select
          ctpp.candidate_id,
          (
            jsonb_build_object(
              'line_id', (ctpp.timesheet_id::text || ':03:case'),
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
              'amount_ex_vat', ctpp.case_resolution_section_amount_ex_vat,
              'amount_display', ctpp.case_resolution_section_amount_display,
              'is_advanced', (ctpp.override_id is not null),
              'advanced_override_id', case when ctpp.override_id is null then null else ctpp.override_id::text end,
              'advanced_reason', ctpp.override_reason,
              'blocked_reason_codes', case
                when jsonb_typeof(ctpp.case_resolution_summary_json->'blocked_reason_codes') = 'array'
                then coalesce(ctpp.case_resolution_summary_json->'blocked_reason_codes', '[]'::jsonb)
                else '[]'::jsonb
              end,
              'is_excluded_from_allocation', false,
              'is_ready_for_draft', false,
              'segment_rows', '[]'::jsonb,
              'segment_count', 0
            )
            || jsonb_build_object(
              'presentation_section', 'CASES_RESOLUTIONS',
              'presentation_role', 'PARENT',
              'presentation_line_id', (ctpp.timesheet_id::text || ':03:case'),
              'presentation_parent_line_id', ctpp.timesheet_id::text,
              'real_business_timesheet_id', ctpp.timesheet_id::text,
              'total_segment_count', ctpp.total_segment_count,
              'ready_segment_count', ctpp.ready_segment_count,
              'blocked_visible_segment_count', ctpp.blocked_visible_segment_count,
              'hidden_indefinite_segment_count', ctpp.hidden_indefinite_segment_count,
              'is_partially_ready', false,
              'is_partially_blocked', false,
              'section_amount_ex_vat', ctpp.case_resolution_section_amount_ex_vat,
              'section_amount_display', ctpp.case_resolution_section_amount_display,
              'section_segment_rows', '[]'::jsonb,
              'section_segment_count', 0,
              'section_non_segment_amount_ex_vat', ctpp.case_resolution_section_amount_ex_vat
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
              'presentation_reason', 'CASE_RESOLUTION_REQUIRED',
              'presentation_advisory_text', 'Resolve this case before draft'
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
          ctpp.case_resolution_section_amount_ex_vat as amount_ex_vat,
          false as is_excluded_from_allocation
        from canonical_timesheet_presentation_state ctpp
        where ctpp.has_case_resolution_presentation = true


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
              'payee_entity_kind', ctpp.payee_entity_kind,
              'payee_entity_id', case when ctpp.payee_entity_id is null then null else ctpp.payee_entity_id::text end,
              'payee_bank_hash', ctpp.payee_bank_hash,
              'bank_details_hash', ctpp.payee_bank_hash,
              'name_check_status', ctpp.payee_name_check_status,
              'name_check_has_override', ctpp.payee_name_check_has_override,
              'payee_map_present', ctpp.payee_map_present,
              'blockers', coalesce(ctpp.payee_blockers, '[]'::jsonb),
              'payee_blockers', coalesce(ctpp.payee_blockers, '[]'::jsonb)
            )
            || jsonb_build_object(
              'amount_ex_vat', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.amount_ex_vat
                else ctpp.blocked_section_amount_ex_vat
              end,
              'amount_display', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.amount_display
                else ctpp.blocked_section_amount_display
              end,
              'is_advanced', (ctpp.override_id is not null),
              'advanced_override_id', case when ctpp.override_id is null then null else ctpp.override_id::text end,
              'advanced_reason', ctpp.override_reason,
              'blocked_reason_codes', (
                coalesce(ctpp.payee_blockers, '[]'::jsonb)
                ||
                (case
                  when ctpp.has_active_timesheet_snooze = true then jsonb_build_array('BLOCKED_DATED_SNOOZE')
                  else '[]'::jsonb
                end)
              ),
              'is_excluded_from_allocation', (ctpp.has_active_timesheet_snooze = true),
              'is_ready_for_draft', case
                when ctpp.has_active_timesheet_snooze = true then ctpp.is_ready_for_draft
                else false
              end,
              'segment_rows', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.visible_segment_rows_json
                else ctpp.blocked_visible_segment_rows_json
              end,
              'segment_count', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then jsonb_array_length(coalesce(ctpp.visible_segment_rows_json, '[]'::jsonb))
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
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.amount_ex_vat
                else ctpp.blocked_section_amount_ex_vat
              end,
              'section_amount_display', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.amount_display
                else ctpp.blocked_section_amount_display
              end,
              'section_segment_rows', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.visible_segment_rows_json
                else ctpp.blocked_visible_segment_rows_json
              end,
              'section_segment_count', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then jsonb_array_length(coalesce(ctpp.visible_segment_rows_json, '[]'::jsonb))
                else jsonb_array_length(coalesce(ctpp.blocked_visible_segment_rows_json, '[]'::jsonb))
              end,
              'section_non_segment_amount_ex_vat', case
                when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.non_segment_amount_ex_vat
                else coalesce(ctpp.blocked_expense_amount_ex_vat, 0)
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
                when ctpp.is_partially_blocked then 'PARTIAL_BLOCKED_FOR_PAY'
                else 'BLOCKED_FOR_PAY'
              end,
              'presentation_advisory_text', case
                when ctpp.is_partially_blocked and ctpp.ready_segment_count > 0 and ctpp.ready_expense_count > 0 then 'Some segments and expenses are ready to pay'
                when ctpp.is_partially_blocked and ctpp.ready_expense_count > 0 then 'Some expenses are ready to pay'
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
            when ctpp.has_active_timesheet_snooze = true or ctpp.has_non_resolution_readiness_block = true then ctpp.amount_ex_vat
            else ctpp.blocked_section_amount_ex_vat
          end as amount_ex_vat,
          (ctpp.has_active_timesheet_snooze = true) as is_excluded_from_allocation
        from canonical_timesheet_presentation_state ctpp
        where ctpp.has_blocked_presentation = true
          and round(coalesce(ctpp.amount_ex_vat,0),2) <> 0
          and not (ctpp.snooze_id is not null and ctpp.snooze_until_date is null)
          and (
            ctpp.has_active_timesheet_snooze = true
            or ctpp.has_non_resolution_readiness_block = true
            or ctpp.blocked_visible_segment_count > 0
            or ctpp.blocked_expense_count > 0
          )


  ;

  -- An aggregate ordinary timesheet parent is presentation evidence, not an
  -- independently payable economic component.  Negative parents without an
  -- actionable case were previously left under READY_TO_PAY and only made
  -- non-selectable later by the strict line contract.  That was structurally
  -- safe but visually false: a negative payment cannot be paid.  Move the
  -- single parent row to Blocked while retaining its exact amount and lineage.
  UPDATE canonical_timesheet_presentation_rows AS presentation_row
  SET
    line_json = presentation_row.line_json
      || pg_catalog.jsonb_build_object(
        'presentation_section', 'BLOCKED_FOR_PAY',
        'readiness_state', 'BLOCKED_FOR_PAY',
        'presentation_reason', 'NEGATIVE_ORDINARY_PRESENTATION_ONLY',
        'presentation_advisory_text', 'Negative ordinary entitlement cannot be paid directly',
        'blocked_reason_codes', COALESCE(presentation_row.line_json->'blocked_reason_codes', '[]'::jsonb)
          || pg_catalog.jsonb_build_array('NEGATIVE_ORDINARY_PRESENTATION_ONLY'),
        'draftable', false,
        'is_ready_for_draft', false,
        'selection_allowed', false,
        'is_excluded_from_allocation', true
      ),
    is_excluded_from_allocation = true
  WHERE pg_catalog.upper(COALESCE(presentation_row.line_json->>'line_type', '')) = 'TIMESHEET_PAYMENT'
    AND pg_catalog.upper(COALESCE(presentation_row.line_json->>'presentation_role', '')) = 'PARENT'
    AND pg_catalog.upper(COALESCE(presentation_row.line_json->>'presentation_section', '')) = 'READY_TO_PAY'
    AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
      presentation_row.line_json->'case_resolution_summary'->>'case_needs_resolution',
      'false'
    ))) NOT IN ('true','t','1','yes','y','on')
    AND pg_catalog.round(COALESCE(presentation_row.amount_ex_vat, 0), 2) < 0;

  create temporary table finance_case_lines on commit drop as
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
          round(coalesce(fcrr.due_amount_ex_vat, 0), 2) as due_amount_ex_vat,
          coalesce(finance_due_meta.nominal_due_amount_ex_vat, 0) as nominal_due_amount_ex_vat,
          round(coalesce(finance_case_baseline.outstanding_amount, 0), 2)
            as recovery_source_outstanding_ex_vat,
          round(coalesce(finance_case_baseline.active_reserved_amount, 0), 2)
            as recovery_active_reserved_ex_vat,
          finance_due_meta.recovery_created_at_utc as semantic_recovery_sort_at_utc,
          fcrr.is_blocked as case_is_blocked,
          (
            coalesce(fcrr.blocked_reason_codes, '[]'::jsonb)
            ||
            (case
              when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is null then jsonb_build_array('BLOCKED_INDEFINITE_SNOOZE')
              when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null then jsonb_build_array('BLOCKED_DATED_SNOOZE')
              else '[]'::jsonb
            end)
            ||
            (case
              when round(coalesce(fcrr.due_amount_ex_vat, 0), 2) = 0
               and coalesce(finance_due_meta.nominal_due_amount_ex_vat, 0) > 0
               and fcrr.case_type in (
                 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
                 'OVERPAYMENT'::public.pay_finance_case_type_enum,
                 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
               ) then jsonb_build_array('NO_PAY_HEADROOM')
              else '[]'::jsonb
            end)
          ) as blocked_reason_codes,
          fcrr.case_resolution_summary_json,
          upper(btrim(coalesce(fcrr.case_resolution_summary_json->>'finance_resolution_clearability_state', 'NOT_REQUIRED'))) as finance_resolution_clearability_state,
          upper(btrim(coalesce(fcrr.case_resolution_summary_json->>'current_saved_resolution_family', ''))) as current_saved_resolution_family,
          upper(btrim(coalesce(fcrr.case_resolution_summary_json->>'current_saved_resolution_owner_kind', 'NONE'))) as current_saved_resolution_owner_kind,
          nullif(btrim(coalesce(fcrr.case_resolution_summary_json->>'finance_resolution_clear_block_reason', '')), '') as finance_resolution_clear_block_reason,
          case
            when coalesce(fcrr.case_resolution_summary_json->>'current_saved_resolution_linked_timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              then (fcrr.case_resolution_summary_json->>'current_saved_resolution_linked_timesheet_id')::uuid
            else null::uuid
          end as current_saved_resolution_linked_timesheet_id,
          fcrr.taxable_manual_debt_resolution_json,
          fcrr.case_components_json,
          fcrr.oneoff_bank_details_present,
          fcrr.is_candidate_directed_oneoff_payout,
          fcrr.appears_on_umbrella_remittance,
          fcrr.generates_candidate_payment_advice,
          fcrr.snooze_allowed,
          fcrr.lifecycle_status_display,
          finance_case_baseline.payout_status as source_payout_status,
          component_identity.component_count,
          component_identity.finance_component_id,
          component_identity.component_key_type,
          component_identity.component_key_value,
          component_identity.component_classification,
          component_identity.component_source_basis_json,
          case
            when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID' then 'LOAN_PAYOUT'
            when fcrr.case_type = 'PAYMENT_ADVANCE' then 'PAYMENT_ADVANCE_REPAYMENT'
            when fcrr.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
            when fcrr.case_type = 'UNDERPAYMENT' then 'UNDERPAYMENT_PAYMENT'
            when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
            when fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
            else fcrr.case_type::text
          end as line_type,
          case
            when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID' then 'Loan payment'
            when fcrr.case_type = 'PAYMENT_ADVANCE' then 'Loan repayment'
            when fcrr.case_type = 'OVERPAYMENT' then 'Overpayment recovery'
            when fcrr.case_type = 'UNDERPAYMENT' then 'Underpayment payment'
            when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'Manual credit adjustment payment'
            when fcrr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'Manual debt adjustment deduction'
            else replace(fcrr.case_type::text, '_', ' ')
          end as item_type_label,
          case
            when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID' then 'PAYMENT'
            when fcrr.case_type = 'UNDERPAYMENT' then 'PAYMENT'
            when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'PAYMENT'
            else 'DEDUCTION'
          end as item_direction,
          case
            when fcrr.candidate_pay_method = 'PAYE'
             and fcrr.case_type = 'PAYMENT_ADVANCE'
             and upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID'
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
             and fcrr.case_type = 'UNDERPAYMENT'
             and fcrr.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
            then 'GROSS_ADD'
            when fcrr.candidate_pay_method = 'PAYE'
             and fcrr.case_type = 'UNDERPAYMENT'
             and fcrr.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
            then 'NET_ADD'
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
            when fcrr.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID' then round(coalesce(fcrr.due_amount_ex_vat,0),2)
            when fcrr.case_type = 'UNDERPAYMENT' then round(coalesce(fcrr.due_amount_ex_vat,0),2)
            when fcrr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then round(coalesce(fcrr.due_amount_ex_vat,0),2)
            else round(-coalesce(fcrr.due_amount_ex_vat,0),2)
          end as signed_amount_ex_vat,
          case
            when fcrr.active_snooze_id is not null then 'BLOCKED_FOR_PAY'
            when lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on') then 'CASES_RESOLUTIONS'
            when fcrr.is_blocked then 'BLOCKED_FOR_PAY'
            when round(coalesce(fcrr.due_amount_ex_vat,0),2) = 0 then 'BLOCKED_FOR_PAY'
            else 'READY_TO_PAY'
          end as readiness_state,
          case
            when fcrr.active_snooze_id is not null then 'BLOCKED_FOR_PAY'
            when lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on') then 'CASES_RESOLUTIONS'
            when fcrr.is_blocked then 'BLOCKED_FOR_PAY'
            when round(coalesce(fcrr.due_amount_ex_vat,0),2) = 0 then 'BLOCKED_FOR_PAY'
            else 'READY_TO_PAY'
          end as presentation_section,
          case
            when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is null then 'INDEFINITE_SNOOZE'
            when fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null then 'DATED_SNOOZE'
            when lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on') then 'CASE_RESOLUTION_REQUIRED'
            when fcrr.is_blocked then 'CASE_BLOCKED'
            when round(coalesce(fcrr.due_amount_ex_vat,0),2) = 0 and coalesce(finance_due_meta.nominal_due_amount_ex_vat, 0) > 0 then 'NO_PAY_HEADROOM'
            else 'READY_TO_PAY'
          end as presentation_reason,
          lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_resolution_satisfied_now', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on') as is_case_resolution_satisfied,
          lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on') as case_needs_resolution_now,
          (
            fcrr.is_blocked = false
            and fcrr.active_snooze_id is null
            and lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) not in ('true', 't', '1', 'yes', 'y', 'on')
            and round(coalesce(fcrr.due_amount_ex_vat,0),2) > 0
          ) as draftable
        from finance_case_resolution_rollup fcrr
        left join finance_case_baseline_scope finance_case_baseline
          on finance_case_baseline.finance_case_id = fcrr.finance_case_id
         and finance_case_baseline.candidate_id = fcrr.candidate_id
        left join lateral (
          select
            round(coalesce(max(fcrrb.nominal_due_amount), 0), 2) as nominal_due_amount_ex_vat,
            min(fcrrb.created_at) as recovery_created_at_utc
          from finance_case_recovery_rows_base fcrrb
          where fcrrb.finance_case_id = fcrr.finance_case_id
        ) finance_due_meta on true
        left join lateral (
          with component_rows as (
            select
              component_element.value as component_json,
              component_element.ordinality
            from jsonb_array_elements(
              case
                when jsonb_typeof(coalesce(fcrr.case_components_json, '[]'::jsonb)) = 'array' then coalesce(fcrr.case_components_json, '[]'::jsonb)
                else '[]'::jsonb
              end
            ) with ordinality as component_element(value, ordinality)
            where component_element.value is not null
              and jsonb_typeof(component_element.value) = 'object'
          )
          select
            count(*)::integer as component_count,
            case
              when count(*) = 1
               and (array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'finance_component_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              then ((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'finance_component_id')::uuid
              else null::uuid
            end as finance_component_id,
            coalesce(
              case when count(*) = 1 then upper(nullif(btrim(coalesce((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'component_key_type', '')), '')) else null::text end,
              case when fcrr.linked_timesheet_id is not null then 'TS_TOTAL' else 'ADJUSTMENT_CODE' end
            ) as component_key_type,
            coalesce(
              case when count(*) = 1 then nullif(btrim(coalesce((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'component_key_value', '')), '') else null::text end,
              case when fcrr.linked_timesheet_id is not null then 'TOTAL' else fcrr.finance_case_id::text end
            ) as component_key_value,
            case
              when count(*) = 1 and upper(nullif(btrim(coalesce((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'classification', '')), '')) = 'TAXABLE_CHANNEL_SENSITIVE'
                then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              when count(*) = 1 and upper(nullif(btrim(coalesce((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
                then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
              when count(*) = 1 and upper(nullif(btrim(coalesce((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->>'classification', '')), '')) = 'NET_PAY_FIXED_RECOVERY'
                then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
              else null::public.pay_finance_component_classification_enum
            end as component_classification,
            case
              when count(*) = 1 and jsonb_typeof((array_agg(component_rows.component_json order by component_rows.ordinality))[1]->'source_basis_json') = 'object'
                then (array_agg(component_rows.component_json order by component_rows.ordinality))[1]->'source_basis_json'
              else '{}'::jsonb
            end as component_source_basis_json
          from component_rows
        ) component_identity on true
        where (
          round(coalesce(fcrr.due_amount_ex_vat,0),2) > 0
          or fcrr.active_snooze_id is not null
          or coalesce(fcrr.is_blocked, false) = true
          or lower(btrim(coalesce(fcrr.case_resolution_summary_json->>'case_needs_resolution', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
          or (
            round(coalesce(fcrr.due_amount_ex_vat,0),2) = 0
            and coalesce(finance_due_meta.nominal_due_amount_ex_vat, 0) > 0
            and fcrr.case_type in (
              'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
              'OVERPAYMENT'::public.pay_finance_case_type_enum,
              'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
            )
          )
        )


  ;

  create temporary table hidden_recovery_template_lines on commit drop as
        select
          fcrrb.candidate_id,
          fcrrb.finance_case_id,
          case
            when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
            when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
            when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
            else fcrrb.case_type::text
          end as recovery_family,
          ('advance:' || fcrrb.finance_case_id::text) as source_ref,
          row_number() over (
            partition by fcrrb.candidate_id,
                         case
                           when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
                           when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
                           when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                           else fcrrb.case_type::text
                         end
            order by coalesce(fcbs.created_at, fcrrb.created_at), fcrrb.finance_case_id
          )::integer as sort_order,
          jsonb_strip_nulls(
            jsonb_build_object(
              'template_state', 'HIDDEN_ZERO_TAKE',
              'candidate_id', fcrrb.candidate_id::text,
              'finance_case_id', fcrrb.finance_case_id::text,
              'recovery_family', case
                when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
                when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
                when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                else fcrrb.case_type::text
              end,
              'case_type', fcrrb.case_type::text,
              'source_ref', ('advance:' || fcrrb.finance_case_id::text),
              'pay_channel', upper(coalesce(fcrrb.candidate_pay_method, '')),
              'umbrella_id', case when c.umbrella_id is null then null else c.umbrella_id::text end,
              'paye_treatment', case
                when upper(coalesce(fcrrb.candidate_pay_method, '')) = 'PAYE'
                 and fcrrb.case_type = 'PAYMENT_ADVANCE'
                then 'NET_DEDUCT'
                when upper(coalesce(fcrrb.candidate_pay_method, '')) = 'PAYE'
                 and fcrrb.case_type = 'OVERPAYMENT'
                 and fcbs.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
                then 'GROSS_DEDUCT'
                when upper(coalesce(fcrrb.candidate_pay_method, '')) = 'PAYE'
                 and fcrrb.case_type = 'OVERPAYMENT'
                 and fcbs.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
                then 'NET_DEDUCT'
                when upper(coalesce(fcrrb.candidate_pay_method, '')) = 'PAYE'
                 and fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT'
                 and fcbs.taxability = 'TAXABLE'::public.pay_finance_taxability_enum
                then 'GROSS_DEDUCT'
                when upper(coalesce(fcrrb.candidate_pay_method, '')) = 'PAYE'
                 and fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT'
                 and fcbs.taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
                then 'NET_DEDUCT'
                else 'NONE'
              end,
              'finance_component_id', null,
              'frozen_component_key_type', 'CASE_TOTAL',
              'frozen_component_key_value', 'TOTAL',
              'frozen_component_classification', null,
              'frozen_component_snapshot_json', null,
              'frozen_source_basis_json', jsonb_strip_nulls(
                jsonb_build_object(
                  'case_type', fcrrb.case_type::text,
                  'recovery_family', case
                    when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
                    when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
                    when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                    else fcrrb.case_type::text
                  end,
                  'pay_channel', upper(coalesce(fcrrb.candidate_pay_method, '')),
                  'frozen_source_pay_method', upper(coalesce(fcrrb.candidate_pay_method, '')),
                  'frozen_target_pay_method', upper(coalesce(fcrrb.candidate_pay_method, '')),
                  'taxability', case when fcbs.taxability is null then null else fcbs.taxability::text end,
                  'routing_kind', case when fcbs.routing_kind is null then null else fcbs.routing_kind::text end,
                  'nominal_due_amount_ex_vat', round(coalesce(fcrrb.nominal_due_amount, 0), 2),
                  'current_due_amount_ex_vat', round(coalesce(fcrr.due_amount_ex_vat, 0), 2),
                  'outstanding_amount', round(coalesce(fcbs.outstanding_amount, 0), 2),
                  'weekly_due', round(coalesce(fcbs.weekly_due, 0), 2),
                  'active_reserved_amount', round(coalesce(fcbs.active_reserved_amount, 0), 2),
                  'minimum_earnings_threshold', fcrrb.minimum_earnings_threshold,
                  'take_home_floor_override', fcrrb.take_home_floor_override,
                  'default_take_home_floor', fcrrb.default_take_home_floor,
                  'run_earnings_headroom_ex', round(coalesce(fcrrb.run_earnings_headroom_ex, 0), 2),
                  'run_take_home_before', round(coalesce(fcrrb.run_take_home_before, 0), 2),
                  'payout_status', case when fcrrb.payout_status is null then null else fcrrb.payout_status::text end,
                  'next_due_week_start', case when fcbs.next_due_week_start is null then null else fcbs.next_due_week_start::text end,
                  'sort_order', row_number() over (
                    partition by fcrrb.candidate_id,
                                 case
                                   when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
                                   when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
                                   when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                                   else fcrrb.case_type::text
                                 end
                    order by coalesce(fcbs.created_at, fcrrb.created_at), fcrrb.finance_case_id
                  )
                )
              ),
              'frozen_source_pay_method', upper(coalesce(fcrrb.candidate_pay_method, '')),
              'frozen_target_pay_method', upper(coalesce(fcrrb.candidate_pay_method, '')),
              'frozen_source_amount', round(coalesce(fcbs.outstanding_amount, fcrrb.nominal_due_amount, 0), 2),
              'frozen_outstanding_amount', round(coalesce(fcbs.outstanding_amount, 0), 2),
              'weekly_due', round(coalesce(fcbs.weekly_due, 0), 2),
              'minimum_earnings_threshold', fcrrb.minimum_earnings_threshold,
              'take_home_floor_override', fcrrb.take_home_floor_override,
              'default_take_home_floor', fcrrb.default_take_home_floor,
              'payout_status', case when fcrrb.payout_status is null then null else fcrrb.payout_status::text end,
              'next_due_week_start', case when fcbs.next_due_week_start is null then null else fcbs.next_due_week_start::text end,
              'sort_order', row_number() over (
                partition by fcrrb.candidate_id,
                             case
                               when fcrrb.case_type = 'PAYMENT_ADVANCE' then 'LOAN_REPAYMENT'
                               when fcrrb.case_type = 'OVERPAYMENT' then 'OVERPAYMENT_RECOVERY'
                               when fcrrb.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                               else fcrrb.case_type::text
                             end
                order by coalesce(fcbs.created_at, fcrrb.created_at), fcrrb.finance_case_id
              ),
              'frozen_resolution_mode', coalesce(
                nullif(btrim(coalesce(fcrr.taxable_manual_debt_resolution_json->>'resolution_mode', '')), ''),
                nullif(btrim(coalesce(fcrr.taxable_manual_debt_resolution_json->>'mode', '')), ''),
                nullif(btrim(coalesce(fcrr.case_resolution_summary_json->'non_bucket_resolution'->>'resolution_mode', '')), ''),
                nullif(btrim(coalesce(fcrr.case_resolution_summary_json->'non_bucket_resolution'->>'mode', '')), '')
              ),
              'frozen_resolution_payload_json', case
                when fcrr.taxable_manual_debt_resolution_json is not null then fcrr.taxable_manual_debt_resolution_json
                when jsonb_typeof(fcrr.case_resolution_summary_json->'non_bucket_resolution') = 'object' then fcrr.case_resolution_summary_json->'non_bucket_resolution'
                else null
              end,
              'frozen_resolution_result_json', fcrr.case_resolution_summary_json,
              'case_components_json', coalesce(fcrr.case_components_json, '[]'::jsonb),
              'case_resolution_summary_json', coalesce(fcrr.case_resolution_summary_json, '{}'::jsonb),
              'taxable_manual_debt_resolution_json', fcrr.taxable_manual_debt_resolution_json,
              'eligibility_state', jsonb_build_object(
                'candidate_ready_for_draft', coalesce(fcrr.candidate_ready_for_draft, false),
                'case_is_blocked', coalesce(fcrr.is_blocked, false),
                'has_active_dated_snooze', (fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null)
              )
            )
          ) as template_json
        from finance_case_recovery_rows_base fcrrb
        join finance_case_baseline_scope fcbs
          on fcbs.finance_case_id = fcrrb.finance_case_id
        join finance_case_resolution_rollup fcrr
          on fcrr.finance_case_id = fcrrb.finance_case_id
        left join public.candidates c
          on c.id = fcrrb.candidate_id
        where fcrrb.case_type in (
            'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
            'OVERPAYMENT'::public.pay_finance_case_type_enum,
            'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
          )
          and round(coalesce(fcrrb.nominal_due_amount, 0), 2) > 0
          and round(coalesce(fcrr.due_amount_ex_vat, 0), 2) = 0
          and coalesce(fcrr.candidate_ready_for_draft, false) = true
          and coalesce(fcrr.is_blocked, false) = false
          and not (fcrr.active_snooze_id is not null and fcrr.active_snooze_until_date is not null)
          and (
            fcrrb.case_type <> 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
            or upper(coalesce(fcrrb.payout_status::text, '')) = 'PAID'
          )
  ;



  CREATE TEMPORARY TABLE IF NOT EXISTS manual_adjustment_carry_forward_scope (
    id uuid,
    source_pay_batch_id uuid,
    source_pay_batch_item_id uuid,
    source_pay_bank_transfer_id uuid,
    source_pay_batch_candidate_id uuid,
    source_correction_request_id uuid,
    source_correction_work_item_id uuid,
    candidate_id uuid,
    umbrella_id uuid,
    client_id uuid,
    timesheet_id uuid,
    pay_channel text,
    adjustment_direction text,
    amount_ex_vat numeric,
    amount_vat numeric,
    amount_inc_vat numeric,
    amount_basis text,
    paye_treatment text,
    tax_treatment_json jsonb,
    description text,
    reason text,
    source_ref text,
    source_operation_source_key text,
    source_snapshot_json jsonb,
    status text,
    target_pay_batch_id uuid,
    target_pay_batch_item_id uuid,
    target_operation_source_key text,
    created_at_utc timestamptz,
    updated_at_utc timestamptz,
    reserved_at_utc timestamptz,
    consumed_at_utc timestamptz,
    released_at_utc timestamptz,
    cancelled_at_utc timestamptz,
    status_reason text,
    source_batch_ref text
  ) ON COMMIT DROP;

  create temporary table manual_adjustment_carry_forward_lines on commit drop as
        select
          carry_forward_scope.candidate_id,
          cp.cand_tms_ref,
          cp.cand_display_name,
          cp.payee_entity_kind,
          cp.payee_entity_id,
          carry_forward_scope.id as manual_adjustment_carry_forward_id,
          carry_forward_scope.source_pay_batch_id,
          carry_forward_scope.source_pay_batch_item_id,
          carry_forward_scope.source_pay_bank_transfer_id,
          carry_forward_scope.source_pay_batch_candidate_id,
          carry_forward_scope.source_correction_request_id,
          carry_forward_scope.source_correction_work_item_id,
          carry_forward_scope.umbrella_id,
          carry_forward_scope.client_id,
          carry_forward_scope.timesheet_id,
          upper(btrim(coalesce(carry_forward_scope.pay_channel, ''))) as pay_channel,
          carry_forward_scope.adjustment_direction,
          round(coalesce(carry_forward_scope.amount_ex_vat, 0), 2)::numeric as amount_ex_vat,
          round(coalesce(carry_forward_scope.amount_vat, 0), 2)::numeric as amount_vat,
          round(coalesce(carry_forward_scope.amount_inc_vat, 0), 2)::numeric as amount_inc_vat,
          carry_forward_scope.amount_basis,
          carry_forward_scope.paye_treatment,
          coalesce(carry_forward_scope.tax_treatment_json, '{}'::jsonb) as tax_treatment_json,
          nullif(btrim(coalesce(carry_forward_scope.description, '')), '') as original_description,
          carry_forward_scope.reason,
          carry_forward_scope.source_ref,
          carry_forward_scope.source_operation_source_key,
          coalesce(carry_forward_scope.source_snapshot_json, '{}'::jsonb) as source_snapshot_json,
          carry_forward_scope.status,
          carry_forward_scope.target_pay_batch_id,
          carry_forward_scope.target_pay_batch_item_id,
          carry_forward_scope.target_operation_source_key,
          coalesce(nullif(btrim(carry_forward_scope.source_batch_ref), ''), carry_forward_scope.source_pay_batch_id::text) as source_batch_ref,
          ('carry_forward:' || carry_forward_scope.id::text) as preview_row_id,
          ('carry_forward:' || carry_forward_scope.id::text) as operation_source_key,
          ('Carried forward from cancelled payment '
            || coalesce(nullif(btrim(carry_forward_scope.source_batch_ref), ''), carry_forward_scope.source_pay_batch_id::text)
            || ': '
            || coalesce(nullif(btrim(carry_forward_scope.description), ''), nullif(btrim(carry_forward_scope.reason), ''), 'Manual adjustment')
          ) as display_description
        from manual_adjustment_carry_forward_scope as carry_forward_scope
        join cand_payee cp
          on cp.candidate_id = carry_forward_scope.candidate_id
        where carry_forward_scope.candidate_id = p_candidate_id
          and upper(btrim(coalesce(carry_forward_scope.status, ''))) in ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT')
          and round(coalesce(carry_forward_scope.amount_inc_vat, 0), 2) <> 0
  ;

  -- Aggregate timesheet rows are presentation parents only.  Exact economic
  -- allocation authority already exists in case_components_json; promote each
  -- positive, resolved component as its own top-level Ready-to-Pay row instead
  -- of making the aggregate parent selectable.  This preserves Policy X's
  -- existing TS_DAY / expense / correction identities and prevents a recovery
  -- row from becoming the only selectable row for a candidate.
  create temporary table timesheet_allocation_component_lines on commit drop as
        with component_rows as (
          select
            ctl.*,
            component.value as component_json,
            upper(nullif(btrim(coalesce(component.value->>'component_key_type','')), '')) as component_key_type,
            nullif(btrim(coalesce(component.value->>'component_key_value','')), '') as component_key_value,
            round(case
              when exists (
                select 1
                from canonical_timesheet_segment_rows resolved_segment
                where resolved_segment.candidate_id=ctl.candidate_id
                  and resolved_segment.timesheet_id=ctl.timesheet_id
              )
              and (
                lower(btrim(coalesce(ctl.case_resolution_summary_json->>'has_resolved_rate','false'))) in ('true','t','1','yes','y','on')
                or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_applied','false'))) in ('true','t','1','yes','y','on')
                or lower(btrim(coalesce(ctl.case_resolution_summary_json->>'resolved_rate_active','false'))) in ('true','t','1','yes','y','on')
                or (
                  coalesce(ctl.case_resolution_summary_json->>'resolved_rate_component_count','') ~ '^[0-9]+$'
                  and (ctl.case_resolution_summary_json->>'resolved_rate_component_count')::integer > 0
                )
              ) then coalesce(
                nullif(component.value->>'ready_preview_amount_ex_vat','')::numeric,
                nullif(component.value->>'target_pay_ex_vat','')::numeric,
                nullif(component.value->>'preview_component_amount_ex_vat','')::numeric
              )
              else coalesce(
                nullif(component.value->>'component_amount_ex_vat','')::numeric,
                nullif(component.value->>'authoritative_outstanding_ex_vat','')::numeric,
                nullif(component.value->>'preview_due_amount_ex_vat','')::numeric,
                nullif(component.value->>'ready_preview_amount_ex_vat','')::numeric,
                nullif(component.value->>'target_pay_ex_vat','')::numeric,
                nullif(component.value->>'preview_component_amount_ex_vat','')::numeric,
                0::numeric
              )
            end, 2) as component_amount_ex_vat,
            coalesce(nullif(btrim(coalesce(component.value->>'component_fingerprint','')), ''), md5(component.value::text)) as component_fingerprint,
            coalesce(
              nullif(btrim(coalesce(component.value#>>'{source_basis_json,segment_stable_key}','')), ''),
              nullif(btrim(coalesce(component.value#>>'{source_basis_json,segment_id}','')), ''),
              nullif(btrim(coalesce(component.value#>>'{source_basis_json,segment_key}','')), ''),
              nullif(btrim(coalesce(component.value#>>'{source_basis_json,work_date}','')), ''),
              nullif(btrim(coalesce(component.value#>>'{source_basis_json,date}','')), ''),
              nullif(btrim(coalesce(component.value->>'component_key_value','')), '')
            ) as stable_component_identity,
            lower(btrim(coalesce(component.value->>'requires_resolution','false'))) in ('true','t','1','yes','y','on') as requires_resolution
          from canonical_timesheet_lines ctl
          cross join lateral jsonb_array_elements(
            case when jsonb_typeof(ctl.case_components_json)='array'
              then ctl.case_components_json else '[]'::jsonb end
          ) component(value)
        ), eligible_components as (
          select
            component_rows.*,
            coalesce(segment_match.segment_match_count, 0)::integer as segment_match_count,
            segment_match.segment_json as matched_segment_json,
            count(*) over (
              partition by component_rows.candidate_id,
                component_rows.timesheet_id,
                component_rows.component_key_type,
                component_rows.stable_component_identity
            ) as stable_component_identity_count,
            case
              when component_rows.component_key_type='TS_DAY' then
                component_rows.timesheet_id::text || ':segment:' || component_rows.stable_component_identity
                || case
                  when count(*) over (
                    partition by component_rows.candidate_id,
                      component_rows.timesheet_id,
                      component_rows.component_key_type,
                      component_rows.stable_component_identity
                  ) > 1 then ':bucket:' || lower(coalesce(
                    nullif(btrim(coalesce(component_rows.component_json->>'bucket_code','')), ''),
                    nullif(btrim(coalesce(component_rows.component_json#>>'{source_basis_json,bucket_code}','')), ''),
                    '~'
                  ))
                  else ''
                end
              when component_rows.component_key_type='EXPENSE_CODE' then
                component_rows.timesheet_id::text || ':component:expense:' || component_rows.component_fingerprint
              when lower(coalesce(component_rows.component_json->>'source_family_key','')) like 'correction%'
              then 'correction-chain:'
                || coalesce(
                  nullif(btrim(coalesce(component_rows.component_json->>'source_family_key','')), ''),
                  component_rows.timesheet_id::text
                )
                || ':' || lower(component_rows.component_key_type)
                || ':' || component_rows.component_key_value
              else component_rows.timesheet_id::text
                || ':component:' || lower(component_rows.component_key_type)
                || ':' || component_rows.component_fingerprint
            end as allocation_line_key
          from component_rows
          left join lateral (
            select
              count(*)::integer as segment_match_count,
              (jsonb_agg(
                matched_segment.segment_json
                order by matched_segment.match_rank,
                         matched_segment.seg_ord,
                         matched_segment.segment_stable_key
              )->0) as segment_json
            from (
              select
                segment_row.seg_ord,
                segment_row.segment_stable_key,
                segment_row.segment_base_json as segment_json,
                case
                  when nullif(btrim(coalesce(component_rows.stable_component_identity, '')), '') is not null
                   and segment_row.segment_stable_key is not distinct from component_rows.stable_component_identity
                    then 10
                  else 20
                end as match_rank
              from canonical_timesheet_segment_rows as segment_row
              where component_rows.component_key_type = 'TS_DAY'
                and segment_row.candidate_id = component_rows.candidate_id
                and segment_row.timesheet_id = component_rows.timesheet_id
                and segment_row.presentation_segment_state = 'READY'
                and (
                  (
                    nullif(btrim(coalesce(component_rows.stable_component_identity, '')), '') is not null
                    and segment_row.segment_stable_key is not distinct from component_rows.stable_component_identity
                  )
                  or (
                    not exists (
                      select 1
                      from canonical_timesheet_segment_rows as exact_segment
                      where exact_segment.candidate_id = component_rows.candidate_id
                        and exact_segment.timesheet_id = component_rows.timesheet_id
                        and exact_segment.presentation_segment_state = 'READY'
                        and nullif(btrim(coalesce(component_rows.stable_component_identity, '')), '') is not null
                        and exact_segment.segment_stable_key is not distinct from component_rows.stable_component_identity
                    )
                    and segment_row.segment_date::text is not distinct from component_rows.component_key_value
                  )
                )
            ) as matched_segment
          ) as segment_match on true
          where component_rows.component_key_type in ('TS_DAY','EXPENSE_CODE','ADDITIONAL_CODE','ADJUSTMENT_CODE')
            and component_rows.component_key_value is not null
            and component_rows.stable_component_identity is not null
            and (
              component_rows.component_key_type <> 'TS_DAY'
              or not exists (
                select 1
                from canonical_timesheet_segment_rows as exact_non_ready_segment
                where exact_non_ready_segment.candidate_id = component_rows.candidate_id
                  and exact_non_ready_segment.timesheet_id = component_rows.timesheet_id
                  and exact_non_ready_segment.segment_stable_key is not distinct from component_rows.stable_component_identity
                  and exact_non_ready_segment.presentation_segment_state in ('BLOCKED_VISIBLE', 'HIDDEN_INDEFINITE')
              )
            )
            and component_rows.component_amount_ex_vat > 0
            and component_rows.requires_resolution is not true
            and component_rows.is_ready_for_draft is true
            and component_rows.case_is_blocked is false
            and component_rows.snooze_id is null
            and coalesce(jsonb_array_length(component_rows.payee_blockers),0)=0
        )
        select
          eligible_components.candidate_id,
          jsonb_strip_nulls(
            jsonb_build_object(
              'preview_row_id', eligible_components.allocation_line_key,
              'line_id', eligible_components.allocation_line_key,
              'line_key', eligible_components.allocation_line_key,
              'row_key', eligible_components.allocation_line_key,
              'source_ref', eligible_components.allocation_line_key,
              'parent_line_key', eligible_components.timesheet_id::text,
              'candidate_id', eligible_components.candidate_id::text,
              'tms_ref', eligible_components.cand_tms_ref,
              'display_name', eligible_components.cand_display_name,
              'line_type', 'TIMESHEET_PAYMENT',
              'case_type', 'TIMESHEET_PAYMENT',
              'case_key', 'timesheet:' || eligible_components.timesheet_id::text
            )
            || jsonb_build_object(
              'finance_case_id', null,
              'finance_component_id', nullif(eligible_components.component_json->>'finance_component_id',''),
              'timesheet_id', eligible_components.timesheet_id::text,
              'real_business_timesheet_id', eligible_components.timesheet_id::text,
              'booking_id', eligible_components.booking_id,
              'client_id', case when eligible_components.client_id is null then null else eligible_components.client_id::text end,
              'client_name', eligible_components.client_name,
              'week_ending_date', case when eligible_components.week_ending_date is null then null else eligible_components.week_ending_date::text end,
              'linked_shift_date', case when eligible_components.component_key_type='TS_DAY' then eligible_components.component_key_value else null end,
              'component_key_type', eligible_components.component_key_type,
              'component_key_value', eligible_components.component_key_value,
              'key_type', eligible_components.component_key_type,
              'key_value', eligible_components.component_key_value,
              'economic_key', jsonb_build_object(
                'timesheet_id', eligible_components.timesheet_id::text,
                'key_type', eligible_components.component_key_type,
                'key_value', eligible_components.component_key_value
              ),
              'component_fingerprint', eligible_components.component_fingerprint,
              'bucket_code', nullif(btrim(coalesce(eligible_components.component_json->>'bucket_code','')), ''),
              'physical_bucket_key', eligible_components.component_json->'physical_bucket_key',
              'physical_bucket_digest', eligible_components.component_json->'physical_bucket_digest',
              'source_family_key', eligible_components.component_json->'source_family_key',
              'source_basis_fingerprint', eligible_components.component_json->'source_basis_fingerprint',
              'source_basis_json', eligible_components.component_json->'source_basis_json',
              'source_units', eligible_components.component_json->'source_units',
              'source_rate', eligible_components.component_json->'source_rate',
              'source_charge_rate', eligible_components.component_json->'source_charge_rate',
              'source_pay_ex_vat', eligible_components.component_json->'source_pay_ex_vat',
              'source_charge_ex_vat', eligible_components.component_json->'source_charge_ex_vat',
              'target_rate', eligible_components.component_json->'target_rate',
              'target_pay_ex_vat', eligible_components.component_json->'target_pay_ex_vat',
              'source_pay_method', eligible_components.component_json->'source_pay_method',
              'current_target_pay_method', eligible_components.component_json->'current_target_pay_method',
              'segment_id', eligible_components.matched_segment_json->'segment_id',
              'segment_key', eligible_components.matched_segment_json->'segment_key',
              'segment_stable_key', eligible_components.matched_segment_json->'segment_stable_key',
              'date', eligible_components.matched_segment_json->'date',
              'work_date', coalesce(
                eligible_components.matched_segment_json->'work_date',
                eligible_components.matched_segment_json->'date'
              ),
              'role', eligible_components.matched_segment_json->'role',
              'band', eligible_components.matched_segment_json->'band',
              'start', eligible_components.matched_segment_json->'start',
              'finish', eligible_components.matched_segment_json->'finish',
              'start_utc', eligible_components.matched_segment_json->'start_utc',
              'end_utc', eligible_components.matched_segment_json->'end_utc',
              'break_start', eligible_components.matched_segment_json->'break_start',
              'break_end', eligible_components.matched_segment_json->'break_end',
              'break_mins', eligible_components.matched_segment_json->'break_mins',
              'breaks', eligible_components.matched_segment_json->'breaks',
              'ref_num', eligible_components.matched_segment_json->'ref_num'
            )
            || jsonb_build_object(
              'case_resolution_summary', coalesce(eligible_components.case_resolution_summary_json, '{}'::jsonb),
              'case_resolution_summary_json', coalesce(eligible_components.case_resolution_summary_json, '{}'::jsonb),
              'has_resolved_rate', eligible_components.case_resolution_summary_json->'has_resolved_rate',
              'resolved_rate_applied', eligible_components.case_resolution_summary_json->'resolved_rate_applied',
              'resolved_rate_active', eligible_components.case_resolution_summary_json->'resolved_rate_active',
              'case_resolution_satisfied_now', eligible_components.case_resolution_summary_json->'case_resolution_satisfied_now',
              'resolved_rate_family', eligible_components.case_resolution_summary_json->'resolved_rate_family',
              'resolved_rate_component_count', eligible_components.case_resolution_summary_json->'resolved_rate_component_count',
              'resolved_rate_candidate_id', coalesce(
                eligible_components.case_resolution_summary_json->'resolved_rate_candidate_id',
                eligible_components.case_resolution_summary_json->'resolution_candidate_id'
              ),
              'resolved_rate_timesheet_id', coalesce(
                eligible_components.case_resolution_summary_json->'resolved_rate_timesheet_id',
                eligible_components.case_resolution_summary_json->'resolution_timesheet_id'
              ),
              'resolved_rate_case_key', coalesce(
                eligible_components.case_resolution_summary_json->'resolved_rate_case_key',
                eligible_components.case_resolution_summary_json->'resolution_case_key'
              ),
              'resolution_candidate_id', eligible_components.case_resolution_summary_json->'resolution_candidate_id',
              'resolution_timesheet_id', eligible_components.case_resolution_summary_json->'resolution_timesheet_id',
              'resolution_case_key', eligible_components.case_resolution_summary_json->'resolution_case_key',
              'case_resolution_id', eligible_components.case_resolution_summary_json->'case_resolution_id',
              'case_resolution_ids', eligible_components.case_resolution_summary_json->'case_resolution_ids',
              'resolution_identity_keys', eligible_components.case_resolution_summary_json->'resolution_identity_keys',
              'resolved_rate_clear_payload_json', eligible_components.case_resolution_summary_json->'resolved_rate_clear_payload_json',
              'case_components', jsonb_build_array(eligible_components.component_json),
              'amount_ex_vat', eligible_components.component_amount_ex_vat,
              'amount_display', eligible_components.component_amount_ex_vat,
              'item_direction', 'PAYMENT',
              'pay_channel', eligible_components.candidate_pay_method,
              'paye_treatment', case when eligible_components.candidate_pay_method='PAYE' then 'GROSS_ADD' else 'NONE' end,
              'route_type', 'NORMAL_PAYMENT',
              'payee_entity_kind', eligible_components.payee_entity_kind,
              'payee_entity_id', case when eligible_components.payee_entity_id is null then null else eligible_components.payee_entity_id::text end,
              'payee_bank_hash', eligible_components.payee_bank_hash,
              'bank_details_hash', eligible_components.payee_bank_hash,
              'name_check_status', eligible_components.payee_name_check_status,
              'name_check_has_override', eligible_components.payee_name_check_has_override,
              'payee_map_present', eligible_components.payee_map_present,
              'blockers', eligible_components.payee_blockers,
              'payee_blockers', eligible_components.payee_blockers,
              'presentation_section', 'READY_TO_PAY',
              'presentation_role', 'ALLOCATION_COMPONENT',
              'presentation_parent_line_id', eligible_components.timesheet_id::text,
              'readiness_state', 'READY_TO_PAY',
              'is_excluded_from_allocation', false,
              'is_ready_for_draft', true,
              'draftable', true,
              'selection_allowed', true,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ) as line_json,
          eligible_components.candidate_pay_method as pay_channel,
          case when eligible_components.candidate_pay_method='PAYE' then 'GROSS_ADD' else 'NONE' end as paye_treatment,
          eligible_components.component_amount_ex_vat as amount_ex_vat,
          false as is_excluded_from_allocation,
          eligible_components.component_key_type,
          eligible_components.segment_match_count
        from eligible_components

  ;

  if exists (
    select 1
    from timesheet_allocation_component_lines as component_line
    where component_line.component_key_type = 'TS_DAY'
      and component_line.segment_match_count = 0
  ) then
    SELECT pg_catalog.jsonb_build_object(
      'code', 'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_MISSING',
      'candidate_id', component_line.candidate_id,
      'timesheet_id', component_line.timesheet_id,
      'component_key_type', component_line.component_key_type,
      'component_key_value', component_line.key_value,
      'source_basis_fingerprint', component_line.line_json->>'source_basis_fingerprint',
      'match_count', component_line.segment_match_count
    )
    INTO v_allocation_segment_failure
    FROM timesheet_allocation_component_lines AS component_line
    WHERE component_line.component_key_type = 'TS_DAY'
      AND component_line.segment_match_count = 0
    ORDER BY component_line.candidate_id, component_line.timesheet_id,
      component_line.key_value, component_line.line_key
    LIMIT 1;

    raise exception 'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_MISSING'
      using errcode = 'P0001', detail = v_allocation_segment_failure::text;
  end if;

  if exists (
    select 1
    from timesheet_allocation_component_lines as component_line
    where component_line.component_key_type = 'TS_DAY'
      and component_line.segment_match_count > 1
  ) then
    SELECT pg_catalog.jsonb_build_object(
      'code', 'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_AMBIGUOUS',
      'candidate_id', component_line.candidate_id,
      'timesheet_id', component_line.timesheet_id,
      'component_key_type', component_line.component_key_type,
      'component_key_value', component_line.key_value,
      'source_basis_fingerprint', component_line.line_json->>'source_basis_fingerprint',
      'match_count', component_line.segment_match_count
    )
    INTO v_allocation_segment_failure
    FROM timesheet_allocation_component_lines AS component_line
    WHERE component_line.component_key_type = 'TS_DAY'
      AND component_line.segment_match_count > 1
    ORDER BY component_line.candidate_id, component_line.timesheet_id,
      component_line.key_value, component_line.line_key
    LIMIT 1;

    raise exception 'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_AMBIGUOUS'
      using errcode = 'P0001', detail = v_allocation_segment_failure::text;
  end if;

  -- The finance resolver's run-level headroom predates semantic allocation
  -- children and therefore used aggregate presentation parents.  V3 replaces
  -- that display-only basis with exact positive keyed allocation authority.
  -- Nominal debt is never changed: only the amount recoverable in this pay run
  -- is capped, in the existing oldest-case order, to same-candidate headroom.
  -- A28: honour the slice hand-over only inside the exact authoritative Workbench build that set it.
  v_a28_slice_applies := v_a28_slice IS NOT NULL
    AND COALESCE((v_a28_slice->>'final')::boolean, false)
    AND COALESCE(v_a28_slice->>'build_token', '') <> ''
    AND v_a28_slice->>'build_token' = COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token', true), '');
  IF v_a28_slice_applies THEN
    v_a28_slice_candidate_id := (v_a28_slice->>'candidate_id')::uuid;
  END IF;
  create temporary table semantic_finance_case_lines on commit drop as
        with allocation_headroom as (
          select
            component_line.candidate_id,
            round(coalesce(sum(component_line.amount_ex_vat),0),2) as ordinary_positive_headroom
          from timesheet_allocation_component_lines component_line
          where component_line.amount_ex_vat > 0
          group by component_line.candidate_id
        ), ranked as (
          select
            finance_line.*,
            (coalesce(headroom.ordinary_positive_headroom,0)
              + case when v_a28_slice_applies and finance_line.candidate_id = v_a28_slice_candidate_id
                  then coalesce((v_a28_slice->>'prior_ordinary_positive_headroom_ex')::numeric,0) else 0 end
            )::numeric as ordinary_positive_headroom, /* A28 */
            coalesce(
              sum(
                case
                  when finance_line.draftable is true
                   and finance_line.item_direction='DEDUCTION'
                   and finance_line.signed_amount_ex_vat < 0
                  then abs(finance_line.signed_amount_ex_vat)
                  else 0
                end
              ) over (
                partition by finance_line.candidate_id
                order by finance_line.semantic_recovery_sort_at_utc nulls last,
                         finance_line.finance_case_id
                rows between unbounded preceding and 1 preceding
              ),
              0
            )::numeric as prior_semantic_recovery_amount,
            (
              finance_line.draftable is true
              and finance_line.item_direction='DEDUCTION'
              and finance_line.signed_amount_ex_vat < 0
            ) as is_semantic_recovery
          from finance_case_lines finance_line
          left join allocation_headroom headroom
            on headroom.candidate_id=finance_line.candidate_id
        ), capped as (
          select
            ranked.*,
            case
              when v_semantic_ready_publication_enabled and ranked.is_semantic_recovery
              then round(least(
                abs(ranked.signed_amount_ex_vat),
                greatest(ranked.ordinary_positive_headroom-ranked.prior_semantic_recovery_amount,0)
              ),2)
              else round(abs(ranked.signed_amount_ex_vat),2)
            end as semantic_recovery_due_amount
          from ranked
        )
        select
          capped.*,
          case
            when v_semantic_ready_publication_enabled and capped.is_semantic_recovery
            then -capped.semantic_recovery_due_amount
            else capped.signed_amount_ex_vat
          end as semantic_signed_amount_ex_vat,
          case
            when v_semantic_ready_publication_enabled and capped.is_semantic_recovery
            then capped.draftable and capped.semantic_recovery_due_amount > 0
            else capped.draftable
          end as semantic_draftable,
          case
            when v_semantic_ready_publication_enabled
             and capped.is_semantic_recovery
             and capped.semantic_recovery_due_amount = 0
            then 'BLOCKED_FOR_PAY'
            else capped.readiness_state
          end as semantic_readiness_state,
          case
            when v_semantic_ready_publication_enabled
             and capped.is_semantic_recovery
             and capped.semantic_recovery_due_amount = 0
            then 'BLOCKED_FOR_PAY'
            else capped.presentation_section
          end as semantic_presentation_section,
          case
            when v_semantic_ready_publication_enabled
             and capped.is_semantic_recovery
             and capped.semantic_recovery_due_amount = 0
            then 'NO_PAY_HEADROOM'
            else capped.presentation_reason
          end as semantic_presentation_reason,
          case
            when v_semantic_ready_publication_enabled
             and capped.is_semantic_recovery
             and capped.semantic_recovery_due_amount = 0
            then capped.blocked_reason_codes || jsonb_build_array('NO_PAY_HEADROOM')
            else capped.blocked_reason_codes
          end as semantic_blocked_reason_codes,
          (
            v_semantic_ready_publication_enabled
            and capped.is_semantic_recovery
            and capped.semantic_recovery_due_amount < abs(capped.signed_amount_ex_vat)
          ) as semantic_recovery_headroom_capped
        from capped

  ;

  create temporary table timesheet_canonical_preview_lines on commit drop as
        select
          ctpr.candidate_id,
          (
            ctpr.line_json
            || jsonb_build_object(
              'preview_row_id', coalesce(nullif(btrim(coalesce(ctpr.line_json->>'line_id','')), ''), md5(ctpr.line_json::text)),
              'readiness_state', case
                when upper(coalesce(ctpr.line_json->>'presentation_section','')) = 'CASES_RESOLUTIONS'
                then 'CASES_RESOLUTIONS'
                when upper(coalesce(ctpr.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY'
                  or coalesce(nullif(ctpr.line_json->>'is_ready_for_draft','')::boolean, false) = false
                then 'BLOCKED_FOR_PAY'
                else 'READY_TO_PAY'
              end,
              'draftable', CASE WHEN v_semantic_ready_publication_enabled THEN false ELSE (
                upper(coalesce(ctpr.line_json->>'presentation_section','')) = 'READY_TO_PAY'
                and coalesce(nullif(ctpr.line_json->>'is_excluded_from_allocation','')::boolean, false) = false
                and coalesce(nullif(ctpr.line_json->>'is_ready_for_draft','')::boolean, false) = true
              ) END,
              'is_ready_for_draft', CASE WHEN v_semantic_ready_publication_enabled THEN false
                ELSE coalesce(nullif(ctpr.line_json->>'is_ready_for_draft','')::boolean,false) END,
              'selection_allowed', CASE WHEN v_semantic_ready_publication_enabled THEN false
                ELSE coalesce(nullif(ctpr.line_json->>'selection_allowed','')::boolean,
                  coalesce(nullif(ctpr.line_json->>'draftable','')::boolean,false)) END,
              'is_excluded_from_allocation', CASE WHEN v_semantic_ready_publication_enabled THEN true
                ELSE coalesce(nullif(ctpr.line_json->>'is_excluded_from_allocation','')::boolean,false) END
            )
          ) as line_json,
          ctpr.pay_channel,
          ctpr.paye_treatment,
          ctpr.amount_ex_vat,
          CASE WHEN v_semantic_ready_publication_enabled THEN true
            ELSE ctpr.is_excluded_from_allocation END as is_excluded_from_allocation
        from canonical_timesheet_presentation_rows ctpr

        union all

        select
          component_line.candidate_id,
          component_line.line_json,
          component_line.pay_channel,
          component_line.paye_treatment,
          component_line.amount_ex_vat,
          component_line.is_excluded_from_allocation
        from timesheet_allocation_component_lines component_line
        where v_semantic_ready_publication_enabled

  ;

  create temporary table canonical_preview_lines on commit drop as
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
              'case_is_blocked', (
                fcl.case_is_blocked
                or (v_semantic_ready_publication_enabled
                  and fcl.is_semantic_recovery
                  and fcl.semantic_recovery_due_amount = 0)
              )
            )
            || jsonb_build_object(
              'case_resolution_summary', fcl.case_resolution_summary_json,
              'case_components', fcl.case_components_json,
              'finance_component_id', case when fcl.finance_component_id is null then null else fcl.finance_component_id::text end,
              'component_key_type', fcl.component_key_type,
              'component_key_value', fcl.component_key_value,
              'key_type', fcl.component_key_type,
              'key_value', fcl.component_key_value,
              'economic_key', jsonb_strip_nulls(jsonb_build_object(
                'timesheet_id', case when fcl.linked_timesheet_id is null then null else fcl.linked_timesheet_id::text end,
                'key_type', fcl.component_key_type,
                'key_value', fcl.component_key_value
              )),
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
              'blocked_reason_codes', fcl.semantic_blocked_reason_codes,
              'readiness_state', fcl.semantic_readiness_state,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
              'draftable', fcl.semantic_draftable,
              'snooze_allowed', fcl.snooze_allowed,
              'oneoff_bank_details_present', fcl.oneoff_bank_details_present,
              'is_candidate_directed_oneoff_payout', fcl.is_candidate_directed_oneoff_payout,
              'appears_on_umbrella_remittance', fcl.appears_on_umbrella_remittance,
              'generates_candidate_payment_advice', fcl.generates_candidate_payment_advice,
              'adjustment_comment', fcl.adjustment_comment,
              'amount_ex_vat', fcl.semantic_signed_amount_ex_vat,
              'amount_display', fcl.semantic_signed_amount_ex_vat,
              'nominal_due_amount_ex_vat', round(coalesce(fcl.nominal_due_amount_ex_vat, 0), 2),
              'recoverable_this_pay_run_ex_vat', case
                when v_semantic_ready_publication_enabled and fcl.is_semantic_recovery
                then fcl.semantic_recovery_due_amount
                else round(greatest(coalesce(fcl.due_amount_ex_vat, 0), 0), 2)
              end,
              'semantic_ordinary_positive_headroom_ex_vat', case
                when v_semantic_ready_publication_enabled then fcl.ordinary_positive_headroom
                else null end,
              'semantic_recovery_headroom_capped', fcl.semantic_recovery_headroom_capped,
              'next_due_week_start', case when fcl.next_due_week_start is null then null else fcl.next_due_week_start::text end,
              'is_advanced', false,
              'advanced_override_id', null,
              'advanced_reason', null
            )
            || case
              when fcl.line_type = 'OVERPAYMENT_RECOVERY' then jsonb_build_object(
                'recovery_residual_contract_version', 1,
                'recovery_source_outstanding_ex_vat', round(
                  coalesce(fcl.recovery_source_outstanding_ex_vat, 0),
                  2
                ),
                'recovery_active_reserved_ex_vat', round(
                  coalesce(fcl.recovery_active_reserved_ex_vat, 0),
                  2
                ),
                'recovery_residual_outstanding_ex_vat', round(
                  coalesce(fcl.nominal_due_amount_ex_vat, 0),
                  2
                )
              )
              else '{}'::jsonb
            end
            || jsonb_build_object(
              'resolution_state', case
                when fcl.finance_resolution_clearability_state = 'RESOLVED_AND_CLEARABLE' then 'RESOLVED'
                when fcl.finance_resolution_clearability_state = 'REQUIRES_RESOLUTION' then 'REQUIRES_RESOLUTION'
                when fcl.finance_resolution_clearability_state = 'STALE_OR_AMBIGUOUS' then 'STALE'
                else 'NOT_REQUIRED'
              end,
              'resolution_badge', case when fcl.finance_resolution_clearability_state = 'RESOLVED_AND_CLEARABLE' then 'RESOLVED' else null end,
              'is_case_resolution_satisfied', fcl.is_case_resolution_satisfied,
              'case_needs_resolution_now', fcl.case_needs_resolution_now,
              'finance_resolution_clearability_state', fcl.finance_resolution_clearability_state,
              'finance_resolution_clear_block_reason', fcl.finance_resolution_clear_block_reason,
              'current_saved_resolution_family', nullif(fcl.current_saved_resolution_family, ''),
              'current_saved_resolution_owner_kind', fcl.current_saved_resolution_owner_kind,
              'case_resolution_actions', jsonb_strip_nulls(jsonb_build_object(
                'apply', case when fcl.case_needs_resolution_now then jsonb_strip_nulls(jsonb_build_object(
                  'enabled', true,
                  'action', 'APPLY_CASE_RESOLUTION',
                  'candidate_id', fcl.candidate_id::text,
                  'finance_case_id', fcl.finance_case_id::text,
                  'case_key', ('finance:' || fcl.finance_case_id::text),
                  'timesheet_id', case when fcl.linked_timesheet_id is null then null else fcl.linked_timesheet_id::text end,
                  'resolution_family', fcl.case_resolution_summary_json->>'resolution_family',
                  'label', nullif(btrim(coalesce(fcl.case_resolution_summary_json->>'resolution_action_label','')), '')
                )) else null end,
                'clear', case
                  when fcl.finance_resolution_clearability_state = 'RESOLVED_AND_CLEARABLE'
                   and fcl.current_saved_resolution_family in ('TAXABLE_CHANNEL_RESTRUCTURE', 'NON_BUCKET')
                  then jsonb_strip_nulls(jsonb_build_object(
                  'enabled', true,
                  'action', 'CLEAR_CASE_RESOLUTION',
                  'candidate_id', fcl.candidate_id::text,
                  'finance_case_id', fcl.finance_case_id::text,
                  'case_key', ('finance:' || fcl.finance_case_id::text),
                  'linked_timesheet_id', case
                    when fcl.current_saved_resolution_family = 'NON_BUCKET'
                     and fcl.current_saved_resolution_linked_timesheet_id is not null
                      then fcl.current_saved_resolution_linked_timesheet_id::text
                    else null
                  end,
                  'resolution_family', fcl.current_saved_resolution_family,
                  'label', case
                    when fcl.current_saved_resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' then 'Cancel Resolved Pay Channel'
                    when fcl.current_saved_resolution_family = 'NON_BUCKET' then 'Cancel Resolved Gross Total'
                    else null
                  end
                )) else null end
              )),
              'apply_case_resolution_action', case when fcl.case_needs_resolution_now then jsonb_strip_nulls(jsonb_build_object(
                'enabled', true,
                'action', 'APPLY_CASE_RESOLUTION',
                'candidate_id', fcl.candidate_id::text,
                'finance_case_id', fcl.finance_case_id::text,
                'case_key', ('finance:' || fcl.finance_case_id::text),
                'timesheet_id', case when fcl.linked_timesheet_id is null then null else fcl.linked_timesheet_id::text end,
                'resolution_family', fcl.case_resolution_summary_json->>'resolution_family',
                'label', nullif(btrim(coalesce(fcl.case_resolution_summary_json->>'resolution_action_label','')), '')
              )) else null end,
              'clear_case_resolution_action', case
                when fcl.finance_resolution_clearability_state = 'RESOLVED_AND_CLEARABLE'
                 and fcl.current_saved_resolution_family in ('TAXABLE_CHANNEL_RESTRUCTURE', 'NON_BUCKET')
                then jsonb_strip_nulls(jsonb_build_object(
                'enabled', true,
                'action', 'CLEAR_CASE_RESOLUTION',
                'candidate_id', fcl.candidate_id::text,
                'finance_case_id', fcl.finance_case_id::text,
                'case_key', ('finance:' || fcl.finance_case_id::text),
                'linked_timesheet_id', case
                  when fcl.current_saved_resolution_family = 'NON_BUCKET'
                   and fcl.current_saved_resolution_linked_timesheet_id is not null
                    then fcl.current_saved_resolution_linked_timesheet_id::text
                  else null
                end,
                'resolution_family', fcl.current_saved_resolution_family,
                'label', case
                  when fcl.current_saved_resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' then 'Cancel Resolved Pay Channel'
                  when fcl.current_saved_resolution_family = 'NON_BUCKET' then 'Cancel Resolved Gross Total'
                  else null
                end
              )) else null end
            )
            || jsonb_build_object(
              'is_excluded_from_allocation', (fcl.semantic_draftable is not true),
              'selection_allowed', fcl.semantic_draftable,
              'is_ready_for_draft', fcl.semantic_draftable,
              'presentation_section', fcl.semantic_presentation_section,
              'presentation_role', 'PARENT',
              'presentation_line_id', ('finance:' || fcl.finance_case_id::text || ':' || lower(fcl.line_type)),
              'presentation_parent_line_id', ('finance:' || fcl.finance_case_id::text),
              'presentation_reason', fcl.semantic_presentation_reason,
              'source_ref', ('advance:' || fcl.finance_case_id::text),
              'snooze_kind', case
                when fcl.case_type = 'PAYMENT_ADVANCE' and upper(coalesce(fcl.source_payout_status::text,'')) = 'PAID' then 'PAYMENT_ADVANCE_REPAYMENT'
                when fcl.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'MANUAL_DEBT_RECOVERY'
                when fcl.case_type = 'UNDERPAYMENT' then 'UNDERPAYMENT_PAYMENT'
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
          fcl.semantic_signed_amount_ex_vat as amount_ex_vat,
          (fcl.semantic_draftable is not true) as is_excluded_from_allocation
        from semantic_finance_case_lines fcl

        union all

        select
          cf_lines.candidate_id,
          (
            jsonb_build_object(
              'preview_row_id', cf_lines.preview_row_id,
              'line_id', cf_lines.preview_row_id,
              'candidate_id', cf_lines.candidate_id::text,
              'tms_ref', cf_lines.cand_tms_ref,
              'display_name', cf_lines.cand_display_name,
              'payee_entity_kind', cf_lines.payee_entity_kind,
              'payee_entity_id', CASE WHEN cf_lines.payee_entity_id IS NULL THEN NULL ELSE cf_lines.payee_entity_id::text END,
              'payee_context', jsonb_build_object(
                'payee_entity_kind', cf_lines.payee_entity_kind,
                'payee_entity_id', CASE WHEN cf_lines.payee_entity_id IS NULL THEN NULL ELSE cf_lines.payee_entity_id::text END,
                'pay_channel', cf_lines.pay_channel,
                'umbrella_id', CASE WHEN cf_lines.umbrella_id IS NULL THEN NULL ELSE cf_lines.umbrella_id::text END
              ),
              'line_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
              'case_key', ('carry_forward:' || cf_lines.manual_adjustment_carry_forward_id::text),
              'case_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
              'case_is_blocked', false
            )
            || jsonb_build_object(
              'case_resolution_summary', '{}'::jsonb,
              'case_components', '[]'::jsonb,
              'item_type_label', 'Manual adjustment carry-forward',
              'item_direction', CASE WHEN coalesce(cf_lines.amount_inc_vat, 0) < 0 THEN 'DEBIT' ELSE 'CREDIT' END,
              'manual_adjustment_carry_forward_id', cf_lines.manual_adjustment_carry_forward_id::text,
              'component_key_type', 'MANUAL_CARRY_FORWARD',
              'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text,
              'key_type', 'MANUAL_CARRY_FORWARD',
              'key_value', cf_lines.manual_adjustment_carry_forward_id::text,
              'economic_key', jsonb_strip_nulls(jsonb_build_object(
                'timesheet_id', CASE WHEN cf_lines.timesheet_id IS NULL THEN NULL ELSE cf_lines.timesheet_id::text END,
                'key_type', 'MANUAL_CARRY_FORWARD',
                'key_value', cf_lines.manual_adjustment_carry_forward_id::text
              )),
              'source_ref', ('carry_forward:' || cf_lines.manual_adjustment_carry_forward_id::text),
              'operation_source_key', cf_lines.operation_source_key,
              'source_operation_source_key', cf_lines.source_operation_source_key,
              'description', cf_lines.display_description,
              'adjustment_comment', cf_lines.display_description,
              'original_description', cf_lines.original_description,
              'reason', cf_lines.reason
            )
            || jsonb_build_object(
              'source_pay_batch_id', CASE WHEN cf_lines.source_pay_batch_id IS NULL THEN NULL ELSE cf_lines.source_pay_batch_id::text END,
              'source_pay_batch_item_id', CASE WHEN cf_lines.source_pay_batch_item_id IS NULL THEN NULL ELSE cf_lines.source_pay_batch_item_id::text END,
              'source_pay_bank_transfer_id', CASE WHEN cf_lines.source_pay_bank_transfer_id IS NULL THEN NULL ELSE cf_lines.source_pay_bank_transfer_id::text END,
              'source_pay_batch_candidate_id', CASE WHEN cf_lines.source_pay_batch_candidate_id IS NULL THEN NULL ELSE cf_lines.source_pay_batch_candidate_id::text END,
              'source_correction_request_id', CASE WHEN cf_lines.source_correction_request_id IS NULL THEN NULL ELSE cf_lines.source_correction_request_id::text END,
              'source_correction_work_item_id', CASE WHEN cf_lines.source_correction_work_item_id IS NULL THEN NULL ELSE cf_lines.source_correction_work_item_id::text END,
              'source_batch_ref', cf_lines.source_batch_ref,
              'umbrella_id', CASE WHEN cf_lines.umbrella_id IS NULL THEN NULL ELSE cf_lines.umbrella_id::text END,
              'client_id', CASE WHEN cf_lines.client_id IS NULL THEN NULL ELSE cf_lines.client_id::text END,
              'timesheet_id', CASE WHEN cf_lines.timesheet_id IS NULL THEN NULL ELSE cf_lines.timesheet_id::text END,
              'pay_channel', cf_lines.pay_channel,
              'candidate_pay_method', cf_lines.pay_channel
            )
            || jsonb_build_object(
              'paye_treatment', cf_lines.paye_treatment,
              'tax_treatment_json', cf_lines.tax_treatment_json,
              'taxability', cf_lines.tax_treatment_json->>'taxability',
              'amount_basis', cf_lines.amount_basis,
              'adjustment_direction', cf_lines.adjustment_direction,
              'amount_ex_vat', cf_lines.amount_ex_vat,
              'amount_vat', cf_lines.amount_vat,
              'amount_inc_vat', cf_lines.amount_inc_vat,
              'amount_display', cf_lines.amount_inc_vat,
              'payment_amount_ex_vat', cf_lines.amount_ex_vat,
              'payment_amount_vat', cf_lines.amount_vat,
              'payment_amount_inc_vat', cf_lines.amount_inc_vat
            )
            || jsonb_build_object(
              'payment_amount', cf_lines.amount_inc_vat,
              'source_snapshot_json', cf_lines.source_snapshot_json,
              'manual_adjustment_carry_forward_status', cf_lines.status,
              'target_pay_batch_id', CASE WHEN cf_lines.target_pay_batch_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_id::text END,
              'target_pay_batch_item_id', CASE WHEN cf_lines.target_pay_batch_item_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_item_id::text END,
              'target_operation_source_key', cf_lines.target_operation_source_key,
              'readiness_state', 'READY_TO_PAY',
              'draftable', true,
              'is_ready_for_draft', true,
              'selection_allowed', true,
              'is_excluded_from_allocation', false,
              'presentation_section', 'READY_TO_PAY',
              'presentation_role', 'PARENT'
            )
            || jsonb_build_object(
              'presentation_line_id', cf_lines.preview_row_id,
              'presentation_parent_line_id', cf_lines.preview_row_id,
              'presentation_reason', 'READY_TO_PAY',
              'route_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
              'routing_kind', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
              'appears_on_umbrella_remittance', (cf_lines.pay_channel = 'UMBRELLA'),
              'generates_candidate_payment_advice', true
            )
          ) as line_json,
          cf_lines.pay_channel,
          cf_lines.paye_treatment,
          cf_lines.amount_ex_vat,
          false as is_excluded_from_allocation
        from manual_adjustment_carry_forward_lines cf_lines

  ;

  create temporary table candidate_preview_line_rollup on commit drop as
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

  ;

  create temporary table candidate_preview_timesheet_rollup on commit drop as
        with emitted_public_timesheets as (
          select
            tcpl.candidate_id,
            (tcpl.line_json->>'real_business_timesheet_id')::uuid as timesheet_id,
            bool_or(
              upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'READY_TO_PAY'
              and coalesce(nullif(tcpl.line_json->>'draftable','')::boolean, false) = true
            ) as has_ready_public_line,
            bool_or(
              upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY'
            ) as has_blocked_public_line,
            round(
              coalesce(sum(tcpl.amount_ex_vat) filter (
                where upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'READY_TO_PAY'
                  and coalesce(nullif(tcpl.line_json->>'draftable','')::boolean, false) = true
              ), 0),
              2
            ) as ready_public_amount_ex_vat
          from timesheet_canonical_preview_lines tcpl
          where upper(coalesce(tcpl.line_json->>'line_type','')) = 'TIMESHEET_PAYMENT'
            and nullif(btrim(coalesce(tcpl.line_json->>'real_business_timesheet_id','')), '') is not null
            and upper(coalesce(tcpl.line_json->>'presentation_section','')) in ('READY_TO_PAY','BLOCKED_FOR_PAY')
          group by
            tcpl.candidate_id,
            (tcpl.line_json->>'real_business_timesheet_id')::uuid
        )
        select
          ctpp.candidate_id,
          round(
            coalesce(sum(ept.ready_public_amount_ex_vat), 0),
            2
          ) as ready_timesheet_total_ex_vat,
          count(*) filter (
            where coalesce(ept.has_blocked_public_line, false) = true
          )::int as blocked_timesheet_preview_count,
          count(*) filter (
            where coalesce(ept.has_ready_public_line, false) = true
          )::int as ready_timesheet_preview_count,
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'timesheet_id', ctpp.timesheet_id::text,
                'week_ending_date', case when ctpp.week_ending_date is null then null else ctpp.week_ending_date::text end,
                'client_id', case when ctpp.client_id is null then null else ctpp.client_id::text end,
                'client_name', ctpp.client_name,
                'payment_amount_ex_vat', ept.ready_public_amount_ex_vat,
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
                'delta_expenses_pay_ex_vat', coalesce((
                  select round(sum(coalesce(
                    case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                    0::numeric
                  )), 2)
                  from jsonb_array_elements(coalesce(ctpp.case_components_json, '[]'::jsonb)) as expense_component(value)
                  where upper(btrim(coalesce(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
                    and upper(btrim(coalesce(expense_component.value->>'component_key_value', expense_component.value->>'expense_code', ''))) = 'EXPENSES'
                    and upper(btrim(coalesce(expense_component.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
                ), 0),
                'delta_travel_pay_ex_vat', coalesce((
                  select round(sum(coalesce(
                    case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                    0::numeric
                  )), 2)
                  from jsonb_array_elements(coalesce(ctpp.case_components_json, '[]'::jsonb)) as expense_component(value)
                  where upper(btrim(coalesce(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
                    and upper(btrim(coalesce(expense_component.value->>'component_key_value', expense_component.value->>'expense_code', ''))) = 'TRAVEL'
                    and upper(btrim(coalesce(expense_component.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
                ), 0),
                'delta_accommodation_pay_ex_vat', coalesce((
                  select round(sum(coalesce(
                    case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                    0::numeric
                  )), 2)
                  from jsonb_array_elements(coalesce(ctpp.case_components_json, '[]'::jsonb)) as expense_component(value)
                  where upper(btrim(coalesce(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
                    and upper(btrim(coalesce(expense_component.value->>'component_key_value', expense_component.value->>'expense_code', ''))) = 'ACCOMMODATION'
                    and upper(btrim(coalesce(expense_component.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
                ), 0),
                'delta_other_pay_ex_vat', coalesce((
                  select round(sum(coalesce(
                    case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                    0::numeric
                  )), 2)
                  from jsonb_array_elements(coalesce(ctpp.case_components_json, '[]'::jsonb)) as expense_component(value)
                  where upper(btrim(coalesce(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
                    and upper(btrim(coalesce(expense_component.value->>'component_key_value', expense_component.value->>'expense_code', ''))) = 'OTHER'
                    and upper(btrim(coalesce(expense_component.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
                ), 0),
                'delta_mileage_pay_ex_vat', coalesce((
                  select round(sum(coalesce(
                    case when coalesce(expense_component.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'ready_preview_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'preview_component_amount_ex_vat')::numeric else null::numeric end,
                    case when coalesce(expense_component.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$' then (expense_component.value->>'component_amount_ex_vat')::numeric else null::numeric end,
                    0::numeric
                  )), 2)
                  from jsonb_array_elements(coalesce(ctpp.case_components_json, '[]'::jsonb)) as expense_component(value)
                  where upper(btrim(coalesce(expense_component.value->>'component_key_type', ''))) = 'EXPENSE_CODE'
                    and upper(btrim(coalesce(expense_component.value->>'component_key_value', expense_component.value->>'expense_code', ''))) = 'MILEAGE'
                    and upper(btrim(coalesce(expense_component.value->>'presentation_section', 'READY_TO_PAY'))) = 'READY_TO_PAY'
                ), 0),
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
            ) filter (where coalesce(ept.has_ready_public_line, false) = true),
            '[]'::jsonb
          ) as ready_timesheets_itemisation
        from canonical_timesheet_presentation_state ctpp
        left join emitted_public_timesheets ept
          on ept.candidate_id = ctpp.candidate_id
         and ept.timesheet_id = ctpp.timesheet_id
        left join timesheet_case_rollup_effective tcr
          on tcr.timesheet_id = ctpp.timesheet_id
         and tcr.candidate_id = ctpp.candidate_id
        group by ctpp.candidate_id

  ;

  v_canonical_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_canonical_started_at)) * 1000.0)::numeric, 3);

  return jsonb_build_object(
    'candidate_id', v_candidate_id::text,
    'canonical_preview_line_count', (select count(*)::int from canonical_preview_lines),
    'ready_preview_line_count', coalesce((select sum(case when coalesce(nullif(cpl.line_json->>'draftable','')::boolean, false) = true then 1 else 0 end)::int from canonical_preview_lines cpl), 0),
    'blocked_preview_line_count', coalesce((select sum(case when upper(coalesce(cpl.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY' then 1 else 0 end)::int from canonical_preview_lines cpl), 0),
    'hidden_recovery_template_line_count', (select count(*)::int from hidden_recovery_template_lines),
    'manual_adjustment_carry_forward_line_count', (select count(*)::int from manual_adjustment_carry_forward_lines),
    'hidden_recovery_template_lines', coalesce(
      (
        select jsonb_agg(
                 hrtl.template_json
                 order by hrtl.candidate_id, hrtl.recovery_family, hrtl.sort_order, hrtl.finance_case_id
               )
        from hidden_recovery_template_lines hrtl
      ),
      '[]'::jsonb
    ),
    'source_build_canonical_diagnostics', jsonb_build_object(
      'source_build_mode', COALESCE(v_source_build_mode, false),
      'precollected_context_required', COALESCE(v_source_build_context_required, false),
      'collect_called_inside_canonical', COALESCE(v_collect_called_inside_canonical, false),
      'recollect_attempted', COALESCE(v_collect_recollect_attempted, false),
      'recollect_blocked', COALESCE(v_collect_recollect_blocked, false),
      'source_rows_seen', COALESCE((select count(*)::int from ts_baseline), 0),
      'canonical_preview_line_count', (select count(*)::int from canonical_preview_lines),
      'ready_preview_line_count', coalesce((select sum(case when coalesce(nullif(cpl.line_json->>'draftable','')::boolean, false) = true then 1 else 0 end)::int from canonical_preview_lines cpl), 0),
      'blocked_preview_line_count', coalesce((select sum(case when upper(coalesce(cpl.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY' then 1 else 0 end)::int from canonical_preview_lines cpl), 0),
      'canonical_elapsed_ms', v_canonical_elapsed_ms,
      'semantic_ready_observe_enabled',v_semantic_ready_observe_enabled,
      'semantic_ready_publication_enabled',v_semantic_ready_publication_enabled,
      'proposed_semantic_allocation_component_count',CASE
        WHEN v_semantic_ready_observe_enabled OR v_semantic_ready_publication_enabled
        THEN (SELECT count(*)::integer FROM timesheet_allocation_component_lines)
        ELSE 0 END,
      'proposed_semantic_allocation_component_amount',CASE
        WHEN v_semantic_ready_observe_enabled OR v_semantic_ready_publication_enabled
        THEN COALESCE((SELECT round(sum(component_line.amount_ex_vat),2)
          FROM timesheet_allocation_component_lines AS component_line),0)
        ELSE 0 END,
      'proposed_semantic_recovery_amount',CASE
        WHEN v_semantic_ready_observe_enabled OR v_semantic_ready_publication_enabled
        THEN COALESCE((SELECT round(sum(
          CASE WHEN finance_line.is_semantic_recovery
            THEN -least(
              abs(finance_line.signed_amount_ex_vat),
              greatest(finance_line.ordinary_positive_headroom-finance_line.prior_semantic_recovery_amount,0)
            ) ELSE 0 END
        ),2) FROM semantic_finance_case_lines AS finance_line),0)
        ELSE 0 END,
      'proposed_semantic_ready_amount',CASE
        WHEN v_semantic_ready_observe_enabled OR v_semantic_ready_publication_enabled
        THEN COALESCE((SELECT round(sum(component_line.amount_ex_vat),2)
          FROM timesheet_allocation_component_lines AS component_line),0)
          + COALESCE((SELECT round(sum(
            CASE WHEN finance_line.is_semantic_recovery
              THEN -least(
                abs(finance_line.signed_amount_ex_vat),
                greatest(finance_line.ordinary_positive_headroom-finance_line.prior_semantic_recovery_amount,0)
              ) ELSE 0 END
          ),2) FROM semantic_finance_case_lines AS finance_line),0)
        ELSE 0 END,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    )
  );
end;
$function$
;

-- [REPLACES] public.pay_preview_candidate_build_finance_case_baseline(p_context_json jsonb, p_candidate_id uuid)  winner piece A28
CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_finance_case_baseline(p_context_json jsonb, p_candidate_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  -- A28 reconciliation slicing (final slice only; bound to the current Workbench build token)
  v_a28_slice jsonb := CASE WHEN COALESCE(current_setting('cloudtms.pay_workbench_reconcile_slice_v1', true), '') ~ '^\s*\{'
    THEN current_setting('cloudtms.pay_workbench_reconcile_slice_v1', true)::jsonb ELSE NULL::jsonb END;
  v_a28_slice_applies boolean := false;
  v_a28_slice_candidate_id uuid := NULL::uuid;
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
  v_workbench_resolution_session_id uuid := null::uuid;
  v_workbench_resolution_session_id_text text := null::text;
begin
  PERFORM public._imp_debug_audit(
    v_actor_user_id,
    'PAY_PREVIEW_FINANCE_CASE_BASELINE_START',
    jsonb_build_object(
      'candidate_id', p_candidate_id,
      'context_pay_date', p_context_json->>'pay_date',
      'context_pay_week_start', p_context_json->>'pay_week_start'
    ),
    'pay_preview_finance',
    COALESCE(p_candidate_id::text, 'NO_CANDIDATE_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'p_context_json must be a JSON object';
  end if;

  if v_candidate_id is null then
    raise exception 'candidate_id is required';
  end if;

  v_workbench_resolution_session_id_text := NULLIF(BTRIM(COALESCE(
    v_context_json->>'workbench_resolution_session_id',
    v_context_json->>'workbench_session_id',
    v_context_json->>'session_id',
    v_context_json#>>'{workbench,resolution_session_id}',
    v_context_json#>>'{workbench,session_id}',
    ''
  )), '');

  IF v_workbench_resolution_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_workbench_resolution_session_id := v_workbench_resolution_session_id_text::uuid;

    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS resolution_session
    WHERE resolution_session.id = v_workbench_resolution_session_id
      AND UPPER(BTRIM(COALESCE(resolution_session.status, ''))) = 'OPEN'
      AND resolution_session.discarded_at_utc IS NULL
      AND (
        v_candidate_id = ANY(COALESCE(resolution_session.scope_candidate_ids, ARRAY[]::uuid[]))
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_session_scope AS resolution_scope
          WHERE resolution_scope.session_id = resolution_session.id
            AND resolution_scope.candidate_id = v_candidate_id
        )
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_preview_rows AS resolution_preview_row
          WHERE resolution_preview_row.session_id = resolution_session.id
            AND resolution_preview_row.candidate_id = v_candidate_id
        )
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_line_work AS resolution_line_work
          WHERE resolution_line_work.session_id = resolution_session.id
            AND resolution_line_work.candidate_id = v_candidate_id
        )
      );

    IF NOT FOUND THEN
      v_workbench_resolution_session_id := NULL::uuid;
    END IF;
  ELSE
    v_workbench_resolution_session_id := NULL::uuid;
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

  if to_regclass('pg_temp.cand_payee') is null then
    perform public.pay_preview_candidate_build_payee_baseline(v_context_json, v_candidate_id);
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

  drop table if exists pg_temp.finance_case_repaid_wtd, pg_temp.finance_case_recovery_rows_base, pg_temp.manual_debt_recovery_rows, pg_temp.manual_debt_recovery_allocations, pg_temp.manual_debt_recovery_totals, pg_temp.overpayment_recovery_rows, pg_temp.overpayment_recovery_allocations, pg_temp.overpayment_recovery_totals, pg_temp.payment_advance_recovery_rows, pg_temp.payment_advance_recovery_allocations, pg_temp.finance_case_protected_allocations, pg_temp.finance_case_payee_readiness, pg_temp.finance_case_component_rows, pg_temp.finance_case_component_review_rows, pg_temp.finance_case_component_review_rows_effective, pg_temp.finance_case_due_source_amounts, pg_temp.finance_case_component_due_source_base, pg_temp.finance_case_component_due_source_shares, pg_temp.finance_case_component_due_source_allocations, pg_temp.finance_case_component_due_preview_base, pg_temp.finance_case_component_due_preview_allocations, pg_temp.finance_case_taxable_manual_debt_resolution, pg_temp.finance_case_bucket_resolution_overlay, pg_temp.finance_case_nonbucket_resolution_overlay, pg_temp.finance_case_nonbucket_resolution_overlay_alloc, pg_temp.finance_case_taxable_channel_restructure_resolution, pg_temp.finance_case_resolution_owner_state, pg_temp.finance_case_resolution_rollup;

  create temporary table finance_case_repaid_wtd on commit drop as
        select
          nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as finance_case_id,
          round(sum(abs(coalesce(pbi.amount_ex_vat,0))),2) as repaid_wtd_ex
        from public.pay_batch_items pbi
        join public.pay_batch_candidates pbc
          on pbc.id = pbi.pay_batch_candidate_id
        join public.pay_batches pb
          on pb.id = pbc.pay_batch_id
        left join public.pay_bank_transfers pbt
          on pbt.id = pbi.pay_bank_transfer_id
        where coalesce(pbi.is_voided, false) = false
          and pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
          and pbi.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY')
          and pbi.repayment_week_start = v_week_start
          and upper(coalesce(pb.status::text,'')) <> 'CANCELLED'
          and not exists (
            select 1
            from public.pay_payment_correction_items pci_repaid_wtd
            where pci_repaid_wtd.pay_batch_item_id = pbi.id
              and pci_repaid_wtd.status = 'APPLIED'
              and pci_repaid_wtd.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
          )
          and (
            upper(btrim(coalesce(pbc.settlement_status, ''))) = 'SETTLED'
            or pbc.settled_at_utc is not null
            or upper(btrim(coalesce(pbt.status, ''))) = 'COMPLETED'
            or pbt.completed_at_utc is not null
            or exists (
              select 1
              from public.pay_advance_reservations par_repaid_wtd
              where par_repaid_wtd.pay_batch_item_id = pbi.id
                and (
                  upper(btrim(coalesce(par_repaid_wtd.status, ''))) = 'SETTLED'
                  or par_repaid_wtd.settled_at_utc is not null
                )
            )
          )
        group by nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid

  ;


  /* Policy X: recovery headroom is pre-draft live truth, but it must use the
     same central economic-outstanding authority that preview materialisation
     uses.  Raw timesheet deltas whose settled baseline is already exhausted
     must not create funds from which a recovery can be taken. */
  drop table if exists pg_temp.candidate_authoritative_recovery_headroom;

  create temporary table candidate_authoritative_recovery_headroom on commit drop as
        with recovery_timesheet_scope as (
          select
            cr.candidate_id,
            coalesce(
              array_agg(distinct tcr.timesheet_id order by tcr.timesheet_id)
                filter (where tcr.timesheet_id is not null),
              array[]::uuid[]
            ) as timesheet_ids
          from candidate_rollup cr
          left join timesheet_case_rollup_payable tcr
            on tcr.candidate_id = cr.candidate_id
           and coalesce(tcr.is_blocked, false) = false
          group by cr.candidate_id
        )
        select
          rts.candidate_id,
          round(
            coalesce(
              sum(greatest(coalesce(oc.outstanding_ex_vat, 0), 0)),
              0
            ),
            2
          )::numeric(12,2) as authoritative_recovery_headroom_ex
        from recovery_timesheet_scope rts
        left join lateral public._pay_outstanding_components(
          rts.timesheet_ids,
          null::uuid
        ) oc on true
        group by rts.candidate_id

  ;


  -- A28: honour the slice hand-over only inside the exact authoritative Workbench build that set it.
  v_a28_slice_applies := v_a28_slice IS NOT NULL
    AND COALESCE((v_a28_slice->>'final')::boolean, false)
    AND COALESCE(v_a28_slice->>'build_token', '') <> ''
    AND v_a28_slice->>'build_token' = COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token', true), '');
  IF v_a28_slice_applies THEN
    v_a28_slice_candidate_id := (v_a28_slice->>'candidate_id')::uuid;
  END IF;
  IF v_a28_slice_applies THEN
    -- The final slice's own payable Timesheets plus the carried, additive headroom of the earlier slices equals
    -- the headroom an unsliced reconciliation of the whole Candidate computes.
    UPDATE pg_temp.candidate_authoritative_recovery_headroom
    SET authoritative_recovery_headroom_ex = ROUND(authoritative_recovery_headroom_ex
      + COALESCE((v_a28_slice->>'prior_recovery_headroom_ex')::numeric, 0), 2)::numeric(12,2)
    WHERE candidate_id = v_a28_slice_candidate_id;
  END IF;

create temporary table finance_case_recovery_rows_base on commit drop as
        select
          vfcr.finance_case_id,
          vfcr.candidate_id,
          vfcr.case_type,
          vfcr.taxability,
          upper(coalesce(cr.cand_pay_method,'')) as candidate_pay_method,
          round(greatest(coalesce(carh.authoritative_recovery_headroom_ex,0),0),2)::numeric(12,2) as run_earnings_headroom_ex,
          round(
            greatest(
              coalesce(pwb.paid_wtd_before,0)
              + greatest(coalesce(carh.authoritative_recovery_headroom_ex,0),0),
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
        join candidate_authoritative_recovery_headroom carh
          on carh.candidate_id = vfcr.candidate_id
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

  ;

  create temporary table manual_debt_recovery_rows on commit drop as
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

  ;

  create temporary table manual_debt_recovery_allocations on commit drop as
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

  ;

  create temporary table manual_debt_recovery_totals on commit drop as
        select
          mdra.candidate_id,
          round(sum(mdra.protected_recoverable_amount),2)::numeric(12,2) as protected_recoverable_total
        from manual_debt_recovery_allocations mdra
        group by mdra.candidate_id

  ;

  create temporary table overpayment_recovery_rows on commit drop as
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

  ;

  create temporary table overpayment_recovery_allocations on commit drop as
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

  ;

  create temporary table overpayment_recovery_totals on commit drop as
        select
          opra.candidate_id,
          round(sum(opra.protected_recoverable_amount),2)::numeric(12,2) as protected_recoverable_total
        from overpayment_recovery_allocations opra
        group by opra.candidate_id

  ;

  create temporary table payment_advance_recovery_rows on commit drop as
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

  ;

  create temporary table payment_advance_recovery_allocations on commit drop as
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

  ;

  create temporary table finance_case_protected_allocations on commit drop as
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

  ;

  create temporary table finance_case_payee_readiness on commit drop as
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

  ;

  create temporary table finance_case_component_rows on commit drop as
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

  ;


  WITH policy_x_finance_component_rows AS (
    SELECT
      component_row.ctid AS row_ctid,
      policy_key.key_type AS resolved_key_type,
      policy_key.key_value AS resolved_key_value
    FROM finance_case_component_rows AS component_row
    LEFT JOIN LATERAL public._pay_policy_x_resolve_pre_draft_economic_key(
      p_timesheet_id => CASE
        WHEN NULLIF(BTRIM(COALESCE(component_row.source_basis_json->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(component_row.source_basis_json->>'timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END,
      p_live_source_json => COALESCE(component_row.source_basis_json, '{}'::jsonb)
        || jsonb_build_object(
          'finance_case_id', component_row.finance_case_id::text,
          'finance_component_id', CASE WHEN component_row.finance_component_id IS NULL THEN NULL ELSE component_row.finance_component_id::text END,
          'source_family_key', component_row.source_family_key,
          'component_key_type', component_row.component_key_type,
          'component_key_value', component_row.component_key_value,
          'classification', CASE WHEN component_row.classification IS NULL THEN NULL ELSE component_row.classification::text END,
          'item_type', CASE
            WHEN component_row.component_key_type IN ('TS_DAY', 'TS_TOTAL') THEN 'SEGMENT_DELTA'
            WHEN component_row.component_key_type = 'EXPENSE_CODE'
             AND UPPER(BTRIM(COALESCE(component_row.component_key_value, ''))) = 'MILEAGE' THEN 'MILEAGE_DELTA'
            WHEN component_row.component_key_type = 'EXPENSE_CODE' THEN 'EXPENSE_DELTA'
            WHEN component_row.component_key_type = 'ADDITIONAL_CODE' THEN 'EXPENSE_DELTA'
            WHEN component_row.component_key_type = 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
            ELSE 'ADJUSTMENT_DELTA'
          END
        ),
      p_item_type => CASE
        WHEN component_row.component_key_type IN ('TS_DAY', 'TS_TOTAL') THEN 'SEGMENT_DELTA'
        WHEN component_row.component_key_type = 'EXPENSE_CODE'
         AND UPPER(BTRIM(COALESCE(component_row.component_key_value, ''))) = 'MILEAGE' THEN 'MILEAGE_DELTA'
        WHEN component_row.component_key_type = 'EXPENSE_CODE' THEN 'EXPENSE_DELTA'
        WHEN component_row.component_key_type = 'ADDITIONAL_CODE' THEN 'EXPENSE_DELTA'
        WHEN component_row.component_key_type = 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
        ELSE 'ADJUSTMENT_DELTA'
      END,
      p_key_type_hint => NULL::text,
      p_key_value_hint => NULL::text,
      p_work_date => CASE
        WHEN NULLIF(BTRIM(COALESCE(component_row.source_basis_json->>'work_date', component_row.source_basis_json->>'date', '')), '') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN NULLIF(BTRIM(COALESCE(component_row.source_basis_json->>'work_date', component_row.source_basis_json->>'date', '')), '')::date
        ELSE NULL::date
      END
    ) AS policy_key ON true
  )
  UPDATE finance_case_component_rows AS component_row
  SET component_key_type = policy_x_finance_component_rows.resolved_key_type,
      component_key_value = policy_x_finance_component_rows.resolved_key_value,
      current_component_fingerprint = public.pay_finance_component_fingerprint(
        component_row.source_family_key,
        policy_x_finance_component_rows.resolved_key_type,
        policy_x_finance_component_rows.resolved_key_value,
        component_row.classification,
        component_row.source_pay_method,
        component_row.current_target_pay_method,
        COALESCE(component_row.source_basis_json, '{}'::jsonb),
        round(COALESCE(component_row.source_amount, 0), 2),
        CASE
          WHEN COALESCE(component_row.saved_resolution_payload_json->>'relevant_erni_pct', component_row.saved_resolution_result_json->>'relevant_erni_pct', '') ~ '^-?\d+(\.\d+)?$'
            THEN COALESCE(component_row.saved_resolution_payload_json->>'relevant_erni_pct', component_row.saved_resolution_result_json->>'relevant_erni_pct')::numeric
          ELSE v_erni_pct
        END,
        COALESCE(component_row.saved_resolution_payload_json, component_row.saved_resolution_result_json, '{}'::jsonb)
      )
  FROM policy_x_finance_component_rows
  WHERE component_row.ctid = policy_x_finance_component_rows.row_ctid
    AND policy_x_finance_component_rows.resolved_key_type IS NOT NULL
    AND policy_x_finance_component_rows.resolved_key_value IS NOT NULL;

  create temporary table finance_case_component_review_rows on commit drop as
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

  ;

  create temporary table finance_case_component_review_rows_effective on commit drop as
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
    and fccr.case_type <> 'MANUAL_DEBT_ADJUSTMENT'
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
                and fccr.case_type <> 'MANUAL_DEBT_ADJUSTMENT'
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

  ;


  IF v_workbench_resolution_session_id IS NOT NULL THEN
    CREATE TEMPORARY TABLE finance_case_bucket_resolution_overlay ON COMMIT DROP AS
    WITH stored_resolution_components AS (
      SELECT
        stored_resolution.id AS resolution_id,
        stored_resolution.case_key,
        stored_resolution.timesheet_id,
        stored_resolution.source_basis_fingerprint,
        stored_resolution.source_family_key,
        stored_resolution.bucket_code,
        stored_resolution.component_key_type,
        stored_resolution.component_key_value,
        stored_resolution.payload_json,
        stored_resolution.updated_at_utc,
        bucket_element.ordinality::integer AS bucket_ordinal,
        bucket_element.value AS bucket_json
      FROM public.banking_pay_workbench_session_case_resolutions AS stored_resolution
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(stored_resolution.payload_json->'bucket_resolutions') = 'array'
            THEN stored_resolution.payload_json->'bucket_resolutions'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS bucket_element(value, ordinality)
      WHERE stored_resolution.session_id = v_workbench_resolution_session_id
        AND stored_resolution.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(stored_resolution.resolution_family, ''))) = 'BUCKETED'
        AND jsonb_typeof(bucket_element.value) = 'object'
    )
    SELECT DISTINCT ON (
      resolved_component.finance_case_id,
      resolved_component.source_basis_fingerprint,
      resolved_component.source_family_key,
      COALESCE(resolved_component.bucket_code, ''),
      resolved_component.component_key_type,
      resolved_component.component_key_value
    )
      resolved_component.resolution_id,
      resolved_component.finance_case_id,
      resolved_component.case_key,
      resolved_component.timesheet_id,
      resolved_component.source_basis_fingerprint,
      resolved_component.source_family_key,
      resolved_component.bucket_code,
      resolved_component.component_key_type,
      resolved_component.component_key_value,
      resolved_component.payload_json,
      resolved_component.bucket_json,
      resolved_component.classification,
      resolved_component.source_pay_method,
      resolved_component.target_pay_method,
      resolved_component.source_units,
      resolved_component.source_rate,
      resolved_component.source_charge_rate,
      resolved_component.resolution_mode,
      resolved_component.target_rate
    FROM (
      SELECT
        stored_component.resolution_id,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(stored_component.payload_json->>'finance_case_id', stored_component.bucket_json->>'finance_case_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(stored_component.payload_json->>'finance_case_id', stored_component.bucket_json->>'finance_case_id', '')), '')::uuid
          WHEN NULLIF(BTRIM(COALESCE(stored_component.case_key, '')), '') ~* '^finance:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN substring(NULLIF(BTRIM(COALESCE(stored_component.case_key, '')), '') from 9)::uuid
          ELSE NULL::uuid
        END AS finance_case_id,
        stored_component.case_key,
        stored_component.timesheet_id,
        COALESCE(
          NULLIF(BTRIM(COALESCE(stored_component.source_basis_fingerprint, '')), ''),
          NULLIF(BTRIM(COALESCE(stored_component.bucket_json->>'source_basis_fingerprint', '')), ''),
          CASE
            WHEN jsonb_typeof(stored_component.bucket_json->'source_basis_json') = 'object'
              THEN md5((stored_component.bucket_json->'source_basis_json')::text)
            ELSE NULL::text
          END
        ) AS source_basis_fingerprint,
        NULLIF(BTRIM(COALESCE(stored_component.source_family_key, stored_component.bucket_json->>'source_family_key', '')), '') AS source_family_key,
        NULLIF(UPPER(BTRIM(COALESCE(stored_component.bucket_code, stored_component.bucket_json->>'bucket_code', ''))), '') AS bucket_code,
        NULLIF(UPPER(BTRIM(COALESCE(stored_component.component_key_type, stored_component.bucket_json->>'component_key_type', ''))), '') AS component_key_type,
        NULLIF(BTRIM(COALESCE(stored_component.component_key_value, stored_component.bucket_json->>'component_key_value', '')), '') AS component_key_value,
        stored_component.payload_json,
        stored_component.bucket_json,
        UPPER(BTRIM(COALESCE(stored_component.bucket_json->>'classification', ''))) AS classification,
        UPPER(BTRIM(COALESCE(stored_component.bucket_json->>'source_pay_method', ''))) AS source_pay_method,
        UPPER(BTRIM(COALESCE(
          stored_component.bucket_json->>'target_pay_method',
          stored_component.bucket_json->>'current_target_pay_method',
          stored_component.payload_json->>'target_pay_method',
          stored_component.payload_json->>'current_target_pay_method',
          ''
        ))) AS target_pay_method,
        CASE
          WHEN COALESCE(stored_component.bucket_json->>'source_units', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN round((stored_component.bucket_json->>'source_units')::numeric, 6)
          ELSE NULL::numeric
        END AS source_units,
        CASE
          WHEN COALESCE(stored_component.bucket_json->>'source_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN round((stored_component.bucket_json->>'source_rate')::numeric, 6)
          ELSE NULL::numeric
        END AS source_rate,
        CASE
          WHEN COALESCE(stored_component.bucket_json->>'source_charge_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN round((stored_component.bucket_json->>'source_charge_rate')::numeric, 6)
          ELSE NULL::numeric
        END AS source_charge_rate,
        UPPER(BTRIM(COALESCE(stored_component.bucket_json->>'resolution_mode', stored_component.payload_json->>'resolution_mode', ''))) AS resolution_mode,
        CASE
          WHEN COALESCE(stored_component.bucket_json->>'target_rate', stored_component.payload_json->>'target_rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN round(COALESCE(stored_component.bucket_json->>'target_rate', stored_component.payload_json->>'target_rate')::numeric, 2)
          ELSE NULL::numeric
        END AS target_rate,
        stored_component.updated_at_utc,
        stored_component.bucket_ordinal
      FROM stored_resolution_components AS stored_component
    ) AS resolved_component
    WHERE resolved_component.finance_case_id IS NOT NULL
      AND resolved_component.source_basis_fingerprint IS NOT NULL
      AND resolved_component.source_family_key IS NOT NULL
      AND resolved_component.component_key_type IS NOT NULL
      AND resolved_component.component_key_value IS NOT NULL
      AND resolved_component.resolution_mode IN ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_REPLACEMENT_RATE')
      AND resolved_component.target_rate IS NOT NULL
      AND resolved_component.target_rate >= 0
    ORDER BY
      resolved_component.finance_case_id,
      resolved_component.source_basis_fingerprint,
      resolved_component.source_family_key,
      COALESCE(resolved_component.bucket_code, ''),
      resolved_component.component_key_type,
      resolved_component.component_key_value,
      resolved_component.updated_at_utc DESC,
      resolved_component.resolution_id DESC,
      resolved_component.bucket_ordinal DESC;

    UPDATE finance_case_component_review_rows_effective AS effective_component
    SET approved_resolution_mode = resolution_overlay.resolution_mode,
        approved_target_rate = resolution_overlay.target_rate,
        saved_target_pay_method = COALESCE(NULLIF(resolution_overlay.target_pay_method, ''), effective_component.current_target_pay_method),
        saved_resolution_mode = resolution_overlay.resolution_mode::public.pay_finance_component_resolution_mode_enum,
        saved_resolution_payload_json = jsonb_strip_nulls(
          COALESCE(resolution_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'resolution_family', 'BUCKETED',
            'resolution_mode', resolution_overlay.resolution_mode,
            'target_pay_method', COALESCE(NULLIF(resolution_overlay.target_pay_method, ''), effective_component.current_target_pay_method),
            'target_units', round(effective_component.source_units, 6),
            'target_rate', round(resolution_overlay.target_rate, 2)
          )
        ),
        saved_resolution_result_json = jsonb_strip_nulls(
          COALESCE(resolution_overlay.bucket_json, '{}'::jsonb)
          || jsonb_build_object(
            'target_units', round(effective_component.source_units, 6),
            'target_rate', round(resolution_overlay.target_rate, 2),
            'target_amount_ex_vat', round(effective_component.source_units * resolution_overlay.target_rate, 2),
            'target_pay_ex_vat', round(effective_component.source_units * resolution_overlay.target_rate, 2),
            'target_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_margin_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate), 2)
            END,
            'margin_delta_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL OR effective_component.source_margin_ex_vat IS NULL THEN NULL
              ELSE round(
                (effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate))
                - effective_component.source_margin_ex_vat,
                2
              )
            END
          )
        ),
        resolution_fingerprint = effective_component.current_component_fingerprint,
        is_resolution_stale = false,
        stale_reason = NULL,
        requires_resolution = false,
        case_resolution_satisfied_now_component = true,
        suggestion_provenance = 'PREVIEW_CASE_RESOLUTION',
        is_fresh_suggested_resolution = false,
        is_reusable_saved_resolution = true,
        is_stale_saved_resolution = false,
        suggested_resolution_payload_json = jsonb_strip_nulls(
          COALESCE(resolution_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'resolution_family', 'BUCKETED',
            'resolution_mode', resolution_overlay.resolution_mode,
            'target_pay_method', COALESCE(NULLIF(resolution_overlay.target_pay_method, ''), effective_component.current_target_pay_method),
            'target_units', round(effective_component.source_units, 6),
            'target_rate', round(resolution_overlay.target_rate, 2)
          )
        ),
        suggested_resolution_result_json = jsonb_strip_nulls(
          COALESCE(resolution_overlay.bucket_json, '{}'::jsonb)
          || jsonb_build_object(
            'target_units', round(effective_component.source_units, 6),
            'replacement_rate', round(resolution_overlay.target_rate, 2),
            'target_rate', round(resolution_overlay.target_rate, 2),
            'target_amount_ex_vat', round(effective_component.source_units * resolution_overlay.target_rate, 2),
            'target_pay_ex_vat', round(effective_component.source_units * resolution_overlay.target_rate, 2),
            'target_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_margin_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate), 2)
            END,
            'margin_delta_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL OR effective_component.source_margin_ex_vat IS NULL THEN NULL
              ELSE round(
                (effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate))
                - effective_component.source_margin_ex_vat,
                2
              )
            END
          )
        ),
        target_pay_ex_vat = round(effective_component.source_units * resolution_overlay.target_rate, 2),
        target_charge_ex_vat = effective_component.source_charge_component_ex_vat,
        target_margin_ex_vat = CASE
          WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
          ELSE round(effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate), 2)
        END,
        margin_delta_ex_vat = CASE
          WHEN effective_component.source_charge_component_ex_vat IS NULL OR effective_component.source_margin_ex_vat IS NULL THEN NULL
          ELSE round(
            (effective_component.source_charge_component_ex_vat - (effective_component.source_units * resolution_overlay.target_rate))
            - effective_component.source_margin_ex_vat,
            2
          )
        END,
        suggestion_explanation_text = 'This component uses the current Workbench Case Resolution selected for this finance case.'
    FROM finance_case_bucket_resolution_overlay AS resolution_overlay
    WHERE effective_component.candidate_id = v_candidate_id
      AND effective_component.finance_case_id = resolution_overlay.finance_case_id
      AND resolution_overlay.case_key = ('finance:' || effective_component.finance_case_id::text)
      AND effective_component.source_family_key = resolution_overlay.source_family_key
      AND md5(COALESCE(effective_component.source_basis_json::text, '{}'::text)) = resolution_overlay.source_basis_fingerprint
      AND COALESCE(NULLIF(UPPER(BTRIM(COALESCE(effective_component.source_basis_json->>'bucket_code', ''))), ''), '') = COALESCE(resolution_overlay.bucket_code, '')
      AND effective_component.component_key_type = resolution_overlay.component_key_type
      AND effective_component.component_key_value = resolution_overlay.component_key_value
      AND round(effective_component.source_units, 6) = resolution_overlay.source_units
      AND round(effective_component.source_rate, 6) = resolution_overlay.source_rate
      AND round(effective_component.source_charge_rate, 6) = resolution_overlay.source_charge_rate
      AND (resolution_overlay.classification = '' OR UPPER(effective_component.classification::text) = resolution_overlay.classification)
      AND (resolution_overlay.source_pay_method = '' OR UPPER(BTRIM(COALESCE(effective_component.source_pay_method, ''))) = resolution_overlay.source_pay_method)
      AND (resolution_overlay.target_pay_method = '' OR UPPER(BTRIM(COALESCE(effective_component.current_target_pay_method, ''))) = resolution_overlay.target_pay_method)
      AND effective_component.is_actionable_resolution_row = true;

    CREATE TEMPORARY TABLE finance_case_nonbucket_resolution_overlay ON COMMIT DROP AS
    SELECT DISTINCT ON (resolved_nonbucket.finance_case_id)
      resolved_nonbucket.resolution_id,
      resolved_nonbucket.finance_case_id,
      resolved_nonbucket.case_key,
      resolved_nonbucket.payload_json,
      resolved_nonbucket.resolution_mode,
      resolved_nonbucket.target_amount_ex_vat
    FROM (
      SELECT
        stored_resolution.id AS resolution_id,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(stored_resolution.payload_json->>'finance_case_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(stored_resolution.payload_json->>'finance_case_id', '')), '')::uuid
          WHEN NULLIF(BTRIM(COALESCE(stored_resolution.case_key, '')), '') ~* '^finance:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN substring(NULLIF(BTRIM(COALESCE(stored_resolution.case_key, '')), '') from 9)::uuid
          ELSE NULL::uuid
        END AS finance_case_id,
        stored_resolution.case_key,
        stored_resolution.payload_json,
        UPPER(BTRIM(COALESCE(stored_resolution.payload_json->>'resolution_mode', ''))) AS resolution_mode,
        CASE
          WHEN COALESCE(stored_resolution.payload_json->>'target_amount_ex_vat', stored_resolution.payload_json->>'target_amount', stored_resolution.payload_json->>'amount_ex_vat', stored_resolution.payload_json->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN round(COALESCE(stored_resolution.payload_json->>'target_amount_ex_vat', stored_resolution.payload_json->>'target_amount', stored_resolution.payload_json->>'amount_ex_vat', stored_resolution.payload_json->>'amount')::numeric, 2)
          ELSE NULL::numeric
        END AS target_amount_ex_vat,
        stored_resolution.updated_at_utc
      FROM public.banking_pay_workbench_session_case_resolutions AS stored_resolution
      WHERE stored_resolution.session_id = v_workbench_resolution_session_id
        AND stored_resolution.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(stored_resolution.resolution_family, ''))) = 'NON_BUCKET'
    ) AS resolved_nonbucket
    WHERE resolved_nonbucket.finance_case_id IS NOT NULL
      AND resolved_nonbucket.resolution_mode IN ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_AMOUNT')
      AND resolved_nonbucket.target_amount_ex_vat IS NOT NULL
      AND resolved_nonbucket.target_amount_ex_vat >= 0
    ORDER BY
      resolved_nonbucket.finance_case_id,
      resolved_nonbucket.updated_at_utc DESC,
      resolved_nonbucket.resolution_id DESC;

    CREATE TEMPORARY TABLE finance_case_nonbucket_resolution_overlay_alloc ON COMMIT DROP AS
    WITH target_components AS (
      SELECT
        effective_component.ctid AS row_ctid,
        effective_component.finance_case_id,
        effective_component.finance_component_id,
        resolution_overlay.resolution_id,
        resolution_overlay.payload_json,
        resolution_overlay.resolution_mode,
        resolution_overlay.target_amount_ex_vat,
        round(coalesce(effective_component.remaining_source_amount, 0), 2) AS remaining_source_amount,
        round(sum(coalesce(effective_component.remaining_source_amount, 0)) OVER (PARTITION BY effective_component.finance_case_id), 2) AS total_remaining_source_amount,
        row_number() OVER (PARTITION BY effective_component.finance_case_id ORDER BY effective_component.finance_component_id) AS component_ord,
        count(*) OVER (PARTITION BY effective_component.finance_case_id) AS component_count
      FROM finance_case_component_review_rows_effective AS effective_component
      JOIN finance_case_nonbucket_resolution_overlay AS resolution_overlay
        ON resolution_overlay.finance_case_id = effective_component.finance_case_id
       AND resolution_overlay.case_key = ('finance:' || effective_component.finance_case_id::text)
      WHERE effective_component.candidate_id = v_candidate_id
        AND effective_component.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
        AND effective_component.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND round(coalesce(effective_component.remaining_source_amount, 0), 2) > 0
    ), preliminary_components AS (
      SELECT
        target_components.*,
        round(
          CASE
            WHEN coalesce(target_components.target_amount_ex_vat, 0) = 0 THEN 0::numeric
            WHEN coalesce(target_components.total_remaining_source_amount, 0) = 0 THEN 0::numeric
            WHEN target_components.component_count = 1 THEN target_components.target_amount_ex_vat
            WHEN target_components.component_ord < target_components.component_count THEN target_components.target_amount_ex_vat * target_components.remaining_source_amount / nullif(target_components.total_remaining_source_amount, 0)
            ELSE 0::numeric
          END,
          2
        ) AS preliminary_target_amount_ex_vat
      FROM target_components
    )
    SELECT
      preliminary_components.row_ctid,
      preliminary_components.finance_case_id,
      preliminary_components.finance_component_id,
      preliminary_components.resolution_id,
      preliminary_components.payload_json,
      preliminary_components.resolution_mode,
      round(
        CASE
          WHEN preliminary_components.component_count = 1 THEN preliminary_components.target_amount_ex_vat
          WHEN preliminary_components.component_ord < preliminary_components.component_count THEN preliminary_components.preliminary_target_amount_ex_vat
          ELSE preliminary_components.target_amount_ex_vat - coalesce(
            sum(preliminary_components.preliminary_target_amount_ex_vat) OVER (
              PARTITION BY preliminary_components.finance_case_id
              ORDER BY preliminary_components.component_ord
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
          )
        END,
        2
      ) AS allocated_target_amount_ex_vat
    FROM preliminary_components;

    UPDATE finance_case_component_review_rows_effective AS effective_component
    SET approved_nonbucket_resolution_mode = nonbucket_overlay.resolution_mode,
        approved_nonbucket_target_amount_ex_vat = nonbucket_overlay.allocated_target_amount_ex_vat,
        saved_target_pay_method = effective_component.current_target_pay_method,
        saved_resolution_mode = nonbucket_overlay.resolution_mode::public.pay_finance_component_resolution_mode_enum,
        saved_resolution_payload_json = jsonb_strip_nulls(
          COALESCE(nonbucket_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'resolution_family', 'NON_BUCKET',
            'resolution_mode', nonbucket_overlay.resolution_mode,
            'target_pay_method', effective_component.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2),
            'target_amount_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2)
          )
        ),
        saved_resolution_result_json = jsonb_strip_nulls(
          COALESCE(nonbucket_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'target_pay_method', effective_component.current_target_pay_method,
            'target_amount_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2),
            'target_pay_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2),
            'source_pay_ex_vat', round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2),
            'source_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_margin_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(effective_component.source_charge_component_ex_vat - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
            END,
            'margin_delta_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2) - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
            END
          )
        ),
        resolution_fingerprint = effective_component.current_component_fingerprint,
        is_resolution_stale = false,
        stale_reason = NULL,
        requires_resolution = false,
        case_resolution_satisfied_now_component = true,
        suggestion_provenance = 'PREVIEW_CASE_RESOLUTION',
        is_fresh_suggested_resolution = false,
        is_reusable_saved_resolution = true,
        is_stale_saved_resolution = false,
        suggested_resolution_payload_json = jsonb_strip_nulls(
          COALESCE(nonbucket_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'resolution_family', 'NON_BUCKET',
            'resolution_mode', nonbucket_overlay.resolution_mode,
            'target_pay_method', effective_component.current_target_pay_method,
            'applied_basis_source_amount_ex_vat', round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2),
            'target_amount_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2)
          )
        ),
        suggested_resolution_result_json = jsonb_strip_nulls(
          COALESCE(nonbucket_overlay.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'target_pay_method', effective_component.current_target_pay_method,
            'target_amount_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2),
            'target_pay_ex_vat', round(nonbucket_overlay.allocated_target_amount_ex_vat, 2),
            'source_pay_ex_vat', round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2),
            'source_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_charge_ex_vat', effective_component.source_charge_component_ex_vat,
            'target_margin_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(effective_component.source_charge_component_ex_vat - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
            END,
            'margin_delta_ex_vat', CASE
              WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
              ELSE round(round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2) - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
            END
          )
        ),
        target_pay_ex_vat = round(nonbucket_overlay.allocated_target_amount_ex_vat, 2),
        target_charge_ex_vat = effective_component.source_charge_component_ex_vat,
        target_margin_ex_vat = CASE
          WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
          ELSE round(effective_component.source_charge_component_ex_vat - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
        END,
        margin_delta_ex_vat = CASE
          WHEN effective_component.source_charge_component_ex_vat IS NULL THEN NULL
          ELSE round(round(coalesce(effective_component.remaining_source_amount, effective_component.source_amount, 0), 2) - nonbucket_overlay.allocated_target_amount_ex_vat, 2)
        END,
        suggestion_explanation_text = 'This finance case uses the current Workbench non-bucket Case Resolution selected for this session.'
    FROM finance_case_nonbucket_resolution_overlay_alloc AS nonbucket_overlay
    WHERE effective_component.ctid = nonbucket_overlay.row_ctid;
  END IF;

  create temporary table finance_case_due_source_amounts on commit drop as
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
                when vfcr.case_type = 'UNDERPAYMENT' then case
                  when vfcr.lifecycle_status_display in ('Paid','Cancelled') then 0::numeric
                  else greatest(coalesce(vfcr.outstanding_amount, vfcr.original_amount, 0) - coalesce(vfcr.active_reserved_amount,0), 0)
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

  ;

  create temporary table finance_case_component_due_source_base on commit drop as
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

  ;

  create temporary table finance_case_component_due_source_shares on commit drop as
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

  ;

  create temporary table finance_case_component_due_source_allocations on commit drop as
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

  ;

  create temporary table finance_case_component_due_preview_base on commit drop as
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

  ;

  create temporary table finance_case_component_due_preview_allocations on commit drop as
        select
          fcdpb.finance_case_id,
          fcdpb.finance_component_id,
          round(fcdpb.preliminary_preview_due_amount_ex_vat, 2) as allocated_preview_due_amount_ex_vat
        from finance_case_component_due_preview_base fcdpb

  ;

  create temporary table finance_case_taxable_channel_restructure_resolution on commit drop as
        with component_base as (
          select
            vfcr.finance_case_id,
            vfcr.case_type,
            vfcr.candidate_id,
            vfcr.client_id,
            vfcr.next_due_week_start,
            vfcr.weekly_due,
            vfcr.weeks_total,
            vfcr.outstanding_amount,
            vfcr.schedule_json,
            cp.cand_pay_method as target_pay_method,
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
            fccr.current_component_fingerprint,
            fccr.is_stale_saved_resolution,
            fccr.is_reusable_saved_resolution,
            fccr.stale_reason,
            (
              fccr.finance_component_id is not null
              and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              and upper(coalesce(fccr.source_pay_method,'')) in ('PAYE','UMBRELLA')
              and upper(coalesce(fccr.current_target_pay_method,'')) in ('PAYE','UMBRELLA')
              and upper(coalesce(fccr.source_pay_method,'')) <> upper(coalesce(fccr.current_target_pay_method,''))
              and round(coalesce(fccr.remaining_source_amount,0),2) > 0
            ) as has_taxable_channel_mismatch,
            (
              fccr.finance_component_id is not null
              and fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              and upper(coalesce(fccr.source_pay_method,'')) in ('PAYE','UMBRELLA')
              and upper(coalesce(fccr.current_target_pay_method,'')) in ('PAYE','UMBRELLA')
              and upper(coalesce(fccr.source_pay_method,'')) <> upper(coalesce(fccr.current_target_pay_method,''))
              and round(coalesce(fccr.remaining_source_amount,0),2) > 0
              and (
                coalesce(fccr.is_stale_saved_resolution,false) = true
                or nullif(btrim(coalesce(fccr.saved_target_pay_method,'')), '') is null
                or upper(coalesce(fccr.saved_target_pay_method,'')) <> upper(coalesce(fccr.current_target_pay_method,''))
                or (
                  fccr.resolution_fingerprint is not null
                  and fccr.current_component_fingerprint is not null
                  and fccr.resolution_fingerprint is distinct from fccr.current_component_fingerprint
                )
              )
            ) as requires_component_restructure
          from finance_case_baseline_scope vfcr
          join cand_payee cp
            on cp.candidate_id = vfcr.candidate_id
          join finance_case_component_review_rows_effective fccr
            on fccr.finance_case_id = vfcr.finance_case_id
          where vfcr.case_type in (
            'OVERPAYMENT'::public.pay_finance_case_type_enum,
            'UNDERPAYMENT'::public.pay_finance_case_type_enum,
            'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
            'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
          )
            and fccr.finance_component_id is not null
            and round(coalesce(fccr.remaining_source_amount,0),2) > 0
        ),
        component_projected as (
          select
            cb.*,
            round(
              case
                when cb.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
                 and cb.source_pay_method = 'UMBRELLA'
                 and cb.current_target_pay_method = 'PAYE'
                  then public._pay_convert_umbrella_to_paye_ex(round(coalesce(cb.remaining_source_amount,0),2), v_erni_pct)
                when cb.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
                 and cb.source_pay_method = 'PAYE'
                 and cb.current_target_pay_method = 'UMBRELLA'
                  then coalesce((public._pay_convert_paye_to_umbrella(round(coalesce(cb.remaining_source_amount,0),2), v_erni_pct, v_vat_rate_pct, coalesce(cb.umb_vat_chargeable,false))->>'ex')::numeric, 0)
                else round(coalesce(cb.remaining_source_amount,0),2)
              end,
              2
            ) as target_remaining_ex
          from component_base cb
        ),
        component_projected_vat as (
          select
            cpj.*,
            round(
              case
                when cpj.current_target_pay_method = 'UMBRELLA'
                  then coalesce((public._pay_umbrella_vat_calc(round(coalesce(cpj.target_remaining_ex,0),2), v_vat_rate_pct, coalesce(cpj.umb_vat_chargeable,false))->>'vat')::numeric, 0)
                else 0
              end,
              2
            ) as target_remaining_vat,
            round(
              coalesce(cpj.target_remaining_ex,0)
              + case
                  when cpj.current_target_pay_method = 'UMBRELLA'
                    then coalesce((public._pay_umbrella_vat_calc(round(coalesce(cpj.target_remaining_ex,0),2), v_vat_rate_pct, coalesce(cpj.umb_vat_chargeable,false))->>'vat')::numeric, 0)
                  else 0
                end,
              2
            ) as target_remaining_inc
          from component_projected cpj
        ),
        case_shape as (
          select
            cpv.finance_case_id,
            (array_agg(cpv.case_type))[1] as case_type,
            (array_remove(array_agg(cpv.candidate_id), null::uuid))[1] as candidate_id,
            (array_remove(array_agg(cpv.client_id), null::uuid))[1] as client_id,
            max(cpv.next_due_week_start) as next_due_week_start,
            max(cpv.weekly_due) as weekly_due,
            max(cpv.weeks_total) as weeks_total,
            max(cpv.outstanding_amount) as outstanding_amount,
            coalesce((jsonb_agg(cpv.schedule_json order by cpv.finance_component_id) filter (where cpv.schedule_json is not null))->0, '[]'::jsonb) as schedule_json,
            max(cpv.target_pay_method) as target_pay_method,
            count(*)::int as open_component_count,
            count(*) filter (where cpv.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum)::int as open_taxable_count,
            count(*) filter (where cpv.classification in ('REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum, 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum))::int as open_fixed_count,
            bool_or(cpv.has_taxable_channel_mismatch) as has_taxable_channel_mismatch,
            bool_or(cpv.requires_component_restructure) as requires_restructure,
            bool_or(coalesce(cpv.is_reusable_saved_resolution,false)) as is_reusable_saved_resolution,
            bool_or(coalesce(cpv.is_stale_saved_resolution,false)) as is_stale_saved_resolution,
            round(coalesce(sum(cpv.remaining_source_amount),0),2) as existing_source_total_ex,
            round(coalesce(sum(cpv.remaining_source_amount) filter (where cpv.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum),0),2) as existing_taxable_source_total_ex,
            round(coalesce(sum(cpv.target_remaining_ex),0),2) as suggested_target_total_ex,
            round(coalesce(sum(cpv.target_remaining_vat),0),2) as suggested_target_total_vat,
            round(coalesce(sum(cpv.target_remaining_inc),0),2) as suggested_target_total_inc,
            round(coalesce(sum(cpv.target_remaining_ex) filter (where cpv.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum),0),2) as suggested_taxable_target_total_ex,
            bool_or(coalesce(cpv.umb_vat_chargeable,false)) as umbrella_vat_chargeable,
            coalesce(
              jsonb_agg(
                jsonb_strip_nulls(jsonb_build_object(
                  'finance_component_id', cpv.finance_component_id::text,
                  'source_family_key', cpv.source_family_key,
                  'component_key_type', cpv.component_key_type,
                  'component_key_value', cpv.component_key_value,
                  'classification', cpv.classification::text,
                  'source_pay_method', cpv.source_pay_method,
                  'target_pay_method', cpv.current_target_pay_method,
                  'source_remaining_amount_ex_vat', round(coalesce(cpv.remaining_source_amount,0),2),
                  'target_remaining_amount_ex_vat', round(coalesce(cpv.target_remaining_ex,0),2),
                  'target_remaining_amount_vat', round(coalesce(cpv.target_remaining_vat,0),2),
                  'target_remaining_amount_inc_vat', round(coalesce(cpv.target_remaining_inc,0),2),
                  'requires_component_conversion', cpv.has_taxable_channel_mismatch,
                  'requires_restructure', cpv.requires_component_restructure,
                  'source_units', cpv.source_units,
                  'source_rate', cpv.source_rate,
                  'source_charge_rate', cpv.source_charge_rate,
                  'source_charge_ex_vat', cpv.source_charge_ex_vat,
                  'saved_target_pay_method', cpv.saved_target_pay_method,
                  'saved_resolution_mode', case when cpv.saved_resolution_mode is null then null else cpv.saved_resolution_mode::text end,
                  'is_reusable_saved_resolution', cpv.is_reusable_saved_resolution,
                  'is_stale_saved_resolution', cpv.is_stale_saved_resolution,
                  'stale_reason', cpv.stale_reason
                ))
                order by cpv.classification::text, cpv.component_key_type, cpv.component_key_value, cpv.finance_component_id
              ),
              '[]'::jsonb
            ) as component_breakdown_json
          from component_projected_vat cpv
          group by cpv.finance_case_id
        ),
        case_with_schedule as (
          select
            cs.*,
            coalesce(cs.next_due_week_start, v_week_start) as restructure_start_week_start,
            coalesce(
              (
                select count(*)::integer
                from jsonb_array_elements(coalesce(cs.schedule_json,'[]'::jsonb)) sched(schedule_entry)
                where jsonb_typeof(sched.schedule_entry) = 'object'
                  and coalesce(sched.schedule_entry->>'week_start','') ~ '^\d{4}-\d{2}-\d{2}$'
                  and (cs.next_due_week_start is null or (sched.schedule_entry->>'week_start')::date >= cs.next_due_week_start)
                  and coalesce(sched.schedule_entry->>'amount','') ~ '^-?\d+(\.\d+)?$'
                  and abs((sched.schedule_entry->>'amount')::numeric) > 0
              ),
              case
                when coalesce(cs.weekly_due,0) > 0 and coalesce(cs.existing_source_total_ex,0) > 0 then greatest(ceil(cs.existing_source_total_ex / cs.weekly_due)::integer, 1)
                when coalesce(cs.weeks_total,0) > 0 then cs.weeks_total
                else 1
              end
            ) as existing_weeks_remaining,
            coalesce(
              nullif(round(coalesce(cs.weekly_due,0),2),0),
              (
                select round(abs((sched2.schedule_entry->>'amount')::numeric),2)
                from jsonb_array_elements(coalesce(cs.schedule_json,'[]'::jsonb)) sched2(schedule_entry)
                where jsonb_typeof(sched2.schedule_entry) = 'object'
                  and coalesce(sched2.schedule_entry->>'week_start','') ~ '^\d{4}-\d{2}-\d{2}$'
                  and (cs.next_due_week_start is null or (sched2.schedule_entry->>'week_start')::date >= cs.next_due_week_start)
                  and coalesce(sched2.schedule_entry->>'amount','') ~ '^-?\d+(\.\d+)?$'
                  and abs((sched2.schedule_entry->>'amount')::numeric) > 0
                order by (sched2.schedule_entry->>'week_start')::date asc
                limit 1
              ),
              round(ceil((cs.existing_source_total_ex / greatest(coalesce(cs.weeks_total,1),1)) * 100) / 100, 2)
            ) as existing_weekly_due,
            coalesce(
              (
                select round(abs((sched3.schedule_entry->>'amount')::numeric),2)
                from jsonb_array_elements(coalesce(cs.schedule_json,'[]'::jsonb)) sched3(schedule_entry)
                where jsonb_typeof(sched3.schedule_entry) = 'object'
                  and coalesce(sched3.schedule_entry->>'week_start','') ~ '^\d{4}-\d{2}-\d{2}$'
                  and (cs.next_due_week_start is null or (sched3.schedule_entry->>'week_start')::date >= cs.next_due_week_start)
                  and coalesce(sched3.schedule_entry->>'amount','') ~ '^-?\d+(\.\d+)?$'
                  and abs((sched3.schedule_entry->>'amount')::numeric) > 0
                order by (sched3.schedule_entry->>'week_start')::date desc
                limit 1
              ),
              0::numeric
            ) as existing_final_week_amount
          from case_shape cs
        ),
        open_batch as (
          select
            cws.finance_case_id,
            exists (
              select 1
              from public.pay_batch_items pbi_open
              join public.pay_batch_candidates pbc_open
                on pbc_open.id = pbi_open.pay_batch_candidate_id
              join public.pay_batches pb_open
                on pb_open.id = pbc_open.pay_batch_id
              where coalesce(pbi_open.is_voided,false) = false
                and pb_open.cancelled_at_utc is null
                and upper(coalesce(pb_open.status::text,'')) not in ('CANCELLED','COMPLETED','SETTLED')
                and (
                  pbi_open.finance_case_id = cws.finance_case_id
                  or exists (
                    select 1
                    from public.pay_finance_case_components pfc_open
                    where pfc_open.finance_case_id = cws.finance_case_id
                      and pfc_open.closed_at_utc is null
                      and pfc_open.id = pbi_open.finance_component_id
                  )
                )
            ) as has_open_batch_item
          from case_with_schedule cws
        )
        select
          cws.finance_case_id,
          coalesce(cws.requires_restructure,false) as has_dedicated_resolution_payload,
          coalesce(cws.requires_restructure,false) as use_dedicated_blocker,
          case
            when coalesce(cws.requires_restructure,false) = true then jsonb_strip_nulls(jsonb_build_object(
              'resolution_kind', 'TAXABLE_CHANNEL_RESTRUCTURE',
              'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',
              'resolution_action_label', 'Suggested Restructure',
              'modal_title', 'Finance Restructure',
              'finance_case_id', cws.finance_case_id::text,
              'case_type', cws.case_type::text,
              'source_method', (
                select coalesce(jsonb_agg(distinct cb.source_pay_method), '[]'::jsonb)
                from component_base cb
                where cb.finance_case_id = cws.finance_case_id
                  and cb.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              ),
              'target_method', cws.target_pay_method,
              'can_apply', coalesce(ob.has_open_batch_item,false) = false,
              'open_batch_blocked_reason', case when coalesce(ob.has_open_batch_item,false) = true then 'CASE_ALREADY_IN_OPEN_BATCH' else null end,
              'existing_arrangement', jsonb_build_object(
                'source_remaining_balance_ex_vat', cws.existing_source_total_ex,
                'taxable_source_remaining_balance_ex_vat', cws.existing_taxable_source_total_ex,
                'weekly_due', round(coalesce(cws.existing_weekly_due,0),2),
                'weeks_remaining', greatest(coalesce(cws.existing_weeks_remaining,1),1),
                'final_week_amount', case
                  when coalesce(cws.existing_final_week_amount,0) > 0 then round(cws.existing_final_week_amount,2)
                  else round(least(round(coalesce(cws.existing_weekly_due,0),2), greatest(cws.existing_source_total_ex - (round(coalesce(cws.existing_weekly_due,0),2) * greatest(coalesce(cws.existing_weeks_remaining,1) - 1,0)),0)),2)
                end,
                'start_week_start', cws.restructure_start_week_start::text,
                'schedule_json', coalesce(cws.schedule_json,'[]'::jsonb)
              ),
              'suggested_arrangement', jsonb_build_object(
                'target_remaining_balance_ex_vat', cws.suggested_target_total_ex,
                'target_remaining_balance_vat', cws.suggested_target_total_vat,
                'target_remaining_balance_inc_vat', cws.suggested_target_total_inc,
                'taxable_target_remaining_balance_ex_vat', cws.suggested_taxable_target_total_ex,
                'weekly_due', round(
                  greatest(
                    round(coalesce(cws.existing_weekly_due,0),2)
                    * case when cws.existing_source_total_ex > 0 then cws.suggested_target_total_ex / cws.existing_source_total_ex else 1 end,
                    0.01
                  ),
                  2
                ),
                'weeks_remaining', greatest(coalesce(cws.existing_weeks_remaining,1),1),
                'final_week_amount', round(
                  least(
                    greatest(round(coalesce(cws.existing_weekly_due,0),2) * case when cws.existing_source_total_ex > 0 then cws.suggested_target_total_ex / cws.existing_source_total_ex else 1 end, 0.01),
                    greatest(
                      cws.suggested_target_total_ex
                      - (round(greatest(round(coalesce(cws.existing_weekly_due,0),2) * case when cws.existing_source_total_ex > 0 then cws.suggested_target_total_ex / cws.existing_source_total_ex else 1 end, 0.01),2) * greatest(coalesce(cws.existing_weeks_remaining,1) - 1,0)),
                      0
                    )
                  ),
                  2
                ),
                'erni_rate_pct', round(v_erni_pct,6),
                'erni_component_ex_vat', round(abs(cws.existing_taxable_source_total_ex - cws.suggested_taxable_target_total_ex),2),
                'vat_rate_pct', round(v_vat_rate_pct,6),
                'vat_amount', cws.suggested_target_total_vat,
                'umbrella_vat_chargeable', coalesce(cws.umbrella_vat_chargeable,false)
              ),
              'component_breakdown', cws.component_breakdown_json,
              'suggestion_explanation_text', 'This taxable finance case must be durably restructured onto the current pay method before it can move to Ready to Pay.'
            ))
            else null::jsonb
          end as taxable_channel_restructure_resolution_json
        from case_with_schedule cws
        left join open_batch ob
          on ob.finance_case_id = cws.finance_case_id

  ;

  /*
   * Cancellation ownership is deliberately independent from the blocker that
   * an applied resolution has satisfied.  A finance line is clearable only
   * when one exact, current, server-owned resolution can be proved.  This is
   * pre-Draft live authority; frozen Draft and downstream artefacts are not
   * consulted or rewritten here (Policy X).
   */
  create temporary table finance_case_resolution_owner_state on commit drop as
  with taxable_owner_rows as (
    select
      component_row.finance_case_id,
      component_row.finance_component_id,
      component_row.classification,
      component_row.saved_target_pay_method,
      component_row.saved_resolution_mode,
      component_row.saved_resolution_payload_json,
      component_row.saved_resolution_result_json,
      component_row.resolution_fingerprint,
      component_row.is_resolution_stale,
      component_row.current_target_pay_method,
      component_row.current_component_fingerprint,
      (
        component_row.saved_target_pay_method is not null
        or component_row.saved_resolution_mode is not null
        or component_row.saved_resolution_payload_json is not null
        or component_row.saved_resolution_result_json is not null
        or nullif(btrim(coalesce(component_row.resolution_fingerprint, '')), '') is not null
        or coalesce(component_row.is_resolution_stale, false) = true
      ) as has_saved_evidence,
      (
        component_row.saved_resolution_mode is not null
        and jsonb_typeof(component_row.saved_resolution_payload_json) = 'object'
        and jsonb_typeof(component_row.saved_resolution_result_json) = 'object'
        and (
          case upper(btrim(coalesce(component_row.saved_resolution_payload_json->>'resolution_family', '')))
            when 'TAXABLE_CHANNEL' then 'TAXABLE_CHANNEL_RESTRUCTURE'
            else upper(btrim(coalesce(component_row.saved_resolution_payload_json->>'resolution_family', '')))
          end
        ) = 'TAXABLE_CHANNEL_RESTRUCTURE'
        and (
          case upper(btrim(coalesce(component_row.saved_resolution_result_json->>'resolution_family', '')))
            when 'TAXABLE_CHANNEL' then 'TAXABLE_CHANNEL_RESTRUCTURE'
            else upper(btrim(coalesce(component_row.saved_resolution_result_json->>'resolution_family', '')))
          end
        ) = 'TAXABLE_CHANNEL_RESTRUCTURE'
        and coalesce(component_row.is_resolution_stale, false) = false
        and nullif(btrim(coalesce(component_row.saved_target_pay_method, '')), '') is not null
        and upper(btrim(component_row.saved_target_pay_method)) = upper(btrim(coalesce(component_row.current_target_pay_method, '')))
        and nullif(btrim(coalesce(component_row.resolution_fingerprint, '')), '') is not null
        and component_row.resolution_fingerprint is not distinct from component_row.current_component_fingerprint
      ) as is_current_taxable_owner_component
    from finance_case_component_review_rows as component_row
    where component_row.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      and round(coalesce(component_row.remaining_source_amount, 0), 2) > 0
  ), taxable_owner_state as (
    select
      taxable_row.finance_case_id,
      count(*)::integer as taxable_component_count,
      count(*) filter (where taxable_row.is_current_taxable_owner_component)::integer as current_taxable_component_count,
      count(*) filter (where taxable_row.has_saved_evidence)::integer as taxable_saved_evidence_count
    from taxable_owner_rows as taxable_row
    group by taxable_row.finance_case_id
  ), nonbucket_owner_rows as (
    select
      stored_resolution.id as resolution_id,
      case
        when stored_resolution.case_key ~* '^finance:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then substring(stored_resolution.case_key from 9)::uuid
        else null::uuid
      end as finance_case_id,
      stored_resolution.case_key,
      stored_resolution.timesheet_id,
      stored_resolution.payload_json,
      linked_identity.linked_timesheet_id,
      (
        jsonb_typeof(stored_resolution.payload_json) = 'object'
        and stored_resolution.case_key ~* '^finance:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        and upper(btrim(coalesce(stored_resolution.resolution_family, ''))) = 'NON_BUCKET'
        and upper(btrim(coalesce(stored_resolution.payload_json->>'resolution_mode', ''))) in ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_AMOUNT')
        and coalesce(
          stored_resolution.payload_json->>'target_amount_ex_vat',
          stored_resolution.payload_json->>'target_amount',
          stored_resolution.payload_json->>'amount_ex_vat',
          stored_resolution.payload_json->>'amount',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
        and case
          when coalesce(
            stored_resolution.payload_json->>'target_amount_ex_vat',
            stored_resolution.payload_json->>'target_amount',
            stored_resolution.payload_json->>'amount_ex_vat',
            stored_resolution.payload_json->>'amount',
            ''
          ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          then coalesce(
            stored_resolution.payload_json->>'target_amount_ex_vat',
            stored_resolution.payload_json->>'target_amount',
            stored_resolution.payload_json->>'amount_ex_vat',
            stored_resolution.payload_json->>'amount'
          )::numeric >= 0
          else false
        end
        and (
          nullif(btrim(coalesce(stored_resolution.payload_json->>'finance_case_id', '')), '') is null
          or btrim(stored_resolution.payload_json->>'finance_case_id') = substring(stored_resolution.case_key from 9)
        )
        and linked_identity.identity_is_valid
      ) as is_valid_nonbucket_owner
    from public.banking_pay_workbench_session_case_resolutions as stored_resolution
    left join lateral (
      select
        case
          when count(distinct identity_value) <= 1
            and count(*) filter (where identity_value is null and raw_value <> '') = 0
          then min(identity_value)::uuid
          else null::uuid
        end as linked_timesheet_id,
        (
          count(distinct identity_value) <= 1
          and count(*) filter (where identity_value is null and raw_value <> '') = 0
        ) as identity_is_valid
      from (
        select
          raw_identity.raw_value,
          case
            when raw_identity.raw_value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              then lower(raw_identity.raw_value)
            else null::text
          end as identity_value
        from unnest(array[
          coalesce(stored_resolution.timesheet_id::text, ''),
          btrim(coalesce(stored_resolution.payload_json->>'linked_timesheet_id', '')),
          btrim(coalesce(stored_resolution.payload_json->>'timesheet_id', ''))
        ]) as raw_identity(raw_value)
      ) as normalized_identity
    ) as linked_identity on true
    where stored_resolution.session_id = v_workbench_resolution_session_id
      and stored_resolution.candidate_id = v_candidate_id
      and upper(btrim(coalesce(stored_resolution.resolution_family, ''))) = 'NON_BUCKET'
  ), nonbucket_owner_state as (
    select
      nonbucket_row.finance_case_id,
      count(*)::integer as nonbucket_owner_row_count,
      count(*) filter (where nonbucket_row.is_valid_nonbucket_owner)::integer as valid_nonbucket_owner_row_count,
      case
        when count(*) = 1 and count(*) filter (where nonbucket_row.is_valid_nonbucket_owner) = 1
          then min(nonbucket_row.linked_timesheet_id::text)::uuid
        else null::uuid
      end as current_nonbucket_linked_timesheet_id
    from nonbucket_owner_rows as nonbucket_row
    where nonbucket_row.finance_case_id is not null
    group by nonbucket_row.finance_case_id
  )
  select
    scope_row.finance_case_id,
    coalesce(taxable_state.taxable_component_count, 0) as taxable_component_count,
    coalesce(taxable_state.current_taxable_component_count, 0) as current_taxable_component_count,
    coalesce(taxable_state.taxable_saved_evidence_count, 0) as taxable_saved_evidence_count,
    coalesce(nonbucket_state.nonbucket_owner_row_count, 0) as nonbucket_owner_row_count,
    coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) as valid_nonbucket_owner_row_count,
    case
      when coalesce(taxable_state.taxable_component_count, 0) > 0
       and taxable_state.current_taxable_component_count = taxable_state.taxable_component_count
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 0
       and scope_row.case_type in ('OVERPAYMENT', 'UNDERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT')
        then 'TAXABLE_CHANNEL_RESTRUCTURE'
      when coalesce(taxable_state.taxable_saved_evidence_count, 0) = 0
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 1
       and coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) = 1
       and scope_row.case_type = 'MANUAL_DEBT_ADJUSTMENT'
        then 'NON_BUCKET'
      else null::text
    end as current_saved_resolution_family,
    case
      when coalesce(taxable_state.taxable_component_count, 0) > 0
       and taxable_state.current_taxable_component_count = taxable_state.taxable_component_count
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 0
       and scope_row.case_type in ('OVERPAYMENT', 'UNDERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT')
        then 'FINANCE_COMPONENT_SET'
      when coalesce(taxable_state.taxable_saved_evidence_count, 0) = 0
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 1
       and coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) = 1
       and scope_row.case_type = 'MANUAL_DEBT_ADJUSTMENT'
        then 'SESSION_NON_BUCKET'
      when coalesce(taxable_state.taxable_saved_evidence_count, 0) > 0
        or coalesce(nonbucket_state.nonbucket_owner_row_count, 0) > 0
        then 'AMBIGUOUS'
      else 'NONE'
    end as current_saved_resolution_owner_kind,
    case
      when coalesce(taxable_state.taxable_component_count, 0) > 0
       and taxable_state.current_taxable_component_count = taxable_state.taxable_component_count
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 0
        then taxable_state.current_taxable_component_count
      when coalesce(taxable_state.taxable_saved_evidence_count, 0) = 0
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 1
       and coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) = 1
        then 1
      else 0
    end::integer as current_saved_resolution_owner_count,
    (
      (coalesce(taxable_state.taxable_component_count, 0) > 0
       and taxable_state.current_taxable_component_count = taxable_state.taxable_component_count
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 0
       and scope_row.case_type in ('OVERPAYMENT', 'UNDERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT'))
      or
      (coalesce(taxable_state.taxable_saved_evidence_count, 0) = 0
       and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 1
       and coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) = 1
       and scope_row.case_type = 'MANUAL_DEBT_ADJUSTMENT')
    ) as has_current_saved_resolution,
    (
      (
        coalesce(taxable_state.taxable_saved_evidence_count, 0) > 0
        and not (
          coalesce(taxable_state.taxable_component_count, 0) > 0
          and taxable_state.current_taxable_component_count = taxable_state.taxable_component_count
          and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 0
          and scope_row.case_type in ('OVERPAYMENT', 'UNDERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT')
        )
      )
      or coalesce(nonbucket_state.nonbucket_owner_row_count, 0) > 1
      or (
        coalesce(nonbucket_state.nonbucket_owner_row_count, 0) = 1
        and coalesce(nonbucket_state.valid_nonbucket_owner_row_count, 0) <> 1
      )
      or (
        coalesce(taxable_state.taxable_saved_evidence_count, 0) > 0
        and coalesce(nonbucket_state.nonbucket_owner_row_count, 0) > 0
      )
    ) as has_ambiguous_saved_resolution,
    nonbucket_state.current_nonbucket_linked_timesheet_id as current_saved_resolution_linked_timesheet_id
  from finance_case_baseline_scope as scope_row
  left join taxable_owner_state as taxable_state
    on taxable_state.finance_case_id = scope_row.finance_case_id
  left join nonbucket_owner_state as nonbucket_state
    on nonbucket_state.finance_case_id = scope_row.finance_case_id
  where scope_row.finance_case_id is not null;

  create temporary table finance_case_resolution_rollup on commit drop as
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
            coalesce(fctcr.has_dedicated_resolution_payload,false) as has_taxable_channel_restructure_payload,
            coalesce(fctcr.use_dedicated_blocker,false) as use_taxable_channel_restructure_blocker,
            fctcr.taxable_channel_restructure_resolution_json as taxable_channel_restructure_resolution_json,
            case
              when coalesce(fctcr.use_dedicated_blocker,false) = true then 'TAXABLE_CHANNEL_RESTRUCTURE'::text
              when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'NON_BUCKET'::text
              else 'BUCKETED'::text
            end as resolution_family,
            case
              when coalesce(fctcr.use_dedicated_blocker,false) = true then 'Suggested Restructure'::text
              when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'Suggested Gross Total'::text
              else 'Suggested Rate'::text
            end as resolution_action_label,
            fcros.current_saved_resolution_family,
            fcros.current_saved_resolution_owner_kind,
            coalesce(fcros.current_saved_resolution_owner_count, 0)::integer as current_saved_resolution_owner_count,
            coalesce(fcros.has_current_saved_resolution, false) as has_current_saved_resolution,
            coalesce(fcros.has_ambiguous_saved_resolution, false) as has_ambiguous_saved_resolution,
            fcros.current_saved_resolution_linked_timesheet_id,
            coalesce(count(fccr.finance_component_id) filter (
              where fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            ), 0)::int as open_taxable_count,
            coalesce(count(fccr.finance_component_id) filter (
              where fccr.classification in (
                'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
                'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
              )
            ), 0)::int as open_reimbursement_count,
            (coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true), 0)
              + case when coalesce(fctcr.use_dedicated_blocker,false) = true then 1 else 0 end)::int as unresolved_taxable_count,
            coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.is_stale_saved_resolution,false) = true), 0)::int as stale_count,
            (coalesce(count(fccr.finance_component_id) filter (
              where fccr.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            ), 0) > 0
             and
             coalesce(count(fccr.finance_component_id) filter (
              where fccr.classification in (
                'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
                'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
              )
            ), 0) > 0) as is_mixed_case,
            (
              coalesce(fctcr.use_dedicated_blocker,false) = true
              or coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true),0) > 0
            ) as case_needs_resolution,
            (
              coalesce(fctcr.use_dedicated_blocker,false) = false
              and coalesce(count(fccr.finance_component_id) filter (where coalesce(fccr.requires_resolution,false) = true),0) = 0
            ) as case_resolution_satisfied_now,
            case when coalesce(fctcr.use_dedicated_blocker,false) = true then null::jsonb else coalesce(
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
            ) end as non_bucket_resolution_json,
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
                    else case when fccr.classification in ('REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum, 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum) then 'FIXED' when coalesce(fccr.requires_resolution,false) = true then 'REQUIRED' else 'RESOLVED' end
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
          left join finance_case_taxable_channel_restructure_resolution fctcr
            on fctcr.finance_case_id = vfcr.finance_case_id
          left join finance_case_resolution_owner_state fcros
            on fcros.finance_case_id = vfcr.finance_case_id
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
            vfcr.payout_status,
            fctcr.has_dedicated_resolution_payload,
            fctcr.use_dedicated_blocker,
            fctcr.taxable_channel_restructure_resolution_json,
            fcros.current_saved_resolution_family,
            fcros.current_saved_resolution_owner_kind,
            fcros.current_saved_resolution_owner_count,
            fcros.has_current_saved_resolution,
            fcros.has_ambiguous_saved_resolution,
            fcros.current_saved_resolution_linked_timesheet_id
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
          case when g.case_needs_resolution then g.resolution_family else null::text end as required_resolution_family,
          g.current_saved_resolution_family,
          g.current_saved_resolution_owner_kind,
          g.current_saved_resolution_owner_count,
          g.has_current_saved_resolution,
          g.has_ambiguous_saved_resolution,
          g.current_saved_resolution_linked_timesheet_id,
          case
            when g.has_ambiguous_saved_resolution then 'STALE_OR_AMBIGUOUS'
            when g.has_current_saved_resolution then 'RESOLVED_AND_CLEARABLE'
            when g.case_needs_resolution then 'REQUIRES_RESOLUTION'
            else 'NOT_REQUIRED'
          end as finance_resolution_clearability_state,
          case
            when g.has_ambiguous_saved_resolution then 'SAVED_RESOLUTION_OWNER_STALE_OR_AMBIGUOUS'
            else null::text
          end as finance_resolution_clear_block_reason,
          g.case_needs_resolution,
          g.case_resolution_satisfied_now,
          g.resolution_action_label,
          g.taxable_channel_restructure_resolution_json,
          null::jsonb as linked_resolution_scope_json,
          (
            g.case_needs_resolution = true
            or jsonb_array_length(coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)) > 0
          ) as is_blocked,
          (
            coalesce(g.payee_blocked_reason_codes, '[]'::jsonb)
            ||
            (case
              when g.resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_TAXABLE_CHANNEL_RESTRUCTURE')
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
              'required_resolution_family', case when g.case_needs_resolution then g.resolution_family else null end,
              'current_saved_resolution_family', g.current_saved_resolution_family,
              'current_saved_resolution_owner_kind', g.current_saved_resolution_owner_kind,
              'current_saved_resolution_owner_count', g.current_saved_resolution_owner_count,
              'has_current_saved_resolution', g.has_current_saved_resolution,
              'has_ambiguous_saved_resolution', g.has_ambiguous_saved_resolution,
              'current_saved_resolution_linked_timesheet_id', case
                when g.current_saved_resolution_linked_timesheet_id is null then null
                else g.current_saved_resolution_linked_timesheet_id::text
              end,
              'finance_resolution_clearability_state', case
                when g.has_ambiguous_saved_resolution then 'STALE_OR_AMBIGUOUS'
                when g.has_current_saved_resolution then 'RESOLVED_AND_CLEARABLE'
                when g.case_needs_resolution then 'REQUIRES_RESOLUTION'
                else 'NOT_REQUIRED'
              end,
              'finance_resolution_clear_block_reason', case
                when g.has_ambiguous_saved_resolution then 'SAVED_RESOLUTION_OWNER_STALE_OR_AMBIGUOUS'
                else null
              end,
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
                  when g.resolution_family = 'TAXABLE_CHANNEL_RESTRUCTURE' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_TAXABLE_CHANNEL_RESTRUCTURE')
                  when g.resolution_family = 'NON_BUCKET' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_NON_BUCKET_RESOLUTION')
                  when g.resolution_family = 'BUCKETED' and g.case_needs_resolution = true then jsonb_build_array('BLOCKED_TAXABLE_RESOLUTION')
                  else '[]'::jsonb
                end)
              ),
              'non_bucket_resolution', g.non_bucket_resolution_json,
              'taxable_channel_restructure', g.taxable_channel_restructure_resolution_json,
              'taxable_channel_restructure_resolution', g.taxable_channel_restructure_resolution_json
            )
          ) as case_resolution_summary_json,
          case
            when g.resolution_family = 'NON_BUCKET'
              and jsonb_typeof(g.non_bucket_resolution_json) = 'object'
            then g.non_bucket_resolution_json
            else null::jsonb
          end as taxable_manual_debt_resolution_json,
          g.case_components_json
        from grouped g

  ;

  PERFORM public._imp_debug_audit(
    v_actor_user_id,
    'PAY_PREVIEW_FINANCE_CASE_BASELINE_RESULT',
    jsonb_build_object(
      'candidate_id', v_candidate_id,
      'finance_case_component_count', (select count(*)::int from finance_case_component_review_rows_effective),
      'finance_case_resolution_count', (select count(*)::int from finance_case_resolution_rollup),
      'finance_case_repaid_wtd_count', (select count(*)::int from finance_case_repaid_wtd)
    ),
    'pay_preview_finance',
    COALESCE(v_candidate_id::text, 'NO_CANDIDATE_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  return jsonb_build_object(
    'candidate_id', v_candidate_id::text,
    'finance_case_component_count', (select count(*)::int from finance_case_component_review_rows_effective),
    'finance_case_resolution_count', (select count(*)::int from finance_case_resolution_rollup)
  );

exception
  when others then
    PERFORM public._imp_debug_audit(
      v_actor_user_id,
      'PAY_PREVIEW_FINANCE_CASE_BASELINE_ERROR',
      jsonb_build_object(
        'candidate_id', p_candidate_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_preview_finance',
      COALESCE(p_candidate_id::text, 'NO_CANDIDATE_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );
    RAISE;
end;
$function$
;

-- [REPLACES] public.pay_preview_candidate_collect_scope(p_context_json jsonb, p_candidate_id uuid, p_cursor_json jsonb, p_limit integer)  winner piece A16
CREATE OR REPLACE FUNCTION public.pay_preview_candidate_collect_scope(p_context_json jsonb, p_candidate_id uuid DEFAULT NULL::uuid, p_cursor_json jsonb DEFAULT NULL::jsonb, p_limit integer DEFAULT 100)
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
  v_refresh_scope_kind text := NULL;
  v_effective_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_targeted_timesheet_ids_requested_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_requested_json jsonb := '[]'::jsonb;
  v_pay_channel_scope text := 'ALL';
  v_candidate_pay_method text := NULL::text;
  v_target_pay_batch_id_raw text := NULL::text;
  v_target_pay_batch_id uuid := NULL::uuid;
  v_target_operation_source_key text := NULL::text;
  v_source_build_mode boolean := false;
  v_source_build_requested_scope_kind text := NULL::text;
  v_source_build_actual_scope_kind text := NULL::text;
  v_source_build_cursor_in_json jsonb := NULL::jsonb;
  v_source_build_cursor_out_json jsonb := NULL::jsonb;
  v_source_build_page_limit integer := 100;
  v_source_build_last_timesheet_id uuid := NULL::uuid;
  v_source_build_next_timesheet_id uuid := NULL::uuid;
  v_source_build_target_page_count integer := 0;
  v_source_build_target_total_count integer := 0;
  v_source_build_has_more boolean := false;
  v_source_build_targeted_payload_received boolean := false;
  v_source_build_fallback_used boolean := false;
  v_source_build_fallback_reason text := NULL::text;
  v_source_build_allow_full_fallback boolean := false;
  v_source_build_candidate_filter_applied_early boolean := false;
  v_source_build_timesheet_filter_applied_early boolean := false;
  v_source_build_large_aggregation_avoided boolean := false;
  v_source_build_collect_started_at timestamptz := clock_timestamp();
  v_source_build_collect_elapsed_ms numeric := 0;
  v_lifecycle_reconciliation_mode boolean := false;
  v_historic_metadata_allowed boolean := false;
  v_signed_finance_reconciliation boolean := false;
  v_exclude_lifecycle_ineligible_current_payability boolean := false;
  v_authoritative_timesheet_scope boolean := false;
  v_internal_reconciliation_requested boolean := false;
  v_internal_reconciliation_authorised boolean := false;
  v_current_internal_authority_token text := NULL::text;
begin
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  if jsonb_typeof(v_context_json) <> 'object' then
    raise exception 'p_context_json must be a JSON object';
  end if;


  v_source_build_mode := (
    LOWER(BTRIM(COALESCE(
      v_context_json->>'source_build_mode',
      v_context_json->>'workbench_source_build_mode',
      v_context_json #>> '{source_build,enabled}',
      v_context_json #>> '{workbench,source_build_mode}',
      ''
    ))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR UPPER(BTRIM(COALESCE(
      v_context_json->>'job_type',
      v_context_json #>> '{job,type}',
      v_context_json #>> '{source_build,job_type}',
      v_context_json #>> '{workbench,job_type}',
      ''
    ))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
  );

  v_lifecycle_reconciliation_mode := LOWER(BTRIM(COALESCE(
    v_context_json->>'lifecycle_reconciliation_mode',
    v_context_json #>> '{preview_decisions_json,lifecycle_reconciliation_mode}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_historic_metadata_allowed := LOWER(BTRIM(COALESCE(
    v_context_json->>'historic_metadata_allowed',
    v_context_json #>> '{preview_decisions_json,historic_metadata_allowed}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_signed_finance_reconciliation := LOWER(BTRIM(COALESCE(
    v_context_json->>'signed_finance_reconciliation',
    v_context_json #>> '{preview_decisions_json,signed_finance_reconciliation}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_exclude_lifecycle_ineligible_current_payability := LOWER(BTRIM(COALESCE(
    v_context_json->>'exclude_lifecycle_ineligible_current_payability',
    v_context_json #>> '{preview_decisions_json,exclude_lifecycle_ineligible_current_payability}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_authoritative_timesheet_scope := LOWER(BTRIM(COALESCE(
    v_context_json->>'authoritative_timesheet_scope',
    v_context_json #>> '{preview_decisions_json,authoritative_timesheet_scope}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_internal_reconciliation_requested :=
    v_source_build_mode
    OR v_lifecycle_reconciliation_mode
    OR v_historic_metadata_allowed
    OR v_signed_finance_reconciliation
    OR v_exclude_lifecycle_ineligible_current_payability
    OR v_authoritative_timesheet_scope;

  v_current_internal_authority_token := NULLIF(BTRIM(COALESCE(
    current_setting('cloudtms.pay_workbench_overpayment_sync_token', true),
    ''
  )), '');

  v_internal_reconciliation_authorised :=
    v_internal_reconciliation_requested
    AND v_source_build_mode
    AND v_current_internal_authority_token IS NOT NULL;

  IF v_internal_reconciliation_requested
     AND v_internal_reconciliation_authorised IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PREVIEW_COLLECT_SCOPE_INTERNAL_AUTHORITY_REQUIRED'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'source_build_mode', COALESCE(v_source_build_mode, false),
              'transaction_authority_present', v_current_internal_authority_token IS NOT NULL,
              'lifecycle_reconciliation_mode', v_lifecycle_reconciliation_mode,
              'historic_metadata_allowed', v_historic_metadata_allowed,
              'signed_finance_reconciliation', v_signed_finance_reconciliation,
              'exclude_lifecycle_ineligible_current_payability', v_exclude_lifecycle_ineligible_current_payability,
              'authoritative_timesheet_scope', v_authoritative_timesheet_scope
            )::text;
  END IF;

  IF v_historic_metadata_allowed
     AND (
       v_authoritative_timesheet_scope IS NOT TRUE
       OR UPPER(BTRIM(COALESCE(
         v_context_json->>'refresh_scope_kind',
         v_context_json #>> '{preview_decisions_json,refresh_scope_kind}',
         ''
       ))) <> 'TARGETED_TIMESHEETS'
     ) THEN
    RAISE EXCEPTION 'PAY_PREVIEW_COLLECT_SCOPE_HISTORIC_METADATA_SCOPE_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'authoritative_timesheet_scope', v_authoritative_timesheet_scope,
              'refresh_scope_kind', COALESCE(
                v_context_json->>'refresh_scope_kind',
                v_context_json #>> '{preview_decisions_json,refresh_scope_kind}'
              )
            )::text;
  END IF;

  v_source_build_cursor_in_json := COALESCE(
    p_cursor_json,
    v_context_json->'source_cursor',
    v_context_json #> '{source_build,cursor}',
    v_context_json #> '{workbench,source_cursor}',
    NULL::jsonb
  );

  IF NULLIF(BTRIM(COALESCE(
       v_context_json->>'source_page_limit',
       v_context_json #>> '{source_build,limit}',
       v_context_json #>> '{workbench,source_build_limit}',
       ''
     )), '') ~ '^[0-9]+$' THEN
    v_source_build_page_limit := LEAST(
      GREATEST(
        NULLIF(BTRIM(COALESCE(
          v_context_json->>'source_page_limit',
          v_context_json #>> '{source_build,limit}',
          v_context_json #>> '{workbench,source_build_limit}',
          ''
        )), '')::integer,
        1
      ),
      100
    );
  ELSE
    v_source_build_page_limit := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  END IF;

  IF COALESCE(v_source_build_cursor_in_json->>'last_timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_source_build_last_timesheet_id := (v_source_build_cursor_in_json->>'last_timesheet_id')::uuid;
  END IF;

  v_source_build_allow_full_fallback := LOWER(BTRIM(COALESCE(
    v_context_json->>'source_build_allow_full_fallback',
    v_context_json #>> '{source_build,allow_full_fallback}',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');


  /*
    5.3 PAGE mode: when no single candidate is supplied, this function acts as a
    bounded pre-draft scope collector. It returns one candidate page only and
    never aggregates all candidate ids for workbench open.
  */
  IF v_candidate_id IS NULL THEN
    RETURN public.pay_preview_build_context(
      p_pay_date => NULLIF(BTRIM(COALESCE(v_context_json->>'pay_date', '')), '')::date,
      p_week_ending_cutoff => NULLIF(BTRIM(COALESCE(v_context_json->>'week_ending_cutoff_date', v_context_json->>'week_ending_cutoff', '')), '')::date,
      p_actor_user_id => CASE
        WHEN NULLIF(BTRIM(COALESCE(v_context_json #>> '{filters,actor_user_id}', v_context_json->>'actor_user_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(v_context_json #>> '{filters,actor_user_id}', v_context_json->>'actor_user_id', '')), '')::uuid
        ELSE NULL::uuid
      END,
      p_candidate_id => NULL::uuid,
      p_client_id => CASE
        WHEN NULLIF(BTRIM(COALESCE(v_context_json #>> '{filters,client_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(v_context_json #>> '{filters,client_id}', '')), '')::uuid
        ELSE NULL::uuid
      END,
      p_preview_decisions_json => COALESCE(v_context_json->'preview_decisions_json', '{}'::jsonb)
        || jsonb_build_object(
          'preview_context_mode', 'PAGE',
          'scope_limit', LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100),
          'scope_cursor', COALESCE(p_cursor_json, NULL::jsonb)
        )
    );
  END IF;


  v_pay_date := nullif(btrim(coalesce(v_context_json->>'pay_date', '')), '')::date;
  v_week_ending_cutoff := DATE '9999-12-31';
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

  v_pay_channel_scope := NULLIF(UPPER(BTRIM(COALESCE(
    v_context_json->>'pay_channel_scope',
    v_context_json #>> '{filters,pay_channel_scope}',
    v_context_json #>> '{preview_decisions_json,pay_channel_scope}',
    'ALL'
  ))), '');
  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    v_pay_channel_scope := 'ALL';
  END IF;

  v_target_pay_batch_id_raw := NULLIF(BTRIM(COALESCE(
    v_context_json->>'target_pay_batch_id',
    v_context_json->>'pay_batch_id',
    v_context_json->>'draft_pay_batch_id',
    v_context_json #>> '{preview_decisions_json,target_pay_batch_id}',
    v_context_json #>> '{preview_decisions_json,pay_batch_id}',
    ''
  )), '');
  IF v_target_pay_batch_id_raw ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_target_pay_batch_id := v_target_pay_batch_id_raw::uuid;
  END IF;

  v_target_operation_source_key := NULLIF(BTRIM(COALESCE(
    v_context_json->>'target_operation_source_key',
    v_context_json #>> '{preview_decisions_json,target_operation_source_key}',
    ''
  )), '');

  SELECT UPPER(NULLIF(BTRIM(COALESCE(public.candidates.pay_method, '')), ''))
  INTO v_candidate_pay_method
  FROM public.candidates
  WHERE public.candidates.id = v_candidate_id;

  v_refresh_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(v_context_json->>'refresh_scope_kind', v_context_json #>> '{preview_decisions_json,refresh_scope_kind}', ''))), '');
  IF v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_refresh_scope_kind := NULL;
  END IF;

  SELECT COALESCE(jsonb_agg(targeted_scope_ids.targeted_timesheet_id ORDER BY targeted_scope_ids.targeted_timesheet_id), '[]'::jsonb)
  INTO v_targeted_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(targeted_scope_values.targeted_timesheet_id_raw), '') AS targeted_timesheet_id
    FROM (
      SELECT targeted_scope_array.value AS targeted_timesheet_id_raw
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_context_json->'targeted_timesheet_ids') = 'array' THEN v_context_json->'targeted_timesheet_ids'
          WHEN jsonb_typeof(v_context_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_context_json->>'targeted_timesheet_ids')
          WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,targeted_timesheet_ids}') = 'array' THEN v_context_json #> '{preview_decisions_json,targeted_timesheet_ids}'
          WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_context_json #>> '{preview_decisions_json,targeted_timesheet_ids}')
          ELSE '[]'::jsonb
        END
      ) AS targeted_scope_array(value)
    ) AS targeted_scope_values
    WHERE NULLIF(BTRIM(targeted_scope_values.targeted_timesheet_id_raw), '') IS NOT NULL
  ) AS targeted_scope_ids;

  SELECT COALESCE(jsonb_agg(linked_scope_ids.linked_timesheet_id ORDER BY linked_scope_ids.linked_timesheet_id), '[]'::jsonb)
  INTO v_linked_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(linked_scope_values.linked_timesheet_id_raw), '') AS linked_timesheet_id
    FROM (
      SELECT linked_scope_array.value AS linked_timesheet_id_raw
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_context_json->'linked_timesheet_ids') = 'array' THEN v_context_json->'linked_timesheet_ids'
          WHEN jsonb_typeof(v_context_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_context_json->>'linked_timesheet_ids')
          WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,linked_timesheet_ids}') = 'array' THEN v_context_json #> '{preview_decisions_json,linked_timesheet_ids}'
          WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_context_json #>> '{preview_decisions_json,linked_timesheet_ids}')
          ELSE '[]'::jsonb
        END
      ) AS linked_scope_array(value)
    ) AS linked_scope_values
    WHERE NULLIF(BTRIM(linked_scope_values.linked_timesheet_id_raw), '') IS NOT NULL
  ) AS linked_scope_ids;


  v_targeted_timesheet_ids_requested_json := CASE
    WHEN jsonb_typeof(v_context_json->'targeted_timesheet_ids_requested') = 'array' THEN v_context_json->'targeted_timesheet_ids_requested'
    WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,targeted_timesheet_ids_requested}') = 'array' THEN v_context_json #> '{preview_decisions_json,targeted_timesheet_ids_requested}'
    ELSE COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)
  END;

  v_linked_timesheet_ids_requested_json := CASE
    WHEN jsonb_typeof(v_context_json->'linked_timesheet_ids_requested') = 'array' THEN v_context_json->'linked_timesheet_ids_requested'
    WHEN jsonb_typeof(v_context_json #> '{preview_decisions_json,linked_timesheet_ids_requested}') = 'array' THEN v_context_json #> '{preview_decisions_json,linked_timesheet_ids_requested}'
    ELSE COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)
  END;

  v_source_build_requested_scope_kind := COALESCE(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE');
  v_source_build_targeted_payload_received := (
    jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
    OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0
  );

  IF COALESCE(v_source_build_mode, false) = true
     AND v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND (
       jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
       OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0
     ) THEN
    WITH source_requested_ids AS (
      SELECT
        'TARGETED'::text AS source_scope_kind,
        NULLIF(BTRIM(target_values.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) AS target_values(value)
      WHERE NULLIF(BTRIM(target_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

      UNION ALL

      SELECT
        'LINKED'::text AS source_scope_kind,
        NULLIF(BTRIM(linked_values.value), '')::uuid AS timesheet_id
      FROM jsonb_array_elements_text(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) AS linked_values(value)
      WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ),
    source_requested_dedup AS (
      SELECT DISTINCT ON (source_requested_ids.timesheet_id)
        source_requested_ids.source_scope_kind,
        source_requested_ids.timesheet_id
      FROM source_requested_ids
      WHERE source_requested_ids.timesheet_id IS NOT NULL
      ORDER BY
        source_requested_ids.timesheet_id,
        CASE WHEN source_requested_ids.source_scope_kind = 'TARGETED' THEN 0 ELSE 1 END
    ),
    source_remaining_ids AS (
      SELECT
        source_requested_dedup.source_scope_kind,
        source_requested_dedup.timesheet_id
      FROM source_requested_dedup
      WHERE v_source_build_last_timesheet_id IS NULL
         OR source_requested_dedup.timesheet_id > v_source_build_last_timesheet_id
    ),
    source_page AS (
      SELECT
        source_remaining_ids.source_scope_kind,
        source_remaining_ids.timesheet_id
      FROM source_remaining_ids
      ORDER BY source_remaining_ids.timesheet_id
      LIMIT v_source_build_page_limit
    )
    SELECT
      COALESCE(
        jsonb_agg(source_page.timesheet_id ORDER BY source_page.timesheet_id)
          FILTER (WHERE source_page.source_scope_kind = 'TARGETED'),
        '[]'::jsonb
      ),
      COALESCE(
        jsonb_agg(source_page.timesheet_id ORDER BY source_page.timesheet_id)
          FILTER (WHERE source_page.source_scope_kind = 'LINKED'),
        '[]'::jsonb
      ),
      count(source_page.timesheet_id)::integer,
      COALESCE((SELECT count(*)::integer FROM source_remaining_ids), 0),
      (
        SELECT ordered_source_page.timesheet_id
        FROM source_page AS ordered_source_page
        ORDER BY ordered_source_page.timesheet_id DESC
        LIMIT 1
      )
    INTO
      v_targeted_timesheet_ids_json,
      v_linked_timesheet_ids_json,
      v_source_build_target_page_count,
      v_source_build_target_total_count,
      v_source_build_next_timesheet_id
    FROM source_page;

    v_targeted_timesheet_ids_requested_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
    v_linked_timesheet_ids_requested_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);
    v_source_build_has_more := v_source_build_target_total_count > v_source_build_target_page_count;
    v_source_build_cursor_out_json := CASE
      WHEN v_source_build_next_timesheet_id IS NULL THEN NULL::jsonb
      ELSE jsonb_build_object(
        'last_timesheet_id', v_source_build_next_timesheet_id::text,
        'source_page_limit', v_source_build_page_limit,
        'requested_refresh_scope_kind', v_source_build_requested_scope_kind
      )
    END;
    v_source_build_candidate_filter_applied_early := true;
    v_source_build_timesheet_filter_applied_early := true;
    v_source_build_large_aggregation_avoided := true;
  ELSIF COALESCE(v_source_build_mode, false) = true
        AND COALESCE(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE') = 'CANDIDATE_FULL_LIVE'
        AND v_candidate_id IS NOT NULL THEN
    WITH retained_finance_cases AS (
      SELECT retained_case.id AS finance_case_id,
             retained_case.linked_timesheet_id
      FROM public.pay_advances AS retained_case
      WHERE retained_case.candidate_id = v_candidate_id
        AND retained_case.linked_timesheet_id IS NOT NULL
        AND (
          retained_case.advance_kind IN (
            'OVERPAYMENT'::public.pay_advance_kind_enum,
            'UNDERPAYMENT'::public.pay_advance_kind_enum
          )
          OR retained_case.case_type IN (
            'OVERPAYMENT'::public.pay_finance_case_type_enum,
            'UNDERPAYMENT'::public.pay_finance_case_type_enum
          )
        )
        AND (
          (
            retained_case.status IN (
              'ACTIVE'::public.pay_advance_status_enum,
              'PAUSED'::public.pay_advance_status_enum
            )
            AND ROUND(COALESCE(retained_case.outstanding_amount, 0), 2) > 0
          )
          OR EXISTS (
            SELECT 1
            FROM public.pay_finance_case_components AS retained_component
            WHERE retained_component.finance_case_id = retained_case.id
              AND retained_component.candidate_id = v_candidate_id
              AND (
                retained_component.closed_at_utc IS NULL
                OR ROUND(COALESCE(retained_component.remaining_source_amount, 0), 2) > 0
              )
          )
          OR EXISTS (
            SELECT 1
            FROM public.pay_advance_reservations AS retained_reservation
            WHERE retained_reservation.finance_case_id = retained_case.id
              AND UPPER(BTRIM(COALESCE(retained_reservation.status, ''))) IN ('RESERVED', 'COMMITTED', 'SETTLED')
              AND retained_reservation.released_at_utc IS NULL
          )
          OR EXISTS (
            SELECT 1
            FROM public.pay_batch_items AS retained_item
            JOIN public.pay_batch_candidates AS retained_candidate
              ON retained_candidate.id = retained_item.pay_batch_candidate_id
             AND retained_candidate.candidate_id = v_candidate_id
            WHERE retained_item.finance_case_id = retained_case.id
              AND retained_item.is_voided IS NOT TRUE
          )
          OR EXISTS (
            SELECT 1
            FROM public.pay_payment_correction_items AS retained_correction
            WHERE retained_correction.finance_case_id = retained_case.id
              AND retained_correction.candidate_id = v_candidate_id
              AND retained_correction.status = 'APPLIED'
          )
        )
    ),
    candidate_source_seed AS (
      SELECT candidate_scope_financials.timesheet_id
      FROM public.timesheets_financials AS candidate_scope_financials
      JOIN public.timesheets AS candidate_scope_timesheets
        ON candidate_scope_timesheets.timesheet_id = candidate_scope_financials.timesheet_id
      JOIN public.candidates AS candidate_scope_candidates
        ON candidate_scope_candidates.id = candidate_scope_financials.candidate_id
      WHERE candidate_scope_financials.is_current = true
        AND candidate_scope_financials.candidate_id = v_candidate_id
        AND COALESCE(candidate_scope_financials.pay_on_hold, false) = false
        AND COALESCE(candidate_scope_financials.has_rate_issue, false) = false
        AND COALESCE(candidate_scope_financials.has_pay_channel_issue, false) = false
        AND UPPER(COALESCE(candidate_scope_financials.processing_status::text, '')) NOT IN (
          'UNASSIGNED',
          'CLIENT_UNRESOLVED',
          'RATE_MISSING',
          'PAY_CHANNEL_MISSING'
        )
        AND UPPER(COALESCE(candidate_scope_candidates.pay_method, '')) IN ('PAYE', 'UMBRELLA')
        AND (
          v_pay_channel_scope = 'ALL'
          OR UPPER(COALESCE(candidate_scope_candidates.pay_method, '')) = v_pay_channel_scope
        )
        AND (v_client_id IS NULL OR candidate_scope_financials.client_id = v_client_id)
        AND (
          (
            candidate_scope_timesheets.authorised_at_server IS NOT NULL
            AND candidate_scope_timesheets.revoked_at IS NULL
            AND candidate_scope_timesheets.archived_at_utc IS NULL
          )
          OR EXISTS (
            SELECT 1
            FROM public.timesheet_payment_overrides AS candidate_scope_overrides
            WHERE candidate_scope_overrides.timesheet_id = candidate_scope_financials.timesheet_id
              AND candidate_scope_overrides.candidate_id = v_candidate_id
              AND candidate_scope_overrides.cleared_at_utc IS NULL
              AND candidate_scope_overrides.consumed_at_utc IS NULL
              AND candidate_scope_overrides.consumed_by_pay_batch_id IS NULL
              AND UPPER(COALESCE(candidate_scope_overrides.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
          )
        )

      UNION

      SELECT retained_finance_cases.linked_timesheet_id
      FROM retained_finance_cases

      UNION

      SELECT retained_component.linked_timesheet_id
      FROM public.pay_finance_case_components AS retained_component
      JOIN retained_finance_cases
        ON retained_finance_cases.finance_case_id = retained_component.finance_case_id
      WHERE retained_component.candidate_id = v_candidate_id
        AND retained_component.linked_timesheet_id IS NOT NULL

      UNION

      SELECT retained_item.timesheet_id
      FROM public.pay_batch_items AS retained_item
      JOIN public.pay_batch_candidates AS retained_candidate
        ON retained_candidate.id = retained_item.pay_batch_candidate_id
       AND retained_candidate.candidate_id = v_candidate_id
      JOIN retained_finance_cases
        ON retained_finance_cases.finance_case_id = retained_item.finance_case_id
      WHERE retained_item.timesheet_id IS NOT NULL
        AND retained_item.is_voided IS NOT TRUE

      UNION

      SELECT retained_correction.timesheet_id
      FROM public.pay_payment_correction_items AS retained_correction
      JOIN retained_finance_cases
        ON retained_finance_cases.finance_case_id = retained_correction.finance_case_id
      WHERE retained_correction.candidate_id = v_candidate_id
        AND retained_correction.timesheet_id IS NOT NULL
        AND retained_correction.status = 'APPLIED'

      UNION

      SELECT forced_source_timesheet.timesheet_id
      FROM (
        SELECT NULLIF(BTRIM(forced_source_values.value), '')::uuid AS timesheet_id
        FROM jsonb_array_elements_text(
          CASE
            WHEN v_internal_reconciliation_authorised IS TRUE
             AND jsonb_typeof(v_context_json->'source_build_force_include_timesheet_ids') = 'array'
              THEN v_context_json->'source_build_force_include_timesheet_ids'
            ELSE '[]'::jsonb
          END
        ) WITH ORDINALITY AS forced_source_values(value, position)
        WHERE forced_source_values.position <= 500
          AND NULLIF(BTRIM(forced_source_values.value), '') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS forced_source_timesheet
      JOIN public.timesheets AS forced_candidate_timesheet
        ON forced_candidate_timesheet.timesheet_id = forced_source_timesheet.timesheet_id
      WHERE EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS forced_candidate_financials
        WHERE forced_candidate_financials.timesheet_id = forced_source_timesheet.timesheet_id
          AND forced_candidate_financials.candidate_id = v_candidate_id
      )
    ),
    candidate_source_timesheets AS (
      SELECT DISTINCT candidate_source_seed.timesheet_id
      FROM candidate_source_seed
      WHERE candidate_source_seed.timesheet_id IS NOT NULL
        AND (
          v_source_build_last_timesheet_id IS NULL
          OR candidate_source_seed.timesheet_id > v_source_build_last_timesheet_id
        )
    ),
    candidate_source_page AS (
      SELECT candidate_source_timesheets.timesheet_id
      FROM candidate_source_timesheets
      ORDER BY candidate_source_timesheets.timesheet_id
      LIMIT v_source_build_page_limit
    )
    SELECT
      COALESCE(jsonb_agg(candidate_source_page.timesheet_id ORDER BY candidate_source_page.timesheet_id), '[]'::jsonb),
      count(candidate_source_page.timesheet_id)::integer,
      COALESCE((SELECT count(*)::integer FROM candidate_source_timesheets), 0),
      (
        SELECT ordered_candidate_source_page.timesheet_id
        FROM candidate_source_page AS ordered_candidate_source_page
        ORDER BY ordered_candidate_source_page.timesheet_id DESC
        LIMIT 1
      )
    INTO
      v_targeted_timesheet_ids_json,
      v_source_build_target_page_count,
      v_source_build_target_total_count,
      v_source_build_next_timesheet_id
    FROM candidate_source_page;

    v_targeted_timesheet_ids_requested_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
    v_linked_timesheet_ids_json := '[]'::jsonb;
    v_linked_timesheet_ids_requested_json := '[]'::jsonb;
    v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
    v_source_build_has_more := v_source_build_target_total_count > v_source_build_target_page_count;
    v_source_build_cursor_out_json := CASE
      WHEN v_source_build_next_timesheet_id IS NULL THEN NULL::jsonb
      ELSE jsonb_build_object(
        'last_timesheet_id', v_source_build_next_timesheet_id::text,
        'source_page_limit', v_source_build_page_limit,
        'requested_refresh_scope_kind', v_source_build_requested_scope_kind
      )
    END;
    v_source_build_candidate_filter_applied_early := true;
    v_source_build_timesheet_filter_applied_early := true;
    v_source_build_large_aggregation_avoided := true;
    v_authoritative_timesheet_scope := v_internal_reconciliation_authorised IS TRUE
      AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0;
    v_historic_metadata_allowed := v_signed_finance_reconciliation IS TRUE
      AND v_authoritative_timesheet_scope IS TRUE;
  END IF;

  IF COALESCE(v_source_build_mode, false) = true
     AND v_source_build_requested_scope_kind = 'TARGETED_TIMESHEETS'
     AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) = 0
     AND jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) = 0 THEN
    IF COALESCE(v_source_build_targeted_payload_received, false) = true THEN
      v_effective_refresh_scope_kind := 'TARGETED_TIMESHEETS';
    ELSIF v_source_build_allow_full_fallback = true THEN
      v_source_build_fallback_used := true;
      v_source_build_fallback_reason := COALESCE(
        NULLIF(BTRIM(v_context_json->>'source_build_fallback_reason'), ''),
        NULLIF(BTRIM(v_context_json #>> '{source_build,fallback_reason}'), ''),
        'TARGETED_TIMESHEETS_REQUESTED_WITHOUT_TARGET_IDS'
      );
      v_effective_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    ELSE
      RAISE EXCEPTION 'SOURCE_BUILD_TARGETED_TIMESHEETS_REQUIRES_TARGET_IDS'
        USING DETAIL = 'WORKBENCH_CANDIDATE_SOURCE_BUILD TARGETED_TIMESHEETS requires targeted_timesheet_ids or linked_timesheet_ids unless source_build_allow_full_fallback is true.';
    END IF;
  ELSIF v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
        AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) = 0
        AND jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) = 0
        AND COALESCE(v_source_build_mode, false) = false THEN
    v_effective_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
  ELSE
    v_effective_refresh_scope_kind := COALESCE(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE');
  END IF;

  v_source_build_actual_scope_kind := v_effective_refresh_scope_kind;

  IF COALESCE(v_source_build_mode, false) = true
     AND v_effective_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND (
       jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
       OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0
     ) THEN
    v_source_build_candidate_filter_applied_early := true;
    v_source_build_timesheet_filter_applied_early := true;
    v_source_build_large_aggregation_avoided := true;
  END IF;

  if v_pay_date is null then
    raise exception 'pay_date missing from p_context_json';
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

  drop table if exists pg_temp.pay_preview_candidate_context, pg_temp.targeted_refresh_requested_timesheet_ids, pg_temp.linked_refresh_requested_timesheet_ids, pg_temp.targeted_refresh_rotation_scope, pg_temp.targeted_refresh_scope_projection, pg_temp.targeted_refresh_timesheet_ids, pg_temp.linked_refresh_timesheet_ids, pg_temp.targeted_refresh_family_timesheet_ids, pg_temp.linked_refresh_family_timesheet_ids, pg_temp.targeted_refresh_all_family_timesheet_ids, pg_temp.targeted_refresh_all_family_projection, pg_temp.targeted_refresh_all_baseline_projection, pg_temp.targeted_refresh_all_timesheet_ids, pg_temp.active_snoozes, pg_temp.active_timesheet_payment_snoozes, pg_temp.active_segment_snoozes, pg_temp.active_timesheet_payment_overrides, pg_temp.force_include, pg_temp.reserved_batch_items, pg_temp.reserved_by_source_ref, pg_temp.reserved_total_by_timesheet, pg_temp.reserved_segment_key_map, pg_temp.reserved_segment_sums, pg_temp.reserved_preview_segment_ords, pg_temp.reserved_additional_by_code, pg_temp.eligible_tsfin, pg_temp.debted_overpayment_cases, pg_temp.umb_map, pg_temp.adjustment_family_projection, pg_temp.adj, pg_temp.ts_current, pg_temp.targeted_baseline_only_ts_current, pg_temp.active_settled_artifact_components, pg_temp.active_settled_artifact_baseline_by_timesheet, pg_temp.ts_baseline, pg_temp.finance_case_baseline_scope, pg_temp.manual_adjustment_carry_forward_scope, pg_temp.segment_status, pg_temp.blocked_items_all, pg_temp.blocked_items, pg_temp.blocked_items_snoozed, pg_temp.do_not_pay_all, pg_temp.do_not_pay_items, pg_temp.do_not_pay_items_snoozed, pg_temp.ts_deltas, pg_temp.ts_itemised, pg_temp.worked_time_current_segment_rows, pg_temp.worked_time_baseline_segment_rows, pg_temp.worked_time_current_ranked, pg_temp.worked_time_baseline_ranked, pg_temp.worked_time_bucket_ids, pg_temp.worked_time_bucket_agg, pg_temp.worked_time_bucket_calc, pg_temp.worked_time_bucket_alloc, pg_temp.worked_time_bucket_effective, pg_temp.worked_time_bucket_component_rows, pg_temp.worked_time_key_totals, pg_temp.worked_time_bucket_component_sums, pg_temp.worked_time_amount_fallback_rows, pg_temp.timesheet_component_rows, pg_temp.timesheet_component_match_rows, pg_temp.transient_timesheet_component_rows, pg_temp.transient_timesheet_component_review_rows, pg_temp.timesheet_case_actionable_basis, pg_temp.timesheet_live_scope, pg_temp.timesheet_linked_scope_counts, pg_temp.transient_timesheet_component_review_rows_effective, pg_temp.timesheet_case_rollup, pg_temp.finance_candidate_seed, pg_temp.candidate_base, pg_temp.timesheet_candidate_rollup, pg_temp.candidate_rollup, pg_temp.blocked_counts, pg_temp.do_not_pay_counts, pg_temp.loan_due, pg_temp.overpayment_balances, pg_temp.loan_due_this_week, pg_temp.loan_repaid_wtd, pg_temp.paid_wtd_before, pg_temp.cand_enriched, pg_temp.payee_baseline_rows, pg_temp.payees_src, pg_temp.payees, pg_temp.payees_enriched, pg_temp.payees_json, pg_temp.cand_payee0, pg_temp.cand_payee, pg_temp.timesheet_case_rollup_effective, pg_temp.finance_case_repaid_wtd, pg_temp.finance_case_recovery_rows_base, pg_temp.manual_debt_recovery_rows, pg_temp.manual_debt_recovery_allocations, pg_temp.manual_debt_recovery_totals, pg_temp.overpayment_recovery_rows, pg_temp.overpayment_recovery_allocations, pg_temp.overpayment_recovery_totals, pg_temp.payment_advance_recovery_rows, pg_temp.payment_advance_recovery_allocations, pg_temp.finance_case_protected_allocations, pg_temp.finance_case_payee_readiness, pg_temp.finance_case_component_rows, pg_temp.finance_case_component_review_rows, pg_temp.finance_case_component_review_rows_effective, pg_temp.finance_case_due_source_amounts, pg_temp.finance_case_component_due_source_base, pg_temp.finance_case_component_due_source_shares, pg_temp.finance_case_component_due_source_allocations, pg_temp.finance_case_component_due_preview_base, pg_temp.finance_case_component_due_preview_allocations, pg_temp.finance_case_taxable_manual_debt_resolution, pg_temp.finance_case_resolution_rollup, pg_temp.canonical_timesheet_lines, pg_temp.timesheet_active_segment_snooze_meta, pg_temp.canonical_timesheet_segment_rows, pg_temp.canonical_timesheet_segment_rollup, pg_temp.canonical_timesheet_presentation_seed, pg_temp.canonical_timesheet_presentation_state, pg_temp.canonical_timesheet_presentation_rows, pg_temp.finance_case_lines, pg_temp.timesheet_canonical_preview_lines, pg_temp.canonical_preview_lines, pg_temp.candidate_preview_line_rollup, pg_temp.candidate_preview_timesheet_rollup, pg_temp.summary_json, pg_temp.timesheet_case_states_flat, pg_temp.finance_case_states_flat, pg_temp.candidate_case_states_flat, pg_temp.candidate_case_states, pg_temp.case_resolution_states_json, pg_temp.finance_candidate_totals, pg_temp.candidate_finance_itemisation, pg_temp.paye_summary_breakdown_json, pg_temp.timesheet_baseline_component_rows, pg_temp.finance_baseline_component_rows, pg_temp.baseline_component_rows_json;

  create temporary table pay_preview_candidate_context on commit drop as
    select
      v_context_json as context_json,
      v_candidate_id as candidate_id,
      v_pay_date as pay_date,
      v_week_ending_cutoff as week_ending_cutoff,
      v_client_id as client_id,
      v_actor_user_id as actor_user_id,
      v_week_start as week_start,
      v_today_uk as today_uk,
      v_pay_eligibility_months_back as pay_eligibility_months_back,
      v_pay_eligibility_weeks_ahead as pay_eligibility_weeks_ahead,
      v_eligibility_from_date as eligibility_from_date,
      v_eligibility_to_date as eligibility_to_date,
      v_vat_rate_pct as vat_rate_pct,
      v_erni_pct as erni_pct,
      v_rail_provider_default as rail_provider_default,
      v_rail_env_default as rail_env_default,
      v_rail_supports_scheduling as rail_supports_scheduling,
      v_rail_supports_name_check as rail_supports_name_check,
      v_rail_supports_auto_execute as rail_supports_auto_execute,
      v_default_schedule_umbrella_local as default_schedule_umbrella_local,
      v_default_schedule_paye_local as default_schedule_paye_local,
      v_funds_warning_hours_json as funds_warning_hours_json,
      v_need_name_check as need_name_check,
      v_requires_payee_map as requires_payee_map,
      v_paye_guardrails as paye_guardrails,
      v_effective_refresh_scope_kind as refresh_scope_kind,
      v_targeted_timesheet_ids_json as targeted_timesheet_ids_json,
      v_linked_timesheet_ids_json as linked_timesheet_ids_json,
      v_lifecycle_reconciliation_mode as lifecycle_reconciliation_mode,
      v_historic_metadata_allowed as historic_metadata_allowed,
      v_signed_finance_reconciliation as signed_finance_reconciliation,
      v_exclude_lifecycle_ineligible_current_payability as exclude_lifecycle_ineligible_current_payability,
      v_authoritative_timesheet_scope as authoritative_timesheet_scope
  ;

  create temporary table targeted_refresh_requested_timesheet_ids on commit drop as
    select distinct
      nullif(btrim(targeted_scope_array.value), '')::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_targeted_timesheet_ids_requested_json, '[]'::jsonb)) as targeted_scope_array(value)
    where nullif(btrim(targeted_scope_array.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ;

  create temporary table linked_refresh_requested_timesheet_ids on commit drop as
    select distinct
      nullif(btrim(linked_scope_array.value), '')::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_linked_timesheet_ids_requested_json, '[]'::jsonb)) as linked_scope_array(value)
    where nullif(btrim(linked_scope_array.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ;

  create temporary table targeted_refresh_rotation_scope on commit drop as
    select
      requested_scope.source_scope_kind,
      requested_scope.requested_timesheet_id as source_requested_timesheet_id,
      rotation_scope_rows.requested_timesheet_id,
      rotation_scope_rows.booking_id,
      rotation_scope_rows.canonical_timesheet_id,
      rotation_scope_rows.family_timesheet_id,
      rotation_scope_rows.family_is_current,
      rotation_scope_rows.family_version,
      rotation_scope_rows.requested_is_canonical
    from (
      select 'TARGETED'::text as source_scope_kind, targeted_requested.timesheet_id as requested_timesheet_id
      from targeted_refresh_requested_timesheet_ids as targeted_requested
      union
      select 'LINKED'::text as source_scope_kind, linked_requested.timesheet_id as requested_timesheet_id
      from linked_refresh_requested_timesheet_ids as linked_requested
      union
      select 'TARGETED'::text as source_scope_kind, nullif(btrim(targeted_context_values.value), '')::uuid as requested_timesheet_id
      from jsonb_array_elements_text(coalesce(v_targeted_timesheet_ids_json, '[]'::jsonb)) as targeted_context_values(value)
      where nullif(btrim(targeted_context_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      union
      select 'LINKED'::text as source_scope_kind, nullif(btrim(linked_context_values.value), '')::uuid as requested_timesheet_id
      from jsonb_array_elements_text(coalesce(v_linked_timesheet_ids_json, '[]'::jsonb)) as linked_context_values(value)
      where nullif(btrim(linked_context_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) as requested_scope
    join public._pay_timesheet_rotation_scope(
      (
        select coalesce(
          array_agg(distinct all_requested_scope.requested_timesheet_id order by all_requested_scope.requested_timesheet_id),
          array[]::uuid[]
        )
        from (
          select targeted_requested.timesheet_id as requested_timesheet_id
          from targeted_refresh_requested_timesheet_ids as targeted_requested
          union
          select linked_requested.timesheet_id as requested_timesheet_id
          from linked_refresh_requested_timesheet_ids as linked_requested
          union
          select nullif(btrim(targeted_context_values.value), '')::uuid as requested_timesheet_id
          from jsonb_array_elements_text(coalesce(v_targeted_timesheet_ids_json, '[]'::jsonb)) as targeted_context_values(value)
          where nullif(btrim(targeted_context_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          union
          select nullif(btrim(linked_context_values.value), '')::uuid as requested_timesheet_id
          from jsonb_array_elements_text(coalesce(v_linked_timesheet_ids_json, '[]'::jsonb)) as linked_context_values(value)
          where nullif(btrim(linked_context_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) as all_requested_scope
        where all_requested_scope.requested_timesheet_id is not null
      )
    ) as rotation_scope_rows
      on rotation_scope_rows.requested_timesheet_id = requested_scope.requested_timesheet_id
  ;

  create temporary table targeted_refresh_scope_projection on commit drop as
    select distinct
      targeted_refresh_rotation_scope.source_scope_kind,
      targeted_refresh_rotation_scope.source_requested_timesheet_id,
      coalesce(targeted_refresh_rotation_scope.canonical_timesheet_id, targeted_refresh_rotation_scope.source_requested_timesheet_id) as canonical_timesheet_id,
      targeted_refresh_rotation_scope.family_timesheet_id,
      targeted_refresh_rotation_scope.family_is_current,
      targeted_refresh_rotation_scope.family_version,
      targeted_refresh_rotation_scope.booking_id,
      coalesce(targeted_refresh_rotation_scope.requested_is_canonical, false) as requested_is_canonical
    from targeted_refresh_rotation_scope
    where targeted_refresh_rotation_scope.source_requested_timesheet_id is not null
      and targeted_refresh_rotation_scope.family_timesheet_id is not null
  ;

  create temporary table targeted_refresh_timesheet_ids on commit drop as
    select distinct
      targeted_refresh_scope_projection.canonical_timesheet_id as timesheet_id
    from targeted_refresh_scope_projection
    where targeted_refresh_scope_projection.source_scope_kind = 'TARGETED'
      and targeted_refresh_scope_projection.canonical_timesheet_id is not null
  ;

  create temporary table linked_refresh_timesheet_ids on commit drop as
    select distinct
      targeted_refresh_scope_projection.canonical_timesheet_id as timesheet_id
    from targeted_refresh_scope_projection
    where targeted_refresh_scope_projection.source_scope_kind = 'LINKED'
      and targeted_refresh_scope_projection.canonical_timesheet_id is not null
  ;

  create temporary table targeted_refresh_family_timesheet_ids on commit drop as
    select distinct
      targeted_refresh_scope_projection.family_timesheet_id
    from targeted_refresh_scope_projection
    where targeted_refresh_scope_projection.source_scope_kind = 'TARGETED'
      and targeted_refresh_scope_projection.family_timesheet_id is not null
  ;

  create temporary table linked_refresh_family_timesheet_ids on commit drop as
    select distinct
      targeted_refresh_scope_projection.family_timesheet_id
    from targeted_refresh_scope_projection
    where targeted_refresh_scope_projection.source_scope_kind = 'LINKED'
      and targeted_refresh_scope_projection.family_timesheet_id is not null
  ;

  create temporary table targeted_refresh_all_family_timesheet_ids on commit drop as
    select distinct targeted_refresh_family_timesheet_ids.family_timesheet_id
    from targeted_refresh_family_timesheet_ids
    union
    select distinct linked_refresh_family_timesheet_ids.family_timesheet_id
    from linked_refresh_family_timesheet_ids
  ;

  create temporary table targeted_refresh_all_family_projection on commit drop as
    select distinct
      targeted_refresh_scope_projection.canonical_timesheet_id,
      targeted_refresh_scope_projection.family_timesheet_id,
      targeted_refresh_scope_projection.family_is_current,
      targeted_refresh_scope_projection.family_version,
      targeted_refresh_scope_projection.booking_id
    from targeted_refresh_scope_projection
    where targeted_refresh_scope_projection.canonical_timesheet_id is not null
      and targeted_refresh_scope_projection.family_timesheet_id is not null
  ;

  create temporary table targeted_refresh_all_baseline_projection on commit drop as
    select distinct on (targeted_refresh_all_family_projection.canonical_timesheet_id)
      targeted_refresh_all_family_projection.canonical_timesheet_id,
      targeted_refresh_all_family_projection.family_timesheet_id,
      targeted_refresh_all_family_projection.family_is_current,
      targeted_refresh_all_family_projection.family_version,
      targeted_refresh_all_family_projection.booking_id
    from targeted_refresh_all_family_projection
    join public.timesheet_pay_state as baseline_projection_state
      on baseline_projection_state.timesheet_id = targeted_refresh_all_family_projection.family_timesheet_id
    order by
      targeted_refresh_all_family_projection.canonical_timesheet_id,
      (targeted_refresh_all_family_projection.family_timesheet_id = targeted_refresh_all_family_projection.canonical_timesheet_id) desc,
      targeted_refresh_all_family_projection.family_is_current desc nulls last,
      targeted_refresh_all_family_projection.family_version desc nulls last,
      targeted_refresh_all_family_projection.family_timesheet_id
  ;

  create temporary table targeted_refresh_all_timesheet_ids on commit drop as
    select distinct x.timesheet_id
    from (
      select trti.timesheet_id
      from targeted_refresh_timesheet_ids trti
      union
      select lrti.timesheet_id
      from linked_refresh_timesheet_ids lrti
    ) as x
    where x.timesheet_id is not null
  ;
  create temporary table active_snoozes on commit drop as
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
          and s.cancelled_at_utc is null
          and s.candidate_id = v_candidate_id
          and (
            s.snooze_until_date is null
            or s.snooze_until_date >= v_today_uk
          )
          and (
            v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            or s.timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
            or s.timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
            or s.timesheet_id in (select trf.family_timesheet_id from targeted_refresh_all_family_timesheet_ids trf)
            or (
              s.booking_id is not null
              and exists (
                select 1
                from public.timesheets ts_scope
                where ts_scope.booking_id = s.booking_id
                  and (
                    ts_scope.timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
                    or ts_scope.timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
                  )
              )
            )
            or (s.timesheet_id is null and s.booking_id is null)
          )
  
  ;

  create temporary table active_timesheet_payment_snoozes on commit drop as
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
  
  ;

  create temporary table active_segment_snoozes on commit drop as
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
  
  ;

  create temporary table active_timesheet_payment_overrides on commit drop as
        select
          coalesce(tpo_family_projection.canonical_timesheet_id, tpo.timesheet_id) as timesheet_id,
          tpo.candidate_id,
          tpo.id as override_id,
          tpo.reason as override_reason,
          tpo.created_at_utc
        from public.timesheet_payment_overrides tpo
        left join targeted_refresh_all_family_projection as tpo_family_projection
          on tpo_family_projection.family_timesheet_id = tpo.timesheet_id
        where tpo.cleared_at_utc is null
          and tpo.consumed_at_utc is null
          and tpo.consumed_by_pay_batch_id is null
          and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
          and tpo.candidate_id = v_candidate_id
          and (
            v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            or tpo.timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
            or tpo.timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
            or tpo.timesheet_id in (select trf.family_timesheet_id from targeted_refresh_all_family_timesheet_ids trf)
          )
  
  ;

  create temporary table force_include on commit drop as
        select distinct
          coalesce(tpo_family_projection.canonical_timesheet_id, tpo.timesheet_id) as timesheet_id
        from public.timesheet_payment_overrides tpo
        left join targeted_refresh_all_family_projection as tpo_family_projection
          on tpo_family_projection.family_timesheet_id = tpo.timesheet_id
        where tpo.cleared_at_utc is null
          and tpo.consumed_at_utc is null
          and tpo.consumed_by_pay_batch_id is null
          and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
          and tpo.timesheet_id is not null
          and tpo.candidate_id = v_candidate_id
          and (
            v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            or tpo.timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
            or tpo.timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
            or tpo.timesheet_id in (select trf.family_timesheet_id from targeted_refresh_all_family_timesheet_ids trf)
          )
  
  ;

  create temporary table reserved_batch_items on commit drop as
        -- Items are considered "reserved" if they belong to an ACTIVE (non-cancelled, non-settled) batch.
        -- Policy X/source-target rule: entitlement reservations must subtract the positive ORIGINAL SOURCE
        -- entitlement amount, not the resolved target payout amount. Finance bookkeeping item types are excluded.
        -- Rotation rule: discover candidate/scoped reservation items by family id, project them to the canonical
        -- current timesheet id for pre-draft preview, and resolve artifact keys in one set-based helper call.
        with reserved_batch_item_candidates as (
          select distinct
            reserved_pay_batch_item.id as pay_batch_item_id,
            reserved_pay_batch_candidate.pay_batch_id as pay_batch_id,
            reserved_pay_batch_item.timesheet_id as source_timesheet_id,
            reserved_pay_batch_item.segment_key as segment_key,
            reserved_pay_batch_item.source_ref as source_ref,
            reserved_pay_batch_item.item_type as item_type,
            round(public._pay_batch_item_source_reservation_amount_ex_vat(reserved_pay_batch_item.id), 2) as amount_ex_vat
          from public.pay_batch_items as reserved_pay_batch_item
          join public.pay_batch_candidates as reserved_pay_batch_candidate
            on reserved_pay_batch_candidate.id = reserved_pay_batch_item.pay_batch_candidate_id
          join public.pay_batches as reserved_pay_batch
            on reserved_pay_batch.id = reserved_pay_batch_candidate.pay_batch_id
          left join public.pay_bank_transfers as reserved_pay_bank_transfer
            on reserved_pay_bank_transfer.id = reserved_pay_batch_item.pay_bank_transfer_id
          where reserved_pay_batch_item.timesheet_id is not null
            and reserved_pay_batch_candidate.candidate_id = v_candidate_id
            and (
              v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
              or reserved_pay_batch_item.timesheet_id in (
                select targeted_family_scope.family_timesheet_id
                from targeted_refresh_all_family_timesheet_ids as targeted_family_scope
              )
            )
            and upper(coalesce(reserved_pay_batch_item.pay_channel,'')) in ('PAYE','UMBRELLA')
            and reserved_pay_batch_item.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
            and public._pay_batch_item_source_reservation_amount_ex_vat(reserved_pay_batch_item.id) is not null
            and coalesce(reserved_pay_batch_item.is_voided, false) = false
            and not exists (
              select 1
              from public.pay_payment_correction_items as applied_corrections
              where applied_corrections.pay_batch_item_id = reserved_pay_batch_item.id
                and applied_corrections.status = 'APPLIED'
                and applied_corrections.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
            )
            and not (
              upper(btrim(coalesce(reserved_pay_batch_candidate.settlement_status, ''))) = 'SETTLED'
              or reserved_pay_batch_candidate.settled_at_utc is not null
              or upper(btrim(coalesce(reserved_pay_bank_transfer.status, ''))) = 'COMPLETED'
              or reserved_pay_bank_transfer.completed_at_utc is not null
            )
            and (
              public._pay_batch_status_is_active_reservation(reserved_pay_batch.status)
              or exists (
                select 1
                from public.pay_payment_correction_requests as open_correction_requests
                join lateral public._pay_payment_correction_selected_items(
                  open_correction_requests.pay_batch_id,
                  case
                    when jsonb_typeof(open_correction_requests.plan_json->'selected_pay_batch_item_ids') = 'array' then
                      coalesce(open_correction_requests.selection_json, '{}'::jsonb)
                      || jsonb_build_object(
                        'pay_batch_item_ids', open_correction_requests.plan_json->'selected_pay_batch_item_ids',
                        'expected_pay_batch_item_ids', open_correction_requests.plan_json->'selected_pay_batch_item_ids'
                      )
                    when jsonb_typeof(open_correction_requests.plan_json#>'{selection,selected_pay_batch_item_ids}') = 'array' then
                      coalesce(open_correction_requests.selection_json, '{}'::jsonb)
                      || jsonb_build_object(
                        'pay_batch_item_ids', open_correction_requests.plan_json#>'{selection,selected_pay_batch_item_ids}',
                        'expected_pay_batch_item_ids', open_correction_requests.plan_json#>'{selection,selected_pay_batch_item_ids}'
                      )
                    when jsonb_typeof(open_correction_requests.plan_json#>'{selection,pay_batch_item_ids}') = 'array' then
                      coalesce(open_correction_requests.selection_json, '{}'::jsonb)
                      || jsonb_build_object(
                        'pay_batch_item_ids', open_correction_requests.plan_json#>'{selection,pay_batch_item_ids}',
                        'expected_pay_batch_item_ids', open_correction_requests.plan_json#>'{selection,pay_batch_item_ids}'
                      )
                    else
                      coalesce(open_correction_requests.selection_json, '{}'::jsonb)
                  end,
                  false
                ) as open_correction_selected_items
                  on open_correction_selected_items.pay_batch_id = open_correction_requests.pay_batch_id
                where open_correction_requests.pay_batch_id = reserved_pay_batch_candidate.pay_batch_id
                  and open_correction_requests.status in ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING','BLOCKED')
                  and open_correction_requests.correction_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')
                  and open_correction_selected_items.pay_batch_item_id = reserved_pay_batch_item.id
              )
              or exists (
                select 1
                from public.pay_bank_transfer_events as unresolved_terminal_failure_events
                where unresolved_terminal_failure_events.pay_batch_id = reserved_pay_batch_candidate.pay_batch_id
                  and unresolved_terminal_failure_events.pay_bank_transfer_id = reserved_pay_batch_item.pay_bank_transfer_id
                  and unresolved_terminal_failure_events.mapping_status = 'MATCHED'
                  and unresolved_terminal_failure_events.normalised_state in ('FAILED','DECLINED','REJECTED','CANCELLED')
                  and not exists (
                    select 1
                    from public.pay_payment_correction_items as applied_failure_corrections
                    where applied_failure_corrections.pay_batch_item_id = reserved_pay_batch_item.id
                      and applied_failure_corrections.status = 'APPLIED'
                      and applied_failure_corrections.correction_item_kind in ('NO_MONEY_UNWIND','PRE_BANK_CANCEL','SETTLED_REVERSAL')
                  )
              )
            )
        ),
        reserved_batch_item_rotation_scope as (
          select
            rotation_scope_rows.requested_timesheet_id,
            rotation_scope_rows.canonical_timesheet_id,
            rotation_scope_rows.family_timesheet_id,
            rotation_scope_rows.family_is_current,
            rotation_scope_rows.family_version
          from public._pay_timesheet_rotation_scope(
            (
              select coalesce(
                array_agg(distinct reserved_batch_item_candidates.source_timesheet_id order by reserved_batch_item_candidates.source_timesheet_id),
                array[]::uuid[]
              )
              from reserved_batch_item_candidates
              where reserved_batch_item_candidates.source_timesheet_id is not null
            )
          ) as rotation_scope_rows
        ),
        reserved_batch_item_projection as (
          select distinct on (reserved_batch_item_rotation_scope.requested_timesheet_id)
            reserved_batch_item_rotation_scope.requested_timesheet_id as requested_timesheet_id,
            coalesce(
              reserved_batch_item_rotation_scope.canonical_timesheet_id,
              reserved_batch_item_rotation_scope.requested_timesheet_id
            ) as projected_timesheet_id
          from reserved_batch_item_rotation_scope
          where reserved_batch_item_rotation_scope.requested_timesheet_id is not null
          order by
            reserved_batch_item_rotation_scope.requested_timesheet_id,
            reserved_batch_item_rotation_scope.family_is_current desc nulls last,
            reserved_batch_item_rotation_scope.family_version desc nulls last,
            reserved_batch_item_rotation_scope.family_timesheet_id
        ),
        reserved_batch_item_projected as (
          select
            reserved_batch_item_candidates.pay_batch_item_id,
            reserved_batch_item_candidates.pay_batch_id,
            coalesce(
              reserved_batch_item_projection.projected_timesheet_id,
              reserved_batch_item_candidates.source_timesheet_id
            ) as projected_timesheet_id,
            reserved_batch_item_candidates.segment_key,
            reserved_batch_item_candidates.source_ref,
            reserved_batch_item_candidates.item_type,
            reserved_batch_item_candidates.amount_ex_vat
          from reserved_batch_item_candidates
          left join reserved_batch_item_projection
            on reserved_batch_item_projection.requested_timesheet_id = reserved_batch_item_candidates.source_timesheet_id
        ),
        reserved_batch_item_id_array as (
          select
            case
              when count(*) = 0 then array['00000000-0000-0000-0000-000000000000'::uuid]
              else array_agg(reserved_batch_item_projected.pay_batch_item_id order by reserved_batch_item_projected.pay_batch_item_id)
            end as pay_batch_item_ids
          from reserved_batch_item_projected
        ),
        reserved_batch_item_components as (
          select distinct
            economic_component_rows.pay_batch_item_id,
            economic_component_rows.key_type,
            economic_component_rows.key_value,
            economic_component_rows.key_resolution_source,
            economic_component_rows.key_resolution_failure_reason
          from reserved_batch_item_id_array
          join lateral public._pay_batch_item_economic_components(
            p_pay_batch_id => null::uuid,
            p_pay_batch_item_ids => reserved_batch_item_id_array.pay_batch_item_ids
          ) as economic_component_rows
            on true
        )
        select
          reserved_batch_item_projected.pay_batch_item_id as pay_batch_item_id,
          reserved_batch_item_projected.pay_batch_id as pay_batch_id,
          reserved_batch_item_projected.projected_timesheet_id as timesheet_id,
          reserved_batch_item_projected.segment_key as segment_key,
          -- Normalise segment_id (some legacy items store it in source_ref 'seg:<id>')
          nullif(
            btrim(coalesce(
              case
                when nullif(btrim(coalesce(reserved_batch_item_projected.segment_key,'')), '') is not null then reserved_batch_item_projected.segment_key
                when nullif(btrim(coalesce(reserved_batch_item_projected.source_ref,'')), '') like 'seg:%' then split_part(reserved_batch_item_projected.source_ref, ':', 2)
                else null
              end,
              ''
            )),
            ''
          ) as segment_id_norm,
          reserved_batch_item_projected.source_ref as source_ref,
          reserved_batch_item_projected.item_type as item_type,
          reserved_batch_item_components.key_type as component_key_type,
          reserved_batch_item_components.key_value as component_key_value,
          reserved_batch_item_components.key_resolution_source as key_resolution_source,
          reserved_batch_item_components.key_resolution_failure_reason as key_resolution_failure_reason,
          reserved_batch_item_projected.amount_ex_vat as amount_ex_vat
        from reserved_batch_item_projected
        left join reserved_batch_item_components
          on reserved_batch_item_components.pay_batch_item_id = reserved_batch_item_projected.pay_batch_item_id

  ;

  create temporary table reserved_by_source_ref on commit drop as
        select
          rbi.timesheet_id,
          rbi.source_ref,
          round(sum(rbi.amount_ex_vat),2) as reserved_amount_ex_vat
        from reserved_batch_items rbi
        where rbi.source_ref is not null
          and btrim(coalesce(rbi.source_ref,'')) <> ''
        group by rbi.timesheet_id, rbi.source_ref
  
  ;

  create temporary table reserved_total_by_timesheet on commit drop as
        select
          rbi.timesheet_id,
          round(sum(rbi.amount_ex_vat),2) as reserved_total_ex_vat
        from reserved_batch_items rbi
        group by rbi.timesheet_id
  
  ;

  create temporary table reserved_segment_key_map on commit drop as
        select
          rbi.timesheet_id,
          rbi.segment_id_norm as segment_id_norm,
          rbi.component_key_type as component_key_type,
          rbi.component_key_value as component_key_value
        from reserved_batch_items rbi
        where rbi.item_type = 'SEGMENT_DELTA'
          and rbi.component_key_type in ('TS_DAY','TS_TOTAL')
          and rbi.component_key_value is not null
          and btrim(coalesce(rbi.component_key_value,'')) <> ''
          and (rbi.component_key_type <> 'TS_DAY' or rbi.component_key_value ~ '^\d{4}-\d{2}-\d{2}$')
  
  ;

  create temporary table reserved_segment_sums on commit drop as
        select
          rbi.timesheet_id,
          rbi.component_key_type,
          rbi.component_key_value,
          round(sum(rbi.amount_ex_vat),2) as reserved_amount_ex_vat
        from reserved_batch_items rbi
        where rbi.item_type = 'SEGMENT_DELTA'
          and rbi.component_key_type in ('TS_DAY','TS_TOTAL')
          and rbi.component_key_value is not null
          and btrim(coalesce(rbi.component_key_value,'')) <> ''
          and (rbi.component_key_type <> 'TS_DAY' or rbi.component_key_value ~ '^\d{4}-\d{2}-\d{2}$')
        group by rbi.timesheet_id, rbi.component_key_type, rbi.component_key_value
  
  ;

  create temporary table reserved_preview_segment_ords on commit drop as
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
  
  ;

  create temporary table reserved_additional_by_code on commit drop as
        select
          rbi.timesheet_id,
          nullif(btrim(coalesce(bd.bucket_code,'')), '') as code,
          round(
            sum(
              case
                when nullif(btrim(coalesce(bd.meta_json->>'source_amount_ex_vat','')), '') ~ '^-?\d+(\.\d+)?$'
                  then abs((nullif(btrim(coalesce(bd.meta_json->>'source_amount_ex_vat','')), ''))::numeric)
                when nullif(btrim(coalesce(bd.meta_json->>'source_pay_ex_vat','')), '') ~ '^-?\d+(\.\d+)?$'
                  then abs((nullif(btrim(coalesce(bd.meta_json->>'source_pay_ex_vat','')), ''))::numeric)
                else abs(coalesce(bd.amount_ex_vat,0))
              end
            ),
            2
          ) as reserved_amount_ex_vat
        from reserved_batch_items rbi
        join public.pay_batch_item_breakdowns bd
          on bd.pay_batch_item_id = rbi.pay_batch_item_id
        where rbi.item_type in ('ADJUSTMENT_DELTA','SEGMENT_DELTA')
          and bd.line_kind = 'ADDITIONAL_UNIT'
          and bd.bucket_code is not null
          and btrim(coalesce(bd.bucket_code,'')) <> ''
        group by rbi.timesheet_id, nullif(btrim(coalesce(bd.bucket_code,'')), '')
  
  ;

  create temporary table eligible_tsfin on commit drop as
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
          coalesce((private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)#>>'{values,require_reference_to_pay}')::boolean,false) as require_reference_to_pay,

          -- Timesheet advance: force-include even if unauthorised/outside window
          (fi.timesheet_id is not null) as is_forced_advance,

          tf.invoice_breakdown_json,

          ts.sheet_scope as ts_sheet_scope,
          ts.worked_start_iso as ts_worked_start_iso,
          ts.worked_end_iso as ts_worked_end_iso,
          ts.break_start_iso as ts_break_start_iso,
          ts.break_end_iso as ts_break_end_iso,
          ts.break_minutes as ts_break_minutes,
          ts.actual_schedule_json as ts_actual_schedule_json,
          tf.worked_start_iso as tf_worked_start_iso,
          tf.worked_end_iso as tf_worked_end_iso,
          tf.break_start_iso as tf_break_start_iso,
          tf.break_end_iso as tf_break_end_iso,
          tf.break_minutes as tf_break_minutes,
          tf.actual_schedule_json as tf_actual_schedule_json,
          coalesce(tf.worked_start_iso, ts.worked_start_iso) as effective_worked_start_iso,
          coalesce(tf.worked_end_iso, ts.worked_end_iso) as effective_worked_end_iso,
          coalesce(tf.break_start_iso, ts.break_start_iso) as effective_break_start_iso,
          coalesce(tf.break_end_iso, ts.break_end_iso) as effective_break_end_iso,
          coalesce(tf.break_minutes, ts.break_minutes) as effective_break_minutes,
          case
            when jsonb_typeof(tf.actual_schedule_json) = 'object' then tf.actual_schedule_json
            when jsonb_typeof(tf.actual_schedule_json) = 'array' then (
              select tf_sched_item.value
              from jsonb_array_elements(tf.actual_schedule_json) as tf_sched_item(value)
              where tf_sched_item.value is not null
                and jsonb_typeof(tf_sched_item.value) = 'object'
              limit 1
            )
            when jsonb_typeof(ts.actual_schedule_json) = 'object' then ts.actual_schedule_json
            when jsonb_typeof(ts.actual_schedule_json) = 'array' then (
              select ts_sched_item.value
              from jsonb_array_elements(ts.actual_schedule_json) as ts_sched_item(value)
              where ts_sched_item.value is not null
                and jsonb_typeof(ts_sched_item.value) = 'object'
              limit 1
            )
            else null::jsonb
          end as effective_daily_schedule_json,

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
          and ts.archived_at_utc is null
          and coalesce(tf.pay_on_hold,false) = false
          and coalesce(tf.has_rate_issue,false) = false
          and coalesce(tf.has_pay_channel_issue,false) = false
          and upper(coalesce(tf.processing_status::text,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
          and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')

          -- ✅ Eligibility: targeted rows only for TARGETED_TIMESHEETS; current live authorised rows / force include for CANDIDATE_FULL_LIVE
          and (
            (
              v_effective_refresh_scope_kind = 'TARGETED_TIMESHEETS'
              and (
                (
                  tf.timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
                  or tf.timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
                )
                and (
                  (
                    ts.authorised_at_server is not null
                    and ts.revoked_at is null
                  )
                  or fi.timesheet_id is not null
                )
              )
            )
            or (
              v_effective_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
              and (
                (
                  ts.authorised_at_server is not null
                  and ts.revoked_at is null
                )
                or fi.timesheet_id is not null
              )
            )
          )

          -- ✅ Optional filters (default ALL/ALL when NULL)
          and (v_candidate_id is null or tf.candidate_id = v_candidate_id)
          and (v_client_id is null or tf.client_id = v_client_id)
  
  ;

  create temporary table debted_overpayment_cases on commit drop as
        select
          pa.candidate_id,
          coalesce(pay_advance_family_projection.canonical_timesheet_id, pa.linked_timesheet_id) as timesheet_id,
          pa.baseline_signature
        from public.pay_advances pa
        left join targeted_refresh_all_family_projection as pay_advance_family_projection
          on pay_advance_family_projection.family_timesheet_id = pa.linked_timesheet_id
        where pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
          and pa.status in ('ACTIVE'::public.pay_advance_status_enum, 'PAID_OFF'::public.pay_advance_status_enum)
          and pa.candidate_id = v_candidate_id
          and pa.linked_timesheet_id is not null
          and (
            v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            or pa.linked_timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
            or pa.linked_timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
            or pa.linked_timesheet_id in (select trf.family_timesheet_id from targeted_refresh_all_family_timesheet_ids trf)
          )
        group by
          pa.candidate_id,
          coalesce(pay_advance_family_projection.canonical_timesheet_id, pa.linked_timesheet_id),
          pa.baseline_signature
  
  ;

  create temporary table umb_map on commit drop as
        select
          u.id as umbrella_id,
          coalesce(u.enabled,false) as umb_enabled,
          coalesce(u.vat_chargeable,false) as vat_chargeable,
          -- ✅ Bank readiness (umbrella)
          u.bank_details_hash as umb_bank_hash
        from public.umbrellas u
  
  ;

  create temporary table adjustment_family_projection on commit drop as
        with eligible_current_timesheet_ids as (
          select distinct
            eligible_tsfin.timesheet_id as timesheet_id
          from eligible_tsfin
          where eligible_tsfin.timesheet_id is not null
        ),
        full_live_rotation_scope as (
          select
            full_live_scope.requested_timesheet_id,
            full_live_scope.canonical_timesheet_id,
            full_live_scope.family_timesheet_id,
            full_live_scope.family_is_current,
            full_live_scope.family_version,
            full_live_scope.booking_id
          from public._pay_timesheet_rotation_scope(
            case
              when v_effective_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' then (
                select coalesce(
                  array_agg(distinct eligible_current_timesheet_ids.timesheet_id order by eligible_current_timesheet_ids.timesheet_id),
                  array[]::uuid[]
                )
                from eligible_current_timesheet_ids
              )
              else array[]::uuid[]
            end
          ) as full_live_scope
        )
        select distinct
          adjustment_projection_rows.canonical_timesheet_id,
          adjustment_projection_rows.family_timesheet_id
        from (
          select
            eligible_tsfin.timesheet_id as canonical_timesheet_id,
            targeted_refresh_all_family_projection.family_timesheet_id as family_timesheet_id
          from eligible_tsfin
          join targeted_refresh_all_family_projection
            on targeted_refresh_all_family_projection.canonical_timesheet_id = eligible_tsfin.timesheet_id
          where v_effective_refresh_scope_kind = 'TARGETED_TIMESHEETS'
            and eligible_tsfin.timesheet_id is not null
            and targeted_refresh_all_family_projection.family_timesheet_id is not null

          union all

          select
            coalesce(full_live_rotation_scope.canonical_timesheet_id, full_live_rotation_scope.requested_timesheet_id) as canonical_timesheet_id,
            full_live_rotation_scope.family_timesheet_id as family_timesheet_id
          from full_live_rotation_scope
          join eligible_current_timesheet_ids
            on eligible_current_timesheet_ids.timesheet_id = full_live_rotation_scope.requested_timesheet_id
          where v_effective_refresh_scope_kind = 'CANDIDATE_FULL_LIVE'
            and coalesce(full_live_rotation_scope.canonical_timesheet_id, full_live_rotation_scope.requested_timesheet_id) is not null
            and full_live_rotation_scope.family_timesheet_id is not null
        ) as adjustment_projection_rows
        where adjustment_projection_rows.canonical_timesheet_id is not null
          and adjustment_projection_rows.family_timesheet_id is not null
  
  ;

  create temporary table adj on commit drop as
        select distinct on (
          adjustment_family_projection.canonical_timesheet_id,
          adjustment_rows.id
        )
          adjustment_rows.id as adj_id,
          adjustment_family_projection.canonical_timesheet_id as timesheet_id,
          adjustment_rows.timesheet_id as source_timesheet_id,
          adjustment_rows.candidate_id,
          adjustment_rows.created_at as adjustment_created_at,
          round(coalesce(adjustment_rows.delta_pay_ex_vat,0),2) as delta_pay_ex_vat
        from adjustment_family_projection
        join public.ts_pay_adjustments as adjustment_rows
          on adjustment_rows.timesheet_id = adjustment_family_projection.family_timesheet_id
        where adjustment_rows.as_advance = false
          and adjustment_rows.timesheet_id is not null
          and adjustment_rows.id is not null
          and adjustment_rows.candidate_id = v_candidate_id
        order by
          adjustment_family_projection.canonical_timesheet_id,
          adjustment_rows.id,
          adjustment_rows.created_at nulls last,
          adjustment_rows.timesheet_id
  
  ;

  create temporary table ts_current on commit drop as
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
                  'nhsp_shift_id', nullif(btrim(coalesce(seg->>'nhsp_shift_id','')), ''),
                  'shift_id', coalesce(
                    nullif(btrim(coalesce(seg->>'shift_id','')), ''),
                    nullif(btrim(coalesce(seg->>'nhsp_shift_id','')), '')
                  ),
                  'external_row_key', nullif(btrim(coalesce(seg->>'external_row_key','')), ''),
                  'hr_request_id', nullif(btrim(coalesce(seg->>'hr_request_id','')), ''),
                  'request_id', coalesce(
                    nullif(btrim(coalesce(seg->>'request_id','')), ''),
                    nullif(btrim(coalesce(seg->>'hr_request_id','')), '')
                  ),
                  'source_system', nullif(btrim(coalesce(seg->>'source_system','')), ''),
                  'latest_import_id', nullif(btrim(coalesce(seg->>'latest_import_id','')), ''),
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
            else case
              when upper(coalesce(e.ts_sheet_scope::text,'')) = 'DAILY'
              then jsonb_build_array(
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
                  'date', case
                    when e.effective_worked_start_iso is not null then ((e.effective_worked_start_iso at time zone 'Europe/London')::date)::text
                    else coalesce(
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'date','')), ''),
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'work_date','')), ''),
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'ymd','')), ''),
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'date_ymd','')), '')
                    )
                  end,
                  'segment_key', ('ts:' || e.timesheet_id::text),
                  'segment_stable_key', ('timesheet:' || coalesce(e.ts_booking_id, e.timesheet_id::text)),
                  'start_utc', case
                    when e.effective_worked_start_iso is not null then e.effective_worked_start_iso::text
                    else null
                  end,
                  'end_utc', case
                    when e.effective_worked_end_iso is not null then e.effective_worked_end_iso::text
                    else null
                  end,
                  'start', case
                    when e.effective_worked_start_iso is not null then to_char((e.effective_worked_start_iso at time zone 'Europe/London'), 'HH24:MI')
                    else coalesce(
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'start','')), ''),
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'worked_start','')), '')
                    )
                  end,
                  'end', case
                    when e.effective_worked_end_iso is not null then to_char((e.effective_worked_end_iso at time zone 'Europe/London'), 'HH24:MI')
                    else coalesce(
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'end','')), ''),
                      nullif(btrim(coalesce(e.effective_daily_schedule_json->>'worked_end','')), '')
                    )
                  end,
                  'break_start', case
                    when e.effective_break_start_iso is not null then to_char((e.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI')
                    else nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_start','')), '')
                  end,
                  'break_end', case
                    when e.effective_break_end_iso is not null then to_char((e.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI')
                    else nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_end','')), '')
                  end,
                  'break_mins', coalesce(
                    e.effective_break_minutes::numeric,
                    nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                    nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                    nullif(btrim(coalesce(e.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                    nullif(btrim(coalesce(e.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
                    case
                      when e.effective_break_start_iso is not null and e.effective_break_end_iso is not null
                        then greatest(
                          0::numeric,
                          round((extract(epoch from (e.effective_break_end_iso - e.effective_break_start_iso)) / 60.0)::numeric, 0)
                        )
                      else null::numeric
                    end
                  ),
                  'breaks', case
                    when jsonb_typeof(e.effective_daily_schedule_json->'breaks') = 'array' then e.effective_daily_schedule_json->'breaks'
                    when e.effective_break_start_iso is not null and e.effective_break_end_iso is not null
                      then jsonb_build_array(
                        jsonb_build_object(
                          'start', to_char((e.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI'),
                          'end', to_char((e.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI'),
                          'break_mins', coalesce(
                            e.effective_break_minutes::numeric,
                            case
                              when e.effective_break_start_iso is not null and e.effective_break_end_iso is not null
                                then greatest(
                                  0::numeric,
                                  round((extract(epoch from (e.effective_break_end_iso - e.effective_break_start_iso)) / 60.0)::numeric, 0)
                                )
                              else null::numeric
                            end
                          )
                        )
                      )
                    when nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_start','')), '') is not null
                     and nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_end','')), '') is not null
                      then jsonb_build_array(
                        jsonb_build_object(
                          'start', nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_start','')), ''),
                          'end', nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_end','')), ''),
                          'break_mins', coalesce(
                            e.effective_break_minutes::numeric,
                            nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                            nullif(btrim(coalesce(e.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                            nullif(btrim(coalesce(e.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                            nullif(btrim(coalesce(e.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                          )
                        )
                      )
                    else '[]'::jsonb
                  end,
                  'client_name', e.ts_client_name,
                  'role', e.ts_role,
                  'band', e.ts_band
                )
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
            end
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

          -- Canonical additional truth is the TSFIN top-level additional bucket plus
          -- additional_units_json. SEGMENTS invoice_breakdown_json.additional is not a
          -- Banking Pay entitlement source: in expense-only manual rows it can be a
          -- catch-all presentation artefact for non-segment totals.
          round(coalesce(e.additional_pay_ex_vat,0),2) as current_additional_pay_ex_vat,
          round(coalesce(e.additional_charge_ex_vat,0),2) as current_additional_charge_ex_vat,
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
                order by a.adjustment_created_at nulls last, a.adj_id
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
  
  ;

  create temporary table targeted_baseline_only_ts_current on commit drop as
        select
          coalesce(tf_any.candidate_id, v_candidate_id) as candidate_id,
          tri.canonical_timesheet_id as timesheet_id,
          tf_any.id as tsfin_id,
          tf_any.client_id as client_id,
          ts.booking_id as ts_booking_id,
          coalesce(tf_any.role, con.role, ts.job_title_norm) as ts_role,
          coalesce(tf_any.band, con.band, ts.band) as ts_band,
          ts.week_ending_date as ts_week_ending_date,
          cl.name as ts_client_name,
          upper(coalesce(tf_any.pay_method, cand.pay_method, '')) as ts_pay_method,
          upper(coalesce(cand.pay_method, '')) as cand_pay_method,
          cand.tms_ref as cand_tms_ref,
          cand.display_name as cand_display_name,
          cand.umbrella_id as cand_umbrella_id,
          cand.bank_details_hash as cand_bank_hash,
          ts.reference_number,
          coalesce((private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)#>>'{values,require_reference_to_pay}')::boolean,false) as require_reference_to_pay,
          false as is_forced_advance,
          coalesce(um.vat_chargeable,false) as umb_vat_chargeable,
          coalesce(um.umb_enabled,false) as umb_enabled,
          um.umb_bank_hash,
          case
            when upper(coalesce(ts.sheet_scope::text,'')) = 'DAILY'
            then jsonb_build_array(
              jsonb_build_object(
                'segment_id', ('ts:' || tri.canonical_timesheet_id::text),
                'pay_amount', 0,
                'charge_amount', 0,
                'units', 0,
                'hours', 0,
                'hours_day', 0,
                'hours_night', 0,
                'hours_sat', 0,
                'hours_sun', 0,
                'hours_bh', 0,
                'exclude_from_pay', false,
                'ref_num', nullif(btrim(coalesce(ts.reference_number,'')), ''),
                'date', case
                  when daily_sched.effective_worked_start_iso is not null then ((daily_sched.effective_worked_start_iso at time zone 'Europe/London')::date)::text
                  else coalesce(
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'date','')), ''),
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'work_date','')), ''),
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'ymd','')), ''),
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'date_ymd','')), '')
                  )
                end,
                'segment_key', ('ts:' || tri.canonical_timesheet_id::text),
                'segment_stable_key', ('timesheet:' || coalesce(ts.booking_id, tri.canonical_timesheet_id::text)),
                'start_utc', case
                  when daily_sched.effective_worked_start_iso is not null then daily_sched.effective_worked_start_iso::text
                  else null
                end,
                'end_utc', case
                  when daily_sched.effective_worked_end_iso is not null then daily_sched.effective_worked_end_iso::text
                  else null
                end,
                'start', case
                  when daily_sched.effective_worked_start_iso is not null then to_char((daily_sched.effective_worked_start_iso at time zone 'Europe/London'), 'HH24:MI')
                  else coalesce(
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'start','')), ''),
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'worked_start','')), '')
                  )
                end,
                'end', case
                  when daily_sched.effective_worked_end_iso is not null then to_char((daily_sched.effective_worked_end_iso at time zone 'Europe/London'), 'HH24:MI')
                  else coalesce(
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'end','')), ''),
                    nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'worked_end','')), '')
                  )
                end,
                'break_start', case
                  when daily_sched.effective_break_start_iso is not null then to_char((daily_sched.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI')
                  else nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_start','')), '')
                end,
                'break_end', case
                  when daily_sched.effective_break_end_iso is not null then to_char((daily_sched.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI')
                  else nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_end','')), '')
                end,
                'break_mins', coalesce(
                  daily_sched.effective_break_minutes::numeric,
                  nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                  nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                  nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                  nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
                  case
                    when daily_sched.effective_break_start_iso is not null and daily_sched.effective_break_end_iso is not null
                      then greatest(
                        0::numeric,
                        round((extract(epoch from (daily_sched.effective_break_end_iso - daily_sched.effective_break_start_iso)) / 60.0)::numeric, 0)
                      )
                    else null::numeric
                  end
                ),
                'breaks', case
                  when jsonb_typeof(daily_sched.effective_daily_schedule_json->'breaks') = 'array' then daily_sched.effective_daily_schedule_json->'breaks'
                  when daily_sched.effective_break_start_iso is not null and daily_sched.effective_break_end_iso is not null
                    then jsonb_build_array(
                      jsonb_build_object(
                        'start', to_char((daily_sched.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI'),
                        'end', to_char((daily_sched.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI'),
                        'break_mins', coalesce(
                          daily_sched.effective_break_minutes::numeric,
                          case
                            when daily_sched.effective_break_start_iso is not null and daily_sched.effective_break_end_iso is not null
                              then greatest(
                                0::numeric,
                                round((extract(epoch from (daily_sched.effective_break_end_iso - daily_sched.effective_break_start_iso)) / 60.0)::numeric, 0)
                              )
                            else null::numeric
                          end
                        )
                      )
                    )
                  when nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_start','')), '') is not null
                   and nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_end','')), '') is not null
                    then jsonb_build_array(
                      jsonb_build_object(
                        'start', nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_start','')), ''),
                        'end', nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_end','')), ''),
                        'break_mins', coalesce(
                          daily_sched.effective_break_minutes::numeric,
                          nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                          nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                          nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                          nullif(btrim(coalesce(daily_sched.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                        )
                      )
                    )
                  else '[]'::jsonb
                end,
                'client_name', cl.name,
                'role', coalesce(tf_any.role, con.role, ts.job_title_norm),
                'band', coalesce(tf_any.band, con.band, ts.band)
              )
            )
            else jsonb_build_array(
              jsonb_build_object(
                'segment_id', ('ts:' || tri.canonical_timesheet_id::text),
                'pay_amount', 0,
                'charge_amount', 0,
                'units', 0,
                'hours', 0,
                'hours_day', 0,
                'hours_night', 0,
                'hours_sat', 0,
                'hours_sun', 0,
                'hours_bh', 0,
                'exclude_from_pay', false,
                'ref_num', nullif(btrim(coalesce(ts.reference_number,'')), ''),
                'date', null,
                'segment_key', ('ts:' || tri.canonical_timesheet_id::text),
                'segment_stable_key', ('timesheet:' || coalesce(ts.booking_id, tri.canonical_timesheet_id::text)),
                'start_utc', null,
                'end_utc', null,
                'start', null,
                'end', null,
                'break_start', null,
                'break_end', null,
                'break_mins', null,
                'breaks', '[]'::jsonb,
                'client_name', cl.name,
                'role', coalesce(tf_any.role, con.role, ts.job_title_norm),
                'band', coalesce(tf_any.band, con.band, ts.band)
              )
            )
          end as current_segments_json,
          0::numeric as total_hours,
          0::numeric as total_pay_ex_vat,
          0::numeric as total_charge_ex_vat,
          0::numeric as hours_day,
          0::numeric as hours_night,
          0::numeric as hours_sat,
          0::numeric as hours_sun,
          0::numeric as hours_bh,
          null::numeric as pay_day,
          null::numeric as pay_night,
          null::numeric as pay_sat,
          null::numeric as pay_sun,
          null::numeric as pay_bh,
          null::numeric as charge_day,
          null::numeric as charge_night,
          null::numeric as charge_sat,
          null::numeric as charge_sun,
          null::numeric as charge_bh,
          '{}'::jsonb as current_additional_units_json,
          0::numeric as current_additional_pay_ex_vat,
          0::numeric as current_additional_charge_ex_vat,
          0::numeric as current_expenses_pay_ex_vat,
          0::numeric as current_expenses_charge_ex_vat,
          0::numeric as current_travel_pay_ex_vat,
          0::numeric as current_travel_charge_ex_vat,
          0::numeric as current_accommodation_pay_ex_vat,
          0::numeric as current_accommodation_charge_ex_vat,
          0::numeric as current_other_pay_ex_vat,
          0::numeric as current_other_charge_ex_vat,
          0::numeric as current_mileage_pay_ex_vat,
          0::numeric as current_mileage_charge_ex_vat,
          '[]'::jsonb as current_adjustments_json,
          tps.last_settled_snapshot_json,
          coalesce(
            tps.last_settled_signature,
            md5(coalesce(tps.last_settled_snapshot_json::text, '{}'))
          ) as baseline_signature,
          (doc.timesheet_id is not null) as has_active_overpayment_case
        from targeted_refresh_all_baseline_projection tri
        join public.timesheet_pay_state tps
          on tps.timesheet_id = tri.family_timesheet_id
        left join lateral (
          select tf.*
          from public.timesheets_financials tf
          where tf.timesheet_id = tri.canonical_timesheet_id
          order by tf.is_current desc, tf.computed_at_utc desc nulls last, tf.id desc
          limit 1
        ) tf_any on true
        join public.candidates cand
          on cand.id = coalesce(tf_any.candidate_id, v_candidate_id)
        left join public.timesheets ts
          on ts.timesheet_id = tri.canonical_timesheet_id
        left join lateral (
          select
            coalesce(tf_any.worked_start_iso, ts.worked_start_iso) as effective_worked_start_iso,
            coalesce(tf_any.worked_end_iso, ts.worked_end_iso) as effective_worked_end_iso,
            coalesce(tf_any.break_start_iso, ts.break_start_iso) as effective_break_start_iso,
            coalesce(tf_any.break_end_iso, ts.break_end_iso) as effective_break_end_iso,
            coalesce(tf_any.break_minutes, ts.break_minutes) as effective_break_minutes,
            case
              when jsonb_typeof(tf_any.actual_schedule_json) = 'object' then tf_any.actual_schedule_json
              when jsonb_typeof(tf_any.actual_schedule_json) = 'array' then (
                select tf_sched_item.value
                from jsonb_array_elements(tf_any.actual_schedule_json) as tf_sched_item(value)
                where tf_sched_item.value is not null
                  and jsonb_typeof(tf_sched_item.value) = 'object'
                limit 1
              )
              when jsonb_typeof(ts.actual_schedule_json) = 'object' then ts.actual_schedule_json
              when jsonb_typeof(ts.actual_schedule_json) = 'array' then (
                select ts_sched_item.value
                from jsonb_array_elements(ts.actual_schedule_json) as ts_sched_item(value)
                where ts_sched_item.value is not null
                  and jsonb_typeof(ts_sched_item.value) = 'object'
                limit 1
              )
              else null::jsonb
            end as effective_daily_schedule_json
        ) daily_sched on true
        left join public.contracts con
          on con.id = ts.contract_id
        left join public.client_settings cs
          on cs.client_id = tf_any.client_id
        left join public.clients cl
          on cl.id = tf_any.client_id
        left join umb_map um
          on um.umbrella_id = cand.umbrella_id
        left join debted_overpayment_cases doc
          on doc.candidate_id = coalesce(tf_any.candidate_id, v_candidate_id)
         and doc.timesheet_id = tri.canonical_timesheet_id
         and coalesce(doc.baseline_signature, '') = coalesce(tps.last_settled_signature, md5(coalesce(tps.last_settled_snapshot_json::text, '{}')))
        where v_effective_refresh_scope_kind = 'TARGETED_TIMESHEETS'
          and (
            tps.last_settled_snapshot_json is not null
            or tps.last_settled_signature is not null
            or tps.last_settled_pay_batch_id is not null
            or tps.last_settled_at_utc is not null
          )
          and not exists (
            select 1
            from ts_current cur
            where cur.timesheet_id = tri.canonical_timesheet_id
          )
  ;

  create temporary table active_settled_artifact_components on commit drop as
        select
          active_settled_components.timesheet_id,
          active_settled_components.key_type,
          active_settled_components.key_value,
          active_settled_components.amount_ex_vat,
          active_settled_components.amount_inc_vat
        from public._pay_active_settled_components(
          ARRAY(
            select distinct active_baseline_scope.timesheet_id
            from (
              select ts_current.timesheet_id
              from ts_current
              where ts_current.timesheet_id is not null
              union
              select targeted_baseline_only_ts_current.timesheet_id
              from targeted_baseline_only_ts_current
              where targeted_baseline_only_ts_current.timesheet_id is not null
            ) as active_baseline_scope
          )
        ) as active_settled_components
        where active_settled_components.timesheet_id is not null
          and active_settled_components.key_type is not null
          and active_settled_components.key_value is not null
  ;

  create temporary table active_settled_artifact_baseline_by_timesheet on commit drop as
        select
          active_settled_artifact_components.timesheet_id,
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'key_type', active_settled_artifact_components.key_type,
                'key_value', active_settled_artifact_components.key_value,
                'amount_ex_vat', active_settled_artifact_components.amount_ex_vat,
                'amount_inc_vat', active_settled_artifact_components.amount_inc_vat,
                'baseline_source', 'ACTIVE_SETTLED_ARTIFACT'
              )
              order by active_settled_artifact_components.key_type, active_settled_artifact_components.key_value
            ),
            '[]'::jsonb
          ) as active_settled_components_json,
          round(coalesce(sum(coalesce(active_settled_artifact_components.amount_ex_vat, 0)), 0), 2)::numeric as active_settled_amount_ex_vat,
          round(coalesce(sum(coalesce(active_settled_artifact_components.amount_inc_vat, 0)), 0), 2)::numeric as active_settled_amount_inc_vat,
          count(*)::int as active_settled_component_count
        from active_settled_artifact_components
        group by active_settled_artifact_components.timesheet_id
  ;

  create temporary table ts_baseline on commit drop as
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

          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then
              jsonb_build_object(
                'baseline_source', 'ACTIVE_SETTLED_ARTIFACT',
                'active_settled_artifact_components', coalesce(active_settled_artifact_baseline.active_settled_components_json, '[]'::jsonb),
                'active_settled_amount_ex_vat', coalesce(active_settled_artifact_baseline.active_settled_amount_ex_vat, 0),
                'active_settled_amount_inc_vat', coalesce(active_settled_artifact_baseline.active_settled_amount_inc_vat, 0),
                'legacy_timesheet_pay_state_snapshot_json', coalesce(t.last_settled_snapshot_json, '{}'::jsonb)
              )
            else coalesce(t.last_settled_snapshot_json, '{}'::jsonb)
          end as base_json,
          coalesce(active_settled_artifact_baseline.active_settled_components_json, '[]'::jsonb) as active_settled_artifact_components_json,
          coalesce(active_settled_artifact_baseline.active_settled_component_count, 0)::int as active_settled_artifact_component_count,
          coalesce(active_settled_artifact_baseline.active_settled_amount_ex_vat, 0)::numeric as active_settled_artifact_amount_ex_vat,
          coalesce(active_settled_artifact_baseline.active_settled_amount_inc_vat, 0)::numeric as active_settled_artifact_amount_inc_vat,
          (coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0) as has_active_settled_artifact_baseline,
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
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then 0::numeric
            when coalesce(t.last_settled_snapshot_json->>'hours_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,day}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'hours_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,day}')::numeric), 6)
            else 0::numeric
          end as bas_hours_day,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then 0::numeric
            when coalesce(t.last_settled_snapshot_json->>'hours_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,night}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'hours_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,night}')::numeric), 6)
            else 0::numeric
          end as bas_hours_night,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then 0::numeric
            when coalesce(t.last_settled_snapshot_json->>'hours_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sat}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'hours_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sat}')::numeric), 6)
            else 0::numeric
          end as bas_hours_sat,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then 0::numeric
            when coalesce(t.last_settled_snapshot_json->>'hours_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sun}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'hours_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,sun}')::numeric), 6)
            else 0::numeric
          end as bas_hours_sun,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then 0::numeric
            when coalesce(t.last_settled_snapshot_json->>'hours_bh', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,bh}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'hours_bh')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,bh}')::numeric), 6)
            else 0::numeric
          end as bas_hours_bh,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'pay_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,day}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'pay_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,day}')::numeric), 6)
            else null::numeric
          end as bas_pay_day,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'pay_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,night}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'pay_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,night}')::numeric), 6)
            else null::numeric
          end as bas_pay_night,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'pay_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sat}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'pay_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sat}')::numeric), 6)
            else null::numeric
          end as bas_pay_sat,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'pay_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sun}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'pay_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,sun}')::numeric), 6)
            else null::numeric
          end as bas_pay_sun,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'pay_bh', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,bh}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'pay_bh')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,pay_rates,bh}')::numeric), 6)
            else null::numeric
          end as bas_pay_bh,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'charge_day', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,day}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'charge_day')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,day}')::numeric), 6)
            else null::numeric
          end as bas_charge_day,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'charge_night', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,night}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'charge_night')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,night}')::numeric), 6)
            else null::numeric
          end as bas_charge_night,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'charge_sat', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sat}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'charge_sat')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sat}')::numeric), 6)
            else null::numeric
          end as bas_charge_sat,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
            when coalesce(t.last_settled_snapshot_json->>'charge_sun', t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sun}', '') ~ '^-?\d+(\.\d+)?$'
              then round(coalesce((t.last_settled_snapshot_json->>'charge_sun')::numeric, (t.last_settled_snapshot_json #>> '{invoice_breakdown_json,base_hours,charge_rates,sun}')::numeric), 6)
            else null::numeric
          end as bas_charge_sun,
          case
            when coalesce(active_settled_artifact_baseline.active_settled_component_count, 0) > 0 then null::numeric
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
        from (
          select * from ts_current
          union all
          select * from targeted_baseline_only_ts_current
        ) t
        left join active_settled_artifact_baseline_by_timesheet as active_settled_artifact_baseline
          on active_settled_artifact_baseline.timesheet_id = t.timesheet_id
  
  ;

  create temporary table finance_case_baseline_scope on commit drop as
        select
          vfcr.*
        from public.v_finance_cases_register vfcr
        where vfcr.case_type in (
          'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'UNDERPAYMENT'::public.pay_finance_case_type_enum,
          'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
          'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
        )
          and upper(coalesce(vfcr.status::text,'')) = 'ACTIVE'
          and coalesce(vfcr.outstanding_amount,0) > 0
          and (
            (
              vfcr.case_type in (
                'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
                'UNDERPAYMENT'::public.pay_finance_case_type_enum,
                'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
              )
              and (
                upper(coalesce(vfcr.payout_status::text,'')) <> 'PAID'
                or vfcr.next_due_week_start is null
                or vfcr.next_due_week_start <= v_week_start
              )
            )
            or (
              vfcr.case_type in (
                'OVERPAYMENT'::public.pay_finance_case_type_enum,
                'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
              )
              and (
                vfcr.next_due_week_start is null
                or vfcr.next_due_week_start <= v_week_start
              )
            )
            or (
              vfcr.case_type in (
                'OVERPAYMENT'::public.pay_finance_case_type_enum,
                'UNDERPAYMENT'::public.pay_finance_case_type_enum,
                'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
                'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
              )
              and exists (
                select 1
                from public.pay_finance_case_components as pfc_restructure_scope
                join public.candidates as c_restructure_scope
                  on c_restructure_scope.id = pfc_restructure_scope.candidate_id
                where pfc_restructure_scope.finance_case_id = vfcr.finance_case_id
                  and pfc_restructure_scope.closed_at_utc is null
                  and pfc_restructure_scope.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
                  and round(coalesce(pfc_restructure_scope.remaining_source_amount, 0), 2) > 0
                  and upper(coalesce(pfc_restructure_scope.source_pay_method, '')) in ('PAYE', 'UMBRELLA')
                  and upper(coalesce(c_restructure_scope.pay_method, '')) in ('PAYE', 'UMBRELLA')
                  and upper(coalesce(pfc_restructure_scope.source_pay_method, '')) <> upper(coalesce(c_restructure_scope.pay_method, ''))
                  and (
                    coalesce(pfc_restructure_scope.is_resolution_stale, false) = true
                    or nullif(btrim(coalesce(pfc_restructure_scope.saved_target_pay_method, '')), '') is null
                    or upper(coalesce(pfc_restructure_scope.saved_target_pay_method, '')) <> upper(coalesce(c_restructure_scope.pay_method, ''))
                  )
              )
            )
          )
          and (v_candidate_id is null or vfcr.candidate_id = v_candidate_id)
          and (v_client_id is null or vfcr.client_id = v_client_id)
          and (
            v_effective_refresh_scope_kind <> 'TARGETED_TIMESHEETS'
            or vfcr.linked_timesheet_id is null
            -- S-08 D5 (ruling 4): in the authoritative Workbench source build the preparation scope no
            -- longer pulls in Timesheets merely because an advance, recovery or manual adjustment is linked
            -- to them, so those Candidate balances are read here as balances whatever Timesheet they are
            -- linked to.  Underpayments stay Timesheet-scoped (they are owed only while authorised).
            or (
              v_internal_reconciliation_authorised is true
              and coalesce(v_source_build_mode, false) = true
              and vfcr.case_type in (
                'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
                'OVERPAYMENT'::public.pay_finance_case_type_enum,
                'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
                'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
              )
            )
            or vfcr.linked_timesheet_id in (select trti.timesheet_id from targeted_refresh_timesheet_ids trti)
            or vfcr.linked_timesheet_id in (select lrti.timesheet_id from linked_refresh_timesheet_ids lrti)
            or vfcr.linked_timesheet_id in (select trf.family_timesheet_id from targeted_refresh_all_family_timesheet_ids trf)
          )
  ;



  create temporary table manual_adjustment_carry_forward_scope on commit drop as
        select
          carry_forward_rows.id,
          carry_forward_rows.source_pay_batch_id,
          carry_forward_rows.source_pay_batch_item_id,
          carry_forward_rows.source_pay_bank_transfer_id,
          carry_forward_rows.source_pay_batch_candidate_id,
          carry_forward_rows.source_correction_request_id,
          carry_forward_rows.source_correction_work_item_id,
          carry_forward_rows.candidate_id,
          carry_forward_rows.umbrella_id,
          carry_forward_rows.client_id,
          carry_forward_rows.timesheet_id,
          carry_forward_rows.pay_channel,
          carry_forward_rows.adjustment_direction,
          carry_forward_rows.amount_ex_vat,
          carry_forward_rows.amount_vat,
          carry_forward_rows.amount_inc_vat,
          carry_forward_rows.amount_basis,
          carry_forward_rows.paye_treatment,
          carry_forward_rows.tax_treatment_json,
          carry_forward_rows.description,
          carry_forward_rows.reason,
          carry_forward_rows.source_ref,
          carry_forward_rows.source_operation_source_key,
          carry_forward_rows.source_snapshot_json,
          carry_forward_rows.status,
          carry_forward_rows.target_pay_batch_id,
          carry_forward_rows.target_pay_batch_item_id,
          carry_forward_rows.target_operation_source_key,
          carry_forward_rows.created_at_utc,
          carry_forward_rows.updated_at_utc,
          carry_forward_rows.reserved_at_utc,
          carry_forward_rows.consumed_at_utc,
          carry_forward_rows.released_at_utc,
          carry_forward_rows.cancelled_at_utc,
          carry_forward_rows.status_reason,
          COALESCE(source_batch_rows.execution_commit_ref, carry_forward_rows.source_pay_batch_id::text) AS source_batch_ref
        from public.pay_manual_adjustment_carry_forwards as carry_forward_rows
        left join public.pay_batches as source_batch_rows
          on source_batch_rows.id = carry_forward_rows.source_pay_batch_id
        where carry_forward_rows.candidate_id = v_candidate_id
          and upper(btrim(coalesce(carry_forward_rows.status, ''))) in ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT')
          and upper(btrim(coalesce(carry_forward_rows.status, ''))) not in ('CONSUMED_IN_BATCH', 'CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW')
          and (
            (
              upper(btrim(coalesce(carry_forward_rows.status, ''))) = 'PENDING_CARRY_FORWARD'
              and carry_forward_rows.target_pay_batch_id is null
              and carry_forward_rows.target_pay_batch_item_id is null
              and nullif(btrim(coalesce(carry_forward_rows.target_operation_source_key, '')), '') is null
            )
            or (
              upper(btrim(coalesce(carry_forward_rows.status, ''))) = 'RESERVED_IN_DRAFT'
              and (
                (v_target_pay_batch_id is not null and carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
                or (
                  v_target_operation_source_key is not null
                  and nullif(btrim(coalesce(carry_forward_rows.target_operation_source_key, '')), '') = v_target_operation_source_key
                )
              )
            )
          )
          and (
            (
              v_pay_channel_scope = 'ALL'
              and (
                v_candidate_pay_method is null
                or upper(btrim(coalesce(carry_forward_rows.pay_channel, ''))) = v_candidate_pay_method
              )
            )
            or (
              v_pay_channel_scope <> 'ALL'
              and upper(btrim(coalesce(carry_forward_rows.pay_channel, ''))) = v_pay_channel_scope
            )
          )
  ;

  v_source_build_collect_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_source_build_collect_started_at)) * 1000.0)::numeric, 3);

  return jsonb_build_object(
    'candidate_id', v_candidate_id::text,
    'refresh_scope_kind', v_effective_refresh_scope_kind,
    'targeted_timesheet_ids', v_targeted_timesheet_ids_json,
    'linked_timesheet_ids', v_linked_timesheet_ids_json,
    'ts_baseline_count', (select count(*)::int from ts_baseline),
    'finance_case_scope_count', (select count(*)::int from finance_case_baseline_scope),
    'manual_adjustment_carry_forward_count', (select count(*)::int from manual_adjustment_carry_forward_scope),
    'active_snooze_count', (select count(*)::int from active_snoozes),
    'source_build_collect_diagnostics', jsonb_build_object(
      'source_build_mode', COALESCE(v_source_build_mode, false),
      'requested_refresh_scope_kind', COALESCE(v_source_build_requested_scope_kind, 'CANDIDATE_FULL_LIVE'),
      'actual_refresh_scope_kind', COALESCE(v_source_build_actual_scope_kind, v_effective_refresh_scope_kind),
      'targeted_payload_received', COALESCE(v_source_build_targeted_payload_received, false),
      'targeted_timesheet_count', jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)),
      'linked_timesheet_count', jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)),
      'candidate_filter_applied_early', COALESCE(v_source_build_candidate_filter_applied_early, false),
      'timesheet_filter_applied_early', COALESCE(v_source_build_timesheet_filter_applied_early, false),
      'fallback_used', COALESCE(v_source_build_fallback_used, false),
      'fallback_reason', v_source_build_fallback_reason,
      'source_page_limit', v_source_build_page_limit,
      'source_page_count', v_source_build_target_page_count,
      'source_remaining_after_cursor_count', v_source_build_target_total_count,
      'cursor_in', COALESCE(v_source_build_cursor_in_json, 'null'::jsonb),
      'cursor_out', COALESCE(v_source_build_cursor_out_json, 'null'::jsonb),
      'cursor_advanced', (v_source_build_next_timesheet_id IS NOT NULL),
      'has_more', COALESCE(v_source_build_has_more, false),
      'large_aggregation_avoided', COALESCE(v_source_build_large_aggregation_avoided, false),
      'collect_elapsed_ms', v_source_build_collect_elapsed_ms,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    )
    || jsonb_build_object(
      'lifecycle_reconciliation_mode', v_lifecycle_reconciliation_mode,
      'historic_metadata_allowed', v_historic_metadata_allowed,
      'signed_finance_reconciliation', v_signed_finance_reconciliation,
      'exclude_lifecycle_ineligible_current_payability', v_exclude_lifecycle_ineligible_current_payability,
      'authoritative_timesheet_scope', v_authoritative_timesheet_scope,
      'internal_reconciliation_authorised', v_internal_reconciliation_authorised
    )
  );
end;
$function$;
ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb) SET plpgsql_check.mode TO 'disabled';
commit;
