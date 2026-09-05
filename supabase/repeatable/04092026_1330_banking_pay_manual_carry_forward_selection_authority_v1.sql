-- CloudTMS Banking Pay manual carry-forward selection authority.
-- A current, unconsumed carry-forward that the canonical owner already marks
-- Ready and draftable must also carry the explicit selection flag required by
-- the Workbench selection contract. No amount, tax, VAT or payment policy is
-- recalculated here.
-- Generated from the prior exact owner by the checked repository generator.

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
            coalesce(headroom.ordinary_positive_headroom,0)::numeric as ordinary_positive_headroom,
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
$function$;

ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  OWNER TO postgres;
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET search_path TO 'public';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.mode TO 'disabled';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.profiler TO 'off';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.tracer TO 'off';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.constants_tracing TO 'off';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.cursors_leaks TO 'off';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.strict_cursors_leaks TO 'off';
ALTER FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  SET plpgsql_check.fatal_errors TO 'off';
REVOKE ALL ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  TO postgres,service_role;
