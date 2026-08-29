-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 9efefa077b39.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.tsfin_write_snapshots_and_complete(p_rows jsonb)
 RETURNS TABLE(ok_count integer, fail_count integer, errors jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET plpgsql_check.mode TO 'disabled'
AS $function$
declare
  v_ok int := 0;
  v_fail int := 0;
  v_errors jsonb := '[]'::jsonb;

  r record;
  snap jsonb;

  prev public.timesheets_financials%rowtype;
  v_prev_id uuid;

  v_outbox_id uuid;
  v_timesheet_id uuid;

  v_err text;
  did_prepare boolean;

  -- ✅ validate segments JSON before writing
  v_ib  jsonb;
  v_bad int;

  -- ✅ expense component rollup (sum components; fallback to expenses_* only if sum == 0)
  v_cat_pay numeric;
  v_cat_charge numeric;
  v_fallback_exp_pay numeric;
  v_fallback_exp_charge numeric;
  v_exp_pay numeric;
  v_exp_charge numeric;

  -- ✅ additional + mileage rollup + patched totals/breakdown
  v_add_units_json jsonb;
  v_add_pay numeric;
  v_add_charge numeric;
  v_mil_pay numeric;
  v_mil_charge numeric;

  v_nonseg_pay numeric;
  v_nonseg_charge numeric;
  v_nonseg_margin numeric;

  v_core_pay numeric;
  v_core_charge numeric;

  v_total_pay numeric;
  v_total_charge numeric;
  v_margin numeric;

  v_add_obj jsonb;
  v_tot_obj jsonb;

  v_mode text;
  v_seg jsonb;
  v_exclude boolean;

  -- ✅ ERNI policy (PAYE only; NEVER applies to expenses/mileage)
  v_policy jsonb;
  v_apply_to text;
  v_erni_pct_raw numeric;
  v_erni_mult numeric;
  v_pay_method_u text;
  v_erni_applies boolean;

  v_wage_pay numeric;
  v_wage_pay_cost numeric;
  v_reimb_pay numeric;
  v_pay_cost numeric;

  v_nonseg_wage_pay numeric;
  v_nonseg_wage_pay_cost numeric;
  v_nonseg_reimb_pay numeric;
  v_nonseg_pay_cost numeric;

  v_processing_status public.ts_fin_processing_status_enum;
  v_candidate_assignment public.candidate_assignment_enum;
  v_basis public.timesheet_fin_basis_enum;
  v_timesheet_version int;
  v_is_stale boolean;
  v_stale_reason text;
  v_temp_log_enabled boolean := false;
  v_signature_diag_json jsonb := '{}'::jsonb;

  v_live_timesheet_authorised_at_server timestamptz := NULL::timestamptz;
  v_live_contract_week_id uuid := NULL::uuid;
  v_live_contract_week_status text := NULL::text;
  v_incoming_processing_status text := NULL::text;
  v_incoming_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_by_user_id uuid := NULL::uuid;
  v_live_authorised boolean := false;
  v_can_preserve_authorised_at boolean := false;
  v_lifecycle_decision text := 'WRITTEN';
begin
  perform public._ctms_assert_tsfin_batch_units_v1(coalesce(p_rows, '[]'::jsonb));
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    ok_count := 0;
    fail_count := 0;
    errors := '[]'::jsonb;
    return next;
    return;
  end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid     as outbox_id,
      nullif(elem->>'timesheet_id','')::uuid  as timesheet_id,
      elem->'snapshot'                        as snapshot
    from jsonb_array_elements(p_rows) as elem
  loop
    v_outbox_id := r.outbox_id;
    v_timesheet_id := r.timesheet_id;
    snap := r.snapshot;

    did_prepare := false;
    v_prev_id := null;
    v_live_timesheet_authorised_at_server := NULL::timestamptz;
    v_live_contract_week_id := NULL::uuid;
    v_live_contract_week_status := NULL::text;
    v_incoming_processing_status := NULL::text;
    v_incoming_authorised_at_utc := NULL::timestamptz;
    v_result_authorised_at_utc := NULL::timestamptz;
    v_result_authorised_by_user_id := NULL::uuid;
    v_live_authorised := false;
    v_can_preserve_authorised_at := false;
    v_lifecycle_decision := 'WRITTEN';

    begin
      if v_outbox_id is null or v_timesheet_id is null or snap is null then
        raise exception 'INVALID_BATCH_ROW';
      end if;

      -- SAFETY: do not write if current snapshot is paid
      if exists (
        select 1
        from public.timesheets_financials tf
        where tf.timesheet_id = v_timesheet_id
          and tf.is_current = true
          and tf.paid_at_utc is not null
      ) then
        perform public.tsfin_work_success(v_outbox_id);
        v_ok := v_ok + 1;
        continue;
      end if;

      -- Capture live lifecycle and current snapshot (for restore-on-fail + preserving manual fields).
      select ts.authorised_at_server
      into v_live_timesheet_authorised_at_server
      from public.timesheets AS ts
      where ts.timesheet_id = v_timesheet_id
        and coalesce(ts.is_current, true) = true
      order by ts.updated_at desc nulls last,
               ts.created_at desc nulls last,
               ts.timesheet_id desc
      limit 1
      for update;

      select cw.id,
             cw.status::text
      into v_live_contract_week_id,
           v_live_contract_week_status
      from public.contract_weeks AS cw
      where cw.timesheet_id = v_timesheet_id
      order by cw.updated_at desc nulls last,
               cw.created_at desc nulls last,
               cw.id desc
      limit 1
      for update;

      select *
      into prev
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      order by tf.computed_at_utc desc nulls last,
               tf.created_at desc nulls last,
               tf.updated_at desc nulls last,
               tf.id desc
      limit 1
      for update;

      v_prev_id := prev.id;

      -- Resolve enums/typed fields once (avoid divergence between flags and stored status)
      v_timesheet_version :=
        coalesce(nullif(snap->>'timesheet_version','')::int, 1);

      v_basis :=
        coalesce(
          nullif(snap->>'basis','')::public.timesheet_fin_basis_enum,
          'SELF_REPORTED'::public.timesheet_fin_basis_enum
        );

      v_candidate_assignment :=
        coalesce(
          nullif(snap->>'candidate_assignment','')::public.candidate_assignment_enum,
          'UNASSIGNED'::public.candidate_assignment_enum
        );

      v_processing_status :=
        coalesce(
          nullif(snap->>'processing_status','')::public.ts_fin_processing_status_enum,
          'UNASSIGNED'::public.ts_fin_processing_status_enum
        );

      -- ✅ validate invoice_breakdown_json (prevents persisting null/invalid segments)
      v_ib := coalesce(snap->'invoice_breakdown_json', '{}'::jsonb);
      if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
        v_ib := '{}'::jsonb;
      end if;

      v_bad := public._tsfin_invalid_segment_count(v_ib);
      if v_bad > 0 then
        raise exception 'INVALID_SEGMENTS_JSON:%', v_bad;
      end if;

      v_incoming_processing_status := CASE
        WHEN v_processing_status IS NULL THEN NULL::text
        ELSE v_processing_status::text
      END;
      v_incoming_authorised_at_utc := NULLIF(snap->>'authorised_at_utc','')::timestamptz;
      v_result_authorised_at_utc := coalesce(v_incoming_authorised_at_utc, prev.authorised_at_utc);
      v_result_authorised_by_user_id := coalesce(
        nullif(snap->>'authorised_by_user_id','')::uuid,
        prev.authorised_by_user_id
      );
      v_live_authorised := (
           v_live_timesheet_authorised_at_server IS NOT NULL
        OR upper(btrim(coalesce(v_live_contract_week_status, ''))) = 'AUTHORISED'
        OR prev.authorised_at_utc IS NOT NULL
        OR prev.processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
      );

      IF v_live_authorised
         AND (
              v_processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR v_processing_status NOT IN (
                'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                'READY_FOR_HR'::public.ts_fin_processing_status_enum
              )
           OR v_result_authorised_at_utc IS NULL
         ) THEN
        v_can_preserve_authorised_at := coalesce(
          prev.authorised_at_utc,
          v_live_timesheet_authorised_at_server,
          v_incoming_authorised_at_utc
        ) IS NOT NULL;

        IF NOT v_can_preserve_authorised_at THEN
          v_lifecycle_decision := 'REJECTED';
          PERFORM public._temp_diag_log(
            'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
            'TEMP_TIMESHEET_LIFECYCLE',
            v_timesheet_id::text,
            jsonb_strip_nulls(
              jsonb_build_object(
                'function_name', 'tsfin_write_snapshots_and_complete',
                'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
                'timesheet_id', v_timesheet_id::text,
                'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
                'incoming_processing_status', v_incoming_processing_status,
                'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
                'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
                'live_contract_week_status', v_live_contract_week_status,
                'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
                'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
                'decision', v_lifecycle_decision,
                'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
                'result_authorised_at_utc_present', false,
                'rejection_reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP',
                'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
              )
            )
          );
          RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
            DETAIL = jsonb_build_object(
              'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
              'timesheet_id', v_timesheet_id::text,
              'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
              'incoming_processing_status', v_incoming_processing_status,
              'live_contract_week_status', v_live_contract_week_status,
              'reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP'
            )::text;
        END IF;

        v_processing_status := CASE
          WHEN prev.processing_status IN (
                 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                 'READY_FOR_HR'::public.ts_fin_processing_status_enum
               )
            THEN prev.processing_status
          WHEN v_processing_status IN (
                 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                 'READY_FOR_HR'::public.ts_fin_processing_status_enum
               )
            THEN v_processing_status
          ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        END;
        v_result_authorised_at_utc := coalesce(
          prev.authorised_at_utc,
          v_live_timesheet_authorised_at_server,
          v_incoming_authorised_at_utc
        );
        v_result_authorised_by_user_id := coalesce(
          prev.authorised_by_user_id,
          v_result_authorised_by_user_id
        );
        v_lifecycle_decision := 'PRESERVED';
      END IF;

      PERFORM public._temp_diag_log(
        'TSFIN_SNAPSHOT_LIVE_LIFECYCLE_GUARD',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_timesheet_id::text,
        jsonb_strip_nulls(
          jsonb_build_object(
            'function_name', 'tsfin_write_snapshots_and_complete',
            'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
            'timesheet_id', v_timesheet_id::text,
            'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
            'incoming_processing_status', v_incoming_processing_status,
            'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
            'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
            'live_contract_week_id', CASE WHEN v_live_contract_week_id IS NULL THEN NULL::text ELSE v_live_contract_week_id::text END,
            'live_contract_week_status', v_live_contract_week_status
          )
          || jsonb_build_object(
            'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
            'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
            'decision', v_lifecycle_decision,
            'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
            'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL,
            'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
          )
        )
      );

      IF v_lifecycle_decision = 'PRESERVED' THEN
        PERFORM public._temp_diag_log(
          'TSFIN_SNAPSHOT_LIFECYCLE_STATUS_PRESERVED',
          'TEMP_TIMESHEET_LIFECYCLE',
          v_timesheet_id::text,
          jsonb_strip_nulls(
            jsonb_build_object(
              'function_name', 'tsfin_write_snapshots_and_complete',
              'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
              'timesheet_id', v_timesheet_id::text,
              'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
              'incoming_processing_status', v_incoming_processing_status,
              'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
              'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
              'live_contract_week_status', v_live_contract_week_status,
              'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
              'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
              'decision', v_lifecycle_decision,
              'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
              'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL
            )
          )
        );
      END IF;

      -- Guard + rotate current -> history (invoice-lock protected)
      perform public.tsfin_prepare_write(v_timesheet_id);
      did_prepare := true;

      -- ✅ compute expense totals (sum components first; fallback to expenses_* only if sum == 0)
      v_cat_pay :=
          coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0);

      v_cat_charge :=
          coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0);

      v_fallback_exp_pay :=
        coalesce(nullif(snap->>'expenses_pay_ex_vat','')::numeric, prev.expenses_pay_ex_vat, 0);

      v_fallback_exp_charge :=
        coalesce(nullif(snap->>'expenses_charge_ex_vat','')::numeric, prev.expenses_charge_ex_vat, 0);

      v_exp_pay :=
        case when coalesce(v_cat_pay, 0) <> 0 then v_cat_pay else v_fallback_exp_pay end;

      v_exp_charge :=
        case when coalesce(v_cat_charge, 0) <> 0 then v_cat_charge else v_fallback_exp_charge end;

      v_exp_pay := round(coalesce(v_exp_pay,0), 2);
      v_exp_charge := round(coalesce(v_exp_charge,0), 2);

      -- ✅ additional + mileage rollup (snap → prev fallback)
      v_add_units_json := coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb);

      v_add_pay := coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0);
      v_add_charge := coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0);

      v_mil_pay := coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0);
      v_mil_charge := coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0);

      v_add_pay := round(coalesce(v_add_pay,0), 2);
      v_add_charge := round(coalesce(v_add_charge,0), 2);
      v_mil_pay := round(coalesce(v_mil_pay,0), 2);
      v_mil_charge := round(coalesce(v_mil_charge,0), 2);

      -- Non-segment totals (additional + expenses + mileage)
      v_nonseg_pay := round(v_add_pay + v_exp_pay + v_mil_pay, 2);
      v_nonseg_charge := round(v_add_charge + v_exp_charge + v_mil_charge, 2);

      -- ✅ recompute core totals from breakdown (SEGMENTS sums; else base_hours)
      v_core_pay := 0;
      v_core_charge := 0;

      v_mode := upper(coalesce(v_ib->>'mode',''));

      if v_mode = 'SEGMENTS' and jsonb_typeof(v_ib->'segments') = 'array' then
        for v_seg in
          select value from jsonb_array_elements(v_ib->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          -- charge sum includes all segments (negative allowed)
          begin
            v_core_charge := v_core_charge
              + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
          exception when others then
            null;
          end;

          -- pay sum respects exclude_from_pay when present/true
          v_exclude := false;
          begin
            v_exclude := coalesce(nullif(btrim(coalesce(v_seg->>'exclude_from_pay','')), '')::boolean, false);
          exception when others then
            v_exclude := false;
          end;

          if not v_exclude then
            begin
              v_core_pay := v_core_pay
                + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
            exception when others then
              null;
            end;
          end if;
        end loop;
      else
        begin
          v_core_pay := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,pay_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_pay := 0;
        end;

        begin
          v_core_charge := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,charge_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_charge := 0;
        end;
      end if;

      v_core_pay := round(coalesce(v_core_pay,0), 2);
      v_core_charge := round(coalesce(v_core_charge,0), 2);

      -- Totals (pay/charge) are pure ex-vat figures (NO ERNI in totals)
      v_total_pay := round(v_core_pay + v_nonseg_pay, 2);
      v_total_charge := round(v_core_charge + v_nonseg_charge, 2);

      -- ✅ ERNI-aware margin:
      -- - ERNI applies ONLY to PAYE candidates
      -- - ERNI uplifts ONLY wage-like pay (core pay + additional pay)
      -- - ERNI NEVER applies to expenses or mileage
      v_policy := coalesce(snap->'policy_snapshot_json', prev.policy_snapshot_json, '{}'::jsonb);
      if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
        v_policy := '{}'::jsonb;
      end if;

      v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

      v_erni_pct_raw := 0;
      begin
        v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
      exception when others then
        v_erni_pct_raw := 0;
      end;

      v_erni_mult := 1;
      if coalesce(v_erni_pct_raw,0) > 0 then
        if v_erni_pct_raw > 1 then
          v_erni_mult := 1 + (v_erni_pct_raw / 100);
        else
          v_erni_mult := 1 + v_erni_pct_raw;
        end if;
      end if;

      v_pay_method_u :=
        upper(
          coalesce(
            nullif(btrim(coalesce(snap->>'pay_method','')), ''),
            nullif(btrim(coalesce(prev.pay_method::text,'')), ''),
            ''
          )
        );

      -- IMPORTANT: PAYE only. apply_erni_to can be ALL/PAYE_ONLY but never makes it apply to non-PAYE.
      v_erni_applies :=
        (v_pay_method_u = 'PAYE')
        and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

      -- overall margin cost:
      v_wage_pay := round(v_core_pay + v_add_pay, 2);         -- wage-like pay only
      v_reimb_pay := round(v_exp_pay + v_mil_pay, 2);         -- reimbursements (never ERNI)

      v_wage_pay_cost := v_wage_pay;
      if v_erni_applies then
        v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
      end if;

      v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
      v_margin := round(v_total_charge - v_pay_cost, 2);

      -- non-segment contribution margin (apply ERNI only to additional pay; never to expenses/mileage)
      v_nonseg_wage_pay := v_add_pay;
      v_nonseg_reimb_pay := round(v_exp_pay + v_mil_pay, 2);

      v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
      if v_erni_applies then
        v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
      end if;

      v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
      v_nonseg_margin := round(v_nonseg_charge - v_nonseg_pay_cost, 2);

      -- ✅ patch invoice_breakdown_json.additional (preserve units if present; else set from stored additional_units_json)
      v_add_obj := case
        when v_ib ? 'additional' and jsonb_typeof(v_ib->'additional') = 'object' then v_ib->'additional'
        else '{}'::jsonb
      end;

      if not (v_add_obj ? 'units') or jsonb_typeof(v_add_obj->'units') <> 'object' then
        if v_add_units_json is null or jsonb_typeof(v_add_units_json) <> 'object' then
          v_add_units_json := '{}'::jsonb;
        end if;
        v_add_obj := jsonb_set(v_add_obj, '{units}', v_add_units_json, true);
      end if;

      -- additional now represents combined non-segment totals (additional + expenses + mileage)
      v_add_obj := jsonb_set(v_add_obj, '{pay_ex_vat}', to_jsonb(v_nonseg_pay), true);
      v_add_obj := jsonb_set(v_add_obj, '{charge_ex_vat}', to_jsonb(v_nonseg_charge), true);
      v_add_obj := jsonb_set(v_add_obj, '{margin_ex_vat}', to_jsonb(v_nonseg_margin), true);

      v_ib := jsonb_set(v_ib, '{additional}', v_add_obj, true);

      -- ✅ patch invoice_breakdown_json.totals to match computed totals (margin includes PAYE-only ERNI on wage pay)
      v_tot_obj := case
        when v_ib ? 'totals' and jsonb_typeof(v_ib->'totals') = 'object' then v_ib->'totals'
        else '{}'::jsonb
      end;

      v_tot_obj := jsonb_set(v_tot_obj, '{total_pay_ex_vat}', to_jsonb(v_total_pay), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{total_charge_ex_vat}', to_jsonb(v_total_charge), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{margin_ex_vat}', to_jsonb(v_margin), true);

      v_ib := jsonb_set(v_ib, '{totals}', v_tot_obj, true);

      -- ✅ stale flag tie-off (clear stale on a successful READY_FOR_INVOICE snapshot)
      v_is_stale := coalesce(nullif(snap->>'is_stale','')::boolean, false);
      v_stale_reason := nullif(snap->>'stale_reason','');

      if v_processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then
        v_is_stale := false;
        v_stale_reason := null;
      end if;

      -- Insert new current snapshot (explicit columns)
      insert into public.timesheets_financials (
        timesheet_id,
        timesheet_version,
        basis,
        is_current,
        is_stale,
        stale_reason,

        worked_start_iso,
        worked_end_iso,
        break_start_iso,
        break_end_iso,
        break_minutes,

        candidate_id,
        client_id,
        role,
        band,
        pay_method,

        policy_snapshot_json,
        rate_source_refs_json,

        hours_day,
        hours_night,
        hours_sat,
        hours_sun,
        hours_bh,

        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,

        total_hours,
        total_pay_ex_vat,
        total_charge_ex_vat,
        margin_ex_vat,

        computed_at_utc,

        occupant_key_norm,
        candidate_assignment,
        processing_status,

        -- Totals + description + legacy evidence pointers
        expenses_pay_ex_vat,
        expenses_charge_ex_vat,
        expenses_description,
        expenses_evidence_r2_key,
        expenses_evidence_manifest,

        -- category expense columns
        travel_pay_ex_vat,
        travel_charge_ex_vat,
        accommodation_pay_ex_vat,
        accommodation_charge_ex_vat,
        other_pay_ex_vat,
        other_charge_ex_vat,

        mileage_pay_ex_vat,
        mileage_charge_ex_vat,
        mileage_units,
        mileage_evidence_r2_key,
        mileage_evidence_manifest,
        mileage_pay_rate,
        mileage_charge_rate,

        po_number,

        pay_on_hold,
        pay_on_hold_reason,
        pay_on_hold_since_utc,

        pay_wtr_rate_pct_snapshot,
        pay_vat_rate_pct_snapshot,
        pay_vat_amount_snapshot,
        pay_total_inc_vat_snapshot,

        payment_reference,
        remittance_last_sent_at_utc,
        remittance_send_count,

        processed_by_user_id,
        processed_at_utc,
        authorised_by_user_id,
        authorised_at_utc,

        additional_units_json,
        additional_pay_ex_vat,
        additional_charge_ex_vat,
        additional_margin_ex_vat,

        invoice_breakdown_json,

        nhsp_import_id,

        has_rate_issue,
        has_pay_channel_issue,

        hr_crosscheck_status,
        hr_crosscheck_issues,

        external_source_rows_json,

        actual_schedule_json,
        actual_minutes_by_day_json
      )
      values (
        v_timesheet_id,
        v_timesheet_version,
        v_basis,
        true,

        v_is_stale,
        v_stale_reason,

        nullif(snap->>'worked_start_iso','')::timestamptz,
        nullif(snap->>'worked_end_iso','')::timestamptz,
        nullif(snap->>'break_start_iso','')::timestamptz,
        nullif(snap->>'break_end_iso','')::timestamptz,
        nullif(snap->>'break_minutes','')::int,

        nullif(snap->>'candidate_id','')::uuid,
        nullif(snap->>'client_id','')::uuid,
        nullif(snap->>'role',''),
        nullif(snap->>'band',''),
        nullif(snap->>'pay_method',''),

        coalesce(snap->'policy_snapshot_json', '{}'::jsonb),
        coalesce(snap->'rate_source_refs_json', '{}'::jsonb),

        coalesce(nullif(snap->>'hours_day','')::numeric, 0),
        coalesce(nullif(snap->>'hours_night','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sat','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sun','')::numeric, 0),
        coalesce(nullif(snap->>'hours_bh','')::numeric, 0),

        nullif(snap->>'pay_day','')::numeric,
        nullif(snap->>'pay_night','')::numeric,
        nullif(snap->>'pay_sat','')::numeric,
        nullif(snap->>'pay_sun','')::numeric,
        nullif(snap->>'pay_bh','')::numeric,

        nullif(snap->>'charge_day','')::numeric,
        nullif(snap->>'charge_night','')::numeric,
        nullif(snap->>'charge_sat','')::numeric,
        nullif(snap->>'charge_sun','')::numeric,
        nullif(snap->>'charge_bh','')::numeric,

        coalesce(nullif(snap->>'total_hours','')::numeric, 0),

        -- totals are computed from core + (additional + expenses + mileage) (NO ERNI in totals)
        coalesce(v_total_pay, 0),
        coalesce(v_total_charge, 0),

        -- margin includes PAYE-only ERNI on wage pay (core + additional), never on expenses/mileage
        coalesce(v_margin, 0),

        now(),

        nullif(snap->>'occupant_key_norm',''),
        v_candidate_assignment,
        v_processing_status,

        -- expenses_pay_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_pay, 0),

        -- expenses_charge_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_charge, 0),

        nullif(snap->>'expenses_description',''),
        nullif(snap->>'expenses_evidence_r2_key',''),
        coalesce(snap->'expenses_evidence_manifest', prev.expenses_evidence_manifest),

        -- category columns (preserved from prev if absent in snapshot)
        coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0),
        coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0),
        coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0),
        coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0),

        -- preserve mileage pay/charge from prev if absent
        coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0),
        coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0),

        coalesce(nullif(snap->>'mileage_units','')::numeric, prev.mileage_units, 0),
        nullif(snap->>'mileage_evidence_r2_key',''),
        coalesce(snap->'mileage_evidence_manifest', prev.mileage_evidence_manifest),
        nullif(snap->>'mileage_pay_rate','')::numeric,
        nullif(snap->>'mileage_charge_rate','')::numeric,

        coalesce(nullif(snap->>'po_number',''), prev.po_number),

        coalesce(nullif(snap->>'pay_on_hold','')::boolean, prev.pay_on_hold, false),
        coalesce(nullif(snap->>'pay_on_hold_reason',''), prev.pay_on_hold_reason),
        coalesce(nullif(snap->>'pay_on_hold_since_utc','')::timestamptz, prev.pay_on_hold_since_utc),

        nullif(snap->>'pay_wtr_rate_pct_snapshot','')::numeric,
        nullif(snap->>'pay_vat_rate_pct_snapshot','')::numeric,
        coalesce(nullif(snap->>'pay_vat_amount_snapshot','')::numeric, 0),
        coalesce(nullif(snap->>'pay_total_inc_vat_snapshot','')::numeric, 0),

        coalesce(nullif(snap->>'payment_reference',''), prev.payment_reference),
        coalesce(nullif(snap->>'remittance_last_sent_at_utc','')::timestamptz, prev.remittance_last_sent_at_utc),
        coalesce(nullif(snap->>'remittance_send_count','')::int, prev.remittance_send_count, 0),

        coalesce(nullif(snap->>'processed_by_user_id','')::uuid, prev.processed_by_user_id),
        coalesce(nullif(snap->>'processed_at_utc','')::timestamptz, prev.processed_at_utc),
        v_result_authorised_by_user_id,
        v_result_authorised_at_utc,

        -- preserve additional units from prev if absent
        coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb),

        -- preserve additional pay/charge/margin from prev if absent
        coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0),
        coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0),
        coalesce(nullif(snap->>'additional_margin_ex_vat','')::numeric, prev.additional_margin_ex_vat, 0),

        -- patched breakdown (additional + totals updated)
        v_ib,

        nullif(snap->>'nhsp_import_id','')::uuid,

        coalesce(nullif(snap->>'has_rate_issue','')::boolean, false),
        coalesce(nullif(snap->>'has_pay_channel_issue','')::boolean, false),

        coalesce(nullif(snap->>'hr_crosscheck_status',''), prev.hr_crosscheck_status),
        coalesce(
          case
            when snap ? 'hr_crosscheck_issues' and jsonb_typeof(snap->'hr_crosscheck_issues') = 'array'
              then array(select jsonb_array_elements_text(snap->'hr_crosscheck_issues'))
            else null
          end,
          prev.hr_crosscheck_issues
        ),

        coalesce(snap->'external_source_rows_json', prev.external_source_rows_json),

        coalesce(snap->'actual_schedule_json', prev.actual_schedule_json),
        coalesce(snap->'actual_minutes_by_day_json', prev.actual_minutes_by_day_json)
      );

      perform public.tsfin_work_success(v_outbox_id);
      v_ok := v_ok + 1;

      IF COALESCE(v_temp_log_enabled, false) THEN
        v_signature_diag_json := public.timesheet_lifecycle_guard_signature_v1(v_timesheet_id, NULL::uuid, true);
        PERFORM public._temp_diag_log(
          'TSFIN_SIGNATURE_AFTER_SNAPSHOT',
          'TEMP_TIMESHEET_LIFECYCLE',
          v_timesheet_id::text,
          jsonb_strip_nulls(jsonb_build_object(
            'tag', 'TSFIN_SIGNATURE_AFTER_SNAPSHOT',
            'function_name', 'tsfin_write_snapshots_and_complete',
            'outbox_id', v_outbox_id,
            'timesheet_id', v_timesheet_id,
            'signature', NULLIF(BTRIM(COALESCE(v_signature_diag_json ->> 'backend_row_signature', v_signature_diag_json ->> 'row_signature', v_signature_diag_json ->> 'signature', '')), ''),
            'signature_payload', v_signature_diag_json,
            'processing_status', CASE WHEN v_processing_status IS NULL THEN NULL ELSE v_processing_status::text END,
            'previous_financials_id', v_prev_id
          ))
        );
      END IF;

    exception
      when others then
        v_err := sqlerrm;

        -- Restore previous snapshot if we already rotated current->history and insert failed.
        if did_prepare and v_prev_id is not null then
          update public.timesheets_financials
          set is_current = true,
              updated_at = now()
          where id = v_prev_id
            and locked_by_invoice_id is null
            and paid_at_utc is null;
        end if;

        -- If locked, don't retry forever: delete outbox row
        if v_err = 'TSFIN_LOCKED' then
          perform public.tsfin_work_success(v_outbox_id);
          v_ok := v_ok + 1;
        else
          perform public.tsfin_work_fail(v_outbox_id, v_err);
          v_fail := v_fail + 1;

          v_errors := v_errors || jsonb_build_array(
            jsonb_build_object(
              'outbox_id', v_outbox_id::text,
              'timesheet_id', v_timesheet_id::text,
              'error', v_err
            )
          );
        end if;
    end;
  end loop;

  ok_count := v_ok;
  fail_count := v_fail;
  errors := v_errors;
  return next;
end;
$function$;

REVOKE ALL ON FUNCTION public.tsfin_write_snapshots_and_complete(jsonb)
FROM public, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.tsfin_write_snapshots_and_complete(jsonb)
TO service_role;
