-- Exact A34 finance-case baseline after the historical H1/H2 closure and A28 preview reassertion.
-- Generated from supabase/repeatable/26092026_0207_banking_pay_stage2_recovery_order_floor_v1.sql; function SHA-256 485daff26dd6d09f8d9abfac28a01d8173c9d0d21f3cfd3f15b4b99c6dcbf9f3.
-- Restores the reviewed definition and its planner-only jit setting; invokes no payment/provider action.
\set ON_ERROR_STOP on
BEGIN;
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
            order by private.s3p_recovery_order_key_v1(fcrrb.finance_case_id), fcrrb.finance_case_id /* S3-WP3 Q6-A ruling 31: creation order, loans by payout (was created_at, id) */
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
            order by private.s3p_recovery_order_key_v1(fcrrb.finance_case_id), fcrrb.finance_case_id /* S3-WP3 Q6-A ruling 31: creation order, loans by payout (was created_at, id) */
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
            order by private.s3p_recovery_order_key_v1(fcrrb.finance_case_id), fcrrb.finance_case_id /* S3-WP3 Q6-A ruling 31: creation order, loans by payout (was created_at, id) */
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
$function$;
ALTER FUNCTION public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid) SET jit = off;
COMMIT;
