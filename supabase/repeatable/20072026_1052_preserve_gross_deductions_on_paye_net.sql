

CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness(p_pay_batch_id uuid, p_actor_user_id uuid, p_allow_large_full_scan boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now_utc timestamptz := now();
  v_date_context jsonb := '{}'::jsonb;
  v_today_uk date := NULL::date;

  v_pay_date date;
  v_week_start date;
  v_batch_kind_fixed text;
  v_scope text;

  v_ts_ids uuid[] := array[]::uuid[];

  v_is_stale boolean := false;
  v_reasons text[] := array[]::text[];

  v_diffs jsonb := '[]'::jsonb;

  v_ts_changed_ct int := 0;
  v_key_diff_ct int := 0;
  v_finance_reservation_diff_ct int := 0;
  v_snooze_diff_ct int := 0;
  v_restructure_diff_ct int := 0;
  v_writeoff_diff_ct int := 0;
  v_timesheet_override_diff_ct int := 0;
  v_paye_guardrail_diff_ct int := 0;
  v_overpay_ded_diff_ct int := 0;
  v_manual_debt_ded_diff_ct int := 0;
  v_loan_ded_diff_ct int := 0;
  v_ded_diff_ct int := 0;
  v_paye_net_diff_ct int := 0;

  v_batch_created_at_utc timestamptz;
  v_batch_status text;
  v_batch_is_active_reservation boolean := false;
  v_finance_reservation_expected_status text := 'RESERVED';
  v_same_week_paye_override_used boolean := false;
  v_paye_guardrails jsonb := '{}'::jsonb;

  v_diff_limit int := 500;

  -- Scalable execution guard. Normal execute operation flow must use the
  -- chunked freshness RPCs. This legacy/full validator remains available for
  -- small/manual diagnostics, or explicit admin diagnostics when
  -- p_allow_large_full_scan = true. Counts are derived only from frozen
  -- batch/payment artifacts, never live TSFIN.
  v_allow_large_full_scan boolean := false;
  v_candidate_count integer := 0;
  v_item_count integer := 0;
  v_item_breakdown_count integer := 0;
  v_transfer_count integer := 0;
  v_large_batch boolean := false;
  v_large_batch_reasons jsonb := '[]'::jsonb;
  v_chunked_required_diff jsonb := '[]'::jsonb;
  v_carry_forward_freshness_result jsonb := '{}'::jsonb;
  v_carry_forward_blocker_count integer := 0;
begin
  PERFORM public.banking_pay_hot_path_budget_apply('DIAGNOSTIC');

  v_allow_large_full_scan := coalesce(p_allow_large_full_scan, false);

  if p_pay_batch_id is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'pay_batch_validate_freshness: actor_user_id is required';
  end if;

  select
    pb.pay_date,
    pb.batch_kind_fixed,
    pb.created_at_utc,
    upper(coalesce(pb.status, '')),
    coalesce(pb.same_week_paye_override_used, false)
  into
    v_pay_date,
    v_batch_kind_fixed,
    v_batch_created_at_utc,
    v_batch_status,
    v_same_week_paye_override_used
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  limit 1;

  if v_pay_date is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id not found (%).', p_pay_batch_id::text;
  end if;

  v_date_context := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_today_uk := case
    when coalesce(v_date_context->>'london_current_date', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      then (v_date_context->>'london_current_date')::date
    else NULL::date
  end;

  if v_today_uk is null then
    raise exception 'PAY_BATCH_FRESHNESS_LONDON_DATE_UNAVAILABLE'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_LONDON_DATE_UNAVAILABLE',
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The current Europe/London business date could not be resolved for full batch freshness.'
            )::text;
  end if;

  select count(*)::integer
  into v_candidate_count
  from public.pay_batch_candidates as guard_candidate_count
  where guard_candidate_count.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_item_count
  from public.pay_batch_items as guard_item_count
  join public.pay_batch_candidates as guard_item_candidate
    on guard_item_candidate.id = guard_item_count.pay_batch_candidate_id
  where guard_item_candidate.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_item_breakdown_count
  from public.pay_batch_item_breakdowns as guard_breakdown_count
  join public.pay_batch_items as guard_breakdown_item
    on guard_breakdown_item.id = guard_breakdown_count.pay_batch_item_id
  join public.pay_batch_candidates as guard_breakdown_candidate
    on guard_breakdown_candidate.id = guard_breakdown_item.pay_batch_candidate_id
  where guard_breakdown_candidate.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_transfer_count
  from public.pay_bank_transfers as guard_transfer_count
  where guard_transfer_count.pay_batch_id = p_pay_batch_id;

  v_large_batch := (
    coalesce(v_candidate_count, 0) > 100
    or coalesce(v_item_count, 0) > 250
    or coalesce(v_transfer_count, 0) > 100
    or coalesce(v_item_breakdown_count, 0) > 500
  );

  select coalesce(jsonb_agg(guard_reason.reason_json order by guard_reason.reason_key), '[]'::jsonb)
  into v_large_batch_reasons
  from (
    select
      'candidate_count'::text as reason_key,
      jsonb_build_object('metric', 'candidate_count', 'count', coalesce(v_candidate_count, 0), 'threshold', 100) as reason_json
    where coalesce(v_candidate_count, 0) > 100

    union all

    select
      'item_count'::text as reason_key,
      jsonb_build_object('metric', 'item_count', 'count', coalesce(v_item_count, 0), 'threshold', 250) as reason_json
    where coalesce(v_item_count, 0) > 250

    union all

    select
      'transfer_count'::text as reason_key,
      jsonb_build_object('metric', 'transfer_count', 'count', coalesce(v_transfer_count, 0), 'threshold', 100) as reason_json
    where coalesce(v_transfer_count, 0) > 100

    union all

    select
      'item_breakdown_count'::text as reason_key,
      jsonb_build_object('metric', 'item_breakdown_count', 'count', coalesce(v_item_breakdown_count, 0), 'threshold', 500) as reason_json
    where coalesce(v_item_breakdown_count, 0) > 500
  ) as guard_reason;

  if v_large_batch and v_allow_large_full_scan is not true then
    v_chunked_required_diff := jsonb_build_array(
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', 'FRESHNESS_MODE',
        'key_value', 'FULL_BATCH_SCAN_GUARD',
        'expected', jsonb_build_object(
          'required_path', 'chunked_freshness',
          'seed_rpc', 'pay_batch_freshness_scope_seed',
          'chunk_rpc', 'pay_batch_validate_freshness_chunk',
          'aggregate_rpc', 'pay_batch_freshness_result_get'
        ),
        'actual', jsonb_build_object(
          'requested_path', 'pay_batch_validate_freshness_full_scan',
          'p_allow_large_full_scan', v_allow_large_full_scan,
          'candidate_count', coalesce(v_candidate_count, 0),
          'item_count', coalesce(v_item_count, 0),
          'transfer_count', coalesce(v_transfer_count, 0),
          'item_breakdown_count', coalesce(v_item_breakdown_count, 0),
          'large_batch_reasons', coalesce(v_large_batch_reasons, '[]'::jsonb)
        )
      )
    );

    return jsonb_build_object(
      'is_stale', true,
      'stale_reasons', jsonb_build_array('FULL_BATCH_FRESHNESS_REQUIRES_CHUNKED_VALIDATION'),
      'diff', v_chunked_required_diff,
      'requires_chunked_freshness', true,
      'code', 'FULL_BATCH_FRESHNESS_REQUIRES_CHUNKED_VALIDATION',
      'message', 'This batch is too large for the legacy full-batch freshness validator. Use the chunked freshness operation path.',
      'pay_batch_id', p_pay_batch_id::text,
      'counts', jsonb_build_object(
        'candidate_count', coalesce(v_candidate_count, 0),
        'item_count', coalesce(v_item_count, 0),
        'transfer_count', coalesce(v_transfer_count, 0),
        'item_breakdown_count', coalesce(v_item_breakdown_count, 0)
      ),
      'thresholds', jsonb_build_object(
        'candidate_count', 100,
        'item_count', 250,
        'transfer_count', 100,
        'item_breakdown_count', 500
      ),
      'large_batch_reasons', coalesce(v_large_batch_reasons, '[]'::jsonb)
    );
  end if;

  v_batch_is_active_reservation := public._pay_batch_status_is_active_reservation(v_batch_status);

  if v_batch_status in ('AUTHORISED_FOR_PAYMENT','SCHEDULED','EXECUTING') then
    v_finance_reservation_expected_status := 'COMMITTED';
  else
    v_finance_reservation_expected_status := 'RESERVED';
  end if;

  v_week_start := public._pay_week_start_monday(v_pay_date);

  select
    max(pbi.pay_channel)
  into
    v_scope
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
    and pbi.pay_channel in ('PAYE','UMBRELLA');

  v_scope := upper(btrim(coalesce(v_scope,'')));
  if v_scope not in ('PAYE','UMBRELLA') then
    v_scope := null;
  end if;

  select
    coalesce(
      (
        select array_agg(distinct t1.timesheet_id)
        from (
          select pbts.timesheet_id
          from public.pay_batch_timesheet_snapshots pbts
          where pbts.pay_batch_id = p_pay_batch_id
            and pbts.timesheet_id is not null

          union all

          select pbi2.timesheet_id
          from public.pay_batch_items pbi2
          join public.pay_batch_candidates pbc2
            on pbc2.id = pbi2.pay_batch_candidate_id
          where pbc2.pay_batch_id = p_pay_batch_id
            and coalesce(pbi2.is_voided, false) = false
            and not exists (
              select 1
              from public.pay_payment_correction_items as ppc_pbi2_fresh_exclusion
              where ppc_pbi2_fresh_exclusion.pay_batch_item_id = pbi2.id
                and ppc_pbi2_fresh_exclusion.status = 'APPLIED'
                and ppc_pbi2_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
            )
            and pbi2.timesheet_id is not null
        ) t1
      ),
      array[]::uuid[]
    )
  into v_ts_ids;

  create temporary table if not exists pg_temp.tmp_fresh_ts_diffs (
    timesheet_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_text text null,
    actual_text text null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_key_diffs (
    timesheet_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_candidate_earnings (
    pay_batch_candidate_id uuid not null,
    candidate_id uuid not null,
    awaiting_net_amount boolean not null,
    earnings_before_loan_ex numeric(12,2) not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_overpay_alloc (
    candidate_id uuid not null,
    advance_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_candidate_expected_overpay (
    candidate_id uuid not null,
    expected_overpayment_recovery_ex numeric(12,2) not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_loan_alloc (
    candidate_id uuid not null,
    loan_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_deduction_diffs (
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_paye_net_diffs (
    candidate_id uuid not null,
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_manual_debt_alloc (
    candidate_id uuid not null,
    finance_case_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_state_diffs (
    timesheet_id uuid null,
    candidate_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_text text null,
    actual_text text null,
    ord int not null
  ) on commit drop;

  truncate table pg_temp.tmp_fresh_ts_diffs;
  truncate table pg_temp.tmp_fresh_key_diffs;
  truncate table pg_temp.tmp_fresh_candidate_earnings;
  truncate table pg_temp.tmp_fresh_expected_overpay_alloc;
  truncate table pg_temp.tmp_fresh_candidate_expected_overpay;
  truncate table pg_temp.tmp_fresh_expected_loan_alloc;
  truncate table pg_temp.tmp_fresh_expected_manual_debt_alloc;
  truncate table pg_temp.tmp_fresh_deduction_diffs;
  truncate table pg_temp.tmp_fresh_paye_net_diffs;
  truncate table pg_temp.tmp_fresh_state_diffs;

  ---------------------------------------------------------------------------
  -- TIMESHEET_CHANGED: Policy X keeps blocking freshness checks in the
  -- economic entitlement keyspace. The legacy whole-snapshot signature
  -- comparison is intentionally disabled as a blocking stale gate here.
  ---------------------------------------------------------------------------
  v_ts_changed_ct := 0;

  ---------------------------------------------------------------------------
  -- RESERVATION_CHANGED: compare this batch's frozen source entitlement
  -- against the current entitlement available before this batch.
  --
  -- Policy X (post-draft): the batch side is resolved only from frozen
  -- batch artifacts via _pay_batch_item_economic_components. Current
  -- live truth is comparison-only via _pay_current_timesheet_entitlement_components.
  -- This section must not call the broad outstanding helper, must not
  -- remap batch items from live TSFIN, and must not validate resolved rows
  -- against target payout amounts.
  ---------------------------------------------------------------------------
  if v_batch_is_active_reservation then
    insert into pg_temp.tmp_fresh_key_diffs (
      timesheet_id,
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with
    this_components_raw as (
      select
        pbec.pay_batch_item_id,
        pbec.timesheet_id,
        upper(nullif(btrim(coalesce(pbec.item_type, '')), '')) as item_type,
        upper(nullif(btrim(coalesce(pbec.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(pbec.key_value, '')), '') as key_value,
        pbec.source_amount_ex_vat as source_amount_ex_vat_raw,
        pbec.target_amount_ex_vat as target_amount_ex_vat_raw,
        nullif(btrim(coalesce(pbec.key_resolution_failure_reason, '')), '') as key_resolution_failure_reason
      from public._pay_batch_item_economic_components(
        p_pay_batch_id => p_pay_batch_id,
        p_pay_batch_item_ids => null::uuid[]
      ) as pbec
      join public.pay_batch_items as pbi_this_fresh
        on pbi_this_fresh.id = pbec.pay_batch_item_id
      join public.pay_batch_candidates as pbc_this_fresh
        on pbc_this_fresh.id = pbi_this_fresh.pay_batch_candidate_id
      where pbc_this_fresh.pay_batch_id = p_pay_batch_id
        and coalesce(pbi_this_fresh.is_voided, false) = false
        and not exists (
          select 1
          from public.pay_payment_correction_items as ppc_this_fresh_exclusion
          where ppc_this_fresh_exclusion.pay_batch_item_id = pbi_this_fresh.id
            and ppc_this_fresh_exclusion.status = 'APPLIED'
            and ppc_this_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
        )
        and upper(nullif(btrim(coalesce(pbec.item_type, '')), '')) in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    ),
    this_component_resolution_failures as (
      select
        tcr.timesheet_id,
        case
          when tcr.key_resolution_failure_reason is not null then tcr.key_resolution_failure_reason
          when tcr.key_type is null or tcr.key_value is null then 'KEY_RESOLUTION_FAILURE'
          when tcr.key_type not in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE') then 'KEY_RESOLUTION_FAILURE'
          when tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$' then 'TS_DAY_KEY_VALUE_NOT_DATE'
          when tcr.source_amount_ex_vat_raw is null then 'SOURCE_RESOLUTION_FAILURE'
          else 'SOURCE_RESOLUTION_FAILURE'
        end as key_type,
        coalesce(tcr.pay_batch_item_id::text, 'UNKNOWN_PAY_BATCH_ITEM') as key_value,
        0::numeric(12,2) as expected_ex,
        1::numeric(12,2) as actual_ex,
        2 as ord
      from this_components_raw as tcr
      where tcr.key_resolution_failure_reason is not null
         or tcr.key_type is null
         or tcr.key_value is null
         or tcr.key_type not in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
         or (tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$')
         or tcr.source_amount_ex_vat_raw is null
    ),
    this_components as (
      select
        tcr.timesheet_id,
        tcr.key_type,
        tcr.key_value,
        round(sum(round(coalesce(tcr.source_amount_ex_vat_raw, 0), 2)), 2)::numeric(12,2) as current_source_ex_vat
      from this_components_raw as tcr
      where tcr.timesheet_id is not null
        and tcr.key_type is not null
        and tcr.key_value is not null
        and tcr.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$')
        and tcr.key_resolution_failure_reason is null
        and tcr.source_amount_ex_vat_raw is not null
      group by
        tcr.timesheet_id,
        tcr.key_type,
        tcr.key_value
    ),
    current_truth_baseline as (
      select
        ctec.timesheet_id,
        upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(ctec.key_value, '')), '') as key_value,
        round(sum(coalesce(ctec.truth_ex_vat, 0)), 2)::numeric(12,2) as truth_ex_vat,
        round(sum(coalesce(ctec.baseline_ex_vat, 0)), 2)::numeric(12,2) as baseline_ex_vat
      from public._pay_current_timesheet_entitlement_components(v_ts_ids) as ctec
      where ctec.timesheet_id is not null
        and ctec.key_type is not null
        and btrim(ctec.key_type) <> ''
        and ctec.key_value is not null
        and btrim(ctec.key_value) <> ''
        and upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (
          upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) = 'TS_DAY'
          and nullif(btrim(coalesce(ctec.key_value, '')), '') !~ '^\d{4}-\d{2}-\d{2}$'
        )
      group by
        ctec.timesheet_id,
        upper(nullif(btrim(coalesce(ctec.key_type, '')), '')),
        nullif(btrim(coalesce(ctec.key_value, '')), '')
    ),
    all_reserved as (
      select
        rc.timesheet_id,
        upper(nullif(btrim(coalesce(rc.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(rc.key_value, '')), '') as key_value,
        round(sum(coalesce(rc.amount_ex_vat, 0)), 2)::numeric(12,2) as all_reserved_ex_vat
      from public._pay_reserved_components(v_ts_ids) as rc
      where rc.timesheet_id is not null
        and rc.key_type is not null
        and btrim(rc.key_type) <> ''
        and rc.key_value is not null
        and btrim(rc.key_value) <> ''
        and upper(nullif(btrim(coalesce(rc.key_type, '')), '')) in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (
          upper(nullif(btrim(coalesce(rc.key_type, '')), '')) = 'TS_DAY'
          and nullif(btrim(coalesce(rc.key_value, '')), '') !~ '^\d{4}-\d{2}-\d{2}$'
        )
      group by
        rc.timesheet_id,
        upper(nullif(btrim(coalesce(rc.key_type, '')), '')),
        nullif(btrim(coalesce(rc.key_value, '')), '')
    ),
    reservation_comparison as (
      select
        tc.timesheet_id,
        tc.key_type,
        tc.key_value,
        tc.current_source_ex_vat as expected_ex,
        round(
          coalesce(ctb.truth_ex_vat, 0)
          - coalesce(ctb.baseline_ex_vat, 0)
          - (
              coalesce(ar.all_reserved_ex_vat, 0)
              - coalesce(tc.current_source_ex_vat, 0)
            ),
          2
        )::numeric(12,2) as actual_ex,
        2 as ord
      from this_components as tc
      left join current_truth_baseline as ctb
        on ctb.timesheet_id = tc.timesheet_id
       and ctb.key_type = tc.key_type
       and ctb.key_value = tc.key_value
      left join all_reserved as ar
        on ar.timesheet_id = tc.timesheet_id
       and ar.key_type = tc.key_type
       and ar.key_value = tc.key_value
    ),
    reservation_mismatches as (
      select
        rc.timesheet_id,
        rc.key_type,
        rc.key_value,
        rc.expected_ex,
        rc.actual_ex,
        rc.ord
      from reservation_comparison as rc
      where abs(round(coalesce(rc.actual_ex, 0), 2) - round(coalesce(rc.expected_ex, 0), 2)) > 0.01
    )
    select
      tcrf.timesheet_id,
      tcrf.key_type,
      tcrf.key_value,
      tcrf.expected_ex,
      tcrf.actual_ex,
      tcrf.ord
    from this_component_resolution_failures as tcrf

    union all

    select
      rm.timesheet_id,
      rm.key_type,
      rm.key_value,
      rm.expected_ex,
      rm.actual_ex,
      rm.ord
    from reservation_mismatches as rm;
  end if;

  select count(*)::int
  into v_key_diff_ct
  from pg_temp.tmp_fresh_key_diffs;

  if v_key_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'RESERVATION_CHANGED');
  end if;

  ---------------------------------------------------------------------------
  -- STATE_CHANGED: finance reservation rows, snoozes, restructure/write-off,
  -- timesheet-payment-override changes, and PAYE guardrail changes.
  ---------------------------------------------------------------------------
  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_finance_items as (
    select
      pbi.timesheet_id,
      pbc.candidate_id,
      pbi.id as pay_batch_item_id,
      pbi.finance_case_id,
      pbi.reservation_id,
      pbi.item_type,
      pbi.repayment_week_start,
      round(abs(coalesce(pbi.frozen_source_amount, case when pbi.frozen_target_amount_ex_vat is null and pbi.frozen_resolution_mode is null and pbi.frozen_resolution_result_json is null then pbi.amount_ex_vat else null end, 0)), 2)::numeric(12,2) as reserved_amount_ex
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
      and pbi.finance_case_id is not null
  ),
  reservation_rows as (
    select
      par.id as reservation_id,
      par.finance_case_id,
      par.pay_batch_id,
      par.pay_batch_candidate_id,
      par.pay_batch_item_id,
      round(coalesce(par.reserved_source_amount, par.reserved_amount, 0), 2)::numeric(12,2) as reserved_amount_ex,
      par.repayment_week_start,
      upper(coalesce(par.status, '')) as reservation_status
    from public.pay_advance_reservations par
    where par.pay_batch_id = p_pay_batch_id
  )
  select
    bfi.timesheet_id,
    bfi.candidate_id,
    'FINANCE_RESERVATION' as key_type,
    bfi.pay_batch_item_id::text as key_value,
    jsonb_build_object(
      'finance_case_id', bfi.finance_case_id::text,
      'reservation_id', case when bfi.reservation_id is null then null else bfi.reservation_id::text end,
      'reserved_amount_ex', bfi.reserved_amount_ex,
      'repayment_week_start', case when bfi.repayment_week_start is null then null else bfi.repayment_week_start::text end,
      'status', v_finance_reservation_expected_status
    )::text as expected_text,
    case
      when rr.reservation_id is null then null
      else jsonb_build_object(
        'reservation_id', rr.reservation_id::text,
        'finance_case_id', rr.finance_case_id::text,
        'reserved_amount_ex', rr.reserved_amount_ex,
        'repayment_week_start', case when rr.repayment_week_start is null then null else rr.repayment_week_start::text end,
        'status', rr.reservation_status
      )::text
    end as actual_text,
    33 as ord
  from batch_finance_items bfi
  left join reservation_rows rr
    on rr.pay_batch_item_id = bfi.pay_batch_item_id
  where rr.reservation_id is null
     or rr.finance_case_id is distinct from bfi.finance_case_id
     or round(coalesce(rr.reserved_amount_ex, 0), 2) <> round(coalesce(bfi.reserved_amount_ex, 0), 2)
     or rr.repayment_week_start is distinct from bfi.repayment_week_start
     or rr.reservation_status <> v_finance_reservation_expected_status
     or (bfi.reservation_id is not null and rr.reservation_id is distinct from bfi.reservation_id);

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_items as (
    select
      pbi.timesheet_id,
      pbc.candidate_id,
      pbi.id as pay_batch_item_id,
      pbi.item_type,
      pbi.source_ref,
      pbi.finance_case_id,
      coalesce(
        case
          when pbi.finance_case_id is not null then 'advance:' || pbi.finance_case_id::text
          else null::text
        end,
        lower(nullif(btrim(coalesce(pbi.source_ref, '')), ''))
      ) as exact_source_ref,
      case
        when coalesce(
          case
            when pbi.finance_case_id is not null then 'advance:' || pbi.finance_case_id::text
            else null::text
          end,
          lower(nullif(btrim(coalesce(pbi.source_ref, '')), ''))
        ) ~* '^timesheet-expense:' then 'TIMESHEET_EXPENSE'
        when pbi.finance_case_id is not null
          or lower(nullif(btrim(coalesce(pbi.source_ref, '')), '')) ~* '^advance:' then 'FINANCE_CASE'
        when nullif(btrim(coalesce(pbi.source_ref, '')), '') is not null then 'EXACT_SOURCE'
        else 'TIMESHEET_OR_SEGMENT'
      end as source_identity_kind,
      nullif(btrim(coalesce(pbi.segment_key, '')), '') as segment_key,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(coalesce(pbi.segment_key, '')), '')
      ) as segment_stable_key,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment,id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment,id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment,id}'), '')
      ) as segment_id,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{booking,booking_id}'), ''),
        (
          select coalesce(
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking,booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{timesheet,booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{target,booking_id}'), '')
          )
          from public.pay_batch_timesheet_snapshots as snapshot_row
          where snapshot_row.pay_batch_id = p_pay_batch_id
            and snapshot_row.timesheet_id = pbi.timesheet_id
          order by snapshot_row.id
          limit 1
        )
      ) as booking_id
    from public.pay_batch_items as pbi
    join public.pay_batch_candidates as pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
  ), matched_snoozes as (
    select
      bi.timesheet_id,
      bi.candidate_id,
      bi.pay_batch_item_id,
      bi.item_type,
      bi.exact_source_ref,
      bi.source_identity_kind,
      bi.segment_stable_key,
      bi.segment_id,
      bi.booking_id,
      active_snooze.snooze_id,
      active_snooze.snooze_kind,
      active_snooze.snooze_until_date,
      active_snooze.note,
      active_snooze.source_ref,
      active_snooze.segment_stable_key as snooze_segment_stable_key,
      active_snooze.segment_id as snooze_segment_id,
      active_snooze.booking_id as snooze_booking_id,
      active_snooze.match_scope
    from batch_items as bi
    join lateral (
      select
        pis.id as snooze_id,
        upper(coalesce(pis.snooze_kind, '')) as snooze_kind,
        pis.snooze_until_date,
        pis.note,
        lower(nullif(btrim(coalesce(pis.source_ref, '')), '')) as source_ref,
        pis.segment_stable_key,
        pis.segment_id,
        pis.booking_id,
        case
          when pis.source_ref is not null then 'EXACT_SOURCE_REF'
          else 'LEGACY_TIMESHEET_SEGMENT'
        end as match_scope
      from public.pay_item_snoozes as pis
      where pis.candidate_id = bi.candidate_id
        and pis.cleared_at_utc is null
        and pis.cancelled_at_utc is null
        and (
          pis.snooze_until_date is null
          or pis.snooze_until_date >= v_today_uk
        )
        and (
          (
            bi.exact_source_ref is not null
            and lower(nullif(btrim(coalesce(pis.source_ref, '')), '')) = bi.exact_source_ref
          )
          or (
            bi.exact_source_ref is null
            and pis.source_ref is null
            and (
              (
                bi.timesheet_id is not null
                and pis.timesheet_id = bi.timesheet_id
              )
              or (
                nullif(btrim(coalesce(pis.booking_id, '')), '') is not null
                and nullif(btrim(coalesce(pis.booking_id, '')), '') = bi.booking_id
              )
            )
            and (
              (
                nullif(btrim(coalesce(pis.segment_stable_key, '')), '') is null
                and nullif(btrim(coalesce(pis.segment_id, '')), '') is null
              )
              or (
                bi.segment_stable_key is not null
                and nullif(btrim(coalesce(pis.segment_stable_key, '')), '') = bi.segment_stable_key
              )
              or (
                bi.segment_id is not null
                and nullif(btrim(coalesce(pis.segment_id, '')), '') = bi.segment_id
              )
              or (
                bi.segment_key is not null
                and bi.segment_key in (
                  nullif(btrim(coalesce(pis.segment_stable_key, '')), ''),
                  nullif(btrim(coalesce(pis.segment_id, '')), '')
                )
              )
            )
          )
        )
      order by pis.updated_at_utc desc, pis.created_at_utc desc, pis.id
      limit 1
    ) as active_snooze on true
  )
  select
    ms.timesheet_id,
    ms.candidate_id,
    'SNOOZE' as key_type,
    ms.pay_batch_item_id::text as key_value,
    'NO_ACTIVE_SNOOZE' as expected_text,
    jsonb_build_object(
      'snooze_id', ms.snooze_id::text,
      'snooze_kind', ms.snooze_kind,
      'snooze_until_date', case when ms.snooze_until_date is null then null else ms.snooze_until_date::text end,
      'note', ms.note,
      'source_ref', ms.source_ref,
      'match_scope', ms.match_scope,
      'frozen_item_source_ref', ms.exact_source_ref,
      'frozen_source_identity_kind', ms.source_identity_kind,
      'frozen_segment_stable_key', ms.segment_stable_key,
      'frozen_segment_id', ms.segment_id,
      'frozen_booking_id', ms.booking_id,
      'active_as_of_london_date', v_today_uk::text
    )::text as actual_text,
    34 as ord
  from matched_snoozes as ms;

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with referenced_finance_cases as (
    select distinct
      pbc.candidate_id,
      coalesce(
        pbi.finance_case_id,
        case
          when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
          else null
        end
      ) as finance_case_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and (
        pbi.finance_case_id is not null
        or pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
      )
  )
  select
    null::uuid as timesheet_id,
    rfc.candidate_id,
    'RESTRUCTURE' as key_type,
    rfc.finance_case_id::text as key_value,
    'NO_RESTRUCTURE_AFTER_BATCH_CREATED' as expected_text,
    jsonb_build_object(
      'finance_case_id', rfc.finance_case_id::text,
      'event_type', pfce.event_type,
      'event_at_utc', pfce.event_at_utc
    )::text as actual_text,
    35 as ord
  from referenced_finance_cases rfc
  join public.pay_finance_case_events pfce
    on pfce.finance_case_id = rfc.finance_case_id
  where pfce.event_at_utc >= coalesce(v_batch_created_at_utc, v_now_utc)
    and upper(coalesce(pfce.event_type, '')) like '%RESTRUCTURE%';

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with referenced_finance_cases as (
    select distinct
      pbc.candidate_id,
      coalesce(
        pbi.finance_case_id,
        case
          when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
          else null
        end
      ) as finance_case_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and (
        pbi.finance_case_id is not null
        or pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
      )
  )
  select
    null::uuid as timesheet_id,
    rfc.candidate_id,
    'WRITE_OFF' as key_type,
    rfc.finance_case_id::text as key_value,
    'NOT_WRITTEN_OFF' as expected_text,
    jsonb_build_object(
      'finance_case_id', pa.id::text,
      'written_off_at_utc', pa.written_off_at_utc,
      'status', pa.status::text
    )::text as actual_text,
    36 as ord
  from referenced_finance_cases rfc
  join public.pay_advances pa
    on pa.id = rfc.finance_case_id
  where pa.written_off_at_utc is not null
    and pa.written_off_at_utc >= coalesce(v_batch_created_at_utc, v_now_utc);

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_ts as (
    select distinct
      pbi.timesheet_id,
      pbc.candidate_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi.timesheet_id is not null
  ),
  batch_entitlement_ts as (
    select distinct
      pbi.timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and pbi.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      and pbi.timesheet_id is not null
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_entitlement_fresh_exclusion
        where ppc_pbi_entitlement_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_entitlement_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_entitlement_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
  ),
  override_changes as (
    select
      bt.timesheet_id,
      bt.candidate_id,
      tpo.id as override_id,
      tpo.override_type,
      tpo.created_at_utc,
      tpo.consumed_at_utc,
      tpo.consumed_by_pay_batch_id,
      tpo.cleared_at_utc,
      consumed_batch.status as consumed_batch_status,
      coalesce(tpo.created_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as created_after_batch,
      coalesce(tpo.cleared_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as cleared_after_batch,
      coalesce(tpo.consumed_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as consumed_after_batch,
      (
        upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
        and tpo.consumed_by_pay_batch_id = p_pay_batch_id
        and tpo.consumed_at_utc is not null
        and tpo.cleared_at_utc is null
        and coalesce(tpo.created_at_utc, '-infinity'::timestamptz) < coalesce(v_batch_created_at_utc, v_now_utc)
        and exists (
          select 1
          from batch_entitlement_ts bet
          where bet.timesheet_id = bt.timesheet_id
        )
      ) as expected_same_batch_advance_consumption
    from batch_ts bt
    join public.timesheet_payment_overrides tpo
      on tpo.timesheet_id = bt.timesheet_id
    left join public.pay_batches consumed_batch
      on consumed_batch.id = tpo.consumed_by_pay_batch_id
    where upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
  )
  select
    oc.timesheet_id,
    oc.candidate_id,
    'TIMESHEET_ADVANCE_OVERRIDE' as key_type,
    oc.timesheet_id::text as key_value,
    'NO_OVERRIDE_CHANGE_AFTER_BATCH_CREATED' as expected_text,
    jsonb_build_object(
      'override_id', oc.override_id::text,
      'override_type', oc.override_type,
      'created_at_utc', oc.created_at_utc,
      'consumed_at_utc', oc.consumed_at_utc,
      'consumed_by_pay_batch_id', case when oc.consumed_by_pay_batch_id is null then null else oc.consumed_by_pay_batch_id::text end,
      'consumed_batch_status', oc.consumed_batch_status,
      'cleared_at_utc', oc.cleared_at_utc
    )::text as actual_text,
    37 as ord
  from override_changes oc
  where oc.created_after_batch
     or oc.cleared_after_batch
     or (
          oc.consumed_after_batch
          and oc.expected_same_batch_advance_consumption is not true
          and (
            oc.consumed_by_pay_batch_id is null
            or upper(coalesce(oc.consumed_batch_status, '')) <> 'CANCELLED'
          )
        );

  select count(*)::int
  into v_finance_reservation_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'FINANCE_RESERVATION';

  if v_finance_reservation_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'FINANCE_RESERVATION_CHANGED');
  end if;

  select count(*)::int
  into v_snooze_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'SNOOZE';

  if v_snooze_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'SNOOZE_CHANGED');
  end if;

  select count(*)::int
  into v_restructure_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'RESTRUCTURE';

  if v_restructure_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'RESTRUCTURE_CHANGED');
  end if;

  select count(*)::int
  into v_writeoff_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'WRITE_OFF';

  if v_writeoff_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'WRITE_OFF_CHANGED');
  end if;

  select count(*)::int
  into v_timesheet_override_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'TIMESHEET_ADVANCE_OVERRIDE';

  if v_timesheet_override_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'TIMESHEET_PAYMENT_OVERRIDE_CHANGED');
  end if;

  if v_scope = 'PAYE' and coalesce(v_batch_kind_fixed, '') <> 'LOANS' then
    v_paye_guardrails := public.pay_paye_guardrails(
      p_pay_date => v_pay_date,
      p_ignore_pay_batch_id => p_pay_batch_id,
      p_actor_user_id => p_actor_user_id
    );

    if coalesce((v_paye_guardrails->>'create_paye_blocked')::boolean, false) = true
       or (
            coalesce((v_paye_guardrails->>'override_required')::boolean, false) = true
            and coalesce(v_same_week_paye_override_used, false) = false
          )
    then
      insert into pg_temp.tmp_fresh_state_diffs (
        timesheet_id,
        candidate_id,
        key_type,
        key_value,
        expected_text,
        actual_text,
        ord
      )
      values (
        null,
        null,
        'PAYE_GUARDRAILS',
        p_pay_batch_id::text,
        jsonb_build_object(
          'same_week_paye_override_used', coalesce(v_same_week_paye_override_used, false)
        )::text,
        v_paye_guardrails::text,
        38
      );

      select count(*)::int
      into v_paye_guardrail_diff_ct
      from pg_temp.tmp_fresh_state_diffs tsd
      where tsd.key_type = 'PAYE_GUARDRAILS';

      if v_paye_guardrail_diff_ct > 0 then
        v_is_stale := true;
        v_reasons := array_append(v_reasons, 'PAYE_GUARDRAILS_CHANGED');
      end if;
    end if;
  end if;


  ---------------------------------------------------------------------------
  -- DEDUCTION_CHANGED: recompute expected net-side OVERPAYMENT_RECOVERY,
  -- MANUAL_DEBT_RECOVERY, and LOAN_REPAYMENT from frozen batch-local
  -- deduction templates only. PAYE gross-side deductions are already reflected
  -- in the entered PAYE net, so their frozen rows are guarded by reservation and
  -- frozen-state checks above rather than being reprojected from that net.
  -- No live finance register / pay_advances authority is permitted here.
  ---------------------------------------------------------------------------
  if v_scope in ('PAYE','UMBRELLA') then
    create temporary table if not exists pg_temp.tmp_fresh_recovery_templates (
      item_type text not null,
      pay_batch_candidate_id uuid not null,
      candidate_id uuid not null,
      finance_case_id uuid null,
      source_ref text null,
      pay_channel text null,
      umbrella_id uuid null,
      is_mismatch boolean null,
      paye_treatment text null,
      finance_component_id uuid null,
      frozen_component_snapshot_json jsonb null,
      frozen_component_key_type text null,
      frozen_component_key_value text null,
      frozen_component_classification public.pay_finance_component_classification_enum null,
      frozen_source_basis_json jsonb null,
      frozen_source_pay_method text null,
      frozen_target_pay_method text null,
      frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
      frozen_resolution_payload_json jsonb null,
      frozen_resolution_result_json jsonb null,
      frozen_source_amount numeric(12,2) null,
      frozen_target_amount_ex_vat numeric(12,2) null,
      frozen_target_amount_vat numeric(12,2) null,
      frozen_target_amount_inc_vat numeric(12,2) null,
      payout_instruction_snapshot_json jsonb null,
      template_sort_at timestamptz null,
      sort_order integer not null,
      frozen_case_type text null,
      frozen_payout_status text null,
      frozen_remaining_source_amount numeric(12,2) not null,
      frozen_weekly_due_amount numeric(12,2) not null,
      frozen_next_due_week_start date null,
      minimum_earnings_threshold numeric(12,2) null,
      take_home_floor_override numeric(12,2) null,
      default_take_home_floor numeric(12,2) null
    ) on commit drop;

    truncate table pg_temp.tmp_fresh_recovery_templates;

    insert into pg_temp.tmp_fresh_recovery_templates (
      item_type,
      pay_batch_candidate_id,
      candidate_id,
      finance_case_id,
      source_ref,
      pay_channel,
      umbrella_id,
      is_mismatch,
      paye_treatment,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json,
      template_sort_at,
      sort_order,
      frozen_case_type,
      frozen_payout_status,
      frozen_remaining_source_amount,
      frozen_weekly_due_amount,
      frozen_next_due_week_start,
      minimum_earnings_threshold,
      take_home_floor_override,
      default_take_home_floor
    )
    select distinct on (
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      coalesce(
        coalesce(
          pbi_rt.finance_case_id,
          case
            when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
            else null::uuid
          end
        )::text,
        ''
      ),
      coalesce(pbi_rt.finance_component_id::text, ''),
      coalesce(pbi_rt.source_ref, '')
    )
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      pbc_rt.candidate_id,
      coalesce(
        pbi_rt.finance_case_id,
        case
          when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
          else null::uuid
        end
      ) as finance_case_id,
      pbi_rt.source_ref,
      pbi_rt.pay_channel,
      pbi_rt.umbrella_id,
      pbi_rt.is_mismatch,
      pbi_rt.paye_treatment,
      pbi_rt.finance_component_id,
      pbi_rt.frozen_component_snapshot_json,
      pbi_rt.frozen_component_key_type,
      pbi_rt.frozen_component_key_value,
      pbi_rt.frozen_component_classification,
      pbi_rt.frozen_source_basis_json,
      pbi_rt.frozen_source_pay_method,
      pbi_rt.frozen_target_pay_method,
      pbi_rt.frozen_resolution_mode,
      pbi_rt.frozen_resolution_payload_json,
      pbi_rt.frozen_resolution_result_json,
      round(coalesce(pbi_rt.frozen_source_amount, 0), 2)::numeric(12,2) as frozen_source_amount,
      round(coalesce(pbi_rt.frozen_target_amount_ex_vat, 0), 2)::numeric(12,2) as frozen_target_amount_ex_vat,
      round(coalesce(pbi_rt.frozen_target_amount_vat, 0), 2)::numeric(12,2) as frozen_target_amount_vat,
      round(coalesce(pbi_rt.frozen_target_amount_inc_vat, 0), 2)::numeric(12,2) as frozen_target_amount_inc_vat,
      pbi_rt.payout_instruction_snapshot_json,
      coalesce(pbi_rt.created_at, pbi_rt.updated_at, now()) as template_sort_at,
      case
        when nullif(btrim(coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order')), '') ~ '^-?[0-9]+$'
          then (coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order'))::integer
        else 2147483647
      end as sort_order,
      upper(coalesce(
        nullif(btrim(pbi_rt.frozen_source_basis_json->>'case_type'), ''),
        case pbi_rt.item_type
          when 'OVERPAYMENT_RECOVERY' then 'OVERPAYMENT'
          when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE'
          when 'MANUAL_DEBT_RECOVERY' then 'MANUAL_DEBT_ADJUSTMENT'
          else null
        end
      )) as frozen_case_type,
      upper(nullif(btrim(pbi_rt.frozen_source_basis_json->>'payout_status'), '')) as frozen_payout_status,
      round(greatest(
        coalesce(
          case
            when pbi_rt.frozen_source_amount is not null then abs(pbi_rt.frozen_source_amount)
            when nullif(btrim(pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'remaining_source_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'outstanding_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'outstanding_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'amount'), '') is not null then abs((pbi_rt.frozen_source_basis_json->>'amount')::numeric)
            else 0::numeric
          end,
          0::numeric
        ),
        0::numeric
      ), 2)::numeric(12,2) as frozen_remaining_source_amount,
      case
        when nullif(btrim(pbi_rt.frozen_resolution_result_json->>'case_source_weekly_due'), '') is not null
          then round(greatest(abs((pbi_rt.frozen_resolution_result_json->>'case_source_weekly_due')::numeric), 0), 2)::numeric(12,2)
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'weekly_due'), '') is not null
          then round(greatest(abs((pbi_rt.frozen_source_basis_json->>'weekly_due')::numeric), 0), 2)::numeric(12,2)
        else 0::numeric(12,2)
      end as frozen_weekly_due_amount,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'next_due_week_start'), '') is null then null::date
        else (pbi_rt.frozen_source_basis_json->>'next_due_week_start')::date
      end as frozen_next_due_week_start,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold')::numeric, 0), 2)::numeric(12,2)
      end as minimum_earnings_threshold,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'take_home_floor_override'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'take_home_floor_override')::numeric, 0), 2)::numeric(12,2)
      end as take_home_floor_override,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'default_take_home_floor'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'default_take_home_floor')::numeric, 0), 2)::numeric(12,2)
      end as default_take_home_floor
    from public.pay_batch_items pbi_rt
    join public.pay_batch_candidates pbc_rt
      on pbc_rt.id = pbi_rt.pay_batch_candidate_id
    where pbc_rt.pay_batch_id = p_pay_batch_id
      and coalesce(pbi_rt.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_rt_fresh_exclusion
        where ppc_pbi_rt_fresh_exclusion.pay_batch_item_id = pbi_rt.id
          and ppc_pbi_rt_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_rt_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi_rt.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
    order by
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      coalesce(
        coalesce(
          pbi_rt.finance_case_id,
          case
            when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
            else null::uuid
          end
        )::text,
        ''
      ),
      coalesce(pbi_rt.finance_component_id::text, ''),
      coalesce(pbi_rt.source_ref, ''),
      pbi_rt.is_voided desc,
      coalesce(pbi_rt.updated_at, pbi_rt.created_at) desc,
      pbi_rt.id desc;

    insert into pg_temp.tmp_fresh_state_diffs (
      timesheet_id,
      candidate_id,
      key_type,
      key_value,
      expected_text,
      actual_text,
      ord
    )
    select
      null::uuid as timesheet_id,
      pbc_bad.candidate_id,
      'DEDUCTION_TEMPLATE' as key_type,
      pbi_bad.id::text as key_value,
      'FROZEN_TEMPLATE_WITH_FINANCE_CASE_ID' as expected_text,
      jsonb_build_object(
        'pay_batch_item_id', pbi_bad.id::text,
        'item_type', pbi_bad.item_type,
        'source_ref', pbi_bad.source_ref,
        'finance_case_id', pbi_bad.finance_case_id::text
      )::text as actual_text,
      30 as ord
    from public.pay_batch_items pbi_bad
    join public.pay_batch_candidates pbc_bad
      on pbc_bad.id = pbi_bad.pay_batch_candidate_id
    where pbc_bad.pay_batch_id = p_pay_batch_id
      and coalesce(pbi_bad.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_bad_fresh_exclusion
        where ppc_pbi_bad_fresh_exclusion.pay_batch_item_id = pbi_bad.id
          and ppc_pbi_bad_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_bad_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi_bad.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
      and pbi_bad.finance_case_id is null;

    insert into pg_temp.tmp_fresh_expected_overpay_alloc (
      candidate_id,
      advance_id,
      take_ex,
      ord
    )
    with candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        greatest(
          coalesce(
            case
              when v_scope = 'PAYE' then pni.net_amount
              else (
                select sum(pbi_pos.amount_ex_vat)
                from public.pay_batch_items pbi_pos
                where pbi_pos.pay_batch_candidate_id = pbc.id
                  and coalesce(pbi_pos.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_pos_fresh_exclusion
        where ppc_pbi_pos_fresh_exclusion.pay_batch_item_id = pbi_pos.id
          and ppc_pbi_pos_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_pos_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
                  and pbi_pos.amount_ex_vat > 0
                  and pbi_pos.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
              )
            end,
            0
          ),
          0
        )::numeric(12,2) as earnings_pool_ex
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'OVERPAYMENT_RECOVERY'
        and rt.finance_case_id is not null
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    alloc_base as (
      select
        cb.candidate_id,
        ct.finance_case_id,
        ct.frozen_remaining_source_amount,
        ct.sort_order,
        ct.template_sort_at,
        cb.earnings_pool_ex,
        sum(ct.frozen_remaining_source_amount) over (
          partition by cb.candidate_id
          order by ct.sort_order, ct.template_sort_at, ct.finance_case_id, ct.finance_component_id, coalesce(ct.source_ref, '')
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      where (v_scope <> 'PAYE' or cb.awaiting_net_amount = false)
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'OVERPAYMENT')) = 'OVERPAYMENT'
    )
    select
      ab.candidate_id,
      ab.finance_case_id as advance_id,
      round(
        least(
          ab.frozen_remaining_source_amount,
          greatest(ab.earnings_pool_ex - coalesce(ab.cum_before_ex, 0), 0)
        ),
        2
      )::numeric(12,2) as take_ex,
      31 as ord
    from alloc_base ab
    where round(
      least(
        ab.frozen_remaining_source_amount,
        greatest(ab.earnings_pool_ex - coalesce(ab.cum_before_ex, 0), 0)
      ),
      2
    ) > 0;

    insert into pg_temp.tmp_fresh_candidate_expected_overpay (
      candidate_id,
      expected_overpayment_recovery_ex
    )
    select
      teo.candidate_id,
      round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpayment_recovery_ex
    from pg_temp.tmp_fresh_expected_overpay_alloc teo
    group by teo.candidate_id;

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_overpay as (
      select
        coalesce(
          pbi.finance_case_id,
          case
            when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi.frozen_source_amount), -pbi.amount_ex_vat, -pbi.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = p_pay_batch_id
        and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi.item_type = 'OVERPAYMENT_RECOVERY'
        and pbi.repayment_week_start = v_week_start
        and (
          v_scope <> 'PAYE'
          or (
            coalesce(pbc.awaiting_net_amount, false) = false
            and upper(coalesce(pbi.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
          )
        )
      group by 1
    ),
    actual_overpay as (
      select
        teo.advance_id as finance_case_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.advance_id
    ),
    union_keys as (
      select beo.finance_case_id from batch_expected_overpay beo
      union
      select ao.finance_case_id from actual_overpay ao
    )
    select
      'OVERPAYMENT_RECOVERY' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(beo.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(ao.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      31 as ord
    from union_keys uk
    left join batch_expected_overpay beo
      on beo.finance_case_id = uk.finance_case_id
    left join actual_overpay ao
      on ao.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(beo.expected_ex, 0), 2) <> round(coalesce(ao.actual_ex, 0), 2);

    insert into pg_temp.tmp_fresh_expected_loan_alloc (
      candidate_id,
      loan_id,
      take_ex,
      ord
    )
    with batch_expected_overpay_by_candidate as (
      select
        teo.candidate_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpay_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.candidate_id
    ),
    candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        greatest(coalesce(pni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'LOAN_REPAYMENT'
        and rt.finance_case_id is not null
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    loan_seed as (
      select
        cb.pay_batch_candidate_id,
        cb.candidate_id,
        ct.finance_case_id,
        ct.template_sort_at,
        ct.sort_order,
        round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2)::numeric(12,2) as nominal_due_amount,
        ct.minimum_earnings_threshold,
        ct.take_home_floor_override,
        round(greatest(cb.paye_net_amount - coalesce(beo.expected_overpay_ex, 0), 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
        round(greatest(cb.paye_net_amount - coalesce(beo.expected_overpay_ex, 0), 0), 2)::numeric(12,2) as run_take_home_before_ex
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      left join batch_expected_overpay_by_candidate beo
        on beo.candidate_id = cb.candidate_id
      where cb.awaiting_net_amount = false
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'PAYMENT_ADVANCE')) = 'PAYMENT_ADVANCE'
        and upper(coalesce(ct.frozen_payout_status, 'PAID')) = 'PAID'
        and (ct.frozen_next_due_week_start is null or ct.frozen_next_due_week_start <= v_week_start)
        and round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2) > 0
    ),
    loan_inputs as (
      select
        ls.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'sort_order', ls.sort_order,
              'finance_case_id', ls.finance_case_id::text,
              'case_type', 'PAYMENT_ADVANCE',
              'payout_status', 'PAID',
              'nominal_due_amount', ls.nominal_due_amount,
              'minimum_earnings_threshold', ls.minimum_earnings_threshold,
              'take_home_floor_override', ls.take_home_floor_override
            )
            order by ls.sort_order, ls.template_sort_at, ls.finance_case_id
          ),
          '[]'::jsonb
        ) as recovery_rows_json,
        max(ls.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
        max(ls.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex
      from loan_seed ls
      group by ls.candidate_id
    ),
    loan_alloc as (
      select
        li.candidate_id,
        lra.finance_case_id,
        round(coalesce(lra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
      from loan_inputs li
      cross join lateral public._pay_finance_protected_recovery_allocate(
        li.recovery_rows_json,
        li.run_earnings_headroom_ex,
        li.run_take_home_before_ex,
        null::numeric
      ) lra
    )
    select
      la.candidate_id,
      la.finance_case_id as loan_id,
      la.take_ex,
      33 as ord
    from loan_alloc la
    where round(coalesce(la.take_ex, 0), 2) > 0;

    insert into pg_temp.tmp_fresh_expected_manual_debt_alloc (
      candidate_id,
      finance_case_id,
      take_ex,
      ord
    )
    with batch_expected_overpay_by_candidate as (
      select
        teo.candidate_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpay_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.candidate_id
    ),
    batch_expected_loan_by_candidate as (
      select
        tel.candidate_id,
        round(sum(tel.take_ex), 2)::numeric(12,2) as expected_loan_ex
      from pg_temp.tmp_fresh_expected_loan_alloc tel
      group by tel.candidate_id
    ),
    candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        round(greatest(coalesce(pbc.gross_preview, 0), 0), 2)::numeric(12,2) as gross_preview_ex,
        greatest(coalesce(pni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount,
        case
          when v_scope = 'PAYE' then round(greatest(coalesce(pni.net_amount, 0), 0), 2)::numeric(12,2)
          else null::numeric(12,2)
        end as run_take_home_base_ex
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'MANUAL_DEBT_RECOVERY'
        and rt.finance_case_id is not null
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    manual_template_values as (
      select
        cb.pay_batch_candidate_id,
        cb.candidate_id,
        ct.finance_case_id,
        ct.template_sort_at,
        ct.sort_order,
        ct.frozen_case_type,
        ct.frozen_next_due_week_start,
        ct.minimum_earnings_threshold,
        ct.take_home_floor_override,
        ct.frozen_weekly_due_amount,
        ct.frozen_remaining_source_amount,
        nullif(ct.default_take_home_floor, 0)::numeric(12,2) as default_take_home_floor,
        round(greatest(cb.gross_preview_ex, 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
        case
          when v_scope = 'PAYE' then round(greatest(coalesce(cb.run_take_home_base_ex, 0) - coalesce(beo.expected_overpay_ex, 0) - coalesce(bl.expected_loan_ex, 0), 0), 2)::numeric(12,2)
          else null::numeric(12,2)
        end as run_take_home_before_ex,
        sched_md.scheduled_due_amount
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      left join batch_expected_overpay_by_candidate beo
        on beo.candidate_id = cb.candidate_id
      left join batch_expected_loan_by_candidate bl
        on bl.candidate_id = cb.candidate_id
      left join lateral (
        select
          round(greatest(abs((sched_item.value->>'amount')::numeric), 0), 2)::numeric(12,2) as scheduled_due_amount
        from jsonb_array_elements(
          case
            when jsonb_typeof(coalesce(ct.frozen_source_basis_json->'schedule_json', '[]'::jsonb)) = 'array'
              then coalesce(ct.frozen_source_basis_json->'schedule_json', '[]'::jsonb)
            else '[]'::jsonb
          end
        ) as sched_item(value)
        where nullif(btrim(sched_item.value->>'week_start'), '') is not null
          and (sched_item.value->>'week_start')::date = v_week_start
        limit 1
      ) sched_md on true
      where (v_scope <> 'PAYE' or cb.awaiting_net_amount = false)
    ),
    manual_seed as (
      select
        mtv.candidate_id,
        mtv.finance_case_id,
        mtv.template_sort_at,
        mtv.sort_order,
        round(greatest(least(coalesce(mtv.scheduled_due_amount, mtv.frozen_weekly_due_amount, 0::numeric(12,2)), mtv.frozen_remaining_source_amount), 0), 2)::numeric(12,2) as nominal_due_amount,
        mtv.minimum_earnings_threshold,
        mtv.take_home_floor_override,
        mtv.run_earnings_headroom_ex,
        mtv.run_take_home_before_ex,
        mtv.default_take_home_floor
      from manual_template_values mtv
      where upper(coalesce(mtv.frozen_case_type, 'MANUAL_DEBT_ADJUSTMENT')) = 'MANUAL_DEBT_ADJUSTMENT'
        and mtv.frozen_remaining_source_amount > 0
        and (mtv.frozen_next_due_week_start is null or mtv.frozen_next_due_week_start <= v_week_start)
        and round(greatest(least(coalesce(mtv.scheduled_due_amount, mtv.frozen_weekly_due_amount, 0::numeric(12,2)), mtv.frozen_remaining_source_amount), 0), 2) > 0
    ),
    manual_inputs as (
      select
        ms.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'sort_order', ms.sort_order,
              'finance_case_id', ms.finance_case_id::text,
              'case_type', 'MANUAL_DEBT_ADJUSTMENT',
              'payout_status', null,
              'nominal_due_amount', ms.nominal_due_amount,
              'minimum_earnings_threshold', ms.minimum_earnings_threshold,
              'take_home_floor_override', ms.take_home_floor_override
            )
            order by ms.sort_order, ms.template_sort_at, ms.finance_case_id
          ),
          '[]'::jsonb
        ) as recovery_rows_json,
        max(ms.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
        max(ms.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex,
        nullif(max(ms.default_take_home_floor), 0)::numeric(12,2) as default_take_home_floor
      from manual_seed ms
      group by ms.candidate_id
    ),
    manual_alloc as (
      select
        mi.candidate_id,
        mdra.finance_case_id,
        round(coalesce(mdra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
      from manual_inputs mi
      cross join lateral public._pay_finance_protected_recovery_allocate(
        mi.recovery_rows_json,
        mi.run_earnings_headroom_ex,
        mi.run_take_home_before_ex,
        mi.default_take_home_floor
      ) mdra
    )
    select
      ma.candidate_id,
      ma.finance_case_id,
      ma.take_ex,
      32 as ord
    from manual_alloc ma
    where round(coalesce(ma.take_ex, 0), 2) > 0;

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_manual as (
      select
        coalesce(
          pbi_md.finance_case_id,
          case
            when pbi_md.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_md.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi_md.frozen_source_amount), -pbi_md.amount_ex_vat, -pbi_md.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi_md
      join public.pay_batch_candidates pbc_md
        on pbc_md.id = pbi_md.pay_batch_candidate_id
      where pbc_md.pay_batch_id = p_pay_batch_id
        and coalesce(pbi_md.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_md_fresh_exclusion
        where ppc_pbi_md_fresh_exclusion.pay_batch_item_id = pbi_md.id
          and ppc_pbi_md_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_md_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi_md.item_type = 'MANUAL_DEBT_RECOVERY'
        and pbi_md.repayment_week_start = v_week_start
        and (
          v_scope <> 'PAYE'
          or (
            coalesce(pbc_md.awaiting_net_amount, false) = false
            and upper(coalesce(pbi_md.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
          )
        )
      group by 1
    ),
    actual_manual as (
      select
        temd.finance_case_id,
        round(sum(temd.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_manual_debt_alloc temd
      group by temd.finance_case_id
    ),
    union_keys as (
      select bem.finance_case_id from batch_expected_manual bem
      union
      select am.finance_case_id from actual_manual am
    )
    select
      'MANUAL_DEBT_RECOVERY' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(bem.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(am.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      32 as ord
    from union_keys uk
    left join batch_expected_manual bem
      on bem.finance_case_id = uk.finance_case_id
    left join actual_manual am
      on am.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(bem.expected_ex, 0), 2) <> round(coalesce(am.actual_ex, 0), 2);

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_loans as (
      select
        coalesce(
          pbi_ln.finance_case_id,
          case
            when pbi_ln.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_ln.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi_ln.frozen_source_amount), -pbi_ln.amount_ex_vat, -pbi_ln.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi_ln
      join public.pay_batch_candidates pbc_ln
        on pbc_ln.id = pbi_ln.pay_batch_candidate_id
      where pbc_ln.pay_batch_id = p_pay_batch_id
        and coalesce(pbi_ln.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_ln_fresh_exclusion
        where ppc_pbi_ln_fresh_exclusion.pay_batch_item_id = pbi_ln.id
          and ppc_pbi_ln_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_ln_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi_ln.item_type = 'LOAN_REPAYMENT'
        and pbi_ln.repayment_week_start = v_week_start
        and (
          v_scope <> 'PAYE'
          or (
            coalesce(pbc_ln.awaiting_net_amount, false) = false
            and upper(coalesce(pbi_ln.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
          )
        )
      group by 1
    ),
    actual_loans as (
      select
        tel.loan_id as finance_case_id,
        round(sum(tel.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_loan_alloc tel
      group by tel.loan_id
    ),
    union_keys as (
      select bel.finance_case_id from batch_expected_loans bel
      union
      select al.finance_case_id from actual_loans al
    )
    select
      'LOAN_REPAYMENT' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(bel.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(al.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      33 as ord
    from union_keys uk
    left join batch_expected_loans bel
      on bel.finance_case_id = uk.finance_case_id
    left join actual_loans al
      on al.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(bel.expected_ex, 0), 2) <> round(coalesce(al.actual_ex, 0), 2);

    select count(*)::int
    into v_overpay_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'OVERPAYMENT_RECOVERY';

    select count(*)::int
    into v_manual_debt_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'MANUAL_DEBT_RECOVERY';

    select count(*)::int
    into v_loan_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'LOAN_REPAYMENT';

    v_ded_diff_ct := coalesce(v_overpay_ded_diff_ct, 0)
                     + coalesce(v_manual_debt_ded_diff_ct, 0)
                     + coalesce(v_loan_ded_diff_ct, 0);

    if exists (
      select 1
      from pg_temp.tmp_fresh_state_diffs tsd
      where tsd.key_type = 'DEDUCTION_TEMPLATE'
    ) then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_TEMPLATE_MISSING');
    end if;

    if v_ded_diff_ct > 0 then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_CHANGED');
    end if;
  end if;
  ---------------------------------------------------------------------------
  -- PAYE_NET_CHANGED:
  -- PAYE net entry and PAYE net edits are draft-owned mutable batch data,
  -- not live-truth freshness drift. Users must be able to enter, save,
  -- revisit, and amend PAYE net values on an existing draft without making
  -- the batch stale.
  --
  -- Real live-truth drift for the batch remains covered by the economic-key,
  -- reservation, deduction, snooze, override, restructuring, writeoff, and
  -- guardrail checks above. PAYE_NET is therefore intentionally excluded from
  -- stale detection here.
  ---------------------------------------------------------------------------
  if v_scope = 'PAYE' and coalesce(v_batch_kind_fixed,'') <> 'LOANS' then
    v_paye_net_diff_ct := 0;
  end if;

  ---------------------------------------------------------------------------
  -- Build final diff array from the temp diff tables.
  ---------------------------------------------------------------------------
  with diff_rows as (
    select
      t.ord,
      coalesce(t.timesheet_id::text, '') as sort1,
      t.key_type as sort2,
      t.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', t.timesheet_id::text,
        'key_type', t.key_type,
        'key_value', t.key_value,
        'expected', t.expected_text,
        'actual', t.actual_text
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_ts_diffs t0
      order by coalesce(t0.timesheet_id::text, ''), t0.key_type, t0.key_value
      limit v_diff_limit
    ) t

    union all

    select
      k.ord,
      coalesce(k.timesheet_id::text, '') as sort1,
      k.key_type as sort2,
      k.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', k.timesheet_id::text,
        'key_type', k.key_type,
        'key_value', k.key_value,
        'expected', k.expected_ex,
        'actual', k.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_key_diffs k0
      order by coalesce(k0.timesheet_id::text, ''), k0.key_type, k0.key_value
      limit v_diff_limit
    ) k

    union all

    select
      d.ord,
      '' as sort1,
      d.key_type as sort2,
      d.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', d.key_type,
        'key_value', d.key_value,
        'expected', d.expected_ex,
        'actual', d.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_deduction_diffs d0
      order by d0.ord, d0.key_type, d0.key_value
      limit v_diff_limit
    ) d

    union all

    select
      p.ord,
      p.candidate_id::text as sort1,
      p.key_type as sort2,
      p.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', p.key_type,
        'key_value', p.key_value,
        'expected', p.expected_ex,
        'actual', p.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_paye_net_diffs p0
      order by p0.candidate_id::text, p0.key_type, p0.key_value
      limit v_diff_limit
    ) p

    union all

    select
      s.ord,
      coalesce(s.candidate_id::text, coalesce(s.timesheet_id::text, '')) as sort1,
      s.key_type as sort2,
      s.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', s.timesheet_id::text,
        'key_type', s.key_type,
        'key_value', s.key_value,
        'expected', s.expected_text,
        'actual', s.actual_text
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_state_diffs s0
      order by coalesce(s0.candidate_id::text, coalesce(s0.timesheet_id::text, '')), s0.key_type, s0.key_value
      limit v_diff_limit
    ) s

    union all

    select
      99 as ord,
      '' as sort1,
      'INFO' as sort2,
      'COUNTS' as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', 'INFO',
        'key_value', 'COUNTS',
        'expected', jsonb_build_object(
          'ts_changed', v_ts_changed_ct,
          'key_diffs', v_key_diff_ct,
          'finance_reservation_diffs', v_finance_reservation_diff_ct,
          'snooze_diffs', v_snooze_diff_ct,
          'restructure_diffs', v_restructure_diff_ct,
          'writeoff_diffs', v_writeoff_diff_ct,
          'timesheet_override_diffs', v_timesheet_override_diff_ct,
          'paye_guardrail_diffs', v_paye_guardrail_diff_ct,
          'ded_diffs', v_ded_diff_ct,
          'overpayment_ded_diffs', v_overpay_ded_diff_ct,
          'manual_debt_ded_diffs', v_manual_debt_ded_diff_ct,
          'loan_ded_diffs', v_loan_ded_diff_ct,
          'paye_net_diffs', v_paye_net_diff_ct
        ),
        'actual', null
      ) as diff_json
  )
  select
    coalesce(
      jsonb_agg(dr.diff_json order by dr.ord, dr.sort1, dr.sort2, dr.sort3),
      '[]'::jsonb
    )
  into v_diffs
  from diff_rows dr;

  v_carry_forward_freshness_result := public._pay_manual_adjustment_carry_forward_freshness_check(
    p_pay_batch_id,
    NULL::uuid[],
    NULL::uuid[],
    jsonb_build_object('pay_batch_id', p_pay_batch_id::text),
    p_actor_user_id
  );

  v_carry_forward_blocker_count := jsonb_array_length(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb));

  IF COALESCE(v_carry_forward_blocker_count, 0) > 0 THEN
    v_is_stale := true;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') IN (
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_AMOUNT_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_SOURCE_AMOUNT_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_RESERVED_ELSEWHERE',
        'RESERVED_CARRY_FORWARD_MISSING_FROM_TARGET_BATCH',
        'RESERVED_CARRY_FORWARD_TARGET_ITEM_VOIDED',
        'PENDING_CARRY_FORWARD_NOT_INCLUDED_IN_TARGET_BATCH'
      )
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward changed');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward was consumed elsewhere');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
    ) THEN
      v_reasons := array_append(v_reasons, 'Source payment scope changed');
    END IF;

    SELECT COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', NULL,
          'key_type', 'MANUAL_CARRY_FORWARD',
          'key_value', COALESCE(carry_forward_blockers.blocker_value->>'carry_forward_id', carry_forward_blockers.blocker_value->>'code'),
          'expected', jsonb_build_object(
            'status', 'fresh',
            'message', 'Manual adjustment carry-forward remains reserved for this batch with unchanged signed amounts.'
          ),
          'actual', carry_forward_blockers.blocker_value
        )
        ORDER BY COALESCE(carry_forward_blockers.blocker_value->>'carry_forward_id', ''), COALESCE(carry_forward_blockers.blocker_value->>'code', '')
      ),
      '[]'::jsonb
    )
    INTO v_carry_forward_freshness_result
    FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value);

    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || COALESCE(v_carry_forward_freshness_result, '[]'::jsonb);
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS correction_items
    WHERE correction_items.pay_batch_id = p_pay_batch_id
      AND correction_items.status = 'APPLIED'
      AND correction_items.correction_item_kind = 'PRE_BANK_CANCEL'
  ) THEN
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'Payment was cancelled/recalculated');
    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || jsonb_build_array(jsonb_build_object(
      'timesheet_id', NULL,
      'key_type', 'PAYMENT_CORRECTION',
      'key_value', 'PRE_BANK_CANCEL',
      'expected', jsonb_build_object('payment_scope', 'active'),
      'actual', jsonb_build_object('payment_scope', 'cancelled/recalculated')
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS correction_items
    WHERE correction_items.pay_batch_id = p_pay_batch_id
      AND correction_items.status = 'APPLIED'
      AND correction_items.correction_item_kind = 'NO_MONEY_UNWIND'
  ) THEN
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'Financials were rewound');
    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || jsonb_build_array(jsonb_build_object(
      'timesheet_id', NULL,
      'key_type', 'PAYMENT_CORRECTION',
      'key_value', 'NO_MONEY_UNWIND',
      'expected', jsonb_build_object('financials', 'reserved/active'),
      'actual', jsonb_build_object('financials', 'rewound')
    ));
  END IF;

  if array_length(v_reasons,1) is not null then
    select coalesce(array_agg(distinct r order by r), array[]::text[])
    into v_reasons
    from unnest(v_reasons) r;
  end if;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_VALIDATE_FRESHNESS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'pay_date', v_pay_date::text,
        'week_start', v_week_start::text,
        'batch_kind_fixed', coalesce(v_batch_kind_fixed, null),
        'scope', coalesce(v_scope, null),
        'is_stale', v_is_stale,
        'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
        'counts', jsonb_build_object(
          'timesheet_changed', v_ts_changed_ct,
          'stable_key_diffs', v_key_diff_ct,
          'finance_reservation_diffs', v_finance_reservation_diff_ct,
          'snooze_diffs', v_snooze_diff_ct,
          'restructure_diffs', v_restructure_diff_ct,
          'writeoff_diffs', v_writeoff_diff_ct,
          'timesheet_override_diffs', v_timesheet_override_diff_ct,
          'paye_guardrail_diffs', v_paye_guardrail_diff_ct,
          'deduction_diffs', v_ded_diff_ct,
          'overpayment_deduction_diffs', v_overpay_ded_diff_ct,
          'manual_debt_deduction_diffs', v_manual_debt_ded_diff_ct,
          'loan_deduction_diffs', v_loan_ded_diff_ct,
          'paye_net_diffs', v_paye_net_diff_ct
        )
      ),
      'pay_batches',
      p_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'is_stale', v_is_stale,
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'diff', coalesce(v_diffs, '[]'::jsonb)
  );
end;
$function$;
