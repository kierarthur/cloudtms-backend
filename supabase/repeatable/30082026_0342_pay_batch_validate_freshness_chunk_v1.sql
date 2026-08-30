-- Complete current source owner for the five-argument Banking Pay freshness chunk.
-- The signed baseline and current contract already contain this exact routine;
-- LEGACY_UPGRADE proved that it previously lacked an active repeatable owner.
-- The function body below is byte-for-byte the signed baseline definition.

CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness_chunk(p_operation_id uuid, p_chunk_id uuid, p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_diff_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
declare
  v_operation public.banking_pay_operations%rowtype;
  v_today_uk date := (clock_timestamp() AT TIME ZONE 'Europe/London')::date;
  v_chunk public.banking_pay_operation_chunks%rowtype;
  v_batch public.pay_batches%rowtype;
  v_actor_is_valid boolean := true;
  v_diff_limit integer;
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_item_ids uuid[] := array[]::uuid[];
  v_candidate_ids uuid[] := array[]::uuid[];
  v_checked_units integer := 0;
  v_checked_item_count integer := 0;
  v_checked_timesheet_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_key_diff_count integer := 0;
  v_other_reservation_count integer := 0;
  v_finance_reservation_diff_count integer := 0;
  v_snooze_count integer := 0;
  v_restructure_or_writeoff_count integer := 0;
  v_timesheet_override_count integer := 0;
  v_deduction_diff_count integer := 0;
  v_candidate_scope_deduction_diff_count integer := 0;
  v_stale_count integer := 0;
  v_is_stale boolean := false;
  v_reasons text[] := array[]::text[];
  v_stale_reason_counts jsonb := '{}'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;
  v_chunk_blockers jsonb := '[]'::jsonb;
  v_chunk_result_hash text := null;
  v_result jsonb := '{}'::jsonb;
  v_carry_forward_freshness_result jsonb := '{}'::jsonb;
  v_carry_forward_blocker_count integer := 0;
begin
  if p_operation_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_ID_REQUIRED')::text;
  end if;

  if p_chunk_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_CHUNK_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_CHUNK_ID_REQUIRED')::text;
  end if;

  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  v_diff_limit := coalesce(p_diff_limit, 50);
  if v_diff_limit < 0 then
    v_diff_limit := 0;
  elsif v_diff_limit > 250 then
    v_diff_limit := 250;
  end if;

  select operation_row.*
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_operation_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_NOT_FOUND',
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_operation.pay_batch_id is not null and v_operation.pay_batch_id <> p_pay_batch_id then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_BATCH_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_BATCH_MISMATCH',
              'operation_id', p_operation_id::text,
              'operation_pay_batch_id', v_operation.pay_batch_id::text,
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  select pay_batch_row.*
  into v_batch
  from public.pay_batches as pay_batch_row
  where pay_batch_row.id = p_pay_batch_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  if p_actor_user_id is not null then
    select exists (
      select 1
      from public.tms_users as actor_user
      where actor_user.id = p_actor_user_id
        and coalesce(actor_user.is_active, false) = true
    )
    into v_actor_is_valid;

    if coalesce(v_actor_is_valid, false) is not true then
      raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_ACTOR_NOT_ALLOWED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    end if;
  end if;

  select chunk_row.*
  into v_chunk
  from public.banking_pay_operation_chunks as chunk_row
  where chunk_row.id = p_chunk_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_NOT_FOUND',
              'chunk_id', p_chunk_id::text
            )::text;
  end if;

  if v_chunk.operation_id <> p_operation_id then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_MISMATCH',
              'chunk_id', p_chunk_id::text,
              'chunk_operation_id', v_chunk.operation_id::text,
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_chunk.phase <> 'VALIDATE_FRESHNESS' or v_chunk.chunk_type <> 'FRESHNESS_VALIDATE' then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_CHUNK_KIND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_CHUNK_KIND',
              'chunk_id', p_chunk_id::text,
              'phase', v_chunk.phase,
              'chunk_type', v_chunk.chunk_type
            )::text;
  end if;

  if jsonb_typeof(v_chunk.payload_json) <> 'object' or jsonb_typeof(v_chunk.payload_json->'units') <> 'array' then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_PAYLOAD'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_PAYLOAD',
              'chunk_id', p_chunk_id::text
            )::text;
  end if;

  select coalesce(array_agg(distinct (unit_value->>'timesheet_id')::uuid) filter (where nullif(unit_value->>'timesheet_id', '') is not null), array[]::uuid[]),
         coalesce(array_agg(distinct (item_id_text.item_id)::uuid) filter (where item_id_text.item_id is not null), array[]::uuid[]),
         coalesce(array_agg(distinct (unit_value->>'candidate_id')::uuid) filter (where nullif(unit_value->>'candidate_id', '') is not null), array[]::uuid[]),
         count(*)::integer
  into v_timesheet_ids, v_item_ids, v_candidate_ids, v_checked_units
  from jsonb_array_elements(v_chunk.payload_json->'units') as unit_items(unit_value)
  left join lateral jsonb_array_elements_text(coalesce(unit_items.unit_value->'pay_batch_item_ids', '[]'::jsonb)) as item_id_text(item_id)
    on true;

  v_checked_timesheet_count := coalesce(array_length(v_timesheet_ids, 1), 0);
  v_checked_item_count := coalesce(array_length(v_item_ids, 1), 0);

  create temporary table if not exists pg_temp.tmp_validate_freshness_chunk_diffs (
    reason text not null,
    timesheet_id uuid null,
    pay_batch_item_id uuid null,
    candidate_id uuid null,
    key_type text null,
    key_value text null,
    expected jsonb null,
    actual jsonb null,
    ord integer not null
  ) on commit drop;

  truncate table pg_temp.tmp_validate_freshness_chunk_diffs;

  if v_checked_item_count > 0 then
  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
      reason,
      timesheet_id,
      pay_batch_item_id,
      candidate_id,
      key_type,
      key_value,
      expected,
      actual,
      ord
    )
    select
      'KEY_RESOLUTION_FAILED',
      batch_component.timesheet_id,
      batch_component.pay_batch_item_id,
      null::uuid,
      batch_component.key_type,
      batch_component.key_value,
      jsonb_build_object('batch_component', batch_component.pay_batch_item_id::text),
      jsonb_build_object('failure_reason', batch_component.key_resolution_failure_reason),
      10
    from public._pay_batch_item_economic_components(null::uuid, v_item_ids) as batch_component
    where nullif(btrim(coalesce(batch_component.key_resolution_failure_reason, '')), '') is not null;
  
    get diagnostics v_key_resolution_failure_count = row_count;
  
    /*
      Policy X / chunked freshness:
      Do not compare frozen batch economic components to live current entitlement
      or live reserved-component helpers in the chunk validator. The batch-side
      economic keyspace has already been resolved above through
      _pay_batch_item_economic_components(...) and unresolved frozen economic
      keys are surfaced as KEY_RESOLUTION_FAILED. Any later payment-scope,
      carry-forward, finance reservation, snooze, write-off, restructure, or
      correction drift is handled by the explicit frozen/artifact checks below.
    */
    v_key_diff_count := 0;
    v_other_reservation_count := 0;
  
  
  else
    v_key_resolution_failure_count := 0;
    v_key_diff_count := 0;
    v_other_reservation_count := 0;
  end if;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'FINANCE_RESERVATION_CHANGED',
    batch_item.timesheet_id,
    batch_item.id,
    batch_candidate.candidate_id,
    'FINANCE_RESERVATION',
    coalesce(batch_item.finance_case_id::text, batch_item.reservation_id::text, batch_item.finance_component_id::text, batch_item.id::text),
    jsonb_build_object('expected_status', case when upper(coalesce(v_batch.status, '')) in ('AUTHORISED_FOR_PAYMENT', 'SCHEDULED', 'EXECUTING') then 'COMMITTED' else 'RESERVED' end),
    jsonb_build_object('actual_status', reservation_row.status, 'reservation_id', reservation_row.id::text),
    40
  from public.pay_batch_items as batch_item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id = batch_item.pay_batch_candidate_id
  left join public.pay_advance_reservations as reservation_row
    on reservation_row.pay_batch_item_id = batch_item.id
  where batch_item.id = any(v_item_ids)
    and batch_item.finance_case_id is not null
    and (
      reservation_row.id is null
      or upper(coalesce(reservation_row.status, '')) <> case when upper(coalesce(v_batch.status, '')) in ('AUTHORISED_FOR_PAYMENT', 'SCHEDULED', 'EXECUTING') then 'COMMITTED' else 'RESERVED' end
    );

  get diagnostics v_finance_reservation_diff_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  with chunk_item_scope as (
    select
      batch_item.id as pay_batch_item_id,
      batch_item.timesheet_id,
      batch_candidate.candidate_id,
      nullif(btrim(coalesce(batch_item.source_ref, '')), '') as source_ref,
      nullif(btrim(coalesce(batch_item.segment_key, '')), '') as segment_key,
      coalesce(
        nullif(btrim(batch_item.frozen_source_basis_json #>> '{booking_id}'), ''),
        nullif(btrim(batch_item.frozen_source_basis_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(batch_item.frozen_component_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(batch_item.frozen_component_snapshot_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(batch_item.payout_instruction_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(batch_item.payout_instruction_snapshot_json #>> '{booking,booking_id}'), '')
      ) as frozen_booking_id,
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      upper(btrim(coalesce(batch_item.item_type, ''))) as item_type
    from public.pay_batch_items as batch_item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id = batch_item.pay_batch_candidate_id
    where batch_item.id = any(v_item_ids)
      and batch_candidate.pay_batch_id = p_pay_batch_id
  ), chunk_booking_scope as (
    select distinct
      chunk_item_scope.frozen_booking_id as booking_id
    from chunk_item_scope
    where chunk_item_scope.frozen_booking_id is not null

    union

    select distinct
      coalesce(
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{timesheet,booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{target,booking_id}'), '')
      ) as booking_id
    from public.pay_batch_timesheet_snapshots as snapshot_row
    where snapshot_row.pay_batch_id = p_pay_batch_id
      and snapshot_row.timesheet_id = any(v_timesheet_ids)
      and coalesce(
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{timesheet,booking_id}'), ''),
        nullif(btrim(snapshot_row.target_snapshot_json #>> '{target,booking_id}'), '')
      ) is not null
  ), scoped_snoozes as (
    select distinct
      snooze_row.id,
      snooze_row.timesheet_id,
      snooze_row.candidate_id,
      snooze_row.source_ref,
      snooze_row.segment_stable_key,
      snooze_row.segment_id,
      snooze_row.booking_id,
      snooze_row.snooze_kind,
      snooze_row.snooze_until_date
    from public.pay_item_snoozes as snooze_row
    where snooze_row.cleared_at_utc is null
      and snooze_row.cancelled_at_utc is null
      and (
        snooze_row.snooze_until_date is null
        or snooze_row.snooze_until_date >= v_today_uk
      )
      and (
        (snooze_row.timesheet_id is not null and snooze_row.timesheet_id = any(v_timesheet_ids))
        or exists (
          select 1
          from chunk_item_scope as source_ref_scope
          where source_ref_scope.source_ref is not null
            and nullif(btrim(coalesce(snooze_row.source_ref, '')), '') = source_ref_scope.source_ref
        )
        or exists (
          select 1
          from chunk_item_scope as segment_scope
          where segment_scope.segment_key is not null
            and (
              nullif(btrim(coalesce(snooze_row.segment_id, '')), '') = segment_scope.segment_key
              or nullif(btrim(coalesce(snooze_row.segment_stable_key, '')), '') = segment_scope.segment_key
            )
        )
        or exists (
          select 1
          from chunk_booking_scope as booking_scope
          where booking_scope.booking_id is not null
            and nullif(btrim(coalesce(snooze_row.booking_id, '')), '') = booking_scope.booking_id
        )
        or (
          snooze_row.candidate_id = any(v_candidate_ids)
          and snooze_row.timesheet_id is null
          and nullif(btrim(coalesce(snooze_row.source_ref, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.segment_id, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.segment_stable_key, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.booking_id, '')), '') is null
          and upper(btrim(coalesce(snooze_row.snooze_kind, ''))) in ('FINANCE', 'FINANCE_CASE', 'LOAN', 'OVERPAYMENT', 'MANUAL_DEBT', 'PAY_ADVANCE', 'RECOVERY')
          and exists (
            select 1
            from chunk_item_scope as finance_scope
            where finance_scope.candidate_id = snooze_row.candidate_id
              and (finance_scope.finance_case_id is not null or finance_scope.finance_component_id is not null)
          )
        )
      )
  )
  select
    'ACTIVE_SNOOZE_CHANGED',
    scoped_snoozes.timesheet_id,
    null::uuid,
    scoped_snoozes.candidate_id,
    'SNOOZE',
    coalesce(scoped_snoozes.source_ref, scoped_snoozes.segment_stable_key, scoped_snoozes.segment_id, scoped_snoozes.booking_id, scoped_snoozes.id::text),
    jsonb_build_object('expected', 'no_active_snooze_affecting_batch_scope'),
    jsonb_build_object('snooze_id', scoped_snoozes.id::text, 'snooze_kind', scoped_snoozes.snooze_kind, 'snooze_until_date', scoped_snoozes.snooze_until_date),
    50
  from scoped_snoozes;

  get diagnostics v_snooze_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'FINANCE_CASE_RESTRUCTURE_OR_WRITEOFF_CHANGED',
    batch_item.timesheet_id,
    batch_item.id,
    batch_candidate.candidate_id,
    'FINANCE_CASE',
    batch_item.finance_case_id::text,
    jsonb_build_object('expected', 'finance_case_open_and_not_written_off'),
    jsonb_build_object('advance_status', advance_row.status, 'written_off_at_utc', advance_row.written_off_at_utc),
    60
  from public.pay_batch_items as batch_item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id = batch_item.pay_batch_candidate_id
  join public.pay_advances as advance_row
    on advance_row.id = batch_item.finance_case_id
  where batch_item.id = any(v_item_ids)
    and batch_item.finance_case_id is not null
    and (
      advance_row.written_off_at_utc is not null
      or upper(coalesce(advance_row.status::text, '')) in ('CANCELLED', 'PAID_OFF')
    );

  get diagnostics v_restructure_or_writeoff_count = row_count;

  v_timesheet_override_count := 0;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  with candidate_finance_items as (
    select
      batch_candidate.candidate_id,
      batch_item.id as pay_batch_item_id,
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      round(abs(coalesce(batch_item.amount_ex_vat, 0)), 2)::numeric as item_amount_ex_vat
    from public.pay_batch_items as batch_item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id = batch_item.pay_batch_candidate_id
    where batch_candidate.pay_batch_id = p_pay_batch_id
      and batch_candidate.candidate_id = any(v_candidate_ids)
      and coalesce(batch_item.is_voided, false) = false
      and batch_item.item_type in ('LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY')
      and (batch_item.finance_case_id is not null or batch_item.finance_component_id is not null)
  ), finance_item_totals as (
    select
      candidate_finance_items.candidate_id,
      candidate_finance_items.finance_case_id,
      candidate_finance_items.finance_component_id,
      round(sum(candidate_finance_items.item_amount_ex_vat), 2)::numeric as item_total_ex_vat
    from candidate_finance_items
    group by candidate_finance_items.candidate_id, candidate_finance_items.finance_case_id, candidate_finance_items.finance_component_id
  ), reservation_totals as (
    select
      candidate_finance_items.candidate_id,
      reservation_row.finance_case_id,
      reservation_row.finance_component_id,
      round(sum(abs(coalesce(reservation_row.reserved_amount, reservation_row.frozen_rounded_target_amount, 0))), 2)::numeric as reservation_total_ex_vat
    from candidate_finance_items
    join public.pay_advance_reservations as reservation_row
      on reservation_row.pay_batch_item_id = candidate_finance_items.pay_batch_item_id
    group by candidate_finance_items.candidate_id, reservation_row.finance_case_id, reservation_row.finance_component_id
  ), mismatches as (
    select
      finance_item_totals.candidate_id,
      finance_item_totals.finance_case_id,
      finance_item_totals.finance_component_id,
      finance_item_totals.item_total_ex_vat,
      coalesce(reservation_totals.reservation_total_ex_vat, 0)::numeric as reservation_total_ex_vat
    from finance_item_totals
    left join reservation_totals
      on reservation_totals.candidate_id is not distinct from finance_item_totals.candidate_id
     and reservation_totals.finance_case_id is not distinct from finance_item_totals.finance_case_id
     and reservation_totals.finance_component_id is not distinct from finance_item_totals.finance_component_id
    where finance_item_totals.item_total_ex_vat <> coalesce(reservation_totals.reservation_total_ex_vat, 0)
  )
  select
    'CANDIDATE_SCOPE_DEDUCTION_MISMATCH',
    null::uuid,
    null::uuid,
    mismatches.candidate_id,
    'DEDUCTION_RECOVERY',
    coalesce(mismatches.finance_case_id::text, mismatches.finance_component_id::text),
    jsonb_build_object('candidate_batch_finance_item_total_ex_vat', mismatches.item_total_ex_vat),
    jsonb_build_object('candidate_batch_reservation_total_ex_vat', mismatches.reservation_total_ex_vat),
    80
  from mismatches;

  get diagnostics v_candidate_scope_deduction_diff_count = row_count;
  v_deduction_diff_count := v_candidate_scope_deduction_diff_count;

  v_carry_forward_freshness_result := public._pay_manual_adjustment_carry_forward_freshness_check(
    p_pay_batch_id,
    v_candidate_ids,
    v_item_ids,
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'candidate_ids', COALESCE((
        SELECT jsonb_agg(chunk_candidate_ids.candidate_id::text ORDER BY chunk_candidate_ids.candidate_id::text)
        FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS chunk_candidate_ids(candidate_id)
      ), '[]'::jsonb),
      'pay_batch_item_ids', COALESCE((
        SELECT jsonb_agg(chunk_item_ids.pay_batch_item_id::text ORDER BY chunk_item_ids.pay_batch_item_id::text)
        FROM unnest(COALESCE(v_item_ids, ARRAY[]::uuid[])) AS chunk_item_ids(pay_batch_item_id)
      ), '[]'::jsonb)
    ),
    p_actor_user_id
  );

  v_carry_forward_blocker_count := jsonb_array_length(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb));

  IF COALESCE(v_carry_forward_blocker_count, 0) > 0 THEN
    INSERT INTO pg_temp.tmp_validate_freshness_chunk_diffs (
      reason,
      timesheet_id,
      pay_batch_item_id,
      candidate_id,
      key_type,
      key_value,
      expected,
      actual,
      ord
    )
    SELECT
      CASE
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
          THEN 'Manual adjustment carry-forward was consumed elsewhere'
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
          THEN 'Source payment scope changed'
        ELSE 'Manual adjustment carry-forward changed'
      END AS reason,
      NULL::uuid AS timesheet_id,
      NULL::uuid AS pay_batch_item_id,
      NULL::uuid AS candidate_id,
      'MANUAL_CARRY_FORWARD'::text AS key_type,
      COALESCE(carry_forward_blockers.blocker_value->>'carry_forward_id', carry_forward_blockers.blocker_value->>'code') AS key_value,
      jsonb_build_object(
        'status', 'fresh',
        'message', 'Manual adjustment carry-forward remains reserved for this batch with unchanged signed amounts.'
      ) AS expected,
      carry_forward_blockers.blocker_value AS actual,
      90 AS ord
    FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value);

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_reason_blockers(blocker_value)
      WHERE COALESCE(carry_forward_reason_blockers.blocker_value->>'code', '') NOT IN (
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE',
        'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
      )
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward changed');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_reason_blockers(blocker_value)
      WHERE COALESCE(carry_forward_reason_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward was consumed elsewhere');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_reason_blockers(blocker_value)
      WHERE COALESCE(carry_forward_reason_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
    ) THEN
      v_reasons := array_append(v_reasons, 'Source payment scope changed');
    END IF;
  END IF;

  INSERT INTO pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  SELECT
    CASE
      WHEN correction_items.correction_item_kind = 'PRE_BANK_CANCEL' THEN 'Payment was cancelled/recalculated'
      WHEN correction_items.correction_item_kind = 'NO_MONEY_UNWIND' THEN 'Financials were rewound'
      ELSE 'Payment scope changed'
    END,
    NULL::uuid,
    correction_items.pay_batch_item_id,
    correction_items.candidate_id,
    'PAYMENT_CORRECTION',
    correction_items.correction_item_kind,
    jsonb_build_object('payment_scope', 'active'),
    jsonb_build_object(
      'payment_scope', correction_items.correction_item_kind,
      'correction_request_id', correction_items.correction_request_id::text
    ),
    91
  FROM public.pay_payment_correction_items AS correction_items
  WHERE correction_items.pay_batch_id = p_pay_batch_id
    AND correction_items.status = 'APPLIED'
    AND correction_items.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
    AND (
      COALESCE(array_length(v_item_ids, 1), 0) = 0
      OR correction_items.pay_batch_item_id = ANY(v_item_ids)
    );

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS pre_bank_correction_reason_items
    WHERE pre_bank_correction_reason_items.pay_batch_id = p_pay_batch_id
      AND pre_bank_correction_reason_items.status = 'APPLIED'
      AND pre_bank_correction_reason_items.correction_item_kind = 'PRE_BANK_CANCEL'
      AND (
        COALESCE(array_length(v_item_ids, 1), 0) = 0
        OR pre_bank_correction_reason_items.pay_batch_item_id = ANY(v_item_ids)
      )
  ) THEN
    v_reasons := array_append(v_reasons, 'Payment was cancelled/recalculated');
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS no_money_correction_reason_items
    WHERE no_money_correction_reason_items.pay_batch_id = p_pay_batch_id
      AND no_money_correction_reason_items.status = 'APPLIED'
      AND no_money_correction_reason_items.correction_item_kind = 'NO_MONEY_UNWIND'
      AND (
        COALESCE(array_length(v_item_ids, 1), 0) = 0
        OR no_money_correction_reason_items.pay_batch_item_id = ANY(v_item_ids)
      )
  ) THEN
    v_reasons := array_append(v_reasons, 'Financials were rewound');
  END IF;

  select count(*)::integer
  into v_stale_count
  from pg_temp.tmp_validate_freshness_chunk_diffs as diff_count_row;

  v_is_stale := coalesce(v_stale_count, 0) > 0;

  if v_key_resolution_failure_count > 0 then
    v_reasons := array_append(v_reasons, 'KEY_RESOLUTION_FAILED');
  end if;
  if greatest(coalesce(v_key_diff_count, 0) - coalesce(v_other_reservation_count, 0), 0) > 0 then
    v_reasons := array_append(v_reasons, 'RESERVATION_CHANGED');
  end if;
  if v_other_reservation_count > 0 then
    v_reasons := array_append(v_reasons, 'OTHER_ACTIVE_RESERVATION');
  end if;
  if v_finance_reservation_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'FINANCE_RESERVATION_CHANGED');
  end if;
  if v_snooze_count > 0 then
    v_reasons := array_append(v_reasons, 'ACTIVE_SNOOZE_CHANGED');
  end if;
  if v_restructure_or_writeoff_count > 0 then
    v_reasons := array_append(v_reasons, 'FINANCE_CASE_RESTRUCTURE_OR_WRITEOFF_CHANGED');
  end if;
  if v_timesheet_override_count > 0 then
    v_reasons := array_append(v_reasons, 'TIMESHEET_PAYMENT_OVERRIDE_CHANGED');
  end if;
  if v_deduction_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'DEDUCTION_RECOVERY_CHANGED');
  end if;
  if v_candidate_scope_deduction_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'CANDIDATE_SCOPE_DEDUCTION_MISMATCH');
  end if;

  select coalesce(jsonb_object_agg(reason_counts.reason, reason_counts.reason_count order by reason_counts.reason), '{}'::jsonb)
  into v_stale_reason_counts
  from (
    select diff_rows.reason, count(*)::integer as reason_count
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    group by diff_rows.reason
  ) as reason_counts;

  if coalesce(v_deduction_diff_count, 0) > 0 then
    v_stale_reason_counts := coalesce(v_stale_reason_counts, '{}'::jsonb)
      || jsonb_build_object('DEDUCTION_RECOVERY_CHANGED', v_deduction_diff_count);
  end if;

  select coalesce(jsonb_agg(
    jsonb_build_object(
      'reason', diff_sample_rows.reason,
      'timesheet_id', case when diff_sample_rows.timesheet_id is null then null else diff_sample_rows.timesheet_id::text end,
      'pay_batch_item_id', case when diff_sample_rows.pay_batch_item_id is null then null else diff_sample_rows.pay_batch_item_id::text end,
      'candidate_id', case when diff_sample_rows.candidate_id is null then null else diff_sample_rows.candidate_id::text end,
      'key_type', diff_sample_rows.key_type,
      'key_value', diff_sample_rows.key_value,
      'expected', diff_sample_rows.expected,
      'actual', diff_sample_rows.actual
    )
    order by diff_sample_rows.ord, diff_sample_rows.reason, diff_sample_rows.timesheet_id nulls last, diff_sample_rows.pay_batch_item_id nulls last
  ), '[]'::jsonb)
  into v_diff_sample
  from (
    select diff_rows.*
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    order by diff_rows.ord, diff_rows.reason, diff_rows.timesheet_id nulls last, diff_rows.pay_batch_item_id nulls last
    limit v_diff_limit
  ) as diff_sample_rows;

  select coalesce(jsonb_agg(
    jsonb_build_object(
      'code', case
        when blocker_rows.reason = 'Payment was cancelled/recalculated' then 'PAYMENT_WAS_CANCELLED_RECALCULATED'
        when blocker_rows.reason = 'Financials were rewound' then 'FINANCIALS_WERE_REWOUND'
        when blocker_rows.reason = 'Source payment scope changed' then 'SOURCE_PAYMENT_SCOPE_CHANGED'
        when blocker_rows.reason = 'Manual adjustment carry-forward changed' then 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CHANGED'
        when blocker_rows.reason = 'Manual adjustment carry-forward was consumed elsewhere' then 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
        else upper(regexp_replace(blocker_rows.reason, '[^A-Za-z0-9]+', '_', 'g'))
      end,
      'reason', blocker_rows.reason,
      'message', blocker_rows.reason,
      'timesheet_id', case when blocker_rows.timesheet_id is null then null else blocker_rows.timesheet_id::text end,
      'pay_batch_item_id', case when blocker_rows.pay_batch_item_id is null then null else blocker_rows.pay_batch_item_id::text end,
      'candidate_id', case when blocker_rows.candidate_id is null then null else blocker_rows.candidate_id::text end,
      'key_type', blocker_rows.key_type,
      'key_value', blocker_rows.key_value,
      'expected', blocker_rows.expected,
      'actual', blocker_rows.actual
    )
    order by blocker_rows.ord, blocker_rows.reason, blocker_rows.timesheet_id nulls last, blocker_rows.pay_batch_item_id nulls last
  ), '[]'::jsonb)
  into v_chunk_blockers
  from (
    select diff_rows.*
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    order by diff_rows.ord, diff_rows.reason, diff_rows.timesheet_id nulls last, diff_rows.pay_batch_item_id nulls last
    limit v_diff_limit
  ) as blocker_rows;

  if array_length(v_reasons, 1) is not null then
    select array_agg(distinct reason_value order by reason_value)
    into v_reasons
    from unnest(v_reasons) as reason_values(reason_value);
  end if;

  v_result := jsonb_build_object(
    'ok', true,
    'is_stale', v_is_stale,
    'checked_units', coalesce(v_checked_units, 0),
    'checked_count', coalesce(v_checked_units, 0),
    'checked_item_count', coalesce(v_checked_item_count, 0),
    'checked_timesheet_count', coalesce(v_checked_timesheet_count, 0),
    'stale_count', coalesce(v_stale_count, 0),
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'stale_reason_counts', coalesce(v_stale_reason_counts, '{}'::jsonb),
    'blockers', coalesce(v_chunk_blockers, '[]'::jsonb),
    'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
    'diff_sample', coalesce(v_diff_sample, '[]'::jsonb),
    'counts', jsonb_build_object(
      'reservation_changed', greatest(coalesce(v_key_diff_count, 0) - coalesce(v_other_reservation_count, 0), 0),
      'economic_key_changed', 0,
      'other_active_reservation', coalesce(v_other_reservation_count, 0),
      'finance_reservation_changed', coalesce(v_finance_reservation_diff_count, 0),
      'active_snooze_changed', coalesce(v_snooze_count, 0),
      'finance_case_restructure_or_writeoff_changed', coalesce(v_restructure_or_writeoff_count, 0),
      'timesheet_payment_override_changed', coalesce(v_timesheet_override_count, 0),
      'deduction_recovery_changed', coalesce(v_deduction_diff_count, 0),
      'candidate_scope_deduction_mismatch', coalesce(v_candidate_scope_deduction_diff_count, 0)
    ),
    'operation_id', p_operation_id::text,
    'chunk_id', p_chunk_id::text,
    'pay_batch_id', p_pay_batch_id::text
  );

  v_chunk_result_hash := md5(v_result::text);
  v_result := v_result || jsonb_build_object('chunk_result_hash', v_chunk_result_hash);

  update public.banking_pay_operation_chunks as chunk_update
  set result_json = v_result,
      error_json = null,
      completed_count = coalesce(v_checked_units, 0),
      failed_count = case when v_is_stale then coalesce(v_stale_count, 0) else 0 end,
      updated_at_utc = now()
  where chunk_update.id = p_chunk_id;

    return jsonb_build_object(
    'ok', true,
    'is_stale', v_is_stale,
    'checked_count', coalesce(v_checked_units, 0),
    'stale_count', coalesce(v_stale_count, 0),
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'stale_reason_counts', coalesce(v_stale_reason_counts, '{}'::jsonb),
    'blockers', coalesce(v_chunk_blockers, '[]'::jsonb),
    'diff_sample', coalesce(v_diff_sample, '[]'::jsonb),
    'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
    'chunk_result_hash', v_chunk_result_hash,
    'result', v_result
  );
end;
$function$;

alter function public.pay_batch_validate_freshness_chunk(uuid, uuid, uuid, uuid, integer) owner to "postgres";
revoke all privileges on function public.pay_batch_validate_freshness_chunk(uuid, uuid, uuid, uuid, integer) from PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin;
grant execute on function public.pay_batch_validate_freshness_chunk(uuid, uuid, uuid, uuid, integer) to "postgres";
grant execute on function public.pay_batch_validate_freshness_chunk(uuid, uuid, uuid, uuid, integer) to service_role;
