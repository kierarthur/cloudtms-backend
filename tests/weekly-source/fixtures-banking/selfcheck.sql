-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- Self-check: asserts that every built state really has the shape it claims.
--
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f install.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f build-all.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f selfcheck.sql
--
-- Every assertion reads the database, never the builder's own return value.

\set ON_ERROR_STOP on
set statement_timeout = '300s';

do $selfcheck$
declare
  v_state jsonb;
  v_count bigint;
  v_text text;
  v_bool boolean;
  v_batch record;
  v_checks integer := 0;
  v_failing_candidates integer := 0;
begin
  ---------------------------------------------------------------------------
  -- 0. isolation: nothing was installed into public or private
  ---------------------------------------------------------------------------
  select count(*) into v_count
  from pg_proc as routine_row
  join pg_namespace as schema_row on schema_row.oid = routine_row.pronamespace
  where schema_row.nspname in ('public', 'private')
    and routine_row.proname like 'ws_banking_fixture%';
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: fixture objects leaked into public/private (%).', v_count;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 1. every state was registered
  ---------------------------------------------------------------------------
  select count(*) into v_count from ws_banking_fixture.state_register;
  if v_count < 29 then
    raise exception 'SELFCHECK_FAILED: expected at least 29 registered states, found %.', v_count;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 2. every seed carries an installed-writer citation
  ---------------------------------------------------------------------------
  select count(*) into v_count
  from ws_banking_fixture.seed_register
  where nullif(btrim(coalesce(installed_writer, '')), '') is null
     or nullif(btrim(coalesce(cited_lines, '')), '') is null;
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: % seeds have no installed-writer citation.', v_count;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 3. the constraint checker really fails closed
  ---------------------------------------------------------------------------
  begin
    perform ws_banking_fixture.assert_rows_satisfy_constraints(
      'public.pay_batches'::regclass, 'fixture_row.id = ''00000000-0000-0000-0000-000000000000''::uuid',
      'selfcheck-negative');
    raise exception 'SELFCHECK_FAILED: the constraint checker accepted a predicate matching no rows.';
  exception
    when sqlstate 'P0001' then
      if sqlerrm not like 'WS_BANKING_FIXTURE_CONSTRAINT_CHECK_MATCHED_NO_ROWS%' then
        raise;
      end if;
  end;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 4. Binding A, whole batch (R1)
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_cancelled_whole_binding_a';

  select batch_row.status, batch_row.cancelled_at_utc, batch_row.cancel_reason
  into v_batch from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;

  if v_batch.status <> 'CANCELLED' or v_batch.cancelled_at_utc is null then
    raise exception 'SELFCHECK_FAILED: batch_cancelled_whole_binding_a is % / cancelled_at_utc %.',
      v_batch.status, v_batch.cancelled_at_utc;
  end if;

  select count(*) into v_count
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and item_row.is_voided = false;
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: batch_cancelled_whole_binding_a still has % non-voided items.', v_count;
  end if;

  select request_row.status into v_text
  from public.pay_payment_correction_requests as request_row
  where request_row.id = (v_state->>'correction_request_id')::uuid;
  if v_text <> 'APPLIED' then
    raise exception 'SELFCHECK_FAILED: batch_cancelled_whole_binding_a correction request is %.', v_text;
  end if;

  select count(*) into v_count
  from public.pay_payment_correction_work_items as work_row
  where work_row.correction_request_id = (v_state->>'correction_request_id')::uuid
    and work_row.status = 'APPLIED'
    and work_row.result_json->>'result_code' = 'APPLIED';
  if v_count < 1 then
    raise exception 'SELFCHECK_FAILED: batch_cancelled_whole_binding_a has no APPLIED work item.';
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 5. Binding A, one Candidate out of many; DRAFT remainder (R41, R3)
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'candidate_cancelled_out_of_multi_draft';

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'DRAFT' then
    raise exception 'SELFCHECK_FAILED: the multi-Candidate remainder batch is % (expected DRAFT).', v_text;
  end if;

  select count(*) filter (where item_row.is_voided),
         count(*) filter (where not item_row.is_voided)
  into v_count, v_checks
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: expected exactly one voided item in the multi-Candidate Draft, found %.', v_count;
  end if;
  v_checks := 6;

  ---------------------------------------------------------------------------
  -- 6. APPLIED_WITH_BLOCKERS (R19, R38)
  ---------------------------------------------------------------------------
  for v_text in select unnest(array['outside_family', 'inside_family', 'no_item_identity'])
  loop
    select result_json into v_state from ws_banking_fixture.state_register
    where state_key = 'applied_with_blockers_' || v_text;

    select count(*) into v_count
    from public.pay_payment_correction_requests as request_row
    where request_row.id = (v_state->>'correction_request_id')::uuid
      and request_row.status = 'APPLIED_WITH_BLOCKERS';
    if v_count <> 1 then
      raise exception 'SELFCHECK_FAILED: applied_with_blockers_% is not APPLIED_WITH_BLOCKERS.', v_text;
    end if;

    select count(*) into v_count
    from public.pay_payment_correction_work_items as work_row
    where work_row.correction_request_id = (v_state->>'correction_request_id')::uuid
      and jsonb_array_length(coalesce(work_row.result_json->'blockers', '[]'::jsonb)) > 0;
    if v_count < 1 then
      raise exception 'SELFCHECK_FAILED: applied_with_blockers_% recorded no blocker.', v_text;
    end if;

    if v_text = 'no_item_identity' then
      select count(*) into v_count
      from public.pay_payment_correction_work_items as work_row,
           lateral jsonb_array_elements(coalesce(work_row.result_json->'blockers', '[]'::jsonb)) as blocker(value)
      where work_row.correction_request_id = (v_state->>'correction_request_id')::uuid
        and blocker.value ? 'pay_batch_item_id';
      if v_count <> 0 then
        raise exception 'SELFCHECK_FAILED: the NO_ITEM_IDENTITY blocker carries an item identity.';
      end if;
    end if;
  end loop;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 7. Binding B (R29)
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_aborted_failed_draft_create';

  select batch_row.status, batch_row.cancelled_at_utc into v_batch
  from public.pay_batches as batch_row where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_batch.status <> 'CANCELLED' or v_batch.cancelled_at_utc is null then
    raise exception 'SELFCHECK_FAILED: Binding B batch is % / cancelled_at_utc %.',
      v_batch.status, v_batch.cancelled_at_utc;
  end if;

  select count(*) into v_count
  from public.pay_payment_correction_requests as request_row
  where request_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: Binding B must have NO correction request; found %.', v_count;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 8. Binding C (R30)
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'paye_net_manual_void';

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'DRAFT' then
    raise exception 'SELFCHECK_FAILED: the Binding C batch is % (expected a live DRAFT).', v_text;
  end if;

  select item_row.is_voided into v_bool from public.pay_batch_items as item_row
  where item_row.id = (v_state->>'voided_pay_batch_item_id')::uuid;
  if v_bool is not true then
    raise exception 'SELFCHECK_FAILED: the Binding C deduction item is not voided.';
  end if;

  select reservation_row.status || ':' || coalesce(reservation_row.released_reason, '')
  into v_text
  from public.pay_advance_reservations as reservation_row
  where reservation_row.id = (v_state->>'released_reservation_id')::uuid;
  if v_text <> 'RELEASED:PAYE_NET_REPROJECTION' then
    raise exception 'SELFCHECK_FAILED: the Binding C reservation is %.', v_text;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 9. settled batch (R2) — §5.2 shape
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_settled';

  select batch_row.status || ':' || batch_row.execution_commit_state
         || ':' || (batch_row.completed_at_utc is not null)::text
         || ':' || (batch_row.execution_committed_at_utc is not null)::text
  into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'SETTLED:COMMITTED:true:true' then
    raise exception 'SELFCHECK_FAILED: batch_settled shape is %.', v_text;
  end if;

  select count(*) into v_count
  from public.pay_batch_candidates as candidate_row
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and candidate_row.settlement_status = 'SETTLED'
    and candidate_row.settled_at_utc is not null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: batch_settled has % settled Candidate rows (expected 1).', v_count;
  end if;

  -- exactly one history row per (timesheet, batch), and its signature equals the
  -- signature of the snapshot the settle rail's selector would choose
  select count(*) into v_count
  from public.timesheet_pay_state_history as history_row
  where history_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: batch_settled has % history rows (expected 1).', v_count;
  end if;

  select count(*) into v_count
  from public.timesheet_pay_state_history as history_row
  join lateral (
    select distinct on (snapshot_row.timesheet_id) snapshot_row.signature
    from public.pay_batch_timesheet_snapshots as snapshot_row
    where snapshot_row.pay_batch_id = history_row.pay_batch_id
      and snapshot_row.timesheet_id = history_row.timesheet_id
    order by snapshot_row.timesheet_id, snapshot_row.created_at_utc desc, snapshot_row.id
  ) as chosen on true
  where history_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and history_row.signature = chosen.signature
    and history_row.signature <> '';
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: batch_settled history signature does not match the chosen snapshot.';
  end if;

  -- the bound transfer classifies FINAL_PAID under the INSTALLED adapter
  select count(*) into v_count
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  join public.pay_bank_transfers as transfer_row on transfer_row.id = item_row.pay_bank_transfer_id
  cross join lateral public._pay_rail_state_money_movement_classify(
    transfer_row.status, transfer_row.rail_state,
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb),
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb)) as movement
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and movement.is_final_money_moved;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: batch_settled has % final-money-moved transfers (expected 1).', v_count;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 10. R16 — SETTLED but still SCHEDULED
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settled_batch_retains_schedule_kind';
  select batch_row.status || ':' || coalesce(batch_row.schedule_kind, 'NULL')
         || ':' || (batch_row.scheduled_at_utc is not null)::text
  into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'SETTLED:SCHEDULED:true' then
    raise exception 'SELFCHECK_FAILED: R16 shape is %.', v_text;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 11. R43 — FAILED with completed_at_utc and mixed per-Candidate outcomes
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_failed_completed_mixed';
  select batch_row.status || ':' || (batch_row.completed_at_utc is not null)::text
         || ':' || batch_row.execution_commit_state
  into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'FAILED:true:COMMITTED' then
    raise exception 'SELFCHECK_FAILED: R43 batch shape is %.', v_text;
  end if;

  select string_agg(distinct coalesce(candidate_row.settlement_status, 'NULL'), ',' order by coalesce(candidate_row.settlement_status, 'NULL'))
  into v_text
  from public.pay_batch_candidates as candidate_row
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'FAILED,SETTLED' then
    raise exception 'SELFCHECK_FAILED: R43 Candidate outcomes are % (expected FAILED,SETTLED).', v_text;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 12. R4 — partial settlement
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'partial_settlement';
  select string_agg(distinct coalesce(candidate_row.settlement_status, 'NULL'), ',' order by coalesce(candidate_row.settlement_status, 'NULL'))
  into v_text
  from public.pay_batch_candidates as candidate_row
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'PENDING,SETTLED' then
    raise exception 'SELFCHECK_FAILED: R4 Candidate outcomes are % (expected PENDING,SETTLED).', v_text;
  end if;
  v_checks := v_checks + 1;

  ---------------------------------------------------------------------------
  -- 13. R20 / R34 — duplicate history rows
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_history_duplicate_different_signature';
  select count(*), count(distinct history_row.signature)
  into v_count, v_checks
  from public.timesheet_pay_state_history as history_row
  where history_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 2 or v_checks <> 2 then
    raise exception 'SELFCHECK_FAILED: R20 expected 2 rows with 2 signatures, found % / %.', v_count, v_checks;
  end if;

  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_history_duplicate_identical';
  select count(*), count(distinct history_row.signature)
  into v_count, v_checks
  from public.timesheet_pay_state_history as history_row
  where history_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 2 or v_checks <> 1 then
    raise exception 'SELFCHECK_FAILED: R34 expected 2 rows with 1 signature, found % / %.', v_count, v_checks;
  end if;
  v_checks := 13;

  ---------------------------------------------------------------------------
  -- 14. R35 — snapshot conflicts
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_snapshot_missing';
  select count(*) into v_count from public.pay_batch_timesheet_snapshots as snapshot_row
  where snapshot_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: the MISSING snapshot state has % snapshots.', v_count;
  end if;

  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_snapshot_empty_signature';
  select count(*) into v_count from public.pay_batch_timesheet_snapshots as snapshot_row
  where snapshot_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and snapshot_row.signature = '';
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the EMPTY_SIGNATURE snapshot state has % empty signatures.', v_count;
  end if;

  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_snapshot_conflicting_second';
  select count(*), count(distinct snapshot_row.target_snapshot_json)
  into v_count, v_checks
  from public.pay_batch_timesheet_snapshots as snapshot_row
  where snapshot_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 2 or v_checks <> 2 then
    raise exception 'SELFCHECK_FAILED: the CONFLICTING_SECOND state has % rows / % distinct targets.', v_count, v_checks;
  end if;

  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'settlement_snapshot_signature_mismatch';
  select count(*) into v_count
  from public.timesheet_pay_state_history as history_row
  join public.pay_batch_timesheet_snapshots as snapshot_row
    on snapshot_row.pay_batch_id = history_row.pay_batch_id
   and snapshot_row.timesheet_id = history_row.timesheet_id
  where history_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and history_row.signature = snapshot_row.signature;
  if v_count <> 0 then
    raise exception 'SELFCHECK_FAILED: the SIGNATURE_MISMATCH state has a matching signature.';
  end if;
  v_checks := 14;

  ---------------------------------------------------------------------------
  -- 15. R28 — one root settled in two batches
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'root_settled_in_two_batches';
  select count(*), count(distinct history_row.pay_batch_id)
  into v_count, v_checks
  from public.timesheet_pay_state_history as history_row
  where history_row.timesheet_id = (v_state->>'timesheet_id')::uuid;
  if v_count <> 2 or v_checks <> 2 then
    raise exception 'SELFCHECK_FAILED: R28 expected 2 history rows in 2 batches, found % / %.', v_count, v_checks;
  end if;

  select count(*) into v_count
  from public.timesheet_pay_state as cache_row
  where cache_row.timesheet_id = (v_state->>'timesheet_id')::uuid
    and cache_row.last_settled_pay_batch_id is not null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R28 last-settled cache row missing.';
  end if;
  v_checks := 15;

  ---------------------------------------------------------------------------
  -- 16. R5 / R18 — transfer classification under the installed adapter
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'transfers_unknown_and_pending';
  if (v_state->'unknown_transfer'->'classifier'->>'cash_state') <> 'UNKNOWN'
     or (v_state->'pending_transfer'->'classifier'->>'cash_state') <> 'PENDING_NON_FINAL' then
    raise exception 'SELFCHECK_FAILED: R5 classifications are % / %.',
      v_state->'unknown_transfer'->'classifier'->>'cash_state',
      v_state->'pending_transfer'->'classifier'->>'cash_state';
  end if;

  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'transfers_returned_and_reversed';
  if (v_state->'returned_transfer'->'classifier'->>'cash_state') <> 'UNKNOWN'
     or (v_state->'reversed_transfer'->'classifier'->>'cash_state') <> 'UNKNOWN'
     or (v_state->'raw_committed_transfer'->'classifier'->>'cash_state') <> 'UNKNOWN'
     or (v_state->'raw_executed_transfer'->'classifier'->>'cash_state') <> 'UNKNOWN' then
    raise exception 'SELFCHECK_FAILED: R18 expected every transfer to classify UNKNOWN.';
  end if;
  v_checks := 16;

  ---------------------------------------------------------------------------
  -- 17. R17 / R36 — operation history forms
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'operation_history_forms';

  select count(*) into v_count from public.banking_pay_operations as operation_row
  where operation_row.id = (v_state->'complete_frozen_operation'->>'banking_pay_operation_id')::uuid
    and operation_row.status = 'COMPLETE'
    and operation_row.scope_freeze_status = 'FROZEN'
    and operation_row.lease_expires_at_utc is null
    and operation_row.lock_expires_at_utc is null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R17 COMPLETE+FROZEN operation shape wrong.';
  end if;

  select count(*) into v_count from public.banking_pay_operations as operation_row
  where operation_row.id = (v_state->'terminal_legacy_lock_operation'->>'banking_pay_operation_id')::uuid
    and operation_row.status = 'COMPLETE'
    and operation_row.lease_expires_at_utc is null
    and operation_row.lock_expires_at_utc > clock_timestamp();
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R36 terminal-with-future-legacy-lock shape wrong.';
  end if;
  v_checks := 17;

  ---------------------------------------------------------------------------
  -- 18. R44 — batch-level Umbrella transfers
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'umbrella_batch_level_transfers';

  select count(*) into v_count
  from public.pay_bank_transfers as transfer_row
  where transfer_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and transfer_row.candidate_id is null
    and transfer_row.umbrella_id is not null;
  if v_count <> 3 then
    raise exception 'SELFCHECK_FAILED: R44 expected 3 batch-level Umbrella transfers, found %.', v_count;
  end if;

  -- the frozen item keeps Umbrella one while the Candidate now points at two
  select count(*) into v_count
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  join public.candidates as candidate_record on candidate_record.id = candidate_row.candidate_id
  where item_row.id = (v_state->>'frozen_umbrella_item_id')::uuid
    and item_row.umbrella_id is distinct from candidate_record.umbrella_id;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R44 frozen item Umbrella still equals the Candidate''s current Umbrella.';
  end if;

  -- and the second item has no frozen Umbrella at all
  select count(*) into v_count
  from public.pay_batch_items as item_row
  where item_row.id = (v_state->>'missing_frozen_umbrella_item_id')::uuid
    and item_row.umbrella_id is null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R44 missing-frozen-evidence item carries an Umbrella.';
  end if;
  v_checks := 18;

  ---------------------------------------------------------------------------
  -- 19. reservations, all four states
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'reservation_lifecycle';
  select count(distinct reservation_row.status) into v_count
  from public.pay_advance_reservations as reservation_row
  where reservation_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid;
  if v_count <> 4 then
    raise exception 'SELFCHECK_FAILED: reservation_lifecycle has % distinct statuses (expected 4).', v_count;
  end if;
  v_checks := 19;

  ---------------------------------------------------------------------------
  -- 20. R31 — the synthetic unbound void
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'synthetic_unbound_void';

  select count(*) into v_count
  from public.pay_batch_items as item_row
  join public.pay_advance_reservations as reservation_row on reservation_row.pay_batch_item_id = item_row.id
  join public.pay_bank_transfers as transfer_row on transfer_row.id = item_row.pay_bank_transfer_id
  cross join lateral public._pay_rail_state_money_movement_classify(
    transfer_row.status, transfer_row.rail_state,
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb),
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb)) as movement
  where item_row.id = (v_state->'unbound_void'->>'pay_batch_item_id')::uuid
    and item_row.is_voided
    and reservation_row.status = 'RESERVED'
    and movement.is_final_money_moved;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: R31 unbound void does not violate the §5.1 common conditions as intended.';
  end if;

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'SETTLED' then
    raise exception 'SELFCHECK_FAILED: R31 batch is % (expected a terminal SETTLED batch).', v_text;
  end if;
  v_checks := 20;

  ---------------------------------------------------------------------------
  -- 21. census unbound writer 1 — an item inserted already voided
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'born_voided_item';
  select count(*) into v_count
  from public.pay_batch_items as item_row
  where item_row.id = (v_state->'born_voided_item'->>'pay_batch_item_id')::uuid
    and item_row.is_voided
    and item_row.reservation_id is null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the born-voided item is not voided-with-no-reservation.';
  end if;
  v_checks := 21;

  ---------------------------------------------------------------------------
  -- 22. the rotated family, with a whitespace-padded booking id, resolves
  ---------------------------------------------------------------------------
  select count(*) into v_count
  from public._pay_timesheet_rotation_scope(array[ws_banking_fixture.fid('timesheet:a:v2')])
  where booking_id = '  ws-fixture-booking-a  ';
  if v_count <> 2 then
    raise exception 'SELFCHECK_FAILED: the rotated family resolved to % rows (expected 2).', v_count;
  end if;

  select count(*) into v_count
  from public._pay_timesheet_rotation_scope(array[ws_banking_fixture.fid('timesheet:a:v2')])
  where requested_is_canonical and family_is_current and family_version = 2;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the rotated family has no single canonical current member.';
  end if;
  v_checks := 22;

  ---------------------------------------------------------------------------
  -- 23. R1 terminal — the operation Binding A's third bullet requires
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_cancelled_whole_binding_a_complete';

  select count(*) into v_count
  from public.banking_pay_operations as operation_row
  where operation_row.id = (v_state->>'correction_operation_id')::uuid
    and operation_row.status = 'COMPLETE'
    and operation_row.phase = 'COMPLETE'
    and operation_row.completed_at_utc is not null
    and operation_row.lease_expires_at_utc is null
    and operation_row.lock_expires_at_utc is null;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the terminal Binding A operation is not COMPLETE/COMPLETE.';
  end if;

  -- and the installed REFRESH_WORKBENCH chunk really ran, rather than being skipped
  if coalesce((v_state->'terminal'->'refresh_workbench_attempt'
                ->>'installed_refresh_workbench_chunk_ran')::boolean, false) is not true then
    raise exception 'SELFCHECK_FAILED: the installed REFRESH_WORKBENCH chunk did not run; detail %.',
      v_state->'terminal'->'refresh_workbench_attempt';
  end if;

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'CANCELLED' then
    raise exception 'SELFCHECK_FAILED: the terminal Binding A batch is %.', v_text;
  end if;
  v_checks := 23;

  ---------------------------------------------------------------------------
  -- 24. the mid-flight state is still a distinct, testable state
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'batch_cancelled_whole_binding_a';
  select count(*) into v_count
  from public.banking_pay_operations as operation_row
  where operation_row.id = (v_state->>'correction_operation_id')::uuid
    and operation_row.status = 'RUNNING'
    and operation_row.phase = 'REFRESH_WORKBENCH';
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the mid-flight Binding A state is no longer mid-flight.';
  end if;
  v_checks := 24;

  ---------------------------------------------------------------------------
  -- 25. R3 / R19 second case — same family, same Candidate, DRAFT remainder
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'same_family_draft_remainder';

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'DRAFT' then
    raise exception 'SELFCHECK_FAILED: the same-family remainder batch is % (expected DRAFT).', v_text;
  end if;

  select count(*) filter (where item_row.is_voided),
         count(*) filter (where not item_row.is_voided),
         count(distinct candidate_row.candidate_id)
  into v_count, v_checks, v_failing_candidates
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  where candidate_row.pay_batch_id = (v_state->>'pay_batch_id')::uuid
    and item_row.timesheet_id = (v_state->>'family_timesheet_id')::uuid;
  if v_count <> 1 or v_checks <> 1 or v_failing_candidates <> 1 then
    raise exception 'SELFCHECK_FAILED: the same-family remainder has % voided / % active items across % Candidates (expected 1 / 1 / 1).',
      v_count, v_checks, v_failing_candidates;
  end if;
  v_checks := 25;

  ---------------------------------------------------------------------------
  -- 26. R41 second half — the remainder settled and paid the other Candidate
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'multi_draft_remainder_settled';

  select batch_row.status into v_text from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;
  if v_text <> 'SETTLED' then
    raise exception 'SELFCHECK_FAILED: the R41 remainder batch is % (expected SETTLED).', v_text;
  end if;

  -- the cancelled Candidate is not settled and its item is voided
  select count(*) into v_count
  from public.pay_batch_candidates as candidate_row
  join public.pay_batch_items as item_row on item_row.pay_batch_candidate_id = candidate_row.id
  where candidate_row.id = (v_state->>'cancelled_pay_batch_candidate_id')::uuid
    and candidate_row.settlement_status is null
    and item_row.is_voided;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the R41 cancelled Candidate is not left unsettled-with-a-voided-item.';
  end if;

  -- the surviving Candidate is settled and has its settlement history
  select count(*) into v_count
  from public.pay_batch_candidates as candidate_row
  join public.pay_batch_items as item_row on item_row.pay_batch_candidate_id = candidate_row.id
  join public.timesheet_pay_state_history as history_row
    on history_row.timesheet_id = item_row.timesheet_id
   and history_row.pay_batch_id = candidate_row.pay_batch_id
  where candidate_row.id = (v_state->>'surviving_pay_batch_candidate_id')::uuid
    and candidate_row.settlement_status = 'SETTLED'
    and candidate_row.settled_at_utc is not null
    and not item_row.is_voided;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the R41 surviving Candidate has no settled proof.';
  end if;
  v_checks := 26;

  ---------------------------------------------------------------------------
  -- 27. Binding C: the installed writer's null `timesheet_id` (WP-08a §4.2)
  ---------------------------------------------------------------------------
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'paye_net_manual_void';

  select count(*) into v_count
  from public.pay_batch_items as item_row
  where item_row.id = (v_state->>'voided_pay_batch_item_id')::uuid
    and item_row.is_voided
    and item_row.timesheet_id is null
    and item_row.item_type = 'LOAN_REPAYMENT';
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the Binding C voided item is not a LOAN_REPAYMENT with a null timesheet_id.';
  end if;

  -- the fixture must keep saying so out loud, not only in a comment
  if coalesce((v_state->>'installed_writer_supplies_null_timesheet_id')::boolean, false) is not true
     or (v_state->>'voided_item_timesheet_id') is not null then
    raise exception 'SELFCHECK_FAILED: the Binding C state no longer records the null timesheet_id fact.';
  end if;
  v_checks := 27;

  ---------------------------------------------------------------------------
  -- 28. what the installed pre-bank cancellation leaves a BOUND transfer as
  ---------------------------------------------------------------------------
  -- Observed, not assumed: after the real chain voids the item, the bound
  -- transfer is untouched and classifies PENDING_NON_FINAL, so `proof/32 §5.1`'s
  -- common condition ("every transfer bound to i classifies
  -- is_terminal_no_money = true, or no transfer is bound") is NOT satisfied.
  select result_json into v_state from ws_banking_fixture.state_register
  where state_key = 'pre_bank_cancel_voided_transfer';

  select count(*) into v_count
  from public.pay_batch_items as item_row
  where item_row.id = (v_state->>'bound_pay_batch_item_id')::uuid
    and item_row.is_voided
    and item_row.pay_bank_transfer_id = (v_state->>'pay_bank_transfer_id')::uuid;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the pre-bank-cancel state has no voided item still bound to its transfer.';
  end if;

  -- the recorded verdict must match what the installed classifier says NOW
  select count(*) into v_count
  from public.pay_bank_transfers as transfer_row
  cross join lateral public._pay_rail_state_money_movement_classify(
    transfer_row.status, transfer_row.rail_state,
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb),
    coalesce(transfer_row.rail_meta_json, '{}'::jsonb)) as movement
  where transfer_row.id = (v_state->>'pay_bank_transfer_id')::uuid
    and movement.cash_state = (v_state->'installed_classifier_verdict'->>'cash_state')
    and movement.is_terminal_no_money is not true;
  if v_count <> 1 then
    raise exception 'SELFCHECK_FAILED: the recorded classifier verdict no longer matches the installed classifier.';
  end if;
  v_checks := 28;

  raise notice 'SELFCHECK PASSED: % assertion groups.', v_checks;
end;
$selfcheck$;

select 'selfcheck' as result,
       (select count(*) from ws_banking_fixture.state_register) as states,
       (select count(*) from ws_banking_fixture.seed_register) as seeds,
       (select count(*) from ws_banking_fixture.state_register where build_method = 'REAL_OWNER') as real_owner_states,
       (select count(*) from ws_banking_fixture.state_register where build_method = 'NAMED_SEED') as named_seed_states,
       (select count(*) from ws_banking_fixture.state_register where build_method = 'MIXED') as mixed_states;
