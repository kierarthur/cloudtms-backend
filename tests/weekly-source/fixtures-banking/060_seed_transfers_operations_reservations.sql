-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- NAMED SEEDS for transfers, operations, reservations, born-voided items,
-- unbound voids and correction blockers.
--
-- A NOTE THAT MATTERS FOR `R18`.
--   `proof/32 §4.3` C5 and `§12 R18` speak of a `REVERSED` transfer.  The
--   installed constraint `pay_bank_transfers_status_chk_v3` does NOT permit
--   'REVERSED' in `pay_bank_transfers.status`; it permits
--   PENDING, PROCESSING, UNKNOWN, COMPLETED, FAILED, DECLINED, REJECTED,
--   CANCELLED, VOIDED, RETURNED, REVERTED, BLOCKED, SUBMISSION_FAILED,
--   FAILED_BEFORE_COMMIT.  `REVERSED` can only reach the classifier through
--   `rail_state` (unconstrained) or the rail/provider JSON, and the installed
--   classifier reads exactly that: its ambiguity test is
--   `v_ambiguous_return_or_revert := EXISTS (... status_text IN ('RETURNED','REVERTED','REVERSED'))`
--   over the whole term array, which includes `rail_state`.  The `REVERSED`
--   fixture therefore carries `status = 'REVERTED'` with `rail_state = 'REVERSED'`,
--   and the constraint checker proves it is a legal row.  Seeding
--   `status = 'REVERSED'` would be a shape the installed writers can never produce.
--
--   The same applies to `COMMITTED` / `EXECUTED` raw strings in `R18`: they are
--   not legal `status` values and are seeded into `rail_state`.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- Transfers
-- ---------------------------------------------------------------------------
-- p_options:
--   status                 a value permitted by `pay_bank_transfers_status_chk_v3`
--   rail_state             free text; this is where REVERSED / COMMITTED / EXECUTED go
--   rail_meta_json         the JSON the classifier reads as both event payload and
--                          provider meta (`proof/32 §4.3` C5 calls the adapter with
--                          `COALESCE(transfer.rail_meta_json,'{}')` twice)
--   candidate_id           null for a batch-level Umbrella transfer (`R44`)
--   umbrella_id            required when `candidate_id` is null
--                          (`pay_bank_transfers_payee_present_chk`)
--   pay_channel            'PAYE' or 'UMBRELLA'
--   bind_to_item_id        also set `pay_batch_items.pay_bank_transfer_id`
--   with_provider_event    add a MATCHED `pay_bank_transfer_events` row
--   provider_event_state   the event's `normalised_state`
create or replace function ws_banking_fixture.seed_transfer_v1(
  p_state_key text,
  p_transfer_key text,
  p_pay_batch_id uuid,
  p_options jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
as $$
declare
  v_transfer uuid := ws_banking_fixture.fid(p_state_key || ':transfer:' || p_transfer_key);
  v_event uuid := ws_banking_fixture.fid(p_state_key || ':transfer_event:' || p_transfer_key);
  v_status text := coalesce(p_options->>'status', 'PENDING');
  v_rail_state text := p_options->>'rail_state';
  v_candidate uuid := nullif(p_options->>'candidate_id', '')::uuid;
  v_umbrella uuid := nullif(p_options->>'umbrella_id', '')::uuid;
  v_channel text := coalesce(p_options->>'pay_channel', 'PAYE');
  v_amount numeric := coalesce((p_options->>'amount')::numeric, 100.00);
  v_bind_item uuid := nullif(p_options->>'bind_to_item_id', '')::uuid;
  v_classification record;
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.pay_bank_transfers(
    id, pay_batch_id, candidate_id, umbrella_id, pay_channel, amount, status,
    rail_provider, rail_env, rail_state, rail_meta_json,
    payee_entity_kind, payee_entity_id, request_id, transfer_group_key,
    completed_at_utc
  ) values (
    v_transfer, p_pay_batch_id, v_candidate, v_umbrella, v_channel, v_amount, v_status,
    'CSV', 'SANDBOX', v_rail_state,
    coalesce(p_options->'rail_meta_json', '{}'::jsonb),
    case when v_candidate is null then 'UMBRELLA' else 'CANDIDATE' end,
    coalesce(v_candidate, v_umbrella),
    'ws-fixture-request:' || p_state_key || ':' || p_transfer_key,
    'ws-fixture-group:' || p_state_key || ':' || p_transfer_key,
    case when upper(v_status) = 'COMPLETED' then now() else null end);

  if v_bind_item is not null then
    update public.pay_batch_items set pay_bank_transfer_id = v_transfer where id = v_bind_item;
  end if;

  if coalesce((p_options->>'with_provider_event')::boolean, false) then
    insert into public.pay_bank_transfer_events(
      id, pay_batch_id, pay_bank_transfer_id, candidate_id, umbrella_id,
      provider_key, provider_event_id, provider_reference, provider_state,
      normalised_state, event_source, event_time_utc, amount, mapping_status,
      mapping_method, idempotency_key, raw_payload, provider_event_transport
    ) values (
      v_event, p_pay_batch_id, v_transfer, v_candidate, v_umbrella,
      'REVOLUT',
      'ws-fixture-provider-event:' || p_state_key || ':' || p_transfer_key,
      'ws-fixture-provider-ref:' || p_state_key || ':' || p_transfer_key,
      lower(coalesce(p_options->>'provider_event_state', 'completed')),
      upper(coalesce(p_options->>'provider_event_state', 'COMPLETED')),
      'PROVIDER_WEBHOOK', now(), v_amount, 'MATCHED', 'TRANSFER_ID',
      'ws-fixture-idem:' || p_state_key || ':' || p_transfer_key,
      jsonb_build_object('status', upper(coalesce(p_options->>'provider_event_state', 'COMPLETED'))),
      'PROVIDER_WEBHOOK');
  end if;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':transfer:' || p_transfer_key, p_state_key,
    'public.pay_bank_transfers, public.pay_bank_transfer_events',
    'public.pay_execute_bank_transfer_chunk_prepare / public.pay_settle_rail / public.pay_bank_event_ingest (call-only; never driven here)',
    'census 01 §3 rows `public.pay_execute_bank_transfer_chunk_prepare` ("pay_batch_items write sets '
    || 'pay_bank_transfer_id, updated_at") and `public.pay_bank_event_ingest`; legal values from '
    || '`pay_bank_transfers_status_chk_v3`, `pay_bank_transfers_payee_present_chk`, '
    || '`pay_bank_transfers_payee_entity_kind_chk`; classification by the exact adapter '
    || '`public._pay_rail_state_money_movement_classify` that `proof/32 §4.3` C5 names',
    format('status=%s rail_state=%s candidate_id=%s umbrella_id=%s', v_status, v_rail_state, v_candidate, v_umbrella));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_bank_transfers'::regclass, format('fixture_row.id = %L', v_transfer),
    p_state_key || ':pay_bank_transfers');

  if coalesce((p_options->>'with_provider_event')::boolean, false) then
    perform ws_banking_fixture.assert_rows_satisfy_constraints(
      'public.pay_bank_transfer_events'::regclass, format('fixture_row.id = %L', v_event),
      p_state_key || ':pay_bank_transfer_events');
  end if;

  -- Record what the INSTALLED classifier makes of the seeded row, so a fixture
  -- can never silently drift from the state it claims to represent.
  select * into v_classification
  from public._pay_rail_state_money_movement_classify(
    v_status, v_rail_state,
    coalesce(p_options->'rail_meta_json', '{}'::jsonb),
    coalesce(p_options->'rail_meta_json', '{}'::jsonb));

  return jsonb_build_object(
    'pay_bank_transfer_id', v_transfer,
    'status', v_status,
    'rail_state', v_rail_state,
    'bound_pay_batch_item_id', v_bind_item,
    'classifier', jsonb_build_object(
      'cash_state', v_classification.cash_state,
      'is_final_money_moved', v_classification.is_final_money_moved,
      'is_terminal_no_money', v_classification.is_terminal_no_money,
      'is_pending_non_final', v_classification.is_pending_non_final,
      'reason', v_classification.reason)
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- Banking Pay operations
-- ---------------------------------------------------------------------------
-- p_options:
--   operation_type         default 'PAYMENT_EXECUTE'
--   status                 default 'COMPLETE'
--   phase                  default 'COMPLETE'
--   scope_freeze_status    'NONE' | 'SEEDING' | 'FROZEN'   (`R17` keeps FROZEN on a
--                          COMPLETE operation, which the finish owner never resets)
--   lease_expires_in       interval text, or null for a null lease
--   lock_expires_in        interval text, or null; a FUTURE value on a terminal
--                          operation is the legacy lock form of `R36`
create or replace function ws_banking_fixture.seed_operation_v1(
  p_state_key text,
  p_operation_key text,
  p_pay_batch_id uuid,
  p_options jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
as $$
declare
  v_operation uuid := ws_banking_fixture.fid(p_state_key || ':operation:' || p_operation_key);
  v_type text := coalesce(p_options->>'operation_type', 'PAYMENT_EXECUTE');
  v_status text := coalesce(p_options->>'status', 'COMPLETE');
  v_phase text := coalesce(p_options->>'phase', 'COMPLETE');
  v_freeze text := coalesce(p_options->>'scope_freeze_status', 'NONE');
  v_lease interval := nullif(p_options->>'lease_expires_in', '')::interval;
  v_lock interval := nullif(p_options->>'lock_expires_in', '')::interval;
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.banking_pay_operations(
    id, operation_type, status, phase, actor_user_id, workbench_session_id, pay_batch_id,
    idempotency_key, input_json, scope_freeze_status, scope_frozen_at_utc,
    lease_owner, lease_expires_at_utc, locked_by, lock_expires_at_utc,
    completed_at_utc, failed_at_utc
  ) values (
    v_operation, v_type, v_status, v_phase,
    ws_banking_fixture.fid('user:actor'), ws_banking_fixture.fid('workbench_session:main'),
    p_pay_batch_id,
    'ws-banking-fixture:' || p_state_key || ':' || p_operation_key,
    coalesce(p_options->'input_json', '{}'::jsonb),
    v_freeze,
    case when v_freeze = 'FROZEN' then now() - interval '1 hour' else null end,
    case when v_lease is null then null else 'ws-banking-fixture-worker' end,
    case when v_lease is null then null else now() + v_lease end,
    case when v_lock is null then null else 'ws-banking-fixture-worker' end,
    case when v_lock is null then null else now() + v_lock end,
    case when upper(v_status) = 'COMPLETE' then now() - interval '30 minutes' else null end,
    case when upper(v_status) = 'FAILED' then now() - interval '30 minutes' else null end);

  perform ws_banking_fixture.register_seed(
    p_state_key || ':operation:' || p_operation_key, p_state_key,
    'public.banking_pay_operations',
    'public.banking_pay_operation_finish / public.banking_pay_draft_operation_finish_v8 (call-only; never driven here)',
    '`proof/32 §4.3` C4: "a COMPLETE, FAILED or CANCELLED operation is history even when '
    || 'scope_freeze_status = ''FROZEN'' remains: 09082026_1128_banking_pay_operation_finish_post_draft_authority.sql:420-422 '
    || 'nulls lease_expires_at_utc and lock_expires_at_utc and never resets scope_freeze_status"; the schema keeps '
    || 'both lease forms (`supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql:1061-1071`); '
    || 'legal values from `banking_pay_operations_status_chk`, `banking_pay_operations_operation_type_chk`, '
    || '`banking_pay_operations_scope_freeze_status_chk`',
    format('status=%s phase=%s scope_freeze_status=%s lease=%s lock=%s', v_status, v_phase, v_freeze, v_lease, v_lock));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.banking_pay_operations'::regclass, format('fixture_row.id = %L', v_operation),
    p_state_key || ':banking_pay_operations');

  return jsonb_build_object(
    'banking_pay_operation_id', v_operation,
    'operation_type', v_type, 'status', v_status, 'phase', v_phase,
    'scope_freeze_status', v_freeze,
    'lease_expires_in', v_lease, 'lock_expires_in', v_lock);
end;
$$;

-- ---------------------------------------------------------------------------
-- Reservations
-- ---------------------------------------------------------------------------
-- The four states `pay_advance_reservations_status_chk` permits.  `proof/32 §4.3`
-- C2 freezes on RESERVED or COMMITTED; `§5.1`/`§5.2` require SETTLED or RELEASED.
create or replace function ws_banking_fixture.seed_reservation_v1(
  p_state_key text,
  p_reservation_key text,
  p_pay_batch_id uuid,
  p_pay_batch_candidate_id uuid,
  p_pay_batch_item_id uuid,
  p_status text,
  p_reserved_amount numeric default 25.00,
  p_bind_item boolean default true
)
returns jsonb
language plpgsql
as $$
declare
  v_reservation uuid := ws_banking_fixture.fid(p_state_key || ':reservation:' || p_reservation_key);
  v_case uuid := ws_banking_fixture.fid(p_state_key || ':reservation_case:' || p_reservation_key);
  v_candidate_id uuid;
begin
  perform ws_banking_fixture.assert_local_only();

  if p_status not in ('RESERVED', 'COMMITTED', 'SETTLED', 'RELEASED') then
    raise exception 'WS_BANKING_FIXTURE_RESERVATION_STATUS_INVALID'
      using errcode = 'P0001', detail = jsonb_build_object('status', p_status)::text;
  end if;

  select candidate_row.candidate_id into v_candidate_id
  from public.pay_batch_candidates as candidate_row
  where candidate_row.id = p_pay_batch_candidate_id;

  insert into public.pay_advances(
    id, candidate_id, reason, original_amount, outstanding_amount, case_type, advance_kind
  ) values (
    v_case, v_candidate_id, 'MANUAL_ADVANCE'::pay_advance_reason_enum,
    p_reserved_amount, p_reserved_amount,
    'PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'LEGACY_ADVANCE'::pay_advance_kind_enum)
  on conflict (id) do nothing;

  insert into public.pay_advance_reservations(
    id, finance_case_id, pay_batch_id, pay_batch_candidate_id, pay_batch_item_id,
    reserved_amount, status, committed_at_utc, settled_at_utc, released_at_utc, released_reason
  ) values (
    v_reservation, v_case, p_pay_batch_id, p_pay_batch_candidate_id, p_pay_batch_item_id,
    p_reserved_amount, p_status,
    case when p_status in ('COMMITTED', 'SETTLED') then now() - interval '2 hours' else null end,
    case when p_status = 'SETTLED' then now() - interval '1 hour' else null end,
    case when p_status = 'RELEASED' then now() - interval '1 hour' else null end,
    case when p_status = 'RELEASED' then 'PRE_BANK_CANCEL' else null end);

  if coalesce(p_bind_item, true) and p_pay_batch_item_id is not null then
    update public.pay_batch_items set reservation_id = v_reservation where id = p_pay_batch_item_id;
  end if;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':reservation:' || p_reservation_key, p_state_key,
    'public.pay_advance_reservations',
    'public.pay_batch_finalize_reservations_and_markers (RESERVED), public.pay_batch_schedule (RESERVED->COMMITTED), '
    || 'public.pay_settle_rail (COMMITTED->SETTLED), public.pay_pre_bank_cancel_apply_work_item (->RELEASED)',
    'census 01 §2.1 #9 ("its two reservation writes are RESERVED -> COMMITTED and COMMITTED -> SETTLED, both bound by '
    || 'pay_batch_id AND pay_batch_item_id") and #1 ("pay_advance_reservations ... SET status=''RELEASED'', '
    || 'released_reason=''PRE_BANK_CANCEL''"); census 01 §3 row `public.pay_batch_schedule` '
    || '("reservations RESERVED -> ''COMMITTED''"); legal values from `pay_advance_reservations_status_chk`',
    format('status=%s', p_status));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_advance_reservations'::regclass, format('fixture_row.id = %L', v_reservation),
    p_state_key || ':pay_advance_reservations');

  return jsonb_build_object(
    'pay_advance_reservation_id', v_reservation,
    'finance_case_id', v_case,
    'status', p_status);
end;
$$;

-- ---------------------------------------------------------------------------
-- An item inserted already voided  (census 01 §4.3 unbound writer 1)
-- ---------------------------------------------------------------------------
-- `public.pay_batch_apply_finance_adjustments` has eight `INSERT INTO
-- public.pay_batch_items`; one of them — the dormant recovery template fed from
-- `tmp_pay_build_dormant_recovery_template_stage` — supplies `true as is_voided`,
-- creating a row that never existed in a payable state.  `proof/32 §4.2` class 2
-- cannot bind it, so a terminal batch makes it CENSUS_ERROR and a live batch
-- makes it ACTIVE.  It is an open ruling (contract section 18, OR-3), so the
-- fixture exists to prove the direction of failure, not to assert a verdict.
create or replace function ws_banking_fixture.seed_born_voided_item_v1(
  p_state_key text,
  p_pay_batch_candidate_id uuid,
  p_timesheet_id uuid default null,
  p_item_type text default 'OVERPAYMENT_RECOVERY',
  p_amount numeric default 15.00
)
returns jsonb
language plpgsql
as $$
declare
  v_item uuid := ws_banking_fixture.fid(p_state_key || ':born_voided_item');
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.pay_batch_items(
    id, pay_batch_candidate_id, item_type, timesheet_id, pay_channel,
    amount_ex_vat, amount_vat, amount_inc_vat, is_voided
  ) values (
    v_item, p_pay_batch_candidate_id, p_item_type, p_timesheet_id, 'PAYE',
    p_amount, 0, p_amount, true);

  perform ws_banking_fixture.register_seed(
    p_state_key || ':born_voided_item', p_state_key,
    'public.pay_batch_items',
    'public.pay_batch_apply_finance_adjustments',
    'census 01 §2.1 #10: "of its 8 INSERT INTO public.pay_batch_items, 3 omit is_voided, 4 supply '
    || '`false as is_voided`, and one — the dormant recovery-template insert fed from '
    || '`tmp_pay_build_dormant_recovery_template_stage` — supplies `true as is_voided`, creating a row '
    || 'that is born voided"; census 01 §4.3 records it as an UNBOUND writer whose failure direction is closed',
    'Open ruling OR-3 (contract section 18): recorded, not adjudicated.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_items'::regclass, format('fixture_row.id = %L', v_item),
    p_state_key || ':born_voided_item');

  return jsonb_build_object('pay_batch_item_id', v_item, 'item_type', p_item_type, 'is_voided', true);
end;
$$;

-- ---------------------------------------------------------------------------
-- A synthetic unbound void  (`R31`)
-- ---------------------------------------------------------------------------
-- `R31`: "A voided item in a terminal batch fits no binding (synthetic writer)".
-- The item is voided with NO correction request (so not Binding A), in a batch
-- that is SETTLED rather than CANCELLED (so not Binding B), and its reservation is
-- still `RESERVED` and its bound transfer classifies FINAL_PAID, so the `§5.1`
-- common conditions fail and Binding C cannot prove it either.
create or replace function ws_banking_fixture.seed_unbound_void_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_pay_batch_candidate_id uuid,
  p_timesheet_id uuid,
  p_amount numeric default 40.00
)
returns jsonb
language plpgsql
as $$
declare
  v_item uuid := ws_banking_fixture.fid(p_state_key || ':unbound_void_item');
  v_reservation jsonb;
  v_transfer jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.pay_batch_items(
    id, pay_batch_candidate_id, item_type, timesheet_id, pay_channel,
    amount_ex_vat, amount_vat, amount_inc_vat, is_voided
  ) values (
    v_item, p_pay_batch_candidate_id, 'TIMESHEET_PAYMENT', p_timesheet_id, 'PAYE',
    p_amount, 0, p_amount, true);

  v_reservation := ws_banking_fixture.seed_reservation_v1(
    p_state_key, 'unbound_void', p_pay_batch_id, p_pay_batch_candidate_id, v_item,
    'RESERVED', p_amount, true);

  v_transfer := ws_banking_fixture.seed_transfer_v1(
    p_state_key, 'unbound_void', p_pay_batch_id,
    jsonb_build_object(
      'status', 'COMPLETED',
      'rail_state', 'COMPLETED',
      'candidate_id', (select candidate_row.candidate_id from public.pay_batch_candidates as candidate_row
                       where candidate_row.id = p_pay_batch_candidate_id),
      'amount', p_amount,
      'bind_to_item_id', v_item,
      'with_provider_event', true,
      'provider_event_state', 'COMPLETED'));

  perform ws_banking_fixture.register_seed(
    p_state_key || ':unbound_void', p_state_key,
    'public.pay_batch_items',
    'SYNTHETIC — no installed writer produces this shape',
    '`proof/32 §12 R31`: "A voided item in a terminal batch fits no binding (synthetic writer)". '
    || 'Deliberately violates the `§5.1` common conditions: the reservation is still RESERVED and the bound '
    || 'transfer classifies `is_final_money_moved = true`, so no binding can prove the void and `§4.2` class 1 '
    || 'must classify it CENSUS_ERROR rather than a silent FROZEN',
    'The only intentionally non-installed shape in this library; it exists to prove the refusal.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_items'::regclass, format('fixture_row.id = %L', v_item),
    p_state_key || ':unbound_void_item');

  return jsonb_build_object(
    'pay_batch_item_id', v_item,
    'reservation', v_reservation,
    'transfer', v_transfer);
end;
$$;

-- ---------------------------------------------------------------------------
-- APPLIED_WITH_BLOCKERS  (`R19`, `R38`)
-- ---------------------------------------------------------------------------
-- Applied to a correction request the REAL owner chain has already produced, so
-- the request, its membership, its work items and the voided items are genuine;
-- only the top-level status and the recorded blockers are seeded.
--
-- p_blocker_mode:
--   'OUTSIDE_FAMILY'   every blocker names an item id outside the member family
--                      -> `proof/32 §5.1` Binding A permits the void          (`R19` released half)
--   'INSIDE_FAMILY'    a blocker names a family item                          (`R19` frozen half)
--   'NO_ITEM_IDENTITY' a blocker with no exact item identity at all           (`R38`)
create or replace function ws_banking_fixture.seed_applied_with_blockers_v1(
  p_state_key text,
  p_correction_request_id uuid,
  p_blocker_mode text,
  p_blocked_item_id uuid default null
)
returns jsonb
language plpgsql
as $$
declare
  v_blocker jsonb;
  v_work_item_ids uuid[];
begin
  perform ws_banking_fixture.assert_local_only();

  if p_blocker_mode not in ('OUTSIDE_FAMILY', 'INSIDE_FAMILY', 'NO_ITEM_IDENTITY') then
    raise exception 'WS_BANKING_FIXTURE_BLOCKER_MODE_INVALID'
      using errcode = 'P0001', detail = jsonb_build_object('blocker_mode', p_blocker_mode)::text;
  end if;

  if p_blocker_mode = 'NO_ITEM_IDENTITY' then
    v_blocker := jsonb_build_object(
      'code', 'PAYMENT_CORRECTION_BLOCKER',
      'message', 'A blocker was recorded without an exact payment item identity.');
  else
    if p_blocked_item_id is null then
      raise exception 'WS_BANKING_FIXTURE_BLOCKER_ITEM_REQUIRED'
        using errcode = 'P0001', detail = jsonb_build_object('blocker_mode', p_blocker_mode)::text;
    end if;
    v_blocker := jsonb_build_object(
      'code', 'PAYMENT_CORRECTION_BLOCKER',
      'message', 'A blocker was recorded against an exact payment item.',
      'pay_batch_item_id', p_blocked_item_id,
      'pay_batch_item_ids', jsonb_build_array(p_blocked_item_id),
      'scope', case when p_blocker_mode = 'INSIDE_FAMILY' then 'MEMBER_FAMILY' else 'OUTSIDE_MEMBER_FAMILY' end);
  end if;

  update public.pay_payment_correction_requests
  set status = 'APPLIED_WITH_BLOCKERS',
      plan_json = plan_json || jsonb_build_object('blockers', jsonb_build_array(v_blocker))
  where id = p_correction_request_id;

  update public.pay_payment_correction_work_items
  set result_json = result_json || jsonb_build_object('blockers', jsonb_build_array(v_blocker))
  where correction_request_id = p_correction_request_id;

  select coalesce(array_agg(work_row.id order by work_row.id), array[]::uuid[])
  into v_work_item_ids
  from public.pay_payment_correction_work_items as work_row
  where work_row.correction_request_id = p_correction_request_id;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':applied_with_blockers', p_state_key,
    'public.pay_payment_correction_requests, public.pay_payment_correction_work_items',
    'public.pay_payment_correction_process_chunk (the installed owner that promotes a request to APPLIED_WITH_BLOCKERS)',
    '`pay_payment_correction_requests_status_chk` permits APPLIED_WITH_BLOCKERS; `proof/32 §5.1` Binding A '
    || 'second bullet: "when r.status = ''APPLIED_WITH_BLOCKERS'', every blocker recorded in result_json/work-item '
    || 'rows is bound to an item id OUTSIDE F(root); any blocker naming a family item keeps the root FROZEN, and so '
    || 'does a blocker that carries no exact item identity"',
    format('blocker_mode=%s', p_blocker_mode));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_payment_correction_requests'::regclass,
    format('fixture_row.id = %L', p_correction_request_id),
    p_state_key || ':pay_payment_correction_requests');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_payment_correction_work_items'::regclass,
    format('fixture_row.correction_request_id = %L', p_correction_request_id),
    p_state_key || ':pay_payment_correction_work_items');

  return jsonb_build_object(
    'correction_request_id', p_correction_request_id,
    'blocker_mode', p_blocker_mode,
    'blocker', v_blocker,
    'work_item_ids', to_jsonb(v_work_item_ids));
end;
$$;
