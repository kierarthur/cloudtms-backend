-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- ONE FUNCTION PER NAMED EVIDENCE STATE.
--
-- Every function returns a jsonb identity map and records itself in
-- `ws_banking_fixture.state_register` with its build method
-- (REAL_OWNER / NAMED_SEED / MIXED) and the proofs it serves.
--
-- Some states are built by the real pre-bank cancellation chain, which must
-- finish its PROCESS_CHUNKS / FINALISE phases in a LATER TRANSACTION (see
-- `030_real_owner_cancellation.sql`).  Those states return
-- `"requires_finalise": true`; the caller then calls
-- `ws_banking_fixture.finalise_pending_cancellations_v1()` once per transaction
-- until it reports `pending = 0`.  `selfcheck.sql` and `load-banking-fixtures.mjs`
-- both do that.

\set ON_ERROR_STOP on

create table if not exists ws_banking_fixture.pending_cancellation (
  state_key text primary key,
  correction_request_id uuid not null,
  correction_operation_id uuid not null
);

create or replace function ws_banking_fixture.finalise_pending_cancellations_v1()
returns jsonb
language plpgsql
as $$
declare
  v_row record;
  v_result jsonb;
  v_results jsonb := '[]'::jsonb;
  v_pending integer := 0;
begin
  perform ws_banking_fixture.assert_local_only();

  for v_row in
    select * from ws_banking_fixture.pending_cancellation order by state_key
  loop
    v_result := ws_banking_fixture.finalise_pre_bank_cancellation_v1(
      v_row.correction_request_id, v_row.correction_operation_id);

    v_results := v_results || jsonb_build_array(
      jsonb_build_object('state_key', v_row.state_key) || v_result);

    if coalesce((v_result->>'has_more')::boolean, false) then
      v_pending := v_pending + 1;
    else
      delete from ws_banking_fixture.pending_cancellation where state_key = v_row.state_key;
    end if;
  end loop;

  return jsonb_build_object('pending', v_pending, 'results', v_results);
end;
$$;

-- ===========================================================================
-- Cancellation states (real installed owners)
-- ===========================================================================

-- R1 — whole batch cancelled through the installed pre-bank cancellation chain.
create or replace function ws_banking_fixture.state_batch_cancelled_whole_binding_a_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_cancelled_whole_binding_a';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_c_id',
      'timesheet_id', v_base->>'timesheet_c_v1',
      'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'REAL_OWNER',
    'R1; proof/32 §5.1 Binding A; proof/32 §4.2 class 2',
    v_draft || v_cancel || jsonb_build_object('requires_finalise', true));
end;
$$;

-- R41, R3, UNA-019 — one Candidate cancelled out of a multi-Candidate Draft, the
-- remainder left alive.  `proof/32 §4.2` class 2: "Binding A proves the item on
-- its own evidence whatever the status of the remainder batch"; `§4.3` C6: the
-- `DRAFT` remainder keeps the OTHER root frozen.
create or replace function ws_banking_fixture.state_candidate_cancelled_out_of_multi_draft_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'candidate_cancelled_out_of_multi_draft';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00,
        'pay_channel', 'PAYE')),
    'MIXED');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'REAL_OWNER',
    'R41; R3; UNA-019; proof/32 §4.3 C6; proof/36 §4 W3/W4',
    v_draft || v_cancel || jsonb_build_object('requires_finalise', true));
end;
$$;

-- R19 / R38 — the same real chain, then the request promoted to
-- APPLIED_WITH_BLOCKERS with a blocker outside the family, inside the family, or
-- with no exact item identity.
create or replace function ws_banking_fixture.state_applied_with_blockers_v1(p_blocker_mode text)
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'applied_with_blockers_' || lower(p_blocker_mode);
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00,
        'pay_channel', 'PAYE')),
    'MIXED');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  -- The blocker is applied only AFTER the chain has finished, by
  -- `finish_applied_with_blockers_v1`.  Promoting the request to
  -- APPLIED_WITH_BLOCKERS early would take it out of the installed batch-gate
  -- status list ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED',
  -- 'PROCESSING' in `private.pay_payment_mutation_guard_v1`) and the next
  -- installed phase would refuse PAYMENT_CORRECTION_GATE_OWNER_MISMATCH.
  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    case p_blocker_mode
      when 'OUTSIDE_FAMILY' then 'R19 (released half); proof/32 §5.1 Binding A'
      when 'INSIDE_FAMILY' then 'R19 (frozen half); proof/32 §5.3'
      else 'R38; proof/32 §5.3'
    end,
    v_draft || v_cancel || jsonb_build_object(
      'requires_finalise', true,
      'blocker_mode', p_blocker_mode,
      'requires_blocker_finish', true));
end;
$$;

-- Applied after the chain has finished; see the note in
-- `state_applied_with_blockers_v1`.
create or replace function ws_banking_fixture.finish_applied_with_blockers_v1(p_blocker_mode text)
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'applied_with_blockers_' || lower(p_blocker_mode);
  v_state jsonb;
  v_blocked_item uuid;
  v_blockers jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  select state_row.result_json into v_state
  from ws_banking_fixture.state_register as state_row
  where state_row.state_key = v_key;

  if v_state is null then
    raise exception 'WS_BANKING_FIXTURE_STATE_NOT_BUILT'
      using errcode = 'P0001', detail = jsonb_build_object('state_key', v_key)::text;
  end if;

  -- index 0 is the member-family (Candidate A) item; index 1 belongs to
  -- Candidate B and is therefore outside the member family.
  v_blocked_item := case
    when p_blocker_mode = 'INSIDE_FAMILY' then ((v_state->'pay_batch_item_ids')->>0)::uuid
    when p_blocker_mode = 'OUTSIDE_FAMILY' then ((v_state->'pay_batch_item_ids')->>1)::uuid
    else null
  end;

  v_blockers := ws_banking_fixture.seed_applied_with_blockers_v1(
    v_key, (v_state->>'correction_request_id')::uuid, p_blocker_mode, v_blocked_item);

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    case p_blocker_mode
      when 'OUTSIDE_FAMILY' then 'R19 (released half); proof/32 §5.1 Binding A'
      when 'INSIDE_FAMILY' then 'R19 (frozen half); proof/32 §5.3'
      else 'R38; proof/32 §5.3'
    end,
    v_state || jsonb_build_object('blockers', v_blockers, 'requires_blocker_finish', false));
end;
$$;

-- R29 — Binding B, produced by `public.pay_batch_abort_failed_draft_create_partial`.
create or replace function ws_banking_fixture.state_batch_aborted_failed_draft_create_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_aborted_failed_draft_create';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_abort jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_b_id',
      'timesheet_id', v_base->>'timesheet_b_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_abort := ws_banking_fixture.drive_draft_create_abort_v1(v_key, (v_draft->>'pay_batch_id')::uuid);

  return ws_banking_fixture.register_state(v_key, 'REAL_OWNER',
    'R29; proof/32 §5.1 Binding B; proof/32 §5.4 row `pay_batch_abort_failed_draft_create_partial`',
    v_draft || v_abort);
end;
$$;

-- R30 — Binding C, produced by `public.pay_set_paye_net_manual` inside a live Draft.
create or replace function ws_banking_fixture.state_paye_net_manual_void_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'paye_net_manual_void';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_void jsonb;
begin
  -- Candidate D / family E are reserved for this state: `pay_set_paye_net_manual`
  -- runs `pay_batch_validate_freshness`, which refuses BATCH_STALE with
  -- PAYE_DRAFT_ALREADY_EXISTS when the Candidate has another live PAYE Draft.
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_d_id',
      'timesheet_id', v_base->>'timesheet_e_v1',
      'amount', 50.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_void := ws_banking_fixture.drive_paye_net_manual_void_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    ((v_draft->'pay_batch_candidate_ids')->>0)::uuid,
    35.00, 15.00);

  return ws_banking_fixture.register_state(v_key, 'REAL_OWNER',
    'R30; proof/32 §5.1 Binding C; proof/32 §5.4 row `pay_set_paye_net_manual`',
    v_draft || v_void);
end;
$$;

-- ===========================================================================
-- Settlement states (named seeds)
-- ===========================================================================

-- Shared helper: settle every non-voided item of a batch.
create or replace function ws_banking_fixture.apply_settlement_evidence_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_options jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
as $$
declare
  v_member record;
  v_snapshot jsonb;
  v_history jsonb;
  v_transfer jsonb;
  v_reservation jsonb;
  v_members jsonb := '[]'::jsonb;
  v_terminality jsonb;
  v_snapshot_mode text := coalesce(p_options->>'snapshot_mode', 'PRESENT');
  v_history_mode text := coalesce(p_options->>'history_mode', 'SINGLE');
  v_reservation_status text := coalesce(p_options->>'reservation_status', 'SETTLED');
  -- The position this settlement RESTATES. Banking Pay does not accumulate:
  -- each settlement snapshot restates the complete position for the root and
  -- shift and the money moved is the residual (WP-11a review finding F1).
  v_position text := upper(coalesce(p_options->>'position', 'BASE'));
  v_write_cache boolean := coalesce((p_options->>'write_last_settled_cache')::boolean, true);
begin
  perform ws_banking_fixture.assert_local_only();

  for v_member in
    select item_row.id as item_id, item_row.timesheet_id, item_row.pay_channel,
           candidate_row.id as pay_batch_candidate_id, candidate_row.candidate_id,
           candidate_row.settlement_status
    from public.pay_batch_items as item_row
    join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
    where candidate_row.pay_batch_id = p_pay_batch_id
      and item_row.is_voided = false
      and item_row.timesheet_id is not null
    order by item_row.id
  loop
    v_snapshot := ws_banking_fixture.seed_timesheet_snapshot_v1(
      p_state_key, p_pay_batch_id, v_member.timesheet_id, v_member.candidate_id,
      v_member.pay_channel, v_snapshot_mode, v_position);

    v_transfer := ws_banking_fixture.seed_transfer_v1(
      p_state_key, 'settled:' || v_member.item_id::text, p_pay_batch_id,
      jsonb_build_object(
        'status', coalesce(p_options->>'transfer_status', 'COMPLETED'),
        'rail_state', coalesce(p_options->>'transfer_rail_state', 'COMPLETED'),
        'candidate_id', v_member.candidate_id,
        'pay_channel', v_member.pay_channel,
        'bind_to_item_id', v_member.item_id,
        'with_provider_event', true,
        'provider_event_state', coalesce(p_options->>'provider_event_state', 'COMPLETED')));

    if v_reservation_status <> 'NONE' then
      v_reservation := ws_banking_fixture.seed_reservation_v1(
        p_state_key, 'settled:' || v_member.item_id::text, p_pay_batch_id,
        v_member.pay_batch_candidate_id, null, v_reservation_status, 25.00, false);
    else
      v_reservation := null;
    end if;

    -- A Candidate whose outcome is not SETTLED gets no settlement history: the
    -- history row is written only by the settle rail's settled path.
    if coalesce(p_options->'candidate_outcomes'->>v_member.pay_batch_candidate_id::text, 'SETTLED') = 'SETTLED'
       and v_snapshot_mode <> 'MISSING' then
      v_history := ws_banking_fixture.seed_settlement_history_v1(
        p_state_key, p_pay_batch_id, v_member.timesheet_id,
        v_snapshot->>'signature', v_history_mode, v_position);

      if v_write_cache then
        perform ws_banking_fixture.seed_last_settled_cache_v1(
          p_state_key, v_member.timesheet_id, p_pay_batch_id, v_snapshot->>'signature', v_position);
      end if;
    else
      v_history := jsonb_build_object('mode', 'NONE', 'history_ids', '[]'::jsonb);
    end if;

    v_members := v_members || jsonb_build_array(jsonb_build_object(
      'pay_batch_item_id', v_member.item_id,
      'timesheet_id', v_member.timesheet_id,
      'pay_batch_candidate_id', v_member.pay_batch_candidate_id,
      'snapshot', v_snapshot,
      'history', v_history,
      'transfer', v_transfer,
      'reservation', v_reservation));
  end loop;

  v_terminality := ws_banking_fixture.seed_batch_terminality_v1(p_state_key, p_pay_batch_id, p_options);

  return jsonb_build_object('members', v_members, 'terminality', v_terminality,
    'position', v_position);
end;
$$;

-- R2 — a plainly settled batch.
create or replace function ws_banking_fixture.state_batch_settled_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_settled';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_a_id',
      'timesheet_id', v_base->>'timesheet_a_v2',
      'amount', 100.00, 'pay_channel', 'UMBRELLA',
      'umbrella_id', v_base->>'umbrella_one_id')),
    'UMBRELLA');

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(v_key, (v_draft->>'pay_batch_id')::uuid);

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R2; proof/32 §5.2; proof/32 §4.2 class 3', v_draft || v_settled);
end;
$$;

-- R16 — a SETTLED batch that still carries `schedule_kind = 'SCHEDULED'`.
create or replace function ws_banking_fixture.state_settled_batch_retains_schedule_kind_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'settled_batch_retains_schedule_kind';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_b_id',
      'timesheet_id', v_base->>'timesheet_b_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('retain_schedule_kind', true));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R16; proof/32 §4.1 and §4.3 C3 (round-1 C3 defect)', v_draft || v_settled);
end;
$$;

-- R43 — a multi-Candidate batch completed with failed payments.
create or replace function ws_banking_fixture.state_batch_failed_completed_mixed_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_failed_completed_mixed';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
  v_settled_candidate uuid;
  v_failed_candidate uuid;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00,
        'pay_channel', 'PAYE')),
    'MIXED');

  v_settled_candidate := ((v_draft->'pay_batch_candidate_ids')->>0)::uuid;
  v_failed_candidate := ((v_draft->'pay_batch_candidate_ids')->>1)::uuid;

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'batch_status', 'FAILED',
      'candidate_outcomes', jsonb_build_object(
        v_settled_candidate::text, 'SETTLED',
        v_failed_candidate::text, 'FAILED')));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R43; proof/32 §4.1 (FAILED with completed_at_utc is terminal); §4.2 classes 3 and 4; §5.3',
    v_draft || v_settled || jsonb_build_object(
      'settled_pay_batch_candidate_id', v_settled_candidate,
      'failed_pay_batch_candidate_id', v_failed_candidate));
end;
$$;

-- R4 — partial settlement: one member settled, one still active.
create or replace function ws_banking_fixture.state_partial_settlement_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'partial_settlement';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
  v_settled_candidate uuid;
  v_pending_candidate uuid;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_c_id',
        'timesheet_id', v_base->>'timesheet_c_v1', 'amount', 70.00,
        'pay_channel', 'PAYE')),
    'MIXED');

  v_settled_candidate := ((v_draft->'pay_batch_candidate_ids')->>0)::uuid;
  v_pending_candidate := ((v_draft->'pay_batch_candidate_ids')->>1)::uuid;

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'batch_status', 'SETTLED',
      'candidate_outcomes', jsonb_build_object(
        v_settled_candidate::text, 'SETTLED',
        v_pending_candidate::text, 'PENDING')));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R4; proof/32 §4.3 C1 (non-voided item whose own candidate row is not SETTLED); §5.3',
    v_draft || v_settled || jsonb_build_object(
      'settled_pay_batch_candidate_id', v_settled_candidate,
      'pending_pay_batch_candidate_id', v_pending_candidate));
end;
$$;

-- R20, R34 — two settlement-history rows for the same pair.
create or replace function ws_banking_fixture.state_settlement_history_conflict_v1(p_history_mode text)
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'settlement_history_' || lower(p_history_mode);
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_c_id',
      'timesheet_id', v_base->>'timesheet_c_v1',
      'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('history_mode', p_history_mode));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    case p_history_mode
      when 'DUPLICATE_DIFFERENT_SIGNATURE' then 'R20; proof/32 §5.2 SETTLEMENT_HISTORY_CONFLICT'
      else 'R34; proof/32 §5.2 SETTLEMENT_HISTORY_CONFLICT (identical duplicate)'
    end,
    v_draft || v_settled);
end;
$$;

-- R35 — snapshot missing, empty signature, conflicting second snapshot,
-- history signature not equal to the chosen snapshot's.
create or replace function ws_banking_fixture.state_settlement_snapshot_conflict_v1(p_mode text)
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'settlement_snapshot_' || lower(p_mode);
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_settled jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_c_id',
      'timesheet_id', v_base->>'timesheet_c_v1',
      'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    case
      when p_mode = 'SIGNATURE_MISMATCH'
        then jsonb_build_object('snapshot_mode', 'PRESENT', 'history_mode', 'SIGNATURE_MISMATCH')
      else jsonb_build_object('snapshot_mode', p_mode)
    end);

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R35; proof/32 §5.2 SETTLEMENT_SNAPSHOT_CONFLICT', v_draft || v_settled);
end;
$$;

-- R28 — one root settled in two different batches.
create or replace function ws_banking_fixture.state_root_settled_in_two_batches_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'root_settled_in_two_batches';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft_one jsonb;
  v_draft_two jsonb;
  v_settled_one jsonb;
  v_settled_two jsonb;
begin
  v_draft_one := ws_banking_fixture.seed_draft_batch_v1(v_key || ':first',
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_a_id',
      'timesheet_id', v_base->>'timesheet_d_v1',
      'amount', 60.00, 'pay_channel', 'UMBRELLA',
      'umbrella_id', v_base->>'umbrella_one_id')),
    'UMBRELLA');

  v_draft_two := ws_banking_fixture.seed_draft_batch_v1(v_key || ':second',
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_a_id',
      'timesheet_id', v_base->>'timesheet_d_v1',
      'amount', 12.00, 'pay_channel', 'UMBRELLA',
      'umbrella_id', v_base->>'umbrella_one_id')),
    'UMBRELLA');

  -- The first batch writes the cache, then the second overwrites it.  That is
  -- exactly why `proof/32 §5.2` forbids using the cache as authority.
  --
  -- THE SECOND SETTLEMENT RESTATES, IT DOES NOT ADD.  The installed chain pays
  -- the residual against the previous position, so the second snapshot carries
  -- the whole week again with one hour more on the Monday shift: 15.00 then
  -- 16.00, never 31.00 (WP-11a review finding F1).  The shift keeps its
  -- `segment_id`, so the two rows are visibly one shift restated.
  v_settled_one := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key || ':first', (v_draft_one->>'pay_batch_id')::uuid,
    jsonb_build_object('position', 'BASE'));
  v_settled_two := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key || ':second', (v_draft_two->>'pay_batch_id')::uuid,
    jsonb_build_object('position', 'UPWARD'));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R28; proof/32 §5.2 (the last-settled cache is never authority); WP-11a review F1 '
    || '(upward adjustment: the second settlement RESTATES 16.00, it does not add to 31.00)',
    jsonb_build_object(
      'first_batch', v_draft_one || v_settled_one,
      'second_batch', v_draft_two || v_settled_two,
      'timesheet_id', v_base->>'timesheet_d_v1',
      'model_case', 'UPWARD_ADJUSTMENT',
      'position_sequence', jsonb_build_array('BASE', 'UPWARD'),
      'restated_total_hours', jsonb_build_array(15.00, 16.00)));
end;
$$;

-- WP-11a review F1, second model case: a DOWNWARD recovery on the demoted
-- version of a ROTATED family.  Family A is the only rotated family in the
-- library (`timesheet_a_v1` superseded, `timesheet_a_v2` current) and the
-- settlement here is attached to version 1, which is the shape WP-11a asked
-- for in its N1 ("the rotated family A with its settlement on version 1") so
-- `UI-012` can be proved on a fixture instead of on a self-contained seed.
--
-- The second settlement RESTATES the week at 14.00 after a recovery takes an
-- hour off the Monday shift.  It does NOT subtract from a running total, and a
-- reader that adds the two rows together would report 29.00, which is the
-- defect F1 names.
--
-- Family A already carries three settlements on version 2 (`batch_settled`,
-- `partial_settlement`, `batch_failed_completed_mixed`).  That is deliberate
-- and is stated in the README: family A is the library's multi-settlement,
-- multi-version family, so a reader that cannot prove the restatement reading
-- has a real fixture to fail closed on, with a reason, which is a CORRECT
-- outcome rather than a missing figure.
create or replace function ws_banking_fixture.state_rotated_root_restated_downward_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'rotated_root_restated_downward';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft_one jsonb;
  v_draft_two jsonb;
  v_settled_one jsonb;
  v_settled_two jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  v_draft_one := ws_banking_fixture.seed_draft_batch_v1(v_key || ':first',
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_a_id',
      'timesheet_id', v_base->>'timesheet_a_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');
  v_draft_two := ws_banking_fixture.seed_draft_batch_v1(v_key || ':second',
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_a_id',
      'timesheet_id', v_base->>'timesheet_a_v1',
      -- `pay_batch_candidates_net_bank_nonneg_chk` refuses a negative bank
      -- amount, and the money column is not what the hours reader uses: the
      -- recovery is expressed by the RESTATED position, not by a negative row.
      'amount', 10.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_settled_one := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key || ':first', (v_draft_one->>'pay_batch_id')::uuid,
    jsonb_build_object('position', 'BASE'));
  v_settled_two := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key || ':second', (v_draft_two->>'pay_batch_id')::uuid,
    jsonb_build_object('position', 'DOWNWARD'));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'WP-11a review F1 (downward recovery: the second settlement RESTATES 14.00); '
    || 'WP-11a N1 (rotated family A, settlement on the demoted version 1); UI-012',
    jsonb_build_object(
      'first_batch', v_draft_one || v_settled_one,
      'second_batch', v_draft_two || v_settled_two,
      'timesheet_id', v_base->>'timesheet_a_v1',
      'canonical_timesheet_id', v_base->>'timesheet_a_v2',
      'model_case', 'DOWNWARD_RECOVERY_ON_DEMOTED_VERSION',
      'position_sequence', jsonb_build_array('BASE', 'DOWNWARD'),
      'restated_total_hours', jsonb_build_array(15.00, 14.00)));
end;
$$;
-- ===========================================================================
-- Transfer, operation, reservation and void states
-- ===========================================================================

-- R5 — provider-unknown and PENDING_NON_FINAL transfers on a live batch.
create or replace function ws_banking_fixture.state_transfers_unknown_and_pending_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'transfers_unknown_and_pending';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_unknown jsonb;
  v_pending jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00, 'pay_channel', 'PAYE'),
      jsonb_build_object('candidate_id', v_base->>'candidate_c_id',
        'timesheet_id', v_base->>'timesheet_c_v1', 'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_unknown := ws_banking_fixture.seed_transfer_v1(v_key, 'provider_unknown',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'status', 'UNKNOWN', 'rail_state', 'UNKNOWN',
      'candidate_id', v_base->>'candidate_b_id',
      'amount', 80.00,
      'bind_to_item_id', ((v_draft->'pay_batch_item_ids')->>0)::uuid));

  v_pending := ws_banking_fixture.seed_transfer_v1(v_key, 'pending_non_final',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'status', 'PROCESSING', 'rail_state', 'IN_FLIGHT',
      'candidate_id', v_base->>'candidate_c_id',
      'amount', 70.00,
      'bind_to_item_id', ((v_draft->'pay_batch_item_ids')->>1)::uuid));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R5; proof/32 §4.3 C5; §5.3 (provider-unknown, scheduled, executing -> FROZEN indefinitely)',
    v_draft || jsonb_build_object('unknown_transfer', v_unknown, 'pending_transfer', v_pending));
end;
$$;

-- R18 — RETURNED and REVERSED transfers with no provider evidence, plus the bare
-- COMMITTED / EXECUTED raw strings.
create or replace function ws_banking_fixture.state_transfers_returned_and_reversed_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'transfers_returned_and_reversed';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_returned jsonb;
  v_reversed jsonb;
  v_committed jsonb;
  v_executed jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_b_id',
      'timesheet_id', v_base->>'timesheet_b_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_returned := ws_banking_fixture.seed_transfer_v1(v_key, 'returned',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'RETURNED', 'rail_state', 'RETURNED',
      'candidate_id', v_base->>'candidate_b_id', 'amount', 80.00,
      'bind_to_item_id', ((v_draft->'pay_batch_item_ids')->>0)::uuid));

  -- `pay_bank_transfers_status_chk_v3` has no 'REVERSED'; the word reaches the
  -- installed classifier through `rail_state`.  See the header of
  -- `060_seed_transfers_operations_reservations.sql`.
  v_reversed := ws_banking_fixture.seed_transfer_v1(v_key, 'reversed',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'REVERTED', 'rail_state', 'REVERSED',
      'candidate_id', v_base->>'candidate_b_id', 'amount', 80.00));

  v_committed := ws_banking_fixture.seed_transfer_v1(v_key, 'raw_committed',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'UNKNOWN', 'rail_state', 'COMMITTED',
      'candidate_id', v_base->>'candidate_b_id', 'amount', 80.00));

  v_executed := ws_banking_fixture.seed_transfer_v1(v_key, 'raw_executed',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'UNKNOWN', 'rail_state', 'EXECUTED',
      'candidate_id', v_base->>'candidate_b_id', 'amount', 80.00));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R18; proof/32 §4.3 C5 (ambiguous without supporting provider evidence)',
    v_draft || jsonb_build_object(
      'returned_transfer', v_returned, 'reversed_transfer', v_reversed,
      'raw_committed_transfer', v_committed, 'raw_executed_transfer', v_executed));
end;
$$;

-- R17 — a COMPLETE operation that keeps `scope_freeze_status = 'FROZEN'`.
-- R36 — a terminal operation whose legacy `lock_expires_at_utc` is in the future.
create or replace function ws_banking_fixture.state_operation_history_forms_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'operation_history_forms';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_complete_frozen jsonb;
  v_terminal_legacy_lock jsonb;
  v_live jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_c_id',
      'timesheet_id', v_base->>'timesheet_c_v1',
      'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_complete_frozen := ws_banking_fixture.seed_operation_v1(v_key, 'complete_frozen',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('operation_type', 'PAYMENT_EXECUTE', 'status', 'COMPLETE',
      'phase', 'COMPLETE', 'scope_freeze_status', 'FROZEN'));

  v_terminal_legacy_lock := ws_banking_fixture.seed_operation_v1(v_key, 'terminal_legacy_lock',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('operation_type', 'PAYMENT_EXECUTE', 'status', 'COMPLETE',
      'phase', 'COMPLETE', 'scope_freeze_status', 'NONE',
      'lock_expires_in', '1 hour'));

  v_live := ws_banking_fixture.seed_operation_v1(v_key, 'live_running',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('operation_type', 'PAYMENT_EXECUTE', 'status', 'RUNNING',
      'phase', 'EXECUTE', 'scope_freeze_status', 'FROZEN',
      'lease_expires_in', '30 minutes'));

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R17; R36; proof/32 §4.3 C4 (round-1 C4 defect, and both lease forms)',
    v_draft || jsonb_build_object(
      'complete_frozen_operation', v_complete_frozen,
      'terminal_legacy_lock_operation', v_terminal_legacy_lock,
      'live_operation', v_live));
end;
$$;

-- R44 — batch-level Umbrella transfers with a null `candidate_id`.
create or replace function ws_banking_fixture.state_umbrella_batch_level_transfers_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'umbrella_batch_level_transfers';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_matched jsonb;
  v_contradictory jsonb;
  v_ambiguous jsonb;
  v_item_matched uuid;
  v_item_missing uuid;
begin
  -- item 1 carries a frozen `umbrella_id` and PAYE-free UMBRELLA channel;
  -- item 2 carries NO frozen umbrella at all, which is the "missing frozen item
  -- evidence" arm of `R44`.
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_c_id',
        'timesheet_id', v_base->>'timesheet_c_v1', 'amount', 70.00,
        'pay_channel', 'UMBRELLA')),
    'UMBRELLA');

  v_item_matched := ((v_draft->'pay_batch_item_ids')->>0)::uuid;
  v_item_missing := ((v_draft->'pay_batch_item_ids')->>1)::uuid;

  -- (1) matched: same batch, null candidate_id, non-null umbrella_id equal to the
  --     frozen item umbrella, same frozen pay_channel — all six §4.3 C5 conditions.
  v_matched := ws_banking_fixture.seed_transfer_v1(v_key, 'umbrella_matched',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'PROCESSING', 'rail_state', 'PROCESSING',
      'candidate_id', null, 'umbrella_id', v_base->>'umbrella_one_id',
      'pay_channel', 'UMBRELLA', 'amount', 100.00));

  -- (2) contradictory: the transfer names Umbrella two while the frozen item
  --     names Umbrella one.
  v_contradictory := ws_banking_fixture.seed_transfer_v1(v_key, 'umbrella_contradictory',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'PROCESSING', 'rail_state', 'PROCESSING',
      'candidate_id', null, 'umbrella_id', v_base->>'umbrella_two_id',
      'pay_channel', 'UMBRELLA', 'amount', 100.00));

  -- (3) ambiguous: the transfer's pay_channel does not match the frozen item's,
  --     so condition (5) of §4.3 C5 cannot be satisfied.
  v_ambiguous := ws_banking_fixture.seed_transfer_v1(v_key, 'umbrella_ambiguous',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object('status', 'PROCESSING', 'rail_state', 'PROCESSING',
      'candidate_id', null, 'umbrella_id', v_base->>'umbrella_one_id',
      'pay_channel', 'PAYE', 'amount', 100.00));

  -- (5) the Candidate's CURRENT Umbrella is changed after the Draft was frozen.
  --     `proof/32 §4.3` C5: "the Candidate's current Umbrella is mutable pre-Draft
  --     truth that must never redefine a frozen Draft or payment".
  update public.candidates
  set umbrella_id = (v_base->>'umbrella_two_id')::uuid
  where id = (v_base->>'candidate_a_id')::uuid;

  perform ws_banking_fixture.register_seed(
    v_key || ':candidate_umbrella_changed_after_draft', v_key,
    'public.candidates',
    'ordinary Candidate maintenance (Office edit)',
    '`proof/32 §4.3` C5: "The Umbrella is read only from the frozen Draft item '
    || '(supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql:3291-3307): '
    || 'pay_batch_candidates has no Umbrella column (:3220-3243) and the Candidate''s current Umbrella is '
    || 'mutable pre-Draft truth that must never redefine a frozen Draft or payment"',
    'Candidate A moves from Umbrella one to Umbrella two AFTER the Draft item froze Umbrella one.');

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R44; proof/32 §4.3 C5 six-condition Umbrella rule',
    v_draft || jsonb_build_object(
      'frozen_umbrella_item_id', v_item_matched,
      'missing_frozen_umbrella_item_id', v_item_missing,
      'matched_transfer', v_matched,
      'contradictory_transfer', v_contradictory,
      'ambiguous_transfer', v_ambiguous,
      'candidate_current_umbrella_changed_to', v_base->>'umbrella_two_id'));
end;
$$;

-- All four reservation states.
create or replace function ws_banking_fixture.state_reservation_lifecycle_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'reservation_lifecycle';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_status text;
  v_results jsonb := '{}'::jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_b_id',
      'timesheet_id', v_base->>'timesheet_b_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');

  foreach v_status in array array['RESERVED', 'COMMITTED', 'SETTLED', 'RELEASED']
  loop
    v_results := v_results || jsonb_build_object(
      v_status,
      ws_banking_fixture.seed_reservation_v1(
        v_key, lower(v_status), (v_draft->>'pay_batch_id')::uuid,
        ((v_draft->'pay_batch_candidate_ids')->>0)::uuid, null, v_status, 25.00, false));
  end loop;

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'proof/32 §4.3 C2; §5.1 common conditions; §5.2; pay_advance_reservations_status_chk',
    v_draft || jsonb_build_object('reservations', v_results));
end;
$$;

-- R31 — a voided item in a terminal batch that fits no binding.
create or replace function ws_banking_fixture.state_synthetic_unbound_void_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'synthetic_unbound_void';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_void jsonb;
  v_terminality jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_c_id',
      'timesheet_id', v_base->>'timesheet_c_v1',
      'amount', 70.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_void := ws_banking_fixture.seed_unbound_void_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    ((v_draft->'pay_batch_candidate_ids')->>0)::uuid,
    (v_base->>'timesheet_c_v1')::uuid);

  v_terminality := ws_banking_fixture.seed_batch_terminality_v1(v_key, (v_draft->>'pay_batch_id')::uuid);

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'R31; proof/32 §4.2 class 1; §5.3 (never a silent FROZEN)',
    v_draft || jsonb_build_object('unbound_void', v_void, 'terminality', v_terminality));
end;
$$;

-- Census unbound writer 1 — an item inserted already voided.
create or replace function ws_banking_fixture.state_born_voided_item_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'born_voided_item';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_item jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_b_id',
      'timesheet_id', v_base->>'timesheet_b_v1',
      'amount', 80.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_item := ws_banking_fixture.seed_born_voided_item_v1(
    v_key, ((v_draft->'pay_batch_candidate_ids')->>0)::uuid, (v_base->>'timesheet_b_v1')::uuid);

  return ws_banking_fixture.register_state(v_key, 'NAMED_SEED',
    'census 01 §4.3 unbound writer 1; proof/32 §4.2 classes 1 and 4; contract section 18 OR-3',
    v_draft || jsonb_build_object('born_voided_item', v_item));
end;
$$;

-- ===========================================================================
-- Continuation states (WP-08a handoff §4, orchestrator items 1, 3 and 4)
-- ===========================================================================

-- R1 terminal — the same real Binding A chain, driven to a terminal
-- `PAYMENT_CORRECTION` operation.  `proof/32 §5.1` Binding A requires
-- `status = 'COMPLETE'` and `phase = 'COMPLETE'`; without this state no fixture
-- proves Binding A end to end (WP-08a handoff §4.1).
--
-- Candidate E and family F belong to this state alone: the installed
-- REFRESH_WORKBENCH phase proves the preceding scope invalidation against the
-- Candidate's live scope generation, so a Candidate another state has since
-- dirtied can no longer complete its own correction.
create or replace function ws_banking_fixture.state_batch_cancelled_whole_binding_a_complete_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_cancelled_whole_binding_a_complete';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(jsonb_build_object(
      'candidate_id', v_base->>'candidate_e_id',
      'timesheet_id', v_base->>'timesheet_f_v1',
      'amount', 40.00, 'pay_channel', 'PAYE')),
    'PAYE');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R1 terminal; proof/32 §5.1 Binding A third bullet (operation COMPLETE/COMPLETE)',
    v_draft || v_cancel || jsonb_build_object(
      'requires_finalise', true, 'requires_terminal_finish', true));
end;
$$;

create or replace function ws_banking_fixture.finish_batch_cancelled_whole_binding_a_complete_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'batch_cancelled_whole_binding_a_complete';
  v_state jsonb;
  v_terminal jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  select state_row.result_json into v_state
  from ws_banking_fixture.state_register as state_row where state_row.state_key = v_key;

  if v_state is null then
    raise exception 'WS_BANKING_FIXTURE_STATE_NOT_BUILT'
      using errcode = 'P0001', detail = jsonb_build_object('state_key', v_key)::text;
  end if;

  v_terminal := ws_banking_fixture.complete_correction_operation_v1(
    v_key,
    (v_state->>'correction_request_id')::uuid,
    (v_state->>'correction_operation_id')::uuid);

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R1 terminal; proof/32 §5.1 Binding A third bullet (operation COMPLETE/COMPLETE)',
    v_state || jsonb_build_object('terminal', v_terminal, 'requires_terminal_finish', false));
end;
$$;

-- R3 / R19 second case — a DRAFT remainder that still holds a non-voided item of
-- the SAME family as the voided one (WP-08a handoff §4.3).
--
-- An installed candidate-scoped correction always voids EVERY non-voided item of
-- the Candidate: `pay_payment_correction_selection_prepare_chunk_v1` collects
-- them with `FROM public.pay_batch_items AS item_row WHERE
-- item_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id AND
-- COALESCE(item_row.is_voided, false) IS NOT TRUE` and no item-type or item-id
-- narrowing reaches it from the request descriptor.  A same-Candidate,
-- same-family remainder therefore cannot come from one correction; it comes from
-- the Draft re-projection that follows the cancellation, which inserts a fresh
-- item for the same Timesheet while the batch is back in `DRAFT`.  That
-- re-projected item is the only seeded part of this state.
create or replace function ws_banking_fixture.state_same_family_draft_remainder_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'same_family_draft_remainder';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_a_id',
        'timesheet_id', v_base->>'timesheet_a_v2', 'amount', 100.00,
        'pay_channel', 'UMBRELLA', 'umbrella_id', v_base->>'umbrella_one_id'),
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00,
        'pay_channel', 'PAYE')),
    'MIXED');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R3; R19 second case; proof/32 4.3 C6 (the DRAFT remainder keeps the root frozen)',
    v_draft || v_cancel || jsonb_build_object(
      'requires_finalise', true, 'requires_reprojection_finish', true));
end;
$$;

create or replace function ws_banking_fixture.finish_same_family_draft_remainder_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'same_family_draft_remainder';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_state jsonb;
  v_reprojected uuid := ws_banking_fixture.fid('same_family_draft_remainder:reprojected_family_item');
  v_batch_candidate uuid;
  v_batch_status text;
begin
  perform ws_banking_fixture.assert_local_only();

  select state_row.result_json into v_state
  from ws_banking_fixture.state_register as state_row where state_row.state_key = v_key;

  if v_state is null then
    raise exception 'WS_BANKING_FIXTURE_STATE_NOT_BUILT'
      using errcode = 'P0001', detail = jsonb_build_object('state_key', v_key)::text;
  end if;

  v_batch_candidate := ((v_state->'pay_batch_candidate_ids')->>0)::uuid;

  select batch_row.status into v_batch_status
  from public.pay_batches as batch_row
  where batch_row.id = (v_state->>'pay_batch_id')::uuid;

  if v_batch_status <> 'DRAFT' then
    raise exception 'WS_BANKING_FIXTURE_REMAINDER_NOT_DRAFT'
      using errcode = 'P0001',
            detail = jsonb_build_object('state_key', v_key, 'pay_batch_status', v_batch_status)::text;
  end if;

  insert into public.pay_batch_items(
    id, pay_batch_candidate_id, item_type, timesheet_id, pay_channel,
    amount_ex_vat, amount_vat, amount_inc_vat, umbrella_id, is_voided
  ) values (
    v_reprojected, v_batch_candidate, 'TIMESHEET_PAYMENT',
    (v_base->>'timesheet_a_v2')::uuid, 'UMBRELLA',
    100.00, 0, 100.00, (v_base->>'umbrella_one_id')::uuid, false);

  perform ws_banking_fixture.register_seed(
    'same_family_draft_remainder:reprojected_family_item', v_key,
    'public.pay_batch_items',
    'public.pay_batch_apply_finance_adjustments / public.pay_batch_insert_items_from_preview (Draft re-projection; call-only, never driven here)',
    'census 01 4.6: "Both paths are pre-bank Draft reshaping" - pay_batch_apply_finance_adjustments '
    || 'hard-deletes and re-inserts pay_batch_items during a Draft rebuild, and '
    || 'pay_batch_insert_items_from_preview inserts without naming is_voided so the column default '
    || '(false) applies.  The shape is identical to the ordinary TIMESHEET_PAYMENT item '
    || 'seed_draft_batch_v1 writes.  It cannot come from the correction itself: '
    || 'pay_payment_correction_selection_prepare_chunk_v1 collects EVERY non-voided item of the '
    || 'Candidate (WHERE item_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id AND '
    || 'COALESCE(item_row.is_voided, false) IS NOT TRUE) with no item-type or item-id narrowing',
    'Same Candidate, same family, same Timesheet: one item voided under Binding A, one active, batch DRAFT.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_items'::regclass, format('fixture_row.id = %L', v_reprojected),
    'same_family_draft_remainder:reprojected_family_item');

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R3; R19 second case; proof/32 4.3 C6 (the DRAFT remainder keeps the root frozen)',
    v_state || jsonb_build_object(
      'reprojected_family_item_id', v_reprojected,
      'family_timesheet_id', v_base->>'timesheet_a_v2',
      'requires_reprojection_finish', false));
end;
$$;

-- R41 second half / UNA-019 - one Candidate cancelled out of a multi-Candidate
-- Draft, and then the remainder settles and pays the other Candidate.
-- proof/32 12 R41: "checked while the remainder Draft lives and again after it
-- has paid the other Candidates ... released both times".
-- Candidates F and G belong to this state alone.
create or replace function ws_banking_fixture.state_multi_draft_remainder_settled_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'multi_draft_remainder_settled';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_cancel jsonb;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_f_id',
        'timesheet_id', v_base->>'timesheet_g_v1', 'amount', 30.00,
        'pay_channel', 'PAYE'),
      jsonb_build_object('candidate_id', v_base->>'candidate_g_id',
        'timesheet_id', v_base->>'timesheet_h_v1', 'amount', 90.00,
        'pay_channel', 'PAYE')),
    'PAYE');

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R41 second half; UNA-019; proof/32 4.2 class 2 (Binding A proves the void whatever the remainder does)',
    v_draft || v_cancel || jsonb_build_object(
      'requires_finalise', true, 'requires_settlement_finish', true,
      'cancelled_pay_batch_candidate_id', ((v_draft->'pay_batch_candidate_ids')->>0)::uuid,
      'surviving_pay_batch_candidate_id', ((v_draft->'pay_batch_candidate_ids')->>1)::uuid));
end;
$$;

create or replace function ws_banking_fixture.finish_multi_draft_remainder_settled_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'multi_draft_remainder_settled';
  v_state jsonb;
  v_settled jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  select state_row.result_json into v_state
  from ws_banking_fixture.state_register as state_row where state_row.state_key = v_key;

  if v_state is null then
    raise exception 'WS_BANKING_FIXTURE_STATE_NOT_BUILT'
      using errcode = 'P0001', detail = jsonb_build_object('state_key', v_key)::text;
  end if;

  -- The cancelled Candidate is left unsettled: the settle rail writes
  -- settlement_status = 'SETTLED' only for the Candidates it actually pays, and
  -- this Candidate's only item is voided.
  v_settled := ws_banking_fixture.apply_settlement_evidence_v1(
    v_key, (v_state->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'batch_status', 'SETTLED',
      'candidate_outcomes', jsonb_build_object(
        v_state->>'cancelled_pay_batch_candidate_id', 'null',
        v_state->>'surviving_pay_batch_candidate_id', 'SETTLED')));

  return ws_banking_fixture.register_state(v_key, 'MIXED',
    'R41 second half; UNA-019; proof/32 4.2 class 2 (Binding A proves the void whatever the remainder does)',
    v_state || jsonb_build_object('settlement', v_settled, 'requires_settlement_finish', false));
end;
$$;

-- What the installed pre-bank cancellation really does to a BOUND TRANSFER, and
-- what the installed classifier then makes of it (orchestrator item 5).
--
-- OBSERVED, on a real run of the installed chain against a DRAFT batch: the
-- bound transfer is left EXACTLY as it was — `status = 'PENDING'`, `rail_state`
-- null, `amount` unchanged, `failed_reason` null, and no `pre_bank_cancel_applied`
-- marker in `rail_meta_json` — even though the item it was bound to is now
-- voided.  The installed classifier then returns `PENDING_NON_FINAL`.
--
-- That is a consequence worth stating: `proof/32 §5.1`'s common conditions
-- require "every transfer bound to `i` (`i.pay_bank_transfer_id`) classifies
-- `is_terminal_no_money = true`, or no transfer is bound".  A pre-bank
-- cancellation that leaves a bound `PENDING` transfer therefore CANNOT be proved
-- by Binding A, and `§4.2` class 1 makes the voided item a `CENSUS_ERROR`.
--
-- `public.pay_pre_bank_cancel_apply_work_item` does contain a transfer
-- recalculation which, when the remaining non-voided amount reaches zero and the
-- transfer is not already COMPLETED, writes (installed body lines 2060-2100):
--
--     UPDATE public.pay_bank_transfers AS transfer_to_recalculate
--     SET amount = recalculated_transfers.remaining_amount,
--         status = CASE WHEN recalculated_transfers.remaining_amount = 0
--                        AND upper(btrim(COALESCE(transfer_to_recalculate.status,''))) NOT IN ('COMPLETED')
--                       THEN 'VOIDED' ELSE transfer_to_recalculate.status END,
--         failed_reason = ... 'PRE_BANK_CANCEL_VOIDED' ...,
--         rail_meta_json = ... 'pre_bank_cancel_applied', true,
--                              'pre_bank_cancel_status_note',
--                              'Transfer amount became zero after selected pre-bank cancellation; status set to VOIDED.' ...
--
-- It never writes `rail_state`.  Its sibling
-- `public.pay_no_money_unwind_apply_work_item` writes `status = 'FAILED'` with
-- `failed_reason = 'NO_MONEY_UNWIND'` instead (installed body lines 1752-1763).
--
-- That branch did NOT fire on this run, and this package does not claim to know
-- the exact gate that opens it — establishing that is open work for WP-16c and
-- WP-08b.  What is proved here is only what was observed.
--
-- It matters either way, because `VOIDED` is absent from the installed
-- classifier's terminal list and so resolves to `UNKNOWN`, while `FAILED`
-- resolves to `TERMINAL_NO_MONEY`.  The two Binding A writers can therefore
-- leave transfers that classify differently, and a DRAFT-batch cancellation can
-- leave one that classifies as neither.  The state records the installed
-- classifier's verdict on the real row rather than asserting one.
create or replace function ws_banking_fixture.state_pre_bank_cancel_voided_transfer_v1()
returns jsonb
language plpgsql
as $$
declare
  v_key text := 'pre_bank_cancel_voided_transfer';
  v_base jsonb := ws_banking_fixture.base_world_v1();
  v_draft jsonb;
  v_transfer jsonb;
  v_cancel jsonb;
  v_item uuid;
  v_transfer_id uuid;
  v_after record;
  v_classified record;
begin
  v_draft := ws_banking_fixture.seed_draft_batch_v1(v_key,
    jsonb_build_array(
      jsonb_build_object('candidate_id', v_base->>'candidate_c_id',
        'timesheet_id', v_base->>'timesheet_c_v1', 'amount', 70.00,
        'pay_channel', 'PAYE'),
      jsonb_build_object('candidate_id', v_base->>'candidate_b_id',
        'timesheet_id', v_base->>'timesheet_b_v1', 'amount', 80.00,
        'pay_channel', 'PAYE')),
    'PAYE');

  v_item := ((v_draft->'pay_batch_item_ids')->>0)::uuid;

  -- A PENDING transfer bound to the item, as the execution preparer leaves it
  -- before the money moves.
  v_transfer := ws_banking_fixture.seed_transfer_v1(v_key, 'bound_pending',
    (v_draft->>'pay_batch_id')::uuid,
    jsonb_build_object(
      'status', 'PENDING',
      'candidate_id', v_base->>'candidate_c_id',
      'amount', 70.00,
      'bind_to_item_id', v_item));
  v_transfer_id := (v_transfer->>'pay_bank_transfer_id')::uuid;

  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    v_key, (v_draft->>'pay_batch_id')::uuid,
    array[((v_draft->'pay_batch_candidate_ids')->>0)::uuid]);

  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values (v_key, (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;

  select transfer_row.status, transfer_row.rail_state, transfer_row.amount,
         transfer_row.failed_reason,
         coalesce(transfer_row.rail_meta_json, '{}'::jsonb) as rail_meta_json
  into v_after
  from public.pay_bank_transfers as transfer_row
  where transfer_row.id = v_transfer_id;

  select * into v_classified
  from public._pay_rail_state_money_movement_classify(
    v_after.status, v_after.rail_state, v_after.rail_meta_json, v_after.rail_meta_json);

  return ws_banking_fixture.register_state(v_key, 'REAL_OWNER',
    'proof/32 5.1 Binding A common conditions (a bound transfer that is not terminal-no-money); 4.3 C5; '
    || 'proof/36 4 W2 (is_terminal_no_money permitted only when the item is VOIDED_TERMINAL under Binding A or B)',
    v_draft || v_cancel || jsonb_build_object(
      'requires_finalise', true,
      'bound_pay_batch_item_id', v_item,
      'pay_bank_transfer_id', v_transfer_id,
      'transfer_after_cancellation', jsonb_build_object(
        'status', v_after.status,
        'rail_state', v_after.rail_state,
        'amount', v_after.amount,
        'failed_reason', v_after.failed_reason,
        'rail_meta_json', v_after.rail_meta_json),
      'installed_classifier_verdict', jsonb_build_object(
        'cash_state', v_classified.cash_state,
        'is_final_money_moved', v_classified.is_final_money_moved,
        'is_terminal_no_money', v_classified.is_terminal_no_money,
        'is_pending_non_final', v_classified.is_pending_non_final,
        'reason', v_classified.reason)));
end;
$$;
