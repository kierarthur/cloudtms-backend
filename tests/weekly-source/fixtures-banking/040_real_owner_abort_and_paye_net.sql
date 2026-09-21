-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- REAL INSTALLED OWNER drivers for Binding B and Binding C.
--
--   Binding B  public.pay_batch_abort_failed_draft_create_partial
--              (`proof/32 §5.1` Binding B, `§5.4` row 5; census 01 §2.1 #4:
--               "voids items (SET is_voided = TRUE, ...), releases reservations
--               (CASE ... status='RESERVED' THEN 'RELEASED'), and
--               UPDATE public.pay_batches AS batch_update SET status = 'CANCELLED',
--               cancelled_at_utc = ...").  Serves `R29`.
--
--   Binding C  public.pay_set_paye_net_manual
--              (`proof/32 §5.1` Binding C, `§5.4` row 7; census 01 §2.1 #6:
--               "two void statements (pbi_d_void for OVERPAYMENT_RECOVERY /
--               LOAN_REPAYMENT, pbi_md_void for MANUAL_DEBT_RECOVERY), each
--               SET is_voided = true, reservation_id = null ... and two releases
--               (par_del, par_md) SET status='RELEASED',
--               released_reason='PAYE_NET_REPROJECTION'").  Serves `R30`.
--
-- Neither owner is defined, wrapped or altered here; both are called.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- Binding B — abort of a failed Draft create
-- ---------------------------------------------------------------------------
-- The installed owner requires a FAILED DRAFT_CREATE operation bound to the
-- batch and its Workbench session, and a batch that is still DRAFT /
-- DRAFT_CREATED with `execution_commit_state = 'NOT_SUBMITTED'` and no
-- submission evidence.  The operation row is a NAMED SEED (the Draft create
-- itself is on the contract section 2 do-not-touch list and is never run); the
-- void and the cancellation are produced by the installed owner.
create or replace function ws_banking_fixture.drive_draft_create_abort_v1(
  p_state_key text,
  p_pay_batch_id uuid
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_operation uuid := ws_banking_fixture.fid(p_state_key || ':draft_create_operation');
  v_abort jsonb;
  v_batch record;
  v_voided_item_ids uuid[];
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.banking_pay_operations(
    id, operation_type, status, phase, actor_user_id, workbench_session_id, pay_batch_id,
    idempotency_key, input_json, failed_at_utc
  ) values (
    v_operation, 'DRAFT_CREATE', 'FAILED', 'FINALISE_RESERVATIONS',
    v_actor, ws_banking_fixture.fid('workbench_session:main'), p_pay_batch_id,
    'ws-banking-fixture:' || p_state_key || ':draft-create',
    jsonb_build_object(
      'source_snapshot_run_id', ws_banking_fixture.fid('snapshot_run:main'),
      'source_session_version', 1),
    now());

  perform ws_banking_fixture.register_seed(
    p_state_key || ':seed_failed_draft_create_operation',
    p_state_key,
    'public.banking_pay_operations',
    'Workbench Draft create failure finalisation (call-only; never driven here)',
    'shape required by the installed `public.pay_batch_abort_failed_draft_create_partial` entry guards: '
    || 'operation_type DRAFT_CREATE, status IN (FAILED, ERROR, REVIEW_REQUIRED, NEEDS_REVIEW, REVIEW), '
    || 'workbench_session_id NOT NULL and equal to `pay_batches.source_workbench_session_id`, '
    || 'input_json.source_snapshot_run_id / source_session_version equal to the batch''s',
    'Create Draft is on the contract section 2 do-not-touch list; only its FAILED residue is seeded.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.banking_pay_operations'::regclass,
    format('fixture_row.id = %L', v_operation),
    p_state_key || ':banking_pay_operations');

  v_abort := public.pay_batch_abort_failed_draft_create_partial(
    v_operation, p_pay_batch_id, v_actor,
    'ws banking fixture: aborting a failed Draft create',
    jsonb_build_object('code', 'DRAFT_CREATE_OPERATION_FAILED', 'message', 'ws banking fixture'));

  if coalesce((v_abort->>'ok')::boolean, false) is not true then
    raise exception 'WS_BANKING_FIXTURE_DRAFT_ABORT_FAILED'
      using errcode = 'P0001', detail = v_abort::text;
  end if;

  select batch_row.status, batch_row.cancelled_at_utc
  into v_batch
  from public.pay_batches as batch_row
  where batch_row.id = p_pay_batch_id;

  select coalesce(array_agg(item_row.id order by item_row.id), array[]::uuid[])
  into v_voided_item_ids
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  where candidate_row.pay_batch_id = p_pay_batch_id
    and item_row.is_voided;

  return jsonb_build_object(
    'draft_create_operation_id', v_operation,
    'abort_result', v_abort,
    'pay_batch_status', v_batch.status,
    'pay_batch_cancelled_at_utc', v_batch.cancelled_at_utc,
    'voided_pay_batch_item_ids', to_jsonb(v_voided_item_ids)
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- Binding C — PAYE net re-projection voids a recovery item inside a live Draft
-- ---------------------------------------------------------------------------
-- Adds one finance case, one `LOAN_REPAYMENT` item and its `RESERVED`
-- reservation to an existing DRAFT batch (all named seeds), then calls the
-- installed `public.pay_set_paye_net_manual`, which voids the deduction item,
-- nulls its `reservation_id` and releases the reservation with
-- `released_reason = 'PAYE_NET_REPROJECTION'`.
--
-- THE VOIDED ITEM HAS A NULL `timesheet_id`, AND THAT IS THE INSTALLED TRUTH.
--   WP-08a asked whether this was a fixture artefact.  It is not.  The installed
--   `public.pay_batch_apply_finance_adjustments` is the writer that creates the
--   three item families `pay_set_paye_net_manual` voids, and it supplies
--   `null::uuid as timesheet_id` for every one of them:
--
--     'OVERPAYMENT_RECOVERY'  -> null::uuid as timesheet_id   (installed body line 2218)
--     'MANUAL_DEBT_RECOVERY'  -> null::uuid as timesheet_id   (installed body line 2863)
--     'LOAN_REPAYMENT'        -> null::uuid as timesheet_id   (installed body line 3286)
--     dormant recovery template (`stage_rows.recovery_family as item_type`,
--                                census 01 §4.3 unbound writer 1)
--                             -> null::uuid as timesheet_id   (installed body line 3597)
--
--   Only the PAYOUT families carry one:
--     'LOAN_PAYOUT'           -> a.linked_timesheet_id        (installed body line 661)
--     'UNDERPAYMENT_PAYMENT'  -> fa.linked_timesheet_id       (installed body line 1619)
--     'MANUAL_CREDIT_PAYOUT'  -> null::uuid                   (installed body line 1151)
--
--   Consequence, and it is a real one: `proof/32 §4.2` enumerates items by
--   `timesheet_id = t` for `t ∈ F(root)`, so a Binding C void of a recovery item
--   is NEVER enumerated, classified or bound by the census.  A census that
--   expects to see it will not; a census that requires every voided item in a
--   terminal batch to be bindable must scope that requirement to enumerated
--   items, or it will never fire on these at all.  The same is true of the
--   born-voided dormant template, which answers the practical half of open
--   ruling OR-3: it cannot become a `CENSUS_ERROR` because it is never
--   enumerated.
--
--   The fixture keeps the null, because manufacturing a `timesheet_id` would be
--   a shape no installed writer produces.  `selfcheck.sql` asserts the null so
--   the fact stays testable rather than living only in this comment.
create or replace function ws_banking_fixture.drive_paye_net_manual_void_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_pay_batch_candidate_id uuid,
  p_net_amount numeric default 55.00,
  p_loan_amount numeric default 25.00
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_candidate_id uuid;
  v_case uuid := ws_banking_fixture.fid(p_state_key || ':finance_case');
  v_item uuid := ws_banking_fixture.fid(p_state_key || ':loan_item');
  v_reservation uuid := ws_banking_fixture.fid(p_state_key || ':loan_reservation');
  v_result jsonb;
  v_item_after record;
  v_reservation_after record;
  v_item_timesheet_id uuid;
begin
  perform ws_banking_fixture.assert_local_only();

  select candidate_row.candidate_id into v_candidate_id
  from public.pay_batch_candidates as candidate_row
  where candidate_row.id = p_pay_batch_candidate_id;

  insert into public.pay_advances(
    id, candidate_id, reason, original_amount, outstanding_amount, case_type, advance_kind
  ) values (
    v_case, v_candidate_id, 'MANUAL_ADVANCE'::pay_advance_reason_enum,
    p_loan_amount, p_loan_amount,
    'PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'LEGACY_ADVANCE'::pay_advance_kind_enum);

  insert into public.pay_batch_items(
    id, pay_batch_candidate_id, item_type, pay_channel, amount_ex_vat, amount_vat,
    amount_inc_vat, finance_case_id, is_voided
  ) values (
    v_item, p_pay_batch_candidate_id, 'LOAN_REPAYMENT', 'PAYE',
    p_loan_amount, 0, p_loan_amount, v_case, false);

  insert into public.pay_advance_reservations(
    id, finance_case_id, pay_batch_id, pay_batch_candidate_id, pay_batch_item_id,
    reserved_amount, status
  ) values (
    v_reservation, v_case, p_pay_batch_id, p_pay_batch_candidate_id, v_item,
    p_loan_amount, 'RESERVED');

  update public.pay_batch_items set reservation_id = v_reservation where id = v_item;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':seed_loan_repayment_deduction',
    p_state_key,
    'public.pay_advances, public.pay_batch_items, public.pay_advance_reservations',
    'public.pay_batch_apply_finance_adjustments / public.pay_batch_finalize_reservations_and_markers (call-only; never driven here)',
    'census 01 §3 row `public.pay_batch_finalize_reservations_and_markers`: "Inserts reservations as RESERVED with '
    || 'ON CONFLICT ... DO UPDATE rebinding ids only (status untouched); pay_batch_items write sets reservation_id, updated_at"; '
    || 'the deduction item shape is the `LOAN_REPAYMENT` item `public.pay_set_paye_net_manual` selects into `_tmp_ded_item_ids`',
    'Only the pre-existing deduction is seeded; the void and the release are made by the installed owner.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_advances'::regclass, format('fixture_row.id = %L', v_case), p_state_key || ':pay_advances');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_items'::regclass, format('fixture_row.id = %L', v_item), p_state_key || ':pay_batch_items');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_advance_reservations'::regclass, format('fixture_row.id = %L', v_reservation), p_state_key || ':pay_advance_reservations');

  v_result := public.pay_set_paye_net_manual(
    p_pay_batch_id,
    jsonb_build_array(jsonb_build_object(
      'pay_batch_candidate_id', p_pay_batch_candidate_id,
      'net_amount', p_net_amount)),
    v_actor);

  select item_row.is_voided, item_row.reservation_id
  into v_item_after
  from public.pay_batch_items as item_row where item_row.id = v_item;

  select reservation_row.status, reservation_row.released_reason
  into v_reservation_after
  from public.pay_advance_reservations as reservation_row where reservation_row.id = v_reservation;

  if v_item_after.is_voided is not true
     or coalesce(v_reservation_after.status, '') <> 'RELEASED'
     or coalesce(v_reservation_after.released_reason, '') <> 'PAYE_NET_REPROJECTION' then
    raise exception 'WS_BANKING_FIXTURE_PAYE_NET_VOID_NOT_PRODUCED'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'item_is_voided', v_item_after.is_voided,
              'reservation_status', v_reservation_after.status,
              'released_reason', v_reservation_after.released_reason,
              'paye_net_result', v_result
            )::text;
  end if;

  select item_row.timesheet_id into v_item_timesheet_id
  from public.pay_batch_items as item_row where item_row.id = v_item;

  return jsonb_build_object(
    'finance_case_id', v_case,
    'voided_pay_batch_item_id', v_item,
    'voided_item_timesheet_id', v_item_timesheet_id,
    'installed_writer_supplies_null_timesheet_id', true,
    'installed_writer_citation',
      'public.pay_batch_apply_finance_adjustments supplies null::uuid as timesheet_id for '
      || 'OVERPAYMENT_RECOVERY (installed body line 2218), MANUAL_DEBT_RECOVERY (2863), '
      || 'LOAN_REPAYMENT (3286) and the dormant recovery template (3597); only LOAN_PAYOUT (661) '
      || 'and UNDERPAYMENT_PAYMENT (1619) carry linked_timesheet_id',
    'census_consequence',
      'proof/32 §4.2 enumerates items by timesheet_id = t over F(root), so a Binding C recovery void '
      || 'is never enumerated by the census',
    'released_reservation_id', v_reservation,
    'released_reason', v_reservation_after.released_reason,
    'paye_net_result', v_result
  );
end;
$$;
