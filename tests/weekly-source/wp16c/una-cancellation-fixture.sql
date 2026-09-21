-- Weekly Source Plan 6.2 — Gate 12 (WP-16c). COMMITTED fixture for UNA-015 and
-- UNA-019, the two rows WP-07 handed over because they "need several
-- transactions, which a rollback-only verifier cannot provide"
-- (`WP-07_NEEDS.md` N7).
--
-- WHY THIS EXISTS BESIDE WP-16a's LIBRARY.  WP-16a's 30 named states build
-- Banking Pay evidence on roots that were never first-authorised, because the
-- library deliberately contains no Weekly Source objects.  UNA-015 and UNA-019
-- need the opposite order: a root that is **first-authorised, and only then**
-- cancelled out of a Draft.  So this fixture builds its own small world,
-- authorises it through the REAL installed Weekly Source owner, and then drives
-- WP-16a's own REAL cancellation helpers against it.  Nothing is re-implemented:
-- `seed_draft_batch_v1`, `drive_pre_bank_cancellation_v1`,
-- `finalise_pending_cancellations_v1`, `complete_correction_operation_v1` and
-- `apply_settlement_evidence_v1` are WP-16a's, called with this fixture's ids.
--
-- RUN IT WITHOUT AN EXPLICIT TRANSACTION.  psql autocommit gives each statement
-- its own transaction, which the installed correction chain requires:
-- `pay_payment_correction_expand_work` sets `run_after_utc = clock_timestamp()`
-- while `banking_pay_operation_claim_next` compares against `now()`, so a later
-- phase can only be claimed from a later transaction (WP-16a README, "Two
-- transactions, and why").
--
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f <fixtures-banking>/install.sql
--   psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f una-cancellation-fixture.sql
--
-- Roots produced:
--   U1  `e6000000-…-000000000301`  whole-batch cancellation  -> UNA-015
--   U2  `e6000000-…-000000000302`  one Candidate out of a two-Candidate Draft,
--       with U3's Candidate as the remainder that later pays  -> UNA-019
--
-- Nothing here defines, wraps or re-creates a Banking Pay owner.

\set ON_ERROR_STOP on
set statement_timeout = '300s';

-- WP-16a's base world supplies the actor, the Workbench session and the
-- snapshot run that `seed_draft_batch_v1` freezes into every Draft.
select ws_banking_fixture.base_world_v1() ->> 'actor_user_id' as base_world_actor;
select ws_banking_fixture.enable_candidate_cancellation_flag_v1() as cancellation_flag;

-- ---------------------------------------------------------------------------
-- This fixture's own Weekly-Source-authorisable world.
-- ---------------------------------------------------------------------------
do $seed_wp16c_una_world$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_client uuid := ws_banking_fixture.fid('client:main');
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  perform ws_banking_fixture.assert_local_only();

  for v_index in 1..3 loop
    v_candidate := ('e6000000-0000-4000-8000-0000000001' || lpad(v_index::text, 2, '0'))::uuid;
    v_contract  := ('e6000000-0000-4000-8000-0000000002' || lpad(v_index::text, 2, '0'))::uuid;
    v_timesheet := ('e6000000-0000-4000-8000-0000000003' || lpad(v_index::text, 2, '0'))::uuid;

    insert into public.candidates(id, display_name, tms_ref, pay_method)
    values (v_candidate, 'WP16C UNA Candidate ' || v_index, 'WP16C-UNA-' || v_index, 'PAYE')
    on conflict (id) do nothing;

    -- The Contract names its Candidate: the withdrawal owner resolves the
    -- Candidate from `contracts.candidate_id` and cross-checks the current TSFIN
    -- owner, and refuses CANDIDATE_UNRESOLVED without it.
    insert into public.contracts(
      id, candidate_id, client_id, start_date, end_date, pay_method_snapshot,
      rates_json, weekly_timesheet_source, self_bill, no_timesheet_required,
      requires_hr, autoprocess_hr
    ) values (
      v_contract, v_candidate, v_client, date '2026-01-01', date '2026-12-31', 'PAYE',
      '{}'::jsonb, 'HEALTHROSTER', true, true, true, true)
    on conflict (id) do nothing;

    insert into public.timesheets(
      timesheet_id, booking_id, version, is_current, status, sheet_scope,
      submission_mode, line_type, occupant_key_norm, hospital_norm, ward_norm,
      job_title_norm, shift_label_norm, week_ending_date, contract_id,
      actual_schedule_json, qr_payload_json, is_adjustment, created_at, updated_at
    ) values (
      v_timesheet, 'WP16C-UNA-BK-' || lpad(v_index::text, 2, '0'), 1, true,
      'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,
      'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,
      'wp16c-una-occupant-' || v_index, 'wp16c-una-hospital', 'wp16c-una-ward',
      'wp16c-una-role', 'weekly-0', date '2026-03-15', v_contract,
      '[]'::jsonb, '{}'::jsonb, false, statement_timestamp(), statement_timestamp())
    on conflict (timesheet_id) do nothing;

    insert into public.contract_weeks(
      id, contract_id, week_ending_date, additional_seq, status,
      submission_mode_snapshot, timesheet_id, is_adjustment
    ) values (
      ('e6000000-0000-4000-8000-0000000004' || lpad(v_index::text, 2, '0'))::uuid,
      v_contract, date '2026-03-15', 0,
      'SUBMITTED'::public.contract_week_status_enum,
      'MANUAL'::public.submission_mode_enum, v_timesheet, false)
    on conflict (id) do nothing;

    insert into public.timesheets_financials(
      id, timesheet_id, timesheet_version, is_current, candidate_id, client_id,
      processing_status, total_hours, total_pay_ex_vat, total_charge_ex_vat
    ) values (
      ('e6000000-0000-4000-8000-0000000005' || lpad(v_index::text, 2, '0'))::uuid,
      v_timesheet, 1, true, v_candidate, v_client,
      'PENDING_AUTH'::public.ts_fin_processing_status_enum, 10, 100, 200)
    on conflict (id) do nothing;
  end loop;
end
$seed_wp16c_una_world$;

-- The installed dirty trigger queued a job for every Candidate this fixture
-- touched; the installed serial gate would otherwise report
-- CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION and every gated owner would
-- refuse WEEKLY_SOURCE_CANDIDATE_BUSY (WP-07 finding F1).
update public.banking_pay_workbench_jobs
   set status = 'SUCCEEDED', completed_at_utc = clock_timestamp()
 where status in ('QUEUED', 'RUNNING');

-- ---------------------------------------------------------------------------
-- First authorisation, through the REAL installed owner, BEFORE Banking Pay
-- sees the roots.  This is the ordering UNA-015 and UNA-019 describe and the
-- ordering WP-16a's library cannot produce.
-- ---------------------------------------------------------------------------
do $authorise_wp16c_una_roots$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_index integer;
  v_timesheet uuid;
  v_result jsonb;
begin
  perform set_config('request.jwt.claim.role', 'service_role', true);
  for v_index in 1..3 loop
    v_timesheet := ('e6000000-0000-4000-8000-0000000003' || lpad(v_index::text, 2, '0'))::uuid;
    v_result := public.weekly_source_first_authorise_v1(v_timesheet, v_timesheet, null, v_actor);
    if coalesce((v_result->>'ok')::boolean, false) is not true then
      raise exception 'WP16C_UNA_FIXTURE_AUTHORISE_FAILED for %: %', v_timesheet, v_result::text;
    end if;
    update public.banking_pay_workbench_jobs
       set status = 'SUCCEEDED', completed_at_utc = clock_timestamp()
     where status in ('QUEUED', 'RUNNING');
  end loop;
end
$authorise_wp16c_una_roots$;

-- ---------------------------------------------------------------------------
-- UNA-015: a whole-batch cancellation of an AUTHORISED root, driven through the
-- real installed correction chain to Binding A.
-- ---------------------------------------------------------------------------
select ws_banking_fixture.seed_draft_batch_v1(
  'wp16c_una015',
  jsonb_build_array(jsonb_build_object(
    'candidate_id', 'e6000000-0000-4000-8000-000000000101'::uuid,
    'timesheet_id', 'e6000000-0000-4000-8000-000000000301'::uuid,
    'amount', 100.00, 'pay_channel', 'PAYE')),
  'PAYE') ->> 'pay_batch_id' as una015_batch;

-- The drive owns steps 1-7; step 8 (FINALISE) is claimed from a later
-- transaction by `finalise_pending_cancellations_v1`, which reads the queue
-- table, so the request and operation ids are registered there and in the state
-- register exactly as WP-16a's own state functions do.
do $drive_wp16c_una015$
declare
  v_cancel jsonb;
begin
  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    'wp16c_una015',
    ws_banking_fixture.fid('wp16c_una015:batch'),
    array[ws_banking_fixture.fid('wp16c_una015:batch_candidate:1')]);
  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values ('wp16c_una015', (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;
  perform ws_banking_fixture.register_state('wp16c_una015', 'REAL_OWNER',
    'WP-16c Gate 12: an AUTHORISED root cancelled through the real installed chain',
    v_cancel);
end
$drive_wp16c_una015$;

-- ---------------------------------------------------------------------------
-- UNA-019: this Candidate cancelled out of a two-Candidate Draft.  The
-- remainder Draft stays alive; the second half of the row settles it later.
-- ---------------------------------------------------------------------------
select ws_banking_fixture.seed_draft_batch_v1(
  'wp16c_una019',
  jsonb_build_array(
    jsonb_build_object(
      'candidate_id', 'e6000000-0000-4000-8000-000000000102'::uuid,
      'timesheet_id', 'e6000000-0000-4000-8000-000000000302'::uuid,
      'amount', 90.00, 'pay_channel', 'PAYE'),
    jsonb_build_object(
      'candidate_id', 'e6000000-0000-4000-8000-000000000103'::uuid,
      'timesheet_id', 'e6000000-0000-4000-8000-000000000303'::uuid,
      'amount', 70.00, 'pay_channel', 'PAYE')),
  'PAYE') ->> 'pay_batch_id' as una019_batch;

-- The drive owns steps 1-7; step 8 (FINALISE) is claimed from a later
-- transaction by `finalise_pending_cancellations_v1`, which reads the queue
-- table, so the request and operation ids are registered there and in the state
-- register exactly as WP-16a's own state functions do.
do $drive_wp16c_una019$
declare
  v_cancel jsonb;
begin
  v_cancel := ws_banking_fixture.drive_pre_bank_cancellation_v1(
    'wp16c_una019',
    ws_banking_fixture.fid('wp16c_una019:batch'),
    array[ws_banking_fixture.fid('wp16c_una019:batch_candidate:1')]);
  insert into ws_banking_fixture.pending_cancellation(state_key, correction_request_id, correction_operation_id)
  values ('wp16c_una019', (v_cancel->>'correction_request_id')::uuid, (v_cancel->>'correction_operation_id')::uuid)
  on conflict (state_key) do update
  set correction_request_id = excluded.correction_request_id,
      correction_operation_id = excluded.correction_operation_id;
  perform ws_banking_fixture.register_state('wp16c_una019', 'REAL_OWNER',
    'WP-16c Gate 12: an AUTHORISED root cancelled through the real installed chain',
    v_cancel);
end
$drive_wp16c_una019$;

-- Advance both correction chains through PROCESS_CHUNKS and FINALISE.  Each
-- call is its own transaction; five is more than the chain needs.
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_1;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_2;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_3;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_4;
select ws_banking_fixture.finalise_pending_cancellations_v1() ->> 'pending' as pending_after_5;

-- Binding A's third bullet requires the correction operation to be terminal
-- (COMPLETE/COMPLETE) with both lease forms clear; until then the freeze census
-- correctly classes the voided item ACTIVE and W3 refuses.  Each completion is
-- its own transaction, as WP-16a's own finisher is.
select ws_banking_fixture.complete_correction_operation_v1(
  'wp16c_una015',
  (select (result_json->>'correction_request_id')::uuid from ws_banking_fixture.state_register where state_key='wp16c_una015'),
  (select (result_json->>'correction_operation_id')::uuid from ws_banking_fixture.state_register where state_key='wp16c_una015')
) ->> 'phase' as una015_operation_phase;

select ws_banking_fixture.complete_correction_operation_v1(
  'wp16c_una019',
  (select (result_json->>'correction_request_id')::uuid from ws_banking_fixture.state_register where state_key='wp16c_una019'),
  (select (result_json->>'correction_operation_id')::uuid from ws_banking_fixture.state_register where state_key='wp16c_una019')
) ->> 'phase' as una019_operation_phase;

update public.banking_pay_workbench_jobs
   set status = 'SUCCEEDED', completed_at_utc = clock_timestamp()
 where status in ('QUEUED', 'RUNNING');

select 'WP16C_UNA_CANCELLATION_FIXTURE_READY' as result,
       (select status::text from public.pay_batches
         where id = ws_banking_fixture.fid('wp16c_una015:batch')) as una015_batch_status,
       (select status::text from public.pay_batches
         where id = ws_banking_fixture.fid('wp16c_una019:batch')) as una019_batch_status,
       (select count(*) from public.pay_batch_items
         where id = ws_banking_fixture.fid('wp16c_una015:item:1') and is_voided) as una015_voided,
       (select count(*) from public.pay_batch_items
         where id = ws_banking_fixture.fid('wp16c_una019:item:1') and is_voided) as una019_voided,
       (select count(*) from public.pay_batch_items
         where id = ws_banking_fixture.fid('wp16c_una019:item:2') and not is_voided) as una019_remainder_live;
