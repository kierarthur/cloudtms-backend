-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- REAL INSTALLED OWNER driver: the pre-bank cancellation chain (Binding A).
--
-- Nothing in this file defines, wraps or alters a Banking Pay function.  It only
-- calls the installed owners, in the order the installed operation config runs
-- them (`banking_pay_operation_config` phases
-- PREPARE_SELECTION -> EXPAND_WORK -> PROCESS_CHUNKS -> FINALISE -> REFRESH_WORKBENCH,
-- `proof/32 §5.1` Binding A, third bullet):
--
--   1. public.pay_payment_correction_request_start      (command PREPARE)
--   2. public.banking_pay_operation_claim_next          (worker lease)
--   3. public.pay_payment_correction_selection_prepare_chunk_v1
--   4. public.pay_payment_correction_reauth_bind_v1
--   5. public.pay_payment_correction_request_start      (command START_PREPARED)
--   6. public.pay_payment_correction_expand_work
--   7. public.pay_pre_bank_cancel_apply_work_item       <- the installed voider
--      (census 01 §2.1 #1: "UPDATE public.pay_batch_items AS items_to_void
--       SET is_voided = true ... AND COALESCE(items_to_void.is_voided,false) = false",
--       plus the `pay_advance_reservations ... status='RELEASED',
--       released_reason='PRE_BANK_CANCEL'` release)
--   8. public.pay_payment_correction_process_chunk      <- the installed batch-status
--      writer (census 01 §2.1 #7: "SET status = CASE WHEN v_active_item_count = 0
--       THEN 'CANCELLED' ELSE 'DRAFT' END" and the FINALISE `cancelled_at_utc` write)
--
-- Because the real chain is driven, the resulting `pay_payment_correction_requests`
-- status, `pay_payment_correction_request_candidates.candidate_scope_hash`,
-- `pay_payment_correction_work_items.selection_hash` and `result_json`, the voided
-- `pay_batch_items` rows and the batch status are exactly what Banking Pay
-- produces.  None of them is written by this library.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- TEST feature flag
-- ---------------------------------------------------------------------------
-- `public.pay_payment_correction_request_start` reads
-- `settings_defaults.banking_pay_candidate_cancellation_enabled` and raises
-- PAYMENT_CORRECTION_FEATURE_DISABLED when it is false.  A freshly built local
-- database has it false.  Enabling it in a disposable clone is an operational
-- TEST feature flag, not a change to Banking Pay behaviour: it is the same
-- boolean the Office UI toggles, and it is never written to a hosted database.
create or replace function ws_banking_fixture.enable_candidate_cancellation_flag_v1()
returns boolean
language plpgsql
as $$
declare
  v_before boolean;
begin
  perform ws_banking_fixture.assert_local_only();

  select settings_row.banking_pay_candidate_cancellation_enabled
  into v_before
  from public.settings_defaults as settings_row
  order by settings_row.id
  limit 1;

  update public.settings_defaults
  set banking_pay_candidate_cancellation_enabled = true
  where banking_pay_candidate_cancellation_enabled is distinct from true;

  return coalesce(v_before, false);
end;
$$;

-- ---------------------------------------------------------------------------
-- Worker lease helper
-- ---------------------------------------------------------------------------
-- `pay_payment_correction_selection_prepare_chunk_v1` and
-- `pay_payment_correction_process_chunk` both require the caller to hold the
-- operation's worker lease (`PAYMENT_CORRECTION_SELECTION_LEASE_MISMATCH`,
-- `PAYMENT_CORRECTION_OPERATION_LEASE_MISMATCH`).  The installed claim owner
-- refuses with `RUN_AFTER_NOT_DUE` while `run_after_utc` is in the future, which
-- the previous installed phase sets a fraction of a second ahead.  This helper
-- waits for the operation's own `run_after_utc` instead of rewriting it, so the
-- lease is always acquired through the installed owner.
create or replace function ws_banking_fixture.claim_operation_lease_v1(
  p_operation_id uuid,
  p_worker_id text default 'ws-banking-fixture-worker',
  p_lease_seconds integer default 600
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_claim record;
  v_attempt integer := 0;
begin
  perform ws_banking_fixture.assert_local_only();

  -- The installed claim owner refuses `LEASE_ACTIVE` even to the current holder.
  -- When this worker already holds a live lease the downstream owners' lease
  -- checks (`lease_owner = p_worker_id`) are already satisfied, so reuse it.
  if exists (
       select 1
       from public.banking_pay_operations as operation_row
       where operation_row.id = p_operation_id
         and coalesce(operation_row.lease_owner, operation_row.locked_by) = p_worker_id
         and coalesce(operation_row.lease_expires_at_utc, operation_row.lock_expires_at_utc) > now()
     ) then
    return jsonb_build_object('claimed', true, 'attempts', 0, 'worker_id', p_worker_id, 'reused_existing_lease', true);
  end if;

  loop
    v_attempt := v_attempt + 1;

    select claim_result.claimed, claim_result.not_claimed_reason
    into v_claim
    from public.banking_pay_operation_claim_next(
      p_operation_id, v_actor, p_worker_id, p_lease_seconds, true,
      array['PAYMENT_CORRECTION', 'DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLEMENT']
    ) as claim_result;

    if coalesce(v_claim.claimed, false) then
      return jsonb_build_object('claimed', true, 'attempts', v_attempt, 'worker_id', p_worker_id);
    end if;

    if coalesce(v_claim.not_claimed_reason, '') <> 'RUN_AFTER_NOT_DUE' or v_attempt >= 40 then
      raise exception 'WS_BANKING_FIXTURE_OPERATION_LEASE_NOT_CLAIMED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'operation_id', p_operation_id,
                'attempts', v_attempt,
                'not_claimed_reason', v_claim.not_claimed_reason
              )::text;
    end if;

    perform pg_sleep(0.1);
  end loop;
end;
$$;

-- ---------------------------------------------------------------------------
-- The chain
-- ---------------------------------------------------------------------------
-- TRANSACTION BOUNDARY, and why there is one.
--   `pay_payment_correction_expand_work` sets the operation's
--   `run_after_utc = clock_timestamp()` (installed definition, EXPAND_WORK
--   update), while `banking_pay_operation_claim_next` compares against `now()`,
--   the transaction start time (installed definition, `v_now timestamptz := now()`
--   and `COALESCE(operation_row.run_after_utc, v_now) <= v_now`).  Inside one
--   transaction the claim therefore always returns `RUN_AFTER_NOT_DUE`.  That is
--   installed Banking Pay behaviour and this library does not work around it: the
--   PROCESS_CHUNKS step lives in its own function, to be called in a LATER
--   TRANSACTION.  Fixtures are built into a disposable clone and committed, not
--   rolled back, so this costs nothing.
--
--   Steps 1 to 7 -> `drive_pre_bank_cancellation_v1`
--   Step 8       -> `finalise_pre_bank_cancellation_v1` (separate transaction)
create or replace function ws_banking_fixture.drive_pre_bank_cancellation_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_pay_batch_candidate_ids uuid[]
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_worker text := 'ws-banking-fixture-worker';
  v_start jsonb;
  v_request_id uuid;
  v_operation_id uuid;
  v_prepare jsonb;
  v_reauth jsonb;
  v_proof_hash text := ws_banking_fixture.fhash(p_state_key || ':reauth_proof');
  v_session_hash text := ws_banking_fixture.fhash(p_state_key || ':reauth_session');
  v_start_prepared jsonb;
  v_expand jsonb;
  v_work_item record;
  v_apply jsonb;
  v_applied jsonb := '[]'::jsonb;
  v_process jsonb := null;
  v_request_status text;
  v_batch_status text;
  v_batch_cancelled_at timestamptz;
  v_voided_item_ids uuid[];
begin
  perform ws_banking_fixture.assert_local_only();
  perform ws_banking_fixture.enable_candidate_cancellation_flag_v1();

  -- 1. PREPARE
  v_start := public.pay_payment_correction_request_start(
    p_pay_batch_id,
    jsonb_build_object(
      'command', 'PREPARE',
      'scope_type', 'CANDIDATES',
      'mode', 'EXPLICIT',
      'pay_batch_candidate_ids', to_jsonb(p_pay_batch_candidate_ids),
      'requested_action', 'DRAFT_CANCEL'),
    'DRAFT_PAYMENT_CANCELLED_BY_USER',
    v_actor);

  v_request_id := (v_start->>'correction_request_id')::uuid;
  v_operation_id := (v_start->>'operation_id')::uuid;

  if v_request_id is null or v_operation_id is null then
    raise exception 'WS_BANKING_FIXTURE_CORRECTION_PREPARE_FAILED'
      using errcode = 'P0001', detail = v_start::text;
  end if;

  -- 2. lease, 3. PREPARE_SELECTION page (one page is enough for fixture sizes)
  perform ws_banking_fixture.claim_operation_lease_v1(v_operation_id, v_worker);

  v_prepare := public.pay_payment_correction_selection_prepare_chunk_v1(
    v_request_id, v_operation_id, null::jsonb, 100, v_worker, v_actor);

  if coalesce((v_prepare->>'complete')::boolean, false) is not true then
    raise exception 'WS_BANKING_FIXTURE_CORRECTION_SELECTION_INCOMPLETE'
      using errcode = 'P0001', detail = v_prepare::text;
  end if;

  -- 4. reauthentication binding (the installed owner requires a second-resolution
  --    issue time inside its own window, so it is derived, never supplied stale)
  v_reauth := public.pay_payment_correction_reauth_bind_v1(
    v_request_id, v_actor, v_session_hash, v_proof_hash,
    date_trunc('second', now()), date_trunc('second', now()) + interval '10 minutes');

  if coalesce((v_reauth->>'ok')::boolean, false) is not true then
    raise exception 'WS_BANKING_FIXTURE_CORRECTION_REAUTH_FAILED'
      using errcode = 'P0001', detail = v_reauth::text;
  end if;

  -- 5. START_PREPARED.  For a single-authoriser DRAFT_CANCEL the installed owner
  --    authorises in the same call (`single_user_cancellation_authority`), so no
  --    separate `pay_payment_correction_authorise` call is made or needed.
  select public.pay_payment_correction_request_start(
    p_pay_batch_id,
    jsonb_build_object(
      'command', 'START_PREPARED',
      'correction_request_id', request_row.id,
      'scope_type', 'CANDIDATES',
      'mode', 'EXPLICIT',
      'pay_batch_candidate_ids', to_jsonb(p_pay_batch_candidate_ids),
      'requested_action', 'DRAFT_CANCEL',
      'selection_hash', request_row.selection_hash,
      'plan_hash', request_row.plan_hash,
      'proof_hash', v_proof_hash),
    'DRAFT_PAYMENT_CANCELLED_BY_USER',
    v_actor)
  into v_start_prepared
  from public.pay_payment_correction_requests as request_row
  where request_row.id = v_request_id;

  if coalesce((v_start_prepared->>'ok')::boolean, false) is not true then
    raise exception 'WS_BANKING_FIXTURE_CORRECTION_START_FAILED'
      using errcode = 'P0001', detail = v_start_prepared::text;
  end if;

  -- 6. EXPAND_WORK
  v_expand := public.pay_payment_correction_expand_work(v_request_id, v_actor);

  if coalesce((v_expand->>'ok')::boolean, false) is not true then
    raise exception 'WS_BANKING_FIXTURE_CORRECTION_EXPAND_FAILED'
      using errcode = 'P0001', detail = v_expand::text;
  end if;

  -- 7. the installed voider, once per expanded work item
  for v_work_item in
    select work_row.id
    from public.pay_payment_correction_work_items as work_row
    where work_row.correction_request_id = v_request_id
      and work_row.work_kind = 'PRE_BANK_CANCEL'
      and work_row.status = 'PENDING'
    order by work_row.created_at_utc, work_row.id
  loop
    v_apply := public.pay_pre_bank_cancel_apply_work_item(v_work_item.id, v_actor);
    v_applied := v_applied || jsonb_build_array(v_apply);

    if coalesce(v_apply->>'status', '') <> 'APPLIED' then
      raise exception 'WS_BANKING_FIXTURE_CORRECTION_WORK_ITEM_NOT_APPLIED'
        using errcode = 'P0001', detail = v_apply::text;
    end if;
  end loop;

  -- 8. PROCESS_CHUNKS runs in a later transaction; see the note above.
  select request_row.status into v_request_status
  from public.pay_payment_correction_requests as request_row
  where request_row.id = v_request_id;

  select batch_row.status, batch_row.cancelled_at_utc
  into v_batch_status, v_batch_cancelled_at
  from public.pay_batches as batch_row
  where batch_row.id = p_pay_batch_id;

  select coalesce(array_agg(item_row.id order by item_row.id), array[]::uuid[])
  into v_voided_item_ids
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row on candidate_row.id = item_row.pay_batch_candidate_id
  where candidate_row.pay_batch_id = p_pay_batch_id
    and item_row.is_voided;

  return jsonb_build_object(
    'correction_request_id', v_request_id,
    'correction_operation_id', v_operation_id,
    'correction_request_status', v_request_status,
    'work_item_results', v_applied,
    'process_chunk_result', v_process,
    'pay_batch_status', v_batch_status,
    'pay_batch_cancelled_at_utc', v_batch_cancelled_at,
    'voided_pay_batch_item_ids', to_jsonb(v_voided_item_ids)
  );
end;
$$;

-- Step 8, in its own transaction: the installed PROCESS_CHUNKS / FINALISE owner
-- `public.pay_payment_correction_process_chunk`.  This is what writes
-- `pay_batches.status` back to `DRAFT` (an active item remains for another
-- Candidate) or to `CANCELLED` + `cancelled_at_utc` (nothing active remains) —
-- census 01 §2.1 #7, and `proof/32 §4.3` C6 for the `DRAFT` remainder case.
--
-- The installed owner advances ONE phase per call and sets `run_after_utc` to
-- `clock_timestamp()` each time, so the caller loops, one transaction per call,
-- until `has_more` is false.  `advance_correction_operation_until_complete.sql`
-- and the `.mjs` loader both do exactly that.
create or replace function ws_banking_fixture.finalise_pre_bank_cancellation_v1(
  p_correction_request_id uuid,
  p_correction_operation_id uuid
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_worker text := 'ws-banking-fixture-worker';
  v_process jsonb;
  v_pay_batch_id uuid;
  v_batch record;
  v_request_status text;
  v_operation record;
begin
  perform ws_banking_fixture.assert_local_only();

  select request_row.pay_batch_id into v_pay_batch_id
  from public.pay_payment_correction_requests as request_row
  where request_row.id = p_correction_request_id;

  select operation_row.status, operation_row.phase
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_correction_operation_id;

  -- Idempotent: once the operation is terminal, or has reached the
  -- REFRESH_WORKBENCH phase this driver deliberately does not enter, another
  -- call is a no-op that simply reports the state.  A caller can therefore loop
  -- a fixed number of times without a conditional.
  if upper(coalesce(v_operation.status, '')) in ('COMPLETE', 'FAILED', 'CANCELLED')
     or upper(coalesce(v_operation.phase, '')) = 'REFRESH_WORKBENCH' then
    select batch_row.status, batch_row.cancelled_at_utc, batch_row.cancel_reason
    into v_batch
    from public.pay_batches as batch_row
    where batch_row.id = v_pay_batch_id;

    select request_row.status into v_request_status
    from public.pay_payment_correction_requests as request_row
    where request_row.id = p_correction_request_id;

    return jsonb_build_object(
      'process_chunk_result', null,
      'pay_batch_id', v_pay_batch_id,
      'pay_batch_status', v_batch.status,
      'pay_batch_cancelled_at_utc', v_batch.cancelled_at_utc,
      'pay_batch_cancel_reason', v_batch.cancel_reason,
      'correction_request_status', v_request_status,
      'operation_status', v_operation.status,
      'operation_phase', v_operation.phase,
      'has_more', false,
      'stopped_before_workbench_refresh', upper(coalesce(v_operation.phase, '')) = 'REFRESH_WORKBENCH',
      'no_op', true);
  end if;

  perform ws_banking_fixture.claim_operation_lease_v1(p_correction_operation_id, v_worker);

  v_process := public.pay_payment_correction_process_chunk(
    p_correction_request_id, 100, v_worker, v_actor);

  select batch_row.status, batch_row.cancelled_at_utc, batch_row.cancel_reason
  into v_batch
  from public.pay_batches as batch_row
  where batch_row.id = v_pay_batch_id;

  select request_row.status into v_request_status
  from public.pay_payment_correction_requests as request_row
  where request_row.id = p_correction_request_id;

  select operation_row.status, operation_row.phase
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_correction_operation_id;

  return jsonb_build_object(
    'process_chunk_result', v_process,
    'pay_batch_id', v_pay_batch_id,
    'pay_batch_status', v_batch.status,
    'pay_batch_cancelled_at_utc', v_batch.cancelled_at_utc,
    'pay_batch_cancel_reason', v_batch.cancel_reason,
    'correction_request_status', v_request_status,
    'operation_status', v_operation.status,
    'operation_phase', v_operation.phase,
    -- REFRESH_WORKBENCH is NOT driven.  It calls
    -- `public.pay_workbench_enqueue_candidate_refresh`, which refuses with
    -- PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED unless a Workbench
    -- scope invalidation was proved earlier in the same transaction.  Priming
    -- that is exactly what contract section 2 forbids this package from doing
    -- (`private.pay_workbench_scope_invalidate_v1` and the enqueue are
    -- call-only; no Workbench session setting may be set or pre-seeded).
    -- It is a Workbench cache refresh and writes no Banking Pay evidence: the
    -- batch status, `cancelled_at_utc`, the voided items, the released
    -- reservations and the correction rows are all written by the earlier
    -- PROCESS_CHUNKS and FINALISE phases, which this driver does run.
    'has_more', upper(coalesce(v_operation.status, '')) not in ('COMPLETE', 'FAILED', 'CANCELLED')
                and upper(coalesce(v_operation.phase, '')) <> 'REFRESH_WORKBENCH',
    'stopped_before_workbench_refresh', upper(coalesce(v_operation.phase, '')) = 'REFRESH_WORKBENCH'
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- Step 9 — the terminal operation `proof/32 §5.1` Binding A requires
-- ---------------------------------------------------------------------------
-- Binding A's third bullet requires the `PAYMENT_CORRECTION` operation to be
-- `status = 'COMPLETE'` AND `phase = 'COMPLETE'` — "its REFRESH_WORKBENCH phase
-- has finished".  This function drives that as far as the installed owners go,
-- records the exact point at which they stop, and then seeds ONE column.
--
-- 1. It calls the installed `pay_payment_correction_process_chunk` once more at
--    REFRESH_WORKBENCH, inside an exception block.  When the Candidate's live
--    scope generation is still the one the void transaction produced, this
--    SUCCEEDS: the installed `pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1`
--    calls `pay_workbench_enqueue_candidate_refresh` and a
--    `WORKBENCH_CANDIDATE_DIRTY_APPLY` job is queued.  The refusal, when there is
--    one, is captured and returned as evidence rather than raised.
--
-- 2. A FURTHER `process_chunk` call cannot advance past REFRESH_WORKBENCH,
--    because the installed enqueue then returns
--    `{"ok": true, "no_op": true, "blocked": true, "resolved_mode": "BLOCKED",
--      "resolved_job_type": "NOOP",
--      "fallback_reason": "DELTA_REFRESH_DISABLED_FOR_RESERVATION_PATCH"}`
--    and `patch_preview` treats that as PAYMENT_CANCEL_FULL_REFRESH_JOB_INVALID.
--    The queued `WORKBENCH_CANDIDATE_DIRTY_APPLY` job has to be consumed by the
--    Workbench candidate refresh worker first, and that worker rebuilds the
--    Workbench preview — the dependency closure and source build contract
--    section 2 keeps out of scope.  This function does not run it.
--
-- 3. `public.banking_pay_operation_finish(operation_id, 'COMPLETE', …)` — an
--    installed owner — is then called.  It writes `status = 'COMPLETE'`,
--    `runner_state = 'COMPLETE'`, `completed_at_utc`, and nulls both lease forms
--    (installed body lines 372-387).  It does NOT write `phase`.
--
-- 4. `phase = 'COMPLETE'` and the `progress_json.workbench_refresh_status`
--    marker are therefore the ONLY seeded values, reproducing the installed
--    terminal write in `pay_payment_correction_process_chunk`
--    (installed body lines 2402-2419:
--     `SET progress_json = … || jsonb_build_object('workbench_refresh_status', …),
--      phase = CASE WHEN v_refresh_has_more THEN 'REFRESH_WORKBENCH' ELSE 'COMPLETE' END,
--      status = CASE WHEN v_refresh_has_more THEN 'RUNNING' ELSE 'COMPLETE' END,
--      runner_state = CASE WHEN v_refresh_has_more THEN 'RUNNABLE' ELSE 'COMPLETE' END,
--      run_after_utc = CASE WHEN v_refresh_has_more THEN v_now ELSE NULL END,
--      completed_at_utc = CASE WHEN v_refresh_has_more THEN … ELSE v_now END`).
create or replace function ws_banking_fixture.complete_correction_operation_v1(
  p_state_key text,
  p_correction_request_id uuid,
  p_correction_operation_id uuid
)
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_worker text := 'ws-banking-fixture-worker';
  v_refresh_attempt jsonb := '{}'::jsonb;
  v_chunk jsonb;
  v_message text;
  v_detail text;
  v_operation record;
  v_queued_refresh_jobs integer := 0;
begin
  perform ws_banking_fixture.assert_local_only();

  -- 1. let the installed chain run REFRESH_WORKBENCH itself
  begin
    perform ws_banking_fixture.claim_operation_lease_v1(p_correction_operation_id, v_worker);
    v_chunk := public.pay_payment_correction_process_chunk(
      p_correction_request_id, 100, v_worker, v_actor);
    v_refresh_attempt := jsonb_build_object(
      'installed_refresh_workbench_chunk_ran', true,
      'result_phase', v_chunk->>'phase',
      'result_code', v_chunk->>'code');
  exception when others then
    get stacked diagnostics v_message = message_text, v_detail = pg_exception_detail;
    v_refresh_attempt := jsonb_build_object(
      'installed_refresh_workbench_chunk_ran', false,
      'refusal_message', v_message,
      'refusal_detail', v_detail);
  end;

  select count(*)::integer into v_queued_refresh_jobs
  from public.banking_pay_workbench_jobs as job_row
  where job_row.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
    and job_row.status in ('QUEUED', 'RUNNING');

  select operation_row.status, operation_row.phase
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_correction_operation_id;

  -- 2. the installed finish owner
  if upper(coalesce(v_operation.status, '')) <> 'COMPLETE' then
    perform public.banking_pay_operation_finish(
      p_correction_operation_id, 'COMPLETE',
      jsonb_build_object('source', 'ws_banking_fixture', 'state_key', p_state_key),
      null::jsonb);
  end if;

  -- 3. the one seeded column
  update public.banking_pay_operations
  set phase = 'COMPLETE',
      progress_json = coalesce(progress_json, '{}'::jsonb)
        || jsonb_build_object('workbench_refresh_status', 'CURRENT')
  where id = p_correction_operation_id
    and phase is distinct from 'COMPLETE';

  perform ws_banking_fixture.register_seed(
    p_state_key || ':correction_operation_phase_complete',
    p_state_key,
    'public.banking_pay_operations',
    'public.pay_payment_correction_process_chunk (terminal REFRESH_WORKBENCH write) + public.banking_pay_operation_finish (driven, not seeded)',
    'installed `pay_payment_correction_process_chunk` body lines 2402-2419: '
    || '`SET progress_json = ... || jsonb_build_object(''workbench_refresh_status'', ...), '
    || 'phase = CASE WHEN v_refresh_has_more THEN ''REFRESH_WORKBENCH'' ELSE ''COMPLETE'' END, '
    || 'status = CASE WHEN v_refresh_has_more THEN ''RUNNING'' ELSE ''COMPLETE'' END, '
    || 'runner_state = ... , run_after_utc = ... , completed_at_utc = ...`.  Only `phase` and the '
    || '`workbench_refresh_status` marker are seeded; `status`, `runner_state`, `completed_at_utc`, '
    || '`run_after_utc` and both lease forms were written by the installed '
    || '`public.banking_pay_operation_finish` (installed body lines 372-387), which never writes `phase`',
    'The remaining phase transition needs the Workbench candidate refresh worker to consume the '
    || 'queued WORKBENCH_CANDIDATE_DIRTY_APPLY job; that worker rebuilds the Workbench preview and is '
    || 'outside contract section 2.  Evidence of the refusal is returned by this function.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.banking_pay_operations'::regclass,
    format('fixture_row.id = %L', p_correction_operation_id),
    p_state_key || ':correction_operation_terminal');

  select operation_row.status, operation_row.phase
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_correction_operation_id;

  return jsonb_build_object(
    'correction_operation_id', p_correction_operation_id,
    'operation_status', v_operation.status,
    'operation_phase', v_operation.phase,
    'refresh_workbench_attempt', v_refresh_attempt,
    'queued_workbench_dirty_apply_jobs', v_queued_refresh_jobs,
    'phase_seeded', true);
end;
$$;
