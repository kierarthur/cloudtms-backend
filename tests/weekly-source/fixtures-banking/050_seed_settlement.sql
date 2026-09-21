-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
-- NAMED SEEDS for settlement evidence.
--
-- WHY THESE ARE SEEDS, NOT A DRIVEN OWNER.
--   `public.pay_settle_rail` is the sole installed writer of batch terminality
--   (census 01 §4.2).  Its full-batch path refuses unless the batch already
--   carries a server-frozen `execution_intent_json` naming a PAYMENT_EXECUTE
--   operation, and exactly one AUTHORISED `pay_batch_auth_requests` row bound to
--   that operation whose own `execution_intent_json` reproduces the authorised
--   PAYE-net state hash, bank-payment projection hash and explicit-zero counts
--   (installed definition: BATCH_EXECUTION_INTENT_REQUIRED,
--   EXECUTION_OPERATION_REQUIRED, EXECUTION_OPERATION_INVALID,
--   AUTHORISED_EXECUTION_INTENT_REQUIRED).  Reaching it means driving payment
--   execution and authorisation, which contract section 2 places on the
--   do-not-touch list, and which decision D2 puts out of scope.  No repository
--   test drives it either: `grep -rl pay_settle_rail tests/ supabase/verification/`
--   returns only static SQL-contract tests.
--
--   So settlement evidence is seeded to the exact shape the installed writer
--   produces, and every seeded column is cited below to the installed
--   `pg_get_functiondef(public.pay_settle_rail)` statement that writes it.
--
-- CITATIONS (installed definition; the pack cites the same statements by file
-- line as `04082026_1211_pay_settle_rail.sql`):
--
--   batch      UPDATE public.pay_batches AS pb2 SET status = v_batch_status,
--              completed_at_utc = CASE WHEN v_batch_status IN ('SETTLED','FAILED')
--                THEN COALESCE(pb2.completed_at_utc, v_now) ELSE pb2.completed_at_utc END,
--              last_status_checked_at_utc = v_now, total_bank_out = ...,
--              execution_commit_state = CASE WHEN COALESCE(v_completed_transfer_count,0) > 0
--                THEN 'COMMITTED' ... END,
--              execution_commit_ref = ..., execution_committed_at_utc = ...,
--              settlement_confirmation_json = ...
--              (installed body lines 6689-6735; pack cites `:6693-6699`).
--              `schedule_kind` and `scheduled_at_utc` are NOT in the SET list, which
--              is why a SETTLED batch can still carry `schedule_kind = 'SCHEDULED'`
--              (`proof/32 §4.1`, round-1 C3 defect; `R16`).
--
--   candidate  SET settlement_status = 'SETTLED',
--                  settled_at_utc = COALESCE(candidate_update.settled_at_utc, v_now)
--              (installed body lines 2349-2360 and 5051-5052; pack cites `:2352-2363`).
--
--   history    with chosen as (select distinct on (pbs.timesheet_id) pbs.timesheet_id,
--                  pbs.target_snapshot_json, pbs.signature
--                from public.pay_batch_timesheet_snapshots pbs
--                where pbs.pay_batch_id = p_pay_batch_id and pbs.candidate_id in (...)
--                order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id)
--              insert into public.timesheet_pay_state_history(timesheet_id, pay_batch_id,
--                  settled_at_utc, snapshot_json, signature)
--              select c.timesheet_id, p_pay_batch_id, v_now, c.target_snapshot_json, c.signature
--              from chosen c where not exists (select 1 from public.timesheet_pay_state_history
--                existing_history where existing_history.timesheet_id = c.timesheet_id
--                and existing_history.pay_batch_id = p_pay_batch_id)
--              (installed body lines 5292-5320; pack cites snapshot selection `:5297-5305`,
--              insert `:5306-5326`).  The seed therefore copies the CHOSEN snapshot's
--              `target_snapshot_json` and `signature` into the history row, which is what
--              `proof/32 §5.2` proves against.
--
--   cache      insert into public.timesheet_pay_state(... last_settled_signature,
--                  last_settled_pay_batch_id, last_settled_at_utc ...)
--              on conflict (timesheet_id) do update ...
--              (installed body lines 5383-5414; pack cites `:5387-5414`).  `proof/32 §5.2`
--              records that this cache is NEVER authority; the seed writes it only so the
--              fixture matches the installed post-settlement state.
--
--   snapshots  `public.pay_batch_timesheet_snapshots` is NOT written by the settle rail;
--              the rail reads it and raises MISSING_FROZEN_SNAPSHOTS or
--              AMBIGUOUS_TARGET_SNAPSHOT.  The rows are frozen by the Draft/freeze
--              owners, which contract section 2 keeps out of scope, so they are seeded.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- WP-16c ADDITION: real hour segments inside the frozen snapshot, RESTATED
-- ---------------------------------------------------------------------------
-- WHY.  WP-11a proved that every settled fixture family returned
-- `UNAVAILABLE / SNAPSHOT_SEGMENTS_ABSENT` from
-- `private.weekly_source_settlement_allocation_v1`, because the three seeded
-- snapshot literals carried no `segments` array at all, so NO paid-hours figure
-- could be proved on a fixture (`WP-11a_NEEDS.md` N1).  The verdict itself was
-- correct; what was missing was any fixture that could carry real paid hours.
--
-- THE MODEL, AND WHY IT IS NOT A SUM.  The independent review of WP-11a
-- (`WP-11a_REVIEW.md` finding F1) executed the installed chain and established
-- that Banking Pay does NOT accumulate settlements: each settlement snapshot
-- RESTATES the Timesheet's complete position for that root and shift, and the
-- money moved is the residual against the previous position.  A week paid at 8
-- hours and later adjusted to 9 is a 9-hour position, not a 17-hour one.  These
-- seeds therefore restate, and a shift keeps the SAME `segment_id` across every
-- settlement of the same root, so a reader can see that it is one shift
-- restated rather than two different shifts to be added together.
--
-- WHAT THE INSTALLED WRITERS REALLY PRODUCE (the fixture rule: reproduce the
-- installed writer's output, never invent a shape):
--
--   * `public.pay_preview_candidate_build_timesheet_snapshots` (installed)
--     builds `target_snapshot_json` from the CURRENT `timesheets_financials`
--     row: `'segments', invoice_breakdown_json->'segments'` together with the
--     five top-level bucket totals `hours_day`, `hours_night`, `hours_sat`,
--     `hours_sun`, `hours_bh` -- the whole current position -- and sets
--     `base_snapshot_json` to the previous `last_settled_snapshot_json`.
--   * `public.pay_batch_create_timesheet_snapshots` (installed) copies that
--     `segments` array into `pay_batch_timesheet_snapshots.target_snapshot_json`
--     through its `display_metadata_json` merge, and signs the row
--     `md5(snapshot_rows.target_snapshot_json::text)` at definition line 329.
--     These seeds sign the same way, so the signature is verifiable against the
--     content it signs (`WP-11a_REVIEW.md` finding F2).
--   * `public._tsfin_invalid_segment_count` (installed) is the structural rule
--     for one element: a JSON object carrying a non-blank `segment_id`.
--   * `public.pay_settle_rail` copies the CHOSEN snapshot's
--     `target_snapshot_json` verbatim into
--     `public.timesheet_pay_state_history.snapshot_json` (installed body lines
--     5292-5320, cited in this file's header).  The reader derives its hours
--     from the HISTORY row, so the history seed must carry the identical
--     object.  Before this change the history seed built its own literal
--     instead of copying the snapshot's, which is both why N1's literal request
--     would not have lifted `SNAPSHOT_SEGMENTS_ABSENT` and a fidelity defect in
--     its own right.
--
-- POSITIONS.  `BASE` is the position as first paid.  `UPWARD` is the same week
-- restated after a later source change adds an hour to the Monday shift.
-- `DOWNWARD` is the same week restated after a recovery takes an hour off it.
-- Only the Monday shift moves, so the residual is unambiguous:
--
--   BASE      Mon 8.00 day + Wed 4.00 night + Sat 3.00 sat  = 15.00
--   UPWARD    Mon 9.00 day + Wed 4.00 night + Sat 3.00 sat  = 16.00
--   DOWNWARD  Mon 7.00 day + Wed 4.00 night + Sat 3.00 sat  = 14.00
create or replace function ws_banking_fixture.snapshot_segments_v1(
  p_timesheet_id uuid,
  p_position text default 'BASE',
  p_week_ending date default date '2026-03-15'
)
returns jsonb
language sql
immutable
as $function$
  with shifts as (
    select 1 as ordinal, (p_week_ending - 6) as work_date, 'DAY'::text as bucket,
           case upper(coalesce(p_position,'BASE'))
             when 'UPWARD' then 9.00 when 'DOWNWARD' then 7.00 else 8.00 end as hours,
           30 as break_mins, time '08:00' as starts, time '16:00' as ends
    union all
    select 2, (p_week_ending - 4), 'NIGHT', 4.00, 30, time '20:00', time '00:30'
    union all
    select 3, (p_week_ending - 1), 'SAT', 3.00, 0, time '09:00', time '15:00'
  )
  select coalesce(jsonb_agg(
    jsonb_build_object(
      'segment_id', 'ws-fixture-seg:' || p_timesheet_id::text || ':' || shifts.ordinal::text,
      'date', to_char(shifts.work_date, 'YYYY-MM-DD'),
      'start_utc', to_char(shifts.work_date + shifts.starts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
      'end_utc', to_char(
        case when shifts.ends < shifts.starts then shifts.work_date + 1 else shifts.work_date end
        + shifts.ends, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
      'break_mins', shifts.break_mins,
      'hours_day', case when shifts.bucket = 'DAY' then shifts.hours else 0 end,
      'hours_night', case when shifts.bucket = 'NIGHT' then shifts.hours else 0 end,
      'hours_sat', case when shifts.bucket = 'SAT' then shifts.hours else 0 end,
      'hours_sun', 0,
      'hours_bh', 0,
      'exclude_from_pay', false)
    order by shifts.ordinal), '[]'::jsonb)
  from shifts;
$function$;

-- The single builder of the frozen target object.  Every place that must carry
-- the SAME frozen snapshot (the snapshot row, the settled-history copy the rail
-- makes of it, and the never-authoritative last-settled cache) calls this, so
-- they cannot drift apart the way three separate literals did.
create or replace function ws_banking_fixture.snapshot_target_json_v1(
  p_pay_batch_id uuid,
  p_timesheet_id uuid,
  p_position text default 'BASE',
  p_variant text default null
)
returns jsonb
language sql
immutable
as $function$
  with segment_rows as (
    select ws_banking_fixture.snapshot_segments_v1(p_timesheet_id, p_position) as value
  ), totals as (
    select
      coalesce(sum((element.value->>'hours_day')::numeric),0) as hours_day,
      coalesce(sum((element.value->>'hours_night')::numeric),0) as hours_night,
      coalesce(sum((element.value->>'hours_sat')::numeric),0) as hours_sat,
      coalesce(sum((element.value->>'hours_sun')::numeric),0) as hours_sun,
      coalesce(sum((element.value->>'hours_bh')::numeric),0) as hours_bh
    from segment_rows, lateral jsonb_array_elements(segment_rows.value) as element(value)
  )
  select jsonb_build_object(
           'fixture', 'target',
           'timesheet_id', p_timesheet_id,
           'pay_batch_id', p_pay_batch_id,
           'position', upper(coalesce(p_position,'BASE')),
           'segments', segment_rows.value,
           'hours_day', totals.hours_day,
           'hours_night', totals.hours_night,
           'hours_sat', totals.hours_sat,
           'hours_sun', totals.hours_sun,
           'hours_bh', totals.hours_bh)
         || case when p_variant is null then '{}'::jsonb
                 else jsonb_build_object('variant', p_variant) end
  from segment_rows, totals;
$function$;

-- `public.pay_batch_create_timesheet_snapshots` signs the frozen row
-- `md5(target_snapshot_json::text)` (installed definition line 329), so the
-- signature is verifiable against the content it signs.  The seeds sign the
-- same way; the deliberate `EMPTY_SIGNATURE` and `SIGNATURE_MISMATCH` modes
-- still produce their own wrong values on purpose.
create or replace function ws_banking_fixture.snapshot_signature_v1(p_target jsonb)
returns text
language sql
immutable
as $function$
  select md5(p_target::text);
$function$;

-- ---------------------------------------------------------------------------
-- Frozen per-Timesheet snapshots
-- ---------------------------------------------------------------------------
-- p_mode:
--   'PRESENT'             one row, non-empty signature                      (normal)
--   'MISSING'             no row at all                                     (`R35`)
--   'EMPTY_SIGNATURE'     one row whose `signature` is the empty string     (`R35`)
--   'CONFLICTING_SECOND'  a second row for the same (batch, timesheet) on the other
--                         `pay_channel`, with a DIFFERENT `target_snapshot_json` and a
--                         different signature.  `ux_pay_batch_timesheet_snapshots_key`
--                         is `(pay_batch_id, timesheet_id, pay_channel)`, so this is the
--                         only shape a conflicting second row can take, and it is exactly
--                         what the rail's `count(distinct s.target_snapshot_json) > 1`
--                         AMBIGUOUS_TARGET_SNAPSHOT test detects                (`R35`)
create or replace function ws_banking_fixture.seed_timesheet_snapshot_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_timesheet_id uuid,
  p_candidate_id uuid,
  p_pay_channel text default 'PAYE',
  p_mode text default 'PRESENT',
  p_position text default 'BASE'
)
returns jsonb
language plpgsql
as $$
declare
  v_primary uuid := ws_banking_fixture.fid(p_state_key || ':snapshot:' || p_timesheet_id::text);
  v_secondary uuid := ws_banking_fixture.fid(p_state_key || ':snapshot2:' || p_timesheet_id::text);
  -- The frozen target is built once and signed the way the installed writer
  -- signs it, md5(target::text), so the signature is verifiable against the
  -- content the hours are derived from (WP-11a review finding F2).
  v_target jsonb := ws_banking_fixture.snapshot_target_json_v1(
    p_pay_batch_id, p_timesheet_id, p_position);
  v_target_conflict jsonb := ws_banking_fixture.snapshot_target_json_v1(
    p_pay_batch_id, p_timesheet_id, p_position, 'conflict');
  v_signature text := ws_banking_fixture.snapshot_signature_v1(v_target);
  v_other_channel text := case when p_pay_channel = 'PAYE' then 'UMBRELLA' else 'PAYE' end;
begin
  perform ws_banking_fixture.assert_local_only();

  if p_mode = 'MISSING' then
    perform ws_banking_fixture.register_seed(
      p_state_key || ':snapshot_missing:' || p_timesheet_id::text, p_state_key,
      'public.pay_batch_timesheet_snapshots',
      'Banking Pay Draft snapshot freeze (call-only; never driven here)',
      'deliberate absence; `public.pay_settle_rail` raises MISSING_FROZEN_SNAPSHOTS and `proof/32 §5.2` '
      || 'rejects a missing row as CENSUS_ERROR SETTLEMENT_SNAPSHOT_CONFLICT',
      'R35 negative case.');
    return jsonb_build_object('mode', p_mode, 'snapshot_ids', '[]'::jsonb, 'signature', null);
  end if;

  insert into public.pay_batch_timesheet_snapshots(
    id, pay_batch_id, timesheet_id, candidate_id, pay_channel,
    base_snapshot_json, target_snapshot_json, signature, created_at_utc
  ) values (
    v_primary, p_pay_batch_id, p_timesheet_id, p_candidate_id, p_pay_channel,
    jsonb_build_object('fixture', 'base', 'timesheet_id', p_timesheet_id),
    v_target,
    case when p_mode = 'EMPTY_SIGNATURE' then '' else v_signature end,
    now());

  if p_mode = 'CONFLICTING_SECOND' then
    insert into public.pay_batch_timesheet_snapshots(
      id, pay_batch_id, timesheet_id, candidate_id, pay_channel,
      base_snapshot_json, target_snapshot_json, signature, created_at_utc
    ) values (
      v_secondary, p_pay_batch_id, p_timesheet_id, p_candidate_id, v_other_channel,
      jsonb_build_object('fixture', 'base', 'timesheet_id', p_timesheet_id, 'variant', 'conflict'),
      v_target_conflict,
      ws_banking_fixture.snapshot_signature_v1(v_target_conflict),
      now() + interval '1 second');
  end if;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':snapshot:' || p_timesheet_id::text, p_state_key,
    'public.pay_batch_timesheet_snapshots',
    'Banking Pay Draft snapshot freeze (call-only; never driven here)',
    '`public.pay_settle_rail` installed body lines 5269-5290 (`count(distinct s.target_snapshot_json) > 1` '
    || '-> AMBIGUOUS_TARGET_SNAPSHOT) and 5292-5300 (`distinct on (pbs.timesheet_id) ... order by '
    || 'pbs.timesheet_id, pbs.created_at_utc desc, pbs.id`); unique key '
    || '`ux_pay_batch_timesheet_snapshots_key (pay_batch_id, timesheet_id, pay_channel)`',
    format('mode=%s', p_mode));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_timesheet_snapshots'::regclass,
    format('fixture_row.pay_batch_id = %L and fixture_row.timesheet_id = %L', p_pay_batch_id, p_timesheet_id),
    p_state_key || ':pay_batch_timesheet_snapshots');

  return jsonb_build_object(
    'mode', p_mode,
    'snapshot_ids', case when p_mode = 'CONFLICTING_SECOND'
                         then to_jsonb(array[v_primary, v_secondary])
                         else to_jsonb(array[v_primary]) end,
    'signature', case when p_mode = 'EMPTY_SIGNATURE' then '' else v_signature end,
    'position', upper(coalesce(p_position,'BASE')),
    'total_hours', (v_target->>'hours_day')::numeric+(v_target->>'hours_night')::numeric
                   +(v_target->>'hours_sat')::numeric+(v_target->>'hours_sun')::numeric
                   +(v_target->>'hours_bh')::numeric
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- Settlement history rows
-- ---------------------------------------------------------------------------
-- p_mode:
--   'SINGLE'                        one row, signature copied from the chosen snapshot
--   'DUPLICATE_DIFFERENT_SIGNATURE' two rows for the same `(timesheet_id, pay_batch_id)`
--                                   with different signatures                  (`R20`)
--   'DUPLICATE_IDENTICAL'           two rows with identical `snapshot_json` and
--                                   `signature`                                (`R34`)
--   'SIGNATURE_MISMATCH'            one row whose signature is not the chosen
--                                   snapshot's                                 (`R35`)
--   'NONE'                          no row (an unsettled or failed Candidate)
--
-- `timesheet_pay_state_history` has only the ordinary index
-- `idx_timesheet_pay_state_history_ts (timesheet_id, settled_at_utc DESC)` and no
-- uniqueness on the pair, which is exactly why `proof/32 §5.2` says a second row
-- fails closed even when identical: the database permits it.
create or replace function ws_banking_fixture.seed_settlement_history_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_timesheet_id uuid,
  p_signature text,
  p_mode text default 'SINGLE',
  p_position text default 'BASE'
)
returns jsonb
language plpgsql
as $$
declare
  v_first uuid := ws_banking_fixture.fid(p_state_key || ':history:' || p_timesheet_id::text);
  v_second uuid := ws_banking_fixture.fid(p_state_key || ':history2:' || p_timesheet_id::text);
  -- The rail copies the CHOSEN snapshot's target_snapshot_json verbatim into
  -- the history row, so the history seed builds it from the same builder rather
  -- than from a second literal that could drift away from the signed content.
  v_snapshot_json jsonb := ws_banking_fixture.snapshot_target_json_v1(
    p_pay_batch_id, p_timesheet_id, p_position);
  v_signature text := p_signature;
  v_ids uuid[] := array[]::uuid[];
begin
  perform ws_banking_fixture.assert_local_only();

  if p_mode = 'NONE' then
    return jsonb_build_object('mode', p_mode, 'history_ids', '[]'::jsonb);
  end if;

  if p_mode = 'SIGNATURE_MISMATCH' then
    v_signature := ws_banking_fixture.fhash(p_state_key || ':history_mismatch:' || p_timesheet_id::text);
  end if;

  insert into public.timesheet_pay_state_history(
    id, timesheet_id, pay_batch_id, settled_at_utc, snapshot_json, signature
  ) values (v_first, p_timesheet_id, p_pay_batch_id, now(), v_snapshot_json, v_signature);
  v_ids := v_ids || v_first;

  if p_mode in ('DUPLICATE_DIFFERENT_SIGNATURE', 'DUPLICATE_IDENTICAL') then
    insert into public.timesheet_pay_state_history(
      id, timesheet_id, pay_batch_id, settled_at_utc, snapshot_json, signature
    ) values (
      v_second, p_timesheet_id, p_pay_batch_id, now() + interval '1 second',
      case when p_mode = 'DUPLICATE_IDENTICAL' then v_snapshot_json
           else v_snapshot_json || jsonb_build_object('variant', 'second') end,
      case when p_mode = 'DUPLICATE_IDENTICAL' then v_signature
           else ws_banking_fixture.fhash(p_state_key || ':signature_second:' || p_timesheet_id::text) end);
    v_ids := v_ids || v_second;
  end if;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':history:' || p_timesheet_id::text, p_state_key,
    'public.timesheet_pay_state_history',
    'public.pay_settle_rail',
    'installed body lines 5292-5320: `with chosen as (select distinct on (pbs.timesheet_id) ...) '
    || 'insert into public.timesheet_pay_state_history(timesheet_id, pay_batch_id, settled_at_utc, '
    || 'snapshot_json, signature) select c.timesheet_id, p_pay_batch_id, v_now, c.target_snapshot_json, '
    || 'c.signature from chosen c where not exists (...)` — guarded only by WHERE NOT EXISTS, with no '
    || 'uniqueness on (timesheet_id, pay_batch_id) in the schema',
    format('mode=%s', p_mode));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.timesheet_pay_state_history'::regclass,
    format('fixture_row.id = any(%L::uuid[])', v_ids),
    p_state_key || ':timesheet_pay_state_history');

  return jsonb_build_object('mode', p_mode, 'history_ids', to_jsonb(v_ids), 'signature', v_signature);
end;
$$;

-- ---------------------------------------------------------------------------
-- Batch terminality and per-Candidate settlement outcome
-- ---------------------------------------------------------------------------
-- p_options:
--   batch_status            'SETTLED' (default) or 'FAILED'
--   retain_schedule_kind    true -> leave `schedule_kind='SCHEDULED'` and
--                           `scheduled_at_utc` set, which the settle rail never
--                           clears (`proof/32 §4.1`, `R16`)
--   candidate_outcomes      {"<pay_batch_candidate_id>": "SETTLED"|"FAILED"|"PENDING"|"PARTIAL"|"UNPAID"|null}
--                           any Candidate not named defaults to 'SETTLED'
--   execution_commit_state  'COMMITTED' (default); the rail sets COMMITTED whenever a
--                           completed transfer exists
create or replace function ws_banking_fixture.seed_batch_terminality_v1(
  p_state_key text,
  p_pay_batch_id uuid,
  p_options jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
as $$
declare
  v_batch_status text := upper(coalesce(p_options->>'batch_status', 'SETTLED'));
  v_retain_schedule boolean := coalesce((p_options->>'retain_schedule_kind')::boolean, false);
  v_commit_state text := upper(coalesce(p_options->>'execution_commit_state', 'COMMITTED'));
  v_outcomes jsonb := coalesce(p_options->'candidate_outcomes', '{}'::jsonb);
  v_now timestamptz := now();
  v_candidate record;
  v_outcome text;
  v_applied jsonb := '{}'::jsonb;
begin
  perform ws_banking_fixture.assert_local_only();

  if v_batch_status not in ('SETTLED', 'FAILED') then
    raise exception 'WS_BANKING_FIXTURE_UNSUPPORTED_TERMINAL_STATUS'
      using errcode = 'P0001', detail = jsonb_build_object('batch_status', v_batch_status)::text;
  end if;

  update public.pay_batches
  set status = v_batch_status,
      completed_at_utc = coalesce(completed_at_utc, v_now),
      last_status_checked_at_utc = v_now,
      execution_commit_state = v_commit_state,
      execution_commit_ref = coalesce(execution_commit_ref, 'ws-fixture-commit:' || p_state_key),
      execution_committed_at_utc = coalesce(execution_committed_at_utc, v_now),
      settlement_confirmation_json = jsonb_build_object('settlement_mode', 'STANDARD_BANK'),
      schedule_kind = case when v_retain_schedule then 'SCHEDULED' else schedule_kind end,
      scheduled_at_utc = case when v_retain_schedule then coalesce(scheduled_at_utc, v_now - interval '1 hour')
                              else scheduled_at_utc end
  where id = p_pay_batch_id;

  for v_candidate in
    select candidate_row.id
    from public.pay_batch_candidates as candidate_row
    where candidate_row.pay_batch_id = p_pay_batch_id
    order by candidate_row.id
  loop
    v_outcome := coalesce(v_outcomes->>v_candidate.id::text, 'SETTLED');

    if v_outcome = 'null' then
      update public.pay_batch_candidates
      set settlement_status = null, settled_at_utc = null
      where id = v_candidate.id;
    else
      update public.pay_batch_candidates
      set settlement_status = v_outcome,
          settled_at_utc = case when v_outcome = 'SETTLED' then coalesce(settled_at_utc, v_now) else null end,
          paye_state = case when v_outcome = 'SETTLED' then 'SETTLED' else paye_state end
      where id = v_candidate.id;
    end if;

    v_applied := v_applied || jsonb_build_object(v_candidate.id::text, v_outcome);
  end loop;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':batch_terminality', p_state_key,
    'public.pay_batches, public.pay_batch_candidates',
    'public.pay_settle_rail',
    'installed body lines 6689-6735 (`SET status = v_batch_status, completed_at_utc = CASE WHEN '
    || 'v_batch_status IN (''SETTLED'',''FAILED'') THEN COALESCE(pb2.completed_at_utc, v_now) ... '
    || 'execution_commit_state = CASE WHEN COALESCE(v_completed_transfer_count,0) > 0 THEN ''COMMITTED'' ...`) '
    || 'and lines 2349-2360 / 5051-5052 (`SET settlement_status = ''SETTLED'', settled_at_utc = '
    || 'COALESCE(candidate_update.settled_at_utc, v_now)`).  `schedule_kind` and `scheduled_at_utc` are '
    || 'absent from the batch SET list, so they survive settlement',
    format('batch_status=%s retain_schedule_kind=%s', v_batch_status, v_retain_schedule));

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batches'::regclass, format('fixture_row.id = %L', p_pay_batch_id),
    p_state_key || ':pay_batches');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_candidates'::regclass, format('fixture_row.pay_batch_id = %L', p_pay_batch_id),
    p_state_key || ':pay_batch_candidates');

  return jsonb_build_object(
    'pay_batch_id', p_pay_batch_id,
    'batch_status', v_batch_status,
    'retained_schedule_kind', v_retain_schedule,
    'candidate_outcomes', v_applied
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- The one-Timesheet settlement cache the rail also writes
-- ---------------------------------------------------------------------------
-- `proof/32 §5.2`: this cache holds only the LAST settled batch and is never
-- authority.  It is written so the fixture matches the installed post-settlement
-- state, and so a test can prove the census does NOT consult it (`R28`).
create or replace function ws_banking_fixture.seed_last_settled_cache_v1(
  p_state_key text,
  p_timesheet_id uuid,
  p_pay_batch_id uuid,
  p_signature text,
  p_position text default 'BASE'
)
returns jsonb
language plpgsql
as $$
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.timesheet_pay_state(
    timesheet_id, last_settled_snapshot_json, last_settled_signature,
    last_settled_pay_batch_id, last_settled_at_utc
  ) values (
    p_timesheet_id,
    ws_banking_fixture.snapshot_target_json_v1(p_pay_batch_id, p_timesheet_id, p_position),
    p_signature, p_pay_batch_id, now())
  on conflict (timesheet_id) do update
  set last_settled_snapshot_json = excluded.last_settled_snapshot_json,
      last_settled_signature = excluded.last_settled_signature,
      last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
      last_settled_at_utc = excluded.last_settled_at_utc;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':last_settled_cache:' || p_timesheet_id::text, p_state_key,
    'public.timesheet_pay_state',
    'public.pay_settle_rail',
    'installed body lines 5383-5414: `insert into public.timesheet_pay_state(timesheet_id, '
    || 'last_settled_snapshot_json, last_settled_signature, last_settled_pay_batch_id, '
    || 'last_settled_at_utc, ...) ... on conflict (timesheet_id) do update ...`',
    '`proof/32 §5.2`: never authority; a root settled in two batches would otherwise be unprovable.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.timesheet_pay_state'::regclass,
    format('fixture_row.timesheet_id = %L', p_timesheet_id),
    p_state_key || ':timesheet_pay_state');

  return jsonb_build_object('timesheet_id', p_timesheet_id, 'last_settled_pay_batch_id', p_pay_batch_id);
end;
$$;
