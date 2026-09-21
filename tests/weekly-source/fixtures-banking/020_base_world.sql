-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a), base world.
--
-- Creates the Candidate, Contract, Timesheet families and TSFIN rows every
-- Banking Pay evidence state is attached to.  Nothing here is Banking Pay
-- evidence; it is the ordinary business world the evidence hangs off.
--
-- Families created (`proof/32 §4.0`, family key is `timesheets.booking_id`,
-- current version is the `is_current = true` row with the highest `version`):
--
--   A  booking id `'  ws-fixture-booking-a  '`  — ROTATED family, two versions:
--      v1 superseded (`is_current = false`), v2 current.  The booking id carries
--      surrounding whitespace on purpose (`R37`, `ROT`).
--   B  booking id `'ws-fixture-booking-b'`      — single current version.
--   C  booking id `'ws-fixture-booking-c'`      — single current version.
--   D  booking id `'ws-fixture-booking-d'`      — single current version.
--
-- Candidates: A, B, C.  Candidate A starts on Umbrella one (`R44` needs the
-- Candidate's current Umbrella to be changeable after the Draft is frozen).

\set ON_ERROR_STOP on

create or replace function ws_banking_fixture.base_world_v1()
returns jsonb
language plpgsql
as $$
declare
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_client uuid := ws_banking_fixture.fid('client:main');
  v_contract uuid := ws_banking_fixture.fid('contract:main');
  v_umbrella_one uuid := ws_banking_fixture.fid('umbrella:one');
  v_umbrella_two uuid := ws_banking_fixture.fid('umbrella:two');
  v_candidate_a uuid := ws_banking_fixture.fid('candidate:a');
  v_candidate_b uuid := ws_banking_fixture.fid('candidate:b');
  v_candidate_c uuid := ws_banking_fixture.fid('candidate:c');
  -- Candidate D and family E are reserved for `state_paye_net_manual_void_v1`.
  -- `public.pay_set_paye_net_manual` calls `public.pay_batch_validate_freshness`,
  -- which refuses with BATCH_STALE / PAYE_DRAFT_ALREADY_EXISTS when the same
  -- Candidate already has another live PAYE Draft, so that state needs a
  -- Candidate no other state puts into a Draft.
  v_candidate_d uuid := ws_banking_fixture.fid('candidate:d');
  -- Candidates E, F and G are each used by exactly ONE state.  The installed
  -- REFRESH_WORKBENCH phase proves the preceding scope invalidation against the
  -- Candidate's live scope generation
  -- (`public.pay_workbench_enqueue_candidate_refresh`,
  -- PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED), so a Candidate whose
  -- generation another state has since bumped can no longer complete its own
  -- correction.  States that must reach a terminal operation therefore get a
  -- Candidate nothing else touches.
  v_candidate_e uuid := ws_banking_fixture.fid('candidate:e');
  v_candidate_f uuid := ws_banking_fixture.fid('candidate:f');
  v_candidate_g uuid := ws_banking_fixture.fid('candidate:g');
  v_snapshot_run uuid := ws_banking_fixture.fid('snapshot_run:main');
  v_session uuid := ws_banking_fixture.fid('workbench_session:main');
begin
  perform ws_banking_fixture.assert_local_only();

  if exists (select 1 from public.tms_users where id = v_actor) then
    return ws_banking_fixture.base_world_identities_v1();
  end if;

  insert into public.tms_users(id, email, password_hash, role, is_active)
  values (v_actor, 'ws-banking-fixture@example.invalid', 'UNUSABLE_LOCAL_FIXTURE', 'admin', true);

  insert into public.clients(id, name)
  values (v_client, 'WS banking fixture client');

  -- WP-16c ADDITION.  `public._contract_settings_effective_core_v1` raises
  -- CONTRACT_SETTINGS_CLIENT_SETTINGS_NOT_FOUND without an effective
  -- `client_settings` row, and the ordinary authorise owner reaches it through
  -- `v_timesheets_summary_base`.  Without this row no fixture root can be
  -- authorised at all, so `UNA-015` and `UNA-019` could not run.  Shape copied
  -- from the Gate 3 verifier's own world (`supabase/verification/
  -- 17092026_0600_weekly_source_first_authorisation_v1.sql`).
  insert into public.client_settings(client_id, vat_rate_pct, effective_from)
  values (v_client, 20, date '2026-01-01')
  on conflict do nothing;

  insert into public.umbrellas(id, name)
  values (v_umbrella_one, 'WS banking fixture umbrella one'),
         (v_umbrella_two, 'WS banking fixture umbrella two');

  insert into public.candidates(id, display_name, tms_ref, pay_method, umbrella_id)
  values (v_candidate_a, 'WS fixture candidate A', 'WSFIX-A', 'UMBRELLA', v_umbrella_one),
         (v_candidate_b, 'WS fixture candidate B', 'WSFIX-B', 'PAYE', null),
         (v_candidate_c, 'WS fixture candidate C', 'WSFIX-C', 'PAYE', null),
         (v_candidate_d, 'WS fixture candidate D', 'WSFIX-D', 'PAYE', null),
         (v_candidate_e, 'WS fixture candidate E', 'WSFIX-E', 'PAYE', null),
         (v_candidate_f, 'WS fixture candidate F', 'WSFIX-F', 'PAYE', null),
         (v_candidate_g, 'WS fixture candidate G', 'WSFIX-G', 'PAYE', null);

  insert into public.contracts(id, client_id, start_date, end_date, pay_method_snapshot)
  values (v_contract, v_client, date '2026-01-01', date '2026-12-31', 'PAYE');

  -- WP-16c ADDITION. One Contract PER FAMILY, each naming the Candidate that
  -- family's current TSFIN row names.
  --
  -- WHY.  `contract:main` carries no `candidate_id`, and the Weekly Source
  -- first-authorisation and withdrawal owners resolve the Candidate from
  -- `contracts.candidate_id` and then cross-check it against the current TSFIN
  -- owner.  With a null there, EVERY fixture root refused
  -- `WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE / CANDIDATE_UNRESOLVED` before any
  -- test could begin, which is WP-08b handoff N5 and is what blocked `UNA-015`
  -- and `UNA-019` from running at all.  Banking Pay reads the Candidate from
  -- `pay_batch_candidates.candidate_id`, never from the Contract, so none of the
  -- 30 evidence states changes shape; `contract:main` is still created so any
  -- caller reading `base_world_identities_v1()->>'contract_id'` still resolves.
  insert into public.contracts(id, client_id, candidate_id, start_date, end_date, pay_method_snapshot)
  values
    (ws_banking_fixture.fid('contract:a'), v_client, v_candidate_a, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:b'), v_client, v_candidate_b, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:c'), v_client, v_candidate_c, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:d'), v_client, v_candidate_a, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:e'), v_client, v_candidate_d, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:f'), v_client, v_candidate_e, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:g'), v_client, v_candidate_f, date '2026-01-01', date '2026-12-31', 'PAYE'),
    (ws_banking_fixture.fid('contract:h'), v_client, v_candidate_g, date '2026-01-01', date '2026-12-31', 'PAYE');

  -- Family A: rotated, whitespace-padded booking id, v1 superseded + v2 current.
  insert into public.timesheets(
    timesheet_id, booking_id, occupant_key_norm, hospital_norm, ward_norm, job_title_norm,
    week_ending_date, contract_id, version, is_current, status
  ) values
    (ws_banking_fixture.fid('timesheet:a:v1'), '  ws-fixture-booking-a  ',
     'ws fixture occupant a', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:a'), 1, false, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:a:v2'), '  ws-fixture-booking-a  ',
     'ws fixture occupant a', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:a'), 2, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:b:v1'), 'ws-fixture-booking-b',
     'ws fixture occupant b', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:b'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:c:v1'), 'ws-fixture-booking-c',
     'ws fixture occupant c', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:c'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:d:v1'), 'ws-fixture-booking-d',
     'ws fixture occupant d', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:d'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:e:v1'), 'ws-fixture-booking-e',
     'ws fixture occupant e', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:e'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:f:v1'), 'ws-fixture-booking-f',
     'ws fixture occupant f', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:f'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:g:v1'), 'ws-fixture-booking-g',
     'ws fixture occupant g', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:g'), 1, true, 'RECEIVED'),
    (ws_banking_fixture.fid('timesheet:h:v1'), 'ws-fixture-booking-h',
     'ws fixture occupant h', 'ws fixture hospital', 'ws fixture ward', 'ws fixture role',
     date '2026-03-15', ws_banking_fixture.fid('contract:h'), 1, true, 'RECEIVED');

  -- TSFIN: current financial row for every current Timesheet.  The superseded
  -- version A/v1 deliberately also carries a TSFIN row that is not current, so a
  -- census that walks the whole family sees financials on an earlier version
  -- (`proof/32 §4.0`, `R21`).
  insert into public.timesheets_financials(
    id, timesheet_id, timesheet_version, basis, is_current, candidate_id, client_id,
    pay_method, total_hours, total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat
  ) values
    (ws_banking_fixture.fid('tsfin:a:v1'), ws_banking_fixture.fid('timesheet:a:v1'), 1,
     'SELF_REPORTED', false, v_candidate_a, v_client, 'UMBRELLA', 9, 90.00, 126.00, 36.00),
    (ws_banking_fixture.fid('tsfin:a:v2'), ws_banking_fixture.fid('timesheet:a:v2'), 2,
     'SELF_REPORTED', true, v_candidate_a, v_client, 'UMBRELLA', 10, 100.00, 140.00, 40.00),
    (ws_banking_fixture.fid('tsfin:b:v1'), ws_banking_fixture.fid('timesheet:b:v1'), 1,
     'SELF_REPORTED', true, v_candidate_b, v_client, 'PAYE', 8, 80.00, 110.00, 30.00),
    (ws_banking_fixture.fid('tsfin:c:v1'), ws_banking_fixture.fid('timesheet:c:v1'), 1,
     'SELF_REPORTED', true, v_candidate_c, v_client, 'PAYE', 7, 70.00, 98.00, 28.00),
    (ws_banking_fixture.fid('tsfin:d:v1'), ws_banking_fixture.fid('timesheet:d:v1'), 1,
     'SELF_REPORTED', true, v_candidate_a, v_client, 'UMBRELLA', 6, 60.00, 84.00, 24.00),
    (ws_banking_fixture.fid('tsfin:e:v1'), ws_banking_fixture.fid('timesheet:e:v1'), 1,
     'SELF_REPORTED', true, v_candidate_d, v_client, 'PAYE', 5, 50.00, 70.00, 20.00),
    (ws_banking_fixture.fid('tsfin:f:v1'), ws_banking_fixture.fid('timesheet:f:v1'), 1,
     'SELF_REPORTED', true, v_candidate_e, v_client, 'PAYE', 4, 40.00, 56.00, 16.00),
    (ws_banking_fixture.fid('tsfin:g:v1'), ws_banking_fixture.fid('timesheet:g:v1'), 1,
     'SELF_REPORTED', true, v_candidate_f, v_client, 'PAYE', 3, 30.00, 42.00, 12.00),
    (ws_banking_fixture.fid('tsfin:h:v1'), ws_banking_fixture.fid('timesheet:h:v1'), 1,
     'SELF_REPORTED', true, v_candidate_g, v_client, 'PAYE', 9, 90.00, 126.00, 36.00);

  insert into public.banking_pay_snapshot_runs(
    id, pay_date, week_ending_cutoff, pay_week_start, eligibility_from_date, eligibility_to_date
  ) values (
    v_snapshot_run, date '2026-03-20', date '2026-03-15', date '2026-03-09',
    date '2026-03-01', date '2026-03-31');

  insert into public.banking_pay_workbench_sessions(
    id, actor_user_id, pay_date, week_ending_cutoff, session_signature,
    source_snapshot_run_id, version, progress_counter_version, progress_json
  ) values (
    v_session, v_actor, date '2026-03-20', date '2026-03-15',
    'ws banking fixture session', v_snapshot_run, 1, 4, '{"ready":true}'::jsonb);

  -- Session scope rows for every fixture Candidate.  The installed
  -- REFRESH_WORKBENCH phase of a correction operation calls
  -- `public.pay_workbench_enqueue_candidate_refresh`, which raises
  -- "candidate % is not in session scope" when the Candidate has no scope row.
  -- Shape copied from the repository's own Banking selection fixture
  -- `tests/fixtures/28082026_1429_banking_pay_selection_setup.sql`
  -- (`banking_pay_workbench_session_scope` insert, status READY, seeded true,
  -- dirty false, CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3 attestation).
  insert into public.banking_pay_workbench_session_scope(
    session_id, candidate_id, scope_ordinal, status, seeded, dirty,
    certified_preview_publication_attestation_json
  )
  select v_session, scope_row.candidate_id, scope_row.ordinal, 'READY', true, false,
         '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
  from (values (v_candidate_a, 1), (v_candidate_b, 2), (v_candidate_c, 3), (v_candidate_d, 4),
               (v_candidate_e, 5), (v_candidate_f, 6), (v_candidate_g, 7))
    as scope_row(candidate_id, ordinal);

  -- A candidate-scope generation counter must exist before the Workbench
  -- refresh owners run; the repository fixture establishes the same minimum.
  insert into public.app_change_counters(entity_key, seq)
  values ('pay_candidate_scope_generation', 1)
  on conflict (entity_key) do update
  set seq = greatest(public.app_change_counters.seq, excluded.seq);

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.timesheets'::regclass,
    'btrim(fixture_row.booking_id) like ''ws-fixture-booking-%''',
    'base_world_v1:timesheets');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.timesheets_financials'::regclass,
    format('fixture_row.candidate_id in (%L,%L,%L,%L,%L,%L,%L)',
      v_candidate_a, v_candidate_b, v_candidate_c, v_candidate_d,
      v_candidate_e, v_candidate_f, v_candidate_g),
    'base_world_v1:timesheets_financials');

  return ws_banking_fixture.base_world_identities_v1();
end;
$$;

-- The identity map, separate so a state function can read it without rebuilding.
create or replace function ws_banking_fixture.base_world_identities_v1()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'actor_user_id',   ws_banking_fixture.fid('user:actor'),
    'client_id',       ws_banking_fixture.fid('client:main'),
    'contract_id',     ws_banking_fixture.fid('contract:main'),
    'contract_a_id',   ws_banking_fixture.fid('contract:a'),
    'contract_b_id',   ws_banking_fixture.fid('contract:b'),
    'contract_c_id',   ws_banking_fixture.fid('contract:c'),
    'contract_d_id',   ws_banking_fixture.fid('contract:d'),
    'contract_e_id',   ws_banking_fixture.fid('contract:e'),
    'contract_f_id',   ws_banking_fixture.fid('contract:f'),
    'contract_g_id',   ws_banking_fixture.fid('contract:g'),
    'contract_h_id',   ws_banking_fixture.fid('contract:h'),
    'umbrella_one_id', ws_banking_fixture.fid('umbrella:one'),
    'umbrella_two_id', ws_banking_fixture.fid('umbrella:two'),
    'candidate_a_id',  ws_banking_fixture.fid('candidate:a'),
    'candidate_b_id',  ws_banking_fixture.fid('candidate:b'),
    'candidate_c_id',  ws_banking_fixture.fid('candidate:c'),
    'candidate_d_id',  ws_banking_fixture.fid('candidate:d'),
    'candidate_e_id',  ws_banking_fixture.fid('candidate:e'),
    'candidate_f_id',  ws_banking_fixture.fid('candidate:f'),
    'candidate_g_id',  ws_banking_fixture.fid('candidate:g'),
    'timesheet_a_v1',  ws_banking_fixture.fid('timesheet:a:v1'),
    'timesheet_a_v2',  ws_banking_fixture.fid('timesheet:a:v2'),
    'timesheet_b_v1',  ws_banking_fixture.fid('timesheet:b:v1'),
    'timesheet_c_v1',  ws_banking_fixture.fid('timesheet:c:v1'),
    'timesheet_d_v1',  ws_banking_fixture.fid('timesheet:d:v1'),
    'timesheet_e_v1',  ws_banking_fixture.fid('timesheet:e:v1'),
    'timesheet_f_v1',  ws_banking_fixture.fid('timesheet:f:v1'),
    'timesheet_g_v1',  ws_banking_fixture.fid('timesheet:g:v1'),
    'timesheet_h_v1',  ws_banking_fixture.fid('timesheet:h:v1'),
    'booking_a',       '  ws-fixture-booking-a  ',
    'booking_b',       'ws-fixture-booking-b',
    'booking_c',       'ws-fixture-booking-c',
    'booking_d',       'ws-fixture-booking-d',
    'booking_e',       'ws-fixture-booking-e',
    'booking_f',       'ws-fixture-booking-f',
    'booking_g',       'ws-fixture-booking-g',
    'booking_h',       'ws-fixture-booking-h',
    'snapshot_run_id', ws_banking_fixture.fid('snapshot_run:main'),
    'session_id',      ws_banking_fixture.fid('workbench_session:main')
  );
$$;

-- ---------------------------------------------------------------------------
-- Draft batch helper
-- ---------------------------------------------------------------------------
-- Creates a DRAFT `pay_batches` row plus one `pay_batch_candidates` row per
-- supplied Candidate and one non-voided `TIMESHEET_PAYMENT` item per Timesheet.
-- This is the pre-Draft shape a Workbench Draft create leaves behind; it is a
-- NAMED SEED, registered below, because Create Draft is on the contract's
-- do-not-touch list (contract section 2) and is never driven by this library.
create or replace function ws_banking_fixture.seed_draft_batch_v1(
  p_state_key text,
  p_members jsonb,          -- [{"candidate_id":uuid,"timesheet_id":uuid,"amount":numeric,"pay_channel":"PAYE"|"UMBRELLA","umbrella_id":uuid|null}]
  p_batch_kind text default 'PAYE'
)
returns jsonb
language plpgsql
as $$
declare
  v_batch uuid := ws_banking_fixture.fid(p_state_key || ':batch');
  v_actor uuid := ws_banking_fixture.fid('user:actor');
  v_member jsonb;
  v_ordinal integer := 0;
  v_batch_candidate uuid;
  v_item uuid;
  v_items uuid[] := array[]::uuid[];
  v_batch_candidates uuid[] := array[]::uuid[];
begin
  perform ws_banking_fixture.assert_local_only();

  insert into public.pay_batches(
    id, pay_date, status, banking_system_snapshot, external_paye_system_snapshot,
    rail_provider_snapshot, rail_env_snapshot, batch_kind_fixed, created_by_user_id,
    source_workbench_session_id, source_snapshot_run_id, source_session_version,
    execution_commit_state
  ) values (
    v_batch, date '2026-03-20', 'DRAFT', 'MONZO_CSV', 'CSV', 'CSV', 'SANDBOX',
    p_batch_kind, v_actor,
    ws_banking_fixture.fid('workbench_session:main'),
    ws_banking_fixture.fid('snapshot_run:main'), 1,
    'NOT_SUBMITTED');

  for v_member in select * from jsonb_array_elements(p_members)
  loop
    v_ordinal := v_ordinal + 1;
    v_batch_candidate := ws_banking_fixture.fid(p_state_key || ':batch_candidate:' || v_ordinal::text);
    v_item := ws_banking_fixture.fid(p_state_key || ':item:' || v_ordinal::text);

    insert into public.pay_batch_candidates(
      id, pay_batch_id, candidate_id, candidate_tms_ref, candidate_display_name,
      paye_state, settlement_status, gross_preview, net_bank_amount
    )
    select v_batch_candidate, v_batch, (v_member->>'candidate_id')::uuid,
           candidate_row.tms_ref, candidate_row.display_name,
           'READY', 'PENDING',
           (v_member->>'amount')::numeric, (v_member->>'amount')::numeric
    from public.candidates as candidate_row
    where candidate_row.id = (v_member->>'candidate_id')::uuid
    on conflict (pay_batch_id, candidate_id) do nothing;

    select existing_candidate.id into v_batch_candidate
    from public.pay_batch_candidates as existing_candidate
    where existing_candidate.pay_batch_id = v_batch
      and existing_candidate.candidate_id = (v_member->>'candidate_id')::uuid;

    insert into public.pay_batch_items(
      id, pay_batch_candidate_id, item_type, timesheet_id, pay_channel,
      amount_ex_vat, amount_vat, amount_inc_vat, umbrella_id, is_voided
    ) values (
      v_item, v_batch_candidate, 'TIMESHEET_PAYMENT', (v_member->>'timesheet_id')::uuid,
      coalesce(v_member->>'pay_channel', 'PAYE'),
      (v_member->>'amount')::numeric, 0, (v_member->>'amount')::numeric,
      nullif(v_member->>'umbrella_id', '')::uuid, false);

    v_items := v_items || v_item;
    v_batch_candidates := v_batch_candidates || v_batch_candidate;
  end loop;

  perform ws_banking_fixture.register_seed(
    p_state_key || ':seed_draft_batch_v1',
    p_state_key,
    'public.pay_batches, public.pay_batch_candidates, public.pay_batch_items',
    'public.pay_batch_insert_items_from_preview / Workbench Draft create (call-only; never driven here)',
    'census 01 §3 row `public.pay_batch_insert_items_from_preview`: "INSERT column list does not name is_voided (column default applies)"; '
    || '`pay_batches` DRAFT + `execution_commit_state = NOT_SUBMITTED` is the installed default '
    || '(`pay_batches.execution_commit_state` default `NOT_SUBMITTED`, `pay_batches_execution_commit_state_chk`)',
    'Create Draft is on the contract section 2 do-not-touch list, so the pre-Draft shape is seeded, never produced.');

  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batches'::regclass, format('fixture_row.id = %L', v_batch), p_state_key || ':pay_batches');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_candidates'::regclass, format('fixture_row.pay_batch_id = %L', v_batch), p_state_key || ':pay_batch_candidates');
  perform ws_banking_fixture.assert_rows_satisfy_constraints(
    'public.pay_batch_items'::regclass, format('fixture_row.id = any(%L::uuid[])', v_items), p_state_key || ':pay_batch_items');

  return jsonb_build_object(
    'pay_batch_id', v_batch,
    'pay_batch_candidate_ids', to_jsonb(v_batch_candidates),
    'pay_batch_item_ids', to_jsonb(v_items)
  );
end;
$$;
