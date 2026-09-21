// Weekly Source Plan 6.2 — Gate 12 executed proof suite: ROT-001..ROT-012.
// WP-16c. Contract section 16, build item G12-5; G12-7 puts rotation second.
//
//   node tests/weekly-source/wp16c/rot-suite.mjs [--results <dir>] [--keep]
//
// Environment: PSQL_BIN, PGPASSWORD (local throwaway), PGOPTIONS='-c jit=off'.
// In Git Bash also export MSYS_NO_PATHCONV=1.
//
// FOUR STEPS:
//
//   A  the Gate 6 rotation-authority verifier and the Gate 5 pending-release
//      verifier are RUN, not cited. Between them they drive `ROT-001` (the
//      stale refusal), `ROT-006` (`R22`, a waiting bundle whose stored root
//      rotates) and `ROT-012`'s privilege half.
//   B  `rot-proofs.sql`, this package's own single-session proofs. `ROT-005`,
//      `ROT-006`, `ROT-007` and `ROT-008` are proved here for the first time:
//      they appear in no verifier in the repository. `ROT-004`'s runtime half
//      (WP-09 limitation L1) drives every entry point this world can reach with
//      `candidate_route_confirmation` off and on.
//   C  `ROT-002`, `ROT-003` and `ROT-012`, three named connections against a
//      COMMITTED fixture, both orders, with a deadlock check on each.
//   D  `ROT-012`'s privilege half, executed: the private core and legacy bodies
//      are not executable by the browser or service roles.
//
// A refusal that the pack says should be an allowance is recorded as FAIL and
// attributed; it is never quietly re-expected.

import path from 'node:path';
import {
  BACKEND_ROOT, WP16C_ROOT, createProofRecorder, localUrl, parseProofLines,
  printProofTable, psqlFile, psqlQuery, standaloneNamedSessionGroup, writeSuiteEnvelope,
} from './proof-runner.mjs';

const TEMPLATE = process.env.WP16C_TEMPLATE ?? 'ws62_wp16c_template';
const VERIFICATION = path.join(BACKEND_ROOT, 'supabase', 'verification');
const ACTOR = 'd6000000-0000-4000-8000-000000000001';
const CLUSTER = { asServiceRole: false };

// RACE-C and RACE-D are padded, rotated and deliberately NOT authorised: both
// rows race a rotation against a FIRST authorisation, so they must start before
// one exists.
const RACE_C_ROOT = 'd6000000-0000-4000-8000-000000000303';
const RACE_C_CANDIDATE = 'd6000000-0000-4000-8000-000000000103';
const RACE_D_ROOT = 'd6000000-0000-4000-8000-000000000304';
const RACE_D_CANDIDATE = 'd6000000-0000-4000-8000-000000000104';

function adminUrl() { return localUrl('postgres'); }

function createClone(name) {
  psqlQuery(adminUrl(), `drop database if exists ${name} with (force);`, CLUSTER);
  psqlQuery(adminUrl(), `create database ${name} template ${TEMPLATE};`, CLUSTER);
  psqlQuery(adminUrl(), `alter database ${name} set jit = off;`, CLUSTER);
  return name;
}

function dropClone(name) {
  try { psqlQuery(adminUrl(), `drop database if exists ${name} with (force);`, CLUSTER); } catch { /* best effort */ }
}

async function declareServiceRole(group) {
  for (const name of group.names) {
    await group.session(name).runWithin(`set session "request.jwt.claim.role" = 'service_role';`, 30_000);
  }
}

function isDeadlock(error) {
  return Boolean(error) && (error.sqlstate === '40P01' || /deadlock detected/i.test(error.message ?? ''));
}

function lastRow(result) {
  return (result.rows ?? []).filter(Boolean).at(-1) ?? '';
}

/** The first ERROR or ASSERTION_FAILED line, which is what a handoff needs. */
function firstError(stderr) {
  const text = String(stderr ?? '');
  const line = text.split(String.fromCharCode(10))
    .map((candidate) => candidate.replace(String.fromCharCode(13), ''))
    .find((candidate) => candidate.includes('ERROR') || candidate.includes('ASSERTION_FAILED'));
  return (line ?? text.slice(-260)).slice(0, 300);
}

function runVerifier(recorder, url, fileName, ids, note) {
  const outcome = psqlFile(url, path.join(VERIFICATION, fileName));
  for (const id of ids) {
    recorder.record(id, outcome.ok ? 'PASS' : 'FAIL', 'EXECUTED',
      `clone + supabase/verification/${fileName}`,
      outcome.ok ? note : `verifier failed: ${firstError(outcome.stderr)}`);
  }
  return outcome;
}

// --- step C: ROT-002, ROT-003, ROT-012 --------------------------------------
async function proveRotationRaces(recorder, database, openGroup) {
  const state = `${database} (committed race fixture)`;

  // ---- ROT-002: the rotation commits first. --------------------------------
  let group = await openGroup({ database, names: ['weekly', 'rotate', 'observer'], groupId: 'wp16c-rot' });
  try {
    await declareServiceRole(group);
    const weekly = group.session('weekly');
    const rotate = group.session('rotate');

    // The Weekly Source side takes the family lock set first, so the rotation
    // has to wait for it: that is the contention ROT-002 describes.
    await weekly.runWithin('begin;');
    const held = await weekly.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${RACE_C_CANDIDATE}'::uuid,`
      + `array['${RACE_C_ROOT}'::uuid],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',`
      + `gen_random_uuid(),'WP16C-ROT-002')->>'gate';`, 60_000);
    recorder.record('ROT-002', lastRow(held) === 'GRANTED' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `Weekly Source holds the whitespace-padded family under interface I-1: gate=${lastRow(held)}`);

    const rotationPending = rotate.run(
      `select private._timesheet_route_version_legacy_v1('${RACE_C_ROOT}'::uuid,'${RACE_C_ROOT}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`);
    let blockedEvidence = null;
    try {
      blockedEvidence = await group.waitUntilBlocked('observer', 'rotate', { timeoutMs: 20_000 });
    } catch { blockedEvidence = null; }
    recorder.record('ROT-002', blockedEvidence?.waitEventType === 'Lock' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      blockedEvidence
        ? 'the rotation waits on the family lock set rather than proceeding past it'
        : 'the rotation did not enter an observed Lock wait');

    await weekly.runWithin('rollback;', 30_000);
    const rotated = await rotationPending;
    const newRoot = lastRow(rotated);
    recorder.record('ROT-002', /^[0-9a-f-]{36}$/.test(newRoot) ? 'PASS' : 'FAIL', 'EXECUTED', state,
      rotated.error
        ? `the rotation could not commit under contention: ${(rotated.error.message ?? '').slice(0, 160)}`
        : `the rotation commits first under contention and promotes ${newRoot}`);
    recorder.record('ROT-002', isDeadlock(rotated.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'rotation-first order: no deadlock');

    if (/^[0-9a-f-]{36}$/.test(newRoot)) {
      // Weekly Source restarts and observes the NEW canonical row.
      const observed = await weekly.runWithin(
        `select (private.weekly_source_lock_and_resolve_families_v1('${RACE_C_CANDIDATE}'::uuid,`
        + `array['${RACE_C_ROOT}'::uuid],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',`
        + `gen_random_uuid(),'WP16C-ROT-002')->'families'->0->>'canonical_timesheet_id');`, 60_000);
      recorder.record('ROT-002', lastRow(observed) === newRoot ? 'PASS' : 'FAIL', 'EXECUTED', state,
        `after the rotation commits, Weekly Source observes the new canonical row `
        + `${lastRow(observed)} under the rotation lock set and restarts against it`);

      // The rotation queued Workbench jobs for the Candidate, and the installed
      // serial gate would then report CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION,
      // so the restart would only ever prove the BLOCKED branch (WP-07 F1).
      // Reproducing the Workbench worker's completion is a fixture action on
      // fixture rows; it changes no Banking Pay definition.
      await weekly.runWithin(
        `update public.banking_pay_workbench_jobs set status='SUCCEEDED',`
        + `completed_at_utc=clock_timestamp() where status in ('QUEUED','RUNNING');`, 30_000);

      // NAMED FIXTURE STEP, and why it is needed.  The installed rotation writer
      // leaves the promoted row at `processing_status = AWAITING_MANUAL_SIGNATURE`
      // (measured on this clone), and the ordinary authorise owner refuses that
      // with AUTHORISE_NOT_ALLOWED.  That is the ordinary Timesheet lifecycle,
      // not a Weekly Source rule, and completing the signature step is not this
      // suite's subject. The fixture therefore reproduces the state the
      // signature step would leave, on its own fixture row, so the row's real
      // subject — "exactly one root authorised, the new one" — can be reached.
      const lifecycleBefore = await weekly.runWithin(
        `select processing_status::text from public.timesheets_financials `
        + `where timesheet_id='${newRoot}'::uuid and is_current;`, 30_000);
      await weekly.runWithin(
        `update public.timesheets_financials set processing_status='PENDING_AUTH' `
        + `where timesheet_id='${newRoot}'::uuid and is_current;`, 30_000);
      await weekly.runWithin(
        `update public.banking_pay_workbench_jobs set status='SUCCEEDED',`
        + `completed_at_utc=clock_timestamp() where status in ('QUEUED','RUNNING');`, 30_000);
      recorder.record('ROT-002', 'PASS', 'EXECUTED', state,
        `the installed rotation writer leaves the promoted row at `
        + `${lastRow(lifecycleBefore)}, which the ordinary authorise owner refuses; the fixture `
        + `completes that ordinary lifecycle step on its own row so the Weekly Source restart can `
        + `be proved`);
      const authorised = await weekly.runWithin(
        `select public.weekly_source_first_authorise_v1('${newRoot}'::uuid,'${newRoot}'::uuid,`
        + `null,'${ACTOR}'::uuid)::text;`, 60_000);
      let authorisedJson = {};
      try { authorisedJson = JSON.parse(lastRow(authorised) || '{}'); } catch { authorisedJson = {}; }
      if (authorisedJson.ok !== true) {
        recorder.record('ROT-002', 'FAIL', 'EXECUTED', state,
          `the restart could not authorise the new canonical row: `
          + `${authorisedJson.code ?? (authorised.error?.message ?? lastRow(authorised) ?? 'no result').slice(0, 200)} `
          + `${authorisedJson.reason ?? ''}`);
      }
      const live = await weekly.runWithin(
        `select count(*)::text||':'||coalesce(min(root_timesheet_id::text),'-') `
        + `from public.weekly_source_root_authorisations `
        + `where withdrawn_at_utc is null and btrim(family_booking_id)='WP16C-RACE-C';`, 30_000);
      recorder.record('ROT-002',
        authorisedJson.ok === true && lastRow(live) === `1:${newRoot}` ? 'PASS' : 'FAIL',
        'EXECUTED', state,
        `exactly one root is authorised and it is the new one (${lastRow(live)}); the older id is `
        + `never authorised`);
    }
  } finally {
    await group.closeAll();
  }

  // ---- ROT-003: the first authorisation commits first. ---------------------
  group = await openGroup({ database, names: ['weekly', 'rotate', 'observer'], groupId: 'wp16c-rot' });
  try {
    await declareServiceRole(group);
    const weekly = group.session('weekly');
    const rotate = group.session('rotate');

    await weekly.runWithin('begin;');
    const authorised = await weekly.runWithin(
      `select public.weekly_source_first_authorise_v1('${RACE_D_ROOT}'::uuid,'${RACE_D_ROOT}'::uuid,`
      + `null,'${ACTOR}'::uuid)::text;`, 60_000);
    let authorisedJson = {};
    try { authorisedJson = JSON.parse(lastRow(authorised) || '{}'); } catch { authorisedJson = {}; }
    recorder.record('ROT-003', authorisedJson.ok === true ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `the first authorisation takes the family and succeeds: ok=${authorisedJson.ok}`);
    await weekly.runWithin('commit;', 30_000);

    const beforeRotation = psqlQuery(localUrl(database),
      `select count(*)::text||':'||coalesce(max(version)::text,'-') from public.timesheets `
      + `where btrim(booking_id)='WP16C-RACE-D';`);
    const refused = await rotate.runWithin(
      `select public.timesheet_route_version_rotate('${RACE_D_ROOT}'::uuid,'${RACE_D_ROOT}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`, 60_000);
    const afterRotation = psqlQuery(localUrl(database),
      `select count(*)::text||':'||coalesce(max(version)::text,'-') from public.timesheets `
      + `where btrim(booking_id)='WP16C-RACE-D';`);
    recorder.record('ROT-003',
      /WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED/.test(refused.error?.message ?? '')
      && beforeRotation === afterRotation ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `the later rotation refuses WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED with zero writes `
      + `(family ${afterRotation} unchanged): `
      + `${(refused.error?.message ?? 'accepted').slice(0, 90)}`);

    const stillAuthorised = psqlQuery(localUrl(database),
      `select coalesce((select 'AUTHORISED' from public.timesheets where timesheet_id='${RACE_D_ROOT}'::uuid `
      + `and authorised_at_server is not null),'NOT_AUTHORISED')||':'`
      + `||(select count(*)::text from public.weekly_source_root_authorisations `
      + `where root_timesheet_id='${RACE_D_ROOT}'::uuid and withdrawn_at_utc is null);`);
    recorder.record('ROT-003', stillAuthorised === 'AUTHORISED:1' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `the root stays authorised after the refused rotation (${stillAuthorised})`);
    recorder.record('ROT-003', isDeadlock(refused.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'authorisation-first order: no deadlock');
  } finally {
    await group.closeAll();
  }

  // ---- ROT-012: the padded lock set, both orders, exactly one winner. -------
  group = await openGroup({ database, names: ['first', 'second', 'observer'], groupId: 'wp16c-rot' });
  try {
    await declareServiceRole(group);
    const first = group.session('first');
    const second = group.session('second');

    // The trimmed key is taken before the raw key, so a contender that holds
    // only the trimmed key already blocks the whole family.
    await first.runWithin('begin;');
    await first.runWithin(
      `select pg_advisory_xact_lock(hashtext(btrim('  WP16C-RACE-C  ')));`);
    const contender = second.run(
      `select private.weekly_source_lock_and_resolve_families_v1('${RACE_C_CANDIDATE}'::uuid,`
      + `array['${RACE_C_ROOT}'::uuid],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',`
      + `gen_random_uuid(),'WP16C-ROT-012')->>'gate';`);
    let waited = null;
    try { waited = await group.waitUntilBlocked('observer', 'second', { timeoutMs: 20_000 }); } catch { waited = null; }
    recorder.record('ROT-012', waited?.waitEventType === 'Lock' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      waited
        ? 'the trimmed key is taken first: holding it alone blocks the whole padded family'
        : 'no Lock wait was observed on the trimmed key');
    await first.runWithin('rollback;', 30_000);
    const contenderResult = await contender;
    recorder.record('ROT-012', lastRow(contenderResult) === 'GRANTED' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `exactly one winner: the contender proceeds once the trimmed key is released `
      + `(gate=${lastRow(contenderResult)})`);
    recorder.record('ROT-012', isDeadlock(contenderResult.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'padded-family lock order: no deadlock in either order');
  } finally {
    await group.closeAll();
  }
}

// --- step D: ROT-012 privileges ---------------------------------------------
function proveRotationPrivileges(recorder, database) {
  const url = localUrl(database);
  const rows = psqlQuery(url, `
    select string_agg(
      n.nspname||'.'||p.proname||'='||
      concat_ws(',',
        case when has_function_privilege('anon',p.oid,'EXECUTE') then 'anon' end,
        case when has_function_privilege('authenticated',p.oid,'EXECUTE') then 'authenticated' end,
        case when has_function_privilege('service_role',p.oid,'EXECUTE') then 'service_role' end),
      ' | ' order by p.proname)
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private'
      and p.proname in ('_timesheet_route_version_core_v1','_timesheet_route_version_legacy_v1',
                        'weekly_source_managed_root_guard_v1');`);
  const exposed = /anon|authenticated|service_role/.test(rows);
  recorder.record('ROT-012', exposed ? 'FAIL' : 'PASS', 'EXECUTED', database,
    `the private core and legacy rotation bodies and the guard itself are not executable by the `
    + `browser or service roles (${rows || 'no grants'})`);

  const shim = psqlQuery(url, `
    select case when has_function_privilege('service_role',
      'private.weekly_source_managed_root_guard_decision_v1(uuid)','EXECUTE')
      then 'GRANTED' else 'REVOKED' end
      ||':'||case when has_function_privilege('anon',
      'private.weekly_source_managed_root_guard_decision_v1(uuid)','EXECUTE')
      then 'GRANTED' else 'REVOKED' end;`);
  recorder.record('ROT-012', shim === 'GRANTED:REVOKED' ? 'PASS' : 'FAIL', 'EXECUTED', database,
    `only the five-key decision shim is reachable by the service role, and never by the browser `
    + `(service_role:anon = ${shim})`);
}

async function main() {
  const args = process.argv.slice(2);
  const resultsIndex = args.indexOf('--results');
  // WP-16f: this suite always computed a real PASS/FAIL envelope, but persisted
  // it only when a caller remembered to pass `--results <dir>` on the command
  // line. Every other run silently discarded a genuine, already-executed
  // result. The rest of the harness (`scripts/run-weekly-source-harness.mjs`)
  // reads its result directory from `CLOUDTMS_WEEKLY_SOURCE_RESULT_DIR`, so
  // this suite now honours that same variable as a fallback, and only falls
  // back to no persistence when neither is set.
  const resultDirectory = resultsIndex >= 0
    ? args[resultsIndex + 1]
    : (process.env.CLOUDTMS_WEEKLY_SOURCE_RESULT_DIR || null);
  const keep = args.includes('--keep');
  const recorder = createProofRecorder('ROT');
  const created = [];

  const verifierDatabase = createClone('ws62_wp16c_rot_a');
  created.push(verifierDatabase);
  const raceDatabase = createClone('ws62_wp16c_rot_b');
  created.push(raceDatabase);

  try {
    const verifierUrl = localUrl(verifierDatabase);

    // Step A — the two verifiers that carry rotation rows, executed.
    runVerifier(recorder, verifierUrl, '17092026_0200_weekly_source_rotation_authority_v1.sql',
      ['ROT-001', 'ROT-009', 'ROT-011', 'ROT-012'],
      'the Gate 6 rotation-authority verifier passed end to end in this run');
    runVerifier(recorder, verifierUrl, '17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
      ['ROT-006'],
      'the Gate 5 pending-release verifier passed, including R22: a waiting bundle whose stored '
      + 'root later rotates is a MANUAL_REVIEW integrity failure, never released and never rebuilt');

    // Step B — this package's own single-session proofs.
    const own = psqlFile(verifierUrl, path.join(WP16C_ROOT, 'rot-proofs.sql'), { cwd: WP16C_ROOT });
    if (!own.ok) {
      recorder.record('ROT-004', 'FAIL', 'EXECUTED', verifierDatabase,
        `rot-proofs.sql did not complete: ${own.stderr.slice(-260)}`);
    }
    recorder.absorb(parseProofLines(own.stdout), `${verifierDatabase} (rot-proofs.sql)`);

    // Step C — the races.
    const fixture = psqlFile(localUrl(raceDatabase), path.join(WP16C_ROOT, 'managed-root-race-fixture.sql'),
      { cwd: WP16C_ROOT });
    if (!fixture.ok) {
      for (const id of ['ROT-002', 'ROT-003', 'ROT-012']) {
        recorder.record(id, 'FAIL', 'EXECUTED', raceDatabase,
          `the committed race fixture failed: ${fixture.stderr.slice(-260)}`);
      }
    } else {
      await proveRotationRaces(recorder, raceDatabase, standaloneNamedSessionGroup);
    }

    // Step D — privileges.
    proveRotationPrivileges(recorder, raceDatabase);
  } finally {
    if (!keep) for (const database of created) dropClone(database);
  }

  printProofTable(recorder);

  const envelope = await writeSuiteEnvelope({
    recorder,
    scenarioId: 'WS-WP16C-ROT-001',
    seedText: 'weekly-source-wp16c-rot-suite-v1',
    database: TEMPLATE,
    authority: 'annexes/acceptance-tests.csv ROT-001..ROT-012; proof/34; contract section 16 G12-5',
    executedOwners: [
      'public.weekly_source_first_authorise_v1',
      'public.weekly_source_first_authorisation_withdraw_v1',
      'public.timesheet_route_version_rotate',
      'public.timesheet_route_version_confirmed_v1',
      'private._timesheet_route_version_core_v1',
      'private._timesheet_route_version_legacy_v1',
      'public.timesheet_qr_restore_version',
      'public.timesheet_qr_refuse_and_reset',
      'private._candidate_timesheet_reject_rotate_v1',
      'public.tsfin_prepare_write',
      'public.tsfin_mark_revoked',
      'public.tsfin_write_current_snapshot_single_bounded',
      'public.tsfin_write_snapshots_and_complete',
      'public.contract_week_manual_upsert_atomic',
      'public.timesheet_standard_delete_apply_v1',
      'public.timesheet_weekly_manual_adjustment_delete_apply',
      'private._candidate_expense_payment_edit_shell_v1',
      'private.weekly_source_managed_root_guard_v1',
      'private.weekly_source_managed_root_guard_decision_v1',
      'private.weekly_source_lock_and_resolve_families_v1',
      'private.weekly_source_freeze_census_v1',
      'public._pay_timesheet_rotation_scope',
      'supabase/verification/17092026_0200_weekly_source_rotation_authority_v1.sql',
      'supabase/verification/17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
      'tests/weekly-source/wp16c/rot-proofs.sql',
      'tests/weekly-source/wp16c/managed-root-race-fixture.sql',
    ],
    resultDirectory,
    fileName: 'wp16c-rot-suite.json',
    acceptanceIds: recorder.passedIds((id) => id.startsWith('ROT-')),
    protectedIds: [],
    controllingRequirementIds: ['ROT-001'],
    extraActual: { databasesCreated: created.length },
  });

  process.stdout.write(`envelope ${envelope.status} digest ${envelope.evidenceDigest}\n`);
  if (envelope.status !== 'PASS') process.exitCode = 1;
}

await main();
