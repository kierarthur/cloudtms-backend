// Weekly Source Plan 6.2 — Gate 12 executed proof suite: UNA-001..UNA-019.
// WP-16c. Contract section 16, build item G12-5; runnability order G12-7 puts
// this suite first because it needs the least new ground.
//
//   node tests/weekly-source/wp16c/una-suite.mjs [--results <dir>] [--keep]
//
// Environment: PSQL_BIN, PGPASSWORD (local throwaway), PGOPTIONS='-c jit=off'.
// In Git Bash also export MSYS_NO_PATHCONV=1.
//
// FIVE STEPS, each of which EXECUTES the owner it is about (Part 1 rule 1):
//
//   A  the Gate 3 verifier is RUN, not cited. It drives `UNA-001..UNA-012`,
//      `UNA-016`, `UNA-017` and `UNA-018` through the real installed owners in
//      one rolled-back transaction. Part 1 rule 6 forbids claiming a verifier
//      result you did not get, so the suite gets it.
//   B  `una-proofs.sql`, this package's own single-session proofs: an
//      independent end-to-end lifecycle (`UNA-001`, `UNA-011`), the direct-call
//      and availability half of `UNA-010`, `UNA-012`'s ordinary-owner
//      differential, `UNA-013`'s DEC-061 invoice immutability, and `UNA-018`'s
//      replay and old-physical-id limbs.
//   C  the Gate 9 read-projection verifier is RUN for `UI-022`, which is
//      `UNA-013`'s remaining clause: the Office heading
//      "Not authorised for pay - invoiced from source" after a withdrawal on a
//      week invoiced from source.
//   D  `UNA-014`, two and three named connections against a COMMITTED fixture:
//      withdrawal against a Workbench source build holding the Candidate serial
//      key, withdrawal against Draft preparation, and withdrawal against a
//      concurrent rotation on a whitespace-padded rotated family, in both
//      orders, with a deadlock check on each.
//   E  `UNA-015` and `UNA-019` against WP-16a's real-owner Banking Pay
//      fixtures, which need several transactions and so cannot live in a
//      rolled-back verifier.
//
// Every outcome is recorded with the database state that produced it. A refusal
// that the pack says should be an allowance is recorded as FAIL and attributed
// in the report; it is never quietly re-expected.

import { mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  BACKEND_ROOT, WP16C_ROOT, createProofRecorder, localUrl, parseProofLines,
  printProofTable, psqlFile, psqlQuery, psqlTry, standaloneNamedSessionGroup,
  writeSuiteEnvelope,
} from './proof-runner.mjs';

const TEMPLATE = process.env.WP16C_TEMPLATE ?? 'ws62_wp16c_template';
const ACTOR = 'd6000000-0000-4000-8000-000000000001';
const CANDIDATE_A = 'd6000000-0000-4000-8000-000000000101';
const ROOT_A = 'd6000000-0000-4000-8000-000000000301';
const ROOT_B = 'd6000000-0000-4000-8000-000000000302';
const VERIFICATION = path.join(BACKEND_ROOT, 'supabase', 'verification');
const FIXTURES_BANKING = path.join(BACKEND_ROOT, 'tests', 'weekly-source', 'fixtures-banking');

function adminUrl() { return localUrl('postgres'); }

/**
 * Every Weekly Source public owner raises WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED
 * for any other caller, so a named session that has not declared its role only
 * ever proves the role guard.
 */
async function declareServiceRole(group) {
  for (const name of group.names) {
    await group.session(name).runWithin(`set session "request.jwt.claim.role" = 'service_role';`, 30_000);
  }
}

// CREATE/DROP DATABASE cannot run inside a transaction block, and a statement
// carrying the service-role declaration is one implicit transaction, so clone
// management is the one place that asks for no role prefix.
const CLUSTER = { asServiceRole: false };

function createClone(name) {
  psqlQuery(adminUrl(), `drop database if exists ${name} with (force);`, CLUSTER);
  psqlQuery(adminUrl(), `create database ${name} template ${TEMPLATE};`, CLUSTER);
  psqlQuery(adminUrl(), `alter database ${name} set jit = off;`, CLUSTER);
  return name;
}

function dropClone(name) {
  try { psqlQuery(adminUrl(), `drop database if exists ${name} with (force);`, CLUSTER); } catch { /* best effort */ }
}

function signatureOf(url, timesheetId) {
  return psqlQuery(url, `select nullif(btrim(coalesce(
      signature->>'backend_row_signature', signature->>'row_signature','')),'')
    from public.timesheet_lifecycle_guard_signature_v1('${timesheetId}'::uuid,
      (select id from public.contract_weeks where timesheet_id='${timesheetId}'::uuid), false) as signature;`);
}

// --- step A / C: run another package's verifier and get its real result ------
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
    recorder.record(
      id,
      outcome.ok ? 'PASS' : 'FAIL',
      'EXECUTED',
      `clone + supabase/verification/${fileName}`,
      outcome.ok ? note : `verifier failed: ${firstError(outcome.stderr)}`,
    );
  }
  return outcome;
}

// --- step D: UNA-014 ---------------------------------------------------------
const SERIAL_KEY_SQL = (candidate) =>
  `pg_advisory_xact_lock(hashtextextended(public._pay_workbench_candidate_serial_key('${candidate}'::uuid), 24062027))`;

function isDeadlock(error) {
  return Boolean(error) && (error.sqlstate === '40P01' || /deadlock detected/i.test(error.message ?? ''));
}

function lastRow(result) {
  return (result.rows ?? []).filter(Boolean).at(-1) ?? '';
}

async function proveRaces(recorder, database, openGroup) {
  const url = localUrl(database);
  const state = `${database} (committed race fixture)`;
  const signatureA = signatureOf(url, ROOT_A);

  // ---- D1: the Workbench source build holds the Candidate serial key. -------
  let group = await openGroup({ database, names: ['builder', 'office', 'observer'], groupId: 'wp16c-una' });
  try {
    await declareServiceRole(group);
    const builder = group.session('builder');
    const office = group.session('office');
    await builder.runWithin('begin;');
    await builder.runWithin(`select ${SERIAL_KEY_SQL(CANDIDATE_A)};`);

    const blocked = await office.runWithin(
      `select public.weekly_source_first_authorisation_withdraw_v1('${ROOT_A}'::uuid,'${ROOT_A}'::uuid,`
      + `'${signatureA}','${ACTOR}'::uuid)::text;`);
    const blockedJson = JSON.parse(lastRow(blocked) || '{}');
    recorder.record('UNA-014', blockedJson.code === 'WEEKLY_SOURCE_CANDIDATE_BUSY' && blockedJson.retryable === true
      ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `while a Workbench source build holds the Candidate serial key the withdrawal is refused `
      + `${blockedJson.code ?? 'NONE'} with retryable=${blockedJson.retryable}`);

    const liveWhileBlocked = await office.runWithin(
      `select count(*)::text from public.weekly_source_root_authorisations `
      + `where root_timesheet_id='${ROOT_A}'::uuid and withdrawn_at_utc is null;`);
    recorder.record('UNA-014', lastRow(liveWhileBlocked) === '1' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      'the refused withdrawal wrote nothing: the live generation is still there');

    await builder.runWithin('commit;');

    // Exactly one winner: the retry now succeeds against the freed key.
    const winner = await office.runWithin(
      `select public.weekly_source_first_authorisation_withdraw_v1('${ROOT_A}'::uuid,'${ROOT_A}'::uuid,`
      + `'${signatureA}','${ACTOR}'::uuid)::text;`);
    const winnerJson = JSON.parse(lastRow(winner) || '{}');
    recorder.record('UNA-014', winnerJson.withdrawn === true ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `after the source build commits the retry wins: withdrawn=${winnerJson.withdrawn}, `
      + `replayed=${winnerJson.replayed}`);

    // The withdrawal committed first, so the root can never be admitted: it is
    // no longer authorised and the owner itself reports the control gone.
    const afterState = await office.runWithin(
      `select coalesce((select 'AUTHORISED' from public.timesheets `
      + `where timesheet_id='${ROOT_A}'::uuid and authorised_at_server is not null),'NOT_AUTHORISED')`
      + `||':'||coalesce(public.weekly_source_first_authorisation_withdraw_available_v1('${ROOT_A}'::uuid)->>'code','NONE');`);
    recorder.record('UNA-014', lastRow(afterState) === 'NOT_AUTHORISED:WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT'
      ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `once the withdrawal has committed the root is never admitted again: ${lastRow(afterState)}`);
    recorder.record('UNA-014', isDeadlock(blocked.error) || isDeadlock(winner.error) ? 'FAIL' : 'PASS',
      'EXECUTED', state, 'serial-gate race: no deadlock in either direction');
  } finally {
    await group.closeAll();
  }

  // ---- D2: Draft preparation wins the race; the withdrawal refuses. ---------
  group = await openGroup({ database, names: ['banking', 'office', 'observer'], groupId: 'wp16c-una' });
  try {
    await declareServiceRole(group);
    const banking = group.session('banking');
    const office = group.session('office');
    const signatureB = signatureOf(url, ROOT_B);

    // The Draft is a FIXTURE in the existing evidence tables, in exactly the
    // column shape the Gate 3 verifier's own `seed_batch` helper uses: Create
    // Draft is on contract section 2's do-not-touch list and decision D2 puts
    // Banking Pay's new logic out of scope.
    await banking.runWithin('begin;');
    const draftInsert = await banking.runWithin(
      `insert into public.pay_batches(id,pay_date,status,banking_system_snapshot,`
      + `external_paye_system_snapshot,rail_provider_snapshot,rail_env_snapshot,execution_commit_state)`
      + ` values ('d6000000-0000-4000-8000-000000000f01'::uuid,'2026-09-18','DRAFT','REVOLUT_API','SAGE',`
      + `'REVOLUT','SANDBOX','NOT_SUBMITTED');`
      + `insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id)`
      + ` values ('d6000000-0000-4000-8000-000000000f02'::uuid,'d6000000-0000-4000-8000-000000000f01'::uuid,`
      + `'d6000000-0000-4000-8000-000000000102'::uuid);`
      + `insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,pay_channel,timesheet_id,`
      + `is_voided,amount_ex_vat,amount_inc_vat)`
      + ` values ('d6000000-0000-4000-8000-000000000f03'::uuid,'d6000000-0000-4000-8000-000000000f02'::uuid,`
      + `'TIMESHEET_PAYMENT','PAYE','${ROOT_B}'::uuid,false,100,100);`);
    recorder.record('UNA-014', draftInsert.error ? 'FAIL' : 'PASS', 'EXECUTED', state,
      draftInsert.error
        ? `the Draft fixture could not be seeded: ${(draftInsert.error.message ?? '').slice(0, 160)}`
        : 'a Draft holding a live family item is prepared in the rival session');

    // The withdrawal starts while the Draft is still uncommitted: it must not
    // see it (no dirty read) and must not be able to commit past it either.
    const beforeCommit = await office.runWithin(
      `select coalesce(public.weekly_source_first_authorisation_withdraw_available_v1('${ROOT_B}'::uuid)->>'available','?');`);
    recorder.record('UNA-014', lastRow(beforeCommit) === 'true' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `an uncommitted Draft is invisible to the other session: available=${lastRow(beforeCommit)}`);

    await banking.runWithin('commit;');

    const refused = await office.runWithin(
      `select public.weekly_source_first_authorisation_withdraw_v1('${ROOT_B}'::uuid,'${ROOT_B}'::uuid,`
      + `'${signatureB}','${ACTOR}'::uuid)::text;`);
    const refusedJson = JSON.parse(lastRow(refused) || '{}');
    recorder.record('UNA-014', refusedJson.code === 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE' ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `the Draft exists first, so the withdrawal refuses ${refusedJson.code ?? 'NONE'} `
      + `(retryable=${refusedJson.retryable})`);

    const draftUntouched = await office.runWithin(
      `select count(*)::text from public.pay_batch_items where id='d6000000-0000-4000-8000-000000000f03'::uuid `
      + `and is_voided=false and amount_ex_vat=100;`);
    recorder.record('UNA-014', lastRow(draftUntouched) === '1' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      'the Draft is untouched by the refused withdrawal');
    recorder.record('UNA-014', isDeadlock(refused.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'Draft race: no deadlock');

    // Clean the Draft away again so the rotation race starts from a clean root.
    await office.runWithin(`delete from public.pay_batch_items where id='d6000000-0000-4000-8000-000000000f03'::uuid;`);
    await office.runWithin(`delete from public.pay_batch_candidates where id='d6000000-0000-4000-8000-000000000f02'::uuid;`);
    await office.runWithin(`delete from public.pay_batches where id='d6000000-0000-4000-8000-000000000f01'::uuid;`);
  } finally {
    await group.closeAll();
  }

  // ---- D3: withdrawal against a concurrent rotation, both orders. -----------
  group = await openGroup({ database, names: ['weekly', 'rotate', 'observer'], groupId: 'wp16c-una' });
  try {
    await declareServiceRole(group);
    const weekly = group.session('weekly');
    const rotate = group.session('rotate');

    // Order 1: the withdrawal holds the family, the rotation dispatcher is
    // refused by WP-09's managed-root guard before it takes any lock.
    await weekly.runWithin('begin;');
    const gate = await weekly.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('d6000000-0000-4000-8000-000000000102'::uuid,`
      + `array['${ROOT_B}'::uuid],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',`
      + `gen_random_uuid(),'WP16C-UNA-014')->>'gate';`);
    recorder.record('UNA-014', lastRow(gate) === 'GRANTED' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `the withdrawal path holds the padded family under interface I-1: gate=${lastRow(gate)}`);

    const guarded = await rotate.runWithin(
      `select public.timesheet_route_version_rotate('${ROOT_B}'::uuid,'${ROOT_B}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`, 30_000);
    recorder.record('UNA-014',
      /WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED/.test(guarded.error?.message ?? '') ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `a concurrent rotation on the managed family is refused by the guard before any lock: `
      + `${(guarded.error?.message ?? 'accepted').slice(0, 120)}`);
    recorder.record('UNA-014', isDeadlock(guarded.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'rotation-against-withdrawal, order 1: no deadlock');
    await weekly.runWithin('rollback;');

    // Order 2: the rotation commits first through the installed legacy body the
    // dispatcher itself calls (the shape census section 3.2's import owners
    // reach), and the withdrawal then refuses as stale rather than acting on a
    // superseded row.
    const rotated = await rotate.runWithin(
      `select private._timesheet_route_version_legacy_v1('${ROOT_B}'::uuid,'${ROOT_B}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`, 60_000);
    const newRoot = lastRow(rotated);
    recorder.record('UNA-014', /^[0-9a-f-]{36}$/.test(newRoot) ? 'PASS' : 'FAIL', 'EXECUTED', state,
      rotated.error
        ? `the rotation could not commit first: ${(rotated.error.message ?? '').slice(0, 160)}`
        : `the rotation commits first and promotes ${newRoot}`);

    if (/^[0-9a-f-]{36}$/.test(newRoot)) {
      const signatureStale = signatureOf(url, ROOT_B);
      const stale = await weekly.runWithin(
        `select public.weekly_source_first_authorisation_withdraw_v1('${ROOT_B}'::uuid,'${ROOT_B}'::uuid,`
        + `'${signatureStale}','${ACTOR}'::uuid)::text;`, 60_000);
      const staleJson = JSON.parse(lastRow(stale) || '{}');
      const staleReason = staleJson.reason
        ?? (staleJson.failed_checks ?? []).flatMap((check) => check.reasons ?? []).join(',')
        ?? 'n/a';
      recorder.record('UNA-014', staleJson.ok === false ? 'PASS' : 'FAIL', 'EXECUTED', state,
        `the later withdrawal refuses as stale rather than acting on the superseded row: `
        + `${staleJson.code ?? 'NONE'} / ${staleReason || 'n/a'}`);
      recorder.record('UNA-014', isDeadlock(stale.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
        'rotation-against-withdrawal, order 2: no deadlock');
    }
  } finally {
    await group.closeAll();
  }
}

// --- step E: UNA-015 and UNA-019, authorised first, then cancelled ----------
//
// WP-16a's 30 named states build Banking Pay evidence on roots that were never
// first-authorised, because the library deliberately contains no Weekly Source
// objects. These two rows need the opposite order, so
// `una-cancellation-fixture.sql` builds its own world, authorises it through the
// REAL installed Weekly Source owner, and only then drives WP-16a's own REAL
// cancellation helpers against it.
const UNA015_ROOT = 'e6000000-0000-4000-8000-000000000301';
const UNA019_ROOT = 'e6000000-0000-4000-8000-000000000302';
const UNA019_REMAINDER_ROOT = 'e6000000-0000-4000-8000-000000000303';

function withdrawalVerdict(url, root, actor) {
  const signature = signatureOf(url, root);
  const raw = psqlQuery(url,
    `select public.weekly_source_first_authorisation_withdraw_v1('${root}'::uuid,'${root}'::uuid,`
    + `'${signature}','${actor}'::uuid)::text;`);
  try { return JSON.parse(raw || '{}'); } catch { return {}; }
}

function failedChecks(verdict) {
  return (verdict.failed_checks ?? [])
    .map((check) => `${check.check}:${(check.reasons ?? []).join('/')}`)
    .join(' ');
}

/** Everything Weekly Source must not have written, as one comparable string. */
function bankingFootprint(url, root) {
  return psqlQuery(url, `
    select (select count(*) from public.pay_batch_items item where item.timesheet_id='${root}'::uuid)::text
      ||'/'||(select count(*) from public.pay_batch_items item
              where item.timesheet_id='${root}'::uuid and item.is_voided)::text
      ||'/'||(select count(*) from public.timesheet_pay_state_history history
              where history.timesheet_id='${root}'::uuid)::text
      ||'/'||(select count(*) from public.pay_bank_transfers)::text
      ||'/'||(select count(*) from public.ts_pay_adjustments adjustment
              where adjustment.timesheet_id='${root}'::uuid)::text;`);
}

function proveFixtureWithdrawals(recorder, database) {
  const url = localUrl(database);
  const install = psqlFile(url, path.join(FIXTURES_BANKING, 'install.sql'), { cwd: FIXTURES_BANKING });
  if (!install.ok) {
    for (const id of ['UNA-015', 'UNA-019']) {
      recorder.record(id, 'FAIL', 'ENVIRONMENT', database,
        `WP-16a fixture install failed: ${install.stderr.slice(-200)}`);
    }
    return;
  }
  const fixture = psqlFile(url, path.join(WP16C_ROOT, 'una-cancellation-fixture.sql'), { cwd: WP16C_ROOT });
  if (!fixture.ok) {
    for (const id of ['UNA-015', 'UNA-019']) {
      recorder.record(id, 'FAIL', 'EXECUTED', database,
        `the cancellation fixture failed: ${fixture.stderr.slice(-260)}`);
    }
    return;
  }
  const actor = psqlQuery(url, `select ws_banking_fixture.base_world_v1()->>'actor_user_id';`);

  // Shape first, so a later verdict can never be read as evidence of a state
  // the fixture did not actually reach.
  const shape = psqlQuery(url, `
    select (select status::text from public.pay_batches
             where id=ws_banking_fixture.fid('wp16c_una015:batch'))
      ||'|'||(select status::text from public.pay_batches
               where id=ws_banking_fixture.fid('wp16c_una019:batch'))
      ||'|'||(select is_voided::text from public.pay_batch_items
               where id=ws_banking_fixture.fid('wp16c_una015:item:1'))
      ||'|'||(select is_voided::text from public.pay_batch_items
               where id=ws_banking_fixture.fid('wp16c_una019:item:1'))
      ||'|'||(select is_voided::text from public.pay_batch_items
               where id=ws_banking_fixture.fid('wp16c_una019:item:2'));`);
  const shapeParts = shape.split('|');
  recorder.record('UNA-015',
    shapeParts[0] === 'CANCELLED' && shapeParts[2] === 'true' ? 'PASS' : 'FAIL',
    'EXECUTED', `${database}/wp16c_una015`,
    `the real installed correction chain cancelled the whole batch and voided the family item `
    + `(batch/batch/items ${shape})`);
  recorder.record('UNA-019',
    shapeParts[1] === 'DRAFT' && shapeParts[3] === 'true' && shapeParts[4] === 'false' ? 'PASS' : 'FAIL',
    'EXECUTED', `${database}/wp16c_una019`,
    `one Candidate is cancelled out of the two-Candidate Draft and the remainder item stays live `
    + `(batch/batch/items ${shape})`);

  // UNA-015 — a complete, terminal-no-money whole-batch cancellation.
  const beforeA = bankingFootprint(url, UNA015_ROOT);
  const verdictA = withdrawalVerdict(url, UNA015_ROOT, actor);
  const afterA = bankingFootprint(url, UNA015_ROOT);
  recorder.record('UNA-015', verdictA.withdrawn === true && beforeA === afterA ? 'PASS' : 'FAIL',
    'EXECUTED', `${database}/wp16c_una015`,
    verdictA.withdrawn === true
      ? `withdrawal allowed after a complete Binding A cancellation, and Weekly Source wrote `
        + `nothing in Banking Pay (items/voided/history/transfers/adjustments ${afterA} unchanged)`
      : `withdrawal refused ${verdictA.code ?? 'NONE'} (${failedChecks(verdictA)})`);

  // UNA-019 first half — the remainder Draft is still alive.
  const beforeB = bankingFootprint(url, UNA019_ROOT);
  const verdictB = withdrawalVerdict(url, UNA019_ROOT, actor);
  const afterB = bankingFootprint(url, UNA019_ROOT);
  recorder.record('UNA-019', verdictB.withdrawn === true && beforeB === afterB ? 'PASS' : 'FAIL',
    'EXECUTED', `${database}/wp16c_una019 (remainder alive)`,
    verdictB.withdrawn === true
      ? `withdrawal allowed while the remainder Draft lives: Binding A proves the family's voided `
        + `item whatever the remainder batch's status (footprint ${afterB} unchanged)`
      : `withdrawal refused ${verdictB.code ?? 'NONE'} (${failedChecks(verdictB)})`);

  // UNA-019 second half — the remainder pays the other Candidate, and this root
  // is asked again. Another Candidate's settlement in the same batch is never
  // this root's evidence.
  psqlTry(url, `delete from public.weekly_source_root_authorisations where root_timesheet_id='${UNA019_ROOT}'::uuid;`);
  psqlTry(url, `update public.banking_pay_workbench_jobs set status='SUCCEEDED',
    completed_at_utc=clock_timestamp() where status in ('QUEUED','RUNNING');`);
  const reauthorise = psqlTry(url,
    `select public.weekly_source_first_authorise_v1('${UNA019_ROOT}'::uuid,'${UNA019_ROOT}'::uuid,`
    + `null,'${actor}'::uuid)::text;`);
  psqlTry(url, `update public.banking_pay_workbench_jobs set status='SUCCEEDED',
    completed_at_utc=clock_timestamp() where status in ('QUEUED','RUNNING');`);
  // Only the Candidates the rail actually pays are marked settled. This
  // Candidate's single item is voided, so its `pay_batch_candidates` row stays
  // unsettled — which is exactly what makes "another Candidate's settlement in
  // the same batch is never this root's evidence" a real test rather than a
  // tautology. WP-16a's own `finish_multi_draft_remainder_settled_v1` shapes it
  // the same way.
  const settle = psqlTry(url, `select ws_banking_fixture.apply_settlement_evidence_v1(
    'wp16c_una019_remainder', ws_banking_fixture.fid('wp16c_una019:batch'),
    jsonb_build_object(
      'position','BASE',
      'batch_status','SETTLED',
      'candidate_outcomes', jsonb_build_object(
        ws_banking_fixture.fid('wp16c_una019:batch_candidate:1')::text, 'null',
        ws_banking_fixture.fid('wp16c_una019:batch_candidate:2')::text, 'SETTLED')))::text;`);
  psqlTry(url, `update public.banking_pay_workbench_jobs set status='SUCCEEDED',
    completed_at_utc=clock_timestamp() where status in ('QUEUED','RUNNING');`);
  const remainderState = psqlQuery(url, `
    select (select status::text from public.pay_batches
             where id=ws_banking_fixture.fid('wp16c_una019:batch'))
      ||'|'||(select count(*)::text from public.timesheet_pay_state_history history
              where history.timesheet_id='${UNA019_REMAINDER_ROOT}'::uuid)
      ||'|'||(select count(*)::text from public.timesheet_pay_state_history history
              where history.timesheet_id='${UNA019_ROOT}'::uuid);`);

  if (!reauthorise.ok || !settle.ok) {
    recorder.record('UNA-019', 'FAIL', 'EXECUTED', `${database}/wp16c_una019 (remainder settled)`,
      `could not reach the second half: ${(reauthorise.stderr || settle.stderr).slice(-200)}`);
    return;
  }
  const beforeC = bankingFootprint(url, UNA019_ROOT);
  const verdictC = withdrawalVerdict(url, UNA019_ROOT, actor);
  const afterC = bankingFootprint(url, UNA019_ROOT);
  recorder.record('UNA-019', verdictC.withdrawn === true && beforeC === afterC ? 'PASS' : 'FAIL',
    'EXECUTED', `${database}/wp16c_una019 (remainder settled)`,
    verdictC.withdrawn === true
      ? `withdrawal allowed again after the remainder paid the other Candidate: another Candidate's `
        + `settlement in the same batch is never this root's evidence `
        + `(batch/other-history/own-history ${remainderState}; footprint ${afterC} unchanged)`
      : `withdrawal refused ${verdictC.code ?? 'NONE'} after the remainder settled `
        + `(${failedChecks(verdictC)})`);
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
  const recorder = createProofRecorder('UNA');
  const created = [];

  const verifierDatabase = createClone('ws62_wp16c_una_a');
  created.push(verifierDatabase);
  const raceDatabase = createClone('ws62_wp16c_una_b');
  created.push(raceDatabase);
  const fixtureDatabase = createClone('ws62_wp16c_una_c');
  created.push(fixtureDatabase);

  try {
    const verifierUrl = localUrl(verifierDatabase);

    // Step A — the Gate 3 verifier, executed.
    runVerifier(recorder, verifierUrl, '17092026_0600_weekly_source_first_authorisation_v1.sql',
      ['UNA-001', 'UNA-002', 'UNA-003', 'UNA-004', 'UNA-005', 'UNA-006', 'UNA-007', 'UNA-008',
        'UNA-009', 'UNA-010', 'UNA-011', 'UNA-012', 'UNA-016', 'UNA-017', 'UNA-018'],
      'the Gate 3 first-authorisation and withdrawal verifier passed end to end in this run');

    // Step B — this package's own proofs.
    const own = psqlFile(verifierUrl, path.join(WP16C_ROOT, 'una-proofs.sql'), { cwd: WP16C_ROOT });
    if (!own.ok) {
      recorder.record('UNA-001', 'FAIL', 'EXECUTED', verifierDatabase,
        `una-proofs.sql did not complete: ${own.stderr.slice(-260)}`);
    }
    recorder.absorb(parseProofLines(own.stdout), `${verifierDatabase} (una-proofs.sql)`);

    // Step C — UI-022, the remaining clause of UNA-013.
    runVerifier(recorder, verifierUrl, '15092026_1534_weekly_source_read_projections_v1.sql',
      ['UNA-013'],
      'UI-022 executed: after a withdrawal on a week invoiced from source the Office Weekly detail '
      + 'and Simple Timesheet render the DEC-061 Option A heading');

    // Step D — UNA-014.
    const fixture = psqlFile(localUrl(raceDatabase), path.join(WP16C_ROOT, 'managed-root-race-fixture.sql'), { cwd: WP16C_ROOT });
    if (!fixture.ok) {
      recorder.record('UNA-014', 'FAIL', 'EXECUTED', raceDatabase,
        `the committed race fixture failed: ${fixture.stderr.slice(-260)}`);
    } else {
      await proveRaces(recorder, raceDatabase, standaloneNamedSessionGroup);
    }

    // Step E — UNA-015 and UNA-019.
    proveFixtureWithdrawals(recorder, fixtureDatabase);
  } finally {
    if (!keep) for (const database of created) dropClone(database);
  }

  printProofTable(recorder);

  const envelope = await writeSuiteEnvelope({
    recorder,
    scenarioId: 'WS-WP16C-UNA-001',
    seedText: 'weekly-source-wp16c-una-suite-v1',
    database: TEMPLATE,
    authority: 'annexes/acceptance-tests.csv UNA-001..UNA-019; proof/36; contract section 16 G12-5',
    executedOwners: [
      'public.weekly_source_first_authorise_v1',
      'public.weekly_source_first_authorisation_withdraw_v1',
      'public.weekly_source_first_authorisation_withdraw_available_v1',
      'public.timesheet_authorise_generic_atomic',
      'public.timesheet_unauthorise_atomic',
      'public.timesheet_route_version_rotate',
      'private._timesheet_route_version_legacy_v1',
      'private.weekly_source_lock_and_resolve_families_v1',
      'private.weekly_source_office_unauthorise_action_state_v1',
      'public._pay_workbench_candidate_serial_key',
      'supabase/verification/17092026_0600_weekly_source_first_authorisation_v1.sql',
      'supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql',
      'tests/weekly-source/wp16c/una-proofs.sql',
      'tests/weekly-source/wp16c/managed-root-race-fixture.sql',
      'tests/weekly-source/fixtures-banking/build-all.sql',
    ],
    resultDirectory,
    fileName: 'wp16c-una-suite.json',
    acceptanceIds: recorder.passedIds((id) => id.startsWith('UNA-')),
    controllingRequirementIds: ['UNA-001'],
    extraActual: { databasesCreated: created.length },
  });

  process.stdout.write(`envelope ${envelope.status} digest ${envelope.evidenceDigest}\n`);
  if (envelope.status !== 'PASS') process.exitCode = 1;
}

await main();
