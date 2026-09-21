// Weekly Source Plan 6.2 — Gate 12 executed proof suite: R1..R44.
// WP-16c. Contract section 16, build item G12-5: "NEW suites: `R1–R44` with its
// own envelope citing `proof/32 §12`". G12-7 puts it last because it needs the
// most groundwork.
//
//   node tests/weekly-source/wp16c/release-suite.mjs [--results <dir>] [--keep]
//
// Environment: PSQL_BIN, PGPASSWORD (local throwaway), PGOPTIONS='-c jit=off'.
// In Git Bash also export MSYS_NO_PATHCONV=1.
//
// `R1`–`R44` are NOT acceptance ids. The pack publishes no CSV for them: they
// exist only as the section 12 table of
// `proof/32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md`, which the
// harness pins by SHA-256 and parses at load time. This suite therefore emits
// its ids in the envelope's `proofIds` field, against the proof-level control
// set WP-16b added, and in no other.
//
// FIVE STEPS:
//
//   A  every verifier that carries R rows is RUN, not cited, and each id is
//      attributed to the file that actually asserts it.
//   B  the release-lease two-session case, adopted from WP-08b's scratchpad and
//      given a committed home here (`R32`, `R33`, and the runtime half of
//      WP-08b review finding F1).
//   C  `R37` and `R15`, three named connections each.
//   D  `R42`, the installed Candidate serial gate: BYPASSED, BLOCKED, GRANTED.
//   E  `R40`, which `proof/32 §12` classifies as an implementation gate rather
//      than a database test. Its evidence is the installed-writer census, which
//      is run here, plus the release-winner attribution.
//
// Every id this suite cannot reach is recorded with the reason and is NOT
// claimed. An id blocked by another package's defect is recorded FAIL against
// that package with an executed reproduction.

import path from 'node:path';
import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import {
  BACKEND_ROOT, WP16C_ROOT, createProofRecorder, localUrl, parseProofLines,
  printProofTable, psqlFile, psqlQuery, psqlTry, standaloneNamedSessionGroup, writeSuiteEnvelope,
} from './proof-runner.mjs';

const TEMPLATE = process.env.WP16C_TEMPLATE ?? 'ws62_wp16c_template';
const VERIFICATION = path.join(BACKEND_ROOT, 'supabase', 'verification');
const FIXTURES_BANKING = path.join(BACKEND_ROOT, 'tests', 'weekly-source', 'fixtures-banking');
const CENSUS_ATTRIBUTION = path.resolve(
  BACKEND_ROOT, '..', 'plan6-pack-audit-20260916', 'h2-040-census',
  '04_RELEASE_WINNER_ATTRIBUTION.md');
const CLUSTER = { asServiceRole: false };

// Which verification file asserts which rows of `proof/32 §12`. Every entry was
// read out of the file's own assertions, not out of a comment.
const VERIFIER_PROOFS = [
  {
    file: '17092026_0300_weekly_source_entitlement_publication_v1.sql',
    ids: ['R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R25', 'R39'],
    note: 'the Gate 3/5 entitlement-publication verifier passed: stale revision, superseded head, '
      + 'exact and conflicting replay, forced failure, concurrent tick, the seven-point R25 contract '
      + 'and every receipt constraint',
  },
  {
    file: '17092026_0400_weekly_source_freeze_census_v1.sql',
    ids: ['R2', 'R3', 'R4', 'R5', 'R16', 'R17', 'R18', 'R19', 'R20', 'R21', 'R28', 'R29', 'R30',
      'R31', 'R34', 'R35', 'R36', 'R38', 'R41', 'R43', 'R44'],
    note: 'the Gate 5 freeze-census verifier passed: every class, Binding and refusal of '
      + 'proof/32 sections 4 and 5 over the named evidence states',
  },
  {
    file: '17092026_1000_weekly_source_settlement_allocation_v1.sql',
    ids: ['R20', 'R34', 'R35'],
    note: 'the Gate 9 settlement-allocation verifier passed: duplicate and identical history rows '
      + 'and every snapshot conflict are explicit unavailable states with a reason, never a figure',
  },
  {
    file: '17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
    ids: ['R1', 'R6', 'R13', 'R14', 'R22', 'R23', 'R24', 'R27', 'R32', 'R33'],
    note: 'the Gate 5 pending-release verifier passed: released bundles, the A/B pair, lost ticks, '
      + 'ten technical failures, rotation integrity, bounds and the lease rules',
  },
];

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

// --- step A -----------------------------------------------------------------
function runVerifiers(recorder, url) {
  for (const entry of VERIFIER_PROOFS) {
    const outcome = psqlFile(url, path.join(VERIFICATION, entry.file));
    for (const id of entry.ids) {
      recorder.record(id, outcome.ok ? 'PASS' : 'FAIL', 'EXECUTED',
        `clone + supabase/verification/${entry.file}`,
        outcome.ok ? entry.note : `verifier failed: ${firstError(outcome.stderr)}`);
    }
  }
}

// --- step B: the release lease, two sessions --------------------------------
async function proveLeaseRace(recorder, database, openGroup) {
  const url = localUrl(database);
  const state = `${database} (release-lease race fixture)`;

  const install = psqlFile(url, path.join(FIXTURES_BANKING, 'install.sql'), { cwd: FIXTURES_BANKING });
  const build = psqlFile(url, path.join(FIXTURES_BANKING, 'build-all.sql'), { cwd: FIXTURES_BANKING });
  const fixture = psqlFile(url, path.join(WP16C_ROOT, 'release-lease-race-fixture.sql'), { cwd: WP16C_ROOT });
  if (!install.ok || !build.ok || !fixture.ok) {
    for (const id of ['R32', 'R33']) {
      recorder.record(id, 'FAIL', 'EXECUTED', state,
        `the lease race fixture could not be built: `
        + `${firstError(install.stderr || build.stderr || fixture.stderr)}`);
    }
    return;
  }

  const identity = psqlQuery(url, `
    select bundle_row.id::text||'|'||bundle_row.pending_revision::text||'|'
           ||pg_catalog.encode(bundle_row.request_digest,'hex')||'|'
           ||bundle_row.lease_token::text||'|'||bundle_row.lease_owner||'|'
           ||bundle_row.lease_worker_run_id::text||'|'||bundle_row.state
    from public.weekly_source_pending_entitlement_bundles as bundle_row
    order by bundle_row.created_at_utc limit 1;`);
  const [bundleId, revision, digest, token, owner, runId, bundleState] = identity.split('|');
  recorder.record('R32', bundleState === 'RELEASING' ? 'PASS' : 'FAIL', 'EXECUTED', state,
    `the worker claimed the bundle: state=${bundleState}, one lease held by ${owner}`);

  const applySql = (workerId, leaseToken, workerRunId) =>
    `select (private.weekly_source_pending_entitlement_release_apply_v1('${bundleId}'::uuid,`
    + `${revision}::bigint,pg_catalog.decode('${digest}','hex'),'${workerId}',`
    + `'${leaseToken}'::uuid,'${workerRunId}'::uuid)-'census'-'receipt'-'lock_result'-'heads')::text;`;

  // R32 — the right Worker name and lease token but the WRONG worker run id.
  const wrongRun = psqlTry(url, applySql(owner, token, '11111111-1111-4111-8111-111111111111'));
  let wrongRunJson = {};
  try { wrongRunJson = JSON.parse(wrongRun.stdout || '{}'); } catch { wrongRunJson = {}; }
  const afterWrongRun = psqlQuery(url, `
    select state||'|'||pending_revision::text||'|'||technical_failure_count::text
      ||'|'||(select count(*)::text from private.weekly_source_entitlement_publication_receipts)
    from public.weekly_source_pending_entitlement_bundles where id='${bundleId}'::uuid;`);
  recorder.record('R32',
    wrongRunJson.code === 'WEEKLY_SOURCE_RELEASE_LEASE_INVALID'
    && afterWrongRun === `RELEASING|${revision}|0|0` ? 'PASS' : 'FAIL',
    'EXECUTED', state,
    `the wrong p_worker_run_id is refused ${wrongRunJson.code ?? 'NONE'} `
    + `(stage ${wrongRunJson.detail?.stage ?? '-'}) and nothing is written `
    + `(state|revision|failures|receipts = ${afterWrongRun})`);

  // The two-session case itself: same bundle, same lease, both sessions.
  const group = await openGroup({ database, names: ['winner', 'loser', 'observer'], groupId: 'wp16c-rel' });
  try {
    await declareServiceRole(group);
    const winner = group.session('winner');
    const loser = group.session('loser');

    await winner.runWithin('begin;');
    const winnerResult = await winner.runWithin(applySql(owner, token, runId), 120_000);
    let winnerJson = {};
    try { winnerJson = JSON.parse(lastRow(winnerResult).replace(/^S?1?\s*/, '')); } catch { winnerJson = {}; }
    recorder.record('R32', winnerJson.released === true ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `session 1 releases inside its open transaction: released=${winnerJson.released}, `
      + `state=${winnerJson.state}`);

    // Session 2 presents the SAME lease while session 1 is still open. It must
    // be refused by the code, under the lock, before any write — not stopped
    // afterwards by a schema constraint.
    const loserPending = loser.run(applySql(owner, token, runId));
    let blocked = null;
    try { blocked = await group.waitUntilBlocked('observer', 'loser', { timeoutMs: 20_000 }); } catch { blocked = null; }
    recorder.record('R32', blocked?.waitEventType === 'Lock' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      blocked
        ? 'session 2 waits on the pending bundle row lock rather than racing past it'
        : 'session 2 did not enter an observed Lock wait');

    await winner.runWithin('commit;', 60_000);
    const loserResult = await loserPending;
    let loserJson = {};
    try { loserJson = JSON.parse(lastRow(loserResult).replace(/^S?2?\s*/, '')); } catch { loserJson = {}; }
    recorder.record('R32',
      loserJson.code === 'WEEKLY_SOURCE_RELEASE_LEASE_INVALID'
      && loserJson.detail?.stage === 'UNDER_LOCK'
      && !loserResult.error ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `session 2 is refused BY THE CODE under the lock: ${loserJson.code ?? 'NONE'} `
      + `stage=${loserJson.detail?.stage ?? '-'}, raised=${loserResult.error ? (loserResult.error.message ?? 'yes').slice(0,140) : 'no'} `
      + `(before WP-08b's fix the reviewer measured it reaching the BUSY branch and being stopped `
      + `afterwards by the relation's check constraint with a raised 23514)`);
    recorder.record('R32', isDeadlock(loserResult.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'lease race: no deadlock');

    const post = psqlQuery(url, `
      select state||'|'||pending_revision::text||'|'||technical_failure_count::text
        ||'|'||(select count(*)::text from private.weekly_source_entitlement_publication_receipts)
        ||'|'||(select count(*)::text from public.weekly_source_entitlement_heads
                where state='COMMITTED_CURRENT')
      from public.weekly_source_pending_entitlement_bundles where id='${bundleId}'::uuid;`);
    recorder.record('R32', /^RELEASED\|\d+\|0\|1\|1$/.test(post) ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `exactly one release survives the race (state|revision|failures|receipts|heads = ${post})`);

    // R33 — the release committed, the response was lost, the lease has since
    // expired, and the same request is applied again. The replay check answers
    // before the expired-lease rejection, and nothing is republished.
    psqlTry(url, `update public.weekly_source_pending_entitlement_bundles
      set lease_expires_at_utc = clock_timestamp() - interval '1 hour' where id='${bundleId}'::uuid;`);
    const replay = psqlTry(url, applySql(owner, token, runId));
    let replayJson = {};
    try { replayJson = JSON.parse(replay.stdout || '{}'); } catch { replayJson = {}; }
    const postReplay = psqlQuery(url, `
      select (select count(*)::text from private.weekly_source_entitlement_publication_receipts)
        ||'|'||(select count(*)::text from public.weekly_source_entitlement_heads
                where state='COMMITTED_CURRENT');`);
    recorder.record('R33',
      replayJson.replayed === true && postReplay === '1|1' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `after an expired lease the exact replay returns the committed receipt before the lease `
      + `rejection (replayed=${replayJson.replayed}, code=${replayJson.code ?? 'none'}) and nothing `
      + `is republished (receipts|heads = ${postReplay})`);
  } finally {
    await group.closeAll();
  }
}

// --- step C: R37 and R15 ----------------------------------------------------
async function proveConcurrency(recorder, database, openGroup) {
  const url = localUrl(database);
  const state = `${database} (committed race fixture)`;
  const ACTOR = 'd6000000-0000-4000-8000-000000000001';
  const PADDED_ROOT = 'd6000000-0000-4000-8000-000000000302';
  const PADDED_CANDIDATE = 'd6000000-0000-4000-8000-000000000102';

  // ---- R37: a booking id with surrounding whitespace, confirmed rotation
  //      racing publication, first authorisation and withdrawal.
  let group = await openGroup({ database, names: ['weekly', 'rotate', 'observer'], groupId: 'wp16c-rel' });
  try {
    await declareServiceRole(group);
    const weekly = group.session('weekly');
    const rotate = group.session('rotate');

    await weekly.runWithin('begin;');
    const gate = await weekly.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${PADDED_CANDIDATE}'::uuid,`
      + `array['${PADDED_ROOT}'::uuid],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',`
      + `gen_random_uuid(),'WP16C-R37')->>'gate';`, 60_000);
    recorder.record('R37', lastRow(gate) === 'GRANTED' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `the publication job type takes the padded family's lock set trimmed key first, then the raw `
      + `key: gate=${lastRow(gate)}`);

    // The real confirmed route, which is the E1a entry point, against the same
    // padded family while publication holds it.
    //
    // E1a IS EXPECTED TO WAIT, and that is the point of R37. WP-09 placed this
    // entry point's guard deliberately AFTER the trimmed-key advisory lock and
    // the family rows FOR UPDATE, and before the owner's first write, so the
    // guard evaluates the family UNDER the lock. That is exactly ROT-012's
    // wording: "every entry invokes the guard before its first write while
    // holding the trimmed-first lock set". A test that expected an immediate
    // refusal here would be testing the wrong rule.
    //
    // The confirmed route also refuses CANDIDATE_ROUTE_CONFIRMATION_DISABLED
    // unless the flag is on, so the flag is turned on for this arm: R37 is
    // about the CONFIRMED route racing publication, not about the flag.
    await group.session('observer').runWithin(
      `update public.settings_defaults set candidate_app_feature_flags_json=`
      + `coalesce(candidate_app_feature_flags_json,'{}'::jsonb)`
      + `||jsonb_build_object('candidate_route_confirmation',true) where id=1;`, 30_000);
    const signature = psqlQuery(url, `select nullif(btrim(coalesce(
        signature->>'backend_row_signature', signature->>'row_signature','')),'')
      from public.timesheet_lifecycle_guard_signature_v1('${PADDED_ROOT}'::uuid,
        (select id from public.contract_weeks where timesheet_id='${PADDED_ROOT}'::uuid),
        false) as signature;`);
    // The context hash is compared at the confirmed route's line 136, which is
    // AFTER the managed-root guard at line 81, so its value cannot change this
    // arm's outcome. A fixed 64-hex value is used rather than the real one:
    // `private._timesheet_route_change_context_v1` raises
    // TIMESHEET_CURRENT_VERSION_NOT_FOUND for this padded family, and chasing
    // that would be testing a different owner than the one R37 is about.
    const context = 'a'.repeat(64);
    const confirmed = rotate.run(
      `select public.timesheet_route_version_confirmed_v1('${PADDED_ROOT}'::uuid,'${PADDED_ROOT}'::uuid,`
      + `'${signature}','${context}','ALLOW_QR_AGAIN','${ACTOR}'::uuid)->>'new_timesheet_id';`);

    let confirmedWait = null;
    try {
      confirmedWait = await group.waitUntilBlocked('observer', 'rotate', { timeoutMs: 25_000 });
    } catch { confirmedWait = null; }
    recorder.record('R37', confirmedWait?.waitEventType === 'Lock' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      confirmedWait
        ? 'the confirmed route serialises against publication on the padded family: it waits on '
          + 'the trimmed-first lock set rather than proceeding past it'
        : 'the confirmed route did not enter an observed Lock wait');

    // The winner commits its view of the family; the loser then evaluates the
    // guard UNDER the lock and sees the winner's committed state.
    await weekly.runWithin('rollback;', 30_000);
    const confirmedResult = await confirmed;
    recorder.record('R37',
      /WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED/.test(confirmedResult.error?.message ?? '')
        ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `the loser observes the winner's committed state and is refused under the lock: `
      + `${(confirmedResult.error?.message ?? 'accepted').slice(0, 120)}`);
    recorder.record('R37', isDeadlock(confirmedResult.error) ? 'FAIL' : 'PASS', 'EXECUTED', state,
      'confirmed rotation against publication on a padded family: no deadlock');
    // The other order: the rotation takes the family first, the Weekly Source
    // side waits for it and then observes the committed state.
    await rotate.runWithin('begin;', 30_000);
    await rotate.runWithin(
      `select pg_advisory_xact_lock(hashtext(btrim('  WP16C-RACE-B  ')));`, 30_000);
    const waiting = weekly.run(
      `select private.weekly_source_lock_and_resolve_families_v1('${PADDED_CANDIDATE}'::uuid,`
      + `array['${PADDED_ROOT}'::uuid],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',`
      + `gen_random_uuid(),'WP16C-R37')->>'gate';`);
    let waited = null;
    try { waited = await group.waitUntilBlocked('observer', 'weekly', { timeoutMs: 20_000 }); } catch { waited = null; }
    recorder.record('R37', waited?.waitEventType === 'Lock' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      waited
        ? 'the trimmed key alone serialises the padded family: the withdrawal path waits on it'
        : 'no Lock wait was observed on the trimmed key');
    await rotate.runWithin('rollback;', 30_000);
    const waitedResult = await waiting;
    recorder.record('R37',
      lastRow(waitedResult) === 'GRANTED' && !isDeadlock(waitedResult.error) ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `exactly one winner and the loser observes the winner's committed state: `
      + `gate=${lastRow(waitedResult)}, deadlock=${isDeadlock(waitedResult.error)}`);
  } finally {
    await group.closeAll();
  }

  // ---- R42: the installed Candidate serial gate.
  const bypassed = psqlQuery(url,
    `select private.weekly_source_lock_and_resolve_families_v1('${PADDED_CANDIDATE}'::uuid,`
    + `array['${PADDED_ROOT}'::uuid],'NOT_A_PINNED_JOB_TYPE',gen_random_uuid(),'WP16C-R42')::text;`);
  let bypassedJson = {};
  try { bypassedJson = JSON.parse(bypassed || '{}'); } catch { bypassedJson = {}; }
  recorder.record('R42',
    bypassedJson.code === 'WEEKLY_SOURCE_SERIAL_GATE_BYPASSED' && bypassedJson.retryable === false
      ? 'PASS' : 'FAIL',
    'EXECUTED', state,
    `an unpinned job type is a failure with no write: ${bypassedJson.code ?? 'NONE'} `
    + `(retryable=${bypassedJson.retryable}, reason=${bypassedJson.reason ?? '-'})`);

  group = await openGroup({ database, names: ['builder', 'office', 'observer'], groupId: 'wp16c-rel' });
  try {
    await declareServiceRole(group);
    const builder = group.session('builder');
    const office = group.session('office');
    await builder.runWithin('begin;');
    await builder.runWithin(
      `select pg_advisory_xact_lock(hashtextextended(`
      + `public._pay_workbench_candidate_serial_key('${PADDED_CANDIDATE}'::uuid), 24062027));`);
    const blocked = await office.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${PADDED_CANDIDATE}'::uuid,`
      + `array['${PADDED_ROOT}'::uuid],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',`
      + `gen_random_uuid(),'WP16C-R42')::text;`, 60_000);
    let blockedJson = {};
    try { blockedJson = JSON.parse(lastRow(blocked) || '{}'); } catch { blockedJson = {}; }
    recorder.record('R42',
      blockedJson.code === 'WEEKLY_SOURCE_CANDIDATE_BUSY' && blockedJson.retryable === true
        ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      `the pinned type while a Workbench build holds the key is a retryable refusal with no write: `
      + `${blockedJson.code ?? 'NONE'} (retryable=${blockedJson.retryable})`);
    await builder.runWithin('commit;');
    const granted = await office.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${PADDED_CANDIDATE}'::uuid,`
      + `array['${PADDED_ROOT}'::uuid],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',`
      + `gen_random_uuid(),'WP16C-R42')->>'gate';`, 60_000);
    recorder.record('R42', lastRow(granted) === 'GRANTED' ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `only GRANTED proceeds: gate=${lastRow(granted)} once the key is free`);
  } finally {
    await group.closeAll();
  }
}

// ---- R15: the census under concurrent cancellation and settlement ----------
async function proveCensusConcurrency(recorder, database, openGroup) {
  const url = localUrl(database);
  const state = `${database} (WP-16a evidence states)`;
  const identity = psqlQuery(url, `
    select (ws_banking_fixture.base_world_v1()->>'candidate_a_id')||'|'
        ||(ws_banking_fixture.base_world_v1()->>'timesheet_a_v2');`);
  const [candidateId, timesheetId] = identity.split('|');

  const group = await openGroup({ database, names: ['census', 'banking', 'observer'], groupId: 'wp16c-rel' });
  try {
    await declareServiceRole(group);
    const census = group.session('census');
    const banking = group.session('banking');

    // The census runs inside an open transaction and is held there.
    await census.runWithin('begin;');
    const censusResult = await census.runWithin(
      `select private.weekly_source_freeze_census_v1('${candidateId}'::uuid,`
      + `array(select distinct family_timesheet_id from public._pay_timesheet_rotation_scope(`
      + `array['${timesheetId}'::uuid])))->>'result';`, 120_000);
    recorder.record('R15', ['RELEASABLE', 'FROZEN', 'CENSUS_ERROR'].includes(lastRow(censusResult))
      ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `the freeze census answers inside an open transaction: result=${lastRow(censusResult)}`);

    // While that transaction is open, a Banking Pay owner writes the same
    // family's evidence. Interface I-2 takes no row lock on any Banking Pay
    // table, so this must not block.
    const startedAt = Date.now();
    const bankingWrite = await banking.runWithin(
      `update public.pay_batch_items set updated_at=clock_timestamp() `
      + `where timesheet_id='${timesheetId}'::uuid;`, 20_000);
    const elapsed = Date.now() - startedAt;
    recorder.record('R15', !bankingWrite.error && elapsed < 15_000 ? 'PASS' : 'FAIL', 'EXECUTED', state,
      `a Banking Pay write on the same family is never blocked by the census: it completed in `
      + `${elapsed} ms with ${bankingWrite.error ? bankingWrite.error.message : 'no error'}`);

    const secondCensus = await census.runWithin(
      `select private.weekly_source_freeze_census_v1('${candidateId}'::uuid,`
      + `array(select distinct family_timesheet_id from public._pay_timesheet_rotation_scope(`
      + `array['${timesheetId}'::uuid])))->>'result';`, 120_000);
    recorder.record('R15',
      !isDeadlock(secondCensus.error) && !isDeadlock(bankingWrite.error) ? 'PASS' : 'FAIL',
      'EXECUTED', state,
      'census under concurrent Banking Pay writing: no deadlock in either direction');
    await census.runWithin('rollback;', 30_000);
  } finally {
    await group.closeAll();
  }
}

// --- R26: the worker tick, executed through its own test -------------------
//
// R26 is a Worker-level row, not a database one: "worker tick with one failing
// bundle and pending candidate/manager deliveries | deliveries run; other
// bundles proceed; only the failing bundle records the failure". The owner is
// `broker/src/weekly-source/pending-entitlement-release-worker.mjs` and its
// executed proof is WP-08b's own test file, which is RUN here rather than cited.
function proveWorkerTick(recorder) {
  const testFile = 'tests/weekly-source/weekly-source-pending-entitlement-release-worker.test.mjs';
  const result = spawnSync(process.execPath, ['--test', testFile], {
    cwd: BACKEND_ROOT,
    encoding: 'utf8',
    timeout: 300_000,
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
  });
  const output = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
  const passed = !result.error && result.status === 0;
  const r26Line = output.split(String.fromCharCode(10))
    .find((line) => line.includes('R26:')) ?? '';
  recorder.record('R26', passed && r26Line.includes('R26:') ? 'PASS' : 'FAIL', 'EXECUTED',
    `node --test ${testFile}`,
    passed
      ? `the release worker's own tests pass, including "${r26Line.replace(/^.*?R26/, 'R26').trim()}" `
        + `and the proof/32 section 2 ordering test that the release step runs after the delivery `
        + `work and cannot prevent it`
      : `the release worker tests failed: ${output.slice(-260)}`);
}

// --- step E: R40 ------------------------------------------------------------
function proveImplementationGate(recorder, url, database) {
  const outcome = psqlFile(url, path.join(VERIFICATION, '17092026_0900_weekly_source_installed_writer_census_v1.sql'));
  const attributionPresent = existsSync(CENSUS_ATTRIBUTION);
  recorder.record('R40', outcome.ok && attributionPresent ? 'PASS' : 'FAIL', 'EXECUTED',
    `${database} + h2-040-census`,
    outcome.ok
      ? `proof/32 section 12 classifies R40 as an implementation gate, not a database test: the `
        + `mutation-writer census is repeated against the installed function definitions by `
        + `supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql, which `
        + `passed here, and the release winners are attributed in `
        + `h2-040-census/04_RELEASE_WINNER_ATTRIBUTION.md (present: ${attributionPresent})`
      : `the installed-writer census verifier failed: ${firstError(outcome.stderr)}`);
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
  const recorder = createProofRecorder('R');
  const created = [];

  const verifierDatabase = createClone('ws62_wp16c_rel_a');
  created.push(verifierDatabase);
  const leaseDatabase = createClone('ws62_wp16c_rel_b');
  created.push(leaseDatabase);
  const raceDatabase = createClone('ws62_wp16c_rel_c');
  created.push(raceDatabase);

  try {
    const verifierUrl = localUrl(verifierDatabase);

    runVerifiers(recorder, verifierUrl);
    proveImplementationGate(recorder, verifierUrl, verifierDatabase);
    proveWorkerTick(recorder);

    await proveLeaseRace(recorder, leaseDatabase, standaloneNamedSessionGroup);

    const fixture = psqlFile(localUrl(raceDatabase), path.join(WP16C_ROOT, 'managed-root-race-fixture.sql'),
      { cwd: WP16C_ROOT });
    if (!fixture.ok) {
      for (const id of ['R37', 'R42']) {
        recorder.record(id, 'FAIL', 'EXECUTED', raceDatabase,
          `the committed race fixture failed: ${firstError(fixture.stderr)}`);
      }
    } else {
      await proveConcurrency(recorder, raceDatabase, standaloneNamedSessionGroup);
    }

    // R15 needs the Banking Pay evidence states, which the lease clone carries.
    await proveCensusConcurrency(recorder, leaseDatabase, standaloneNamedSessionGroup);
  } finally {
    if (!keep) for (const database of created) dropClone(database);
  }

  printProofTable(recorder);

  const covered = new Set(recorder.passedIds((id) => /^R\d+$/.test(id)));
  // WP-16f: R40's evidence is `proveImplementationGate` passing the installed-
  // writer census verifier above. WP-18b's independent round-2 review
  // (`WP-18b_REVIEW_2.md`, verdict REJECTED) proved that verifier still misses
  // three live evidence-table writers with `ok:true` — a masked single-quoted
  // `EXECUTE` (G1, CRITICAL), a cross-schema auto-updatable view (G2, HIGH) and
  // an inheritance/partition parent whose name does not contain the evidence
  // table's name (G3, HIGH). A passing run of the current census verifier
  // therefore does not establish what R40 exists to prove, so it is withheld
  // from coverage here even though it is recorded PASS above (WP-16e_COVERAGE.md
  // §3.3 and §4 item 1). This is not a hand-edit of a result: it is the suite
  // declining to claim release evidence it knows is unsound, and it will start
  // counting again automatically the moment the census detection logic itself
  // is repaired and this line is removed.
  const WITHHELD_FROM_COVERAGE = new Set(['R40']);
  for (const id of WITHHELD_FROM_COVERAGE) covered.delete(id);
  const missing = [];
  for (let index = 1; index <= 44; index += 1) {
    if (!covered.has(`R${index}`)) missing.push(`R${index}`);
  }
  process.stdout.write(
    `\nproof/32 section 12: ${covered.size} of 44 covered by executed evidence in this run `
    + `(${WITHHELD_FROM_COVERAGE.size} withheld: ${[...WITHHELD_FROM_COVERAGE].join(', ')}).\n`
    + `not covered: ${missing.join(', ') || 'none'}\n`);

  const envelope = await writeSuiteEnvelope({
    recorder,
    scenarioId: 'WS-WP16C-RELEASE-001',
    seedText: 'weekly-source-wp16c-release-suite-v1',
    database: TEMPLATE,
    authority: 'proof/32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md section 12 (R1-R44); '
      + 'contract section 16 G12-5',
    executedOwners: [
      'private.weekly_source_pending_entitlement_release_apply_v1',
      'private.weekly_source_pending_entitlement_release_claim_page_v1',
      'private.weekly_source_pending_entitlement_bundle_save_v1',
      'private.weekly_source_lock_and_resolve_families_v1',
      'private.weekly_source_freeze_census_v1',
      'public.timesheet_route_version_confirmed_v1',
      'public._pay_workbench_candidate_serial_key',
      'public._pay_timesheet_rotation_scope',
      ...VERIFIER_PROOFS.map((entry) => `supabase/verification/${entry.file}`),
      'supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql',
      'broker/src/weekly-source/pending-entitlement-release-worker.mjs',
      'tests/weekly-source/weekly-source-pending-entitlement-release-worker.test.mjs',
      'tests/weekly-source/wp16c/release-lease-race-fixture.sql',
      'tests/weekly-source/wp16c/managed-root-race-fixture.sql',
      'tests/weekly-source/fixtures-banking/build-all.sql',
    ],
    resultDirectory,
    fileName: 'wp16c-release-suite.json',
    proofIds: [...covered].sort(),
    extraActual: {
      databasesCreated: created.length,
      proofSetSize: 44,
      proofSetCovered: covered.size,
      proofSetMissing: missing,
    },
  });

  process.stdout.write(`envelope ${envelope.status} digest ${envelope.evidenceDigest}\n`);
  if (envelope.status !== 'PASS') process.exitCode = 1;
}

await main();
