// Weekly Source Plan 6.2 — WP-23 transport proof.
//
//   node tests/weekly-source/wp23-transport-e2e.mjs
//
// Environment: PSQL_BIN, PGPASSWORD (local throwaway), PGOPTIONS='-c jit=off',
// WP23_DATABASE (default `ws62_wp23_proof`).  In Git Bash also export
// MSYS_NO_PATHCONV=1.
//
// WHY THIS FILE EXISTS.  WP-19b proved that eleven new Weekly Source service
// RPCs had no caller outside the database, and the Gate 13 hostile finance
// review's finding F2 is a report of exactly that class: source that exists and
// is never exercised.  A stub `rpc` proves nothing, so nothing here asserts that
// a function was called.  Every row drives a REAL broker route — the same
// exported function objects `broker/src/index.js` mounts — over real HTTP into a
// real PostgreSQL 17.11 build, and then measures the database with an
// independent query.
//
// The only substitution is PostgREST itself, which this cluster does not run:
// `tests/weekly-source/wp23-postgrest-local.mjs` serves `/rest/v1/...` from the
// same database, one transaction per request, as `service_role`, returning
// PostgREST's own error shape with the real SQLSTATE.  Everything above that
// line is the shipped broker code, unmodified.
//
// Safety: local disposable database only, never a hosted target, never LIVE.
// No email and no push notification is sent: the Candidate push owner writes a
// `candidate_notifications` row through the installed boundary, and the delivery
// transport is not run by this file.

import { createHmac } from 'node:crypto';

import {
  dispatchWeeklySourceRequest,
  WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT,
} from '../../broker/src/weekly-source/routes.js';
import {
  recordGuardRefusalAfterRollback,
  isManagedRootGuardRefusal,
} from '../../broker/src/weekly-source/guard-refusal-record.mjs';
import { weeklySourceFirstAuthorisationInternals } from '../../broker/src/index.js';
import { createLocalPostgrest } from './wp23-postgrest-local.mjs';

const DATABASE = process.env.WP23_DATABASE || 'ws62_wp23_proof';
const SESSION_SECRET = 'wp23-local-proof-session-secret';

const OFFICE_ACTOR = 'c9000000-0000-4000-8000-000000000001';
const ROOT_1 = 'c9000000-0000-4000-8000-000000000301';
const ROOT_2 = 'c9000000-0000-4000-8000-000000000302';
const ROOT_3 = 'c9000000-0000-4000-8000-000000000303';
const PENDING_BUNDLE = 'c9000000-0000-4000-8000-000000000804';

const postgrest = createLocalPostgrest({ database: DATABASE });
let ORIGIN = '';

const results = [];
function record(id, owner, passed, detail) {
  results.push({ id, owner, result: passed ? 'PASS' : 'FAIL', detail: String(detail) });
  process.stdout.write(`WP23|${id}|${passed ? 'PASS' : 'FAIL'}|${owner}|${String(detail)}\n`);
}

// --- measurement, independent of the route under test -----------------------
import { spawnSync } from 'node:child_process';
function query(sql) {
  const run = spawnSync(process.env.PSQL_BIN || 'psql', [
    '-X', '-tA', '-v', 'ON_ERROR_STOP=1',
    `postgresql://postgres@127.0.0.1:55433/${DATABASE}`, '-c', sql,
  ], {
    encoding: 'utf8',
    env: { ...process.env, PGPASSWORD: process.env.PGPASSWORD || 'localonly' },
    maxBuffer: 32 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`measurement failed: ${run.stderr || run.stdout}`);
  return String(run.stdout).trim();
}
const count = (sql) => Number(query(sql));

// --- the broker's own transport, reproduced exactly --------------------------
// `sbRpc` in `broker/src/index.js` POSTs the argument object to
// `${SUPABASE_URL}/rest/v1/rpc/<fn>` and, on a non-2xx, throws an Error carrying
// `status`, `body` and `json`.  Weekly Source functions take none of its Banking
// Pay route-class handling, so this is the whole of it for them.
async function sbRpcLocal(functionName, args) {
  const response = await fetch(`${ORIGIN}/rest/v1/rpc/${encodeURIComponent(functionName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', apikey: 'local', authorization: 'Bearer local' },
    body: JSON.stringify(args || {}),
  });
  const text = await response.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  if (!response.ok) {
    const error = new Error(`RPC ${functionName} failed ${response.status}: ${text}`);
    error.status = response.status;
    error.body = text;
    error.json = json;
    error.fn = functionName;
    throw error;
  }
  return json;
}

const guardRefusalLog = [];
const officeDependencies = () => ({
  requireUser: async () => ({ id: OFFICE_ACTOR, role: 'admin' }),
  rpc: (functionName, args) => sbRpcLocal(functionName, args),
  logGuardRefusalRecordFailure: (entry) => guardRefusalLog.push(entry),
});

const httpRequest = (routePath, body, method = 'POST', headers = {}) => new Request(
  `https://wp23.invalid${routePath}`,
  {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    ...(method === 'GET' ? {} : { body: JSON.stringify(body ?? {}) }),
  },
);

async function drive(routePath, body, method = 'POST') {
  const response = await dispatchWeeklySourceRequest(
    httpRequest(routePath, body, method), {}, {}, officeDependencies(),
  );
  return { status: response.status, payload: await response.json() };
}
const command = (action, payload) => drive('/api/weekly-source/v1/commands', { action, payload });

// --- the Office Authorise route ---------------------------------------------
// `handleTimesheetAuthoriseGeneric` authenticates with the broker's own compact
// token: base64url(JSON) '.' base64url(HMAC-SHA256).  Minting one here is a test
// concern only; no production code is changed to allow it.
const base64url = (buffer) => Buffer.from(buffer).toString('base64')
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

function officeBearerToken() {
  const payload = {
    typ: 'access',
    sub: OFFICE_ACTOR,
    email: 'wp23-office@example.test',
    role: 'admin',
    sv: 1,
    sid: 'wp23-proof-session',
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const data = base64url(Buffer.from(JSON.stringify(payload), 'utf8'));
  const signature = base64url(createHmac('sha256', SESSION_SECRET).update(data).digest());
  return `${data}.${signature}`;
}

function brokerEnv() {
  return {
    SUPABASE_URL: ORIGIN,
    SUPABASE_SERVICE_ROLE_KEY: 'local-service-role',
    SUPABASE_ANON_KEY: 'local-anon',
    SESSION_TOKEN_SECRET: SESSION_SECRET,
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    ALLOWED_ORIGIN: 'https://wp23.invalid',
  };
}

async function driveOfficeAuthorise(timesheetId, expectedRowSignature) {
  const request = new Request(`https://wp23.invalid/api/timesheets/${timesheetId}/authorise`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${officeBearerToken()}`,
      origin: 'https://wp23.invalid',
    },
    body: JSON.stringify({
      expected_timesheet_id: timesheetId,
      current_timesheet_id: timesheetId,
      expected_row_signature: expectedRowSignature,
      backend_row_signature: expectedRowSignature,
      context: 'timesheet_modal',
    }),
  });
  const response = await weeklySourceFirstAuthorisationInternals
    .handleTimesheetAuthoriseGeneric(brokerEnv(), request, timesheetId, null, null);
  let payload = null;
  try { payload = await response.clone().json(); } catch { payload = null; }
  return { status: response.status, payload };
}

function rowSignature(timesheetId) {
  return query(
    `select coalesce(nullif(btrim(coalesce(signature->>'backend_row_signature',
                                           signature->>'row_signature','')),''),'')
       from public.timesheet_lifecycle_guard_signature_v1(
         '${timesheetId}'::uuid,
         (select id from public.contract_weeks where timesheet_id='${timesheetId}'::uuid),
         false) as signature`,
  );
}

const drainWorkbench = () => query('select public.wp23_drain_workbench_jobs()');

// ===========================================================================

async function main() {
  ORIGIN = await postgrest.listen();
  process.stdout.write(`WP23 transport proof against ${DATABASE} via ${ORIGIN}\n\n`);

  // -- 0. the shipped route contract declares the new wiring -------------------
  record(
    'WP23-000', 'route contract',
    WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.positionalRpcCommands.FIRST_AUTHORISE === 'weekly_source_first_authorise_v1'
      && WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.readRoutes.includes('/timesheets/:timesheetId/audit')
      && WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.guardRefusalRecordedAfterRollback === true,
    'the shipped route contract names the new commands, the new reads and the refusal recorder',
  );

  // =========================================================================
  // A. Gate 13 hostile review F2 — the REAL Office Authorise route.
  // =========================================================================
  drainWorkbench();
  const boundBefore = count(
    `select count(*) from public.weekly_source_root_authorisations where root_timesheet_id='${ROOT_1}'`,
  );
  const officeAuthorise = await driveOfficeAuthorise(ROOT_1, rowSignature(ROOT_1));
  drainWorkbench();
  const boundAfter = count(
    `select count(*) from public.weekly_source_root_authorisations where root_timesheet_id='${ROOT_1}'`,
  );
  const authorisedStatus = query(
    `select coalesce(processing_status::text,'?') from public.timesheets_financials
      where timesheet_id='${ROOT_1}' and is_current`,
  );
  record(
    'WP23-F2-A', 'public.weekly_source_first_authorise_v1 (Office Authorise route)',
    officeAuthorise.status === 200 && boundBefore === 0 && boundAfter === 1,
    `POST /api/timesheets/:id/authorise on a Weekly-Source-bound week -> ${officeAuthorise.status}; `
    + `weekly_source_root_authorisations ${boundBefore} -> ${boundAfter}; `
    + `generation=${officeAuthorise.payload?.weekly_source_first_authorisation?.authorisation_generation ?? '?'}; `
    + `tsfin processing_status now ${authorisedStatus}`,
  );

  // The ordinary Weekly week must be untouched by the new branch.
  const ordinaryBefore = count('select count(*) from public.weekly_source_root_authorisations');
  const ordinaryAuthorise = await driveOfficeAuthorise(ROOT_3, rowSignature(ROOT_3));
  drainWorkbench();
  const ordinaryAfter = count('select count(*) from public.weekly_source_root_authorisations');
  const ordinaryStatus = query(
    `select coalesce(processing_status::text,'?') from public.timesheets_financials
      where timesheet_id='${ROOT_3}' and is_current`,
  );
  record(
    'WP23-F2-B', 'timesheet_authorise_generic_atomic (unchanged ordinary path)',
    ordinaryAuthorise.status === 200 && ordinaryAfter === ordinaryBefore,
    `the same route on a week that is NOT in a Weekly Source group -> ${ordinaryAuthorise.status}; `
    + `root authorisations unchanged at ${ordinaryAfter}; tsfin processing_status now ${ordinaryStatus}`,
  );

  // The routing decision itself, driven against the real presentation owner.
  const boundDecision = await weeklySourceFirstAuthorisationInternals
    .weeklySourceAuthoriseRouting(sbRpcLocal, ROOT_2, OFFICE_ACTOR);
  const ordinaryDecision = await weeklySourceFirstAuthorisationInternals
    .weeklySourceAuthoriseRouting(sbRpcLocal, ROOT_3, OFFICE_ACTOR);
  record(
    'WP23-F2-C', 'weeklySourceAuthoriseRouting',
    boundDecision.bound === true && ordinaryDecision.bound === false,
    `the database decides: bound root -> bound=${boundDecision.bound}, `
    + `ordinary root -> bound=${ordinaryDecision.bound}`,
  );

  // =========================================================================
  // B. Gate 11 — the audit chronology and the hours export (WP-19b G2).
  // =========================================================================
  const chronology = await drive(`/api/weekly-source/v1/timesheets/${ROOT_1}/audit`, null, 'GET');
  const events = Array.isArray(chronology.payload?.events) ? chronology.payload.events : [];
  const narrated = events.filter((entry) => String(entry?.narrative ?? '').trim().length > 0).length;
  const firstAuthorisationAudit = count(
    "select count(*) from public.audit_events where action='WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED'",
  );
  record(
    'WP23-G2-A', 'public.weekly_source_timesheet_audit_chronology_v1',
    chronology.status === 200 && chronology.payload?.ok === true
      && events.length >= 1 && narrated === events.length && firstAuthorisationAudit >= 1,
    `GET /timesheets/:id/audit -> ${chronology.status} ok=${chronology.payload?.ok} `
    + `event_count=${chronology.payload?.event_count} narrated=${narrated}/${events.length}; `
    + `audit_events WEEKLY_SOURCE_FIRST_AUTHORISATION = ${firstAuthorisationAudit}; `
    + `first narrative: ${String(events[0]?.narrative ?? '').slice(0, 120)}`,
  );

  const exportRead = await drive(`/api/weekly-source/v1/timesheets/${ROOT_1}/hours-export`, null, 'GET');
  const hours = exportRead.payload?.weekly_source_hours ?? null;
  const separated = !!hours && ['submitted_hours', 'source_hours', 'approved_hours', 'paid_hours']
    .every((key) => Object.prototype.hasOwnProperty.call(hours, key));
  record(
    'WP23-G2-B', 'public.weekly_source_timesheet_hours_export_v1',
    exportRead.status === 200 && exportRead.payload?.ok === true && separated,
    `GET /timesheets/:id/hours-export -> ${exportRead.status} ok=${exportRead.payload?.ok}; `
    + `the four separated facts present=${separated}; `
    + `invoice_movements=${Array.isArray(hours?.invoice_movements) ? hours.invoice_movements.length : 'absent'}`,
  );

  // =========================================================================
  // C. the availability owner, reached through the real presentation route.
  // =========================================================================
  const presentation = await drive(`/api/weekly-source/v1/timesheets/${ROOT_1}/presentation`, null, 'GET');
  const presentationText = JSON.stringify(presentation.payload);
  record(
    'WP23-G7-B', 'public.weekly_source_first_authorisation_withdraw_available_v1',
    presentation.status === 200
      && presentationText.includes('WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAW_AVAILABLE_V1'),
    `GET /timesheets/:id/presentation -> ${presentation.status}; the payload carries `
    + 'availability_source=WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAW_AVAILABLE_V1, which only the '
    + `owner answering can produce; unauthorise.available=${presentation.payload?.unauthorise?.available}`,
  );

  // =========================================================================
  // D. the returned-refusal recorder (WP-14 handoff N7).
  // =========================================================================
  const refusalAuditBefore = count(
    `select count(*) from public.audit_events where object_id_text='${ROOT_1}'`,
  );
  const managedRecord = await command('RECORD_GUARD_REFUSAL', { timesheet_id: ROOT_1, entry_point: 'E7' });
  const refusalAuditAfter = count(
    `select count(*) from public.audit_events where object_id_text='${ROOT_1}'`,
  );
  const unmanagedRecord = await command('RECORD_GUARD_REFUSAL', { timesheet_id: ROOT_3, entry_point: 'E7' });
  record(
    'WP23-G5-A', 'public.weekly_source_audit_guard_refusal_record_v1',
    managedRecord.status === 200 && unmanagedRecord.status === 200
      && String(unmanagedRecord.payload?.reason ?? '') === 'NOT_A_REFUSAL',
    `POST /commands RECORD_GUARD_REFUSAL on a MANAGED root -> ${managedRecord.status} `
    + `${JSON.stringify(managedRecord.payload).slice(0, 180)}; audit_events for the root `
    + `${refusalAuditBefore} -> ${refusalAuditAfter}; on an UNMANAGED root reason=`
    + `${unmanagedRecord.payload?.reason}`,
  );

  // =========================================================================
  // E. the Candidate hours-only push re-drive.
  // =========================================================================
  const notificationsBefore = count('select count(*) from public.candidate_notifications');
  const push = await command('PUSH_CANDIDATE_HOURS', { timesheet_id: ROOT_1 });
  const notificationsAfter = count('select count(*) from public.candidate_notifications');
  record(
    'WP23-PUSH-A', 'public.weekly_source_candidate_hours_push_v1',
    push.status === 200 && push.payload !== null && typeof push.payload === 'object',
    `POST /commands PUSH_CANDIDATE_HOURS -> ${push.status} `
    + `${JSON.stringify(push.payload).slice(0, 200)}; `
    + `candidate_notifications ${notificationsBefore} -> ${notificationsAfter}`,
  );
  const pushRefused = await command('PUSH_CANDIDATE_HOURS', {
    timesheet_id: ROOT_1, actor_user_id: OFFICE_ACTOR,
  });
  record(
    'WP23-PUSH-B', 'routes.js strictEnvelopeRequest',
    pushRefused.status === 400
      && pushRefused.payload?.error_code === 'WEEKLY_SOURCE_COMMAND_ACTOR_NOT_ACCEPTED',
    `a browser-supplied actor on a closed-envelope owner is refused at the transport -> `
    + `${pushRefused.status} ${pushRefused.payload?.error_code}`,
  );

  // =========================================================================
  // F. G5-6 — the Office reopen of a manual-review bundle.
  // =========================================================================
  const bundleBefore = query(
    `select state||'|'||technical_failure_count||'|'||pending_revision
       from public.weekly_source_pending_entitlement_bundles where id='${PENDING_BUNDLE}'`,
  );
  const bundleAuditBefore = count(
    `select count(*) from public.audit_events where object_id_text='${PENDING_BUNDLE}'`,
  );
  const reopen = await command('REOPEN_PENDING_BUNDLE', {
    pending_bundle_id: PENDING_BUNDLE,
    reason: 'WP-23 transport proof: returned to the queue by the Office.',
  });
  const bundleAfter = query(
    `select state||'|'||technical_failure_count||'|'||pending_revision
       from public.weekly_source_pending_entitlement_bundles where id='${PENDING_BUNDLE}'`,
  );
  const bundleAuditAfter = count(
    `select count(*) from public.audit_events where object_id_text='${PENDING_BUNDLE}'`,
  );
  const releasedRows = count(
    `select count(*) from public.weekly_source_pending_entitlement_bundles
      where id='${PENDING_BUNDLE}' and released_at_utc is not null`,
  );
  const [state, failures] = bundleAfter.split('|');
  record(
    'WP23-G6-A', 'public.weekly_source_pending_entitlement_bundle_reopen_v1',
    reopen.status === 200 && reopen.payload?.ok === true
      && state === 'PENDING' && Number(failures) === 0
      && bundleAuditAfter === bundleAuditBefore + 1 && releasedRows === 0,
    `POST /commands REOPEN_PENDING_BUNDLE -> ${reopen.status} ok=${reopen.payload?.ok}; `
    + `state|failures|revision ${bundleBefore} -> ${bundleAfter}; `
    + `one new audit row (${bundleAuditBefore} -> ${bundleAuditAfter}); never released (${releasedRows})`,
  );
  const reopenAgain = await command('REOPEN_PENDING_BUNDLE', {
    pending_bundle_id: PENDING_BUNDLE, reason: 'WP-23 second attempt.',
  });
  record(
    'WP23-G6-B', 'public.weekly_source_pending_entitlement_bundle_reopen_v1',
    reopenAgain.payload?.ok === false
      && String(reopenAgain.payload?.code ?? '') === 'WEEKLY_SOURCE_PENDING_BUNDLE_NOT_IN_MANUAL_REVIEW',
    `a second reopen through the same route is refused ${reopenAgain.payload?.code}`,
  );

  // =========================================================================
  // G. R5-05 — external publication arrival and its outstanding inputs.
  // =========================================================================
  const arrivalsBefore = count('select count(*) from private.weekly_source_external_publication_arrivals');
  const bookingId = query(`select booking_id from public.timesheets where timesheet_id='${ROOT_2}'`);
  const arrival = await command('RECORD_EXTERNAL_PUBLICATION_ARRIVAL', {
    schema_version: 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_ARRIVAL_V1',
    external_system: 'C1',
    source_identity: {
      root_timesheet_id: ROOT_2,
      root_family_booking_id: bookingId,
      final_revision_id: 'c9000000-0000-4000-8000-000000000701',
      source_cycle_id: 'c9000000-0000-4000-8000-000000000702',
    },
    generation: { root_timesheet_version: 1, source_revision_number: 1, head_revision: 1 },
    digest: {
      source_generation_sha256: 'a'.repeat(64),
      entitlement_sha256: 'b'.repeat(64),
      publication_receipt_sha256: 'c'.repeat(64),
    },
  });
  const arrivalsAfter = count('select count(*) from private.weekly_source_external_publication_arrivals');
  const headsForRoot = count(
    `select count(*) from public.weekly_source_entitlement_heads where root_timesheet_id='${ROOT_2}'`,
  );
  record(
    'WP23-G11-A', 'public.weekly_source_external_publication_arrival_v1',
    arrival.status === 200 && typeof arrival.payload?.outcome === 'string' && headsForRoot === 0,
    `POST /commands RECORD_EXTERNAL_PUBLICATION_ARRIVAL -> ${arrival.status} `
    + `outcome=${arrival.payload?.outcome} reason_code=${arrival.payload?.reason_code ?? 'none'}; `
    + `arrivals ${arrivalsBefore} -> ${arrivalsAfter}; the CloudTMS head is untouched (${headsForRoot})`,
  );
  const pendingInputs = await drive(
    `/api/weekly-source/v1/timesheets/${ROOT_2}/external-publication/pending-inputs`, null, 'GET',
  );
  record(
    'WP23-G11-B', 'public.weekly_source_external_publication_pending_inputs_v1',
    pendingInputs.status === 200 && pendingInputs.payload?.ok === true,
    `GET /timesheets/:id/external-publication/pending-inputs -> ${pendingInputs.status} `
    + `ok=${pendingInputs.payload?.ok} outstanding=${pendingInputs.payload?.outstanding ?? '?'}`,
  );

  // =========================================================================
  // H. D13 — the post-rollback guard-refusal recorder.
  // =========================================================================
  // The refusal is produced by the REAL installed guard at a real guarded entry
  // point on the now-managed root, over the same transport, so its SQLSTATE,
  // message and DETAIL are the guard's own and its transaction is gone before
  // the record is attempted.
  let raised = null;
  try {
    await sbRpcLocal('tsfin_prepare_write', { p_timesheet_id: ROOT_1 });
  } catch (error) {
    raised = error;
  }
  const detected = raised !== null && isManagedRootGuardRefusal(raised);
  const guardAuditBefore = count(
    "select count(*) from public.audit_events where action='WEEKLY_SOURCE_ROTATION_REFUSED'",
  );
  let outcome = null;
  if (detected) {
    outcome = await recordGuardRefusalAfterRollback({
      rpc: (name, args) => sbRpcLocal(name, args),
      error: raised,
      correlationId: 'wp23-proof-correlation-0001',
      caller: 'broker:tsfin_prepare_write',
      actorUserId: OFFICE_ACTOR,
    });
  }
  const guardAuditAfter = count(
    "select count(*) from public.audit_events where action='WEEKLY_SOURCE_ROTATION_REFUSED'",
  );
  const correlated = count(
    "select count(*) from public.audit_events "
    + "where after_json->>'correlation_id'='wp23-proof-correlation-0001' and after_json->>'record_source'='CAUGHT_REFUSAL_POST_ROLLBACK'",
  );
  record(
    'WP23-D13-A', 'public.weekly_source_guard_refusal_record_after_rollback_v1',
    detected && outcome?.recorded === true && guardAuditAfter > guardAuditBefore && correlated >= 1,
    `the real guard raised ${raised?.json?.code} ${raised?.json?.message}; the durable caller `
    + `recorded it on a SEPARATE transaction after the rollback (recorded=${outcome?.recorded}`
    + `${outcome?.reason ? ` reason=${outcome.reason}` : ''}); guard-refusal audit rows `
    + `${guardAuditBefore} -> ${guardAuditAfter}; rows carrying the attempt's correlation id = ${correlated}`,
  );
  record(
    'WP23-D13-B', 'public.weekly_source_guard_refusal_record_after_rollback_v1',
    raised?.json?.code === '55000'
      && raised?.json?.message === 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
    'the refusal is unchanged by the record: the caller still sees '
    + `${raised?.json?.code} ${raised?.json?.message}`,
  );

  // =========================================================================
  // I. the route-level wrapper, driven through the real Weekly Source boundary.
  // =========================================================================
  const wrapperBefore = count(
    "select count(*) from public.audit_events where action='WEEKLY_SOURCE_ROTATION_REFUSED'",
  );
  const replay = await command('FIRST_AUTHORISE', { timesheet_id: ROOT_1 });
  const wrapperAfter = count(
    "select count(*) from public.audit_events where action='WEEKLY_SOURCE_ROTATION_REFUSED'",
  );
  record(
    'WP23-G7-A', 'routes.js FIRST_AUTHORISE + withRefusalRecording',
    replay.status === 200 && replay.payload?.ok === false
      && String(replay.payload?.code ?? '') === 'WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED',
    `POST /commands FIRST_AUTHORISE on the already-authorised root reaches the owner and is `
    + `refused ${replay.payload?.code}; guard-refusal audit rows ${wrapperBefore} -> ${wrapperAfter}`,
  );

  // =========================================================================
  // J. the positional withdrawal owner, reached through the jsonb request
  //    wrapper WP-07c built for exactly this transport.
  // =========================================================================
  // A second Weekly-Source-bound week is authorised through the SAME real Office
  // route, so the F2 fix is shown to be general rather than a single-row result,
  // and then withdrawn through the Office Unauthorise command.
  drainWorkbench();
  const secondAuthorise = await driveOfficeAuthorise(ROOT_2, rowSignature(ROOT_2));
  drainWorkbench();
  const liveBefore = count(
    `select count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='${ROOT_2}' and withdrawn_at_utc is null`,
  );
  const withdraw = await command('WITHDRAW_FIRST_AUTHORISATION', {
    timesheet_id: ROOT_2,
    expected_timesheet_id: ROOT_2,
    expected_row_signature: rowSignature(ROOT_2),
  });
  drainWorkbench();
  const liveAfter = count(
    `select count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='${ROOT_2}' and withdrawn_at_utc is null`,
  );
  const withdrawnRows = count(
    `select count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='${ROOT_2}' and withdrawn_at_utc is not null`,
  );
  record(
    'WP23-G3-A', 'public.weekly_source_first_authorisation_withdraw_v1 (through the jsonb wrapper)',
    secondAuthorise.status === 200 && withdraw.status === 200
      && liveBefore === 1 && liveAfter === 0 && withdrawnRows === 1,
    `a second bound week authorised through the real Office route -> ${secondAuthorise.status}; `
    + `POST /commands WITHDRAW_FIRST_AUTHORISATION -> ${withdraw.status} `
    + `payload=${JSON.stringify(withdraw.payload).slice(0, 200)}; `
    + `live generations ${liveBefore} -> ${liveAfter}; withdrawn generations = ${withdrawnRows}`,
  );

  // The same command on the root the fixture's pending bundle names is refused
  // by the owner's own W1 check, with no write: the route carries the owner's
  // structured verdict rather than deciding anything itself.
  const refusedWithdraw = await command('WITHDRAW_FIRST_AUTHORISATION', {
    timesheet_id: ROOT_1,
    expected_timesheet_id: ROOT_1,
    expected_row_signature: rowSignature(ROOT_1),
  });
  const stillLive = count(
    `select count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='${ROOT_1}' and withdrawn_at_utc is null`,
  );
  record(
    'WP23-G3-B', 'public.weekly_source_first_authorisation_withdraw_v1 (refusal path)',
    refusedWithdraw.status === 200 && refusedWithdraw.payload?.ok === false
      && Array.isArray(refusedWithdraw.payload?.checks) && stillLive === 1,
    `the owner's own W1..W9 verdict reaches the Office: code=${refusedWithdraw.payload?.code} `
    + `checks=${refusedWithdraw.payload?.checks?.length ?? 0}; the live generation is untouched `
    + `(${stillLive})`,
  );

  // -------------------------------------------------------------------------
  await postgrest.close();
  const failed = results.filter((row) => row.result === 'FAIL');
  process.stdout.write(`\nWP23 transport proof: ${results.length - failed.length}/${results.length} PASS\n`);
  for (const row of failed) process.stdout.write(`  FAIL ${row.id} ${row.owner}: ${row.detail}\n`);
  process.exitCode = failed.length === 0 ? 0 : 1;
}

await main();
