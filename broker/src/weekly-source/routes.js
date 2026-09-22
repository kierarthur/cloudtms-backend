import { parseWeeklySourceFile } from './index.js';
import {
  isManagedRootGuardRefusal,
  recordGuardRefusalAfterRollback,
} from './guard-refusal-record.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_COMMAND_BYTES = 262_144;
const MAX_UPLOAD_PREVIEW_BYTES = 25 * 1024 * 1024;

const JSON_HEADERS = Object.freeze({
  'content-type': 'application/json; charset=utf-8',
  'cache-control': 'no-store',
});

const DIRECT_RPC_COMMANDS = Object.freeze({
  QUERY_SYNC: 'weekly_source_query_sync_atomic_v1',
  REMIND_CANDIDATE: 'weekly_source_candidate_reminder_atomic_v1',
  ACKNOWLEDGE_NOTICE: 'weekly_source_office_notification_ack_atomic_v1',
  NO_SHIFTS_TO_IMPORT: 'weekly_source_no_shifts_attest_atomic_v1',
  ADMIT_SOURCE_INVOICE: 'weekly_source_invoice_admit_atomic_v1',
  MOVE_SOURCE_INVOICE: 'weekly_source_invoice_move_atomic_v1',
  ACCEPT_NHSP_SOURCE_CHARGES: 'weekly_source_charge_accept_atomic_v1',
  // WP-12 handoff N2: the Office `Unauthorise` control on a Weekly-Source-managed
  // root had no route behind it, so the screen could not complete a withdrawal at
  // all. The owner takes one jsonb request like every other Weekly Source owner
  // here, so it dispatches through the ordinary `callRpc` unchanged.
  //
  // `actor_user_id` is injected below from the authenticated user and is never
  // taken from the browser. Permission is open ruling OR-5, confirmed in
  // HANDOVER 2 round 5: the ordinary Timesheet-authorise permission, enforced
  // server side by the owner and by the unchanged `timesheet_unauthorise_atomic`
  // it calls. This entry adds no new privilege.
  WITHDRAW_FIRST_AUTHORISATION: 'weekly_source_first_authorisation_withdraw_request_v1',
  // WP-23, from the WP-19b code-completeness sweep (gap G2 / contract section 15
  // Gate 11, Annex A XSG-028).  `public.weekly_source_audit_guard_refusal_record_v1`
  // is WP-14's recorder for a managed-root guard refusal a caller obtained
  // WITHOUT raising.  A caller cannot assert a refusal: the owner reads WP-03's
  // read-only decision shim server-side and returns `reason='NOT_A_REFUSAL'` for
  // an unmanaged root, so this command records a fact rather than taking one.
  // It accepts `timesheet_id` and `entry_point` and takes the actor from the
  // authenticated Office user like every other command here (WP-14 handoff N7).
  RECORD_GUARD_REFUSAL: 'weekly_source_audit_guard_refusal_record_v1',
});

// WP-23.  Owners that take a jsonb request but REFUSE `actor_user_id` — their
// request envelopes are closed key sets checked by the owner itself.  They
// therefore cannot go through `DIRECT_RPC_COMMANDS`, which injects the actor.
// The Office actor is still established for every one of them: `requireOfficeUser`
// runs before the dispatch, exactly as it does for the actor-bearing commands.
// Raised as a question in `IMPL\handoffs\WP-23_NEEDS.md`: these Gate 11 and
// round-5 Part E owners carry no actor of their own, so the acting Office user
// is authenticated at the route and is not visible to the database.
const STRICT_ENVELOPE_RPC_COMMANDS = Object.freeze({
  // Gate 11, contract section 15: "Candidate hours-only push when approved hours
  // change".  The automatic push is WP-14's trigger on
  // `weekly_source_entitlement_heads`.  This command is the RE-DRIVE for the
  // case WP-14's own owner records as `WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD`:
  // the push boundary failed, the publication correctly stood, and the Candidate
  // was never told.  The boundary's `dedupe_key` is keyed on the Timesheet plus a
  // digest of the approved hours (WP-14 report section 6.1), so a re-drive can
  // never notify twice and unchanged hours are a no-op.
  PUSH_CANDIDATE_HOURS: {
    rpc: 'weekly_source_candidate_hours_push_v1',
    allowedKeys: ['timesheet_id'],
  },
  // Annex A R5-05, HANDOVER 2 round 5 Part E: "External C1 publication after a
  // CloudTMS head exists".  WP-06d handoff N2 fixes this envelope exactly, and
  // requires the caller to branch on the returned `outcome` and never on the
  // presence of a row.  The owner records evidence only: it cannot write a head,
  // and `EXTERNAL_PUBLICATION_PENDING_SUCCESSOR` is not an instruction to publish.
  RECORD_EXTERNAL_PUBLICATION_ARRIVAL: {
    rpc: 'weekly_source_external_publication_arrival_v1',
    allowedKeys: ['schema_version', 'external_system', 'source_identity', 'generation', 'digest'],
  },
});

// WP-23.  Owners the contract fixes at POSITIONAL signatures rather than the
// Weekly Source jsonb-request shape.  Part 1 rule 2 and this package's brief
// forbid changing a proved database owner to suit a caller, so the transport
// bends instead: these dispatch with named arguments, which is what PostgREST
// does with a request body whose keys are the parameter names.
const POSITIONAL_RPC_COMMANDS = Object.freeze({
  // Contract section 7, G3-1: the NEW first-authorisation wrapper — serial gate
  // `WORKBENCH_CANDIDATE_FIRST_AUTHORISATION`, rotation lock set, family
  // resolution, the stale refusal `WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION`,
  // ONE call to the unchanged ordinary Authorise, and the lineage generation
  // insert.  WP-19b gap G7: nothing reached the public wrapper, so none of those
  // interpositions ran on the real Office first-authorisation path.
  //
  // The signature is WP-07's and is not changed here.  `expected_row_signature`
  // is optional in the owner (a null signature skips the optimistic check), and
  // the Office control supplies the trusted signature it already holds for the
  // Unauthorise control.
  FIRST_AUTHORISE: {
    rpc: 'weekly_source_first_authorise_v1',
    actorParameter: 'p_actor_user_id',
    parameters: [
      { key: 'timesheet_id', parameter: 'p_timesheet_id', kind: 'uuid', required: true },
      { key: 'expected_timesheet_id', parameter: 'p_expected_timesheet_id', kind: 'uuid', required: false },
      { key: 'expected_row_signature', parameter: 'p_expected_row_signature', kind: 'text', required: false },
    ],
  },
  // Contract section 9, G5-6: `MANUAL_REVIEW` -> `PENDING`, resets the counter,
  // one audit row, never releases.  WP-19b gap G6: a bundle that reached
  // MANUAL_REVIEW could not be returned to the queue by any human using the
  // product.  The owner requires a non-empty reason and an ACTIVE actor.
  REOPEN_PENDING_BUNDLE: {
    rpc: 'weekly_source_pending_entitlement_bundle_reopen_v1',
    actorParameter: 'p_actor_user_id',
    parameters: [
      { key: 'pending_bundle_id', parameter: 'p_pending_bundle_id', kind: 'uuid', required: true },
      { key: 'reason', parameter: 'p_reason', kind: 'text', required: true },
    ],
  },
});

const BULK_QUERY_ACTIONS = new Set([
  'ASK_CANDIDATES',
  'SEND_MANAGER_NOW',
  'ACCEPT_SYSTEM_HOURS',
]);

const PROTECTED_ACTIONS = new Set([
  'APPROVE_PROTECTED_HOURS',
  'AMEND_PROTECTED_HOURS',
  'WITHDRAW_PROTECTED_HOURS',
  'WAIT_FOR_SOURCE',
  'ACCEPT_SOURCE_AND_RECONCILE',
  'RECORD_NOT_WORKED',
]);

const FORBIDDEN_BROWSER_FINANCIAL_KEYS = new Set([
  'pay_ex_vat',
  'charge_ex_vat',
  'pay_amount',
  'charge_amount',
  'gross_pay',
  'gross_charge',
  'residual',
  'recovery_amount',
  'overpayment_amount',
  'underpayment_amount',
  'c1_request',
  'c1_sources',
  'c1_components',
  'target_snapshot',
  'rate_classification',
]);

export class WeeklySourceRouteError extends Error {
  constructor(code, message, status = 400, details = {}) {
    super(message);
    this.name = 'WeeklySourceRouteError';
    this.code = code;
    this.status = status;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, status, details) {
  throw new WeeklySourceRouteError(code, message, status, details);
}

function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload), { status, headers: JSON_HEADERS });
}

function unwrapRpc(value, functionName) {
  let payload = value;
  if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
  if (payload && typeof payload === 'object' && !Array.isArray(payload)
      && Object.prototype.hasOwnProperty.call(payload, functionName)) {
    payload = payload[functionName];
  }
  if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
  return payload;
}

function plainObject(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is invalid.`, 400);
  }
  return value;
}

function canonicalUuid(value, code, label) {
  const text = String(value ?? '').trim().toLowerCase();
  if (!UUID_PATTERN.test(text)) fail(code, `${label} is invalid.`, 400);
  return text;
}

function assertNoBrowserFinancialFacts(value, path = '$', seen = new Set()) {
  if (!value || typeof value !== 'object') return;
  if (seen.has(value)) fail('WEEKLY_SOURCE_COMMAND_INVALID', 'The request is invalid.', 400);
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertNoBrowserFinancialFacts(entry, `${path}[${index}]`, seen));
  } else {
    for (const [key, entry] of Object.entries(value)) {
      if (FORBIDDEN_BROWSER_FINANCIAL_KEYS.has(String(key).toLowerCase())) {
        fail(
          'WEEKLY_SOURCE_BROWSER_FINANCIAL_FACT_FORBIDDEN',
          'The request contains a value that must be calculated by CloudTMS.',
          400,
          { path: `${path}.${key}` },
        );
      }
      assertNoBrowserFinancialFacts(entry, `${path}.${key}`, seen);
    }
  }
  seen.delete(value);
}

async function readJsonBody(req, maximumBytes = MAX_COMMAND_BYTES) {
  const contentLength = Number(req.headers.get('content-length') || 0);
  if (Number.isFinite(contentLength) && contentLength > maximumBytes) {
    fail('WEEKLY_SOURCE_REQUEST_TOO_LARGE', 'The request is too large.', 413);
  }
  let text;
  try {
    text = await req.text();
  } catch {
    fail('WEEKLY_SOURCE_REQUEST_INVALID', 'The request could not be read.', 400);
  }
  if (new TextEncoder().encode(text).byteLength > maximumBytes) {
    fail('WEEKLY_SOURCE_REQUEST_TOO_LARGE', 'The request is too large.', 413);
  }
  try {
    return plainObject(JSON.parse(text || '{}'), 'WEEKLY_SOURCE_REQUEST_INVALID', 'Request');
  } catch (error) {
    if (error instanceof WeeklySourceRouteError) throw error;
    fail('WEEKLY_SOURCE_REQUEST_INVALID', 'The request must be valid JSON.', 400);
  }
}

function queryRequest(url, actorUserId) {
  const request = { actor_user_id: actorUserId };
  for (const [key, value] of url.searchParams.entries()) {
    if (key === 'actor_user_id') continue;
    if (Object.prototype.hasOwnProperty.call(request, key)) {
      const current = request[key];
      request[key] = Array.isArray(current) ? [...current, value] : [current, value];
    } else request[key] = value;
  }
  return request;
}

async function requireOfficeUser(req, env, dependencies) {
  if (typeof dependencies.requireUser !== 'function') {
    fail('WEEKLY_SOURCE_AUTHORITY_UNAVAILABLE', 'Weekly source access is unavailable.', 503);
  }
  const user = await dependencies.requireUser(env, req, ['admin']);
  if (!user?.id) fail('WEEKLY_SOURCE_AUTHENTICATION_REQUIRED', 'Sign in is required.', 401);
  return { ...user, id: canonicalUuid(user.id, 'WEEKLY_SOURCE_ACTOR_INVALID', 'Office user') };
}

async function callRpc(dependencies, name, pRequest, timeoutMs = 45_000) {
  if (typeof dependencies.rpc !== 'function') {
    fail('WEEKLY_SOURCE_DATA_AUTHORITY_UNAVAILABLE', 'Weekly source data is unavailable.', 503);
  }
  const value = await dependencies.rpc(name, { p_request: pRequest }, { timeoutMs });
  return unwrapRpc(value, name);
}

/**
 * WP-23.  The same transport as `callRpc`, for an owner whose signature the
 * contract fixes positionally.  PostgREST maps a request body's keys onto the
 * function's own parameter names, so the argument object IS the call.
 */
async function callPositionalRpc(dependencies, name, namedArguments, timeoutMs = 45_000) {
  if (typeof dependencies.rpc !== 'function') {
    fail('WEEKLY_SOURCE_DATA_AUTHORITY_UNAVAILABLE', 'Weekly source data is unavailable.', 503);
  }
  const value = await dependencies.rpc(name, namedArguments, { timeoutMs });
  return unwrapRpc(value, name);
}

/**
 * WP-23.  Build the named arguments for a positional command from the browser
 * payload.  The actor is ALWAYS the authenticated Office user and is never taken
 * from the browser, exactly as `handleCommand` does for every other command.
 */
function positionalArguments(definition, payload, actorUserId) {
  const namedArguments = { [definition.actorParameter]: actorUserId };
  for (const spec of definition.parameters) {
    const present = Object.prototype.hasOwnProperty.call(payload, spec.key);
    const raw = present ? payload[spec.key] : null;
    if (spec.kind === 'uuid') {
      if (!present || raw === null || raw === '') {
        if (spec.required) {
          fail('WEEKLY_SOURCE_COMMAND_INVALID', `${spec.key} is required.`, 400);
        }
        namedArguments[spec.parameter] = null;
        continue;
      }
      namedArguments[spec.parameter] = canonicalUuid(raw, 'WEEKLY_SOURCE_COMMAND_INVALID', spec.key);
      continue;
    }
    const text = raw === null || raw === undefined ? '' : String(raw).trim();
    if (!text) {
      if (spec.required) {
        fail('WEEKLY_SOURCE_COMMAND_INVALID', `${spec.key} is required.`, 400);
      }
      namedArguments[spec.parameter] = null;
      continue;
    }
    namedArguments[spec.parameter] = text;
  }
  return namedArguments;
}

/**
 * WP-23.  The request for an owner whose envelope is a closed key set the owner
 * itself checks.  An unknown key is refused HERE with a transport error rather
 * than being sent on to raise `22023` inside the database, and `actor_user_id`
 * is refused explicitly because these owners have no actor parameter at all.
 */
function strictEnvelopeRequest(definition, payload) {
  const allowed = new Set(definition.allowedKeys);
  const request = {};
  for (const [key, value] of Object.entries(payload)) {
    if (key === 'actor_user_id') {
      fail(
        'WEEKLY_SOURCE_COMMAND_ACTOR_NOT_ACCEPTED',
        'This action does not take an actor from the browser.',
        400,
      );
    }
    if (!allowed.has(key)) {
      fail('WEEKLY_SOURCE_COMMAND_INVALID', `${key} is not part of this action.`, 400);
    }
    request[key] = value;
  }
  return request;
}

/**
 * WP-23, ruling A2 / decision D13.  Every durable Office call this route makes
 * can meet the managed-root rotation guard, which refuses by RAISING and so
 * rolls its own transaction back.  The refusal is recorded after the rollback on
 * a fresh transaction, with the SAME correlation identity the attempt carried,
 * and the refusal itself is rethrown unchanged whether the record succeeds or
 * not (WP-14c handoff N1, rules 1 to 4).
 */
async function withRefusalRecording(dependencies, caller, actorUserId, attempt) {
  const correlationId = `ws62-office:${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
  try {
    return await attempt(correlationId);
  } catch (error) {
    if (isManagedRootGuardRefusal(error)) {
      await recordGuardRefusalAfterRollback({
        rpc: dependencies.rpc,
        error,
        correlationId,
        caller,
        actorUserId,
        onRecordFailure: typeof dependencies.logGuardRefusalRecordFailure === 'function'
          ? dependencies.logGuardRefusalRecordFailure
          : null,
      });
    }
    throw error;
  }
}

function weeklySourceDeploymentContext(env) {
  const agencyId = canonicalUuid(
    env?.MYTMS_OFFICE_AGENCY_ID,
    'WEEKLY_SOURCE_SETTINGS_DEPLOYMENT_UNAVAILABLE',
    'Office Agency',
  );
  const environment = String(env?.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase();
  if (!['TEST', 'LIVE'].includes(environment)) {
    fail(
      'WEEKLY_SOURCE_SETTINGS_DEPLOYMENT_UNAVAILABLE',
      'Weekly source settings are unavailable.',
      503,
    );
  }
  return { agency_id: agencyId, environment };
}

function settingsQuery(url, allowedKeys = []) {
  const allowed = new Set(allowedKeys);
  const result = {};
  for (const [key, value] of url.searchParams.entries()) {
    if (!allowed.has(key) || Object.prototype.hasOwnProperty.call(result, key)) {
      fail('WEEKLY_SOURCE_SETTINGS_REQUEST_INVALID', 'The settings request is invalid.', 400);
    }
    result[key] = value;
  }
  return result;
}

/**
 * WP-23.  Gate 11's read owners and WP-06d's pending-inputs owner each check a
 * closed request key set themselves.  A query string on those routes is refused
 * at the transport rather than forwarded to raise `22023` in the database.
 */
function assertNoQueryString(url) {
  for (const _key of url.searchParams.keys()) {
    fail('WEEKLY_SOURCE_READ_REQUEST_INVALID', 'This read takes no options.', 400);
  }
}

function assertNoBrowserDeploymentScope(body) {
  for (const key of ['actor_user_id', 'agency_id', 'environment']) {
    if (Object.prototype.hasOwnProperty.call(body, key)) {
      fail(
        'WEEKLY_SOURCE_SETTINGS_SCOPE_FORBIDDEN',
        'The settings request contains a value controlled by CloudTMS.',
        400,
      );
    }
  }
}

async function handleSettingsGet(req, env, dependencies, user, kind, id = null) {
  const deployment = weeklySourceDeploymentContext(env);
  const query = settingsQuery(new URL(req.url), id ? ['effective_date'] : []);
  const definitions = {
    global: ['weekly_source_global_settings_get_v1', null],
    groups: ['weekly_source_source_groups_get_v1', null],
    clients: ['weekly_source_client_settings_get_v1', 'client_id'],
    contracts: ['weekly_source_contract_settings_get_v1', 'contract_id'],
  };
  const [rpcName, idKey] = definitions[kind] || [];
  if (!rpcName) fail('WEEKLY_SOURCE_SETTINGS_ROUTE_INVALID', 'This settings route is unavailable.', 404);
  const result = await callRpc(dependencies, rpcName, {
    actor_user_id: user.id,
    ...deployment,
    ...query,
    ...(idKey ? { [idKey]: canonicalUuid(id, 'WEEKLY_SOURCE_SETTINGS_ID_INVALID', kind) } : {}),
  });
  return jsonResponse(200, result);
}

async function handleSettingsPut(req, env, dependencies, user, kind, id = null) {
  const deployment = weeklySourceDeploymentContext(env);
  settingsQuery(new URL(req.url));
  const body = await readJsonBody(req, 64 * 1024);
  assertNoBrowserDeploymentScope(body);
  assertNoBrowserFinancialFacts(body);
  const definitions = {
    global: ['weekly_source_global_settings_save_atomic_v1', null],
    groups: ['weekly_source_source_group_save_atomic_v1', null],
    clients: ['weekly_source_client_settings_save_atomic_v1', 'client_id'],
    contracts: ['weekly_source_contract_settings_save_atomic_v1', 'contract_id'],
  };
  const [rpcName, idKey] = definitions[kind] || [];
  if (!rpcName) fail('WEEKLY_SOURCE_SETTINGS_ROUTE_INVALID', 'This settings route is unavailable.', 404);
  const result = await callRpc(dependencies, rpcName, {
    ...body,
    actor_user_id: user.id,
    ...deployment,
    ...(idKey ? { [idKey]: canonicalUuid(id, 'WEEKLY_SOURCE_SETTINGS_ID_INVALID', kind) } : {}),
  });
  return jsonResponse(200, result);
}

async function handleWorkspace(req, env, dependencies, user) {
  const url = new URL(req.url);
  const result = await callRpc(
    dependencies,
    'weekly_source_office_workspace_v1',
    queryRequest(url, user.id),
  );
  return jsonResponse(200, result);
}

async function handleOfficeNotifications(req, env, dependencies, user) {
  const url = new URL(req.url);
  const allowed = new Set(['limit', 'open_only']);
  for (const key of url.searchParams.keys()) {
    if (!allowed.has(key)) {
      fail('WEEKLY_SOURCE_NOTIFICATIONS_QUERY_INVALID', 'This notification filter is unavailable.', 400);
    }
  }
  const limitText = String(url.searchParams.get('limit') || '100').trim();
  const openOnlyText = String(url.searchParams.get('open_only') || 'false').trim().toLowerCase();
  if (!/^\d+$/.test(limitText) || Number(limitText) < 1 || Number(limitText) > 100
      || !['true', 'false'].includes(openOnlyText)) {
    fail('WEEKLY_SOURCE_NOTIFICATIONS_QUERY_INVALID', 'The notification filter is invalid.', 400);
  }
  const result = await callRpc(dependencies, 'weekly_source_office_notifications_list_v1', {
    actor_user_id: user.id,
    limit: Number(limitText),
    open_only: openOnlyText === 'true',
  });
  return jsonResponse(200, result);
}

async function handleTimesheetPresentation(req, env, dependencies, user, timesheetId) {
  const url = new URL(req.url);
  const result = await callRpc(dependencies, 'weekly_source_office_timesheet_presentation_v1', {
    ...queryRequest(url, user.id),
    timesheet_id: canonicalUuid(
      timesheetId,
      'WEEKLY_SOURCE_TIMESHEET_INVALID',
      'Timesheet',
    ),
  });
  return jsonResponse(200, result);
}

/**
 * WP-23, WP-19b gap G2.  Contract section 15, Gate 11: "plain-English Timesheet
 * Audit chronology ... exports separating submitted, source, approved or paid,
 * and invoice movements", with the exit criterion "Office can explain what was
 * submitted, approved, invoiced, paid and later changed without reading raw
 * JSON."  Both owners were built, granted and registered, and had no caller
 * anywhere: Gate 11 had no transport at all.
 *
 * Both are STABLE reads keyed on one Timesheet, and BOTH refuse any key other
 * than `timesheet_id` — so, unlike every other read on this boundary, the
 * authenticated Office actor cannot be passed to the database.  The actor is
 * still established: `requireOfficeUser` runs first, as it does for every route
 * here.  The awkwardness is raised in `IMPL\handoffs\WP-23_NEEDS.md` rather than
 * closed by editing WP-14's owners.
 */
async function handleTimesheetAuditChronology(req, env, dependencies, user, timesheetId) {
  // The owner's request is a closed one-key set, so a query string is refused
  // rather than forwarded.
  assertNoQueryString(new URL(req.url));
  const result = await callRpc(dependencies, 'weekly_source_timesheet_audit_chronology_v1', {
    timesheet_id: canonicalUuid(timesheetId, 'WEEKLY_SOURCE_TIMESHEET_INVALID', 'Timesheet'),
  });
  return jsonResponse(200, result);
}

async function handleTimesheetHoursExport(req, env, dependencies, user, timesheetId) {
  assertNoQueryString(new URL(req.url));
  const result = await callRpc(dependencies, 'weekly_source_timesheet_hours_export_v1', {
    timesheet_id: canonicalUuid(timesheetId, 'WEEKLY_SOURCE_TIMESHEET_INVALID', 'Timesheet'),
  });
  return jsonResponse(200, result);
}

/**
 * WP-23, WP-19b gap G11 / Annex A R5-05.  The read half of HANDOVER 2 round 5
 * Part E: which successor inputs are still outstanding for a root whose CloudTMS
 * head is authoritative.  WP-06d's owner fixes the envelope as exactly
 * `schema_version` plus `root_timesheet_id`, and refuses anything else.
 */
async function handleExternalPublicationPendingInputs(req, env, dependencies, user, timesheetId) {
  assertNoQueryString(new URL(req.url));
  const result = await callRpc(
    dependencies,
    'weekly_source_external_publication_pending_inputs_v1',
    {
      schema_version: 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_PENDING_INPUTS_V1',
      root_timesheet_id: canonicalUuid(timesheetId, 'WEEKLY_SOURCE_TIMESHEET_INVALID', 'Timesheet'),
    },
  );
  return jsonResponse(200, result);
}

async function handleCommand(req, env, ctx, dependencies, user) {
  const body = await readJsonBody(req);
  const action = String(body.action ?? '').trim().toUpperCase();
  const payload = plainObject(body.payload ?? {}, 'WEEKLY_SOURCE_COMMAND_INVALID', 'Command');
  assertNoBrowserFinancialFacts(payload);
  const request = { ...payload, actor_user_id: user.id };

  if (BULK_QUERY_ACTIONS.has(action)) {
    try {
      const result = await callRpc(
        dependencies,
        'weekly_source_office_bulk_query_action_atomic_v1',
        { ...request, action },
      );
      return jsonResponse(200, result);
    } catch (error) {
      if (action === 'ASK_CANDIDATES'
          && /WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE/i.test(
            `${String(error?.code || '')} ${String(error?.message || '')}`
          )) {
        return jsonResponse(200, {
          ok: false,
          status: 'UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT',
          accepted: false,
        });
      }
      throw error;
    }
  }

  if (action === 'FINALISE_WEEK') {
    if (typeof dependencies.orchestrateFinalisation !== 'function') {
      fail(
        'WEEKLY_SOURCE_FINALISATION_UNAVAILABLE',
        'The final source cannot be completed right now.',
        503,
      );
    }
    const result = await dependencies.orchestrateFinalisation({
      request,
      actor: user,
      env,
      ctx,
    });
    return jsonResponse(200, result);
  }

  if (action === 'RECOVER_FINALISED_PAY') {
    if (typeof dependencies.recoverFinalisedPay !== 'function') {
      fail(
        'WEEKLY_SOURCE_FINALISATION_RECOVERY_UNAVAILABLE',
        'The approved-hours update cannot be checked right now.',
        503,
      );
    }
    const result = await dependencies.recoverFinalisedPay({
      request,
      actor: user,
      env,
      ctx,
    });
    return jsonResponse(200, result);
  }

  // WP-23.  The positional and strict-envelope owners are dispatched before the
  // jsonb-request set, because their request is NOT `{...payload, actor_user_id}`.
  if (POSITIONAL_RPC_COMMANDS[action]) {
    const definition = POSITIONAL_RPC_COMMANDS[action];
    const namedArguments = positionalArguments(definition, payload, user.id);
    const result = await withRefusalRecording(
      dependencies,
      `broker:weekly-source:${definition.rpc}`,
      user.id,
      () => callPositionalRpc(dependencies, definition.rpc, namedArguments),
    );
    return jsonResponse(200, result);
  }
  if (STRICT_ENVELOPE_RPC_COMMANDS[action]) {
    const definition = STRICT_ENVELOPE_RPC_COMMANDS[action];
    const result = await withRefusalRecording(
      dependencies,
      `broker:weekly-source:${definition.rpc}`,
      user.id,
      () => callRpc(dependencies, definition.rpc, strictEnvelopeRequest(definition, payload)),
    );
    return jsonResponse(200, result);
  }
  if (DIRECT_RPC_COMMANDS[action]) {
    try {
      const result = await withRefusalRecording(
        dependencies,
        `broker:weekly-source:${DIRECT_RPC_COMMANDS[action]}`,
        user.id,
        () => callRpc(dependencies, DIRECT_RPC_COMMANDS[action], request),
      );
      return jsonResponse(200, result);
    } catch (error) {
      if (action === 'ASK_CANDIDATES'
          && /WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE/i.test(
            `${String(error?.code || '')} ${String(error?.message || '')}`
          )) {
        return jsonResponse(200, {
          ok: false,
          status: 'UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT',
          candidate_id: request.candidate_id ?? null,
          accepted: false,
        });
      }
      throw error;
    }
  }
  if (PROTECTED_ACTIONS.has(action)) {
    if (typeof dependencies.orchestrateProtectedAction !== 'function') {
      fail(
        'WEEKLY_SOURCE_PROTECTED_ACTION_UNAVAILABLE',
        'Approved hours cannot be changed right now.',
        503,
      );
    }
    const result = await dependencies.orchestrateProtectedAction({
      action,
      request,
      actor: user,
      env,
      ctx,
    });
    return jsonResponse(200, result);
  }
  if (action === 'PREVIEW_CORRECT_FINAL_SOURCE') {
    if (typeof dependencies.previewCorrectFinalSource !== 'function') {
      fail('WEEKLY_SOURCE_CORRECTION_UNAVAILABLE', 'The final source cannot be corrected right now.', 503);
    }
    const result = await dependencies.previewCorrectFinalSource({ request, actor: user, env, ctx });
    return jsonResponse(200, result);
  }
  if (action === 'APPLY_CORRECT_FINAL_SOURCE') {
    if (typeof dependencies.applyCorrectFinalSource !== 'function') {
      fail('WEEKLY_SOURCE_CORRECTION_UNAVAILABLE', 'The final source cannot be corrected right now.', 503);
    }
    const result = await dependencies.applyCorrectFinalSource({ request, actor: user, env, ctx });
    return jsonResponse(200, result);
  }
  fail('WEEKLY_SOURCE_COMMAND_NOT_SUPPORTED', 'This action is not available.', 400);
}

async function loadUploadBytes(body, env, dependencies) {
  const fileKey = String(body.file_key ?? '').trim();
  if (!fileKey || fileKey.length > 1024) {
    fail('WEEKLY_SOURCE_FILE_REQUIRED', 'Choose a source file.', 400);
  }
  if (typeof dependencies.loadFileBytes !== 'function') {
    fail('WEEKLY_SOURCE_FILE_STORAGE_UNAVAILABLE', 'The source file cannot be read right now.', 503);
  }
  const loaded = await dependencies.loadFileBytes(env, fileKey, {
    maximumBytes: MAX_UPLOAD_PREVIEW_BYTES,
  });
  const bytes = loaded instanceof Uint8Array
    ? loaded
    : loaded instanceof ArrayBuffer
      ? new Uint8Array(loaded)
      : null;
  if (!bytes || bytes.byteLength === 0) fail('WEEKLY_SOURCE_FILE_EMPTY', 'The source file is empty.', 400);
  if (bytes.byteLength > MAX_UPLOAD_PREVIEW_BYTES) {
    fail('WEEKLY_SOURCE_FILE_TOO_LARGE', 'The source file is too large.', 413);
  }
  return { fileKey, bytes };
}

async function handleUploadPreview(req, env, dependencies, user) {
  const body = await readJsonBody(req, 64 * 1024);
  const { fileKey, bytes } = await loadUploadBytes(body, env, dependencies);
  let parsed;
  if (typeof dependencies.previewUpload === 'function') {
    ({ parsed } = await dependencies.previewUpload({
      body, bytes, actor: user, env, fileKey, parseWeeklySourceFile,
    }));
  } else {
    const parserOptions = plainObject(body.parser_options ?? {}, 'WEEKLY_SOURCE_PARSER_OPTIONS_INVALID', 'Source options');
    parsed = await parseWeeklySourceFile(bytes, parserOptions);
  }
  if (typeof dependencies.previewUpload !== 'function'
      && typeof dependencies.recordUploadPreview === 'function') {
    await dependencies.recordUploadPreview({ body, parsed, actor: user, env, fileKey });
  }
  return jsonResponse(200, {
    ok: parsed.ok === true,
    file_key: fileKey,
    preview: parsed,
  });
}

async function handleUploadAccept(req, env, ctx, dependencies, user) {
  const body = await readJsonBody(req, 128 * 1024);
  assertNoBrowserFinancialFacts(body);
  const { fileKey, bytes } = await loadUploadBytes(body, env, dependencies);
  if (typeof dependencies.acceptUpload !== 'function') {
    fail('WEEKLY_SOURCE_UPLOAD_UNAVAILABLE', 'The source file cannot be accepted right now.', 503);
  }
  const result = await dependencies.acceptUpload({
    body,
    bytes,
    fileKey,
    actor: user,
    env,
    ctx,
    parseWeeklySourceFile,
  });
  const modeA = await dispatchModeA(dependencies, env, user.id, result?.publication_id);
  return jsonResponse(200, modeA ? { ...result, mode_a: modeA } : result);
}

// G8-1 (XSG-009). `24 §14`: "The unified import acceptance route must dispatch
// this policy to the existing validation-only Mode A owner. Merely labelling
// the row as Timesheet evidence is insufficient." `25 §9` says the same of the
// unified upload owner. The acceptance route therefore hands the published
// projection to `public.weekly_source_mode_a_dispatch_atomic_v1`, which bridges
// the signed-Timesheet-authority rows into the established import-review
// Weekly validation-only route and records that owner's comparisons.
//
// Nothing here compares hours, decides a reference, sends an email or
// authorises anything: the established owners do all of it. When a publication
// holds no signed-Timesheet-authority row the dispatcher returns
// `dispatched: false` and the response is unchanged.
//
// WP-37, closing WP-31's finding F2. Dispatching was only half the route.
// `24 §14` also says "a reference is written only after the shift hours match",
// and `25 §9` "A matching source row writes the reference… Complete coverage
// may auto-authorise only when every shift has exactly one match". Both rules
// lived in `public.weekly_source_mode_a_reference_apply_atomic_v1`, which had
// NO caller: after two `EXACT_MATCH` comparisons the reference was never
// written, no reference-apply operation existed and the signed Timesheet stayed
// `STORED`. The only route that reached the rule was the ordinary Imports
// screen, which bypasses the Weekly Source ledger entirely — a rule that holds
// on one route and not the other is not implemented.
//
// So the acceptance route now calls the second owner too, exactly as the
// transport package wired the first-authorisation owner: the DATABASE decides
// (match state, reference, auto-authorisation are all read from the established
// owners inside that RPC), the broker reproduces no rule of its own, and a
// failure fails CLOSED — nothing is written and the caller is told.
async function dispatchModeA(dependencies, env, actorUserId, publicationId) {
  if (!publicationId) return null;
  const dispatched = await modeARpc(
    dependencies, 'weekly_source_mode_a_dispatch_atomic_v1', publicationId,
    { actor_user_id: actorUserId, publication_id: publicationId },
    'WEEKLY_SOURCE_MODE_A_DISPATCH_FAILED',
    'The signed-Timesheet evidence could not be sent for checking.',
  );
  if (dispatched.dispatched !== true) return null;
  const applied = await modeARpc(
    dependencies, 'weekly_source_mode_a_reference_apply_atomic_v1', publicationId,
    { actor_user_id: actorUserId, publication_id: publicationId },
    'WEEKLY_SOURCE_MODE_A_REFERENCE_APPLY_FAILED',
    'The source references could not be applied to the signed Timesheets.',
  );
  const operations = Array.isArray(applied.operations) ? applied.operations : [];
  const followUp = await runModeAAuthorisationFollowUp(dependencies, env, actorUserId, applied);
  return Object.freeze({
    dispatched: true,
    timesheet_authority_rows: dispatched.timesheet_authority_rows ?? 0,
    comparisons_written: dispatched.comparisons_written ?? 0,
    // The established owner's own answers, recorded and passed on unchanged.
    reference_apply_operations: operations.length,
    auto_authorise_requested: operations.filter(
      (operation) => operation?.auto_authorise_requested === true,
    ).length,
    superseded_heads_refused: applied.superseded_heads_refused ?? 0,
    auto_authorise_follow_up: followUp,
  });
}

/**
 * WP-37, reporting WP-31's finding F5 rather than pretending it is closed.
 *
 * `acceptUpload` commits the publication BEFORE Mode A runs, so by the time
 * either owner here can refuse, the projection is already `CURRENT`. Making the
 * two atomic would need one owner and is not this package's change. What this
 * does fix is the reading: every Mode A failure now carries the publication and
 * its state, so Office is not shown a bare error for an upload that was in fact
 * published, and re-accepting the same file re-runs only this step. The
 * underlying database code is preserved in `reason` and the HTTP status is kept
 * where the transport gave one.
 */
async function modeARpc(dependencies, name, publicationId, request, code, message) {
  const details = { publication_id: publicationId, publication_state: 'CURRENT' };
  let result;
  try {
    result = await callRpc(dependencies, name, request);
  } catch (error) {
    if (error instanceof WeeklySourceRouteError) throw error;
    const status = Number(error?.status);
    fail(code, message, Number.isInteger(status) && status >= 400 && status <= 599 ? status : 502, {
      ...details,
      reason: String(error?.json?.message || error?.message || 'UNKNOWN').slice(0, 300),
    });
  }
  if (result?.ok !== true) fail(code, message, 502, details);
  return result;
}

/**
 * WP-37. `hr_weekly_apply_transactional` deliberately does not authorise
 * anything: it commits the reference work, records the operation and NAMES the
 * post-commit targets in `auto_authorise_timesheet_ids`. The ordinary Imports
 * route then runs `createImportReviewPostCommitRunner`'s follow-up, which
 * settles TSFIN and calls `timesheet_authorise_bulk_atomic`. Without that step
 * `25 §9`'s "Complete coverage may auto-authorise" stops one move short on this
 * route and the signed Timesheet stays `STORED`.
 *
 * So the acceptance route runs the SAME established follow-up, with the same
 * `(import_id, operation_id, request_hash)` handle the database just recorded.
 * No rule is reproduced here: which Timesheets may be authorised, and whether
 * they are, remain entirely the established owners' decisions. It fails CLOSED
 * — when the database named targets and the follow-up is not available, or it
 * does not complete, the caller is told rather than being shown a silent
 * success.
 */
async function runModeAAuthorisationFollowUp(dependencies, env, actorUserId, applied) {
  const clients = Array.isArray(applied.clients) ? applied.clients : [];
  const pending = clients.filter((client) => client?.applied === true
    && Array.isArray(client?.auto_authorise_timesheet_ids)
    && client.auto_authorise_timesheet_ids.length > 0);
  if (pending.length === 0) return Object.freeze({ required: false, completed: 0 });
  if (typeof dependencies.runImportReviewPostCommit !== 'function') {
    fail(
      'WEEKLY_SOURCE_MODE_A_AUTHORISATION_FOLLOW_UP_UNAVAILABLE',
      'The signed Timesheets could not be authorised after their references were applied.',
      503,
      { publication_id: applied.publication_id, publication_state: 'CURRENT' },
    );
  }
  let completed = 0;
  for (const client of pending) {
    let result = null;
    try {
      result = await dependencies.runImportReviewPostCommit(env, {
        importId: client.import_id,
        operationId: client.operation_id,
        actorUserId,
        requestHash: client.request_hash,
        applyResult: client.apply_result,
      });
    } catch (error) {
      fail(
        'WEEKLY_SOURCE_MODE_A_AUTHORISATION_FOLLOW_UP_FAILED',
        'The signed Timesheets could not be authorised after their references were applied.',
        502,
        {
          publication_id: applied.publication_id,
          publication_state: 'CURRENT',
          import_id: client.import_id,
          reason: String(error?.message || 'UNKNOWN').slice(0, 200),
        },
      );
    }
    if (result?.ok !== true) {
      fail(
        'WEEKLY_SOURCE_MODE_A_AUTHORISATION_FOLLOW_UP_INCOMPLETE',
        'The signed Timesheets could not be authorised after their references were applied.',
        502,
        {
          publication_id: applied.publication_id,
          publication_state: 'CURRENT',
          import_id: client.import_id,
          tsfin_follow_up_status: result?.tsfin_follow_up_status ?? null,
        },
      );
    }
    completed += 1;
  }
  return Object.freeze({ required: true, completed });
}

function routeErrorResponse(error) {
  if (error instanceof WeeklySourceRouteError) {
    return jsonResponse(error.status, {
      ok: false,
      error_code: error.code,
      message: error.message,
      ...(Object.keys(error.details).length ? { details: error.details } : {}),
    });
  }
  const status = Number(error?.status);
  const message = String(error?.message || 'Weekly source request failed.');
  const errorCode = String(error?.code || '');
  const conflict = /STALE|CONFLICT|BUSY|LOCK|SUPERSEDED|FINALIS/i.test(`${errorCode} ${message}`)
    || errorCode === 'C1_DURABLE_RECOVERY_REQUIRED'
    || errorCode === 'C1_DURABLE_RECOVERY_NOT_REQUIRED';
  return jsonResponse(
    Number.isInteger(status) && status >= 400 && status <= 599 ? status : (conflict ? 409 : 500),
    {
      ok: false,
      error_code: errorCode || (conflict ? 'WEEKLY_SOURCE_CONFLICT' : 'WEEKLY_SOURCE_REQUEST_FAILED'),
      message,
    },
  );
}

/**
 * The single Office HTTP boundary for Weekly Source. It validates transport
 * shapes and actor identity, but all business authority remains in the
 * database/service owners supplied through dependencies.
 */
export async function dispatchWeeklySourceRequest(req, env, ctx, dependencies = {}) {
  const url = new URL(req.url);
  const path = url.pathname;
  const officePrefix = '/api/weekly-source/v1';
  if (!path.startsWith(`${officePrefix}/`) && path !== officePrefix) return null;

  try {
    const user = await requireOfficeUser(req, env, dependencies);
    if (req.method === 'GET' && path === `${officePrefix}/workspace`) {
      return await handleWorkspace(req, env, dependencies, user);
    }
    if (req.method === 'GET' && path === `${officePrefix}/notifications`) {
      return await handleOfficeNotifications(req, env, dependencies, user);
    }
    const presentationMatch = path.match(/^\/api\/weekly-source\/v1\/timesheets\/([0-9a-f-]+)\/presentation$/i);
    if (req.method === 'GET' && presentationMatch) {
      return await handleTimesheetPresentation(req, env, dependencies, user, presentationMatch[1]);
    }
    // WP-23 / WP-19b G2: Gate 11's two read owners, which had no transport.
    const auditMatch = path.match(/^\/api\/weekly-source\/v1\/timesheets\/([0-9a-f-]+)\/audit$/i);
    if (req.method === 'GET' && auditMatch) {
      return await handleTimesheetAuditChronology(req, env, dependencies, user, auditMatch[1]);
    }
    const hoursExportMatch = path.match(/^\/api\/weekly-source\/v1\/timesheets\/([0-9a-f-]+)\/hours-export$/i);
    if (req.method === 'GET' && hoursExportMatch) {
      return await handleTimesheetHoursExport(req, env, dependencies, user, hoursExportMatch[1]);
    }
    // WP-23 / WP-19b G11: round 5 Part E, the outstanding successor inputs.
    const pendingInputsMatch = path.match(
      /^\/api\/weekly-source\/v1\/timesheets\/([0-9a-f-]+)\/external-publication\/pending-inputs$/i,
    );
    if (req.method === 'GET' && pendingInputsMatch) {
      return await handleExternalPublicationPendingInputs(
        req, env, dependencies, user, pendingInputsMatch[1],
      );
    }
    if (path === `${officePrefix}/settings/global` && req.method === 'GET') {
      return await handleSettingsGet(req, env, dependencies, user, 'global');
    }
    if (path === `${officePrefix}/settings/global` && req.method === 'PUT') {
      return await handleSettingsPut(req, env, dependencies, user, 'global');
    }
    if (path === `${officePrefix}/settings/source-groups` && req.method === 'GET') {
      return await handleSettingsGet(req, env, dependencies, user, 'groups');
    }
    if (path === `${officePrefix}/settings/source-groups` && req.method === 'PUT') {
      return await handleSettingsPut(req, env, dependencies, user, 'groups');
    }
    const clientSettingsMatch = path.match(/^\/api\/weekly-source\/v1\/settings\/clients\/([0-9a-f-]+)$/i);
    if (clientSettingsMatch && req.method === 'GET') {
      return await handleSettingsGet(req, env, dependencies, user, 'clients', clientSettingsMatch[1]);
    }
    if (clientSettingsMatch && req.method === 'PUT') {
      return await handleSettingsPut(req, env, dependencies, user, 'clients', clientSettingsMatch[1]);
    }
    const contractSettingsMatch = path.match(/^\/api\/weekly-source\/v1\/settings\/contracts\/([0-9a-f-]+)$/i);
    if (contractSettingsMatch && req.method === 'GET') {
      return await handleSettingsGet(req, env, dependencies, user, 'contracts', contractSettingsMatch[1]);
    }
    if (contractSettingsMatch && req.method === 'PUT') {
      return await handleSettingsPut(req, env, dependencies, user, 'contracts', contractSettingsMatch[1]);
    }
    if (req.method === 'POST' && path === `${officePrefix}/uploads/preview`) {
      return await handleUploadPreview(req, env, dependencies, user);
    }
    if (req.method === 'POST' && path === `${officePrefix}/uploads/accept`) {
      return await handleUploadAccept(req, env, ctx, dependencies, user);
    }
    if (req.method === 'POST' && path === `${officePrefix}/commands`) {
      return await handleCommand(req, env, ctx, dependencies, user);
    }
    return jsonResponse(404, {
      ok: false,
      error_code: 'WEEKLY_SOURCE_ROUTE_NOT_FOUND',
      message: 'This Weekly source route is not available.',
    });
  } catch (error) {
    return routeErrorResponse(error);
  }
}

export const WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_OFFICE_HTTP_V1',
  prefix: '/api/weekly-source/v1',
  directRpcCommands: DIRECT_RPC_COMMANDS,
  // WP-23.  The commands whose owners do not take the Weekly Source
  // jsonb-request shape, kept separate so a reader can see at a glance which
  // owner signature each one is bound to.
  strictEnvelopeRpcCommands: Object.freeze(Object.fromEntries(
    Object.entries(STRICT_ENVELOPE_RPC_COMMANDS).map(([action, definition]) => [action, definition.rpc]),
  )),
  positionalRpcCommands: Object.freeze(Object.fromEntries(
    Object.entries(POSITIONAL_RPC_COMMANDS).map(([action, definition]) => [action, definition.rpc]),
  )),
  // WP-23 / WP-19b G2 and G11.  Gate 11's audit chronology and hours export, and
  // round 5 Part E's outstanding-successor-inputs read.
  readRoutes: Object.freeze([
    '/timesheets/:timesheetId/presentation',
    '/timesheets/:timesheetId/audit',
    '/timesheets/:timesheetId/hours-export',
    '/timesheets/:timesheetId/external-publication/pending-inputs',
  ]),
  // WP-23, ruling A2 / decision D13: a managed-root guard refusal raised by any
  // command on this boundary is recorded after the rollback, on a separate
  // transaction, with the correlation identity the attempt carried.
  guardRefusalRecordedAfterRollback: true,
  protectedActions: Object.freeze([...PROTECTED_ACTIONS]),
  settingsRoutes: Object.freeze([
    '/settings/global',
    '/settings/source-groups',
    '/settings/clients/:clientId',
    '/settings/contracts/:contractId',
  ]),
  // G8-1 (XSG-009). The upload acceptance route dispatches
  // signed-Timesheet-authority rows to the established validation-only Mode A
  // owner; it never routes them to source finalisation, and it never carries a
  // comparison, reference or email decision of its own (`24 §14`; `25 §9`).
  modeADispatch: Object.freeze({
    route: '/uploads/accept',
    owner: 'weekly_source_mode_a_dispatch_atomic_v1',
    establishedComparisonOwner: 'hr_weekly_validation_preview',
    establishedReferenceOwner: 'hr_weekly_apply_transactional',
    referenceApplyOwner: 'weekly_source_mode_a_reference_apply_atomic_v1',
    // WP-37 (WP-31 finding F2): the reference-apply owner is CALLED by this
    // route, not merely named by this contract. Proof:
    // `tests/weekly-source/wp37-mode-a-route-e2e.mjs`.
    referenceApplyCalledByRoute: true,
    candidateQueried: false,
    secureManagerLink: false,
  }),
  browserFinancialFactsAccepted: false,
});
