import {
  candidateAppBackendInternals,
  handleCandidateAppRequest
} from './candidate-app-backend.js';
import { controlPlaneRpc } from '../../candidate-broker/src/control-plane-client.js';

export const CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH =
  '/api/manager-email-e2e-proof/start';

export const CANDIDATE_MANAGER_EMAIL_E2E_IDS = Object.freeze({
  actor: 'f1000000-0000-4000-8000-000000000001',
  client: 'f1000000-0000-4000-8000-000000000002',
  candidate: 'f1000000-0000-4000-8000-000000000003',
  contract: 'f1000000-0000-4000-8000-000000000004',
  contractWeek: 'f1000000-0000-4000-8000-000000000005',
  account: 'f1000000-0000-4000-8000-000000000006',
  session: 'f1000000-0000-4000-8000-000000000007',
  workflow: 'f1000000-0000-4000-8000-000000000008',
  createKey: 'f1000000-0000-4000-8000-000000000101',
  candidateSignatureKey: 'f1000000-0000-4000-8000-000000000102',
  submitKey: 'f1000000-0000-4000-8000-000000000103',
  managerEmailKey: 'f1000000-0000-4000-8000-000000000104'
});

const SYNTHETIC_MARKER = 'MYTMS_MANAGER_EMAIL_E2E_PROOF_V1';
const MAX_JSON_BYTES = 256 * 1024;
const CANDIDATE_SIGNATURE_PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';

function text(value) {
  return String(value == null ? '' : value).trim();
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'referrer-policy': 'no-referrer',
      'x-content-type-options': 'nosniff'
    }
  });
}

function proofEnabled(env) {
  return text(env.CANDIDATE_MANAGER_E2E_PROOF_ENABLED).toUpperCase() === 'TRUE'
    && text(env.CANDIDATE_APP_ENVIRONMENT).toUpperCase() === 'TEST';
}

function serviceHeaders(env, profile = 'public') {
  const key = text(env.SUPABASE_SERVICE_ROLE_KEY);
  if (!key) throw new Error('E2E_AGENCY_SERVICE_AUTH_UNAVAILABLE');
  return {
    apikey: key,
    authorization: `Bearer ${key}`,
    'content-type': 'application/json; charset=utf-8',
    accept: 'application/json',
    'content-profile': profile,
    'accept-profile': profile,
    'x-client-info': 'cloudtms-manager-email-e2e-proof-v1'
  };
}

async function boundedJson(response) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > MAX_JSON_BYTES) throw new Error('E2E_DEPENDENCY_RESPONSE_TOO_LARGE');
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_JSON_BYTES) throw new Error('E2E_DEPENDENCY_RESPONSE_TOO_LARGE');
  if (!bytes.byteLength) return null;
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error('E2E_DEPENDENCY_RESPONSE_INVALID');
  }
}

async function agencyRest(env, table, query = '', options = {}) {
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  if (!/^https:\/\/[^/]+$/i.test(base)) throw new Error('E2E_AGENCY_CONFIGURATION_UNAVAILABLE');
  const response = await fetch(`${base}/rest/v1/${table}${query ? `?${query}` : ''}`, {
    method: options.method || 'GET',
    headers: {
      ...serviceHeaders(env),
      ...(options.prefer ? { prefer: options.prefer } : {})
    },
    ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
    signal: AbortSignal.timeout(10_000)
  });
  const payload = await boundedJson(response);
  if (!response.ok) throw new Error(`E2E_AGENCY_REST_FAILED:${response.status}:${table}`);
  return payload;
}

async function controlSettings(env) {
  const base = text(env.MYTMS_CONTROL_PLANE_URL).replace(/\/$/, '');
  const key = text(env.MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY);
  const agencyId = text(env.MYTMS_OFFICE_AGENCY_ID).toLowerCase();
  if (!/^https:\/\/[^/]+$/i.test(base) || !key
      || !/^[0-9a-f-]{36}$/.test(agencyId)) {
    throw new Error('E2E_CONTROL_CONFIGURATION_UNAVAILABLE');
  }
  const query = new URLSearchParams({
    agency_id: `eq.${agencyId}`,
    select: 'agency_id,test_recipient_allowlist_json'
  });
  const response = await fetch(`${base}/rest/v1/agency_app_settings?${query}`, {
    headers: {
      apikey: key,
      authorization: `Bearer ${key}`,
      accept: 'application/json',
      'accept-profile': 'control',
      'x-client-info': 'cloudtms-manager-email-e2e-proof-v1'
    },
    signal: AbortSignal.timeout(8_000)
  });
  const rows = await boundedJson(response);
  if (!response.ok || !Array.isArray(rows) || rows.length !== 1) {
    throw new Error('E2E_CONTROL_SETTINGS_UNAVAILABLE');
  }
  const allowlist = rows[0].test_recipient_allowlist_json;
  if (!Array.isArray(allowlist) || allowlist.length !== 1) {
    throw new Error('E2E_TEST_RECIPIENT_ALLOWLIST_MUST_CONTAIN_EXACTLY_ONE_ADDRESS');
  }
  const recipient = text(allowlist[0]).toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipient)) {
    throw new Error('E2E_TEST_RECIPIENT_ALLOWLIST_INVALID');
  }
  return { agencyId, recipient };
}

function utcDate(value) {
  return new Date(value).toISOString().slice(0, 10);
}

function proofDates(now = new Date()) {
  const day = now.getUTCDay();
  const untilSunday = (7 - day) % 7;
  const weekEnding = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + untilSunday
  ));
  const workDate = new Date(weekEnding.getTime() - 2 * 24 * 60 * 60 * 1000);
  const contractStart = new Date(workDate.getTime() - 30 * 24 * 60 * 60 * 1000);
  const contractEnd = new Date(weekEnding.getTime() + 30 * 24 * 60 * 60 * 1000);
  return {
    weekEnding: utcDate(weekEnding),
    workDate: utcDate(workDate),
    contractStart: utcDate(contractStart),
    contractEnd: utcDate(contractEnd),
    weekEndingWeekday: weekEnding.getUTCDay()
  };
}

async function exactRowsAbsent(env) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  const checks = await Promise.all([
    agencyRest(env, 'clients', `id=eq.${ids.client}&select=id`),
    agencyRest(env, 'candidates', `id=eq.${ids.candidate}&select=id`),
    agencyRest(env, 'contracts', `id=eq.${ids.contract}&select=id`),
    agencyRest(env, 'candidate_submission_workflows', `id=eq.${ids.workflow}&select=id`),
    agencyRest(env, 'candidate_app_accounts', `id=eq.${ids.account}&select=id`),
    agencyRest(env, 'candidate_app_sessions', `id=eq.${ids.session}&select=id`)
  ]);
  if (checks.some(rows => !Array.isArray(rows) || rows.length !== 0)) {
    throw new Error('E2E_FIXED_FIXTURE_ALREADY_EXISTS');
  }
}

async function settingsBaseline(env) {
  const rows = await agencyRest(env, 'settings_defaults',
    'id=eq.1&select=id,candidate_app_environment,candidate_app_system_actor_user_id,candidate_app_feature_flags_json');
  if (!Array.isArray(rows) || rows.length !== 1) throw new Error('E2E_SETTINGS_BASELINE_UNAVAILABLE');
  const row = rows[0];
  const flags = row.candidate_app_feature_flags_json;
  if (row.candidate_app_environment !== 'TEST' || row.candidate_app_system_actor_user_id != null
      || !flags || typeof flags !== 'object' || Array.isArray(flags)
      || flags.candidate_app_writes !== false || flags.candidate_manager_approval !== false) {
    throw new Error('E2E_DISABLED_BASELINE_REQUIRED');
  }
  return row;
}

async function seedFixture(env, recipient, baseline, now = new Date()) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  const dates = proofDates(now);
  const flags = {
    ...baseline.candidate_app_feature_flags_json,
    candidate_app_writes: true,
    candidate_manager_approval: true
  };
  await agencyRest(env, 'tms_users', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.actor,
      email: 'mytms-e2e-system@example.test',
      role: 'admin', is_active: true,
      password_hash: 'disabled-synthetic-manager-email-e2e-proof'
    }
  });
  await agencyRest(env, 'settings_defaults', 'id=eq.1', {
    method: 'PATCH', prefer: 'return=minimal',
    body: {
      candidate_app_system_actor_user_id: ids.actor,
      candidate_app_feature_flags_json: flags
    }
  });
  await agencyRest(env, 'clients', '', {
    method: 'POST', prefer: 'return=minimal',
    body: { id: ids.client, name: 'Synthetic MyTMS manager email proof client' }
  });
  await agencyRest(env, 'candidates', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.candidate,
      email: 'mytms-e2e-candidate@example.test', active: true,
      key_norm: 'MYTMS-E2E-CANDIDATE', display_name: 'Synthetic Candidate',
      first_name: 'Synthetic', last_name: 'Candidate', pay_method: 'PAYE'
    }
  });
  await agencyRest(env, 'contracts', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.contract, candidate_id: ids.candidate, client_id: ids.client,
      start_date: dates.contractStart, end_date: dates.contractEnd,
      pay_method_snapshot: 'PAYE', week_ending_weekday_snapshot: dates.weekEndingWeekday,
      default_submission_mode: 'ELECTRONIC', role: 'Registered Nurse', band: '5',
      rates_json: {
        paye_day: 20, paye_night: 20, paye_sat: 20, paye_sun: 20, paye_bh: 20,
        umb_day: 20, umb_night: 20, umb_sat: 20, umb_sun: 20, umb_bh: 20,
        charge_day: 25, charge_night: 25, charge_sat: 25, charge_sun: 25, charge_bh: 25
      },
      candidate_manager_approval_policy_json: {
        mode: 'OVERRIDE', approved_emails: [recipient],
        approved_domains: [], allow_free_business_email: false
      }
    }
  });
  await agencyRest(env, 'contract_weeks', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.contractWeek, contract_id: ids.contract,
      week_ending_date: dates.weekEnding, status: 'OPEN',
      submission_mode_snapshot: 'ELECTRONIC'
    }
  });
  await agencyRest(env, 'candidate_app_accounts', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.account, environment: 'TEST',
      email_normalized: 'mytms-e2e-candidate@example.test', status: 'ACTIVE',
      password_scheme: 'PBKDF2-HMAC-SHA256', password_scheme_version: 1,
      password_salt: `\\x${'41'.repeat(16)}`,
      password_digest: `\\x${'42'.repeat(32)}`,
      password_changed_at_utc: now.toISOString()
    }
  });
  await agencyRest(env, 'candidate_app_sessions', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.session, account_id: ids.account, environment: 'TEST',
      selected_candidate_id: ids.candidate, status: 'ACTIVE', rotation: 0,
      refresh_token_hash: `\\x${'43'.repeat(32)}`,
      expires_at_utc: new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000).toISOString(),
      absolute_expires_at_utc: new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000).toISOString()
    }
  });
  return dates;
}

async function candidateRequest(env, deps, ctx, token, path, method, body = null, bytes = null) {
  const headers = new Headers({ authorization: `Bearer ${token}` });
  let requestBody;
  if (bytes) {
    headers.set('content-type', 'image/png');
    headers.set('content-length', String(bytes.byteLength));
    requestBody = bytes;
  } else if (body != null) {
    headers.set('content-type', 'application/json; charset=utf-8');
    requestBody = JSON.stringify(body);
  }
  const response = await handleCandidateAppRequest(new Request(`https://private.test${path}`, {
    method, headers, ...(requestBody == null ? {} : { body: requestBody })
  }), env, ctx, deps);
  const payload = await boundedJson(response);
  if (!response || !response.ok || !payload || payload.ok === false) {
    throw new Error(`E2E_CANDIDATE_STEP_FAILED:${response?.status || 500}:${payload?.error_code || 'UNKNOWN'}`);
  }
  return payload;
}

function candidateSignatureBytes() {
  const binary = atob(CANDIDATE_SIGNATURE_PNG_BASE64);
  return Uint8Array.from(binary, character => character.charCodeAt(0));
}

async function runJourneyToOutbox(env, ctx, deps, recipient, dates) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  const session = {
    session_id: ids.session, rotation: 0,
    issued_at_utc: new Date().toISOString()
  };
  const token = await candidateAppBackendInternals.createAccessToken(env, session);
  await candidateRequest(env, deps, ctx, token, '/candidate-app/v1/workflows', 'POST', {
    workflow_id: ids.workflow, idempotency_key: ids.createKey,
    workflow: {
      workflow_kind: 'CONTRACT_HOURS', scope: 'WEEKLY', route: 'ELECTRONIC',
      contract_id: ids.contract, contract_week_id: ids.contractWeek,
      week_ending_date: dates.weekEnding,
      input_snapshot: { source: SYNTHETIC_MARKER }
    }
  });
  const signature = candidateSignatureBytes();
  const prepared = await candidateRequest(
    env, deps, ctx, token,
    `/candidate-app/v1/workflows/${ids.workflow}/components/prepare`, 'POST', {
      generation: 1,
      component_kind: 'CANDIDATE_SIGNATURE', document_role: 'CANDIDATE_SIGNATURE',
      media_type: 'image/png', byte_size: signature.byteLength,
      idempotency_key: ids.candidateSignatureKey
    }
  );
  await candidateRequest(env, deps, ctx, token, prepared.upload.url, 'PUT', null, signature);

  const background = [];
  const submitContext = {
    waitUntil(promise) { background.push(Promise.resolve(promise)); }
  };
  await candidateRequest(
    env, deps, submitContext, token,
    `/candidate-app/v1/workflows/${ids.workflow}/actions/worker-submit`, 'POST', {
      generation: 1, idempotency_key: ids.submitKey,
      candidate_signature_component_id: prepared.component_id,
      candidate_signed_at_utc: new Date().toISOString(),
      approval_route: 'EMAIL',
      immutable_submission: {
        actual_schedule_json: [{
          date: dates.workDate, start: '09:00', end: '17:00',
          break_minutes: 30, ref_num: 'MYTMS-E2E-PROOF'
        }]
      }
    }
  );
  await Promise.all(background);

  const workflowRows = await agencyRest(env, 'candidate_submission_workflows',
    `id=eq.${ids.workflow}&select=id,state,generation`);
  if (!Array.isArray(workflowRows) || workflowRows.length !== 1
      || workflowRows[0].state !== 'READY_FOR_MANAGER_APPROVAL'
      || Number(workflowRows[0].generation) !== 2) {
    throw new Error('E2E_REVIEW_DOCUMENT_NOT_READY');
  }

  const approval = await candidateRequest(
    env, deps, ctx, token,
    `/candidate-app/v1/workflows/${ids.workflow}/actions/create-email-approval-request`, 'POST', {
      generation: 2, idempotency_key: ids.managerEmailKey,
      manager_email: recipient
    }
  );
  const managerToken = await candidateAppBackendInternals.deterministicOpaqueToken(
    env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET,
    'candidate-email-handoff-v1', ids.workflow, 2,
    'CREATE_EMAIL_APPROVAL_REQUEST', ids.managerEmailKey, recipient, 'INITIAL'
  );
  const origin = await controlPlaneRpc(env, 'control', 'manager_review_origin_resolve_v1', {
    p_agency_id: text(env.MYTMS_OFFICE_AGENCY_ID),
    p_environment: 'TEST'
  });
  const publicOrigin = text(origin?.manager_review_public_origin).replace(/\/$/, '');
  if (!/^https:\/\/[^/]+$/i.test(publicOrigin)) throw new Error('E2E_MANAGER_ORIGIN_UNAVAILABLE');
  return {
    approval,
    managerUrl: `${publicOrigin}/manager/timesheet/${ids.workflow}#token=${encodeURIComponent(managerToken)}`
  };
}

export async function handleCandidateManagerEmailE2EProof(request, env, ctx, deps) {
  if (request.method !== 'POST'
      || new URL(request.url).pathname !== CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH) {
    return json(404, { ok: false, error_code: 'E2E_PROOF_ROUTE_NOT_FOUND' });
  }
  if (!proofEnabled(env)) {
    return json(404, { ok: false, error_code: 'E2E_PROOF_DISABLED' });
  }
  try {
    const { recipient } = await controlSettings(env);
    await exactRowsAbsent(env);
    const baseline = await settingsBaseline(env);
    const dates = await seedFixture(env, recipient, baseline);
    const result = await runJourneyToOutbox(env, ctx, deps, recipient, dates);
    return json(201, {
      ok: true,
      contract_version: SYNTHETIC_MARKER,
      workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
      approval_request_id: result.approval.approval_request_id,
      mail_outbox_id: result.approval.mail_outbox_id,
      manager_url: result.managerUrl,
      provider_send_performed: false,
      disabled_state_restoration_required: true
    });
  } catch (error) {
    return json(409, {
      ok: false,
      error_code: text(error?.message || error).split(':').slice(0, 3).join(':') || 'E2E_PROOF_FAILED'
    });
  }
}

export const candidateManagerEmailE2EProofInternals = Object.freeze({
  boundedJson,
  candidateSignatureBytes,
  controlSettings,
  exactRowsAbsent,
  proofDates,
  proofEnabled,
  runJourneyToOutbox,
  seedFixture,
  settingsBaseline
});
