import {
  candidateAppBackendInternals
} from './candidate-app-backend.js';
import { controlPlaneRpc } from '../../candidate-broker/src/control-plane-client.js';
import { getMyTmsOfficeSettings } from './mytms-office-control.js';

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
const SYNTHETIC_CANDIDATE_EMAIL = 'mytms-e2e-candidate@example.test';
const SYNTHETIC_CANDIDATE_PASSWORD = 'Synthetic-Manager-Proof-Only-2026!';
const MAX_JSON_BYTES = 256 * 1024;
const CANDIDATE_SIGNATURE_PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';

function text(value) {
  return String(value == null ? '' : value).trim();
}

function safeProofDiagnostic(error) {
  return text(error?.message || error)
    .replace(/https?:\/\/[^\s]+/gi, '[url]')
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi, '[uuid]')
    .replace(/[0-9a-f]{64}/gi, '[sha256]')
    .replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, '[email]')
    .replace(/#token=[^\s]+/gi, '#token=[redacted]')
    .slice(0, 300) || 'UNKNOWN';
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

async function controlSettings(env, user) {
  const agencyId = text(env.MYTMS_OFFICE_AGENCY_ID).toLowerCase();
  if (!/^[0-9a-f-]{36}$/.test(agencyId)) {
    throw new Error('E2E_CONTROL_CONFIGURATION_UNAVAILABLE');
  }
  const settings = await getMyTmsOfficeSettings(env, user);
  if (!settings || settings.ok !== true) {
    throw new Error('E2E_CONTROL_SETTINGS_UNAVAILABLE');
  }
  const allowlist = settings.test_recipient_allowlist;
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
      || flags.candidate_account_registration !== false
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
    candidate_account_registration: true,
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
  const password = await candidateAppBackendInternals.derivePasswordVerifier(
    SYNTHETIC_CANDIDATE_PASSWORD
  );
  await agencyRest(env, 'candidate_app_accounts', '', {
    method: 'POST', prefer: 'return=minimal',
    body: {
      id: ids.account, environment: 'TEST',
      email_normalized: SYNTHETIC_CANDIDATE_EMAIL, status: 'ACTIVE',
      password_scheme: password.scheme, password_scheme_version: password.scheme_version,
      password_salt: `\\x${password.salt_hex}`,
      password_digest: `\\x${password.digest_hex}`,
      password_params_json: password.params,
      password_changed_at_utc: now.toISOString()
    }
  });
  return dates;
}

function candidateSignatureBytes() {
  const binary = atob(CANDIDATE_SIGNATURE_PNG_BASE64);
  return Uint8Array.from(binary, character => character.charCodeAt(0));
}

async function managerLinkFromOutbox(env, outboxId) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  const origin = await controlPlaneRpc(env, 'control', 'manager_review_origin_resolve_v1', {
    p_agency_id: text(env.MYTMS_OFFICE_AGENCY_ID),
    p_environment_label: 'TEST'
  });
  const publicOrigin = text(origin?.manager_review_public_origin).replace(/\/$/, '');
  if (!/^https:\/\/[^/]+$/i.test(publicOrigin)) throw new Error('E2E_MANAGER_ORIGIN_UNAVAILABLE');
  const checkedOutboxId = text(outboxId).toLowerCase();
  if (!/^[0-9a-f-]{36}$/.test(checkedOutboxId)) throw new Error('E2E_MANAGER_OUTBOX_UNAVAILABLE');
  const outboxRows = await agencyRest(env, 'mail_outbox',
    `id=eq.${encodeURIComponent(checkedOutboxId)}&select=body_text,status,email_type,context_kind,context_id,payment_scope_json`);
  if (!Array.isArray(outboxRows) || outboxRows.length !== 1) {
    throw new Error('E2E_MANAGER_OUTBOX_UNAVAILABLE');
  }
  if (text(outboxRows[0].context_kind).toUpperCase() !== 'CANDIDATE_WORKFLOW'
      || text(outboxRows[0].context_id).toLowerCase() !== ids.workflow
      || text(outboxRows[0].payment_scope_json?.candidate_manager_workflow_id).toLowerCase() !== ids.workflow
      || text(outboxRows[0].payment_scope_json?.candidate_manager_mail_kind).toUpperCase() !== 'INITIAL'
      || text(outboxRows[0].email_type).toUpperCase() !== 'CANDIDATE_APP_TRANSACTIONAL') {
    throw new Error('E2E_MANAGER_OUTBOX_MISMATCH');
  }
  const escapedOrigin = publicOrigin.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const managerLink = text(outboxRows[0].body_text).match(
    new RegExp(`${escapedOrigin}/manager/timesheet/${ids.workflow}#token=[^\\s]+`)
  )?.[0] || '';
  if (!managerLink) throw new Error('E2E_MANAGER_LINK_UNAVAILABLE');
  return {
    managerUrl: managerLink,
    providerSendPerformed: text(outboxRows[0].status).toUpperCase() === 'SENT'
  };
}

async function renderPendingReview(env, deps) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  const rows = await agencyRest(env, 'candidate_submission_workflows',
    `id=eq.${ids.workflow}&select=id,state,generation,last_mutation_response_json`);
  if (!Array.isArray(rows) || rows.length !== 1
      || text(rows[0].state).toUpperCase() !== 'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
      || Number(rows[0].generation) !== 2) {
    throw new Error('E2E_REVIEW_RENDER_STATE_INVALID');
  }
  const renderContract = rows[0].last_mutation_response_json?.render_contract;
  if (!renderContract || !Array.isArray(renderContract.components)
      || renderContract.components.length !== 1
      || text(renderContract.workflow_id).toLowerCase() !== ids.workflow
      || Number(renderContract.workflow_generation) !== 2) {
    throw new Error('E2E_REVIEW_RENDER_CONTRACT_INVALID');
  }
  try {
    const rendered = await candidateAppBackendInternals.renderAndRegister(
      env, deps, renderContract, 'REVIEW'
    );
    return { ok: true, rendered_count: rendered.length };
  } catch (error) {
    return {
      ok: false,
      error_code: 'E2E_REVIEW_RENDER_FAILED',
      error_diagnostic: safeProofDiagnostic(error)
    };
  }
}

function boundedStorageKey(value) {
  const key = text(value);
  return key && key.length <= 1024 && !key.startsWith('/') && !key.includes('..')
    && !/[\x00-\x1f\x7f]/.test(key) ? key : null;
}

async function cleanupSyntheticStorage(env) {
  const ids = CANDIDATE_MANAGER_EMAIL_E2E_IDS;
  if (!env.R2 || typeof env.R2.delete !== 'function') {
    throw new Error('E2E_STORAGE_CONFIGURATION_UNAVAILABLE');
  }
  const [clients, candidates, contracts, accounts, workflows, components, timesheets] = await Promise.all([
    agencyRest(env, 'clients', `id=eq.${ids.client}&select=id,name`),
    agencyRest(env, 'candidates', `id=eq.${ids.candidate}&select=id,email,key_norm`),
    agencyRest(env, 'contracts', `id=eq.${ids.contract}&select=id,candidate_id,client_id`),
    agencyRest(env, 'candidate_app_accounts', `id=eq.${ids.account}&select=id,email_normalized`),
    agencyRest(env, 'candidate_submission_workflows',
      `id=eq.${ids.workflow}&select=id,account_id,candidate_id,contract_id`),
    agencyRest(env, 'candidate_submission_components',
      `workflow_id=eq.${ids.workflow}&select=id,storage_key,review_storage_key,final_signed_storage_key`),
    agencyRest(env, 'timesheets',
      `candidate_workflow_id=eq.${ids.workflow}&select=timesheet_id,contract_id,candidate_workflow_id,r2_nurse_key,r2_auth_key,manual_pdf_r2_key,qr_r2_key`)
  ]);
  if (clients?.length !== 1 || clients[0].name !== 'Synthetic MyTMS manager email proof client'
      || candidates?.length !== 1 || candidates[0].email !== SYNTHETIC_CANDIDATE_EMAIL
      || candidates[0].key_norm !== 'MYTMS-E2E-CANDIDATE'
      || contracts?.length !== 1 || text(contracts[0].candidate_id).toLowerCase() !== ids.candidate
      || text(contracts[0].client_id).toLowerCase() !== ids.client
      || accounts?.length !== 1 || accounts[0].email_normalized !== SYNTHETIC_CANDIDATE_EMAIL
      || workflows?.length !== 1 || text(workflows[0].account_id).toLowerCase() !== ids.account
      || text(workflows[0].candidate_id).toLowerCase() !== ids.candidate
      || text(workflows[0].contract_id).toLowerCase() !== ids.contract
      || !Array.isArray(components) || components.length < 1 || components.length > 20
      || !Array.isArray(timesheets) || timesheets.length > 2
      || timesheets.some(row => text(row.contract_id).toLowerCase() !== ids.contract
        || text(row.candidate_workflow_id).toLowerCase() !== ids.workflow)) {
    throw new Error('E2E_STORAGE_FIXTURE_IDENTITY_INVALID');
  }
  const rawKeys = [
    ...components.flatMap(row => [row.storage_key, row.review_storage_key, row.final_signed_storage_key]),
    ...timesheets.flatMap(row => [row.r2_nurse_key, row.r2_auth_key, row.manual_pdf_r2_key, row.qr_r2_key])
  ].filter(Boolean);
  const validatedKeys = rawKeys.map(boundedStorageKey);
  if (validatedKeys.some(key => !key)) throw new Error('E2E_STORAGE_KEY_SET_INVALID');
  const keys = [...new Set(validatedKeys)];
  if (!keys.length || keys.length > 32) throw new Error('E2E_STORAGE_KEY_SET_INVALID');
  await env.R2.delete(keys);
  return { ok: true, deleted_storage_object_count: keys.length };
}

export async function handleCandidateManagerEmailE2EProof(request, env, ctx, deps, user) {
  if (request.method !== 'POST'
      || new URL(request.url).pathname !== CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH) {
    return json(404, { ok: false, error_code: 'E2E_PROOF_ROUTE_NOT_FOUND' });
  }
  if (!proofEnabled(env)) {
    return json(404, { ok: false, error_code: 'E2E_PROOF_DISABLED' });
  }
  try {
    const body = await boundedJson(request);
    const action = text(body?.action || 'SEED').toUpperCase();
    const { recipient } = await controlSettings(env, user);
    if (action === 'RENDER') {
      const result = await renderPendingReview(env, deps);
      return json(result.ok ? 200 : 409, {
        ...result,
        contract_version: SYNTHETIC_MARKER,
        workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
        provider_send_performed: false
      });
    }
    if (action === 'LOOKUP') {
      const result = await managerLinkFromOutbox(env, body?.mail_outbox_id);
      return json(200, {
        ok: true,
        contract_version: SYNTHETIC_MARKER,
        workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
        manager_url: result.managerUrl,
        provider_send_performed: result.providerSendPerformed
      });
    }
    if (action === 'CLEANUP_STORAGE') {
      const result = await cleanupSyntheticStorage(env);
      return json(200, {
        ...result,
        contract_version: SYNTHETIC_MARKER,
        workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
        provider_send_performed: false
      });
    }
    if (action !== 'SEED') throw new Error('E2E_PROOF_ACTION_INVALID');
    await exactRowsAbsent(env);
    const baseline = await settingsBaseline(env);
    const dates = await seedFixture(env, recipient, baseline);
    return json(201, {
      ok: true,
      contract_version: SYNTHETIC_MARKER,
      workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
      candidate_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.candidate,
      week_ending_date: dates.weekEnding,
      work_date: dates.workDate,
      candidate_proof_ready: true,
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
  boundedStorageKey,
  cleanupSyntheticStorage,
  exactRowsAbsent,
  managerLinkFromOutbox,
  proofDates,
  proofEnabled,
  renderPendingReview,
  safeProofDiagnostic,
  seedFixture,
  settingsBaseline
});
