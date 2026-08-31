import assert from 'node:assert/strict';
import test from 'node:test';

import cloudTmsWorker from '../broker/src/index.js';
import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import {
  adoptMyTmsCandidate,
  deleteMyTmsAgencyLogo,
  getMyTmsAgencyLogo,
  getMyTmsCandidateStatus,
  getMyTmsDailyInformationSettings,
  getMyTmsHomeAnnouncementSettings,
  getMyTmsManagerEmailSettings,
  MyTmsOfficeError,
  myTmsOfficeInternals,
  previewMyTmsTemplate,
  previewMyTmsManagerEmailTemplate,
  previewMyTmsHomeAnnouncement,
  queueMyTmsIdentityChallenge,
  reserveAndQueueMyTmsInvitation,
  sanitizeMyTmsEmailHtml,
  sanitizeManagerEmailHtml,
  setMyTmsAgencyLogo,
  setMyTmsManagerEmailTemplates,
  setMyTmsDailyInformationSettings,
  setMyTmsHomeAnnouncement,
  setMyTmsMembershipState,
  setMyTmsOfficeSettings
} from '../broker/src/mytms-office-control.js';

const IDS = Object.freeze({
  agency: '10000000-0000-4000-8000-000000000004',
  challenge: '10000000-0000-4000-8000-000000000040',
  outbox: '10000000-0000-4000-8000-000000000041'
});

function officeEnvironment(overrides = {}) {
  return {
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
    MYTMS_OFFICE_CONTROL_ENABLED: 'TRUE',
    MYTMS_OFFICE_AGENCY_ID: IDS.agency,
    MYTMS_OFFICE_ACTOR_IDENTITY_SECRET: 'test-office-actor-secret-not-live',
    ...overrides
  };
}

function managerTemplates() {
  const kinds = ['INITIAL', 'REMINDER', 'RENEWAL', 'WITHDRAWAL', 'CANCELLATION'];
  const type = Object.fromEntries(kinds.map(kind => [kind, {
    subject: `${kind} subject`, body_text: `${kind} plain text.`,
    body_html: `<p>${kind} safe HTML.</p>`,
    button_text: ['INITIAL', 'REMINDER', 'RENEWAL'].includes(kind) ? 'Review and approve' : null,
    include_link: ['INITIAL', 'REMINDER', 'RENEWAL'].includes(kind)
  }]));
  return {
    schema_version: 'CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1',
    TIMESHEET: structuredClone(type), EXPENSE_CLAIM: structuredClone(type)
  };
}

function agencyLogoPngBytes() {
  const bytes = new Uint8Array(24);
  bytes.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], 0);
  const view = new DataView(bytes.buffer);
  view.setUint32(16, 384, false);
  view.setUint32(20, 384, false);
  return bytes;
}

function memoryR2() {
  const objects = new Map();
  return {
    objects,
    async head(key) { return objects.get(key)?.head || null; },
    async put(key, value, options = {}) {
      const bytes = new Uint8Array(value);
      objects.set(key, {
        bytes,
        head: {
          httpMetadata: options.httpMetadata || {},
          customMetadata: options.customMetadata || {}
        }
      });
      return { key };
    },
    async get(key) {
      const stored = objects.get(key);
      if (!stored) return null;
      return {
        ...stored.head,
        async arrayBuffer() {
          return stored.bytes.buffer.slice(
            stored.bytes.byteOffset,
            stored.bytes.byteOffset + stored.bytes.byteLength
          );
        }
      };
    }
  };
}

test('agency logo upload, read and delete use private content-addressed storage and preserve history', async () => {
  const originalFetch = globalThis.fetch;
  const bucket = memoryR2();
  const bytes = agencyLogoPngBytes();
  const digest = Buffer.from(await crypto.subtle.digest('SHA-256', bytes)).toString('hex');
  const key = `candidate-app/branding/${digest}.png`;
  let pointer = null;
  const documentBrandingPointer = 'Assets/LEGACY-DOCUMENT-LOGO.png';
  let settingsVersion = 3;
  const calls = [];
  globalThis.fetch = async (url, init = {}) => {
    const requestUrl = url instanceof Request ? url.url : String(url);
    const method = url instanceof Request ? url.method : (init.method || 'GET');
    const requestBody = url instanceof Request
      ? await url.clone().json().catch(() => null)
      : (init.body ? JSON.parse(init.body) : null);
    calls.push({ url: requestUrl, method });
    if (requestUrl.includes('/rest/v1/settings_defaults')) {
      if (method === 'PATCH') {
        assert.equal(Object.hasOwn(requestBody, 'agency_logo'), false);
        pointer = requestBody.candidate_app_logo_asset_key;
        return Response.json([{
          agency_logo: documentBrandingPointer,
          candidate_app_logo_asset_key: pointer
        }]);
      }
      return Response.json([{
        agency_name: 'Arthur Rai Medical Services',
        agency_logo: documentBrandingPointer,
        candidate_app_logo_asset_key: pointer
      }]);
    }
    if (requestUrl.includes('/rpc/agency_app_settings_get_v1')) {
      return Response.json({ ok: true, version: settingsVersion, logo_asset_key: pointer });
    }
    if (requestUrl.includes('/rpc/agency_app_settings_set_v1')) {
      settingsVersion += 1;
      return Response.json({
        ok: true, version: settingsVersion,
        logo_asset_key: requestBody.p_settings.logo_asset_key
      });
    }
    throw new Error(`unexpected agency logo request: ${requestUrl}`);
  };
  const env = officeEnvironment({
    SUPABASE_URL: 'https://agency.test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role',
    MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-service-role',
    R2: bucket
  });
  const user = { id: IDS.challenge };
  try {
    const uploaded = await setMyTmsAgencyLogo(env, user, {
      expected_logo_asset_key: null,
      data_url: `data:image/png;base64,${Buffer.from(bytes).toString('base64')}`,
      idempotency_key: 'agency-logo-upload-0001'
    });
    assert.equal(uploaded.logo_asset_key, key);
    assert.equal(uploaded.preview_data_url.startsWith('data:image/png;base64,'), true);
    assert.equal(uploaded.size_bytes, bytes.byteLength);
    assert.equal(pointer, key);
    assert.equal(bucket.objects.has(key), true);

    const current = await getMyTmsAgencyLogo(env, user);
    assert.equal(current.has_logo, true);
    assert.equal(current.sha256_hex, digest);

    const deleted = await deleteMyTmsAgencyLogo(env, user, {
      expected_logo_asset_key: key,
      idempotency_key: 'agency-logo-delete-0001'
    });
    assert.equal(deleted.has_logo, false);
    assert.equal(pointer, null);
    assert.equal(bucket.objects.has(key), true, 'historical immutable logo object is retained');
    assert.equal(calls.some(call => call.method === 'DELETE'), false);
    assert.equal(documentBrandingPointer, 'Assets/LEGACY-DOCUMENT-LOGO.png');
    assert.equal(calls.every(call => !call.url.includes('agency_logo=eq.')), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('email HTML sanitizer removes active content and retains only approved links', () => {
  const result = sanitizeMyTmsEmailHtml(
    '<xmp><img src=x onerror=alert(1)></xmp>'
      + '<script>alert(1)</script><svg><a href="javascript:alert(1)">bad</a></svg>'
      + '<p onclick="alert(1)" style="position:fixed;color:#123456">Hello {{candidate_name}}</p>'
      + '<a target="_blank" href="https://example.test/invite">Continue</a>'
      + '<a href="mailto:support@example.test">Support</a>'
      + '<img src="data:image/svg+xml,boom" onerror="boom">'
  );

  assert.doesNotMatch(result, /script|svg|xmp|onerror|onclick|javascript:|data:/i);
  assert.doesNotMatch(result, /position\s*:/i);
  assert.match(result, /Hello \{\{candidate_name\}\}/);
  assert.match(result, /href="https:\/\/example\.test\/invite"/);
  assert.match(result, /rel="noopener noreferrer"/);
  assert.match(result, /href="mailto:support@example\.test"/);
});

test('template preview is admin-context-bound and returns sanitizer identity', async () => {
  const result = await previewMyTmsTemplate(
    officeEnvironment(),
    { id: '10000000-0000-4000-8000-000000000042' },
    { html: '<p>Hello <strong>{{candidate_name}}</strong><script>bad()</script></p>' }
  );
  assert.equal(result.ok, true);
  assert.equal(
    result.sanitizer_policy_version,
    'MYTMS_EMAIL_HTML_V1_SANITIZE_HTML_2_17_7'
  );
  assert.match(result.sanitized_content_sha256_hex, /^[a-f0-9]{64}$/);
  assert.doesNotMatch(result.sanitized_html, /script|bad\(\)/i);
  assert.match(result.sanitized_html, /\{\{candidate_name\}\}/);
});

test('manager template sanitizer never permits agency-authored links or active content', () => {
  assert.equal(sanitizeManagerEmailHtml('<p><strong>Review safely</strong></p>'), '<p><strong>Review safely</strong></p>');
  assert.throws(
    () => sanitizeManagerEmailHtml('<p>Review</p><a href="https://wrong.example">Wrong link</a>'),
    error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_MANAGER_TEMPLATE_INVALID'
  );
});

test('manager template preview appends server-owned link wording but never a real credential', async () => {
  const result = await previewMyTmsManagerEmailTemplate(
    officeEnvironment(), { id: IDS.challenge },
    { kind: 'INITIAL', template: managerTemplates().TIMESHEET.INITIAL }
  );
  assert.equal(result.ok, true);
  assert.match(result.preview_html, /expires seven days/i);
  assert.match(result.preview_html, /Review and approve/);
  assert.doesNotMatch(result.preview_html, /token=|href=/i);
});

test('manager settings combine agency templates with read-only platform origin authority', async () => {
  const originalFetch = globalThis.fetch;
  const templates = managerTemplates();
  const calls = [];
  globalThis.fetch = async (url, init) => {
    const requestUrl = url instanceof Request ? url.url : String(url);
    calls.push(requestUrl);
    if (requestUrl.includes('/rpc/candidate_manager_email_settings_get_v1')) {
      return Response.json({
        ok: true, templates, version: 3,
        sanitizer_policy_version: 'MANAGER_EMAIL_SAFE_HTML_V1',
        semantic_sha256_hex: 'a'.repeat(64), updated_at_utc: '2026-08-23T01:00:00Z'
      });
    }
    if (requestUrl.includes('/rpc/manager_review_origin_get_v1')) {
      return Response.json({
        ok: true, settings_version: 8,
        manager_review_public_origin: 'https://testmode.arthur-rai.co.uk',
        manager_review_origin_state: 'TEST_READY',
        manager_review_origin_semantic_sha256_hex: 'b'.repeat(64),
        manager_review_origin_verified_at_utc: '2026-08-23T01:00:00Z'
      });
    }
    throw new Error('unexpected manager settings request');
  };
  try {
    const result = await getMyTmsManagerEmailSettings(officeEnvironment({
      SUPABASE_URL: 'https://agency.test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role',
      MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-service-role'
    }), { id: IDS.challenge });
    assert.equal(result.agency_template_version, 3);
    assert.equal(result.manager_origin.ownership, 'PLATFORM');
    assert.equal(result.manager_origin.public_origin, 'https://testmode.arthur-rai.co.uk');
    assert.equal(calls.length, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('manager template save is exact, versioned and rejects unknown objects before database access', async () => {
  const originalFetch = globalThis.fetch;
  let captured;
  globalThis.fetch = async (url, init) => {
    captured = { url: String(url), body: JSON.parse(init.body) };
    return Response.json({
      ok: true, templates: captured.body.p_templates, version: 4,
      sanitizer_policy_version: 'MANAGER_EMAIL_SAFE_HTML_V1',
      semantic_sha256_hex: 'c'.repeat(64), updated_at_utc: '2026-08-23T01:01:00Z'
    });
  };
  const env = officeEnvironment({
    SUPABASE_URL: 'https://agency.test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role'
  });
  try {
    const result = await setMyTmsManagerEmailTemplates(env, { id: IDS.challenge }, {
      expected_version: 3, idempotency_key: 'manager-settings-save-1',
      templates: managerTemplates()
    });
    assert.equal(result.version, 4);
    assert.match(captured.url, /candidate_manager_email_settings_set_v1$/);
    assert.equal(captured.body.p_expected_version, 3);
    assert.equal(captured.body.p_idempotency_key, 'manager-settings-save-1');
    assert.equal(Object.hasOwn(captured.body, 'p_semantic_sha256_hex'), false);
    await assert.rejects(
      setMyTmsManagerEmailTemplates(env, { id: IDS.challenge }, {
        expected_version: 4, idempotency_key: 'manager-settings-save-2',
        templates: { ...managerTemplates(), unexpected: true }
      }),
      error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_MANAGER_TEMPLATE_INVALID'
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate Home announcement read, preview and save stay agency-owned and versioned', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(init.body);
    calls.push({ url: String(url), body });
    if (String(url).endsWith('candidate_home_announcement_settings_get_v1')) {
      return Response.json({ ok: true, announcement_text: 'Welcome.', version: 2, semantic_sha256_hex: 'a'.repeat(64), updated_at_utc: '2026-08-25T10:00:00Z' });
    }
    if (String(url).endsWith('candidate_home_announcement_settings_preview_v1')) {
      return Response.json({ ok: true, announcement_text: String(body.p_announcement_text).trim(), semantic_sha256_hex: 'b'.repeat(64) });
    }
    if (String(url).endsWith('candidate_home_announcement_settings_set_v1')) {
      return Response.json({ ok: true, announcement_text: body.p_announcement_text, version: 3, semantic_sha256_hex: 'c'.repeat(64), updated_at_utc: body.p_now_utc });
    }
    throw new Error('unexpected Home announcement request');
  };
  const env = officeEnvironment({
    SUPABASE_URL: 'https://agency.test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role'
  });
  try {
    const current = await getMyTmsHomeAnnouncementSettings(env, { id: IDS.challenge });
    assert.equal(current.announcement_text, 'Welcome.');
    assert.equal(current.version, 2);
    const preview = await previewMyTmsHomeAnnouncement(env, { id: IDS.challenge }, { announcement_text: '  Christmas timesheets close on 21 December.  ' });
    assert.equal(preview.announcement_text, 'Christmas timesheets close on 21 December.');
    const saved = await setMyTmsHomeAnnouncement(env, { id: IDS.challenge }, {
      expected_version: 2, idempotency_key: 'home-announcement-save-0001',
      announcement_text: preview.announcement_text
    });
    assert.equal(saved.version, 3);
    assert.equal(calls[2].body.p_expected_version, 2);
    assert.match(calls[2].body.p_actor_identity_hmac_hex, /^[a-f0-9]{64}$/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate Home announcement rejects unsafe Office requests before database access', async () => {
  await assert.rejects(
    setMyTmsHomeAnnouncement(officeEnvironment(), { id: IDS.challenge }, {
      expected_version: 1, idempotency_key: 'short', announcement_text: 'Message'
    }),
    error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_SETTINGS_REQUEST_INVALID'
  );
});

test('MyTMS Places and contacts read and save use one narrow versioned agency boundary', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(init.body);
    calls.push({ url: String(url), body });
    const common = {
      ok: true,
      hospital_addresses: [{
        hospital_name: 'North General Hospital',
        address: '1 Health Street\nLondon',
        telephone: '020 7123 4567',
        map_query: 'North General Hospital, London'
      }],
      accommodation_contacts: [{
        hospital_name: 'North General Hospital',
        office_name: 'Staff accommodation office',
        telephone: '020 7987 6543',
        email: 'housing@example.invalid',
        working_hours: 'Monday to Friday\n09:00–17:00'
      }],
      version: String(url).endsWith('candidate_daily_information_settings_get_v1') ? 2 : 3,
      semantic_sha256_hex: 'd'.repeat(64),
      updated_at_utc: '2026-08-30T03:20:00Z'
    };
    return Response.json(common);
  };
  const env = officeEnvironment({
    SUPABASE_URL: 'https://agency.test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role'
  });
  try {
    const current = await getMyTmsDailyInformationSettings(env, { id: IDS.challenge });
    assert.equal(current.version, 2);
    assert.equal(current.hospital_addresses[0].hospital_name, 'North General Hospital');

    const saved = await setMyTmsDailyInformationSettings(env, { id: IDS.challenge }, {
      expected_version: 2,
      idempotency_key: 'daily-information-save-0001',
      hospital_addresses: current.hospital_addresses,
      accommodation_contacts: current.accommodation_contacts
    });
    assert.equal(saved.version, 3);
    assert.match(calls[0].url, /candidate_daily_information_settings_get_v1$/);
    assert.match(calls[1].url, /candidate_daily_information_settings_set_v1$/);
    assert.deepEqual(calls[1].body.p_hospital_addresses, current.hospital_addresses);
    assert.deepEqual(calls[1].body.p_accommodation_contacts, current.accommodation_contacts);
    assert.equal(calls[1].body.p_expected_version, 2);
    assert.match(calls[1].body.p_actor_identity_hmac_hex, /^[a-f0-9]{64}$/);
    assert.deepEqual(Object.keys(calls[1].body).sort(), [
      'p_accommodation_contacts', 'p_actor_identity_hmac_hex', 'p_expected_version',
      'p_hospital_addresses', 'p_idempotency_key', 'p_now_utc'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('MyTMS Places and contacts fail closed on malformed reads and incomplete saves', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json({
    ok: true,
    hospital_addresses: [{ hospital_name: 'North General Hospital' }],
    accommodation_contacts: [],
    version: 1,
    semantic_sha256_hex: 'a'.repeat(64)
  });
  try {
    await assert.rejects(
      getMyTmsDailyInformationSettings(officeEnvironment({
        SUPABASE_URL: 'https://agency.test.invalid',
        SUPABASE_SERVICE_ROLE_KEY: 'test-agency-service-role'
      }), { id: IDS.challenge }),
      error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_OFFICE_RESPONSE_INVALID'
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
  await assert.rejects(
    setMyTmsDailyInformationSettings(officeEnvironment(), { id: IDS.challenge }, {
      expected_version: 1,
      idempotency_key: 'daily-information-save-0002',
      hospital_addresses: []
    }),
    error => error instanceof MyTmsOfficeError
      && error.code === 'CANDIDATE_DAILY_INFORMATION_REQUEST_INVALID'
  );
});

test('agency settings writes reject platform-owned fields and enforce invitation safety bounds', async () => {
  await assert.rejects(
    setMyTmsOfficeSettings(officeEnvironment(), { id: IDS.challenge }, {
      expected_version: 1, idempotency_key: 'platform-field',
      settings: { android_store_url: 'https://store.example' }
    }),
    error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_PLATFORM_SETTING_READ_ONLY'
  );
  await assert.rejects(
    setMyTmsOfficeSettings(officeEnvironment(), { id: IDS.challenge }, {
      expected_version: 1, idempotency_key: 'unsafe-expiry',
      settings: { invitation_expiry_seconds: 60 }
    }),
    error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_SETTINGS_REQUEST_INVALID'
  );
  for (const [idempotencyKey, settings] of [
    ['expiry-over-seven-days', { invitation_expiry_seconds: 604_801 }],
    ['resend-under-fifteen-minutes', { resend_minimum_seconds: 899 }],
    ['resend-over-one-day', { resend_minimum_seconds: 86_401 }],
    ['more-than-five-resends', { maximum_resends: 6 }]
  ]) {
    await assert.rejects(
      setMyTmsOfficeSettings(officeEnvironment(), { id: IDS.challenge }, {
        expected_version: 1, idempotency_key: idempotencyKey, settings
      }),
      error => error instanceof MyTmsOfficeError && error.code === 'MYTMS_SETTINGS_REQUEST_INVALID'
    );
  }
});

test('dangerous feature activation is rejected unless separately authorised', async () => {
  await assert.rejects(
    setMyTmsOfficeSettings(
      officeEnvironment(),
      { id: '10000000-0000-4000-8000-000000000042' },
      {
        expected_version: 1,
        idempotency_key: 'settings-test-1',
        settings: { invitation_email_enabled: true }
      }
    ),
    (error) => error instanceof MyTmsOfficeError
      && error.status === 403
      && error.code === 'MYTMS_ACTIVATION_NOT_AUTHORIZED'
  );
});

test('all material activation gates are disabled by default', () => {
  const env = officeEnvironment();
  for (const field of [
    'invitation_email_enabled', 'access_reminder_enabled', 'provisioning_enabled',
    'membership_admin_enabled', 'google_target_switch_enabled', 'push_delivery_enabled'
  ]) {
    assert.equal(myTmsOfficeInternals.activationAllowed(env, field), false, field);
  }
});

test('an open invitation offers both resend and cancellation without weakening either gate', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (request) => {
    const url = request instanceof Request ? request.url : String(request);
    if (url.includes('/rest/v1/candidates?')) {
      return Response.json([{
        id: IDS.challenge, email: 'candidate@example.test', display_name: 'Test Candidate',
        first_name: 'Test', last_name: 'Candidate', key_norm: 'CID1-ABCDE', active: true
      }]);
    }
    if (url.includes('/rpc/candidate_mytms_status_get_v1')) {
      return Response.json({
        ok: true, state: 'PENDING', action_code: 'CANCEL_INVITATION',
        delivery_state: 'PROVIDER_ACCEPTED', invitation_generation: 2,
        membership_id: IDS.outbox, membership_generation: 3, internal_only: true
      });
    }
    if (url.includes('/rpc/agency_app_settings_get_v1')) {
      return Response.json({
        ok: true, version: 4, membership_admin_enabled: true,
        invitation_email_enabled: true, access_reminder_enabled: true
      });
    }
    if (url.includes('/rest/v1/settings_defaults?')) {
      return Response.json([{ agency_name: 'Arthur Rai Medical Services Limited' }]);
    }
    throw new Error(`unexpected request ${url}`);
  };
  try {
    const result = await getMyTmsCandidateStatus(officeEnvironment({
      SUPABASE_URL: 'https://miget-agency-gateway.test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-miget-postgrest-role-not-live',
      MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-role-not-live',
      MYTMS_OFFICE_INVITATION_ACTIVATION_AUTHORIZED: 'TRUE',
      MYTMS_MEMBERSHIP_ADMIN_ACTIVATION_AUTHORIZED: 'TRUE'
    }), { id: IDS.challenge }, IDS.challenge);
    assert.deepEqual(result.actions.map(({ code, enabled }) => ({ code, enabled })), [
      { code: 'RESEND_INVITATION', enabled: true },
      { code: 'CANCEL_INVITATION', enabled: true }
    ]);
    assert.equal(result.action.code, 'CANCEL_INVITATION');
    assert.equal(result.agency_display_name, 'Arthur Rai Medical Services Limited');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate invitation identity comes only from agency settings and propagates to control and mail', async () => {
  const originalFetch = globalThis.fetch;
  const invitationId = '10000000-0000-4000-8000-000000000043';
  const calls = [];
  let outboxBody = null;
  globalThis.fetch = async (request, init = {}) => {
    const url = request instanceof Request ? request.url : String(request);
    const method = request instanceof Request ? request.method : String(init.method || 'GET');
    const body = method === 'GET' ? null
      : request instanceof Request ? await request.clone().json() : JSON.parse(String(init.body || '{}'));
    if (url.includes('/rest/v1/candidates?')) {
      return Response.json([{
        id: IDS.challenge, email: 'candidate@example.test', display_name: 'Test Candidate',
        first_name: 'Test', last_name: 'Candidate', key_norm: 'CID1-ABCDE', active: true
      }]);
    }
    if (url.includes('/rest/v1/settings_defaults?')) {
      calls.push('AGENCY_SETTINGS');
      return Response.json([{ agency_name: 'Arthur Rai Medical Services Limited' }]);
    }
    if (url.includes('/rpc/candidate_mytms_status_get_v1')) {
      return Response.json({ ok: true, state: 'NOT_INVITED', action_code: 'INVITE_TO_MYTMS' });
    }
    if (url.includes('/rpc/agency_app_settings_get_v1')) {
      return Response.json({
        ok: true, version: 4, invitation_email_enabled: true, access_reminder_enabled: false,
        membership_admin_enabled: true, invitation_expiry_seconds: 604800,
        planned_test_web_origin: 'https://mycloudtms.arthur-rai.co.uk',
        invitation_subject: 'Join {{agency_name}} on MyTMS',
        invitation_html_sanitized: '<p>{{agency_name}} has invited {{candidate_name}}.</p><p><a href="{{mytms_invitation_url}}">Join MyTMS</a></p>',
        invitation_text: '{{agency_name}} has invited {{candidate_name}}. {{mytms_invitation_url}}'
      });
    }
    if (url.includes('/rpc/invitation_reserve_v1')) {
      calls.push('CONTROL_RESERVE');
      assert.equal(body.p_office_context.agency_display_name, 'Arthur Rai Medical Services Limited');
      assert.equal(Object.hasOwn(body.p_office_context, 'MYTMS_OFFICE_AGENCY_DISPLAY_NAME'), false);
      return Response.json({
        ok: true, status: 'RESERVED', invitation_id: invitationId, generation: 1,
        idempotent_replay: false
      });
    }
    if (url.includes('/rest/v1/mail_outbox?on_conflict=')) {
      calls.push('AGENCY_OUTBOX');
      outboxBody = body;
      return Response.json([{ id: IDS.outbox }], { status: 201 });
    }
    if (url.includes('/rpc/invitation_delivery_record_v1')) {
      calls.push('CONTROL_DELIVERY');
      return Response.json({ ok: true, status: 'RECORDED' });
    }
    throw new Error(`unexpected request ${url}`);
  };
  try {
    const result = await reserveAndQueueMyTmsInvitation(officeEnvironment({
      SUPABASE_URL: 'https://miget-agency-gateway.test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-miget-postgrest-role-not-live',
      MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-role-not-live',
      MYTMS_OFFICE_INVITATION_ACTIVATION_AUTHORIZED: 'TRUE',
      MYTMS_OFFICE_INVITATION_DELIVERY_ENABLED: 'TRUE',
      MYTMS_INVITATION_TOKEN_SECRET: 'test-invitation-token-secret-not-live'
    }), { id: IDS.challenge }, IDS.challenge, {
      intent: 'INVITE', idempotency_key: IDS.outbox, expected_settings_version: 4
    });
    assert.equal(result.status, 'OUTBOX_ACCEPTED');
    assert.match(outboxBody.subject, /Arthur Rai Medical Services Limited/);
    assert.match(outboxBody.body_html, /Arthur Rai Medical Services Limited/);
    assert.match(outboxBody.body_text, /Arthur Rai Medical Services Limited/);
    assert.doesNotMatch(`${outboxBody.subject}\n${outboxBody.body_html}\n${outboxBody.body_text}`, /CloudTMS/);
    assert.deepEqual(calls, [
      'AGENCY_SETTINGS', 'CONTROL_RESERVE', 'AGENCY_OUTBOX', 'CONTROL_DELIVERY'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('missing agency identity fails closed before invitation reservation or outbox creation', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (request) => {
    const url = request instanceof Request ? request.url : String(request);
    calls.push(url);
    if (url.includes('/rest/v1/candidates?')) {
      return Response.json([{
        id: IDS.challenge, email: 'candidate@example.test', display_name: 'Test Candidate',
        first_name: 'Test', last_name: 'Candidate', key_norm: 'CID1-ABCDE', active: true
      }]);
    }
    if (url.includes('/rpc/candidate_mytms_status_get_v1')) {
      return Response.json({ ok: true, state: 'NOT_INVITED', action_code: 'INVITE_TO_MYTMS' });
    }
    if (url.includes('/rpc/agency_app_settings_get_v1')) {
      return Response.json({
        ok: true, version: 4, invitation_email_enabled: true,
        access_reminder_enabled: false, membership_admin_enabled: true
      });
    }
    if (url.includes('/rest/v1/settings_defaults?')) {
      return Response.json([{ agency_name: '   ' }]);
    }
    throw new Error(`invitation authority must not be reached: ${url}`);
  };
  try {
    await assert.rejects(
      getMyTmsCandidateStatus(officeEnvironment({
        SUPABASE_URL: 'https://miget-agency-gateway.test.invalid',
        SUPABASE_SERVICE_ROLE_KEY: 'test-miget-postgrest-role-not-live',
        MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
        MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-role-not-live'
      }), { id: IDS.challenge }, IDS.challenge),
      (error) => error instanceof MyTmsOfficeError
        && error.status === 503
        && error.code === 'MYTMS_AGENCY_PRESENTATION_UNAVAILABLE'
    );
    assert.equal(calls.some((url) => url.includes('/rpc/invitation_reserve_v1')), false);
    assert.equal(calls.some((url) => url.includes('/rest/v1/mail_outbox?')), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('adoption and membership mutation stop at the disabled-first admin gate', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async request => {
    const url = request instanceof Request ? request.url : String(request);
    calls.push(url);
    if (url.includes('/rpc/agency_app_settings_get_v1')) {
      return Response.json({
        ok: true, version: 1, membership_admin_enabled: false,
        invitation_email_enabled: false, access_reminder_enabled: false
      });
    }
    throw new Error('no adoption or agency mutation may pass a disabled gate');
  };
  const env = officeEnvironment({
    MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-role-not-live'
  });
  try {
    await assert.rejects(
      adoptMyTmsCandidate(env, { id: IDS.challenge }, IDS.challenge, {
        idempotency_key: 'adoption-disabled',
        proof: { account_id: IDS.outbox }
      }),
      error => error instanceof MyTmsOfficeError
        && error.code === 'MYTMS_MEMBERSHIP_ADMIN_DISABLED'
    );
    await assert.rejects(
      setMyTmsMembershipState(env, { id: IDS.challenge }, IDS.outbox, {
        candidate_id: IDS.challenge, global_account_id: IDS.outbox,
        transition: 'ACTIVATE', expected_generation: 1,
        idempotency_key: 'membership-disabled'
      }),
      error => error instanceof MyTmsOfficeError
        && error.code === 'MYTMS_MEMBERSHIP_ADMIN_DISABLED'
    );
    assert.equal(calls.some((url) => /adopt|membership_(?:transition|state)/i.test(url)), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('membership revocation commits centrally before the Miget agency projection and trusts no browser account id', async () => {
  const originalFetch = globalThis.fetch;
  const membershipId = IDS.outbox;
  const globalAccountId = '10000000-0000-4000-8000-000000000045';
  const calls = [];
  globalThis.fetch = async (request, init = {}) => {
    const url = request instanceof Request ? request.url : String(request);
    const method = request instanceof Request ? request.method : String(init.method || 'GET');
    const body = method !== 'GET'
      ? request instanceof Request ? await request.clone().json() : JSON.parse(String(init.body || '{}'))
      : null;
    if (url.includes('/rpc/agency_app_settings_get_v1')) {
      calls.push('SETTINGS');
      return Response.json({
        ok: true, version: 3, membership_admin_enabled: true,
        invitation_email_enabled: true, access_reminder_enabled: true
      });
    }
    if (url.includes('/rest/v1/candidates?')) {
      calls.push('CANDIDATE');
      return Response.json([{
        id: IDS.challenge, email: 'candidate@example.test', display_name: 'Test Candidate',
        first_name: 'Test', last_name: 'Candidate', key_norm: 'CID1-ABCDE', active: true
      }]);
    }
    if (url.includes('/rpc/membership_state_set_v1')) {
      calls.push('CENTRAL_REVOKE');
      assert.equal(body.p_membership_id, membershipId);
      assert.equal(body.p_transition, 'REVOKE');
      assert.equal(Object.hasOwn(body, 'global_account_id'), false);
      return Response.json({
        ok: true, status: 'REVOKED', state: 'REVOKED', generation: 5,
        global_account_id_internal: globalAccountId,
        local_candidate_id_internal: IDS.challenge,
        idempotent_replay: false, internal_only: true
      });
    }
    if (url.includes('/rpc/candidate_app_federated_membership_link_set_v1')) {
      calls.push('AGENCY_PROJECTION');
      assert.equal(body.p_membership_id, membershipId);
      assert.equal(body.p_membership_generation, 5);
      assert.equal(body.p_candidate_id, IDS.challenge);
      assert.equal(body.p_target_state, 'REVOKED');
      return Response.json({ ok: true, status: 'UPDATED' });
    }
    throw new Error(`unexpected request ${url}`);
  };
  const env = officeEnvironment({
    MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-role-not-live',
    MYTMS_MEMBERSHIP_ADMIN_ACTIVATION_AUTHORIZED: 'TRUE',
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_FEDERATED_IDENTITY_SECRET: 'test-federated-identity-secret-not-live',
    SUPABASE_URL: 'https://miget-agency-gateway.test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-miget-postgrest-role-not-live'
  });
  try {
    const result = await setMyTmsMembershipState(
      env, { id: IDS.challenge }, membershipId,
      {
        candidate_id: IDS.challenge, transition: 'REVOKE', expected_generation: 4,
        idempotency_key: 'membership-revoke-central-first', reason: 'Office access revoked'
      }
    );
    assert.deepEqual(result, {
      ok: true, status: 'REVOKED', state: 'REVOKED',
      membership_generation: 5, idempotent_replay: false
    });
    assert.deepEqual(calls, ['SETTINGS', 'CANDIDATE', 'CENTRAL_REVOKE', 'AGENCY_PROJECTION']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('invitation delivery reference parser rejects cross-row and malformed identities', () => {
  const invitationId = '10000000-0000-4000-8000-000000000043';
  const hash = 'a'.repeat(64);
  const row = {
    id: IDS.outbox,
    context_kind: 'MYTMS_INVITATION',
    context_id: invitationId,
    reference: `mytms_invitation:${invitationId}:2:${hash}`
  };
  assert.deepEqual(myTmsOfficeInternals.parseInvitationReference(row), {
    invitationId,
    generation: 2,
    semanticHash: hash,
    outboxId: IDS.outbox
  });
  assert.equal(myTmsOfficeInternals.parseInvitationReference({
    ...row,
    context_id: '10000000-0000-4000-8000-000000000044'
  }), null);
  assert.equal(myTmsOfficeInternals.parseInvitationReference({
    ...row,
    reference: `mytms_invitation:${invitationId}:0:${hash}`
  }), null);
});

test('identity challenge delivery queues only through a deterministic TEST outbox seam', async () => {
  const originalFetch = globalThis.fetch;
  let captured = null;
  globalThis.fetch = async (url, init) => {
    captured = { url: String(url), init, body: JSON.parse(init.body) };
    return new Response(JSON.stringify([{ id: IDS.outbox }]), {
      status: 201,
      headers: { 'content-type': 'application/json' }
    });
  };
  try {
    const result = await queueMyTmsIdentityChallenge(
      officeEnvironment({
        MYTMS_IDENTITY_DELIVERY_ENABLED: 'TRUE',
        MYTMS_IDENTITY_WEB_ORIGIN: 'https://mytms.test.example',
        SUPABASE_URL: 'https://test-data-plane.example',
        SUPABASE_SERVICE_ROLE_KEY: 'test-service-role-placeholder'
      }),
      {
        email: 'candidate@example.test',
        purpose: 'ACTIVATE',
        challenge_id: IDS.challenge,
        token: 'test-challenge-token-that-is-long-enough-to-be-valid',
        deterministic_outbox_key: `mytms:identity:${IDS.challenge}`
      }
    );
    assert.deepEqual(result, { ok: true, accepted: true });
    assert.match(captured.url, /^https:\/\/test-data-plane\.example\/rest\/v1\/mail_outbox/);
    assert.equal(captured.body.context_kind, 'MYTMS_IDENTITY_CHALLENGE');
    assert.equal(captured.body.context_id, IDS.challenge);
    assert.equal(captured.body.deterministic_outbox_key, `mytms:identity:${IDS.challenge}`);
    assert.match(
      captured.body.body_html,
      new RegExp(
        `https:\\/\\/mytms\\.test\\.example\\/candidate\\/activate#token=`
          + `[^&]+&amp;challenge=${IDS.challenge}`
      )
    );
    assert.doesNotMatch(captured.body.body_html, /[?&]token=/);
    assert.doesNotMatch(captured.body.body_html, /javascript:|onerror|<script/i);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Office MyTMS routes retain existing admin authentication', async () => {
  for (const [url, method, body] of [
    ['https://office.test.example/api/mytms/settings', 'GET', undefined],
    ['https://office.test.example/api/mytms/manager-email-settings', 'GET', undefined],
    ['https://office.test.example/api/mytms/home-announcement', 'GET', undefined],
    [`https://office.test.example/api/mytms/candidates/${IDS.challenge}/adopt`, 'POST', '{}'],
    [`https://office.test.example/api/mytms/memberships/${IDS.outbox}/state`, 'POST', '{}']
  ]) {
    const response = await cloudTmsWorker.fetch(
      new Request(url, { method, ...(body ? { body } : {}) }), {}, {}
    );
    assert.equal(response.status, 401, url);
    assert.deepEqual(await response.json(), {
      ok: false,
      error_code: 'MYTMS_OFFICE_PERMISSION_DENIED'
    });
  }
});

test('identity delivery route requires service authentication before disabled feature state', async () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-private-service-secret-that-is-not-live'
  };
  const unsigned = new Request(
    'https://office.test.example/private/mytms-control/v1/auth/challenge-delivery',
    { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }
  );
  const rejected = await cloudTmsWorker.fetch(unsigned.clone(), env, {});
  assert.equal(rejected.status, 401);

  const signed = await signCandidatePrivateRequest(unsigned, env);
  const disabled = await cloudTmsWorker.fetch(signed, env, {});
  assert.equal(disabled.status, 403);
  assert.deepEqual(await disabled.json(), {
    ok: false,
    error_code: 'MYTMS_IDENTITY_DELIVERY_DISABLED'
  });
});
