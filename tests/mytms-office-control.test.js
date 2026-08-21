import assert from 'node:assert/strict';
import test from 'node:test';

import cloudTmsWorker from '../broker/src/index.js';
import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import {
  adoptMyTmsCandidate,
  MyTmsOfficeError,
  myTmsOfficeInternals,
  previewMyTmsTemplate,
  queueMyTmsIdentityChallenge,
  sanitizeMyTmsEmailHtml,
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

test('adoption and membership mutation stop at the disabled-first admin gate', async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async request => {
    calls += 1;
    const url = request instanceof Request ? request.url : String(request);
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
    assert.equal(calls, 2);
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
    assert.match(captured.body.body_html, /https:\/\/mytms\.test\.example\/auth\/verify\?token=/);
    assert.doesNotMatch(captured.body.body_html, /javascript:|onerror|<script/i);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Office MyTMS routes retain existing admin authentication', async () => {
  for (const [url, method, body] of [
    ['https://office.test.example/api/mytms/settings', 'GET', undefined],
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
