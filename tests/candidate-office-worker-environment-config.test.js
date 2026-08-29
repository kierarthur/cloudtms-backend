import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const directory = path.dirname(fileURLToPath(import.meta.url));
const wrangler = fs.readFileSync(path.resolve(directory, '../wrangler.toml'), 'utf8');
const testEnvironment = wrangler.split('[env.test.vars]')[1]?.split('[[env.test.r2_buckets]]')[0] || '';
const liveEnvironment = wrangler.split('[env.production.vars]')[1]?.split('[[env.production.r2_buckets]]')[0] || '';

test('normal TEST Worker supplies the Candidate Office environment authority', () => {
  assert.match(wrangler, /\[env\.test\]\s*\r?\nname\s*=\s*"test-cloudtms-backend"/);
  assert.match(testEnvironment, /^CANDIDATE_APP_ENVIRONMENT\s*=\s*"TEST"\s*$/m);
  assert.equal((testEnvironment.match(/^CANDIDATE_APP_ENVIRONMENT\s*=/gm) || []).length, 1);
});

test('LIVE receives the same Office code with separate disabled-first authority', () => {
  assert.match(wrangler, /\[env\.production\]\s*\r?\nname\s*=\s*"cloudtms"\s*\r?\ncompatibility_flags\s*=\s*\["nodejs_compat",\s*"global_fetch_strictly_public"\]\s*\r?\npreview_urls\s*=\s*false/);
  const expected = new Map([
    ['CANDIDATE_APP_ENVIRONMENT', 'LIVE'],
    ['MYTMS_CONTROL_PLANE_ENABLED', 'TRUE'],
    ['MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED', 'FALSE'],
    ['MYTMS_CONTROL_PLANE_URL', 'https://cloudtms-mytms-miget-gateway.kier-88a.workers.dev'],
    ['MYTMS_OFFICE_CONTROL_ENABLED', 'TRUE'],
    ['MYTMS_OFFICE_AGENCY_ID', '71db56e1-11c2-4d0d-88e3-224aa064c536'],
    ['MYTMS_OFFICE_INVITATION_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_OFFICE_INVITATION_DELIVERY_ENABLED', 'FALSE'],
    ['MYTMS_IDENTITY_DELIVERY_ENABLED', 'FALSE'],
    ['MYTMS_IDENTITY_WEB_ORIGIN', 'https://mycloudtms.arthur-rai.co.uk'],
    ['MYTMS_GOOGLE_PROVISIONING_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_GOOGLE_SWITCH_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_MEMBERSHIP_ADMIN_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_PUSH_DELIVERY_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['BANKING_PAY_CORRECTION_TENANT_ID_V1', '0be11296-ded7-48f4-8ea3-58d3301a8395'],
    ['INVOICE_ASYNC_PIPELINE_ENABLED', 'false'],
    ['INVOICE_ASYNC_SCHEDULED_ENABLED', 'false'],
    ['INVOICE_DOCUMENT_PROCESSOR_ENABLED', 'false'],
    ['INVOICE_QUEUE_CONTINUATION_ENABLED', 'false'],
    ['BANKING_PAY_CONTINUATION_ENABLED', 'false']
  ]);
  for (const [name, value] of expected) {
    assert.match(liveEnvironment, new RegExp(`^${name}\\s*=\\s*"${value.replaceAll('.', '\\.')}"\\s*$`, 'm'));
    assert.equal((liveEnvironment.match(new RegExp(`^${name}\\s*=`, 'gm')) || []).length, 1);
  }
  assert.equal(
    /^MYTMS_OFFICE_AGENCY_DISPLAY_NAME\s*=/m.test(liveEnvironment), false,
    'LIVE agency display name must remain database-owned'
  );
});

test('normal TEST Worker installs only the expressly authorised MyTMS TEST controls', () => {
  assert.match(wrangler, /\[env\.test\]\s*\r?\nname\s*=\s*"test-cloudtms-backend"\s*\r?\ncompatibility_flags\s*=\s*\["nodejs_compat",\s*"global_fetch_strictly_public"\]\s*\r?\npreview_urls\s*=\s*false/);
  assert.equal((wrangler.match(/^preview_urls\s*=/gm) || []).length, 2);
  const expected = new Map([
    ['MYTMS_CONTROL_PLANE_ENABLED', 'TRUE'],
    ['MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED', 'TRUE'],
    ['MYTMS_CONTROL_PLANE_URL', 'https://cloudtms-mytms-miget-gateway.kier-88a.workers.dev'],
    ['MYTMS_OFFICE_CONTROL_ENABLED', 'TRUE'],
    ['MYTMS_OFFICE_AGENCY_ID', '6d0aadb2-ddc8-4ee4-ab37-871bae4a0d88'],
    ['MYTMS_OFFICE_INVITATION_ACTIVATION_AUTHORIZED', 'TRUE'],
    ['MYTMS_OFFICE_INVITATION_DELIVERY_ENABLED', 'TRUE'],
    ['MYTMS_IDENTITY_DELIVERY_ENABLED', 'TRUE'],
    ['MYTMS_IDENTITY_WEB_ORIGIN', 'https://mycloudtms.arthur-rai.co.uk'],
    ['MYTMS_GOOGLE_PROVISIONING_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_GOOGLE_SWITCH_ACTIVATION_AUTHORIZED', 'FALSE'],
    ['MYTMS_MEMBERSHIP_ADMIN_ACTIVATION_AUTHORIZED', 'TRUE'],
    ['MYTMS_PUSH_DELIVERY_ACTIVATION_AUTHORIZED', 'FALSE']
  ]);
  for (const [name, value] of expected) {
    assert.match(testEnvironment, new RegExp(`^${name}\\s*=\\s*"${value.replaceAll('.', '\\.')}"\\s*$`, 'm'));
    assert.equal((testEnvironment.match(new RegExp(`^${name}\\s*=`, 'gm')) || []).length, 1);
  }
  assert.equal(
    /^MYTMS_OFFICE_AGENCY_DISPLAY_NAME\s*=/m.test(testEnvironment), false,
    'the agency display name must come from agency settings_defaults, not Worker configuration'
  );
  assert.match(
    testEnvironment,
    /^ALLOWED_ORIGINS\s*=\s*"[^"]*https:\/\/mycloudtms\.arthur-rai\.co\.uk[^"]*"\s*$/m
  );
});
