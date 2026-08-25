import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const verifier = readFileSync(
  new URL('../supabase/verification/23082026_0250_candidate_manager_email_security_verification.sql', import.meta.url),
  'utf8'
);

test('manager email security verification accepts authorised operational TEST state', () => {
  assert.match(verifier, /candidate_app_environment[\s\S]*<>'TEST'/i);
  assert.match(verifier, /jsonb_typeof\(f\.value\)<>'boolean'/i);
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_OPERATIONAL_TEST_STATE_INVALID/i);
  assert.doesNotMatch(verifier, /CANDIDATE_MANAGER_EMAIL_INSTALL_NOT_INERT/i);
  assert.doesNotMatch(verifier, /f\.value='true'::jsonb/i);
});

test('manager email operational verification retains the structural security boundary', () => {
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_RLS_NOT_FORCED/i);
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_TABLE_PRIVILEGE_INVALID/i);
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_FUNCTION_SECURITY_INVALID/i);
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_BROWSER_FUNCTION_PRIVILEGE/i);
  assert.match(verifier, /CANDIDATE_MANAGER_EMAIL_TEMPLATE_AUTHORITY_INVALID/i);
});
