import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const replacement = read(
  'supabase',
  'repeatable',
  '29082026_0801_banking_pay_version_one_refresh_v1.sql',
);
const verifier = read(
  'supabase',
  'verification',
  '29082026_0804_banking_pay_version_one_refresh_verification.sql',
);
const release = JSON.parse(read('supabase', 'release', 'current-release.json'));

test('version-one refresh skips only the impossible previous-version rebase', () => {
  assert.match(replacement, /IF v_session\.version = 1 THEN/);
  assert.match(replacement, /SESSION_VERSION_ONE_HAS_NO_PREVIOUS_VERSION/);
  assert.match(replacement, /ELSE\s+v_rebase_result := private\.pay_workbench_candidate_session_version_rebase_v1\(/s);
  assert.match(replacement, /v_session\.version - 1/);
  assert.match(replacement, /pay_workbench_enqueue_session_candidate_refresh/);
});

test('version-one refresh preserves current, active-owner, rebase and canonical enqueue routes', () => {
  for (const marker of [
    "'route', 'CURRENT_NO_CHANGE'",
    "'route', 'ACTIVE_OWNER'",
    "'route', 'SESSION_VERSION_REBASE'",
    "'force_refresh', false",
    "'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'",
    "'policy_x_scope', 'PRE_DRAFT_LIVE_TRUTH'",
  ]) assert.match(replacement, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.doesNotMatch(replacement, /pay_batch_execute|pay_batch_settle|provider|remittance|communications/i);
});

test('version-one refresh retains service-only empty-search-path authority', () => {
  assert.match(replacement, /SECURITY DEFINER\s+SET search_path TO ''/s);
  assert.match(replacement, /REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC,anon,authenticated/);
  assert.match(replacement, /GRANT EXECUTE ON FUNCTION[\s\S]*TO service_role/);
  assert.doesNotMatch(replacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('mandatory rollback verifier proves first use, replay and no Draft creation', () => {
  assert.match(verifier, /v_session_id[\s\S]*version,progress_counter_version/);
  assert.match(verifier, /SESSION_VERSION_ONE_HAS_NO_PREVIOUS_VERSION/);
  assert.match(verifier, /WORKBENCH_CANDIDATE_SOURCE_BUILD/);
  assert.match(verifier, /queued_count'[\s\S]*<> 0/);
  assert.match(verifier, /reused_count'[\s\S]*<> 1/);
  assert.match(verifier, /BANKING_PAY_VERSION_ONE_REFRESH_CREATED_DRAFT/);
  assert.match(verifier, /rollback;/i);
});

test('version-one first-use proof is mandatory for NEW and UPGRADE', () => {
  const verification = 'supabase/verification/29082026_0804_banking_pay_version_one_refresh_verification.sql';
  assert.ok(release.verificationFiles.includes(verification));
  assert.ok(release.newVerificationFiles.includes(verification));
});
