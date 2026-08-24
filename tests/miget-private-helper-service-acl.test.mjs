import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const migration = readFileSync(
  new URL('../supabase/migrations/24082026_1702_miget_private_helper_service_acl.sql', import.meta.url),
  'utf8'
);
const release = JSON.parse(readFileSync(
  new URL('../supabase/release/current-release.json', import.meta.url),
  'utf8'
));

test('Miget provider-owner ACL repair is narrow, data-free, and verified', () => {
  assert.match(migration, /to_regprocedure\([\s\S]*_candidate_manager_email_claim_route_current_v1/i);
  assert.match(migration, /grant execute on function private\._candidate_manager_email_claim_route_current_v1[\s\S]*to service_role/i);
  assert.match(migration, /has_function_privilege\([\s\S]*service_role[\s\S]*EXECUTE/i);
  assert.doesNotMatch(migration, /\b(insert|update|delete|truncate)\b/i);
});

test('protected releases execute the service-role route-guard verifier', () => {
  assert.ok(release.verificationFiles.includes(
    'supabase/verification/23082026_0822_candidate_manager_email_claim_route_guard_verification.sql'
  ));
});
