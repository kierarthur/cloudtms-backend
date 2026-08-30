import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const repeatable = fs.readFileSync(
  new URL('../supabase/repeatable/30082026_1113_candidate_paper_return_proof_transaction_boundary_v1.sql', import.meta.url),
  'utf8'
);
const authorityVerifier = fs.readFileSync(
  new URL('../supabase/verification/23082026_1347_candidate_app_finalisation_authority_verification.sql', import.meta.url),
  'utf8'
);
const boundaryVerifier = fs.readFileSync(
  new URL('../supabase/verification/30082026_1113_candidate_paper_return_proof_transaction_boundary_verification.sql', import.meta.url),
  'utf8'
);
const contract = JSON.parse(fs.readFileSync(
  new URL('../supabase/release/current-contract.json', import.meta.url),
  'utf8'
));

test('returned-paper proof retains its lock and is classified as a write-capable transaction', () => {
  assert.match(repeatable, /candidate_paper_return_proof_validate_v1[\s\S]*language plpgsql\s+volatile\s+security definer/);
  assert.match(repeatable, /_candidate_session_context_v1\(\s*p_session_id,p_environment,null,p_now_utc,true\s*\)/);
  assert.match(repeatable, /CANDIDATE_PAPER_QR_PROOF_MISMATCH/);
  assert.match(repeatable, /CANDIDATE_PAPER_QR_PROOF_FORBIDDEN/);
  assert.match(repeatable, /notify pgrst, 'reload schema'/);
});

test('catalogue verifiers require VOLATILE only for the QR proof and retain the service-only boundary', () => {
  assert.match(authorityVerifier, /candidate_app_timesheet_detail_v2[\s\S]*'s'::"char"/);
  assert.match(authorityVerifier, /candidate_break_entry_context_get_v1[\s\S]*'s'::"char"/);
  assert.match(authorityVerifier, /candidate_paper_return_proof_validate_v1[\s\S]*'v'::"char"/);
  assert.match(boundaryVerifier, /v_volatility<>'v'/);
  assert.match(boundaryVerifier, /v_owner<>current_user/);
  assert.match(authorityVerifier, /owner_name<>current_user/);
  assert.match(boundaryVerifier, /has_function_privilege\('service_role'/);
  assert.match(boundaryVerifier, /has_function_privilege\('anon'/);
  assert.match(boundaryVerifier, /has_function_privilege\('authenticated'/);
});

test('changed SQL contains no schema-qualified PostgreSQL conditional constructs', () => {
  for (const illegal of ['pg_catalog.coalesce', 'pg_catalog.nullif', 'pg_catalog.least', 'pg_catalog.greatest']) {
    assert.equal(repeatable.toLowerCase().includes(illegal), false, illegal);
  }
});

test('generated PostgreSQL contract records the QR proof as VOLATILE', () => {
  const routine = contract.routines.find(item =>
    item.schema === 'public'
    && item.identity.startsWith('candidate_paper_return_proof_validate_v1(')
  );
  assert.ok(routine);
  assert.equal(routine.volatility, 'v');
  assert.equal(routine.security_definer, true);
  assert.deepEqual(routine.acl.map(item => item.grantee), ['postgres', 'service_role']);
});
