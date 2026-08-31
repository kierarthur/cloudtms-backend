import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const migration = fs.readFileSync(
  new URL('../supabase/migrations/31082026_0923_candidate_workflow_mutation_receipt_lookup_index_compatibility.sql', import.meta.url),
  'utf8'
);
const verification = fs.readFileSync(
  new URL('../supabase/verification/31082026_0925_candidate_workflow_mutation_receipt_lookup_verification.sql', import.meta.url),
  'utf8'
);

test('workflow mutation replay has a request-key-first bounded index', () => {
  assert.match(migration, /\(\s*correlation_id,\s*ts_utc desc,\s*id desc\s*\)/i);
  assert.match(migration, /where\s+object_type\s*=\s*'candidate_workflow_mutation_receipt'/i);
  assert.doesNotMatch(migration, /\binclude\s*\(/i);
  assert.doesNotMatch(migration, /drop\s+(?:index|table)|delete\s+from|truncate/i);
});

test('managed release verifies the exact replay index shape', () => {
  assert.match(verification, /idx_audit_candidate_workflow_mutation_request_v1/);
  assert.match(verification, /CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_MISSING/);
  assert.match(verification, /CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_INVALID/);
});
