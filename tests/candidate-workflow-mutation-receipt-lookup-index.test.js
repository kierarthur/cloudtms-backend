import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const migration = fs.readFileSync(
  new URL('../supabase/migrations/31082026_0924_candidate_workflow_mutation_receipt_lookup_index.sql', import.meta.url),
  'utf8'
);
const verification = fs.readFileSync(
  new URL('../supabase/verification/31082026_0925_candidate_workflow_mutation_receipt_lookup_verification.sql', import.meta.url),
  'utf8'
);

test('workflow mutation replay has a request-key-first covering index', () => {
  assert.match(migration, /\(\s*correlation_id,\s*ts_utc desc,\s*id desc\s*\)/i);
  assert.match(migration, /include\s*\(\s*object_id_text,\s*before_json,\s*after_json\s*\)/i);
  assert.match(migration, /where\s+object_type\s*=\s*'candidate_workflow_mutation_receipt'/i);
  assert.doesNotMatch(migration, /drop\s+(?:index|table)|delete\s+from|truncate/i);
});

test('managed release verifies the exact replay index shape', () => {
  assert.match(verification, /idx_audit_candidate_workflow_mutation_request_v1/);
  assert.match(verification, /CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_MISSING/);
  assert.match(verification, /CANDIDATE_WORKFLOW_MUTATION_REQUEST_INDEX_INVALID/);
});
