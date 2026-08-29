import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (file) => fs.readFileSync(path.join(root, file), 'utf8');

const replacement = read('supabase/repeatable/29082026_0922_candidate_daily_reconciliation_linked_window_v1.sql');
const verifier = read('supabase/verification/29082026_0923_candidate_daily_reconciliation_linked_window_verification.sql');
const contract = read('broker/src/candidate-daily-contract-v1.js');
const release = JSON.parse(read('supabase/release/current-release.json'));

test('linked-window reconciliation checks all identities without repeating sync refresh per date', () => {
  assert.match(replacement, /classification','NOT_ENROLLED'/);
  assert.match(replacement, /v_reconciled_candidates/);
  assert.equal((replacement.match(/perform private\._candidate_daily_refresh_sync_state_v1\(/g) || []).length, 1);
  assert.doesNotMatch(replacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(verifier, /v_refresh_calls<>1/);
});

test('closed Worker response contract accepts the benign not-enrolled result only for reconciliation', () => {
  assert.match(contract, /'MATCH', 'REPAIR_PROJECTION', 'CANONICAL_COMMAND_REQUIRED', 'NOT_ENROLLED'/);
  assert.match(contract, /options\.kind === 'reconciliation'/);
});

test('linked-window verifier is mandatory for managed UPGRADE and clean NEW', () => {
  const file = 'supabase/verification/29082026_0923_candidate_daily_reconciliation_linked_window_verification.sql';
  assert.ok(release.verificationFiles.includes(file));
  assert.ok(release.newVerificationFiles.includes(file));
});
