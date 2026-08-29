import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const migration = fs.readFileSync(path.join(root,
  'supabase/migrations/28082026_0629_candidate_pre_fix_weekly_cancellation_residue_repair.sql'), 'utf8');
const verification = fs.readFileSync(path.join(root,
  'supabase/verification/28082026_0630_candidate_pre_fix_weekly_cancellation_residue_repair_verification.sql'), 'utf8');

test('pre-fix cancellation repair is bounded to cancelled weekly Candidate residues', () => {
  assert.match(migration, /state='CANCELLED'[\s\S]*scope='WEEKLY'[\s\S]*workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/i);
  assert.match(migration, /v_residue_count>100[\s\S]*CANDIDATE_CANCELLATION_RESIDUE_SCOPE_UNEXPECTED/i);
  assert.match(migration, /_candidate_route_family_v1[\s\S]*IMPORT_AUTHORITATIVE[\s\S]*CANDIDATE_RECORD_VIEW_ONLY/i);
});

test('pre-fix cancellation repair reuses the protected reset and preserves history', () => {
  assert.match(migration, /_candidate_weekly_withdrawal_reset_v1\(\$1,\$2,\$3\)/i);
  assert.doesNotMatch(migration, /\bdelete\s+from\b/i);
  assert.doesNotMatch(migration, /\btruncate\b/i);
  assert.doesNotMatch(migration, /\bpg_catalog\.(?:coalesce|nullif|least|greatest)\b/i);
});

test('post-release verification fails closed if any trapped cancellation remains', () => {
  assert.match(verification, /_candidate_weekly_withdrawal_reset_v1/i);
  assert.match(verification, /v_residue_count<>0[\s\S]*CANDIDATE_CANCELLATION_RESIDUE_REMAINS/i);
  assert.doesNotMatch(verification, /\bpg_catalog\.(?:coalesce|nullif|least|greatest)\b/i);
});
