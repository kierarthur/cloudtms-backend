const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const closurePath = 'supabase/repeatable/05092026_1200_banking_pay_draft_v8_final_authority_closure.sql';
const h1Path = 'supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql';
const weeklyPath = 'supabase/repeatable/27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql';
const canonicalPath = 'supabase/repeatable/04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql';
const allocationPath = 'supabase/repeatable/04092026_1360_banking_pay_manual_carry_forward_allocation_seed_v8.sql';
const integrityPath = 'supabase/repeatable/05092026_0310_banking_pay_draft_integrity_setwise_v8.sql';
const candidateCancelIntegrityPath = 'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql';
const cancellationCompletionPath = 'supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql';
const reservationEvidencePrecedencePath = 'supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql';
const sql = fs.readFileSync(path.join(root, closurePath), 'utf8');

test('final H1/H2 closure restores only the seven reviewed current owners after H1', async () => {
  const { closureFor, sqlDateKey } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const closure = closureFor(closurePath);

  assert.ok(sqlDateKey(h1Path).localeCompare(sqlDateKey(closurePath)) < 0);
  assert.deepEqual(
    [...sql.matchAll(/^\\ir\s+([^\s;]+)\s*;?\s*$/gm)].map(match => `supabase/repeatable/${match[1]}`),
    [
      h1Path,
      weeklyPath,
      canonicalPath,
      allocationPath,
      integrityPath,
      candidateCancelIntegrityPath,
      cancellationCompletionPath,
      reservationEvidencePrecedencePath,
    ],
  );
  assert.ok(closure.paths.indexOf(h1Path) >= 0);
  assert.ok(closure.paths.lastIndexOf(weeklyPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(canonicalPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(allocationPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(integrityPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(candidateCancelIntegrityPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(cancellationCompletionPath) > closure.paths.indexOf(h1Path));
  assert.ok(closure.paths.lastIndexOf(reservationEvidencePrecedencePath) > closure.paths.indexOf(h1Path));
  assert.doesNotMatch(sql, /^CREATE(?: OR REPLACE)? FUNCTION\b/gmi);
  assert.doesNotMatch(sql, /^\s*(?:BEGIN|COMMIT|ROLLBACK)\s*;/gmi);
});

test('final closure is policy-neutral and the included owners remain single-function authorities', () => {
  assert.match(sql, /changes no[\s\S]*selection, amount, tax, VAT, payment, provider or cancellation policy/i);
  for (const relativePath of [
    weeklyPath,
    canonicalPath,
    allocationPath,
    integrityPath,
    candidateCancelIntegrityPath,
    cancellationCompletionPath,
    reservationEvidencePrecedencePath,
  ]) {
    const owner = fs.readFileSync(path.join(root, relativePath), 'utf8');
    assert.equal((owner.match(/^CREATE OR REPLACE FUNCTION\b/gm) || []).length, 1, relativePath);
    assert.doesNotMatch(owner, /^\\ir\b/gm);
  }
});
