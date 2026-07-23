const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..');
const cancellationGuardPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '23072026_1402_disable_plpgsql_check_for_banking_cancel.sql'
);

const correctionGuardPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '23072026_1217_disable_plpgsql_check_for_correction_chain_banking.sql'
);

const cancellationGuard = fs.readFileSync(cancellationGuardPath, 'utf8');
const correctionGuard = fs.readFileSync(correctionGuardPath, 'utf8');

test('draft cancellation and post-cancel refresh entry points disable faulty passive instrumentation', () => {
  for (const signature of [
    /ALTER FUNCTION public\.pay_payment_cancelability_diagnostic\(\s*uuid,\s*jsonb,\s*uuid,\s*text\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_payment_cancel_not_sent_and_recalculate_complete_v1\(\s*uuid,\s*jsonb,\s*uuid,\s*text,\s*text,\s*jsonb\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1\(\s*uuid,\s*uuid,\s*text,\s*uuid,\s*jsonb\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_workbench_session_clear_all_decisions\(\s*uuid,\s*uuid\s*\) SET plpgsql_check\.mode TO 'disabled';/s
  ]) {
    assert.match(cancellationGuard, signature);
  }
});

test('instrumentation guards do not redefine Banking Pay economic or mutation bodies', () => {
  const combined = `${correctionGuard}\n${cancellationGuard}`;
  assert.doesNotMatch(combined, /\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b/i);
  assert.doesNotMatch(combined, /\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b/i);
  assert.doesNotMatch(combined, /\bpay_batch_items\b|\bpay_advance_reservations\b|\bpay_bank_transfers\b/i);
});
