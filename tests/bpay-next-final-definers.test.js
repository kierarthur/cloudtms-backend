import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyFinalDefiners, closeFinalDefinerSelection } from '../scripts/bpay-next-final-definers.mjs';
import { inventory } from '../scripts/cloudtms-db-release-lib.mjs';

// This is the mandatory F-04 static source gate, not a runtime/fixture test.
test('canonical NEW and changed-only final routine authority is closed', () => {
  verifyFinalDefiners();
});
test('explicit changed-only selection cannot omit an unchanged required final closure', () => {
  assert.throws(() => verifyFinalDefiners({ changed: new Set(), selectedMigrations: [],
    selectedRoots: ['supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql'] }),
  /BPAY_NEXT_PENDING_FINAL_REQUIRED/);
});

test('legacy predecessor selection retains the registered final owner when companion list was empty', () => {
  const current = inventory();
  const predecessor = current.repeatables.find(row => row.path === 'supabase/repeatable/13062026_1544_process_authorise_unprocess_unauthorise.sql');
  assert.ok(predecessor);
  const nativeStartPredecessor = current.repeatables.find(row => row.path === 'supabase/repeatable/10102026_1624_candidate_notification_consumers_v1.sql');
  assert.ok(nativeStartPredecessor);
  const closed = closeFinalDefinerSelection({ migrations: [], repeatables: [predecessor, nativeStartPredecessor] }, current);
  const selected = new Set(closed.repeatables.map(row => row.path));
  assert.ok(selected.has('supabase/repeatable/10102026_0942_weekly_source_manual_upsert_busy_passthrough_v1.sql'));
  assert.ok(selected.has('supabase/repeatable/13102026_1418_candidate_runtime_reload_authority_v1.sql'));
});
