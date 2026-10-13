import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyFinalDefiners } from '../scripts/bpay-next-final-definers.mjs';

// This is the mandatory F-04 static source gate, not a runtime/fixture test.
test('canonical NEW and changed-only final routine authority is closed', () => {
  verifyFinalDefiners();
});
test('explicit changed-only selection cannot omit an unchanged required final closure', () => {
  assert.throws(() => verifyFinalDefiners({ changed: new Set(), selectedMigrations: [],
    selectedRoots: ['supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql'] }),
  /BPAY_NEXT_PENDING_FINAL_REQUIRED/);
});
