import test from 'node:test';
import { verifyFinalDefiners } from '../scripts/bpay-next-final-definers.mjs';

// This is the mandatory F-04 static source gate, not a runtime/fixture test.
test('canonical NEW and changed-only final routine authority is closed', () => {
  verifyFinalDefiners();
});
