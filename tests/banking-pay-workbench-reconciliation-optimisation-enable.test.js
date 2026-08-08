import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const migration = fs.readFileSync(
  path.join(root, 'supabase/migrations/08082026_0332_enable_banking_pay_reconciliation_optimisation_v1.sql'),
  'utf8'
);

test('TEST enablement is singleton, versioned, and fail-closed', () => {
  assert.match(migration, /reconciliation_optimization_version\s*=\s*1/i);
  assert.match(migration, /WHERE\s+id\s*=\s*1[\s\S]+reconciliation_optimization_version\s*=\s*0/i);
  assert.match(migration, /GET DIAGNOSTICS\s+v_updated\s*=\s*ROW_COUNT/i);
  assert.match(migration, /IF\s+v_updated\s*<>\s*1/i);
  assert.doesNotMatch(migration, /statement_timeout|lease_seconds|parallelism|parallel_bursts/i);
});
