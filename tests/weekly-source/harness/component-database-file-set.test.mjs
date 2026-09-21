import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  HANDOVER2_OWNED_REPEATABLE_EXCLUSIONS,
  WEEKLY_SOURCE_COMPONENT_MIGRATIONS,
  WEEKLY_SOURCE_COMPONENT_REPEATABLES,
  orderedWeeklySourceComponentFiles,
  verifyWeeklySourceComponentFileSet,
} from './component-database-file-set.mjs';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

test('component database file set is exact, present and excludes HANDOVER 2 owners', async () => {
  const result = await verifyWeeklySourceComponentFileSet(repoRoot);
  assert.equal(result.migrationCount, 9);
  assert.equal(result.repeatableCount, 77);
  assert(result.files.some((file) => file.relative === 'supabase/repeatable/02022026_payroll_ID_new_tables_and_triggers.sql'));
  assert.equal(result.files.length, 86);
  assert.equal(new Set(result.files.map((file) => file.relative)).size, 86);
  assert(result.files.every((file) => /^[a-f0-9]{64}$/.test(file.sha256)));
  for (const excluded of HANDOVER2_OWNED_REPEATABLE_EXCLUSIONS) {
    assert(!result.files.some((file) => file.relative === excluded), `${excluded} is separately owned`);
  }
  assert(result.files.some((file) => file.relative.endsWith('21072026_1235_30_timesheet_unauthorise_atomic.sql')));
  assert(result.files.some((file) => file.relative.endsWith('21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql')));
  assert(result.files.some((file) => file.relative.endsWith('21092026_1817_weekly_source_audit_event_order.sql')));
  assert(result.files.some((file) => file.relative.endsWith('21092026_2012_invoice_discounting_ledger_revision.sql')));
});

test('component SQL applies all migrations before canonically ordered repeatables', () => {
  const ordered = orderedWeeklySourceComponentFiles();
  assert.deepEqual(ordered.slice(0, WEEKLY_SOURCE_COMPONENT_MIGRATIONS.length).sort(), [...WEEKLY_SOURCE_COMPONENT_MIGRATIONS].sort());
  assert.deepEqual(ordered.slice(WEEKLY_SOURCE_COMPONENT_MIGRATIONS.length).sort(), [...WEEKLY_SOURCE_COMPONENT_REPEATABLES].sort());
});
