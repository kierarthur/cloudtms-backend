import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const source = fs.readFileSync(path.join(root,
  'supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql'), 'utf8');

test('writer census permits only the exact provider-mapped finalizer on agency TEST', () => {
  assert.match(source, /inventory_row\.definition_sha256 = '49fbfaa7e3fbcefe7fde5e6937f2ac2b772fc62528460ae22246f98f7ad01d08'/);
  assert.match(source, /observed_sha = '0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4'/);
  assert.match(source, /inventory_row\.routine_name = 'pay_batch_finalize_reservations_and_markers'/);
  assert.match(source, /pg_catalog\.current_database\(\) = 'cloudtms_test_clone'/);
  assert.match(source, /identity_row\.singleton and identity_row\.environment = 'TEST'/);
  assert.match(source, /mapped_proc\.proconfig = array\['search_path=public'\]::text\[\]/);
  assert.match(source, /if observed_sha <> inventory_row\.definition_sha256\s+and not \(/);
});
