import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const workflow = readFileSync(new URL('../.github/workflows/database-contract-export.yml', import.meta.url), 'utf8');

test('database contract export is TEST-only, read-only, and data-free', () => {
  assert.match(workflow, /name: Database contract export \(TEST read-only\)/);
  assert.match(workflow, /environment: database-test/);
  assert.match(workflow, /CLOUDTMS_ENVIRONMENT: TEST/);
  assert.match(workflow, /npm run db:plan -- --environment=TEST --mode=UPGRADE/);
  assert.match(workflow, /npm run db:contract:export/);
  assert.match(workflow, /supabase\/release\/current-contract\.json/);
  assert.doesNotMatch(workflow, /\bLIVE\b/);
  assert.doesNotMatch(workflow, /db:apply|phase=APPLY|CLOUDTMS_RELEASE_APPROVAL/);
});
