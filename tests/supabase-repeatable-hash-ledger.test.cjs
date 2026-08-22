const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const engine = fs.readFileSync(path.join(root, 'scripts', 'cloudtms-db-release.mjs'), 'utf8');
const library = fs.readFileSync(path.join(root, 'scripts', 'cloudtms-db-release-lib.mjs'), 'utf8');
const pushWorkflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'supabase-migrate.yml'), 'utf8');

test('repeatable deployment is closure-hashed and applies only changed authority', () => {
  assert.match(library, /export function closureFor/);
  assert.match(library, /Recursive SQL include cycle/);
  assert.match(library, /Buffer\.concat\(ordered\.flatMap/);
  assert.match(engine, /private\.cloudtms_repeatable_ledger/);
  assert.match(engine, /const pendingRepeatables = current\.repeatables\.filter/);
  assert.match(engine, /for \(const item of pendingRepeatables\)/);
  assert.doesNotMatch(engine, /for \(const item of current\.repeatables\)[\s\S]*psql\(\{ file: item\.path \}\)/);
});

test('migration authority is immutable and adoption cannot blind-bootstrap drift', () => {
  assert.match(engine, /migration-lock\.json/);
  assert.match(engine, /Installed migration hash mismatch/);
  assert.match(engine, /Installed migration is absent from repository/);
  assert.match(engine, /mode === 'ADOPT'[\s\S]*compareExpected\(release\.contractPath\)/);
  assert.doesNotMatch(engine, /git diff --quiet/);
  assert.doesNotMatch(engine, /BOOTSTRAP_EXISTING_REPEATABLES/);
});

test('ordinary pushes verify source but cannot mutate a database', () => {
  assert.match(pushWorkflow, /npm run db:check/);
  assert.doesNotMatch(pushWorkflow, /CLOUDTMS_DATABASE_URL|DB_URL|psql|supabase db push/);
});
