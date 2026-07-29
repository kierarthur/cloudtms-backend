const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workflow = fs.readFileSync(
  path.resolve(__dirname, '../.github/workflows/supabase-migrate.yml'),
  'utf8'
);

test('Supabase deployment applies only new or content-changed repeatables', () => {
  assert.match(workflow, /create table if not exists public\.schema_repeatables/);
  assert.match(workflow, /content_sha256 text not null/);
  assert.match(
    workflow,
    /tablename not in \('schema_migrations', 'schema_repeatables'\)/,
    'ledger tables must not make a genuinely empty database look pre-populated'
  );
  assert.match(workflow, /sha256sum "\$1"/);
  assert.match(workflow, /declare -A REPEATABLE_LEDGER_HASHES/);
  assert.match(workflow, /select filename, content_sha256/);
  assert.doesNotMatch(
    workflow,
    /repeatable_ledger_hash \(\)/,
    'unchanged files must not cause one database query per repeatable'
  );
  assert.match(workflow, /SKIP unchanged repeatable/);
  assert.match(workflow, /APPLY new\/changed repeatable/);
  assert.match(
    workflow,
    /apply_file "\$f"\s+record_repeatable_hash "\$base" "\$content_hash"/,
    'a repeatable hash must be recorded only after the file applies successfully'
  );
  assert.doesNotMatch(
    workflow,
    /for f in "\$\{REP_FILES_SORTED\[@\]\}"; do\s+echo "APPLY repeatable:[\s\S]*apply_file "\$f"\s+done/,
    'unchanged repeatables must not be replayed wholesale'
  );
});

test('first hash-ledger run is fail-closed and bootstraps only unchanged source', () => {
  assert.match(workflow, /fetch-depth: 0/);
  assert.match(workflow, /BEFORE_SHA: \$\{\{ github\.event\.before \|\| '' \}\}/);
  assert.match(workflow, /BOOTSTRAP_EXISTING_REPEATABLES=false/);
  assert.match(
    workflow,
    /REPEATABLE_BOOTSTRAP_COMPLETE="\$\{REPEATABLE_LEDGER_HASHES\[__BOOTSTRAPPED__\]:-\}"/
  );
  assert.match(workflow, /values \('__BOOTSTRAPPED__', 'v1', now\(\)\)/);
  assert.match(workflow, /GITHUB_EVENT_NAME[^]*!= "push"/);
  assert.match(workflow, /Cannot safely bootstrap repeatable hashes/);
  assert.match(workflow, /git diff --quiet "\$\{BEFORE_SHA\}" "\$\{GITHUB_SHA\}"/);
  assert.match(workflow, /BOOTSTRAP_SQL="\$\(mktemp\)"/);
  assert.match(workflow, /BASELINE complete for unchanged repeatables/);
});

test('unchanged migrations are also checked from one loaded ledger', () => {
  assert.match(workflow, /declare -A MIGRATION_LEDGER_NAMES/);
  assert.match(workflow, /select filename\s+from public\.schema_migrations/);
  assert.match(workflow, /already="\$\{MIGRATION_LEDGER_NAMES\[\$base\]:-\}"/);
  assert.doesNotMatch(
    workflow,
    /select 1 from public\.schema_migrations where filename/,
    'the workflow must not query PostgreSQL once for every unchanged migration'
  );
  assert.match(workflow, /MIGRATION_BOOTSTRAP_SQL="\$\(mktemp\)"/);
});
