import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(path, 'utf8');

test('permanent Miget operations connector exposes all nine read-only tools to three fixed databases', () => {
  const source = read('infra/miget/cloudtms-miget-operations/src/index.ts');
  const config = read('infra/miget/cloudtms-miget-operations/wrangler.jsonc');
  const tools = [...source.matchAll(/server\.registerTool\(\s*\r?\n?\s*"([^"]+)"/g)].map((match) => match[1]);

  assert.deepEqual(tools.sort(), [
    'miget_db_catalog_summary',
    'miget_db_get_rpc_definition',
    'miget_db_list_rpcs',
    'miget_db_performance_summary',
    'miget_db_release_ledger',
    'miget_db_security_audit',
    'miget_inspect_postgres',
    'miget_list_infrastructure',
    'miget_verify_codex_parity_route',
  ].sort());
  assert.match(source, /z\.enum\(\["agency_test", "mytms_test", "agency_live"\]\)/);
  assert.match(source, /database === "agency_live"[\s\S]*env\.LIVE_HYPERDRIVE/);
  assert.match(source, /database === "agency_live"[\s\S]*env\.LIVE_MIGET_POSTGRES_SERVICE_ID/);
  assert.match(source, /begin read only/);
  assert.doesNotMatch(source, /\bsql:\s*z\./i);
  assert.match(config, /"binding": "LIVE_HYPERDRIVE"/);
  assert.match(config, /"id": "19fc41d1b2e34b9b8a97b4961f6e6fb5"/);
});
