// Explicit disposable-local target. No hosted URL or credential is accepted.
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
module.exports = function localQuery(sql) {
  assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL, 'Opt-in disposable PG17 executable required');
  const result = spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL, [
    '-X', '-q', '-A', '-t', '-h', '127.0.0.1', '-p', '55441', '-U', 'postgres',
    '-d', 'banking_modal_v2_test', '-v', 'ON_ERROR_STOP=1'
  ], { input: `BEGIN READ ONLY; SET LOCAL statement_timeout='10s';
    SELECT jsonb_build_object('database',current_database(),'version',current_setting('server_version_num'));
    ${sql}
    ROLLBACK;`, encoding: 'utf8', timeout: 20000, maxBuffer: 8 * 1024 * 1024 });
  assert.equal(result.status, 0, result.error?.message || result.stderr);
  const lines = result.stdout.trim().split(/\r?\n/).map(line => JSON.parse(line));
  assert.equal(lines[0].database, 'banking_modal_v2_test');
  assert.ok(Number(lines[0].version) >= 170000 && Number(lines[0].version) < 180000);
  return lines.slice(1);
};
