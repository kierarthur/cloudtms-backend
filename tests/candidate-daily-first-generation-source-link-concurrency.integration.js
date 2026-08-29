import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import test from 'node:test';

const enabled = process.env.CANDIDATE_DAILY_POSTGRES_CHAIN === '1';
const container = String(process.env.CANDIDATE_DAILY_PG_CONTAINER || '').trim();
const port = String(process.env.CANDIDATE_DAILY_PG_PORT || process.env.PGPORT || '5432');

function psqlCommand() {
  if (container) {
    return {
      command: 'docker',
      args: ['exec', '-i', container, 'psql', '-X', '-U', 'postgres', '-d', 'postgres',
        '-tA', '-v', 'ON_ERROR_STOP=1']
    };
  }
  return {
    command: 'psql',
    args: ['-X', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-d', 'postgres',
      '-tA', '-v', 'ON_ERROR_STOP=1']
  };
}

function psql(sql) {
  const { command, args } = psqlCommand();
  const result = spawnSync(command, args, { encoding: 'utf8', env: process.env, input: sql });
  if (result.status !== 0) throw new Error(`${result.stderr || ''}\n${result.stdout || ''}`.trim());
  return String(result.stdout || '').trim();
}

function psqlAsync(sql) {
  const { command, args } = psqlCommand();
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env: process.env, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => {
      if (code !== 0) reject(new Error(`${stderr}\n${stdout}`.trim()));
      else resolve(stdout.trim());
    });
    child.stdin.end(sql);
  });
}

function jsonResult(output) {
  const line = String(output).split(/\r?\n/).find(value => value.trim().startsWith('{'));
  assert.ok(line, `expected JSON result, received: ${output}`);
  return JSON.parse(line);
}

function callSql({ batchId, idempotencyKey, item, sleep = false }) {
  const context = {
    policy: 'SIGNED_SYSTEM_SYNC', environment: 'TEST', system_auth_verified: true,
    nonce_consumed: true, environment_trusted: true, stable_operation_identity: true,
    approved_source_mapping: true, source_scope_ready: true, authority_mode_compatible: true,
    transition_ready: true
  };
  const call = `public.candidate_daily_rota_generation_publish_atomic_v1(
    '${JSON.stringify(context)}'::jsonb,
    '${batchId}'::uuid,
    '${idempotencyKey}',
    '${JSON.stringify([item])}'::jsonb,
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  )`;
  return `begin; select ${call}::text;${sleep ? ' select pg_sleep(1);' : ''} commit;`;
}

function generationItem(globalKey, sourceHmac) {
  const days = Array.from({ length: 14 }, (_, index) => ({
    date: new Date(Date.UTC(2026, 7, 17 + index)).toISOString().slice(0, 10),
    booked: false,
    system_blocked: false,
    source_row_hash: createHash('sha256').update(`r15-day-${index}`).digest('hex')
  }));
  return {
    candidate_global_key: globalKey,
    candidate_source_hmac: sourceHmac,
    source_hmac_key_version: 1,
    source_event_id: `r15-concurrent-${sourceHmac.slice(0, 24)}`,
    source_revision: `phase3.${createHash('sha256').update('r15-concurrent-revision').digest('hex')}`,
    source_hash: createHash('sha256').update('r15-concurrent-source').digest('hex'),
    window_start: '2026-08-17',
    days,
    source_event_time: '2026-08-18T00:00:00.000Z',
    item_key: `r15.concurrent.${sourceHmac.slice(0, 24)}`
  };
}

test('parallel first generation converges to one Candidate link, scope and generation', {
  skip: !enabled
}, async () => {
  const candidateId = randomUUID();
  const globalKey = `CID1-${candidateId.replaceAll('-', '').slice(0, 24).toUpperCase()}`;
  const sourceHmac = createHash('sha256').update(`r15-source:${candidateId}`).digest('hex');
  const item = generationItem(globalKey, sourceHmac);

  psql(`insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values('${candidateId}','r15-${candidateId}@example.invalid','R15 Concurrent Candidate',
      'R15','Concurrent',true,'${globalKey}')`);

  const first = psqlAsync(callSql({
    batchId: randomUUID(), idempotencyKey: `r15-concurrent-first-${randomUUID()}`,
    item, sleep: true
  }));
  await new Promise(resolve => setTimeout(resolve, 150));
  const second = psqlAsync(callSql({
    batchId: randomUUID(), idempotencyKey: `r15-concurrent-second-${randomUUID()}`,
    item
  }));
  const [firstResult, secondResult] = (await Promise.all([first, second])).map(jsonResult);

  assert.equal(firstResult.outcomes[0].status, 'COMMITTED');
  assert.equal(secondResult.outcomes[0].status, 'REPLAYED');
  assert.equal(secondResult.outcomes[0].generation_id, firstResult.outcomes[0].generation_id);
  assert.equal(psql(`select count(*) from private.candidate_daily_source_links
    where environment='TEST' and identifier_hmac='${sourceHmac}'`), '1');
  assert.equal(psql(`select count(*) from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id='${candidateId}' and authority_mode='GOOGLE_PRIMARY'`), '1');
  assert.equal(psql(`select count(*) from public.candidate_daily_rota_generations
    where environment='TEST' and candidate_id='${candidateId}'`), '1');
  assert.equal(psql(`select count(*) from public.candidates where id='${candidateId}'`), '1');
});
