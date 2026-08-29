import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import test from 'node:test';

const enabled = process.env.CANDIDATE_DAILY_POSTGRES_CHAIN === '1';
const container = String(process.env.CANDIDATE_DAILY_PG_CONTAINER || '').trim();
const port = String(process.env.CANDIDATE_DAILY_PG_PORT || process.env.PGPORT || '5432');

function command() {
  if (container) return { bin: 'docker', args: ['exec', '-i', container, 'psql', '-X', '-U', 'postgres', '-d', 'postgres', '-tA', '-v', 'ON_ERROR_STOP=1'] };
  return { bin: 'psql', args: ['-X', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-d', 'postgres', '-tA', '-v', 'ON_ERROR_STOP=1'] };
}

function psql(sql) {
  const { bin, args } = command();
  const result = spawnSync(bin, args, { input: sql, encoding: 'utf8', env: process.env });
  if (result.status !== 0) throw new Error(`${result.stderr}\n${result.stdout}`.trim());
  return String(result.stdout || '').trim();
}

function psqlAsync(sql) {
  const { bin, args } = command();
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { env: process.env, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', value => { stdout += value; });
    child.stderr.on('data', value => { stderr += value; });
    child.on('error', reject);
    child.on('close', code => code === 0 ? resolve(stdout.trim()) : reject(new Error(`${stderr}\n${stdout}`.trim())));
    child.stdin.end(sql);
  });
}

function json(output) {
  const line = String(output).split(/\r?\n/).find(value => value.trim().startsWith('{'));
  assert.ok(line, `expected JSON output, received: ${output}`);
  return JSON.parse(line);
}

const system = {
  policy: 'SIGNED_SYSTEM_SYNC', environment: 'TEST', system_auth_verified: true,
  nonce_consumed: true, environment_trusted: true, stable_operation_identity: true,
  approved_source_mapping: true, source_scope_ready: true, authority_mode_compatible: true,
  transition_ready: true
};

function keyFromUuid(value) {
  return `CID1-${value.replaceAll('-', '').slice(0, 24).toUpperCase()}`;
}

function item(globalKey, sourceHmac, suffix) {
  return {
    candidate_global_key: globalKey,
    candidate_source_hmac: sourceHmac,
    source_hmac_key_version: 1,
    source_event_id: `r16-concurrency-${suffix}`,
    source_revision: `phase3.${createHash('sha256').update(`revision-${suffix}`).digest('hex')}`,
    source_hash: createHash('sha256').update(`source-${suffix}`).digest('hex'),
    window_start: '2026-08-17',
    days: Array.from({ length: 14 }, (_, index) => ({
      date: new Date(Date.UTC(2026, 7, 17 + index)).toISOString().slice(0, 10),
      booked: false,
      system_blocked: false,
      source_row_hash: createHash('sha256').update(`${suffix}-day-${index}`).digest('hex')
    })),
    source_event_time: '2026-08-18T00:00:00.000Z',
    item_key: `r16.concurrent.${suffix}`
  };
}

function generationSql(generationItem, sleep = false) {
  return `begin;
    select public.candidate_daily_rota_generation_publish_atomic_v1(
      '${JSON.stringify(system)}'::jsonb,'${randomUUID()}'::uuid,
      'candidate-r16-${randomUUID()}','${JSON.stringify([generationItem])}'::jsonb,
      '01K2ABCDEFGHJKMNPQRSTVWXYZ')::text;
    ${sleep ? 'select pg_sleep(1);' : ''}
    commit;`;
}

function insertCandidate(candidateId, globalKey, active = true) {
  psql(`insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values('${candidateId}','r16-${candidateId}@example.invalid','R16 Concurrent Candidate',
      'R16','Concurrent',${active},'${globalKey}')`);
}

test('first generation cannot race a normalized-equivalent active Candidate insert or activation', {
  skip: !enabled
}, async () => {
  const owner = randomUUID();
  const inactive = randomUUID();
  const insertRace = randomUUID();
  const globalKey = keyFromUuid(owner);
  const sourceHmac = createHash('sha256').update(`r16-normalized:${owner}`).digest('hex');
  insertCandidate(owner, `  ${globalKey.toLowerCase()}  `);
  insertCandidate(inactive, globalKey, false);

  const generation = psqlAsync(generationSql(item(globalKey, sourceHmac, 'normalized-race'), true));
  await new Promise(resolve => setTimeout(resolve, 150));
  const insertion = psqlAsync(`insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values('${insertRace}','r16-${insertRace}@example.invalid','R16 Insert Race','R16','Insert Race',true,'${globalKey}');`);
  const activation = psqlAsync(`update public.candidates set active=true where id='${inactive}';`);
  const [generationOutput, insertionOutcome, activationOutcome] = await Promise.all([
    generation,
    insertion.then(() => 'committed', error => error.message),
    activation.then(() => 'committed', error => error.message)
  ]);

  assert.equal(json(generationOutput).outcomes[0].status, 'COMMITTED');
  assert.match(insertionOutcome, /duplicate key value|unique constraint/i);
  assert.match(activationOutcome, /duplicate key value|unique constraint/i);
  assert.equal(psql(`select count(*) from public.candidates where active and
    upper(btrim(key_norm))='${globalKey}'`), '1');
});

test('two Candidate CID1 owners racing for one source HMAC converge to one historical owner', {
  skip: !enabled
}, async () => {
  const firstCandidate = randomUUID();
  const secondCandidate = randomUUID();
  const firstKey = keyFromUuid(firstCandidate);
  const secondKey = keyFromUuid(secondCandidate);
  const sourceHmac = createHash('sha256').update(`r16-shared:${firstCandidate}`).digest('hex');
  insertCandidate(firstCandidate, firstKey);
  insertCandidate(secondCandidate, secondKey);

  const first = psqlAsync(generationSql(item(firstKey, sourceHmac, 'source-race-a'), true));
  await new Promise(resolve => setTimeout(resolve, 150));
  const second = psqlAsync(generationSql(item(secondKey, sourceHmac, 'source-race-b')));
  const [firstResult, secondResult] = (await Promise.all([first, second])).map(value => json(value));

  assert.equal(firstResult.outcomes[0].status, 'COMMITTED');
  assert.equal(secondResult.outcomes[0].status, 'REJECTED');
  assert.equal(secondResult.outcomes[0].error_code, 'IDENTITY_LINK_CONFLICT');
  assert.equal(psql(`select count(*) from private.candidate_daily_source_links
    where environment='TEST' and identifier_hmac='${sourceHmac}'`), '1');
  assert.equal(psql(`select count(*) from public.candidate_daily_rota_generations
    where environment='TEST' and candidate_id='${secondCandidate}'`), '0');
});

test('first generation and Office source-link transition cannot acquire the same HMAC', {
  skip: !enabled
}, async () => {
  const generationCandidate = randomUUID();
  const officeCandidate = randomUUID();
  const generationKey = keyFromUuid(generationCandidate);
  const officeKey = keyFromUuid(officeCandidate);
  const sourceHmac = createHash('sha256').update(`r16-office:${generationCandidate}`).digest('hex');
  insertCandidate(generationCandidate, generationKey);
  insertCandidate(officeCandidate, officeKey);
  psql(`insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
    values('TEST','${officeCandidate}','GOOGLE_PRIMARY');
    insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST','${officeCandidate}',false,'R16 Office race fixture','${'e'.repeat(64)}');`);

  const transitionContext = { ...system, actor_user_id: '00000000-0000-4000-8000-00000000f501' };
  const transitionItem = {
    candidate_id: officeCandidate,
    expected_authority_mode: 'GOOGLE_PRIMARY', expected_canonical_version: 0,
    expected_entitlement_enabled: false, new_authority_mode: 'GOOGLE_PRIMARY',
    entitlement_enabled: false, in_flight_disposition: 'DRAINED',
    source_link: { identifier_hmac: sourceHmac, hmac_key_version: 1, state: 'PRIMARY' }
  };
  const transitionSql = `select public.candidate_daily_authority_transition_atomic_v1(
    '${JSON.stringify(transitionContext)}'::jsonb,'${randomUUID()}'::uuid,
    'candidate-r16-office-${randomUUID()}','${JSON.stringify([transitionItem])}'::jsonb,
    '00000000-0000-4000-8000-00000000f502'::uuid,'R16 Office race proof',
    '${'f'.repeat(64)}','01K2ABCDEFGHJKMNPQRSTVWXYZ')::text;`;

  const generation = psqlAsync(generationSql(item(generationKey, sourceHmac, 'office-race'), true));
  await new Promise(resolve => setTimeout(resolve, 150));
  const [generationOutput, transitionOutput] = await Promise.all([
    generation, psqlAsync(transitionSql)
  ]);
  const transitionResult = json(transitionOutput);

  assert.equal(json(generationOutput).outcomes[0].status, 'COMMITTED');
  assert.equal(transitionResult.outcomes[0].status, 'REJECTED');
  assert.equal(transitionResult.outcomes[0].error_code, 'IDENTITY_LINK_CONFLICT');
  assert.equal(psql(`select count(*) from private.candidate_daily_source_links
    where environment='TEST' and identifier_hmac='${sourceHmac}'`), '1');
  assert.equal(psql(`select count(*) from private.candidate_daily_authority_transitions
    where environment='TEST' and candidate_id='${officeCandidate}'`), '0');
  assert.equal(psql(`select count(*) from public.candidate_daily_rota_generations
    where environment='TEST' and candidate_id='${officeCandidate}'`), '0');
});
