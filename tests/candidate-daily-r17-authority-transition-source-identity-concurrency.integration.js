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
  const lines = String(output).split(/\r?\n/).filter(value => value.trim().startsWith('{'));
  assert.ok(lines.length, `expected JSON output, received: ${output}`);
  return JSON.parse(lines.at(-1));
}

const system = {
  policy: 'SIGNED_SYSTEM_SYNC', environment: 'TEST', system_auth_verified: true,
  nonce_consumed: true, environment_trusted: true, stable_operation_identity: true,
  approved_source_mapping: true, source_scope_ready: true, authority_mode_compatible: true,
  transition_ready: true
};
const actor = '00000000-0000-4000-8000-000000017501';
const approver = '00000000-0000-4000-8000-000000017502';

function globalKey(candidateId) {
  return `CID1-${candidateId.replaceAll('-', '').slice(0, 24).toUpperCase()}`;
}

function insertCandidate(candidateId) {
  psql(`
    insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values('${candidateId}','r17-${candidateId}@example.invalid','R17 Concurrent Candidate',
      'R17','Concurrent',true,'${globalKey(candidateId)}');
    insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode,canonical_version)
    values('TEST','${candidateId}','GOOGLE_PRIMARY',0);
    insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST','${candidateId}',false,'R17 concurrency fixture','${'e'.repeat(64)}');
  `);
}

function transitionItem(candidateId, sourceHmac) {
  return {
    candidate_id: candidateId,
    expected_authority_mode: 'GOOGLE_PRIMARY', expected_canonical_version: 0,
    expected_entitlement_enabled: false, new_authority_mode: 'GOOGLE_PRIMARY',
    entitlement_enabled: false, in_flight_disposition: 'DRAINED',
    source_link: { identifier_hmac: sourceHmac, hmac_key_version: 1, state: 'PRIMARY' }
  };
}

function transitionSql(items, { delayTrigger = false, sleepAfter = false } = {}) {
  const context = { ...system, actor_user_id: actor };
  return `begin;
    set local lock_timeout='8s';
    set local statement_timeout='12s';
    ${delayTrigger ? "select set_config('r17.delay_source_link','on',true);" : ''}
    select public.candidate_daily_authority_transition_atomic_v1(
      '${JSON.stringify(context)}'::jsonb,'${randomUUID()}'::uuid,
      'candidate-r17-transition-${randomUUID()}','${JSON.stringify(items)}'::jsonb,
      '${approver}'::uuid,'R17 deterministic source lock proof','${'f'.repeat(64)}',
      '01K2ABCDEFGHJKMNPQRSTVWXYZ')::text;
    ${sleepAfter ? 'select pg_sleep(1);' : ''}
    commit;`;
}

function generationItem(candidateId, sourceHmac, suffix) {
  return {
    candidate_global_key: globalKey(candidateId),
    candidate_source_hmac: sourceHmac,
    source_hmac_key_version: 1,
    source_event_id: `r17-generation-${suffix}`,
    source_revision: `phase3.${createHash('sha256').update(`revision-${suffix}`).digest('hex')}`,
    source_hash: createHash('sha256').update(`source-${suffix}`).digest('hex'),
    window_start: '2026-08-17',
    days: Array.from({ length: 14 }, (_, index) => ({
      date: new Date(Date.UTC(2026, 7, 17 + index)).toISOString().slice(0, 10),
      booked: false, system_blocked: false,
      source_row_hash: createHash('sha256').update(`${suffix}-day-${index}`).digest('hex')
    })),
    source_event_time: '2026-08-18T00:00:00.000Z',
    item_key: `r17.concurrent.${suffix}`
  };
}

function generationSql(item) {
  return `begin;
    set local lock_timeout='8s';
    set local statement_timeout='12s';
    select public.candidate_daily_rota_generation_publish_atomic_v1(
      '${JSON.stringify(system)}'::jsonb,'${randomUUID()}'::uuid,
      'candidate-r17-generation-${randomUUID()}','${JSON.stringify([item])}'::jsonb,
      '01K2ABCDEFGHJKMNPQRSTVWXYZ')::text;
    commit;`;
}

test('actual first generation and actual authority transition share SOURCE-before-scope order without 40P01', {
  skip: !enabled
}, async () => {
  const candidateId = randomUUID();
  const sourceHmac = createHash('sha256').update(`r17-generation-transition:${candidateId}`).digest('hex');
  insertCandidate(candidateId);

  // This test-only trigger widens the exact old inversion window. Under the R17
  // order the transition already owns SOURCE and scope before the delay; the
  // generation waits at SOURCE instead of forming a wait cycle.
  psql(`
    create or replace function private._r17_delay_source_link_test_v1()
    returns trigger language plpgsql set search_path='' as $function$
    begin
      if pg_catalog.current_setting('r17.delay_source_link',true)='on' then
        perform pg_catalog.pg_sleep(1);
      end if;
      return new;
    end
    $function$;
    drop trigger if exists aaa_r17_delay_source_link_test on private.candidate_daily_source_links;
    create trigger aaa_r17_delay_source_link_test
    before insert on private.candidate_daily_source_links
    for each row execute function private._r17_delay_source_link_test_v1();
  `);

  try {
    const transition = psqlAsync(transitionSql([transitionItem(candidateId, sourceHmac)], { delayTrigger: true }));
    await new Promise(resolve => setTimeout(resolve, 200));
    const generation = psqlAsync(generationSql(generationItem(candidateId, sourceHmac,
      `shared-lock-${candidateId.replaceAll('-', '')}`)));
    const [transitionResult, generationResult] = (await Promise.all([transition, generation])).map(json);

    assert.equal(transitionResult.outcomes[0].status, 'COMMITTED');
    assert.ok(['COMMITTED', 'REPLAYED'].includes(generationResult.outcomes[0].status),
      `generation did not converge after the shared lock: ${JSON.stringify(generationResult)}`);
    assert.equal(psql(`select count(*) from private.candidate_daily_source_links
      where environment='TEST' and candidate_id='${candidateId}' and identifier_hmac='${sourceHmac}'`), '1');
    assert.equal(psql(`select count(*) from public.candidate_daily_rota_generations
      where environment='TEST' and candidate_id='${candidateId}'`), '1');
  } finally {
    psql(`drop trigger if exists aaa_r17_delay_source_link_test on private.candidate_daily_source_links;
      drop function if exists private._r17_delay_source_link_test_v1();`);
  }
});

test('two transitions with opposite item order acquire two SOURCE identities deterministically', {
  skip: !enabled
}, async () => {
  const firstCandidate = randomUUID();
  const secondCandidate = randomUUID();
  const firstHmac = createHash('sha256').update(`r17-opposite-a:${firstCandidate}`).digest('hex');
  const secondHmac = createHash('sha256').update(`r17-opposite-b:${secondCandidate}`).digest('hex');
  insertCandidate(firstCandidate);
  insertCandidate(secondCandidate);

  const firstItems = [
    transitionItem(firstCandidate, firstHmac),
    transitionItem(secondCandidate, secondHmac)
  ];
  const secondItems = [...firstItems].reverse();
  const first = psqlAsync(transitionSql(firstItems, { sleepAfter: true }));
  await new Promise(resolve => setTimeout(resolve, 150));
  const second = psqlAsync(transitionSql(secondItems));
  const [firstResult, secondResult] = (await Promise.all([first, second])).map(json);

  assert.deepEqual(firstResult.outcomes.map(value => value.status), ['COMMITTED', 'COMMITTED']);
  assert.deepEqual(secondResult.outcomes.map(value => value.status), ['REJECTED', 'REJECTED']);
  assert.deepEqual(secondResult.outcomes.map(value => value.error_code),
    ['IDENTITY_LINK_CONFLICT', 'IDENTITY_LINK_CONFLICT']);
  assert.equal(psql(`select count(*) from private.candidate_daily_source_links
    where environment='TEST' and candidate_id in ('${firstCandidate}','${secondCandidate}')`), '2');
});
