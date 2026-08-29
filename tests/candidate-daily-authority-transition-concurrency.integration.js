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
  if (result.status !== 0) {
    throw new Error(`${result.stderr || ''}\n${result.stdout || ''}`.trim());
  }
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

function transitionSql({ batchId, key, item, sleep = false }) {
  const context = {
    policy: 'SIGNED_SYSTEM_SYNC', environment: 'TEST', system_auth_verified: true,
    nonce_consumed: true, environment_trusted: true, stable_operation_identity: true,
    approved_source_mapping: true, source_scope_ready: true, authority_mode_compatible: true,
    transition_ready: true, actor_user_id: '00000000-0000-4000-8000-00000000e601'
  };
  const call = `public.candidate_daily_authority_transition_atomic_v1(
    '${JSON.stringify(context)}'::jsonb,
    '${batchId}'::uuid,
    '${key}',
    '${JSON.stringify([item])}'::jsonb,
    '00000000-0000-4000-8000-00000000e602'::uuid,
    'R9 parallel authority proof',
    '${'a'.repeat(64)}',
    '01K2ABCDEF0123456789ABCDE1'
  )`;
  return `begin; select ${call}::text;${sleep ? ' select pg_sleep(1);' : ''} commit;`;
}

function fixtureSql(candidateId, generationId, generationBatchId) {
  const compactCandidate = candidateId.replaceAll('-', '');
  const sourceHmac = createHash('sha256').update(`candidate-daily-r9:${candidateId}`).digest('hex');
  return `
    insert into public.candidates(id,email,display_name,first_name,last_name,active)
    values('${candidateId}','${compactCandidate}@candidate-daily-r9.invalid',
      'Candidate Daily R9 Concurrent','Candidate','Daily R9 Concurrent',true);
    insert into private.candidate_daily_authority_scopes(
      environment,candidate_id,authority_mode,canonical_version)
    values('TEST','${candidateId}','GOOGLE_PRIMARY',0);
    insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST','${candidateId}',false,'R9 concurrency fixture','${'9'.repeat(64)}');
    insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
      canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,
      valid_from_utc,evidence_sha256)
    values('TEST','${candidateId}','GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      gen_random_uuid(),'${sourceHmac}',1,'PRIMARY',now()-interval '1 day','${'8'.repeat(64)}');
    insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,
      operation_class,idempotency_key,request_hash,item_keys_json,item_count,state,terminal_http_status,
      terminal_response_body,terminal_response_sha256,correlation_id,completed_at_utc)
    values('${generationBatchId}','TEST','SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH',
      'r9-generation-${compactCandidate}','${'7'.repeat(64)}',jsonb_build_array('${candidateId}'),
      1,'COMPLETED',200,'{}'::jsonb,'${'6'.repeat(64)}','01K2ABCDEF0123456789ABCDE1',now());
    insert into public.candidate_daily_rota_generations(generation_id,environment,candidate_id,
      generation_version,window_start,window_end,state,expected_day_count,actual_day_count,source_system,
      source_event_id,source_revision,source_event_time,item_key,source_hash,generation_row_hash,
      batch_receipt_id,correlation_id,activated_at_utc,published_at_utc)
    values('${generationId}','TEST','${candidateId}',1,date '2026-08-17',date '2026-08-30','ACTIVE',14,14,
      'MASTER_ROTA','r9-concurrent-source-${compactCandidate}','r9-concurrent-revision',now(),
      'r9-concurrent-item-${compactCandidate}','${'5'.repeat(64)}','${'4'.repeat(64)}','${generationBatchId}',
      '01K2ABCDEF0123456789ABCDE1',now(),now());
    insert into public.candidate_daily_rota_days(
      generation_id,environment,candidate_id,rota_date,booked,system_blocked,source_row_hash)
    select '${generationId}','TEST','${candidateId}',date '2026-08-17'+day,false,false,
      repeat(substr(md5('${candidateId}'||day::text),1,1),64)
    from generate_series(0,13) day;
    update private.candidate_daily_authority_scopes
    set active_generation_id='${generationId}'
    where environment='TEST' and candidate_id='${candidateId}';
    insert into private.candidate_daily_sync_state(environment,candidate_id,target,
      accepted_canonical_cursor,required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,
      effective_visible_cursor,observed_source_revision,state,last_acknowledged_at_utc,last_pulled_at_utc,
      last_reconciled_at_utc)
    values('TEST','${candidateId}','MASTER_AVAILABILITY_SHEET',0,0,0,0,0,
      'sheet-revision-ready','READY',clock_timestamp(),clock_timestamp(),clock_timestamp());
  `;
}

test('parallel exact replay and parallel different-key transition remain single-winner', {
  skip: !enabled
}, async () => {
  const candidateId = randomUUID();
  const generationId = randomUUID();
  const generationBatchId = randomUUID();
  psql(fixtureSql(candidateId, generationId, generationBatchId));

  const noChangeItem = {
    candidate_id: candidateId,
    expected_authority_mode: 'GOOGLE_PRIMARY',
    expected_canonical_version: 0,
    expected_entitlement_enabled: false,
    new_authority_mode: 'GOOGLE_PRIMARY',
    entitlement_enabled: false,
    in_flight_disposition: 'NONE'
  };
  const replayBatch = randomUUID();
  const replayKey = `r9-parallel-exact-${randomUUID()}`;
  const firstReplay = psqlAsync(transitionSql({
    batchId: replayBatch, key: replayKey, item: noChangeItem, sleep: true
  }));
  await new Promise(resolve => setTimeout(resolve, 150));
  const secondReplay = psqlAsync(transitionSql({ batchId: replayBatch, key: replayKey, item: noChangeItem }));
  const [firstReplayOutput, secondReplayOutput] = await Promise.all([firstReplay, secondReplay]);
  const firstReplayResult = jsonResult(firstReplayOutput);
  const secondReplayResult = jsonResult(secondReplayOutput);
  assert.equal(firstReplayResult.outcomes[0].status, 'NO_CHANGE');
  assert.equal(secondReplayResult.outcomes[0].status, 'NO_CHANGE');
  assert.equal(secondReplayResult._idempotent_replay, true);
  assert.equal(secondReplayResult.batch_receipt_id, firstReplayResult.batch_receipt_id);

  const transitionItem = {
    candidate_id: candidateId,
    expected_authority_mode: 'GOOGLE_PRIMARY',
    expected_canonical_version: 0,
    expected_entitlement_enabled: false,
    new_authority_mode: 'SUPABASE_PRIMARY',
    entitlement_enabled: false,
    in_flight_disposition: 'DRAINED',
    expected_generation_id: generationId,
    expected_generation_version: 1,
    expected_accepted_canonical_cursor: 0,
    expected_required_visible_cursor: 0,
    expected_effective_visible_cursor: 0
  };
  const firstTransition = psqlAsync(transitionSql({
    batchId: randomUUID(), key: `r9-parallel-winner-${randomUUID()}`,
    item: transitionItem, sleep: true
  }));
  await new Promise(resolve => setTimeout(resolve, 150));
  const secondTransition = psqlAsync(transitionSql({
    batchId: randomUUID(), key: `r9-parallel-loser-${randomUUID()}`,
    item: transitionItem
  }));
  const [firstTransitionOutput, secondTransitionOutput] = await Promise.all([
    firstTransition, secondTransition
  ]);
  const firstTransitionResult = jsonResult(firstTransitionOutput);
  const secondTransitionResult = jsonResult(secondTransitionOutput);
  assert.equal(firstTransitionResult.outcomes[0].status, 'COMMITTED');
  assert.equal(secondTransitionResult.outcomes[0].status, 'REJECTED');
  assert.equal(secondTransitionResult.outcomes[0].error_code, 'SEMANTIC_REJECTION');
  assert.equal(psql(`select count(*) from private.candidate_daily_authority_transitions
    where environment='TEST' and candidate_id='${candidateId}'`), '1');
  assert.equal(psql(`select authority_mode from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id='${candidateId}'`), 'SUPABASE_PRIMARY');
});

test('parallel first rollback attempts cannot cross unresolved projection work', {
  skip: !enabled
}, async () => {
  const candidateId = randomUUID();
  const generationId = randomUUID();
  const generationBatchId = randomUUID();
  const commandId = randomUUID();
  psql(fixtureSql(candidateId, generationId, generationBatchId));
  psql(`
    update private.candidate_daily_authority_scopes
    set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id='${candidateId}';
    insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
      command_class,idempotency_key,request_sha256,canonical_version_before,canonical_version_after,state,
      terminal_http_status,terminal_body_json,terminal_body_sha256,correlation_id,completed_at_utc)
    values('${commandId}','TEST','${candidateId}','CANDIDATE','AVAILABILITY_APPLY',
      'r10-rollback-concurrency-command-${candidateId.replaceAll('-', '')}','${'b'.repeat(64)}',0,1,
      'COMPLETED',200,'{}'::jsonb,'${'c'.repeat(64)}','01K2ABCDEF0123456789ABCDE1',now());
    insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,candidate_id,
      availability_date,availability_version,preference,command_id,state,correlation_id)
    values(gen_random_uuid(),'TEST','${candidateId}',date '2026-08-17',1,'LONG_DAY','${commandId}',
      'PENDING','01K2ABCDEF0123456789ABCDE1');
  `);

  const rollbackItem = {
    candidate_id: candidateId,
    expected_authority_mode: 'SUPABASE_PRIMARY',
    expected_canonical_version: 0,
    expected_entitlement_enabled: false,
    new_authority_mode: 'ROLLBACK_PENDING',
    entitlement_enabled: false,
    in_flight_disposition: 'NONE'
  };
  const first = psqlAsync(transitionSql({
    batchId: randomUUID(), key: `r10-parallel-rollback-a-${randomUUID()}`,
    item: rollbackItem, sleep: true
  }));
  await new Promise(resolve => setTimeout(resolve, 150));
  const second = psqlAsync(transitionSql({
    batchId: randomUUID(), key: `r10-parallel-rollback-b-${randomUUID()}`,
    item: rollbackItem
  }));
  const [firstResult, secondResult] = (await Promise.all([first, second])).map(jsonResult);
  for (const result of [firstResult, secondResult]) {
    assert.equal(result.outcomes[0].status, 'REJECTED');
    assert.equal(result.outcomes[0].error_code, 'CANDIDATE_DAILY_NOT_READY');
  }
  assert.equal(psql(`select authority_mode from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id='${candidateId}'`), 'SUPABASE_PRIMARY');
  assert.equal(psql(`select transition_in_progress from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id='${candidateId}'`), 'f');
  assert.equal(psql(`select count(*) from private.candidate_daily_authority_transitions
    where environment='TEST' and candidate_id='${candidateId}'`), '0');
});
