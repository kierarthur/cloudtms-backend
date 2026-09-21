import assert from 'node:assert/strict';
import test from 'node:test';

import {
  prepareWeeklySourceC1Publication,
  publishWeeklySourceC1Prepared,
  resumeWeeklySourceC1CheckpointFromRecovery,
} from '../broker/src/banking-pay/weekly-source-c1-publication.mjs';
import { buildWeeklyProtectedC1Stream } from '../broker/src/banking-pay/weekly-source-c1-stream.mjs';

const ID = Object.freeze({
  agency: '20000000-0000-4000-8000-000000000001',
  actor: '20000000-0000-4000-8000-000000000002',
  request: '20000000-0000-4000-8000-000000000003',
  candidate: '20000000-0000-4000-8000-000000000004',
  contract: '20000000-0000-4000-8000-000000000005',
  root: '20000000-0000-4000-8000-000000000006',
  financial: '20000000-0000-4000-8000-000000000007',
  source: '20000000-0000-4000-8000-000000000008',
  component: '20000000-0000-4000-8000-000000000009',
  operation: '20000000-0000-4000-8000-00000000000a',
  publication: '20000000-0000-4000-8000-00000000000b',
});
const H = Object.freeze({ a: 'a'.repeat(64), b: 'b'.repeat(64), c: 'c'.repeat(64) });

async function preparedPublication() {
  const stream = await buildWeeklyProtectedC1Stream({
    agency_id: ID.agency,
    actor_user_id: ID.actor,
    request_id: ID.request,
    candidate_id: ID.candidate,
    contract_id: ID.contract,
    root_timesheet_id: ID.root,
    week_ending_date: '2026-09-20',
    source_mode: 'HEALTHROSTER_WEEKLY',
    client_sources: [{
      source_system: 'HEALTHROSTER', external_identity: 'source-1', external_revision: '1',
      document_sha256: H.a, client_source_id: ID.source, source_complete: true,
      source_present: true, approved_minutes: 450, work_date: '2026-09-14',
    }],
    root_financial: {
      source_system: 'CLOUDTMS_TIMESHEET_FINANCIAL', external_identity: ID.financial,
      external_revision: '1', document_sha256: H.b, financial_row_id: ID.financial,
      root_version: 1, financial_timesheet_version: 1, financial_revision_digest: 'fin-v1',
      work_date: null,
    },
    provider: {
      source_system: 'CLOUDTMS_PROVIDER', external_identity: ID.candidate,
      external_revision: '1', document_sha256: H.c, source_pay_method: 'PAYE',
      umbrella_id: null, provider_authority_sha256: H.c, target_pay_method: 'PAYE',
      target_umbrella_id: null, target_enabled: null, target_vat_chargeable: null,
      work_date: null,
    },
    components: [{
      authority: { kind: 'APPROVED_COMPONENT' },
      component: {
        component_ordinal: 1, component_id: ID.component, source_ordinal: null,
        source_key: `10:${ID.root}:SEGMENT:000000000001`, component_kind: 'WORKED_TIME',
        economic_key_type: 'TS_DAY', economic_key_value: '2026-09-14',
        component_member_identity: 'event-1', segment_id: 'segment-1',
        segment_key: 'segment-key-1', segment_stable_key: 'event-1', work_date: '2026-09-14',
        reference_number: null, hours_day: '7.500000', hours_night: '0.000000',
        hours_sat: '0.000000', hours_sun: '0.000000', hours_bh: '0.000000',
        additional_code_raw: null, unit_count: null, unit_pay_rate: null,
        unit_charge_rate: null, expense_code: null,
        pay_ex_vat: '100.00', charge_ex_vat: '200.00', exclude_from_pay: false,
        origin: 'WEEKLY_SOURCE_APPROVED_TARGET',
      },
    }],
  });
  return prepareWeeklySourceC1Publication({
    prepared_stream: stream,
    start_facts: {
      request_sequence: 1n,
      expected_head_revision: 0n,
      financial_row_id: ID.financial,
    },
  });
}

function reply(sequence, overrides = {}) {
  return {
    status: 'PROGRESS', phase: 'SOURCES', operation_id: ID.operation,
    owner_epoch: 1n, next_sequence: BigInt(sequence + 1), receipt_sha256: sequence.toString(16).padStart(64, '0'),
    input_records_consumed: 0n, has_more: true, ...overrides,
  };
}

test('binds exact stream counts, digests and flattened records to the start request', async () => {
  const publication = await preparedPublication();
  assert.equal(publication.request.expected_source_count, '5');
  assert.equal(publication.request.expected_component_count, '1');
  assert.match(publication.request.approval_sha256, /^[0-9a-f]{64}$/);
  assert.equal(
    publication.records.find((row) => row.authority_kind === 'OFFICE_APPROVAL').source_document_sha256,
    publication.stream.sources.at(-1).source_document_sha256,
  );
  assert.deepEqual(publication.records.map((row) => row.record_type), [
    'SOURCE', 'PART', 'SOURCE', 'PART', 'SOURCE', 'PART', 'SOURCE', 'PART', 'SOURCE', 'PART', 'COMPONENT',
  ]);
  assert.match(publication.request_sha256, /^[0-9a-f]{64}$/);
});

test('publishes staged prefixes once and checkpoints every durable cursor', async () => {
  const publication = await preparedPublication();
  const calls = [];
  let sequence = 0;
  const adapter = {
    async start() { calls.push(['start']); sequence += 1; return reply(sequence); },
    async stage(_cursor, records) {
      calls.push(['stage', records.length]);
      sequence += 1;
      return reply(sequence, { input_records_consumed: BigInt(Math.min(2, records.length)) });
    },
    async continueValidation() {
      calls.push(['validate']); sequence += 1;
      return reply(sequence, { phase: 'VERIFY', has_more: false });
    },
    async certify() {
      calls.push(['certify']); sequence += 1;
      return reply(sequence, { phase: 'CERTIFIED', status: 'READY', has_more: false });
    },
    async publish() {
      calls.push(['publish']); sequence += 1;
      return reply(sequence, {
        phase: 'PUBLISHED', status: 'PUBLISHED', has_more: false,
        publication_id: ID.publication, head_revision: 1n,
      });
    },
  };
  const checkpoints = [];
  const result = await publishWeeklySourceC1Prepared({
    adapter,
    publication,
    max_stage_records: 4,
    on_checkpoint: async (entry) => checkpoints.push(entry),
  });
  assert.equal(result.result.status, 'PUBLISHED');
  assert.equal(result.records_consumed, publication.records.length);
  assert.deepEqual(calls.map((entry) => entry[0]), [
    'start', 'stage', 'stage', 'stage', 'stage', 'stage', 'stage', 'validate', 'certify', 'publish',
  ]);
  assert.equal(checkpoints.length, calls.length);
  assert.equal(checkpoints.at(-1).phase, 'PUBLISH');
});

test('stops on explicit pending and refuses zero-progress or unknown outcomes', async () => {
  const publication = await preparedPublication();
  const pending = await publishWeeklySourceC1Prepared({
    publication,
    adapter: { async start() { return reply(1, { status: 'PENDING', phase: 'FROZEN' }); } },
  });
  assert.equal(pending.result.status, 'PENDING');

  await assert.rejects(publishWeeklySourceC1Prepared({
    publication,
    adapter: {
      async start() { return reply(1); },
      async stage(operation) { return { ...reply(1), next_sequence: operation.next_sequence, receipt_sha256: operation.receipt_sha256 }; },
    },
  }), { code: 'C1_PUBLICATION_NO_PROGRESS' });

  const unknown = Object.assign(new Error('unknown outcome'), { code: 'C1_UNKNOWN_OUTCOME' });
  const seen = [];
  await assert.rejects(publishWeeklySourceC1Prepared({
    publication,
    adapter: { async start() { throw unknown; } },
    on_checkpoint: async (entry) => seen.push(entry),
  }), unknown);
  assert.deepEqual(seen, []);
});

test('continues from the exact durable suffix without repeating start or accepted records', async () => {
  const publication = await preparedPublication();
  const calls = [];
  let sequence = 8;
  const prior = reply(7, {
    input_records_consumed: 3n,
    receipt_sha256: '7'.repeat(64),
    next_sequence: 8n,
  });
  const result = await publishWeeklySourceC1Prepared({
    publication,
    resume_checkpoint: {
      phase: 'STAGE',
      result: prior,
      next_record_offset: 3,
    },
    max_stage_records: 4,
    adapter: {
      async start() { throw new Error('start must not repeat'); },
      async stage(_operation, records) {
        calls.push(['stage', records.length, records[0].record_type]);
        sequence += 1;
        return reply(sequence, { input_records_consumed: BigInt(records.length) });
      },
      async continueValidation() {
        calls.push(['validate']); sequence += 1;
        return reply(sequence, { phase: 'VERIFY', has_more: false });
      },
      async certify() {
        calls.push(['certify']); sequence += 1;
        return reply(sequence, { phase: 'CERTIFIED', status: 'READY', has_more: false });
      },
      async publish() {
        calls.push(['publish']); sequence += 1;
        return reply(sequence, {
          phase: 'PUBLISHED', status: 'PUBLISHED', has_more: false,
          publication_id: ID.publication, head_revision: 1n,
        });
      },
    },
  });
  assert.equal(result.resumed, true);
  assert.equal(result.records_consumed, publication.records.length);
  assert.equal(calls.some(([name]) => name === 'start'), false);
  assert.equal(calls[0][0], 'stage');
  assert.equal(calls[0][1], 4);
});

test('surfaces the exact adapter recovery envelope before stopping on an unknown outcome', async () => {
  const publication = await preparedPublication();
  const recoveryCall = Object.freeze({
    contract: 'WEEKLY_SOURCE_C1_UNKNOWN_CALL_V1',
    method: 'start',
  });
  const unknown = Object.assign(new Error('transport outcome unknown'), {
    code: 'C1_OUTCOME_UNKNOWN',
    recoveryCall,
  });
  const saved = [];
  await assert.rejects(publishWeeklySourceC1Prepared({
    publication,
    adapter: { async start() { throw unknown; } },
    on_unknown: async (entry) => saved.push(entry),
  }), unknown);
  assert.equal(saved.length, 1);
  assert.equal(saved[0].phase, 'START');
  assert.equal(saved[0].recovery_call, recoveryCall);
  assert.equal(saved[0].next_record_offset, 0);
});

test('recovered partial STAGE resumes at its exact accepted prefix and publishes only the suffix', async () => {
  const publication = await preparedPublication();
  const priorOffset = 3;
  const accepted = 2;
  const recoveredResult = reply(11, {
    phase: 'SOURCES',
    input_records_consumed: BigInt(accepted),
    replayed: true,
    has_more: true,
  });
  const resume = resumeWeeklySourceC1CheckpointFromRecovery({
    unknown_checkpoint: {
      phase: 'STAGE',
      record_offset: priorOffset,
      next_record_offset: priorOffset,
      records_submitted: 4,
    },
    recovery: {
      contract: 'WEEKLY_SOURCE_C1_RECOVERY_V1',
      ok: true,
      decision: 'REPLAYED',
      code: 'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY',
      status: {},
      result: recoveredResult,
      replay_attempted: true,
    },
  });
  assert.equal(resume.next_record_offset, priorOffset + accepted);

  const submitted = [];
  let sequence = 12;
  const completed = await publishWeeklySourceC1Prepared({
    publication,
    resume_checkpoint: resume,
    max_stage_records: 3,
    adapter: {
      async start() { throw new Error('start must not repeat after recovery'); },
      async stage(_operation, records) {
        submitted.push(...records);
        sequence += 1;
        return reply(sequence, { input_records_consumed: BigInt(records.length) });
      },
      async continueValidation() {
        sequence += 1;
        return reply(sequence, { phase: 'VERIFY', has_more: false });
      },
      async certify() {
        sequence += 1;
        return reply(sequence, { phase: 'CERTIFIED', status: 'READY', has_more: false });
      },
      async publish() {
        sequence += 1;
        return reply(sequence, {
          phase: 'PUBLISHED', status: 'PUBLISHED', has_more: false,
          publication_id: ID.publication, head_revision: 1n,
        });
      },
    },
  });
  assert.equal(completed.result.status, 'PUBLISHED');
  assert.equal(completed.records_consumed, publication.records.length);
  assert.deepEqual(submitted, publication.records.slice(priorOffset + accepted));
});

test('STAGE recovery refuses a status-only result or an impossible accepted count', async () => {
  const base = {
    unknown_checkpoint: {
      phase: 'STAGE', record_offset: 2, next_record_offset: 2, records_submitted: 3,
    },
  };
  assert.throws(() => resumeWeeklySourceC1CheckpointFromRecovery({
    ...base,
    recovery: {
      contract: 'WEEKLY_SOURCE_C1_RECOVERY_V1', ok: true,
      decision: 'COMMITTED', code: 'C1_RECOVERY_COMMITTED_RECEIPT',
      result: reply(2), replay_attempted: false,
    },
  }), { code: 'C1_PUBLICATION_STAGE_RECOVERY_UNPROVED' });
  assert.throws(() => resumeWeeklySourceC1CheckpointFromRecovery({
    ...base,
    recovery: {
      contract: 'WEEKLY_SOURCE_C1_RECOVERY_V1', ok: true,
      decision: 'REPLAYED', code: 'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY',
      result: reply(2, { input_records_consumed: 4n }), replay_attempted: true,
    },
  }), { code: 'C1_PUBLICATION_CONSUMED_INVALID' });
});
