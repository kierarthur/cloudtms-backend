import assert from 'node:assert/strict';
import test from 'node:test';

import {
  publishDurableWeeklySourceC1,
  WeeklySourceC1DurablePublicationError,
} from '../broker/src/banking-pay/weekly-source-c1-durable-publication.mjs';
import {
  prepareWeeklySourceC1Publication,
} from '../broker/src/banking-pay/weekly-source-c1-publication.mjs';
import {
  stringifyWeeklySourceC1Json,
} from '../broker/src/banking-pay/weekly-source-c1-adapter.mjs';
import { buildWeeklyProtectedC1Stream } from '../broker/src/banking-pay/weekly-source-c1-stream.mjs';

const ID = Object.freeze({
  agency: '30000000-0000-4000-8000-000000000001',
  actor: '30000000-0000-4000-8000-000000000002',
  request: '30000000-0000-4000-8000-000000000003',
  candidate: '30000000-0000-4000-8000-000000000004',
  contract: '30000000-0000-4000-8000-000000000005',
  root: '30000000-0000-4000-8000-000000000006',
  financial: '30000000-0000-4000-8000-000000000007',
  source: '30000000-0000-4000-8000-000000000008',
  component: '30000000-0000-4000-8000-000000000009',
  operation: '30000000-0000-4000-8000-00000000000a',
  publication: '30000000-0000-4000-8000-00000000000b',
});
const HASH = Object.freeze({ a: 'a'.repeat(64), b: 'b'.repeat(64), c: 'c'.repeat(64) });

async function durableReadFixture(state = 'READY') {
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
      document_sha256: HASH.a, client_source_id: ID.source, source_complete: true,
      source_present: true, approved_minutes: 450, work_date: '2026-09-14',
    }],
    root_financial: {
      source_system: 'CLOUDTMS_TIMESHEET_FINANCIAL', external_identity: ID.financial,
      external_revision: '1', document_sha256: HASH.b, financial_row_id: ID.financial,
      root_version: 1, financial_timesheet_version: 1, financial_revision_digest: 'fin-v1',
      work_date: null,
    },
    provider: {
      source_system: 'CLOUDTMS_PROVIDER', external_identity: ID.candidate,
      external_revision: '1', document_sha256: HASH.c, source_pay_method: 'PAYE',
      umbrella_id: null, provider_authority_sha256: HASH.c, target_pay_method: 'PAYE',
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
  const prepared = await prepareWeeklySourceC1Publication({
    prepared_stream: stream,
    start_facts: {
      request_sequence: 9_007_199_254_740_993n,
      expected_head_revision: 9_007_199_254_740_995n,
      financial_row_id: ID.financial,
    },
  });
  const toStored = (value) => {
    if (typeof value === 'bigint') return value.toString(10);
    if (Array.isArray(value)) return value.map(toStored);
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, toStored(entry)]));
    }
    return value;
  };
  return {
    ok: true,
    contract: 'WEEKLY_PROTECTED_C1_READ_V1',
    publication_request_id: ID.request,
    state,
    publication: toStored({
      request: prepared.request,
      request_sha256: prepared.request_sha256,
      records: prepared.records,
    }),
    resume_checkpoint: null,
    unknown_checkpoint: null,
  };
}

function controlReply(requestHash, sequence, overrides = {}) {
  const value = BigInt(sequence);
  return {
    contract: 'WEEKLY_SOURCE_C1_V1', ok: true, status: 'PROGRESS', code: 'OK',
    operation_id: ID.operation, scope_id: null, owner_epoch: 1n,
    sequence: value, next_sequence: value + 1n, phase: 'SOURCES',
    source_cursor: 0n, component_cursor: 0n, verify_cursor: 0n,
    rows_read: 0n, rows_written: 0n, work_used: 1n, processed_bytes: 1n,
    has_more: true, replayed: false, retry_after_ms: 0n,
    request_sha256: requestHash, checkpoint_sha256: HASH.a,
    receipt_sha256: value.toString(16).padStart(64, '0'), publication_id: null,
    head_revision: null, source_identity_sha256: null, operation_created: false,
    input_records_consumed: 0n, part_cursor: 0n, verify_part_cursor: 0n,
    verify_byte_offset: 0n, ...overrides,
  };
}

test('durable publication preserves bigint tokens, checkpoints each C1 call and completes once', async () => {
  const read = await durableReadFixture();
  const dataCalls = [];
  const c1Calls = [];
  let sequence = 1;
  const dataRpc = async (name, parameters) => {
    dataCalls.push([name, parameters]);
    if (name === 'weekly_exceptional_pay_read_c1_request_v1') return read;
    if (name === 'weekly_exceptional_pay_record_c1_checkpoint_v1') return { ok: true };
    if (name === 'weekly_exceptional_pay_complete_c1_publication_v1') {
      return { ok: true, outcome: 'PUBLISHED', publication_request_id: ID.request };
    }
    throw new Error(`unexpected data RPC ${name}`);
  };
  const c1RawRpc = async (name, parametersJson) => {
    c1Calls.push([name, parametersJson]);
    const parameters = JSON.parse(parametersJson);
    const requestHash = read.publication.request_sha256;
    if (name === 'weekly_source_start_c1') {
      assert.match(parametersJson, /9007199254740993/);
      return stringifyWeeklySourceC1Json(controlReply(requestHash, sequence++, { operation_created: true }));
    }
    if (name === 'weekly_source_stage_c1') {
      return stringifyWeeklySourceC1Json(controlReply(requestHash, sequence++, {
        input_records_consumed: BigInt(parameters.p_records_json.length),
      }));
    }
    if (name === 'weekly_source_continue_c1') {
      return stringifyWeeklySourceC1Json(controlReply(requestHash, sequence++, {
        phase: 'VERIFY_COMPONENTS', has_more: false,
      }));
    }
    if (name === 'weekly_source_certify_c1') {
      return stringifyWeeklySourceC1Json(controlReply(requestHash, sequence++, {
        phase: 'CERTIFIED', status: 'READY', has_more: false,
      }));
    }
    if (name === 'weekly_source_publish_c1') {
      return stringifyWeeklySourceC1Json(controlReply(requestHash, sequence++, {
        phase: 'PUBLISHED', status: 'PUBLISHED', has_more: false,
        publication_id: ID.publication, head_revision: 9_007_199_254_741_111n,
        source_identity_sha256: HASH.b,
      }));
    }
    throw new Error(`unexpected C1 RPC ${name}`);
  };

  const result = await publishDurableWeeklySourceC1({
    actor_user_id: ID.actor,
    publication_request_id: ID.request,
    expected_request_sha256: read.publication.request_sha256,
    data_rpc: dataRpc,
    c1_raw_rpc: c1RawRpc,
    max_stage_records: 256,
  });
  assert.equal(result.outcome, 'PUBLISHED');
  assert.equal(c1Calls.filter(([name]) => name === 'weekly_source_start_c1').length, 1);
  assert.equal(dataCalls.filter(([name]) => name === 'weekly_exceptional_pay_record_c1_checkpoint_v1').length, c1Calls.length);
  assert.equal(dataCalls.at(-1)[0], 'weekly_exceptional_pay_complete_c1_publication_v1');
  const finalCheckpoint = dataCalls.filter(([name]) => name === 'weekly_exceptional_pay_record_c1_checkpoint_v1').at(-1)[1];
  assert.equal(typeof finalCheckpoint.p_request.result.head_revision, 'bigint');
});

test('an unknown C1 outcome is durably recorded once and never retried automatically', async () => {
  const read = await durableReadFixture();
  const dataCalls = [];
  let c1CallCount = 0;
  await assert.rejects(publishDurableWeeklySourceC1({
    actor_user_id: ID.actor,
    publication_request_id: ID.request,
    expected_request_sha256: read.publication.request_sha256,
    data_rpc: async (name, parameters) => {
      dataCalls.push([name, parameters]);
      if (name === 'weekly_exceptional_pay_read_c1_request_v1') return read;
      if (name === 'weekly_exceptional_pay_record_c1_unknown_v1') return { ok: true };
      throw new Error(`unexpected data RPC ${name}`);
    },
    c1_raw_rpc: async () => {
      c1CallCount += 1;
      throw new Error('connection ended before a response was received');
    },
  }), (error) => (
    error instanceof WeeklySourceC1DurablePublicationError
    && error.code === 'C1_DURABLE_RECOVERY_REQUIRED'
  ));
  assert.equal(c1CallCount, 1);
  const unknown = dataCalls.find(([name]) => name === 'weekly_exceptional_pay_record_c1_unknown_v1')[1].p_request;
  assert.equal(unknown.recovery_call.stream_kind, 'START');
  assert.equal(unknown.recovery_call.stream_id, ID.request);
  assert.equal(unknown.records_submitted, 0);
});

test('a published durable request returns without another C1 call', async () => {
  const read = await durableReadFixture('PUBLISHED');
  let c1Called = false;
  const result = await publishDurableWeeklySourceC1({
    actor_user_id: ID.actor,
    publication_request_id: ID.request,
    expected_request_sha256: read.publication.request_sha256,
    data_rpc: async () => read,
    c1_raw_rpc: async () => { c1Called = true; throw new Error('must not call C1'); },
  });
  assert.equal(result.idempotent_replay, true);
  assert.equal(c1Called, false);
});

