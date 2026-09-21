import assert from 'node:assert/strict';
import test from 'node:test';

import {
  authorWeeklySourceC1Stream,
  bindWeeklySourceC1Start,
  hashWeeklySourceC1OfficeIntent,
} from '../broker/src/banking-pay/weekly-source-c1-authoring.mjs';

const ID = Object.freeze({
  agency: 'a1000000-0000-4000-8000-000000000001',
  actor: 'a2000000-0000-4000-8000-000000000002',
  candidate: 'a3000000-0000-4000-8000-000000000003',
  contract: 'a4000000-0000-4000-8000-000000000004',
  root: 'a5000000-0000-4000-8000-000000000005',
  financial: 'a6000000-0000-4000-8000-000000000006',
  request: 'a7000000-0000-4000-8000-000000000007',
  source1: 'a8000000-0000-4000-8000-000000000008',
  source2: 'a9000000-0000-4000-8000-000000000009',
  component: 'aa000000-0000-4000-8000-000000000010',
});

const common = {
  root_timesheet_id: ID.root,
  candidate_id: ID.candidate,
  contract_id: ID.contract,
};

async function fixture() {
  const provisionalComponent = {
    component_ordinal: 1,
    component_id: ID.component,
    source_ordinal: 1,
    source_key: `10:${ID.root}:SEGMENT:000000000001`,
    component_kind: 'WORKED_TIME',
    economic_key_type: 'TS_DAY',
    economic_key_value: '2026-09-01',
    component_member_identity: 'DAY',
    segment_id: `ts:${ID.root}:abc12345`,
    segment_key: 'd:2026-09-01|s:09:00|e:17:00|b:30',
    segment_stable_key: 'event:a1000000-0000-4000-8000-000000000099',
    work_date: '2026-09-01',
    reference_number: null,
    hours_day: '7.500000',
    hours_night: '0.000000',
    hours_sat: '0.000000',
    hours_sun: '0.000000',
    hours_bh: '0.000000',
    additional_code_raw: null,
    unit_count: null,
    unit_pay_rate: null,
    unit_charge_rate: null,
    expense_code: null,
    pay_ex_vat: '112.50',
    charge_ex_vat: '187.50',
    exclude_from_pay: false,
    origin: 'WEEKLY_SOURCE_APPROVED_TARGET',
  };

  // The Office intent binds the completed entitlement. Build the component
  // stream once with a placeholder approval document, then rebuild the final
  // stream with the exact resulting intent. The component fold is independent
  // of source documents and is therefore identical.
  const baseSources = [{
    source_ordinal: 1,
    source_id: ID.source1,
    authority_kind: 'APPROVED_COMPONENT',
    source_system: 'CLOUDTMS_WEEKLY_SOURCE',
    external_identity: ID.component,
    external_revision: '1',
    work_date: '2026-09-01',
    source_document_sha256: '1'.repeat(64),
    ...common,
    document: {
      contract: 'C1_APPROVED_COMPONENT_V1',
      document_sha256: '1'.repeat(64),
      component_id: ID.component,
    },
  }];
  const pre = await authorWeeklySourceC1Stream({
    agency_id: ID.agency,
    sources: baseSources,
    components: [provisionalComponent],
  });
  const intent = await hashWeeklySourceC1OfficeIntent({
    agency_id: ID.agency,
    actor_user_id: ID.actor,
    request_id: ID.request,
    ...common,
    week_ending_date: '2026-09-06',
    entitlement_sha256: pre.entitlement_sha256,
    decision: 'APPROVE_ENTITLEMENT',
  });
  const stream = await authorWeeklySourceC1Stream({
    agency_id: ID.agency,
    sources: [...baseSources, {
      source_ordinal: 2,
      source_id: ID.source2,
      authority_kind: 'OFFICE_APPROVAL',
      source_system: 'CLOUDTMS_WEEKLY_SOURCE',
      external_identity: ID.request,
      external_revision: '1',
      work_date: null,
      source_document_sha256: '2'.repeat(64),
      ...common,
      document: {
        contract: 'C1_OFFICE_APPROVAL_V1',
        document_sha256: '2'.repeat(64),
        actor_user_id: ID.actor,
        request_id: ID.request,
        approval_intent_sha256: intent,
        entitlement_sha256: pre.entitlement_sha256,
        decision: 'APPROVE_ENTITLEMENT',
      },
    }],
    components: [provisionalComponent],
  });
  return { stream, intent };
}

test('authors deterministic source/component records, parts and stream commitments', async () => {
  const first = await fixture();
  const second = await fixture();
  assert.deepEqual(first, second);
  assert.match(first.intent, /^[0-9a-f]{64}$/);
  assert.match(first.stream.source_manifest_sha256, /^[0-9a-f]{64}$/);
  assert.match(first.stream.entitlement_sha256, /^[0-9a-f]{64}$/);
  assert.equal(first.stream.expected_source_count, '2');
  assert.equal(first.stream.expected_component_count, '1');
  assert.equal(first.stream.is_zero_entitlement, false);
  assert.equal(first.stream.sources[0].parts[0].record_type, 'PART');
});

test('binds outer approval and request after both stream commitments exist', async () => {
  const { stream } = await fixture();
  const bound = await bindWeeklySourceC1Start({
    agency_id: ID.agency,
    request_id: ID.request,
    request_sequence: '1',
    actor_user_id: ID.actor,
    candidate_id: ID.candidate,
    contract_id: ID.contract,
    root_timesheet_id: ID.root,
    week_ending_date: '2026-09-06',
    source_mode: 'HEALTHROSTER_WEEKLY',
    expected_head_revision: '0',
    expected_source_count: stream.expected_source_count,
    expected_component_count: stream.expected_component_count,
    expected_payload_bytes: stream.expected_payload_bytes,
    source_manifest_sha256: stream.source_manifest_sha256,
    entitlement_sha256: stream.entitlement_sha256,
    is_zero_entitlement: false,
    financial_row_id: ID.financial,
  });
  assert.match(bound.start.approval_sha256, /^[0-9a-f]{64}$/);
  assert.match(bound.request_sha256, /^[0-9a-f]{64}$/);
  assert.notEqual(bound.start.approval_sha256, bound.request_sha256);
});

test('preserves null versus empty and refuses order/lexical ambiguity', async () => {
  const { stream } = await fixture();
  const bad = { ...stream.components[0], component_ordinal: 2 };
  delete bad.component_sha256;
  delete bad.record_type;
  await assert.rejects(
    authorWeeklySourceC1Stream({
      agency_id: ID.agency,
      sources: [{
        source_ordinal: 1,
        source_id: ID.source1,
        authority_kind: 'APPROVED_COMPONENT',
        source_system: 'CLOUDTMS_WEEKLY_SOURCE',
        external_identity: ID.component,
        external_revision: '1',
        work_date: null,
        source_document_sha256: '1'.repeat(64),
        ...common,
        document: {
          contract: 'C1_APPROVED_COMPONENT_V1',
          document_sha256: '1'.repeat(64),
        },
      }],
      components: [bad],
    }),
    { code: 'C1_AUTHORING_COMPONENT_ORDER_INVALID' },
  );
});

test('preserves the original evidence digest and refuses a normalized-document substitution', async () => {
  const { stream } = await fixture();
  assert.equal(stream.sources[0].source_document_sha256, '1'.repeat(64));
  assert.notEqual(stream.sources[0].source_document_sha256, stream.sources[0].parts[0].fragment_sha256);

  await assert.rejects(
    authorWeeklySourceC1Stream({
      agency_id: ID.agency,
      sources: [{
        source_ordinal: 1,
        source_id: ID.source1,
        authority_kind: 'CLIENT_SOURCE',
        source_system: 'CLOUDTMS_WEEKLY_SOURCE',
        external_identity: 'source-1',
        external_revision: '1',
        source_document_sha256: '3'.repeat(64),
        work_date: '2026-09-01',
        ...common,
        document: {
          contract: 'C1_CLIENT_SOURCE_V1',
          document_sha256: '4'.repeat(64),
        },
      }],
      components: [],
    }),
    { code: 'C1_AUTHORING_DOCUMENT_AUTHORITY_MISMATCH' },
  );
});
