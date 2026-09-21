import assert from 'node:assert/strict';
import test from 'node:test';

import { buildWeeklyProtectedC1Stream } from '../broker/src/banking-pay/weekly-source-c1-stream.mjs';

const ID = Object.freeze({
  agency: '10000000-0000-4000-8000-000000000001',
  actor: '10000000-0000-4000-8000-000000000002',
  request: '10000000-0000-4000-8000-000000000003',
  candidate: '10000000-0000-4000-8000-000000000004',
  contract: '10000000-0000-4000-8000-000000000005',
  root: '10000000-0000-4000-8000-000000000006',
  financial: '10000000-0000-4000-8000-000000000007',
  clientSource: '10000000-0000-4000-8000-000000000008',
  component: '10000000-0000-4000-8000-000000000009',
});
const H = Object.freeze({ a: 'a'.repeat(64), b: 'b'.repeat(64), c: 'c'.repeat(64) });

function baseInput(expectedEntitlementSha256 = null) {
  return {
    agency_id: ID.agency,
    actor_user_id: ID.actor,
    request_id: ID.request,
    candidate_id: ID.candidate,
    contract_id: ID.contract,
    root_timesheet_id: ID.root,
    week_ending_date: '2026-09-20',
    source_mode: 'HEALTHROSTER_WEEKLY',
    client_sources: [{
      source_system: 'HEALTHROSTER',
      external_identity: 'upload-1',
      external_revision: '1',
      document_sha256: H.a,
      client_source_id: ID.clientSource,
      source_complete: true,
      source_present: true,
      approved_minutes: 450,
      work_date: '2026-09-14',
    }],
    root_financial: {
      source_system: 'CLOUDTMS_TIMESHEET_FINANCIAL',
      external_identity: ID.financial,
      external_revision: '3',
      document_sha256: H.b,
      financial_row_id: ID.financial,
      root_version: 2,
      financial_timesheet_version: 2,
      financial_revision_digest: 'existing-md5-authority',
      work_date: null,
    },
    provider: {
      source_system: 'CLOUDTMS_PROVIDER',
      external_identity: ID.candidate,
      external_revision: '7',
      document_sha256: H.c,
      source_pay_method: 'PAYE',
      umbrella_id: null,
      provider_authority_sha256: H.c,
      target_pay_method: 'PAYE',
      target_umbrella_id: null,
      target_enabled: null,
      target_vat_chargeable: null,
      work_date: null,
    },
    ...(expectedEntitlementSha256 ? { expected_entitlement_sha256: expectedEntitlementSha256 } : {}),
    components: [{
      authority: { kind: 'APPROVED_COMPONENT' },
      component: {
        component_ordinal: 1,
        component_id: ID.component,
        source_ordinal: null,
        source_key: `10:${ID.root}:SEGMENT:000000000001`,
        component_kind: 'WORKED_TIME',
        economic_key_type: 'TS_DAY',
        economic_key_value: '2026-09-14',
        component_member_identity: 'event:one',
        segment_id: 'segment-1',
        segment_key: 'segment-key-1',
        segment_stable_key: 'event:one',
        work_date: '2026-09-14',
        reference_number: null,
        hours_day: '7.500000', hours_night: '0.000000', hours_sat: '0.000000',
        hours_sun: '0.000000', hours_bh: '0.000000', additional_code_raw: null,
        unit_count: null, unit_pay_rate: null, unit_charge_rate: null,
        expense_code: null,
        pay_ex_vat: '100.00', charge_ex_vat: '200.00',
        exclude_from_pay: false, origin: 'WEEKLY_SOURCE_APPROVED_TARGET',
      },
    }],
  };
}

test('builds every required normalized source, binds approval non-circularly and is deterministic', async () => {
  const first = await buildWeeklyProtectedC1Stream(baseInput());
  const second = await buildWeeklyProtectedC1Stream(baseInput());
  assert.deepEqual(first, second);
  assert.deepEqual(first.stream.sources.map((row) => row.authority_kind), [
    'CLIENT_SOURCE', 'ROOT_FINANCIAL', 'PROVIDER', 'APPROVED_COMPONENT', 'OFFICE_APPROVAL',
  ]);
  assert.equal(first.stream.components[0].source_ordinal, 4);
  assert.equal(first.stream.components[0].source_key, `10:${ID.root}:SEGMENT:000000000001`);
  assert.match(first.stream.entitlement_sha256, /^[0-9a-f]{64}$/);
  assert.equal(first.stream.is_zero_entitlement, false);
  assert.equal(first.stream.sources.at(-1).authority_kind, 'OFFICE_APPROVAL');
});

test('refuses incomplete authority and a component population not bound by Office approval', async () => {
  const incomplete = baseInput();
  incomplete.client_sources[0].source_complete = false;
  await assert.rejects(buildWeeklyProtectedC1Stream(incomplete), { code: 'C1_STREAM_CLIENT_SOURCE_INCOMPLETE' });

  await assert.rejects(buildWeeklyProtectedC1Stream(baseInput('f'.repeat(64))), {
    code: 'C1_STREAM_APPROVED_ENTITLEMENT_MISMATCH',
  });
});
