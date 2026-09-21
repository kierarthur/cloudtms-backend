import assert from 'node:assert/strict';
import test from 'node:test';
import { canonicalDigest } from './canonical-json.mjs';
import { createC1ContractEmulator, WEEKLY_SOURCE_C1_EMULATOR_CONTRACT } from './c1-contract-emulator.mjs';

const REQUEST_ID = '10000000-0000-4000-8000-000000000001';
const REQUEST_SHA = 'a'.repeat(64);
const SOURCE_ID = '20000000-0000-4000-8000-000000000002';
const COMPONENT_ID = '30000000-0000-4000-8000-000000000003';

function stageRequest(overrides = {}) {
  return {
    schema_version: 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_V1',
    c1_request: {
      request_id: REQUEST_ID,
      request_sha256: REQUEST_SHA,
      source_mode: 'HEALTHROSTER_WEEKLY',
      source_manifest_sha256: 'b'.repeat(64),
      entitlement_sha256: 'c'.repeat(64),
      approval_sha256: 'd'.repeat(64),
      expected_source_count: '1',
      expected_component_count: '1',
      is_zero_entitlement: false,
    },
    c1_sources: [{
      source_ordinal: 1,
      source_id: SOURCE_ID,
      authority_kind: 'APPROVED_COMPONENT',
      source_system: 'CLOUDTMS_WEEKLY_SOURCE',
      external_identity: COMPONENT_ID,
      source_row_sha256: 'e'.repeat(64),
      record_type: 'SOURCE',
      parts: [],
    }],
    c1_components: [{
      component_ordinal: 1,
      component_id: COMPONENT_ID,
      source_ordinal: 1,
      source_key: '10:root:SEGMENT:000000000001',
      component_kind: 'WORKED_TIME',
      component_sha256: 'f'.repeat(64),
      record_type: 'COMPONENT',
    }],
    ...overrides,
  };
}

function exchange(response = { ok: true, category: 'COMPLETE_ENTITLEMENT', response_code: 'SEALED_CONTRACT_ACCEPTED' }) {
  const request = stageRequest();
  return {
    expectedRequestId: REQUEST_ID,
    expectedRequestSha256: REQUEST_SHA,
    expectedInputDigest: canonicalDigest(request),
    response,
  };
}

test('TH-021 emulator validates one exact seal without calculating Workbench outcomes', async () => {
  const emulator = createC1ContractEmulator({
    exchanges: [exchange()],
    verifyRequestSeal: async () => true,
  });
  assert.deepEqual(await emulator.publish(stageRequest()), {
    ok: true,
    category: 'COMPLETE_ENTITLEMENT',
    response_code: 'SEALED_CONTRACT_ACCEPTED',
  });
  assert.equal(emulator.assertComplete().releaseEvidenceEligible, false);
  assert.deepEqual(await emulator.publish(stageRequest()), {
    ok: true,
    category: 'COMPLETE_ENTITLEMENT',
    response_code: 'SEALED_CONTRACT_ACCEPTED',
  });
  assert.equal(emulator.calls().length, 2);
  assert.equal(emulator.calls()[1].replay, true);
  assert.throws(() => emulator.assertFinalReleaseEligible(), (error) => error.code === 'C1_EMULATOR_NOT_RELEASE_EVIDENCE');
  assert.equal(WEEKLY_SOURCE_C1_EMULATOR_CONTRACT.calculatesResidual, false);
  assert.equal(WEEKLY_SOURCE_C1_EMULATOR_CONTRACT.createsDraft, false);
});

test('TH-021 emulator refuses seal drift and Banking-owned result fields', async () => {
  const emulator = createC1ContractEmulator({
    exchanges: [exchange({ ok: false, category: 'WAITING', response_code: 'WAITING' })],
  });
  const mismatched = stageRequest();
  mismatched.c1_request.request_sha256 = 'b'.repeat(64);
  await assert.rejects(() => emulator.publish(mismatched), (error) => error.code === 'C1_EMULATOR_REQUEST_SEAL_MISMATCH');
  assert.throws(() => createC1ContractEmulator({
    exchanges: [{
      expectedRequestId: REQUEST_ID,
      expectedRequestSha256: REQUEST_SHA,
      expectedInputDigest: canonicalDigest(stageRequest()),
      response: { ok: true, category: 'COMPLETE_ENTITLEMENT', response_code: 'BAD', residual_pence: '20' },
    }],
  }), (error) => error.code === 'C1_EMULATOR_BANKING_RESULT_FORBIDDEN');
});

test('TH-021 emulator refuses body drift, idempotency conflicts and false empty certification', async () => {
  const emulator = createC1ContractEmulator({ exchanges: [exchange()] });
  await emulator.publish(stageRequest());
  const changed = stageRequest({ reason: 'changed after first publication' });
  await assert.rejects(
    () => emulator.publish(changed),
    (error) => error.code === 'C1_EMULATOR_IDEMPOTENCY_CONFLICT',
  );

  const emptyRequest = stageRequest({
    c1_request: {
      ...stageRequest().c1_request,
      expected_component_count: '0',
      is_zero_entitlement: true,
    },
    c1_components: [],
  });
  const emptyEmulator = createC1ContractEmulator({
    exchanges: [{
      expectedRequestId: REQUEST_ID,
      expectedRequestSha256: REQUEST_SHA,
      expectedInputDigest: canonicalDigest(emptyRequest),
      response: { ok: true, category: 'COMPLETE_ENTITLEMENT', response_code: 'WRONG_EMPTY_RESULT' },
    }],
  });
  await assert.rejects(
    () => emptyEmulator.publish(emptyRequest),
    (error) => error.code === 'C1_EMULATOR_EMPTY_CERTIFICATION_INVALID',
  );
});
