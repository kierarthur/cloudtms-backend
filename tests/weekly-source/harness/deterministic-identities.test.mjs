import assert from 'node:assert/strict';
import test from 'node:test';
import {
  deriveDeterministicUuid,
  deriveExternalSourceKey,
  DeterministicIdentityRegistry,
  ID_NAMESPACE
} from './deterministic-identities.mjs';

test('TH-003 SHA-256 namespace procedure returns the same fixed UUID every time', () => {
  assert.equal(ID_NAMESPACE, 'cloudtms.weekly-source.test.scenario.v1');
  const first = deriveDeterministicUuid('WS-HARNESS-FOUNDATION-001', 'candidate', 0);
  const second = deriveDeterministicUuid('WS-HARNESS-FOUNDATION-001', 'candidate', 0);
  assert.equal(first, second);
  assert.equal(first, '21352371-a695-8a6f-a139-a2a4b84dff79');
  assert.match(first, /^[a-f0-9]{8}-[a-f0-9]{4}-8[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
  assert.notEqual(first, deriveDeterministicUuid('WS-HARNESS-FOUNDATION-001', 'client', 0));
});

test('TH-003 registry refuses a digest collision between distinct entity roles', () => {
  const registry = new DeterministicIdentityRegistry('WS-HARNESS-FOUNDATION-001', {
    digestFunction: () => Buffer.alloc(32, 7)
  });
  registry.uuid('candidate', 0);
  assert.throws(() => registry.uuid('client', 0), /collision/);
  assert.equal(registry.uuid('candidate', 0), registry.uuid('candidate', 0));
});

test('TH-003 external source keys are separate, stable and profile-safe', () => {
  const key = deriveExternalSourceKey('WS-HARNESS-FOUNDATION-001', 'nhsp.request', 4, { prefix: 'NHSP', maxLength: 48 });
  assert.equal(key, deriveExternalSourceKey('WS-HARNESS-FOUNDATION-001', 'nhsp.request', 4, { prefix: 'NHSP', maxLength: 48 }));
  assert.match(key, /^NHSP-[A-Z0-9_-]+$/);
  assert(key.length <= 48);

  const registry = new DeterministicIdentityRegistry('WS-HARNESS-FOUNDATION-001', {
    digestFunction: () => Buffer.alloc(32, 9)
  });
  registry.externalKey('abcdefghij1', 0, { maxLength: 12 });
  assert.throws(() => registry.externalKey('abcdefghij2', 0, { maxLength: 12 }), /collision/);

  const declared = new DeterministicIdentityRegistry('WS-HARNESS-FOUNDATION-001', {
    digestFunction: () => Buffer.alloc(32, 9)
  });
  declared.externalKey('abcdefghij1', 0, { maxLength: 12, declaredCollisionGroup: 'DUPLICATE_REQUEST' });
  assert.doesNotThrow(() => declared.externalKey('abcdefghij2', 0, { maxLength: 12, declaredCollisionGroup: 'DUPLICATE_REQUEST' }));
});
