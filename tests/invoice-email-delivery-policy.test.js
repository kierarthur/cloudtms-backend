import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '../broker/src/index.js'), 'utf8');
const helperStart = source.indexOf('function invoiceEmailDeliveryPolicySummary');
const helperEnd = source.indexOf('async function handleGetInvoice', helperStart);
assert.notEqual(helperStart, -1);
assert.notEqual(helperEnd, -1);
const helperSource = source.slice(helperStart, helperEnd);
const context = {};
vm.runInNewContext(
  `${helperSource}; this.invoiceEmailDeliveryPolicySummary = invoiceEmailDeliveryPolicySummary;`,
  context
);
const summarise = context.invoiceEmailDeliveryPolicySummary;

test('invoice detail exposes only the resolved email-delivery decision', () => {
  const result = summarise({
    issue: {
      route_policy: {
        delivery_suppressed: false,
        suppression_reason: null,
        do_not_send: false,
        self_bill: true,
        route_policy_hash: 'abc123',
        canonical_to: ['private@example.test'],
        canonical_cc: ['private-cc@example.test']
      }
    }
  });

  assert.deepEqual(
    { ...result },
    {
      resolved: true,
      delivery_suppressed: false,
      suppression_reason: null,
      do_not_send: false,
      self_bill: true,
      route_policy_hash: 'abc123'
    }
  );
  assert.equal(Object.hasOwn(result, 'canonical_to'), false);
  assert.equal(Object.hasOwn(result, 'canonical_cc'), false);
});

test('invoice detail preserves an authoritative suppressed decision and reason', () => {
  const result = summarise({
    issue: {
      route_policy: {
        delivery_suppressed: true,
        suppression_reason: 'SELF_BILL_SUPPRESSED',
        do_not_send: false,
        self_bill: true
      }
    }
  });

  assert.equal(result.delivery_suppressed, true);
  assert.equal(result.suppression_reason, 'SELF_BILL_SUPPRESSED');
});

test('invoice detail does not invent a resolved policy when the resolver result is absent', () => {
  assert.equal(summarise({}), null);
  assert.equal(summarise({ issue: { route_policy: {} } }), null);
});

test('GET invoice includes the sanitised delivery decision in every response branch', () => {
  const handlerStart = source.indexOf('async function handleGetInvoice');
  const handlerEnd = source.indexOf('\nasync function ', handlerStart + 20);
  const handler = source.slice(handlerStart, handlerEnd);
  assert.match(handler, /const emailDeliveryPolicy = invoiceEmailDeliveryPolicySummary\(invoiceAsyncDetail\)/);
  assert.equal((handler.match(/email_delivery_policy: emailDeliveryPolicy/g) || []).length, 2);
});
