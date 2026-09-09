const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const digest = (source) => crypto.createHash('sha256').update(source).digest('hex');

const historicalCancel = read('supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
const historicalPublisher = read('supabase/repeatable/07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const cancelOwner = read('supabase/repeatable/08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql');
const publisherOwner = read('supabase/repeatable/08092026_1201_banking_pay_certified_preview_final_selection_count_v1.sql');
const indexMigration = read('supabase/migrations/08092026_1159_banking_pay_cancel_return_frozen_scope_lookup_v1.sql');
const verifier = read('supabase/verification/08092026_1203_banking_pay_cancel_return_selection_intent_verification.sql');
const generator = read('scripts/generate-banking-pay-cancel-return-selection-intent-v1.mjs');

function assertCancelContract(source) {
  assert.match(source, /CREATE OR REPLACE FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1\(/);
  assert.match(source, /banking_pay_draft_frozen_candidate_scopes_v8/);
  assert.match(source, /banking_pay_draft_frozen_constituent_payloads_v8/);
  assert.match(source, /_pay_workbench_preview_selection_key_v1/);
  assert.match(source, /false,\s*'UNSELECTED',\s*-1000,\s*'POST_CANCEL_RETURN_UNSELECTED',\s*'PENDING'/);
  assert.match(source, /ON CONFLICT \(target_session_id, source_preview_row_id\) DO NOTHING/);
  assert.match(source, /PAYMENT_CANCEL_SELECTION_INTENT_IDENTITY_INCOMPLETE/);
  assert.match(source, /PAYMENT_CANCEL_SELECTION_INTENT_REGISTRATION_CONFLICT/);
  assert.match(source, /PAYMENT_CANCEL_CURRENT_SELECTION_INTENT_NOT_APPLIED/);
  assert.match(source, /status IN \('PENDING', 'APPLIED'\)/);
  assert.match(source, /'policy_x_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'/);
}

function assertPublisherContract(source) {
  assert.match(source, /CREATE OR REPLACE FUNCTION private\.pay_workbench_publish_certified_source_preview_v1\(/);
  assert.match(source, /pending_selection_intent/);
  assert.match(source, /pending_selection_intent\.status = 'PENDING'/);
  assert.match(source, /v_selected_count := pg_catalog\.jsonb_array_length\(v_selected_ids\)/);
  const selectedIds = source.indexOf('INTO v_selected_ids');
  const finalCount = source.indexOf('v_selected_count := pg_catalog.jsonb_array_length(v_selected_ids)', selectedIds);
  const attestation = source.indexOf("'selected_row_count', v_selected_count", finalCount);
  assert.ok(selectedIds > 0 && finalCount > selectedIds && attestation > finalCount);
}

test('historical owners remain byte-identical and the generator has exact inputs', () => {
  assert.equal(digest(historicalCancel), 'f383a98b37b90b4d26c43e794618aae77e464d07b1309451ab69a881a4e76152');
  assert.equal(digest(historicalPublisher), '7e640e8b03db73fa416bbf02585906e450d2c7828aa700ca635ede4d18ddfec0');
  assert.match(generator, /19072026_1816_cancel_refresh_supersede_finance_dirty\.sql/);
  assert.match(generator, /07082026_2154_pay_workbench_publish_certified_source_preview_v1\.sql/);
  assert.match(generator, /replaceExactlyOnce/);
});

test('cancel owner carries only frozen selection identity and changes no economics', () => {
  assertCancelContract(cancelOwner);
  assert.doesNotMatch(cancelOwner, /UPDATE public\.pay_batch_items/);
  assert.doesNotMatch(cancelOwner, /UPDATE public\.pay_batch_candidates/);
  assert.doesNotMatch(cancelOwner, /UPDATE public\.pay_batches/);
  assert.doesNotMatch(cancelOwner, /UPDATE public\.pay_advance_reservations/);
  assert.doesNotMatch(cancelOwner, /INSERT INTO public\.pay_bank_transfers/);
  assert.doesNotMatch(cancelOwner, /statement_timeout|lock_timeout/);
});

test('cancel identity comes from the durable frozen payload rather than retained preview history', () => {
  const identityStart = cancelOwner.indexOf('CREATE TEMPORARY TABLE pg_temp._bpay_cancelled_selection_intents');
  const identityEnd = cancelOwner.indexOf('SELECT COALESCE(pg_catalog.sum(frozen_scope.constituent_count)', identityStart);
  const identity = cancelOwner.slice(identityStart, identityEnd);
  assert.match(identity, /frozen_payload\.payload_json->>'section'/);
  assert.match(identity, /frozen_payload\.payload_json->>'key_type'/);
  assert.match(identity, /frozen_payload\.payload_json->>'key_value'/);
  assert.doesNotMatch(identity, /JOIN public\.banking_pay_workbench_preview_rows/);
});

test('publisher cannot bypass pending carry and attests the actual final selection count', () => {
  assertPublisherContract(publisherOwner);
  assert.doesNotMatch(publisherOwner, /UPDATE public\.pay_batch_items/);
  assert.doesNotMatch(publisherOwner, /UPDATE public\.pay_batches/);
  assert.doesNotMatch(publisherOwner, /statement_timeout|lock_timeout/);
});

test('lookup is indexed by exact batch and Candidate without changing stored facts', () => {
  assert.match(indexMigration, /CREATE INDEX CONCURRENTLY banking_pay_draft_frozen_scopes_v8_batch_candidate_idx/);
  assert.match(indexMigration, /pay_batch_id,[\s\S]*candidate_id,[\s\S]*resolved_pay_channel,[\s\S]*operation_id/);
  assert.doesNotMatch(indexMigration, /INSERT INTO|UPDATE |DELETE FROM/);
  assert.match(verifier, /BANKING_PAY_CANCEL_RETURN_SCOPE_INDEX_MISSING/);
  assert.equal(
    (verifier.match(/pg_catalog\.pg_get_userbyid\(procedure_row\.proowner\) = current_user/g) || []).length,
    2,
    'both installed owners must bind to the actual release owner on PostgreSQL and Miget',
  );
  assert.doesNotMatch(verifier, /pg_catalog\.pg_get_userbyid\(procedure_row\.proowner\) = 'postgres'/);
});

test('every critical boundary is killed by a focused mutation', () => {
  const mutations = [
    ['carry reason', cancelOwner, cancelOwner.replace(/'POST_CANCEL_RETURN_UNSELECTED',\r?\n\s*'PENDING'/, "'REMOVED',\n      'PENDING'"), assertCancelContract],
    ['identity completeness', cancelOwner, cancelOwner.replaceAll('PAYMENT_CANCEL_SELECTION_INTENT_IDENTITY_INCOMPLETE', 'REMOVED'), assertCancelContract],
    ['registration conflict', cancelOwner, cancelOwner.replaceAll('PAYMENT_CANCEL_SELECTION_INTENT_REGISTRATION_CONFLICT', 'REMOVED'), assertCancelContract],
    ['direct apply', cancelOwner, cancelOwner.replaceAll('PAYMENT_CANCEL_CURRENT_SELECTION_INTENT_NOT_APPLIED', 'REMOVED'), assertCancelContract],
    ['pending carry', publisherOwner, publisherOwner.replace("pending_selection_intent.status = 'PENDING'", "pending_selection_intent.status = 'REMOVED'"), assertPublisherContract],
    ['final count', publisherOwner, publisherOwner.replace('v_selected_count := pg_catalog.jsonb_array_length(v_selected_ids)', 'v_selected_count := 0'), assertPublisherContract]
  ];
  for (const [label, original, source, assertion] of mutations) {
    assert.notEqual(source, original, `${label} mutation did not apply`);
    assert.throws(() => assertion(source), `${label} mutation survived`);
  }
});
