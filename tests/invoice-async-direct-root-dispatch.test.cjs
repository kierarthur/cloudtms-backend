const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const generation = fs.readFileSync(path.resolve(
  __dirname,
  '../supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_advance_batch.sql'
), 'utf8');

const issue = fs.readFileSync(path.resolve(
  __dirname,
  '../supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_issue_advance_batch.sql'
), 'utf8');

test('generation dispatcher preserves direct non-batch operations and gates batch members', () => {
  assert.match(generation, /o\.entity_type is distinct from 'INVOICE_BATCH'\s+or\s+\(\s+c\.is_manifest_member/s);
  assert.match(generation, /private\._invoice_generation_advance_core_v8\(/);
  assert.match(generation, /MANIFEST_CLAIM_NOT_RELEASED/);
});

test('issue dispatcher preserves direct non-batch operations and gates batch members', () => {
  assert.match(issue, /o\.entity_type is distinct from 'INVOICE_BATCH'\s+or\s+\(\s+c\.is_manifest_member/s);
  assert.match(issue, /private\._invoice_issue_advance_core_v8\(/);
  assert.match(issue, /MANIFEST_CLAIM_NOT_RELEASED/);
});
