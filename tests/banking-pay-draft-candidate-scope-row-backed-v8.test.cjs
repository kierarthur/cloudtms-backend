const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const ownerPath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_2313_banking_pay_draft_candidate_scope_row_backed_v8.sql'
);
const owner = fs.readFileSync(ownerPath, 'utf8');

test('candidate-scope owner is one bounded public page and one owner-only partition core', () => {
  assert.match(owner, /CREATE OR REPLACE FUNCTION private\.pay_workbench_prepare_draft_scope_from_certificate_partition_v8/);
  assert.match(owner, /CREATE OR REPLACE FUNCTION public\.pay_workbench_prepare_draft_scope_from_frozen_page_v8/);
  assert.equal((owner.match(/CREATE OR REPLACE FUNCTION/g) || []).length, 2);
  assert.match(owner, /p_limit integer DEFAULT 256/);
  assert.match(owner, /IF v_limit < 1 OR v_limit > 256/);
  assert.match(owner, /LIMIT \(v_limit \+ 1\)/);
});

test('scope membership is copied set-wise from certificate partitions and frozen refs', () => {
  assert.match(owner, /FROM private\.banking_pay_workbench_settled_certificate_partition_members_v8 AS member/);
  assert.match(owner, /JOIN private\.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref/);
  assert.match(owner, /INSERT INTO private\.banking_pay_draft_frozen_candidate_scope_members_v8/);
  assert.match(owner, /ORDER BY member\.member_ordinal/);
  assert.match(owner, /v_min_member_ordinal IS DISTINCT FROM 0/);
  assert.match(owner, /v_max_member_ordinal IS DISTINCT FROM v_partition\.constituent_count - 1/);
  assert.match(owner, /member\.stable_identity_digest_sha256 IS DISTINCT FROM entry\.constituent_digest_sha256/);
  assert.doesNotMatch(owner, /member\.stable_identity_digest_sha256 IS DISTINCT FROM entry\.source_identity_digest_sha256/);
  assert.match(owner, /v_identity_mismatch_count <> 0/);
  assert.match(owner, /v_candidate_channel_mismatch_count <> 0/);
});

test('legacy shell remains compact and the certified path never reconstructs selected arrays', () => {
  assert.match(owner, /INSERT INTO public\.banking_pay_operation_candidate_scope/);
  assert.match(owner, /'\[\]'::jsonb, '\[\]'::jsonb, '\[\]'::jsonb/);
  assert.match(owner, /selected_preview_row_ids_json <> '\[\]'::jsonb/);
  assert.match(owner, /selected_canonical_preview_lines_json <> '\[\]'::jsonb/);
  assert.doesNotMatch(owner, /jsonb_agg\s*\(/i);
  assert.doesNotMatch(owner, /array_agg\s*\(/i);
  assert.doesNotMatch(owner, /selected_preview_row_ids_json\s*=\s*EXCLUDED/i);
});

test('scope construction validates identity, amount and exact counts but computes no policy', () => {
  assert.match(owner, /entry\.candidate_id IS DISTINCT FROM v_partition\.candidate_id/);
  assert.match(owner, /entry\.resolved_pay_channel IS DISTINCT FROM v_partition\.resolved_pay_channel/);
  assert.match(owner, /v_amount_total IS DISTINCT FROM v_partition\.canonical_amount_ex_vat_total::numeric/);
  assert.match(owner, /DRAFT_SCOPE_MEMBER_GAP/);
  assert.match(owner, /DRAFT_SCOPE_DIGEST_MISMATCH/);
  assert.doesNotMatch(owner, /FROM (?:public|private)\.(?:finance_cases|payment_advances|banking_pay_operation_candidate_allocation_rows|pay_batch_items|pay_batch_item_breakdowns)/i);
  assert.doesNotMatch(owner, /pay_batch_apply_finance_adjustments|pay_batch_finalize_reservations_and_markers/i);
});

test('response-loss replay is exact and terminal completeness is independently counted', () => {
  assert.match(owner, /WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT/);
  assert.match(owner, /expected_previous_receipt_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256/);
  assert.match(owner, /'replayed', true/);
  assert.match(owner, /v_total_scope_count IS DISTINCT FROM v_scope\.partition_count/);
  assert.match(owner, /v_total_member_count IS DISTINCT FROM v_scope\.constituent_count/);
});

test('locks and privileges remain bounded', () => {
  assert.match(owner, /banking_pay_hot_path_budget_apply\('WORKBENCH_CHUNK'\)/);
  assert.doesNotMatch(owner, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout)/i);
  assert.doesNotMatch(owner, /FROM public\.candidates[^;]*FOR (?:UPDATE|SHARE)/is);
  assert.doesNotMatch(owner, /FROM public\.banking_pay_workbench_candidate_publications[^;]*FOR (?:UPDATE|SHARE)/is);
  assert.match(owner, /REVOKE ALL ON FUNCTION private\.pay_workbench_prepare_draft_scope_from_certificate_partition_v8\(uuid,uuid,integer,text\) FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.pay_workbench_prepare_draft_scope_from_frozen_page_v8\(uuid,integer,integer,text\) FROM PUBLIC, anon, authenticated/);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_prepare_draft_scope_from_frozen_page_v8\(uuid,integer,integer,text\) TO service_role/);
});

test('PostgreSQL conditional constructs are never schema-qualified', () => {
  assert.doesNotMatch(owner, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
