const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const owner = read('supabase/repeatable/02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql');
const verifier = read('supabase/verification/02092026_1041_banking_pay_draft_insert_items_finance_handoff_verification.sql');
const runtime = read('tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql');
const scopeOwner = read('supabase/repeatable/02092026_2313_banking_pay_draft_candidate_scope_row_backed_v8.sql');
const orchestrator = read('supabase/repeatable/02092026_2330_banking_pay_draft_bounded_advance_v8.sql');
const payeNetOwner = read('supabase/repeatable/21072026_1235_48_pay_set_paye_net_manual.sql');

const deferredVisibleAliases = [
  'OVERPAYMENT_RECOVERY',
  'PAYMENT_ADVANCE_REPAYMENT',
  'LOAN_PAYOUT',
  'UNDERPAYMENT_PAYMENT',
  'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
];
const identityVisibleAliases = [
  'OVERPAYMENT_RECOVERY',
  'MANUAL_DEBT_RECOVERY',
  'PAYMENT_ADVANCE_REPAYMENT',
  'LOAN_PAYOUT',
  'UNDERPAYMENT_PAYMENT',
  'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
];

function declaredAliases(source, declarationName) {
  const match = source.match(new RegExp(`${declarationName} constant text\\[\\] := ARRAY\\[([\\s\\S]*?)\\]::text\\[\\];`));
  assert.ok(match, `${declarationName} declaration is missing`);
  return [...match[1].matchAll(/'([A-Z0-9_]+)'/g)].map((entry) => entry[1]);
}

test('V8 scope is row-backed and never calls the legacy globally capped scope routine', () => {
  assert.match(scopeOwner, /pay_workbench_prepare_draft_scope_from_certificate_partition_v8/);
  assert.match(scopeOwner, /pay_workbench_prepare_draft_scope_from_frozen_page_v8/);
  assert.match(scopeOwner, /banking_pay_workbench_settled_certificate_partition_members_v8/);
  assert.match(scopeOwner, /banking_pay_draft_frozen_candidate_scope_members_v8/);
  assert.doesNotMatch(scopeOwner, /pay_workbench_prepare_draft_scope_seed\s*\(/);
  assert.doesNotMatch(scopeOwner, /jsonb_agg\s*\(|array_agg\s*\(/i);
});

test('INSERT_ITEMS replacement is one exact owner with unchanged callable metadata', () => {
  assert.equal((owner.match(/CREATE OR REPLACE FUNCTION/g) || []).length, 1);
  assert.match(owner, /CREATE OR REPLACE FUNCTION public\.pay_batch_insert_items_from_preview\(\s*p_pay_batch_id uuid,\s*p_actor_user_id uuid DEFAULT NULL::uuid,\s*p_operation_id uuid DEFAULT NULL::uuid,\s*p_candidate_scope_ids jsonb DEFAULT NULL::jsonb\s*\)/s);
  assert.match(owner, /LANGUAGE plpgsql\s+SECURITY DEFINER\s+SET search_path TO 'public'/s);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.pay_batch_insert_items_from_preview\(uuid, uuid, uuid, jsonb\)\s+FROM PUBLIC, anon, authenticated/s);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.pay_batch_insert_items_from_preview\(uuid, uuid, uuid, jsonb\)\s+TO service_role/s);
});

test('only the five later-finance-owned visible aliases enter the certified defer handoff', () => {
  assert.deepEqual(declaredAliases(owner, 'v_certified_finance_identity_aliases'), identityVisibleAliases);
  assert.deepEqual(declaredAliases(owner, 'v_deferred_finance_aliases'), deferredVisibleAliases);
  assert.doesNotMatch(owner.match(/v_deferred_finance_aliases constant text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/)[1], /MANUAL_DEBT_RECOVERY|LOAN_REPAYMENT|MANUAL_CREDIT_PAYOUT/);
});

test('manual debt preserves the existing pre-Worksheet frozen-item lifecycle', () => {
  assert.match(owner, /MANUAL_DEBT_RECOVERY remains on\s+-- the established INSERT_ITEMS/);
  assert.match(owner, /finance_row\.visible_alias = 'MANUAL_DEBT_RECOVERY'[\s\S]*?finance_row\.certification_ok/);
  assert.match(owner, /WHEN UPPER\(COALESCE\(prepared_rows\.line_json->>'line_type',[\s\S]*?\) = 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_RECOVERY'/);
  assert.match(owner, /allocation_row\.allocation_basis_json->'finance_component'/);
  assert.match(owner, /finance_component_json->>'classification'/);
  assert.match(owner, /finance_component_json->>'source_pay_method'/);
  assert.match(owner, /WHEN UPPER\(COALESCE\(prepared_rows\.line_json->>'line_type',[\s\S]*?THEN UPPER\(NULLIF\(BTRIM\(COALESCE\(prepared_rows\.line_json->>'paye_treatment'/);
  assert.match(owner, /DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_INVALID/);
  assert.match(owner, /component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public\.pay_finance_component_classification_enum/);
  assert.match(owner, /normalised_source_rows\.item_type <> 'MANUAL_DEBT_RECOVERY'[\s\S]*?normalised_source_rows\.component_classification = 'TAXABLE_CHANNEL_SENSITIVE'/);
  assert.match(owner, /normalised_rows\.item_type = 'MANUAL_DEBT_RECOVERY' THEN normalised_rows\.finance_component_json/);
  assert.match(owner, /normalised_rows\.item_type = 'MANUAL_DEBT_RECOVERY'[\s\S]*?finance_component_json->'source_basis_json'/);
  assert.match(runtime, /pay_batch_paye_net_inputs/);
  assert.match(runtime, /expected_paye_treatment/);
  assert.match(runtime, /expected_amount_vat/);
  assert.match(runtime, /payout_instruction_snapshot_json->>'taxability'/);
  assert.match(runtime, /payout_instruction_snapshot_json->>'pay_channel'/);
  assert.match(payeNetOwner, /PAYE net is produced after gross-side deductions have already been sent[\s\S]*?Reproject only net-side deductions here/);
  assert.match(payeNetOwner, /pbi_md_del\.item_type = 'MANUAL_DEBT_RECOVERY'[\s\S]*?coalesce\(pbi_md_del\.paye_treatment, 'NET_DEDUCT'\)/);
  assert.match(payeNetOwner, /released_reason = 'PAYE_NET_REPROJECTION'/);
});

test('visible and frozen vocabularies retain their deliberate policy translations', () => {
  assert.match(owner, /WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'/);
  assert.match(owner, /WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_PAYOUT'/);
  assert.match(runtime, /H2_F013_HIDDEN_LOAN_REPAYMENT_VISIBLE_INPUT_ACCEPTED/);
  assert.match(runtime, /visible_alias in \('LOAN_REPAYMENT','MANUAL_CREDIT_PAYOUT'\)/);
});

test('the handoff validates producer-owned finance identity without inventing an economic key shape', () => {
  assert.match(owner, /allocation_row\.finance_case_id IS NOT NULL/);
  assert.match(owner, /allocation_row\.finance_component_id IS NOT NULL/);
  assert.match(owner, /allocation_row\.source_ref = 'advance:' \|\| allocation_row\.finance_case_id::text/);
  assert.match(owner, /\{line,finance_case_id\}/);
  assert.match(owner, /\{line,finance_component_id\}/);
  assert.match(owner, /\{finance_component,finance_component_id\}/);
  assert.match(owner, /\{line,amount_ex_vat\}/);
  assert.match(owner, /\{line,case_components\}/);
  assert.doesNotMatch(owner, /finance.*key_type.*(?:CASE_TOTAL|TOTAL)/i);
  assert.match(owner, /DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID/);
});

test('the existing finance owner remains the sole materialisation and economic authority', () => {
  assert.doesNotMatch(owner, /pay_batch_apply_finance_adjustments\s*\(/);
  assert.doesNotMatch(owner, /pay_batch_finalize_reservations_and_markers\s*\(/);
  assert.doesNotMatch(owner, /UPDATE\s+public\.finance_cases/i);
  assert.doesNotMatch(owner, /UPDATE\s+public\.pay_advance/i);
  assert.match(orchestrator, /WHEN 'INSERT_ITEMS' THEN public\.pay_batch_insert_items_from_preview/);
  assert.match(orchestrator, /WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN public\.pay_batch_apply_finance_adjustments/);
});

test('row-backed V8 scope feeds INSERT_ITEMS without rebuilding selected constituent arrays', () => {
  assert.equal((owner.match(/private\.pay_workbench_draft_scope_line_rows_v8\s*\(/g) || []).length, 2);
  assert.doesNotMatch(owner, /pay_workbench_prepare_draft_scope_seed\s*\(/);
  assert.doesNotMatch(owner, /selected_preview_row_ids_json\s*->/i);
  assert.doesNotMatch(owner, /jsonb_agg\s*\([^)]*selected_(?:preview|canonical)/i);
});

test('100 is an owner-call page size, not a global Draft selection ceiling', () => {
  assert.equal((owner.match(/LIMIT 100/g) || []).length, 1);
  assert.match(owner, /'has_more', EXISTS \(/);
  assert.match(owner, /remaining_row\.candidate_scope_id IN/);
  assert.match(owner, /NOT \(UPPER\(BTRIM\(COALESCE\(remaining_row\.allocation_type, ''\)\)\) = ANY\(v_deferred_finance_aliases\)\)/);
  assert.match(orchestrator, /WHEN v_phase IN \('SEED_ALLOCATION_ROWS','APPLY_FINANCE_ADJUSTMENTS'\) THEN 50\s+WHEN v_phase = 'FINALISE_RESERVATIONS' THEN 1\s+ELSE 100/s);
  assert.doesNotMatch(scopeOwner, /selected_constituent_count\s*>\s*100|jsonb_array_length\([^)]*\)\s*>\s*100/i);
});

test('response-loss replay accepts only exact later-finance links and rejects wrong links', () => {
  assert.match(owner, /DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH/);
  assert.match(owner, /existing_item\.operation_source_key = finance_row\.operation_source_key/);
  assert.match(owner, /existing_item\.item_type = finance_row\.expected_item_type/);
  assert.match(owner, /existing_item\.finance_case_id IS NOT DISTINCT FROM finance_row\.finance_case_id/);
  assert.match(owner, /existing_item\.finance_component_id IS NOT DISTINCT FROM finance_row\.finance_component_id/);
  assert.match(owner, /existing_item\.source_ref IS NOT DISTINCT FROM finance_row\.source_ref/);
  assert.match(owner, /ROUND\(COALESCE\(existing_item\.amount_ex_vat, 0\), 2\) = finance_row\.allocated_amount/);
  assert.match(runtime, /H2_F013_EXACT_FINANCE_LINK_RESPONSE_LOSS_REPLAY_CHANGED/);
  assert.match(runtime, /H2_F013_WRONG_ORDINARY_FINANCE_LINK_ACCEPTED/);
});

test('verifier fail-closes alias, policy, metadata and ACL drift', () => {
  assert.match(verifier, /BANKING_PAY_DRAFT_FINANCE_HANDOFF_VISIBLE_ALIAS_CARDINALITY_CHANGED/);
  assert.match(verifier, /BANKING_PAY_DRAFT_FINANCE_HANDOFF_NON_DEFERRED_ALIAS_ACCEPTED/);
  assert.match(verifier, /BANKING_PAY_DRAFT_FINANCE_HANDOFF_HIDDEN_ALIAS_ACCEPTED/);
  assert.match(verifier, /BANKING_PAY_DRAFT_FINANCE_HANDOFF_DEFINITION_DRIFT/);
  assert.match(verifier, /BANKING_PAY_DRAFT_FINANCE_HANDOFF_METADATA_OR_ACL_DRIFT/);
  assert.match(verifier, /routine_owner<>current_user/);
  assert.doesNotMatch(verifier, /routine_owner<>'postgres'/);
  assert.match(verifier, /has_function_privilege\('anon'/);
  assert.match(verifier, /has_function_privilege\('authenticated'/);
  assert.match(verifier, /has_function_privilege\('service_role'/);
});

test('bounded mutations cannot silently widen policy or bypass certified evidence', () => {
  const guard = (source) => {
    try {
      return JSON.stringify(declaredAliases(source, 'v_certified_finance_identity_aliases')) === JSON.stringify(identityVisibleAliases)
        && JSON.stringify(declaredAliases(source, 'v_deferred_finance_aliases')) === JSON.stringify(deferredVisibleAliases)
        && !declaredAliases(source, 'v_deferred_finance_aliases').includes('MANUAL_DEBT_RECOVERY')
        && /MANUAL_DEBT_RECOVERY remains on\s+-- the established INSERT_ITEMS/.test(source)
        && /finance_row\.visible_alias = 'MANUAL_DEBT_RECOVERY'[\s\S]*?finance_row\.certification_ok/.test(source)
        && source.includes("WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'")
        && source.includes("WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_PAYOUT'")
        && source.includes('DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID')
        && source.includes('DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_INVALID')
        && source.includes('DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH')
        && source.includes("allocation_row.allocation_basis_json->'finance_component'")
        && source.includes("finance_component_json->>'classification'")
        && /normalised_source_rows\.item_type <> 'MANUAL_DEBT_RECOVERY'[\s\S]*?normalised_source_rows\.component_classification = 'TAXABLE_CHANNEL_SENSITIVE'/.test(source)
        && source.includes("allocation_row.source_ref = 'advance:' || allocation_row.finance_case_id::text")
        && (source.match(/private\.pay_workbench_draft_scope_line_rows_v8\s*\(/g) || []).length === 2
        && !/pay_batch_apply_finance_adjustments\s*\(/.test(source);
    } catch {
      return false;
    }
  };
  const mutations = [
    owner.replace("    'OVERPAYMENT_RECOVERY',\n", ''),
    owner.replace("  v_deferred_finance_aliases constant text[] := ARRAY[\n", "  v_deferred_finance_aliases constant text[] := ARRAY[\n    'MANUAL_DEBT_RECOVERY',\n"),
    owner.replace("    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'\n", "    'LOAN_REPAYMENT'\n"),
    owner.replaceAll("finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'", 'FALSE'),
    owner.replace("WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'", "WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'PAYMENT_ADVANCE_REPAYMENT'"),
    owner.replace("WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_PAYOUT'", "WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'"),
    owner.replaceAll('DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID', 'DRAFT_FINANCE_HANDOFF_ACCEPTED_WITHOUT_EVIDENCE'),
    owner.replaceAll('DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_INVALID', 'DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_ACCEPTED_WITHOUT_EVIDENCE'),
    owner.replaceAll('DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH', 'DRAFT_FINANCE_PRELINK_REPAIRED'),
    owner.replaceAll("allocation_row.allocation_basis_json->'finance_component'", "'{}'::jsonb"),
    owner.replaceAll("finance_component_json->>'classification'", "line_json->>'classification'"),
    owner.replace("            normalised_source_rows.item_type <> 'MANUAL_DEBT_RECOVERY'", "            TRUE"),
    owner.replace("allocation_row.source_ref = 'advance:' || allocation_row.finance_case_id::text", 'TRUE'),
    owner.replace(/private\.pay_workbench_draft_scope_line_rows_v8\s*\(/, 'jsonb_array_elements(')
  ];
  assert.equal(guard(owner), true);
  assert.equal(mutations.length, 14);
  mutations.forEach((mutation, index) => assert.equal(guard(mutation), false, `mutation ${index + 1} survived`));
});

test('PostgreSQL conditionals remain unqualified and no timeout is relaxed', () => {
  assert.doesNotMatch(owner, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(owner, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.match(owner, /banking_pay_hot_path_budget_apply\('WORKBENCH_CHUNK'\)/);
});
