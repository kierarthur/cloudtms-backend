const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const canonicalPath = path.resolve(
  __dirname,
  '../supabase/repeatable/21072026_1820_00_import_review_internal_core.sql'
);
const source = fs.readFileSync(canonicalPath, 'utf8');
const hrPhase3Source = fs.readFileSync(
  path.resolve(
    __dirname,
    '../supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql'
  ),
  'utf8'
);
const nhspPhase3Source = fs.readFileSync(
  path.resolve(
    __dirname,
    '../supabase/repeatable/21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql'
  ),
  'utf8'
);

const extractFunction = (sql, name) => {
  const start = new RegExp(
    `create\\s+or\\s+replace\\s+function\\s+public\\.${name}\\s*\\(`,
    'i'
  ).exec(sql);
  assert.ok(start, `${name} must exist in canonical source`);
  const tail = sql.slice(start.index);
  const end = /\r?\n\$function\$;/.exec(tail);
  assert.ok(end, `${name} must have a complete function body`);
  return tail.slice(0, end.index + end[0].length);
};

const helper = extractFunction(
  source,
  '_import_review_effective_invoice_balance_core_v1'
);
const catalog = extractFunction(source, '_import_review_action_catalog_core_v1');
const applyEnvelope = extractFunction(source, '_import_review_apply_envelope_core_v1');

test('the production delta preserves the locked helper signature and bounds', () => {
  assert.match(helper, /p_import_id uuid,[\s\S]*p_source_items jsonb/);
  assert.match(helper, /p_max_sources integer default 100/);
  assert.match(helper, /p_max_invoice_lines_per_source integer default 512/);
  assert.match(helper, /p_max_audit_rows_per_source integer default 256/);
  assert.match(helper, /p_max_operations_per_source integer default 128/);
  assert.match(helper, /returns table\(source_identity text,balance_json jsonb\)/);
});

test('completed operation authority is validated before invoice scope is built', () => {
  const operationLoad = helper.indexOf('with matching_requests as');
  const historyScope = helper.indexOf('into v_hist_ids');
  const invoiceScope = helper.indexOf('with directly_scoped as');
  assert.ok(operationLoad > 0);
  assert.ok(operationLoad < historyScope);
  assert.ok(historyScope < invoiceScope);
  assert.match(helper, /operation_state='COMPLETE'/);
  assert.match(helper, /committed_at_utc is not null and t\.finalised_at_utc is not null/);
});

test('operation evidence requires one request, applied result, and policy unit', () => {
  assert.match(helper, /t\.request_count=1 and t\.applied_count=1 and t\.policy_count=1 and t\.outcome_count=1/);
  assert.match(helper, /request_unit->>'action_id'=t\.applied_unit->>'action_id'/);
  assert.match(helper, /request_unit->>'action_id'=t\.policy_unit->>'action_id'/);
  assert.match(helper, /reviewed_unit_fingerprint'=t\.request_unit->>'unit_fingerprint'/);
  assert.match(helper, /applied_unit->>'reconciliation_fingerprint'=t\.request_unit->>'reconciliation_fingerprint'/);
});

test('fresh and mutable generation repair modes are validated by route', () => {
  assert.match(helper, /when t\.request_unit->>'route'='CREATE_REVERSAL_REPLACEMENT'/);
  assert.match(helper, /coalesce\(t\.request_unit->>'repair_identity_mode',''\) in \('','CREATE_NEW_GENERATION'\)/);
  assert.match(helper, /t\.applied_unit->>'repair_identity_mode'='CREATE_NEW_GENERATION'/);
  assert.match(helper, /when t\.request_unit->>'route'='AMEND_EXISTING_REPLACEMENT'/);
  assert.match(helper, /'RETAIN_EXISTING_CORRECTION_ID','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'/);
  assert.match(helper, /t\.applied_unit->>'repair_identity_mode'=t\.request_unit->>'repair_identity_mode'/);
});

test('the real request and both phase-3 producers match the route-aware repair contract', () => {
  assert.match(
    applyEnvelope,
    /'repair_identity_mode',d\.summary_json->>'repair_identity_mode'/
  );
  for (const phase3 of [hrPhase3Source, nhspPhase3Source]) {
    assert.match(
      phase3,
      /'repair_identity_mode',coalesce\(v_repair_identity_mode,'CREATE_NEW_GENERATION'\)/
    );
  }
  assert.match(helper, /when t\.request_unit->>'route'='CREATE_REVERSAL_REPLACEMENT'/);
  assert.match(
    helper,
    /coalesce\(t\.request_unit->>'repair_identity_mode',''\) in \('','CREATE_NEW_GENERATION'\)/
  );
  assert.match(helper, /t\.applied_unit->>'repair_identity_mode'='CREATE_NEW_GENERATION'/);
});

test('completed operation authority re-attests source scope and the immutable request unit', () => {
  assert.match(helper, /request_unit->>'invoice_stream'=v_invoice_stream/);
  assert.match(helper, /request_unit->>'source_scope_fingerprint'=v_scope_fingerprint/);
  assert.match(helper, /action_outcome->>'evidence_fingerprint'/);
  assert.match(helper, /concat_ws\('\|','unit-v2'/);
  assert.match(helper, /request_unit->>'unit_fingerprint'=encode\(digest/);
});

test('operation and applied-result fingerprints are independently re-attested', () => {
  assert.match(helper, /operation_contract-'operation_contract_fingerprint'(?:::\s*text)?/);
  assert.match(helper, /policy_envelope'\)-'envelope_fingerprint'(?:::\s*text)?/);
  assert.match(helper, /applied_result_fingerprint'=encode\(digest/);
  assert.match(helper, /'M_active_member_ids',t\.applied_unit->'applied_member_ids'/);
  assert.match(helper, /'parent_timesheet_id',\(t\.applied_unit->>'parent_timesheet_id'\)::uuid/);
});

test('all valid completed generations are accumulated rather than newest-only selected', () => {
  assert.match(helper, /jsonb_agg\(jsonb_build_object\([\s\S]*filter\(where e\.valid_historical_authority\)/);
  assert.match(helper, /order by e\.finalised_at_utc,e\.operation_id,e\.request_unit->>'action_id'/);
  assert.doesNotMatch(
    helper.slice(helper.indexOf('with matching_requests as'), helper.indexOf('into v_hist_ids')),
    /limit\s+1/i
  );
});

test('validated applied member IDs enter historical invoice scope', () => {
  assert.match(helper, /into v_operation_member_ids/);
  assert.match(helper, /union all select unnest\(v_operation_member_ids\)/);
  assert.match(helper, /v_audit_ids\|\|v_operation_member_ids/);
  assert.match(helper, /historical_missing_timesheet_ids',to_jsonb\(v_missing_ids\)/);
});

test('canonical member ownership accepts only a proven archived-role re-key lineage', () => {
  assert.match(helper, /v_member_supersession_map/);
  assert.match(helper, /FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED/);
  assert.match(helper, /historical_line\.created_at<=coalesce/);
  assert.match(helper, /join public\.invoices historical_invoice/);
  assert.match(helper, /historical_invoice\.status::text in \('ISSUED','PAID','ON_HOLD'\)/);
  assert.match(helper, /EXPENSE\(_\.\*\)\?\|MILEAGE\|TRAVEL\|ACCOMMODATION/);
  assert.match(helper, /superseded_correction_id/);
  assert.match(helper, /canonical_correction_id/);
  assert.match(helper, /left_evidence\.correction_kind<>right_evidence\.correction_kind/);
  assert.match(helper, /into v_member_role_map,v_member_role_conflict/);
});

test('archived-member supersession is canonicalised transitively and fails closed on graph conflicts', () => {
  assert.match(helper, /with recursive direct_edges as/);
  assert.match(helper, /walk\.path\|\|edge\.canonical_correction_id/);
  assert.match(helper, /'supersession_depth',entry\.depth/);
  assert.match(helper, /'supersession_path',to_jsonb\(entry\.path\)/);
  assert.match(helper, /v_member_supersession_conflict/);
  assert.match(helper, /count\(distinct edge\.canonical_correction_id\)<>1/);
  assert.match(helper, /exists\(select 1 from walk where cycle\)/);
});

test('deleted operation-proven members can scope exact HOURS_WEEKLY lines', () => {
  assert.match(helper, /v_operation_member_scope_proven and v_line_type='HOURS_WEEKLY'/);
  assert.match(helper, /v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY'/);
  assert.match(helper, /elsif v_single_source or v_operation_member_scope_proven then/);
});

test('one canonical economic ledger drives B and correction role state', () => {
  assert.match(helper, /'economic_state','EFFECTIVE'/);
  assert.match(helper, /'economic_state','PENDING'/);
  assert.match(helper, /from jsonb_array_elements\(v_line_evidence\) component/);
  const roleSection = helper.slice(
    helper.indexOf('with correction_seed as'),
    helper.indexOf('v_partial_invoice_state:=')
  );
  assert.doesNotMatch(roleSection, /join public\.invoice_lines/);
});

test('role economics are deduplicated by physical invoice-line scope', () => {
  assert.match(helper, /select coalesce\(array_agg\(distinct x order by x\).*v_effective_line_ids/);
  assert.match(helper, /'invoice_line_id',v_line\.id/);
  assert.doesNotMatch(helper, /dedup.*operation_id/i);
});

test('separable non-hours components remain excluded from B and role state', () => {
  assert.match(helper, /EXPENSE\(_\.\*\)\?\|MILEAGE\|TRAVEL\|ACCOMMODATION\|REIMBURSEMENT\|ADDITION/);
  assert.match(helper, /v_ignored_nonhours_line_ids:=array_append/);
  assert.match(helper, /continue;/);
});

test('credits require a full mirror and allocate multi-source money by exact frozen segment', () => {
  assert.match(helper, /round\(coalesce\(v_line\.total_pay_ex_vat,0\),2\)<>-round/);
  assert.match(helper, /v_original_seg_count>1/);
  assert.match(helper, /v_component_pay:=-coalesce\(\(v_original_seg->>'pay_amount'\)::numeric,0\)/);
  assert.match(helper, /v_component_charge:=-coalesce\(\(v_original_seg->>'charge_amount'\)::numeric,0\)/);
  assert.match(helper, /v_component_margin:=v_component_charge-v_component_pay/);
});

test('credits require exact header, line, client, and source provenance', () => {
  assert.match(helper, /v_line\.original_invoice_id is distinct from v_original_line\.invoice_id/);
  assert.match(helper, /original_invoice_line_id' is distinct from v_original_line\.id::text/);
  assert.match(helper, /v_line\.invoice_client_id is distinct from v_original_invoice\.client_id/);
  assert.match(helper, /credit_member->>'source_identity'=v_source_identity/);
  assert.match(helper, /original_member->>'source_identity'=v_source_identity/);
});

test('archived-only generations cannot become mutable', () => {
  assert.match(helper, /g\.archived_only_role_count=2 then 'ARCHIVED_AUDIT_ONLY'/);
  assert.match(helper, /g\.active_role_count>0 or g\.missing_operation_role_count=2/);
  assert.doesNotMatch(
    helper.slice(helper.indexOf("then 'ARCHIVED_AUDIT_ONLY'"), helper.indexOf('v_partial_invoice_state:=')),
    /archived_only_role_count=2[\s\S]*then 'MUTABLE'/
  );
});

test('missing mutable roles require durable completed-operation proof', () => {
  assert.match(helper, /operation_proven_missing_member_ids/);
  assert.match(helper, /and coalesce\(\(member->>'operation_proven'\)::boolean,false\)/);
  assert.match(helper, /missing_operation_role_count=2/);
});

test('multiple effective physical members for one role fail closed', () => {
  assert.match(helper, /economic_member_duplicate/);
  assert.match(helper, /DUPLICATE_EFFECTIVE_ROLE_WITHOUT_REPAIR_LINEAGE/);
  assert.match(helper, /g\.active_duplicate or g\.economic_member_duplicate/);
});

test('latest fully invoiced operation may reconstruct a deleted schedule only on exact bucket equality', () => {
  assert.match(helper, /v_fully_invoiced_generation_ids\[cardinality\(v_fully_invoiced_generation_ids\)\]/);
  for (const bucket of ['hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh']) {
    assert.match(helper, new RegExp(`A_hours,${bucket}`));
  }
  assert.match(helper, /jsonb_array_length\(unit->'A_schedule_json'\)=1/);
});

test('effective-zero source safety distinguishes full credit from paid-uninvoiced', () => {
  assert.match(helper, /v_effective_zero:=v_b_hours_zero and v_b_money_zero/);
  assert.match(helper, /v_effective_component_count>0/);
  assert.match(helper, /IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE/);
  assert.match(helper, /CURRENT_SOURCE_PAID_AND_INVOICE_LINED/);
  assert.match(helper, /CURRENT_SOURCE_INVOICE_LINED_AFTER_EFFECTIVE_ZERO/);
});

test('safe current source requires one ordinary current row and a safe lifecycle', () => {
  assert.match(helper, /v_current_source_count=1/);
  assert.match(helper, /not v_current_source_invoice_lined/);
  assert.match(helper, /not v_current_source_paid/);
  assert.match(helper, /v_current_source_unlocked and v_current_source_fresh and v_current_source_segment_unlocked/);
  assert.match(helper, /v_current_source_contract_week_safe and v_current_source_invoice_operation_clear/);
});

test('source safety participates in review/apply staleness fingerprinting', () => {
  assert.match(helper, /'reconciliation-v3'/);
  assert.match(helper, /v_current_source_safe,v_current_source_safety_reason/);
  assert.match(helper, /v_current_source_invoice_lined,v_current_source_paid/);
  assert.match(helper, /current_source_safe_for_effective_zero_amendment/);
  assert.match(helper, /effective_zero_source_safety_reason/);
});


test('historical operations survive latest-import replacement through decision outcomes', () => {
  assert.match(helper, /join public\.import_review_action_outcomes outcome on outcome\.action_id=decision\.action_id/);
  assert.match(helper, /decision\.shift_id=v_source_shift_id and outcome\.shift_id=v_source_shift_id/);
  assert.match(helper, /where op\.id=any\(v_operation_ids\)/);
  assert.match(helper, /v_operation_count:=cardinality\(v_operation_ids\)/);
});

test('zero hours with a non-zero frozen monetary balance blocks as non-standard', () => {
  assert.match(helper, /v_b_hours_zero:=v_b_day=0/);
  assert.match(helper, /v_b_money_zero:=round\(v_b_pay,2\)=0/);
  assert.match(helper, /when v_b_hours_zero and not v_b_money_zero then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'/);
  assert.match(helper, /'effective_money_net_is_zero',v_b_money_zero/);
});
test('existing catalog blocker precedence and route order remain unchanged', () => {
  const blocker = catalog.indexOf("when nullif(m.reconciliation_balance->>'blocking_code','') is not null");
  const mutable = catalog.indexOf("then 'AMEND_EXISTING_REPLACEMENT'");
  const reversal = catalog.indexOf("then 'CREATE_REVERSAL_REPLACEMENT'");
  const paid = catalog.indexOf("then 'AMEND_PAID_UNINVOICED_SOURCE'");
  const ordinary = catalog.indexOf("then 'AMEND_SOURCE'");
  assert.ok(blocker > 0);
  assert.ok(blocker < mutable && mutable < reversal && reversal < paid && paid < ordinary);
});

test('helper remains read-only and contains no Banking Pay or invoice mutation path', () => {
  assert.doesNotMatch(helper, /\b(insert\s+into|update|delete\s+from|merge\s+into)\b/i);
  assert.doesNotMatch(helper, /pay_workbench|pay_batch|provider|settlement|remittance/i);
  assert.doesNotMatch(helper, /invoice_create|invoice_issue|invoice_unissue|credit_note_create/i);
});
