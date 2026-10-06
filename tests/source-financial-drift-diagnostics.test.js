import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read=name=>fs.readFileSync(new URL('../'+name,import.meta.url),'utf8');
const root=read('supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql');
const helper=read('supabase/verification/support/06102026_1716_source_financial_drift_diagnostics.sql');
const controls=read('supabase/verification/support/06102026_1723_source_financial_drift_diagnostic_controls.sql');

test('financial drift details preserve full-row fail-closed guards and original worker bound',()=>{
  assert.match(root,/for v_call in 1\.\.3 loop/);
  assert.match(root,/if v_after is distinct from v_before then\s+raise exception using errcode='P0001',message='PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT',[\s\S]*?detail=pg_temp\.ws_verify_financial_drift_detail\(v_before,v_after,v_call\);/);
  assert.match(root,/pg_temp\.ws_verify_financial_drift_capture\(v_relation::regclass,'BEFORE'\)\s+is distinct from v_hash/);
  assert.match(root,/PAID_FIXTURE_ECONOMIC_BASELINE_CHANGED_DURING_CAPTURE/);
  assert.match(root,/06102026_1723_source_financial_drift_diagnostic_controls\.sql/);
});
test('diagnostics are temporary digest-only evidence, with no Banking/runtime replacement',()=>{
  assert.match(helper,/on commit drop/);
  assert.doesNotMatch(helper,/create(?: or replace)? function (?:public|private)\./i);
  assert.doesNotMatch(helper,/grant|notify pgrst|disable trigger|statement_timeout/i);
  for(const field of ['added_rows','removed_rows','changed_rows','changed_fields_row_counts','before_matches_guard','after_matches_guard']) assert.ok(helper.includes("'"+field+"'"));
  assert.match(helper,/extensions\.digest\(j::text,'sha256'\)/);
  assert.match(helper,/extensions\.digest\(f\.value::text,'sha256'\)/);
});
test('runtime controls cover exact full hashes, counts, privacy, composite NULL swaps and snapshot mismatches',()=>{
  for(const check of ['CONTROL_COMPLETE_ROW_HASH_MISMATCH','CONTROL_UNCHANGED_NOT_EMPTY','CONTROL_DETAIL_COUNTS_OR_PRIVACY_FAILED','CONTROL_COMPOSITE_NULL_SWAP_NOT_EXACT','CONTROL_LATER_SNAPSHOT_MISMATCH_NOT_REPORTED']) assert.ok(controls.includes(check));
  assert.doesNotMatch(controls,/\b(?:update|insert into|delete from) public\./i);
  assert.match(controls,/after_matches_guard'<>'false'/);
});
