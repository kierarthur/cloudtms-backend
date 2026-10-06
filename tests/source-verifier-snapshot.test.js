import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read=name=>fs.readFileSync(new URL('../supabase/verification/'+name,import.meta.url),'utf8');
const direct=[
 '15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
 '15092026_1534_weekly_source_read_projections_v1.sql',
 '17092026_1100_weekly_source_candidate_view_producer_v1.sql',
 '17092026_1200_weekly_source_audit_and_export_v1.sql',
];
const nested=[
 ['15092026_1534_weekly_source_correct_final_source_v1.sql',1],
 ['15092026_1534_weekly_source_invoice_admission_v1.sql',2],
 ['02092026_1833_weekly_source_invoice_issue_validator_v1.sql',2],
];
test('all complete-financial-row verifier roots require one rollback transaction snapshot',()=>{
 for(const name of direct) {
  const sql=read(name);
  assert.match(sql,/^begin isolation level repeatable read;$/m,name);
  assert.match(sql,/\\ir support\/06102026_1818_source_verifier_snapshot_guard\.sql/,name);
  assert.doesNotMatch(sql,/^begin;$/m,name);
  assert.match(sql,/rollback;/,name);
  assert.match(sql,/if v_after is distinct from v_before then/);
  assert.match(sql,/PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT/);
  assert.match(sql,/for v_call in 1\.\.3 loop/);
  assert.match(sql,/12,v_claim_now,null::uuid,v_candidate,'SOURCE_PAID_FIXTURE_SETUP_TWELVE',180/);
 }
});
test('every included ordinary verifier has a snapshot established by its caller, in both invoice modes',()=>{
 for(const [name,count] of nested) {
  const sql=read(name);
  assert.equal((sql.match(/^begin isolation level repeatable read;$/gm)||[]).length,count,name);
  assert.equal((sql.match(/\\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1\.sql/g)||[]).length,count,name);
  assert.equal((sql.match(/^rollback;$/gm)||[]).length,count,name);
  assert.doesNotMatch(sql,/^begin;$/m,name);
  assert.ok(sql.indexOf('begin isolation level repeatable read;')<sql.indexOf('\\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql'),name);
 }
 const discovered=fs.readdirSync(new URL('../supabase/verification/',import.meta.url))
  .filter(name=>name.endsWith('.sql')&&read(name).includes('\\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql')).sort();
 assert.deepEqual(discovered,nested.map(([name])=>name).sort());
});
test('isolation guard fails closed rather than altering late transaction settings or replacing owners',()=>{
 const guard=read('support/06102026_1818_source_verifier_snapshot_guard.sql');
 assert.match(guard,/current_setting\('transaction_isolation'\)<>'repeatable read'/);
 assert.match(guard,/SOURCE_VERIFIER_TRANSACTION_SNAPSHOT_REQUIRED/);
 assert.doesNotMatch(guard,/create(?: or replace)? function|set (?:local |session |transaction)|\b(?:update|insert into|delete from) public\.|disable trigger|statement_timeout/i);
 const fingerprint=read('support/06102026_1410_source_full_row_fingerprints.sql');
 assert.match(fingerprint,/to_jsonb\(row_value\)::text/);
 assert.doesNotMatch(fingerprint,/to_jsonb\(row_value\)\s*-/);
});
