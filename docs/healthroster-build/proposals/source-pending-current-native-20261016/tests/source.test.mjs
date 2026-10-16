// Source equality/order/security checks, not PostgreSQL/native fixture proof.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {scanManagedSql} from '../../../../../scripts/cloudtms-managed-release-sql.mjs';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),root=path.resolve(base,'../../../..');
const receipt=JSON.parse(readFileSync(path.join(base,'SOURCE_RECEIPT.json'),'utf8'));
const sha=value=>createHash('sha256').update(value).digest('hex');
const read=file=>readFileSync(path.join(base,file),'utf8');
const sql=n=>read(receipt.outputs[n-1].path);
function complete(text,name){const start=text.toLowerCase().indexOf('create or replace function '+name.toLowerCase()+'('),open=text.indexOf('$function$',start),end=text.indexOf('$function$;',open+10);assert.ok(start>=0&&open>start&&end>open);return text.slice(start,end+12).trimEnd()+'\n';}
for(const [index,item] of receipt.outputs.entries()){
 test('complete exact finite function '+item.signature,()=>{
  let actual=read(item.path);assert.equal(sha(actual),item.sha256);assert.ok(!actual.includes('\r'));
  assert.equal((actual.match(/create or replace function /gi)||[]).length,1);
  assert.equal(scanManagedSql(actual).statements.length,1);
  if(index===5)return;
  const input=receipt.inputs[index],file=execFileSync('git',['show',`${input.commit}:${input.path}`],{cwd:root,encoding:'utf8',maxBuffer:8e6});
  assert.equal(sha(file),input.whole_file_sha256);
  const parent=complete(file,input.signature);assert.equal(sha(parent),input.complete_function_sha256);
  assert.equal(actual.slice(0,actual.indexOf('$function$')),parent.slice(0,parent.indexOf('$function$')));
  for(const delta of item.deltas.toReversed()){assert.equal(actual.split(delta.to).length,2);actual=actual.replace(delta.to,delta.from);}
  assert.equal(actual,parent);
 });
}
test('classifier retains whole native ownership predicate and active-component refusal',()=>{
 const output=receipt.outputs[0],input=receipt.inputs[0],file=execFileSync('git',['show',`${input.commit}:${input.path}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}),parent=complete(file,input.signature);
 const actual=sql(1),start=parent.indexOf('  SELECT EXISTS ('),end=parent.indexOf('  IF v_source AND EXISTS (');
 assert.equal(actual.slice(actual.indexOf('  SELECT EXISTS ('),actual.indexOf('  IF v_source AND EXISTS (')).replace('AND (EXISTS (','AND EXISTS (').replace(') OR v_original_invoice)',')'),parent.slice(start,end));
 assert.equal(actual.slice(actual.indexOf('  IF v_source AND EXISTS (')),parent.slice(end));
 assert.ok(actual.indexOf('timesheet_contract_native_pending_for_v1')<actual.indexOf('  SELECT EXISTS ('));
 assert.ok(actual.includes('v_pending is not null and v_alias.original_timesheet_id is distinct from p_timesheet_id'));
 assert.equal(output.deltas.length,3);
});
test('original-A exception requires independently certified Source and exact accepted member/family/Contract',()=>{
 for(const file of [sql(1),sql(6)])for(const clause of [
  'timesheet_contract_source_original_eligible_v1(p_timesheet_id)',
  'operation.id=v_alias.accepted_operation_id',"operation.purpose='CONTRACT_CORRECTION'",
  "operation.invoice_authority in('SOURCE_FINANCIAL','MIXED')",'member.family_root_id=v_alias.family_root_id',
  'member.role=v_alias.member_role and member.from_timesheet_id=p_timesheet_id',
  'member.from_version=original.version and member.from_contract_id is not distinct from original.contract_id',
  'member.original_booking_id=original.booking_id','operation.candidate_id=original_contract.candidate_id',
  'operation.actual_client_id=original_contract.client_id'])assert.ok(file.includes(clause),clause);
 assert.ok(sql(6).includes('private._candidate_expense_source_family_v1(p_timesheet_id) is distinct from true'));
});
test('root identity preserves exact existing closed current-cardinality failure before booking resolver',()=>{
 const actual=sql(2);assert.ok(actual.indexOf('timesheet_contract_native_pending_for_v1')<actual.indexOf('  with scope as ('));
 assert.ok(actual.includes("'reason','CURRENT_ROW_CARDINALITY','timesheet_id',p_timesheet_id"));
 const delta=receipt.outputs[1].deltas[0].to;assert.ok(!delta.includes('canonical_timesheet_id'));assert.ok(!delta.includes('max('));
});
test('family locks retain native Candidate/booking/row order and compare complete H103 after waits',()=>{
 const actual=sql(3),pre=actual.indexOf('v_physical_before:=v_physical_before||'),post=actual.indexOf('v_physical_after:=v_physical_after||');
 const advisory=actual.indexOf('perform pg_catalog.pg_advisory_xact_lock('),rowlock=actual.indexOf('    for update;');
 assert.ok(pre>0&&pre<advisory&&advisory<rowlock&&rowlock<post);
 assert.ok(post<actual.indexOf('  -- Step 3:'));
 assert.equal((actual.match(/timesheet_contract_native_pending_for_v1\(v_requested\)/g)||[]).length,2);
 assert.ok(actual.includes('v_physical_after is distinct from v_physical_before'));
 // No second physical lock protocol or changed native booking/Candidate body.
 for(const d of receipt.outputs[2].deltas)assert.ok(!/timesheet_contract_move_family_lock_v1|pg_advisory_xact_lock|for update/i.test(d.to));
});
test('current pay query certifies pending before existing resolver and NULL/ordinary arms',()=>{
 const actual=sql(4);assert.ok(actual.indexOf('timesheet_contract_native_pending_for_v1')<actual.indexOf('v_identity:=private.weekly_source_resolve_root_identity_v1'));
 assert.ok(actual.includes("raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'"));
 assert.equal(receipt.outputs[3].deltas.length,1);
});
test('decision explicit family checks pending and revalidates saved H103 after physical waits',()=>{
 const actual=sql(5),explicit=actual.indexOf('  if root is not null then'),check=actual.indexOf('timesheet_contract_native_pending_for_v1(root)'),lineage=actual.indexOf('   select l.* into lineage');
 assert.ok(explicit<check&&check<lineage);
 assert.ok(actual.includes("primary_scope->>'current_timesheet_id' is null"));
 const lock=actual.indexOf('perform private.timesheet_contract_move_family_lock_v1'),recheck=actual.indexOf("scope_item#>>'{current_placement,requested_timesheet_id}'");
 assert.ok(lock<recheck&&recheck<actual.indexOf(' -- Enrich only factual invoice/contact/protection'));
 assert.ok(actual.includes("is distinct from scope_item->'current_placement'"));
 assert.ok(actual.includes('  else lineage:=null;family:=null;primary_scope:=null;'));
});
test('Invoice duty retains exact nine-key native return and never resolves current pay identity for certified A',()=>{
 const actual=sql(6),derived=receipt.derived[0],start=actual.indexOf("  return jsonb_build_object('root_timesheet_id',v_root.timesheet_id,"),end=actual.indexOf('\nend;',start);
 assert.equal(sha(actual.slice(start,end)),derived.exact_native_return_sha256);
 assert.equal(derived.scope_keys.length,9);
 assert.ok(!actual.includes('weekly_source_resolve_root_identity_v1'));
 assert.ok(!actual.includes('weekly_source_target_family_for_root_v1'));
 assert.equal((actual.match(/return private.weekly_source_pay_query_scope_v1/g)||[]).length,1);
 assert.ok(actual.indexOf('v_alias.original_timesheet_id is distinct from p_timesheet_id')<actual.indexOf('return private.weekly_source_pay_query_scope_v1'));
 assert.ok(actual.includes('v_family.root_timesheet_id is distinct from p_timesheet_id'));
 assert.ok(!/current_timesheet_id'\s*,/.test(actual));
});
test('committed Source and genuine Local saved replay are unchanged frozen complete source',()=>{
 for(const file of ['forward/41_atomic_source_acceptance.sqlinc','forward/25_actual_local_release_and_saved_status.sqlinc']){
  const relative='docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/'+file;
  assert.equal(readFileSync(path.join(root,relative),'utf8'),execFileSync('git',['show',`${receipt.source_pay_decision_frozen_commit}:${relative}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}));
 }
 const accept=readFileSync(path.join(root,'docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/forward/41_atomic_source_acceptance.sqlinc'),'utf8');
 assert.ok(accept.indexOf("j.metadata_state='COMMITTED'")<accept.indexOf('perform private.bpay_next_source_pay_decision_cut_v1'));
});
test('finite owner-only ACL and six intended signatures, with no Banking or public owner body',()=>{
 const acl=read('forward/99_acl.sqlinc');assert.equal(sha(acl),receipt.acl.sha256);
 assert.equal((acl.match(/alter function /g)||[]).length,6);assert.equal((acl.match(/revoke all on function /g)||[]).length,6);
 assert.ok(!/grant|create or replace function public\.|create or replace function private\.bpay_/i.test(receipt.outputs.map(o=>read(o.path)).join('\n')+acl));
 assert.ok(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(receipt.outputs.map(o=>read(o.path)).join('\n')));
});
