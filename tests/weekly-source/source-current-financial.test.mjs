import test from 'node:test';
import assert from 'node:assert/strict';
import { loadWeeklyProtectedCurrentFinancialSource } from '../../broker/src/weekly-source/source-current-financial.mjs';
const id = n => `95000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash = 'a'.repeat(64);
function context() {
  return { family_id:id(1),root_timesheet_id:id(2),contract_id:id(3),work_event_id:id(4),source_cycle_id:id(5),
    root_financial:{financial_row_id:id(6),document_sha256:'b'.repeat(64)},
    protected_current_placement:{version:'PROTECTED_CURRENT_PLACEMENT_V1',moved:true,family_id:id(1),family_root_id:id(2),member_key:id(2),
      original_root_timesheet_id:id(2),original_contract_id:id(3),current_timesheet_id:id(7),current_contract_id:id(8),current_financial_id:id(9),
      current_timesheet_version:2,current_approval_work_id:null,original_canonical_work_id:null,move_operation_id:id(10),placement_revision:'1',
      immutable_original_evidence_sha256:hash,source_invoice_certificate_sha256:hash,placement_certificate_sha256:hash} };
}
function reply() {
  return {version:'SOURCE_CURRENT_FINANCIAL_SOURCE_RESULT_V1',family_id:id(1),work_event_id:id(4),source_cycle_id:id(5),
    placement_certificate_sha256:hash,current_timesheet_id:id(7),current_contract_id:id(8),root_financial:{source_system:'CLOUDTMS_TIMESHEET_FINANCIAL',
      external_identity:id(9),external_revision:'2',document_sha256:hash,financial_row_id:id(9),root_version:2,financial_timesheet_version:2,
      financial_revision_digest:hash}};
}
test('Actual B native financial fact is a separate exact service read and preserves every original A context field', async () => {
  const facts=context();const before=structuredClone(facts);const calls=[];
  const result=await loadWeeklyProtectedCurrentFinancialSource({context:facts,actorUserId:id(11),dataRpc:async(fn,args)=>{
    calls.push([fn,args]);return reply();}});
  assert.deepEqual(calls,[['weekly_source_protected_current_financial_source_v1',{p_request:{version:'SOURCE_CURRENT_FINANCIAL_SOURCE_REQUEST_V1',
    actor_user_id:id(11),family_id:id(1),work_event_id:id(4),source_cycle_id:id(5),expected_placement_certificate_sha256:hash}}]]);
  assert.equal(result.financial_row_id,id(9));assert.equal(result.document_sha256,hash);assert.deepEqual(facts,before);
  assert.equal(facts.root_financial.financial_row_id,id(6));assert.equal(facts.root_timesheet_id,id(2));
});
test('Cross-family/event/cycle/placement/B/Contract/financial/version/hash or extra financial read fields refuse', async () => {
  for(const mutate of [r=>{r.family_id=id(20);},r=>{r.work_event_id=id(20);},r=>{r.source_cycle_id=id(20);},
    r=>{r.placement_certificate_sha256='c'.repeat(64);},r=>{r.current_timesheet_id=id(20);},r=>{r.current_contract_id=id(20);},
    r=>{r.root_financial.financial_row_id=id(20);},r=>{r.root_financial.external_identity=id(20);},r=>{r.root_financial.root_version=3;},
    r=>{r.root_financial.financial_timesheet_version=3;},r=>{r.root_financial.external_revision='02';},
    r=>{r.root_financial.financial_revision_digest='b'.repeat(64);},r=>{r.root_financial.row_sha256=hash;},r=>{r.actor_user_id=id(11);}]){
    const value=reply();mutate(value);
    await assert.rejects(loadWeeklyProtectedCurrentFinancialSource({context:context(),actorUserId:id(11),dataRpc:async()=>value}),
      {code:'SOURCE_NATIVE_RESULT_INVALID'});
  }
});
test('Unmoved flow reads no B source; a missing real B financial prerequisite is never replaced by A or a fake row', async () => {
  const calls=[];const facts=context();facts.protected_current_placement.moved=false;
  assert.equal(await loadWeeklyProtectedCurrentFinancialSource({context:facts,actorUserId:id(11),dataRpc:async()=>{calls.push('unexpected');}}),null);
  const missing=context();missing.protected_current_placement.current_financial_id=null;
  await assert.rejects(loadWeeklyProtectedCurrentFinancialSource({context:missing,actorUserId:id(11),dataRpc:async()=>{calls.push('unexpected');}}),
    {code:'SOURCE_CURRENT_FINANCIAL_SOURCE_REQUIRED'});
  assert.deepEqual(calls,[]);
});
