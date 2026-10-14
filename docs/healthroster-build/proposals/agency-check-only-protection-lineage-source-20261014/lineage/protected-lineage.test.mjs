import assert from 'node:assert/strict';
import test from 'node:test';
import {buildWeeklySourceProjectionRows} from './test-mount/broker/src/weekly-source/upload-publication-owner.mjs';

// UNIT_ONLY actual mapper input. These tests do not certify the database scope,
// immutable claim, financial output or native Source/protection qualification.
const ID={candidate:'10000000-0000-4000-8000-000000000001',
  client:'10000000-0000-4000-8000-000000000005',
  uploadRow:'10000000-0000-4000-8000-000000000008',
  contract:'30000000-0000-4000-8000-000000000001'};
const profile='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1';
const match=(n,extra={})=>({work_event_id:`40000000-0000-4000-8000-00000000000${n}`,
  contract_id:ID.contract,schedule_compatible:true,retained_source_identity:false,protected_state:'ACTIVE',...extra});
function project(mode,extra={},contractExtra={}){
  const source=mode==='SOURCE';
  const contract={contract_id:ID.contract,candidate_id:ID.candidate,client_id:ID.client,
    valid_from:'2026-01-01',valid_to:null,weekly_source_applicable:true,
    effective_policy:{authority_mode:source?'SOURCE_AUTHORITY':'TIMESHEET_AUTHORITY',
      c1_source_mode:source?'HEALTHROSTER_WEEKLY':null,weekly_rate_classification_method:'SPLIT_RATE_WINDOWS',policy_sha256:'a'.repeat(64)},
    effective_rule_set:{values:{invoice_sender:mode==='AGENCY'?'AGENCY_INVOICE':'CLIENT_SELF_BILL',checking_profile_kind:profile}},
    settings_authority:{values:{}},rates_json:{},pay_type:'PAYE',...contractExtra};
  return buildWeeklySourceProjectionRows({profile_id:profile,rows:[{
    upload_row_id:ID.uploadRow,source_row_ordinal:2,candidate_match_count:1,
    candidate_id:ID.candidate,client_id:ID.client,work_date:'2026-09-21',
    row_finalisation_state:'SOURCE_UNFINALISED',external_source_key:'HR-1',contracts:[contract],...extra,
  }]})[0];
}
for(const mode of ['SOURCE','AGENCY']){
  test(`${mode}: unique compatible protected identity reaches the actual mapper`,()=>{
    const one=match(1,{start:'09:00',end:'16:00',break_minutes:15});
    const r=project(mode,{protected_matches:[one]});assert.equal(r.mapping_state,'RESOLVED');assert.equal(r.prior_work_event_id,one.work_event_id);
  });
  test(`${mode}: two real candidates are not reduced to a first row`,()=>{
    const choices=[match(1),match(2)];assert.equal(project(mode,{protected_matches:choices}).blocker_code,'PROTECTED_SHIFT_MATCH_REQUIRED');
    assert.equal(project(mode,{protected_matches:choices,office_selected_work_event_id:choices[1].work_event_id}).prior_work_event_id,choices[1].work_event_id);
  });
  test(`${mode}: retained native source identity outranks merely overlapping work`,()=>{
    const retained=match(1,{retained_source_identity:true,schedule_compatible:false});
    assert.equal(project(mode,{protected_matches:[retained,match(2)]}).prior_work_event_id,retained.work_event_id);
  });
  test(`${mode}: foreign Contract cannot become the protected economic identity`,()=>{
    const foreign=match(1,{contract_id:'30000000-0000-4000-8000-000000000099'});
    assert.equal(project(mode,{protected_matches:[foreign]}).prior_work_event_id,undefined);
    assert.equal(project(mode,{protected_matches:[foreign],office_selected_work_event_id:foreign.work_event_id}).blocker_code,'PROTECTED_SHIFT_MATCH_REQUIRED');
  });
  test(`${mode}: stale/incompatible identity and conflicting retained prior refuse`,()=>{
    assert.equal(project(mode,{protected_matches:[match(1,{schedule_compatible:false})]}).blocker_code,'PROTECTED_SHIFT_MATCH_REQUIRED');
    assert.equal(project(mode,{protected_matches:[match(1)],prior_work_event_id:match(2).work_event_id}).blocker_code,'PROTECTED_SHIFT_MATCH_REQUIRED');
  });
  test(`${mode}: explicit separate-shift choice retains the existing separate branch`,()=>{
    const r=project(mode,{protected_matches:[match(1,{schedule_compatible:false})],office_separate_shift:true});
    assert.equal(r.mapping_state,'RESOLVED');assert.equal(r.prior_work_event_id,undefined);
  });
}
test('ordinary Timesheet authority does not acquire the Agency matching branch',()=>{
  assert.equal(project('ORDINARY',{protected_matches:[match(1)]}).prior_work_event_id,undefined);
});
test('missing native rule set does not assert Agency scope',()=>{
  assert.equal(project('AGENCY',{protected_matches:[match(1)]},{effective_rule_set:undefined}).prior_work_event_id,undefined);
});
test('a different native authority mode does not mint an Agency matching scope',()=>{
  assert.equal(project('AGENCY',{protected_matches:[match(1)]},{effective_policy:{authority_mode:'UNRECOGNISED',
    c1_source_mode:null,weekly_rate_classification_method:'SPLIT_RATE_WINDOWS',policy_sha256:'a'.repeat(64)}}).prior_work_event_id,undefined);
});
