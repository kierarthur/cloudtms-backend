// Prepared structural native tests only. This script runs no SQL or network.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),id=n=>`98000000-0000-0000-0000-${String(n).padStart(12,'0')}`,h='a'.repeat(64),vectors=[];
const frame={family:id(1),run:id(2),decision:id(3),request:id(4)};
function add(name,source,accept,extra={}){vectors.push({name,source,accept,sqlNull:false,...frame,...extra});}
const next=()=>({kind:'NATIVE_STATUS',status:{ok:true,route:'NEXT',published:true,receipt:{ok:true,outcome:'PUBLISHED_NEXT',idempotent_replay:true,family_id:id(1),orchestration_run_id:id(2),approval_id:id(5),generation_id:id(6),work_id:id(7),revision_id:id(8),command_id:id(9),agency_sequence:'9007199254740993',new_family_bound_version:'9223372036854775807',approved_pay_ex_vat:'0.00',state:'NEXT_PUBLISHED'}}});
const local=(pending=false,unauthorised=false)=>({kind:'NATIVE_STATUS',status:{ok:true,route:'SOURCE_LOCAL',published:false,receipt:{authority:'SOURCE_LOCAL',family_id:id(1),orchestration_run_id:id(2),publication_request_id:id(10),request_sha256:h,generation_id:id(6),state:pending?'PENDING_FREEZE':'COMPLETE',requires_first_authorisation:unauthorised,idempotent_replay:true,result:{ok:true,authority:unauthorised?'UNAUTHORISED_TSFIN':'COMMON_ENTITLEMENT_HEAD',outcome:pending?'SAVED_PENDING_FREEZE':'PUBLISHED',state:pending?'PENDING':'LIVE',idempotent_replay:true,family_id:id(1),generation_id:id(6),publication_request_id:id(10),requires_first_authorisation:unauthorised},banking_receipt:null}}});
const mapped=(state='APPLIED')=>{const v=local(),b={source_head_id:id(11),work_id:id(7),revision_id:id(8),command_id:id(9),agency_sequence:'9007199254740993',publication_status:state==='APPLIED'?'APPLIED':state==='PENDING'?'QUEUED':'APPLYING',application:{ok:['APPLIED','PENDING'].includes(state),code:['APPLIED','PENDING'].includes(state)?'OK':state,work_id:id(7),owner_epoch:'9007199254740993',current_revision_id:id(8),applied_revision_id:state==='APPLIED'?id(8):id(18),source_event_id:id(11),source_head_id:id(11),financial_snapshot_id:null,physical_root_id:id(12),physical_root_version:3,revision_seal:'2026-10-17T06:00:00.000000Z',source_inventory_digest:h,application_state:state,command_id:id(9),agency_sequence:'9007199254740993',financial_view_revision:'0'}};v.status.published=true;v.status.receipt.banking_receipt=b;return v;};
add('ordinary_SQL_NULL_is_not_Source_owned',null,true,{sqlNull:true,decision:null,request:null});
add('retained_Source_run_cannot_return_SQL_NULL',null,false,{sqlNull:true});
add('JSON_null_is_not_SQL_NULL',null,false);
add('outer_array_is_not_a_tagged_union',[],false);
for(const [name,mutate]of[['status_array',v=>v.status=[]],['status_null',v=>v.status=null],['receipt_scalar',v=>v.status.receipt=false]]){const v=next();mutate(v);add('scalar_structure_refuses_'+name,v,false);}
for(const reason of['NOT_APPLIED_DEPENDENCY','APPLICATION_INCOMPLETE','NATIVE_RESULT_UNKNOWN'])add('exact_saved_continuation_'+reason,{kind:'SOURCE_RESULT_REQUIRED',decision_id:id(3),request_id:id(4),reason},true);
for(const [name,mutate]of[['foreign_decision',v=>v.decision_id=id(99)],['foreign_request',v=>v.request_id=id(99)],['unknown_reason',v=>v.reason='READY'],['extra_field',v=>v.summary={}],['missing_key',v=>delete v.request_id]]){const v={kind:'SOURCE_RESULT_REQUIRED',decision_id:id(3),request_id:id(4),reason:'APPLICATION_INCOMPLETE'};mutate(v);add('continuation_refuses_'+name,v,false);}
add('NEXT_genuine_closed_shape_and_int63_strings',next(),true);
for(const key of Object.keys(next().status.receipt)){const v=next();delete v.status.receipt[key];add('NEXT_missing_'+key,v,false);}
for(const [name,mutate]of[['unpublished',v=>v.status.published=false],['false_replay',v=>v.status.receipt.idempotent_replay=false],['wrong_run',v=>v.status.receipt.orchestration_run_id=id(99)],['wrong_family',v=>v.status.receipt.family_id=id(99)],['extra_field',v=>v.status.receipt.original_native_result={}],['zero',v=>v.status.receipt.agency_sequence='0'],['leading_zero',v=>v.status.receipt.agency_sequence='01'],['unsafe_Number',v=>v.status.receipt.agency_sequence=9007199254740992],['overflow',v=>v.status.receipt.agency_sequence='9223372036854775808'],['fraction',v=>v.status.receipt.approved_pay_ex_vat='0.001']]){const v=next();mutate(v);add('NEXT_refuses_'+name,v,false);}
add('Local_unauthorised_TSFIN_control_saved_without_Bank_mapping',local(false,true),true);
add('Local_pending_freeze_control_saved_without_Bank_mapping',local(true,false),true);
add('Local_complete_released_head_finance_still_unmapped',local(false,false),true);
for(const state of['APPLIED','PENDING','UNAVAILABLE','INCOMPATIBLE'])add('Local_actual_application_union_'+state,mapped(state),true);
for(const [name,mutate]of[['banking_scalar',v=>v.status.receipt.banking_receipt=false],['banking_array',v=>v.status.receipt.banking_receipt=[]],['application_array',v=>v.status.receipt.banking_receipt.application=[]]]){const v=mapped();mutate(v);add('Local_scalar_structure_refuses_'+name,v,false);}
for(const key of Object.keys(mapped().status.receipt.banking_receipt.application)){const v=mapped();delete v.status.receipt.banking_receipt.application[key];add('Local_application_missing_'+key,v,false);}
for(const [name,mutate]of[['extra_bank_field',v=>v.status.receipt.banking_receipt.opaque={}],['extra_app_field',v=>v.status.receipt.banking_receipt.application.opaque={}],['wrong_head',v=>v.status.receipt.banking_receipt.application.source_event_id=id(99)],['wrong_work',v=>v.status.receipt.banking_receipt.application.work_id=id(99)],['wrong_command',v=>v.status.receipt.banking_receipt.application.command_id=id(99)],['wrong_sequence',v=>v.status.receipt.banking_receipt.application.agency_sequence='2'],['Number_epoch',v=>v.status.receipt.banking_receipt.application.owner_epoch=3],['overflow_revision',v=>v.status.receipt.banking_receipt.application.financial_view_revision='9223372036854775808'],['unsafe_native_root_version',v=>v.status.receipt.banking_receipt.application.physical_root_version=9007199254740992],['wrong_current',v=>v.status.receipt.banking_receipt.application.current_revision_id=id(99)],['uncommitted_barrier',v=>v.status.receipt.banking_receipt.publication_status='APPLYING'],['pending_false_APPLIED',v=>{v.status.receipt.banking_receipt.application.application_state='PENDING';v.status.receipt.banking_receipt.application.applied_revision_id=id(18);}],['missing_APPLIED_revision',v=>v.status.receipt.banking_receipt.application.applied_revision_id=null],['published_flag_conflict',v=>v.status.published=false]]){const v=mapped();mutate(v);add('Local_refuses_'+name,v,false);}
for(const [name,v]of[['pending_with_bank',local(true,false)],['first_authorisation_with_bank',local(false,true)]]){v.status.published=true;v.status.receipt.banking_receipt=mapped().status.receipt.banking_receipt;add('Local_refuses_'+name,v,false);}
fs.writeFileSync(path.join(here,'NATIVE_STATUS_CHILD_VECTORS.json'),JSON.stringify({native:'NOT_RUN',preparedCases:vectors.length,vectors,qualification:'STRUCTURAL_CHILD_VALIDATION_ONLY_NO_GENUINE_BANK_OR_FINANCIAL_PROOF'},null,2)+'\n');
const data=JSON.stringify(vectors).replaceAll("'","''"),sql=`\\set ON_ERROR_STOP on
begin;
-- PREPARED / NOT_RUN by author. Pure validator inputs, not financial fixtures.
do $proof$
declare fixture jsonb;actual jsonb;rejected boolean;
begin
 for fixture in select value from jsonb_array_elements('${data}'::jsonb) loop
  rejected:=false;
  begin
   actual:=private.weekly_source_pay_decision_native_status_child_v1(
    case when fixture->'sqlNull'='true'::jsonb then null else fixture->'source' end,
    (fixture->>'family')::uuid,(fixture->>'run')::uuid,(fixture->>'decision')::uuid,(fixture->>'request')::uuid);
  exception when sqlstate '55000' then
   if sqlerrm not in('SOURCE_NATIVE_STATUS_INVALID','SOURCE_NATIVE_OWNERSHIP_CONTRADICTION') then raise;end if;
   rejected:=true;
  end;
  if (fixture->'accept'='true'::jsonb)=rejected then raise exception 'SOURCE_NATIVE_CHILD_VECTOR_FAILED: %',fixture->>'name';end if;
  if not rejected and actual is distinct from (case when fixture->'sqlNull'='true'::jsonb then null else fixture->'source' end) then
   raise exception 'SOURCE_NATIVE_CHILD_VECTOR_CHANGED_INPUT: %',fixture->>'name';end if;
 end loop;
end;
$proof$;
rollback;
`;
fs.writeFileSync(path.join(here,'NATIVE_STATUS_CHILD_VECTORS.sql'),sql);
console.log(JSON.stringify({preparedCases:vectors.length,native:'NOT_RUN'}));
