const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA=/^[0-9a-f]{64}$/;
const KEYS=['version','operation_id','family_root_id','placement_revision','original_timesheet_id','current_timesheet_id','original_contract_label','current_contract_label','moved_at_utc','evidence_origin_timesheet_id','current_mutable_timesheet_id','inherited_evidence','original_detail_target','current_detail_target','unchanged_facts_sha256','current_state','is_history'];
function fail(){throw new Error('CANDIDATE_CONTRACT_MOVE_INVALID');}
function closed(value,keys){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==keys.length||keys.some(k=>!Object.hasOwn(value,k)))fail();}
function uuid(value){if(typeof value!=='string'||!UUID.test(value))fail();return value;}
function target(value,expected){closed(value,['identity_kind','id','path']);if(value.identity_kind!=='TIMESHEET'||uuid(value.id)!==expected||value.path!==`/candidate-app/v1/timesheets/${expected}`)fail();return {...value};}
/** Same native 17-key Candidate fragment; it is metadata, never document authority. */
export function decodeCandidateContractMove(value){
 closed(value,KEYS);
 if(value.version!=='CANDIDATE_CONTRACT_MOVE_V1'||typeof value.placement_revision!=='string'||! /^[1-9][0-9]{0,18}$/.test(value.placement_revision)||BigInt(value.placement_revision)>9223372036854775807n)fail();
 for(const k of ['operation_id','family_root_id','original_timesheet_id','current_timesheet_id','evidence_origin_timesheet_id','current_mutable_timesheet_id'])uuid(value[k]);
 if(value.current_mutable_timesheet_id!==value.current_timesheet_id||value.original_timesheet_id===value.current_timesheet_id)fail();
 for(const k of ['original_contract_label','current_contract_label'])if(typeof value[k]!=='string'||value[k].trim().length===0)fail();
 if(typeof value.moved_at_utc!=='string'||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(value.moved_at_utc)||!Number.isFinite(Date.parse(value.moved_at_utc)))fail();
 const day=value.moved_at_utc.slice(0,10);if(new Date(day+'T00:00:00Z').toISOString().slice(0,10)!==day)fail();
 if(typeof value.unchanged_facts_sha256!=='string'||!SHA.test(value.unchanged_facts_sha256)||!['PENDING','SIGNED','APPROVED','FINANCIAL_REVIEW'].includes(value.current_state)||typeof value.is_history!=='boolean'||!Array.isArray(value.inherited_evidence))fail();
 const seen=new Set();
 const inherited=value.inherited_evidence.map(e=>{closed(e,['kind','id','version','sha256']);uuid(e.id);
  if(!['HOURS_TIMESHEET','EXPENSE_SUMMARY','MILEAGE_FORM','EXPENSE_EVIDENCE','SIGNED_RETURN','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE','PAPER_DOCUMENT'].includes(e.kind)||e.version!==null&&(typeof e.version!=='string'||! /^[1-9][0-9]*$/.test(e.version))||typeof e.sha256!=='string'||!SHA.test(e.sha256)||seen.has(e.id))fail();seen.add(e.id);return {...e};});
 return {...value,inherited_evidence:inherited,original_detail_target:target(value.original_detail_target,value.original_timesheet_id),current_detail_target:target(value.current_detail_target,value.current_timesheet_id)};
}
