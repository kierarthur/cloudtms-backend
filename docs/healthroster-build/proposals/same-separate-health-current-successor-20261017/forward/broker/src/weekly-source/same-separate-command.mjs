import{parsePushTimingJson}from'./push-timing-office.mjs';
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/,HASH=/^[0-9a-f]{64}$/;
const id=v=>typeof v==='string'&&UUID.test(v),hash=v=>typeof v==='string'&&HASH.test(v),closed=(v,keys)=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
export const positivePaidCounter=v=>typeof v==='string'&&/^[1-9][0-9]*$/.test(v)&&(v.length<19||(v.length===19&&v<='9223372036854775807'));
const common=['request_id','upload_id','upload_row_id','projection_publication_id','expected_authority_scope_version','expected_row_manifest_hash','candidate_id','client_id','contract_id','match_reason','pay_effect','paid_overlap_target_work_event_id','expected_paid_overlap_inputs_sha256'],correction=['correction_session_id','expected_correction_session_version','expected_comparison_manifest_hash','expected_issue_set_hash'];
function invalid(){throw Object.assign(new Error('WEEKLY_SOURCE_PAID_MATCH_COMMAND_INVALID'),{code:'WEEKLY_SOURCE_PAID_MATCH_COMMAND_INVALID',status:400});}
export function isPaidMatchCommand(body){return typeof body?.action==='string'&&body.action.trim().toUpperCase()==='RECHECK_SOURCE'&&body.payload!==null&&typeof body.payload==='object'&&!Array.isArray(body.payload)&&['pay_effect','paid_overlap_target_work_event_id','expected_paid_overlap_inputs_sha256'].some(k=>Object.hasOwn(body.payload,k));}
export function validatePaidMatchCommand(body){
 if(!closed(body,['action','payload'])||body.action!=='RECHECK_SOURCE')invalid();const p=body.payload,correct=p!==null&&typeof p==='object'&&correction.some(k=>Object.hasOwn(p,k)),same=p?.pay_effect==='NONE',keys=[...common,same?'work_event_id':'separate_shift',...(correct?correction:[])];
 if(!closed(p,keys)||!['NORMAL','NONE'].includes(p.pay_effect)||!positivePaidCounter(p.expected_authority_scope_version)
  ||!['request_id','upload_id','upload_row_id','projection_publication_id','candidate_id','client_id','contract_id','paid_overlap_target_work_event_id'].every(k=>id(p[k]))
  ||!hash(p.expected_row_manifest_hash)||!hash(p.expected_paid_overlap_inputs_sha256)||typeof p.match_reason!=='string'||p.match_reason!==p.match_reason.trim()||[...p.match_reason].length<1||[...p.match_reason].length>1000
  ||(same?(!id(p.work_event_id)||p.work_event_id!==p.paid_overlap_target_work_event_id):p.separate_shift!==true)
  ||(correct&&(!id(p.correction_session_id)||!positivePaidCounter(p.expected_correction_session_version)||!hash(p.expected_comparison_manifest_hash)||!hash(p.expected_issue_set_hash))))invalid();return p;
}
export function parsePaidMatchCommand(text){let body;try{body=parsePushTimingJson(text);}catch{invalid();}validatePaidMatchCommand(body);return body;}
const reasons=new Set(['WEEKLY_SOURCE_PAID_OVERLAP_SCOPE_UNAVAILABLE','WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED','WEEKLY_SOURCE_PAID_OVERLAP_AUTOMATIC_REKEY','WEEKLY_SOURCE_PAID_OVERLAP_CONTRACT_NOT_ELIGIBLE','WEEKLY_SOURCE_PAID_MATCH_QUESTION_TOO_LARGE','WEEKLY_SOURCE_PAID_OVERLAP_PAID_AUTHORITY_UNAVAILABLE']);
const day=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&v.slice(0,4)!=='0000'&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
export function decodePaidMatchQuestion(q){
 const keys=['schema_version','state','reason_code','upload_row_id','upload_id','projection_publication_id','candidate_id','client_id','contract_id','expected_authority_scope_version','expected_row_manifest_hash','expected_paid_overlap_inputs_sha256','choices'];
 if(!closed(q,keys)||q.schema_version!=='WEEKLY_SOURCE_PAID_MATCH_QUESTION_V1'||!['READY','UNAVAILABLE'].includes(q.state)||!['upload_row_id','upload_id','projection_publication_id'].every(k=>id(q[k]))||!positivePaidCounter(q.expected_authority_scope_version)||!Array.isArray(q.choices)||q.choices.length>100)invalid();
 if(q.state==='UNAVAILABLE'){if(!reasons.has(q.reason_code)||!['candidate_id','client_id','contract_id'].every(k=>q[k]===null||id(q[k]))||(q.expected_row_manifest_hash!==null&&!hash(q.expected_row_manifest_hash))||q.expected_paid_overlap_inputs_sha256!==null||q.choices.length)invalid();return q;}
 if(q.reason_code!==null||!['candidate_id','client_id','contract_id'].every(k=>id(q[k]))||!hash(q.expected_row_manifest_hash)||!hash(q.expected_paid_overlap_inputs_sha256)||!q.choices.length)invalid();
 const seen=new Set();for(const c of q.choices){if(!closed(c,['work_event_id','work_date','start','end','break_minutes','contract_id','same_allowed'])||!id(c.work_event_id)||!id(c.contract_id)||seen.has(c.work_event_id)||!day(c.work_date)||![c.start,c.end].every(v=>typeof v==='string'&&/^(?:[01][0-9]|2[0-3]):[0-5][0-9]$/.test(v))||(c.break_minutes!==null&&(!Number.isSafeInteger(c.break_minutes)||c.break_minutes<0||c.break_minutes>2147483647))||typeof c.same_allowed!=='boolean'||c.same_allowed!==(c.contract_id===q.contract_id))invalid();seen.add(c.work_event_id);}return q;
}
