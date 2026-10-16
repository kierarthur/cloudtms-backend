import { parsePushTimingJson } from './push-timing-office.mjs';

export const CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS=Object.freeze({
 CONTACT_MANUAL_ENQUIRY_PREVIEW:'weekly_source_contact_manual_enquiry_preview_v1',
 CONTACT_MANUAL_ENQUIRY_ACCEPT:'weekly_source_contact_manual_enquiry_accept_v1',
});
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX=/^[0-9a-f]{64}$/;
const uuid=v=>typeof v==='string'&&UUID.test(v);
const exact=(v,keys)=>v!==null&&typeof v==='object'&&!Array.isArray(v)
 &&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const revision=v=>typeof v==='string'&&/^(0|[1-9][0-9]*)$/.test(v)
 &&(v.length<19||(v.length===19&&v<='9223372036854775807'));
const dense=v=>Array.isArray(v)&&Object.keys(v).length===v.length;
export class ContactManualEnquiryError extends Error{constructor(code,status=400){super(code);this.code=code;this.status=status;}}
const invalid=()=>{throw new ContactManualEnquiryError('CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID');};
export function isContactManualEnquiryCommand(action){return typeof action==='string'&&Object.hasOwn(CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS,action);}
export function parseContactManualEnquiryBody(raw){
 try{if(typeof raw!=='string'||new TextEncoder().encode(raw).byteLength>262_144)invalid();return parsePushTimingJson(raw);}
 catch{invalid();}
}

// Exact commissioned browser envelope. Actor/deployment come only from the
// authenticated Office host and its deployment owner, never nested browser data.
export function contactManualEnquiryRequest(body,actor,deployment){
 if(!exact(body,['action','payload'])||!isContactManualEnquiryCommand(body.action))invalid();
 const accept=body.action==='CONTACT_MANUAL_ENQUIRY_ACCEPT';const p=body.payload;
 if(!exact(p,['version','request_id','audience','reason','scopes',...(accept?['review_id','review_digest']:[])])
  ||p.version!=='CONTACT_MANUAL_ENQUIRY_V1'||!uuid(p.request_id)||!['WORKER','MANAGER'].includes(p.audience)
  ||typeof p.reason!=='string'||!p.reason.trim()||[...p.reason].length>1000
  ||!dense(p.scopes)||p.scopes.length<1||p.scopes.length>100
  ||(accept&&(!uuid(p.review_id)||typeof p.review_digest!=='string'||!HEX.test(p.review_digest))))invalid();
 const scopes=new Set(),issues=new Set();let count=0;
 for(const s of p.scopes){
  if(!exact(s,['candidate_id','client_id','selected_issue_ids','expected_scope_revision','expected_checking_revision'])
   ||!uuid(s.candidate_id)||!uuid(s.client_id)||!revision(s.expected_scope_revision)||!revision(s.expected_checking_revision)
   ||!dense(s.selected_issue_ids)||s.selected_issue_ids.length<1||s.selected_issue_ids.length>500)invalid();
  const key=`${s.candidate_id}/${s.client_id}`;if(scopes.has(key))invalid();scopes.add(key);
  for(const issue of s.selected_issue_ids){if(!uuid(issue)||issues.has(issue))invalid();issues.add(issue);count++;}
 }
 if(count>500)invalid();
 if(!actor||!uuid(actor.id)||actor.is_active!==true||actor.role!=='admin'){
  throw new ContactManualEnquiryError('WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED',403);
 }
 if(!exact(deployment,['agency_id','environment'])||!uuid(deployment.agency_id)||!['TEST','LIVE'].includes(deployment.environment)){
  throw new ContactManualEnquiryError('CONTACT_DEPLOYMENT_UNAVAILABLE',503);
 }
 // No sorting, numeric coercion, trimming or browser reason default: the exact
 // accepted request is what the immutable server review/idempotency seals.
 return {...p,actor_user_id:actor.id,...deployment};
}
