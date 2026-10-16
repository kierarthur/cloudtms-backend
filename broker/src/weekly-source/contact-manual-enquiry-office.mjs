import {CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS,ContactManualEnquiryError,contactManualEnquiryRequest} from './contact-manual-enquiry-request.mjs';
import {decodeContactManualEnquiryReview,decodeContactManualEnquiryReceipt} from './contact-manual-enquiry-contract.mjs';

export {CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS,isContactManualEnquiryCommand,parseContactManualEnquiryBody} from './contact-manual-enquiry-request.mjs';

// Exact finite native public wrapper vocabulary. No SQLSTATE, detail, unknown
// exception message or error belonging to another RPC is browser authority.
export const CONTACT_MANUAL_ENQUIRY_REFUSALS=Object.freeze({
 CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID:400,
 WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED:403,
 CONTACT_SCOPE_NOT_FOUND:403,
 CONTACT_MANUAL_ENQUIRY_IDEMPOTENCY_CONFLICT:409,
 CONTACT_MANUAL_ENQUIRY_REVISION_CONFLICT:409,
 CONTACT_MANUAL_ENQUIRY_REVIEW_STALE:409,
 CONTACT_MANUAL_ENQUIRY_REVIEW_EXPIRED:409,
 CONTACT_MANUAL_ENQUIRY_NOT_ELIGIBLE:409,
 CONTACT_SOURCE_ORDINAL_EXHAUSTED:409,
 CONTACT_CHECKING_REVISION_EXHAUSTED:409,
 CONTACT_DEPLOYMENT_UNAVAILABLE:503,
 CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE:503,
});
const unavailable='CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE';
const response=(value,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}});

/** Source component only. Root mounts this at the existing Office command
 * owner after the complete native producer/ACL package has passed review.
 * One request makes one RPC; an uncertain ACCEPT never creates another UUID,
 * reconstructs a review, resends a provider message or retries implicitly. */
export async function handleContactManualEnquiryOfficeCommand({body,user,deploymentContext,rpc,search=''}){
 let rpcName;
 try{
  if(search)throw new ContactManualEnquiryError('CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID');
  let deployment;
  try{deployment=deploymentContext();}
  catch{throw new ContactManualEnquiryError('CONTACT_DEPLOYMENT_UNAVAILABLE',503);}
  const request=contactManualEnquiryRequest(body,user,deployment);
  rpcName=CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS[body.action];
  const value=await rpc(rpcName,request);
  const decoded=body.action==='CONTACT_MANUAL_ENQUIRY_PREVIEW'
   ?decodeContactManualEnquiryReview(value,request):decodeContactManualEnquiryReceipt(value,request);
  return response(decoded);
 }catch(error){
  const local=error instanceof ContactManualEnquiryError&&typeof error.code==='string'&&Object.hasOwn(CONTACT_MANUAL_ENQUIRY_REFUSALS,error.code);
  const native=rpcName!==undefined&&error?.fn===rpcName&&typeof error?.json?.message==='string'
   &&Object.hasOwn(CONTACT_MANUAL_ENQUIRY_REFUSALS,error?.json?.message);
  const code=local?error.code:native?error.json.message:unavailable;
  return response({ok:false,error_code:code},CONTACT_MANUAL_ENQUIRY_REFUSALS[code]);
 }
}
