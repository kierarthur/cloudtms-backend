import { decodeSourcePayDecisionBrowserRequest } from './source-pay-decision-request.mjs';
import { decodeSourcePayDecisionReview, decodeSourcePayDecisionReceipt } from './source-pay-decision-contract.mjs';
import { parsePushTimingJson } from './push-timing-office.mjs';
import { isActivePaymentActor } from '../payment-actor-permission.js';
import { WeeklyProtectedActionError } from './protected-action-orchestrator.mjs';
import { sourceNativeClosed, sourcePayDecisionReadAction } from './source-native-contract.mjs';

const prefix='/api/weekly-source/pay-decisions';
const UUID='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const uuid=value=>typeof value==='string'&&new RegExp('^'+UUID+'$').test(value);
const maximumBytes=262_144;
export const SOURCE_PAY_DECISION_HTTP_ROUTES=Object.freeze({
 preview:'POST '+prefix+'/preview',accept:'POST '+prefix+'/:decision_id/accept',
 resume:'POST '+prefix+'/:decision_id/resume-financial',read:'GET '+prefix+'/:decision_id?request_id=:request_id',
 publicReadProjection:'DEPENDENCY_REQUIRED',native:'NOT_RUN'
});
export function matchSourcePayDecisionRoute(path){
 if(path!==prefix&&!path.startsWith(prefix+'/'))return null;
 if(path===prefix+'/preview')return{action:'PREVIEW',method:'POST',decisionId:null};
 const m=new RegExp('^'+prefix+'/('+UUID+')(?:/(accept|resume-financial))?$').exec(path);
 return m?{action:m[2]==='accept'?'ACCEPT':m[2]==='resume-financial'?'RESUME_FINANCIAL':'READ_RESULT',method:m[2]?'POST':'GET',decisionId:m[1]}:{action:null};
}
const fail=(code,status)=>{throw Object.assign(new Error(code),{code,status,sourceHttpLocal:true});};
const response=(status,value)=>Response.json(value,{status,headers:{'cache-control':'no-store'}});
async function body(req){
 const length=req.headers.get('content-length');
 if(length!==null&&(!/^\d+$/.test(length)||Number(length)>maximumBytes))fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);
 let bytes,text,value;
 try{bytes=await req.arrayBuffer();if(bytes.byteLength>maximumBytes)fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);value=parsePushTimingJson(text);}
 catch{fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);}
 return value;
}
const localCodes=Object.freeze({SOURCE_PAY_DECISION_REQUEST_INVALID:400,SOURCE_PAY_DECISION_RESULT_INVALID:502,
 SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED:503,SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED:503,
 SOURCE_PAY_DECISION_ROUTE_NOT_FOUND:404,SOURCE_PAY_DECISION_METHOD_NOT_ALLOWED:405,PAYMENT_PERMISSION_REQUIRED:403});
// Every SQL refusal is bound to the genuine RPC name and SQLSTATE. An unknown
// exception never leaks private fields or becomes an assertion of rollback.
const nativeCodes=Object.freeze({
 SOURCE_PAY_DECISION_ACTOR_REQUIRED:['42501',403],SOURCE_PAY_DECISION_REQUEST_INVALID:['22023',400],
 SOURCE_PAY_DECISION_REQUEST_CONFLICT:['23505',409],SOURCE_PAY_DECISION_REVIEW_CHANGED:[['P0001','40001'],409],
 SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED:['55000',503]
});
const calls=new Set(['weekly_source_pay_decision_preview_begin_v1','weekly_source_pay_decision_preview_quote_context_v1',
 'weekly_source_pay_decision_preview_quote_v1','weekly_source_pay_decision_preview_ready_v1','weekly_source_pay_decision_preview_v1',
 'weekly_source_pay_decision_confirmation_prepare_v1','weekly_source_pay_decision_confirmation_context_v1',
 'weekly_source_pay_decision_unprotect_run_prepare_v1','weekly_source_pay_decision_unprotect_quote_context_v1',
 'weekly_exceptional_pay_action_context_v1','weekly_source_pay_decision_unprotect_prepare_v1',
 'weekly_source_pay_decision_accept_v1','weekly_source_pay_decision_read_source_v1']);
export async function handleSourcePayDecisionHttp({req,url,user,route,preview,confirm,readSource}){
 let request;
 try{
  if(!uuid(user?.id)||!isActivePaymentActor(user))fail('PAYMENT_PERMISSION_REQUIRED',403);
  if(!route?.action)fail('SOURCE_PAY_DECISION_ROUTE_NOT_FOUND',404);
  if(req.method!==route.method)fail('SOURCE_PAY_DECISION_METHOD_NOT_ALLOWED',405);
  if(route.action==='READ_RESULT'){
   if(req.body!==null||[...url.searchParams].length!==1||url.searchParams.getAll('request_id').length!==1)fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);
   request=decodeSourcePayDecisionBrowserRequest({version:'SOURCE_PAY_DECISION_V1',action:'READ_RESULT',decision_id:route.decisionId,request_id:url.searchParams.get('request_id')},'READ_RESULT');
   if(typeof readSource!=='function')fail('SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',503);
   // Authenticate/reprove exact saved request/current SQL scope through its
   // already declared real owner. Its private nine-key result is never sent.
   await readSource({request,actorUserId:user.id});
   fail('SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED',503);
  }
  if(url.search)fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);
  request=decodeSourcePayDecisionBrowserRequest(await body(req),route.action);
  if(route.decisionId!==null&&request.decision_id!==route.decisionId)fail('SOURCE_PAY_DECISION_REQUEST_INVALID',400);
  if(route.action==='PREVIEW'){
   if(typeof preview!=='function')fail('SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',503);
   return response(200,decodeSourcePayDecisionReview(await preview({request,actorUserId:user.id}),{decision_id:request.decision_id,request_id:request.request_id,decision:request.decision}));
  }
  if(typeof confirm!=='function')fail('SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',503);
  return response(200,decodeSourcePayDecisionReceipt(await confirm({request,actorUserId:user.id}),{
   decision_id:request.decision_id,request_id:request.request_id,review_id:request.review_id,actor_user_id:user.id,
   prior_dispositions_sha256:request.expected_prior_dispositions_sha256,...(route.action==='ACCEPT'?{decision:request.decision}:{})}));
 }catch(error){
  if(error instanceof WeeklyProtectedActionError&&error.status===409&&request){
   const d=error.details;
   if(error.code==='SOURCE_PAY_DECISION_RESULT_REQUIRED'&&sourceNativeClosed(d,['retryable','decision_id','request_id','next_action'])&&d.retryable===false&&d.decision_id===request.decision_id&&d.request_id===request.request_id){
    const a=sourcePayDecisionReadAction(d.decision_id,d.request_id);
    if(sourceNativeClosed(d.next_action,Object.keys(a))&&Object.keys(a).every(k=>d.next_action[k]===a[k]))return response(409,{ok:false,code:error.code,error_code:error.code,retryable:false,message:'Open the saved Source pay decision to continue.',details:{retryable:false,decision_id:d.decision_id,request_id:d.request_id,next_action:a}});
   }
   if(error.code==='SOURCE_PAY_DECISION_REVIEW_CHANGED'&&sourceNativeClosed(d,['retryable'])&&d.retryable===false)return response(409,{ok:false,error_code:error.code,retryable:false});
  }
  let code='SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',status=503;
  if((error?.sourceHttpLocal===true||['SOURCE_PAY_DECISION_REQUEST_INVALID','SOURCE_PAY_DECISION_RESULT_INVALID','SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED'].includes(error?.code))&&Object.hasOwn(localCodes,error.code)){code=error.code;status=localCodes[code];}
  else if(calls.has(error?.fn)&&Object.hasOwn(nativeCodes,error?.json?.message)){
   const [state,http]=nativeCodes[error.json.message];if((Array.isArray(state)?state:[state]).includes(error.json.code)){code=error.json.message;status=http;}
  }
  return response(status,{ok:false,error_code:code,retryable:false});
 }
}
