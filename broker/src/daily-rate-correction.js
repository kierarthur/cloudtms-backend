// Sole Health Daily HTTP owner. Root mounts only with the reviewed native SQL
// closure. No ordinary processing writer, channel decision or Banking dialog.
import {deriveCanonicalDailyFinancialContext,buildCanonicalDailyFinancialSnapshot,normaliseCanonicalDailyRole,normaliseCanonicalDailyBand,parseCanonicalDailyRoleCodes} from './daily-canonical-authority.js';
import {VERSIONS,validatePreviewRequest,validateAcceptRequest,validatePreview,validateResult} from './contract-correction-v3.mjs';

const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ROOT='/api/timesheets';
const RPC=Object.freeze({context:'timesheet_daily_rate_correction_context_v1',preview:'timesheet_daily_rate_correction_preview_v1',accept:'timesheet_daily_rate_correction_accept_v1',result:'timesheet_daily_rate_correction_result_v1',rates:'tsfin_resolve_rates_batch'});
export const DAILY_RATE_CORRECTION_LIMITS=Object.freeze({requestBytes:16384,responseBytes:131072,contextBytes:2097152,timeoutMs:45000});
class DailyError extends Error{constructor(code,status=503,retryable=false){super(code);this.code=code;this.status=status;this.retryable=retryable;}}
const fail=(...args)=>{throw new DailyError(...args);};
const json=(status,value)=>new Response(JSON.stringify(value),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store'}});
const equal=(a,b)=>{if(a===b)return true;if(a===null||b===null||typeof a!=='object'||typeof b!=='object'||Array.isArray(a)!==Array.isArray(b))return false;const ak=Object.keys(a).sort(),bk=Object.keys(b).sort();return ak.length===bk.length&&ak.every((k,i)=>k===bk[i]&&equal(a[k],b[k]));};
function uniqueJson(text){
 const stack=[];
 for(let i=0;i<text.length;i++){
  if(text[i]==='{'||text[i]==='['){stack.push(text[i]==='{'?new Set():null);if(stack.length>32)throw new TypeError('JSON_DEPTH');}
  else if(text[i]==='}'||text[i]===']')stack.pop();
  else if(text[i]==='"'){
   const start=i++;for(;i<text.length;i++){if(text[i]==='\\')i++;else if(text[i]==='"')break;}
   const token=JSON.parse(text.slice(start,i+1));let next=i+1;while(next<text.length&&/\s/.test(text[next]))next++;
   if(text[next]===':'){const keys=stack.at(-1);if(!(keys instanceof Set)||keys.has(token))throw new TypeError('JSON_DUPLICATE_KEY');keys.add(token);}
  }
 }
 return JSON.parse(text);
}
async function boundedJson(source,maximum,signal,request=false){
 const invalid=()=>fail(request?'DAILY_RATE_REQUEST_INVALID':'DAILY_RATE_RESPONSE_INVALID',request?400:502);
 if(!source.body)invalid();
 const length=source.headers.get('content-length');
 if(length&&(!/^[0-9]+$/.test(length)||Number(length)>maximum))fail(request?'DAILY_RATE_REQUEST_TOO_LARGE':'DAILY_RATE_RESPONSE_TOO_LARGE',request?413:502);
 const reader=source.body.getReader(),chunks=[];let size=0;
 const cancel=()=>{void reader.cancel().catch(()=>{});};signal?.addEventListener('abort',cancel,{once:true});
 try{
  while(true){if(signal?.aborted)throw new DOMException('Aborted','AbortError');const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>maximum){cancel();fail(request?'DAILY_RATE_REQUEST_TOO_LARGE':'DAILY_RATE_RESPONSE_TOO_LARGE',request?413:502);}chunks.push(part.value);}
  const data=new Uint8Array(size);let offset=0;for(const chunk of chunks){data.set(chunk,offset);offset+=chunk.byteLength;}
  try{return uniqueJson(new TextDecoder('utf-8',{fatal:true}).decode(data));}catch{invalid();}
 }finally{signal?.removeEventListener('abort',cancel);reader.releaseLock();}
}
function rpcFailure(payload,status){
 const code=payload?.code;
 if(code==='PGRST202')fail('DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',503,true);
 if(code==='42501'||status===401||status===403)fail('DAILY_RATE_ACTOR_NOT_ALLOWED',status===401?401:403);
 if(code==='40001'||code==='40P01'||payload?.message==='REVIEW_STALE')fail('REVIEW_STALE',409);
 if(['22023','22P02','22P05'].includes(code))fail('DAILY_RATE_REQUEST_INVALID',400);
 if(code==='23514')fail('DAILY_RATE_NATIVE_REVIEW_REQUIRED',409);
 fail('DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',503,true);
}
export function createDailyRateCorrectionRpc({baseUrl,headers,fetchImpl=fetch}){
 const base=new URL(String(baseUrl||'').replace(/\/+$/,'')+'/rest/v1/rpc/');
 if(!['http:','https:'].includes(base.protocol)||base.username||base.password||base.search||base.hash)throw new TypeError('DAILY_RATE_TRANSPORT_INVALID');
 return async(name,args,{signal}={})=>{
  if(!Object.values(RPC).includes(name)||!(signal instanceof AbortSignal))throw new TypeError('DAILY_RATE_TRANSPORT_INVALID');
  const h=new Headers(headers);h.set('content-type','application/json');h.set('accept','application/json');
  const body=JSON.stringify(args);
  if(new TextEncoder().encode(body).byteLength>DAILY_RATE_CORRECTION_LIMITS.contextBytes)fail('DAILY_RATE_PREPARATION_TOO_LARGE',503);
  return fetchImpl(new URL(name,base),{method:'POST',headers:h,body,signal,redirect:'error'});
 };
}
async function rpc(deps,name,args,signal,maximum=DAILY_RATE_CORRECTION_LIMITS.contextBytes){
 if(typeof deps.rpc!=='function')fail('DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',503,true);
 const response=await deps.rpc(name,args,{signal,timeoutMs:DAILY_RATE_CORRECTION_LIMITS.timeoutMs});
 if(!(response instanceof Response))fail('DAILY_RATE_RESPONSE_INVALID',502);
 const payload=await boundedJson(response,maximum,signal);
 if(!response.ok)rpcFailure(payload,response.status);
 return payload;
}
function decode(fn,value,options){try{return fn(value,options);}catch{fail('DAILY_RATE_RESPONSE_INVALID',502);}}
async function actor(request,env,deps,write){
 if(typeof deps.requireUser!=='function')fail('DAILY_RATE_AUTHORITY_UNAVAILABLE',503,true);
 const user=await deps.requireUser(env,request,[]);
 if(!UUID.test(user?.id||'')||user.is_active!==true)fail('DAILY_RATE_AUTHENTICATION_REQUIRED',401);
 if(write&&String(user.role||'').trim().toLowerCase()!=='admin')fail('DAILY_RATE_ACTOR_NOT_ALLOWED',403);
 return user;
}
async function browserBody(request,signal){
 if(!/^application\/json(?:\s*;|$)/i.test(request.headers.get('content-type')||''))fail('DAILY_RATE_REQUEST_INVALID',400);
 return boundedJson(request,DAILY_RATE_CORRECTION_LIMITS.requestBytes,signal,true);
}
function contextCheck(context,timesheetId){
 if(context?.version!=='DAILY_RATE_CONTEXT_V1'||context.identity?.timesheet_id!==timesheetId||context.identity.contract_id!==null||context.identity.origin!=='DAILY'
  ||!UUID.test(context.current_financial_id||'')||!context.native_calculation_input||!context.authority_selection_args
  ||!/^[0-9a-f]{64}$/.test(context.context_digest||'')||!/^[0-9a-f]{64}$/.test(context.unchanged_work_sha256||''))fail('DAILY_RATE_RESPONSE_INVALID',502);
}
function prepare(context,ratesRow,env,deps){
 for(const k of ['toLocalParts','subtractBreak','classifyMinutes','resolveEffectivePayChannel'])if(typeof deps[k]!=='function')fail('DAILY_RATE_CALCULATOR_UNAVAILABLE',503,true);
 const derived=deriveCanonicalDailyFinancialContext({effectiveTimesheetId:context.identity.timesheet_id,context:context.native_calculation_input,toLocalParts:deps.toLocalParts,
  parseCandidateRoleCodes:parseCanonicalDailyRoleCodes,normaliseRole:normaliseCanonicalDailyRole,normaliseBand:normaliseCanonicalDailyBand});
 if(!equal(derived.rateRequest,context.authority_selection_args)||!equal(ratesRow,context.rate_authority?.selected_rates))fail('REVIEW_STALE',409);
 const snapshot=buildCanonicalDailyFinancialSnapshot({env,timesheet:derived.timesheet,currentFinancials:derived.currentFinancials,policy:derived.policy,candidate:derived.candidate,
  umbrella:context.native_calculation_input.out_umbrella,candidateId:derived.candidateId,candidateAssignment:derived.candidateAssignment,clientId:derived.clientId,
  role:derived.roleForRates,band:derived.bandForRates,rateType:derived.rateType,ratesRow,effectiveFlags:derived.effectiveFlags,preserveUnprocessed:false,
  rateCorrectionContext:context,subtractBreak:deps.subtractBreak,classifyMinutes:deps.classifyMinutes,resolveEffectivePayChannel:deps.resolveEffectivePayChannel});
 const provenance=context.rate_authority?.rate_provenance;
 return {calculation_snapshot:snapshot,rate_provenance:provenance===null?null:{...provenance,calculation_context_sha256:context.context_digest,worked_facts_sha256:context.unchanged_work_sha256}};
}
async function handle(kind,request,env,deps){
 let timer;const controller=new AbortController();let acceptSent=false;
 try{
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new DailyError(acceptSent?'DAILY_RATE_RESULT_UNCERTAIN':'DAILY_RATE_DEPENDENCY_TIMEOUT',503,true));},deps.timeoutMs??DAILY_RATE_CORRECTION_LIMITS.timeoutMs);});
  return await Promise.race([deadline,(async()=>{
   const url=new URL(request.url),user=await actor(request,env,deps,kind!=='result');
   if(url.search)fail('DAILY_RATE_REQUEST_INVALID',400);
   if(kind==='result'){
    if(request.method!=='GET')fail('DAILY_RATE_METHOD_NOT_ALLOWED',405);
    const match=url.pathname.match(/^\/api\/timesheets\/daily-rate-correction\/operations\/([0-9a-f-]+)$/);
    if(!match||!UUID.test(match[1]))fail('DAILY_RATE_REQUEST_INVALID',400);
    const value=await rpc(deps,RPC.result,{p_args:{version:VERSIONS.DAILY,operation_id:match[1],actor_user_id:user.id}},controller.signal,DAILY_RATE_CORRECTION_LIMITS.responseBytes);
    decode(validateResult,value);if(value.purpose!=='DAILY_RATE_CORRECTION'||value.operation_id!==match[1])fail('DAILY_RATE_RESPONSE_INVALID',502);
    return json(200,value);
   }
   if(request.method!=='POST')fail('DAILY_RATE_METHOD_NOT_ALLOWED',405);
   const match=url.pathname.match(/^\/api\/timesheets\/([0-9a-f-]+)\/daily-rate-correction\/(preview|accept)$/);
   if(!match||!UUID.test(match[1])||match[2]!==kind)fail('DAILY_RATE_REQUEST_INVALID',400);
   const body=await browserBody(request,controller.signal);
   try{(kind==='preview'?validatePreviewRequest:validateAcceptRequest)(body,{daily:true});}catch{fail('DAILY_RATE_REQUEST_INVALID',400);}
   if(kind==='preview'){
    if(body.timesheet_id!==match[1])fail('DAILY_RATE_REQUEST_INVALID',400);
    let context,prepared,preparationError;
    try{
     context=await rpc(deps,RPC.context,{p_args:{timesheet_id:match[1],actor_user_id:user.id}},controller.signal);contextCheck(context,match[1]);
     const rates=await rpc(deps,RPC.rates,{p_items:[context.authority_selection_args]},controller.signal);
     if(!Array.isArray(rates)||rates.length!==1||rates[0]?.k!==match[1])fail('DAILY_RATE_RESPONSE_INVALID',502);
     prepared=prepare(context,rates[0],env,deps);
    }catch(error){preparationError=error;}
    // Identical browser request replay precedes fresh native admission. If new
    // preparation fails, NULL trusted fields permit only the saved replay arm;
    // a fresh review must reject them before metadata/economic writes.
    let value;
    try{value=await rpc(deps,RPC.preview,{p_args:{version:VERSIONS.DAILY,request_id:body.request_id,actor_user_id:user.id,timesheet_id:match[1],
     context_digest:prepared?context.context_digest:null,calculation_snapshot:prepared?.calculation_snapshot??null,
     rate_provenance:prepared?.rate_provenance??null,reason:body.reason??''}},controller.signal,DAILY_RATE_CORRECTION_LIMITS.responseBytes);
    }catch(error){if(preparationError&&![401,403].includes(error.status))throw preparationError;throw error;}
    decode(validatePreview,value,{daily:true});if(value.current_timesheet_id!==match[1])fail('DAILY_RATE_RESPONSE_INVALID',502);
    return json(200,value);
   }
   acceptSent=true;
   const value=await rpc(deps,RPC.accept,{p_args:{...body,actor_user_id:user.id}},controller.signal,DAILY_RATE_CORRECTION_LIMITS.responseBytes);
   decode(validateResult,value);if(value.purpose!=='DAILY_RATE_CORRECTION'||value.operation_id!==body.operation_id||value.placement.original_timesheet_id!==match[1])fail('DAILY_RATE_RESPONSE_INVALID',502);
   return json(200,value);
  })()]);
 }catch(error){
  if(acceptSent&&(!(error instanceof DailyError)||error.status>=500))return json(503,{ok:false,error_code:'DAILY_RATE_RESULT_UNCERTAIN',retryable:true});
  if(error instanceof DailyError)return json(error.status,{ok:false,error_code:error.code,retryable:error.retryable});
  if(/^DAILY_RATE_(USED_[A-Z_]+|ADDITIONAL_AUTHORITY_REQUIRED|NATIVE_ECONOMIC_BASIS_UNKNOWN|CURRENT_CHANNEL_MISSING|CALCULATION_[A-Z_]+|WORKED_FACTS_MISMATCH)$/.test(error?.message||''))return json(409,{ok:false,error_code:error.message,retryable:false});
  return json(503,{ok:false,error_code:acceptSent?'DAILY_RATE_RESULT_UNCERTAIN':'DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE',retryable:true});
 }finally{clearTimeout(timer);controller.abort();}
}
export const handleDailyRateCorrectionPreview=(request,env,deps={})=>handle('preview',request,env,deps);
export const handleDailyRateCorrectionAccept=(request,env,deps={})=>handle('accept',request,env,deps);
export const handleDailyRateCorrectionGet=(request,env,deps={})=>handle('result',request,env,deps);
export function dispatchDailyRateCorrectionRequest(request,env,ctx,deps={}){
 const path=new URL(request.url).pathname;
 if(path.startsWith(ROOT+'/daily-rate-correction/operations/'))return handleDailyRateCorrectionGet(request,env,deps);
 if(/^\/api\/timesheets\/[^/]+\/daily-rate-correction\/preview$/.test(path))return handleDailyRateCorrectionPreview(request,env,deps);
 if(/^\/api\/timesheets\/[^/]+\/daily-rate-correction\/accept$/.test(path))return handleDailyRateCorrectionAccept(request,env,deps);
 return null;
}
export function registerDailyRateCorrectionRoutes(router,deps){
 if(typeof router?.post!=='function'||typeof router?.get!=='function')throw new TypeError('DAILY_RATE_ROUTER_INVALID');
 router.post(ROOT+'/:id/daily-rate-correction/preview',(request,env)=>handleDailyRateCorrectionPreview(request,env,deps));
 router.post(ROOT+'/:id/daily-rate-correction/accept',(request,env)=>handleDailyRateCorrectionAccept(request,env,deps));
 router.get(ROOT+'/daily-rate-correction/operations/:operation_id',(request,env)=>handleDailyRateCorrectionGet(request,env,deps));
 return router;
}
