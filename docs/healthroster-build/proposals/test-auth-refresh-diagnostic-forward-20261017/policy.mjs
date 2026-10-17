import path from 'node:path';
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const FRONTEND='D:/Development/GitHub/TEST-Frontend';
export const FRONT='https://testmode.arthur-rai.co.uk';
export const BACK='https://test-cloudtms-backend.kier-88a.workers.dev';
export const ENV_FILE=FRONTEND+'/tests/e2e/.auth/playwright-test.env';
export const SHARED_STATE=FRONTEND+'/tests/e2e/.auth/user.json';
export const PW='file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs';
export const ME=BACK+'/api/me';
export const WORKSPACE=BACK+'/api/weekly-source/v1/workspace?tab=queries';
export const check=(ok,code)=>{if(!ok)throw Error(code);};
export function ownedDirectory(value){
 check(typeof value==='string'&&path.isAbsolute(value),'AUTH_OWNED_PATH_REQUIRED');
 const normalized=path.resolve(value).replaceAll('\\','/');
 check(path.posix.dirname(normalized)===ROOT+'/.codex-tmp'
  &&/^test-auth-refresh-20261017-[a-f0-9]{32}$/.test(path.posix.basename(normalized)),'AUTH_OWNED_PATH_REQUIRED');
 return normalized;
}
// Exact pre-existing R3 static origins. Auth and application reads use only the two TEST hosts.
const staticOrigins=new Set(['https://fonts.googleapis.com','https://fonts.gstatic.com',
 'https://cdn.jsdelivr.net','https://cdnjs.cloudflare.com','https://unpkg.com']);
export function requestKind(url,method,type){
 let u;try{u=new URL(url);}catch{return 'REFUSED';}
 if(u.protocol!=='https:'||u.username||u.password||u.hash)return 'REFUSED';
 if(u.origin===BACK){
  if(method==='GET'&&url===BACK+'/readyz')return 'READY';
  if(method==='GET'&&url===ME)return 'ME';
  if(method==='GET'&&url===WORKSPACE)return 'WORKSPACE';
  if(method==='POST'&&!u.search&&u.pathname==='/auth/login')return 'LOGIN';
  if(method==='POST'&&!u.search&&u.pathname==='/auth/refresh')return 'REFRESH';
  if(method==='OPTIONS'&&[BACK+'/auth/login',BACK+'/auth/refresh',ME,WORKSPACE].includes(url))return 'PREFLIGHT';
  return ['GET','HEAD','OPTIONS'].includes(method)?'BLOCKED_READ':'FORBIDDEN_WRITE';
 }
 if(u.origin===FRONT&&method==='GET'){
  if(u.pathname==='/'&&!u.search&&type==='document')return 'HOME';
  if(['script','stylesheet','image','font'].includes(type)
   &&/^\/(?:js|css|assets|fonts)\//.test(u.pathname)&&!u.pathname.includes('..'))return 'STATIC';
  if(u.pathname==='/favicon.ico'&&type==='image')return 'STATIC';
  return 'BLOCKED_READ';
 }
 if(staticOrigins.has(u.origin)&&method==='GET'&&['script','stylesheet','image','font'].includes(type))return 'STATIC';
 return 'REFUSED';
}
export const diagnosticLabels=new Set(['NONE','STATIC_CHANGES_PING','STATIC_USER_GRID_PREFS','OTHER_NORMAL_BACKEND','MULTIPLE']);
export const diagnosticMethods=new Set(['NONE','POST','PATCH','PUT','DELETE','OTHER','MULTIPLE']);
const exactKeys=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&JSON.stringify(Object.keys(v).sort())===JSON.stringify([...keys].sort());
export function blockedWriteLabel(url){
 let u;try{u=new URL(url);}catch{return 'OTHER_NORMAL_BACKEND';}
 // Static source literals only. Never retain or emit URL/query/body/identifiers.
 if(u.origin===BACK&&u.pathname==='/api/changes/ping')return 'STATIC_CHANGES_PING';
 if(u.origin===BACK&&u.pathname==='/api/users/me/grid-prefs')return 'STATIC_USER_GRID_PREFS';
 return 'OTHER_NORMAL_BACKEND';
}
export function closedAdmission(value){
 check(exactKeys(value,['meStatus','actorUuid','admin','workspaceStatus','contract','queries']),'AUTH_CLOSED_RESULT_REQUIRED');
 for(const key of ['meStatus','workspaceStatus'])check(Number.isSafeInteger(value[key])&&value[key]>=0&&value[key]<=599,'AUTH_CLOSED_RESULT_REQUIRED');
 for(const key of ['actorUuid','admin','contract','queries'])check(typeof value[key]==='boolean','AUTH_CLOSED_RESULT_REQUIRED');
 return {meStatus:value.meStatus,actorUuid:value.actorUuid,admin:value.admin,workspaceStatus:value.workspaceStatus,contract:value.contract,queries:value.queries};
}
export function closedDiagnostic(value){
 check(exactKeys(value,['phase','admission','blockedWriteLabel','blockedWriteMethod']),'AUTH_CLOSED_RESULT_REQUIRED');
 const admission=value.admission===null?null:closedAdmission(value.admission);
 check(value.phase===(admission===null?'BEFORE_EXPLICIT_ADMISSION':'EXPLICIT_ADMISSION_RETURNED')
  &&diagnosticLabels.has(value.blockedWriteLabel)&&diagnosticMethods.has(value.blockedWriteMethod),'AUTH_CLOSED_RESULT_REQUIRED');
 check((value.blockedWriteLabel==='NONE')===(value.blockedWriteMethod==='NONE'),'AUTH_CLOSED_RESULT_REQUIRED');
 return {phase:value.phase,admission,blockedWriteLabel:value.blockedWriteLabel,blockedWriteMethod:value.blockedWriteMethod};
}
export function closedResult(value){
 const keys=['browserClosed','code','forbiddenWriteRequests','loginRequests','refusedRequests','refreshRequests','status','version','blockedReadRequests','diagnostic'];
 check(value&&JSON.stringify(Object.keys(value).sort())===JSON.stringify(keys.sort()),'AUTH_CLOSED_RESULT_REQUIRED');
 check(value.version==='TEST_AUTH_REFRESH_PRIVATE_RESULT_V2'&&['AUTH_REFRESHED','HOLD'].includes(value.status)
  &&typeof value.browserClosed==='boolean'&&new Set(['AUTH_REFRESHED','AUTH_2FA_REQUIRED','AUTH_APPROVED_ENV_UNAVAILABLE',
   'AUTH_ME_ADMIN_ADMISSION_HELD','AUTH_QUERY_WORKSPACE_ADMISSION_HELD','AUTH_NETWORK_BOUNDARY_HELD',
   'AUTH_FRESH_STATE_REQUIRED','AUTH_READY_HELD','AUTH_WORK_BUDGET_HELD','AUTH_BROWSER_CLOSE_UNCONFIRMED','AUTH_REFRESH_HELD']).has(value.code),'AUTH_CLOSED_RESULT_REQUIRED');
 for(const key of ['loginRequests','refreshRequests','blockedReadRequests','forbiddenWriteRequests','refusedRequests'])
  check(Number.isSafeInteger(value[key])&&value[key]>=0&&value[key]<=10000,'AUTH_CLOSED_RESULT_REQUIRED');
 closedDiagnostic(value.diagnostic);
 return value;
}
export function networkGuard(){
 const counts={};let violation=false,total=0,admission=null,writeLabel='NONE',writeMethod='NONE';
 return {
  accept(url,method,type){
   if(++total>512){violation=true;return false;}
   const kind=requestKind(url,method,type);counts[kind]=(counts[kind]??0)+1;
   const cap={LOGIN:1,REFRESH:1,READY:1,ME:8,WORKSPACE:2,HOME:1,STATIC:400,PREFLIGHT:24}[kind];
   if(kind==='FORBIDDEN_WRITE'||kind==='REFUSED')violation=true;
   if(kind==='FORBIDDEN_WRITE'){
    const label=blockedWriteLabel(url),verb=['POST','PATCH','PUT','DELETE'].includes(method)?method:'OTHER';
    writeLabel=writeLabel==='NONE'||writeLabel===label?label:'MULTIPLE';
    writeMethod=writeMethod==='NONE'||writeMethod===verb?verb:'MULTIPLE';
   }
   if(cap===undefined)return false;
   if(counts[kind]>cap){violation=true;return false;}
   return true;
  },
  assertAdmission(){check(!violation&&counts.LOGIN===1,'AUTH_NETWORK_BOUNDARY_HELD');},
  recordAdmission(value){admission=closedAdmission(value);},
  diagnostics(){return closedDiagnostic({phase:admission===null?'BEFORE_EXPLICIT_ADMISSION':'EXPLICIT_ADMISSION_RETURNED',admission,blockedWriteLabel:writeLabel,blockedWriteMethod:writeMethod});},
  refuseTransport(){violation=true;counts.REFUSED=(counts.REFUSED??0)+1;},
  summary(){return {loginRequests:counts.LOGIN??0,refreshRequests:counts.REFRESH??0,
   blockedReadRequests:counts.BLOCKED_READ??0,forbiddenWriteRequests:counts.FORBIDDEN_WRITE??0,
   refusedRequests:counts.REFUSED??0};}
 };
}
