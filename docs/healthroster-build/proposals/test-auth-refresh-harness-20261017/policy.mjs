import path from 'node:path';
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const FRONTEND='D:/Development/GitHub/TEST-Frontend';
export const FRONT='https://testmode.arthur-rai.co.uk';
export const BACK='https://test-cloudtms-backend.kier-88a.workers.dev';
export const ENV_FILE=FRONTEND+'/tests/e2e/.auth/playwright-test.env';
export const SHARED_STATE=FRONTEND+'/tests/e2e/.auth/user.json';
export const PW=FRONTEND+'/node_modules/@playwright/test/index.mjs';
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
  if(method==='OPTIONS'&&['/auth/login','/auth/refresh','/api/me','/api/weekly-source/v1/workspace'].includes(u.pathname))return 'PREFLIGHT';
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
export function networkGuard(){
 const counts={};let violation=false;
 return {
  accept(url,method,type){
   const kind=requestKind(url,method,type);counts[kind]=(counts[kind]??0)+1;
   const cap={LOGIN:1,REFRESH:1,READY:1,ME:8,WORKSPACE:2,HOME:1,STATIC:400,PREFLIGHT:24}[kind];
   if(kind==='FORBIDDEN_WRITE'||kind==='REFUSED')violation=true;
   if(cap===undefined)return false;
   if(counts[kind]>cap){violation=true;return false;}
   return true;
  },
  assertAdmission(){check(!violation&&counts.LOGIN===1,'AUTH_NETWORK_BOUNDARY_HELD');},
  summary(){return {loginRequests:counts.LOGIN??0,refreshRequests:counts.REFRESH??0,
   blockedReadRequests:counts.BLOCKED_READ??0,forbiddenWriteRequests:counts.FORBIDDEN_WRITE??0,
   refusedRequests:counts.REFUSED??0};}
 };
}
