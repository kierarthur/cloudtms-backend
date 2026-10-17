import {BACK,FRONT,route,request,check} from './contract.mjs';
const staticOrigins=new Set(['https://fonts.googleapis.com','https://fonts.gstatic.com','https://cdn.jsdelivr.net','https://cdnjs.cloudflare.com','https://unpkg.com']);
export function networkGuard(stage){
 let armed=false,violation=false,total=0,writes=0,ping=0,refused=0,blockedReads=0,home=0,statics=0,preflights=0,me=0,workspace=0;
 const body=stage==='S0'?null:JSON.stringify(request(stage));
 return {
  arm(){check(stage!=='S0'&&!violation&&!armed,'FIXTURE_NETWORK_ARM_REFUSED');armed=true;},
  accept(url,method,type,postData=null){
   if(++total>512){violation=true;return false;}
   let u;try{u=new URL(url);}catch{violation=true;refused++;return false;}
   if(u.protocol!=='https:'||u.username||u.password||u.hash){violation=true;refused++;return false;}
   if(u.origin===BACK){
    if(method==='POST'&&url===BACK+'/api/changes/ping'){
     if(++ping>1)violation=true;return false; // Never forwarded or given a fabricated response.
    }
    if(method==='GET'&&url===BACK+'/api/me'){if(++me>8)violation=true;return !violation;}
    if(method==='GET'&&url===BACK+'/api/weekly-source/v1/workspace?tab=queries'){if(++workspace>2)violation=true;return !violation;}
    if(method==='POST'&&stage!=='S0'&&url===BACK+route[stage]){
     // The only allowed write has the exact reviewed fixture JSON. No caller-selected path/body.
     if(violation||!armed||writes!==0||postData!==body){violation=true;refused++;return false;}writes++;return true;
    }
    if(method==='OPTIONS'&&[BACK+'/api/me',BACK+'/api/weekly-source/v1/workspace?tab=queries',...(armed?[BACK+route[stage]]:[])].includes(url)){
     if(++preflights>24)violation=true;return !violation;
    }
    if(['GET','HEAD','OPTIONS'].includes(method)){blockedReads++;return false;}
    violation=true;refused++;return false; // Includes ALL login/refresh and unrelated writes.
   }
   if(u.origin===FRONT&&method==='GET'){
    if(u.pathname==='/'&&!u.search&&type==='document'){if(++home>1)violation=true;return !violation;}
    if(['script','stylesheet','image','font'].includes(type)&&/^\/(?:js|css|assets|fonts)\//.test(u.pathname)&&!u.pathname.includes('..')
     ||u.pathname==='/favicon.ico'&&type==='image'){if(++statics>400)violation=true;return !violation;}
    blockedReads++;return false;
   }
   if(staticOrigins.has(u.origin)&&method==='GET'&&['script','stylesheet','image','font'].includes(type)){
    if(++statics>400)violation=true;return !violation;
   }
   violation=true;refused++;return false;
  },
  refuseTransport(){violation=true;refused++;},
  assertClean(expectedWrites){check(!violation&&writes===expectedWrites,'FIXTURE_NETWORK_BOUNDARY_HELD');},
  summary(){return {businessWrites:writes,suppressedStartupRequests:ping,refusedRequests:refused,blockedReads};},
 };
}
export async function guardedRoute(route,network){
 const r=route.request();
 if(!network.accept(r.url(),r.method(),r.resourceType(),r.postData()))return route.abort();
 try{
  const response=await route.fetch({maxRedirects:0,timeout:65000});
  if(response.status()>=300&&response.status()<400){network.refuseTransport();await route.abort();}
  else await route.fulfill({response});
  await response.dispose();
 }catch{network.refuseTransport();await route.abort().catch(()=>{});}
}
// No result body, actor ID, URL/query, request body, header or private state is logged.
export async function currentAdmission(page,expectedActor=null){
 const value=await page.evaluate(async expected=>{
  if(typeof authFetch!=='function')return null;
  const r=await authFetch('https://test-cloudtms-backend.kier-88a.workers.dev/api/me',{auth401Mode:'return',signal:AbortSignal.timeout(15000)});
  const j=await r.json().catch(()=>({}));const id=j?.user?.id;
  const admin=Array.isArray(j?.user?.roles)&&j.user.roles.some(r=>typeof r==='string'&&r.toLowerCase()==='admin');
  const w=await authFetch('https://test-cloudtms-backend.kier-88a.workers.dev/api/weekly-source/v1/workspace?tab=queries',{auth401Mode:'return',signal:AbortSignal.timeout(15000)});
  const b=await w.json().catch(()=>({}));
  return {meStatus:r.status,actorId:id,admin,actorMatches:expected===null||id===expected,workspaceStatus:w.status,
   contract:b?.contract==='WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',queries:!!b?.queries&&typeof b.queries==='object'&&!Array.isArray(b.queries)&&Array.isArray(b.queries.rows)};
 },expectedActor);
 check(value?.meStatus===200&&value.admin===true&&value.actorMatches===true&&value.workspaceStatus===200&&value.contract===true&&value.queries===true
  &&typeof value.actorId==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value.actorId),'FIXTURE_CURRENT_ACTOR_ADMISSION_HELD');
 return value; // Private child memory only; parent receives booleans, never actorId.
}
