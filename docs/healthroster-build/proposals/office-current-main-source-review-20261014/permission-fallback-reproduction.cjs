// UNIT_ONLY: actual saved Office module, controlled responses, no HTTP/DB.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const office='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-office-13102026';
const source=fs.readFileSync(path.join(office,'js/weekly-source/import-workspace.js'),'utf8');
const response=(body,ok=true,status=200)=>({ok,status,json:async()=>body});
let calls=0,resolveDiscovery;const discovery=new Promise(resolve=>{resolveDiscovery=resolve;});
const frame={kind:'weekly-source-imports-v1',setTab:async()=>{}};
const root={module:{exports:{}},URLSearchParams,__modalStack:[frame],document:{querySelectorAll:()=>[]},
 setTimeout:()=>{},addEventListener:()=>{},authFetch:async()=>{
  calls++;if(calls===1)return response({error_code:'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED'},false,403);
  if(calls===2)return discovery;
  if(calls===3)return response({context:{subtitle:'NEWER_CURRENT_RESPONSE'}});
  throw Error('UNEXPECTED_CONTROLLED_REQUEST');
 }};
vm.runInNewContext(source,root);const workspace=root.module.exports;
workspace._session.workspace=workspace.normaliseWorkspace({context:{subtitle:'INITIAL'}});
(async()=>{
 const first=workspace.refresh('imports');
 for(let i=0;i<20&&calls<2;i++)await Promise.resolve();assert.equal(calls,2);
 await workspace.refresh('imports');assert.equal(workspace._session.workspace.context.subtitle,'NEWER_CURRENT_RESPONSE');
 const currentSequence=workspace._session.requestSequence;
 resolveDiscovery(response({context:{subtitle:'OLDER_PERMISSION_DISCOVERY'}}));await first;
 const result={unit_only:true,actual_saved_module:true,network_calls:0,database_calls:0,
  stale_discovery_replaces_newer_response:workspace._session.workspace.context.subtitle==='OLDER_PERMISSION_DISCOVERY',
  current_request_sequence:currentSequence,after_request_sequence:workspace._session.requestSequence};
 assert.equal(result.stale_discovery_replaces_newer_response,true);console.log(JSON.stringify(result));
})().catch(error=>{console.error(error.message);process.exitCode=1;});
