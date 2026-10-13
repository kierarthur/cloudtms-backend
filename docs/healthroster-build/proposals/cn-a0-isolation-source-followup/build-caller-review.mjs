import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {functionEvents,sqlTokens,parseTriggerIdentity} from './sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),pins=[];
const read=p=>{const b=fs.readFileSync(path.join(root,p));pins.push({path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');};
const graph=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_CALLEE_LOCK_GRAPH.json')));
const catalogue=JSON.parse(read('supabase/release/current-contract.json')),release=JSON.parse(read('supabase/release/current-release.json'));
const types=new Map();for(const r of [...catalogue.enums,...catalogue.relations]){types.set(r.schema+'.'+r.name,r.schema+'.'+r.name);if(r.schema==='public')types.set(r.name,r.schema+'.'+r.name);}
const sourceCache=new Map(),definition=d=>{if(!sourceCache.has(d.file))sourceCache.set(d.file,read(d.file));const p=functionEvents(sourceCache.get(d.file),{types,defaultSchema:'public'});
 const matches=p.events.filter(e=>e.id===d.id&&e.kind==='CREATE'&&sha(e.definition)===d.definitionSha256);assert.equal(matches.length,1,'Exact whole-definition pin '+d.id);return matches[0];};
const lockDetails=[];for(const n of graph.nodes){const d=definition(n),ts=sqlTokens(d.body),owners=[],rowStatements=[];
 const statement=i=>{let a=i,b=i;while(a>0&&ts[a-1].value!==';')a--;while(b<ts.length-1&&ts[b].value!==';')b++;const text=d.body.slice(ts[a].start,ts[b].end);return {statementText:text,statementSha256:sha(text)};};
 for(let i=0;i<ts.length;i++){const t=ts[i];if(t.kind==='word'&&(/^(?:pg_.*advisory.*lock|set_config|current_setting)$/.test(t.value)))owners.push({bodyLine:d.body.slice(0,t.start).split('\n').length,token:t.value,nextTokens:ts.slice(i+1,i+10).map(t=>t.raw).join(' '),...statement(i),status:'STATIC_EXPRESSION_NO_NATIVE_EXECUTION'});
 if(t.kind==='word'&&t.value==='for'&&['update','share','key','no'].includes(ts[i+1]?.value))rowStatements.push({bodyLine:d.body.slice(0,t.start).split('\n').length,...statement(i),status:'FULL_LOCK_STATEMENT_BRANCH_UNEXECUTED'});}
 if(owners.length||n.locks.length||n.nativeGateCallCandidates.length)lockDetails.push({id:n.id,file:n.file,line:n.line,definitionSha256:n.definitionSha256,clauses:n.locks,rowStatements,ownerExpressions:owners,nativeGateCallCandidates:n.nativeGateCallCandidates});}
const proposalPaths=[
 'cn-a0-lock-fence/canonical-input-assert.sqlinc',
 'cn-a0-lock-fence/13102026_2034_clock_scope.complete-proposed.sqlinc',
 'cn-a0-lock-fence/13102026_2034_a0_native_input_witness.sqlinc',
 'cn-a0-lock-fence/13102026_2034_approval.complete-proposed.sqlinc',
 'cn-a0-phantom-writers/13102026_2153_send_family_fence.sqlinc',
 'cn-a0-phantom-writers/13102026_2153_native_creator_intents.sqlinc',
 'cn-a0-phantom-writers/13102026_2153_request.complete-proposed.sqlinc',
 'cn-a0-phantom-writers/13102026_2153_a0_native_lock_fence.complete-proposed.sqlinc'];
const allNames=new Map();for(const n of graph.nodes){if(!allNames.has(n.name))allNames.set(n.name,[]);allNames.get(n.name).push(n.id);}
const proposalDefinitions=[];for(const p of proposalPaths){const file='docs/healthroster-build/proposals/'+p,text=read(file);for(const e of functionEvents(text,{types,defaultSchema:'public'}).events.filter(e=>e.kind==='CREATE')){proposalDefinitions.push({...e,file});if(!allNames.has(e.name))allNames.set(e.name,[]);if(!allNames.get(e.name).includes(e.id))allNames.get(e.name).push(e.id);}}
const proposalNodes=proposalDefinitions.map(d=>{const ts=sqlTokens(d.body),calls=[],locks=[];for(let i=0;i<ts.length;i++){
 const t=ts[i];if(['word','identifier'].includes(t.kind)&&ts[i+1]?.value==='.'&&['word','identifier'].includes(ts[i+2]?.kind)&&ts[i+3]?.value==='('){const name=t.value+'.'+ts[i+2].value;if(allNames.has(name))calls.push({calleeName:name,targetIds:allNames.get(name),bodyLine:d.body.slice(0,t.start).split('\n').length,status:'STATIC_CALL_EXPRESSION_UNMOUNTED_BRANCH_NOT_EXECUTED',observedInvoked:false});}
 if(t.kind==='word'&&t.value==='for'&&['update','share','key','no'].includes(ts[i+1]?.value))locks.push({bodyLine:d.body.slice(0,t.start).split('\n').length,tokens:ts.slice(i,i+8).map(t=>t.raw).join(' ')});
 }
 return {id:d.id,name:d.name,file:d.file,definitionSha256:sha(d.definition),calls,locks,nonRcRefusal:/current_setting\('transaction_isolation'\)\s*<>\s*'read committed'/i.test(d.body),status:'UNMOUNTED_PREDECESSOR_SOURCE_BODY_NOT_CURRENT_ROOT_REPLACEMENT_AUTHORITY'};});
const proposalGraph={contract:'A0_UNMOUNTED_PROPOSAL_CALL_LOCK_GRAPH_V1',status:'INPUT_ONLY_NO_COMPLETE_EXECUTABLE_ROOT_COMPOSITION',rootHead:graph.rootHead,proposalPaths,proposalNodes,
 note:'These exact frozen bodies augment the saved root graph for review. Later root owner changes require guarded composition; the graph does not install, replace or certify those bodies.',observedNativeInvokedEdges:[]};
fs.writeFileSync(path.join(here,'UNMOUNTED_PROPOSAL_CALL_LOCK_GRAPH.json'),JSON.stringify(proposalGraph,null,2)+'\n');
const scopes=['broker/src','candidate-private-api/src','candidate-synthetic-private-api/src','candidate-broker/src','infra/miget/cloudtms-miget-operations/src'];
const files=[];function list(dir){if(!fs.existsSync(path.join(root,dir)))return;for(const e of fs.readdirSync(path.join(root,dir),{withFileTypes:true})){const p=dir+'/'+e.name;if(e.isDirectory())list(p);else if(/\.(?:js|mjs|ts)$/.test(e.name))files.push(p);}}
scopes.forEach(list);const isolationMatches=[],rpcMatches=[],directMutationCandidates=[];
for(const file of files){const text=read(file),lines=text.split(/\r?\n/);
 for(let i=0;i<lines.length;i++){
  if(/transaction_isolation|default_transaction_isolation|PGRST_DB_TX|db-tx-end|repeatable read|read committed/i.test(lines[i]))isolationMatches.push({file,line:i+1,text:lines[i].trim()});
  if(/timesheet_correction_send_v1|timesheet_correction_authorise_atomic_v1|createTimesheetCorrectionOrchestrator/.test(lines[i]))rpcMatches.push({file,line:i+1,text:lines[i].trim(),status:'SOURCE_REFERENCE_PATH_NOT_NATIVE_INVOCATION'});
  if(/\/rest\/v1\/(?:timesheet_validations|invoice_lines|timesheets|candidates)(?![_a-z])/.test(lines[i])){
   const window=lines.slice(Math.max(0,i-4),Math.min(lines.length,i+40)).join('\n');const m=window.match(/method:\s*['"](POST|PATCH|PUT|DELETE)['"]/i);
   if(m)directMutationCandidates.push({file,line:i+1,method:m[1],table:lines[i].match(/\/rest\/v1\/(\w+)/)?.[1],windowSha256:sha(window),status:'STATIC_LOCAL_WINDOW_CANDIDATE_REQUIRES_ACTUAL_CALL_BOUNDARY_REVIEW'});
  }
 }
}
// Read every mandatory UPGRADE verifier closure, but do not render, edit or execute it.
const mandatory=release.verificationFiles||release.upgradeVerificationFiles||[];
assert.ok(mandatory.length,'Mandatory release verifier list');
const rrRoots=[],verifierCalls=[],conditionalStatements=[];
function verificationExpand(file,seen=new Set()){assert.ok(!seen.has(file));const text=read(file);const children=[];
 for(const m of text.matchAll(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/gm)){const child=path.relative(root,path.resolve(root,path.dirname(file),m[1]??m[2]??m[3])).replaceAll('\\','/');children.push(child);}
 let rr=/\bbegin\s+isolation\s+level\s+repeatable\s+read/i.test(text),guard=/source_verifier_snapshot_guard/.test(text);
 for(const m of text.matchAll(/\b(timesheet_correction_send_v1|timesheet_correction_authorise_atomic_v1|timesheet_correction_a0_\w+)\s*\(/g))verifierCalls.push({file,line:text.slice(0,m.index).split('\n').length,name:m[1],status:'SOURCE_REFERENCE_NOT_EXECUTED'});
 for(const child of children){const c=verificationExpand(child,new Set([...seen,file]));rr||=c.rr;guard||=c.guard;}return {rr,guard};}
for(const file of mandatory){const e=verificationExpand(file);if(e.rr)rrRoots.push({file,snapshotGuard:e.guard});}
const routinesWithIsolationSettings=catalogue.routines.filter(r=>(r.config||[]).some(v=>/transaction_isolation/i.test(v)));
const result={contract:'A0_SUPPORTED_CALLER_ISOLATION_SOURCE_REVIEW_V1',status:'SOURCE_ONLY_ACTUAL_POSTGREST_ISOLATION_NOT_QUALIFIED',rootHead:graph.rootHead,
 scopes,filesRead:files.length,isolationMatches,rpcMatches,directMutationCandidates,routinesWithIsolationSettings,mandatoryVerifierCount:mandatory.length,rrRoots,verifierCalls,
 gateway:{file:'infra/miget/cloudtms-miget-operations/src/index.ts',function:'proxyPostgrest',line:635,behaviour:'Forward one HTTP request; method/body/headers preserved. No caller transaction reuse or database isolation-setting logic.',status:'SOURCE_TRACE_ONLY'},
 lockDetails,proposalGraphFile:'UNMOUNTED_PROPOSAL_CALL_LOCK_GRAPH.json',inputs:pins.sort((a,b)=>a.path.localeCompare(b.path)),
 nativeHolds:['VALIDATION_FIRST_INSERT','INVOICE_FIRST_INSERT','CANDIDATE_ALIAS_RANK','RAW_BOOKING_SIBLING','NATIVE_FIN_LATE_HOOKS'],qualifiedRuntimeIsolation:null,
 prerequisites:['Observe exact deployed PostgREST version and the impersonated service role isolation settings without credentials.','Read effective top-level RPC proconfig; nested helper proconfig is not top-level transaction-start authority.','Prove actual table DML and RPC request isolation separately through the intended gateway with a harmless non-disclosing observation.','Enumerate every supported native/direct SQL creator; arbitrary client-provided headers are not authority.','Preserve mandatory outer RR snapshot-guard requirements; no native A0 positive fixture may be counted successful under its intentional RR refusal.']};
fs.writeFileSync(path.join(here,'APPLICATION_ISOLATION_AND_LOCK_REVIEW.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({files:files.length,isolationMatches:isolationMatches.length,rpcMatches:rpcMatches.length,mandatory:mandatory.length,rrRoots:rrRoots.map(x=>x.file),verifierCalls:verifierCalls.length,lockOwners:lockDetails.length,
 creators:graph.nodes.filter(n=>n.mutations.some(m=>m.operation==='INSERT'&&['timesheet_validations','invoice_lines','timesheets','candidates'].includes(m.table))).map(n=>({id:n.id,file:n.file,line:n.line,tables:[...new Set(n.mutations.filter(m=>m.operation==='INSERT').map(m=>m.table))]}))}));
