import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import vm from 'node:vm';
import assert from 'node:assert/strict';import test from 'node:test';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {createContractPresetWriteTransport} from './contract-preset-write.mjs';
import {scanManagedSql,exactAtomicBody} from './reference/pinned-source-parser.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),read=file=>fs.readFileSync(path.join(here,file),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const root=execFileSync('git',['rev-parse','--show-toplevel'],{cwd:here,encoding:'utf8'}).trim();
const worker=JSON.parse(read('WORKER_COMPLETE_REVERSES.json')),native=JSON.parse(read('NATIVE_COMPLETE_REVERSES.json')),census=JSON.parse(read('CURRENT_SQL_CONTRACT_WRITER_CENSUS.json'));
const git=file=>execFileSync('git',['show',file],{cwd:root,maxBuffer:32*1024*1024});
function reverse(s,hunks){for(const h of hunks){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}return s;}
function definitions(s){const p=scanManagedSql(s);return p.statements.map(st=>p.source.slice(st.start,st.end)).filter(st=>/^create\s+(?:or\s+replace\s+)?function\b/i.test(st));}
function header(s){return s.slice(0,s.search(/\bas\s+\$[a-z0-9_]*\$/i)).replace(/\s+/g,' ').trim();}
test('INPUT_SOURCE all complete chronological SQL input pins equal actual f500 Git objects',()=>{
 const batch=execFileSync('git',['cat-file','--batch'],{cwd:root,input:census.inputs.map(r=>`${census.pin}:${r.path}`).join('\n')+'\n',maxBuffer:512*1024*1024});let offset=0;
 for(const r of census.inputs){const lf=batch.indexOf(10,offset),m=batch.subarray(offset,lf).toString('utf8').match(/^[0-9a-f]{40} blob (\d+)$/);assert.ok(m,r.path);offset=lf+1;const size=Number(m[1]),b=batch.subarray(offset,offset+size);offset+=size+1;assert.equal(b.length,r.bytes,r.path);assert.equal(sha(b),r.sha256,r.path);}assert.equal(offset,batch.length);
 assert.equal(census.migrations,400);assert.equal(census.repeatables,1063);assert.equal(census.ordered_occurrences,2150);
 assert.deepEqual(census.current_literal_contract_writers.map(r=>r.name),['public.contract_manager_authoriser_policy_update_v1','public.contract_week_delete_planned','public.contracts_clone_and_extend_atomic','public.timesheet_weekly_chain_delete_apply']);
});
for(const r of JSON.parse(read('DIRECT_NATIVE_DEPENDENCIES.json')).dependencies)test('INPUT_DIRECT_NATIVE '+r.name,()=>{const b=git(`${r.pin}:${r.path}`);assert.equal(b.length,r.raw_bytes);assert.equal(sha(b),r.raw_sha256);const saved=read(r.reference).trimEnd();assert.equal(sha(saved),r.definition_sha256);assert.ok(definitions(b.toString('utf8')).includes(saved));});
test('INPUT_MOUNTED_CANDIDATE three current f500 runtime files are preserved and excluded from this packet replacements',()=>{const j=JSON.parse(read('MOUNTED_CANDIDATE_RUNTIME_PRESERVE.json'));assert.equal(j.entries.length,3);for(const r of j.entries){const b=git(`${r.pin}:${r.path}`);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);assert.equal(r.action,'PRESERVE_EXACT_CURRENT_CANONICAL_BYTES');}});
for(const [index,file]of ['forward/125_complete_current_worker_index_with_contract_persistence.js','forward/126_complete_agency_off_one_click_index_with_contract_persistence.js'].entries())test('WHOLE_WORKER_REVERSE '+file,()=>{
 const source=read(file),input=worker.inputs[index],before=git(`${input.pin}:${input.path}`).toString('utf8');assert.equal(sha(before),input.sha256);assert.equal(Buffer.byteLength(before),input.bytes);
 assert.equal(sha(source),index===0?worker.current_forward_sha256:worker.joined_forward_sha256);assert.ok(source.startsWith(worker.import_addition));
 assert.equal(reverse(source.slice(worker.import_addition.length),worker.function_hunks),before);
 execFileSync(process.execPath,['--check',path.join(here,file)],{encoding:'utf8'});
});
for(const r of native.records)test('WHOLE_NATIVE_REVERSE '+r.name,()=>{
 const input=git(`${r.input.pin}:${r.input.path}`);assert.equal(input.length,r.input.raw_bytes);assert.equal(sha(input),r.input.raw_sha256);
 const s=read(r.forward),old=read(r.before),h=r.whole_function_hunk;assert.equal(s.trimEnd(),h.after.trimEnd());assert.equal(old.trimEnd(),h.before.trimEnd());
 assert.equal(reverse(h.after,[h]),h.before);assert.equal(header(s),header(old));
 assert.ok(definitions(input.toString('utf8')).some(d=>r.input.definition_sha256?sha(d)===r.input.definition_sha256:input.toString('utf8')===h.before));
});
test('SOURCE eight exact actual admin handlers own ten routed writes; same-read versions retained for all PATCH calculations',()=>{
 const actual=JSON.parse(read('ACTUAL_CONTRACT_WRITER_CENSUS.json')),positive=actual.writes.filter(r=>['POST','PATCH'].includes(r.method));assert.equal(positive.length,10);assert.equal(worker.function_hunks.length,8);assert.equal(worker.call_hunks.length,10);
 for(const h of worker.function_hunks){assert.match(h.after,/const user = await requireUser\(env, req, \['admin'\]\)/);assert.ok(h.after.includes('contractPresetWriteFetch(env, user.id,'));}
 for(const name of ['handleContractsTruncateTailSafely','handleContractsPlanRanges','handleContractsUnplanRanges']){const h=worker.function_hunks.find(r=>r.name===name);assert.ok(h.after.includes('expectedUpdatedAt: c.updated_at'));assert.ok(h.after.includes('updated_at'));}
 const transport=read('contract-preset-write.mjs');assert.ok(!transport.includes('sbGetOne'));assert.ok(!transport.includes('globalThis.fetch'));assert.ok(!transport.includes('waitUntil'));
 assert.ok(worker.call_hunks.filter(h=>h.owner==='handleContractsUpdate'||h.owner==='handleContractsReplace').every(h=>h.after.includes('expectedUpdatedAt:')));
});
test('SOURCE finite native column whitelist covers actual create/duplicate/replacement and assigned patch fields',()=>{
 const sql=read('124_contract_write_with_native_fixed_approval.sqlinc'),part=sql.slice(sql.indexOf('permitted constant'),sql.indexOf('begin\n'));
 const permitted=new Set([...part.matchAll(/'([a-z0-9_]+)'/g)].map(m=>m[1]));assert.ok(permitted.size>40);assert.ok(!permitted.has('id'));assert.ok(!permitted.has('candidate_manager_approval_policy_json'));
 const used=new Set();for(const h of worker.function_hunks){if(['handleContractsCreate','handleContractsDuplicate','handleContractsReplace'].includes(h.name)){
  let section=h.name==='handleContractsCreate'?worker.call_hunks.find(r=>r.owner===h.name).before:h.before.slice(h.before.indexOf(h.name==='handleContractsDuplicate'?'const payload = {':'const patch = {'));
  if(h.name==='handleContractsCreate')section=section.slice(section.indexOf('body: JSON.stringify({')+'body: JSON.stringify({'.length);
  if(h.name!=='handleContractsCreate')section=section.slice(0,section.indexOf('\n  };')>=0?section.indexOf('\n  };'):section.indexOf('\n    };'));
  for(const m of section.matchAll(/^\s{4,6}([a-z][a-z0-9_]+)\s*(?::|,\s*$)/gm))used.add(m[1]);
 }
 if(h.name==='handleContractsUpdate')for(const m of h.before.matchAll(/\b(?:patch|schedulePatch)\.([a-z0-9_]+)\s*=/g))used.add(m[1]);}
 used.delete('candidate_manager_approval_policy_json');for(const key of used)assert.ok(permitted.has(key),key);assert.ok(used.size>35);
 for(const token of ["jsonb_populate_record(null::public.contracts,$1)","format('%I',key)","not(key=any(permitted))"])assert.ok(sql.includes(token));
});
test('SOURCE both private helpers are owner-only and new finite public transport is service-only with admin and exact CAS',()=>{
 const helpers=read('123_contract_fixed_approval_native_helpers.sqlinc'),sql=read('124_contract_write_with_native_fixed_approval.sqlinc');assert.equal(definitions(helpers).length,2);assert.equal(definitions(sql).length,1);
 assert.equal((helpers.match(/revoke all on function private\./g)||[]).length,2);assert.equal((helpers.match(/from public,anon,authenticated,service_role;/g)||[]).length,2);assert.doesNotMatch(helpers,/grant execute|create table|create trigger|set_config|current_setting|http_request/i);
 assert.ok(sql.includes('perform private._weekly_source_settings_require_admin_v1(actor)'));assert.ok(sql.includes('before_row.updated_at is distinct from expected'));assert.ok(sql.includes('target.updated_at is not distinct from $3'));
 assert.ok(sql.includes('grant execute on function public.weekly_source_contract_write_atomic_v1(jsonb) to service_role;'));
 for(const name of ['weekly_source_contract_fixed_approval_locks_v1','weekly_source_contract_fixed_approval_sync_v1','weekly_source_contract_write_atomic_v1'])assert.ok(read('131_contract_fixed_approval_owner_security.sqlinc').includes(name));
});
test('SOURCE derivation uses genuine dated membership/native policy, complete affected Client set, native receipts and real actor',()=>{
 const s=read('123_contract_fixed_approval_native_helpers.sqlinc');for(const token of ['where c.client_id=target_client order by c.id for update nowait',"fixed:=false;group_id:=null;scope_date:=null;",'weekly_source_configured_agency_invoice_v1','_contract_settings_effective_core_v1',"authority->>'configured_route'='HEALTHROSTER_WEEKLY_VALIDATION'",'candidate_manager_authoriser_policy_receipts',"'approval','NOT_REQUIRED'","else before_policy-'approval'",'u.id=p_actor_user_id and u.is_active','m.valid_to+1','a.effective_to_week_ending+1','cp.effective_to+1','c.end_date+1'])assert.ok(s.includes(token),token);
 assert.ok(s.includes('if after_policy is not distinct from before_policy then continue;end if'));assert.ok(s.includes('before_policy,\'after_policy\',after_policy'));assert.ok(s.includes('p_actor_user_id,transaction_timestamp()'));
 assert.ok(!s.includes("'approval','REQUIRED'"));assert.ok(!s.includes('import_authoritative\')::boolean,true'));
 const write=read('124_contract_write_with_native_fixed_approval.sqlinc');assert.ok(write.includes('old_client is distinct from new_client'));assert.ok(write.includes('sync_v1(old_client_contract,actor)'));
});
test('SOURCE locks reuse actual lifecycle/client keys, row locks and nonwaiting race refusal',()=>{
 const s=read('123_contract_fixed_approval_native_helpers.sqlinc');for(const token of ['weekly-source-cycle-lifecycle:','weekly-source-settings-client:','pg_try_advisory_xact_lock','groups_after is distinct from groups_before','for update nowait',"using errcode='40001'"])assert.ok(s.includes(token));
 assert.ok(s.indexOf('foreach identity in array groups_before')<s.indexOf('foreach identity in array clients'));assert.ok(s.indexOf('foreach identity in array clients')<s.indexOf('perform 1 from public.clients'));
 assert.doesNotMatch(s,/pg_advisory_xact_lock\(/);assert.ok(!s.includes('skip locked'));
});
test('SOURCE native clone both Contracts, boundary writers and policy setter preserve actual authority and replay order',()=>{
 const clone=read(native.records[0].forward);assert.equal((clone.match(/:=private.weekly_source_contract_fixed_approval_sync_v1\(/g)||[]).length,2);
 for(const r of native.records.slice(1,3))assert.ok(read(r.forward).includes('PERFORM private.weekly_source_contract_fixed_approval_sync_v1(v_contract_id,p_actor_user_id);'));
 const setter=read(native.records[3].forward);assert.ok(setter.indexOf('v_hash:=')<setter.indexOf('if found then'));assert.ok(setter.indexOf('return v_existing.response_json')<setter.indexOf('sync_v1(p_contract_id'));
 assert.ok(setter.indexOf('v_contract.updated_at is distinct from p_expected_contract_updated_at')<setter.indexOf('sync_v1(p_contract_id'));assert.ok(setter.indexOf('sync_v1(p_contract_id')<setter.indexOf('if v_contract.candidate_manager_approval_policy_json'));
});
test('PACKAGING lexical seven definitions, exact new verifier and one transaction; no SQL compile claim',()=>{
 const expanded=read('00_prospective_contract_fixed_approval.sql').replace(/^\\ir ([^\r\n]+)$/gm,(_,file)=>read(file));assert.ok(exactAtomicBody(expanded));assert.equal(definitions(expanded).length,7);assert.ok(expanded.includes("notify pgrst, 'reload schema';"));
 assert.equal(scanManagedSql(expanded).statements.filter(r=>r.tokens[0]==='BEGIN').length,1);assert.doesNotMatch(expanded,/\bcreate table\b|\bcreate trigger\b|pg_catalog\.(?:coalesce|nullif|greatest|least)\s*\(/i);
 assert.ok(read('131_contract_fixed_approval_owner_security.sqlinc').includes("acl.grantee=0"));assert.equal(census.native,false);
});

const ID='00000000-0000-0000-0000-000000000001',ACTOR='00000000-0000-0000-0000-000000000002',SOURCE='00000000-0000-0000-0000-000000000003';
const VERSION='2026-10-15T11:22:33.123Z',NEXT='2026-10-15T11:23:00.000Z',env={SUPABASE_URL:'https://agency-test.invalid'};
const row={id:ID,updated_at:NEXT,candidate_manager_approval_policy_json:{approved_emails:['manager@example.invalid'],approval:'NOT_REQUIRED'}};
function fixture({result=row,error=null}={}){const calls=[],transport=createContractPresetWriteTransport({sbRpc:async(...args)=>{calls.push(args);if(error)throw error;return result;},unwrapRpcJsonb:value=>value});return{calls,transport};}
const postOptions=values=>({method:'POST',body:JSON.stringify(values)}),patchOptions=values=>({method:'PATCH',body:JSON.stringify(values)});
const url=env.SUPABASE_URL+'/rest/v1/contracts',patchUrl=url+'?id=eq.'+ID;
test('ACTUAL_TRANSPORT dependency validation refuses incomplete factory',()=>{assert.throws(()=>createContractPresetWriteTransport({sbRpc(){}}),/DEPENDENCY_REQUIRED/);});
test('ACTUAL_TRANSPORT POST uses exact actor, unchanged ordinary values and 20-second RPC and returns actual full native row',async()=>{
 const f=fixture(),values={client_id:SOURCE,start_date:'2026-10-20',end_date:'2026-10-30',rates_json:{paye_day:12.5},weekly_timesheet_source:'HEALTHROSTER'};
 const r=await f.transport(env,ACTOR,url,postOptions(values));assert.equal(r.status,201);assert.deepEqual(await r.json(),[row]);assert.equal(f.calls.length,1);assert.equal(f.calls[0][1],'weekly_source_contract_write_atomic_v1');assert.deepEqual(f.calls[0][2],{p_request:{actor_user_id:ACTOR,mode:'POST',values}});assert.deepEqual(f.calls[0][3],{timeoutMs:20000});
});
test('ACTUAL_TRANSPORT PATCH carries original same-read CAS without a fresh read',async()=>{
 const f=fixture(),r=await f.transport(env,ACTOR,patchUrl,patchOptions({end_date:'2026-10-30',updated_at:NEXT}),{expectedUpdatedAt:VERSION});assert.equal(r.status,200);assert.deepEqual(await r.json(),[row]);assert.equal(f.calls[0][2].p_request.expected_updated_at,VERSION);assert.equal(f.calls[0][2].p_request.contract_id,ID);
});
test('ACTUAL_TRANSPORT actual printed-policy URL CAS retained exactly',async()=>{
 const f=fixture(),r=await f.transport(env,ACTOR,patchUrl+'&updated_at=eq.'+encodeURIComponent(VERSION),patchOptions({candidate_paper_submission_enabled_override:true}),{expectedUpdatedAt:'other'});assert.equal(r.status,200);assert.equal(f.calls[0][2].p_request.expected_updated_at,VERSION);
});
test('ACTUAL_TRANSPORT duplicate carries native source identity/version and no caller policy object',async()=>{
 const f=fixture(),original={client_id:SOURCE,candidate_manager_approval_policy_json:{approval:'NOT_REQUIRED',approved_emails:['forged@example.invalid']},start_date:'2026-10-20'};
 const r=await f.transport(env,ACTOR,url,postOptions(original),{policySource:{id:SOURCE,updated_at:VERSION}});assert.equal(r.status,201);const p=f.calls[0][2].p_request;assert.equal(p.policy_source_contract_id,SOURCE);assert.equal(p.policy_source_expected_updated_at,VERSION);assert.ok(!Object.hasOwn(p.values,'candidate_manager_approval_policy_json'));assert.ok(Object.hasOwn(original,'candidate_manager_approval_policy_json'));
});
const invalid=[
 ['foreign origin',ACTOR,'https://foreign.invalid/rest/v1/contracts',postOptions({client_id:SOURCE}),{},400],
 ['foreign table',ACTOR,env.SUPABASE_URL+'/rest/v1/clients',postOptions({client_id:SOURCE}),{},400],
 ['actor absent','',url,postOptions({client_id:SOURCE}),{},400],
 ['DELETE',ACTOR,patchUrl,{method:'DELETE',body:'{}'},{},400],
 ['array payload',ACTOR,url,postOptions([{client_id:SOURCE}]),{},400],
 ['null payload',ACTOR,url,postOptions(null),{},400],
 ['empty payload',ACTOR,url,postOptions({}),{},400],
 ['malformed JSON',ACTOR,url,{method:'POST',body:'{'},{},400],
 ['oversized payload',ACTOR,url,{method:'POST',body:JSON.stringify({x:'a'.repeat(262144)})},{},400],
 ['ordinary caller approval',ACTOR,url,postOptions({candidate_manager_approval_policy_json:{approval:'NOT_REQUIRED'}}),{},400],
 ['duplicate source incomplete',ACTOR,url,postOptions({client_id:SOURCE}),{policySource:{id:SOURCE}},400],
 ['duplicate source nonstring version',ACTOR,url,postOptions({client_id:SOURCE}),{policySource:{id:SOURCE,updated_at:123}},400],
 ['PATCH missing original version',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{},409],
 ['PATCH nonstring version',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:1},400],
 ['PATCH multiple ids',ACTOR,patchUrl+'&id=eq.'+SOURCE,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION},400],
 ['PATCH multiple versions',ACTOR,patchUrl+'&updated_at=eq.a&updated_at=eq.b',patchOptions({end_date:'2026-10-30'}),{},400],
 ['PATCH non-equality',ACTOR,patchUrl+'&updated_at=gt.a',patchOptions({end_date:'2026-10-30'}),{},400],
 ['PATCH policy source',ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{policySource:{id:SOURCE,updated_at:VERSION}},400],
 ['unknown query filter',ACTOR,patchUrl+'&client_id=eq.'+SOURCE,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION},400],
 ['POST id filter',ACTOR,patchUrl,postOptions({client_id:SOURCE}),{},400]
];
for(const [name,actor,target,options,context,status]of invalid)test('ACTUAL_TRANSPORT rejects '+name,async()=>{const f=fixture(),r=await f.transport(env,actor,target,options,context);assert.equal(r.status,status);assert.equal(f.calls.length,0);assert.match((await r.json()).code,/^CANDIDATE_[A-Z0-9_]+$/);});
for(const [name,error,code,status]of [
 ['stale native CAS',{json:{message:'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE'}},'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE',409],
 ['native admin denial',{message:'WEEKLY_SOURCE_ADMIN_REQUIRED'},'WEEKLY_SOURCE_ADMIN_REQUIRED',403],
 ['native invalid values',{message:'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID'},'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID',400],
 ['current RPC cache miss',{json:{code:'PGRST202',message:'routine is absent'}},'CANDIDATE_FIXED_APPROVAL_CONTRACT_DEPENDENCY_UNAVAILABLE',503],
 ['closed unknown failure',{message:'internal private payload must not escape'},'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_FAILED',503]
])test('ACTUAL_TRANSPORT maps '+name,async()=>{const f=fixture({error}),r=await f.transport(env,ACTOR,url,postOptions({client_id:SOURCE}));assert.equal(r.status,status);assert.deepEqual(await r.json(),{code});assert.equal(f.calls.length,1);});
for(const result of [null,false,[],{}, {id:SOURCE}, {id:'invalid'}])test('ACTUAL_TRANSPORT malformed/foreign native result '+JSON.stringify(result),async()=>{const f=fixture({result}),r=await f.transport(env,ACTOR,patchUrl,patchOptions({end_date:'2026-10-30'}),{expectedUpdatedAt:VERSION});assert.equal(r.status,503);assert.deepEqual(await r.json(),{code:'CANDIDATE_FIXED_APPROVAL_CONTRACT_RESULT_INVALID'});});

async function actualPrinted({user={id:ACTOR},body={override:true,expected_contract_updated_at:VERSION,request_key:'request-key-123456'},current={id:ID,updated_at:VERSION,candidate_paper_submission_enabled_override:false},error=null}={}){
 const f=fixture({error}),roles=[];const respond=(value,status=200)=>new Response(JSON.stringify(value),{status,headers:{'content-type':'application/json'}});
 const context=vm.createContext({Response,JSON,Object,String,Array,requireUser:async(_e,_r,r)=>{roles.push(r);return user;},withCORS:(_e,_r,result)=>result,unauthorized:()=>respond({code:'UNAUTHORISED'},401),parseJSONBody:async()=>body,
  badRequest:message=>respond({message},400),notFound:message=>respond({message},404),sbGetOne:async()=>current,enc:encodeURIComponent,sbHeaders:()=>({}),nowIso:()=>NEXT,contractPresetWriteFetch:f.transport,
  printedTimesheetPolicyErrorResponse:(_e,_r,message,status=503,code='UNKNOWN')=>respond({message,code},status),ok:respond});
 const source=worker.function_hunks.find(r=>r.name==='handleContractPrintedTimesheetPolicyUpdate').after,handler=new vm.Script('('+source+')').runInContext(context);
 return{response:await handler(env,{},ID),calls:f.calls,roles};
}
test('ACTUAL_HANDLER complete printed-policy route invokes native transport with authenticated actor and original CAS',async()=>{const r=await actualPrinted();assert.equal(r.response.status,200);assert.deepEqual((await r.response.json()).contract,row);assert.equal(r.calls[0][2].p_request.actor_user_id,ACTOR);assert.equal(r.calls[0][2].p_request.expected_updated_at,VERSION);assert.deepEqual(Array.from(r.roles[0]),['admin']);});
test('ACTUAL_HANDLER complete printed-policy route refuses unauthenticated caller before native transport',async()=>{const r=await actualPrinted({user:null});assert.equal(r.response.status,401);assert.equal(r.calls.length,0);});
test('ACTUAL_HANDLER complete printed-policy route preserves existing pre-write version check',async()=>{const r=await actualPrinted({body:{override:true,expected_contract_updated_at:NEXT,request_key:'request-key-123456'}});assert.equal(r.response.status,409);assert.equal(r.calls.length,0);});
test('ACTUAL_HANDLER complete printed-policy route retains truthful missing-RPC dependency response',async()=>{const r=await actualPrinted({error:{code:'PGRST202'}});assert.equal(r.response.status,503);const p=await r.response.json();assert.equal(p.code,'CANDIDATE_FIXED_APPROVAL_CONTRACT_DEPENDENCY_UNAVAILABLE');assert.match(p.message,/temporarily unavailable/);assert.ok(!p.message.includes('was not changed'));});
test('ACTUAL_HANDLER complete printed-policy route preserves native CAS race response',async()=>{const r=await actualPrinted({error:{message:'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE'}});assert.equal(r.response.status,409);assert.equal((await r.response.json()).code,'CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE');});
