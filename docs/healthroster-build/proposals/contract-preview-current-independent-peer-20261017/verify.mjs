// Finite independent saved/Git/current-owner and adversarial choreography proof.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {here,repo,commit,prefix,sha,get,write} from './capture.mjs';
const ref=path.join(here,'reference'),native='84738fe4057d9d693ccd723eb1835106f1091f57';
const read=p=>fs.readFileSync(path.join(ref,p),'utf8');
const results=[];const check=(name,fn)=>{fn();results.push({name,status:'PASS'});};
const captured=JSON.parse(fs.readFileSync(path.join(here,'CAPTURE.json')));
for(const row of captured.files)check('exact frozen saved/Git: '+row.file,()=>{
 const raw=fs.readFileSync(path.join(ref,row.file));assert.equal(sha(raw),row.sha256);assert.deepEqual(raw,get(commit,prefix+row.file));
});
const pins=JSON.parse(read('SOURCE_PINS.json')),rawSources=new Map();
for(const row of pins.files.filter(x=>x.workspace==='integration')){
 const raw=get(native,row.path);rawSources.set(row.path,raw);
 check('exact complete current input: '+row.path,()=>{assert.equal(sha(raw),row.sha256);assert.equal(raw.length,row.bytes);});
}
const owners=[];
for(const row of pins.completeDeclarations){
 const raw=rawSources.get(row.physical),text=raw.toString('utf8'),needle=fs.readFileSync(path.join(ref,row.output),'utf8').replaceAll('\r\n','\n').trimEnd();
 const norm=text.replaceAll('\r\n','\n');const start=norm.indexOf(needle);
 check('complete selected native declaration: '+row.output,()=>{assert.ok(start>=0);assert.equal(norm.indexOf(needle,start+1),-1);});
 // Obtain raw bytes through exact matched native line interval; no emitted normalization.
 const line=norm.slice(0,start).split('\n').length,lines=needle.split('\n').length;
 const rawLines=text.match(/[^\n]*\n|[^\n]+$/g),piece=Buffer.from(rawLines.slice(line-1,line-1+lines).join(''));
 assert.equal(piece.toString().replaceAll('\r\n','\n').trimEnd(),needle);
 const dest='native-owners/'+path.basename(row.output);write(dest,piece);
 owners.push({name:row.name,signature:row.signature??null,physical:row.physical,startLine:line,lines,rawSha256:sha(piece),rawBytes:piece.length,output:dest,authorInputSha256:sha(fs.readFileSync(path.join(ref,row.output))),comparison:'COMPLETE_NATIVE_DECLARATION_SAME_TEXT_EXACT_RAW_RETAINED'});
}
const familyPath='supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority/01_final_functions.sqlinc';
const familyWhole=rawSources.get(familyPath).toString(),familyStart=familyWhole.indexOf('create or replace function private.timesheet_contract_move_family_lock_v1(');
const familyEnd=familyWhole.indexOf('$function$;',familyWhole.indexOf('as $function$',familyStart))+11;
const family=familyWhole.slice(familyStart,familyEnd),familyRaw=Buffer.from(family);
write('native-owners/private__timesheet_contract_move_family_lock_v1.sqlinc',familyRaw);
const registry=JSON.parse(get(native,'docs/bpay-next/final-definer-closures.json'));
for(const expected of pins.finalOwners)check('final-owner registration: '+expected.signature,()=>{
 const actual=registry.chains.find(x=>x.signature===expected.signature);assert.ok(actual);for(const[k,v]of Object.entries(expected))assert.deepEqual(actual[k],v);
});
const familyOwner=registry.chains.find(x=>x.signature==='private.timesheet_contract_move_family_lock_v1(uuid)');
check('additional family-lock exact current owner',()=>{assert.equal(familyOwner.finalPhysical,familyPath);assert.equal(familyOwner.finalRoot,'supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority_v1.sql');});
write('NATIVE_OWNER_PINS.json',{commit:native,owners,familyLock:{physical:familyPath,startLine:familyWhole.slice(0,familyStart).split('\n').length,sha256:sha(familyRaw),bytes:familyRaw.length,registry:familyOwner},BankBodiesRead:0,native:'NOT_RUN'});
const preview=read('templates/20_preview.sql.tmpl'),scope=read('inputs/private__timesheet_contract_move_scope_v1.sqlinc');
const schema=rawSources.get('supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql').toString();
const operations=schema.slice(schema.indexOf('create table private.timesheet_contract_move_operations ('),schema.indexOf('create table private.timesheet_contract_move_requests ('));
const members=schema.slice(schema.indexOf('create table private.timesheet_contract_move_members ('),schema.indexOf('create table private.timesheet_contract_family_aliases ('));
const deferrable=[...operations.matchAll(/deferrable initially deferred/g)].length;
check('actual operation Candidate immediate FK',()=>{assert.match(operations,/candidate_id uuid not null references public\.candidates\(id\),/);assert.equal(deferrable,1);assert.match(operations,/to_timesheet_id uuid references public\.timesheets\(timesheet_id\) deferrable initially deferred/);});
check('actual operation immediate Timesheet and Contract FKs',()=>{
 for(const field of ['family_root_id','from_timesheet_id'])assert.match(operations,new RegExp(field+' uuid not null references public\\.timesheets\\(timesheet_id\\),'));
 for(const field of ['from_contract_id','to_contract_id'])assert.match(operations,new RegExp(field+' uuid references public\\.contracts\\(id\\),'));
});
check('actual member immediate native FK checks',()=>{assert.match(members,/from_financial_id uuid references public\.timesheets_financials\(id\),/);assert.match(members,/from_timesheet_id uuid not null references public\.timesheets\(timesheet_id\),/);});
check('operation/member insertion precedes owner/native lock stages',()=>{
 const at=['insert into private.timesheet_contract_move_operations','timesheet_contract_move_review_members_v1(','@@BANK_PREVIEW@@','timesheet_contract_move_scope_v1(o.id,true)'].map(x=>preview.indexOf(x));
 assert.ok(at.every((v,i)=>v>=0&&(i===0||v>at[i-1])));
 assert.ok(preview.indexOf("'timesheet-contract-move-request:'")>at.at(-1));
 assert.doesNotMatch(preview.slice(0,at[0]),/for update|pg_advisory_xact_lock|@@BANK_.*LOCK/);
});
check('exact native scope invokes final1003 family lock',()=>{assert.match(scope,/if p_lock then placement:=private\.timesheet_contract_move_family_lock_v1\(o\.from_timesheet_id\)/);});
check('exact Health family lock requires Candidate FOR UPDATE',()=>{assert.match(family,/from public\.candidates c where c\.id=candidate for update/);assert.match(family,/from public\.timesheets t[\s\S]*order by t\.timesheet_id for update/);});
// Source-derived conflict graph only. This is not native PostgreSQL execution.
const locks=new Map(),waiting=[];
function acquire(tx,row,mode){
 const held=locks.get(row)??[];
 const conflicts=held.filter(x=>x.tx!==tx&&(mode==='UPDATE'||x.mode==='UPDATE'));
 if(conflicts.length){waiting.push(...conflicts.map(x=>({from:tx,to:x.tx,row,requested:mode,held:x.mode})));return false;}
 held.push({tx,mode});locks.set(row,held);return true;
}
const candidate='public.candidates(actual same Candidate)';
check('adversarial two distinct Preview Candidate upgrades cycle',()=>{
 assert.equal(acquire('PREVIEW_A',candidate,'KEY_SHARE'),true);assert.equal(acquire('PREVIEW_B',candidate,'KEY_SHARE'),true);
 assert.equal(acquire('PREVIEW_A',candidate,'UPDATE'),false);assert.equal(acquire('PREVIEW_B',candidate,'UPDATE'),false);
 assert.ok(waiting.some(x=>x.from==='PREVIEW_A'&&x.to==='PREVIEW_B'));assert.ok(waiting.some(x=>x.from==='PREVIEW_B'&&x.to==='PREVIEW_A'));
});
check('no own runtime SQL or guessed producer signature',()=>{
 assert.equal(JSON.parse(read('OWNER_BLOCKS.json')).completeRuntimeEmission,'REFUSED_UNTIL_GENUINE_REVIEWED_SUCCESSOR');
 const all=['health/00_public_assert.sqlinc','health/01_projection.sqlinc','templates/10_preview_read.sql.tmpl','templates/20_preview.sql.tmpl','templates/30_current_result.sql.tmpl'].map(read).join('\n');
 assert.doesNotMatch(all,/create (?:or replace )?function private\.bpay_next_|grant .*service_role/i);
 assert.equal((all.match(/@@BANK_[A-Z_]+@@/g)??[]).length,4);
});
write('LOCK_ORDER_TRACE.json',{finding:'CONTRACT_PREVIEW_IMMEDIATE_FK_LOCK_UPGRADE',scope:'SOURCE_COUNTEREXAMPLE_NOT_NATIVE_REPRODUCTION',authorCommit:commit,physicalSchemaSha256:sha(rawSources.get('supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql')),operationCandidateFk:'candidate_id uuid not null references public.candidates(id)',nativeOwner:'private.timesheet_contract_move_family_lock_v1(uuid)',nativeOwnerRawSha256:sha(familyRaw),waitGraph:waiting,conclusion:'HOLD_TEMPLATE20_CONCURRENCY_UNQUALIFIED',BankBodyAssumptions:0,native:'NOT_RUN',requiredBoundary:'Acquire the complete actual native lock domain before any immediate business FK check; preserve exact replay and current native Bank-before-Health order, shared request mutex last. A future composition-local contract may express missing producer lock requirements, but no callable ABI or safe ordering is supplied here.'});
write('SOURCE_CHECK_RESULTS.json',{checks:results.length,results,completeFrozenFiles:captured.files.length,completeCurrentOwners:owners.length,additionalNativeFamilyOwner:1,findings:1,verdict:'HOLD_SOURCE_LOCK_ORDER',native:'NOT_RUN',dbConnections:0,rootWrites:0,OfficeWrites:0,BankBodySemanticReads:0});
console.log(JSON.stringify({checks:results.length,verdict:'HOLD_SOURCE_LOCK_ORDER',frozenFiles:captured.files.length,native:'NOT_RUN'}));
