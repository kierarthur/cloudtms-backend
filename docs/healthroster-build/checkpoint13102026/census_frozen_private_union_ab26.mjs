import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026';
const out=path.dirname(fileURLToPath(import.meta.url));
const commit='ab26a5ff22200be037d215b1100c969d6c0bae73';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:32000000});
if(git(['rev-parse','HEAD']).toString().trim()!==commit)throw Error('SOURCE_HEAD_DRIFT');
const inputs=new Map();
function read(file){const bytes=fs.readFileSync(path.join(root,file));inputs.set(file,{file,bytes:bytes.length,sha256:sha(bytes)});return bytes.toString('utf8');}
const scanner='docs/healthroster-build/proposals/cn-a0-creator-census-forward/sql-function-source-index.mjs';
read(scanner);
const {functionEvents}=await import(pathToFileURL(path.join(root,scanner)));
function registry(file){
 const text=read(file),part=text.split('create or replace function private._weekly_source_acl_private_helper_contract_v1()')[1]?.split('$function$;')[0];
 if(!part)throw Error('REGISTRY_SOURCE_MISSING');
 const rows=[...part.matchAll(/\('([^']+)'\)/g)].map(m=>m[1]);
 if(!rows.length||rows.some(s=>!s.startsWith('private.')))throw Error('REGISTRY_SIGNATURE_REFUSED');
 return rows;
}
function defined(file){return functionEvents(read(file)).events.filter(e=>e.kind==='CREATE'&&e.name.startsWith('private.'));}
const base=registry('supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/12_complete_private_registry_union.sqlinc');
const strictFile='docs/healthroster-build/proposals/protected-source-qualification-operational-gate/COMPLETE_CLOSURE_CENSUS.json';
const strict=JSON.parse(read(strictFile));
const additions=strict.added_private_signatures.map(row=>{
 const match=defined(row.path).filter(e=>e.id===row.signature);
 if(match.length!==1||sha(match[0].definition)!==row.definition_sha256)throw Error('STRICT_LITERAL_DEFINITION_DRIFT');
 return row.signature;
});
const prepared=registry('docs/healthroster-build/proposals/report-prepared-output-retention/08_complete_registry_union.sqlinc');
const oldA0=['canonical-input-assert.sqlinc','13102026_2034_clock_scope.complete-proposed.sqlinc','13102026_2034_a0_native_input_witness.sqlinc','13102026_2034_a0_native_lock_fence.sqlinc']
 .flatMap(file=>defined('docs/healthroster-build/proposals/cn-a0-lock-fence/'+file).map(e=>e.id));
const creator=['13102026_2153_native_creator_intents.sqlinc','13102026_2153_send_family_fence.sqlinc']
 .flatMap(file=>defined('docs/healthroster-build/proposals/cn-a0-phantom-writers/'+file).map(e=>e.id));
const normal=s=>s.replace(/\s+/g,'');
const baseKeys=new Set(base.map(normal)),strictKeys=new Set(strict.proposed_complete_private_signature_union.map(normal));
if(base.length!==232||additions.length!==26||prepared.length!==239||oldA0.length!==4||creator.length!==6)throw Error('PACKAGE_CENSUS_DRIFT');
if(base.some(s=>!strictKeys.has(normal(s)))||prepared.filter(s=>baseKeys.has(normal(s))).length!==232)throw Error('PRESERVED_BASE_SIGNATURE_LOST');
if(new Set([...base,...additions].map(normal)).size!==258||[...base,...additions].some(s=>!strictKeys.has(normal(s))))throw Error('STRICT_UNION_DRIFT');
const groups={preservedRoot:base,strictQualificationAdditions:additions,preparedAdditions:prepared.filter(s=>!baseKeys.has(normal(s))),missingA0NormalizationAndFence:oldA0,creatorAdmission:creator};
const union=new Map();
for(const [group,rows]of Object.entries(groups))for(const signature of rows){
 const key=normal(signature);if(!union.has(key))union.set(key,{signature,groups:[]});union.get(key).groups.push(group);
}
if(union.size!==275)throw Error('DISTINCT_LITERAL_UNION_DRIFT');
if([...union.values()].some(row=>/\b(?:char|character)\b/.test(row.signature)))throw Error('UNREVIEWED_QUOTED_TYPE_BOUNDARY');
for(const pin of inputs.values()){
 const bytes=git(['show',commit+':'+pin.file]);
 if(bytes.toString('utf8').replace(/\r\n/g,'\n')!==fs.readFileSync(path.join(root,pin.file),'utf8').replace(/\r\n/g,'\n'))throw Error('GIT_INPUT_SOURCE_DRIFT');
 pin.gitBlobBytes=bytes.length;pin.gitBlobSha256=sha(bytes);
}
const result={status:'FROZEN_SOURCE_LITERAL_UNION_ONLY_NOT_FINAL_OR_INSTALLED_CENSUS',sourceCommit:'ab26a5ff22200be037d215b1100c969d6c0bae73',
 groupCounts:Object.fromEntries(Object.entries(groups).map(([k,v])=>[k,v.length])),distinctPrivateSignatures:union.size,
 signatures:[...union.values()].sort((a,b)=>a.signature.localeCompare(b.signature)),inputs:[...inputs.values()],
 scope:'Preserved root232 plus26 exact strict definitions plus7 prepared plus4 prior missing-A0 normalization/fence dependencies plus6 creator definitions. Current SECURITY/OFF-rule and Report A/B follow-ons, new Banking source and any subsequent owner/helper changes are outside this finite union. Root must reconstruct literal final registries and independent verifiers from the complete reviewed composition; do not replace current registries with a frozen smaller package or infer a final count from this arithmetic.',
 parserBoundary:'Uses the current corrected finite source scanner; its separate quoted char P3 forward is in progress. None of these literal signature inputs contains that type. This is not a general PostgreSQL type resolver or installed type identity proof.',
 executableRootChanges:0,databaseCalls:0,newServiceGrants:0,newTables:0,nativeQualification:false};
const target=path.join(out,'FROZEN_PRIVATE_SIGNATURE_UNION_AB26_GIT_PINNED.json');
if(fs.existsSync(target))throw Error('EXISTING_CENSUS_REFUSED');
fs.writeFileSync(target,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,groupCounts:result.groupCounts,distinctPrivateSignatures:union.size,inputs:inputs.size,sha256:sha(fs.readFileSync(target))}));
