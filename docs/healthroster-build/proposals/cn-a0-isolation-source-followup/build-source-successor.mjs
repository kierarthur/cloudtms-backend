// Read-only saved-source reconstruction. Outputs only to this adjacent packet.
// Never opens a database, credential, Worker, browser or application endpoint.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url));
const predecessor=path.resolve(here,'../cn-a0-creator-census-forward');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const scanner=fs.readFileSync(path.join(predecessor,'sql-function-source-index.mjs'),'utf8');
const oldText="function typeText(tokens){let s='';for(const t of tokens){if(t.value==='.'||t.value==='['||t.value===']'||t.value==='%')s=s.trimEnd()+t.raw;\n else if(s.endsWith('.')||s.endsWith('[')||s.endsWith('%'))s+=identityToken(t);else s+=(s?' ':'')+identityToken(t);}return s.trim();}";
const newText="// Preserve exact quoted type identity BEFORE the SQL-alias lookup.\n// This remains a finite source resolver; unknown quoted types are conservative.\nfunction typeText(tokens){let s='';for(const t of tokens){const identity=t.kind==='identifier'?t.raw:identityToken(t);\n if(t.value==='.'||t.value==='['||t.value===']'||t.value==='%')s=s.trimEnd()+t.raw;\n else if(s.endsWith('.')||s.endsWith('[')||s.endsWith('%'))s+=identity;else s+=(s?' ':'')+identity;}return s.trim();}";
assert.equal(scanner.split(oldText).length,2,'Exact predecessor scanner guard');
const proposed=scanner.replace(oldText,newText);
assert.equal(proposed.replace(newText,oldText),scanner,'Byte reversal');
fs.writeFileSync(path.join(here,'sql-function-source-index.mjs'),proposed);
const old=await import(pathToFileURL(path.join(predecessor,'sql-function-source-index.mjs')));
const next=await import(pathToFileURL(path.join(here,'sql-function-source-index.mjs')));
const root=path.resolve(process.argv[2]||'');assert.ok(process.argv[2]);
const git=(args)=>{const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(r.status,0,r.stderr);return r.stdout.trim();};
const rootHead=git(['rev-parse','HEAD']);assert.equal(rootHead,'ab26a5ff22200be037d215b1100c969d6c0bae73','Reviewed root pin');
const pins=new Map();const read=p=>{const b=fs.readFileSync(path.join(root,p));pins.set(p,{path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');};
const release=JSON.parse(read('supabase/release/current-release.json'));
const catalogue=JSON.parse(read('supabase/release/current-contract.json'));
read('scripts/cloudtms-db-release-lib.mjs');
const lib=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
const types=new Map();for(const r of [...catalogue.enums,...catalogue.relations]){types.set(r.schema+'.'+r.name,r.schema+'.'+r.name);if(r.schema==='public')types.set(r.name,r.schema+'.'+r.name);}
const context={types,defaultSchema:'public'},states=[old.emptyFunctionIndex(),next.emptyFunctionIndex()];
let createEvents=0;const unresolved=[];
function scan(file,text,lineOffset=0){for(const [i,parser] of [old,next].entries()){
 const p=parser.functionEvents(text,context);
 if(i===1){createEvents+=p.events.filter(e=>e.kind==='CREATE').length;unresolved.push(...p.unresolved.map(u=>({file,line:lineOffset+text.slice(0,u.offset).split('\n').length,kind:u.kind})));}
 parser.applyFunctionEvents(states[i],p.events,e=>({...e,file,line:lineOffset+text.slice(0,e.offset).split('\n').length,definitionSha256:e.definition?sha(e.definition):null}));}}
function expand(file,allowed,stack=[]){assert.ok(!stack.includes(file));const absolute=path.resolve(root,file),limit=path.resolve(root,allowed);assert.ok(absolute===limit||absolute.startsWith(limit+path.sep));
 const lines=read(file).split(/\r?\n/);let part='',startLine=0;
 for(let i=0;i<lines.length;i++){const m=lines[i].match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
 if(m){scan(file,part,startLine);part='';startLine=i+1;const child=path.relative(root,path.resolve(path.dirname(absolute),m[1]??m[2]??m[3])).replaceAll('\\','/');expand(child,allowed,[...stack,file]);}else part+=lines[i]+'\n';}scan(file,part,startLine);}
for(const p of release.baselineFiles||[])scan(p,read(p));
const migrations=lib.sqlFiles('supabase/migrations'),repeatables=lib.sqlFiles('supabase/repeatable');
for(const p of migrations)expand(p,'supabase');for(const p of repeatables)expand(p,'supabase/repeatable');
const identities=s=>[...s.live.values()].map(d=>({id:d.id,file:d.file,line:d.line,sourceDefinitionSha256:d.definitionSha256,renameEvents:d.renameEvents??[]})).sort((a,b)=>a.id.localeCompare(b.id));
assert.deepEqual(identities(states[0]),identities(states[1]),'Entire final source index unchanged');
const frozen=JSON.parse(fs.readFileSync(path.join(predecessor,'FIRST_CREATOR_CENSUS.json'),'utf8'));
assert.deepEqual(identities(states[1]),frozen.fullLiveFunctionIdentities,'Entire corrected frozen census remains exact');
const tokenMutations=body=>{const ts=next.sqlTokens(body),out=[];for(let i=0;i<ts.length;i++){let op,j;
 if(ts[i].kind!=='word')continue;
 if(ts[i].value==='insert'&&ts[i+1]?.value==='into'){op='INSERT';j=i+2;}else if(ts[i].value==='update'){op='UPDATE';j=i+1;}else if(ts[i].value==='delete'&&ts[i+1]?.value==='from'){op='DELETE';j=i+2;}else continue;
 if(ts[j]?.value==='only')j++;let schema='public',table=ts[j]?.value;if(ts[j+1]?.value==='.'){schema=table;table=ts[j+2]?.value;}
 if(catalogue.relations.some(r=>r.schema===schema&&r.name===table))out.push({schema,table,operation:op,bodyLine:body.slice(0,ts[i].start).split('\n').length});}return out;};
const live=states[1].live;
assert.equal([...live.values()].filter(d=>tokenMutations(d.body).some(m=>['timesheet_validations','invoice_lines','timesheets','candidates'].includes(m.table))).length,70);
const byName=new Map();for(const d of live.values()){const n=d.name.split('.').at(-1);if(!byName.has(n))byName.set(n,[]);byName.get(n).push(d);}
const routineNames=new Set([...live.values()].map(d=>d.name));
const triggerEdges=(mutations)=>mutations.flatMap(m=>catalogue.triggers.filter(t=>t.schema===m.schema&&t.table===m.table&&new RegExp('\\b'+m.operation+'\\b','i').test(t.definition.split(/\bON\b/i)[0])).map(t=>({kind:'SAVED_TRIGGER_EVENT_POTENTIAL_WHEN_NOT_EVALUATED',...m,triggerName:t.name,enabled:t.enabled,routineName:next.parseTriggerIdentity(t).routineName,definition:t.definition})));
const allNodes=new Map();
for(const d of live.values()){
 const ts=next.sqlTokens(d.body),calls=[],dynamic=[];
 for(let i=0;i<ts.length;i++){
  const t=ts[i];if(t.kind==='word'&&t.value==='execute')dynamic.push({bodyLine:d.body.slice(0,t.start).split('\n').length,kind:'DYNAMIC_EXECUTE_TARGET_NOT_RESOLVED'});
  if(!['word','identifier'].includes(t.kind))continue;let name,arityStart;
  if(ts[i+1]?.value==='.'&&['word','identifier'].includes(ts[i+2]?.kind)&&ts[i+3]?.value==='('){name=t.value+'.'+ts[i+2].value;arityStart=i+3;}
  else if(ts[i+1]?.value==='('&&ts[i-1]?.value!=='.'&&byName.has(t.value)){name=t.value;arityStart=i+1;}
  if(!name)continue;
  // A qualified relation followed by an INSERT column list is not a callee.
  if(name.includes('.')&&catalogue.relations.some(r=>r.schema+'.'+r.name===name))continue;
  const matches=name.includes('.')?[...live.values()].filter(x=>x.name===name):(byName.get(name)||[]);
  if(!matches.length){if(name.startsWith('public.')||name.startsWith('private.'))calls.push({calleeName:name,targetIds:[],bodyLine:d.body.slice(0,t.start).split('\n').length,status:'SOURCE_CALLEE_ABSENT_OR_DYNAMIC_UNRESOLVED',observedInvoked:false});continue;}
  const prefix=ts.slice(Math.max(0,i-5),i).map(t=>t.raw).join(' ');
  calls.push({calleeName:name,targetIds:matches.map(x=>x.id),bodyLine:d.body.slice(0,t.start).split('\n').length,syntaxContext:prefix,
   status:name.includes('.')?'STATIC_QUALIFIED_CALL_EXPRESSION_BRANCH_AND_OVERLOAD_UNEXECUTED':'UNQUALIFIED_CALL_CANDIDATE_SEARCH_PATH_AND_BRANCH_UNPROVED',observedInvoked:false});
 }
 const mutations=tokenMutations(d.body),locks=[];
 for(let i=0;i<ts.length;i++)if(ts[i].kind==='word'&&ts[i].value==='for'&&['update','share','key','no'].includes(ts[i+1]?.value)){
  const tail=ts.slice(i,i+8).map(t=>t.raw).join(' ');locks.push({bodyLine:d.body.slice(0,ts[i].start).split('\n').length,tokens:tail,status:'LOCK_CLAUSE_BRANCH_UNEXECUTED'});}
 allNodes.set(d.id,{id:d.id,name:d.name,file:d.file,line:d.line,definitionSha256:d.definitionSha256,calls,mutations,triggers:triggerEdges(mutations),locks,dynamic,
  isolationSettingTokens:/\b(?:default_transaction_isolation|transaction_isolation)\b/i.test(d.definition),nativeGateCallCandidates:calls.filter(c=>/gate|lock_family|rotation_scope|try_lock|advisory/.test(c.calleeName))});
}
const roots=new Set(frozen.controllingWriterDefinitions.map(d=>d.id));
for(const d of frozen.nativeOwners)roots.add(d.id);
for(const name of ['public.candidate_delete_apply','public.candidate_delete_eligibility','public.timesheet_correction_send_v1','public.timesheet_correction_prepare_v1','public.timesheet_correction_authorise_atomic_v1','public.tsfin_load_context_batch'])for(const d of live.values())if(d.name===name)roots.add(d.id);
for(const t of catalogue.triggers.filter(t=>t.schema==='public'&&['timesheet_validations','invoice_lines','timesheets','candidates','timesheets_financials','ts_fin_items','contracts'].includes(t.table))){const name=next.parseTriggerIdentity(t).routineName;for(const d of live.values())if(d.name===name)roots.add(d.id);}
const visited=new Set(),queue=[...roots];while(queue.length){const id=queue.shift();if(visited.has(id))continue;visited.add(id);const n=allNodes.get(id);if(!n)continue;
 for(const c of n.calls)queue.push(...c.targetIds);for(const t of n.triggers)for(const d of live.values())if(d.name===t.routineName)queue.push(d.id);}
const graph={contract:'A0_REACHABLE_SAVED_SOURCE_CALL_AND_TRIGGER_GRAPH_V1',status:'SOURCE_CANDIDATES_ONLY_NO_NATIVE_INVOCATION_PROOF',rootHead,roots:[...roots].sort(),nodes:[...visited].sort().map(id=>allNodes.get(id)),
 unresolved,limits:['Branch conditions, function overload argument resolution, dynamic SQL, enabled physical trigger state and effectful paths are not executed.','Unqualified calls are name candidates, not search-path binding proof.','No recorded edge is an observed native invocation.','Recursive automatic trigger edges retain WHEN/constraint/mode conditions for native qualification.'],observedNativeInvokedEdges:[]};
fs.writeFileSync(path.join(here,'SOURCE_CALLEE_LOCK_GRAPH.json'),JSON.stringify(graph,null,2)+'\n');
const proof={contract:'A0_QUOTED_TYPE_SCANNER_SUCCESSOR_PROOF_V1',status:'SOURCE_P3_FIXED_FULL_CURRENT_CENSUS_UNCHANGED_NATIVE_HELD',rootHead,
 predecessor:{path:'../cn-a0-creator-census-forward/sql-function-source-index.mjs',bytes:Buffer.byteLength(scanner),sha256:sha(scanner)},successor:{path:'sql-function-source-index.mjs',bytes:Buffer.byteLength(proposed),sha256:sha(proposed)},byteReverseEqual:true,
 exactReverse:{oldText,newText},counts:{migrationRoots:migrations.length,repeatableRoots:repeatables.length,createEvents,liveFunctions:live.size,controllingWriters:70,firstCreators:frozen.firstCreators.length,historicalWriterOccurrences:frozen.allHistoricalWriterOccurrences.length,graphRoots:roots.size,graphNodes:visited.size},
 fullFinalIdentitiesEqual:true,frozenCorrectedCensusEqual:true,frozenCensusSha256:sha(fs.readFileSync(path.join(predecessor,'FIRST_CREATOR_CENSUS.json'))),
 typeNegatives:{quoted:next.inputIdentity('(p "char")'),unquoted:next.inputIdentity('(p char)'),qualifiedQuoted:next.inputIdentity('(p pg_catalog."char")'),qualifiedUnquoted:next.inputIdentity('(p pg_catalog.char)')},
 inputs:[...pins.values()].sort((a,b)=>a.path.localeCompare(b.path))};
assert.notEqual(proof.typeNegatives.quoted,proof.typeNegatives.unquoted);assert.notEqual(proof.typeNegatives.qualifiedQuoted,proof.typeNegatives.qualifiedUnquoted);
fs.writeFileSync(path.join(here,'SCANNER_SUCCESSOR_PROOF.json'),JSON.stringify(proof,null,2)+'\n');
console.log(JSON.stringify({status:proof.status,...proof.counts,pins:pins.size,byteReverseEqual:true}));
