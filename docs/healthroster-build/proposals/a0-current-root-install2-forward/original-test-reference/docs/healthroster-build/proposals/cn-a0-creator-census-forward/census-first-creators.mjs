import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents,emptyFunctionIndex,applyFunctionEvents,inputIdentity,sqlTokens,parseTriggerIdentity} from './sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=new Map(),state=emptyFunctionIndex(),occurrences=[],unresolved=[];
function read(p){const b=fs.readFileSync(path.join(root,p));inputs.set(p,{path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');}
const release=JSON.parse(read('supabase/release/current-release.json')),catalogue=JSON.parse(read('supabase/release/current-contract.json'));read('scripts/cloudtms-db-release-lib.mjs');
const types=new Map();for(const r of [...catalogue.enums,...catalogue.relations]){types.set(r.schema+'.'+r.name,r.schema+'.'+r.name);if(r.schema==='public')types.set(r.name,r.schema+'.'+r.name);}
const context={types,defaultSchema:'public'};
function validDate(y,m,d){const v=new Date(Date.UTC(y,m-1,d));return v.getUTCFullYear()===y&&v.getUTCMonth()===m-1&&v.getUTCDate()===d;}
function key(p){const b=path.basename(p);let m=b.match(/^(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(.*)$/);if(m&&validDate(+m[1],+m[2],+m[3]))return `${m[1]}${m[2]}${m[3]}${m[4]}`;
 m=b.match(/^(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(20\d{2})(.*)$/);return m&&validDate(+m[3],+m[2],+m[1])?`${m[3]}${m[2]}${m[1]}${m[4]}`:'ZZZZZZZZ_'+b;}
function roots(dir){const g=spawnSync('git',['ls-files','-z','--',dir],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(g.status,0);
 const tracked=g.stdout.split('\0').filter(p=>p.toLowerCase().endsWith('.sql')),disk=fs.readdirSync(path.join(root,dir),{withFileTypes:true}).filter(e=>e.isFile()&&e.name.endsWith('.sql')).map(e=>dir+'/'+e.name);
 return [...new Set([...tracked,...disk])].sort((a,b)=>key(a).localeCompare(key(b))||a.localeCompare(b));}
const migrationRoots=roots('supabase/migrations'),repeatableRoots=roots('supabase/repeatable'),targetTables=['timesheet_validations','invoice_lines','timesheets','candidates'];let sequence=0;
function analyse(body){const tokens=sqlTokens(body),mutations=[],calls=new Set();
 for(let i=0;i<tokens.length;i++){const t=tokens[i];if(t.kind!=='word')continue;
  if(['public','private'].includes(t.value)&&tokens[i+1]?.value==='.'&&['word','identifier'].includes(tokens[i+2]?.kind)&&tokens[i+3]?.value==='(')calls.add(t.value+'.'+tokens[i+2].value);
  let op,index;if(t.value==='insert'&&tokens[i+1]?.value==='into'){op='INSERT INTO';index=i+2;}else if(t.value==='delete'&&tokens[i+1]?.value==='from'){op='DELETE FROM';index=i+2;}else if(t.value==='update'){op='UPDATE';index=i+1;}else continue;
  if(tokens[index]?.value==='only')index++;let table=tokens[index]?.value;
  if(table==='public'&&tokens[index+1]?.value==='.')table=tokens[index+2]?.value;
  if(targetTables.includes(table))mutations.push({table,operation:op,bodyLine:body.slice(0,t.start).split('\n').length});
 }
 const outside=tokens.map(t=>['string','dollar'].includes(t.kind)?' ':t.raw).join(' '),possibleCallees=[...calls].sort();
 return {firstCreatorTables:[...new Set(mutations.filter(m=>m.operation==='INSERT INTO').map(m=>m.table))],writerTables:[...new Set(mutations.map(m=>m.table))],mutations,
  nativeGateCalls:possibleCallees.filter(n=>/\.(weekly_source_(candidate_serial_gate|lock_family_rows|lock_and_resolve_families)_v1|_pay_workbench_candidate_serial_try_gate|_pay_timesheet_rotation_scope)$/.test(n)),
  possibleCallees,provenInvokedEdges:[],parentLockCandidates:[...outside.matchAll(/\b(?:from|join)\s+public\s*\.\s*timesheets\b[\s\S]{0,650}?for\s+(?:update|(?:key\s+)?share)(?:\s+nowait)?/gi)].map(m=>m[0]),
  dynamicExecute:tokens.some(t=>t.kind==='word'&&t.value==='execute'),dynamicLiteralWriterCandidates:tokens.filter(t=>['string','dollar'].includes(t.kind)&&/\b(?:insert\s+into|update|delete\s+from)\s+(?:public\.)?(?:timesheets|timesheet_validations|invoice_lines|candidates)\b/i.test(t.value)).map(t=>({bodyLine:body.slice(0,t.start).split('\n').length,sha256:sha(t.raw)}))};}
function scan(file,text,lineOffset=0){const parsed=functionEvents(text,context);
 applyFunctionEvents(state,parsed.events,e=>{const common={kind:e.kind,file,line:lineOffset+text.slice(0,e.offset).split('\n').length,sequence:sequence++,name:e.name,inputTypes:e.inputTypes,id:e.id};
  if(e.kind!=='CREATE')return {...common,newName:e.newName,ifExists:e.ifExists};const item={...common,header:e.header,definitionSha256:sha(e.definition),...analyse(e.body),status:'LIVE_SOURCE_EVENT_MAP_NOT_ADMISSION_OR_INSTALLED_PROOF'};
  if(item.writerTables.length)occurrences.push(item);return item;});
 for(const u of parsed.unresolved){const {statementShaInput,...meta}=u;unresolved.push({...meta,file,line:lineOffset+text.slice(0,u.offset).split('\n').length,statementSha256:statementShaInput?sha(statementShaInput):undefined});}
 for(const m of parsed.meta)if(/^\\(?:if|elif|else|endif)\b/.test(m.text))unresolved.push({file,line:lineOffset+text.slice(0,m.offset).split('\n').length,kind:'PSQL_CONDITIONAL_SOURCE_EXECUTION_NOT_INFERRED',text:m.text});
}
function expand(file,allowed,stack=[]){assert.ok(!stack.includes(file),'include cycle '+file);const absolute=path.resolve(root,file),limit=path.resolve(root,allowed);assert.ok(absolute===limit||absolute.startsWith(limit+path.sep),'include escape '+file);
 const lines=read(file).split(/\r?\n/);let part='',startLine=0;
 for(let i=0;i<lines.length;i++){const m=lines[i].match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
  if(m){scan(file,part,startLine);part='';startLine=i+1;const child=path.relative(root,path.resolve(path.dirname(absolute),m[1]??m[2]??m[3])).replaceAll('\\','/');expand(child,allowed,[...stack,file]);}
  else part+=lines[i]+'\n';}scan(file,part,startLine);}
for(const f of release.baselineFiles||[])scan(f,read(f));for(const f of migrationRoots)expand(f,'supabase');for(const f of repeatableRoots)expand(f,'supabase/repeatable');
const live=[...state.live.values()],clean=({definition,body,...d})=>d,controlling=live.filter(x=>x.writerTables.length).sort((a,b)=>a.sequence-b.sequence);
const catalogueMap=new Map(catalogue.routines.map(r=>[r.schema+'.'+r.identity.slice(0,r.identity.indexOf('('))+'('+inputIdentity(r.identity,context)+')',r]));
const reconciliation=controlling.map(d=>({id:d.id,file:d.file,line:d.line,sourceDefinitionSha256:d.definitionSha256,
 status:catalogueMap.has(d.id)?'EXACT_SIGNATURE_PRESENT_IN_SAVED_CATALOGUE':'SOURCE_FINAL_OWNER_ABSENT_FROM_SAVED_CATALOGUE',catalogue:catalogueMap.get(d.id)??null}));
const triggers=catalogue.triggers.filter(t=>t.schema==='public'&&targetTables.includes(t.table));const beforeNames=triggers.filter(t=>/\bBEFORE\b/i.test(t.definition)).map(t=>parseTriggerIdentity(t).routineName);
const extra=['private.weekly_source_candidate_serial_gate_v1','private.weekly_source_lock_family_rows_v1','public._pay_workbench_candidate_serial_try_gate','public._pay_workbench_candidate_serial_key',
 'public._pay_timesheet_rotation_scope','private.bpay_source_family_try_lock_v1','public.tsfin_load_context_batch','private.timesheet_correction_signed_witness_v1','private.timesheet_correction_origin_v1','public.timesheet_correction_send_v1'];
const nativeOwners=[...new Set([...beforeNames,...extra])].map(name=>{const matches=live.filter(d=>d.name===name);assert.equal(matches.length,1,'Exact live native owner '+name);const d=matches[0];return {...d,catalogue:catalogueMap.get(d.id)??null};});
fs.writeFileSync(path.join(here,'NATIVE_CREATOR_INPUT_BODIES.sqlinc'),nativeOwners.map(d=>'-- INPUT ONLY '+d.file+' / '+d.name+'\n'+d.definition).join('\n\n')+'\n');
fs.writeFileSync(path.join(here,'NATIVE_FIRST_CREATOR_BODIES.sqlinc'),controlling.filter(d=>d.firstCreatorTables.length).map(d=>'-- INPUT ONLY '+d.file+' / '+d.name+'\n'+d.definition).join('\n\n')+'\n');
const index=read('broker/src/index.js'),http=[];for(const m of index.matchAll(/\/rest\/v1\/(timesheet_validations|invoice_lines|timesheets|candidates)(?![_a-z])/g)){const nearby=index.slice(Math.max(0,m.index-300),m.index+900),method=nearby.match(/method:\s*['"](POST|PATCH|PUT|DELETE)['"]/i)?.[1]??'READ_OR_DELEGATED_UNKNOWN';
 if(method!=='READ_OR_DELEGATED_UNKNOWN')http.push({table:m[1],line:index.slice(0,m.index).split('\n').length,method,callSiteSha256:sha(nearby),status:'NEARBY_METHOD_SEARCH_CANDIDATE_NOT_A_PROVED_MUTATION'});}
const lifecycle=state.history.filter(e=>e.kind!=='CREATE'),dropped=state.history.filter(e=>e.kind==='DROP'&&e.removed),replaced=state.history.filter(e=>e.kind==='CREATE'&&e.replacedId);
const observed={restoreId:'public.timesheet_qr_restore_version(uuid,uuid,text,uuid)',droppedId:'public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid)'};
assert.equal(state.live.has(observed.droppedId),false,'Observed dropped overload cannot remain live');assert.ok(state.live.has(observed.restoreId));
assert.equal(state.live.get(observed.restoreId).writerTables.length,0,'Current restore wrapper cannot retain historical DML');
assert.ok(!live.some(d=>/pending|signed|--|\/\*/i.test(d.inputTypes)),'Comment-contaminated identity');
const result={contract:'HEALTHROSTER_A0_FOUR_TABLE_FIRST_CREATOR_SOURCE_CENSUS_V2',status:'CORRECTED_ORDERED_TOP_LEVEL_SOURCE_EVIDENCE_NATIVE_HELD',
 nativeOrderingInput:'scripts/cloudtms-db-release-lib.mjs',migrationRoots:migrationRoots.length,repeatableRoots:repeatableRoots.length,inputs:[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path)),
 liveFunctionCount:live.length,controllingWriterDefinitions:controlling.map(clean),firstCreators:controlling.filter(d=>d.firstCreatorTables.length).map(clean),allHistoricalWriterOccurrences:occurrences,
 lifecycleEvents:lifecycle,removedFunctionIdentities:dropped.map(e=>({id:e.removedId,file:e.file,line:e.line,sequence:e.sequence})),replacedFunctionCount:replaced.length,
 fullLiveFunctionIdentities:live.map(d=>({id:d.id,file:d.file,line:d.line,sourceDefinitionSha256:d.definitionSha256,renameEvents:d.renameEvents??[]})).sort((a,b)=>a.id.localeCompare(b.id)),
 nativeOwners:nativeOwners.map(clean),triggers,httpCandidates:http,catalogueReconciliation:reconciliation,unresolved:[...unresolved,...state.unresolved],
 observedDefectsResolved:{commentedRestore:clean(state.live.get(observed.restoreId)),droppedOverloadAbsent:true,matchingDropEvents:lifecycle.filter(e=>e.id===observed.droppedId)},
 limitations:['This complete saved-source traversal applies explicit top-level CREATE/REPLACE/DROP/RENAME/SET SCHEMA in release order. DO/dynamic/psql conditional execution is recorded, never inferred as installed DDL.',
 'Canonical signatures are compared to the saved generated catalogue. Source owners absent there remain explicitly unqualified; catalogue metadata is not fresh physical installation authority.',
 'Mutation/callee tokens are source candidates outside quoted strings. Dynamic writer literals are separate candidates; possibleCallees is never a proved invoked edge.',
 'No trigger/caller/absent-row/RR/native acceptance follows from a final source owner map. No DB/API/runtime/build/payment/email action occurred.']};
fs.writeFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({inputs:inputs.size,migrations:migrationRoots.length,repeatables:repeatableRoots.length,liveFunctions:live.length,controllingWriters:controlling.length,
 firstCreators:result.firstCreators.length,historicalWriters:occurrences.length,lifecycleEvents:lifecycle.length,removed:dropped.length,catalogueMissing:reconciliation.filter(r=>!r.catalogue).length,unresolved:result.unresolved.length,nativeOwners:nativeOwners.length}));
