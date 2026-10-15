// Complete source convergence only. No database, remote application, or install action.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {functionEvents, splitSqlStatements, stripSqlComments, sqlTokens} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),P='docs/healthroster-build/proposals/';
const pins={root:'62b4a2e1ce4fff6957a9b2d8183aada46a9a20a9',runtime_root:'9d2873bd9180acd8588a44a6223d70bcf6403310',money:'73488a9f0178ca4d7a3510529cc4570c1739534d',foundation:'228268c9c834faada1fea61a079e555e4dbba093',report:'ffaf93b0b62c6693aa3d90333d39bec48714bca9',native:'c7fcb2519f9d3b93f29e580df6e6cb10c6d30322',shared:'a8c32b4fb1beb3de09b58a460aec7abaf8153195',report_successor:'4822cfc767fda9a57a0de34b9b9beee764097a14'};
const sha=b=>createHash('sha256').update(b).digest('hex'),git=(a,o={})=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...a],{cwd:root,maxBuffer:80e6,...o});
const cache=new Map(),get=(c,f)=>{const k=c+':'+f;if(!cache.has(k))cache.set(k,git(['show',k]).toString());return cache.get(k);};
const outputs=new Map(),write=(f,b)=>outputs.set(f,b),json=(f,v)=>write(f,JSON.stringify(v,null,2)+'\n');
const inputs=[],owners=new Map(),history=[],triggers=new Map(),once=[],onceIds=new Set();
function read(c,file){const raw=get(c,file);let index=inputs.findIndex(x=>x.commit===c&&x.file===file);if(index<0){index=inputs.length;inputs.push({commit:c,file,bytes:Buffer.byteLength(raw),sha256:sha(raw)});}return {raw,index};}
function select(c,file,layer,{oneTime=false,allowOther=false}={}){
 const {raw,index}=read(c,file),parsed=functionEvents(raw);assert.equal(parsed.unresolved.length,0,file);assert.ok(parsed.events.every(e=>e.kind==='CREATE'),file);
 for(const e of parsed.events){const old=owners.get(e.id),item={...e,input_index:index,layer,definition_sha256:sha(e.definition)};history.push({id:e.id,layer,input_index:index,replaces_input_index:old?.input_index??null,identical:!!old&&old.definition_sha256===item.definition_sha256});owners.set(e.id,item);if(oneTime)onceIds.add(e.id);}
 if(oneTime){const out='one-time/'+String(once.length).padStart(2,'0')+'_'+path.posix.basename(file);write(out,raw);once.push({input_index:index,file:out,atomic:true});return;}
 for(const s of splitSqlStatements(raw).statements){const q=stripSqlComments(s.text).trim();if(!q||/^create\s+(?:or\s+replace\s+)?function\b/i.test(q)||/^(?:alter function|revoke\b|grant\b|notify\b)/i.test(q))continue;const t=q.match(/^create\s+(?:or\s+replace\s+)?trigger\s+(\w+)\b/i);if(t){triggers.set(t[1],{name:t[1],statement:s.text.trim(),input_index:index});continue;}assert.ok(allowOther,'UNCLASSIFIED_STATEMENT '+file+' '+q.slice(0,130));}
}
function forward(packet,names){for(const name of names)select(pins.foundation,P+packet+'/forward/'+name,'FOUNDATION');}
function all(packet,exclude=[]){const pref=P+packet+'/forward/';const files=git(['ls-tree','-r','--name-only',pins.foundation,'--',pref]).toString().trim().split('\n').filter(f=>f.endsWith('.sqlinc')&&!exclude.includes(path.posix.basename(f))).sort();assert.ok(files.length,packet);for(const f of files)select(pins.foundation,f,'FOUNDATION');}
// One-time whole atomic pages precede all consumers. Never replay the duplicate
// Keep04 schema or split the reserved namespace check from its guards/index.
select(pins.foundation,P+'kept-pay-identical-rekey-current-root-forward-20261014/forward/00_complete_one_time_kept_decisions.sqlinc','FOUNDATION_SCHEMA',{oneTime:true});
select(pins.foundation,P+'missing-default-current-native-proposal-20261014/forward/04_cancel_exclusion_schema.sqlinc','FOUNDATION_SCHEMA',{oneTime:true});
select(pins.foundation,P+'missing-accept-current-native-construction-20261014/one-time/01_complete_reserved_namespace_guard.sqlinc','FOUNDATION_SCHEMA',{oneTime:true});
all('kept-pay-identical-rekey-current-root-forward-20261014',['00_complete_one_time_kept_decisions.sqlinc','04_kept_as_paid_decisions_schema.sqlinc']);
all('kept-credit-batch-repeatable-successor-20261014');
all('kept-applied-correction-context-successor-20261014');
all('missing-default-current-native-proposal-20261014',['04_cancel_exclusion_schema.sqlinc','06_complete_fin4_default_draft.sqlinc']);
all('missing-retained-snapshot-mapping-successor-20261014');
all('missing-correction-baseline-successor-20261014');
all('missing-correction-expanded-native-scope-successor-20261014');
all('missing-revision-census-guard-successor-20261014');
all('missing-accept-current-native-construction-20261014',['02_missing_accept_proof_guard.sqlinc']);
all('missing-accept-head-continuity-construction-20261014',['01_complete_current_removal.sqlinc']);
all('missing-accept-deferred-anchor-successor-20261014');
all('rekey-source-alias-current-native-successor-20261014',['00_known_source_key.sqlinc']);
all('rekey-source-alias-all-owner-successor-20261014');
const foundationIds=[...owners.keys()];
const moneyFile=P+'money-agency-current-off-module-successor-20261015/SELECTED_CURRENT_MONEY_MODULE_RECIPE.json',money=JSON.parse(read(pins.money,moneyFile).raw);
for(const x of money.one_time_sources_once_only)select(x.commit,x.file,'MONEY_SCHEMA',{oneTime:true});
for(const x of money.selected_complete_files_once_only)select(x.commit,x.file,'MONEY');
// The selected Money recipe references the current three guards separately.
select('c4435202881de34dadba27f308e2c2a2737fd4a2',P+'paid-match-nhsp-correction-scope-prepared-construction-20261015/module/98_paid_choice_revision_triggers.sqlinc','MONEY');
for(const id of onceIds){const e=owners.get(id),src=inputs[e.input_index],raw=get(src.commit,src.file);const initial=once.flatMap(x=>functionEvents(outputs.get(x.file)).events).find(x=>x.id===id);assert.ok(initial);assert.equal(sha(initial.definition),sha(e.definition),'one-time routine diverged '+id);}
// External complete modules are inspected, not silently stacked over the final
// Money owner. The exact census records every cross-lane overlap.
const reportPath=P+'agency-once-only-current-module-20261015/01_complete_current_agency_runtime.sqlinc',reportRaw=read(pins.report,reportPath).raw;
const nativeRecipePath=P+'checklist-legacy-hold-retention-20261015/CURRENT_RECIPE_SUCCESSOR.json',nativeRecipe=JSON.parse(read(pins.native,nativeRecipePath).raw);
const external=new Map(),conflicts=[];
function externalSource(c,file,layer){const {raw,index}=read(c,file);const parsed=functionEvents(raw);assert.equal(parsed.unresolved.length,0,file);for(const e of parsed.events.filter(x=>x.kind==='CREATE')){const item={...e,input_index:index,layer,definition_sha256:sha(e.definition)},old=external.get(e.id)||owners.get(e.id);if(old)conflicts.push({id:e.id,previous_layer:old.layer,incoming_layer:layer,previous_sha256:old.definition_sha256,incoming_sha256:item.definition_sha256,identical:old.definition_sha256===item.definition_sha256,previous_input_index:old.input_index,incoming_input_index:index});external.set(e.id,item);}}
externalSource(pins.report,reportPath,'REPORT');
for(const t of nativeRecipe.targets.filter(t=>/FUNCTION_OWNER/.test(t.kind)))externalSource(pins.native,path.posix.normalize(path.posix.join(path.posix.dirname(nativeRecipePath),t.output)),'NATIVE');
json('CROSS_LANE_OWNER_CENSUS.json',{source_only:true,pins,selected_money_foundation:owners.size,external_functions:external.size,overlaps:conflicts,nonidentical:conflicts.filter(x=>!x.identical),root_owns_final_cross_lane_union:true});
const sharedPath=P+'checklist-final-shared-owner-reconciliation-20261015/',sharedMeta=JSON.parse(read(pins.shared,sharedPath+'COMPLETE_SHARED_OWNER_RECONCILIATION.json').raw);
for(const f of sharedMeta.functions){const{raw,index}=read(pins.shared,sharedPath+f.output),e=functionEvents(raw).events.find(e=>e.id===f.id);assert.ok(e);assert.equal(sha(e.definition),f.definition_sha256);assert.ok(external.has(e.id));external.set(e.id,{...e,input_index:index,layer:'SHARED_NATIVE_REPORT',definition_sha256:f.definition_sha256});}
const finalAgencyPath=sharedPath+sharedMeta.Report_complete_runtime_successor,finalAgency=read(pins.shared,finalAgencyPath).raw;for(const e of functionEvents(finalAgency).events.filter(e=>e.kind==='CREATE')){const current=external.get(e.id);assert.ok(current);assert.equal(e.definition.replaceAll('\r\n','\n'),current.definition.replaceAll('\r\n','\n'),'final Agency owner semantics '+e.id);}
const routingPath=P+'agency-routing-protection-current-source-forward-20261015/',routingSources=[];
for(const name of ['132_complete_capture_with_native_agency_admission.sqlinc','133_complete_request_assert_without_source_router_gate.sqlinc','134_complete_current_authorise_router_with_agency_ordinary_route.sqlinc','136_complete_current_protected_match_with_actual_agency_policy.sqlinc','135_same_protection_identity_guard.sqlinc']){const file=routingPath+'forward/'+name;externalSource(pins.report_successor,file,'REPORT_ROUTING_PROTECTION');routingSources.push(read(pins.report_successor,file).raw);}
// Emit the actual complete financial foundation + Money module once. External
// overlap is explicit; no conflicting historical module may run afterward.
const byName=new Map();for(const e of owners.values()){const ids=byName.get(e.name)||[];ids.push(e.id);byName.set(e.name,ids);}
function calls(e){const ts=sqlTokens(e.body),names=new Set();for(let i=0;i<ts.length-3;i++){const[a,b,c,d]=ts.slice(i,i+4);if(['public','private'].includes(a.value)&&b.value==='.'&&c.kind==='word'&&d.value==='('&&!['into','update','references'].includes(ts[i-1]?.value))names.add(a.value+'.'+c.value);}return [...names];}
const pending=new Map([...owners].filter(([id])=>!onceIds.has(id))),ordered=[];
while(pending.size){const ready=[...pending.values()].filter(e=>!(/\blanguage\s+sql\b/i.test(e.header)&&calls(e).flatMap(n=>byName.get(n)||[]).some(id=>id!==e.id&&pending.has(id))));assert.ok(ready.length,'SQL constructor dependency cycle');for(const e of ready){ordered.push(e);pending.delete(e.id);}}
const selected=[],acl=[],sql=['-- Complete selected Keep/Missing/Accept/rekey + current Money owners, once.'];
for(const e of ordered){sql.push(e.definition);const {definition,body,header,offset,kind,...m}=e;selected.push({...m,emitted_stage:'REPEATABLE'});}
for(const id of onceIds){const e=owners.get(id),{definition,body,header,offset,kind,...m}=e;selected.push({...m,emitted_stage:'ONE_TIME_ATOMIC'});}
for(const e of owners.values()){acl.push('alter function '+e.id+' owner to postgres;','revoke all on function '+e.id+' from public,anon,authenticated,service_role;');if(e.name.startsWith('public.'))acl.push('grant execute on function '+e.id+' to service_role;');}
write('01_complete_money_foundation_runtime.sqlinc',sql.join('\n\n')+'\n');write('02_complete_money_foundation_guards.sqlinc',[...triggers.values()].map(x=>x.statement).join('\n\n')+'\n');write('03_selected_owner_acl_and_schema_reload.sqlinc',acl.join('\n')+"\nnotify pgrst, 'reload schema';\n");
const inv=JSON.parse(get(pins.foundation,P+'money-current-prerequisite-closure-construction-20261015/CURRENT_PREREQUISITE_LITERAL_INVENTORY.json')),rootNames=new Set(inv.canonical_routines.map(x=>x.name)),externalNames=new Set([...external.values()].map(x=>x.name)),tables=new Set();
for(const inp of inputs)for(const m of get(inp.commit,inp.file).matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?((?:public|private)\.[a-z_][a-z0-9_]*)/ig))tables.add(m[1].toLowerCase());
const missing=new Map();for(const e of owners.values())for(const n of calls(e))if(!byName.has(n)&&!rootNames.has(n)&&!externalNames.has(n)&&!tables.has(n)){const xs=missing.get(n)||[];xs.push(e.id);missing.set(n,xs);}
// Whole current owner reversals are derived from the actual root literal owner,
// never from an older proposal preimage. Each saved before is a complete body.
const reversals=[],sourceDifferences=[];let beforeSequence=0;
for(const e of owners.values()){
 const prior=inv.canonical_routines.find(x=>x.id===e.id);if(!prior){reversals.push({id:e.id,kind:'NEW_FUNCTION',after_sha256:e.definition_sha256,reverse_statement:'drop function '+e.id+';'});continue;}
 const {raw,index}=read(pins.root,prior.file),old=functionEvents(raw).events.filter(x=>x.kind==='CREATE'&&x.id===e.id).at(-1);assert.ok(old,'missing exact current literal '+e.id);
 const file='before/'+String(++beforeSequence).padStart(3,'0')+'_'+e.name.replaceAll('.','_')+'.sqlinc';write(file,old.definition+'\n');
 reversals.push({id:e.id,kind:sha(old.definition)===e.definition_sha256?'IDENTICAL_CURRENT':'WHOLE_CURRENT_OWNER',current_input_index:index,before_file:file,before_sha256:sha(old.definition),after_sha256:e.definition_sha256});
 const afterCalls=new Set(calls(e));sourceDifferences.push({id:e.id,current_owner:prior.file,removed_literal_call_names:calls(old).filter(x=>!afterCalls.has(x)),added_literal_call_names:calls(e).filter(x=>!calls(old).includes(x))});
}
json('WHOLE_CURRENT_OWNER_REVERSES.json',{source_only:true,root:pins.root,whole_function_reversals:reversals,new_functions:reversals.filter(x=>x.kind==='NEW_FUNCTION').length,changed_current_functions:reversals.filter(x=>x.kind==='WHOLE_CURRENT_OWNER').length,one_time_pages_rollback:'Server-owned rehearsal rollback only. No destructive durable data/schema undo is authorised or emitted.'});
json('CURRENT_LITERAL_CALL_DIFFERENCES.json',{source_only:true,root:pins.root,functions:sourceDifferences});
// Preserve every current registry row, including private service exceptions.
// Add the exact selected interface set instead of replaying a historical whole
// ACL file that could erase later Source/A0/Banking authority.
const registryPath='supabase/repeatable/14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc';
const rootRegistry=functionEvents(read(pins.root,registryPath).raw).events.filter(e=>e.kind==='CREATE'&&/^private\._weekly_source_acl_(?:table|lifecycle_column|private_helper|service_rpc)_contract_v1$/.test(e.name));assert.equal(rootRegistry.length,4);
const nativeAcl=nativeRecipe.targets.find(t=>t.output.endsWith('central_acl_inventory.sqlinc'));assert.ok(nativeAcl);
const nativeRegistry=functionEvents(read(pins.native,path.posix.normalize(path.posix.join(path.posix.dirname(nativeRecipePath),nativeAcl.output))).raw).events.filter(e=>e.kind==='CREATE');
const registryRows=new Map(),registryDefinitions=[];
for(const old of rootRegistry){const rows=new Map(),rowText=e=>[...e.body.matchAll(/\(\s*'([^']+)'\s*(?:,\s*'([^']+)'\s*(?:,\s*(\d+)\s*)?)?\)/g)].map(m=>({key:m[1]+(m[3]?'\0'+m[2]:''),values:m[3]?[m[1],m[2],Number(m[3])]:m[2]?[m[1],m[2]]:[m[1]]}));assert.ok(rowText(old).length);for(const src of [old,...nativeRegistry.filter(e=>e.id===old.id)])for(const row of rowText(src)){if(rows.has(row.key))assert.deepEqual(rows.get(row.key),row.values,'registry row conflict '+old.name+' '+row.key);rows.set(row.key,row.values);}registryRows.set(old.name,rows);}
const serviceName='private._weekly_source_acl_service_rpc_contract_v1',privateName='private._weekly_source_acl_private_helper_contract_v1';
const serviceRows=registryRows.get(serviceName),privateRows=registryRows.get(privateName);assert.ok(serviceRows&&privateRows);
for(const e of [...owners.values(),...external.values()])if(e.name.startsWith('public.'))serviceRows.set(e.id,[e.id]);
for(const e of [...owners.values(),...external.values()])if(e.name.startsWith('private.')&&!serviceRows.has(e.id))privateRows.set(e.id,[e.id]);
registryRows.get('private._weekly_source_acl_table_contract_v1').set('weekly_source_kept_as_paid_decisions',['weekly_source_kept_as_paid_decisions','IMMUTABLE_APPEND_ONLY']);
for(const old of rootRegistry){const rows=[...registryRows.get(old.name).values()].sort((a,b)=>a.join('\0').localeCompare(b.join('\0')));const body='\n  values\n'+rows.map(row=>'    ('+row.map(x=>typeof x==='number'?String(x):"'"+x.replaceAll("'","''")+"'").join(', ')+')').join(',\n')+';\n';assert.equal(old.definition.split(old.body).length,2);const definition=old.definition.replace(old.body,body);registryDefinitions.push(definition+'\nalter function '+old.id+' owner to postgres;\nrevoke all on function '+old.id+' from public,anon,authenticated,service_role;');}
write('04_complete_current_registry_union.sqlinc',registryDefinitions.join('\n\n')+'\n');
json('CURRENT_REGISTRY_UNION_PINS.json',{source_only:true,root:pins.root,root_registry_input:registryPath,complete_function_count:rootRegistry.length,signature_rows:{private:privateRows.size,service:serviceRows.size},all_current_rows_preserved:true,private_service_exceptions_preserved:[...serviceRows.keys()].filter(x=>x.startsWith('private.')),Banking95_28_not_replaced_by_registry:true,native_and_Agency_targets_are_dependencies_not_mounted_by_this_file:true});
// Definitive once-only cross-lane function union. The two native Candidate
// predecessors are replaced by the sole a8c complete shared successors; the six
// identical OFF owners appear once. Keep every actual Agency registration/guard
// statement separately, including table-qualified repeated trigger names.
const combined=new Map(owners);for(const [id,e]of external){const old=combined.get(id);if(old)assert.equal(old.definition_sha256,e.definition_sha256,'unresolved final financial overlap '+id);combined.set(id,e);}
const allNames=new Map();for(const e of combined.values()){const ids=allNames.get(e.name)||[];ids.push(e.id);allNames.set(e.name,ids);}
const allPending=new Map([...combined].filter(([id])=>!onceIds.has(id))),allOrdered=[];
while(allPending.size){const ready=[...allPending.values()].filter(e=>!(/\blanguage\s+sql\b/i.test(e.header)&&calls(e).flatMap(n=>allNames.get(n)||[]).some(id=>id!==e.id&&allPending.has(id))));assert.ok(ready.length,'cross-lane SQL constructor dependency cycle');for(const e of ready){allOrdered.push(e);allPending.delete(e.id);}}
write('05_complete_current_money_report_native_runtime.sqlinc','-- FINAL COMPLETE FUNCTION UNION. Mutually exclusive with component runtime modules.\n'+allOrdered.map(e=>e.definition).join('\n\n')+'\n');
const allAcl=[];for(const e of combined.values()){allAcl.push('alter function '+e.id+' owner to postgres;','revoke all on function '+e.id+' from public,anon,authenticated,service_role;');if(serviceRows.has(e.id))allAcl.push('grant execute on function '+e.id+' to service_role;');}
write('06_complete_current_union_acl.sqlinc',allAcl.join('\n')+"\nnotify pgrst, 'reload schema';\n");
const agencyStatements=[finalAgency,...routingSources].flatMap(raw=>splitSqlStatements(raw).statements.filter(s=>{const q=stripSqlComments(s.text).trim();return q&&!/^(?:create\s+(?:or\s+replace\s+)?function\b|alter function\b|revoke\b|grant\b|notify\b)/i.test(q);}).map(s=>s.text.trim()));
write('07_complete_current_agency_registration_and_guards.sqlinc',agencyStatements.join('\n\n')+'\n');
json('ONCE_ONLY_SHARED_MODULE_RECIPE.json',{source_only:true,pins,canonical_mounted:false,SQL_compiled:false,native_calls:0,selected_functions:combined.size,repeatable_function_declarations:allOrdered.length,atomic_one_time_functions:onceIds.size,selected_definitions:[...combined.values()].map(({id,name,inputTypes,definition_sha256,input_index,layer})=>({id,name,inputTypes,definition_sha256,input_index,layer})),one_time_money:once,one_time_Agency:{commit:pins.report,file:P+'agency-once-only-current-module-20261015/03_complete_once_only_agency_schemas.sqlinc'},one_time_Agency_protection:{commit:pins.report_successor,file:routingPath+'migration/01_same_planned_check_only_protection_store.sqlinc'},one_time_native_hold_release:'Select the Native lane complete genuine schema prerequisite successor before ANY registration trigger or current hold-release consumer; current root42P01 trial is not passed.',final_repeatable_order:['05_complete_current_money_report_native_runtime.sqlinc','07_complete_current_agency_registration_and_guards.sqlinc','02_complete_money_foundation_guards.sqlinc','04_complete_current_registry_union.sqlinc','06_complete_current_union_acl.sqlinc'],component_runtime_files_EXCLUDED:['01_complete_money_foundation_runtime.sqlinc','03_selected_owner_acl_and_schema_reload.sqlinc','Report94/97 complete runtime','Native50/48 complete runtime include roots','older Money71 complete module'],shared_Candidate_owners:pins.shared,Native_D10_current_successor_selected:true,Money_Report_OFF_six_identical_deduplicated:true,Agency_nonfunction_statements_preserved:agencyStatements.length,exact_current_Worker_complete_union:'CURRENT_RUNTIME_COMPLETE_REVERSES.json',preserve_current_index_and_contract_module:pins.runtime_root,Office_outer_wrapper:'Root selects reviewed current Native finite wrapper after the selected complete Report/D10+Money inner public workspace once; no older public workspace may run after the wrapper.',global_registry_final_current_rows_preserved:true,required_final_native_API_role_census_Office_contract_and_business_gates_unrun:true});
const recipe={source_only:true,pins,canonical_mounted:false,SQL_compiled:false,native_calls:0,full_primary_complete:false,foundation_full_owner_selection_not_helper_subset:true,one_time_atomic_files_once_only:once,repeatable_files_once_only:['01_complete_money_foundation_runtime.sqlinc','02_complete_money_foundation_guards.sqlinc','03_selected_owner_acl_and_schema_reload.sqlinc'],current_registry_union_after_all_external_owners:'04_complete_current_registry_union.sqlinc',selected_complete_owners_once_only:selected,foundation_declared_before_money:foundationIds,selected_trigger_count:triggers.size,actual_service_signatures:selected.filter(x=>x.name.startsWith('public.')).map(x=>x.id),owner_only_private_signatures:selected.filter(x=>x.name.startsWith('private.')).map(x=>x.id),external_literal_names_not_in_selected_or_root_or_current_external_modules:[...missing].map(([name,callers])=>({name,callers})),cross_lane_overlaps:'CROSS_LANE_OWNER_CENSUS.json',provenance_is_not_install_order:true,excluded_duplicate_schema:'Keep04 is already in complete Keep00; Missing namespace proof guard emitted only in its atomic one-time page.',excluded_superseded_whole_owners:['Missing06 FIN4','Missing09 capture transition','Missing15/16/17 prepared owners','continuity01 removal','rekey00 known source key','older Money OFF whole bodies','Report04/05/07 admission whole predecessor','Native admission whole predecessor extraction'],Office:'Complete Report/D10+Money inner workspace selected once; final reviewed Native outer wrapper remains a separate root-owned exact finite composition. Do not reapply this public inner owner after that wrapper.',global_registry:'Exact complete root+Native+Report+Money registry union is emitted; preserve the Bank95/28 consumer files and original nested private service exceptions.',no_DB_or_application_claim:true};
json('CURRENT_MONEY_FOUNDATION_RECIPE.json',recipe);json('COMPLETE_INPUT_AND_OWNER_PROVENANCE.json',{source_only:true,inputs,selection_history:history,triggers:[...triggers.values()].map(({statement,...x})=>({...x,sha256:sha(statement)}))});
for(const [f,b]of outputs){const target=path.join(dir,f);if(process.argv.includes('--verify'))assert.equal(fs.readFileSync(target,'utf8'),b,f);else{assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,b);}}
console.log(JSON.stringify({source_only:true,selected:owners.size,foundation:foundationIds.length,once:once.length,triggers:triggers.size,missing:[...missing.keys()],overlaps:conflicts.length,nonidentical:conflicts.filter(x=>!x.identical).map(x=>x.id)}));
