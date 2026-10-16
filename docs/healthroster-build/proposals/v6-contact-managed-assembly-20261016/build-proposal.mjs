// Source-only proposal generation. Reads committed source; never installs or edits I2.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const i2=path.resolve(repo,'../healthroster-install2-14102026');
const producerCommit='e4661d28219b85c66b854d99c6522b55aae33139';
const rootCommit='32ee8711f54829b5a13a0c59dabc9c29a39a2aa3';
const sourceBase='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/';
const registryPath='docs/bpay-next/final-definer-closures.json';
const reviewCommit='416e0592aeafa8ddcec99bffe06719dbfd945214';
const rootSql='supabase/repeatable/16102026_1210_weekly_contact_producer_completion_v1.sql';
const leafDir='supabase/repeatable/16102026_1210_weekly_contact_producer_completion';
const prerequisiteCommit='18f844f1c01c7d1cf293bc4e92a2ab7e0fb762eb';
const prerequisiteSource='docs/healthroster-build/proposals/source-contact-recomposition-native-20261016/';
const prerequisiteRoot='supabase/repeatable/16102026_1209_weekly_contact_producer_prerequisites_v1.sql';
const prerequisiteDir='supabase/repeatable/16102026_1209_weekly_contact_producer_prerequisites';
const guardSource='docs/healthroster-build/proposals/contact-manual-selection-ordinal-guard-20261016/COMPLETE_GUARD_SUCCESSOR.sqlinc';
const guardTarget=leafDir+'/98_manual_selection_ordinal_guard.sqlinc';
const correctedRecipeSource='docs/healthroster-build/v6-owner-delivery-20261016/COMPLETENESS_SOURCE_AUDIT_20261016/CONTACT_SOURCE_COMPLETION_RECIPE_REVIEWED_SUCCESSOR.json';
const sha=value=>createHash('sha256').update(value).digest('hex');
const lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
function git(args,maxBuffer=60_000_000){const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:repo,maxBuffer,windowsHide:true});assert.equal(r.status,0,r.stderr.toString());return r.stdout;}
const blob=(commit,file)=>git(['show',`${commit}:${file}`]);
function write(file,content){fs.mkdirSync(path.dirname(path.join(here,file)),{recursive:true});fs.writeFileSync(path.join(here,file),content);}
const json=(file,document)=>write(file,JSON.stringify(document,null,2)+'\n');
function input(commit,file){const bytes=blob(commit,file);return {commit,path:file,bytes,sha256:sha(bytes)};}
// Parser tooling is source-only and must still equal the exact captured root version.
for(const file of ['scripts/cloudtms-managed-release-sql.mjs','scripts/bpay-next-final-definers.mjs'])
 assert.equal(sha(fs.readFileSync(path.join(i2,file))),sha(blob(rootCommit,file)),'PARSER_TOOLING_DRIFT '+file);
const {scanManagedSql,readManagedClosure,exactAtomicBody}=await import(pathToFileURL(path.join(i2,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(i2,'scripts/bpay-next-final-definers.mjs')));
const recipeInput=input(producerCommit,sourceBase+'SOURCE_COMPLETION_RECIPE.json');
const frozenRecipe=JSON.parse(recipeInput.bytes),recipeSuccessorInput=input(rootCommit,correctedRecipeSource);
const correctedRecipe=JSON.parse(recipeSuccessorInput.bytes),oldAcl=frozenRecipe.assembly.repeatable.at(-1).sha256;
assert.equal(oldAcl,'87e01a782adb992b8f63d0cfabbcc4ecd0f32d6da6c3cd665b38d4596675da98');
const undoneRecipe=structuredClone(correctedRecipe);undoneRecipe.assembly.repeatable.at(-1).sha256=oldAcl;
assert.deepEqual(undoneRecipe,frozenRecipe,'RECIPE_ONLY_ONE_FIELD');
write('metadata/CONTACT_SOURCE_COMPLETION_RECIPE_REVIEWED_SUCCESSOR.json',recipeSuccessorInput.bytes);
const runtimeInput=input(producerCommit,sourceBase+'RUNTIME_CONTRIBUTION.json');
const frozenRuntime=JSON.parse(runtimeInput.bytes),runtime=structuredClone(frozenRuntime);
const runtimeLeafInput=input(producerCommit,sourceBase+runtime.leaf.source_path);
assert.equal(sha(runtimeLeafInput.bytes),'6bf1eed7b3dd86b984217c91b37a4a8d6b786511f712ea6daa87cf8f0a41e8ff');
runtime.leaf.sha256=sha(runtimeLeafInput.bytes);
const undoneRuntime=structuredClone(runtime);undoneRuntime.leaf.sha256=frozenRuntime.leaf.sha256;
assert.deepEqual(undoneRuntime,frozenRuntime,'RUNTIME_METADATA_ONLY_ONE_FIELD');
json('metadata/RUNTIME_CONTRIBUTION_REVIEWED_SUCCESSOR.json',runtime);
const sourcePins=[],payloads=[],sourceDefinitions=[];
const migrationNames=['16102026_1200_weekly_contact_producer_journals.sql','16102026_1201_weekly_contact_automatic_followup_tranche.sql','16102026_1202_weekly_contact_submission_cohort_identity.sql'];
function definitions(text,file){return scanManagedSql(text).statements.flatMap(statement=>{
 const sql=text.slice(statement.start,statement.end).trim();return routineDefinitions(sql,file).map(signature=>({
  signature,physical:file,statementSha256:sha(sql),line:text.slice(0,statement.start).split('\n').length,
  language:/\blanguage\s+sql\b/i.test(sql.slice(0,sql.indexOf('as $')))?'SQL':'PLPGSQL',sql}));
});}
for(const [index,row] of correctedRecipe.assembly.one_time.entries()){
 const source=input(producerCommit,sourceBase+row.path);assert.equal(source.sha256,row.sha256);
 assert.equal(lf(source.bytes),source.bytes.toString('utf8'),'RAW_LF_REQUIRED');
 const target='supabase/migrations/'+migrationNames[index];
 const prefix=`-- One-time CloudTMS migration: complete frozen Contact ${row.path}.\n-- Source-only proposal; exact current prerequisites/native contract qualification required.\n\\set ON_ERROR_STOP on\n\nbegin;\n\n`;
 const suffix='\ncommit;\n',text=prefix+source.bytes.toString('utf8')+suffix;
 exactAtomicBody(text);write('payload/'+target,text);
 sourcePins.push({phase:'schema',sourceCommit:producerCommit,sourcePath:source.path,sourceSha256:source.sha256,target,
  targetSha256:sha(text),sourceByteOffset:Buffer.byteLength(prefix),sourceBytes:source.bytes.length,wholePayloadReverseEqualsFrozen:true});
 payloads.push({phase:'schema',path:target,sha256:sha(text),bytes:Buffer.byteLength(text)});
 sourceDefinitions.push(...definitions(text,target));
}
const leaves=[];
for(const row of correctedRecipe.assembly.repeatable){
 const source=input(producerCommit,sourceBase+row.path);assert.equal(source.sha256,row.sha256);
 assert.equal(lf(source.bytes),source.bytes.toString('utf8'),'RAW_LF_REQUIRED');
 const target=leafDir+'/'+path.posix.basename(row.path);
 write('payload/'+target,source.bytes);leaves.push(target);
 sourcePins.push({phase:'repeatable',sourceCommit:producerCommit,sourcePath:source.path,sourceSha256:source.sha256,
  target,targetSha256:source.sha256,sourceBytes:source.bytes.length,wholePayloadEqualsFrozen:true});
 payloads.push({phase:'repeatable',path:target,sha256:source.sha256,bytes:source.bytes.length});
 sourceDefinitions.push(...definitions(source.bytes.toString('utf8'),target));
}
const guard=input(rootCommit,guardSource);assert.equal(guard.sha256,'650f736b80474576dcfc72ffba5eecbbb0aac954b893e3fb2e1cc81c3d83d796');
const correctionInput=input(rootCommit,path.posix.join(path.posix.dirname(guardSource),'CORRECTION.json'));
const correction=JSON.parse(correctionInput.bytes);write('metadata/ORDINAL_GUARD_CORRECTION.json',correctionInput.bytes);
const oldGuard=sourceDefinitions.find(row=>row.signature===correction.source.signature);
assert.equal(lf(guard.bytes).trim().replace(correction.correction.new,correction.correction.old),oldGuard.sql);
write('payload/'+guardTarget,guard.bytes);leaves.splice(leaves.length-1,0,guardTarget);
sourcePins.push({phase:'complete-successor',sourceCommit:rootCommit,sourcePath:guardSource,sourceSha256:guard.sha256,
 target:guardTarget,targetSha256:guard.sha256,wholeFunctionOneDeltaReverseEqualsFrozen:true});
payloads.push({phase:'complete-successor',path:guardTarget,sha256:guard.sha256,bytes:guard.bytes.length});
sourceDefinitions.push(...definitions(lf(guard.bytes),guardTarget));
const rootText=`-- Repeatable CloudTMS function authority: complete Contact producer completion.\n-- Full frozen e466 owners, exact complete guard successor, final unchanged ACL.\n-- After Candidate1003/full Health Source1100/pending1110/Invoice-duty1120.\n\\set ON_ERROR_STOP on\n\nbegin;\n\n${leaves.map(file=>'\\ir '+path.posix.relative(path.posix.dirname(rootSql),file)).join('\n')}\n\ncommit;\n`;
write('payload/'+rootSql,rootText);payloads.push({phase:'root',path:rootSql,sha256:sha(rootText),bytes:Buffer.byteLength(rootText)});
const closure=readManagedClosure(rootSql,path.join(here,'payload'));exactAtomicBody(closure.expanded);
const baseFinalDefinitions=leaves.flatMap(file=>sourceDefinitions.filter(row=>row.physical===file));
assert.equal(new Set(baseFinalDefinitions.map(row=>row.signature)).size,106);assert.equal(baseFinalDefinitions.length,107);
const prerequisiteSpecifications=[
 ['07_native_unsubmitted_recomposition.sqlinc','private.weekly_contact_source_render_attempted_v1(uuid)'],
 ['01_current_native_members.sqlinc','private.weekly_contact_source_candidate_render_assert_v1(uuid)'],
 ['01_current_native_members.sqlinc','private.weekly_contact_source_manager_initial_owned_v1(uuid)'],
 ['01_current_native_members.sqlinc','private.weekly_contact_source_manager_current_need_v1(uuid,uuid,timestamp with time zone)'],
 ['01_current_native_members.sqlinc','private.weekly_contact_source_manager_member_eligible_v1(uuid,uuid,timestamp with time zone)'],
];
const prerequisiteDefinitions=[],prerequisitePins=[];let prerequisiteSql='';
for(const [file,signature]of prerequisiteSpecifications){
 const prior=input(prerequisiteCommit,prerequisiteSource+'forward/'+file),text=lf(prior.bytes);
 const selected=definitions(text,prior.path).find(row=>row.signature===signature);assert(selected,signature);
 prerequisiteSql+=selected.sql+'\n\n';prerequisitePins.push({signature,sourceCommit:prerequisiteCommit,sourcePath:prior.path,
  wholeFileSha256:prior.sha256,completeFunctionStatementSha256:selected.statementSha256,wholeFunctionUnchanged:true});
}
prerequisiteSql=prerequisiteSql.trimEnd()+'\n';
const prerequisiteFunctionPath=prerequisiteDir+'/01_complete_native_helpers.sqlinc';
write('payload/'+prerequisiteFunctionPath,prerequisiteSql);
prerequisiteDefinitions.push(...definitions(prerequisiteSql,prerequisiteFunctionPath).map(row=>({...row,root:prerequisiteRoot})));
sourceDefinitions.push(...prerequisiteDefinitions);
const originalAclInput=input(prerequisiteCommit,prerequisiteSource+'forward/99_acl.sqlinc');
const originalAcl=lf(originalAclInput.bytes),wantedAcl=new Set(prerequisiteSpecifications.map(row=>row[1]));let prerequisiteAcl='';
for(const item of scanManagedSql(originalAcl).statements){
 const sql=originalAcl.slice(item.start,item.end).trim(),match=sql.match(/^(?:alter function|revoke all on function) ([\s\S]+?) (?:owner to postgres|from public,anon,authenticated,service_role);$/i);
 if(!match)continue;const signature=routineDefinitions('create function '+match[1]+' returns void language sql as $$select null$$;','ACL')[0];
 if(wantedAcl.has(signature))prerequisiteAcl+=sql+'\n';
}
assert.equal(scanManagedSql(prerequisiteAcl).statements.length,10);
const prerequisiteAclPath=prerequisiteDir+'/02_original_private_acl.sqlinc';
write('payload/'+prerequisiteAclPath,prerequisiteAcl);
const prerequisiteRootText=`-- Repeatable CloudTMS authority: five genuine original Contact prerequisites.\n-- Exact18f844 complete functions and original private ACL; no stubs or business changes.\n\\set ON_ERROR_STOP on\n\nbegin;\n\n\\ir ${path.posix.relative(path.posix.dirname(prerequisiteRoot),prerequisiteFunctionPath)}\n\\ir ${path.posix.relative(path.posix.dirname(prerequisiteRoot),prerequisiteAclPath)}\n\ncommit;\n`;
write('payload/'+prerequisiteRoot,prerequisiteRootText);
for(const [file,text]of [[prerequisiteFunctionPath,prerequisiteSql],[prerequisiteAclPath,prerequisiteAcl],[prerequisiteRoot,prerequisiteRootText]])
 payloads.push({phase:'explicit-native-prerequisite',path:file,sha256:sha(text),bytes:Buffer.byteLength(text)});
const prerequisiteClosure=readManagedClosure(prerequisiteRoot,path.join(here,'payload'));exactAtomicBody(prerequisiteClosure.expanded);
json('PREREQUISITE_SOURCE_PROOF.json',{version:'CONTACT_FIVE_GENUINE_PREREQUISITE_CONTINUATION_V1',status:'SOURCE_ONLY_NATIVE_NOT_RUN',
 initialDirectMissing:3,additionalTransitiveMissing:2,sourceCommit:prerequisiteCommit,root:prerequisiteRoot,
 orderedFunctions:prerequisitePins,acl:{sourcePath:originalAclInput.path,wholeFileSha256:originalAclInput.sha256,
  target:prerequisiteAclPath,statements:10,exactSelectedOriginalStatements:true,serviceOrBrowserExecute:false},
 targetFunctionPath:prerequisiteFunctionPath,functionLeafSha256:sha(prerequisiteSql),closureSha256:prerequisiteClosure.closureHash,
 authority:'Root explicitly commissioned exact5 finite genuine Health prerequisite continuation after transitive review; original review416e remains historical.',
 qualification:'Complete source definitions/ACL only; native catalogue, compilation, first-use and races NOT_RUN.'});
const finalDefinitions=[...prerequisiteDefinitions,...baseFinalDefinitions];
const finalBySignature=new Map(finalDefinitions.map(row=>[row.signature,row]));assert.equal(finalBySignature.size,111);assert.equal(finalDefinitions.length,112);

// Exact native source census only for these owners and their schema-qualified callees.
// No Bank proposal/body is copied into this Contact proposal.
const names=new Set([...finalBySignature.keys()].map(signature=>signature.split('(')[0]));
const allSource=sourceDefinitions.map(row=>row.sql).join('\n');
for(const match of allSource.matchAll(/\b(private|public)\.([a-z_][a-z0-9_]*)\s*\(/gi))names.add(match[1].toLowerCase()+'.'+match[2].toLowerCase());
const candidates=new Set();
for(let at=0;at<[...names].length;at+=50){
 const pattern='^[[:space:]]*create[[:space:]]+(or[[:space:]]+replace[[:space:]]+)?function[[:space:]]+('+[...names].slice(at,at+50).map(name=>name.replaceAll('.','\\.')).join('|')+')[[:space:]]*\\(';
 const r=spawnSync('git',['grep','-l','-i','-E',pattern,rootCommit,'--','supabase/repeatable','supabase/migrations','supabase/baseline'],{cwd:repo,encoding:'utf8',maxBuffer:4_000_000,windowsHide:true});
 assert([0,1].includes(r.status),r.stderr);for(const line of r.stdout.split(/\r?\n/).filter(Boolean))candidates.add(line.slice(rootCommit.length+1));
}
const nativeFiles=new Map(),nativeDefinitions=[];
for(const file of [...candidates].sort()){
 const bytes=blob(rootCommit,file),text=lf(bytes);nativeFiles.set(file,{sha256:sha(bytes),text});
 nativeDefinitions.push(...definitions(text,file).filter(row=>names.has(row.signature.split('(')[0])));
}
const includes=new Map();
const ir=spawnSync('git',['grep','-n','-E','^[[:space:]]*\\\\ir[[:space:]]',rootCommit,'--','supabase/repeatable'],{cwd:repo,encoding:'utf8',maxBuffer:8_000_000,windowsHide:true});assert.equal(ir.status,0,ir.stderr);
for(const line of ir.stdout.split(/\r?\n/).filter(Boolean)){
 const match=line.slice(rootCommit.length+1).match(/^([^:]+):(\d+):\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);assert(match,line);
 const file=match[1],target=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[3]??match[4]??match[5]));
 if(!includes.has(file))includes.set(file,[]);includes.get(file).push({line:Number(match[2]),target});
}
const nativePaths=git(['ls-tree','-r','--name-only',rootCommit,'--','supabase/repeatable']).toString().split(/\r?\n/).filter(file=>file.endsWith('.sql'));
const dateKey=file=>path.posix.basename(file).replace(/^(\d{2})(\d{2})(20\d{2})/,'$3$2$1');
nativePaths.sort((a,b)=>dateKey(a).localeCompare(dateKey(b))||a.localeCompare(b));
function nativeEvents(file,stack=[]){assert(!stack.includes(file),'INCLUDE_CYCLE');
 const events=[...nativeDefinitions.filter(row=>row.physical===file).map(row=>({line:row.line,definition:row})),...(includes.get(file)??[])].sort((a,b)=>a.line-b.line);
 return events.flatMap(event=>event.definition?[event.definition]:nativeEvents(event.target,[...stack,file]));
}
const actualNative=new Map();
for(const root of nativePaths)for(const definition of nativeEvents(root)){
 if(!actualNative.has(definition.signature))actualNative.set(definition.signature,[]);
 actualNative.get(definition.signature).push({...definition,root});
}
const registryInput=input(rootCommit,registryPath),registry=JSON.parse(registryInput.bytes),proposed=structuredClone(registry);
const priorBySignature=new Map(registry.chains.map(row=>[row.signature,row])),delta=[];
const unique=values=>[...new Set(values)];
for(const [signature,final]of [...finalBySignature].sort((a,b)=>a[0].localeCompare(b[0]))){
 const before=priorBySignature.get(signature)??null,prior=actualNative.get(signature)??[];
 if(before){assert.equal(prior.at(-1)?.root,before.finalRoot,'EXACT_PRIOR_FINAL_ROOT '+signature);assert.equal(prior.at(-1)?.physical,before.finalPhysical,'EXACT_PRIOR_FINAL_PHYSICAL '+signature);}
 const finalRoot=final.root??rootSql;
 const after={...(before??{signature,owner:'Health V6 complete Contact native producer; source-only proposal, native qualification required',baselineSource:rootCommit}),
  finalRoot,finalPhysical:final.physical,
  allowedPhysical:unique([...(before?.allowedPhysical??[]),...prior.map(row=>row.physical),...finalDefinitions.filter(row=>row.signature===signature).map(row=>row.physical)]),
  predecessors:unique([...(before?.predecessors??[]),...prior.map(row=>row.root),...(before?.finalRoot?[before.finalRoot]:[]),...(finalRoot===prerequisiteRoot?[rootSql]:[])]),
  finalClosures:unique([...(before?.finalClosures??[]),...(before?.finalRoot?[before.finalRoot]:[]),finalRoot]),
  healthV6ContactSourceReview:'docs/healthroster-build/proposals/v6-contact-producer-review-20261016/REVIEW.md'};
 if(before)proposed.chains[proposed.chains.findIndex(row=>row.signature===signature)]=after;else proposed.chains.push(after);
 delta.push({signature,before,after,actualPredecessors:prior.map(({sql,...row})=>row),completeFinalStatementSha256:final.statementSha256});
}
assert.deepEqual(proposed.editorChains,registry.editorChains);for(const key of Object.keys(registry).filter(key=>key!=='chains'))assert.deepEqual(proposed[key],registry[key]);
for(const before of registry.chains.filter(row=>!finalBySignature.has(row.signature)))assert.deepEqual(proposed.chains.find(row=>row.signature===before.signature),before);
json('registry/FINAL_DEFINER_DELTA.json',{version:'CONTACT_FINAL_DEFINER_PROPOSAL_V1',rootCommit,registryPath,baseSha256:registryInput.sha256,
 before:registry.chains.length,after:proposed.chains.length,updated:delta.filter(row=>row.before).length,added:delta.filter(row=>!row.before).length,
 editorChainsUnchanged:true,unrelatedChainsUnchanged:true,everyPriorCompanionAndFieldRetained:true,rows:delta});
json('registry/final-definer-closures.proposed.json',proposed);
const byName=new Map();for(const definition of nativeDefinitions){const name=definition.signature.split('(')[0];if(!byName.has(name))byName.set(name,[]);byName.get(name).push(definition);}
const requiredColumns={
 weekly_message_dispatch_commands:'id message_render_id attempt_count provider_accepted_at_utc state environment agency_id tranche_sequence tranche_kind',
 weekly_message_provider_attempts:'dispatch_command_id',weekly_message_dispatch_targets:'id dispatch_command_id attempt_count state',
 weekly_message_target_attempts:'dispatch_target_id',weekly_message_renders:'id state',
 weekly_candidate_outreach_generations:'id state request_kind candidate_id client_id source_cycle_id candidate_cohort_id generation_number membership_hash',
 weekly_source_cycles:'id current_projection_publication_id',weekly_source_groups:'id environment agency_id',
 weekly_discrepancy_incidents:'id candidate_id client_id source_cycle_id source_group_id state candidate_action_state current_comparison_revision_id work_event_id episode_number manager_action_state manager_potential_state',
 weekly_candidate_outreach_memberships:'candidate_generation_id incident_id state comparison_revision_id',
 weekly_issue_comparison_revisions:'id contract_id',weekly_work_events:'id work_date',
 weekly_timesheet_submission_requests:'id candidate_id current_projection_publication_id candidate_cohort_id request_generation source_cycle_id membership_hash state',
 weekly_timesheet_submission_request_memberships:'submission_request_id client_id contract_id week_ending state id',
 timesheets:'contract_id week_ending_date is_current revoked_at archived_at_utc sheet_scope line_type r2_nurse_key img_sha256_nurse',
 weekly_contact_delivery_memberships:'incident_id incident_episode delivery_id audience purpose send_ordinal',
 weekly_manager_review_items:'incident_id incident_episode review_batch_id',weekly_manager_review_batches:'id message_render_id',
 weekly_manager_recipient_routes:'id source_cycle_id protected_recipient_address',
 weekly_source_global_settings:'singleton manager_recent_completed_weeks manager_max_work_age_days',contracts:'id candidate_id client_id',
};
const schemaPattern='(create|alter)[[:space:]]+table[[:space:]]+(if[[:space:]]+not[[:space:]]+exists[[:space:]]+)?public\\.('+Object.keys(requiredColumns).join('|')+')([[:space:](]|$)';
const schemaGrep=spawnSync('git',['grep','-l','-i','-E',schemaPattern,rootCommit,'--','supabase/migrations','supabase/baseline','supabase/repeatable'],{cwd:repo,encoding:'utf8',maxBuffer:4_000_000,windowsHide:true});assert.equal(schemaGrep.status,0,schemaGrep.stderr);
const columnEvidence=new Map(Object.keys(requiredColumns).map(table=>[table,new Map()]));
for(const line of schemaGrep.stdout.split(/\r?\n/).filter(Boolean)){
 const file=line.slice(rootCommit.length+1),bytes=blob(rootCommit,file),text=lf(bytes);
 for(const item of scanManagedSql(text).statements){
  const t=item.tokens,at=t.indexOf('TABLE');if(at!==1||!['CREATE','ALTER'].includes(t[0]))continue;
  let nameAt=at+1;if(t[nameAt]==='IF')nameAt+=3;
  if(t[nameAt]!=='PUBLIC'||t[nameAt+1]!=='.')continue;
  const table=t[nameAt+2]?.toLowerCase(),columns=columnEvidence.get(table);if(!columns)continue;
  const remember=column=>{if(!column||!/^[A-Z_][A-Z0-9_]*$/.test(column))return;
   const key=column.toLowerCase();if(!columns.has(key))columns.set(key,[]);columns.get(key).push({path:file,sha256:sha(bytes)});};
  if(t[0]==='CREATE'){
   let depth=0,want=false;for(const token of t.slice(nameAt+3)){
    if(token==='('){depth++;if(depth===1)want=true;continue;}if(token===')'){depth--;continue;}
    if(token===','&&depth===1){want=true;continue;}
    if(want&&depth===1){if(!['CONSTRAINT','PRIMARY','FOREIGN','UNIQUE','CHECK','EXCLUDE','LIKE'].includes(token))remember(token);want=false;}
   }
  }else for(let index=nameAt+3;index<t.length;index++)if(t[index]==='ADD'){
   let columnAt=index+1;if(t[columnAt]==='COLUMN')columnAt++;if(t[columnAt]==='IF')columnAt+=3;
   if(!['CONSTRAINT','PRIMARY','FOREIGN','UNIQUE','CHECK','EXCLUDE'].includes(t[columnAt]))remember(t[columnAt]);
  }
 }
}
const relationProof=Object.entries(requiredColumns).map(([table,fields])=>({relation:'public.'+table,
 columns:fields.split(' ').map(column=>({column,sourceEvidence:columnEvidence.get(table).get(column)??[]}))}));
const missingColumns=relationProof.flatMap(row=>row.columns.filter(column=>!column.sourceEvidence.length).map(column=>row.relation+'.'+column.column));
json('PREREQUISITE_RELATION_REVIEW.json',{version:'CONTACT_PREREQUISITE_EXACT_SOURCE_RELATIONS_V1',rootCommit,
 qualification:'Manually read all five full functions and mapped each referenced native field to actual pinned CREATE/ADD source. Presence evidence is not native catalogue/type/constraint acceptance.',
 relations:relationProof,missingColumns,native:'NOT_RUN'});
assert.deepEqual(missingColumns,[],'PREREQUISITE_SOURCE_COLUMNS_UNPROVIDED');
const dependencyRows=[];
for(const definition of finalDefinitions){
 const calls=unique([...definition.sql.matchAll(/\b(private|public)\.([a-z_][a-z0-9_]*)\s*\(/gi)].map(match=>match[1].toLowerCase()+'.'+match[2].toLowerCase()))
  .filter(name=>name!==definition.signature.split('(')[0]&&(name.startsWith('private.')||byName.has(name)||[...finalBySignature.keys()].some(key=>key.startsWith(name+'('))));
 for(const name of calls){const own=[...finalBySignature].filter(([signature])=>signature.startsWith(name+'('));
  const native=byName.get(name)??[];dependencyRows.push({caller:definition.signature,language:definition.language,physical:definition.physical,calleeName:name,
   proposalSignatures:own.map(([signature])=>signature),nativeSignatures:unique(native.map(row=>row.signature)),
   nativeEvidence:unique(native.map(row=>row.physical)).map(file=>({path:file,sha256:nativeFiles.get(file).sha256})),
   status:own.length?'COMPLETE_PROPOSAL_DEFINITION':native.length?'EXACT_NATIVE_SOURCE_DECLARATION':'MISSING_AT_PINNED_ROOT_NOT_STUBBED'});
 }
}
json('DEPENDENCY_REVIEW.json',{version:'CONTACT_ACTUAL_SOURCE_DEPENDENCIES_V1',rootCommit,
 qualification:'Source declarations and exact native overload identities only; PG17 resolution/first-use NOT_RUN. Regex call census is conservative, not a catalogue substitute.',
 reservedOrder:{migrations:migrationNames,prerequisiteRepeatable:prerequisiteRoot,repeatable:rootSql,precedesContact:['Candidate1003','Full Health Source1100','Source pending1110','Invoice duty1120']},
 languageSqlFunctions:finalDefinitions.filter(row=>row.language==='SQL').map(({sql,...row})=>row),
 unresolved:dependencyRows.filter(row=>row.status.startsWith('MISSING')),rows:dependencyRows,
 nativeSourceFiles:[...nativeFiles].map(([file,row])=>({path:file,sha256:row.sha256})),
 releaseHolds:['Root1110/1120/fullSource1100 managed source integration remains root-owned and must be complete before activation.',
  'Exact one-time prior schema/constraint/index/data assertions must pass on native PG17; source declaration presence does not prove the catalogue.',
  'All complete Contact owners and final ACL must exist before first use; no stubs/check_function_bodies override.',
  'Final contract, effective ACL/API, currentness, races and whole native acceptance are NOT_RUN.']});
const currentParent=input(rootCommit,runtime.source_path);assert.equal(sha(lf(currentParent.bytes)),runtime.source_sha256_normalized_lf);
let contributed=lf(currentParent.bytes);const anchorCounts=[];
for(const edit of runtime.edits){const count=contributed.split(edit.before).length-1;assert.equal(count,1);anchorCounts.push(count);contributed=contributed.replace(edit.before,edit.after);}
assert.equal(sha(contributed),runtime.result_sha256_normalized_lf);
let reversed=contributed;for(const edit of [...runtime.edits].reverse())reversed=reversed.replace(edit.after,edit.before);assert.equal(reversed,lf(currentParent.bytes));
write('payload/'+runtime.source_path,contributed);write('payload/'+runtime.leaf.mount_path,runtimeLeafInput.bytes);
payloads.push({phase:'runtime-parent',path:runtime.source_path,sha256:sha(contributed),bytes:Buffer.byteLength(contributed)},
 {phase:'runtime-leaf',path:runtime.leaf.mount_path,sha256:runtimeLeafInput.sha256,bytes:runtimeLeafInput.bytes.length});
json('RUNTIME_REVERSE_PROOF.json',{version:'CONTACT_ACTUAL_PARENT_FINITE_CONTRIBUTION_V1',rootCommit,parentPath:runtime.source_path,
 parentRawSha256:currentParent.sha256,parentCanonicalLfSha256:sha(lf(currentParent.bytes)),anchorCounts,edits:runtime.edits,
 resultCanonicalLfSha256:sha(contributed),completeReverseEqualsActualParent:true,leafSourceCommit:producerCommit,
 leafSourcePath:runtimeLeafInput.path,leafSha256:runtimeLeafInput.sha256,metadataOnlyCorrection:{old:frozenRuntime.leaf.sha256,new:runtime.leaf.sha256},
 qualification:'Complete current parent contributes only exact three edits; actual RPC/runtime activation NOT_RUN.'});
json('ASSEMBLY_MANIFEST.json',{version:'CONTACT_COMPLETE_MANAGED_ASSEMBLY_PROPOSAL_V1',status:'SOURCE_ONLY_GATED_NATIVE_NOT_RUN',
 producerCommit,reviewCommit,rootPrerequisiteCheckpoint:rootCommit,reservedRoot:rootSql,
 counts:{schemaFiles:3,frozenRepeatableLeaves:53,addedCompleteGuardSuccessors:1,repeatableLeaves:leaves.length,
  separatelyAuthorizedPrerequisiteFunctions:5,allUniqueFunctions:111,
  frozenUniqueFunctions:106,repeatableFunctionDeclarations:finalDefinitions.length,totalFunctionDeclarations:sourceDefinitions.length,
  expandedRepeatableStatements:scanManagedSql(closure.expanded).statements.length,
  allManagedStatements:scanManagedSql(closure.expanded).statements.length+scanManagedSql(prerequisiteClosure.expanded).statements.length+migrationNames.reduce((sum,name)=>sum+scanManagedSql(fs.readFileSync(path.join(here,'payload/supabase/migrations',name),'utf8')).statements.length,0)},
 frozenRecipeSha256:recipeInput.sha256,reviewedOneFieldRecipeSuccessor:{source:correctedRecipeSource,rootCommit,sha256:recipeSuccessorInput.sha256,oneFieldReverseEquality:true},
 rawFrozenPayloadsUnchanged:56,sourcePins,payloads,orderedRepeatableRoots:[prerequisiteRoot,rootSql],orderedRepeatableLeaves:leaves,repeatableClosureSha256:closure.closureHash,
 retainedEffects:'Every frozen DDL/ACL/trigger statement retained; complete guard successor inserted before unchanged final99; no schema suppression.',
 correctionsOnly:['Complete ordinal guard one-kind successor','One-field SQL99 recipe SHA successor','One-field runtime leaf SHA successor'],
 native:{database:false,locks:false,docker:false,release:false,compile:'NOT_RUN',acl:'NOT_RUN',concurrency:'NOT_RUN',api:'NOT_RUN',partB:'NOT_RUN'}});
console.log(JSON.stringify({rootCommit,counts:{frozenLeaves:56,repeatableLeaves:leaves.length,repeatableFunctions:finalDefinitions.length,unique:finalBySignature.size},
 registry:{updated:delta.filter(row=>row.before).length,added:delta.filter(row=>!row.before).length},dependencies:{rows:dependencyRows.length,missing:dependencyRows.filter(row=>row.status.startsWith('MISSING')).map(row=>row.calleeName)},
 runtimeAnchors:anchorCounts,native:'NOT_RUN'}));
