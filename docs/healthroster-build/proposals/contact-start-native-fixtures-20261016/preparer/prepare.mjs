// SOURCE ONLY. No database client, credential, fetch, socket or deployment.
// Root owns any subsequent native trial, existing managed gates and restoration.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
export const INSTALLED_I1='f743b81fe537830d81b1de93ec4f9cb0331532a7';
export const REVIEWED_ROOT='5feaeae37d1909e44cc3c415f80bf448dda7cbfd';
export const DIRECT_LIMIT=Object.freeze({sqlBytes:8000000,parsedStatements:1000});
export const FIXTURE_MOUNT='tests/healthroster/contact_source_native_start_v1.sql';
export const MARKER='hr_install2_f716c000000040008000000000000001';
const here=path.dirname(fileURLToPath(import.meta.url)),proposal=path.dirname(here);
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const canonical=x=>String(x).replaceAll('\r\n','\n');
const requireThat=(b,c)=>{if(!b)throw Error(c);};
const rootLoad=(root,file)=>import(pathToFileURL(path.join(root,'scripts',file)).href);
function git(root,args,encoding='utf8'){return execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{encoding,windowsHide:true,maxBuffer:96000000,stdio:['pipe','pipe','pipe']});}
export function assertRoot(root,commit=REVIEWED_ROOT){
 requireThat(path.isAbsolute(root)&&commit===REVIEWED_ROOT,'CONTACT_PREPARER_REVIEWED_ROOT_REQUIRED');
 const head=git(root,['rev-parse','HEAD']).trim();
 requireThat(/^[a-f0-9]{40}$/.test(head),'CONTACT_PREPARER_ROOT_HEAD_INVALID');
 const inputs=JSON.parse(fs.readFileSync(path.join(proposal,'SOURCE_MANIFEST.json'),'utf8'));
 const paths=['supabase','scripts','docs/bpay-next/final-definer-closures.json',...inputs.source_inputs.filter(x=>x.path.startsWith('broker/')).map(x=>x.path)];
 requireThat(!git(root,['status','--porcelain','--',...paths]).trim(),'CONTACT_PREPARER_SELECTED_ROOT_DIRTY');
 git(root,['merge-base','--is-ancestor',commit,head]);
 requireThat(!git(root,['diff','--name-only',commit+'..'+head,'--',...paths]).trim(),'CONTACT_PREPARER_SELECTED_ROOT_COMMITTED_DRIFT');
}
export function admitDirectEnvelope(bytes,statements){
 requireThat(Number.isSafeInteger(bytes)&&bytes>=0&&Number.isSafeInteger(statements)&&statements>=0,'CONTACT_PREPARER_BAD_ENVELOPE');
 return {bytes,statements,limits:DIRECT_LIMIT,fits:bytes<=DIRECT_LIMIT.sqlBytes&&statements<=DIRECT_LIMIT.parsedStatements};
}
export function concatUnits(units,scan,{reviewOnly=false}={}){
 let sql='';const index=[],lineMap=[];
 for(const [i,u]of units.entries()){
  requireThat(typeof u.sql==='string'&&sha(u.sql)===u.sqlHash,'CONTACT_PREPARER_UNIT_HASH_DRIFT');
  const comment=`-- CONTACT_PREPARER_UNIT ${i} ${u.kind} ${u.file} SHA256 ${u.sqlHash}\n`;
  sql+=comment;const offset=sql.length,startLine=sql.split('\n').length,byteStart=Buffer.byteLength(sql);
  const parsed=scan(u.sql);
  if(!(reviewOnly&&u.sourceOnlySpecialEnvelope)){requireThat(!parsed.meta.length,'CONTACT_PREPARER_UNEXPANDED_META');
   requireThat(!parsed.statements.some(s=>['BEGIN','COMMIT','ROLLBACK','START','ABORT','PREPARE'].includes(s.tokens[0])),'CONTACT_PREPARER_OUTER_CONTROL');}
  sql+=u.sql+'\n';
  index.push({index:i,file:u.file,kind:u.kind,sourceSqlHash:u.sourceSqlHash,sqlHash:u.sqlHash,bytes:Buffer.byteLength(u.sql),statements:parsed.statements.length,
   byteStart,byteEnd:byteStart+Buffer.byteLength(u.sql),startLine,endLine:sql.split('\n').length-1,sourceFiles:u.sourceFiles,sourceOnlySpecialEnvelope:u.sourceOnlySpecialEnvelope===true});
  for(const [j,s]of parsed.statements.entries())lineMap.push({unit:i,file:u.file,kind:u.kind,statement:j+1,
   startLine:startLine+u.sql.slice(0,s.start).split('\n').length-1,endLine:startLine+u.sql.slice(0,s.end).split('\n').length-1,
   statementHash:sha(u.sql.slice(s.start,s.end))});
  requireThat(sql.slice(offset,offset+u.sql.length)===u.sql,'CONTACT_PREPARER_UNIT_BYTE_MAP_DRIFT');
 }
 return {sql,index,lineMap};
}
export function fixtureUnit(scan){
 const order=['08_before-and-fresh-absence.sql','00_service-boundaries.sql','01_fixture-input.sql',
  'seed/10_generated-artifact.sql','seed/11_observe-native-inserts.sql','seed/12_declare-native-weeks.sql',
  'seed/13_native-publication.sql','seed/14_native-generations.sql','02_construct-native-targets.sql',
  '03_assertion-helpers.sql','04_native-cases.sql','05_fixture-qt-bootstrap.sql','06_run-native-cases.sql','07_capture-absence-set.sql'];
 const sourceFiles=order.map(file=>{const sql=canonical(fs.readFileSync(path.join(proposal,file),'utf8'));return {file,sha256:sha(sql),sql};});
 const sql=sourceFiles.map(x=>`-- CONTACT_FIXTURE_COMPONENT ${x.file} SHA256 ${x.sha256}\n${x.sql}`).join('\n');
 requireThat(!scan(sql).meta.length,'CONTACT_PREPARER_FIXTURE_META');
 requireThat(/count\(\*\)[^;]*25/is.test(sql),'CONTACT_PREPARER_25_CASE_CENSUS_REQUIRED');
 requireThat(!/create\s+(?:or\s+replace\s+)?function\s+(?:public|private)\./i.test(sql),'CONTACT_PREPARER_NATIVE_OWNER_REPLACEMENT');
 requireThat(!/disable\s+trigger|session_replication_role|\bdblink\b/i.test(sql),'CONTACT_PREPARER_FIXTURE_ESCAPE');
 return {file:FIXTURE_MOUNT,kind:'verifier',sql,sqlHash:sha(sql),sourceSqlHash:sha(sql),
  sourceFiles:sourceFiles.map(({sql,...x})=>x),phase:'native_source_contact_prefix_and_25_cases'};
}
function exporterQuery(source,scan){
 const parsed=scan(source),queries=parsed.statements.filter(s=>s.tokens[0]==='WITH');
 requireThat(queries.length===1&&parsed.statements.every(s=>['SET','WITH'].includes(s.tokens[0])),'CONTACT_PREPARER_EXPORTER_SHAPE_DRIFT');
 return parsed.source.slice(queries[0].start,queries[0].end).replace(/;\s*$/,'');
}
export async function baselineProbeSql(root){
 const {scanManagedSql}=await rootLoad(root,'cloudtms-managed-release-sql.mjs');
 const {identityProbeSql}=await rootLoad(root,'healthroster-install1-full-rehearsal.mjs');
 const query=exporterQuery(canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8')),scanManagedSql);
 const identity=identityProbeSql(MARKER).trim().replace(/;\s*$/,'');
 return `-- Fresh read-only connection supplied by root; connectedFresh is transport evidence, never minted here.\nset local search_path=pg_catalog,public;\nwith identity_proof as (${identity}),catalogue as (${query})
 select (identity_proof.jsonb_build_object::jsonb||jsonb_build_object('checkedAt',clock_timestamp(),
  'contract',(select c.jsonb_build_object::jsonb from catalogue c),
  'installed',jsonb_build_object(
   'migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger),
   'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger))))::text from identity_proof;\n`;
}
// Reuse the actual existing predecessor validator and full fresh restoration
// builder. They do not connect. A saved source contract cannot waive a native row.
export async function validateFreshBaseline(root,observation){
 const {collectInstall2ReviewedInstalledSource}=await rootLoad(root,'healthroster-install2-installed-source.mjs');
 const {validateInstall2Predecessor}=await rootLoad(root,'healthroster-install2-native-gate.mjs');
 return validateInstall2Predecessor({observation,reviewedSource:collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:INSTALLED_I1})});
}
export async function restorationSql(root,{identity,relations,observeLiveBusiness=false}){
 const {restorationSnapshotSql}=await rootLoad(root,'healthroster-install1-full-rehearsal.mjs');
 return restorationSnapshotSql({identity,marker:MARKER,relations,observeLiveBusiness,
  exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8')});
}
export async function prepare({root,out=path.join(here,'generated'),commit=REVIEWED_ROOT}={}){
 root=path.resolve(root??'');out=path.resolve(out);assertRoot(root,commit);
 // Loading the reviewed source tooling is pure; never call its psql/export/run APIs.
 const {inventory,mapExecutableSqlSource,canonicalContractHash}=await rootLoad(root,'cloudtms-db-release-lib.mjs');
 const {digest,scanManagedSql,readManagedClosure,exactAtomicBody}=await rootLoad(root,'cloudtms-managed-release-sql.mjs');
 const {pendingInventory,compileManagedUnit}=await rootLoad(root,'cloudtms-managed-release.mjs');
 const {collectInstall2ReviewedInstalledSource}=await rootLoad(root,'healthroster-install2-installed-source.mjs');
 const {closeFinalDefinerSelection,routineDefinitions,definerInventory}=await rootLoad(root,'bpay-next-final-definers.mjs');
 const {readerReleasePhases}=await rootLoad(root,'weekly-source-reader-release.mjs');
 const current=inventory(),installed=collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:INSTALLED_I1});
 const pending=pendingInventory(current,installed.inventory);
 const registry=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json'),'utf8'));
 const census=definerInventory(current),selected=new Set(pending.repeatables.map(x=>x.path)),migrationPaths=new Set(pending.migrations.map(x=>x.path));
 let officialFinalGate='PASS';try{closeFinalDefinerSelection(pending,current);}catch(e){
  requireThat(/^BPAY_NEXT_(?:FINAL_DEFINER_ORDER|FINAL_DEFINER_UNREGISTERED|COMPETING_DEFINER|FINAL_CLOSURE_MISSING|FINAL_CLOSURE_REQUIRED|MIGRATION_FINAL|PENDING_FINAL_REQUIRED)/.test(e.message),'CONTACT_PREPARER_UNCLASSIFIED_FINAL_GATE '+e.message);
  officialFinalGate=e.message;
 }
 // Candidate closure is retained even when the existing registry rejects it;
 // that rejection remains an execution hold. Every whole selected predecessor
 // is followed by its actual source-censused final owner, including unchanged
 // roots. No selected source routine is left with a stale last definition.
 let added;do{added=false;
  for(const chain of registry.chains)if(chain.predecessors?.some(x=>selected.has(x)))for(const file of chain.finalClosures){
   requireThat(current.repeatables.some(x=>x.path===file),'CONTACT_PREPARER_REGISTERED_ROOT_MISSING');if(!selected.has(file)){selected.add(file);added=true;}}
  for(const row of [...census.roots.filter(x=>selected.has(x.path)),...census.migrationRoots.filter(x=>migrationPaths.has(x.path))])for(const d of row.definitions){
   const final=census.routines.get(d.signature)?.at(-1);if(final&&!selected.has(final.root)){selected.add(final.root);added=true;}}
 }while(added);
 const closed={migrations:pending.migrations,repeatables:current.repeatables.filter(x=>selected.has(x.path))};
 const registryDrift=registry.chains.flatMap(e=>{const definitions=census.routines.get(e.signature)??[],actual=definitions.at(-1);
  const competing=definitions.filter(x=>!e.allowedPhysical.includes(x.physical));
  return !actual||actual.root!==e.finalRoot||actual.physical!==e.finalPhysical||competing.length?
   [{signature:e.signature,declaredRoot:e.finalRoot,declaredPhysical:e.finalPhysical,actualFinal:actual??null,unregisteredPhysical:[...new Set(competing.map(x=>x.physical))]}]:[];});
 const phases=readerReleasePhases(closed.repeatables,current.repeatables);
 requireThat(phases.readers.length===0,'CONTACT_PREPARER_REAL_NATIVE_READER_PREPARATION_REQUIRES_FULL_COMPILER');
 const units=[],managedRejections=[],previousOwner=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
 try{
  process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
  for(const [kind,rows]of [['migration',closed.migrations],['repeatable',phases.ordinary]])for(const item of rows){
   let unit;try{unit=compileManagedUnit(item,kind,{root,mapSource:mapExecutableSqlSource});
    if(unit.exception)throw Error('CONTACT_PREPARER_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED');
   }catch(e){
    requireThat(e.message.startsWith('MANAGED_REPLAY_POLICY_HASH_MISMATCH: ')||e.message==='MANAGED_EXECUTABLE_TREE_REQUIRED'||e.message==='CONTACT_PREPARER_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED','CONTACT_PREPARER_MANAGED_CLASSIFICATION_FAILED '+e.message);
    const closure=readManagedClosure(item.path,root,[],mapExecutableSqlSource);
    requireThat((kind==='migration'?digest(closure.ordered[0].source):closure.closureHash)===item.sha256,'CONTACT_PREPARER_REJECTED_UNIT_SOURCE_DRIFT');
    // Preserve every special root's complete expanded native transaction and
    // conditional psql packaging. NO special envelope command is removed.
    // It is source review material only, never an admissible inner unit.
    unit={body:closure.expanded,closure,sourceOnlySpecialEnvelope:true};
    const parsed=scanManagedSql(closure.expanded);
    managedRejections.push({file:item.path,rejection:e.message,executionQualification:'HELD_NOT_APPROVED',
     actualClosureHash:closure.closureHash,sourceOnlySpecialEnvelope:true,removedEnvelopeCommands:[],
     metaCommands:[...new Set(parsed.meta.map(x=>x.text.split(/\s/)[0]))],
     transactionCommands:parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','START','ABORT','PREPARE'].includes(x.tokens[0])).map(x=>x.tokens.join(' '))});
   }
   requireThat(!unit.ownerRlsEnvelope&&!unit.immutableGuardDependency,'CONTACT_PREPARER_SPECIAL_DEPENDENCY_REVIEW_REQUIRED');
   units.push({file:item.path,kind,sql:unit.body,sqlHash:digest(unit.body),sourceSqlHash:item.sha256,
    sourceFiles:unit.closure.ordered.map(x=>({file:x.path,sha256:digest(x.source)})),sourceOnlySpecialEnvelope:unit.sourceOnlySpecialEnvelope===true});
  }
 }finally{if(previousOwner===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previousOwner;}
 const native=concatUnits(units,scanManagedSql,{reviewOnly:true}),fixture=fixtureUnit(scanManagedSql),combined=concatUnits([...units,fixture],scanManagedSql,{reviewOnly:true});
 const sourceSet=new Map();for(const u of units)for(const f of u.sourceFiles){requireThat(!sourceSet.has(f.file)||sourceSet.get(f.file)===f.sha256,'CONTACT_PREPARER_SOURCE_COLLISION');sourceSet.set(f.file,f.sha256);}
 const finalFunctions=new Map();for(const u of units)for(const s of scanManagedSql(u.sql).statements){const sql=u.sql.slice(s.start,s.end);for(const signature of routineDefinitions(sql,u.file))finalFunctions.set(signature,{signature,finalRoot:u.file,statementHash:sha(sql)});}
 // No source graph exclusion is used to admit this plan: every pending source
 // owner plus existing audited final-definer closure is retained, whole ACLs and
 // side-effect/trigger owners included. Static candidates below are diagnostic only.
 const output={
  '10_complete-native-preparation.sql':native.sql,
  '20_genuine-source-prefix-and-25-cases.sql':fixture.sql+'\n',
  '30_complete-trial-inner.sql':combined.sql,
  '00_fresh-I1-predecessor.sql':await baselineProbeSql(root),
  '01_fresh-restoration-relations.sql':`-- Root obtains this fresh under its existing owned turn, before staging.\nselect coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb)::text\n from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in('public','private') and c.relkind in('r','p');\n`,
  'UNIT_INDEX.json':JSON.stringify(combined.index,null,2)+'\n',
  'STATEMENT_MAP.json':JSON.stringify(combined.lineMap,null,2)+'\n',
  'FINAL_REGISTRY_DRIFT.json':JSON.stringify({officialFinalGate,registryDrift,managedRejections},null,2)+'\n',
  'FINAL_ROUTINE_OWNERS.json':JSON.stringify([...finalFunctions.values()].sort((a,b)=>a.signature.localeCompare(b.signature)),null,2)+'\n',
 };
 const exportSql=canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'));
 sourceSet.set('supabase/release/export_contract.sql',sha(exportSql));
 const release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8'));
 const mandatoryVerifierInputs=release.verificationFiles.map(file=>({file,sha256:sha(canonical(fs.readFileSync(path.join(root,file),'utf8')))}));
 for(const file of ['docs/bpay-next/final-definer-closures.json','supabase/release/current-release.json','supabase/release/current-contract.json','supabase/release/migration-lock.json','supabase/release/baseline-repeatable-lock.json','supabase/release/protected-boundary-lock.json'])sourceSet.set(file,sha(canonical(fs.readFileSync(path.join(root,file),'utf8'))));
 const controls=['cloudtms-managed-execution-policy.mjs','cloudtms-db-release-lib.mjs','cloudtms-managed-release.mjs','cloudtms-managed-release-sql.mjs','bpay-next-final-definers.mjs',
 'weekly-source-reader-release.mjs','healthroster-install2-installed-source.mjs','healthroster-install2-native-gate.mjs',
 'healthroster-install1-full-rehearsal.mjs','healthroster-full-rehearsal-protocol.mjs','healthroster-rehearsal-turn.mjs','healthroster-turn-control.mjs',
 'healthroster-live-test-restoration.mjs','healthroster-rehearsal-resource-guard.mjs','healthroster-rehearsal-stream-observer.mjs'];
 for(const f of controls){const relative='scripts/'+f;requireThat(fs.existsSync(path.join(root,relative)),'CONTACT_PREPARER_CONTROL_MISSING '+f);sourceSet.set(relative,sha(canonical(fs.readFileSync(path.join(root,relative),'utf8'))));}
 const pendingSchemaObjects=new Set();
 for(const u of units.filter(x=>x.kind==='migration'))for(const m of u.sql.matchAll(/\bcreate\s+(?:table|sequence|view)\s+(?:if\s+not\s+exists\s+)?((?:public|private)\.[a-z_][a-z0-9_]*)\b/gi))
  if(!installed.contract.relations.some(r=>r.schema+'.'+r.name===m[1]))pendingSchemaObjects.add(m[1]);
 const schemaNames=[...pendingSchemaObjects].sort();
 output['02_before-and-fresh-schema-absence.sql']=`-- Before staging and after full rollback, on fresh read-only connections.\ndo $contact_native_schema_absent$ begin\n if current_database()<>'cloudtms_test_clone' then raise exception 'CONTACT_NATIVE_WRONG_DATABASE';end if;\n if exists(select 1 from unnest(ARRAY[${schemaNames.map(x=>"'"+x+"'").join(',')}]) n where to_regclass(n) is not null) then raise exception 'CONTACT_NATIVE_STAGED_SCHEMA_REMAINS';end if;\nend $contact_native_schema_absent$;\n`;
 const sourceContract=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'));
 const manifest={version:'CONTACT_NATIVE_TRIAL_PREPARER_V1',qualification:'SOURCE_ONLY_NATIVE_NOT_RUN',artifactType:'COMPLETE_SOURCE_REVIEW_PACKAGE_WITH_UNCLASSIFIED_SPECIAL_ENVELOPES_NOT_EXECUTABLE',rootCommit:commit,observedRootHead:git(root,['rev-parse','HEAD']).trim(),selectedReviewedInputsUnchanged:true,installedCommit:INSTALLED_I1,
  installedContractHash:installed.contractHash,installedCounts:{migrations:installed.inventory.migrations.length,repeatables:installed.inventory.repeatables.length},
  currentCounts:{migrations:current.migrations.length,repeatables:current.repeatables.length},pendingCounts:{migrations:pending.migrations.length,repeatables:pending.repeatables.length},
  finalClosureCounts:{migrations:closed.migrations.length,repeatables:closed.repeatables.length},
  closurePolicy:'COMPLETE_PENDING_SOURCE_PLUS_REGISTERED_CLOSURES_AND_ACTUAL_FINAL_SOURCE_OWNERS_NO_DEPENDENCY_EXCLUSION',officialFinalGate,registryDriftCount:registryDrift.length,managedRejections,
  currentContractIsI1:canonicalContractHash(sourceContract)===installed.contractHash,
  pendingCandidateContractQualification:'NOT_GENERATED_OR_ADOPTED_BY_THIS_PREPARER',
  directEnvelope:{...admitDirectEnvelope(Buffer.byteLength(combined.sql),scanManagedSql(combined.sql).statements.length),statementMetric:'SOURCE_PACKAGING_LEXER_RAW_PSQL_NOT_DIRECT_TOOL_SQL_PARSE',directToolParse:'NOT_APPLICABLE_SOURCE_REVIEW_PACKAGE'},
  nativePreparation:{bytes:Buffer.byteLength(native.sql),statements:scanManagedSql(native.sql).statements.length,sha256:sha(native.sql)},
  fixture:{mount:FIXTURE_MOUNT,caseCount:25,bytes:Buffer.byteLength(fixture.sql+'\n'),statements:scanManagedSql(fixture.sql).statements.length,sha256:sha(fixture.sql+'\n'),sourceFiles:fixture.sourceFiles},
  exactNewRelationAbsence:schemaNames,
  finalRoutineCount:finalFunctions.size,
  sourceFiles:[...sourceSet].sort(([a],[b])=>a.localeCompare(b)).map(([file,sha256])=>({file,sha256})),
  payloads:Object.entries(output).map(([file,sql])=>({file,sha256:sha(sql),bytes:Buffer.byteLength(sql)})),
  held:['DIRECT_KIER_ENVELOPE_EXCEEDED','ROOT_SEND5B_FULL_EXACT_COMMIT_MILESTONE_REHEARSAL_REVIEW_REQUIRED',...(officialFinalGate==='PASS'?[]:['EXISTING_FINAL_DEFINER_REGISTRY_REJECTED_CURRENT_SOURCE']),...(managedRejections.length?['EXISTING_MANAGED_REPLAY_POLICY_HASHES_AND_ATOMIC_ENVELOPES_REVIEW_REQUIRED']:[]),'FRESH_I1_CATALOGUE_LEDGER_BASELINE_REQUIRED',
   'EXACT_PENDING_CONTRACT_SEAL_AND_FULL_REHEARSAL_VERIFIER_REVIEW_REQUIRED','FRESH_FULL_RESTORATION_AND_ACTUAL_UUID_ABSENCE_REQUIRED','NATIVE_PREFIX_START_25_CASES_NOT_RUN'],
  mandatoryVerifierInputs,mandatoryVerifierDisposition:'ALL_RETAINED_BY_EXISTING_FULL_COMPILER_AND_EXACT_ROOT_REVIEW_NO_WAIVERS_BY_PREPARER',excludedChecks:[],databaseCalls:0,externalEffects:0,localDatabaseInfrastructure:0,
  sharedOwnerWrites:0,specialEnvelopeCommandsRemoved:0,rawSqlOuterTransactions:managedRejections.reduce((n,r)=>n+r.transactionCommands.length,0),specialEnvelopeUnitCount:managedRejections.length,bankingPublisherSubstitutes:0};
 assertRoot(root,commit);
 fs.mkdirSync(out,{recursive:true});for(const [file,value]of Object.entries(output))fs.writeFileSync(path.join(out,file),value);
 fs.writeFileSync(path.join(out,'MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
 return manifest;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const args=Object.fromEntries(process.argv.slice(2).map(x=>{const m=x.match(/^--(root|out)=(.+)$/);requireThat(m,'CONTACT_PREPARER_ARGUMENT_REFUSED');return [m[1],m[2]];}));
 requireThat(args.root,'CONTACT_PREPARER_ROOT_REQUIRED');const result=await prepare(args);
 console.log(JSON.stringify({rootCommit:result.rootCommit,pending:result.pendingCounts,finalClosure:result.finalClosureCounts,envelope:result.directEnvelope,
  native:'NOT_RUN',out:args.out??path.join(here,'generated')}));
}
