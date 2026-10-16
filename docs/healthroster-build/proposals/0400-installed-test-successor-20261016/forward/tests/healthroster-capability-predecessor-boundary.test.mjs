import{execFileSync}from'node:child_process';
import{createRequire}from'node:module';
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import os from'node:os';import path from'node:path';import{fileURLToPath,pathToFileURL}from'node:url';
import{CAPABILITY_PREDECESSOR_MIGRATION as pin,capabilityPredecessorCompanionFor,capabilityPredecessorMigrationSql}from'../scripts/healthroster-capability-predecessor-boundary.mjs';
import{compileManagedUnit,unitCheckpointSql,assertWriterSql,migrationLedgerSql,managedReleaseContext,managedAdmissionSql,OWNER_RLS_ENVELOPE,IMMUTABLE_GUARD_DEPENDENCY}from'../scripts/cloudtms-managed-release.mjs';
import{compiledAtomicRehearsalUnit,reviewedDiagnosticCodes,rehearsalSourceBinding,assertRehearsalSourceBinding}from'../scripts/healthroster-install1-full-rehearsal.mjs';
import{canonical,digest,readManagedClosure,scanManagedSql,managedManifestHash}from'../scripts/cloudtms-managed-release-sql.mjs';
import{mapExecutableSqlSource,inventory,logicalPostgresExecutablePolicy}from'../scripts/cloudtms-db-release-lib.mjs';
import{BOOTSTRAP_PIN,BASELINE_MIGRATION_PREFIX,MANAGED_EXCEPTIONS,executionPolicy}from'../scripts/cloudtms-managed-execution-policy.mjs';
import{CONTACT_CLAIM_MIGRATION}from'../scripts/healthroster-contact-claim-boundary.mjs';
import{MONEY_PREDECESSOR_MIGRATION}from'../scripts/healthroster-money-predecessor-boundary.mjs';
const targetRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),sourceRoot=process.env.HEALTHROSTER_PROPOSAL_SOURCE_ROOT||targetRoot;
const preservedProposalPath='docs/healthroster-build/v6-owner-delivery-20261016/CAPABILITY_PREDECESSOR_COMPILER_SOURCE_20261016/PROPOSAL.json';
const compilerProposal=JSON.parse(fs.readFileSync(path.join(targetRoot,preservedProposalPath)));
assert.equal(compilerProposal.rootCommit,'32ee8711f54829b5a13a0c59dabc9c29a39a2aa3');
const frozenCommit=compilerProposal.rootCommit,gitRoot=process.env.HEALTHROSTER_FROZEN_BASELINE_GIT_ROOT||sourceRoot;
async function frozenBaselineGraph(){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'hr0400-frozen32ee-')),sources=new Map(),barePackageRewrites=[],executedFiles=[],packageRootRequire=createRequire(path.join(gitRoot,'package.json'));
 function dispose(){const at=fs.realpathSync(root);assert.equal(path.dirname(at),fs.realpathSync(os.tmpdir()));assert(path.basename(at).startsWith('hr0400-frozen32ee-'));fs.rmSync(at,{recursive:true,force:false});}
 function collect(file){
  assert(!file.startsWith('../')&&!path.posix.isAbsolute(file)&&/\.(?:[cm]?js|json|sql)$/.test(file));if(sources.has(file))return;
  const bytes=execFileSync('git',['show',frozenCommit+':'+file],{cwd:gitRoot,maxBuffer:10e6}),text=bytes.toString();sources.set(file,text);
  const to=path.join(root,file);fs.mkdirSync(path.dirname(to),{recursive:true});fs.writeFileSync(to,bytes);
  if(!/\.[cm]?js$/.test(file))return;
  const imports=[...text.matchAll(/(?:^|[;\n])[ \t]*(?:import\b[^;]*?\bfrom\s*|export[ \t]+\{[^}]*\}[ \t]+from\s*|import[ \t]+)(['"])([^'"]+)\1/g),...text.matchAll(/\bimport\(\s*(['"])([^'"]+)\1/g)];let executable=text;const changes=[];
  for(const match of imports){const spec=match[2];if(spec.startsWith('.')){collect(path.posix.normalize(path.posix.join(path.posix.dirname(file),spec)));continue;}if(spec.startsWith('node:'))continue;
   assert.equal(spec,'xlsx','Only the existing exact locked bare package may resolve outside the frozen graph');collect('package.json');collect('package-lock.json');
   const locked=JSON.parse(sources.get('package-lock.json')).packages['node_modules/xlsx'];assert(locked?.version);
   assert.equal(digest(canonical(fs.readFileSync(path.join(gitRoot,'package-lock.json'),'utf8'))),digest(sources.get('package-lock.json')),'Current dependency lock must equal32ee');
   const entry=packageRootRequire.resolve(spec);let packageDir=path.dirname(entry);while(!fs.existsSync(path.join(packageDir,'package.json'))){const parent=path.dirname(packageDir);assert.notEqual(parent,packageDir);packageDir=parent;}const installed=JSON.parse(fs.readFileSync(path.join(packageDir,'package.json')));assert.equal(installed.name,spec);assert.equal(installed.version,locked.version);
   const esm=installed.exports?.['.']?.import;assert.equal(esm,'./xlsx.mjs');const esmEntry=path.resolve(packageDir,esm);assert(esmEntry.startsWith(packageDir+path.sep));
   const from=match[0],url=pathToFileURL(esmEntry).href,toToken=from.replace(match[1]+spec+match[1],match[1]+url+match[1]);assert.equal(executable.split(from).length,2);executable=executable.replace(from,toToken);changes.push({from,to:toToken});barePackageRewrites.push({file,kind:'BARE_PACKAGE_SPECIFIER_ONLY',package:spec,version:locked.version,entrySha256:digest(fs.readFileSync(esmEntry)),lockSha256:digest(sources.get('package-lock.json')),from,to:toToken});
  }
  let reversed=executable;for(const change of changes.toReversed()){assert.equal(reversed.split(change.to).length,2);reversed=reversed.replace(change.to,change.from);}assert.equal(reversed,text);fs.writeFileSync(to,executable);executedFiles.push({file,originalSha256:digest(text),executedSha256:digest(executable),fullReverseEquality:true});
 }
 try{
  for(const p of compilerProposal.pins){collect(p.file);assert.equal(digest(sources.get(p.file)),p.beforeSha256,'Exact frozen critical baseline '+p.file);}collect('supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql');
  const compiler=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release.mjs'))),harness=await import(pathToFileURL(path.join(root,'scripts/healthroster-install1-full-rehearsal.mjs'))),lib=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs'))),sql=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),policy=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-execution-policy.mjs')));
  const files=[...sources].map(([file,text])=>({file,sha256:digest(text)})).sort((a,b)=>a.file.localeCompare(b.file));
  console.log('HR_CAPABILITY_FROZEN_BASELINE '+JSON.stringify({commit:frozenCommit,completeFiles:files.length,relativeImportRewrites:0,currentBackendModulesBorrowed:0,barePackageRewrites,graphSha256:digest(JSON.stringify(files)),executedGraphSha256:digest(JSON.stringify(executedFiles.sort((a,b)=>a.file.localeCompare(b.file)))),allReverseEquality:executedFiles.every(f=>f.fullReverseEquality),criticalFiles:compilerProposal.pins.map(p=>({file:p.file,sha256:p.beforeSha256}))}));
  return{root,sources,compiler,harness,lib,sql,policy,files,dispose};
 }catch(error){dispose();throw error;}
}
const frozenBaseline=await frozenBaselineGraph(),baseCompiler=frozenBaseline.compiler,baseHarness=frozenBaseline.harness;
test.after(()=>frozenBaseline.dispose());
// Installed runs use their real repository inventory. The isolated default-path
// simulation may enumerate the real Git source through the exact unchanged lib;
// this never supplies a baseline compiler/harness or invents an inventory count.
const repoInventory=path.resolve(gitRoot)===path.resolve(sourceRoot)?inventory:(await import(pathToFileURL(path.join(gitRoot,'scripts/cloudtms-db-release-lib.mjs')))).inventory;
assert.equal(digest(canonical(fs.readFileSync(path.join(gitRoot,'scripts/cloudtms-db-release-lib.mjs'),'utf8'))),digest(frozenBaseline.sources.get('scripts/cloudtms-db-release-lib.mjs')));
const inventoryForSource=()=>repoInventory();
const item={path:pin.path,sha256:pin.contentSha256},code=/HEALTHROSTER_CAPABILITY_PREDECESSOR_|MANAGED_SOURCE_CHANGED/;
function fixture(){const root=fs.mkdtempSync(path.join(os.tmpdir(),'hr-capability0400-source-'));
 for(const [file,base]of[[pin.path,sourceRoot],[pin.constructorPath,sourceRoot],[pin.companionPath,targetRoot],...pin.evidenceFiles.map(x=>[x.file,targetRoot])]){const to=path.join(root,file);fs.mkdirSync(path.dirname(to),{recursive:true});fs.copyFileSync(path.join(base,file),to);}return root;}
function cleanup(root){const absolute=fs.realpathSync(root),parent=fs.realpathSync(os.tmpdir());assert.equal(path.dirname(absolute),parent);assert(path.basename(absolute).startsWith('hr-capability0400-source-'));fs.rmSync(absolute,{recursive:true,force:false});}
function withFixture(fn){const root=fixture();try{return fn(root);}finally{cleanup(root);}}
const compile=(root,mapSource=mapExecutableSqlSource)=>compileManagedUnit(item,'migration',{root,mapSource});
const originalMode=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
test.after(()=>{if(originalMode===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=originalMode;});

test('raw and audited source retain one immutable0400 closure, exact body and exact LOCK+DO',()=>withFixture(root=>{
 for(const map of[x=>x,mapExecutableSqlSource]){const u=compile(root,map),sql=capabilityPredecessorMigrationSql(u,mapExecutableSqlSource);assert.equal(u.closure.closureHash,pin.closureSha256);assert.equal(digest(u.body),pin.rawBodySha256);assert.equal(digest(sql),pin.companionSha256);assert.equal(digest(canonical(fs.readFileSync(path.join(root,pin.path),'utf8'))),pin.contentSha256);assert.deepEqual(scanManagedSql(sql).statements.map(x=>x.tokens[0]),['LOCK','DO']);assert.deepEqual(u.capabilityPredecessorCompanion.statementSha256,pin.statementSha256);}
}));
test('actual managed packet retains one transaction and writer/prebody/body/writer/original ledger order',()=>withFixture(root=>{
 const u=compile(root),prebody=capabilityPredecessorMigrationSql(u,mapExecutableSqlSource),context={releaseId:'capability-prebody-source-test'},packet=unitCheckpointSql(u,context);
 assert.equal(packet,`begin;\n${assertWriterSql}\n${prebody}\n${u.body}\n${assertWriterSql}\n${migrationLedgerSql(item,context)}\ncommit;`);
 const statements=scanManagedSql(packet).statements;assert.equal(statements.filter(x=>x.tokens[0]==='BEGIN').length,1);assert.equal(statements.filter(x=>x.tokens[0]==='COMMIT').length,1);assert.equal(packet.split(assertWriterSql).length-1,2);assert.equal(packet.split(prebody).length-1,1);assert.equal(packet.split(u.body).length-1,1);assert.equal(statements.at(-1).tokens[0],'COMMIT');assert(packet.indexOf(prebody)<packet.indexOf(u.body));assert(packet.indexOf(u.body)<packet.indexOf('insert into private.cloudtms_migration_ledger'));assert(packet.includes(pin.contentSha256));
}));
test('actual rehearsal uses the same checked prebody before unchanged body and retains exact receipt',()=>withFixture(root=>{
 const u=compile(root),prebody=capabilityPredecessorMigrationSql(u,mapExecutableSqlSource),r=compiledAtomicRehearsalUnit(u);assert.equal(r.sql,prebody+'\n'+u.body);assert.equal(r.sqlHash,digest(r.sql));assert.equal(r.sourceSqlHash,pin.contentSha256);assert(!scanManagedSql(r.sql).statements.some(x=>['BEGIN','COMMIT','ROLLBACK'].includes(x.tokens[0])));assert.deepEqual(r.atomicCapabilityPredecessorCompanion,{version:pin.version,companionSha256:pin.companionSha256,statementSha256:pin.statementSha256,placement:'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION',evidenceFiles:pin.evidenceFiles,nativeProof:'NOT_RUN'});assert(r.sourceFiles.some(x=>x.file===pin.companionPath&&x.sha256===pin.companionSha256));assert.equal(r.atomicMoneyPredecessorCompanion,undefined);assert.equal(r.atomicContactClaimCompanion,undefined);
}));
test('absent, edited, borrowed, incomplete and postbody receipts refuse release and rehearsal',()=>withFixture(root=>{
 const u=compile(root),c=u.capabilityPredecessorCompanion;
 for(const receipt of[null,{...c,sql:c.sql+'\nselect 1;'},{...c,placement:'AFTER_BODY'},{...c,version:MONEY_PREDECESSOR_MIGRATION.version},{...c,migrationPath:CONTACT_CLAIM_MIGRATION.path},{...c,statementSha256:[]},{...c,evidenceFiles:[]},{...c,sourceFiles:[]},{...c,bodySha256:'0'.repeat(64)}]){const forged={...u,capabilityPredecessorCompanion:receipt};assert.throws(()=>unitCheckpointSql(forged,{releaseId:'x'}),code);assert.throws(()=>compiledAtomicRehearsalUnit(forged),code);}
}));
test('wrong body/kind/item/closure/alternate envelope refuses before packet emission',()=>withFixture(root=>{
 const u=compile(root),patches=[{body:u.body+'\nselect 1;'},{kind:'repeatable'},{item:{...item,path:MONEY_PREDECESSOR_MIGRATION.path}},{item:{...item,sha256:'0'.repeat(64)}},{closure:{...u.closure,closureHash:'0'.repeat(64)}},{closure:{...u.closure,files:new Map([...u.closure.files,['supabase/extra.sql','0'.repeat(64)]])}},{closure:{...u.closure,ordered:[...u.closure.ordered,...u.closure.ordered]}},{closure:{...u.closure,source:u.closure.source+'\n-- changed'}},{exception:{reason:'OTHER'}},{ownerRlsEnvelope:{}},{immutableGuardDependency:{}},{contactClaimCompanion:{}},{moneyPredecessorCompanion:{}}];
 for(const patch of patches){assert.throws(()=>unitCheckpointSql({...u,...patch},{releaseId:'x'}),code);assert.throws(()=>compiledAtomicRehearsalUnit({...u,...patch}),/HEALTHROSTER_CAPABILITY_PREDECESSOR_|REHEARSAL_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED/);}
}));
test('wrong kind, changed raw ordered source and changed selector cannot retain declared hashes',()=>withFixture(root=>{
 const c=readManagedClosure(pin.path,root,[],mapExecutableSqlSource);assert.throws(()=>capabilityPredecessorCompanionFor(c,'repeatable',{root}),code);
 const changed=c.expanded.replace("position('SIGNED_CHECK_ONLY'", "position('OTHER'");assert.notEqual(changed,c.expanded);assert.throws(()=>capabilityPredecessorCompanionFor({...c,source:changed,expanded:changed},'migration',{root}),code);assert.throws(()=>capabilityPredecessorCompanionFor({...c,ordered:[{...c.ordered[0],source:c.ordered[0].source+'\n-- drift'}]},'migration',{root}),code);
}));
test('all exact migration, constructor, program and27 preserved evidence files refuse byte drift',()=>withFixture(root=>{
 for(const file of[pin.path,pin.constructorPath,pin.companionPath,...pin.evidenceFiles.map(x=>x.file)]){const to=path.join(root,file),before=fs.readFileSync(to);try{fs.appendFileSync(to,'\n-- changed exact evidence\n');assert.throws(()=>compile(root),code);}finally{fs.writeFileSync(to,before);}}
}));
test('missing program or evidence cannot fall back to an unguarded migration body',()=>withFixture(root=>{
 for(const file of[pin.companionPath,pin.evidencePath]){const to=path.join(root,file),before=fs.readFileSync(to);try{fs.unlinkSync(to);assert.throws(()=>compile(root));}finally{fs.writeFileSync(to,before);}}
}));
test('lock weakening, default-false selector drift, new expected name or weaker set comparison refuses',()=>withFixture(root=>{
 const to=path.join(root,pin.companionPath),before=fs.readFileSync(to,'utf8');
 for(const [from,toText]of[['in access exclusive mode','in access share mode'],['pg_catalog.pg_get_constraintdef(c.oid) definition','pg_catalog.pg_get_constraintdef(c.oid,true) definition'],['weekly_source_group_rule_assignments_check2','unexpected_check'],['v_actual is distinct from','v_actual is not null and v_actual is distinct from']]){assert(before.includes(from));try{fs.writeFileSync(to,before.replace(from,toText));assert.throws(()=>compile(root),code);}finally{fs.writeFileSync(to,before);}}
}));
test('arbitrary mapper edits to migration, constructor or program refuse',()=>withFixture(root=>{
 for(const file of[pin.path,pin.constructorPath,pin.companionPath])assert.throws(()=>compile(root,(s,f)=>f===file?mapExecutableSqlSource(s,f)+'\nselect 1;':mapExecutableSqlSource(s,f)),code);
}));
test('canonical CRLF preserves the exact reviewed bytes and evidence contract',()=>withFixture(root=>{
 for(const file of[pin.path,pin.constructorPath,pin.companionPath,...pin.evidenceFiles.map(x=>x.file)]){const to=path.join(root,file);fs.writeFileSync(to,canonical(fs.readFileSync(to,'utf8')).replaceAll('\n','\r\n'));}assert.equal(digest(capabilityPredecessorMigrationSql(compile(root),mapExecutableSqlSource)),pin.companionSha256);
}));
test('finite boundary never supplies a companion for foreign, money or contact paths',()=>withFixture(root=>{
 for(const foreign of['supabase/migrations/foreign.sql',MONEY_PREDECESSOR_MIGRATION.path,CONTACT_CLAIM_MIGRATION.path])assert.equal(capabilityPredecessorCompanionFor({path:foreign},'migration',{root}),null);
}));
test('descriptor is immutable and mutable unit receipts cannot mutate its evidence pins',()=>withFixture(root=>{
 assert.throws(()=>{pin.evidenceFiles[0].sha256='0'.repeat(64);},TypeError);assert.throws(()=>pin.statementSha256.push('0'.repeat(64)),TypeError);
 const u=compile(root),before=pin.evidenceFiles[0].sha256;u.capabilityPredecessorCompanion.evidenceFiles[0].sha256='0'.repeat(64);assert.equal(pin.evidenceFiles[0].sha256,before);assert.throws(()=>unitCheckpointSql(u,{releaseId:'x'}),code);
}));
test('managed manifest/admission distinguish this exact new descriptor from existing1430/contact policy',()=>{
 const current=inventoryForSource(),release=JSON.parse(fs.readFileSync(path.join(sourceRoot,'supabase/release/current-release.json'))),context={mode:'UPGRADE',environment:'TEST',releaseId:'capability-prebody-source-test',gitCommit:'a'.repeat(40),expectedHash:'b'.repeat(64),expectedDatabase:'cloudtms_test_clone',customerKey:''};
 const next=managedReleaseContext({root:sourceRoot,current,release,context}),prior=managedManifestHash({root:sourceRoot,current,release,executionPolicy:{bootstrap:BOOTSTRAP_PIN,baselinePrefix:BASELINE_MIGRATION_PREFIX,exceptions:MANAGED_EXCEPTIONS,ownerRlsEnvelope:OWNER_RLS_ENVELOPE,immutableGuardDependency:IMMUTABLE_GUARD_DEPENDENCY,contactClaimAtomicCompanion:CONTACT_CLAIM_MIGRATION,moneyCheckPrebodyCompanion:MONEY_PREDECESSOR_MIGRATION,logicalOwnerMapping:logicalPostgresExecutablePolicy()}});assert.notEqual(next.manifestHash,prior);const admission=managedAdmissionSql(next);assert(admission.includes(pin.version));assert(admission.includes(pin.companionSha256));assert(admission.includes(next.manifestHash));assert(!admission.includes(prior));
});

// Populate only missing proposal-local read inputs for the actual source-binding
// function. Imports still use the read-only source overlay. No Root file changes.
function withBindingInputs(fn){const source=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-install1-full-rehearsal.mjs'),'utf8'),start=source.indexOf('const releaseInputFiles=')+'const releaseInputFiles='.length,stop=source.indexOf('];',start);
 const expression=source.slice(start,stop+1),reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_ENVELOPE_REVIEW.json',calleePath='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_CALLEE_AUDIT.json';
 const files=Function('CAPABILITY_PREDECESSOR_MIGRATION','MONEY_PREDECESSOR_MIGRATION','managedEnvelopeReviewPath','managedCalleeAuditPath','return '+expression)(pin,MONEY_PREDECESSOR_MIGRATION,reviewPath,calleePath),made=[],dirs=new Set();
 try{for(const file of[...files,'supabase/release/current-contract.json']){const to=path.resolve(targetRoot,file);assert(to.startsWith(targetRoot+path.sep));if(fs.existsSync(to))continue;let parent=path.dirname(to);while(parent!==targetRoot&&!fs.existsSync(parent)){dirs.add(parent);parent=path.dirname(parent);}fs.mkdirSync(path.dirname(to),{recursive:true});const bytes=fs.readFileSync(path.join(sourceRoot,file));fs.writeFileSync(to,bytes);made.push({to,sha256:digest(bytes)});}return fn(files);}finally{for(const row of made.toReversed()){assert.equal(digest(fs.readFileSync(row.to)),row.sha256,'Preserve unexpected fixture edit');fs.unlinkSync(row.to);}for(const at of[...dirs].sort((a,b)=>b.length-a.length))if(fs.existsSync(at)&&!fs.readdirSync(at).length)fs.rmdirSync(at);}
}
test('actual rehearsal source binding binds new helper/program/constructor/all27 evidence and detects drift',()=>withBindingInputs(()=>{
 const current=inventoryForSource(),review={sourceFiles:[]},binding=rehearsalSourceBinding({current,review});for(const file of['scripts/healthroster-capability-predecessor-boundary.mjs',pin.companionPath,pin.constructorPath,...pin.evidenceFiles.map(x=>x.file)])assert(binding.inputs.some(x=>x.file===file));assertRehearsalSourceBinding(binding,{current,review,expectedContractHash:binding.contractSha256});
 for(const file of['scripts/healthroster-capability-predecessor-boundary.mjs',pin.companionPath,pin.evidencePath]){const to=path.join(targetRoot,file),before=fs.readFileSync(to);try{fs.appendFileSync(to,'\n-- source binding drift\n');assert.throws(()=>assertRehearsalSourceBinding(binding,{current,review,expectedContractHash:binding.contractSha256}),/REHEARSAL_RELEASE_SOURCE_BINDING_DRIFT/);}finally{fs.writeFileSync(to,before);}}
}));
test('only exact new companion source supplies its two closed catalogue-refusal codes',()=>{
 const codes=reviewedDiagnosticCodes({sourceFiles:[{file:pin.companionPath,sha256:pin.companionSha256}]});assert.deepEqual(codes,['HEALTHROSTER_CAPABILITY_PREDECESSOR_CHECKS_CHANGED','HEALTHROSTER_CAPABILITY_PREDECESSOR_RELATION_CHANGED']);assert.throws(()=>reviewedDiagnosticCodes({sourceFiles:[{file:pin.companionPath,sha256:'0'.repeat(64)}]}),/REHEARSAL_DIAGNOSTIC_SOURCE_DRIFT/);
 const source=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-install1-full-rehearsal.mjs'),'utf8');assert(source.includes('units.filter(unit=>unit.atomicCapabilityPredecessorCompanion)'));assert(source.includes('.filter(row=>row.file===CAPABILITY_PREDECESSOR_MIGRATION.companionPath)'));assert(source.includes('atomicCapabilityPredecessorCompanion:units[i].atomicCapabilityPredecessorCompanion'));
});
test('actual1430 release/rehearsal SQL and receipt remain byte-identical to base',()=>{
 const i={path:MONEY_PREDECESSOR_MIGRATION.path,sha256:MONEY_PREDECESSOR_MIGRATION.contentSha256},before=baseCompiler.compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:frozenBaseline.lib.mapExecutableSqlSource}),after=compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:mapExecutableSqlSource});assert.equal(unitCheckpointSql(after,{releaseId:'preserve'}),baseCompiler.unitCheckpointSql(before,{releaseId:'preserve'}));assert.deepEqual(compiledAtomicRehearsalUnit(after),baseHarness.compiledAtomicRehearsalUnit(before));
});
test('actual Contact0339 postbody composition and receipt remain byte-identical to base',()=>{
 const i={path:CONTACT_CLAIM_MIGRATION.path,sha256:CONTACT_CLAIM_MIGRATION.contentSha256},before=baseCompiler.compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:frozenBaseline.lib.mapExecutableSqlSource}),after=compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:mapExecutableSqlSource});assert.equal(unitCheckpointSql(after,{releaseId:'preserve'}),baseCompiler.unitCheckpointSql(before,{releaseId:'preserve'}));assert.deepEqual(compiledAtomicRehearsalUnit(after),baseHarness.compiledAtomicRehearsalUnit(before));
});
test('actual Invoice native gate packet/literal/callee audit remain exact; only coherent owner/review pins renew',()=>{
 const file='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_ENVELOPE_REVIEW.json',old=JSON.parse(fs.readFileSync(path.join(frozenBaseline.root,file))),next=JSON.parse(fs.readFileSync(path.join(targetRoot,file)));
 for(let i=0;i<old.rows.length;i++)if(old.rows[i].existingNativeGate){const a=old.rows[i].existingNativeGate,b=next.rows[i].existingNativeGate;assert.equal(b.literal,a.literal);assert.equal(b.sha256,a.sha256);assert.equal(b.ownerSourceHash,digest(canonical(fs.readFileSync(path.join(targetRoot,'scripts/cloudtms-managed-release.mjs'),'utf8'))));const closure=readManagedClosure(old.rows[i].path,sourceRoot),exception=executionPolicy(closure),u={kind:'repeatable',item:{path:old.rows[i].path,sha256:old.rows[i].rawClosureHash},exception,script:'HR_REHEARSAL_NATIVE_GATE_BOUNDARY'};assert.equal(unitCheckpointSql(u,{releaseId:'preserve'}),baseCompiler.unitCheckpointSql(u,{releaseId:'preserve'}));}
 const module=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-reviewed-managed-envelope.mjs'),'utf8');assert(module.includes("const reviewHash='"+digest(fs.readFileSync(path.join(targetRoot,file)))+"';"));
});
test('all16 exact edits reverse all four complete files without touching other source',()=>{
 const proposal=JSON.parse(fs.readFileSync(path.join(targetRoot,preservedProposalPath)));assert.equal(proposal.edits.length,16);assert.equal(proposal.pins.length,4);
 for(const p of proposal.pins){let value=fs.readFileSync(path.join(targetRoot,p.file),'utf8');assert.equal(digest(value),p.afterSha256);for(const edit of proposal.edits.filter(e=>e.file===p.file).reverse()){assert.equal(value.split(edit.to).length,2);value=value.replace(edit.to,edit.from);}assert.equal(digest(value),p.beforeSha256);assert.equal(value,frozenBaseline.sources.get(p.file));}
});

test('installed preservation baseline is exact original Git32ee code and cannot borrow current compiler/harness',()=>{
 assert.notEqual(baseCompiler.compileManagedUnit,compileManagedUnit);assert.notEqual(baseCompiler.unitCheckpointSql,unitCheckpointSql);assert.notEqual(baseHarness.compiledAtomicRehearsalUnit,compiledAtomicRehearsalUnit);
 assert(!baseCompiler.compileManagedUnit.toString().includes('capabilityPredecessorCompanion'));assert(!baseHarness.compiledAtomicRehearsalUnit.toString().includes('capabilityPrebody'));
 for(const p of compilerProposal.pins){assert.equal(digest(frozenBaseline.sources.get(p.file)),p.beforeSha256);assert.equal(digest(canonical(fs.readFileSync(path.join(targetRoot,p.file),'utf8'))),p.afterSha256);}
 assert(frozenBaseline.sources.has('scripts/healthroster-money-predecessor-boundary.mjs'));assert(frozenBaseline.sources.has('scripts/healthroster-contact-claim-boundary.mjs'));assert.equal(frozenBaseline.sources.has('scripts/healthroster-capability-predecessor-boundary.mjs'),false);
});
