import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {MONEY_PREDECESSOR_MIGRATION as pin,moneyPredecessorCompanionFor,moneyPredecessorMigrationSql} from '../scripts/healthroster-money-predecessor-boundary.mjs';
import {compileManagedUnit,unitCheckpointSql,assertWriterSql,managedReleaseContext,managedAdmissionSql,
 OWNER_RLS_ENVELOPE,IMMUTABLE_GUARD_DEPENDENCY} from '../scripts/cloudtms-managed-release.mjs';
import {compiledAtomicRehearsalUnit,reviewedDiagnosticCodes} from '../scripts/healthroster-install1-full-rehearsal.mjs';
import {canonical,digest,readManagedClosure,scanManagedSql,managedManifestHash} from '../scripts/cloudtms-managed-release-sql.mjs';
import {mapExecutableSqlSource,inventory,logicalPostgresExecutablePolicy} from '../scripts/cloudtms-db-release-lib.mjs';
import {BOOTSTRAP_PIN,BASELINE_MIGRATION_PREFIX,MANAGED_EXCEPTIONS} from '../scripts/cloudtms-managed-execution-policy.mjs';
import {CONTACT_CLAIM_MIGRATION} from '../scripts/healthroster-contact-claim-boundary.mjs';
const targetRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const sourceRoot=process.env.HEALTHROSTER_PROPOSAL_SOURCE_ROOT||targetRoot;
const code=/HEALTHROSTER_MONEY_PREDECESSOR_|MANAGED_SOURCE_CHANGED/;
const fixture=()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'healthroster-money-predecessor-source-'));
 for(const [file,base] of [[pin.path,sourceRoot],[pin.constructorPath,sourceRoot],[pin.companionPath,targetRoot],[pin.evidencePath,targetRoot]]){
  fs.mkdirSync(path.dirname(path.join(root,file)),{recursive:true});fs.copyFileSync(path.join(base,file),path.join(root,file));
 }
 return root;
};
function cleanup(root){const absolute=path.resolve(root);assert(absolute.startsWith(path.resolve(os.tmpdir())+path.sep));assert(path.basename(absolute).startsWith('healthroster-money-predecessor-source-'));fs.rmSync(absolute,{recursive:true,force:true});}
const item={path:pin.path,sha256:pin.contentSha256};
const compile=(root,mapSource=mapExecutableSqlSource)=>compileManagedUnit(item,'migration',{root,mapSource});
const withFixture=work=>{const root=fixture();try{return work(root);}finally{cleanup(root);}};
const previousOwnerMode=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
test.after(()=>{if(previousOwnerMode===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previousOwnerMode;});

test('the exact raw and audited-owner bodies retain one immutable closure and exact prebody',()=>withFixture(root=>{
 for(const [map,bodyHash] of [[x=>x,pin.rawBodySha256],[mapExecutableSqlSource,pin.mappedBodySha256]]){
  const unit=compile(root,map),prebody=moneyPredecessorMigrationSql(unit,mapExecutableSqlSource);
  assert.equal(unit.closure.closureHash,pin.closureSha256);assert.equal(digest(unit.body),bodyHash);
  assert.equal(digest(prebody),pin.companionSha256);assert.equal(digest(canonical(fs.readFileSync(path.join(root,pin.path)))),pin.contentSha256);
  assert.deepEqual(scanManagedSql(prebody).statements.map(x=>x.tokens[0]),['LOCK','DO']);
 }
}));

test('actual managed packet has one transaction and retains writer / prebody / body / writer / original ledger order',()=>withFixture(root=>{
 const unit=compile(root),prebody=moneyPredecessorMigrationSql(unit,mapExecutableSqlSource),packet=unitCheckpointSql(unit,{releaseId:'money-prebody-source-test'});
 assert.equal(packet,`begin;\n${assertWriterSql}\n${prebody}\n${unit.body}\n${assertWriterSql}\n${packet.slice(packet.indexOf('insert into private.cloudtms_migration_ledger'),-8)}\ncommit;`);
 const rows=scanManagedSql(packet).statements;
 assert.equal(rows.filter(x=>x.tokens[0]==='BEGIN').length,1);assert.equal(rows.filter(x=>x.tokens[0]==='COMMIT').length,1);
 assert.equal(rows.at(-1).tokens[0],'COMMIT');assert.equal(packet.split(assertWriterSql).length-1,2);
 assert.equal(packet.split(prebody).length-1,1);assert.equal(packet.split(unit.body).length-1,1);
 const lockAt=packet.indexOf(prebody),bodyAt=packet.indexOf(unit.body),ledgerAt=packet.indexOf('insert into private.cloudtms_migration_ledger');
 assert(lockAt>packet.indexOf(assertWriterSql));assert(bodyAt>=lockAt+prebody.length);assert(ledgerAt>bodyAt+unit.body.length);
 assert(packet.includes(pin.contentSha256));assert(packet.includes("'money-prebody-source-test'"));
}));

test('actual rehearsal assembly emits the identical checked prebody first without introducing transaction controls',()=>withFixture(root=>{
 const unit=compile(root),prebody=moneyPredecessorMigrationSql(unit,mapExecutableSqlSource),rehearsal=compiledAtomicRehearsalUnit(unit);
 assert.equal(rehearsal.sql,prebody+'\n'+unit.body);assert.equal(rehearsal.sqlHash,digest(rehearsal.sql));
 assert(!scanManagedSql(rehearsal.sql).statements.some(x=>['BEGIN','COMMIT','ROLLBACK'].includes(x.tokens[0])));
 assert.equal(rehearsal.atomicMoneyPredecessorCompanion.placement,'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION');
 assert.equal(rehearsal.atomicMoneyPredecessorCompanion.nativeProof,'NOT_RUN');
 assert(rehearsal.sourceFiles.some(x=>x.file===pin.companionPath&&x.sha256===pin.companionSha256));
 assert.equal(rehearsal.atomicContactClaimCompanion,undefined);
}));

test('absent, edited, borrowed and postbody-only companion receipts cannot produce a release or rehearsal packet',()=>withFixture(root=>{
 const unit=compile(root);
 for(const companion of [null,{...unit.moneyPredecessorCompanion,sql:unit.moneyPredecessorCompanion.sql+'\nselect 1;'},
  {...unit.moneyPredecessorCompanion,placement:'AFTER_BODY'},
  {...unit.moneyPredecessorCompanion,migrationPath:CONTACT_CLAIM_MIGRATION.path},
  {...unit.moneyPredecessorCompanion,statementSha256:[]},
  {...unit.moneyPredecessorCompanion,sourceFiles:[]}]){
  const forged={...unit,moneyPredecessorCompanion:companion};
  assert.throws(()=>unitCheckpointSql(forged,{releaseId:'x'}),code);assert.throws(()=>compiledAtomicRehearsalUnit(forged),code);
 }
}));

test('body, kind, item, closure and alternate-envelope changes refuse before packet emission',()=>withFixture(root=>{
 const unit=compile(root);
 const variants=[{body:unit.body+'\nselect 1;'},{kind:'repeatable'},{item:{...item,path:'supabase/migrations/foreign.sql'}},
  {item:{...item,sha256:'0'.repeat(64)}},{closure:{...unit.closure,closureHash:'0'.repeat(64)}},
  {closure:{...unit.closure,files:new Map([...unit.closure.files,['supabase/foreign.sql','0'.repeat(64)]])}},
  {closure:{...unit.closure,ordered:[...unit.closure.ordered,...unit.closure.ordered]}},
  {closure:{...unit.closure,source:unit.closure.source+'\nselect 1;'}},
  {exception:{reason:'OTHER'}},{ownerRlsEnvelope:{}},{immutableGuardDependency:{}},{contactClaimCompanion:{}}];
 for(const patch of variants)assert.throws(()=>unitCheckpointSql({...unit,...patch},{releaseId:'x'}),code);
}));

test('wrong kind and raw/mapped source tampering fail even if declared item and closure hashes are retained',()=>withFixture(root=>{
 const closure=readManagedClosure(pin.path,root,[],mapExecutableSqlSource);
 assert.throws(()=>moneyPredecessorCompanionFor(closure,'repeatable',{root}),code);
 const expanded=closure.expanded.replace("like '%comparison_result%'","like '%anything%'");assert.notEqual(expanded,closure.expanded);
 assert.throws(()=>moneyPredecessorCompanionFor({...closure,source:expanded,expanded},'migration',{root}),code);
 assert.throws(()=>moneyPredecessorCompanionFor({...closure,ordered:[{...closure.ordered[0],source:closure.ordered[0].source+'\n-- drift'}]},'migration',{root}),code);
}));

test('every preserved source dependency is exact and cannot authorize changed expected constraints or appended SQL',()=>withFixture(root=>{
 for(const file of [pin.path,pin.constructorPath,pin.companionPath,pin.evidencePath]){
  const target=path.join(root,file),original=fs.readFileSync(target);
  fs.appendFileSync(target,'\n-- changed exact source\n');assert.throws(()=>compile(root),code);fs.writeFileSync(target,original);
 }
 const target=path.join(root,pin.companionPath),original=fs.readFileSync(target,'utf8');
 for(const [from,to] of [["'%comparison_result%'","'%foreign%'"],['weekly_source_charge_checks_check2','unexpected_check'],
  ['full join actual a using (name)','left join actual a using (name)'],['in access exclusive mode','in access share mode']]){
  assert(original.includes(from));fs.writeFileSync(target,original.replace(from,to));assert.throws(()=>compile(root),code);
 }
 fs.writeFileSync(target,original);
}));

test('an arbitrary source mapper cannot edit migration or prebody SQL while keeping metadata pins',()=>withFixture(root=>{
 assert.throws(()=>compile(root,(source,file)=>file===pin.companionPath?source+'\nselect 1;':mapExecutableSqlSource(source,file)),code);
 assert.throws(()=>compile(root,(source,file)=>file===pin.path?mapExecutableSqlSource(source,file)+'\nselect 1;':source),code);
}));

test('canonical CRLF source handling preserves the exact reviewed logical bytes',()=>withFixture(root=>{
 for(const file of [pin.path,pin.constructorPath,pin.companionPath,pin.evidencePath]){
  const target=path.join(root,file);fs.writeFileSync(target,canonical(fs.readFileSync(target,'utf8')).replaceAll('\n','\r\n'));
 }
 assert.equal(digest(moneyPredecessorMigrationSql(compile(root),mapExecutableSqlSource)),pin.companionSha256);
}));

test('the finite hook does not classify unrelated paths or the unqualified capability0400 migration',()=>withFixture(root=>{
 for(const foreign of ['supabase/migrations/foreign.sql','supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql',CONTACT_CLAIM_MIGRATION.path])
  assert.equal(moneyPredecessorCompanionFor({path:foreign},'migration',{root}),null);
 assert.equal(Object.values(pin).some(x=>typeof x==='string'&&x.includes('14102026_0400')),false);
}));

test('managed manifest and admission bind the new finite descriptor and prebody hash',()=>{
 const current=inventory(),release=JSON.parse(fs.readFileSync(path.join(sourceRoot,'supabase/release/current-release.json')));
 const context={mode:'UPGRADE',environment:'TEST',releaseId:'money-prebody-source-test',gitCommit:'a'.repeat(40),expectedHash:'b'.repeat(64),expectedDatabase:'cloudtms_test_clone',customerKey:''};
 const actual=managedReleaseContext({root:sourceRoot,current,release,context});
 const prior=managedManifestHash({root:sourceRoot,current,release,executionPolicy:{bootstrap:BOOTSTRAP_PIN,baselinePrefix:BASELINE_MIGRATION_PREFIX,
  exceptions:MANAGED_EXCEPTIONS,ownerRlsEnvelope:OWNER_RLS_ENVELOPE,immutableGuardDependency:IMMUTABLE_GUARD_DEPENDENCY,
  contactClaimAtomicCompanion:CONTACT_CLAIM_MIGRATION,logicalOwnerMapping:logicalPostgresExecutablePolicy()}});
 assert.notEqual(actual.manifestHash,prior);const admission=managedAdmissionSql(actual);
 assert(admission.includes(pin.version));assert(admission.includes(pin.companionSha256));assert(admission.includes(actual.manifestHash));assert(!admission.includes(prior));
});

test('rehearsal source binding contains helper, exact SQL and source evidence dependencies',()=>{
 const source=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-install1-full-rehearsal.mjs'),'utf8');
 assert(source.includes("'scripts/healthroster-money-predecessor-boundary.mjs',MONEY_PREDECESSOR_MIGRATION.companionPath,MONEY_PREDECESSOR_MIGRATION.evidencePath"));
 assert(source.includes('units.push(compiledAtomicRehearsalUnit(compiled));'));
 assert(source.includes('atomicMoneyPredecessorCompanion:units[i].atomicMoneyPredecessorCompanion'));
});

test('only exact pinned prebody source supplies the three closed census-refusal diagnostic codes',()=>{
 const codes=reviewedDiagnosticCodes({sourceFiles:[{file:pin.companionPath,sha256:pin.companionSha256}]});
 assert.deepEqual(codes,['HEALTH_F23_PRICE_ENUM_PREDECESSOR_CHANGED','HEALTH_F23_WEEKLY_SOURCE_BILLING_MOVEMENTS_PREDECESSOR_CHANGED','HEALTH_F23_WEEKLY_SOURCE_CHARGE_CHECKS_PREDECESSOR_CHANGED']);
 const source=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-install1-full-rehearsal.mjs'),'utf8');
 assert(source.includes('units.filter(unit=>unit.atomicMoneyPredecessorCompanion)'));
 assert(source.includes('.filter(row=>row.file===MONEY_PREDECESSOR_MIGRATION.companionPath)'));
});

test('existing Invoice gate retains exact bytes while owner-source/review pins are coherently renewed',()=>{
 const file='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_ENVELOPE_REVIEW.json';
 const previous=JSON.parse(fs.readFileSync(path.join(sourceRoot,file))),next=JSON.parse(fs.readFileSync(path.join(targetRoot,file)));
 for(let i=0;i<previous.rows.length;i++)if(previous.rows[i].existingNativeGate){
  const before=previous.rows[i].existingNativeGate,after=next.rows[i].existingNativeGate;
  assert.equal(after.literal,before.literal);assert.equal(after.sha256,before.sha256);
  assert.equal(after.ownerSourceHash,digest(canonical(fs.readFileSync(path.join(targetRoot,'scripts/cloudtms-managed-release.mjs'),'utf8'))));
 }
 const module=fs.readFileSync(path.join(targetRoot,'scripts/healthroster-reviewed-managed-envelope.mjs'),'utf8');
 assert(module.includes("const reviewHash='"+digest(fs.readFileSync(path.join(targetRoot,file)))+"';"));
});
