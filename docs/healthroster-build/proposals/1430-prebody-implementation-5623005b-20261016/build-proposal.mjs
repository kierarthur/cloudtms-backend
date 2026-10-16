// Source-only exact-anchor proposal assembly. Never modifies the integrator.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..');
const root=path.resolve(own,'../healthroster-install2-14102026');
const sourceCommit='5623005b91e4bc425e79587a88b1c5cad168d5eb';
const forward=path.join(dir,'forward'),hash=x=>createHash('sha256').update(x).digest('hex');
const edits=[],pins=[];
const read=file=>{
 const git=spawnSync('git',['show',`${sourceCommit}:${file}`],{cwd:root,maxBuffer:4*1024*1024});assert.equal(git.status,0);
 const text=git.stdout.toString(),saved=fs.readFileSync(path.join(root,file),'utf8').replaceAll('\r\n','\n');
 assert.equal(saved,text,'Root changed: '+file);return text;
};
const write=(file,text)=>{const target=path.join(forward,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,text);};
function replace(file,source,from,to){assert.equal(source.split(from).length,2,'Exact single anchor: '+file);edits.push({file,expectedCount:1,from,to});return source.replace(from,to);}
const compiler='scripts/cloudtms-managed-release.mjs',harness='scripts/healthroster-install1-full-rehearsal.mjs';
let managed=read(compiler),beforeManaged=managed;
managed=replace(compiler,managed,"import { CONTACT_CLAIM_MIGRATION,contactClaimMigrationCompanionFor,contactClaimMigrationSql } from './healthroster-contact-claim-boundary.mjs';", "import { CONTACT_CLAIM_MIGRATION,contactClaimMigrationCompanionFor,contactClaimMigrationSql } from './healthroster-contact-claim-boundary.mjs';\nimport { MONEY_PREDECESSOR_MIGRATION,moneyPredecessorCompanionFor,moneyPredecessorMigrationSql } from './healthroster-money-predecessor-boundary.mjs';");
managed=replace(compiler,managed,'    contact_claim_atomic_companion_version: CONTACT_CLAIM_MIGRATION.version,','    contact_claim_atomic_companion_version: CONTACT_CLAIM_MIGRATION.version,\n    money_check_prebody_companion_version: MONEY_PREDECESSOR_MIGRATION.version,\n    money_check_prebody_companion_sha256: MONEY_PREDECESSOR_MIGRATION.companionSha256,');
managed=replace(compiler,managed,'  const contactClaimCompanion = contactClaimMigrationCompanionFor(closure,kind,{root,mapSource});','  const contactClaimCompanion = contactClaimMigrationCompanionFor(closure,kind,{root,mapSource});\n  const moneyPredecessorCompanion = moneyPredecessorCompanionFor(closure,kind,{root,mapSource});');
managed=replace(compiler,managed,'ownerRlsEnvelope, immutableGuardDependency,contactClaimCompanion };','ownerRlsEnvelope, immutableGuardDependency,contactClaimCompanion,moneyPredecessorCompanion };');
managed=replace(compiler,managed,'  if(unit.contactClaimCompanion||unit.item.path===CONTACT_CLAIM_MIGRATION.path){',`  if(unit.moneyPredecessorCompanion||unit.item.path===MONEY_PREDECESSOR_MIGRATION.path){
    const prebody=moneyPredecessorMigrationSql(unit,mapExecutableSqlSource);
    return \`begin;\\n\${assertWriterSql}\\n\${prebody}\\n\${unit.body}\\n\${assertWriterSql}\\n\${ledger}\\ncommit;\`;
  }
  if(unit.contactClaimCompanion||unit.item.path===CONTACT_CLAIM_MIGRATION.path){`);
managed=replace(compiler,managed,'      contactClaimAtomicCompanion: CONTACT_CLAIM_MIGRATION,','      contactClaimAtomicCompanion: CONTACT_CLAIM_MIGRATION,\n      moneyCheckPrebodyCompanion: MONEY_PREDECESSOR_MIGRATION,');
write(compiler,managed);pins.push({file:compiler,beforeSha256:hash(beforeManaged),afterSha256:hash(managed)});

let rehearsal=read(harness),beforeRehearsal=rehearsal;
rehearsal=replace(harness,rehearsal,"import { CONTACT_CLAIM_MIGRATION,contactClaimMigrationSql } from './healthroster-contact-claim-boundary.mjs';", "import { CONTACT_CLAIM_MIGRATION,contactClaimMigrationSql } from './healthroster-contact-claim-boundary.mjs';\nimport { MONEY_PREDECESSOR_MIGRATION,moneyPredecessorMigrationSql } from './healthroster-money-predecessor-boundary.mjs';");
rehearsal=replace(harness,rehearsal,"const releaseInputFiles=['scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release.mjs',", "const releaseInputFiles=['scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release.mjs',\n 'scripts/healthroster-money-predecessor-boundary.mjs',MONEY_PREDECESSOR_MIGRATION.companionPath,MONEY_PREDECESSOR_MIGRATION.evidencePath,");
const oldUnit=`  // A protected release's exception may contain transaction commits or gexec.
  // It cannot be silently flattened into this rollback-only outer transaction.
  requireThat(!compiled.exception&&!compiled.ownerRlsEnvelope&&!compiled.immutableGuardDependency,'REHEARSAL_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED');
  const companion=compiled.contactClaimCompanion||item.path===CONTACT_CLAIM_MIGRATION.path
   ?contactClaimMigrationSql(compiled,mapExecutableSqlSource):'';
  const sql=compiled.body+(companion?'\\n'+companion:'');
  units.push({file:item.path,kind,sql,sqlHash:digest(sql),sourceSqlHash:item.sha256,phase:kind,
   sourceFiles:[...(compiled.closure?.ordered.map(x=>({file:x.path,sha256:digest(x.source)}))??[]),
    ...(compiled.contactClaimCompanion?.sourceFiles??[])],
   ...(companion?{atomicContactClaimCompanion:{version:compiled.contactClaimCompanion.version,
    statementSha256:compiled.contactClaimCompanion.statementSha256,nativeProof:'NOT_RUN'}}:{})});`;
rehearsal=replace(harness,rehearsal,oldUnit,'  units.push(compiledAtomicRehearsalUnit(compiled));');
const newUnit=`// Same atomic compilation used by release and rollback harness; no SQL execution.
export function compiledAtomicRehearsalUnit(compiled){
 const {item,kind}=compiled;
 // A protected exception cannot be silently flattened into this outer rollback.
 requireThat(!compiled.exception&&!compiled.ownerRlsEnvelope&&!compiled.immutableGuardDependency,'REHEARSAL_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED');
 const prebody=compiled.moneyPredecessorCompanion||item.path===MONEY_PREDECESSOR_MIGRATION.path
  ?moneyPredecessorMigrationSql(compiled,mapExecutableSqlSource):'';
 const companion=compiled.contactClaimCompanion||item.path===CONTACT_CLAIM_MIGRATION.path
  ?contactClaimMigrationSql(compiled,mapExecutableSqlSource):'';
 const sql=(prebody?prebody+'\\n':'')+compiled.body+(companion?'\\n'+companion:'');
 return {file:item.path,kind,sql,sqlHash:digest(sql),sourceSqlHash:item.sha256,phase:kind,
  sourceFiles:[...(compiled.closure?.ordered.map(x=>({file:x.path,sha256:digest(x.source)}))??[]),
   ...(compiled.contactClaimCompanion?.sourceFiles??[]),...(compiled.moneyPredecessorCompanion?.sourceFiles??[])],
  ...(companion?{atomicContactClaimCompanion:{version:compiled.contactClaimCompanion.version,
   statementSha256:compiled.contactClaimCompanion.statementSha256,nativeProof:'NOT_RUN'}}:{}),
  ...(prebody?{atomicMoneyPredecessorCompanion:{version:compiled.moneyPredecessorCompanion.version,
   companionSha256:compiled.moneyPredecessorCompanion.companionSha256,
   statementSha256:compiled.moneyPredecessorCompanion.statementSha256,
   placement:compiled.moneyPredecessorCompanion.placement,
   evidenceFiles:compiled.moneyPredecessorCompanion.evidenceFiles,nativeProof:'NOT_RUN'}}:{})};
}

`;
rehearsal=replace(harness,rehearsal,'// This compiler takes the freshly observed installed ledgers.',newUnit+'// This compiler takes the freshly observed installed ledgers.');
rehearsal=replace(harness,rehearsal," const diagnosticCodes=[...new Set([...reviewedDiagnosticCodes(review),'REHEARSAL_MARKER_CONTRACT_SHAPE_REFUSED',",` const diagnosticReview={...review,sourceFiles:[...review.sourceFiles,
  ...units.filter(unit=>unit.atomicMoneyPredecessorCompanion).flatMap(unit=>unit.sourceFiles
   .filter(row=>row.file===MONEY_PREDECESSOR_MIGRATION.companionPath))]};
 const diagnosticCodes=[...new Set([...reviewedDiagnosticCodes(diagnosticReview),'REHEARSAL_MARKER_CONTRACT_SHAPE_REFUSED',`);
rehearsal=replace(harness,rehearsal,'  units:rendered.units.map((x,i)=>({...x,phase:units[i].phase,sourceFiles:units[i].sourceFiles??[],clientVariables:units[i].clientVariables??[]})),',`  units:rendered.units.map((x,i)=>({...x,phase:units[i].phase,sourceFiles:units[i].sourceFiles??[],clientVariables:units[i].clientVariables??[],
   ...(units[i].atomicMoneyPredecessorCompanion?{atomicMoneyPredecessorCompanion:units[i].atomicMoneyPredecessorCompanion}:{})})),`);
write(harness,rehearsal);pins.push({file:harness,beforeSha256:hash(beforeRehearsal),afterSha256:hash(rehearsal)});

const envelopeJson='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_ENVELOPE_REVIEW.json';
const originalEnvelope=read(envelopeJson),envelope=JSON.parse(originalEnvelope);let repins=0;
for(const row of envelope.rows)if(row.existingNativeGate){assert.equal(row.existingNativeGate.ownerSourceHash,hash(beforeManaged));row.existingNativeGate.ownerSourceHash=hash(managed);repins++;}
assert.equal(repins,1);const newEnvelope=JSON.stringify(envelope,null,2)+'\n';write(envelopeJson,newEnvelope);
const oldOwner=`"ownerSourceHash": "${hash(beforeManaged)}"`,newOwner=`"ownerSourceHash": "${hash(managed)}"`;
assert.equal(originalEnvelope.split(oldOwner).length,2);assert.equal(originalEnvelope.replace(oldOwner,newOwner),newEnvelope);
edits.push({file:envelopeJson,expectedCount:1,from:oldOwner,to:newOwner});
pins.push({file:envelopeJson,beforeSha256:hash(originalEnvelope),afterSha256:hash(newEnvelope),onlySemanticChange:'existingNativeGate.ownerSourceHash; exact gate literal and sha unchanged'});
const envelopeModule='scripts/healthroster-reviewed-managed-envelope.mjs',beforeEnvelopeModule=read(envelopeModule);
const newEnvelopeModule=replace(envelopeModule,beforeEnvelopeModule,`const reviewHash='${hash(originalEnvelope)}';`,`const reviewHash='${hash(newEnvelope)}';`);
write(envelopeModule,newEnvelopeModule);pins.push({file:envelopeModule,beforeSha256:hash(beforeEnvelopeModule),afterSha256:hash(newEnvelopeModule)});

const oldProof=path.join(own,'docs/healthroster-build/proposals/f23-native-predecessor-review-20261016');
const preservedRoot='docs/healthroster-build/v6-owner-delivery-20261016/F23_PHYSICAL_PREDECESSOR_PEER_REVIEW_20261016';
const preservation=[];
for(const entry of fs.readdirSync(oldProof,{withFileTypes:true}).filter(x=>x.isFile())){
 const bytes=fs.readFileSync(path.join(oldProof,entry.name)),target=preservedRoot+'/'+entry.name;write(target,bytes);
 preservation.push({sourceCommit:'58d3c16d73d42fcbe767d6055b6394383a9aeb96',source:'docs/healthroster-build/proposals/f23-native-predecessor-review-20261016/'+entry.name,target,sha256:hash(bytes)});
}
assert.equal(preservation.length,10);
const sql=fs.readFileSync(path.join(oldProof,'16102026_0943_money_prebody_assertion_proposal.sqlinc'));
assert.equal(hash(sql),'96f5f4e37f330ca510eed591eb36eb86e78e982b4bf93176e4f126231e04fa50');
write('supabase/release/16102026_0943_healthroster_money_predecessor_assertion.sqlinc',sql);
const manifest={version:'HEALTHROSTER_1430_PREBODY_IMPLEMENTATION_PROPOSAL_V1',status:'SOURCE_PROPOSAL_ONLY_ROOT_INTEGRATION_AND_NATIVE_NOT_RUN',rootCommit:sourceCommit,rootPath:root,
 pins,edits,preservation,newFiles:['scripts/healthroster-money-predecessor-boundary.mjs','supabase/release/16102026_0943_healthroster_money_predecessor_assertion.sqlinc','tests/healthroster-money-predecessor-boundary.test.mjs'],
 invariants:['Original407 migration bytes/locks untouched.','Exact LOCK+assertion precedes unchanged1430 body within existing transaction/writer/ledger guards.','Managed manifest/admission now bind exact prebody descriptor and program hash.','Rehearsal uses same checked prebody before body; source binding includes helper/program/evidence.','Contact companion remains after its own unchanged0339 body; Invoice native gate literal and callee audit unchanged.','Capability0400 absent from companion descriptor and unqualified pending-prefix native census.','No F23 dynamic classification or generic exception added.'],native:'NOT_RUN',databaseCalls:0,rootEdits:0,bankEdits:0,deployments:0};
fs.writeFileSync(path.join(dir,'PROPOSAL.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({status:manifest.status,changedExistingFiles:pins.length,preservationFiles:preservation.length,rootCommit:sourceCommit,native:'NOT_RUN'}));
