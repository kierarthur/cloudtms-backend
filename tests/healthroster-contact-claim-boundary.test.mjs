import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import { assertContactClaimBoundary,CONTACT_CLAIM_CORE_PATH,CONTACT_CLAIM_BRIDGE_PATH,CONTACT_CLAIM_MIGRATION,
  contactClaimMigrationCompanionFor,contactClaimMigrationSql } from '../scripts/healthroster-contact-claim-boundary.mjs';
import {compileManagedUnit,unitCheckpointSql,managedReleaseContext,managedAdmissionSql,OWNER_RLS_ENVELOPE,IMMUTABLE_GUARD_DEPENDENCY} from '../scripts/cloudtms-managed-release.mjs';
import {canonical,digest,readManagedClosure,scanManagedSql,managedManifestHash} from '../scripts/cloudtms-managed-release-sql.mjs';
import {BOOTSTRAP_PIN,BASELINE_MIGRATION_PREFIX,MANAGED_EXCEPTIONS} from '../scripts/cloudtms-managed-execution-policy.mjs';
import {mapExecutableSqlSource,logicalPostgresExecutablePolicy,inventory} from '../scripts/cloudtms-db-release-lib.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const read=file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');
const input={parent:read('supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql'),
  core:read(CONTACT_CLAIM_CORE_PATH),bridge:read(CONTACT_CLAIM_BRIDGE_PATH)};
test('actual contact core composes the complete retained Banking owner in both native branches',()=>{
  assert.doesNotThrow(()=>assertContactClaimBoundary(input));
  assert.doesNotThrow(()=>assertContactClaimBoundary(Object.fromEntries(Object.entries(input).map(([k,v])=>[k,v.replaceAll('\r\n','\n').replaceAll('\n','\r\n')]))));
});
for(const [label,from,to]of[
  ['owner gate','v_legacy:=private.bpay_next_legacy_email_claim_legacy_v1();','v_legacy:=true;'],
  ['owner branch','if v_legacy is true then','if true then'],
  ['factual-only NEXT predicate','and private.bpay_next_legacy_email_is_factual_v1(mo.type,mo.context_kind,mo.reference) is true',''],
  ['Candidate authority',"workflow.state='AWAITING_PAPER_RETURN'",'true'],
  ['issued invoice authority',"or i.status<>'ISSUED'",'or false'],
  ['original lease','make_interval(mins=>v_lease)','make_interval(mins=>30)'],
  ['selection locks','for update skip locked limit v_limit','limit v_limit'],
  ['query scope','d.contact_managed','true'],
  ['stamp-before-lease','cross join contact_claim proof ',''],
  ['invoker security','security invoker','security definer']]){
  test('rejects changed '+label+' in either complete native branch',()=>{
    let at=input.core.indexOf(from),count=0;
    while(at>=0){
      const core=input.core.slice(0,at)+to+input.core.slice(at+from.length);
      assert.throws(()=>assertContactClaimBoundary({...input,core}),/HEALTHROSTER_CONTACT_CLAIM_/);
      count++;at=input.core.indexOf(from,at+from.length);
    }
    assert.ok(count>0,'mutation must hit genuine source');
  });
}
test('rejects public bridge bypass, a second core definition and lost parent anchors',()=>{
  for(const bridge of [input.bridge.replace('p_lease_minutes,false','p_lease_minutes,true'),
    input.bridge.replace('security invoker','security definer'),
    input.bridge.replace('return query select * from public._email_outbox_contact_claim_core_v1','return query select * from public.mail_outbox --')])
    assert.throws(()=>assertContactClaimBoundary({...input,bridge}),/HEALTHROSTER_CONTACT_CLAIM_/);
  assert.throws(()=>assertContactClaimBoundary({...input,core:input.core+'\n'+input.core}),/HEALTHROSTER_CONTACT_CLAIM_DEFINITION_COUNT/);
  assert.throws(()=>assertContactClaimBoundary({...input,parent:input.parent.replace("where mo.status='QUEUED'",'where false')}),/HEALTHROSTER_CONTACT_CLAIM_PARENT_SHAPE/);
});

test('real immutable bootstrap correction shares its sole transaction and original ledger receipt',()=>{
  const pin=CONTACT_CLAIM_MIGRATION,item={path:pin.path,sha256:pin.contentSha256};
  const unit=compileManagedUnit(item,'migration',{root,mapSource:mapExecutableSqlSource});
  const packet=unitCheckpointSql(unit,{releaseId:'contact-claim-source-test'});
  const statements=scanManagedSql(packet).statements;
  assert.equal(statements.filter(r=>r.tokens[0]==='BEGIN').length,1);
  assert.equal(statements.filter(r=>r.tokens[0]==='COMMIT').length,1);
  assert.equal(statements.at(-1).tokens[0],'COMMIT');
  const companion=contactClaimMigrationSql(unit,mapExecutableSqlSource);
  assert.equal(digest(canonical(read(pin.path))),pin.contentSha256);
  const bodyAt=packet.indexOf(unit.body),fixAt=packet.indexOf(companion,bodyAt+unit.body.length);
  assert.ok(bodyAt>=0&&fixAt>=bodyAt+unit.body.length);
  assert.ok(fixAt<packet.indexOf('insert into private.cloudtms_migration_ledger'));
  assert.ok(packet.includes(pin.contentSha256));
  assert.equal(digest(companion),pin.statementSha256);
  assert.doesNotMatch(companion,/\b(?:grant|revoke|alter)\s/i);
  assert.throws(()=>unitCheckpointSql({...unit,contactClaimCompanion:null},{releaseId:'x'}),/ATOMIC_SCOPE_INVALID/);
  assert.throws(()=>unitCheckpointSql({...unit,contactClaimCompanion:{...unit.contactClaimCompanion,sql:companion+'\nselect 1;'}},{releaseId:'x'}),/ATOMIC_COMPANION_CHANGED/);
  assert.throws(()=>unitCheckpointSql({...unit,body:unit.body+'\nselect 1;'},{releaseId:'x'}),/ATOMIC_SCOPE_INVALID/);
});

test('bootstrap/source/parent drift and wrong migration kind fail before producing an executable packet',()=>{
  const fixture=fs.mkdtempSync(path.join(os.tmpdir(),'healthroster-contact-atomic-'));
  const pin=CONTACT_CLAIM_MIGRATION;
  const files=[pin.path,pin.parentPath,CONTACT_CLAIM_CORE_PATH,CONTACT_CLAIM_BRIDGE_PATH];
  try{
    for(const file of files){fs.mkdirSync(path.dirname(path.join(fixture,file)),{recursive:true});fs.copyFileSync(path.join(root,file),path.join(fixture,file));}
    const closure=readManagedClosure(pin.path,fixture);
    assert.doesNotThrow(()=>contactClaimMigrationCompanionFor(closure,'migration',{root:fixture}));
    assert.throws(()=>contactClaimMigrationCompanionFor(closure,'repeatable',{root:fixture}),/IMMUTABLE_SOURCE_CHANGED/);
    for(const file of files){
      const target=path.join(fixture,file),original=fs.readFileSync(target);
      fs.appendFileSync(target,'\n-- changed exact source\n');
      if(file===CONTACT_CLAIM_BRIDGE_PATH){
        fs.writeFileSync(target,original.toString().replace('p_lease_minutes,false','p_lease_minutes,true'));
      }
      assert.throws(()=>contactClaimMigrationCompanionFor(readManagedClosure(pin.path,fixture),'migration',{root:fixture}),/HEALTHROSTER_CONTACT_CLAIM_/);
      fs.writeFileSync(target,original);
    }
  }finally{
    assert.ok(path.resolve(fixture).startsWith(path.resolve(os.tmpdir())+path.sep));
    fs.rmSync(fixture,{recursive:true,force:true});
  }
});

test('managed manifest and admission bind the atomic correction, so an older packet cannot resume as this release',()=>{
  const current=inventory(),release=JSON.parse(read('supabase/release/current-release.json'));
  const context={mode:'UPGRADE',environment:'TEST',releaseId:'contact-claim-source-test',gitCommit:'a'.repeat(40),
    expectedHash:'b'.repeat(64),expectedDatabase:'cloudtms_test_clone',customerKey:''};
  const actual=managedReleaseContext({root,current,release,context});
  const previous=managedManifestHash({root,current,release,executionPolicy:{bootstrap:BOOTSTRAP_PIN,
    baselinePrefix:BASELINE_MIGRATION_PREFIX,exceptions:MANAGED_EXCEPTIONS,ownerRlsEnvelope:OWNER_RLS_ENVELOPE,
    immutableGuardDependency:IMMUTABLE_GUARD_DEPENDENCY,logicalOwnerMapping:logicalPostgresExecutablePolicy()}});
  assert.notEqual(actual.manifestHash,previous);
  const admission=managedAdmissionSql(actual);
  assert.ok(admission.includes(CONTACT_CLAIM_MIGRATION.version));
  assert.ok(admission.includes(actual.manifestHash));
  assert.ok(!admission.includes(previous));
});
