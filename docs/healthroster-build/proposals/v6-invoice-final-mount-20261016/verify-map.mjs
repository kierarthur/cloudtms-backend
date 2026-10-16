import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const m=JSON.parse(fs.readFileSync(path.join(here,'FINAL_MOUNT_SELECTION.json')));
const hash=s=>createHash('sha256').update(s).digest('hex');
test('complete base, selected file and unique signature census',()=>{
 assert.equal(m.baseDependencies.length,53);assert.equal(m.selected.length,67);
 const signatures=m.selected.flatMap(x=>x.functions.map(f=>f.signature));assert.equal(signatures.length,94);assert.equal(new Set(signatures).size,94);
 assert.equal(m.selected.reduce((n,r)=>n+r.nonFunctionEffects.length,0),178);
});
test('all final saved payloads match exact raw and canonical immutable selection',()=>{
 for(const r of m.selected){const bytes=fs.readFileSync(path.join(repo,r.path));assert.equal(hash(bytes),r.rawSha256,r.path);assert.equal(hash(bytes.toString().replaceAll('\r\n','\n')),r.canonicalLfSha256,r.path);assert.equal(r.currentSavedDiskEqualsSelectedGitObject,true);}
});
test('final pending and certificate precedence cannot regress to predecessors',()=>{
 for(const r of m.selected){const leaf=path.basename(r.path);if(['025_claim_placement.sqlinc','026_claim_current.sqlinc','027_claim_owned.sqlinc','028_claim_lock.sqlinc','030_claim_sync.sqlinc'].includes(leaf))assert.equal(r.sourceCommit,m.sourceCommits.pending);
 if(['015_source_certificate.sqlinc','035_preflight.sqlinc'].includes(leaf))assert.equal(r.sourceCommit,m.sourceCommits.sourceCertificate);}
 assert.equal(m.selected[0].sourceCommit,m.sourceCommits.dailyIngress);assert.equal(m.rotationPredecessorOnly.length,2);
});
test('mandatory schema, effects, ACL and Physical full workflow boundaries retained',()=>{
 const schema=m.selected.find(r=>r.path.endsWith('/001_billing_schema.sqlinc'));
 assert.equal(schema.nonFunctionEffects.length,16);assert.equal(schema.nonFunctionEffects.filter(e=>/^create table/i.test(e.statement)).length,6);
 assert(m.selected.some(r=>r.path.endsWith('/099_new_private_acl.sqlinc')&&r.nonFunctionEffects.length>40));
 assert(m.retainedNativeParentNonFunctionDependencies.some(r=>r.nonFunctionEffects.some(e=>/^drop function/i.test(e.statement))));
 assert(m.selected.some(r=>r.path.endsWith('/033c_native_vat_policy.sqlinc')&&r.nonFunctionEffects.length===0));
 assert.equal(m.selected.flatMap(r=>r.functions).some(f=>f.signature.startsWith('public.candidate_workflow_transition_atomic_v1(')),false);
 assert(m.physicalOwner.rule.includes('complete final Physical'));assert(m.additionalPendingDependencies.includes('private.timesheet_contract_native_pending_workflow_v1(jsonb,uuid)'));
});
