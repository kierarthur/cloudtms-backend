import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {loadCommittedPrerequisiteJourneys} from './dependency-reference/scripts/weekly-source-protected-prerequisite-evidence.mjs';
import {loadOperationalJourneys} from './dependency-reference/scripts/weekly-source-protected-operational-evidence.mjs';
import {loadTestSetupInputs} from './dependency-reference/scripts/weekly-source-protected-test-setup-evidence.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex');
const read=file=>fs.readFileSync(path.join(out,file),'utf8'),json=file=>JSON.parse(read(file)),proof=json('COMPLETE_CURRENT_SOURCE_REVERSE.json');
const managed=read('scripts/cloudtms-managed-release.mjs'),caller=read('scripts/cloudtms-db-release.mjs'),workflow=read('workflow/database-release.yml');
test('all three complete current owners have exact unique hunks and whole-file reverse',()=>{
 assert.equal(proof.proofs.length,3);for(const p of proof.proofs){const original=execFileSync('git',['show',p.input.commit+':'+p.input.file],{cwd:own,maxBuffer:32000000}).toString('utf8');assert.equal(sha(original),p.input.sha256);let after=original;for(const h of p.hunks){if(h.before==='START_OF_MODULE')after=h.after+after;else{assert.equal(after.split(h.before).length,2);after=after.replace(h.before,()=>h.after);}}assert.equal(after,read(p.output));assert.equal(sha(after),p.output_sha256);for(const h of [...p.hunks].reverse()){if(h.before==='START_OF_MODULE')after=after.slice(h.after.length);else after=after.replace(h.after,()=>h.before);}assert.equal(after,original);}
});
test('current Banking immutable guard reader writer and admission helpers are preserved',()=>{
 const p=json('CURRENT_NATIVE_RELEASE_POLICY_PRESERVATION.json');assert.equal(p.complete_current_helpers.length,16);assert.ok(p.native_reader_and_writer_policy_unchanged);assert.match(managed,/immutableGuardDependencySql\(unit\)/);assert.match(managed,/prepareManagedSourceReaders\(execute\)/);assert.match(managed,/closeFinalDefinerSelection/);assert.match(managed,/admitted && session && !session\.failure && !session\.finished/);
});
test('qualification validation follows actual contract verification and precedes final VERIFIED write',()=>{
 const verify=managed.indexOf("if (verified.sha256 !== context.expectedHash)"),qualify=managed.indexOf('installedQualificationCatalogueEvidence(verified.actual'),record=managed.indexOf("update private.cloudtms_database_releases r set status='VERIFIED'");assert.ok(verify<qualify&&qualify<record);assert.match(managed,/QUALIFICATION_EVIDENCE_REPLACEMENT_REFUSED/);assert.match(managed,/TEST_SETUP_EVIDENCE_REPLACEMENT_REFUSED/);assert.match(managed,/PREREQUISITE_RECORDED_EXECUTION_CHANGED/);assert.match(managed,/OPERATIONAL_ACTUAL_SETUP_CLOSURE_REQUIRED/);
});
test('closed native prerequisites and operational certificates cannot be relabelled',()=>{
 assert.match(managed,/journey\.context\.release_id!==context\.releaseId/);assert.match(managed,/journey\.context\.git_commit!==context\.gitCommit/);assert.match(managed,/journey\.context\.contract_sha256!==context\.expectedHash/);assert.match(managed,/journey\.phase==='FULL_OPERATIONAL'/);assert.match(managed,/journey\.phase==='ROLLBACK_PREREQUISITE'/);assert.match(managed,/assertManagedOperationalEvidence/);assert.match(managed,/TEST_SETUP_NATIVE_CENSUS_CHANGED/);
});
test('actual caller loads only three fixed existing committed bundles without new CLI options',()=>{
 assert.match(caller,/loadCommittedPrerequisiteJourneys\(repoRoot,release,\{\.\.\.releaseVerifierContext,mode,expectedHash\}\)/);assert.match(caller,/loadOperationalJourneys\(repoRoot,release,\{\.\.\.releaseVerifierContext,mode,expectedHash\}\)/);assert.match(caller,/loadTestSetupInputs\(repoRoot,release,\{\.\.\.releaseVerifierContext,mode,expectedHash\}\)/);assert.match(caller,/requireWeeklySourceHandover2Approval/);assert.match(caller,/preapply: runBankingPayCatalogPreapply/);assert.equal(proof.no_new_cli_or_environment_option,true);
});
test('unchanged no-descriptor actual caller context remains an ordinary release',()=>{
 const context={environment:'TEST',expectedDatabase:'cloudtms_test_clone',releaseId:'mock-source-only',gitCommit:'a'.repeat(40),mode:'UPGRADE',expectedHash:'b'.repeat(64)};
 assert.deepEqual(loadCommittedPrerequisiteJourneys(own,{},context),[]);assert.deepEqual(loadOperationalJourneys(own,{},context),[]);assert.deepEqual(loadTestSetupInputs(own,{},context),[]);
});
test('protected workflow only adds historical Git depth to existing approved checkout',()=>{
 const p=proof.proofs.find(row=>row.name==='CURRENT_PROTECTED_RELEASE_WORKFLOW');assert.equal(p.hunks.length,1);assert.equal((workflow.match(/fetch-depth: 0/g)||[]).length,2);assert.equal(p.hunks[0].before,'          ref: ${{ needs.source-gate.outputs.commit }}');assert.equal(proof.no_target_approval_turn_or_release_policy_change,true);
});
test('current dependency closure contains exact fe7 modules and no arbitrary imports',()=>{
 const c=json('CURRENT_DEPENDENCY_CLOSURE.json');assert.equal(c.dependencies.length,20);assert.ok(c.complete_relative_import_closure_resolved);for(const row of c.dependencies){assert.equal(sha(read('dependency-reference/'+row.file)),row.sha256);for(const imported of row.imports){const file=path.posix.normalize(path.posix.join(path.posix.dirname(row.file),imported));assert.ok(c.dependencies.some(dep=>dep.file===file));}}
});
test('recorder9/13 and shared operational optional Report inputs retain exact ABI',()=>{
 const abi=json('THREE_BUNDLE_AND_RECORDER_ABI.json');assert.equal(abi.request_fields.length,9);assert.equal(abi.response_fields.length,13);assert.equal(abi.existing_receipt_bundle_keys.length,3);assert.equal(abi.new_bundle_or_cli_option,false);const source=read('dependency-reference/scripts/weekly-source-protected-operational-evidence.mjs');assert.match(source,/report_fin_native_artifacts/);assert.match(source,/bundle\.qualifications\.length>100/);assert.doesNotMatch(source,/!bundle\.qualifications\.length/);assert.ok(abi.concrete_test_only_protected_entry_followup_required);
});
test('old198 and A0 frozen108 saved artifacts remain exact',()=>{
 for(const dir of ['strict-qualification-native-interfaces','strict-qualification-install1-forward','original-prerequisites-install1-forward','a0-current-root-install2-forward'])for(const row of JSON.parse(fs.readFileSync(path.join(own,'docs/healthroster-build/proposals',dir,'REVIEW_MANIFEST.json'))).files)assert.equal(sha(fs.readFileSync(path.join(own,row.file))),row.sha256);
});
