// Explicit filesystem/Git/artifact model for coordinator tests. No native SQL,
// external network, deployment or genuine acceptance is performed here.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {canonicalContractHash} from '../../scripts/cloudtms-db-release-lib.mjs';
import {assertF5AuthorityCompatibility,f5Verifier} from '../../scripts/healthroster-f5-post-verified-gate.mjs';
import {generateF5SyntheticFixture} from '../../scripts/healthroster-f5-synthetic-fixture.mjs';

export async function createF5PublicationArtifactModel(){
 const base=fs.realpathSync(os.tmpdir()),root=fs.mkdtempSync(path.join(base,'hr-f5-publication-model-'));
 const write=(rel,value)=>{const file=path.join(root,rel);fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,value);};
 const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',
  '-c','user.name=HealthRoster source test','-c','user.email=source-test@example.invalid',...args],
  {cwd:root,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
 const contract={relations:[],functions:[],triggers:[],policies:[],default_acls:[]};
 const verifierBytes=fs.readFileSync(new URL('../../'+f5Verifier,import.meta.url));
 write('supabase/release/current-contract.json',JSON.stringify(contract));
 write(f5Verifier,verifierBytes);write('.gitignore','.codex-tmp/\n');
 git(['init','--quiet']);git(['add','--','.']);git(['commit','--quiet','-m','Explicit synthetic publication source model']);
 const commit=git(['rev-parse','HEAD']),contractHash=canonicalContractHash(contract);
 const id=n=>`f5530000-0000-4000-8000-${String(n).padStart(12,'0')}`;
 const generated=await generateF5SyntheticFixture({authorization:{
  schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1',purpose:'SOURCE_FIXTURE_SETUP',environment:'TEST',
  source_cycle_id:id(3),source_group_id:id(2),agency_id:id(4),journey_actor_user_id:id(1),week_ending:'2026-10-11',
  fixture_manifest:{client_ids:[id(5)]},allowed_operations:['weekly_source_upload_stage_begin_atomic_v1',
   'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1']},
  observed_at_utc:'2026-10-16T12:00:00Z',cycle:{id:id(3),source_group_id:id(2),scope_client_id:id(5),
   finalisation_week_ending:'2026-10-11',previous_cutoff_at_utc:'2026-10-07T22:59:00Z',cutoff_at_utc:'2026-10-14T22:59:00Z',
   state:'OPEN',current_complete_upload_id:null,current_projection_publication_id:null,current_final_revision_id:null}});
 const gate={contract:'F5_POST_VERIFIED_ROLLBACK_GATE_V1',database:'cloudtms_test_clone',release_id:'explicit-synthetic-model-release',
  database_source_commit:commit,database_authority_sha256:assertF5AuthorityCompatibility(root,commit,commit).sha256,
  authority_registration:null,registered_verifier_sha256:createHash('sha256').update(verifierBytes).digest('hex'),
  synthetic_fixture:generated.proof,source_commit:commit,contract_sha256:contractHash,restoration_sha256:'1'.repeat(64),
  qualification:'ROLLBACK_SQL_PASS',features_remain_off:true,provider_or_runtime_acceptance:false};
 const receipt={formatVersion:1,environment:'TEST',database:'cloudtms_test_clone',status:'DATABASE_READY',commit,
  completedAt:new Date().toISOString(),f5Gate:gate,
  databaseRelease:{release_id:gate.release_id,source_commit:commit,contract_sha256:contractHash}};
 return {root,commit,receipt,writeReceipt(destination,value=receipt){
  fs.writeFileSync(path.join(destination,'automatic-test-release.json'),JSON.stringify(value));
 },cleanup(){
  const resolved=fs.realpathSync(root);
  if(path.dirname(resolved)!==base||!path.basename(resolved).startsWith('hr-f5-publication-model-'))throw Error('F5_MODEL_CLEANUP_SCOPE_REFUSED');
  fs.rmSync(resolved,{recursive:true,force:true});
 }};
}
