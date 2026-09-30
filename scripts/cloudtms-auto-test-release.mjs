#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { chooseTestDatabaseRoute } from './automatic-test-release-policy.mjs';
import { canonicalContractHash, exportContract, inventory, psql, readJson, repoRoot, shellGitHead,
  validateExpectedDatabase, validateTarget, verifyIntegrity } from './cloudtms-db-release-lib.mjs';

const command=process.argv[2];
const receiptPath=path.join(repoRoot,'.codex-tmp','automatic-test-release.json');
const receipt={formatVersion:1,environment:'TEST',database:'cloudtms_test_clone',startedAt:new Date().toISOString(),status:'PLANNING',stages:[]};
function save(){fs.mkdirSync(path.dirname(receiptPath),{recursive:true});fs.writeFileSync(receiptPath,JSON.stringify(receipt,null,2)+'\n');}
function run(script,args,extraEnv={}) {
  const start=Date.now();
  const result=spawnSync(process.execPath,[script,...args],{cwd:repoRoot,stdio:'inherit',env:{...process.env,...extraEnv}});
  receipt.stages.push({script,args,elapsedMs:Date.now()-start,status:result.status===0?'PASSED':'FAILED'});save();
  if(result.status!==0) throw new Error(`Release stage failed: ${script}. Application deployment remains blocked; inspect that stage's exact error.`);
}
try {
  if(!['plan','apply'].includes(command)||process.argv.length!==3) throw new Error('Use cloudtms-auto-test-release.mjs plan|apply; target and route are not user-selectable');
  if(process.env.CLOUDTMS_ENVIRONMENT!=='TEST'||process.env.CLOUDTMS_EXPECTED_DATABASE!=='cloudtms_test_clone') throw new Error('Exact TEST configuration required');
  validateTarget('TEST',process.env.CLOUDTMS_EXPECTED_TARGET);
  validateExpectedDatabase('cloudtms_test_clone');
  if(psql({sql:'select current_database();'})!=='cloudtms_test_clone') throw new Error('Connected database mismatch');
  if(psql({sql:"select environment || '|' || coalesce(customer_key,'') from private.cloudtms_database_identity where singleton;"})!=='TEST|') throw new Error('Managed agency TEST identity mismatch');
  const commit=shellGitHead();receipt.commit=commit;
  if(command==='apply'&&(process.env.GITHUB_REPOSITORY!=='kierarthur/cloudtms-backend'||process.env.GITHUB_REF!=='refs/heads/test'||process.env.GITHUB_SHA!==commit))
    throw new Error('Automatic APPLY requires the exact source-gated test head in the protected canonical GitHub workflow');
  verifyIntegrity();
  run('scripts/cloudtms-db-release.mjs',['plan','--environment=TEST','--mode=UPGRADE']);
  const current=inventory();
  const migrations=JSON.parse(psql({sql:"select coalesce(jsonb_agg(jsonb_build_object('path',path,'hash',content_sha256)),'[]') from private.cloudtms_migration_ledger;"}));
  const repeatables=JSON.parse(psql({sql:"select coalesce(jsonb_agg(jsonb_build_object('path',path,'hash',closure_sha256)),'[]') from private.cloudtms_repeatable_ledger;"}));
  const latest=JSON.parse(psql({sql:"select coalesce((select jsonb_build_object('status',status,'commit',git_commit,'releaseId',release_id,'contract',installed_contract_sha256) from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1),'{}');"}));
  const migrationMap=new Map(migrations.map(x=>[x.path,x.hash]));
  const repeatableMap=new Map(repeatables.map(x=>[x.path,x.hash]));
  // PLAN already verifies immutable migrations; retain explicit repeatable inventory checks.
  const known=new Set(current.repeatables.map(x=>x.path));
  if(repeatables.some(x=>!known.has(x.path))) throw new Error('Installed repeatable is absent from the repository');
  let authorityUnchanged=false;
  if(/^[a-f0-9]{40}$/.test(latest.commit||'')) {
    if(spawnSync('git',['cat-file','-e',`${latest.commit}^{commit}`],{cwd:repoRoot,stdio:'ignore'}).status!==0)
      spawnSync('git',['fetch','--no-tags','--depth=1','origin',latest.commit],{cwd:repoRoot,stdio:'ignore'});
    const diff=spawnSync('git',['diff','--name-only',latest.commit,commit,'--','supabase','scripts','.github/workflows/database-release.yml','.github/workflows/automatic-test-release.yml','package.json','package-lock.json'],{cwd:repoRoot,encoding:'utf8'});
    authorityUnchanged=diff.status===0&&!diff.stdout.trim();
  }
  const expected=readJson('supabase/release/current-contract.json');
  const actual=exportContract();
  const state={environment:'TEST',database:'cloudtms_test_clone',identityVerified:true,ledgerVerified:true,
    latestStatus:latest.status||null,verificationAuthorityUnchanged:authorityUnchanged,
    contractMatches:canonicalContractHash(expected)===canonicalContractHash(actual)&&latest.contract===canonicalContractHash(expected),
    pendingMigrations:current.migrations.filter(x=>!migrationMap.has(x.path)).map(x=>x.path),
    pendingRepeatables:current.repeatables.filter(x=>repeatableMap.get(x.path)!==x.sha256).map(x=>x.path),components:[]};
  // The only initial automatic component adapter is the existing exact two-file
  // invoice-evidence engine. Its hard-coded source/verification allowlists remain
  // authoritative; old or edited manifests cannot be repurposed automatically.
  const component=readJson('supabase/release/weekly-source-invoice-evidence-component.json');
  const componentCheck=spawnSync(process.execPath,['scripts/cloudtms-db-component-release.mjs','check','--environment=TEST'],{cwd:repoRoot,stdio:'ignore'});
  const componentDiff=spawnSync('git',['diff','--name-only',component.baseCommit,commit,'--','supabase','scripts/cloudtms-db-component-release.mjs'],{cwd:repoRoot,encoding:'utf8'});
  const componentPaths=component.files.map(x=>x.path);
  const scopeVerified=componentDiff.status===0 && componentDiff.stdout.trim().split(/\r?\n/).filter(Boolean)
    .every(p=>componentPaths.includes(p)||component.verificationFiles.includes(p)||p==='supabase/release/weekly-source-invoice-evidence-component.json');
  state.components.push({id:component.componentId,paths:componentPaths,sourceVerified:componentCheck.status===0,scopeVerified,
    baseVerified:component.files.every(x=>[x.beforeClosureSha256,x.afterClosureSha256].includes(repeatableMap.get(x.path)))});
  const decision=chooseTestDatabaseRoute(state);
  Object.assign(receipt,{decision,previousRelease:latest,pendingMigrations:state.pendingMigrations,pendingRepeatables:state.pendingRepeatables,status:'PLANNED'});save();
  console.log(JSON.stringify({decision,pendingMigrations:state.pendingMigrations.length,pendingRepeatables:state.pendingRepeatables.length,previousStatus:state.latestStatus},null,2));
  if(command==='apply') {
    if(decision.route==='FULL_UPGRADE') run('scripts/cloudtms-db-release.mjs',['apply','--environment=TEST','--mode=UPGRADE'],{CLOUDTMS_RELEASE_APPROVAL:`APPLY TEST UPGRADE ${commit}`});
    else if(decision.route==='APPROVED_COMPONENT'&&decision.component===component.componentId) {
      run('scripts/cloudtms-db-component-release.mjs',['plan','--environment=TEST']);
      run('scripts/cloudtms-db-component-release.mjs',['rehearse','--environment=TEST']);
      run('scripts/cloudtms-db-component-release.mjs',['apply','--environment=TEST'],{CLOUDTMS_RELEASE_APPROVAL:`APPLY TEST COMPONENT ${component.componentId} ${commit}`});
    } else if(decision.route!=='NO_DATABASE_CHANGE') throw new Error('No approved executable component adapter registered');
    receipt.status='DATABASE_READY';receipt.completedAt=new Date().toISOString();save();
    console.log('DATABASE_READY: continue ordered application publication. This is not proof of backend, Office or phone deployment.');
  }
} catch(error) {
  receipt.status='FAILED';receipt.failure=String(error.message).split('\n')[0].slice(0,500);receipt.completedAt=new Date().toISOString();save();
  console.error(receipt.failure);process.exitCode=1;
}
