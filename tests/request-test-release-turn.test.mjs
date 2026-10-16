import test,{after} from 'node:test';
import {createF5PublicationArtifactModel} from './fixtures/f5-publication-artifact-model.mjs';
import assert from 'node:assert/strict';
import {requestTestRelease} from '../scripts/request-test-release.mjs';
const f5Model=await createF5PublicationArtifactModel();after(()=>f5Model.cleanup());
const sha=f5Model.commit,previous='b'.repeat(40),office='c'.repeat(40);
const targets=[['test-cloudtms-backend','deploy/cloudflare/test-cloudtms-backend'],['test-cloudtms-candidate-private-api','deploy/cloudflare/test-candidate-private-api'],
 ['test-cloudtms-candidate-synthetic-private-api','deploy/cloudflare/test-candidate-synthetic-private-api'],['test-cloudtms-candidate-broker','deploy/cloudflare/test-candidate-broker']];
function fixture({loseAt,runPending=false,foreignRun=false,moveSource=false,f5Failure=null}={}){
 const controller=new AbortController(),commands=[],snapshots=[],phases=[];let sourceReads=0;
 const proof={environment:'TEST',backendCommit:sha,checkedAt:new Date().toISOString(),workers:targets.map(([worker,branch])=>({worker,branch,repository:'kierarthur/cloudtms-backend',verified:true,branchCommit:previous,activeVersion:'9a2a1961-4a07-4d65-9fb0-d79816b0f1d4',activeCommit:previous}))};
 const turn={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:42,backendPid:71},signal:controller.signal,
  async assertHeld(phase){phases.push(phase);if(phase===loseAt)controller.abort(Error('SHARED_TURN_LOST'));if(controller.signal.aborted)throw controller.signal.reason;},close(){phases.push('close');}};
 const deps={turn,proof,log(){},save(receipt){snapshots.push(structuredClone(receipt));},delay:async()=>assert.fail('fixture must not sleep'),async command(file,args,{cwd,signal}){
  commands.push([file,...args]);
  if(file==='git'){
   const a=args.slice(4);if(a[0]==='remote')return cwd.includes('office')?'git@github.com:kierarthur/TEST-Frontend.git':'git@github.com:kierarthur/cloudtms-backend.git';
   if(a[0]==='status')return '';if(a[0]==='rev-parse')return cwd.includes('office')?office:sha;
   if(a[0]==='ls-remote'){if(a[2]==='refs/heads/test')return `${moveSource&&++sourceReads>1?previous:sha}\t${a[2]}`;return `${previous}\t${a[2]}`;}
   if(a[0]==='diff')return 'broker/src/index.js';if(['fetch','push'].includes(a[0]))return '';throw Error('unexpected fake git');
  }
  if(args[0]==='workflow')return 'https://github.com/kierarthur/cloudtms-backend/actions/runs/123';
  if(args[0]==='run'&&args[1]==='download'){
   const receipt=structuredClone(f5Model.receipt);if(f5Failure==='restoration')receipt.f5Gate.restoration_sha256=null;
   if(f5Failure==='foreign-release')receipt.databaseRelease.release_id='foreign';
   if(f5Failure==='foreign-source')receipt.f5Gate.source_commit=previous;
   f5Model.writeReceipt(args[args.indexOf('--dir')+1],receipt);return '';
  }
  const p=args[1];
  if(p.endsWith('/actions/runs/123/artifacts?per_page=100'))return JSON.stringify({total_count:f5Failure==='missing'?0:1,
   artifacts:f5Failure==='missing'?[]:[{id:444,name:'automatic-test-release-'+sha,expired:false}]});
  if(p.includes('database-release.yml/runs?per_page'))return JSON.stringify({workflow_runs:foreignRun?[{id:999,status:'in_progress'}]:[]});
  if(p.endsWith('/actions/runs/123')){
   if(runPending){return await new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>reject(signal.reason),{once:true});setImmediate(()=>controller.abort(Error('SHARED_TURN_LOST')));});}
   return JSON.stringify({id:123,head_sha:sha,event:'workflow_dispatch',status:'completed',conclusion:'success',html_url:'https://github.com/kierarthur/cloudtms-backend/actions/runs/123'});
  }
  if(p.includes('/check-runs?'))return JSON.stringify({check_runs:targets.map(([worker],i)=>({id:i+1,app:{slug:'cloudflare-workers-and-pages'},name:`Workers Builds: ${worker}`,started_at:new Date(Date.now()+1000).toISOString(),status:'completed',conclusion:'success'}))});
  if(p.endsWith('pages/builds/latest'))return JSON.stringify({commit:office,status:'built',url:'https://example.test'});
  throw Error('unexpected fake API');
 }};
 return {controller,commands,snapshots,phases,deps};
}
const request=deps=>requestTestRelease({root:f5Model.root,args:['--shared-turn','--office-path=C:/fake/office'],env:{},deps});
const dispatches=commands=>commands.filter(c=>c.includes('workflow')&&c.includes('run'));
const pushes=commands=>commands.filter(c=>c.includes('push'));

test('managed AUTO route retains turn checks through protected wait and ordered Workers then Office',async()=>{
 const f=fixture(),result=await request(f.deps);assert.equal(result.status,'DEPLOYED_ACCEPTANCE_PENDING');assert.equal(dispatches(f.commands).length,1);
 assert.deepEqual(pushes(f.commands).map(c=>c.at(-1)),[...targets.map(([,branch])=>`${sha}:refs/heads/${branch}`),`${office}:refs/heads/main`]);
 assert.ok(f.commands.every(c=>!c.includes('cancel')));assert.ok(f.phases.includes('after-protected-result'));assert.equal(f.phases.at(-1),'close');
});
test('loss before dispatch prevents protected mutation and every source publication',async()=>{
 const f=fixture({loseAt:'protected-dispatch'}),result=await request(f.deps);assert.equal(result.status,'STOPPED');assert.equal(result.sharedTurnLost,true);
 assert.equal(dispatches(f.commands).length,0);assert.equal(pushes(f.commands).length,0);
});
test('loss during long own GitHub read stays responsive, retains exact run, holds publication and does not redispatch or cancel remote work',async()=>{
 const f=fixture({runPending:true}),result=await request(f.deps);assert.equal(result.status,'STOPPED');assert.equal(result.databaseRunId,123);
 assert.equal(result.databaseReconciliationRequired.runId,123);assert.equal(result.databaseReconciliationRequired.remoteStopped,false);
 assert.equal(result.databaseReconciliationRequired.duplicateDispatchAllowed,false);assert.equal(dispatches(f.commands).length,1);assert.equal(pushes(f.commands).length,0);
 assert.ok(f.commands.every(c=>!c.includes('cancel')));
});
test('loss after successful protected result still prevents runtime and Office push',async()=>{
 const f=fixture({loseAt:'before-publication-test-cloudtms-backend'}),result=await request(f.deps);assert.equal(result.status,'STOPPED');assert.equal(pushes(f.commands).length,0);
 assert.equal(result.runtimeOfficePublicationHeld,true);assert.equal(dispatches(f.commands).length,1);
});
test('another active protected run and changed canonical source both refuse dispatch',async()=>{
 for(const options of [{foreignRun:true},{moveSource:true}]){const f=fixture(options),result=await request(f.deps);assert.equal(result.status,'STOPPED');assert.equal(dispatches(f.commands).length,0);assert.equal(pushes(f.commands).length,0);}
});
test('protected failure preserves original failure and never publishes',async()=>{
 const f=fixture(),command=f.deps.command;f.deps.command=(file,args,options)=>args[1]?.endsWith('/actions/runs/123')
  ?JSON.stringify({id:123,head_sha:sha,event:'workflow_dispatch',status:'completed',conclusion:'failure'}):command(file,args,options);
 const result=await request(f.deps);assert.equal(result.status,'STOPPED');assert.match(result.failure,/Protected TEST database release failed/);assert.equal(pushes(f.commands).length,0);
});
test('a repeated command preserves an interrupted own receipt and dispatches no duplicate',async()=>{
 const f=fixture();f.deps.previousReceipt={status:'STOPPED',commit:sha,databaseDispatchAttempted:true,databaseRunId:123};
 const result=await request(f.deps);assert.equal(result.databaseRunId,123);assert.match(result.failure,/REQUIRES_RECONCILIATION/);
 assert.equal(f.commands.length,0);assert.equal(f.snapshots.length,0);
});

test('actual fixed artifact loader must pass before the first Worker push',async()=>{
 const f=fixture(),result=await request(f.deps);assert.equal(result.status,'DEPLOYED_ACCEPTANCE_PENDING');
 const download=f.commands.findIndex(c=>c.includes('download'));const firstPush=f.commands.findIndex(c=>c.includes('push'));
 assert.ok(download>=0&&firstPush>download);assert.equal(result.f5Gate.protected_run_id,123);
 assert.equal(result.f5Gate.protected_artifact_id,444);assert.ok(f.phases.includes('after-f5-prepublication'));
});
test('missing or malformed F5 source/release/restoration artifacts hold every Worker and Office push',async()=>{
 for(const f5Failure of ['missing','restoration','foreign-release','foreign-source']){
  const f=fixture({f5Failure}),result=await request(f.deps);assert.equal(result.status,'STOPPED');
  assert.match(result.failure,/F5_/);assert.equal(pushes(f.commands).length,0);assert.equal(dispatches(f.commands).length,1);
 }
});
