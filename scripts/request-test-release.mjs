#!/usr/bin/env node
// Desktop coordinator. GitHub SSH publishes; protected GitHub Actions owns SQL.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {validateConnectionProof,selectDispatchedRun,classifyApplicationPaths,publicationStages} from './automatic-test-release-policy.mjs';
import {connectTurnControl,runOwnedTurnChild,abortableDelay} from './healthroster-turn-control.mjs';

export async function requestTestRelease({root=process.cwd(),args=process.argv.slice(2),env=process.env,deps={}}={}) {
 const options=Object.fromEntries(args.map(s=>{
  if(s==='--shared-turn')return ['shared-turn',true];
  const m=s.match(/^--(office-path|connections-proof)=(.+)$/);
  if(!m)throw Error('Only --office-path, --connections-proof and --shared-turn are accepted');return [m[1],m[2]];
 }));
 const gh=env.GH_BIN||'gh',out=path.join(root,'.codex-tmp','test-deployment-receipt.json');
 const receipt={formatVersion:2,target:'TEST',startedAt:new Date().toISOString(),status:'PREFLIGHT',stages:[]};
 const save=()=>{if(deps.save)deps.save(receipt);else {fs.mkdirSync(path.dirname(out),{recursive:true});fs.writeFileSync(out,JSON.stringify(receipt,null,2)+'\n');}};
 const stage=(name,result)=>{receipt.stages.push({name,...result,at:new Date().toISOString()});(deps.log??console.log)(`${name}: ${result.status}`);save();};
 let turn;
 try {
  // Preserve an interrupted own dispatch for reconciliation. A repeated command
  // never overwrites its receipt or treats a local stop as a remote cancellation.
  const previous=deps.previousReceipt??(fs.existsSync(out)?JSON.parse(fs.readFileSync(out,'utf8')):null);
  if(previous&&previous.status!=='DEPLOYED_ACCEPTANCE_PENDING'&&(previous.databaseDispatchAttempted||previous.databaseRunId)){
   (deps.log??console.error)('EXISTING_OWN_RELEASE_REQUIRES_RECONCILIATION');
   return {...previous,status:'STOPPED',failure:'EXISTING_OWN_RELEASE_REQUIRES_RECONCILIATION'};
  }
  // IPC must be live. A file containing a historical identity cannot grant a turn.
  if(options['shared-turn']||env.CLOUDTMS_SHARED_TURN_PIPE||env.CLOUDTMS_SHARED_TURN_CAPABILITY){
   turn=deps.turn??await connectTurnControl(env);receipt.sharedTurn=turn.identity;
  } else if(deps.turn)turn=deps.turn;
  const signal=turn?.signal??new AbortController().signal;
  const guard=async phase=>{if(signal.aborted)throw signal.reason;await turn?.assertHeld(phase);if(signal.aborted)throw signal.reason;};
  const command=async(file,commandArgs,cwd=root)=>{
   await guard('before-command');
   const value=deps.command?await deps.command(file,commandArgs,{cwd,signal}):await runOwnedTurnChild({file,args:commandArgs,
    options:{cwd,env,stdio:['ignore','pipe','pipe']},signal,assertHeld:guard});
   await guard('after-command');
   if(typeof value==='string')return value.trim();
   if(value.code!==0)throw Error('RELEASE_COMMAND_FAILED');return value.stdout.trim();
  };
  const git=(arguments_,cwd=root)=>command('git',['-c','maintenance.auto=false','-c','gc.auto=0',...arguments_],cwd);
  const api=async p=>JSON.parse(await command(gh,['api',p]));
  async function waitFor(label,read,done,failed,minutes=30){
   const deadline=Date.now()+minutes*60000;
   while(Date.now()<deadline){
    await guard('before-'+label);const value=await read();await guard('after-'+label);
    if(failed(value))throw Error(`${label} failed; later stages held`);if(done(value))return value;
    (deps.log??console.log)(`${label}: waiting`);await (deps.delay??abortableDelay)(15000,signal);
   }
   throw Error(`${label} exceeded the coordinator wait budget; inspect the existing run, do not dispatch another blindly`);
  }
  await guard('release-preflight');
  const remote=await git(['remote','get-url','origin']);
  if(!/(?:github\.com[:/])kierarthur\/cloudtms-backend(?:\.git)?$/.test(remote))throw Error('Wrong backend repository');
  if(await git(['status','--porcelain']))throw Error('Worktree must be clean: preserve and commit only reviewed task changes first');
  const sha=await git(['rev-parse','HEAD']);receipt.commit=sha;
  if((await git(['ls-remote','origin','refs/heads/test'])).split(/\s/)[0]!==sha)throw Error('Publish the exact reviewed source to test first; this coordinator never commits or merges work');
  const proof=deps.proof??JSON.parse(fs.readFileSync(options['connections-proof']||'','utf8'));
  const targets=[
   ['backend','test-cloudtms-backend','deploy/cloudflare/test-cloudtms-backend',['broker','shared','package.json','package-lock.json','wrangler.toml']],
   ['candidate-private','test-cloudtms-candidate-private-api','deploy/cloudflare/test-candidate-private-api',['candidate-private-api','broker','shared','package.json','package-lock.json']],
   ['candidate-synthetic','test-cloudtms-candidate-synthetic-private-api','deploy/cloudflare/test-candidate-synthetic-private-api',['candidate-synthetic-private-api','broker','shared','package.json','package-lock.json']],
   ['candidate-broker','test-cloudtms-candidate-broker','deploy/cloudflare/test-candidate-broker',['candidate-broker','broker','shared','package.json','package-lock.json']],
  ];
  validateConnectionProof(proof,sha,targets);
  for(const [,worker,branch] of targets){
   const branchCommit=(await git(['ls-remote','origin',`refs/heads/${branch}`])).split(/\s/)[0];
   if(branchCommit!==proof.workers.find(x=>x.worker===worker).branchCommit)throw Error(`Release branch differs from inspected evidence: ${worker}; reconcile before dispatch`);
   await git(['fetch','--no-tags','origin',branch]);
   if(worker==='test-cloudtms-backend')publicationStages(classifyApplicationPaths((await git(['diff','--name-only',branchCommit,sha])).split(/\r?\n/).filter(Boolean)));
  }
  let office;
  if(options['office-path']){
   const cwd=path.resolve(options['office-path']);
   if(await git(['status','--porcelain'],cwd))throw Error('Office worktree is dirty');
   if(!/(?:github\.com[:/])kierarthur\/TEST-Frontend(?:\.git)?$/.test(await git(['remote','get-url','origin'],cwd)))throw Error('Wrong Office repository');
   office={cwd,sha:await git(['rev-parse','HEAD'],cwd),before:(await git(['ls-remote','origin','refs/heads/main'],cwd)).split(/\s/)[0]};receipt.officeCommit=office.sha;
  }
  await guard('before-protected-dispatch');validateConnectionProof(proof,sha,targets);
  if((await git(['ls-remote','origin','refs/heads/test'])).split(/\s/)[0]!==sha)throw Error('Backend test moved before protected dispatch; applications held');
  const active=(await api('repos/kierarthur/cloudtms-backend/actions/workflows/database-release.yml/runs?per_page=100')).workflow_runs;
  if(active.some(run=>run.status!=='completed'))throw Error('PROTECTED_RELEASE_RUNNING_HOLD');
  receipt.status='DATABASE';receipt.databaseDispatchAttempted=true;receipt.databaseDispatchStartedAt=new Date().toISOString();save();
  await guard('protected-dispatch');
  const dispatched=await command(gh,['workflow','run','database-release.yml','--repo','kierarthur/cloudtms-backend','--ref','test','-f','environment=TEST','-f','mode=UPGRADE','-f','phase=AUTO']);
  const dispatchedId=dispatched.match(/https:\/\/github\.com\/kierarthur\/cloudtms-backend\/actions\/runs\/(\d+)/)?.[1];
  if(dispatchedId){receipt.databaseRunId=Number(dispatchedId);receipt.databaseRunUrl=`https://github.com/kierarthur/cloudtms-backend/actions/runs/${dispatchedId}`;save();}
  const run=await waitFor('Protected TEST database release',async()=>{
   const selected=receipt.databaseRunId?await api(`repos/kierarthur/cloudtms-backend/actions/runs/${receipt.databaseRunId}`)
    :selectDispatchedRun((await api(`repos/kierarthur/cloudtms-backend/actions/workflows/database-release.yml/runs?head_sha=${sha}&event=workflow_dispatch&per_page=20`)).workflow_runs,sha,receipt.databaseDispatchStartedAt);
   if(selected&&(selected.head_sha!==sha||selected.event!=='workflow_dispatch'))throw Error('Dispatched workflow source differs from reviewed commit; applications held');
   if(selected&&!receipt.databaseRunId){receipt.databaseRunId=selected.id;receipt.databaseRunUrl=selected.html_url;save();}return selected;
  },r=>r?.status==='completed'&&r.conclusion==='success',r=>r?.status==='completed'&&r.conclusion!=='success',120);
  await guard('after-protected-result');stage('database',{status:'VERIFIED_OR_EXACT_UNCHANGED',runId:run.id,url:run.html_url});
  for(const [name,worker,branch,paths] of targets){
   await guard('before-publication-'+worker);
   const previous=(await git(['ls-remote','origin',`refs/heads/${branch}`])).split(/\s/)[0];
   if(!/^[a-f0-9]{40}$/.test(previous))throw Error(`Release branch must be bootstrapped and verified: ${branch}`);
   const inspected=proof.workers.find(x=>x.worker===worker);
   if(previous!==inspected.branchCommit)throw Error(`Release branch changed during database verification: ${worker}; re-inspect before publishing`);
   await git(['fetch','--no-tags','origin',branch]);
   if(inspected.activeCommit===previous&&!await git(['diff','--name-only',previous,sha,'--',...paths])){stage(name,{status:'UNCHANGED',commit:previous,version:inspected.activeVersion});continue;}
   if(previous===sha)throw Error(`Source branch already at target but active build is unproved: ${worker}; inspect or explicitly rebuild this exact trigger, never claim unchanged`);
   const promotedAt=new Date().toISOString();await guard('push-'+worker);await git(['push','origin',`${sha}:refs/heads/${branch}`]);
   const check=await waitFor(worker,async()=> (await api(`repos/kierarthur/cloudtms-backend/commits/${sha}/check-runs?per_page=100`)).check_runs
    .filter(c=>c.app?.slug==='cloudflare-workers-and-pages'&&c.name===`Workers Builds: ${worker}`&&c.started_at>=promotedAt.replace(/\.\d+Z$/,'Z')).sort((a,b)=>b.id-a.id)[0],
    c=>c?.status==='completed'&&c.conclusion==='success',c=>c?.status==='completed'&&c.conclusion!=='success');
   stage(name,{status:'BUILD_DEPLOY_SUCCEEDED',commit:sha,checkId:check.id});
  }
  if(office){
   await guard('before-office-publication');
   if((await git(['ls-remote','origin','refs/heads/main'],office.cwd)).split(/\s/)[0]!==office.before)throw Error('Office main changed during database/backend release; replan, do not overwrite');
   await guard('push-office');await git(['push','origin',`${office.sha}:refs/heads/main`],office.cwd);
   const build=await waitFor('Office Pages',()=>api('repos/kierarthur/TEST-Frontend/pages/builds/latest'),b=>b.commit===office.sha&&b.status==='built',b=>b.commit===office.sha&&b.status==='errored');
   stage('office',{status:'PAGES_DEPLOY_SUCCEEDED',commit:office.sha,url:build.url});
  }
  await guard('release-complete');receipt.status='DEPLOYED_ACCEPTANCE_PENDING';save();
  (deps.log??console.log)('Deployment completed. Functional browser/phone acceptance has not been run.');
 } catch(error){
  receipt.status='STOPPED';receipt.failure=String(error.message).split('\n')[0].slice(0,600);
  if(turn?.signal.aborted){receipt.sharedTurnLost=true;receipt.runtimeOfficePublicationHeld=true;}
  if(receipt.databaseDispatchAttempted)receipt.databaseReconciliationRequired={commit:receipt.commit,runId:receipt.databaseRunId??null,since:receipt.databaseDispatchStartedAt,
   remoteStopped:false,duplicateDispatchAllowed:false};
  save();(deps.log??console.error)(receipt.failure);
 } finally{turn?.close?.();}
 return receipt;
}

if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
 const receipt=await requestTestRelease();if(receipt.status==='STOPPED')process.exitCode=1;
}
