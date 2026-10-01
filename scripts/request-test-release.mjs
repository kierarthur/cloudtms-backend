#!/usr/bin/env node
// Desktop coordinator. GitHub SSH publishes; protected GitHub Actions owns SQL.
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { validateConnectionProof, selectDispatchedRun, classifyApplicationPaths, publicationStages } from './automatic-test-release-policy.mjs';
const root=process.cwd();
const options=Object.fromEntries(process.argv.slice(2).map(s=>{const m=s.match(/^--(office-path|connections-proof)=(.+)$/);if(!m)throw new Error('Only --office-path and --connections-proof are accepted');return [m[1],m[2]];}));
const gh=process.env.GH_BIN||'gh';
const git=(args,cwd=root)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,encoding:'utf8'}).trim();
const api=p=>JSON.parse(execFileSync(gh,['api',p],{cwd:root,encoding:'utf8'}));
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const out=path.join(root,'.codex-tmp','test-deployment-receipt.json');
const receipt={formatVersion:1,target:'TEST',startedAt:new Date().toISOString(),status:'PREFLIGHT',stages:[]};
function save(){fs.mkdirSync(path.dirname(out),{recursive:true});fs.writeFileSync(out,JSON.stringify(receipt,null,2)+'\n');}
function stage(name,result){receipt.stages.push({name,...result,at:new Date().toISOString()});console.log(`${name}: ${result.status}`);save();}
async function waitFor(label,read,done,failed,minutes=30){
  const deadline=Date.now()+minutes*60000;
  while(Date.now()<deadline){const value=await read();if(failed(value))throw new Error(`${label} failed; later stages held`);if(done(value))return value;console.log(`${label}: waiting`);await sleep(15000);}
  throw new Error(`${label} exceeded the coordinator wait budget; inspect the existing run, do not dispatch another blindly`);
}
try {
  const remote=git(['remote','get-url','origin']);
  if(!/(?:github\.com[:/])kierarthur\/cloudtms-backend(?:\.git)?$/.test(remote)) throw new Error('Wrong backend repository');
  if(git(['status','--porcelain']))throw new Error('Worktree must be clean: preserve and commit only reviewed task changes first');
  const sha=git(['rev-parse','HEAD']);receipt.commit=sha;
  if(git(['ls-remote','origin','refs/heads/test']).split(/\s/)[0]!==sha) throw new Error('Publish the exact reviewed source to test first; this coordinator never commits or merges work');
  const proof=JSON.parse(fs.readFileSync(options['connections-proof']||'', 'utf8'));
  const targets=[
    ['backend','test-cloudtms-backend','deploy/cloudflare/test-cloudtms-backend',['broker','shared','package.json','package-lock.json','wrangler.toml']],
    ['candidate-private','test-cloudtms-candidate-private-api','deploy/cloudflare/test-candidate-private-api',['candidate-private-api','broker','shared','package.json','package-lock.json']],
    ['candidate-synthetic','test-cloudtms-candidate-synthetic-private-api','deploy/cloudflare/test-candidate-synthetic-private-api',['candidate-synthetic-private-api','broker','shared','package.json','package-lock.json']],
    ['candidate-broker','test-cloudtms-candidate-broker','deploy/cloudflare/test-candidate-broker',['candidate-broker','broker','shared','package.json','package-lock.json']],
  ];
  validateConnectionProof(proof,sha,targets);
  for(const [,worker,branch] of targets){
    const branchCommit=git(['ls-remote','origin',`refs/heads/${branch}`]).split(/\s/)[0];
    if(branchCommit!==proof.workers.find(x=>x.worker===worker).branchCommit)throw new Error(`Release branch differs from inspected evidence: ${worker}; reconcile before dispatch`);
    git(['fetch','--no-tags','origin',branch]);
    if(worker==='test-cloudtms-backend')publicationStages(classifyApplicationPaths(git(['diff','--name-only',branchCommit,sha]).split(/\r?\n/).filter(Boolean)));
  }
  // Capture Office identity before any deployment, not a moving HEAD later.
  let office;
  if(options['office-path']){
    const cwd=path.resolve(options['office-path']);
    if(git(['status','--porcelain'],cwd))throw new Error('Office worktree is dirty');
    if(!/(?:github\.com[:/])kierarthur\/TEST-Frontend(?:\.git)?$/.test(git(['remote','get-url','origin'],cwd)))throw new Error('Wrong Office repository');
    office={cwd,sha:git(['rev-parse','HEAD'],cwd),before:git(['ls-remote','origin','refs/heads/main'],cwd).split(/\s/)[0]};receipt.officeCommit=office.sha;
  }
  receipt.status='DATABASE';save();
  const since=new Date().toISOString();
  const dispatched=execFileSync(gh,['workflow','run','database-release.yml','--repo','kierarthur/cloudtms-backend','--ref','test','-f','environment=TEST','-f','mode=UPGRADE','-f','phase=AUTO'],{encoding:'utf8'}).trim();
  const dispatchedId=dispatched.match(/https:\/\/github\.com\/kierarthur\/cloudtms-backend\/actions\/runs\/(\d+)/)?.[1];
  if(dispatchedId){receipt.databaseRunId=Number(dispatchedId);receipt.databaseRunUrl=`https://github.com/kierarthur/cloudtms-backend/actions/runs/${dispatchedId}`;save();}
  const run=await waitFor('Protected TEST database release',()=>{
    const selected=receipt.databaseRunId?api(`repos/kierarthur/cloudtms-backend/actions/runs/${receipt.databaseRunId}`):selectDispatchedRun(api(`repos/kierarthur/cloudtms-backend/actions/workflows/database-release.yml/runs?head_sha=${sha}&event=workflow_dispatch&per_page=20`).workflow_runs,sha,since);
    if(selected&&(selected.head_sha!==sha||selected.event!=='workflow_dispatch'))throw new Error('Dispatched workflow source differs from reviewed commit; applications held');
    if(selected&&!receipt.databaseRunId){receipt.databaseRunId=selected.id;receipt.databaseRunUrl=selected.html_url;save();}
    return selected;
  },r=>r?.status==='completed'&&r.conclusion==='success',r=>r?.status==='completed'&&r.conclusion!=='success',120);
  stage('database',{status:'VERIFIED_OR_EXACT_UNCHANGED',runId:run.id,url:run.html_url});
  // Never use force pushes; moving another release's branch is a merge failure.
  for(const [name,worker,branch,paths] of targets){
    const previous=git(['ls-remote','origin',`refs/heads/${branch}`]).split(/\s/)[0];
    if(!/^[a-f0-9]{40}$/.test(previous))throw new Error(`Release branch must be bootstrapped and verified: ${branch}`);
    const inspected=proof.workers.find(x=>x.worker===worker);
    if(previous!==inspected.branchCommit)throw new Error(`Release branch changed during database verification: ${worker}; re-inspect before publishing`);
    git(['fetch','--no-tags','origin',branch]);
    if(inspected.activeCommit===previous&&!git(['diff','--name-only',previous,sha,'--',...paths])){stage(name,{status:'UNCHANGED',commit:previous,version:inspected.activeVersion});continue;}
    if(previous===sha)throw new Error(`Source branch already at target but active build is unproved: ${worker}; inspect or explicitly rebuild this exact trigger, never claim unchanged`);
    const promotedAt=new Date().toISOString();
    git(['push','origin',`${sha}:refs/heads/${branch}`]);
    const check=await waitFor(worker,()=>api(`repos/kierarthur/cloudtms-backend/commits/${sha}/check-runs?per_page=100`).check_runs.filter(c=>c.app?.slug==='cloudflare-workers-and-pages'&&c.name===`Workers Builds: ${worker}`&&c.started_at>=promotedAt.replace(/\.\d+Z$/,'Z')).sort((a,b)=>b.id-a.id)[0],c=>c?.status==='completed'&&c.conclusion==='success',c=>c?.status==='completed'&&c.conclusion!=='success');
    stage(name,{status:'BUILD_DEPLOY_SUCCEEDED',commit:sha,checkId:check.id});
  }
  if(office){
    if(git(['ls-remote','origin','refs/heads/main'],office.cwd).split(/\s/)[0]!==office.before)throw new Error('Office main changed during database/backend release; replan, do not overwrite');
    git(['push','origin',`${office.sha}:refs/heads/main`],office.cwd);
    const build=await waitFor('Office Pages',()=>api('repos/kierarthur/TEST-Frontend/pages/builds/latest'),b=>b.commit===office.sha&&b.status==='built',b=>b.commit===office.sha&&b.status==='errored');
    stage('office',{status:'PAGES_DEPLOY_SUCCEEDED',commit:office.sha,url:build.url});
  }
  receipt.status='DEPLOYED_ACCEPTANCE_PENDING';save();
  console.log('Deployment completed. Functional browser/phone acceptance has not been run. Stop for the requested model switch.');
}catch(error){receipt.status='STOPPED';receipt.failure=String(error.message).split('\n')[0].slice(0,600);save();console.error(receipt.failure);process.exitCode=1;}
