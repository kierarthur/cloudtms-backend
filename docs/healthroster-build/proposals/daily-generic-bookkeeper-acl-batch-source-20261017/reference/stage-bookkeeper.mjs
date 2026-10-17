// Root-only public bookkeeping. No native/API/browser/auth/control connection.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
import {must,publicPath,publicBase,exactPlan,previousWitness,beforeProof,afterProof,setupAppend,publishPrefix} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017/core.mjs';
const report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',operator='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017',here=report+'/'+"docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017",helperFile=fileURLToPath(import.meta.url),helperRelative="docs/healthroster-build/proposals/daily-genuine-root-stage-bookkeeper-20261017.mjs";
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(repository,...args)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',repository,...args],{maxBuffer:32e6,windowsHide:true,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
export async function run(env=process.env){
 const stage=env.HR_ROOT_STAGE,phase=env.HR_ROOT_PHASE,out=env.HR_ROOT_OUT,source=env.HR_OPERATOR_COMMIT,planFile=publicPath(env.HR_ROOT_PLAN);
 must(['BEFORE','SETUP','AFTER','PUBLISH'].includes(phase)&&['S1','S1a','S2','S3','S4',...phase==='PUBLISH'?['S0']:[]].includes(stage),'BOOKKEEPER_EXACT_PHASE_STAGE_REQUIRED');
 must(/^[0-9a-f]{40}$/.test(source??'')&&/^D:\/Development\/GitHub\/\.codex-worktrees\/healthroster-install2-14102026\/\.codex-tmp\/daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(out??''),'BOOKKEEPER_ACTUAL_SOURCE_OWNED_PATH_REQUIRED');
 must(physical(report)===report&&physical(root)===root&&physical(out)===out&&physical(here)===here.replaceAll('\\','/')&&physical(process.cwd()).startsWith('D:/Development/GitHub'),'BOOKKEEPER_REGISTERED_PHYSICAL_D_REQUIRED');
 for(const [repository,name] of [[report,'healthroster-v2-report-build-13102026'],[root,'healthroster-install2-14102026']]){
  must(physical(git(repository,'rev-parse','--show-toplevel').toString().trim())===repository&&physical(git(repository,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+name&&physical(path.resolve(repository,git(repository,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','BOOKKEEPER_PHYSICAL_GIT_REQUIRED');
 }
 must(git(report,'rev-parse','HEAD').toString().trim()===source,'BOOKKEEPER_FIXED_REPORT_HEAD_REQUIRED');must(git(root,'diff','--cached','--name-only','-z').length===0,'BOOKKEEPER_UNRELATED_STAGING_REFUSED');
 // Public image check only, deliberately NOT sourcePreflight() (no auth path metadata).
 const {reviewedImage}=await import(pathToFileURL(report+'/'+operator+'/git-image-batch.mjs').href),ownRelative=path.relative(report,here).replaceAll('\\','/');
 const current=JSON.parse(fs.readFileSync(report+'/'+operator+'/MANIFEST.json')),own=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));
 reviewedImage(report,source,[operator+'/MANIFEST.json',...current.files.map(r=>operator+'/'+r.file),ownRelative+'/MANIFEST.json',...own.files.map(r=>ownRelative+'/'+r.file)]);
 const pins=JSON.parse(fs.readFileSync(here+'/PARENTS.json'));for(const row of pins.fixedRuntime){const b=fs.readFileSync(report+'/'+row.file);must(b.length===row.bytes&&sha(b)===row.sha256&&b.equals(git(report,'show',row.commit+':'+row.file)),'BOOKKEEPER_ORIGINAL_RUNTIME_OWNER_CHANGED');}
 const rootHead=git(root,'rev-parse','HEAD').toString().trim();
 const helperCommit=env.HR_ROOT_HELPER_COMMIT;
 must(/^[0-9a-f]{40}$/.test(helperCommit??'')&&physical(helperFile)===root+'/'+helperRelative,'BOOKKEEPER_COMMITTED_ROOT_HELPER_REQUIRED');
 const helperBytes=fs.readFileSync(helperFile);
 must([rootHead,'',helperCommit].every(ref=>helperBytes.equals(git(root,'show',ref+':'+helperRelative))),'BOOKKEEPER_EXACT_ROOT_HELPER_IMAGES_REQUIRED');
 git(root,'merge-base','--is-ancestor',helperCommit,rootHead);
 const readPublic=file=>{publicPath(file);must(physical(root+'/'+file)===root+'/'+file,'BOOKKEEPER_PHYSICAL_PUBLIC_FILE_REQUIRED');const b=small(root+'/'+file,2e6);must([rootHead,''].every(ref=>b.equals(git(root,'show',ref+':'+file))),'BOOKKEEPER_COMMITTED_PUBLIC_EVIDENCE_REQUIRED');return b;};
 const read=file=>{
  const exact=new Set([stage+'_BEFORE_INPUT.json',stage+'_BEFORE_NATIVE.json',stage+'_AFTER_INPUT.json',stage+'_AFTER_NATIVE.json',stage+'_RESPONSE.json',stage+'_ENTRY_BINDING.json',stage+'_SETUP_ENTRY.md',stage+'_COORDINATOR_SUMMARY.json']);
  must(exact.has(file)||phase==='PUBLISH'&&file===env.HR_ROOT_QUALIFY_RESULT&&new RegExp('^CLOSED_qualify_'+stage+'_[a-f0-9]{16}\\.json$').test(file),'BOOKKEEPER_CLOSED_PUBLIC_PHASE_FILE_REQUIRED');return small(out+'/'+file,2e6);
 };
 const plan=JSON.parse(readPublic(planFile));exactPlan(plan,stage,source);const prior=plan.previousEvidence?JSON.parse(readPublic(plan.previousEvidence)):null;previousWitness(plan,prior);if(prior)must(prior.originalCoordinatorSummary?.ownedDirectory===out,'BOOKKEEPER_SAME_ACTUAL_PREFIX_SESSION_REQUIRED');
 const contract=await import(pathToFileURL(report+'/'+operator+'/contract.mjs').href),native=await import(pathToFileURL(report+'/'+operator+'/native-read.mjs').href),stagesOwner=await import(pathToFileURL(report+'/'+operator+'/stages.mjs').href),owners={...contract,...native,...stagesOwner};
 const effectFile=publicBase+'DAILY_GENUINE_'+stage+'_EFFECT_ADMISSION_20261017.json',afterFile=publicBase+'DAILY_GENUINE_'+stage+'_AFTER_EFFECT_QUALIFICATION_20261017.json';
 const get=phase=>{const built=JSON.parse(read(stage+'_'+phase+'_INPUT.json')),bytes=read(stage+'_'+phase+'_NATIVE.json');must(built.input.sql&&sha(built.input.sql)===built.receipt.sqlSha256&&sha(built.input.verification_sql)===built.receipt.verificationSqlSha256,'BOOKKEEPER_ACTUAL_INPUT_HASH_REQUIRED');return {built,bytes,result:JSON.parse(bytes)};};
 const pending=[];const write=(file,value)=>{publicPath(file);must(file.startsWith(publicBase)&&file.endsWith('.json')&&physical(path.dirname(root+'/'+file))===path.dirname(root+'/'+file).replaceAll('\\','/'),'BOOKKEEPER_PHYSICAL_QUALIFICATION_DESTINATION_REQUIRED');must(!pending.some(r=>r.file===file),'BOOKKEEPER_DUPLICATE_WRITE_REQUIRED');pending.push({file,bytes:Buffer.from(JSON.stringify(value,null,2)+'\n'),append:false});};
 if(phase==='BEFORE'){
  const data=get('BEFORE'),effect=JSON.parse(readPublic(effectFile));write(plan.beforeEvidence,beforeProof({stage,source,plan,prior,...data,effect,readPublic,now:Date.now()},owners));
 }else if(phase==='SETUP'){
  const file='docs/healthroster-build/TEST_SETUP_MANIFEST.md',old=readPublic(file),entry=JSON.parse(read(stage+'_ENTRY_BINDING.json')),before=get('BEFORE'),effectBytes=readPublic(effectFile),addition=read(stage+'_SETUP_ENTRY.md').toString();
  const next=setupAppend({stage,source,plan,entry,beforeBuilt:before.built,beforeBytes:before.bytes,effectBytes,manifest:old.toString(),addition},owners);must(Buffer.from(next).subarray(0,old.length).equals(old),'BOOKKEEPER_SETUP_PREFIX_REQUIRED');pending.push({file,bytes:Buffer.from(next),parent:old,append:true});
 }else if(phase==='AFTER'){
  const b=get('BEFORE'),data=get('AFTER'),response=JSON.parse(read(stage+'_RESPONSE.json')),afterEffect=JSON.parse(readPublic(afterFile));
  write(plan.afterEvidence,afterProof({stage,source,plan,prior,beforeBuilt:b.built,beforeBytes:b.bytes,beforeResult:b.result,...data,response,afterEffect,readPublic,now:Date.now()},owners));
 }else{
  const before=JSON.parse(readPublic(plan.beforeEvidence)),after=JSON.parse(readPublic(plan.afterEvidence)),response=JSON.parse(read(stage+'_RESPONSE.json')),summary=JSON.parse(read(stage+'_COORDINATOR_SUMMARY.json')),qualify=JSON.parse(read(env.HR_ROOT_QUALIFY_RESULT));
  must(summary.ownedDirectory===out,'BOOKKEEPER_EXACT_RELEASED_SESSION_REQUIRED');
  const beforeBytes=read(stage+'_BEFORE_NATIVE.json'),afterBytes=read(stage+'_AFTER_NATIVE.json'),effect=JSON.parse(readPublic(afterFile));
  must(effect.version==='DAILY_GENUINE_ROOT_AFTER_EFFECT_QUALIFICATION_V1'&&effect.stage===stage&&effect.beforeEnvelopeSha256===sha(beforeBytes)&&effect.afterEnvelopeSha256===sha(afterBytes)&&effect.responseProjectionSha256===sha(JSON.stringify(response.projection)),'BOOKKEEPER_ACTUAL_COMMITTED_AFTER_EFFECT_REQUIRED');for(const k of ['canonicalAfterEvidence','exactApprovedChangesEvidence','backgroundAttributionEvidence'])readPublic(effect[k]);
  write(plan.completedEvidence,publishPrefix({stage,source,plan,prior,beforeProof:before,afterProof:after,qualify,summary,beforeBytes,afterBytes,response},owners));
 }
 must(git(report,'rev-parse','HEAD').toString().trim()===source&&git(root,'rev-parse','HEAD').toString().trim()===rootHead,'BOOKKEEPER_HEAD_MOVED');
 // Decide and validate everything before any write; no dossier or private file is created.
 for(const r of pending){if(r.append)must(fs.readFileSync(root+'/'+r.file).equals(r.parent),'BOOKKEEPER_MANIFEST_CHANGED');else must(!fs.existsSync(root+'/'+r.file),'BOOKKEEPER_QUALIFICATION_HISTORY_EXISTS');}
 for(const r of pending)fs.writeFileSync(root+'/'+r.file,r.bytes,{flag:r.append?'w':'wx'});
 const files=pending.map(r=>r.file);git(root,'add','--',...files);for(const r of pending)must(r.bytes.equals(git(root,'show',':'+r.file)),'BOOKKEEPER_EXACT_INDEX_REQUIRED');git(root,'diff','--cached','--check');
 const selected=git(root,'diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort();must(JSON.stringify(selected)===JSON.stringify([...files].sort()),'BOOKKEEPER_EXACT_OWNED_INDEX_REQUIRED');
 git(root,'commit','-m','Record genuine '+stage+' '+phase+' public qualification','--only','--',...files);const commit=git(root,'rev-parse','HEAD').toString().trim();
 // Accepted S0R7 timing: BEFORE local commit; SETUP publishes both before any submit.
 if(phase!=='BEFORE'){
  git(root,'push','origin','HEAD:refs/heads/codex/healthroster-install2-14102026');
  must(git(root,'ls-remote','origin','refs/heads/codex/healthroster-install2-14102026').toString().split(/\s/)[0]===commit,'BOOKKEEPER_REMOTE_REQUIRED');
 }must(git(report,'rev-parse','HEAD').toString().trim()===source,'BOOKKEEPER_REPORT_HEAD_MOVED');
 return {status:'ROOT_GENUINE_'+stage+'_'+phase+'_COMMITTED',commit,files};
}
function small(file,max){const s=fs.lstatSync(file);must(s.isFile()&&!s.isSymbolicLink()&&s.size<=max,'BOOKKEEPER_SMALL_PUBLIC_FILE_REQUIRED');return fs.readFileSync(file);}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){try{console.log(JSON.stringify(await run()));}catch(error){console.log(JSON.stringify({status:'HOLD',code:/^BOOKKEEPER_[A-Z0-9_]+$/.test(error?.message??'')?error.message:'BOOKKEEPER_HELD',noBusinessRequestAuthored:true}));process.exitCode=1;}}
