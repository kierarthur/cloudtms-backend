// Root-only original-stage public evidence adoption. No transport, auth, SQL, browser or IPC.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {prepareOriginalStageAfter} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/docs/healthroster-build/proposals/daily-original-stages-after-response-clock-forward-20261017/recorder.mjs';
import {validateCommittedOriginalStageAfter} from './validate.mjs';
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
const P='docs/healthroster-build/proposals/';
const OWN=P+'daily-original-stages-root-effect-adapter-20261017/';
const REC=P+'daily-original-stages-after-response-clock-forward-20261017/';
const OP=P+'daily-health-genuine-staged-operator-20261017/';
const BK=P+'daily-genuine-stage-root-bookkeeper-20261017/';
const CORE=BK+'core.mjs';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const must=(v,c)=>{if(!v)throw Error('STAGE_ADAPTER_'+c)};
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
const git=(r,...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',r,...a],{windowsHide:true,maxBuffer:32e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
const small=(f,n=2e6)=>{const s=fs.lstatSync(f);must(s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=n,'BOUNDED_REGULAR_FILE_REQUIRED');return fs.readFileSync(f)};
export function selection(e){
 const a={stage:e.HR_ROOT_STAGE,source:e.HR_OPERATOR_COMMIT,ownCommit:e.HR_STAGE_ADAPTER_COMMIT,recorderCommit:e.HR_STAGE_RECORDER_COMMIT,out:e.HR_ROOT_OUT,planFile:e.HR_ROOT_PLAN};
 must(['S1a','S2','S3','S4'].includes(a.stage),'EXACT_ORIGINAL_STAGE_REQUIRED');
 for(const v of[a.source,a.ownCommit,a.recorderCommit])must(/^[a-f0-9]{40}$/.test(v??''),'ACTUAL_COMMITS_REQUIRED');
 must(/^D:\/Development\/GitHub\/\.codex-worktrees\/healthroster-install2-14102026\/\.codex-tmp\/daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(a.out??''),'ORIGINAL_OWNED_DIRECTORY_REQUIRED');
 must(new RegExp('^docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_'+a.stage+'_[A-Z0-9_]+\\.json$').test(a.planFile??''),'EXACT_PUBLIC_STAGE_PLAN_REQUIRED');return a;
}
export function checkedPublicReader({root,rootHead,physicalPath,readSmall,gitBytes,validatePublicPath}){
 // Literal unchanged original run-local bookkeeper reader; only dependencies injected for tests.
 return file=>{validatePublicPath(file);must(physicalPath(root+'/'+file)===root+'/'+file,'PHYSICAL_PUBLIC_FILE_REQUIRED');const b=readSmall(root+'/'+file,2e6);must([rootHead,''].every(ref=>b.equals(gitBytes(root,'show',ref+':'+file))),'COMMITTED_PUBLIC_EVIDENCE_REQUIRED');return b;};
}
function packet(relative,commit,head){
 git(ROOT,'merge-base','--is-ancestor',commit,head);
 const raw=small(ROOT+'/'+relative+'MANIFEST.json',512000),m=JSON.parse(raw);
 for(const r of [...m.files,{file:'MANIFEST.json',bytes:raw.length,sha256:sha(raw)}]){
  must(typeof r.file==='string'&&!r.file.startsWith('/')&&!r.file.split('/').includes('..'),'RELATIVE_SOURCE_FILE_REQUIRED');
  const file=relative+r.file,b=small(ROOT+'/'+file);must(physical(ROOT+'/'+file)===ROOT+'/'+file&&b.length===r.bytes&&sha(b)===r.sha256&&[commit,head,''].every(ref=>b.equals(git(ROOT,'show',ref+':'+file))),'COMPLETE_SOURCE_PACKET_IMAGES_REQUIRED');
 }
}
export async function run(env=process.env){
 const a=selection(env);must(physical(fileURLToPath(import.meta.url))===ROOT+'/'+OWN+'run.mjs','COMMITTED_ADAPTER_LOCATION_REQUIRED');
 must(physical(process.cwd()).startsWith('D:/Development/GitHub')&&physical(a.out)===a.out,'CONFIGURED_PHYSICAL_D_REQUIRED');
 for(const[r,n]of[[ROOT,'healthroster-install2-14102026'],[REPORT,'healthroster-v2-report-build-13102026']])must(physical(r)===r&&physical(git(r,'rev-parse','--show-toplevel').toString().trim())===r&&physical(git(r,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+n&&physical(path.resolve(r,git(r,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','PHYSICAL_GIT_REQUIRED');
 must(git(ROOT,'symbolic-ref','--short','HEAD').toString().trim()==='codex/healthroster-install2-14102026'&&git(REPORT,'rev-parse','HEAD').toString().trim()===a.source,'FIXED_BRANCH_SOURCE_REQUIRED');
 must(git(ROOT,'diff','--cached','--name-only','-z').length===0,'UNRELATED_STAGING_REFUSED');
 const head=git(ROOT,'rev-parse','HEAD').toString().trim();packet(OWN,a.ownCommit,head);packet(REC,a.recorderCommit,head);
 const source=await import(pathToFileURL(REPORT+'/'+OP+'source.mjs').href),mf=JSON.parse(small(REPORT+'/'+OP+'MANIFEST.json'));
 const bkMf=JSON.parse(small(REPORT+'/'+BK+'MANIFEST.json'));
 source.assertReviewedImage(REPORT,a.source,[OP+'MANIFEST.json',...mf.files.map(r=>OP+r.file),BK+'MANIFEST.json',...bkMf.files.map(r=>BK+r.file)]);
 const{publicPath}=await import(pathToFileURL(REPORT+'/'+CORE).href);
 const reader=()=>checkedPublicReader({root:ROOT,rootHead:git(ROOT,'rev-parse','HEAD').toString().trim(),physicalPath:physical,readSmall:small,gitBytes:git,validatePublicPath:publicPath});
 const readPublic=reader();
 const auxiliary=['S2','S3'].includes(a.stage),names=new Set([a.stage+'_BEFORE_INPUT.json',a.stage+'_BEFORE_NATIVE.json',a.stage+'_AFTER_INPUT.json',a.stage+'_AFTER_NATIVE.json',a.stage+'_RESPONSE.json',a.stage+'_CURRENT_CONSUMPTION.json',...auxiliary?[a.stage+'_AUX_BEFORE_INPUT.json',a.stage+'_AUX_BEFORE_NATIVE.json',a.stage+'_AUX_AFTER_INPUT.json',a.stage+'_AUX_AFTER_NATIVE.json']:[]]);
 const read=name=>{must(names.has(name)&&physical(a.out+'/'+name)===a.out+'/'+name,'EXACT_PUBLIC_STAGE_FILE_REQUIRED');return small(a.out+'/'+name)};
 const args={stage:a.stage,source:a.source,planFile:a.planFile,readPublic,beforeBuilt:JSON.parse(read(a.stage+'_BEFORE_INPUT.json')),beforeInputBytes:read(a.stage+'_BEFORE_INPUT.json'),consumptionBytes:read(a.stage+'_CURRENT_CONSUMPTION.json'),beforeBytes:read(a.stage+'_BEFORE_NATIVE.json'),afterBuilt:JSON.parse(read(a.stage+'_AFTER_INPUT.json')),afterBytes:read(a.stage+'_AFTER_NATIVE.json'),responseBytes:read(a.stage+'_RESPONSE.json'),...auxiliary?{auxBeforeBuilt:JSON.parse(read(a.stage+'_AUX_BEFORE_INPUT.json')),auxBeforeBytes:read(a.stage+'_AUX_BEFORE_NATIVE.json'),auxAfterBuilt:JSON.parse(read(a.stage+'_AUX_AFTER_INPUT.json')),auxAfterBytes:read(a.stage+'_AUX_AFTER_NATIVE.json')}:{},now:Date.now()};
 const result=prepareOriginalStageAfter(args),beforePlan=readPublic(a.planFile),files=result.records.map(r=>r.file);
 must(files.length===5&&new Set(files).size===5&&files.at(-1)===a.planFile,'EXACT_FIVE_PUBLIC_RECORDS_REQUIRED');
 must(git(ROOT,'rev-parse','HEAD').toString().trim()===head&&git(REPORT,'rev-parse','HEAD').toString().trim()===a.source,'SOURCE_MOVED_BEFORE_WRITES');
 for(const r of result.records){publicPath(r.file);must(physical(path.dirname(ROOT+'/'+r.file))===path.dirname(ROOT+'/'+r.file),'PHYSICAL_DESTINATION_REQUIRED');must(r.file===a.planFile?fs.readFileSync(ROOT+'/'+r.file).equals(beforePlan):!fs.existsSync(ROOT+'/'+r.file),'EXISTING_EVIDENCE_REFUSED');}
 for(const r of result.records)fs.writeFileSync(ROOT+'/'+r.file,r.bytes,{flag:r.file===a.planFile?'w':'wx'});
 git(ROOT,'add','--',...files);for(const r of result.records)must(r.bytes.equals(git(ROOT,'show',':'+r.file)),'EXACT_INDEX_REQUIRED');git(ROOT,'diff','--cached','--check');
 must(JSON.stringify(git(ROOT,'diff','--cached','--name-only','-z').toString().split('\0').filter(Boolean).sort())===JSON.stringify([...files].sort()),'EXACT_OWNED_INDEX_REQUIRED');
 git(ROOT,'commit','-m','Record actual ordinary '+a.stage+' source effects','--only','--',...files);const made=git(ROOT,'rev-parse','HEAD').toString().trim();
 // Recompute from actual unchanged bytes with the new committed reader before publication.
 validateCommittedOriginalStageAfter({...args,readPublic:reader(),now:Date.now()});
 git(ROOT,'push','origin','HEAD:refs/heads/codex/healthroster-install2-14102026');must(git(ROOT,'ls-remote','origin','refs/heads/codex/healthroster-install2-14102026').toString().split(/\s/)[0]===made&&git(REPORT,'rev-parse','HEAD').toString().trim()===a.source,'FRESH_REMOTE_FIXED_SOURCE_REQUIRED');
 return{status:'ACTUAL_'+a.stage+'_EFFECT_RECORDS_COMMITTED_ORIGINAL_AFTER_STILL_REQUIRED',commit:made,files,originalAfterProofWritten:false,completedPrefixPublished:false};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))try{console.log(JSON.stringify(await run()))}catch(e){console.log(JSON.stringify({status:'HOLD',code:/^STAGE_ADAPTER_[A-Z0-9_]+$/.test(e?.message??'')?e.message:'STAGE_ADAPTER_HELD',noAutomaticRetry:true}));process.exitCode=1}
