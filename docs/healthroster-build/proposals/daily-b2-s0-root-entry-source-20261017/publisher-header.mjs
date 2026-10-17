// B2 root entry source helpers. No imports create a browser, SQL, holder or IPC.
import fs from'node:fs';import cp from'node:child_process';import{pathToFileURL,fileURLToPath}from'node:url';import{createHash}from'node:crypto';import path from'node:path';
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',BASE='docs/healthroster-build/v6-owner-delivery-20261016/',PROP='docs/healthroster-build/proposals/';
export const CONFIG=BASE+'DAILY_GENUINE_B2_S0_ROOT_CONFIG_20261017.json',SOURCE_REVIEW=BASE+'DAILY_GENUINE_B2_S0_SOURCE_ADMISSION_20261017.json';
const cfgVersion='DAILY_GENUINE_B2_S0_CONFIG_V1',hash=b=>createHash('sha256').update(b).digest('hex'),check=(v,c)=>{if(!v)throw Error('B2_S0_'+c);};
const exact=(v,k)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...k].sort().join('|'),same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const metadataKeys=['functionalReport','functionalRoot','operatorMetadata','coordinatorMetadata','oneMetadata','bookkeeperMetadata','rootMetadata'];
const g=(root,...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',root,...a],{windowsHide:true,maxBuffer:32e6,stdio:['ignore','pipe','ignore']});
const physical=f=>fs.realpathSync.native(f).replaceAll('\\','/');
const entryFiles=[PROP+'daily-genuine-root-s0-b2-bookkeeper-20261017.mjs',PROP+'daily-genuine-s0-b2-tools-orchestration-20261017.js.txt',PROP+'daily-b2-s0-publish-stageplans-20261017.mjs'];
function shape(c,preparation=false){
 check(exact(c,['version','operatorCommit','metadataCommits','entrypointCommit','recipeCommit','authReviewCommit',...preparation?[]:['sourceReviewCommit']])&&c.version===cfgVersion,'CLOSED_REAL_CONFIG_REQUIRED');
 check(exact(c.metadataCommits,metadataKeys)&&[c.operatorCommit,c.entrypointCommit,c.recipeCommit,c.authReviewCommit,...preparation?[]:[c.sourceReviewCommit],...Object.values(c.metadataCommits)].every(v=>/^[a-f0-9]{40}$/.test(v??'')),'REAL_ACTUAL_COMMITS_REQUIRED');return c;
}
export function committedReader(root=ROOT){
 const head=g(root,'rev-parse','HEAD').toString().trim();return (file,historical=null)=>{
  check(typeof file==='string'&&/^docs\/healthroster-build\/[A-Za-z0-9_./-]+$/.test(file)&&!file.split('/').includes('..'),'PUBLIC_FILE_REQUIRED');
  const f=root+'/'+file,s=fs.lstatSync(f);check(physical(f)===f&&s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=2e6,'BOUNDED_PHYSICAL_PUBLIC_REQUIRED');
  const b=fs.readFileSync(f);if(historical){check(/^[a-f0-9]{40}$/.test(historical),'ACTUAL_HISTORY_REQUIRED');g(root,'merge-base','--is-ancestor',historical,head);}
  check([head,'',...historical?[historical]:[]].every(ref=>b.equals(g(root,'show',ref+':'+file))),'CURRENT_COMPLETE_PUBLIC_IMAGES_REQUIRED');check(g(root,'rev-parse','HEAD').toString().trim()===head,'PUBLIC_HEAD_MOVED');return b;
 };
}
function entryImages(c){
 for(const r of[ROOT,REPORT]){check(physical(r)===r&&physical(g(r,'rev-parse','--show-toplevel').toString().trim())===r,'PHYSICAL_D_REQUIRED');check(physical(g(r,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(r)&&physical(path.resolve(r,g(r,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','PHYSICAL_D_GIT_REQUIRED');}
 check(g(REPORT,'rev-parse','HEAD').toString().trim()===c.operatorCommit,'ACTUAL_REPORT_HEAD_REQUIRED');const read=committedReader();for(const file of entryFiles)read(file,c.entrypointCommit);return read;
}
export function loadConfig(env=process.env){
 const raw=committedReader()(CONFIG),c=shape(JSON.parse(raw));check(!env.HR_B2_CONFIG_SHA||env.HR_B2_CONFIG_SHA===hash(raw),'FIXED_CONFIG_CHANGED');check(!env.HR_OPERATOR_COMMIT||env.HR_OPERATOR_COMMIT===c.operatorCommit,'FIXED_OPERATOR_CHANGED');
 const read=entryImages(c),review=JSON.parse(read(SOURCE_REVIEW,c.sourceReviewCommit));
 check(review.version==='DAILY_GENUINE_B2_SOURCE_ADMISSION_V1'&&review.operatorCommit===c.operatorCommit&&same(review.metadataCommits,c.metadataCommits)&&review.entrypointCommit===c.entrypointCommit&&review.recipeCommit===c.recipeCommit&&review.authReviewCommit===c.authReviewCommit&&review.metadataProof?.status==='ACTUAL_DERIVED_METADATA_IMAGES_PASS'&&review.metadataProof.metadataRows?.length===14&&review.publicProof?.status==='ACTUAL_PUBLIC_SOURCE_ONLY_PASS'&&review.publicProof.reportHead===c.operatorCommit,'ACTUAL_COMPLETE_SOURCE_REVIEW_REQUIRED');
 read(BASE+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',c.authReviewCommit);return{config:c,configSha256:hash(raw),readPublic:read};
}
export async function admitConfiguration(c){
 // Root invokes before A to produce genuine review bytes. No config/review is
 // manufactured here: both full real-Git checkers must actually return.
 shape(c,true);const read=entryImages(c),rel=PROP+'daily-b2-real-commit-reseal-source-20261017/',mf=read(rel+'MANIFEST.json',c.recipeCommit),m=JSON.parse(mf);read(BASE+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',c.authReviewCommit);
 check(hash(mf)==='48664620d82be753cd5bc48d6fdff323e4cd080fe72ffe7a37229395cbb7c77c','REVIEWED_RECIPE_REQUIRED');
 for(const row of m.files){check(!row.file.split('/').includes('..'),'RECIPE_RELATIVE_PATH_REQUIRED');const b=read(rel+row.file,c.recipeCommit);check(b.length===row.bytes&&hash(b)===row.sha256,'COMPLETE_RECIPE_IMAGES_REQUIRED');}
 const code='import sys,json;sys.path.insert(0,sys.argv[1]);from reseal import Authority,assert_resealed;from git_values import GitValues;print(json.dumps(assert_resealed(Authority(),json.loads(sys.argv[2]),GitValues(),False)))';
 const raw=cp.execFileSync('python',['-c',code,ROOT+'/'+rel,JSON.stringify(c.metadataCommits)],{cwd:ROOT,windowsHide:true,maxBuffer:2e6,stdio:['ignore','pipe','ignore']}),metadataProof=JSON.parse(raw);
 const {assertPublicSource}=await import(pathToFileURL(ROOT+'/'+rel+'admit-public.mjs').href),publicProof=await assertPublicSource(metadataProof.publicAdmissionConfig);
 check(metadataProof.status==='ACTUAL_DERIVED_METADATA_IMAGES_PASS'&&metadataProof.metadataRows.length===14&&publicProof.status==='ACTUAL_PUBLIC_SOURCE_ONLY_PASS'&&metadataProof.reportHead===c.operatorCommit&&publicProof.reportHead===c.operatorCommit&&publicProof.rootHead===metadataProof.rootHead,'ACTUAL_METADATA_AND_SOURCE_REQUIRED');
 return{version:'DAILY_GENUINE_B2_SOURCE_ADMISSION_V1',operatorCommit:c.operatorCommit,metadataCommits:c.metadataCommits,entrypointCommit:c.entrypointCommit,recipeCommit:c.recipeCommit,authReviewCommit:c.authReviewCommit,metadataProof,publicProof,native:'NOT_RUN',authRefresh:'NOT_RUN'};
}
// Filled with exact public predecision/evidence parent metadata by source build.
const approvedReviewParents=__APPROVED_REVIEW_PARENTS__;
export function preparePredecisions({source,readPublic}){
 check(/^[a-f0-9]{40}$/.test(source??'')&&typeof readPublic==='function','ACTUAL_SOURCE_READER_REQUIRED');const records=[];
 for(const r of approvedReviewParents){const b=readPublic(r.file,r.commit);check(b instanceof Uint8Array&&b.length===r.bytes&&hash(b)===r.sha256,'UNCHANGED_SUBSTANTIVE_REVIEW_REQUIRED');}
 for(const stage of['S1','S1a','S2','S3','S4']){
  const file=BASE+'DAILY_GENUINE_'+stage+'_R10_PREDECISION_20261017.json',r=approvedReviewParents.find(v=>v.file===file),raw=readPublic(file,r.commit),old=JSON.parse(raw);
  check(old.operatorSourceCommit==='6b5ebedecf95c8a1adfda5141bbf3eccf5294d23'&&old.stage===stage&&old.decision==='ROOT_REVIEWED_EXACT_EXISTING_STAGE_EFFECTS','ORIGINAL_MANUAL_PREDECISION_REQUIRED');
  const before='"operatorSourceCommit": "'+old.operatorSourceCommit+'"',after='"operatorSourceCommit": "'+source+'"',text=raw.toString();check(text.split(before).length===2,'EXACT_SOURCE_HUNK_REQUIRED');const next=text.replace(before,()=>after);check(next.replace(after,()=>before)===text,'WHOLE_PREDECISION_INVERSE_REQUIRED');
  records.push({file:BASE+'DAILY_GENUINE_'+stage+'_B2_PREDECISION_20261017.json',bytes:Buffer.from(next),wholeInverse:{before:text,after:next},originalReviewCommit:r.commit,originalReviewSha256:r.sha256});
 }
 return{records,source,actualStagePlansCreated:false,newEffectPermission:false,onlyExistingApprovedSourceRebound:true};
}
export async function prepareConfiguredPredecisions(env=process.env){
 const selected=loadConfig(env),{sourceReviewCommit,...preparation}=selected.config;await admitConfiguration(preparation);
 return preparePredecisions({source:selected.config.operatorCommit,readPublic:selected.readPublic});
}
export function assertPreparedPublicInputs(selected){
 const expected=preparePredecisions({source:selected.config.operatorCommit,readPublic:selected.readPublic});
 for(const r of expected.records)check(r.bytes.equals(selected.readPublic(r.file)),'COMMITTED_EXACT_B2_PREDECISIONS_REQUIRED');
 const plan=JSON.parse(selected.readPublic(BASE+'DAILY_GENUINE_S0_B2_ROOT_EXECUTION_PLAN_20261017.json'));
 check(plan.version==='DAILY_GENUINE_S0_B2_ROOT_EXECUTION_PLAN_V1'&&plan.operatorSourceCommit===selected.config.operatorCommit&&plan.rootMetadataCommit===selected.config.metadataCommits.rootMetadata,'ACTUAL_SOURCE_BOUND_S0_PLAN_REQUIRED');return 5;
}
