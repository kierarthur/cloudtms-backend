// Exact real Git blob transport, public source only. No virtual refs or cross-invocation cache.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
const fail=code=>{throw Error(code);},require=(v,code)=>{if(!v)fail(code);};
export const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
export function assertExecutionContext(operatorRoot,dataRoot){
 require(operatorRoot==='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026'
  &&dataRoot==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'&&operatorRoot!==dataRoot,'FIXTURE_REGISTERED_REPORT_EXECUTION_REQUIRED');
}
export const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,stdio:['ignore','pipe','ignore'],maxBuffer:32e6,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
const validPath=p=>typeof p==='string'&&/^[A-Za-z0-9_./-]+$/.test(p)&&!p.startsWith('/')&&!p.split('/').includes('..');
export function blobImages(root,requests){
 require(requests.length>0&&requests.length<=4096,'FIXTURE_BATCH_REQUEST_BOUND_REQUIRED');
 const expressions=[...new Set(requests.map(({ref,file})=>{require((ref===null||/^[0-9a-f]{40}$/.test(ref))&&validPath(file),'FIXTURE_BATCH_EXACT_IMAGE_REQUIRED');return (ref??'')+':'+file;}))];
 const output=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'cat-file','--batch'],{
  windowsHide:true,input:Buffer.from(expressions.join('\n')+'\n'),stdio:['pipe','pipe','ignore'],maxBuffer:128e6,
  env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
 let at=0;const images=new Map();
 for(const expression of expressions){
  const end=output.indexOf(10,at);require(end>at,'FIXTURE_BATCH_HEADER_REQUIRED');
  const header=output.subarray(at,end).toString('ascii'),match=/^([a-f0-9]{40}) blob (0|[1-9][0-9]*)$/.exec(header);
  require(match,'FIXTURE_BATCH_REAL_BLOB_REQUIRED');const size=Number(match[2]);
  require(Number.isSafeInteger(size)&&size<=32e6&&end+1+size<output.length&&output[end+1+size]===10,'FIXTURE_BATCH_COMPLETE_BLOB_REQUIRED');
  images.set(expression,output.subarray(end+1,end+1+size));at=end+size+2;
 }
 require(at===output.length,'FIXTURE_BATCH_NO_EXTRA_OUTPUT_REQUIRED');
 return {get(ref,file){const b=images.get((ref??'')+':'+file);require(b!==undefined,'FIXTURE_BATCH_IMAGE_NOT_SELECTED');return b;}};
}
export function reviewedImage(root,reviewedCommit,files){
 require(/^[0-9a-f]{40}$/.test(reviewedCommit)&&Array.isArray(files)&&files.length>0&&new Set(files).size===files.length,'FIXTURE_REVIEWED_SOURCE_IMAGE_REQUIRED');
 const head=git(root,'rev-parse','HEAD').toString().trim();try{git(root,'merge-base','--is-ancestor',reviewedCommit,head);}catch{fail('FIXTURE_REVIEWED_SOURCE_ANCESTRY_REQUIRED');}
 for(const f of files)require(validPath(f),'FIXTURE_REVIEWED_SOURCE_PATH_REQUIRED');
 const images=blobImages(root,files.flatMap(file=>[head,reviewedCommit,null].map(ref=>({ref,file}))));
 for(const file of files){const b=fs.readFileSync(root+'/'+file);require(b.equals(images.get(head,file))&&b.equals(images.get(null,file)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');require(b.equals(images.get(reviewedCommit,file)),'FIXTURE_REVIEWED_SOURCE_IMAGE_CHANGED');}
 require(git(root,'rev-parse','HEAD').toString().trim()===head,'FIXTURE_EXECUTION_HEAD_MOVED');
 return {reviewedCommit,executionCommit:head,selectedImagesUnchanged:true};
}
// One reviewed additive metadata successor; original immutable audit row/blob remains required.
export const registrySuccessor=Object.freeze({
  "file": "docs/bpay-next/final-definer-closures.json",
  "original": {
    "commit": "5b4f4e4c196871fa85722435deeee3ad418908f2",
    "bytes": 1746259,
    "sha256": "c10ac5fa3aafa9c2def791badd2b647bea2e980cd88a43af81c9a1cba7e7e00c"
  },
  "approved": {
    "commit": "69dde78cd4d26d863cb7aa1f3aeb53d61bbc088f",
    "bytes": 1753534,
    "sha256": "f357496129f0d1eab1f2a2115a8da788be9c2f4a42f50e4d41baa1c700fc3cc0"
  },
  "review": {
    "file": "docs/healthroster-build/v6-owner-delivery-20261016/HEALTH_CLOSED_PRIVATE_HELPERS_ROOT_REVIEW_20261017.json",
    "commit": "69dde78cd4d26d863cb7aa1f3aeb53d61bbc088f",
    "bytes": 6781,
    "sha256": "0658771b97e9ba5c50340980969bb3b02e3a36a655d9f0b6884e23a8bafe58f9"
  },
  "retainedChains": 1650,
  "addedSignatures": [
    "private.daily_rate_correction_health_financial_projection_assert_v1(jsonb,boolean)",
    "private.daily_rate_correction_health_invoice_effect_v1(jsonb)",
    "private.daily_rate_correction_health_actions_assert_v1(jsonb)",
    "private.daily_rate_correction_health_review_enroll_v1(jsonb)",
    "private.daily_rate_correction_health_revision_apply_v1(jsonb)",
    "private.daily_rate_correction_health_current_placement_v1(uuid,uuid)",
    "private.timesheet_contract_move_public_assert_v1(jsonb,text)",
    "private.timesheet_contract_move_actions_assert_v1(jsonb)",
    "private.timesheet_contract_move_invoice_effect_v1(jsonb,uuid)"
  ],
  "release": {
    "file": "supabase/release/current-release.json",
    "original": {
      "commit": "c276aa49eb4db86ff42e184011a09bf2b59f253e",
      "bytes": 36636,
      "sha256": "1396346c26872e318231f0815594b2efe206eac4180de7b3c9120f14be24726d"
    },
    "approved": {
      "commit": "69dde78cd4d26d863cb7aa1f3aeb53d61bbc088f",
      "bytes": 36808,
      "sha256": "b9b045c6038c46238a8a47945bb201e336ffb8dc0afdee86f9524ece9d29cbe2"
    },
    "verifier": "supabase/verification/17102026_1251_health_closed_private_helpers_security.sql"
  },
  "sourceBuffer": {
    "file": "scripts/bpay-next-final-definers.mjs",
    "original": {
      "commit": "c276aa49eb4db86ff42e184011a09bf2b59f253e",
      "bytes": 13611,
      "sha256": "51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09"
    },
    "approved": {
      "commit": "51177b9abcdc9f089e680c9e7e9ef5285f09ff57",
      "bytes": 13639,
      "sha256": "b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa"
    },
    "beforeLine": "  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });",
    "afterLine": "  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });",
    "review": {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/SHARED_FINAL_DEFINER_GIT_BUFFER_ROOT_REVIEW_20261017.json",
      "commit": "51177b9abcdc9f089e680c9e7e9ef5285f09ff57",
      "bytes": 1356,
      "sha256": "fc7a0a9492fd101f894520b0f97c91a59fc2b3ce1bbe05a7cfc1d4843761ea40"
    }
  }
});
export function assertRegistrySuccessor(row,original,current,approved,review,sha){
 const b=registrySuccessor;
 require(row.file===b.file&&row.bytes===b.original.bytes&&row.sha256===b.original.sha256
  &&original.length===b.original.bytes&&sha(original)===b.original.sha256,'FIXTURE_ORIGINAL_REGISTRY_BINDING_REQUIRED');
 require(current.length===b.approved.bytes&&sha(current)===b.approved.sha256&&current.equals(approved),'FIXTURE_EXACT_APPROVED_REGISTRY_REQUIRED');
 require(review.length===b.review.bytes&&sha(review)===b.review.sha256,'FIXTURE_EXACT_REGISTRY_REVIEW_REQUIRED');
 const old=JSON.parse(original),next=JSON.parse(current),r=JSON.parse(review);
 require(old.chains.length===b.retainedChains&&next.chains.length===b.retainedChains+b.addedSignatures.length
  &&JSON.stringify(next.chains.slice(b.retainedChains).map(v=>v.signature))===JSON.stringify(b.addedSignatures),'FIXTURE_EXACT_ADDITIVE_CHAINS_REQUIRED');
 next.chains.splice(b.retainedChains);
 // Match the genuine metadata writer's ASCII JSON escaping; compare the entire original raw image.
 const restored=JSON.stringify(next,null,2).replace(/[^\x00-\x7e]/g,c=>String.fromCharCode(92)+'u'+c.charCodeAt(0).toString(16).padStart(4,'0'))+String.fromCharCode(10);
 require(restored===original.toString('utf8'),'FIXTURE_ALL_ORIGINAL_REGISTRY_BYTES_REQUIRED');
 require(r.verdict==='ACCEPT_THIRTEEN_EXACT_SOURCE_POSTIMAGES'&&r.preservation.registryChains===1650
  &&r.rootReplays.wholeMetadataInverses===3&&r.rootReplays.registeredChains===1659
  &&r.targets.some(v=>v.target===b.file&&v.bytes===b.approved.bytes&&v.sha256===b.approved.sha256),'FIXTURE_ORIGINAL_HELPER_REVIEW_REQUIRED');
 return true;
}
export function assertReleaseSuccessor(row,original,current,approved,sha){
 const b=registrySuccessor.release;
 require(row.file===b.file&&row.bytes===b.original.bytes&&row.sha256===b.original.sha256
  &&original.length===b.original.bytes&&sha(original)===b.original.sha256,'FIXTURE_ORIGINAL_RELEASE_BINDING_REQUIRED');
 require(current.length===b.approved.bytes&&sha(current)===b.approved.sha256&&current.equals(approved),'FIXTURE_EXACT_APPROVED_RELEASE_REQUIRED');
 const next=JSON.parse(current);
 require(next.verificationFiles.length===183&&next.newVerificationFiles.length===184
  &&next.verificationFiles.pop()===b.verifier&&next.newVerificationFiles.pop()===b.verifier,'FIXTURE_EXACT_TWO_VERIFIER_ADDITIONS_REQUIRED');
 const restored=JSON.stringify(next,null,2).replace(/[^\x00-\x7e]/g,c=>String.fromCharCode(92)+'u'+c.charCodeAt(0).toString(16).padStart(4,'0'))+String.fromCharCode(10);
 require(restored===original.toString('utf8'),'FIXTURE_ALL_ORIGINAL_RELEASE_BYTES_REQUIRED');return true;
}
export function assertBufferSourceSuccessor(row,original,current,approved,review,sha){
 const b=registrySuccessor.sourceBuffer;
 require(row.file===b.file&&row.bytes===b.original.bytes&&row.sha256===b.original.sha256
  &&original.length===b.original.bytes&&sha(original)===b.original.sha256,'FIXTURE_ORIGINAL_BUFFER_SOURCE_REQUIRED');
 require(current.length===b.approved.bytes&&sha(current)===b.approved.sha256&&current.equals(approved),'FIXTURE_EXACT_APPROVED_BUFFER_SOURCE_REQUIRED');
 require(review.length===b.review.bytes&&sha(review)===b.review.sha256,'FIXTURE_EXACT_BUFFER_REVIEW_REQUIRED');
 const text=current.toString('utf8'),r=JSON.parse(review);
 require(text.split(b.afterLine).length===2&&text.replace(b.afterLine,b.beforeLine)===original.toString('utf8'),'FIXTURE_WHOLE_ORIGINAL_BUFFER_SOURCE_REQUIRED');
 require(r.verdict==='ACCEPT_ONE_LINE_COMPLETE_PATH_BUFFER_FORWARD'&&r.beforeSha256===b.original.sha256
  &&r.afterSha256===b.approved.sha256&&r.newOutputCapBytes===8388608
  &&r.allOriginalPathInputsBaseAncestryAssertionsPreserved===true
  &&r.libSourceGatesRegistrySqlNativeCapsPermissionsUnchanged===true,'FIXTURE_ORIGINAL_BUFFER_REVIEW_REQUIRED');return true;
}
export function preflight({here,ROOT,PRIVATE_STATE,check,sha}){
 check(process.versions.node==='24.15.0'&&physical(process.cwd()).startsWith('D:/Development/GitHub'),'FIXTURE_ACTUAL_D_RUNTIME_REQUIRED');
 for(const root of [ROOT,path.resolve(here,'../../../..').replaceAll('\\','/')]){
  check(physical(root)===root&&physical(git(root,'rev-parse','--show-toplevel').toString().trim())===root,'FIXTURE_PHYSICAL_ROOT_REQUIRED');
  check(physical(path.resolve(root,git(root,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','FIXTURE_COMMON_GIT_REQUIRED');
  check(physical(git(root,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(root),'FIXTURE_PHYSICAL_GIT_REQUIRED');
 }
 const top=git(here,'rev-parse','--show-toplevel').toString().trim(),head=git(here,'rev-parse','HEAD').toString().trim(),rootHead=git(ROOT,'rev-parse','HEAD').toString().trim();
 assertExecutionContext(top,ROOT);
 const relative=path.relative(top,here).replaceAll('\\','/');check(relative==='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017','FIXTURE_SOURCE_LOCATION_REQUIRED');
 const manifestFile=relative+'/MANIFEST.json',initial=blobImages(top,[{ref:head,file:manifestFile},{ref:null,file:manifestFile}]),manifestBytes=fs.readFileSync(top+'/'+manifestFile);
 check(manifestBytes.equals(initial.get(head,manifestFile))&&manifestBytes.equals(initial.get(null,manifestFile)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');
 const manifest=JSON.parse(manifestBytes),census=JSON.parse(fs.readFileSync(here+'/reference/audit/CURRENT_SOURCE_CENSUS.json')),
  mappingPins=JSON.parse(fs.readFileSync(here+'/reference/mapping/SOURCE_PINS.json')),pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
 const heads=new Map([[top,head],[ROOT,rootHead]]),requests=new Map();
 const add=(root,file,ref)=>{if(!requests.has(root))requests.set(root,[]);requests.get(root).push({ref,file});};
 const selected=(root,file,current,historical)=>{add(root,file,current);add(root,file,null);if(historical)add(root,file,historical);};
 for(const r of manifest.files)selected(top,relative+'/'+r.file,head);
 for(const r of [...census.inputs,...census.rootDefinitionReceipts])selected(ROOT,r.file,rootHead,census.commit);
 selected(ROOT,registrySuccessor.file,rootHead,registrySuccessor.approved.commit);
 selected(ROOT,registrySuccessor.review.file,rootHead,registrySuccessor.review.commit);
 selected(ROOT,registrySuccessor.release.file,rootHead,registrySuccessor.release.approved.commit);
 selected(ROOT,registrySuccessor.sourceBuffer.file,rootHead,registrySuccessor.sourceBuffer.approved.commit);
 selected(ROOT,registrySuccessor.sourceBuffer.review.file,rootHead,registrySuccessor.sourceBuffer.review.commit);
 for(const r of mappingPins.inputs)selected(ROOT,r.file,rootHead,mappingPins.commit);
 for(const r of [pins.authObservation,...pins.rootRuntimeObservations,pins.turnControl])selected(ROOT,r.file,rootHead,r.commit);
 for(const r of pins.authReusableParents){if(!heads.has(r.root))heads.set(r.root,git(r.root,'rev-parse','HEAD').toString().trim());selected(r.root,r.file,heads.get(r.root),r.commit);selected(top,r.file,head);}
 const batches=new Map([...requests].map(([root,rows])=>[root,blobImages(root,rows)]));
 const same=(root,file,commit)=>{const b=fs.readFileSync(root+'/'+file),images=batches.get(root);check(b.equals(images.get(commit,file))&&b.equals(images.get(null,file)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');return b;};
 for(const r of manifest.files){const b=same(top,relative+'/'+r.file,head);check(b.length===r.bytes&&sha(b)===r.sha256,'FIXTURE_FROZEN_SOURCE_CHANGED');}
 for(const r of [...census.inputs,...census.rootDefinitionReceipts]){
  const b=same(ROOT,r.file,rootHead),original=batches.get(ROOT).get(census.commit,r.file);
  if(r.file===registrySuccessor.file){
   check(census.commit===registrySuccessor.original.commit,'FIXTURE_ORIGINAL_REGISTRY_COMMIT_REQUIRED');
   const review=same(ROOT,registrySuccessor.review.file,rootHead);
   check(review.equals(batches.get(ROOT).get(registrySuccessor.review.commit,registrySuccessor.review.file)),'FIXTURE_CURRENT_REGISTRY_REVIEW_CHANGED');
   assertRegistrySuccessor(r,original,b,batches.get(ROOT).get(registrySuccessor.approved.commit,r.file),review,sha);
  }else check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(original),'FIXTURE_SELECTED_CURRENT_SOURCE_CHANGED');
 }
 for(const r of mappingPins.inputs){
  const b=same(ROOT,r.file,rootHead),original=batches.get(ROOT).get(mappingPins.commit,r.file);
  if(r.file===registrySuccessor.release.file){
   check(mappingPins.commit===registrySuccessor.release.original.commit,'FIXTURE_ORIGINAL_MAPPING_COMMIT_REQUIRED');
   assertReleaseSuccessor(r,original,b,batches.get(ROOT).get(registrySuccessor.release.approved.commit,r.file),sha);
  }else if(r.file===registrySuccessor.sourceBuffer.file){
   check(mappingPins.commit===registrySuccessor.sourceBuffer.original.commit,'FIXTURE_ORIGINAL_MAPPING_COMMIT_REQUIRED');
   const reviewed=registrySuccessor.sourceBuffer.review,review=same(ROOT,reviewed.file,rootHead);
   check(review.equals(batches.get(ROOT).get(reviewed.commit,reviewed.file)),'FIXTURE_CURRENT_BUFFER_REVIEW_CHANGED');
   assertBufferSourceSuccessor(r,original,b,batches.get(ROOT).get(registrySuccessor.sourceBuffer.approved.commit,r.file),review,sha);
  }else check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(original),'FIXTURE_MAPPING_OWNER_CURRENT_SOURCE_CHANGED');
 }
 const front=pins.frontend;check(physical(front.root)===front.root,'FIXTURE_FRONTEND_D_REQUIRED');
 const frontBytes=fs.readFileSync(front.root+'/'+front.file);check(frontBytes.length===front.bytes&&sha(frontBytes)===front.sha256
  &&git(front.root,'rev-parse',front.commit+':'+front.file).toString().trim()===git(front.root,'rev-parse','HEAD:'+front.file).toString().trim()
  &&git(front.root,'rev-parse',':'+front.file).toString().trim()===git(front.root,'rev-parse','HEAD:'+front.file).toString().trim(),'FIXTURE_EXISTING_FRONTEND_SOURCE_CHANGED');
 for(const r of pins.localRuntime){check(physical(r.file)===r.file,'FIXTURE_D_RUNTIME_REQUIRED');const b=fs.readFileSync(r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FIXTURE_EXISTING_RUNTIME_CHANGED');}
 const observed=pins.authObservation;check(sha(same(ROOT,observed.file,rootHead))===observed.sha256&&sha(batches.get(ROOT).get(observed.commit,observed.file))===observed.sha256,'FIXTURE_ACTUAL_AUTH_OBSERVATION_CHANGED');
 for(const r of [...pins.rootRuntimeObservations,pins.turnControl])check(sha(same(ROOT,r.file,rootHead))===r.sha256&&sha(batches.get(ROOT).get(r.commit,r.file))===r.sha256,'FIXTURE_ACTUAL_RUNTIME_OR_TURN_SOURCE_CHANGED');
 for(const r of pins.authReusableParents)check(sha(same(r.root,r.file,heads.get(r.root)))===r.sha256&&sha(batches.get(r.root).get(r.commit,r.file))===r.sha256&&sha(same(top,r.file,head))===r.sha256,'FIXTURE_ORIGINAL_AUTH_COMPONENT_CHANGED');
 const s=fs.lstatSync(PRIVATE_STATE);check(s.isFile()&&!s.isSymbolicLink(),'FIXTURE_APPROVED_STATE_REQUIRED');
 check(physical(path.dirname(PRIVATE_STATE))===path.dirname(PRIVATE_STATE),'FIXTURE_APPROVED_STATE_PHYSICAL_REQUIRED');git(ROOT,'check-ignore','--quiet','--',path.relative(ROOT,PRIVATE_STATE));
 for(const [root,expected] of heads)check(git(root,'rev-parse','HEAD').toString().trim()===expected,'FIXTURE_EXECUTION_HEAD_MOVED');
 return {operatorCommit:head,rootCommit:rootHead};
}
