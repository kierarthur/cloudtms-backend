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
 for(const r of mappingPins.inputs)selected(ROOT,r.file,rootHead,mappingPins.commit);
 for(const r of [pins.authObservation,...pins.rootRuntimeObservations,pins.turnControl])selected(ROOT,r.file,rootHead,r.commit);
 for(const r of pins.authReusableParents){if(!heads.has(r.root))heads.set(r.root,git(r.root,'rev-parse','HEAD').toString().trim());selected(r.root,r.file,heads.get(r.root),r.commit);selected(top,r.file,head);}
 const batches=new Map([...requests].map(([root,rows])=>[root,blobImages(root,rows)]));
 const same=(root,file,commit)=>{const b=fs.readFileSync(root+'/'+file),images=batches.get(root);check(b.equals(images.get(commit,file))&&b.equals(images.get(null,file)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');return b;};
 for(const r of manifest.files){const b=same(top,relative+'/'+r.file,head);check(b.length===r.bytes&&sha(b)===r.sha256,'FIXTURE_FROZEN_SOURCE_CHANGED');}
 for(const r of [...census.inputs,...census.rootDefinitionReceipts]){const b=same(ROOT,r.file,rootHead);check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(batches.get(ROOT).get(census.commit,r.file)),'FIXTURE_SELECTED_CURRENT_SOURCE_CHANGED');}
 for(const r of mappingPins.inputs){const b=same(ROOT,r.file,rootHead);check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(batches.get(ROOT).get(mappingPins.commit,r.file)),'FIXTURE_MAPPING_OWNER_CURRENT_SOURCE_CHANGED');}
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
