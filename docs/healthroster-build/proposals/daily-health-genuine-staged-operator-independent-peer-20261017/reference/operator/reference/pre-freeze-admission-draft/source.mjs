import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {ROOT,PRIVATE_STATE,check,sha} from './contract.mjs';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
export const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,stdio:['ignore','pipe','ignore'],maxBuffer:32e6});
const same=(root,file,commit)=>{const b=fs.readFileSync(root+'/'+file);check(b.equals(git(root,'show',commit+':'+file))&&b.equals(git(root,'show',':'+file)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');return b;};
export function sourcePreflight(){
 check(process.versions.node==='24.15.0'&&physical(process.cwd()).startsWith('D:/Development/GitHub'),'FIXTURE_ACTUAL_D_RUNTIME_REQUIRED');
 for(const root of [ROOT,path.resolve(here,'../../../..').replaceAll('\\','/')]){
  check(physical(root)===root&&physical(git(root,'rev-parse','--show-toplevel').toString().trim())===root,'FIXTURE_PHYSICAL_ROOT_REQUIRED');
  check(physical(path.resolve(root,git(root,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','FIXTURE_COMMON_GIT_REQUIRED');
  const dir=git(root,'rev-parse','--absolute-git-dir').toString().trim();
  check(physical(dir)==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(root),'FIXTURE_PHYSICAL_GIT_REQUIRED');
 }
 const top=git(here,'rev-parse','--show-toplevel').toString().trim(),head=git(here,'rev-parse','HEAD').toString().trim();
 const relative=path.relative(top,here).replaceAll('\\','/');check(relative==='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017','FIXTURE_SOURCE_LOCATION_REQUIRED');
 const manifest=JSON.parse(same(top,relative+'/MANIFEST.json',head));
 for(const r of manifest.files){const b=same(top,relative+'/'+r.file,head);check(b.length===r.bytes&&sha(b)===r.sha256,'FIXTURE_FROZEN_SOURCE_CHANGED');}
 const census=JSON.parse(fs.readFileSync(here+'/reference/audit/CURRENT_SOURCE_CENSUS.json'));
 const rootHead=git(ROOT,'rev-parse','HEAD').toString().trim();
 for(const r of [...census.inputs,...census.rootDefinitionReceipts]){
  const b=same(ROOT,r.file,rootHead);check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(git(ROOT,'show',census.commit+':'+r.file)),'FIXTURE_SELECTED_CURRENT_SOURCE_CHANGED');
 }
 const mappingPins=JSON.parse(fs.readFileSync(here+'/reference/mapping/SOURCE_PINS.json'));
 for(const r of mappingPins.inputs){const b=same(ROOT,r.file,rootHead);check(b.length===r.bytes&&sha(b)===r.sha256
  &&b.equals(git(ROOT,'show',mappingPins.commit+':'+r.file)),'FIXTURE_MAPPING_OWNER_CURRENT_SOURCE_CHANGED');}
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
 const front=pins.frontend;check(physical(front.root)===front.root,'FIXTURE_FRONTEND_D_REQUIRED');
 const frontBytes=fs.readFileSync(front.root+'/'+front.file);check(frontBytes.length===front.bytes&&sha(frontBytes)===front.sha256
  &&git(front.root,'rev-parse',front.commit+':'+front.file).toString().trim()===git(front.root,'rev-parse','HEAD:'+front.file).toString().trim()
  &&git(front.root,'rev-parse',':'+front.file).toString().trim()===git(front.root,'rev-parse','HEAD:'+front.file).toString().trim(),'FIXTURE_EXISTING_FRONTEND_SOURCE_CHANGED');
 for(const r of pins.localRuntime){check(physical(r.file)===r.file,'FIXTURE_D_RUNTIME_REQUIRED');const b=fs.readFileSync(r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FIXTURE_EXISTING_RUNTIME_CHANGED');}
 const observed=pins.authObservation;check(sha(same(ROOT,observed.file,rootHead))===observed.sha256,'FIXTURE_ACTUAL_AUTH_OBSERVATION_CHANGED');
 for(const r of [...pins.rootRuntimeObservations,pins.turnControl])check(sha(same(ROOT,r.file,rootHead))===r.sha256,'FIXTURE_ACTUAL_RUNTIME_OR_TURN_SOURCE_CHANGED');
 for(const r of pins.authReusableParents)check(sha(same(r.root,r.file,git(r.root,'rev-parse','HEAD').toString().trim()))===r.sha256
  &&sha(git(r.root,'show',r.commit+':'+r.file))===r.sha256,'FIXTURE_ORIGINAL_AUTH_COMPONENT_CHANGED');
 // Metadata only. The browser library loads this exact state inside the isolated child.
 const s=fs.lstatSync(PRIVATE_STATE);check(s.isFile()&&!s.isSymbolicLink(),'FIXTURE_APPROVED_STATE_REQUIRED');
 check(physical(path.dirname(PRIVATE_STATE))===path.dirname(PRIVATE_STATE),'FIXTURE_APPROVED_STATE_PHYSICAL_REQUIRED');
 git(ROOT,'check-ignore','--quiet','--',path.relative(ROOT,PRIVATE_STATE));
 return {operatorCommit:head,rootCommit:rootHead};
}
export function committedRootFile(file){
 check(typeof file==='string'&&/^docs\/healthroster-build\/[A-Za-z0-9_./-]+$/.test(file)&&!file.split('/').includes('..'),'FIXTURE_ROOT_EVIDENCE_PATH_REQUIRED');
 return same(ROOT,file,git(ROOT,'rev-parse','HEAD').toString().trim());
}
export function ownedDirectory(value){
 check(typeof value==='string'&&path.isAbsolute(value),'FIXTURE_OWNED_PATH_REQUIRED');const p=path.resolve(value).replaceAll('\\','/');
 check(path.posix.dirname(p)===ROOT+'/.codex-tmp'&&/^daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(path.posix.basename(p)),'FIXTURE_OWNED_PATH_REQUIRED');
 check(physical(path.dirname(p))===ROOT+'/.codex-tmp','FIXTURE_PRIVATE_PARENT_REQUIRED');return p;
}
