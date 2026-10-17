import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {sha,check,ROOT} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';import {blobImages,physical,git} from '../daily-health-genuine-staged-preflight-batch-forward-20261017/git-image-batch.mjs';
export const here=path.dirname(fileURLToPath(import.meta.url)),report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
export function source(){
 check(process.versions.node==='24.15.0'&&physical(report)===report&&physical(ROOT)===ROOT&&physical(process.cwd()).startsWith('D:/Development/GitHub'),'S0_D_SOURCE_REQUIRED');
 const metadata=new Map();
 for(const r of [ROOT,report]){const fields=git(r,'rev-parse','--show-toplevel','--absolute-git-dir','--git-common-dir','HEAD').toString().trim().split(/\r?\n/);check(fields.length===4&&physical(fields[0])===r&&physical(fields[1])==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(r),'S0_PHYSICAL_REPOSITORY_REQUIRED');check(physical(path.resolve(r,fields[2]))==='D:/Development/GitHub/cloudtms-backend/.git','S0_COMMON_GIT_REQUIRED');metadata.set(r,{head:fields[3]});}
 check(path.relative(report,here).replaceAll('\\','/')==='docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-20261017','S0_REGISTERED_REPORT_REQUIRED');
 const rows=JSON.parse(fs.readFileSync(here+'/MANIFEST.json')).files,head=metadata.get(report).head,relative=path.relative(report,here).replaceAll('\\','/');
 const files=[relative+'/MANIFEST.json',...rows.map(r=>relative+'/'+r.file)],images=blobImages(report,files.flatMap(file=>[head,null].map(ref=>({ref,file}))));
 for(const file of files){const b=fs.readFileSync(report+'/'+file);check(b.equals(images.get(head,file))&&b.equals(images.get(null,file)),'S0_HOLDER_COMMITTED_SOURCE_REQUIRED');}
 for(const r of rows){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'S0_HOLDER_SOURCE_CHANGED');}
 const external=JSON.parse(fs.readFileSync(here+'/EXTERNAL_SOURCE_PINS.json')),externalImages=blobImages(report,external.flatMap(r=>[head,r.commit,null].map(ref=>({ref,file:r.file}))));
 for(const r of external){const b=fs.readFileSync(report+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256&&[head,r.commit,null].every(ref=>b.equals(externalImages.get(ref,r.file))),'S0_EXTERNAL_SOURCE_CHANGED');}
 const rootPins=JSON.parse(fs.readFileSync(here+'/ROOT_SOURCE_PINS.json')),groups=new Map();
 for(const r of rootPins){if(!groups.has(r.root))groups.set(r.root,{head:metadata.get(r.root)?.head??git(r.root,'rev-parse','HEAD').toString().trim(),rows:[]});groups.get(r.root).rows.push(r);}
 for(const [root,group] of groups){const h=group.head,i=blobImages(root,group.rows.flatMap(r=>[h,r.commit,null].map(ref=>({ref,file:r.file}))));for(const r of group.rows){const b=fs.readFileSync(r.root+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256&&[h,r.commit,null].every(ref=>b.equals(i.get(ref,r.file))),'S0_ORIGINAL_SHARED_OWNER_CHANGED');}}
 const file='docs/healthroster-build/proposals/daily-health-genuine-staged-preflight-batch-forward-20261017/s0-scope.mjs',expected='8d13d7035db66a125b9b78cf4f96fc70b80b3ecc',b=fs.readFileSync(report+'/'+file),i=blobImages(report,[head,expected,null].map(ref=>({ref,file})));
 check([head,expected,null].every(ref=>b.equals(i.get(ref,file))),'S0_EXACT_CATALOGUE_PROGRAMME_CHANGED');
 check(git(report,'rev-parse','HEAD').toString().trim()===head,'S0_REPORT_HEAD_CHANGED_DURING_CHECK');
 return {holderSourceCommit:head};
}
// Exact original protected-run read/filter with only a bounded synchronous CLI timeout.
export function protectedRelease(){
 const runs=JSON.parse(execFileSync('C:/Program Files/GitHub CLI/gh.exe',['run','list','--repo','kierarthur/cloudtms-backend','--workflow','340102476','--limit','50','--json','databaseId,status'],{encoding:'utf8',windowsHide:true,timeout:15000}));
 if(runs.some(x=>x.status!=='completed'))throw Error('PROTECTED_RELEASE_RUNNING_HOLD');
}
