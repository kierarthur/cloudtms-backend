// Root-only committed source gate; never reads private auth state or invokes sourcePreflight.
import fs from'node:fs';import path from'node:path';import cp from'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';import{check,sha}from'./factory.mjs';
import{blobImages}from'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-preflight-batch-forward-20261017/git-image-batch.mjs';
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',REL='docs/healthroster-build/proposals/qt-audit-readonly-cost1000-20261017',here=path.dirname(fileURLToPath(import.meta.url));
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/'),git=(root,...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore']});
export function source(){
 check(process.versions.node==='24.15.0'&&physical(ROOT)===ROOT&&physical(REPORT)===REPORT&&physical(process.cwd()).startsWith('D:/Development/GitHub/')&&physical(here)===ROOT+'/'+REL,'COST_REAL_REGISTERED_EXECUTION_REQUIRED');
 const reviewed=process.env.HR_QT_COST_SOURCE_COMMIT;check(/^[a-f0-9]{40}$/.test(reviewed??''),'COST_ACTUAL_REVIEWED_COMMIT_REQUIRED');
 const rootHead=git(ROOT,'rev-parse','HEAD').toString().trim(),reportHead=git(REPORT,'rev-parse','HEAD').toString().trim();
 git(REPORT,'merge-base','--is-ancestor','6b5ebedecf95c8a1adfda5141bbf3eccf5294d23',reportHead);git(ROOT,'merge-base','--is-ancestor',reviewed,rootHead);
 const metadata=repo=>git(repo,'rev-parse','--show-toplevel','--absolute-git-dir','--git-common-dir').toString().trim().split(/\r?\n/);
 for(const repo of[ROOT,REPORT]){const a=metadata(repo);check(a.length===3&&physical(a[0])===repo&&physical(a[1])==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(repo)&&physical(path.resolve(repo,a[2]))==='D:/Development/GitHub/cloudtms-backend/.git','COST_PHYSICAL_GIT_REQUIRED');}
 const mf=fs.readFileSync(here+'/MANIFEST.json'),manifest=JSON.parse(mf),rows=[...manifest.files,{file:'MANIFEST.json',bytes:mf.length,sha256:sha(mf)}];
 const selected=blobImages(ROOT,rows.flatMap(r=>[rootHead,reviewed,null].map(ref=>({ref,file:REL+'/'+r.file}))));
 for(const r of rows){
  check(/^[A-Za-z0-9_./-]+$/.test(r.file)&&!r.file.split('/').includes('..'),'COST_EXACT_SOURCE_FILE_REQUIRED');const file=REL+'/'+r.file,b=fs.readFileSync(ROOT+'/'+file);check(b.length===r.bytes&&sha(b)===r.sha256&&[rootHead,reviewed,null].every(ref=>b.equals(selected.get(ref,file))),'COST_EXACT_COMMITTED_SOURCE_REQUIRED');
 }
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')),groups=new Map();
 for(const r of pins){check([ROOT,REPORT].includes(r.root),'COST_PINNED_REGISTERED_ROOT_REQUIRED');if(!groups.has(r.root))groups.set(r.root,{head:r.root===ROOT?rootHead:reportHead,rows:[]});groups.get(r.root).rows.push(r);}
 for(const [root,g] of groups){const i=blobImages(root,g.rows.flatMap(r=>[g.head,r.commit,null].map(ref=>({ref,file:r.file}))));for(const r of g.rows){const b=fs.readFileSync(root+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256&&[g.head,r.commit,null].every(ref=>b.equals(i.get(ref,r.file))),'COST_ORIGINAL_OWNER_CHANGED');}}
 check(git(ROOT,'rev-parse','HEAD').toString().trim()===rootHead&&git(REPORT,'rev-parse','HEAD').toString().trim()===reportHead,'COST_SOURCE_HEAD_MOVED');
 return{rootSourceCommit:reviewed,rootExecutionCommit:rootHead,reportSourceCommit:reportHead};
}
export async function originalHolderSource(){const original=await import(pathToFileURL(REPORT+'/docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-20261017/source.mjs').href);original.source();source();return original;}
export async function protectedRelease(){const original=await originalHolderSource();original.protectedRelease();}
