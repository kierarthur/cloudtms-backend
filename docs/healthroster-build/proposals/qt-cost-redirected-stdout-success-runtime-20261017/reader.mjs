// Fixed public source/progress reader. No child/holder/provider execution and no private file.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import {fileURLToPath}from'node:url';import {createHash}from'node:crypto';import {readFixedProgress}from'./progress-file.mjs';
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',REL='docs/healthroster-build/proposals/qt-cost-redirected-stdout-success-runtime-20261017',COST='docs/healthroster-build/proposals/qt-audit-readonly-cost1000-success-runtime-20261017',COST_BASE='8aa1fa74666a743317cf31c0ef9e2a1aab5985cd';
const check=(v,c)=>{if(!v)throw Error(c);},slash=s=>s.replaceAll('\\','/'),sha=b=>createHash('sha256').update(b).digest('hex');
const git=(...args)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT,...args],{windowsHide:true,maxBuffer:4*1024*1024,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
function source(commit){
 check(/^[a-f0-9]{40}$/.test(commit??''),'COST_BRIDGE_REAL_SOURCE_REQUIRED');
 check(slash(fs.realpathSync.native(ROOT))===ROOT&&slash(fs.realpathSync.native(path.dirname(fileURLToPath(import.meta.url))))===ROOT+'/'+REL,'COST_BRIDGE_FIXED_PHYSICAL_SOURCE_REQUIRED');
 check(git('rev-parse','--show-toplevel').toString().trim()===ROOT,'COST_BRIDGE_ROOT_REQUIRED');
 const gd=git('rev-parse','--absolute-git-dir').toString().trim(),gc=git('rev-parse','--path-format=absolute','--git-common-dir').toString().trim();
 check(slash(fs.realpathSync.native(gd)).startsWith('D:/Development/GitHub/cloudtms-backend/.git/worktrees/')&&slash(fs.realpathSync.native(gc))==='D:/Development/GitHub/cloudtms-backend/.git','COST_BRIDGE_PHYSICAL_GIT_REQUIRED');
 const head=git('rev-parse','HEAD').toString().trim();check(git('cat-file','-t',commit).toString().trim()==='commit','COST_BRIDGE_COMMIT_REQUIRED');git('merge-base','--is-ancestor',commit,head);
 git('merge-base','--is-ancestor',COST_BASE,commit);
 const checkFile=(rel,expected,oldRef)=>{const file=ROOT+'/'+rel,s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink()&&slash(fs.realpathSync.native(file))===file,'COST_BRIDGE_PUBLIC_SOURCE_FILE_REQUIRED');const b=fs.readFileSync(file);if(expected)check(sha(b)===expected,'COST_BRIDGE_ACCEPTED_SOURCE_CHANGED');for(const ref of[head,commit,'',...oldRef?[oldRef]:[]])check(b.equals(git('show',ref+':'+rel)),'COST_BRIDGE_SAVED_INDEX_HEAD_HISTORY_REQUIRED');return b;};
 const mf=JSON.parse(checkFile(REL+'/MANIFEST.json'));check(Array.isArray(mf.files)&&mf.files.length>0,'COST_BRIDGE_SOURCE_MANIFEST_REQUIRED');const names=new Set();
 for(const r of mf.files){check(typeof r.file==='string'&&!r.file.startsWith('/')&&!r.file.split('/').some(p=>p==='..'||p==='')&&!r.file.includes('\\')&&!names.has(r.file)&&Number.isSafeInteger(r.bytes)&&r.bytes>=0&&/^[a-f0-9]{64}$/.test(r.sha256),'COST_BRIDGE_SOURCE_MANIFEST_REQUIRED');names.add(r.file);const b=checkFile(REL+'/'+r.file,r.sha256);check(b.length===r.bytes,'COST_BRIDGE_SOURCE_MANIFEST_REQUIRED');}
 check(names.has('reader.mjs')&&names.has('progress-file.mjs')&&names.has('bridge.mjs'),'COST_BRIDGE_COMPLETE_SOURCE_REQUIRED');
 checkFile(COST+'/MANIFEST.json','b0ad2394b22e7536ee7a0469b0472a2e968d14beff530d9c1c6bb31dcedae339',COST_BASE);checkFile(COST+'/root-tools.mjs','d9c952190c7282da21b7d1570b2e1d353b3a338e31ad2b6e8732b2d94d83e24f',COST_BASE);
 check(git('cat-file','-t','18b0fc1679314576840030217512483e402a0a62').toString().trim()==='commit','COST_BRIDGE_COMMIT_REQUIRED');git('merge-base','--is-ancestor','18b0fc1679314576840030217512483e402a0a62',commit);const receipt=checkFile('docs/healthroster-build/v6-owner-delivery-20261016/QT_COST1000_ACTUAL_SCAN_PROJECT_HEADER_HOLD_20261017.json','843c6f38d289bc86617a29efa9bbc5d3442a8b34845d0b1bf217612fcdeac054','18b0fc1679314576840030217512483e402a0a62');check(receipt.length===32625,'COST_BRIDGE_COMMITTED_RECEIPT_REQUIRED');
 check(git('rev-parse','HEAD').toString().trim()===head,'COST_BRIDGE_SOURCE_HEAD_MOVED');
 return head;
}
export function operation(args){
 check(args.length===5,'COST_BRIDGE_FIXED_ARGUMENTS_REQUIRED');const[nonce,commit,offset,prefixSha256,fileId]=args;
 check(/^(0|[1-9][0-9]{0,4})$/.test(offset),'COST_BRIDGE_CURSOR_REQUIRED');const head=source(commit);
 const result=readFixedProgress(ROOT+'/.codex-tmp',nonce,{offset:Number(offset),prefixSha256,fileId});
 check(git('rev-parse','HEAD').toString().trim()===head,'COST_BRIDGE_SOURCE_HEAD_MOVED');return result;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))try{console.log(JSON.stringify(operation(process.argv.slice(2))));}catch(e){console.log(JSON.stringify({status:'HOLD',code:/^COST_BRIDGE_[A-Z0-9_]+$/.test(e?.message??'')?e.message:'COST_BRIDGE_PUBLIC_READ_HELD'}));process.exitCode=1;}
