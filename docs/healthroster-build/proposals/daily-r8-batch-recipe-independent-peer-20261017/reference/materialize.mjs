// Root-only finite public source materializer. No runtime mount, sourcePreflight, state or browser access.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {ROOT,baseCommit,receiptFile,operator,holder,coordinator,hash,json,validateBinding} from './recipe.mjs';
export const report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
export function assertActualMount(commit,targets,{read=file=>fs.readFileSync(report+'/'+file),run=(...args)=>git(report,...args)}={}){
 if(!/^[0-9a-f]{40}$/.test(commit??''))throw Error('R8_ACTUAL_COMMITTED_MOUNT_REQUIRED');const head=run('rev-parse','HEAD').toString().trim();run('merge-base','--is-ancestor',commit,head);
 for(const t of targets){const b=read(t.target),expected=Buffer.from(t.text);if(!b.equals(expected)||hash(b)!==t.afterSha256||![commit,head,''].every(ref=>b.equals(run('show',ref+':'+t.target))))throw Error('R8_ACTUAL_MOUNT_IMAGE_REQUIRED');}
 if(run('rev-parse','HEAD').toString().trim()!==head)throw Error('R8_REPORT_HEAD_MOVED');return head;
}
export function parse(flags){
 const [phase,rc,receiptCommit,rs,receiptSha256,...rest]=flags;
 if(!['operator','holder','coordinator'].includes(phase)||rc!=='--receipt-commit'||rs!=='--receipt-sha256'||!/^[0-9a-f]{40}$/.test(receiptCommit??'')||!/^[0-9a-f]{64}$/.test(receiptSha256??''))throw Error('R8_FIXED_ARGUMENTS_REQUIRED');
 const result={phase,receiptCommit,receiptSha256};
 if(phase==='operator'&&rest.length===0)return result;
 if(rest[0]==='--operator-commit'&&/^[0-9a-f]{40}$/.test(rest[1]??'')){
  result.operatorCommit=rest[1];if(phase==='holder'&&rest.length===2)return result;
  if(phase==='coordinator'&&rest.length===4&&rest[2]==='--holder-commit'&&/^[0-9a-f]{40}$/.test(rest[3]??''))return {...result,holderCommit:rest[3]};
 }throw Error('R8_FIXED_ARGUMENTS_REQUIRED');
}
export function readPublicBinding(selected,{read=file=>fs.readFileSync(ROOT+'/'+file),run=(...args)=>git(ROOT,...args)}={}){
 const head=run('rev-parse','HEAD').toString().trim();run('merge-base','--is-ancestor',selected.receiptCommit,head);
 const b=read(receiptFile);if(b.length>32768||hash(b)!==selected.receiptSha256||![selected.receiptCommit,head,''].every(ref=>b.equals(run('show',ref+':'+receiptFile))))throw Error('R8_GENUINE_COMMITTED_PUBLIC_RECEIPT_REQUIRED');
 if(run('rev-parse','HEAD').toString().trim()!==head)throw Error('R8_RECEIPT_HEAD_MOVED');
 const binding={observation:{commit:selected.receiptCommit,file:receiptFile,sha256:selected.receiptSha256},publicReceipt:b.toString('utf8')};validateBinding(binding);return binding;
}
export function materialize(flags){
 const selected=parse(flags);
 for(const root of [report,ROOT])if(fs.realpathSync.native(root).replaceAll('\\','/')!==root||git(root,'rev-parse','--show-toplevel').toString().trim().replaceAll('\\','/')!==root)throw Error('R8_REGISTERED_D_ROOT_REQUIRED');
 const head=git(report,'rev-parse','HEAD').toString().trim();git(report,'merge-base','--is-ancestor',baseCommit,head);
 const binding=readPublicBinding(selected),op=operator(binding);let built;
 if(selected.phase==='operator')built=op;
 else{assertActualMount(selected.operatorCommit,op.targets);if(selected.phase==='holder')built=holder(binding,selected.operatorCommit);else{const held=holder(binding,selected.operatorCommit);assertActualMount(selected.holderCommit,held.targets);built=coordinator(binding,selected.operatorCommit,selected.holderCommit);}}
 // Every destination is the complete genuine 4d5873 parent: saved/index/current HEAD/historical raw equality.
 for(const t of built.targets){const b=fs.readFileSync(report+'/'+t.target);if(hash(b)!==t.beforeSha256||![baseCommit,head,''].every(ref=>b.equals(git(report,'show',ref+':'+t.target))))throw Error('R8_FOLLOW_ON_PARENT_CHANGED');}
 if(git(report,'rev-parse','HEAD').toString().trim()!==head)throw Error('R8_REPORT_HEAD_MOVED');
 const parent=report+'/.codex-tmp';if(fs.realpathSync.native(parent).replaceAll('\\','/')!==parent)throw Error('R8_D_OUTPUT_PARENT_REQUIRED');
 const out=parent+'/daily-auth-r8-batch-source-recipe-'+crypto.randomBytes(16).toString('hex');fs.mkdirSync(out);
 const record={...built,targets:built.targets.map(({text,...t})=>t)};
 for(const t of built.targets)fs.writeFileSync(out+'/'+t.file,t.text,{flag:'wx'});fs.writeFileSync(out+'/FULL_PARENT_FORWARD.json',json(record),{flag:'wx'});fs.writeFileSync(out+'/PUBLIC_BINDING.json',json(binding),{flag:'wx'});
 console.log(JSON.stringify({status:'R8_REVIEWED_IMAGES_MATERIALIZED_NOT_MOUNTED',phase:selected.phase,ownedDirectory:out,targets:built.targets.length,recordSha256:hash(json(record)),native:'NOT_RUN',privateStateAccess:'NONE'}));return record;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){try{materialize(process.argv.slice(2));}catch{console.log(JSON.stringify({status:'HOLD',code:'R8_ACTUAL_PUBLIC_RECEIPT_OR_COMMITTED_IMAGE_HELD'}));process.exitCode=1;}}
