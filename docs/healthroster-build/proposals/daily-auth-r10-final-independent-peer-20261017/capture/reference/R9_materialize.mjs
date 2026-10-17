// Root-only public R9 source images. No auth, private target, preflight, browser or native.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {ROOT,report,baseCommit,receiptFile,operator,holder,coordinator,continuation,hash,json,validateBinding,exactCommit} from './recipe.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),relative='docs/healthroster-build/proposals/daily-health-genuine-auth-r9-source-recipe-20261017/';
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
export function assertActualMount(commit,targets,{read=file=>fs.readFileSync(report+'/'+file),run=(...args)=>git(report,...args)}={}){
 if(!/^[0-9a-f]{40}$/.test(commit??''))throw Error('R9_ACTUAL_COMMITTED_MOUNT_REQUIRED');const head=run('rev-parse','HEAD').toString().trim();run('merge-base','--is-ancestor',commit,head);
 for(const t of targets){const b=read(t.target),expected=Buffer.from(t.text);if(!b.equals(expected)||hash(b)!==t.afterSha256||![commit,head,''].every(ref=>b.equals(run('show',ref+':'+t.target))))throw Error('R9_ACTUAL_MOUNT_IMAGE_REQUIRED');}
 if(run('rev-parse','HEAD').toString().trim()!==head)throw Error('R9_REPORT_HEAD_MOVED');return head;
}
export function parse(flags){
 const [phase,p,proposalCommit,rc,receiptCommit,rs,receiptSha256,...rest]=flags;
 if(!['operator','holder','coordinator','continuation'].includes(phase)||p!=='--proposal-commit'||rc!=='--receipt-commit'||rs!=='--receipt-sha256'||!/^[0-9a-f]{64}$/.test(receiptSha256??''))throw Error('R9_FIXED_ARGUMENTS_REQUIRED');exactCommit(proposalCommit);exactCommit(receiptCommit);
 const out={phase,proposalCommit,receiptCommit,receiptSha256},needed=['--operator-commit','--holder-commit','--coordinator-commit'].slice(0,['operator','holder','coordinator','continuation'].indexOf(phase));if(rest.length!==needed.length*2)throw Error('R9_FIXED_ARGUMENTS_REQUIRED');
 for(let n=0;n<needed.length;n++){if(rest[2*n]!==needed[n])throw Error('R9_FIXED_ARGUMENTS_REQUIRED');out[needed[n].slice(2).replace(/-([a-z])/g,(_,c)=>c.toUpperCase())]=exactCommit(rest[2*n+1]);}return out;
}
export function readPublicBinding(selected,{read=file=>fs.readFileSync(ROOT+'/'+file),run=(...args)=>git(ROOT,...args)}={}){
 const head=run('rev-parse','HEAD').toString().trim();run('merge-base','--is-ancestor',selected.receiptCommit,head);
 const b=read(receiptFile);if(b.length>32768||hash(b)!==selected.receiptSha256||![selected.receiptCommit,head,''].every(ref=>b.equals(run('show',ref+':'+receiptFile))))throw Error('R9_GENUINE_COMMITTED_PUBLIC_RECEIPT_REQUIRED');
 if(run('rev-parse','HEAD').toString().trim()!==head)throw Error('R9_RECEIPT_HEAD_MOVED');
 const binding={observation:{commit:selected.receiptCommit,file:receiptFile,sha256:selected.receiptSha256},publicReceipt:b.toString('utf8')};validateBinding(binding);return binding;
}
export function materialize(flags){
 const selected=parse(flags);for(const root of [report,ROOT])if(fs.realpathSync.native(root).replaceAll('\\','/')!==root||git(root,'rev-parse','--show-toplevel').toString().trim().replaceAll('\\','/')!==root)throw Error('R9_REGISTERED_D_ROOT_REQUIRED');if(path.relative(report,here).replaceAll('\\','/')!==relative.slice(0,-1))throw Error('R9_EXACT_PROPOSAL_PATH_REQUIRED');
 const head=git(report,'rev-parse','HEAD').toString().trim();git(report,'merge-base','--is-ancestor',baseCommit,head);
 const manifestBytes=fs.readFileSync(here+'/MANIFEST.json'),manifest=JSON.parse(manifestBytes),own=[{target:relative+'MANIFEST.json',text:manifestBytes.toString(),afterSha256:hash(manifestBytes)},...manifest.files.map(r=>{const b=fs.readFileSync(here+'/'+r.file);if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('R9_FROZEN_PROPOSAL_CHANGED');return {target:relative+r.file,text:b.toString(),afterSha256:r.sha256};})];assertActualMount(selected.proposalCommit,own);
 const binding=readPublicBinding(selected),op=operator(binding),previous=[];let built=op;
 if(selected.phase!=='operator'){assertActualMount(selected.operatorCommit,op.targets);previous.push(...op.targets);built=holder(binding,selected.operatorCommit);}
 if(['coordinator','continuation'].includes(selected.phase)){assertActualMount(selected.holderCommit,built.targets);previous.push(...built.targets);built=coordinator(binding,selected.operatorCommit,selected.holderCommit);}
 if(selected.phase==='continuation'){assertActualMount(selected.coordinatorCommit,built.targets);previous.push(...built.targets);built=continuation(binding,selected.operatorCommit,selected.holderCommit,selected.coordinatorCommit);}
 const parents=JSON.parse(fs.readFileSync(here+'/PARENTS.json'));for(const r of parents.rows){if(r.root===report&&previous.some(t=>t.target===r.file))continue;const b=fs.readFileSync(r.root+'/'+r.file);if(b.length!==r.bytes||hash(b)!==r.sha256||![r.commit,git(r.root,'rev-parse','HEAD').toString().trim(),''].every(ref=>b.equals(git(r.root,'show',ref+':'+r.file))))throw Error('R9_COMPLETE_CURRENT_PARENT_CHANGED');}
 for(const t of built.targets){const b=fs.readFileSync(report+'/'+t.target);if(hash(b)!==t.beforeSha256||![baseCommit,head,''].every(ref=>b.equals(git(report,'show',ref+':'+t.target))))throw Error('R9_FOLLOW_ON_PARENT_CHANGED');}
 if(git(report,'rev-parse','HEAD').toString().trim()!==head)throw Error('R9_REPORT_HEAD_MOVED');const parent=report+'/.codex-tmp';if(fs.realpathSync.native(parent).replaceAll('\\','/')!==parent)throw Error('R9_D_OUTPUT_PARENT_REQUIRED');const out=parent+'/daily-auth-r9-source-'+crypto.randomBytes(16).toString('hex');fs.mkdirSync(out);
 const record={...built,targets:built.targets.map(({text,...t})=>t)};for(const t of built.targets)fs.writeFileSync(out+'/'+t.file,t.text,{flag:'wx'});fs.writeFileSync(out+'/FULL_PARENT_FORWARD.json',json(record),{flag:'wx'});fs.writeFileSync(out+'/PUBLIC_BINDING.json',json(binding),{flag:'wx'});console.log(JSON.stringify({status:'R9_REVIEWED_IMAGES_MATERIALIZED_NOT_MOUNTED',phase:selected.phase,ownedDirectory:out,targets:built.targets.length,recordSha256:hash(json(record)),actualR9Binding:'GENUINE_PUBLIC_RECEIPT',privateStateTargetAccess:'NONE',native:'NOT_RUN'}));return record;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))try{materialize(process.argv.slice(2));}catch{console.log(JSON.stringify({status:'HOLD',code:'R9_ACTUAL_PUBLIC_RECEIPT_OR_COMMITTED_IMAGE_HELD'}));process.exitCode=1;}
