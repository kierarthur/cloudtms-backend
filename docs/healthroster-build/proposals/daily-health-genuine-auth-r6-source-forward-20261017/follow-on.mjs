// Root-only finite metadata materializer. Never mounts runtime or invokes source/preflight/auth.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {operator,holder,coordinator,hash} from './recipe.mjs';
export const report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';const here=path.dirname(fileURLToPath(import.meta.url));
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',report,...args],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
export function assertActualMount(commit,targets,{read=file=>fs.readFileSync(report+'/'+file),run=git}={}){
 if(!/^[0-9a-f]{40}$/.test(commit??''))throw Error('R6_ACTUAL_COMMITTED_MOUNT_REQUIRED');const head=run('rev-parse','HEAD').toString().trim();run('merge-base','--is-ancestor',commit,head);
 for(const t of targets){const b=read(t.target),expected=Buffer.from(t.text);if(!b.equals(expected)||hash(b)!==t.afterSha256||![commit,head,''].every(ref=>b.equals(run('show',ref+':'+t.target))))throw Error('R6_ACTUAL_MOUNT_IMAGE_REQUIRED');}
 if(run('rev-parse','HEAD').toString().trim()!==head)throw Error('R6_REPORT_HEAD_MOVED');return head;
}
export function parse(flags){if(flags.length===3&&flags[0]==='holder'&&flags[1]==='--operator-commit'&&/^[0-9a-f]{40}$/.test(flags[2]))return {phase:'holder',operatorCommit:flags[2]};if(flags.length===5&&flags[0]==='coordinator'&&flags[1]==='--operator-commit'&&flags[3]==='--holder-commit'&&[flags[2],flags[4]].every(v=>/^[0-9a-f]{40}$/.test(v)))return {phase:'coordinator',operatorCommit:flags[2],holderCommit:flags[4]};throw Error('R6_FIXED_FOLLOW_ON_ARGUMENTS_REQUIRED');}
export function materialize(flags){
 const selected=parse(flags),op=operator();assertActualMount(selected.operatorCommit,op.targets);let built;
 if(selected.phase==='holder')built=holder(selected.operatorCommit);else{const held=holder(selected.operatorCommit);assertActualMount(selected.holderCommit,held.targets);built=coordinator(selected.operatorCommit,selected.holderCommit);}
 // The destination metadata must still be its exact reviewed e9cc parent before emission.
 for(const t of built.targets){const b=fs.readFileSync(report+'/'+t.target);if(hash(b)!==t.beforeSha256||!b.equals(git('show','HEAD:'+t.target))||!b.equals(git('show',':'+t.target)))throw Error('R6_FOLLOW_ON_PARENT_CHANGED');}
 const parent=report+'/.codex-tmp';if(fs.realpathSync.native(parent).replaceAll('\\','/')!==parent)throw Error('R6_D_OUTPUT_PARENT_REQUIRED');const out=parent+'/daily-auth-r6-source-forward-'+crypto.randomBytes(16).toString('hex');fs.mkdirSync(out);const record={...built,targets:built.targets.map(({text,...t})=>t)};
 for(const t of built.targets)fs.writeFileSync(out+'/'+t.file,t.text,{flag:'wx'});fs.writeFileSync(out+'/FULL_PARENT_FORWARD.json',JSON.stringify(record,null,2)+'\n',{flag:'wx'});
 console.log(JSON.stringify({status:'R6_REVIEWED_FOLLOW_ON_MATERIALIZED_NOT_MOUNTED',phase:selected.phase,ownedDirectory:out,targets:built.targets.length,recordSha256:hash(fs.readFileSync(out+'/FULL_PARENT_FORWARD.json')),native:'NOT_RUN',privateStateAccess:'NONE'}));return record;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){try{materialize(process.argv.slice(2));}catch{console.log(JSON.stringify({status:'HOLD',code:'R6_ACTUAL_COMMITTED_FOLLOW_ON_HELD'}));process.exitCode=1;}}
