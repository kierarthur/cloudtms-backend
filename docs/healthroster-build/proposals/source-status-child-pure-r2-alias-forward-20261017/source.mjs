// Closed source loader. Importing this module never opens a database or turn.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const ROUTE='mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback';
export const SCOPE='SOURCE_STATUS_CHILD_PURE_NO_BUSINESS_DML_BOTH_COMPLETE_BACKGROUND_RELATIONS_V1';
export const PROGRAMME_SHA='ec52dff75ea03a6886691cab3b426186d07b6636a97e6a94f2ff3b2ef5fe6f64';
export const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
export const sha=value=>crypto.createHash('sha256').update(value).digest('hex');
export const literal=value=>`'${String(value).replaceAll("'","''")}'`;
export const sortWriters=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
const physical=value=>fs.realpathSync.native(value).replaceAll('\\','/');
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:16*1024*1024});
const committedRootBindings=new Set();
export function assertPhysical(root){
 requireThat(typeof root==='string'&&path.isAbsolute(root)&&physical(root)===ROOT,'PURE_REGISTERED_PHYSICAL_ROOT_REQUIRED');
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(process.cwd())),'PURE_ACTUAL_D_WORKING_DIRECTORY_REQUIRED');
 const line=args=>git(root,args).toString('utf8').trim().replaceAll('\\','/');
 requireThat(physical(line(['rev-parse','--show-toplevel']))===ROOT
  &&physical(line(['rev-parse','--absolute-git-dir']))==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-install2-14102026'
  &&physical(path.resolve(root,line(['rev-parse','--git-common-dir'])))==='D:/Development/GitHub/cloudtms-backend/.git','PURE_PHYSICAL_GIT_BINDING_REQUIRED');
 return ROOT;
}
export function assertCommittedAdapter(){
 const top=git(here,['rev-parse','--show-toplevel']).toString('utf8').trim();
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(top)),'PURE_ADAPTER_PHYSICAL_SOURCE_REQUIRED');
 const relative=path.relative(top,here).replaceAll('\\','/');
 requireThat(relative==='docs/healthroster-build/proposals/source-status-child-pure-r2-alias-forward-20261017','PURE_ADAPTER_REPOSITORY_LOCATION_REQUIRED');
 const common=git(here,['rev-parse','--git-common-dir']).toString('utf8').trim();
 requireThat(physical(path.resolve(here,common))==='D:/Development/GitHub/cloudtms-backend/.git','PURE_ADAPTER_SHARED_REPOSITORY_REQUIRED');
 const manifest=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
 requireThat(manifest.version==='SOURCE_STATUS_CHILD_PURE_ADAPTER_EXECUTABLE_V1','PURE_ADAPTER_MANIFEST_NOT_SEALED');
 for(const file of [...manifest.files.map(r=>r.file),'SOURCE_PINS.json','SOURCE_MANIFEST.json','SOURCE_TRANSFERS.json','PROGRAMME_SOURCE_TRANSFER.json','PROGRAMME_MILESTONE_MANIFEST.json']){
  const bytes=fs.readFileSync(path.join(here,file)),target=relative+'/'+file;
  requireThat(bytes.equals(git(here,['show','HEAD:'+target]))&&bytes.equals(git(here,['show',':'+target])),'PURE_ADAPTER_SAVED_INDEX_COMMIT_EQUALITY_REQUIRED');
 }
 return {committed:true,savedIndexEqual:true,sourceCommit:git(here,['rev-parse','HEAD']).toString('utf8').trim()};
}
export async function source(root){
 root=assertPhysical(root);
 const pins=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_PINS.json'),'utf8'));
 requireThat(pins.version==='SOURCE_STATUS_CHILD_PURE_ADAPTER_SOURCE_V1'&&pins.status==='SEALED_SOURCE_ONLY','PURE_SOURCE_PINS_NOT_SEALED');
 const head=git(root,['rev-parse','HEAD']).toString('utf8').trim(),binding=head+'|'+sha(JSON.stringify(pins));
 try{git(root,['diff','--cached','--quiet',head,'--',...pins.rootInputs.map(r=>r.file)]);}catch{throw Error('PURE_ROOT_INDEX_SOURCE_CHANGED');}
 const needsCommitProof=!committedRootBindings.has(binding);
 if(needsCommitProof)git(root,['merge-base','--is-ancestor',pins.rootCommit,head]);
 for(const row of pins.rootInputs){
  const saved=fs.readFileSync(path.join(root,row.file));
  requireThat(saved.length===row.bytes&&sha(saved)===row.sha256,'PURE_REVIEWED_ROOT_SOURCE_CHANGED');
  if(needsCommitProof)requireThat(saved.equals(git(root,['show',pins.rootCommit+':'+row.file]))
   &&saved.equals(git(root,['show',head+':'+row.file])),'PURE_COMMITTED_SOURCE_EQUALITY_REQUIRED');
 }
 committedRootBindings.add(binding); // Saved bytes above are ALWAYS rechecked.
 const executable=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
 requireThat(executable.version==='SOURCE_STATUS_CHILD_PURE_ADAPTER_EXECUTABLE_V1','PURE_ADAPTER_MANIFEST_NOT_SEALED');
 for(const row of executable.files)requireThat(sha(fs.readFileSync(path.join(here,row.file)))===row.sha256,'PURE_ADAPTER_SOURCE_CHANGED');
 requireThat(sha(fs.readFileSync(path.join(here,'SOURCE_PINS.json')))===executable.sourcePinsSha256,'PURE_ADAPTER_PINS_CHANGED');
 const load=file=>import(pathToFileURL(path.join(root,file)).href);
 const builder=await import('./programme/build-program.mjs');
 const programme=builder.buildPureProgram({root});
 requireThat(programme.sqlBytes===20447&&programme.sqlSha256===PROGRAMME_SHA&&programme.definitionCount===1
  &&programme.positiveBoundaryVectors===4&&programme.negativeBoundaryRefusals===12&&programme.existingDependencyReplaced===false&&programme.businessFixtureRows===0,'PURE_EXACT_ORIGINAL_PROGRAMME_REQUIRED');
 const rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),
  sql=await load('scripts/cloudtms-managed-release-sql.mjs'),control=await load('scripts/healthroster-turn-control.mjs');
 requireThat(JSON.stringify(live.backgroundRelations)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']),'PURE_BOTH_BACKGROUND_RELATIONS_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'));
 const writers=sortWriters(contract.routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 requireThat(writers.length===4&&sha(JSON.stringify(writers))===pins.expectedWritersSha256,'PURE_ORIGINAL_WRITER_AUTHORITY_CHANGED');
 return {root,programme,rehearsal,live,sql,control,expectedWriters:writers,pins};
}
export const markerFor=identity=>'hr_install2_'+identity.turnId.slice('hr_turn_'.length);
