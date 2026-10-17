// Closed source loader. Importing this module never opens a database or turn.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const ROUTE='mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback';
export const SCOPE='DAILY_HEALTH_ACTOR_REAL_USERS_BOTH_COMPLETE_BACKGROUND_RELATIONS_V1';
export const PROGRAMME_SHA='69789fd5fef651a39d04d4ef2a9c2d3666be04f551d51f38b833e3a420d9eb3e';
export const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
export const sha=value=>crypto.createHash('sha256').update(value).digest('hex');
export const literal=value=>`'${String(value).replaceAll("'","''")}'`;
export const sortWriters=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
const physical=value=>fs.realpathSync.native(value).replaceAll('\\','/');
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:16*1024*1024});
const committedRootBindings=new Set();
export function assertPhysical(root){
 requireThat(typeof root==='string'&&path.isAbsolute(root)&&physical(root)===ROOT,'ACTOR_REGISTERED_PHYSICAL_ROOT_REQUIRED');
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(process.cwd())),'ACTOR_ACTUAL_D_WORKING_DIRECTORY_REQUIRED');
 const line=args=>git(root,args).toString('utf8').trim().replaceAll('\\','/');
 requireThat(physical(line(['rev-parse','--show-toplevel']))===ROOT
  &&physical(line(['rev-parse','--absolute-git-dir']))==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-install2-14102026'
  &&physical(path.resolve(root,line(['rev-parse','--git-common-dir'])))==='D:/Development/GitHub/cloudtms-backend/.git','ACTOR_PHYSICAL_GIT_BINDING_REQUIRED');
 return ROOT;
}
export function assertCommittedAdapter(){
 const top=git(here,['rev-parse','--show-toplevel']).toString('utf8').trim();
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(top)),'ACTOR_ADAPTER_PHYSICAL_SOURCE_REQUIRED');
 const relative=path.relative(top,here).replaceAll('\\','/');
 requireThat(relative==='docs/healthroster-build/proposals/daily-health-actor-native-proof-20261017','ACTOR_ADAPTER_REPOSITORY_LOCATION_REQUIRED');
 const common=git(here,['rev-parse','--git-common-dir']).toString('utf8').trim();
 requireThat(physical(path.resolve(here,common))==='D:/Development/GitHub/cloudtms-backend/.git','ACTOR_ADAPTER_SHARED_REPOSITORY_REQUIRED');
 const manifest=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
 for(const file of [...manifest.files.map(r=>r.file),'SOURCE_PINS.json','SOURCE_MANIFEST.json','SOURCE_TRANSFERS.json']){
  const bytes=fs.readFileSync(path.join(here,file)),target=relative+'/'+file;
  requireThat(bytes.equals(git(here,['show','HEAD:'+target]))&&bytes.equals(git(here,['show',':'+target])),'ACTOR_ADAPTER_SAVED_INDEX_COMMIT_EQUALITY_REQUIRED');
 }
 return {committed:true,savedIndexEqual:true,sourceCommit:git(here,['rev-parse','HEAD']).toString('utf8').trim()};
}
export async function source(root){
 root=assertPhysical(root);
 const pins=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_PINS.json'),'utf8'));
 const head=git(root,['rev-parse','HEAD']).toString('utf8').trim(),binding=head+'|'+sha(JSON.stringify(pins));
 try{git(root,['diff','--cached','--quiet',head,'--',...pins.rootInputs.map(r=>r.file)]);}catch{throw Error('ACTOR_ROOT_INDEX_SOURCE_CHANGED');}
 const needsCommitProof=!committedRootBindings.has(binding);
 if(needsCommitProof)git(root,['merge-base','--is-ancestor',pins.rootCommit,head]);
 for(const row of pins.rootInputs){
  const saved=fs.readFileSync(path.join(root,row.file));
  requireThat(saved.length===row.bytes&&sha(saved)===row.sha256,'ACTOR_REVIEWED_ROOT_SOURCE_CHANGED');
  if(needsCommitProof)requireThat(saved.equals(git(root,['show',pins.rootCommit+':'+row.file]))
   &&saved.equals(git(root,['show',head+':'+row.file])),'ACTOR_COMMITTED_SOURCE_EQUALITY_REQUIRED');
 }
 committedRootBindings.add(binding); // Saved bytes above are ALWAYS rechecked.
 const executable=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
 for(const row of executable.files)requireThat(sha(fs.readFileSync(path.join(here,row.file)))===row.sha256,'ACTOR_ADAPTER_SOURCE_CHANGED');
 requireThat(sha(fs.readFileSync(path.join(here,'SOURCE_PINS.json')))===executable.sourcePinsSha256,'ACTOR_ADAPTER_PINS_CHANGED');
 const load=file=>import(pathToFileURL(path.join(root,file)).href);
 const builder=await import('./build-program.mjs');
 const programme=await builder.buildActorProgram({root});
 requireThat(programme.sqlSha256===PROGRAMME_SHA&&programme.definitionCount===1&&programme.businessFixtureRows===6
  &&programme.matrixChecks===12&&programme.matrixDenials===10&&programme.sameTransactionChanges===3&&programme.nullGuardRefusals===2,'ACTOR_EXACT_PROGRAMME_REQUIRED');
 const rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),
  sql=await load('scripts/cloudtms-managed-release-sql.mjs'),control=await load('scripts/healthroster-turn-control.mjs');
 requireThat(JSON.stringify(live.backgroundRelations)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']),'ACTOR_BOTH_BACKGROUND_RELATIONS_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'));
 const writers=sortWriters(contract.routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 requireThat(writers.length===4&&sha(JSON.stringify(writers))===pins.expectedWritersSha256,'ACTOR_ORIGINAL_WRITER_AUTHORITY_CHANGED');
 return {root,programme,rehearsal,live,sql,control,expectedWriters:writers,pins};
}
export const markerFor=identity=>'hr_install2_'+identity.turnId.slice('hr_turn_'.length);

export function assertManifestedFixtureEntry(root){
 assertPhysical(root);const target='docs/healthroster-build/TEST_SETUP_MANIFEST.md',actual=fs.readFileSync(path.join(root,target)),entry=fs.readFileSync(path.join(here,'SETUP_ENTRY.md'),'utf8').trim();
 requireThat(actual.toString().split(entry).length===2,'ACTOR_EXACT_COMMITTED_SETUP_ENTRY_REQUIRED');
 requireThat(actual.equals(git(root,['show','HEAD:'+target]))&&actual.equals(git(root,['show',':'+target])),'ACTOR_SETUP_SAVED_INDEX_HEAD_EQUALITY_REQUIRED');return true;
}
