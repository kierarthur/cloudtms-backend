// Closed source authority. Importing never opens a database or turn.
import fs from'node:fs';import path from'node:path';import {execFileSync}from'node:child_process';import {fileURLToPath,pathToFileURL}from'node:url';
import {buildProgram,sha,literal,CASES}from'./build-program.mjs';
export {sha,literal,CASES};export const here=path.dirname(fileURLToPath(import.meta.url));
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const SCOPE='CONTACT_QT_POSITIVE_TWELVE_COMPLETE_RELATIONS_V1';
export const ROUTE='mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback';
export const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:24e6});
const checked=new Set();
export const sortWriters=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
export const markerFor=identity=>'hr_install2_'+identity.turnId.slice('hr_turn_'.length);
export function assertPhysical(root){
 requireThat(typeof root==='string'&&physical(root)===ROOT,'CONTACT_QT_REGISTERED_PHYSICAL_ROOT_REQUIRED');
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(process.cwd())),'CONTACT_QT_ACTUAL_D_CWD_REQUIRED');
 const line=args=>git(root,args).toString().trim().replaceAll('\\','/');
 requireThat(physical(line(['rev-parse','--show-toplevel']))===ROOT
  &&physical(line(['rev-parse','--absolute-git-dir']))==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-install2-14102026'
  &&physical(path.resolve(root,line(['rev-parse','--git-common-dir'])))==='D:/Development/GitHub/cloudtms-backend/.git','CONTACT_QT_PHYSICAL_GIT_BINDING_REQUIRED');return ROOT;
}
export function assertCommittedAdapter(){
 const top=git(here,['rev-parse','--show-toplevel']).toString().trim(),relative=path.relative(top,here).replaceAll('\\','/');
 requireThat(/^D:\/Development\/GitHub(?:\/|$)/.test(physical(top))&&relative==='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017','CONTACT_QT_EXACT_ADAPTER_LOCATION_REQUIRED');
 requireThat(physical(path.resolve(here,git(here,['rev-parse','--git-common-dir']).toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','CONTACT_QT_ADAPTER_COMMON_GIT_REQUIRED');
 const manifest=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));
 for(const file of [...manifest.files.map(r=>r.file),'MANIFEST.json']){const target=relative+'/'+file,b=fs.readFileSync(here+'/'+file);requireThat(b.equals(git(here,['show','HEAD:'+target]))&&b.equals(git(here,['show',':'+target])),'CONTACT_QT_ADAPTER_SAVED_INDEX_HEAD_REQUIRED');}
 return {committed:true,savedIndexEqual:true};
}
export function assertManifestedFixtureEntry(root){
 assertPhysical(root);const file='docs/healthroster-build/TEST_SETUP_MANIFEST.md',b=fs.readFileSync(root+'/'+file),entry=fs.readFileSync(here+'/SETUP_ENTRY.md','utf8').trim();
 requireThat(b.toString().split(entry).length===2,'CONTACT_QT_EXACT_COMMITTED_SETUP_REQUIRED');
 requireThat(b.equals(git(root,['show','HEAD:'+file]))&&b.equals(git(root,['show',':'+file])),'CONTACT_QT_SETUP_SAVED_INDEX_HEAD_REQUIRED');return true;
}
export async function source(root,caseId){
 assertPhysical(root);requireThat(CASES.includes(caseId),'CONTACT_QT_EXACT_CASE_REQUIRED');
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')),head=git(root,['rev-parse','HEAD']).toString().trim(),key=head+'|'+sha(JSON.stringify(pins));
 if(!checked.has(key))git(root,['merge-base','--is-ancestor',pins.rootCommit,head]);
 try{git(root,['diff','--cached','--quiet',head,'--',...pins.rootInputs.map(r=>r.file)]);}catch{throw Error('CONTACT_QT_ROOT_INDEX_SOURCE_CHANGED');}
 for(const row of pins.rootInputs){const b=fs.readFileSync(root+'/'+row.file);requireThat(b.length===row.bytes&&sha(b)===row.sha256,'CONTACT_QT_REVIEWED_ROOT_SOURCE_CHANGED');if(!checked.has(key))requireThat(b.equals(git(root,['show',pins.rootCommit+':'+row.file]))&&b.equals(git(root,['show',head+':'+row.file])),'CONTACT_QT_ROOT_COMMITTED_SOURCE_REQUIRED');}checked.add(key);
 const expected=JSON.parse(fs.readFileSync(here+'/PROGRAMMES.json'))[caseId],programme=await buildProgram({root,caseId});
 requireThat(expected&&programme.sqlSha256===expected.sqlSha256&&programme.sqlBytes===expected.sqlBytes&&programme.statements===expected.statements,'CONTACT_QT_SEALED_PROGRAMME_REQUIRED');
 if(fs.existsSync(here+'/MANIFEST.json'))for(const row of JSON.parse(fs.readFileSync(here+'/MANIFEST.json')).files)requireThat(sha(fs.readFileSync(here+'/'+row.file))===row.sha256,'CONTACT_QT_ADAPTER_SOURCE_CHANGED');
 const load=file=>import(pathToFileURL(root+'/'+file).href),rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),sql=await load('scripts/cloudtms-managed-release-sql.mjs'),control=await load('scripts/healthroster-turn-control.mjs');
 requireThat(JSON.stringify(live.backgroundRelations)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']),'CONTACT_QT_BOTH_BACKGROUND_RELATIONS_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(root+'/supabase/release/current-contract.json')),writers=sortWriters(contract.routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 requireThat(writers.length===4&&sha(JSON.stringify(writers))===pins.expectedWritersSha256,'CONTACT_QT_ORIGINAL_WRITER_AUTHORITY_CHANGED');
 return {root,programme,rehearsal,live,sql,control,expectedWriters:writers,pins};
}
