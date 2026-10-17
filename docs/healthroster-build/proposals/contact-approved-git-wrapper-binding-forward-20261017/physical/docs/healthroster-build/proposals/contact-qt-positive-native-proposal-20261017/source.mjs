// Closed source authority. Importing never opens a database or turn.
import fs from'node:fs';import path from'node:path';import {execFileSync}from'node:child_process';import {fileURLToPath,pathToFileURL}from'node:url';
import {buildProgram,sha,literal,CASES}from'./build-program.mjs';
export {sha,literal,CASES};export const here=path.dirname(fileURLToPath(import.meta.url));
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const SCOPE='CONTACT_QT_POSITIVE_TWELVE_COMPLETE_RELATIONS_V1';
export const ROUTE='mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback';
export const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
const git=(root,args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:24e6,...input===undefined?{}:{input}});
// Read the same complete Git blobs using ordered, length-framed stdin batches.
// There is no cross-invocation cache, text normalization, file sink or fetch.
const GIT_MAX_BUFFER=24e6;
const batchRequest=r=>r&&typeof r.spec==='string'&&/^(?:HEAD|[0-9a-f]{40})?:[^\0\r\n]+$/.test(r.spec)&&Number.isSafeInteger(r.bytes)&&r.bytes>=0;
export function parseContactGitBlobBatch(raw,requests){
 requireThat(Buffer.isBuffer(raw)&&raw.length<=GIT_MAX_BUFFER&&Array.isArray(requests)&&requests.length>0&&requests.every(batchRequest),'CONTACT_QT_GIT_BATCH_ENVELOPE_REQUIRED');
 const bodies=[];let offset=0;
 for(const r of requests){const newline=raw.indexOf(10,offset);requireThat(newline>=offset&&newline-offset<=128,'CONTACT_QT_GIT_BATCH_HEADER_REQUIRED');const header=raw.subarray(offset,newline);requireThat(header.every(b=>b<128),'CONTACT_QT_GIT_BATCH_HEADER_REQUIRED');const m=/^([0-9a-f]{40}) blob (0|[1-9][0-9]*)$/.exec(header.toString('ascii')),size=Number(m?.[2]);requireThat(m&&Number.isSafeInteger(size)&&size===r.bytes,'CONTACT_QT_GIT_BATCH_BLOB_REQUIRED');const end=newline+1+size;requireThat(end<raw.length&&raw[end]===10,'CONTACT_QT_GIT_BATCH_COMPLETE_BLOB_REQUIRED');bodies.push(raw.subarray(newline+1,end));offset=end+1;}
 requireThat(offset===raw.length,'CONTACT_QT_GIT_BATCH_TRAILING_DATA_REFUSED');return bodies;
}
function gitBlobs(root,requests){
 requireThat(Array.isArray(requests)&&requests.length>0&&requests.every(batchRequest)&&new Set(requests.map(r=>r.spec)).size===requests.length,'CONTACT_QT_GIT_BATCH_REQUEST_REQUIRED');
 const result=new Map();let group=[],cost=0;
 const flush=()=>{if(!group.length)return;let raw;try{raw=git(root,['cat-file','--batch'],Buffer.from(group.map(r=>r.spec).join('\n')+'\n'));}catch{throw Error('CONTACT_QT_GIT_BATCH_READ_REFUSED');}const bodies=parseContactGitBlobBatch(raw,group);for(let i=0;i<group.length;i++)result.set(group[i].spec,bodies[i]);group=[];cost=0;};
 for(const r of requests){const next=r.bytes+128;if(next>GIT_MAX_BUFFER){flush();let raw;try{raw=git(root,['show',r.spec]);}catch{throw Error('CONTACT_QT_GIT_BATCH_READ_REFUSED');}requireThat(raw.length===r.bytes,'CONTACT_QT_GIT_BATCH_BLOB_REQUIRED');result.set(r.spec,raw);continue;}if(group.length&&cost+next>GIT_MAX_BUFFER)flush();group.push(r);cost+=next;}
 flush();requireThat(result.size===requests.length,'CONTACT_QT_GIT_BATCH_COMPLETE_REQUESTS_REQUIRED');return result;
}
// Pure byte predicate. Actual source() supplies real saved/index/HEAD and
// committed predecessor bytes; offline tests do not grant source admission.
export function assertPinnedSourceImages(row,{saved,index,current,parent,parentForward=null}){
 requireThat([saved,index,current,parent].every(Buffer.isBuffer),'CONTACT_QT_RAW_SOURCE_IMAGES_REQUIRED');
 requireThat(saved.length===row.bytes&&sha(saved)===row.sha256,'CONTACT_QT_REVIEWED_ROOT_SOURCE_CHANGED');
 requireThat(saved.equals(index)&&saved.equals(current),'CONTACT_QT_ROOT_COMMITTED_SOURCE_REQUIRED');
 requireThat(parentForward?parentForward.file===row.file&&parent.length===parentForward.parentBytes&&sha(parent)===parentForward.parentSha256:saved.equals(parent),'CONTACT_QT_ROOT_COMMITTED_PREDECESSOR_REQUIRED');
 return true;
}
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
 const rows=[...manifest.files.map(r=>r.file),'MANIFEST.json'].map(file=>({target:relative+'/'+file,saved:fs.readFileSync(here+'/'+file)}));
 const images=gitBlobs(here,rows.flatMap(r=>[{spec:'HEAD:'+r.target,bytes:r.saved.length},{spec:':'+r.target,bytes:r.saved.length}]));
 for(const r of rows)requireThat(r.saved.equals(images.get('HEAD:'+r.target))&&r.saved.equals(images.get(':'+r.target)),'CONTACT_QT_ADAPTER_SAVED_INDEX_HEAD_REQUIRED');
 return {committed:true,savedIndexEqual:true};
}
export function assertManifestedFixtureEntry(root){
 assertPhysical(root);const file='docs/healthroster-build/TEST_SETUP_MANIFEST.md',b=fs.readFileSync(root+'/'+file),entry=fs.readFileSync(here+'/SETUP_ENTRY.md','utf8').trim();
 requireThat(b.toString().split(entry).length===2,'CONTACT_QT_EXACT_COMMITTED_SETUP_REQUIRED');
 requireThat(b.equals(git(root,['show','HEAD:'+file]))&&b.equals(git(root,['show',':'+file])),'CONTACT_QT_SETUP_SAVED_INDEX_HEAD_REQUIRED');return true;
}
export async function source(root,caseId){
 assertPhysical(root);requireThat(CASES.includes(caseId),'CONTACT_QT_EXACT_CASE_REQUIRED');
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')),head=git(root,['rev-parse','HEAD']).toString().trim();
 const forward=pins.snapshotSourceForward,forwardNames=['scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-install1-full-rehearsal.mjs'];
 if(forward!==undefined)requireThat(pins.rootCommit==='42802228977b941325f0feb44c11918b0f42190f'&&Array.isArray(forward)&&forward.length===2&&forward.every((r,i)=>Object.keys(r).sort().join('|')==='file|parentBytes|parentSha256'&&r.file===forwardNames[i]&&Number.isSafeInteger(r.parentBytes)&&r.parentBytes>0&&/^[0-9a-f]{64}$/.test(r.parentSha256)),'CONTACT_QT_EXACT_SNAPSHOT_SOURCE_FORWARD_REQUIRED');
 git(root,['merge-base','--is-ancestor',pins.rootCommit,head]);
 try{git(root,['diff','--cached','--quiet',head,'--',...pins.rootInputs.map(r=>r.file)]);}catch{throw Error('CONTACT_QT_ROOT_INDEX_SOURCE_CHANGED');}
 const images=gitBlobs(root,pins.rootInputs.flatMap(row=>[{spec:':'+row.file,bytes:row.bytes},{spec:head+':'+row.file,bytes:row.bytes},{spec:pins.rootCommit+':'+row.file,bytes:forward?.find(r=>r.file===row.file)?.parentBytes??row.bytes}]));
 for(const row of pins.rootInputs){const b=fs.readFileSync(root+'/'+row.file);requireThat(b.length===row.bytes&&sha(b)===row.sha256,'CONTACT_QT_REVIEWED_ROOT_SOURCE_CHANGED');assertPinnedSourceImages(row,{saved:b,index:images.get(':'+row.file),current:images.get(head+':'+row.file),parent:images.get(pins.rootCommit+':'+row.file),parentForward:forward?.find(r=>r.file===row.file)??null});}
 const expected=JSON.parse(fs.readFileSync(here+'/PROGRAMMES.json'))[caseId],programme=await buildProgram({root,caseId});
 requireThat(expected&&programme.sqlSha256===expected.sqlSha256&&programme.sqlBytes===expected.sqlBytes&&programme.statements===expected.statements,'CONTACT_QT_SEALED_PROGRAMME_REQUIRED');
 if(fs.existsSync(here+'/MANIFEST.json'))for(const row of JSON.parse(fs.readFileSync(here+'/MANIFEST.json')).files)requireThat(sha(fs.readFileSync(here+'/'+row.file))===row.sha256,'CONTACT_QT_ADAPTER_SOURCE_CHANGED');
 const load=file=>import(pathToFileURL(root+'/'+file).href),rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),sql=await load('scripts/cloudtms-managed-release-sql.mjs'),control=await load('scripts/healthroster-turn-control.mjs');
 requireThat(JSON.stringify(live.backgroundRelations)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']),'CONTACT_QT_BOTH_BACKGROUND_RELATIONS_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(root+'/supabase/release/current-contract.json')),writers=sortWriters(contract.routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 requireThat(writers.length===4&&sha(JSON.stringify(writers))===pins.expectedWritersSha256,'CONTACT_QT_ORIGINAL_WRITER_AUTHORITY_CHANGED');
 return {root,programme,rehearsal,live,sql,control,expectedWriters:writers,pins};
}
