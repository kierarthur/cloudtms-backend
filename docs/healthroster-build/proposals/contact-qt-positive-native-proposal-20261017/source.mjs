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
// Exactly the reviewed complete-path Git-buffer correction; no general source
// forwarding rule. The original root and both snapshot forwards remain intact.
const APPROVED_FINAL_DEFINER_FORWARD=Object.freeze({
 file:'scripts/bpay-next-final-definers.mjs',parentBytes:13611,parentSha256:'51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09',
 approvedCommit:'51177b9abcdc9f089e680c9e7e9ef5285f09ff57',bytes:13639,sha256:'b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa',
 proofFile:'docs/healthroster-build/proposals/shared-final-definer-git-buffer-forward-20261017/FULL_PARENT_FORWARD.json',proofBytes:759,proofSha256:'48aa1a207637aa27846ea22a146fa57991af585a821bad3e754c89d6da847fcb',
 reviewFile:'docs/healthroster-build/v6-owner-delivery-20261016/SHARED_FINAL_DEFINER_GIT_BUFFER_ROOT_REVIEW_20261017.json',reviewBytes:1356,reviewSha256:'fc7a0a9492fd101f894520b0f97c91a59fc2b3ce1bbe05a7cfc1d4843761ea40'
});
const exactFinalDefinerForward=f=>f&&Object.keys(f).sort().join('|')===Object.keys(APPROVED_FINAL_DEFINER_FORWARD).sort().join('|')&&Object.entries(APPROVED_FINAL_DEFINER_FORWARD).every(([k,v])=>f[k]===v);
export function assertApprovedFinalDefinerForward(pins){
 const f=pins.approvedFinalDefinerForward;
 requireThat(pins.rootCommit==='42802228977b941325f0feb44c11918b0f42190f'&&exactFinalDefinerForward(f),'CONTACT_QT_EXACT_FINAL_DEFINER_FORWARD_REQUIRED');
 requireThat(Array.isArray(pins.rootInputs)&&pins.rootInputs.length===63&&new Set(pins.rootInputs.map(r=>r.file)).size===63,'CONTACT_QT_ORIGINAL_ROOT_INPUT_SET_REQUIRED');
 const rows=pins.rootInputs.filter(r=>r.file===f.file);
 requireThat(rows.length===1&&rows[0].bytes===f.bytes&&rows[0].sha256===f.sha256,'CONTACT_QT_EXACT_FINAL_DEFINER_PIN_REQUIRED');return f;
}
export function assertApprovedFinalDefinerImages(row,images,f){
 requireThat(exactFinalDefinerForward(f)&&row.file===f.file&&row.bytes===f.bytes&&row.sha256===f.sha256,'CONTACT_QT_EXACT_FINAL_DEFINER_FORWARD_REQUIRED');
 const {saved,index,current,parent,approved,proof,review}=images;
 requireThat([saved,index,current,parent,approved,proof,review].every(Buffer.isBuffer),'CONTACT_QT_FINAL_DEFINER_RAW_IMAGES_REQUIRED');
 assertPinnedSourceImages(row,{saved,index,current,parent,parentForward:f});
 requireThat(saved.equals(approved)&&approved.length===f.bytes&&sha(approved)===f.sha256,'CONTACT_QT_FINAL_DEFINER_APPROVED_COMMIT_REQUIRED');
 requireThat(proof.length===f.proofBytes&&sha(proof)===f.proofSha256&&review.length===f.reviewBytes&&sha(review)===f.reviewSha256,'CONTACT_QT_FINAL_DEFINER_COMMITTED_REVIEW_REQUIRED');
 const p=JSON.parse(proof),r=JSON.parse(review),before="  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });",after="  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });";
 requireThat(p.parent==='69dde78cd4d26d863cb7aa1f3aeb53d61bbc088f'&&p.target===f.file&&p.beforeSha256===f.parentSha256&&p.afterSha256===f.sha256&&p.hunks.length===1&&p.hunks[0].before===before&&p.hunks[0].after===after&&p.newMaxBufferBytes===8388608&&p.sourceChanges===1&&p.sqlRuntimePolicyChanges===0&&p.unchangedReleaseBaseCommit==='a6f7423eb71e6e7cd173e705c2290e5408a1d077','CONTACT_QT_FINAL_DEFINER_ONE_LINE_PROOF_REQUIRED');
 requireThat(r.verdict==='ACCEPT_ONE_LINE_COMPLETE_PATH_BUFFER_FORWARD'&&r.authorCommit==='8b846bf0dd1057cf4bd1ae1908088571d8771641'&&r.peerCommit==='3b837183c1dbe5b7817638fa628393074c3fabce'&&r.target===f.file&&r.beforeSha256===f.parentSha256&&r.afterSha256===f.sha256&&r.allOriginalPathInputsBaseAncestryAssertionsPreserved===true&&r.libSourceGatesRegistrySqlNativeCapsPermissionsUnchanged===true,'CONTACT_QT_FINAL_DEFINER_ROOT_REVIEW_REQUIRED');
 const text=saved.toString('utf8');requireThat(text.split(after).length===2&&Buffer.from(text.replace(after,()=>before)).equals(parent),'CONTACT_QT_FINAL_DEFINER_WHOLE_RAW_INVERSE_REQUIRED');return true;
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
 const mechanical=assertApprovedFinalDefinerForward(pins);
 git(root,['merge-base','--is-ancestor',pins.rootCommit,head]);
 git(root,['merge-base','--is-ancestor',mechanical.approvedCommit,head]);
 try{git(root,['diff','--cached','--quiet',head,'--',...pins.rootInputs.map(r=>r.file)]);}catch{throw Error('CONTACT_QT_ROOT_INDEX_SOURCE_CHANGED');}
 const parentForwardFor=row=>forward?.find(r=>r.file===row.file)??(row.file===mechanical.file?mechanical:null);
 const images=gitBlobs(root,[...pins.rootInputs.flatMap(row=>[{spec:':'+row.file,bytes:row.bytes},{spec:head+':'+row.file,bytes:row.bytes},{spec:pins.rootCommit+':'+row.file,bytes:parentForwardFor(row)?.parentBytes??row.bytes}]),
  ...head===mechanical.approvedCommit?[]:[{spec:mechanical.approvedCommit+':'+mechanical.file,bytes:mechanical.bytes}],
  {spec:mechanical.approvedCommit+':'+mechanical.proofFile,bytes:mechanical.proofBytes},{spec:mechanical.approvedCommit+':'+mechanical.reviewFile,bytes:mechanical.reviewBytes}]);
 for(const row of pins.rootInputs){const b=fs.readFileSync(root+'/'+row.file);requireThat(b.length===row.bytes&&sha(b)===row.sha256,'CONTACT_QT_REVIEWED_ROOT_SOURCE_CHANGED');const frame={saved:b,index:images.get(':'+row.file),current:images.get(head+':'+row.file),parent:images.get(pins.rootCommit+':'+row.file)};if(row.file===mechanical.file)assertApprovedFinalDefinerImages(row,{...frame,approved:images.get(mechanical.approvedCommit+':'+row.file),proof:images.get(mechanical.approvedCommit+':'+mechanical.proofFile),review:images.get(mechanical.approvedCommit+':'+mechanical.reviewFile)},mechanical);else assertPinnedSourceImages(row,{...frame,parentForward:forward?.find(r=>r.file===row.file)??null});}
 const expected=JSON.parse(fs.readFileSync(here+'/PROGRAMMES.json'))[caseId],programme=await buildProgram({root,caseId});
 requireThat(expected&&programme.sqlSha256===expected.sqlSha256&&programme.sqlBytes===expected.sqlBytes&&programme.statements===expected.statements,'CONTACT_QT_SEALED_PROGRAMME_REQUIRED');
 if(fs.existsSync(here+'/MANIFEST.json'))for(const row of JSON.parse(fs.readFileSync(here+'/MANIFEST.json')).files)requireThat(sha(fs.readFileSync(here+'/'+row.file))===row.sha256,'CONTACT_QT_ADAPTER_SOURCE_CHANGED');
 const load=file=>import(pathToFileURL(root+'/'+file).href),rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),sql=await load('scripts/cloudtms-managed-release-sql.mjs'),control=await load('scripts/healthroster-turn-control.mjs');
 requireThat(JSON.stringify(live.backgroundRelations)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']),'CONTACT_QT_BOTH_BACKGROUND_RELATIONS_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(root+'/supabase/release/current-contract.json')),writers=sortWriters(contract.routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 requireThat(writers.length===4&&sha(JSON.stringify(writers))===pins.expectedWritersSha256,'CONTACT_QT_ORIGINAL_WRITER_AUTHORITY_CHANGED');
 return {root,programme,rehearsal,live,sql,control,expectedWriters:writers,pins};
}
