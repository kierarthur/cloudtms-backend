import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';
import {domainHash,exact} from './weekly-source-protected-release-qualification.mjs';
const fail=code=>{throw Error(code);};
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const commit=/^[0-9a-f]{40}$/;
// Release/toolchain metadata only. Safe integer version/count fields are
// allowed here; this serializer is never an economic/money authority.
function metadataJsonbText(value){
 if(value===null||typeof value==='boolean'||typeof value==='string')return JSON.stringify(value);
 if(typeof value==='number'&&Number.isSafeInteger(value))return String(value);
 if(Array.isArray(value))return '['+value.map(metadataJsonbText).join(', ')+']';
 if(value&&typeof value==='object')return '{'+Object.keys(value).sort((a,b)=>Buffer.byteLength(a)-Buffer.byteLength(b)
  ||Buffer.compare(Buffer.from(a),Buffer.from(b))).map(k=>JSON.stringify(k)+': '+metadataJsonbText(value[k])).join(', ')+'}';
 fail('QUALIFICATION_RELEASE_METADATA_INVALID');
}
export const releaseAuthorityHash=value=>sha('RELEASE_MANIFEST_AUTHORITY'+'\x1f'+metadataJsonbText(value));
export const sourceInventory=Object.freeze({
 // Every tracked object is selected, including root configuration, docs,
 // AGENTS, proposals and retained native-result files. There is no guessed
 // directory/config whitelist or documentary exclusion.
 selection:'ALL_TRACKED_GIT_OBJECTS',
 manifest:'supabase/release/current-release.json',
 receiptBundles:Object.freeze({weeklySourceProtectedPrerequisites:'supabase/release/weekly-source-protected-prerequisites.json',
  weeklySourceProtectedOperational:'supabase/release/weekly-source-protected-operational.json',
  weeklySourceProtectedTestSetup:'supabase/release/weekly-source-protected-test-setup.json'}),
 required:Object.freeze(['.node-version','.gitattributes','osv-scanner.toml','.github/workflows/database-release.yml','supabase/release/export_contract.sql',
  'supabase/release/current-release.json','supabase/release/current-contract.json','scripts/cloudtms-db-release.mjs'])
});
const selected=file=>typeof file==='string'&&file.length>0&&!file.startsWith('/')&&!file.includes('\\')
 &&!file.split('/').some(part=>part==='..'||part==='.'||part==='');
const receiptPath=file=>Object.values(sourceInventory.receiptBundles).includes(file);
const pathCompare=(a,b)=>Buffer.compare(Buffer.from(a.path),Buffer.from(b.path));
export function parseGitTree(bytes){
 if(!Buffer.isBuffer(bytes))fail('QUALIFICATION_GIT_TREE_INVALID');
 let decoded;
 try{decoded=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}
 catch{fail('QUALIFICATION_GIT_PATH_UTF8_INVALID');}
 if(!Buffer.from(decoded,'utf8').equals(bytes))fail('QUALIFICATION_GIT_PATH_UTF8_INVALID');
 const records=decoded.split('\0');
 if(records.pop()!==''||records.some(record=>record.length===0))fail('QUALIFICATION_GIT_TREE_INVALID');
 const rows=records.map(line=>{
  const match=line.match(/^([0-9]{6}) (blob|tree|commit) ([0-9a-f]{40})\t([\s\S]+)$/);
  if(!match)fail('QUALIFICATION_GIT_TREE_INVALID');
  // All raw records are admitted or refused; none is filtered away.
  // Fatal UTF8 plus roundtrip keeps the original path bytes authoritative.
  if(!selected(match[4])||Buffer.from(match[4],'utf8').toString('utf8')!==match[4])
   fail('QUALIFICATION_GIT_PATH_UNSUPPORTED');
  return {mode:match[1],type:match[2],blob:match[3],path:match[4]};
 }).sort(pathCompare);
 if(!rows.length||new Set(rows.map(row=>row.path)).size!==rows.length)fail('QUALIFICATION_SOURCE_INVENTORY_INVALID');
 if(rows.some(row=>row.type!=='blob'||!['100644','100755'].includes(row.mode)))fail('QUALIFICATION_EXECUTION_OBJECT_UNSUPPORTED');
 if(rows.some(row=>/(^|\/)(node_modules|\.codex-tmp|\.env|\.dev\.vars|.*\.secret)(\.|\/|$)/.test(row.path)))
  fail('QUALIFICATION_TRACKED_PRIVATE_PATH');
 for(const file of sourceInventory.required)if(!rows.some(row=>row.path===file))fail('QUALIFICATION_REQUIRED_SOURCE_MISSING');
 return rows;
}
// Only the release identifier and the three closed evidence descriptors can
// differ. Every verifier/security/control/install/exception/contract field is
// compared. Descriptors and their committed artifact bytes are bound separately.
export function releaseAuthority(bytes){
 const value=JSON.parse(bytes.toString('utf8'));
 if(!value||Array.isArray(value)||typeof value!=='object'||typeof value.releaseId!=='string'||!value.releaseId)
  fail('QUALIFICATION_RELEASE_MANIFEST_INVALID');
 const authority={...value};delete authority.releaseId;
 const descriptors={};
 for(const [key,file] of Object.entries(sourceInventory.receiptBundles)){
  if(value[key]===undefined)continue;
  exact(value[key],['path','sha256'],'QUALIFICATION_EVIDENCE_DESCRIPTOR_INVALID');
  if(value[key].path!==file||!/^[0-9a-f]{64}$/.test(value[key].sha256))fail('QUALIFICATION_EVIDENCE_DESCRIPTOR_INVALID');
  descriptors[key]=value[key];delete authority[key];
 }
 return {authority,descriptors};
}
function assertMeasuredTreeEquivalence(left,right,readBlob,readSha){
 const nativeLeft=left.filter(row=>!receiptPath(row.path)),nativeRight=right.filter(row=>!receiptPath(row.path));
 if(nativeLeft.length!==nativeRight.length)fail('QUALIFICATION_SOURCE_SET_CHANGED');
 const files=nativeLeft.map((before,index)=>{
  const after=nativeRight[index];
  if(!after||before.path!==after.path||before.mode!==after.mode||before.type!==after.type)
   fail('QUALIFICATION_SOURCE_MODE_TYPE_OR_PATH_CHANGED');
  const beforeDigest=readSha(before.blob),afterDigest=readSha(after.blob);
  let comparison='EXACT_GIT_OBJECT',comparisonHash=beforeDigest;
  if(before.path===sourceInventory.manifest){
   const beforeBytes=readBlob(before.blob),afterBytes=readBlob(after.blob);
   const l=releaseAuthority(beforeBytes),r=releaseAuthority(afterBytes);
   if(releaseAuthorityHash(l.authority)!==releaseAuthorityHash(r.authority))
    fail('QUALIFICATION_RELEASE_AUTHORITY_CHANGED');
   comparison='RELEASE_AUTHORITY_EXCEPT_CLOSED_RECEIPT_DESCRIPTORS';comparisonHash=releaseAuthorityHash(l.authority);
  }else if(before.blob!==after.blob||beforeDigest!==afterDigest)fail('QUALIFICATION_NATIVE_SOURCE_CHANGED');
  return {path:before.path,git_mode:before.mode,git_type:before.type,execution_blob_oid:before.blob,installation_blob_oid:after.blob,
   execution_sha256:beforeDigest,installation_sha256:afterDigest,comparison,comparison_sha256:comparisonHash};
 });
 const receiptBindings=[];
 for(const [role,rows] of [['EXECUTION',left],['INSTALLATION',right]]){
  const manifest=releaseAuthority(readBlob(rows.find(row=>row.path===sourceInventory.manifest).blob));
  for(const [key,descriptor] of Object.entries(manifest.descriptors)){
   const row=rows.find(row=>row.path===descriptor.path);
   if(!row||readSha(row.blob)!==descriptor.sha256)fail('QUALIFICATION_RECEIPT_BUNDLE_UNBOUND');
   receiptBindings.push({role,key,path:row.path,git_mode:row.mode,git_type:row.type,blob_oid:row.blob,sha256:descriptor.sha256});
  }
  if(rows.some(row=>receiptPath(row.path)&&!Object.values(manifest.descriptors).some(d=>d.path===row.path)))
   fail('QUALIFICATION_RECEIPT_BUNDLE_UNDESCRIBED');
 }
 return {files,receipt_bindings:receiptBindings};
}
export function assertTreeEquivalence(left,right,readBlob){
 return assertMeasuredTreeEquivalence(left,right,readBlob,id=>sha(readBlob(id)));
}
// Full tracked objects are measured from their actual raw Git bytes. The finite
// synchronous public API is retained. Only digests/size metadata are retained;
// no complete native blob or whole-tree raw output is accumulated in memory.
const gitBatchBudget=32*1024*1024,gitChunkBytes=64*1024;
function assertOwnedProofPath(root,base,temporary){
 const realRoot=fs.realpathSync(root),realBase=fs.realpathSync(base),relative=path.relative(realRoot,realBase);
 if(relative!=='.codex-tmp'||fs.lstatSync(base).isSymbolicLink()||!fs.lstatSync(base).isDirectory())
  fail('QUALIFICATION_SOURCE_PROOF_SCRATCH_PATH_INVALID');
 if(temporary){const relativeTemp=path.relative(realBase,fs.realpathSync(temporary));
  if(!/^qualification-source-proof-[A-Za-z0-9]+$/.test(relativeTemp)||fs.lstatSync(temporary).isSymbolicLink()
   ||!fs.lstatSync(temporary).isDirectory())fail('QUALIFICATION_SOURCE_PROOF_SCRATCH_PATH_INVALID');}
 if(temporary&&typeof process.getuid==='function'){
  const info=fs.lstatSync(temporary);
  if(info.uid!==process.getuid()||(info.mode&0o077)!==0)fail('QUALIFICATION_SOURCE_PROOF_SCRATCH_OWNER_INVALID');
 }
}
function measureGitBlobs(root,ids,git){
 const checked=git(['cat-file','--batch-check=%(objectname) %(objecttype) %(objectsize)'],ids.join('\n')+'\n')
  .toString('utf8').trimEnd().split('\n');
 if(checked.length!==ids.length)fail('QUALIFICATION_GIT_BLOB_INVALID');
 const sizes=new Map();for(let i=0;i<ids.length;i++){
  const row=checked[i].match(/^([0-9a-f]{40}) blob ([0-9]+)$/),size=row?Number(row[2]):NaN;
  if(!row||row[1]!==ids[i]||!Number.isSafeInteger(size)||size<0)fail('QUALIFICATION_GIT_BLOB_INVALID');sizes.set(ids[i],size);
 }
 const batches=[];let current=[],cost=0;
 for(const id of ids){const size=sizes.get(id),next=size+Buffer.byteLength(id+' blob '+size+'\n')+1;
  if(!Number.isSafeInteger(next))fail('QUALIFICATION_GIT_BLOB_INVALID');
  if(current.length&&(cost+next>gitBatchBudget||current.length>=256)){batches.push(current);current=[];cost=0;}
  current.push(id);cost+=next;
  // A single blob above the batch budget is still admitted and fully hashed
  // in chunks. Its finite declared file length is checked; it is never skipped.
  if(cost>=gitBatchBudget){batches.push(current);current=[];cost=0;}
 }if(current.length)batches.push(current);
 const base=path.join(root,'.codex-tmp');fs.mkdirSync(base,{recursive:true});assertOwnedProofPath(root,base);
 const temporary=fs.mkdtempSync(path.join(base,'qualification-source-proof-')),output=path.join(temporary,'objects.batch');
 const digests=new Map();let openFd;
 try{
  assertOwnedProofPath(root,base,temporary);
  for(const batch of batches){
   assertOwnedProofPath(root,base,temporary);openFd=fs.openSync(output,'wx+',0o600);
   const result=spawnSync('git',['--no-replace-objects','-c','maintenance.auto=false','-c','gc.auto=0','cat-file','--batch'],
    {cwd:root,input:batch.join('\n')+'\n',encoding:null,stdio:['pipe',openFd,'pipe'],maxBuffer:1024*1024,timeout:120000,windowsHide:true});
   if(result.error||result.status!==0)fail('QUALIFICATION_GIT_OBJECT_STREAM_FAILED');
   // The same exclusive descriptor is Git's stdout and the measured stream.
   // A path replacement cannot redirect hashing onto another file identity.
   const length=fs.fstatSync(openFd).size;
   const expectedLength=batch.reduce((sum,id)=>sum+Buffer.byteLength(id+' blob '+sizes.get(id)+'\n')+sizes.get(id)+1,0);
   if(!Number.isSafeInteger(expectedLength)||length!==expectedLength)fail('QUALIFICATION_GIT_BLOB_INVALID');
   const chunk=Buffer.allocUnsafe(gitChunkBytes),header=Buffer.allocUnsafe(128);let offset=0;
   for(const id of batch){let headerLength=0;
    while(true){if(headerLength===header.length||fs.readSync(openFd,header,headerLength,1,offset++)!==1)fail('QUALIFICATION_GIT_BLOB_INVALID');
     if(header[headerLength++]===10)break;}
    const matched=header.subarray(0,headerLength-1).toString('utf8').match(/^([0-9a-f]{40}) blob ([0-9]+)$/);
    if(!matched||matched[1]!==id||Number(matched[2])!==sizes.get(id))fail('QUALIFICATION_GIT_BLOB_INVALID');
    let remaining=sizes.get(id);const hash=crypto.createHash('sha256');
    while(remaining){const count=fs.readSync(openFd,chunk,0,Math.min(chunk.length,remaining),offset);
     if(count<1)fail('QUALIFICATION_GIT_BLOB_INVALID');hash.update(chunk.subarray(0,count));offset+=count;remaining-=count;}
    if(fs.readSync(openFd,header,0,1,offset++)!==1||header[0]!==10)fail('QUALIFICATION_GIT_BLOB_INVALID');
    digests.set(id,hash.digest('hex'));
   }
   if(offset!==length)fail('QUALIFICATION_GIT_BLOB_INVALID');fs.closeSync(openFd);openFd=undefined;
   assertOwnedProofPath(root,base,temporary);fs.unlinkSync(output);
  }
  if(digests.size!==ids.length)fail('QUALIFICATION_GIT_BLOB_MISSING');return digests;
 }finally{
  if(openFd!==undefined)try{fs.closeSync(openFd);}catch{}
  // Exact task-owned file and empty directory only. Never recursively remove a
  // workspace or another task's scratch. Cleanup failure refuses qualification.
  try{assertOwnedProofPath(root,base,temporary);if(fs.existsSync(output))fs.unlinkSync(output);fs.rmdirSync(temporary);}
  catch{fail('QUALIFICATION_SOURCE_PROOF_SCRATCH_CLEANUP_FAILED');}
 }
}
export function proveCommittedSources(root,executionCommit,installationCommit){
 if(!commit.test(executionCommit)||!commit.test(installationCommit))fail('QUALIFICATION_SOURCE_COMMIT_INVALID');
 const git=(args,input)=>execFileSync('git',['--no-replace-objects','-c','maintenance.auto=false','-c','gc.auto=0',...args],
  {cwd:root,input,encoding:null,maxBuffer:256*1024*1024});
 const left=parseGitTree(git(['ls-tree','-r','-z',executionCommit])),right=parseGitTree(git(['ls-tree','-r','-z',installationCommit]));
 const ids=[...new Set([...left,...right].map(row=>row.blob))],digests=measureGitBlobs(root,ids,git);
 // Release-authority JSON alone requires materialisation. It retains the old
 // finite capture bound; every other object's raw bytes have already been
 // completely hashed in bounded chunks, including the three receipt bundles.
 const manifestIds=new Set([...left,...right].filter(row=>row.path===sourceInventory.manifest).map(row=>row.blob)),manifests=new Map();
 for(const id of manifestIds){const bytes=git(['cat-file','blob',id]);if(sha(bytes)!==digests.get(id))fail('QUALIFICATION_GIT_BLOB_INVALID');manifests.set(id,bytes);}
 const census=assertMeasuredTreeEquivalence(left,right,id=>{if(!manifests.has(id))fail('QUALIFICATION_GIT_MANIFEST_MISSING');return manifests.get(id);},
  id=>{if(!digests.has(id))fail('QUALIFICATION_GIT_BLOB_MISSING');return digests.get(id);});
 const body={schema_version:'WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2',execution_source_commit:executionCommit,
  installation_source_commit:installationCommit,inventory_schema:'WEEKLY_SOURCE_NATIVE_RELEASE_TOOLCHAIN_VERIFIER_INVENTORY_V2',...census};
 return {...body,source_equivalence_sha256:domainHash(body.schema_version,body)};
}

export function assertExecutionTreeProof(proof,executionCommit,installationCommit){
 exact(proof,['schema_version','execution_source_commit','installation_source_commit','inventory_schema','files','receipt_bindings',
  'source_equivalence_sha256'],'QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
 const {source_equivalence_sha256,...body}=proof;
 if(proof.schema_version!=='WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2'
  ||proof.inventory_schema!=='WEEKLY_SOURCE_NATIVE_RELEASE_TOOLCHAIN_VERIFIER_INVENTORY_V2'
  ||proof.execution_source_commit!==executionCommit||proof.installation_source_commit!==installationCommit
  ||domainHash(proof.schema_version,body)!==source_equivalence_sha256||!Array.isArray(proof.files)||!proof.files.length
  ||!Array.isArray(proof.receipt_bindings)||new Set(proof.files.map(row=>row.path)).size!==proof.files.length)
  fail('QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
 for(const required of sourceInventory.required)if(!proof.files.some(row=>row.path===required))fail('QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
 for(const row of proof.files){
  exact(row,['path','git_mode','git_type','execution_blob_oid','installation_blob_oid','execution_sha256','installation_sha256',
   'comparison','comparison_sha256'],'QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
  if(!selected(row.path)||!['100644','100755'].includes(row.git_mode)||row.git_type!=='blob'
   ||['execution_blob_oid','installation_blob_oid'].some(k=>!/^[0-9a-f]{40}$/.test(row[k]))
   ||['execution_sha256','installation_sha256','comparison_sha256'].some(k=>!/^[0-9a-f]{64}$/.test(row[k])))
   fail('QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
  if(row.path===sourceInventory.manifest){if(row.comparison!=='RELEASE_AUTHORITY_EXCEPT_CLOSED_RECEIPT_DESCRIPTORS')fail('QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');}
  else if(row.comparison!=='EXACT_GIT_OBJECT'||row.execution_blob_oid!==row.installation_blob_oid
   ||row.execution_sha256!==row.installation_sha256)fail('QUALIFICATION_EXECUTION_TREE_PROOF_INVALID');
 }
 return proof;
}
export function loadCommittedBundle(root,release,context,key){
 const descriptor=release[key];if(descriptor===undefined)return null;
 const file=sourceInventory.receiptBundles[key];if(!file)fail('QUALIFICATION_BUNDLE_KIND_INVALID');
 exact(descriptor,['path','sha256'],'QUALIFICATION_EVIDENCE_DESCRIPTOR_INVALID');
 if(context.environment!=='TEST'||context.mode!=='UPGRADE'||context.expectedDatabase!=='cloudtms_test_clone'
  ||descriptor.path!==file||!/^[0-9a-f]{64}$/.test(descriptor.sha256))fail('QUALIFICATION_EVIDENCE_DESCRIPTOR_INVALID');
 const bytes=fs.readFileSync(path.join(root,file));
 const committed=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',context.gitCommit+':'+file],
  {cwd:root,encoding:null,maxBuffer:2*1024*1024});
 if(bytes.length>2*1024*1024||!bytes.equals(committed)||sha(bytes)!==descriptor.sha256)fail('QUALIFICATION_EVIDENCE_SOURCE_MISMATCH');
 return JSON.parse(bytes.toString('utf8'));
}
