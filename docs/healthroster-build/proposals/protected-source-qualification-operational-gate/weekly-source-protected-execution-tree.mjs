import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
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
 prefixes:Object.freeze(['broker/','shared/','candidate-broker/','candidate-private-api/','candidate-synthetic-private-api/',
  'invoice-private-api/','invoice-renderer/','invoice-render-worker/','supabase/','scripts/','tests/','.github/']),
 files:Object.freeze(['.node-version','.npmrc','.gitmodules','package.json','package-lock.json','pnpm-lock.yaml','yarn.lock',
  'wrangler.toml','wrangler.jsonc','tsconfig.json']),
 manifest:'supabase/release/current-release.json',
 receiptBundles:Object.freeze({weeklySourceProtectedPrerequisites:'supabase/release/weekly-source-protected-prerequisites.json',
  weeklySourceProtectedOperational:'supabase/release/weekly-source-protected-operational.json',
  weeklySourceProtectedTestSetup:'supabase/release/weekly-source-protected-test-setup.json'}),
 required:Object.freeze(['.node-version','.github/workflows/database-release.yml','supabase/release/export_contract.sql',
  'supabase/release/current-release.json','supabase/release/current-contract.json','scripts/cloudtms-db-release.mjs'])
});
const selected=file=>sourceInventory.files.includes(file)||sourceInventory.prefixes.some(prefix=>file.startsWith(prefix));
const receiptPath=file=>Object.values(sourceInventory.receiptBundles).includes(file);
const pathCompare=(a,b)=>Buffer.compare(Buffer.from(a.path),Buffer.from(b.path));
export function parseGitTree(bytes){
 const rows=bytes.toString('utf8').split('\0').filter(Boolean).map(line=>{
  const match=line.match(/^([0-9]{6}) (blob|tree|commit) ([0-9a-f]{40})\t([\s\S]+)$/);
  if(!match)fail('QUALIFICATION_GIT_TREE_INVALID');
  return {mode:match[1],type:match[2],blob:match[3],path:match[4]};
 }).filter(row=>selected(row.path)).sort(pathCompare);
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
export function assertTreeEquivalence(left,right,readBlob){
 const nativeLeft=left.filter(row=>!receiptPath(row.path)),nativeRight=right.filter(row=>!receiptPath(row.path));
 if(nativeLeft.length!==nativeRight.length)fail('QUALIFICATION_SOURCE_SET_CHANGED');
 const files=nativeLeft.map((before,index)=>{
  const after=nativeRight[index];
  if(!after||before.path!==after.path||before.mode!==after.mode||before.type!==after.type)
   fail('QUALIFICATION_SOURCE_MODE_TYPE_OR_PATH_CHANGED');
  const beforeBytes=readBlob(before.blob),afterBytes=readBlob(after.blob);
  let comparison='EXACT_GIT_OBJECT',comparisonHash=sha(beforeBytes);
  if(before.path===sourceInventory.manifest){
   const l=releaseAuthority(beforeBytes),r=releaseAuthority(afterBytes);
   if(releaseAuthorityHash(l.authority)!==releaseAuthorityHash(r.authority))
    fail('QUALIFICATION_RELEASE_AUTHORITY_CHANGED');
   comparison='RELEASE_AUTHORITY_EXCEPT_CLOSED_RECEIPT_DESCRIPTORS';comparisonHash=releaseAuthorityHash(l.authority);
  }else if(before.blob!==after.blob||!beforeBytes.equals(afterBytes))fail('QUALIFICATION_NATIVE_SOURCE_CHANGED');
  return {path:before.path,git_mode:before.mode,git_type:before.type,execution_blob_oid:before.blob,installation_blob_oid:after.blob,
   execution_sha256:sha(beforeBytes),installation_sha256:sha(afterBytes),comparison,comparison_sha256:comparisonHash};
 });
 const receiptBindings=[];
 for(const [role,rows] of [['EXECUTION',left],['INSTALLATION',right]]){
  const manifest=releaseAuthority(readBlob(rows.find(row=>row.path===sourceInventory.manifest).blob));
  for(const [key,descriptor] of Object.entries(manifest.descriptors)){
   const row=rows.find(row=>row.path===descriptor.path);
   if(!row||sha(readBlob(row.blob))!==descriptor.sha256)fail('QUALIFICATION_RECEIPT_BUNDLE_UNBOUND');
   receiptBindings.push({role,key,path:row.path,git_mode:row.mode,git_type:row.type,blob_oid:row.blob,sha256:descriptor.sha256});
  }
  if(rows.some(row=>receiptPath(row.path)&&!Object.values(manifest.descriptors).some(d=>d.path===row.path)))
   fail('QUALIFICATION_RECEIPT_BUNDLE_UNDESCRIBED');
 }
 return {files,receipt_bindings:receiptBindings};
}
export function proveCommittedSources(root,executionCommit,installationCommit){
 if(!commit.test(executionCommit)||!commit.test(installationCommit))fail('QUALIFICATION_SOURCE_COMMIT_INVALID');
 const git=(args,input)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],
  {cwd:root,input,encoding:null,maxBuffer:256*1024*1024});
 const left=parseGitTree(git(['ls-tree','-r','-z',executionCommit])),right=parseGitTree(git(['ls-tree','-r','-z',installationCommit]));
 const ids=[...new Set([...left,...right].map(row=>row.blob))],raw=git(['cat-file','--batch'],ids.join('\n')+'\n');
 const blobs=new Map();let offset=0;
 for(const id of ids){
  const end=raw.indexOf(10,offset),header=raw.subarray(offset,end).toString('ascii').match(/^([0-9a-f]{40}) blob ([0-9]+)$/);
  if(end<0||!header||header[1]!==id)fail('QUALIFICATION_GIT_BLOB_INVALID');
  const size=Number(header[2]),start=end+1,bytes=raw.subarray(start,start+size);offset=start+size+1;
  if(!Number.isSafeInteger(size)||bytes.length!==size||raw[offset-1]!==10)fail('QUALIFICATION_GIT_BLOB_INVALID');blobs.set(id,bytes);
 }
 if(offset!==raw.length)fail('QUALIFICATION_GIT_BLOB_INVALID');
 const census=assertTreeEquivalence(left,right,id=>{if(!blobs.has(id))fail('QUALIFICATION_GIT_BLOB_MISSING');return blobs.get(id);});
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
