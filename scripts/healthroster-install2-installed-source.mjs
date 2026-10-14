// Read only exact committed Git objects. No archive, working-copy substitution,
// database connection, credential, fetch, mutation or native-result claim.
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {canonicalContractHash,sqlDateKey} from './cloudtms-db-release-lib.mjs';
import {digest} from './cloudtms-managed-release-sql.mjs';

export function collectInstall2ReviewedInstalledSource({repositoryRoot,installedSourceCommit}){
 if(!path.isAbsolute(repositoryRoot)||!/^[a-f0-9]{40}$/.test(installedSourceCommit??''))throw Error('INSTALL2_INSTALLED_SOURCE_ARGUMENTS_REFUSED');
 const prefix=['-c','gc.auto=0','-c','maintenance.auto=false','-C',repositoryRoot];
 const git=args=>execFileSync('git',[...prefix,...args],{windowsHide:true,stdio:['pipe','pipe','pipe'],maxBuffer:96000000});
 const commit=git(['rev-parse','--verify',installedSourceCommit+'^{commit}']).toString('utf8').trim();
 if(commit!==installedSourceCommit)throw Error('INSTALL2_INSTALLED_COMMIT_OBJECT_REFUSED');
 const tree=git(['ls-tree','-r','-z',commit,'--','supabase/migrations','supabase/repeatable',
  'supabase/release/current-contract.json','scripts/cloudtms-db-release-lib.mjs']).toString('utf8').split('\0').filter(Boolean);
 const entries=tree.map(line=>{
  const match=line.match(/^100(?:644|755) blob ([a-f0-9]{40})\t([^\0\r\n]+)$/);
  if(!match)throw Error('INSTALL2_INSTALLED_TREE_ENVELOPE_REFUSED');
  return {oid:match[1],file:match[2]};
 });
 if(entries.length===0||new Set(entries.map(row=>row.file)).size!==entries.length)throw Error('INSTALL2_INSTALLED_TREE_EMPTY_OR_DUPLICATE');
 const ids=[...new Set(entries.map(row=>row.oid))];
 const batch=execFileSync('git',[...prefix,'cat-file','--batch'],{input:ids.join('\n')+'\n',windowsHide:true,stdio:['pipe','pipe','pipe'],maxBuffer:96000000});
 const objects=new Map();let offset=0;
 for(const oid of ids){
  const newline=batch.indexOf(10,offset);
  if(newline<offset)throw Error('INSTALL2_INSTALLED_BATCH_HEADER_REFUSED');
  const match=batch.subarray(offset,newline).toString('utf8').match(/^([a-f0-9]{40}) blob (\d+)$/);
  const bytes=Number(match?.[2]);
  if(!match||match[1]!==oid||!Number.isSafeInteger(bytes)||bytes<0||newline+1+bytes>=batch.length)
   throw Error('INSTALL2_INSTALLED_BATCH_ENVELOPE_REFUSED');
  const end=newline+1+bytes;
  if(batch[end]!==10)throw Error('INSTALL2_INSTALLED_BATCH_TERMINATOR_REFUSED');
  objects.set(oid,batch.subarray(newline+1,end));offset=end+1;
 }
 if(offset!==batch.length)throw Error('INSTALL2_INSTALLED_BATCH_TRAILING_DATA_REFUSED');
 const sources=new Map(entries.map(row=>[row.file,objects.get(row.oid)]));
 const read=file=>{
  const bytes=sources.get(file);if(!bytes)throw Error('INSTALL2_INSTALLED_SOURCE_INCLUDE_MISSING');
  return Buffer.from(bytes.toString('utf8').replaceAll('\r\n','\n'),'utf8');
 };
 // inventory.sqlFiles includes all tracked nested SQL pages as roots, in
 // addition to root-level disk files. Git-only proof must retain every one.
 const roots=folder=>entries.map(row=>row.file).filter(file=>file.startsWith(folder+'/')&&/\.sql$/i.test(file))
  .sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
 const closure=relative=>{
  const active=new Set(),ordered=[];
  function visit(file){
   if(!file.startsWith('supabase/repeatable/')||file.split('/').includes('..')||active.has(file))
    throw Error('INSTALL2_INSTALLED_CLOSURE_ESCAPE_OR_CYCLE');
   active.add(file);const bytes=read(file);ordered.push({path:file,bytes});
   for(const line of bytes.toString('utf8').split(/\r?\n/)){
    // Exact canonical inventory closureFor line parser, preserving duplicates.
    const match=line.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
    if(match)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3])));
   }
   active.delete(file);
  }
  visit(relative);
  return {path:relative,paths:ordered.map(row=>row.path),sha256:digest(Buffer.concat(ordered.flatMap(row=>[Buffer.from(row.path+'\0'),row.bytes,Buffer.from('\0')])))};
 };
 const inventory={migrations:roots('supabase/migrations').map(file=>({path:file,sha256:digest(read(file))})),
  repeatables:roots('supabase/repeatable').map(closure)};
 if(!inventory.migrations.length||!inventory.repeatables.length)throw Error('INSTALL2_INSTALLED_SOURCE_INVENTORY_EMPTY');
 const contract=JSON.parse(read('supabase/release/current-contract.json').toString('utf8'));
 return {commit,source:'EXACT_COMMITTED_GIT_OBJECTS',checkedAt:new Date().toISOString(),contract,inventory,
  contractHash:canonicalContractHash(contract),canonicalInventoryLibrarySourceHash:digest(read('scripts/cloudtms-db-release-lib.mjs')),
  sourceObjectCount:entries.length,distinctBlobCount:ids.length,sourceTreeEnvelopeSha256:digest(tree.join('\0')+'\0'),
  nativeDatabaseProbed:false,permanentWritesPerformed:false};
}
