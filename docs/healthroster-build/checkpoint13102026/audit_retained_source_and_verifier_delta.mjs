import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026';
const out=path.dirname(fileURLToPath(import.meta.url));
const commit='17db6050abb50ba6c4bd77019dfb29c817e35529';
const original='547c74da5f0a05b09297c042bc7f3cc0c0105b85';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const canonical=b=>b.toString('utf8').replace(/\r\n/g,'\n');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:64000000});
const blobs=new Map();
const blob=file=>{if(!blobs.has(file))blobs.set(file,git(['show',commit+':'+file]));return blobs.get(file);};
if(git(['ls-remote','origin','refs/heads/codex/healthroster-v2-backend-13102026']).toString().trim().split(/\s+/)[0]!==commit)throw Error('REMOTE_SOURCE_DRIFT');
const checkpoint='docs/healthroster-build/checkpoint13102026/';
const preservation=JSON.parse(blob(checkpoint+'CREATOR_PREPARED_QUALIFICATION_FORWARD_PRESERVATION.json'));
let retainedCount=0;const packages=[];
for(const packet of preservation.packets){
  const prefix='docs/healthroster-build/proposals/'+packet.folder;
  const names=fs.readdirSync(path.join(root,prefix)).filter(n=>['SOURCE_MANIFEST.json','CENSUS_FORWARD_SOURCE_MANIFEST.json','REVIEW_MANIFEST.json'].includes(n));
  if(names.length!==1)throw Error('MANIFEST_IDENTITY_AMBIGUOUS');
  const manifestPath=prefix+'/'+names[0],bytes=blob(manifestPath);
  if(sha(bytes)!==packet.manifestSha256)throw Error('GIT_MANIFEST_DRIFT');
  const manifest=JSON.parse(bytes),rows=manifest.files??manifest.artifacts;
  for(const row of rows){
    const raw=row.path??row.file;
    const file=raw.startsWith(prefix+'/')||raw.startsWith('tests/')?raw:prefix+'/'+raw;
    if(file.includes('..')||!file.startsWith(prefix+'/')&&!file.startsWith('tests/'))throw Error('PATH_REFUSED');
    const saved=fs.readFileSync(path.join(root,file)),backedUp=blob(file);
    if(backedUp.length!==row.bytes||sha(backedUp)!==row.sha256||!backedUp.equals(saved))throw Error('GIT_ARTIFACT_DRIFT');
  }
  if(rows.length+1!==packet.fileCount)throw Error('COUNT_DRIFT');
  retainedCount+=rows.length+1;
  packages.push({...packet,gitCommit:commit,exactRawGitAndSavedBytesMatch:true});
}
if(retainedCount!==80)throw Error('RETAINED_COUNT_DRIFT');
const current=JSON.parse(blob(checkpoint+'FULL_VERIFIER_SOURCE_PROTOCOL_CENSUS.json'));
const release=JSON.parse(blob('supabase/release/current-release.json'));
if(JSON.stringify(release.verificationFiles)!==JSON.stringify(current.verifiers.map(r=>r.file)))throw Error('VERIFIER_ORDER_DRIFT');
for(const row of current.verifiers)for(const input of row.inputs){
  const bytes=blob(input.file),saved=fs.readFileSync(path.join(root,input.file));
  if(sha(bytes)!==input.gitBlobSha256||sha(canonical(bytes))!==input.canonicalSha256||canonical(bytes)!==canonical(saved))throw Error('CURRENT_VERIFIER_SOURCE_DRIFT');
}
const archiveDir='docs/healthroster-build/rehearsals/13102026/';
const old=JSON.parse(blob(archiveDir+'FULL_REHEARSAL_EXACT_SOURCE_PLAN.json'));
const review=JSON.parse(blob(archiveDir+'VERIFIER_REVIEW_INDEX.json'));
if(old.commit!==original||old.verifierUnits.length!==168||review.reviewed!==168||review.eligible!==152||review.excluded!==16)throw Error('ARCHIVE_SCOPE_DRIFT');
const oldByPath=new Map(old.verifierUnits.map(r=>[r.file,r]));
const excluded=new Map(review.exclusions.map(r=>[r.file??r.path,r]));
const rows=current.verifiers.map(row=>{
  const previous=oldByPath.get(row.file);
  const changedInputs=previous?previous.closures.filter(c=>sha(canonical(blob(c.file)))!==c.sha256).map(c=>c.file):[];
  const exclusion=excluded.get(row.file);
  return {index:row.index,file:row.file,sourceStatus:!previous?'NEW_VERIFIER_REVIEW_REQUIRED':changedInputs.length?'CHANGED_CLOSURE_REVIEW_REQUIRED':'ARCHIVED_VERIFIER_CLOSURE_UNCHANGED_NOT_EXECUTION_APPROVAL',changedInputs,originalExclusion:exclusion?{index:exclusion.index,decision:exclusion.decision,reason:exclusion.reason}:null,envelope:row.envelope.map(e=>e.command),sourceSqlHash:row.sourceSqlHash};
});
if(rows.filter(r=>r.originalExclusion).length!==16)throw Error('EXCLUSION_LOST');
const controllingSourceDelta=git(['diff','--name-only',original,commit,'--','supabase']).toString().trim().split(/\r?\n/).filter(Boolean);
const result={status:'SOURCE_BACKUP_AND_REHEARSAL_REVIEW_DEPENDENCIES_ONLY_NO_EXECUTION',sourceCommit:commit,exactRemoteVerified:true,retainedFiles:retainedCount,packages,original:{sourceCommit:original,reviewed:168,eligible:152,excluded:16},current:{verifiers:rows.length,unchangedArchivedClosures:rows.filter(r=>r.sourceStatus.startsWith('ARCHIVED')).length,changedClosures:rows.filter(r=>r.sourceStatus.startsWith('CHANGED')).length,newVerifiers:rows.filter(r=>r.sourceStatus.startsWith('NEW')).length,originalExclusionsRetained:16},repeatableReadRequiredSourceRoots:rows.filter(r=>r.envelope.includes('BEGIN ISOLATION LEVEL REPEATABLE READ')).map(r=>r.file),controllingSourceDelta,scope:'Unchanged verifier closures do not establish unchanged invoked controlling owners or installed semantics. Re-read changed/new bodies and all changed controlling/native callees against the actual post-Banking composed release. Original exclusions stay closed. Nine repeatable-read envelopes and native snapshot guards require explicit full-run isolation review; do not strip modes as a waiver or alter ordinary isolation. No database, credential, executor, release, fixture or operational acceptance.',verifiers:rows};
fs.writeFileSync(path.join(out,'SOURCE_BACKUP_AND_VERIFIER_DELTA_CHECKPOINT.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,sourceCommit:commit,retainedFiles:retainedCount,current:result.current,repeatableReadRoots:result.repeatableReadRequiredSourceRoots.length,controllingSourceDelta:controllingSourceDelta.length}));
