import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees';
const root=path.join(base,'healthroster-v2-backend-13102026');
const lane=path.join(base,'healthroster-v2-protection-build-13102026');
const out=path.dirname(fileURLToPath(import.meta.url));
const commit='9201cb609a9f952811c99460f55717ec6d3d9175';
const prefix='docs/healthroster-build/proposals/cn-a0-isolation-source-followup/';
const manifestFile=prefix+'ISOLATION_SOURCE_MANIFEST.json';
const expected='f031b83e3b17c609260ae34508e4f859f8d49f91277ca49c5f43c84851fc7e50';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(cwd,args)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,maxBuffer:32000000});
if(git(lane,['ls-remote','origin','refs/heads/codex/healthroster-v2-protection-build-13102026']).toString().trim().split(/\s+/)[0]!==commit)throw Error('LANE_REMOTE_DRIFT');
const manifestBytes=fs.readFileSync(path.join(lane,manifestFile));
if(sha(manifestBytes)!==expected||!git(lane,['show',commit+':'+manifestFile]).equals(manifestBytes))throw Error('MANIFEST_SAVED_OR_GIT_DRIFT');
const manifest=JSON.parse(manifestBytes);
if(manifest.files.length!==11||manifest.inputs.length!==1625)throw Error('PACKET_CENSUS_DRIFT');
if(git(root,['rev-parse','HEAD']).toString().trim()!==manifest.rootInputGitHead)throw Error('ROOT_INPUT_HEAD_DRIFT');
for(const row of manifest.inputs){
 if(typeof row.path!=='string'||row.path.includes('..')||path.isAbsolute(row.path))throw Error('INPUT_PATH_REFUSED');
 const bytes=fs.readFileSync(path.join(root,row.path));
 if(bytes.length!==row.bytes||sha(bytes)!==row.sha256)throw Error('ROOT_COMPLETE_INPUT_DRIFT');
}
const writes=[];
for(const row of manifest.files){
 if(typeof row.path!=='string'||row.path.includes('..')||!row.path.startsWith(prefix)&&row.path!=='tests/healthroster-cn-a0-isolation-source-followup.test.mjs')throw Error('OUTPUT_PATH_REFUSED');
 const bytes=fs.readFileSync(path.join(lane,row.path));
 if(bytes.length!==row.bytes||sha(bytes)!==row.sha256||!git(lane,['show',commit+':'+row.path]).equals(bytes))throw Error('OUTPUT_SAVED_OR_GIT_DRIFT');
 writes.push({file:row.path,bytes});
}
writes.push({file:manifestFile,bytes:manifestBytes});
for(const row of writes){const target=path.join(root,row.file);if(fs.existsSync(target)&&!fs.readFileSync(target).equals(row.bytes))throw Error('EXISTING_ROOT_OUTPUT_DRIFT');}
for(const row of writes){const target=path.join(root,row.file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,row.bytes);}
const result={status:'FROZEN_A0_ISOLATION_SOURCE_EVIDENCE_RETAINED_NO_RUNTIME_INTEGRATION',laneCommit:commit,rootInputCommit:manifest.rootInputGitHead,manifestSha256:expected,retainedFiles:writes.length,completeRootInputPinsVerified:manifest.inputs.length,rawLaneGitAndSavedBytesMatch:true,
 scannerCorrection:'Quoted internal char vs ordinary character aliases kept distinct; full current census unchanged. Await independent focused critic.',
 nativeHolds:['Actual deployed supported PostgREST/table/RPC isolation and hidden-P0 RR schedule','Validation-first/invoice-first/Candidate alias-rank/raw-booking/native FIN late-hook schedules','All financial/security/document/API/phone/release and final PartB gates'],
 newSqlSignatures:0,rootRuntimeSqlOrContractFilesChanged:0,databaseCalls:0,sourceGraphObservedNativeEdges:0};
const json=JSON.stringify(result,null,2)+'\n';
for(const dir of [out,path.join(root,'docs/healthroster-build/checkpoint13102026')])fs.writeFileSync(path.join(dir,'A0_ISOLATION_FOLLOWUP_PRESERVATION.json'),json);
console.log(JSON.stringify(result));
