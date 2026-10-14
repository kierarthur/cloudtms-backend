import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees';
const root=path.join(base,'healthroster-v2-backend-13102026'),lane=path.join(base,'healthroster-v2-report-build-13102026');
const folder='docs/healthroster-build/proposals/automatic-native-delta-root-integrator',out=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),manifestBytes=fs.readFileSync(path.join(lane,folder,'SOURCE_MANIFEST.json'));
if(sha(manifestBytes)!=='c71ac96db7c8c48fc29885852be5ad69d67a722515d3414cd4f605385db4e16c')throw Error('AUTOMATIC_INTEGRATOR_MANIFEST_CHANGED');
const manifest=JSON.parse(manifestBytes),copies=[];
for(const row of manifest.artifacts){
 if(typeof row.path!=='string'||row.path.split('/').includes('..')||path.isAbsolute(row.path))throw Error('PACKET_PATH_REFUSED');
 const bytes=fs.readFileSync(path.join(lane,folder,row.path));if(bytes.length!==row.bytes||sha(bytes)!==row.sha256)throw Error('PACKET_HASH_CHANGED');
 copies.push({relative:folder+'/'+row.path,bytes});
}
copies.push({relative:folder+'/SOURCE_MANIFEST.json',bytes:manifestBytes});
for(const row of copies){const target=path.join(root,row.relative);if(fs.existsSync(target)&&sha(fs.readFileSync(target))!==sha(row.bytes))throw Error('FROZEN_ROOT_PACKET_CHANGED');}
const integration=await import(pathToFileURL(path.join(lane,folder,'integrator.mjs')));
const plan=await integration.prepare(root,{expectedHead:'9fc45b9a4cc3a8dce400887f1640cfaf233f23be'});
integration.validatePrepared(plan);
for(const row of copies){const target=path.join(root,row.relative);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,row.bytes);}
const result=integration.applyPrepared(plan,{authorisedSourceWrite:true});
const receipt={...result,inputCommit:plan.metadata.observedHead,laneCommit:'e837e900d78d58070e24fb252bf43b8464563169',
 manifestSha256:sha(manifestBytes),frozenPacketFilesPreserved:copies.length,counts:plan.counts,
 completeReverseProofs:plan.proofs,projectionRepairRebases:plan.projectionRebases,runtime:plan.runtime,
 sourceOnly:true,databaseCalls:false,featuresActivated:false,generatedContract:false};
const json=JSON.stringify(receipt,null,2)+'\n';
fs.writeFileSync(path.join(out,'AUTOMATIC_NATIVE_SOURCE_ROOT_INTEGRATION.json'),json);
fs.writeFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/AUTOMATIC_NATIVE_SOURCE_ROOT_INTEGRATION.json'),json);
console.log(JSON.stringify({status:result.status,outputs:result.outputs.length,preserved:copies.length,counts:plan.counts}));
