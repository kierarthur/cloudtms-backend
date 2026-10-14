import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees',root=path.join(base,'healthroster-v2-backend-13102026'),out=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const packets=[
{lane:'protection',folder:'cn-a0-phantom-writers',manifest:'CREATOR_SOURCE_MANIFEST.json',hash:'762f65ac35f11f1c33b779799854c1373eb0ccf9c624e44e6cebc70266cdb33e',key:'path',artifacts:19,status:'FOCUSED_CRITIC_TWO_P2_EVIDENCE_DEFECTS_ACCEPTED_FORWARD_UNMOUNTED_NATIVE_HELD'},
{lane:'money',folder:'protected-source-qualification-operational-gate',manifest:'REVIEW_MANIFEST.json',hash:'66b5b97d5628be25a0555c20c3a5755bf9561bd760afcaaaaed1b9b09f71b700',key:'file',artifacts:44,status:'FROZEN_SOURCE_PROPOSAL_PENDING_FULL_ROOT_AND_INDEPENDENT_REVIEW'}
];
const writes=[],receipts=[];
for(const q of packets){
 const lane=path.join(base,'healthroster-v2-'+q.lane+'-build-13102026'),prefix='docs/healthroster-build/proposals/'+q.folder,manifestRel=prefix+'/'+q.manifest,body=fs.readFileSync(path.join(lane,manifestRel));
 if(sha(body)!==q.hash)throw Error('MANIFEST_DRIFT '+q.folder);const m=JSON.parse(body);if(m.files.length!==q.artifacts)throw Error('ARTIFACT_COUNT_DRIFT');
 for(const row of m.files){const rel=row[q.key];if(typeof rel!=='string'||rel.includes('..')||!rel.startsWith(prefix+'/')&&!rel.startsWith('tests/'))throw Error('ARTIFACT_PATH_REFUSED');
 const bytes=fs.readFileSync(path.join(lane,rel));if(bytes.length!==row.bytes||sha(bytes)!==row.sha256)throw Error('ARTIFACT_BYTES_DRIFT '+rel);writes.push({rel,bytes});}
 writes.push({rel:manifestRel,bytes:body});receipts.push({folder:q.folder,manifestSha256:q.hash,fileCount:m.files.length+1,status:q.status});
}
const seen=new Set();for(const w of writes){if(seen.has(w.rel))throw Error('DUPLICATE_TARGET');seen.add(w.rel);const target=path.join(root,w.rel);if(fs.existsSync(target)&&sha(fs.readFileSync(target))!==sha(w.bytes))throw Error('EXISTING_ROOT_DRIFT '+w.rel);}
for(const w of writes){const target=path.join(root,w.rel);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,w.bytes);}
const receipt={status:'FROZEN_SOURCE_EVIDENCE_PRESERVED_NO_RUNTIME_INTEGRATION',runtimeFilesChanged:0,files:writes.length,packets:receipts,scope:'Immutable proposal evidence only. Current root2205/2206/registry/runtime owners untouched; canonical2206 forward and separate pin-forward receipts remain distinct. No native acceptance or final PartB.'};
const json=JSON.stringify(receipt,null,2)+'\n';fs.writeFileSync(path.join(out,'CREATOR_STRICT_QUALIFICATION_FROZEN_PRESERVATION.json'),json);fs.writeFileSync(path.join(root,'docs/healthroster-build/checkpoint13102026/CREATOR_STRICT_QUALIFICATION_FROZEN_PRESERVATION.json'),json);console.log(JSON.stringify(receipt));

