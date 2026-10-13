import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees',root=path.join(base,'healthroster-v2-backend-13102026'),out=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const packets=[
{lane:'money',folder:'protected-source-qualification-operational-gate-pin-forward-2206',manifest:'REVIEW_MANIFEST.json',hash:'10db4d8d3728129f429a0cfaf8d8670dbbf1125bcc9be53b60ff7d06b1664e8f',key:'file',artifacts:5,status:'HISTORICAL_CDAF_VERIFIER_PIN_FORWARD_ONLY'},
{lane:'money',folder:'protected-source-qualification-operational-gate-pin-forward-2206-trial2',manifest:'REVIEW_MANIFEST.json',hash:'4784752bda1a9c5df7edad45d99715fa114efd25b85c7810e9cdc7c01dd1e272',key:'file',artifacts:5,status:'D67_COMPLETE_VERIFIER_FORWARD_ONLY_NATIVE_GATES_HELD'}
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
const json=JSON.stringify(receipt,null,2)+'\n';fs.writeFileSync(path.join(out,'STRICT_QUALIFICATION_PIN_FORWARD_PRESERVATION.json'),json);fs.writeFileSync(path.join(root,'docs/healthroster-build/checkpoint13102026/STRICT_QUALIFICATION_PIN_FORWARD_PRESERVATION.json'),json);console.log(JSON.stringify(receipt));



