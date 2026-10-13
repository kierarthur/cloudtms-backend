import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees',root=path.join(base,'healthroster-v2-backend-13102026'),out=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const packets=[
{lane:'protection',folder:'cn-a0-creator-census-forward',manifest:'CENSUS_FORWARD_SOURCE_MANIFEST.json',hash:'dd3c0d77e2b7a5edc6641d4de0d8220c187e0b867d8af57c2f1d4c470b4e8d42',key:'path',artifacts:16,status:'FOCUSED_SOURCE_P2_CORRECTIONS_ACCEPTED_NATIVE_RR_HELD'},
{lane:'report',folder:'report-prepared-output-retention',manifest:'SOURCE_MANIFEST.json',hash:'80a2577cbf5016f1a76e323717809c5553070e47b0b9a80e11b1ef3a0af34c70',key:'path',artifacts:29,status:'FROZEN_ORIGINAL_P2_ARRAY_FINDING_ACCEPTED_FORWARD'},
{lane:'report',folder:'report-prepared-output-array-forward',manifest:'SOURCE_MANIFEST.json',hash:'efc2881e0863a2bf09954742f8178819cb7e27d3bc30e79a56b92a6944fbd889',key:'path',artifacts:10,status:'ARRAY_SOURCE_CORRECTION_ACCEPTED_NATIVE_CAPTURE_FINANCIAL_HELD'},
{lane:'money',folder:'strict-qualification-first-use-inventory-forward',manifest:'REVIEW_MANIFEST.json',hash:'e05b9ec2574dcf83c7773171adb50e116f3181d28812ac6e8571dd79fab5c427',key:'file',artifacts:11,status:'FROZEN_FIRST_USE_INVENTORY_FIX_FURTHER_P2_PATH_CORRECTED_ADJACENT'},
{lane:'money',folder:'strict-qualification-git-path-parser-forward',manifest:'REVIEW_MANIFEST.json',hash:'06645fbf3c215137bf306405d03ce6817a78e5b8ce08948ad834e9e66a26d3d3',key:'file',artifacts:9,status:'FATAL_PATH_SOURCE_CORRECTION_ACCEPTED_NATIVE_OPERATIONAL_HELD'}
];
const writes=[],receipts=[];
for(const q of packets){
 const lane=path.join(base,'healthroster-v2-'+q.lane+'-build-13102026'),prefix='docs/healthroster-build/proposals/'+q.folder,manifestRel=prefix+'/'+q.manifest,body=fs.readFileSync(path.join(lane,manifestRel));
 if(sha(body)!==q.hash)throw Error('MANIFEST_DRIFT '+q.folder);const m=JSON.parse(body);if((m.files??m.artifacts).length!==q.artifacts)throw Error('ARTIFACT_COUNT_DRIFT');
 for(const row of (m.files??m.artifacts)){const raw=row[q.key];const rel=raw.startsWith(prefix+'/')||raw.startsWith('tests/')?raw:prefix+'/'+raw;if(typeof rel!=='string'||rel.includes('..')||!rel.startsWith(prefix+'/')&&!rel.startsWith('tests/'))throw Error('ARTIFACT_PATH_REFUSED');
 const bytes=fs.readFileSync(path.join(lane,rel));if(bytes.length!==row.bytes||sha(bytes)!==row.sha256)throw Error('ARTIFACT_BYTES_DRIFT '+rel);writes.push({rel,bytes});}
 writes.push({rel:manifestRel,bytes:body});receipts.push({folder:q.folder,manifestSha256:q.hash,fileCount:(m.files??m.artifacts).length+1,status:q.status});
}
const seen=new Set();for(const w of writes){if(seen.has(w.rel))throw Error('DUPLICATE_TARGET');seen.add(w.rel);const target=path.join(root,w.rel);if(fs.existsSync(target)&&sha(fs.readFileSync(target))!==sha(w.bytes))throw Error('EXISTING_ROOT_DRIFT '+w.rel);}
for(const w of writes){const target=path.join(root,w.rel);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,w.bytes);}
const receipt={status:'FROZEN_SOURCE_EVIDENCE_PRESERVED_NO_RUNTIME_INTEGRATION',runtimeFilesChanged:0,files:writes.length,packets:receipts,scope:'Immutable proposal evidence only. Current root2205/2206/registry/runtime owners untouched; canonical2206 forward and separate pin-forward receipts remain distinct. No native acceptance or final PartB.'};
const json=JSON.stringify(receipt,null,2)+'\n';fs.writeFileSync(path.join(out,'CREATOR_PREPARED_QUALIFICATION_FORWARD_PRESERVATION.json'),json);fs.writeFileSync(path.join(root,'docs/healthroster-build/checkpoint13102026/CREATOR_PREPARED_QUALIFICATION_FORWARD_PRESERVATION.json'),json);console.log(JSON.stringify(receipt));
