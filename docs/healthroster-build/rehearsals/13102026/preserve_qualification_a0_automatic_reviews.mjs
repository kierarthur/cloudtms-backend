import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees',root=path.join(base,'healthroster-v2-backend-13102026'),out=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const packets=[
 {lane:'money',folder:'protected-source-qualification',manifest:'REVIEW_MANIFEST.json',hash:'92761006ed24966678f98a28c3988143873c70acf69a9df9d4c720dd64b57aa7',rows:'files',key:'file',verdict:'SOURCE_CRITIC_REJECTS_OPERATIONAL_ACTIVATION_AS_WRITTEN'},
 {lane:'protection',folder:'cn-a0-lock-fence',manifest:'LOCK_SOURCE_MANIFEST.json',hash:'0c1c69391c772ec5fda1b2945094fdd14128159e6514fa02772fcdc71383ec1d',rows:'files',key:'path',verdict:'UNMOUNTED_NATIVE_CREATOR_PHANTOM_AND_LATE_HOOK_HOLDS'},
 {lane:'report',folder:'automatic-native-delta-root-b9e2-review',manifest:'SOURCE_MANIFEST.json',hash:'e413b82d44e2b417f0b1a270150b17d4db2da418f3b645532c9ab8b9e64790fa',rows:'artifacts',key:'path',verdict:'SOURCE_REVIEW_NATIVE_CAS_FINANCIAL_API_GATES_PENDING'},
 {lane:'report',folder:'automatic-native-delta',manifest:'SOURCE_MANIFEST.json',hash:'8c640b781800532e562405dd8bcbf818b1ec9f8a07f7352dd68fefc95381d939',rows:'files',key:'path',verdict:'FROZEN_SOURCE_PREDECESSOR_FORWARD_STABLE_KEY_REPAIR_REQUIRED'}
];
const writes=[],receipt=[];
for(const q of packets){
 const lane=path.join(base,'healthroster-v2-'+q.lane+'-build-13102026'),prefix='docs/healthroster-build/proposals/'+q.folder,file=prefix+'/'+q.manifest,bytes=fs.readFileSync(path.join(lane,file));
 if(sha(bytes)!==q.hash)throw Error('MANIFEST_DRIFT '+q.folder);const data=JSON.parse(bytes),rows=data[q.rows];if(!Array.isArray(rows))throw Error('MANIFEST_FILE_LIST_MISSING '+q.folder);
 for(const row of rows){const raw=row[q.key]; const rel=typeof raw==='string'&&!raw.includes('/')?prefix+'/'+raw:raw;if(typeof rel!=='string'||!rel.startsWith(prefix+'/')&&!rel.startsWith('tests/weekly-source/')&&rel!=='tests/healthroster-cn-a0-lock-fence.test.mjs'||rel.includes('..'))throw Error('OUTPUT_PATH_REFUSED '+rel);const body=fs.readFileSync(path.join(lane,rel));if(body.length!==row.bytes||sha(body)!==row.sha256)throw Error('OUTPUT_HASH_DRIFT '+rel);writes.push({relative:rel,body});}
 writes.push({relative:file,body:bytes});receipt.push({folder:q.folder,manifestSha256:q.hash,artifactCount:rows.length,status:q.verdict});
}
const critic=fs.readFileSync(path.join(out,'QUALIFICATION_SOURCE_CRITIC.md'));if(sha(critic)!=='6baa6d226e9e57848ee62f03d3e2737cad65933b8d986eb6769619516b8ba9ba')throw Error('CRITIC_REPORT_CHANGED');
writes.push({relative:'docs/healthroster-build/rehearsals/13102026/QUALIFICATION_SOURCE_CRITIC.md',body:critic});
for(const w of writes){const p=path.join(root,w.relative);if(fs.existsSync(p)&&sha(fs.readFileSync(p))!==sha(w.body))throw Error('EXISTING_ROOT_CHANGED '+w.relative);}
for(const w of writes){const p=path.join(root,w.relative);fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,w.body);}
const result={status:'FROZEN_EVIDENCE_PRESERVED_NO_RUNTIME_INTEGRATION',packets:receipt,files:writes.length,criticSha256:sha(critic),sourceCorrectionDisposition:'All three critic findings accepted for forward correction; pendingV2 activation rejected. Source-only frameworks do not constitute runtime or full PartB sign-off.',runtimeFilesChanged:0};
const json=JSON.stringify(result,null,2)+'\n';fs.writeFileSync(path.join(out,'QUALIFICATION_A0_AUTOMATIC_FROZEN_PRESERVATION.json'),json);fs.writeFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/QUALIFICATION_A0_AUTOMATIC_FROZEN_PRESERVATION.json'),json);
console.log(JSON.stringify(result));

