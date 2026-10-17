// Offline source evidence only. No database/credential/turn/tool invocation.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {here,ROOT,sha,sortWriters,requireThat} from './source.mjs';
const rootCommit='728a2c32e624fed2db577b7deba70600d27885b1';
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:16*1024*1024});
const pure='docs/healthroster-build/proposals/daily-health-pure-native-proof-20261017';
const ownerManifest=JSON.parse(fs.readFileSync(path.join(ROOT,pure,'SOURCE_MANIFEST.json'),'utf8'));
const files=[...new Set(['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs',
 'scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs','scripts/cloudtms-managed-release-sql.mjs',
 'supabase/release/export_contract.sql','supabase/release/current-contract.json',pure+'/build-program.mjs',pure+'/SOURCE_MANIFEST.json',
 ...ownerManifest.rootInputs.map(r=>r.file),pure+'/'+ownerManifest.helper.file,pure+'/'+ownerManifest.program.file])];
// Importing the original full rehearsal module also evaluates its static JS
// imports. Pin that complete local module closure, even though this adapter
// calls only snapshot/assertion helpers and never installation/release code.
for(let i=0;i<files.length;i++){
 const file=files[i];if(!file.endsWith('.mjs'))continue;
 const text=fs.readFileSync(path.join(ROOT,file),'utf8');
 for(const match of text.matchAll(/(?:\bfrom\s*|\bimport\s*)['"](\.[^'"]+\.mjs)['"]/g)){
  const dependency=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]));
  requireThat(!dependency.startsWith('../'),'FREEZE_LOCAL_IMPORT_ESCAPE_REFUSED');
  if(!files.includes(dependency))files.push(dependency);
 }
}
const rootInputs=files.map(file=>{
 const bytes=fs.readFileSync(path.join(ROOT,file));requireThat(bytes.equals(git(ROOT,['show',rootCommit+':'+file])),'FREEZE_EXACT_COMMITTED_SOURCE_REQUIRED');
 return {file,bytes:bytes.length,sha256:sha(bytes)};
});
const writers=sortWriters(JSON.parse(fs.readFileSync(path.join(ROOT,'supabase/release/current-contract.json'),'utf8')).routines
 .filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
requireThat(writers.length===4,'FREEZE_EXACT_WRITER_CATALOGUE_REQUIRED');
fs.writeFileSync(path.join(here,'SOURCE_PINS.json'),JSON.stringify({version:'DAILY_HEALTH_PURE_ADAPTER_SOURCE_V1',rootCommit,root:ROOT,rootInputs,
 expectedWritersSha256:sha(JSON.stringify(writers)),expectedWriters: writers.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
 catalogueAndLedger:'WHOLE_UNCHANGED_ORIGINAL_CANONICAL_QUERY',businessSnapshotScope:['private.candidate_daily_batch_receipts','public.ts_financials_outbox'],
 businessProgrammeReadsAndWrites:[],externalEffects:false,native:'NOT_RUN'},null,2)+'\n');
const executable=['source.mjs','input-builder.mjs','fixed-metadata-reader.mjs','control.mjs','result-validator.mjs','hold-pure.mjs'];
const manifest={version:'DAILY_HEALTH_PURE_ADAPTER_EXECUTABLE_V1',sourcePinsSha256:sha(fs.readFileSync(path.join(here,'SOURCE_PINS.json'))),
 files:executable.map(file=>({file,sha256:sha(fs.readFileSync(path.join(here,file)))})),native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({status:'SOURCE_FROZEN_NOT_EXECUTED',rootCommit,rootInputs:rootInputs.length,executableFiles:executable.length,sourcePinsSha256:manifest.sourcePinsSha256}));
