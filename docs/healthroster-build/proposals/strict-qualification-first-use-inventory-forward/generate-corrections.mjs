import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..');
const frozen=path.resolve(out,'../protected-source-qualification-operational-gate');
const root=path.resolve(own,'../healthroster-v2-backend-13102026');
const rootCommit='cea15289e0283e3383e71f0acb8b18ce184647c2';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:10_000_000});
const input=name=>fs.readFileSync(path.join(frozen,name),'utf8');
const deltas=[];
function save(name,body,source,reverse){
 const original=input(source);assert.equal(reverse(body),original,name);
 fs.writeFileSync(path.join(out,name),body);
 deltas.push({file:name,original_file:source,original_sha256:sha(Buffer.from(original)),successor_sha256:sha(Buffer.from(body)),reverse_matches_entire_input:true});
}
const lockBefore="hashtextextended('weekly-source-test-journey:'||auth->>'scope_id',0)";
const lockAfter="hashtextextended('weekly-source-test-journey:'||(auth->>'scope_id'),0)";
const scope=input('TEST_JOURNEY_SCOPE.sqlinc');assert.equal(scope.split(lockBefore).length-1,1);
save('TEST_JOURNEY_SCOPE.sqlinc',scope.replace(lockBefore,()=>lockAfter),'TEST_JOURNEY_SCOPE.sqlinc',body=>body.replace(lockAfter,()=>lockBefore));

const sourceBefore=input('source-equivalence.mjs');
const oldInventory=" prefixes:Object.freeze(['broker/','shared/','candidate-broker/','candidate-private-api/','candidate-synthetic-private-api/',\n  'invoice-private-api/','invoice-renderer/','invoice-render-worker/','supabase/','scripts/','tests/','.github/']),\n files:Object.freeze(['.node-version','.npmrc','.gitmodules','package.json','package-lock.json','pnpm-lock.yaml','yarn.lock',\n  'wrangler.toml','wrangler.jsonc','tsconfig.json']),";
const newInventory=" // Every tracked object is selected, including root configuration, docs,\n // AGENTS, proposals and retained native-result files. There is no guessed\n // directory/config whitelist or documentary exclusion.\n selection:'ALL_TRACKED_GIT_OBJECTS',";
const oldRequired=" required:Object.freeze(['.node-version','.github/workflows/database-release.yml','supabase/release/export_contract.sql',";
const newRequired=" required:Object.freeze(['.node-version','.gitattributes','osv-scanner.toml','.github/workflows/database-release.yml','supabase/release/export_contract.sql',";
const oldSelected="const selected=file=>sourceInventory.files.includes(file)||sourceInventory.prefixes.some(prefix=>file.startsWith(prefix));";
const newSelected="const selected=file=>typeof file==='string'&&file.length>0&&!file.startsWith('/')&&!file.includes('\\\\')\n &&!file.split('/').some(part=>part==='..'||part==='.'||part==='');";
for(const anchor of [oldInventory,oldRequired,oldSelected])assert.equal(sourceBefore.split(anchor).length-1,1);
const sourceAfter=sourceBefore.replace(oldInventory,()=>newInventory).replace(oldRequired,()=>newRequired).replace(oldSelected,()=>newSelected);
save('source-equivalence.mjs',sourceAfter,'source-equivalence.mjs',body=>body.replace(newInventory,()=>oldInventory).replace(newRequired,()=>oldRequired).replace(newSelected,()=>oldSelected));
const nativeBefore=input('weekly-source-protected-execution-tree.mjs');
const nativeAfter=sourceAfter.replace("from '../protected-source-capability/release-qualification.mjs'","from './weekly-source-protected-release-qualification.mjs'");
assert.equal(sourceBefore.replace("from '../protected-source-capability/release-qualification.mjs'","from './weekly-source-protected-release-qualification.mjs'"),nativeBefore);
save('weekly-source-protected-execution-tree.mjs',nativeAfter,'weekly-source-protected-execution-tree.mjs',body=>body.replace(newInventory,()=>oldInventory).replace(newRequired,()=>oldRequired).replace(newSelected,()=>oldSelected));
const certBefore=input('OPERATIONAL_CERTIFICATE.sqlinc');
const certOld="unnest(array['.node-version','.github/workflows/database-release.yml','supabase/release/export_contract.sql',";
const certNew="unnest(array['.node-version','.gitattributes','osv-scanner.toml','.github/workflows/database-release.yml','supabase/release/export_contract.sql',";
assert.equal(certBefore.split(certOld).length-1,1);
save('OPERATIONAL_CERTIFICATE.sqlinc',certBefore.replace(certOld,()=>certNew),'OPERATIONAL_CERTIFICATE.sqlinc',body=>body.replace(certNew,()=>certOld));

const dangerous=/\|\|\s*[A-Za-z_][A-Za-z_0-9.]*\s*->>?/g;
function audit(file,body){return body.split('\n').flatMap((line,i)=>[...line.matchAll(dangerous)].map(hit=>({file,line:i+1,expression:hit[0],source_sha256:sha(Buffer.from(body))})));}
const packetAudit=[];
for(const file of fs.readdirSync(frozen).filter(name=>/\.sqlinc$/.test(name))){const body=input(file);packetAudit.push(...audit(file,body));}
assert.equal(packetAudit.length,1);assert.equal(packetAudit[0].file,'TEST_JOURNEY_SCOPE.sqlinc');
assert.deepEqual(audit('TEST_JOURNEY_SCOPE.sqlinc',scope.replace(lockBefore,()=>lockAfter)),[]);
const nativeAudit=[];
const files=git(['ls-tree','-r','--name-only',rootCommit]).toString('utf8').trim().split('\n').filter(file=>file.startsWith('supabase/repeatable/')&&/\.sql(?:inc)?$/.test(file));
for(const file of files){const body=git(['show',`${rootCommit}:${file}`]).toString('utf8');nativeAudit.push(...audit(file,body));}
const metaChanges=git(['diff','--name-status','d67f94e19000a612ee4f1099c14429d8a8b4bee1',rootCommit]).toString('utf8').trim().split('\n').map(line=>line.split('\t')).map(([status,file])=>{const b=git(['show',`${rootCommit}:${file}`]);return {status,file,sha256:sha(b),bytes:b.length,kind:file.startsWith('docs/')?'DOCUMENTARY_SOURCE_INPUT':'EXECUTION_SOURCE_INPUT'};});
assert.deepEqual(metaChanges.filter(change=>change.file.startsWith('supabase/')),[]);
const pins=JSON.parse(input('COMPLETE_CLOSURE_CENSUS.json')).root_2205_release_acl_local_receipt_verifier_pins.map(pin=>{
 const file=pin.path.replace('../healthroster-v2-backend-13102026/',''), b=git(['show',`${rootCommit}:${file}`]);
 return {file,sha256:sha(b),bytes:b.length};
});
assert.equal(pins.find(pin=>pin.file==='supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql').sha256,'28c053d075970324387d33d86402156fe7bcc55225c03c769abe15c14d18e273');
fs.writeFileSync(path.join(out,'SOURCE_DELTAS_AND_AUDIT.json'),JSON.stringify({schema_version:'STRICT_QUALIFICATION_FIRST_USE_INVENTORY_FORWARD_V1',source_only:true,root_source_commit:rootCommit,
 deltas,packet_json_prefix_findings:packetAudit,packet_successor_findings:[],native_json_prefix_candidates:nativeAudit,
 native_audit_scope:'Every committed native repeatable SQL/sqlinc at the pinned root, read-only. Existing Candidate/JSONB candidates are preserved; this packet changes only the qualified TEST setup owner. These historical/currentness candidates are not native acceptance findings or broad edit authority.',
 root_d67_to_cea_full_metadata_forward:metaChanges,root_input_pins:pins,root_runtime_sql_unchanged_from_d67:true,
 exact_root_private_count_preserved:232,planned_complete_union_unchanged:258,tables_preserved:105,service_signatures_preserved:142,new_signatures:0,
 semantics:'ALL_TRACKED_GIT_OBJECTS. All permanent proposals/docs/AGENTS/native-result changes invalidate R1→R2 except the same three fixed receipt bundles and narrow releaseId/descriptors. Original execution identity is never relabelled.',
 native_open_close_revoke_acceptance:false,actual_source_equivalence_recording:false,actual_remote_acceptance:false,actual_activation:false,lane_database_calls:0},null,2)+'\n');
console.log(JSON.stringify({source_only:true,full_reverse_proofs:deltas.length,packet_prefix_candidates:packetAudit.length,successor_prefix_candidates:0,native_candidates:nativeAudit.length,root_source_commit:rootCommit}));
