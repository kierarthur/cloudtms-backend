// Freeze only this owned proposal. No staging, mutation or external writes.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),prefix='docs/healthroster-build/proposals/same-separate-f23-classification-20261017/';
assert.match(fs.realpathSync.native(here).replaceAll('\\','/'),/^D:\/Development\/GitHub\/\.codex-worktrees\/healthroster-checklist-native-14102026\//);
const sha=x=>createHash('sha256').update(x).digest('hex');
function files(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?files(path.join(dir,e.name)):[path.join(dir,e.name)]);}
const entries=files(here).filter(f=>path.basename(f)!=='MANIFEST.json'&&!path.basename(f).startsWith('PRELIMINARY_')).map(f=>{
 const relative=path.relative(here,f).replaceAll('\\','/'),b=fs.readFileSync(f);return{file:prefix+relative,bytes:b.length,sha256:sha(b),
  ...(relative==='payload/scripts/healthroster-same-separate-type-review.mjs'?{mountTarget:'scripts/healthroster-same-separate-type-review.mjs'}:
   relative==='CLASSIFICATION.json'||relative.startsWith('inputs/owners/')?{mountTarget:prefix+relative}:{})};
}).sort((a,b)=>a.file.localeCompare(b.file));
const d=JSON.parse(fs.readFileSync(here+'/CLASSIFICATION.json')),tests=JSON.parse(fs.readFileSync(here+'/TEST_RESULTS.json')),ast=JSON.parse(fs.readFileSync(here+'/AST_RESULTS.json'));
assert.equal(tests.classificationSha256,sha(fs.readFileSync(here+'/CLASSIFICATION.json')));assert.equal(ast.classificationSha256,tests.classificationSha256);
assert.equal(entries.filter(e=>e.mountTarget).length,39);assert.equal(tests.tests.length,16);assert.equal(ast.uniqueCatalogueDependentSignatures,4);
const manifest={version:'SAME_SEPARATE_F23_FINITE_PROPOSAL_MANIFEST_V1',finalCommit:d.finalCommit,selection:d.selection,
 parent:d.rootRuntimeParent,gateInputSha256:d.gate.sha256,classificationSha256:tests.classificationSha256,
 originalKeptEntryClassificationSha256:'6309ea0cadd0d2cd3e98537c145cd8bb49aef984d74bbfe0f11075f54982a874',
 originalLaterKeepClassificationSha256:'326fded2189c8076c0c6a3ed8d1c5bbae6350f3815a142de128bf5a39e606140',
 mountFiles:39,files:entries,native:'NOT_RUN',fullF23Gate:'ROOT_OWNED_NOT_RUN_HERE',catalogueDependentBodies:'FOUR_UNIQUE_SIX_PHYSICAL_NOT_PROVED',
 existingUntrackedPreserved:true,retainedPreliminaryUntrackedFilesExcluded:true,bankingBodiesRead:0,dbConnections:0,rootWrites:0,
 gitMutationPolicy:{taskScopedOnly:true,gcAuto:0,maintenanceAuto:false,metadataRepairPruneResetRecreate:false}};
const bytes=JSON.stringify(manifest,null,2)+'\n';
if(process.argv.includes('--check'))assert.equal(fs.readFileSync(here+'/MANIFEST.json','utf8'),bytes,'FINITE_MANIFEST_CURRENT_SAVED_EQUAL');else fs.writeFileSync(here+'/MANIFEST.json',bytes);
console.log(JSON.stringify({files:entries.length,mountFiles:39,manifestSha256:sha(bytes),classificationSha256:tests.classificationSha256,native:'NOT_RUN'}));
