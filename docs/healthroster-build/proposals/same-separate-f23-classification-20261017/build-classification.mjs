// Assemble independently proved exact Health inputs; never change integration.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const prefix='docs/healthroster-build/proposals/same-separate-f23-classification-20261017/';
const sha=x=>createHash('sha256').update(x).digest('hex'),canon=x=>x.toString().replaceAll('\r\n','\n');
const census=JSON.parse(fs.readFileSync(here+'/CENSUS.json')),proof=JSON.parse(fs.readFileSync(here+'/PROOF_INPUTS.json'));
assert.equal(proof.censusSha256,sha(fs.readFileSync(here+'/CENSUS.json')));
const owners=proof.owners.map(o=>({...o,...(o.priorFile?{priorFile:prefix+o.priorFile}:{})}));
const dependencies=[
 'scripts/cloudtms-managed-release-sql.mjs','scripts/healthroster-contact-finite-review.mjs',
 'scripts/healthroster-rowtype-type-review.mjs','scripts/healthroster-uuid-cardinality-review.mjs',
 'scripts/healthroster-aggregate-type-review.mjs','scripts/healthroster-dynamic-source-review.mjs',
 'scripts/healthroster-joined-nondynamic-review.mjs','scripts/healthroster-prebody-dynamic-review.mjs',
 'scripts/healthroster-source13-finite-review.mjs','scripts/healthroster-kept-entry-type-review.mjs',
 'scripts/healthroster-later-keep-type-review.mjs',
 'docs/healthroster-build/proposals/kept-entry-f23-classification-20261017/CLASSIFICATION.json',
 'docs/healthroster-build/proposals/later-keep-f23-classification-20261017/CLASSIFICATION.json'
].map(file=>({path:file,sha256:sha(canon(fs.readFileSync(path.join(root,file))))}));
const migration=census.files.find(f=>f.file.startsWith('supabase/migrations/')),
 expenseProducer=census.files.find(f=>path.posix.basename(f.file).startsWith('028_')).file;
const data={version:'HEALTH_SAME_SEPARATE_FINITE_TYPE_PROPOSAL_V1',qualification:'SOURCE_TYPES_AND_CONSTRUCTION_ONLY',
 functionalCommit:census.functional,metadataCommit:census.metadata,finalCommit:census.finalCommit,
 rootRuntimeParent:'ef3d20f4513a6f9fd1aa5e4cf6a69d258f6cd87c',gate:census.gate,
 selection:{commit:census.selectionCommit,path:census.selectionPath,sha256:census.selectionSha256},
 originalSourceAuthority:census.originalSourceAuthority,counts:census.counts,files:census.files,
 schema:proof.schema,migration:{file:migration.file,sha256:migration.sha256},expenseProducer,
 owners,transfers:proof.transfers,classifier:proof.classifier,dependencies,baselineEvidence:proof.baselineEvidence,
 catalogueDependentBodies:{status:'NOT_PROVED',unique:4,physicalOccurrences:6,
  signatures:['private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)','public.weekly_source_invoice_admit_atomic_v1(jsonb)',
   'private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid)','private.weekly_source_nhsp_price_admission_v1(uuid,uuid)'],
  note:'PG17 record-catalogue dependencies are not body compilation proof; original records are preserved.'},
 incompleteNegativePublication:'Count != 1 preserves INCOMPLETE, no_reversal_proved JSON null, paid_writer_permission null. Minimum is not unique-current authority.',
 bankingBodiesRead:0,native:'NOT_RUN'};
fs.writeFileSync(here+'/CLASSIFICATION.json',JSON.stringify(data,null,2)+'\n');
const hash=sha(fs.readFileSync(here+'/CLASSIFICATION.json')),module=here+'/payload/scripts/healthroster-same-separate-type-review.mjs';
let source=fs.readFileSync(module,'utf8');source=source.replace(/export const reviewSha256='(?:BUILD_CLASSIFICATION_SHA256|[0-9a-f]{64})';/,()=>"export const reviewSha256='"+hash+"';");fs.writeFileSync(module,source);
console.log(JSON.stringify({classificationSha256:hash,owners:owners.length,reverses:owners.filter(o=>o.priorFile).length,relations:Object.keys(proof.schema.relations).length,transfers:proof.transfers.length,native:'NOT_RUN'}));
