import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(out,'../../../..');
const sha=b=>createHash('sha256').update(b).digest('hex'),pins=JSON.parse(fs.readFileSync(path.join(out,'SOURCE_INPUTS_AND_OWNER_PINS.json'),'utf8'));
assert.equal(pins.source_only,true);assert.equal(pins.current_root_selected_source_commit,'61ce56ff5a0df2e4e4a85fdbb9efc25267d3c966');
const proofs=[];
for(const pin of pins.source_inputs){const bytes=execFileSync('git',['--no-replace-objects','show',pin.commit+':'+pin.file],{cwd:root,maxBuffer:32*1024*1024});assert.equal(bytes.length,pin.bytes);assert.equal(sha(bytes),pin.sha256);proofs.push({file:pin.file,sha256:pin.sha256});}
for(const owner of pins.complete_existing_owners){const text=fs.readFileSync(path.join(out,owner.complete_definition_file),'utf8'),events=functionEvents(text).events;assert.equal(events.length,1);assert.equal(events[0].id,owner.id);assert.equal(sha(events[0].definition),owner.definition_sha256);}
assert.equal(pins.complete_existing_owners.length,38);assert.equal(pins.selected_existing_tables.length,17);assert.equal(pins.missing_planned_functions.length,3);
for(const file of ['build-source-inputs.mjs','freeze-design.mjs'])execFileSync(process.execPath,['--check',path.join(out,file)],{cwd:root});
for(const owner of pins.complete_existing_owners)assert.ok(!pins.missing_planned_functions.includes(owner.name));
const check={schema_version:'KEPT_PAY_REKEY_DESIGN_SOURCE_CHECK_V1',source_only:true,current_selected_owner_commit:pins.current_root_selected_source_commit,selected_exact_git_input_pins:proofs.length,complete_existing_owner_definitions:pins.complete_existing_owners.length,existing_table_statements:pins.selected_existing_tables.length,missing_planned_native_owners:3,source_pin_and_complete_definition_mismatches:0,inert_syntax_checks:2,primary_plan_sha256:pins.primary_plan.sha256,sql_source_implementation_authored:false,sql_compiled:false,native_fixtures_run:false,decision_store_installed:false,architecture_review_required_before_separate_implementation:true,paid_mapping_snapshot_carry_forward_prepared_context_and_complete_lock_graph_requires_precise_implementation_review:true,qualification_and_final_seal:'HOLD'};
fs.writeFileSync(path.join(out,'SOURCE_CHECK_RESULT.json'),JSON.stringify(check,null,2)+'\n');
function walk(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);}
const files=walk(out).filter(file=>!file.endsWith('FROZEN_ARTIFACT_MANIFEST.json')).sort();
const artifacts=files.map(file=>{const bytes=fs.readFileSync(file);return {file:path.relative(out,file).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)};});
assert.ok(artifacts.every(a=>!a.file.startsWith('../')));
const manifest={schema_version:'KEPT_PAY_REKEY_DESIGN_FROZEN_SOURCE_MANIFEST_V1',source_only:true,current_root_selected_sql_commit:pins.current_root_selected_source_commit,primary_plan_sha256:pins.primary_plan.sha256,artifact_count:artifacts.length,artifacts,implementation_sql_or_runtime_change:false,native_and_catalogue_qualification:false,review_scope:'Missing planned source reconciliation, closest existing store comparison, precise source design only. All actual native and separate code acceptance gates held.'};
fs.writeFileSync(path.join(out,'FROZEN_ARTIFACT_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(fs.readFileSync(path.join(out,'FROZEN_ARTIFACT_MANIFEST.json'))),check_sha256:sha(fs.readFileSync(path.join(out,'SOURCE_CHECK_RESULT.json'))),source_only:true}));
