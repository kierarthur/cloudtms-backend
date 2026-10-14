import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../..'),money='d4b62cb15159d0afb02cfebf8c1e034c68c26550',base='docs/healthroster-build/proposals/',sha=x=>createHash('sha256').update(x).digest('hex');
const read=file=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',money+':'+file],{cwd:own,maxBuffer:32_000_000}),json=file=>JSON.parse(read(file)),pin=file=>({file,commit:money,sha256:sha(read(file))});
const cap=json(base+'protected-source-capability/generated/SOURCE_MANIFEST.json'),q45=json(base+'protected-source-qualification-operational-gate/COMPLETE_CLOSURE_CENSUS.json'),mount=json(base+'strict-qualification-native-interfaces/INTEGRATION_ORDER.json'),runtime=json(base+'strict-qualification-native-interfaces/RUNTIME_SIDECAR_REVERSE.json');
for(const row of q45.ordered_function_inputs)assert.equal(sha(read(row.path)),row.sha256);
const recipe={schema_version:'MONEY_INSTALL1_FORWARD_EXACT_SOURCE_RECIPE_V1',source_only:true,source_recipe_commit:money,actual_install1_commit:'37ddadc0a50721834c9e8a6c06becfc983f0dc32',
 reviewed_new_proposal_manifest_sha256:'bc31de1017d896910b138f6a05cf95cac744fea2d501625c14184f02f057b04f',
 schema_first:[pin(base+'protected-source-capability/generated/01_schema.sqlinc')],
 original_cap_declared_order:cap.sql_install_order.map(file=>pin(base+'protected-source-capability/generated/'+file)),
 final_original_cap_q45_order:q45.ordered_function_inputs.map(row=>({...pin(row.path),composition_mode:'SOURCE_REFERENCE_REBASE_EXISTING_NATIVE_OR_REGISTRY_OWNER_BEFORE_MOUNT'})),q45_order_source:pin(base+'protected-source-qualification-operational-gate/COMPLETE_CLOSURE_CENSUS.json'),
 raw_q45_native_copies_or_old_catalogue_wholesale_mount_allowed:false,
 source_cas_final_pre_money:pin(base+'native-financial-fixtures/CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc'),
 first_use_and_inventory_successors:[pin(base+'strict-qualification-first-use-inventory-forward/OPERATIONAL_CERTIFICATE.sqlinc'),pin(base+'strict-qualification-first-use-inventory-forward/TEST_JOURNEY_SCOPE.sqlinc')],
 final_execution_tree_runtime:pin(base+'strict-qualification-git-path-parser-forward/weekly-source-protected-execution-tree.mjs'),
 money47_mount_order:mount.sql_complete_successors_in_final_order.map(file=>pin(base+'strict-qualification-native-interfaces/'+file)),
 money_runtime:runtime.proofs.map(row=>({...pin(base+'strict-qualification-native-interfaces/'+row.output),destination:row.destination})),
 original_external_runtime_prerequisites:runtime.external_required_scripts,
 mandatory_currentness_rules:['Root begins with actual37 and preserves actual Banking/WP-PP hooks and current release/managed-writer/turn control. Broad schemas/native invoice/pay/Report/windows and5 dual native joined owners are required before this recipe.',
 'CAP generated order documents original dependencies. final_original_cap_q45_order gives the selected source dependency sequence, not permission to mount old native or registry copies verbatim; current owners require whole-source forward/reverse and exact hunk review.',
 'Q45 original01 physical census is superseded by Money13; Q45 TEST_JOURNEY_SCOPE and operational DTO first get the F12 successors, then complete Money OFF and final operational certificate successors.',
 'Q45 original feature/actor/C1 complete copies must be fully rebased into current native owners; they cannot overwrite current Banking refresh/admission or14WP-PP Local. Root owns the5 complete dual joins.',
 'The exact Source CAS schema-forward retains original39 facts; Money08 changes only capability admission/operational rule projection. Do not mount the older02_source_cas again after final schema-forward.',
 'Keep P10 fatal round-trippable all-tracked Git parser final; actual native tree proof covers every supported tracked object and only3 original receipt bundles. No docs/runtime exclusion or receipt-only relabel between changed trees.',
 'Run all Money47 in the listed order after actual original/broad dependencies; do not overwrite later definitions with earlier CAP/Q45 originals. Final complete registry/ACL is recomputed across current nativeBank/Source/Money/Report, not inferred from prepared301.',
 'Native FIN4 is required; actual37 Fin3 is not a surrogate. Root finalJoinedSource must be frozen/recensused and compile/contract/ACL/rehearsal/all genuine native/remote/phone/activation gates pass before installation.'],
 no_database_or_runtime_acceptance:true,no_source_mount_performed:true};
fs.writeFileSync(path.join(out,'MONEY_INSTALL1_FORWARD_RECIPE_14102026.json'),JSON.stringify(recipe,null,2)+'\n');
console.log(JSON.stringify({source_only:true,q45_order:recipe.final_original_cap_q45_order.length,money_source_files:recipe.money47_mount_order.length,sidecars:recipe.money_runtime.length,recipe_sha256:sha(fs.readFileSync(path.join(out,'MONEY_INSTALL1_FORWARD_RECIPE_14102026.json')))}));
