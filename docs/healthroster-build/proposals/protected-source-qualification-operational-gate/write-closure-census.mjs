import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(out,'../../../..'),root=path.resolve(repo,'../healthroster-v2-backend-13102026');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const relative=file=>path.relative(repo,file).replaceAll('\\','/');
const read=file=>fs.readFileSync(file,'utf8').replaceAll('\r\n','\n');
const definitions=(file)=>[...read(file).matchAll(/create or replace function\s+([\w.]+)\s*\(([\s\S]*?)\)\s*returns[\s\S]*?\bas\s+(\$[\w]*\$)[\s\S]*?\3\s*;/gi)]
 .map(row=>({signature:row[1]+'('+row[2].trim().split(',').filter(Boolean).map(arg=>arg.trim().replace(/^\w+\s+/,'').replace(/\s+default[\s\S]*$/i,'').trim()).join(',')+')',
  path:relative(file),definition_sha256:sha(row[0])}));
const cap=path.resolve(out,'../protected-source-capability'),q=path.resolve(out,'../protected-source-qualification'),n=path.resolve(out,'../native-financial-fixtures');
const ordered=[
 path.join(q,'00_complete_current_catalogue.sqlinc'),
 path.join(n,'CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc'),
 path.join(cap,'CAPABILITY_OWNERS.sqlinc'),
 path.join(cap,'generated/04_original_clients.sqlinc'),
 path.join(q,'04_complete_non_self_bill_admission.sqlinc'),
 path.join(q,'QUALIFICATION_STEP.sqlinc'),
 path.join(q,'05_complete_automatic_admission.sqlinc'),
 path.join(out,'CLOSED_DTO.sqlinc'),
 path.join(out,'OPERATIONAL_CERTIFICATE.sqlinc'),
 path.join(out,'01_complete_qualification_scope.sqlinc'),
 path.join(out,'02_complete_prerequisite_reader.sqlinc'),
 path.join(out,'TEST_JOURNEY_SCOPE.sqlinc'),
 ...['03_complete_operational_release_reader.sqlinc','04_complete_capability_receipt.sqlinc','05_complete_bulk_readiness.sqlinc',
  '06_complete_automatic_readiness.sqlinc','07_complete_existing_feature_setup.sqlinc','08_complete_cas_admission.sqlinc',
  '09_complete_actor_non_self_bill_admission.sqlinc','10_complete_actor_bulk_selection.sqlinc','11_complete_actor_own_stage.sqlinc',
  '12_complete_actor_17_prepare_family.sqlinc','12_complete_actor_18_prepare_action.sqlinc','12_complete_actor_19_action_context.sqlinc',
  '12_complete_actor_20_stage_request.sqlinc'].map(file=>path.join(out,file)),
 path.join(cap,'generated/07_complete_stage_proof.sqlinc'),
 path.join(out,'16_complete_bulk_command_presentation.sqlinc')
];
const rootRegistry=path.join(root,'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/12_complete_private_registry_union.sqlinc');
const registry=read(rootRegistry),start=registry.indexOf('create or replace function private._weekly_source_acl_private_helper_contract_v1()'),end=registry.indexOf('$function$;',start);
if(start<0||end<start)throw Error('CENSUS_ROOT_REGISTRY_NOT_FOUND');
const rootPrivate=[...new Set([...registry.slice(start,end).matchAll(/\('([^']+)'\)/g)].map(row=>row[1]))].sort();
if(rootPrivate.length!==232)throw Error('CENSUS_ROOT_SIGNATURE_SET_CHANGED '+rootPrivate.length);
const final=new Map();for(const file of ordered)for(const row of definitions(file))final.set(row.signature,row);
const privateRows=[...final.values()].filter(row=>row.signature.startsWith('private.')).sort((a,b)=>a.signature.localeCompare(b.signature));
const added=privateRows.filter(row=>!rootPrivate.includes(row.signature));
const replacements=privateRows.filter(row=>rootPrivate.includes(row.signature));
const completePrivate=[...new Set([...rootPrivate,...privateRows.map(row=>row.signature)])].sort();
const pins=[path.join(root,'supabase/release/current-release.json'),path.join(root,'supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql'),
 path.join(root,'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta_v1.sql'),
 ...fs.readdirSync(path.dirname(rootRegistry)).filter(file=>file.endsWith('.sqlinc')).map(file=>path.join(path.dirname(rootRegistry),file)),
 ...fs.readdirSync(path.join(root,'supabase/verification')).filter(file=>/^13102026_2206_/.test(file)).map(file=>path.join(root,'supabase/verification',file))];
const payload={schema_version:'PROTECTED_SOURCE_COMPLETE_LITERAL_CLOSURE_CENSUS_V1',source_only:true,native_acceptance:false,
 root_source_commit:'b65df0997ac69188eadf4de2a88f70226fa99532',root_private_registry_pin:relative(rootRegistry),
 root_preserved_private_signatures:rootPrivate,proposed_final_private_definition_rows:privateRows,
 added_private_signatures:added,replaced_private_signatures:replacements,proposed_complete_private_signature_union:completePrivate,
 planned_private_count:completePrivate.length,base_private_count:rootPrivate.length,new_distinct_private_count:added.length,
 root_tables_preserved:105,root_service_signatures_preserved:142,new_tables:0,new_service_grants:0,
 ordered_function_inputs:ordered.map(file=>({path:relative(file),sha256:sha(fs.readFileSync(file))})),
 source_schema_input:{path:relative(path.join(cap,'SCHEMA.sqlinc')),sha256:sha(fs.readFileSync(path.join(cap,'SCHEMA.sqlinc'))),
  purpose:'Root-reviewed one-time extension of two existing assignment CHECK constraints; no table/column'},
 root_2205_release_acl_local_receipt_verifier_pins:pins.map(file=>({path:relative(file),sha256:sha(fs.readFileSync(file)),bytes:fs.statSync(file).size})),
 preserve_root_owners:['public.weekly_exceptional_pay_complete_local_v1(jsonb)',
  'private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)',
  'private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)',
  'private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)',
  'private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)'],
 warnings:['This is a literal source union, not an installed catalogue count.',
  'Root must union current 232 with every listed distinct dependency, not assume 232+8.',
  'Original CAPABILITY_OWNERS definitions are prerequisites only; strict replacements must follow before the closure is callable.',
  'The complete three-argument selection wrapper delegates to the actor-bearing four-argument owner; omitting it prevents cross-session qualification.',
  'Root reasserts current complete registry/final private and browser ACL after all definitions; old whole registries must not overwrite 2205.']};
fs.writeFileSync(path.join(out,'COMPLETE_CLOSURE_CENSUS.json'),JSON.stringify(payload,null,2)+'\n');
// Declare the entire absent dependency set without resetting the root registry.
fs.writeFileSync(path.join(out,'18_complete_owner_private_acl.sqlinc'),privateRows.map(row=>
 'alter function '+row.signature+' owner to postgres;\nrevoke all on function '+row.signature+' from public,anon,authenticated,service_role;').join('\n')+'\n');
console.log(JSON.stringify({root:rootPrivate.length,added:added.length,planned_union:completePrivate.length,pins:pins.length,source_only:true}));
