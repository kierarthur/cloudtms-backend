import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {scanManagedSql} from './PINNED_MANAGED_SQL_SCANNER.mjs';
import {declarationIdentity,dropDeclarationIdentities,stripSqlComments} from './native-source-identity.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex'),bytes=file=>fs.readFileSync(path.join(out,file)),read=file=>JSON.parse(bytes(file)),git=(commit,file)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+file],{cwd:own,maxBuffer:32_000_000});
const r=read('INSTALL1_CONSUMED_OWNER_COMPARISON.json'),plan=read('COMPOSITION_PLAN.json');
function declarations(file){const parsed=scanManagedSql(bytes(file).toString('utf8'));return parsed.statements.map(row=>parsed.source.slice(row.start,row.end)).filter(body=>/^create\s+(?:or\s+replace\s+)?function\b/i.test(body));}
test('all488 prior owners have one explicit exact comparison and no implicit equivalence claim',()=>{
 assert.equal(r.original_consumed,488);assert.equal(new Set(r.comparison.map(row=>row.signature)).size,488);
 assert.equal(r.unchanged,344);assert.equal(r.changed,61);assert.equal(r.absent_in_install1,83);assert.equal(r.unchanged+r.changed+r.absent_in_install1,488);
 for(const row of r.comparison){if(row.status==='UNCHANGED')assert.equal(row.actual.definition_sha256,row.prior.definition_sha256);else if(row.status==='CHANGED')assert.notEqual(row.actual.definition_sha256,row.prior.definition_sha256);else assert.equal(row.actual,null);}
 assert.equal(plan.no_entire_source_equivalence_claim,true);assert.equal(plan.no_receipt_only_equivalence_claim_between37_2151_install2,true);
});
test('complete changed and additional body files match every actual pinned source hash',()=>{
 const changed=r.comparison.filter(row=>row.status==='CHANGED'),actual=declarations('ACTUAL_CHANGED_CONSUMED_OWNERS.sqlinc').map(sha),prior=declarations('PRIOR_CHANGED_CONSUMED_OWNERS.sqlinc').map(sha),extra=declarations('ACTUAL_ADDITIONAL_EXECUTOR_OWNERS.sqlinc').map(sha);
 assert.equal(actual.length,61);assert.equal(prior.length,61);assert.equal(extra.length,71);
 assert.deepEqual(actual,[...changed.map(row=>row.actual.definition_sha256)]);assert.deepEqual(prior,[...changed.map(row=>row.prior.definition_sha256)]);
 assert.deepEqual(extra,r.additional_actual_executor_owners.map(row=>row.definition_sha256));
});
test('trigger declarations are separated from reachability and retained hooks cannot be reported as deleted',()=>{
 assert.equal(r.trigger_changes.length,21);const retained=r.trigger_comparison.filter(row=>row.prior&&row.actual&&!row.actual_reached_by_install1_closure&&row.prior.definition_sha256===row.actual.definition_sha256);
 assert.equal(retained.length,29);assert.deepEqual(plan.still_declared_but_not_currently_reached,retained);
 for(const row of retained)assert.ok(!r.trigger_changes.some(changed=>changed.key===row.key));
 const parsed=scanManagedSql(bytes('ACTUAL_CHANGED_TRIGGER_DECLARATIONS.sqlinc').toString('utf8'));
 assert.deepEqual(parsed.statements.map(row=>sha(parsed.source.slice(row.start,row.end))),r.trigger_changes.filter(row=>row.actual).map(row=>row.actual.definition_sha256));
});
test('new Money forward preserves full immutable sources and adds47 interfaces without replacing currentBank/WP bodies',()=>{
 const bindings=read('MONEY_INTERFACE_INSTALL1_BINDINGS.json'),reverse=read('MONEY_INTERFACE_ENTIRE_SOURCE_REVERSE.json');assert.equal(bindings.complete_interfaces,47);assert.equal(bindings.installed_existing,0);assert.equal(bindings.actual_bank_or_wp_native_bodies_replaced.length,0);
 assert.equal(reverse.complete_definition_reverse_matches_entire_prior_symbol_map,true);assert.equal(reverse.prior_symbol_count,reverse.reversed_symbol_count);assert.equal(sha(bytes(reverse.forward_file)),reverse.forward_sha256);
 const prefix='docs/healthroster-build/proposals/strict-qualification-native-interfaces/',integration=JSON.parse(git(r.frozen_money_commit,prefix+'INTEGRATION_ORDER.json'));
 const exact=integration.sql_complete_successors_in_final_order.map(file=>{const raw=git(r.frozen_money_commit,prefix+file);return '-- Frozen Money '+r.frozen_money_commit+':'+prefix+file+'; raw SHA256 '+sha(raw)+'\n'+raw.toString('utf8');}).join('\n\n')+'\n';
 assert.equal(bytes(reverse.forward_file).toString('utf8'),exact);assert.ok(bindings.interfaceProof.every(row=>row.prior_frozen_declaration_hash_matches_canonical));
});
test('five dual native owners remain explicit root dependencies with both complete predecessor bodies',()=>{
 assert.equal(plan.dual_native_composition_required.length,5);const names=plan.dual_native_composition_required.map(row=>row.signature);
 for(const name of ['public.weekly_exceptional_pay_complete_local_v1(jsonb)','public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)','public.weekly_source_manual_review_resolve_v1(jsonb)','private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)','private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)'])assert.ok(names.includes(name));
 assert.equal(plan.preserve_current_native_owners.length,20);assert.equal(plan.broad_restore_owners.length,41);assert.equal(plan.prior_frozen_census_canonical_corrections.length,4);
 for(const row of plan.dual_native_composition_required){assert.equal(row.responsible_owner,'ROOT_BROAD_NATIVE_COMPOSITION');assert.ok(r.comparison.some(value=>value.signature===row.signature&&value.actual.definition_sha256===row.actual_sha256));}
});
test('recursive tracked SQL roots and every repeated include occurrence match exact canonical release inventory',()=>{
 const pins=read('INSTALL1_SOURCE_INPUT_PINS.json');
 for(const [key,expectedRoots,expectedOccurrences,expectedPaths,expectedSymbols]of [['prior',835,1720,1313,2280],['installed',1019,1847,1433,2440]]){
  const data=pins[key],tracked=execFileSync('git',['ls-tree','-r','--name-only',data.commit],{cwd:own,encoding:'utf8'}).trim().split('\n').filter(file=>/^supabase\/repeatable\/.*\.sql$/.test(file));
  assert.equal(tracked.length,expectedRoots);assert.deepEqual(data.all_recursive_sql_roots.filter(file=>file.startsWith('supabase/repeatable/')).sort(),tracked.sort());
  assert.equal(data.ordered_source_occurrences.length,expectedOccurrences);assert.equal(data.inputs.length,expectedPaths);assert.equal(data.final_semantic_symbol_count,expectedSymbols);
  assert.ok(data.ordered_source_occurrences.length>data.inputs.length);
  for(const file of tracked.filter(file=>file.slice('supabase/repeatable/'.length).includes('/')))assert.ok(data.inputs.some(row=>row.file===file),file);
 }
});
test('multiline commented DROP and named/alias argument identities remove the real semantic symbol',()=>{
 const statement=`drop function if exists public.pay_preview(
  date, -- p_pay_date
  date, -- p_week_ending_cutoff
  uuid, -- p_actor_user_id
  uuid, -- p_candidate_id
  uuid -- p_client_id
 );`;
 assert.equal(dropDeclarationIdentities(statement)[0].signature,'public.pay_preview(date,date,uuid,uuid,uuid)');
 const declared=declarationIdentity('private.vector','p_a int4, p_b timestamptz, OUT p_result jsonb, p_values uuid[] DEFAULT array[]::uuid[]'),drop=dropDeclarationIdentities('drop function private.vector(integer, timestamp with time zone, uuid[]);')[0];
 assert.equal(declared.semantic_signature,drop.semantic_signature);assert.equal(declared.signature,'private.vector(int4,timestamptz,uuid[])');
 const map=new Map([[declared.semantic_signature,{source:true}]]);map.delete(drop.semantic_signature);assert.equal(map.size,0);
 assert.equal(stripSqlComments('"--quoted type" /* outer /* nested */ block */ uuid').replace(/\s+/g,' ').trim(),'"--quoted type" uuid');
 const audit=read('EXPLICIT_FUNCTION_DROP_REVIEW.json');assert.ok(audit.actual_function_drop_declarations.some(row=>row.signature==='public.pay_preview(date,date,uuid,uuid,uuid)'));assert.ok(audit.actual_function_drop_declarations.every(row=>!row.signature.includes('--')));
});
test('old488 are verified committed declarations while four canonical prior owners remain separately reconciled',()=>{
 assert.equal(r.prior_census_canonical_drift.length,4);
 for(const row of r.prior_census_canonical_drift){assert.equal(row.exact_prior_census_is_current_canonical,false);assert.ok(row.canonical_prior);assert.equal(row.prior_frozen.semantic_signature,row.canonical_prior.semantic_signature);assert.notEqual(row.prior_frozen.definition_sha256,row.canonical_prior.definition_sha256);}
 assert.equal(plan.native_fin_overload_precondition.use_three_argument_surrogate,false);assert.equal(plan.native_fin_overload_precondition.full_root_native_composition_required,true);
});
test('actual37 complete Broker reverses exactly and retains all currentBank native source outside two transport hunks',()=>{
 const proof=read('BROKER_INSTALL1_ENTIRE_SOURCE_REVERSE.json');let reversed=bytes(proof.output_file).toString('utf8');assert.equal(sha(reversed),proof.output_sha256);assert.equal(proof.hunks.length,2);
 for(const row of [...proof.hunks].reverse()){assert.equal(reversed.split(row.after).length-1,1);assert.equal(sha(row.before),row.before_sha256);assert.equal(sha(row.after),row.after_sha256);reversed=reversed.replace(row.after,()=>row.before);}
 const actual=git(proof.input_commit,proof.input_file);assert.equal(reversed,actual.toString('utf8'));assert.equal(sha(actual),proof.input_sha256);assert.equal(proof.direct_rpc_callers,2);assert.equal(proof.native_recording_rpc_callers,5);assert.equal(proof.requireUser_actor_callbacks,1);
});
test('runtime and recorder ABI retain closed original paths and propose no shared CLI or turn-control change',()=>{
 const runtime=read('RUNTIME_AND_RECORDER_SOURCE_PINS.json');assert.equal(runtime.runtime_sidecars.length,10);assert.equal(runtime.actual_release_cli_edits_proposed,0);assert.equal(runtime.actual_managed_writer_turn_control_edits_proposed,0);
 assert.equal(runtime.recorder_request_fields,9);assert.equal(runtime.recorder_response_fields,13);assert.equal(runtime.report_slots.length,2);assert.equal(runtime.report_private_signatures.length,2);assert.equal(runtime.report_native_input_key_count,3);assert.equal(runtime.bundle_paths_count,3);
 for(const row of runtime.runtime_sidecars)assert.equal(sha(git(runtime.frozen_money_commit,row.input_file)),row.definition_sha256);
 for(const row of runtime.current_shared_caller_pins)if(row.actual!==null)assert.equal(sha(git(runtime.actual_install1_commit,row.file)),row.actual);
});
test('old reviewed98 and all literal function drop provenance remain immutable and explicitly bounded',()=>{
 const preserve=read('PRIOR_FROZEN_PRESERVATION.json'),file='docs/healthroster-build/proposals/strict-qualification-native-interfaces/REVIEW_MANIFEST.json',raw=git(preserve.commit,file),manifest=JSON.parse(raw);assert.equal(sha(raw),preserve.manifest_sha256);assert.equal(manifest.files.length,98);
 for(const row of manifest.files){assert.equal(sha(fs.readFileSync(path.join(own,row.file))),row.sha256);assert.equal(sha(git(preserve.commit,row.file)),row.sha256);}
 const drop=read('EXPLICIT_FUNCTION_DROP_REVIEW.json');assert.equal(drop.all_literal_qualified_drop_signatures_removed_in_source_order,true);assert.equal(drop.dynamic_ddl_requires_actual_protected_compile_review,true);
 for(const row of [...drop.prior_function_drop_declarations,...drop.actual_function_drop_declarations])assert.equal(sha(row.definition),row.definition_sha256);
});
