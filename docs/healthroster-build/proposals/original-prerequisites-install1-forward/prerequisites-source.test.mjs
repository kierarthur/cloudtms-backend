import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
import {declarationIdentity} from '../strict-qualification-install1-forward/native-source-identity.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..');
const read=file=>fs.readFileSync(path.join(own,file)),local=file=>fs.readFileSync(path.join(out,file),'utf8'),json=file=>JSON.parse(local(file));
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const plan=json('INPUTS_AND_ORDER.json'),definitions=json('COMPLETE_FINAL_DEFINITIONS.json');
const final=signature=>definitions.comparison_to_actual37.find(row=>row.signature===signature);
const body=signature=>{const row=final(signature);assert.ok(row,signature);const source=read(row.destination).toString('utf8');for(const unit of scanManagedSql(source).statements){const text=source.slice(unit.start,unit.end),head=text.match(/^\s*create\s+(?:or\s+replace\s+)?function\s+((?:private|public)\.\w+)\s*\(([\s\S]*?)\)\s*returns\b/i);if(head&&declarationIdentity(head[1],head[2]).signature===signature)return text;}throw Error(signature);};

test('all complete source components preserve immutable input bytes and selected order',()=>{
 assert.equal(plan.ordered_source_occurrences.length,51);
 for(const row of plan.ordered_source_occurrences){const git=execFileSync('git',['show',row.commit+':'+row.file],{cwd:own,maxBuffer:32000000});assert.equal(sha(git),row.sha256);assert.equal(sha(read(row.destination)),row.sha256);assert.equal(read(row.destination).length,row.bytes);}
 assert.deepEqual(plan.chronological_phases.map(row=>row.input_count),[12,8,27,2,2]);
});
test('main source expands only exact relative Supabase support includes',()=>{
 const source=read(plan.main_include).toString('utf8'),parsed=scanManagedSql(source);assert.equal(parsed.statements.length,0);assert.equal(parsed.meta.length,51);
 assert.deepEqual([...source.matchAll(/^\\ir ([^\r\n]+)$/gm)].map(match=>match[1]),plan.ordered_source_occurrences.map(row=>path.posix.basename(row.destination)));
 for(const row of plan.ordered_source_occurrences){assert.match(row.destination,/^supabase\/support\/.*\.sqlinc$/);assert.equal(scanManagedSql(read(row.destination).toString('utf8')).meta.length,0);}
 assert.doesNotMatch(source,/docs\//);assert.equal(fs.readdirSync(path.dirname(path.join(own,plan.main_include))).filter(file=>file.endsWith('.sql')).length,0);
});
test('one-time CAP constraint source is exact and separate from repeatables',()=>{
 const row=plan.one_time_constraint;assert.equal(sha(read(row.destination)),row.sha256);assert.equal(scanManagedSql(read(row.destination).toString('utf8')).statements.length,1);
 assert.doesNotMatch(read(plan.main_include).toString('utf8'),/CAP_CONSTRAINT_ONE_TIME/);assert.match(read(row.destination).toString('utf8'),/purpose_checks<>1 or version_checks<>1/);assert.match(read(row.destination).toString('utf8'),/SCHEMA_ALREADY_EXTENDED/);
});
test('final source definitions have full bodies and no truncated duplicate signature',()=>{
 assert.equal(definitions.all_original_declaration_occurrences,79);assert.equal(definitions.final_distinct_semantic_declarations,38);
 assert.equal(new Set(definitions.comparison_to_actual37.map(row=>row.semantic_signature)).size,38);
 for(const row of definitions.comparison_to_actual37)assert.equal(sha(body(row.signature)),row.definition_sha256);
});
test('Q25 owner and real XID/PID scope survive final Q45 input',()=>{
 assert.match(body('private.weekly_source_protected_qualification_owner_v1()'),/session_user is distinct from owner_name or current_user is distinct from owner_name/);
 const scope=body('private.weekly_source_protected_qualification_assert_v1(uuid,uuid,uuid[],text)');assert.match(scope,/pg_current_xact_id\(\)::text/);assert.match(scope,/pg_backend_pid\(\)::text/);assert.match(scope,/WEEKLY_SOURCE_QUALIFICATION_SCOPE_ADMITTED/);assert.match(scope,/scope_sha256/);
});
test('Q45 actor selection wrapper and full admission retain original native ABI',()=>{
 const wrapper=body('private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb)');assert.match(wrapper,/p_source_cycle_id,p_actor_user_id,p_own_stage_context,null::uuid\[\]/);
 const cas=body('private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[],uuid)');assert.match(cas,/protected_test_scope_assert_v1\(p_cycle_id,p_actor_user_id,clients,null\)/);assert.match(cas,/sqlerrm<>'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE'/);
 for(const name of ['prepare_family','prepare_action','action_context','stage_c1_request'])assert.equal(final('public.weekly_exceptional_pay_'+name+'_v1(jsonb)').phase,'Q45_ACTOR_EVENT_CLOSED_DTO_CROSS_SESSION');
});
test('F12 corrected certificate and full TEST journey owners are final',()=>{
 for(const signature of ['private.weekly_source_protected_operational_certificate_assert_v1(jsonb,uuid,date,date,text)','private.weekly_source_protected_test_scope_assert_v1(uuid,uuid,uuid[],uuid)'])assert.equal(final(signature).phase,'F12_FIRST_USE_FULL_INVENTORY');
 const cert=body('private.weekly_source_protected_operational_certificate_assert_v1(jsonb,uuid,date,date,text)');assert.match(cert,/test_setup_closure/);assert.match(cert,/CLIENT_HTTP/);assert.match(cert,/MYTMS_PHONE/);assert.match(cert,/source_equivalence/);
});
test('original Source39 keys and CAS wrapper are preserved by final schema/lineage input',()=>{
 const source=json('SOURCE39_PRESERVATION.json');assert.equal(source.object_key_count,39);assert.equal(source.fact_keys.length,39);assert.equal(new Set(source.fact_keys).size,39);assert.ok(source.fact_keys.includes('lineages'));assert.ok(source.fact_keys.includes('root_identities'));assert.ok(source.original_wrapper_unchanged);
 assert.equal(final('private.weekly_source_bulk_source_facts_v1(uuid,uuid[])').phase,'FINAL_ORIGINAL_SOURCE_CAS_SCHEMA_LINEAGE');assert.equal(sha(body('private.weekly_source_bulk_source_cas_v1(uuid,uuid[])')),source.wrapper_definition_sha256);
});
test('complete own-created-root lineage equals the actual a634 native owner and NEXT hook',()=>{
 const r=json('LINEAGE_CURRENT_OWNER_BINDING.json');assert.ok(r.entire_current_a634_definition_equal);assert.ok(r.native_next_hook_bytes_equal);assert.equal(r.forward_entire_definition_sha256,r.entire_current_root_definition_sha256);
 const lineage=body('public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)');assert.equal(sha(lineage),r.forward_entire_definition_sha256);assert.match(lineage,/if v_created_timesheet and v_finalisation_lock_operation\.id is null/);assert.match(lineage,/v_legacy_root_set_now is distinct from v_finalisation_root_set/);assert.match(lineage,/v_lineage_observed is distinct from v_lineage_discovery/);assert.match(lineage,/v_known_root_timesheet_id is not null/);
});
test('exact a634 reconciliation accounts for all83 with no external absence claim',()=>{
 const r=json('ROOT_A634_SOURCE_RECONCILIATION.json');assert.equal(r.present_exact_root,83);assert.equal(r.absent_root_external_source_required,0);assert.equal(r.all83.length,83);assert.ok(r.all83.every(row=>row.actual_root));assert.equal(r.prerequisite_owner_overrides_current_root.length,11);
 assert.deepEqual(r.prerequisite_owner_overrides_current_root.filter(row=>row.root_current_native_final_override_after_prerequisite_required).map(row=>row.semantic_signature),['public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)']);
});
test('complete source reverse records restore actual37 and entire a634 maps',()=>{
 const r=json('SOURCE_DECLARATION_REVERSE.json'),current=json('ROOT_A634_SOURCE_DECLARATION_REVERSE.json');assert.equal(r.entire_actual37_symbols_restored,2440);assert.equal(r.entire_reverse_hash_mismatches,0);assert.equal(current.entire_symbols_before,2616);assert.equal(current.entire_symbols_restored,2616);assert.equal(current.entire_reverse_hash_mismatches,0);
 assert.ok(local(r.complete_actual_predecessors_file).includes('create or replace function'));assert.ok(local(current.complete_predecessor_file).includes('create or replace function'));
});
test('runtime integration supplies exact references and prohibits old full caller copies',()=>{
 const r=json('RUNTIME_HUNK_INTEGRATION_RECIPE.json');assert.equal(r.whole_old_managed_writer_bank_or_current_caller_mount_allowed,false);assert.ok(r.require_exact_current_source_hunks_and_entire_reverse);assert.equal(sha(read(r.reviewed_recipe_pin.file)),r.reviewed_recipe_pin.sha256);
 for(const [file,pin]of [['Q25_EXACT_OWNER_DELTA_REFERENCE.json',r.original_owner_delta_inputs[0]],['Q45_EXACT_OWNER_DELTA_REFERENCE.json',r.original_owner_delta_inputs[1]],['Q45_EXACT_INDEX_HUNKS_REFERENCE.json',r.original_owner_delta_inputs[2]]])assert.equal(sha(Buffer.from(local(file))),pin.sha256);
});
test('both earlier frozen packets remain exact saved and committed bytes',()=>{
 for(const dir of ['strict-qualification-native-interfaces','strict-qualification-install1-forward']){const file='docs/healthroster-build/proposals/'+dir+'/REVIEW_MANIFEST.json',manifest=JSON.parse(read(file));for(const row of manifest.files){assert.equal(sha(read(row.file)),row.sha256);assert.equal(sha(execFileSync('git',['show','e9deed4f1c6da8e79f77f6262c9c10cb699b2f1b:'+row.file],{cwd:own,maxBuffer:32000000})),row.sha256);}}
});
