import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
import * as api from './PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),current='a63455e878391cb3ee9732165e58c0db517f48a7';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),local=file=>fs.readFileSync(path.join(out,file),'utf8'),json=file=>JSON.parse(local(file)),source=file=>fs.readFileSync(path.join(own,file),'utf8');
const plan=json('INPUTS_AND_ORDER.json'),proofs=json('COMPLETE_CURRENT_OWNER_REVERSE.json').proofs,union=json('LITERAL_PRIVATE_ADDITION_UNION.json');
const page=i=>source(plan.ordered_source_pages[i-1].output);
test('original119 frozen artifacts and manifests retain exact Git bytes',()=>{
 const p=json('ORIGINAL_FROZEN_PRESERVATION.json');assert.equal(p.manifests.length,5);assert.equal(p.manifests.reduce((n,row)=>n+row.listed_artifacts,0),119);assert.equal(p.original_frozen_packets_modified,false);
 for(const pin of p.manifests)assert.equal(sha(execFileSync('git',['show',current+':'+pin.file],{cwd:own,maxBuffer:32000000})),pin.sha256);
});
test('eight complete sources are contained support includes in dependency order',()=>{
 assert.equal(plan.ordered_source_pages.length,8);const parsed=scanManagedSql(source(plan.main_include));assert.equal(parsed.statements.length,0);assert.equal(parsed.meta.length,8);assert.doesNotMatch(source(plan.main_include),/docs\//);
 assert.deepEqual([...source(plan.main_include).matchAll(/^\\ir (.*)$/gm)].map(m=>m[1]),plan.ordered_source_pages.map(row=>path.posix.basename(row.output)));
 for(const row of plan.ordered_source_pages){assert.equal(scanManagedSql(source(row.output)).meta.length,0);assert.match(row.output,/^supabase\/support\/.*\.sqlinc$/);if(!row.complete_current_hunk_proof)assert.equal(sha(source(row.output)),row.input.sha256);}
});
test('all current adapter approval request and Broker substitutions reverse entire actual files',()=>{
 assert.equal(proofs.length,4);for(const p of proofs){let result=execFileSync('git',['show',current+':'+p.input.file],{cwd:own,maxBuffer:32000000}).toString('utf8');assert.equal(sha(result),p.input.sha256);for(const h of p.hunks){if(h.anchor==='START_OF_MODULE')result=h.replacement+result;else{assert.equal(result.split(h.anchor).length,2);result=result.replace(h.anchor,()=>h.replacement);}}assert.equal(sha(result),p.output_sha256);for(const h of [...p.hunks].reverse()){if(h.anchor==='START_OF_MODULE')result=result.slice(h.replacement.length);else result=result.replace(h.replacement,()=>h.anchor);}assert.equal(sha(result),p.input.sha256);assert.ok(p.entire_reverse_equal);}
});
test('native normalisation uses unchanged stored-row sign copy and actual original/P0 rereads',()=>{
 const approval=page(7);assert.match(approval,/canonical_input_assert_v1\(v_original/);assert.match(approval,/snapshot_identity_assert_v1\(v_original/);assert.match(approval,/snapshot_identity_assert_v1\(v_after_fin/);assert.match(approval,/original_materialised_in_unit/);
 const native=proofs.find(p=>p.name==='CURRENT_COMPLETE_APPROVAL');assert.equal(native.input.sha256,'9d3129c7fdbb4ddf1b8ca5c2a195b3fcfa7ea661e4c4dc0a7f233eea77571fc1');assert.equal(native.hunks.length,8);
});
test('native Candidate/I1 fence precedes established atomic locks and SEND replay is retained',()=>{
 const approval=page(7),anchor=approval.indexOf('v_a0_lock_fence:=private.timesheet_correction_a0_native_lock_fence_v1');assert.ok(anchor>0);assert.ok(anchor<approval.indexOf('perform 1 from public.invoices'));
 const request=page(8),fence=request.indexOf('perform private.timesheet_correction_a0_send_family_fence_v1(v_id)');assert.ok(fence>0&&fence<request.indexOf('-- Lock every existing invoice first'));assert.match(request.slice(0,fence),/return /i);
 const owners=page(4)+page(5)+page(6);assert.match(owners,/weekly_source_candidate_serial_gate_v1/);assert.match(owners,/weekly_source_lock_family_rows_v1/);assert.match(owners,/set_config\('lock_timeout','1ms',true\)/);assert.match(owners,/exception when others then perform set_config\('lock_timeout'/);
});
test('all originalfour plus creatorsix private signatures are literal owner-only additions',()=>{
 assert.equal(union.preceding_normalisation_fence_four.length,4);assert.equal(union.creator_six.length,6);assert.equal(union.literal_union_count,10);assert.equal(union.new_service_or_public_signature_count,0);
 const all=[...union.preceding_normalisation_fence_four,...union.creator_six];assert.equal(new Set(all.map(row=>row.signature)).size,10);
 for(const row of all){const s=source(row.file),name=row.signature.slice(0,row.signature.indexOf('('));assert.match(s,new RegExp('revoke all on function '+name.replaceAll('.','\\.')+'\\([^;]+from public,anon,authenticated,service_role'));assert.ok(api.functionEvents(s).events.some(event=>event.id===row.signature));}
});
test('native Worker read route roles and calculator dispatch match current root without old index copy',()=>{
 const e=json('CURRENT_NATIVE_EXTRACTION_EQUALITY.json');assert.ok(e.original_block_hashes_all_equal);assert.equal(e.current_broker.sha256,'ca621191964eaf1c707b38c3cabaf69ade0a717dc4d0b3b94c183788fe293785');assert.equal(e.actual_daily_source.sha256,'64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b');
 for(const key of ['actual_current_roles','actual_current_weekly_route','actual_current_weekly_calculator_dispatch'])assert.equal(sha(e[key].source),e[key].sha256);
 const runtime=json('RUNTIME_DESTINATIONS.json');assert.equal(runtime.files.length,5);assert.equal(runtime.route_added,false);assert.ok(!fs.existsSync(path.join(out,'runtime/index.js')));assert.ok(!fs.existsSync(path.join(out,'BROKER_INDEX_SOURCE_FORWARD.js')));
});
test('all actual trigger names preserve native ordering and four admitted new triggers',()=>{
 const r=json('CURRENT_NATIVE_TRIGGER_ORDER.json');assert.equal(r.proposed_trigger_count,4);assert.equal(r.order_checks.length,28);assert.ok(r.order_checks.every(row=>row.pass));assert.equal(r.physical_enabled_state_not_observed,true);
 const intents=page(5);assert.equal((intents.match(/create trigger /g)||[]).length,4);assert.match(intents,/before insert or update or delete on public\.candidates/);assert.match(intents,/before insert or update on public\.timesheets/);
});
test('hidden RR and five native schedules remain held without global refusal or invented authority',()=>{
 const c=json('CURRENT_FOUR_TABLE_CREATOR_CENSUS.json');assert.equal(c.qualified_runtime_isolation,null);assert.equal(c.hidden_rr_visibility_solved,false);assert.equal(c.native_holds.length,5);
 const s=page(4)+page(5)+page(6);assert.match(s,/if v_requests is not null then[\s\S]*?transaction_isolation/);assert.doesNotMatch(s,/set_config\('transaction_isolation'|new\.(?:updated_at|rev)\s*:=|create (?:temp )?table|foreign key|pg_advisory_xact_lock|hashtext\(/i);assert.equal(plan.hidden_rr_visibility_solved,false);
});
test('current creator census distinguishes source writers dynamic candidates and native edges',()=>{
 const c=json('CURRENT_FOUR_TABLE_CREATOR_CENSUS.json');assert.equal(c.controlling_literal_writer_candidates.length,71);assert.equal(c.first_insert_creator_candidates.length,30);assert.equal(c.original25_native_owner_reconciliation.length,25);assert.ok(c.original25_native_owner_reconciliation.every(row=>row.entire_definition_equal));assert.ok(c.controlling_literal_writer_candidates.every(row=>row.proven_native_invoked_edges.length===0));
 assert.equal(c.original70_writer_reconciliation.length,70);assert.ok(c.original70_writer_reconciliation.every(row=>row.current));assert.deepEqual(c.additional_current_writers.map(row=>row.signature),['public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb)']);assert.match(local('ADDITIONAL_CURRENT_WRITER_INPUT_ONLY.sqlinc'),/INPUT ONLY current/);
 const schema=json('CURRENT_SCHEMA_AND_NATIVE_HOLDS.json');assert.equal(schema.schema_checks.length,7);assert.ok(schema.schema_checks.every(row=>row.missing.length===0));assert.equal(schema.qualified_runtime_isolation,null);
});
test('corrected event map reconciles three pg_temp helpers and reverses complete finite map',()=>{
 const r=json('CORRECTED_SCANNER_CURRENT_RECONCILIATION.json');assert.equal(r.earlier_qualified_semantic_map_symbols,2616);assert.equal(r.corrected_finite_event_map_symbols,2619);assert.equal(r.only_in_earlier_qualified_map.length,0);assert.equal(r.only_in_corrected_event_map.length,3);assert.ok(r.only_in_corrected_event_map.every(row=>row.signature.startsWith('pg_temp.')));
 const reverse=json('ENTIRE_CURRENT_SOURCE_DECLARATION_REVERSE.json');assert.equal(reverse.source_symbols_restored,2619);assert.equal(reverse.source_symbols_after,2629);assert.equal(reverse.reverse_hash_mismatches,0);assert.equal(json('CURRENT_NATIVE_DECLARATION_CHANGES.json').complete_changes.length,3);
});
test('original test reference is exact and prior198 saved artifacts remain unchanged',()=>{
 const refs=json('ORIGINAL_TEST_REFERENCE_PINS.json');assert.equal(refs.original_tests.length,7);for(const row of refs.files)assert.equal(sha(fs.readFileSync(path.join(out,refs.reference_tree,row.file))),row.sha256);
 for(const dir of ['strict-qualification-native-interfaces','strict-qualification-install1-forward','original-prerequisites-install1-forward']){const p='docs/healthroster-build/proposals/'+dir+'/REVIEW_MANIFEST.json';for(const row of JSON.parse(source(p)).files)assert.equal(sha(fs.readFileSync(path.join(own,row.file))),row.sha256);}
});
