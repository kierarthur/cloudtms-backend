import assert from 'node:assert/strict';import test from 'node:test';import fs from 'node:fs';import path from 'node:path';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import {loadProjectionOwner} from './source-runtime-vm.mjs';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const {module:owner}=loadProjectionOwner(),id=n=>`10000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
function row(extra={}){return {
 upload_row_id:id(1),source_row_ordinal:2,external_source_key:'B',candidate_match_count:1,candidate_id:id(2),client_id:id(3),
 work_date:'2026-10-05',start_at_local:'2026-10-05T09:00:00',end_at_local:'2026-10-05T18:00:00',break_minutes:0,actual_net_minutes:540,
 row_finalisation_state:'SOURCE_WORKED',prior_work_event_id:null,prior_accepted_contract_id:null,protected_matches:[],
 contracts:[{contract_id:id(4),candidate_id:id(2),client_id:id(3),valid_from:'2026-01-01',valid_to:null,pay_type:'PAYE',weekly_source_applicable:true,
  rates_json:{paye_day:10,paye_night:10,paye_sat:10,paye_sun:10,paye_bh:10,charge_day:20,charge_night:20,charge_sat:20,charge_sun:20,charge_bh:20},
  effective_policy:{authority_mode:'SOURCE_AUTHORITY',self_bill_enabled:true,c1_source_mode:'HEALTHROSTER_WEEKLY',weekly_rate_classification_method:'SPLIT_RATE_WINDOWS',duration_break_tie_rule:'EARLIEST_LONGEST_PORTION',policy_sha256:'a'.repeat(64)},
  settings_authority:{values:{timezone_id:'Europe/London'}}}],...extra};}
const project=r=>owner.buildWeeklySourceProjectionRows({profile_id:'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',rows:[r]})[0];
const overlap=paid=>({work_event_id:id(5),native_payment_facts:{paid}});
const choice=effect=>({schema_version:'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1',upload_row_id:id(1),candidate_id:id(2),client_id:id(3),contract_id:id(4),paid_overlap_target_work_event_id:id(5),pay_effect:effect,choice_proof_sha256:'b'.repeat(64)});
test('actual whole Broker module blocks unknown-key paid overlap',()=>assert.equal(project(row({paid_overlap_inputs:{overlaps:[overlap(true)]}})).blocker_code,'PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED'));
test('UNKNOWN paid evidence cannot become a false not-paid admission',()=>assert.equal(project(row({paid_overlap_inputs:{overlaps:[overlap(null)]}})).blocker_code,'PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED'));
test('false native paid evidence keeps ordinary real C1 calculation',()=>{const result=project(row({paid_overlap_inputs:{overlaps:[overlap(false)]}}));assert.equal(result.mapping_state,'RESOLVED');assert.equal(result.economic_snapshot.total_pay_pence,'9000');});
test('Same preserves positive raw Source calculation and real event; Worker asserts no NONE flag',()=>{const result=project(row({paid_overlap_choice:choice('NONE')}));assert.equal(result.mapping_state,'RESOLVED');assert.equal(result.prior_work_event_id,id(5));assert.equal(result.economic_snapshot.total_pay_pence,'9000');assert.ok(!('pay_effect' in result));assert.ok(!('paid_overlap_choice_id' in result));});
test('Separate removes inherited prior and preserves ordinary new-key construction',()=>{const result=project(row({paid_overlap_choice:choice('NORMAL'),prior_work_event_id:id(5)}));assert.equal(result.mapping_state,'RESOLVED');assert.equal(result.prior_work_event_id,null);assert.equal(result.profile_external_key,'B');});
test('foreign Candidate choice does not become a local confirmation',()=>assert.equal(project(row({paid_overlap_choice:{...choice('NONE'),candidate_id:id(6)}})).blocker_code,'PAID_SHIFT_MATCH_CHOICE_STALE'));
test('wrong Contract and malformed proof hash do not become confirmations',()=>{for(const c of [{...choice('NONE'),contract_id:id(7)},{...choice('NONE'),choice_proof_sha256:null}])assert.equal(project(row({paid_overlap_choice:c})).blocker_code,'PAID_SHIFT_MATCH_CHOICE_STALE');});
test('native automatic/known prior retains precedence over paid overlap question',()=>assert.equal(project(row({prior_work_event_id:id(5),paid_overlap_inputs:{overlaps:[overlap(true)]}})).prior_work_event_id,id(5)));
test('all complete owner reverses are raw byte proofs',()=>{
 let checked=0;
 const singletonOutputs={'CURRENT_WRITER_REVERSE.json':'02_complete_office_recheck.sqlinc','FIN4_REVERSE.json':'06_complete_fin4.sqlinc','RUNTIME_REVERSE.json':'broker/src/weekly-source/upload-publication-owner.mjs'};
 for(const file of ['CURRENT_WRITER_REVERSE.json','PROJECTION_REVERSES.json','FIN4_REVERSE.json','INVOICE_CONSUMER_REVERSES.json','PAY_CONSUMER_REVERSES.json','RUNTIME_REVERSE.json']){
  const doc=JSON.parse(read(file));for(const r of doc.records??[doc]){
   const output=r.output??singletonOutputs[file];assert.ok(output,file);
   const before=read('before/'+output.replace(/^one-time\//,''));let after=read((output.startsWith('one-time/')?'':'forward/')+output);
   if(r.after_leaf_sha256)assert.equal(sha(after),r.after_leaf_sha256,output);
   else if(r.after_sha256)assert.equal(sha(after),r.after_sha256,output);
   if(r.after_leaf_sha256&&r.after_definition_sha256){after=after.slice(0,after.indexOf('\nALTER FUNCTION'));}
   if(output.startsWith('one-time/')){const marker='alter table public.weekly_source_billing_movements\n  add constraint';after=after.slice(after.indexOf(marker),after.indexOf('\ncreate unique index'));}
   if(r.after_definition_sha256)assert.equal(sha(after),r.after_definition_sha256,output);
   for(const h of [...r.hunks].reverse()){
    assert.equal(after.split(h.after).length-1,h.occurrences??1,output+': '+h.name);
    after=after.replaceAll(h.after,h.before);
   }
   assert.equal(after,before,output);assert.equal(sha(before),r.before_sha256??r.input_sha256,output);checked++;
  }
 }
 assert.equal(checked,16);
});
test('native complete family scope precedes gate/group/cycle; full set reproof precedes row writes',()=>{const s=read('forward/05_complete_projection_rows.sqlinc');const lock=s.indexOf('v_paid_choices:=');assert.ok(lock<s.indexOf('pg_advisory_xact_lock'));assert.ok(s.indexOf('v_paid_recheck is distinct')<s.indexOf('insert into public.weekly_source_row_resolutions'));assert.match(s,/WEEKLY_SOURCE_CALLER_PAY_EFFECT_FORBIDDEN/);});
test('native financial methods and existing Source raw vectors remain intact',()=>{const s=read('forward/06_complete_fin4.sqlinc');assert.match(s,/current_line\.pay_effect='NORMAL'/);assert.match(s,/join prior_lines prior on prior\.work_event_id=basis\.work_event_id and prior\.id=basis\.id/);assert.match(s,/RETAINED_PAID_MATCH/);assert.match(s,/v_row\.pay_effect,v_row\.paid_overlap_choice_id/);assert.match(s,/weekly_source_finalisation_vector_v1\(v_economic\.id,'PAY',1::smallint\)/);assert.doesNotMatch(s,/update public\.weekly_source_final_revisions set paid_overlap_choice_census/);});
test('invoice companion uses genuine independent origin and existing native charge/hold owner',()=>{const s=read('forward/08_paid_match_invoice_producer.sqlinc');assert.match(s,/weekly_source_invoice_line_amounts_v1\(r\.id,revision\.id\)/);assert.match(s,/weekly_source_invoice_only_vector_v1\('PAY',0\)/);assert.match(s,/invoice_only_snapshot_line_id,pay_effect,final_revision_id/);assert.doesNotMatch(s,/'row_resolution_id',r\.id,'paid_overlap/);});
test('stateful revision census is immutable without blocking ordinary lifecycle',()=>{const s=read('forward/07_paid_choice_revision_guard.sqlinc');assert.match(s,/old\.paid_overlap_choice_census is distinct from new\.paid_overlap_choice_census/);assert.equal((s.match(/create or replace trigger/g)??[]).length,3);assert.doesNotMatch(s,/old\.state.*new\.state/);});
test('actual parent/raw/audit bindings are enforced by historical invoice origin',()=>{const s=read('forward/09_paid_match_invoice_origin_assert.sqlinc');for(const marker of ['row.normalised_row_hash','r.paid_overlap_choice_id','a.object_id_text','journal.request_hash','m.movement_economic_hash','facts ? \'row_resolution_id\''])assert.ok(s.includes(marker),marker);});
test('none-only source presence and original pay basis are distinct in self-bill reader',()=>{const s=read('forward/14_complete_self_bill_shift_facts.sqlinc');assert.match(s,/resolution\.pay_effect='NORMAL'/);assert.match(s,/companion\.pay_effect='NONE'/);assert.match(s,/prior_cycle\.finalisation_week_ending<c\.finalisation_week_ending/);assert.match(s,/prior_snapshot\.pay_effect='NORMAL'/);assert.match(s,/Native|native|immutable/);});
test('finalized source exposes recorded choice; fresh writers refuse its recorded-only schema',()=>{const s=read('forward/03_paid_overlap_choice_current.sqlinc');assert.match(s,/cycle\.current_final_revision_id=revision\.id/);assert.match(s,/WEEKLY_SOURCE_PAID_OVERLAP_RECORDED_CHOICE_V1/);assert.match(s,/'recorded_only',true/);assert.ok(s.indexOf('if found then')<s.indexOf('v_observed:=private.weekly_source_paid_overlap_candidates_v1'));assert.match(read('forward/06_complete_fin4.sqlinc'),/v_paid_choice->>'schema_version' is distinct from 'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1'/);});
test('all new/proposed SQL leaves are finite managed-source statements only',()=>{
 for(const sub of ['forward','one-time'])for(const name of fs.readdirSync(path.join(dir,sub)).filter(n=>n.endsWith('.sqlinc'))){const s=read(sub+'/'+name),parsed=scanManagedSql(s);assert.ok(parsed.statements.length,name);assert.ok(!parsed.meta.length,name);assert.doesNotMatch(s,/create\s+table\s+.*(pay|cash|settlement)/i,name);}
});
