import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const pins=JSON.parse(read('CURRENT_NATIVE_SOURCE_PINS.json')),owner=s=>read(pins.selected_literal_owners.find(r=>r.id===s).saved);
test('all selected whole native literal definitions have unique saved identity/hash',()=>{
 assert.equal(new Set(pins.selected_literal_owners.map(r=>r.saved)).size,pins.selected_literal_owners.length);
 for(const r of pins.selected_literal_owners)assert.equal(sha(read(r.saved)),r.definition_sha256,r.id);
 assert.deepEqual([pins.roots,pins.input_paths,pins.include_occurrences],[1484,1745,2160]);assert.equal(pins.guarded_pg_get_functiondef_read_sites,22);
});
test('current positive classifier comes from treatment-aware owner, not earlier whole-file copies',()=>{
 const r=pins.selected_literal_owners.find(r=>r.id==='private.bpay_next_workbench_refresh_work_core_v1(uuid)');
 assert.equal(r.definition_sha256,'39596ef09ff413e81292f7f0bbea02df551730f343a2687c69ff8ae39b8a2769');
 const s=read(r.saved);assert.match(s,/v_source_event:=v_revision.source_event_id/);assert.match(s,/bpay_next_treatment_approval_revision_v1\(v_revision.id\)/);
 assert.match(s,/v_item.included_source_ex_vat:=v_positive/);assert.match(s,/v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION'/);
});
test('native positive amount/query scope excludes reservation and recovery portion semantics',()=>{
 const s=owner('private.bpay_next_workbench_refresh_work_core_v1(uuid)');assert.match(s,/v_work.work_kind='SOURCE' and v_revision.source_kind in \('SOURCE','PROTECTED'\)/);
 assert.match(s,/v_item.query_qualified and v_item.portion_kind='AVAILABLE'/);assert.match(s,/v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE'/);
 assert.match(s,/v_item.portion_kind:='RESERVED'/);assert.match(s,/v_item.portion_kind:='RECOVERY'/);
 assert.match(s,/v_item.reason_code:='PAY_ON_HOLD';v_item.reason_priority:=40/);assert.match(s,/v_reason:='IN_PREPARATION';v_priority:=50/);
});
test('native gate has a closed five-key factual ABI and no monetary producer',()=>{
 const gate=owner('private.weekly_source_pay_query_gate_v1(uuid)'),s=owner('private.weekly_source_pay_query_facts_v1(uuid)');
 assert.match(gate,/-'hours_incidents'-'manual_reviews'/);assert.match(s,/'ok',true,'code','OK','scope',v_scope,'blocked',v_blocked/);assert.match(s,/'query_state_sha256',encode\(v_digest,'hex'\)/);
 assert.doesNotMatch(s,/'withheld_pence',|'held_source_ex_vat',|'included_source_ex_vat',/);
});
test('actual native accepted cut stores and enforces exact query segment exclusions',()=>{
 const s=owner('private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)'),raw=read('input/13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql');
 assert.match(s,/v_gate->'blocked'='true'::jsonb and v_query_exclusions is null then v_code:='SOURCE_PAY_QUERY_OPEN'/);
 assert.match(s,/insert into private.bpay_next_run_work_query_snapshot/);assert.match(raw,/BPAY_NEXT_SELECTED_SEGMENT_QUERY_SCOPE_INVALID/);
 assert.match(raw,/exclusion.value->>'segment_key'=s.segment_key/);
});
test('exact native source money domain is finite and already two-decimal; no rounding promise',()=>{
 const s=read('input/04102026_0300_banking_pay_next_case_instruction_core.sql');assert.match(s,/create domain private.bpay_next_penny_amount as numeric/);
 assert.match(s,/value=pg_catalog.trunc\(value,2\)/);assert.match(s,/value::text not in \('NaN','Infinity','-Infinity'\)/);
});
test('current native reader couples group/item, real approval, adopted metadata and financial revision',()=>{
 const s=owner('private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)');
 for(const text of ["g.complete and not g.dirty and g.progress_state='CURRENT'","g.current_revision_id=r.id and w.applied_revision_id=r.id and w.approval_state='APPROVED'",
 'g.metadata_version=md.metadata_version and not md.continuation_pending and md.adopted_metadata_version=md.metadata_version',
 'g.financial_view_revision=wc.financial_view_revision',"i.progress_state='CURRENT' and i.projection_revision=g.projection_revision",'i.financial_view_revision=wc.financial_view_revision and i.metadata_version=md.metadata_version'])assert.ok(s.includes(text),text);
});
test('actual current first-authorise grants typed shift-only exemption and preserves native authorisation',()=>{
 const s=owner('private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)');assert.match(s,/Owner11Oct: these hold their exact shift, never the whole root/);
 assert.match(s,/RESOLVE_MANUAL_QUERY/);assert.match(s,/public.timesheet_authorise_generic_atomic\(/);assert.doesNotMatch(s,/'withheld_pence',/);
});
test('native invoice duty is complete outstanding duty, not a newly interpreted hold',()=>{
 const s=owner('private.weekly_source_invoice_duty_v1(uuid)');assert.match(s,/'present',case when v_present then true when v_unknown then null else false end/);
 assert.match(s,/'discovery_complete',not v_unknown,'basis',v_basis/);assert.doesNotMatch(s,/'holds_invoice',|'pending_decision',|'withheld_pence',/);
});
