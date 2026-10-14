import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),parent=path.dirname(dir),sha=b=>createHash('sha256').update(b).digest('hex'),commit='fa66183f01913e86651e82816f5e88f97e07123c';
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const sourceFile='missing-accept-current-native-construction-20261014/forward/08_complete_kept_context.sqlinc',before=fs.readFileSync(path.join(parent,sourceFile));assert.deepEqual(before,execFileSync('git',['show','4c63b81b94406da7d750746520b2ae57b9b24a80:docs/healthroster-build/proposals/'+sourceFile],{cwd:root,maxBuffer:2e6}));
const s=before.toString(),eol=s.includes('\r\n')?'\r\n':'\n',start=s.indexOf('  select coalesce(array_agg(distinct scope.canonical_timesheet_id'),endMarker='  -- Exactly the same real credit-line membership as the planned facts hash.',end=s.indexOf(endMarker,start),block=s.slice(start,end);assert.ok(start>0&&end>start);assert.match(block,/if cardinality\(v_movements\)>1/);assert.match(block,/v_invoiced:=v_invoiced or exists/);
const helper=`-- Source-owned extraction of the existing common payment/invoice facts.
-- Native calculations and paid/UNKNOWN predicates below are byte-preserved.
create or replace function private.weekly_source_work_event_payment_facts_v1(
  p_work_event_id uuid,p_actor_user_id uuid,p_source_cycle_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_event public.weekly_work_events%rowtype;v_resolution public.weekly_source_row_resolutions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_root uuid;v_roots uuid[];v_revision uuid;v_segments jsonb;v_segment jsonb;
  v_movement uuid;v_movements uuid[]:='{}'::uuid[];v_members uuid[];
  v_invoiced boolean:=false;v_invoice_facts jsonb;v_invoice_row jsonb;
  v_paid_state text:='UNKNOWN';v_paid boolean;v_paid_basis jsonb:='[]'::jsonb;v_unknown boolean:=false;
  v_page jsonb;v_header jsonb;v_after jsonb:=null;v_rows jsonb;v_item jsonb;v_seen jsonb:='[]'::jsonb;
  v_pages integer;v_complete boolean;v_page_positive boolean;v_component uuid;v_component_count integer;
  v_matched_component_count integer;v_settlement jsonb;v_shift jsonb;
begin
  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=p_source_cycle_id;
  if v_cycle.source_group_id is distinct from v_event.first_source_group_id
    or (v_cycle.scope_client_id is not null and v_cycle.scope_client_id is distinct from v_event.client_id) then
    raise exception 'WEEKLY_SOURCE_PAYMENT_FACTS_SCOPE_INVALID' using errcode='22023';end if;
  select * into strict v_resolution from public.weekly_source_row_resolutions
    where work_event_id=v_event.id and mapping_state='RESOLVED' order by created_at_utc desc,id desc limit 1;
  if v_resolution.candidate_id is distinct from v_event.candidate_id or v_resolution.client_id is distinct from v_event.client_id
    or not exists(select 1 from public.contracts c where c.id=v_resolution.contract_id
      and c.candidate_id=v_event.candidate_id and c.client_id=v_event.client_id) then
    raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_LINEAGE_UNBOUND' using errcode='55000';end if;
  if p_actor_user_id is not null then perform private.weekly_source_office_authority_v1(p_actor_user_id,'VIEW_PAY_WORK',
    v_event.first_source_group_id,v_event.client_id,v_cycle.finalisation_week_ending);end if;
${block.replaceAll('\r\n','\n')}  return jsonb_build_object('schema_version','WEEKLY_SOURCE_WORK_EVENT_PAYMENT_FACTS_V1',
    'work_event_id',v_event.id,'candidate_id',v_event.candidate_id,'client_id',v_event.client_id,
    'source_group_id',v_event.first_source_group_id,'source_cycle_id',v_cycle.id,
    'root_timesheet_ids',to_jsonb(v_roots),'current_pay_head_movement_ids',to_jsonb(v_movements),
    'current_pay_head_movement_id',v_movements[1],'invoiced',v_invoiced,'paid',v_paid,
    'paid_evidence_state',v_paid_state,'native_paid_basis',v_paid_basis,
    'paid_quantity_known',false,'withheld_amount_known',false);
end;
$function$;
alter function private.weekly_source_work_event_payment_facts_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_work_event_payment_facts_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
`.replaceAll('\n',eol);
const replacement=`  v_payment_facts:=private.weekly_source_work_event_payment_facts_v1(v_event.id,p_actor_user_id,v_cycle);
  v_roots:=array(select value::uuid from jsonb_array_elements_text(v_payment_facts->'root_timesheet_ids') value);
  v_movements:=array(select value::uuid from jsonb_array_elements_text(v_payment_facts->'current_pay_head_movement_ids') value);
  v_invoiced:=(v_payment_facts->>'invoiced')::boolean;
  v_paid:=(v_payment_facts->>'paid')::boolean;
  v_paid_state:=v_payment_facts->>'paid_evidence_state';
  v_paid_basis:=v_payment_facts->'native_paid_basis';
`.replaceAll('\n',eol);
const hunks=[{name:'one Source-owned payment facts delegation variable',before:'  v_unknown boolean:=false;',after:'  v_unknown boolean:=false;v_payment_facts jsonb;'},{name:'one native full payment/invoice facts block extraction and unchanged-field delegation',before:block,after:replacement}];let after=s;
for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,s);
for(const side of ['before','forward','input'])fs.mkdirSync(path.join(dir,side),{recursive:true});fs.writeFileSync(path.join(dir,'before/02_complete_keep_context.sqlinc'),before);fs.writeFileSync(path.join(dir,'forward/01_work_event_payment_facts.sqlinc'),helper);fs.writeFileSync(path.join(dir,'forward/02_complete_keep_context.sqlinc'),after);fs.writeFileSync(path.join(dir,'input/exact_native_payment_block.sqlinc'),block);
fs.writeFileSync(path.join(dir,'COMPLETE_CONTEXT_REVERSE.json'),JSON.stringify({source_only:true,own_input_commit:'4c63b81b94406da7d750746520b2ae57b9b24a80',input_file:'docs/healthroster-build/proposals/'+sourceFile,before_bytes:before.length,before_sha256:sha(before),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),helper_sha256:sha(helper),exact_extracted_block_sha256:sha(block),exact_native_block_bytes:Buffer.byteLength(block),hunks,unique_hunks:hunks.length,whole_reverse_verified:true,paid_classifier_duplicated:false,canonical_mounted:false,native_proof:false},null,2)+'\n');
const files=['supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc','supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql','supabase/repeatable/04102026_0505_banking_pay_next_source_current_hooks_v1.sql','supabase/repeatable/05102026_1112_bpay_next_source_paid_evidence_page_v1.sql','supabase/repeatable/17092026_1000_weekly_source_settlement_allocation_v1.sql','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc','supabase/repeatable/14102026_0541_healthroster_report_current_native_composition_v1.sql','supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields_v1.sql'];
files.push('supabase/migrations/01102026_1201_weekly_source_client_cycle_scope.sql');
const inputs=files.map(file=>{const b=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:5e6});return{commit,file,bytes:b.length,sha256:sha(b)};});fs.writeFileSync(path.join(dir,'CURRENT_NATIVE_INPUT_PINS.json'),JSON.stringify({source_only:true,current_root_commit:commit,inputs,final_owner_order_and_later_overrides_not_yet_sealed:true,canonical_mounted:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({native_block_bytes:Buffer.byteLength(block),whole_context_hunks:hunks.length,helper_sha256:sha(helper),context_sha256:sha(after),current_root:commit}));
