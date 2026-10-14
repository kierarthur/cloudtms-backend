import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex'),commit='fa66183f01913e86651e82816f5e88f97e07123c';
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const records=[];
function compose(file,id,output,hunks){
 const raw=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:3e6});
 const event=functionEvents(raw.toString()).events.find(e=>e.kind==='CREATE'&&e.id===id);assert.ok(event,id);
 const before=event.definition,eol=before.includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 const acl=`\nALTER FUNCTION ${id} OWNER TO postgres;\nREVOKE ALL ON FUNCTION ${id} FROM public,anon,authenticated${id.startsWith('private.')?',service_role':''};\n${id.startsWith('public.')?`GRANT EXECUTE ON FUNCTION ${id} TO service_role;\n`:''}`.replaceAll('\n',eol);
 fs.writeFileSync(path.join(dir,'before',output),before);fs.writeFileSync(path.join(dir,'forward',output),after+acl);
 records.push({commit,file,input_bytes:raw.length,input_sha256:sha(raw),signature:id,output,before_sha256:sha(before),after_definition_sha256:sha(after),after_leaf_sha256:sha(after+acl),hunks,whole_reverse_verified:true});
}
const money='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/02_money_origins.sqlinc';
compose('supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql','private.weekly_source_shift_on_self_bill_v1(uuid,uuid)','14_complete_self_bill_shift_facts.sqlinc',[
 {name:'one payable resolution per actual event; real invoice companions remain separate',before:"    and resolution.mapping_state='RESOLVED';",after:"    and resolution.mapping_state='RESOLVED' and resolution.pay_effect='NORMAL';"},
 {name:'NONE-only source presence uses the genuine confirmed prior payable basis',before:"  if count_rows>1 then raise exception 'WEEKLY_SOURCE_SELF_BILL_SHIFT_AMBIGUOUS' using errcode='55000'; end if;",after:`  if count_rows=0 then
    -- This selects an actual immutable prior row as PAY basis, not as current
    -- upload membership. Real current NONE rows and their immutable confirmations
    -- are the independent source-presence evidence for this branch.
    select array_agg(distinct prior_resolution.id),count(distinct prior_resolution.id)
      into ids,count_rows
    from public.weekly_source_row_resolutions companion
    join public.weekly_source_upload_rows current_row on current_row.id=companion.upload_row_id
    join public.weekly_source_uploads current_upload on current_upload.id=current_row.upload_id
    join public.weekly_source_projection_publications current_publication
      on current_publication.id=v_selection_publication_id and current_publication.upload_id=current_upload.id
        and current_publication.source_cycle_id=c.id and current_publication.authority_scope_kind='CYCLE'
        and current_publication.state=case when preparing_count=1 then 'CORRECTION_READY' else 'CURRENT' end
    join private.weekly_source_office_row_choices choice on choice.id=companion.paid_overlap_choice_id
      and choice.upload_row_id=current_row.id and choice.pay_effect='NONE' and not choice.separate_shift
      and choice.work_event_id=event_fact.id and choice.paid_overlap_target_work_event_id=event_fact.id
      and choice.candidate_id=event_fact.candidate_id and choice.client_id=event_fact.client_id
    cross join lateral jsonb_array_elements(choice.paid_overlap_inputs_json#>'{inputs,overlaps}') item
    join public.weekly_source_final_snapshot_lines prior_snapshot
      on prior_snapshot.id=(item->>'snapshot_line_id')::uuid and prior_snapshot.work_event_id=event_fact.id
        and prior_snapshot.candidate_id=event_fact.candidate_id and prior_snapshot.client_id=event_fact.client_id
        and prior_snapshot.contract_id=choice.contract_id and prior_snapshot.pay_effect='NORMAL'
    join public.weekly_source_row_resolutions prior_resolution on prior_resolution.id=prior_snapshot.row_resolution_id
      and prior_resolution.work_event_id=event_fact.id and prior_resolution.pay_effect='NORMAL'
    join public.weekly_source_final_revisions prior_revision on prior_revision.id=prior_snapshot.final_revision_id
    join public.weekly_source_cycles prior_cycle on prior_cycle.id=prior_revision.source_cycle_id
      and prior_cycle.source_group_id=c.source_group_id and prior_cycle.finalisation_week_ending<c.finalisation_week_ending
    join public.weekly_source_billing_movements prior_movement on prior_movement.id=(item->>'current_pay_head_movement_id')::uuid
      and prior_movement.work_event_id=event_fact.id and prior_movement.candidate_id=event_fact.candidate_id
      and prior_movement.actual_client_id=event_fact.client_id and prior_movement.contract_id=prior_snapshot.contract_id
      and prior_movement.pay_effect='NORMAL' and prior_movement.movement_role in ('POSITIVE','REPLACEMENT')
      and prior_movement.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT')
    where current_upload.id=v_selection_upload_id and current_upload.source_cycle_id=c.id and current_upload.report_scope_id is null
      and current_upload.state=case when preparing_count=1 then 'CORRECTION_READY' else 'CURRENT' end
      and companion.generation=coalesce(current_publication.projection_generation,current_publication.authority_scope_version)
      and companion.mapping_state='RESOLVED' and companion.pay_effect='NONE' and companion.work_event_id=event_fact.id
      and companion.candidate_id=event_fact.candidate_id and companion.client_id=event_fact.client_id
      and companion.contract_id=choice.contract_id and item->>'work_event_id'=event_fact.id::text
      and current_row.row_finalisation_state='SOURCE_WORKED' and current_row.work_date=event_fact.work_date
      and item#>'{native_payment_facts,paid}'='true'::jsonb
      and item#>>'{native_payment_facts,paid_evidence_state}'='SETTLED_ORIGINAL'
      and item->>'snapshot_line_hash'=encode(prior_snapshot.snapshot_line_hash,'hex')
      and item->>'movement_economic_hash'=encode(prior_movement.movement_economic_hash,'hex')
      and exists(select 1 from public.audit_events a where a.object_type='weekly_source_office_row_choices'
        and a.object_id_text=choice.id::text and a.action='WEEKLY_SOURCE_PAID_SHIFT_MATCH_CONFIRMED'
        and a.actor_user_id=choice.actor_user_id and a.after_json->'choice'=to_jsonb(choice)
        and a.after_json->'admission'=choice.paid_overlap_inputs_json);
  end if;
  if count_rows>1 then raise exception 'WEEKLY_SOURCE_SELF_BILL_SHIFT_AMBIGUOUS' using errcode='55000'; end if;`}
]);
compose(money,'private.weekly_source_billing_movement_hash_v1(jsonb)','10_complete_billing_hash.sqlinc',[
 {name:'literal two-field snapshot origin; old producer key sets unchanged',before:"'vat_rounding_revision_id','invoice_window_cycle_id',",after:"'vat_rounding_revision_id','invoice_window_cycle_id','invoice_only_snapshot_line_id','pay_effect',"}
]);
compose(money,'private.weekly_source_invoice_movement_window_id_v1(uuid)','11_complete_movement_window.sqlinc',[
 {name:'actual snapshot companion immutable window witness',before:"      then nullif(m.source_facts_json->>'invoice_window_cycle_id','')::uuid else m.finalisation_cycle_id end;",after:"      then nullif(m.source_facts_json->>'invoice_window_cycle_id','')::uuid\n    when m.pay_effect='NONE' and m.invoice_only_snapshot_line_id is not null\n      then nullif(m.source_facts_json->>'invoice_window_cycle_id','')::uuid else m.finalisation_cycle_id end;"}
]);
compose('supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql','private.weekly_source_invoice_source_amount_movement_valid_v1(uuid)','12_complete_source_amount_valid.sqlinc',[
 {name:'genuine snapshot origin before unchanged historical source amount checks',before:"    if not found or m.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS' or m.transition_id is null",after:"    if not found or m.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS' or (m.transition_id is null and m.invoice_only_snapshot_line_id is null)"},
 {name:'keep ordinary transition proof and assert only new actual snapshot origin',before:"    select * into t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=m.transition_id;\n    if not found or t.final_revision_id<>m.final_revision_id or t.work_event_id<>m.work_event_id then return false; end if;",after:"    if m.invoice_only_snapshot_line_id is not null then\n      perform private.weekly_source_paid_match_invoice_origin_assert_v1(m.id);\n    else\n      select * into t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=m.transition_id;\n      if not found or t.final_revision_id<>m.final_revision_id or t.work_event_id<>m.work_event_id then return false; end if;\n    end if;"},
 {name:'retain all old outcome/role combinations plus bounded NONE snapshot',before:"      or (m.source_line_kind='SOURCE_REBILL_LINE' and t.outcome='NO_CHANGE' and m.movement_role='POSITIVE' and m.total_pay_ex_vat=0)) then return false; end if;",after:"      or (m.source_line_kind='SOURCE_REBILL_LINE' and t.outcome='NO_CHANGE' and m.movement_role='POSITIVE' and m.total_pay_ex_vat=0)\n      or (m.source_line_kind='SOURCE_REBILL_LINE' and m.pay_effect='NONE' and m.invoice_only_snapshot_line_id is not null\n        and m.movement_role='POSITIVE' and m.total_pay_ex_vat=0)) then return false; end if;"},
 {name:'actual raw companion snapshot supplies unchanged full money validation',before:'    select * into sn from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.new_snapshot_line_id;',after:"    select * into sn from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=\n      case when m.pay_effect='NONE' then m.invoice_only_snapshot_line_id else t.new_snapshot_line_id end;"}
].map(h=>({...h,before:h.before.replace(/^  /gm,''),after:h.after.replace(/^  /gm,'')})));
compose('supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/15_complete_native_report_admission.sqlinc','public.weekly_source_invoice_admit_atomic_v1(jsonb)','13_complete_invoice_admission.sqlinc',[
 {name:'actual companion presentation tuple before ordinary expense/transition branches',before:'    elsif v_is_expense then\n      select source_row.* into strict v_source_row',after:`    elsif v_movement.invoice_only_snapshot_line_id is not null then
      perform private.weekly_source_paid_match_invoice_origin_assert_v1(v_movement.id);
      if v_count<>1 or v_is_net then raise exception 'WEEKLY_SOURCE_PAID_MATCH_INVOICE_GROUP_INVALID' using errcode='55000';end if;
      select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
        where snapshot.id=v_movement.invoice_only_snapshot_line_id and snapshot.final_revision_id=v_revision.id;
      v_work_date:=v_snapshot.work_date;v_start:=v_snapshot.start_at_local;v_end:=v_snapshot.end_at_local;
      v_break:=v_snapshot.break_minutes;v_reference:=v_snapshot.external_event_identity;
      -- Genuine Source hours are presentation evidence. Native pay rates and
      -- total remain zero; no payable position is minted from these hours.
      v_hours_day:=(v_snapshot.pay_vector_json#>>'{hours,day}')::numeric;
      v_hours_night:=(v_snapshot.pay_vector_json#>>'{hours,night}')::numeric;
      v_hours_sat:=(v_snapshot.pay_vector_json#>>'{hours,sat}')::numeric;
      v_hours_sun:=(v_snapshot.pay_vector_json#>>'{hours,sun}')::numeric;
      v_hours_bh:=(v_snapshot.pay_vector_json#>>'{hours,bh}')::numeric;
    elsif v_is_expense then
      select source_row.* into strict v_source_row`},
 {name:'typed literal origin does not claim a transition',before:"    v_origin_kind:=case\n      when v_is_rounding then 'INVOICE_VAT_ROUNDING'",after:"    v_origin_kind:=case\n      when v_movement.invoice_only_snapshot_line_id is not null then 'SOURCE_INVOICE_ONLY_SNAPSHOT'\n      when v_is_rounding then 'INVOICE_VAT_ROUNDING'"}
]);
compose('supabase/repeatable/13102026_1705_weekly_source_invoice_adjustments/02_invoice_allocation_assert.sqlinc','private.weekly_source_invoice_allocation_assert_v1(uuid)','19_complete_invoice_allocation.sqlinc',[
 {name:'existing full allocation guard plus current-bound snapshot-origin verification',before:"  return pg_catalog.jsonb_build_object(\n    'ok',true,'is_source_invoice',true,'invoice_id',p_invoice_id,",after:`  perform private.weekly_source_paid_match_invoice_origin_assert_v1(movement.id)
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
    where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      and (movement.pay_effect='NONE' or movement.invoice_only_snapshot_line_id is not null);
  return pg_catalog.jsonb_build_object(
    'ok',true,'is_source_invoice',true,'invoice_id',p_invoice_id,`}
]);
const schemaFile='supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',schema=execFileSync('git',['show',commit+':'+schemaFile],{cwd:root,maxBuffer:2e6}).toString();
const start=schema.indexOf('alter table public.weekly_source_billing_movements\n  add constraint weekly_source_billing_movements_five_origins');
const end=schema.indexOf('\n\n-- PostgreSQL auto-names',start);assert.ok(start>=0&&end>start);
const schemaBefore=schema.slice(start,end),schemaHunks=[
 {name:'six real mutually exclusive origins',before:'+(vat_rounding_revision_id is not null)::integer=1)',after:'+(vat_rounding_revision_id is not null)::integer+(invoice_only_snapshot_line_id is not null)::integer=1)'},
 {name:'rebill snapshot is purpose-bound NONE; original transition arm retained',before:"and (source_line_kind<>'SOURCE_REBILL_LINE' or (transition_id is not null and movement_role='POSITIVE'))",after:"and (source_line_kind<>'SOURCE_REBILL_LINE' or (movement_role='POSITIVE' and (transition_id is not null\n      or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'))))\n    and (invoice_only_snapshot_line_id is null or (source_line_kind='SOURCE_REBILL_LINE' and pay_effect='NONE'\n      and source_profile_kind in ('HEALTHROSTER_ACTUAL_ROWS','GENERIC_COMPLETE_SNAPSHOT')))"}
];let schemaAfter=schemaBefore;for(const h of schemaHunks){assert.equal(schemaAfter.split(h.before).length,2,h.name);schemaAfter=schemaAfter.replace(h.before,h.after);}let reversed=schemaAfter;for(const h of [...schemaHunks].reverse())reversed=reversed.replace(h.after,h.before);assert.equal(reversed,schemaBefore);
fs.writeFileSync(path.join(dir,'before/01_invoice_snapshot_origins.sqlinc'),schemaBefore);
const prefix='-- One-time Source proposal; exact original two coupled CHECK owners retained.\nalter table public.weekly_source_billing_movements\n  drop constraint weekly_source_billing_movements_five_origins,\n  drop constraint weekly_source_billing_movements_invoice_contract,\n  drop constraint weekly_source_billing_movements_invoice_only_zero_pay;\n';
const suffix="\ncreate unique index weekly_source_invoice_only_snapshot_movement_uq on public.weekly_source_billing_movements(invoice_only_snapshot_line_id) where invoice_only_snapshot_line_id is not null;\nalter table public.weekly_source_invoice_presentation_lines drop constraint weekly_source_invoice_presentation_lines_origin_kind_check,\n  add constraint weekly_source_invoice_presentation_lines_origin_kind_check check (origin_kind in ('GENERIC_TRANSITION','HEALTHROSTER_TRANSITION','NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE','SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SELF_BILL_REPORT_STANDALONE_CREDIT','SOURCE_INVOICE_ONLY_SNAPSHOT'));\n";
fs.writeFileSync(path.join(dir,'one-time/01_invoice_snapshot_origins.sqlinc'),prefix+schemaAfter+suffix);
records.push({commit,file:schemaFile,input_sha256:sha(schema),output:'one-time/01_invoice_snapshot_origins.sqlinc',before_sha256:sha(schemaBefore),hunks:schemaHunks,whole_reverse_verified:true});
fs.writeFileSync(path.join(dir,'INVOICE_CONSUMER_REVERSES.json'),JSON.stringify({source_only:true,records,existing_report_path_unchanged:true,native_amount_and_hold_owners_unchanged:true,old_native_hash_inputs_preserved:true,new_origin_classification_and_verifiers_required:true,canonical_mounted:false,native_proof:false,pipeline_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({output:r.output,hunks:r.hunks.length}))));
