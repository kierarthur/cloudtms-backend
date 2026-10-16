import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {factorSourceWitness} from './factor-source-witness.mjs';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const read=p=>fs.readFileSync(path.join(here,p),'utf8').replaceAll('\r\n','\n');
const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
const replace=(s,a,b)=>{a=a.replaceAll('\r\n','\n');b=b.replaceAll('\r\n','\n');assert.equal(s.split(a).length,2,a.slice(0,160));return s.replace(a,b);};
const native=n=>read('reference/'+n.replace('.','_')+'.sqlinc');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
let facts=read('reference/facts_predecessor.sqlinc');
// Keep this predecessor intact. The current kernel seals invoice facts alone.
facts=replace(facts,"v_authority:=case when v_source then 'SOURCE_FINANCIAL'", "v_authority:=case when v_source and v_ordinary then 'MIXED' when v_source then 'SOURCE_FINANCIAL'");
facts=replace(facts,"v_policy:=case when v_source and not v_ordinary", "v_policy:=case when v_source and v_ordinary then 'MIXED' when v_source and not v_ordinary");
facts=replace(facts," if v_source and v_ordinary then v_issues:=array_append(v_issues,'MIXED_INVOICE_AUTHORITY_REQUIRES_SCOPED_REVIEW');end if;", " -- Mixed authority is represented by exact original members, never a blanket Source classification.");
facts=replace(facts,"select coalesce(jsonb_agg(to_jsonb(native_row) order by id),'[]'::jsonb) into v_part from public.weekly_source_billing_movements", "select coalesce(jsonb_agg((to_jsonb(native_row)-'canonical_pay_vector_json'-'total_pay_ex_vat') order by id),'[]'::jsonb) into v_part from public.weekly_source_billing_movements");
facts=replace(facts,"select coalesce(jsonb_agg(to_jsonb(native_row) order by id),'[]'::jsonb) into v_part from public.weekly_work_events", "select coalesce(jsonb_agg(jsonb_build_object('id',native_row.id,'identity',to_jsonb(native_row)-array['current_pay_generation','pay_state','updated_at_utc','current_protected_run_id','current_candidate_timesheet_id']) order by id),'[]'::jsonb) into v_part from public.weekly_work_events");
facts=replace(facts,"when cardinality(p_source_row_ids)>0 then 'NO_FINAL_REPORT_YET'","when cardinality(v_rows)>0 then 'NO_FINAL_REPORT_YET'");
// No physical/current-placement or worker protection rows enter this witness.
write('forward/010_source_invoice_facts.sqlinc',facts);
let family=native('private.weekly_source_invoice_family_timesheet_ids_v1');
family=replace(family,'  with canonical_split_siblings as (',`  with certified_member as (
    select a.family_root_id,a.member_key from private.timesheet_contract_family_aliases a
    join private.timesheet_contract_move_operations o on o.id=a.accepted_operation_id
    join private.timesheet_contract_current_placements p on p.family_root_id=a.family_root_id and p.member_key=a.member_key
    join private.timesheet_contract_move_operations current_owner on current_owner.id=p.operation_id and (current_owner.state='COMPLETE' or private.timesheet_contract_move_accepting_v1(current_owner.id))
    where a.timesheet_id=p_timesheet_id and (o.state in('COMPLETE','SUPERSEDED') or private.timesheet_contract_move_accepting_v1(o.id))
      and current_owner.placement_receipt_json->>'receipt_id'=p.placement_receipt_id::text
  ), certified_aliases as (
    select a.timesheet_id from private.timesheet_contract_family_aliases a
    join certified_member m using(family_root_id,member_key)
    join private.timesheet_contract_move_operations o on o.id=a.accepted_operation_id and (o.state in('COMPLETE','SUPERSEDED') or private.timesheet_contract_move_accepting_v1(o.id))
  ), canonical_split_siblings as (`);
family=replace(family,'    select p_timesheet_id as timesheet_id\r\n    union', '    select p_timesheet_id as timesheet_id\r\n    union select timesheet_id from certified_aliases\r\n    union');
write('forward/020_source_family.sqlinc',family);
let lineage=native('private.weekly_source_finalisation_lineage_assert_v1');
lineage=replace(lineage,'  v_root_identity jsonb;','  v_root_identity jsonb;\n  v_inherited_original boolean;');
lineage=replace(lineage,`  v_root_identity:=private.weekly_source_root_integrity_assert_v1(
    v_lineage.timesheet_id,v_lineage.family_booking_id,v_lineage.timesheet_version
  );`,`  v_inherited_original:=private.timesheet_contract_source_original_eligible_v1(v_lineage.timesheet_id);
  if not v_inherited_original then
    v_root_identity:=private.weekly_source_root_integrity_assert_v1(
      v_lineage.timesheet_id,v_lineage.family_booking_id,v_lineage.timesheet_version);
  end if;`);
lineage=replace(lineage,'     or not v_timesheet.is_current','     or (not v_timesheet.is_current and not v_inherited_original)');
lineage=replace(lineage,`     or v_lineage.family_booking_id is distinct from
        (v_root_identity->>'family_booking_id')
     or v_lineage.timesheet_version is distinct from
        (v_root_identity->>'canonical_version')::integer
     or coalesce((v_root_identity->>'requested_is_canonical')::boolean,false)
        is not true then`,`     or (not v_inherited_original and (
       v_lineage.family_booking_id is distinct from (v_root_identity->>'family_booking_id')
       or v_lineage.timesheet_version is distinct from (v_root_identity->>'canonical_version')::integer
       or coalesce((v_root_identity->>'requested_is_canonical')::boolean,false) is not true)) then`);
write('forward/021_source_lineage.sqlinc',lineage);
let presentation=native('private._invoice_presentation_snapshot_batch');
const currentJoins=[...presentation.matchAll(/\bt\.is_current\b/g)].length;
assert.equal(currentJoins,5);
presentation=presentation.replaceAll('t.is_current','(t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id))');
const scopedPredecessor=read('reference/scoped_snapshot_predecessor.sqlinc');
const scopedRows=scopedPredecessor.slice(scopedPredecessor.indexOf('  scoped_credit_line_rows as materialized ('),scopedPredecessor.indexOf('  timesheet_targets as materialized ('));
assert.ok(scopedRows.length>500);
presentation=replace(presentation,'  timesheet_targets as materialized (',scopedRows+'  timesheet_targets as materialized (');
presentation=replace(presentation,"'lines',coalesce(l.lines,'[]'::jsonb)","'lines',coalesce(scoped_lines.lines,l.lines,'[]'::jsonb)");
presentation=replace(presentation,'    left join invoice_line_rows l on l.invoice_id=i.id','    left join invoice_line_rows l on l.invoice_id=i.id\n    left join scoped_credit_line_rows scoped_lines on scoped_lines.invoice_id=i.id');
presentation=replace(presentation,`  invoice_timesheets_agg as materialized (`, `  invoice_timesheets_agg as materialized (`);
presentation=replace(presentation,'    from invoice_timesheet_rows group by invoice_id',`    from invoice_timesheet_rows original_source
    where not exists(select 1 from public.invoices cn where cn.id=original_source.invoice_id and cn.type='CREDIT_NOTE' and cn.header_snapshot_json?'credit_scope')
    group by invoice_id
    union all
    select cn.id,case when cn.header_snapshot_json#>>'{credit_scope,kind}'='CONTRACT_MOVE'
      then cn.header_snapshot_json#>'{credit_scope,frozen_timesheet_sources}'
      else jsonb_build_array(cn.header_snapshot_json#>'{credit_scope,frozen_timesheet_source}') end
    from requested_invoices requested join public.invoices cn on cn.id=requested.invoice_id
    where cn.type='CREDIT_NOTE' and cn.header_snapshot_json?'credit_scope'`);
write('forward/022_source_presentation.sqlinc',presentation);
let duty=native('private.weekly_source_invoice_duty_v1');
duty=replace(duty,'or not v_root.is_current or v_root.is_adjustment', 'or (not v_root.is_current and not private.timesheet_contract_source_original_eligible_v1(v_root.timesheet_id)) or v_root.is_adjustment');
duty=replace(duty,"or v_root.archived_at_utc is not null or v_root.sheet_scope<>","or (v_root.archived_at_utc is not null and not private.timesheet_contract_source_original_eligible_v1(v_root.timesheet_id)) or v_root.sheet_scope<>");
write('forward/021a_source_duty.sqlinc',duty);
let archived=native('public.invoice_line_archived_timesheet_guard_v1');
archived=replace(archived,`  IF v_current_timesheet_id IS NOT NULL
     AND v_archived_at_utc IS NOT NULL THEN`,`  IF v_current_timesheet_id IS NOT NULL
     AND v_archived_at_utc IS NOT NULL
     AND NOT coalesce(private.invoice_contract_move_line_allowed_v1(to_jsonb(NEW)),false) THEN`);
write('forward/023_source_archive.sqlinc',archived);
let lineOwner=native('private.weekly_source_invoice_line_owner_guard_v1');
lineOwner=replace(lineOwner,'begin\n','begin\n  if tg_op<>\'DELETE\' then perform private.invoice_contract_move_line_reservation_assert_v1(to_jsonb(new));end if;\n');
write('forward/031_source_line_owner.sqlinc',lineOwner);
for(const name of ['private._candidate_expense_current_timesheet_id_v1','private._candidate_expense_owned_timesheet_id_v1']){
 let source=native(name);
 source=replace(source,'begin\r\n',`begin
  v_result:=private.timesheet_contract_claim_placement_v1(p_workflow_id,p_stored_timesheet_id,null);
  if v_result is not null then return v_result;end if;
`);
 write('forward/'+(name.includes('_owned_')?'027':'026')+'_claim_'+(name.includes('_owned_')?'owned':'current')+'.sqlinc',source);
}
let claimLock=native('private._candidate_expense_owner_context_lock_v1');
claimLock=replace(claimLock,'begin\r\n',`begin
  -- Gate/family before component row; do not invert Move's native lock order.
  select component.* into v_component from public.candidate_expense_components component
    where component.expense_component_id=p_expense_component_id;
  if not found then raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002';end if;
  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_component.owning_timesheet_id) then
    perform private.timesheet_contract_move_family_lock_v1(v_component.owning_timesheet_id);
  end if;
`);
claimLock=replace(claimLock,'v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(\r\n    v_component.workflow_id,v_component.owning_timesheet_id\r\n  );',`v_timesheet_id:=coalesce(private.timesheet_contract_claim_placement_v1(
    v_component.workflow_id,v_component.owning_timesheet_id,p_expense_component_id),
    private._candidate_expense_owned_timesheet_id_v1(v_component.workflow_id,v_component.owning_timesheet_id));`);
write('forward/028_claim_lock.sqlinc',claimLock);
let claimFinancial=native('private._candidate_expense_components_sync_financial_v1');
claimFinancial=replace(claimFinancial,'  v_agency text;','  v_agency text;\n  v_base_agency text;');
claimFinancial=replace(claimFinancial,'  for v_component in','  v_base_agency:=v_agency;\n  for v_component in');
claimFinancial=replace(claimFinancial,`and private._candidate_expense_owned_timesheet_id_v1(
        component.workflow_id,component.owning_timesheet_id
      )=p_timesheet_id`,`and coalesce(private.timesheet_contract_claim_placement_v1(
        component.workflow_id,component.owning_timesheet_id,component.expense_component_id),
        private._candidate_expense_owned_timesheet_id_v1(component.workflow_id,component.owning_timesheet_id))=p_timesheet_id`);
claimFinancial=replace(claimFinancial,'  loop\r\n    if v_component.agency_authorisation_state',`  loop
    -- A correction changes current placement, never historical realised pay.
    v_agency:=case when v_component.agency_authorisation_state='PAID' then 'PAID' else v_base_agency end;
    if v_component.agency_authorisation_state`);
write('forward/029_claim_financial.sqlinc',claimFinancial);
let claimSync=native('private._candidate_expense_components_sync_v1');
claimSync=replace(claimSync,'  v_count integer:=0;','  v_count integer:=0;\n  v_current_owner uuid;\n  v_base_agency text;');
claimSync=replace(claimSync,'  for v_value in','  v_base_agency:=v_agency_state;\n  for v_value in');
claimSync=replace(claimSync,'  if v_workflow.target_timesheet_id is not null then',`  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_workflow.target_timesheet_id) then
    perform private.timesheet_contract_move_family_lock_v1(v_workflow.target_timesheet_id);
  end if;
  v_current_owner:=coalesce(private._candidate_expense_current_timesheet_id_v1(v_workflow.id,v_workflow.target_timesheet_id),v_workflow.target_timesheet_id);
  if v_current_owner is not null then`);
// References used for mutable financial/carrier ownership only. The workflow
// row and its immutable submitted/signed target are never updated.
claimSync=claimSync.replaceAll('financial.timesheet_id=v_workflow.target_timesheet_id','financial.timesheet_id=v_current_owner').replaceAll('timesheet.timesheet_id=v_workflow.target_timesheet_id','timesheet.timesheet_id=v_current_owner');
claimSync=replace(claimSync,'  for v_value in',`  v_current_owner:=coalesce(private._candidate_expense_current_timesheet_id_v1(v_workflow.id,v_workflow.target_timesheet_id),v_workflow.target_timesheet_id);
  for v_value in`);
claimSync=replace(claimSync,`    if not found then
      v_before:='{}'::jsonb;`,`    if found then
      v_current_owner:=coalesce(private.timesheet_contract_claim_placement_v1(v_workflow.id,
        v_component.owning_timesheet_id,v_component.expense_component_id),v_component.owning_timesheet_id);
      select * into v_fin from public.timesheets_financials financial
       where financial.timesheet_id=v_current_owner and financial.is_current
       order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc limit 1;
      select * into strict v_timesheet from public.timesheets timesheet
       where timesheet.timesheet_id=v_current_owner and timesheet.is_current and timesheet.archived_at_utc is null;
      v_agency_state:=case
       when v_component.agency_authorisation_state='PAID' then 'PAID'
       when v_fin.paid_at_utc is not null or upper(coalesce(v_timesheet.status::text,''))='PAID' then 'PAID'
       when v_fin.locked_by_invoice_id is not null or upper(coalesce(v_timesheet.status::text,''))='INVOICED' then 'INVOICED'
       when v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null
        or upper(coalesce(v_timesheet.status::text,'')) in('AUTHORISED','AUTHORIZED') then 'AUTHORISED'
       else 'NOT_AUTHORISED' end;
    end if;
    if v_component.expense_component_id is null then
      v_before:='{}'::jsonb;`);
claimSync=claimSync.replaceAll('v_document_generation,v_value.expense_category,v_workflow.target_timesheet_id','v_document_generation,v_value.expense_category,v_current_owner').replaceAll('owning_timesheet_id is distinct from v_workflow.target_timesheet_id','owning_timesheet_id is distinct from v_current_owner').replaceAll('owning_timesheet_id=v_workflow.target_timesheet_id','owning_timesheet_id=v_current_owner');
write('forward/030_claim_sync.sqlinc',claimSync);
let selectedEdit=native('public.invoice_apply_edits');
selectedEdit=replace(selectedEdit,'and tfu_lock.locked_by_invoice_id = p_invoice_id;',`and tfu_lock.locked_by_invoice_id = p_invoice_id
        and private.invoice_contract_move_selected_financial_authorise_v1(to_jsonb(tfu_lock),
          to_jsonb(tfu_lock)||jsonb_build_object('locked_by_invoice_id',null,'locked_at_utc',null,'updated_at',v_now),p_invoice_id);`);
write('forward/036_native_selected_edit.sqlinc',selectedEdit);
let unlock=native('public._inv_unlock_segments_for_invoice');
unlock=replace(unlock,'order by tf.created_at desc\n    limit 1;', 'order by tf.created_at desc\n    limit 1 for update;');
unlock=replace(unlock,'        locked_at_utc = v_new_locked_at\n      where tfu.id = r_tf.id;',`        locked_at_utc = v_new_locked_at
      where tfu.id = r_tf.id and private.invoice_contract_move_selected_financial_authorise_v1(to_jsonb(tfu),
        to_jsonb(tfu)||jsonb_build_object('updated_at',v_now,'invoice_breakdown_json',v_ib,
         'locked_by_invoice_id',v_new_locked,'locked_at_utc',v_new_locked_at),p_invoice_id);`);
unlock=replace(unlock,'        where tfu.id = r_tf.id;',`        where tfu.id = r_tf.id and private.invoice_contract_move_selected_financial_authorise_v1(to_jsonb(tfu),
          to_jsonb(tfu)||jsonb_build_object('updated_at',v_now,'locked_by_invoice_id',null,'locked_at_utc',null),p_invoice_id);`);
write('forward/036a_native_selected_unlock.sqlinc',unlock);
let classification=native('private._invoice_batch_generate_classification_v2');
assert.equal((classification.match(/and timesheet\.is_current/g)||[]).length,3);
classification=classification.replaceAll('and timesheet.is_current','and (timesheet.is_current or private.timesheet_contract_source_original_eligible_v1(timesheet.timesheet_id))');
classification=replace(classification,'where financial.is_current\n      and financial.client_id is not null',`where financial.is_current
      and not private.timesheet_contract_source_pay_only_v1(financial.timesheet_id)
      and financial.client_id is not null`);
write('forward/033a_native_collection.sqlinc',classification);
let grouping=native('private._invoice_generation_resolve_command_groups');
grouping=replace(grouping,'ts.is_current timesheet_is_current','(ts.is_current or private.timesheet_contract_source_original_eligible_v1(ts.timesheet_id)) timesheet_is_current');
grouping=replace(grouping,"when b.timesheet_id is null then 'SOURCE_NOT_RESOLVED'","when b.timesheet_id is null then 'SOURCE_NOT_RESOLVED'\n      when private.timesheet_contract_source_pay_only_v1(b.timesheet_id) then 'SOURCE_ORIGINAL_INVOICE_SUBJECT_REQUIRED'");
grouping=replace(grouping,'tf.processing_status::text processing_status,tf.locked_by_invoice_id,',"tf.processing_status::text processing_status,case when prospective.fact is null then tf.locked_by_invoice_id end locked_by_invoice_id,");
grouping=replace(grouping,'coalesce(ts.contract_id,cw.contract_id) contract_id,',"coalesce((prospective.fact->>'contract_id')::uuid,ts.contract_id,cw.contract_id) contract_id,");
grouping=replace(grouping,'coalesce(parent_ts.contract_id,parent_cw.contract_id) parent_contract_id,',"case when prospective.fact is null then coalesce(parent_ts.contract_id,parent_cw.contract_id) end parent_contract_id,");
grouping=replace(grouping,'  left join lateral (\n    select private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)\n      authority_json',`  left join lateral (
    select private.invoice_contract_move_prospective_source_v1(
      (r.command_json->>'invoice_contract_correction_id')::uuid,r.timesheet_id,p_actor_user_id) fact
  ) prospective on true
  left join lateral (
    select coalesce(prospective.fact->'settings_authority',private._timesheet_settings_authority_frozen_v1(ts.timesheet_id))
      authority_json`);
grouping=replace(grouping,"when seg.segment_json is not null\n        and nullif(seg.segment_json->>'invoice_locked_invoice_id','') is not null", "when seg.segment_json is not null\n        and not (b.command_json?'invoice_contract_correction_id')\n        and nullif(seg.segment_json->>'invoice_locked_invoice_id','') is not null");
grouping=replace(grouping,"'source_member_key',b.source_member_key,","'source_member_key',b.source_member_key,'invoice_contract_correction_id',b.command_json->'invoice_contract_correction_id',");
write('forward/033_native_grouping.sqlinc',grouping);
let issueValidate=native('private._invoice_issue_validate_batch');
issueValidate=replace(issueValidate,'request_counts as materialized (',`cm_credits as materialized (
  select s.invoice_id from invoice_scope s
  where private.invoice_contract_move_credit_invoice_valid_v1(s.invoice_id,false)
),
request_counts as materialized (`);
issueValidate=replace(issueValidate,'  cross join lateral private._invoice_reference_rows_batch(i.ids) r',`  cross join lateral private._invoice_reference_rows_batch(i.ids) r
  where not exists(select 1 from cm_credits cm where cm.invoice_id=r.invoice_id)`);
issueValidate=replace(issueValidate,'  where r.invoice_id is not null\n),\ncorrections as materialized (',`  where r.invoice_id is not null
    and not exists(select 1 from cm_credits cm where cm.invoice_id=r.invoice_id)
),
corrections as materialized (`);
issueValidate=replace(issueValidate,'  where l.timesheet_id is not null\n  group by l.invoice_id,l.timesheet_id',`  where l.timesheet_id is not null
    and not exists(select 1 from cm_credits cm where cm.invoice_id=l.invoice_id)
  group by l.invoice_id,l.timesheet_id`);
issueValidate=replace(issueValidate,'on t.timesheet_id=s.timesheet_id and t.is_current','on t.timesheet_id=s.timesheet_id and (t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id))');
write('forward/079_native_issue_admission.sqlinc',issueValidate);
let sourceReference=native('private._invoice_source_reference_validate_batch');
sourceReference=replace(sourceReference,'t.updated_at,t.is_current,t.revoked_at,', 't.updated_at,(t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id)) is_current,t.revoked_at,');
sourceReference=replace(sourceReference,'select private._timesheet_settings_authority_frozen_v1(t.timesheet_id)\n      authority_json',`select coalesce(private.invoice_contract_move_prospective_source_v1(
      (s.member->>'invoice_contract_correction_id')::uuid,t.timesheet_id,
      (select o.created_by_user_id from private.timesheet_contract_move_operations o where o.id=(s.member->>'invoice_contract_correction_id')::uuid))->'settings_authority',
      private._timesheet_settings_authority_frozen_v1(t.timesheet_id)) authority_json`);
write('forward/033b_native_references.sqlinc',sourceReference);
let sourceVat=native('private._invoice_generation_vat_policy_batch');
sourceVat=replace(sourceVat,'on t.timesheet_id=s.timesheet_id and t.is_current', 'on t.timesheet_id=s.timesheet_id and (t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id))');
write('forward/033c_native_vat_policy.sqlinc',sourceVat);
let invoiceReferences=native('private._invoice_reference_rows_batch');
invoiceReferences=replace(invoiceReferences,'t.timesheet_id=it.timesheet_id and t.is_current', 't.timesheet_id=it.timesheet_id and (t.is_current or private.timesheet_contract_source_original_eligible_v1(t.timesheet_id))');
write('forward/033d_native_invoice_references.sqlinc',invoiceReferences);
let issue=native('private._invoice_issue_advance_core_v8');
issue=replace(issue,"phase=case when s.exact_document_status='READY' then 'FINALISE'", "phase=case when s.exact_document_status='READY' and private.invoice_contract_move_credit_ready_v1(s.invoice_id,true) then 'FINALISE'");
issue=replace(issue,"status=case when s.exact_document_status='READY' then 'QUEUED'", "status=case when s.exact_document_status='READY' and private.invoice_contract_move_credit_ready_v1(s.invoice_id,true) then 'QUEUED'");
issue=replace(issue,"phase=case when v.status='READY' then 'FINALISE'", "phase=case when v.status='READY' and private.invoice_contract_move_credit_ready_v1(c.entity_id,true) then 'FINALISE'");
issue=replace(issue,"status=case when v.status='READY' then 'QUEUED'", "status=case when v.status='READY' and private.invoice_contract_move_credit_ready_v1(c.entity_id,true) then 'QUEUED'");
issue=replace(issue,"      and c.payload_json->>'snapshot_hash'=v.snapshot_hash", "      and c.payload_json->>'snapshot_hash'=v.snapshot_hash\n      and private.invoice_contract_move_credit_ready_v1(i.id,true)");
issue=replace(issue,'    from eligible e where i.id=e.entity_id returning i.id', '    from eligible e where i.id=e.entity_id\n      and private.invoice_contract_move_credit_ready_v1(i.id,true) returning i.id');
write('forward/080_issue_native.sqlinc',issue);
let start=native('private._invoice_operation_start_core_v8');
start=replace(start,'  with raw as materialized (',`  if exists(select 1 from jsonb_array_elements(p_commands) command where command?'invoice_contract_correction_id' or exists(select 1 from jsonb_object_keys(command) key where key like 'source_original_%')) then raise exception 'CONTRACT_MOVE_PROSPECTIVE_INPUT_OWNER_ONLY' using errcode='42501';end if;
  p_commands:=private.invoice_contract_move_issue_commands_v1(p_commands,p_actor_user_id);

  with raw as materialized (`);
write('forward/081_issue_registration.sqlinc',start);
let issueOne=native('public.invoice_issue_one');
issueOne=replace(issueOne,'  v_has_pdf_fresh boolean := false;','  v_has_pdf_fresh boolean := false;\n  v_move_context jsonb;\n  v_move_start jsonb;');
issueOne=replace(issueOne,'begin\r\n  -- Load invoice_debug',`begin
  v_move_context:=private.invoice_contract_move_issue_context_v1(p_invoice_id,p_actor_user_id,true);
  if v_move_context is not null then
    v_move_start:=private._invoice_operation_start_core_v8(jsonb_build_array(jsonb_build_object(
      'command_type','ISSUE_INVOICES','invoice_ids',v_move_context->'invoice_ids','deliver',false)),p_actor_user_id,v_now);
    status:=(select i.status::text from public.invoices i where i.id=p_invoice_id);
    issued_at_utc:=(select i.issued_at_utc from public.invoices i where i.id=p_invoice_id);
    on_hold_reason:=(select i.on_hold_reason from public.invoices i where i.id=p_invoice_id);
    reasons:=array['CONTRACT_CORRECTION_ISSUE_REGISTERED'];return next;return;
  end if;
  -- Load invoice_debug`);
write('forward/082_issue_one.sqlinc',issueOne);
// Explicit DRAFT_ONLY is validated by the scoped owner before shared document
// dispatch. Only that exact purpose skips native automatic credit Issue.
let generation=native('private._invoice_generation_advance_core_v8');
generation=generation.replaceAll('and ts_vat.is_current','and (ts_vat.is_current or private.timesheet_contract_source_original_eligible_v1(ts_vat.timesheet_id))').replaceAll('not ts.is_current or ts.revoked_at','(not ts.is_current and not private.timesheet_contract_source_original_eligible_v1(ts.timesheet_id)) or ts.revoked_at').replaceAll('and ts.is_current','and (ts.is_current or private.timesheet_contract_source_original_eligible_v1(ts.timesheet_id))');
generation=replace(generation,"    where q.command_type='GENERATE_CREDIT_NOTE'\r\n    on conflict(idempotency_key)","    where q.command_type='GENERATE_CREDIT_NOTE'\n      and not private.invoice_contract_move_draft_only_v1(q.chunk_id)\n    on conflict(idempotency_key)");
assert.equal(generation.split("where q.command_type<>'GENERATE_CREDIT_NOTE'").length,3);
generation=generation.replaceAll("where q.command_type<>'GENERATE_CREDIT_NOTE'","where (q.command_type<>'GENERATE_CREDIT_NOTE' or private.invoice_contract_move_draft_only_v1(q.chunk_id))");
generation=replace(generation,"and c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'\n    and i.id=", "and c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'\n    and not private.invoice_contract_move_draft_only_v1(c.id)\n    and i.id=");
generation=factorSourceWitness(generation,replace);
write('forward/070_generation_native.sqlinc',generation);
let generationBatch=native('private._invoice_generation_advance_batch');
generationBatch=replace(generationBatch,'  with claims as materialized (',`  -- Same native GENERATION_GROUP claim/lease; no extra scheduler or chunk type.
  select coalesce(jsonb_agg(x.value),'[]'::jsonb) into v_manifest_claims
    from jsonb_array_elements(p_claims) x(value) join public.invoice_operation_chunks c on c.id=(x.value->>'chunk_id')::uuid
    where c.payload_json->>'preparation_purpose'='CONTRACT_CORRECTION_PREPARE';
  if jsonb_array_length(v_manifest_claims)>0 then
    v_result:=private.invoice_contract_move_prepare_advance_v1(v_manifest_claims,v_now);
    select coalesce(jsonb_agg(x.value),'[]'::jsonb) into p_claims
      from jsonb_array_elements(p_claims) x(value) join public.invoice_operation_chunks c on c.id=(x.value->>'chunk_id')::uuid
      where c.payload_json->>'preparation_purpose' is distinct from 'CONTRACT_CORRECTION_PREPARE';
    if jsonb_array_length(p_claims)=0 then return v_result;end if;
  end if;
  with claims as materialized (`);
write('forward/071_generation_dispatch.sqlinc',generationBatch);
let scoped=native('private.invoice_credit_scoped_advance_v1');
scoped=replace(scoped,'begin\r\n  select c.* into strict v_chunk',`begin
  if exists(select 1 from public.invoice_operation_chunks c where c.id=(p_claim->>'chunk_id')::uuid
    and c.payload_json#>>'{credit_scope,kind}'='CONTRACT_MOVE') then
    return private.invoice_contract_move_credit_advance_v1(p_claim,p_now_utc);
  end if;
  select c.* into strict v_chunk`);
write('forward/060_scoped_native.sqlinc',scoped);
const beforeAcl=fs.readdirSync(path.join(here,'forward')).filter(n=>n.endsWith('.sqlinc')&&n!=='099_new_private_acl.sqlinc').sort();
const currentNames=new Set(JSON.parse(read('CURRENT_NATIVE_INPUTS.json')).owners.map(o=>o.name));
const signatures=new Set();
for(const file of beforeAcl){for(const m of read('forward/'+file).matchAll(/create or replace function\s+(private\.[a-z0-9_]+)\s*\(([\s\S]*?)\)\s*returns/gi)){
 if(currentNames.has(m[1]))continue;
 const types=m[2].split(',').map(s=>s.trim().replace(/\s+default\s+[\s\S]+$/i,'').replace(/^p_[a-z0-9_]+\s+/i,'').trim()).join(',');
 signatures.add(m[1]+'('+types+')');
}}
write('forward/099_new_private_acl.sqlinc',[...signatures].sort().map(s=>`alter function ${s} owner to postgres;\nrevoke all on function ${s} from public,anon,authenticated,service_role;`).join('\n')+'\n');
const files=fs.readdirSync(path.join(here,'forward')).sort();
for(const file of files)write('forward/'+file,read('forward/'+file));
write('SOURCE_MANIFEST.json',JSON.stringify({source_pin:'a934ee1d460b444939234e812babccb90dc3ddff',native:'REQUIRED_NOT_RUN',files:files.map(file=>({file:'forward/'+file,sha256:sha(read('forward/'+file))}))},null,2)+'\n');
console.log(JSON.stringify({files:files.length,native:'REQUIRED_NOT_RUN'}));
