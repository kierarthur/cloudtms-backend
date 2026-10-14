import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {sha256} from './weekly-source-protected-release-qualification.mjs';
const root=process.argv[2],dir=path.dirname(fileURLToPath(import.meta.url));if(!root)throw Error('EXPLICIT_READ_ONLY_ROOT_REQUIRED');
const input='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/05_complete_admission.sqlinc';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:null,maxBuffer:32*1024*1024});
const sourceCommit=git(['rev-parse','HEAD']).toString().trim(),saved=fs.readFileSync(path.join(root,input)),committed=git(['show',sourceCommit+':'+input]);
if(!saved.equals(committed))throw Error('REPORT_NATIVE_ADMISSION_INPUT_NOT_COMMITTED');
const hunks=[];let source=saved.toString('utf8');
function apply(id,before,after){if(source.split(before).length!==2)throw Error('REPORT_NATIVE_ADMISSION_HUNK_NOT_EXACT '+id);
 source=source.replace(before,()=>after);hunks.push({id,old:before,new:after});}
apply('REPORT_NATIVE_LOCALS','  v_count integer;',`  v_count integer;
  v_is_report boolean:=false; v_report_scope public.weekly_source_report_scopes%rowtype;
  v_report_header public.weekly_source_self_bill_reports%rowtype;`);
apply('REPORT_FINAL_AUTHORITY_WITHOUT_EXPORT_COMPLETION',`  if v_revision.state<>'CURRENT'
     or v_revision.source_cycle_id is distinct from v_cycle.id`,
`  v_is_report:=v_revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE';
  if v_is_report then
    select * into strict v_report_scope from public.weekly_source_report_scopes where id=v_revision.report_scope_id for share;
    select * into strict v_report_header from public.weekly_source_self_bill_reports where id=v_report_scope.self_bill_report_id;
    if v_report_scope.current_final_revision_id is distinct from v_revision.id or v_report_scope.source_cycle_id<>v_cycle.id
      or v_report_scope.source_group_id<>v_manifest.source_group_id or v_report_scope.client_id<>v_manifest.client_id
      or v_report_scope.current_complete_upload_id is distinct from v_revision.upload_id or v_report_scope.state<>'FINALISED'
      or v_report_header.source_cycle_id<>v_cycle.id or v_report_header.source_group_id<>v_manifest.source_group_id
      or v_report_header.client_id<>v_manifest.client_id or v_revision.reason<>'INITIAL_FINALISATION'
      or coalesce(v_manifest.invoice_window_cycle_id,v_cycle.id)<>v_cycle.id then
      raise exception 'WEEKLY_SOURCE_REPORT_INVOICE_FINAL_AUTHORITY_STALE' using errcode='40001'; end if;
  end if;
  if v_revision.state<>'CURRENT'
     or v_revision.source_cycle_id is distinct from v_cycle.id`);
apply('RETAIN_ORDINARY_COMPLETION_ARM',`     or not exists(
       select 1 from public.weekly_source_client_cycle_completions completion`,
`     or (not v_is_report and not exists(
       select 1 from public.weekly_source_client_cycle_completions completion`);
apply('CLOSE_ORDINARY_COMPLETION_ARM',`         and completion.completion_kind='FINAL_SOURCE' and completion.state='CURRENT'
     ) then`,
`         and completion.completion_kind='FINAL_SOURCE' and completion.state='CURRENT'
     )) then`);
apply('REPORT_COMPLETE_SOURCE_MEMBERSHIP',`  if v_manifest.invoice_state='ADMITTED' then`,
`  if v_is_report then
    if exists(select 1 from public.weekly_source_manifest_movements mm
      join public.weekly_source_billing_movements movement on movement.id=mm.billing_movement_id
      left join public.weekly_source_upload_rows row_value on row_value.id=movement.self_bill_report_row_id
      where mm.client_manifest_id=v_manifest.id and
       (movement.source_profile_kind<>'SELF_BILL_REPORT' or movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING')
        or movement.total_pay_ex_vat<>0 or movement.canonical_pay_vector_json is distinct from private.weekly_source_invoice_only_vector_v1('PAY',0)
        or (movement.source_line_kind='SELF_BILL_REPORT_LINE' and (row_value.id is null or row_value.upload_id<>v_revision.upload_id
          or row_value.row_finalisation_state<>'SOURCE_REPORT_LINE'
          or movement.source_facts_json->>'upload_row_id' is distinct from row_value.id::text
          or movement.source_facts_json->>'source_row_ordinal' is distinct from row_value.source_row_ordinal::text
          or movement.source_facts_json->>'request_id' is distinct from row_value.external_source_key
          or movement.source_facts_json->>'report_scope_id' is distinct from v_report_scope.id::text
          or movement.source_facts_json->>'self_bill_report_id' is distinct from v_report_header.id::text
          or movement.source_facts_json->>'rule_set_version_id' is distinct from v_revision.rule_set_version_id::text
          or movement.invoice_presentation_charge_pence<>row_value.source_shift_charge_pence
          or movement.vat_amount*100<>row_value.source_vat_pence or movement.total_inc_vat*100<>row_value.source_gross_pence))
        or (movement.source_line_kind='INVOICE_VAT_ROUNDING' and movement.vat_rounding_revision_id is distinct from v_revision.id)))
      or exists(select 1 from public.weekly_source_upload_rows row_value where row_value.upload_id=v_revision.upload_id and
        (select count(*) from public.weekly_source_manifest_movements mm
          join public.weekly_source_billing_movements movement on movement.id=mm.billing_movement_id
          where mm.client_manifest_id=v_manifest.id and movement.self_bill_report_row_id=row_value.id)<>1)
      or (select count(*) from public.weekly_source_upload_rows row_value where row_value.upload_id=v_revision.upload_id)<>v_report_header.line_count
      or (select count(*) from public.weekly_source_manifest_movements mm join public.weekly_source_billing_movements movement on movement.id=mm.billing_movement_id
        where mm.client_manifest_id=v_manifest.id and movement.source_line_kind='INVOICE_VAT_ROUNDING')>1 then
      raise exception 'WEEKLY_SOURCE_REPORT_INVOICE_MANIFEST_INVALID' using errcode='55000'; end if;
  end if;
  if v_manifest.invoice_state='ADMITTED' then`);
apply('REPORT_ORIGINAL_HEADER_FACTS',`      'automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'
    )||case when v_manifest.invoice_window_cycle_id is null`,
`      'automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'
    )||case when not v_is_report then '{}'::jsonb else jsonb_build_object('self_bill_report_id',v_report_header.id,
      'report_scope_id',v_report_scope.id,'report_invoice_date',v_report_header.invoice_date) end||case when v_manifest.invoice_window_cycle_id is null`);
apply('REPORT_NULL_EVENT_SINGLE_MEMBER_ONLY',`       or (select pg_catalog.count(distinct movement.work_event_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1`,
`       or ((select pg_catalog.count(distinct movement.work_event_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1 and not(v_is_report and v_count=1))`);
apply('REPORT_STANDALONE_REAL_NULL_IDENTITY',`    if not (v_is_rounding and v_movement.candidate_id is null) then`,
`    if not ((v_is_rounding or (v_is_report and v_movement.source_line_kind='SELF_BILL_REPORT_LINE'
      and v_movement.movement_role='STANDALONE_NEGATIVE')) and v_movement.candidate_id is null) then`);
apply('REPORT_FROZEN_BOUND_OR_ACTUAL_STANDALONE_PRESENTATION',`        select snapshot.* into strict v_snapshot from public.weekly_source_state_transitions transition_row
          join public.weekly_source_final_snapshot_lines snapshot on snapshot.id=coalesce(transition_row.new_snapshot_line_id,transition_row.previous_snapshot_line_id)
          where transition_row.id=v_identity_movement.transition_id;`,
`        if v_is_report and v_identity_movement.source_line_kind='SELF_BILL_REPORT_LINE' then
          if v_identity_movement.source_facts_json->>'bound_pay_snapshot_line_id' is not null then
            select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
              where snapshot.id=(v_identity_movement.source_facts_json->>'bound_pay_snapshot_line_id')::uuid
                and snapshot.final_revision_id=(v_identity_movement.source_facts_json->>'bound_pay_final_revision_id')::uuid
                and snapshot.client_id=v_manifest.client_id and snapshot.contract_id=v_identity_movement.contract_id
                and snapshot.work_event_id=v_identity_movement.work_event_id;
          else
            if v_identity_movement.movement_role<>'STANDALONE_NEGATIVE' then
              raise exception 'WEEKLY_SOURCE_REPORT_INVOICE_BOUND_PRESENTATION_REQUIRED' using errcode='55000'; end if;
            select * into strict v_source_row from public.weekly_source_upload_rows where id=v_identity_movement.self_bill_report_row_id and upload_id=v_revision.upload_id;
            v_snapshot.work_date:=v_source_row.work_date;v_snapshot.start_at_local:=v_source_row.start_at_local;
            v_snapshot.end_at_local:=v_source_row.end_at_local;v_snapshot.break_minutes:=null;v_snapshot.external_event_identity:=v_source_row.external_source_key;
          end if;
        else
        select snapshot.* into strict v_snapshot from public.weekly_source_state_transitions transition_row
          join public.weekly_source_final_snapshot_lines snapshot on snapshot.id=coalesce(transition_row.new_snapshot_line_id,transition_row.previous_snapshot_line_id)
          where transition_row.id=v_identity_movement.transition_id;
        end if;`);
apply('REPORT_INVOICE_LINE_PRESENTATION_ARM',`    elsif v_is_expense then
      select source_row.* into strict v_source_row`,
`    elsif v_is_report and v_movement.source_line_kind='SELF_BILL_REPORT_LINE' then
      if v_movement.source_facts_json->>'bound_pay_snapshot_line_id' is not null then
        select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
          where snapshot.id=(v_movement.source_facts_json->>'bound_pay_snapshot_line_id')::uuid
            and snapshot.final_revision_id=(v_movement.source_facts_json->>'bound_pay_final_revision_id')::uuid
            and snapshot.client_id=v_manifest.client_id and snapshot.contract_id=v_movement.contract_id and snapshot.work_event_id=v_movement.work_event_id;
        v_work_date:=v_snapshot.work_date;v_start:=v_snapshot.start_at_local;v_end:=v_snapshot.end_at_local;
        v_break:=v_snapshot.break_minutes;v_reference:=v_snapshot.external_event_identity;
      else
        if v_movement.movement_role<>'STANDALONE_NEGATIVE' then
          raise exception 'WEEKLY_SOURCE_REPORT_INVOICE_BOUND_PRESENTATION_REQUIRED' using errcode='55000'; end if;
        select * into strict v_source_row from public.weekly_source_upload_rows where id=v_movement.self_bill_report_row_id and upload_id=v_revision.upload_id;
        v_work_date:=v_source_row.work_date;v_start:=v_source_row.start_at_local;v_end:=v_source_row.end_at_local;
        v_break:=null;v_reference:=v_source_row.external_source_key;
      end if;
    elsif v_is_expense then
      select source_row.* into strict v_source_row`);
apply('REPORT_NATIVE_LINE_KIND',`      when v_movement.source_line_kind in ('INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION') then v_movement.source_line_kind`,
`      when v_movement.source_line_kind in ('INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION','SELF_BILL_REPORT_LINE') then v_movement.source_line_kind`);
apply('REPORT_NATIVE_ORIGIN_KIND',`      when v_is_rounding then 'INVOICE_VAT_ROUNDING'
      when v_is_expense then 'SOURCE_FIXED_EXPENSE'`,
`      when v_is_rounding then 'INVOICE_VAT_ROUNDING'
      when v_is_report and v_movement.source_line_kind='SELF_BILL_REPORT_LINE' then
        case when v_movement.movement_role='STANDALONE_NEGATIVE' then 'SELF_BILL_REPORT_STANDALONE_CREDIT' else 'SELF_BILL_REPORT_LINE' end
      when v_is_expense then 'SOURCE_FIXED_EXPENSE'`);
apply('REPORT_LITERAL_PROFILE',`then 'NHSP_TWO_COMPONENT_PENCE_V1' when private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id)`,
`then 'NHSP_TWO_COMPONENT_PENCE_V1' when v_is_report and v_movement.source_line_kind='SELF_BILL_REPORT_LINE' then 'SOURCE_INVOICE_MONEY_PENCE_V1' when private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id)`);
apply('REPORT_LITERAL_AMOUNT_AUTHORITY',`      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense
        or private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id)`,
`      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense
        or (v_is_report and v_movement.source_line_kind='SELF_BILL_REPORT_LINE')
        or private.weekly_source_invoice_source_amount_movement_valid_v1(v_movement.id)`);
let reversed=source;for(const hunk of [...hunks].reverse()){
 if(reversed.split(hunk.new).length!==2)throw Error('REPORT_NATIVE_ADMISSION_REVERSE_NOT_EXACT '+hunk.id);reversed=reversed.replace(hunk.new,()=>hunk.old);
}if(reversed!==saved.toString('utf8'))throw Error('REPORT_NATIVE_ADMISSION_FULL_REVERSE_FAILED');
const output='15_complete_native_report_admission.sqlinc';fs.writeFileSync(path.join(dir,output),source);
fs.writeFileSync(path.join(dir,'REPORT_ADMISSION_REVERSE.json'),JSON.stringify({schema_version:'REPORT_NATIVE_ADMISSION_COMPLETE_REVERSE_V1',
 source_commit:sourceCommit,input,output,input_sha256:sha256(saved),output_sha256:sha256(source),input_git_saved_equal:true,full_reverse:true,
 status:'SOURCE_ONLY_NATIVE_REPORT_INVOICE_ADMISSION_VALIDATION_NOT_RUN',hunks},null,2)+'\n');
console.log(JSON.stringify({source_commit:sourceCommit,input_sha256:sha256(saved),output_sha256:sha256(source),hunks:hunks.length}));
