import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),commit='2151af656561ad4d68e61e242769ed80a631fcc9';
const file='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/02_money_origins.sqlinc',sha=x=>createHash('sha256').update(x).digest('hex');
const source=execFileSync('git',['show',commit+':'+file],{cwd:own,maxBuffer:4_000_000}).toString('utf8');
const definitions=scanManagedSql(source).statements.map(row=>source.slice(row.start,row.end)).filter(body=>
 /^\s*create or replace function private\.weekly_source_vat_rounding_movement_insert_v1\(p_final_revision_id uuid,p_client_id uuid,p_identity_movement_id uuid,p_invoice_window_cycle_id uuid\)/i.test(body));
assert.equal(definitions.length,1);const before=definitions[0];let value=before;const hunks=[];
function edit(oldValue,newValue){assert.equal(value.split(oldValue).length-1,1);value=value.replace(oldValue,()=>newValue);hunks.push({before:oldValue,after:newValue});}
edit('  id uuid; pending_hold_rows uuid[]; facts jsonb; pay jsonb; charge jsonb; mapping bytea; h bytea; profile text;',
 '  id uuid; pending_hold_rows uuid[]; facts jsonb; pay jsonb; charge jsonb; mapping bytea; h bytea; profile text;\n  report_scope public.weekly_source_report_scopes%rowtype; report public.weekly_source_self_bill_reports%rowtype; report_row public.weekly_source_upload_rows%rowtype; report_upload public.weekly_source_uploads%rowtype;');
const originalGuard=`    if identity_row.final_revision_id<>revision.id or identity_row.actual_client_id<>p_client_id
      or identity_row.source_line_kind='INVOICE_VAT_ROUNDING' or identity_row.candidate_id is null
      or identity_row.contract_id is null or identity_row.work_event_id is null or identity_row.invoice_timesheet_id is null then
      raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_IDENTITY_INVALID' using errcode='55000'; end if;`;
edit(originalGuard,`    if revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE' and profile='SELF_BILL_REPORT'
      and identity_row.source_profile_kind='SELF_BILL_REPORT' and identity_row.source_line_kind='SELF_BILL_REPORT_LINE' then
      -- REPORT_NATIVE_NULLABLE_ROUNDING_ORIGIN. Clone the actual immutable
      -- Report component, including its existing NULLs. Never substitute pay
      -- identity. Report's native compositor must preserve source ordinal;
      -- actual rendered last-component equality is still a qualification gate.
      select * into strict report_scope from public.weekly_source_report_scopes where id=revision.report_scope_id;
      select * into strict report from public.weekly_source_self_bill_reports where id=report_scope.self_bill_report_id;
      select * into strict report_upload from public.weekly_source_uploads where id=revision.upload_id;
      select * into strict report_row from public.weekly_source_upload_rows where id=identity_row.self_bill_report_row_id;
      if revision.reason is distinct from 'INITIAL_FINALISATION'
        or report_scope.source_cycle_id<>revision.source_cycle_id or report_scope.client_id<>p_client_id
        or report.source_cycle_id<>revision.source_cycle_id or report.client_id<>p_client_id or report.source_group_id<>report_scope.source_group_id
        or report_upload.source_cycle_id<>revision.source_cycle_id or report_upload.report_scope_id is distinct from report_scope.id
        or report_upload.content_sha256 is distinct from report.content_sha256
        or p_invoice_window_cycle_id<>revision.source_cycle_id or identity_row.final_revision_id<>revision.id
        or identity_row.actual_client_id<>p_client_id or identity_row.finalisation_cycle_id<>revision.source_cycle_id
        or private.weekly_source_invoice_movement_window_id_v1(identity_row.id)<>p_invoice_window_cycle_id
        or report_row.upload_id<>revision.upload_id or report_row.row_finalisation_state is distinct from 'SOURCE_REPORT_LINE'
        or identity_row.source_facts_json->>'schema_version' is distinct from 'WEEKLY_SOURCE_SELF_BILL_REPORT_FACTS_V1'
        or identity_row.source_facts_json->>'self_bill_report_id' is distinct from report.id::text
        or identity_row.source_facts_json->>'report_scope_id' is distinct from report_scope.id::text
        or identity_row.source_facts_json->>'upload_row_id' is distinct from report_row.id::text
        or identity_row.source_facts_json->>'source_row_ordinal' is distinct from report_row.source_row_ordinal::text
        or identity_row.source_facts_json->>'rule_set_version_id' is distinct from revision.rule_set_version_id::text
        or identity_row.source_facts_json ? 'row_resolution_id'
        or identity_row.total_pay_ex_vat<>0
        or identity_row.canonical_pay_vector_json is distinct from private.weekly_source_invoice_only_vector_v1('PAY',0)
        or identity_row.invoice_presentation_charge_pence is distinct from report_row.source_shift_charge_pence
        or identity_row.vat_amount*100 is distinct from report_row.source_vat_pence::numeric
        or identity_row.total_inc_vat*100 is distinct from report_row.source_gross_pence::numeric
        or report_row.source_gross_pence::numeric is distinct from report_row.source_shift_charge_pence::numeric+report_row.source_vat_pence::numeric
        or exists(select 1 from public.weekly_source_upload_rows later where later.upload_id=revision.upload_id
          and later.source_row_ordinal>report_row.source_row_ordinal)
        or exists(select 1 from public.weekly_source_billing_movements other where other.final_revision_id=revision.id
          and other.actual_client_id=p_client_id and other.source_line_kind<>'SELF_BILL_REPORT_LINE') then
        raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_REPORT_ORIGIN_INVALID' using errcode='55000'; end if;
    else
${originalGuard}
    end if;`);
let reverse=value;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
fs.writeFileSync(path.join(out,'12_complete_report_nullable_rounding_owner.sqlinc'),value.trimEnd()+'\n');
fs.writeFileSync(path.join(out,'REPORT_ROUNDING_ORIGIN_REVERSE.json'),JSON.stringify({source_only:true,input_commit:commit,input_file:file,
 input_file_sha256:sha(source),input_definition_sha256:sha(before),output_definition_sha256:sha(value.trimEnd()+'\n'),
 reverse_hunks_match_entire_original_definition:true,ordinary_four_nonnull_identity_guard_preserved:true,all_native_vat_calculations_hashes_and_zero_pay_fields_unchanged:true,
 supplied_report_identity_is_existing_movement_including_nulls:true,no_new_signature_table_grant_or_selector:true,
 mandatory_external_dependency:'Report-only actual native invoice compositor source ordinal ordering and independently captured frozen/rendered last component full identity',
 actual_postgresql_or_document_acceptance:false,hunks:hunks.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_current_four_argument_factory:true,signature_added:false,ordinary_guard_preserved:true,real_B_document_gate_held:true}));
