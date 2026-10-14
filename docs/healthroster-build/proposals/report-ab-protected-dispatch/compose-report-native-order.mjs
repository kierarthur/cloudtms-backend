import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {sha256} from './weekly-source-protected-release-qualification.mjs';
const root=process.argv[2],dir=path.dirname(fileURLToPath(import.meta.url));if(!root)throw Error('EXPLICIT_READ_ONLY_ROOT_REQUIRED');
const input='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/11_complete_native_snapshot_assembler.sqlinc';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:null,maxBuffer:32*1024*1024});
const sourceCommit=git(['rev-parse','HEAD']).toString().trim(),saved=fs.readFileSync(path.join(root,input)),committed=git(['show',sourceCommit+':'+input]);
if(!saved.equals(committed))throw Error('REPORT_NATIVE_ORDER_INPUT_NOT_COMMITTED');
const hunks=[];let source=saved.toString('utf8');
function apply(id,before,after){if(source.split(before).length!==2)throw Error('REPORT_NATIVE_ORDER_HUNK_NOT_EXACT '+id);
 source=source.replace(before,()=>after);hunks.push({id,old:before,new:after});}
apply('REPORT_ONLY_ORDER_LOCAL','  v_now timestamptz := coalesce(p_now_utc, now());',`  v_now timestamptz := coalesce(p_now_utc, now());
  v_report_invoice record; v_report_revision public.weekly_source_final_revisions%rowtype;
  v_report_scope public.weekly_source_report_scopes%rowtype; v_report_header public.weekly_source_self_bill_reports%rowtype;
  v_report_upload public.weekly_source_uploads%rowtype;`);
apply('REPORT_FULL_NATIVE_MEMBERSHIP_GUARD','  return query\n  with recursive raw_requests as materialized (',`  -- Report order is native immutable source order. A full same-Report invoice
  -- must prove every row/member; no subset, timestamp, UUID or hash sorting
  -- supplies B's last actual line. Ordinary/NHSP/mixed-origin paths are retained.
  for v_report_invoice in
    select distinct invoice.id,invoice.client_id,invoice.header_snapshot_json
    from public.invoices invoice
    join (select case when coalesce(value->>'entity_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and upper(coalesce(value->>'entity_type',''))='INVOICE' then (value->>'entity_id')::uuid end invoice_id
      from jsonb_array_elements(p_requests)) requested on requested.invoice_id=invoice.id
    where exists(select 1 from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
      join public.weekly_source_final_revisions revision on revision.id=movement.final_revision_id
      where binding.invoice_id=invoice.id and binding.state='CURRENT' and revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE')
      or exists(select 1 from public.weekly_source_final_revisions revision
        where revision.id::text=invoice.header_snapshot_json#>>'{meta,final_revision_id}' and revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE')
  loop
    perform 1 from public.invoices where id=v_report_invoice.id for share;
    perform 1 from public.invoice_lines where invoice_id=v_report_invoice.id for share;
    select revision.* into strict v_report_revision
      from public.weekly_source_final_revisions revision
      where revision.id in(select distinct movement.final_revision_id from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
        where binding.invoice_id=v_report_invoice.id and binding.state='CURRENT') for share;
    select * into strict v_report_scope from public.weekly_source_report_scopes where id=v_report_revision.report_scope_id for share;
    select * into strict v_report_header from public.weekly_source_self_bill_reports where id=v_report_scope.self_bill_report_id;
    select * into strict v_report_upload from public.weekly_source_uploads where id=v_report_revision.upload_id;
    -- Rendering binds the retained invoice authority, never the current scope
    -- head. Admission owns CURRENT; an immutable historical document keeps its
    -- original revision/upload after any later permitted native transition.
    if v_report_revision.authority_scope_kind<>'SELF_BILL_REPORT_SCOPE' or v_report_revision.state not in ('CURRENT','PREPARED','SUPERSEDED')
      or v_report_revision.reason<>'INITIAL_FINALISATION'
      or v_report_invoice.header_snapshot_json#>>'{meta,final_revision_id}' is distinct from v_report_revision.id::text
      or v_report_invoice.client_id is distinct from v_report_header.client_id
      or v_report_scope.source_cycle_id<>v_report_revision.source_cycle_id
      or v_report_scope.source_group_id<>v_report_header.source_group_id or v_report_scope.client_id<>v_report_header.client_id
      or v_report_header.source_cycle_id<>v_report_revision.source_cycle_id
      or v_report_upload.report_scope_id is distinct from v_report_scope.id
      or v_report_upload.source_cycle_id is distinct from v_report_revision.source_cycle_id
      or v_report_upload.content_sha256 is distinct from v_report_header.content_sha256
      or (v_report_invoice.header_snapshot_json#>>'{meta,report_scope_id}' is not null
        and v_report_invoice.header_snapshot_json#>>'{meta,report_scope_id}' is distinct from v_report_scope.id::text)
      or (v_report_invoice.header_snapshot_json#>>'{meta,self_bill_report_id}' is not null
        and v_report_invoice.header_snapshot_json#>>'{meta,self_bill_report_id}' is distinct from v_report_header.id::text)
      or exists(select 1 from public.invoice_lines line where line.invoice_id=v_report_invoice.id and
        (select count(*) from public.weekly_source_invoice_line_bindings binding where binding.invoice_line_id=line.id
          and binding.invoice_id=line.invoice_id and binding.state='CURRENT')<>1)
      or exists(select 1 from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
        left join public.weekly_source_upload_rows row_value on row_value.id=movement.self_bill_report_row_id
        where binding.invoice_id=v_report_invoice.id and binding.state='CURRENT' and
          (movement.final_revision_id<>v_report_revision.id or movement.actual_client_id<>v_report_header.client_id
           or movement.source_profile_kind<>'SELF_BILL_REPORT' or movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING')
           or movement.total_pay_ex_vat<>0 or movement.canonical_pay_vector_json is distinct from private.weekly_source_invoice_only_vector_v1('PAY',0)
           or (movement.source_line_kind='SELF_BILL_REPORT_LINE' and (row_value.id is null or row_value.upload_id<>v_report_revision.upload_id
             or row_value.row_finalisation_state<>'SOURCE_REPORT_LINE' or movement.source_facts_json->>'upload_row_id' is distinct from row_value.id::text
             or movement.source_facts_json->>'source_row_ordinal' is distinct from row_value.source_row_ordinal::text
             or movement.source_facts_json->>'report_scope_id' is distinct from v_report_scope.id::text
             or movement.source_facts_json->>'self_bill_report_id' is distinct from v_report_header.id::text
             or movement.source_facts_json->>'rule_set_version_id' is distinct from v_report_revision.rule_set_version_id::text
             or movement.invoice_presentation_charge_pence is distinct from row_value.source_shift_charge_pence
             or movement.vat_amount*100 is distinct from row_value.source_vat_pence::numeric
             or movement.total_inc_vat*100 is distinct from row_value.source_gross_pence::numeric))
           or (movement.source_line_kind='INVOICE_VAT_ROUNDING' and movement.vat_rounding_revision_id is distinct from v_report_revision.id)))
      or (select count(*) from public.weekly_source_upload_rows row_value where row_value.upload_id=v_report_revision.upload_id)<>v_report_header.line_count
      or exists(select 1 from public.weekly_source_upload_rows row_value where row_value.upload_id=v_report_revision.upload_id and
        (select count(*) from public.weekly_source_invoice_line_bindings binding
          join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
          where binding.invoice_id=v_report_invoice.id and binding.state='CURRENT' and movement.self_bill_report_row_id=row_value.id)<>1)
      or (select count(*) from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
        where binding.invoice_id=v_report_invoice.id and binding.state='CURRENT' and movement.source_line_kind='INVOICE_VAT_ROUNDING')>1 then
      raise exception 'WEEKLY_SOURCE_REPORT_PRESENTATION_ORDER_UNQUALIFIED' using errcode='55000';
    end if;
    perform private.weekly_source_invoice_allocation_assert_v1(v_report_invoice.id);
  end loop;
  return query
  with recursive raw_requests as materialized (`);
apply('REPORT_NATIVE_ORDINAL_CTE','  reference_scope as materialized (',`  report_invoice_order as materialized (
    select binding.invoice_id,binding.invoice_line_id,presentation.candidate_display_snapshot report_worker_name,
      presentation.booking_reference_snapshot report_reference,
      case when movement.source_line_kind='SELF_BILL_REPORT_LINE' then row_value.source_row_ordinal::bigint
        when movement.source_line_kind='INVOICE_VAT_ROUNDING' then
          (select max(last_row.source_row_ordinal)::bigint+1 from public.weekly_source_upload_rows last_row where last_row.upload_id=revision.upload_id)
      end report_source_ordinal
    from requested_invoices requested
    join public.weekly_source_invoice_line_bindings binding on binding.invoice_id=requested.invoice_id and binding.state='CURRENT'
    join public.weekly_source_billing_movements movement on movement.id=binding.billing_movement_id
    join public.weekly_source_invoice_presentation_lines presentation on presentation.id=binding.presentation_line_id
    join public.weekly_source_final_revisions revision on revision.id=movement.final_revision_id and revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE'
    left join public.weekly_source_upload_rows row_value on row_value.id=movement.self_bill_report_row_id
  ),
  reference_scope as materialized (`);
apply('REPORT_ACTUAL_DISPLAY_ORDINAL','           row_number() over(partition by il.invoice_id order by il.created_at,il.id)::integer base_display_order,',
`           row_number() over(partition by il.invoice_id order by report_order.report_source_ordinal nulls last,il.created_at,il.id)::integer base_display_order,`);
apply('REPORT_IMMUTABLE_NATIVE_WORKER',"           coalesce(nullif(btrim(vs.candidate_name),''),nullif(btrim(t.occupant_key_norm),'')) worker_name,",
`           case when report_order.invoice_line_id is not null then report_order.report_worker_name
             else coalesce(nullif(btrim(vs.candidate_name),''),nullif(btrim(t.occupant_key_norm),'')) end worker_name,`);
apply('REPORT_IMMUTABLE_NATIVE_REFERENCE','           case when il.timesheet_id is null\n             then coalesce(nullif(btrim(il.meta_json->>\'reference\'),\'\'),',
`           case when report_order.invoice_line_id is not null then report_order.report_reference
             when il.timesheet_id is null
             then coalesce(nullif(btrim(il.meta_json->>'reference'),''),`);
apply('REPORT_EXACT_NATIVE_LINE_JOIN','    from public.invoice_lines il\n    left join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current',
`    from public.invoice_lines il
    left join report_invoice_order report_order on report_order.invoice_id=il.invoice_id and report_order.invoice_line_id=il.id
    left join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current`);
apply('REPORT_ACTUAL_RENDER_ARRAY_ORDER','           ) order by c.created_at,c.id,c.base_display_order) lines,',
`           ) order by case when exists(select 1 from report_invoice_order report_order where report_order.invoice_id=c.invoice_id)
             then c.base_display_order end nulls last,c.created_at,c.id,c.base_display_order) lines,`);
const ending=source.match(/grant execute on function private\._invoice_presentation_snapshot_batch\(jsonb,timestamptz\)\r?\n  to service_role;(?:\r?\n){2,}$/)?.[0];
if(!ending)throw Error('REPORT_NATIVE_ORDER_EOF_NOT_EXACT');
apply('PACKET_EOF_ONLY_NO_BODY_CHANGE',ending,ending.replace(/(?:\r?\n){2,}$/,()=>ending.includes('\r')?'\r\n':'\n'));
let reversed=source;for(const hunk of [...hunks].reverse()){
 if(reversed.split(hunk.new).length!==2)throw Error('REPORT_NATIVE_ORDER_REVERSE_NOT_EXACT '+hunk.id);reversed=reversed.replace(hunk.new,()=>hunk.old);
}if(reversed!==saved.toString('utf8'))throw Error('REPORT_NATIVE_ORDER_FULL_REVERSE_FAILED');
const output='14_complete_native_snapshot_assembler.sqlinc';fs.writeFileSync(path.join(dir,output),source);
fs.writeFileSync(path.join(dir,'REPORT_ORDER_REVERSE.json'),JSON.stringify({schema_version:'REPORT_NATIVE_PRESENTATION_ORDER_COMPLETE_REVERSE_V1',
 source_commit:sourceCommit,input,output,input_sha256:sha256(saved),output_sha256:sha256(source),input_git_saved_equal:true,full_reverse:true,
 status:'SOURCE_ONLY_ORDINARY_BYTES_RETAINED_NATIVE_REPORT_ADMISSION_RENDER_NOT_RUN',hunks},null,2)+'\n');
console.log(JSON.stringify({source_commit:sourceCommit,input_sha256:sha256(saved),output_sha256:sha256(source),hunks:hunks.length}));
