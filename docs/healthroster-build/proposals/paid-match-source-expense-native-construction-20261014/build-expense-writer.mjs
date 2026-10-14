import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),base=path.resolve(dir,'../paid-shift-match-writer-none-construction-20261014');
const sha=b=>createHash('sha256').update(b).digest('hex'),records=[];
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
function compose(input,output,hunks){
 const raw=fs.readFileSync(path.join(base,input)),before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 fs.mkdirSync(path.dirname(path.join(dir,output)),{recursive:true});
 fs.writeFileSync(path.join(dir,'before',path.basename(output)),raw);fs.writeFileSync(path.join(dir,output),after);
 records.push({input:'paid-shift-match-writer-none-construction-20261014/'+input,output,before_sha256:sha(raw),after_sha256:sha(after),hunks,whole_reverse_verified:true});
}
compose('forward/08_paid_match_invoice_producer.sqlinc','forward/05_complete_hours_companion.sqlinc',[
 {name:'HOURS replay cannot choose its genuine expense sibling',before:'where m.invoice_only_snapshot_line_id=s.id;',after:"where m.invoice_only_snapshot_line_id=s.id and m.source_line_kind='SOURCE_REBILL_LINE';"}
]);
compose('forward/09_paid_match_invoice_origin_assert.sqlinc','forward/06_complete_origin_dispatch.sqlinc',[
 {name:'literal new Source expense origin; ordinary full HOURS assertion retained',before:'  select * into strict m from public.weekly_source_billing_movements where id=p_movement_id;',after:"  select * into strict m from public.weekly_source_billing_movements where id=p_movement_id;\n  if m.invoice_only_snapshot_line_id is not null and m.source_line_kind='SOURCE_FIXED_EXPENSE' then\n    return private.weekly_source_paid_match_expense_invoice_origin_assert_v1(m.id);\n  end if;"}
]);
compose('forward/06_complete_fin4.sqlinc','forward/07_complete_fin4_expense.sqlinc',[
 {name:'replace construction-only expense refusal with real native invoice companion path',before:`      -- This first controlled branch has no expense write authority. The
      -- separate real source-fixed-expense composition remains engineering,
      -- and is not represented as an invented omitted-zero expense.
      if v_row.pay_effect='NONE' and exists(select 1 from public.weekly_source_row_expense_policy_snapshots expense
        where expense.row_resolution_id=v_row.resolution_id) then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_EXPENSE_COMPOSITION_REQUIRED' using errcode='55000';end if;
`,after:`      -- NONE expense values retain the genuine raw policy snapshot. Its
      -- invoice companion delegates the existing native amount/vector owner;
      -- the old payable expense generation is never replaced by this row.
`},
 {name:'real raw invoice expense materialised after its exact HOURS companion',before:"      v_movement_id:=private.weekly_source_paid_match_invoice_movement_insert_v1(v_row.id,v_revision_id);\n      if v_movement_id is not null then v_movement_count:=v_movement_count+1;end if;",after:"      v_movement_id:=private.weekly_source_paid_match_invoice_movement_insert_v1(v_row.id,v_revision_id);\n      if v_movement_id is not null then v_movement_count:=v_movement_count+1;end if;\n      v_movement_id:=private.weekly_source_paid_match_expense_invoice_insert_v1(v_row.id,v_revision_id);\n      if v_movement_id is not null then v_movement_count:=v_movement_count+1;end if;"},
 {name:'only genuine NORMAL rows can change payable expense authority',before:"          where source_row.upload_id=v_upload.id and resolution.generation=v_generation\n        ), prior_authority as (",after:"          where source_row.upload_id=v_upload.id and resolution.generation=v_generation\n            and resolution.pay_effect='NORMAL'\n        ), prior_authority as ("},
 {name:'NONE-only explicit Same presence retains old expense authority without fake omission',before:`        -- A kept genuinely absent worked event keeps its existing expense
        -- authority too. No fabricated ROW_PRESENT or zero/recovery generation.`,after:`        -- Explicit real Same rows preserve the genuine previous PAY expense
        -- basis. This is source-present invoice-only evidence, not an absent
        -- observation and not an invented zero/current expense generation.
        if exists(select 1 from public.weekly_source_final_snapshot_lines companion
          where companion.final_revision_id=v_revision_id and companion.work_event_id=v_expense_observation.work_event_id
            and companion.pay_effect='NONE' and companion.paid_overlap_choice_id=
              (v_paid_cache#>>array[companion.upload_row_id::text,'choice_id'])::bigint)
          and not exists(select 1 from public.weekly_source_final_snapshot_lines payable
            where payable.final_revision_id=v_revision_id and payable.work_event_id=v_expense_observation.work_event_id
              and payable.pay_effect='NORMAL') then
          continue;
        end if;
        -- A kept genuinely absent worked event keeps its existing expense
        -- authority too. No fabricated ROW_PRESENT or zero/recovery generation.`}
]);
compose('one-time/01_invoice_snapshot_origins.sqlinc','one-time/08_complete_snapshot_origin_constraints.sqlinc',[
 {name:'actual Source fixed expense snapshot is an alternate genuine origin',before:"and ((expense_authority_generation_id is not null)=(source_line_kind='SOURCE_FIXED_EXPENSE'))\n    and ((expense_authority_generation_id is not null)=(movement_role in ('EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT')))",after:"and ((expense_authority_generation_id is not null or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'\n      and source_line_kind='SOURCE_FIXED_EXPENSE'))=(source_line_kind='SOURCE_FIXED_EXPENSE'))\n    and ((expense_authority_generation_id is not null or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'\n      and source_line_kind='SOURCE_FIXED_EXPENSE'))=(movement_role in ('EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT')))"},
 {name:'closed HOUR or native Source expense component; no payable expense generation',before:"and (invoice_only_snapshot_line_id is null or (source_line_kind='SOURCE_REBILL_LINE' and pay_effect='NONE'\n      and source_profile_kind in ('HEALTHROSTER_ACTUAL_ROWS','GENERIC_COMPLETE_SNAPSHOT')))",after:"and (invoice_only_snapshot_line_id is null or (pay_effect='NONE'\n      and ((source_line_kind='SOURCE_REBILL_LINE' and movement_role='POSITIVE')\n        or (source_line_kind='SOURCE_FIXED_EXPENSE' and movement_role='EXPENSE_POSITIVE'))\n      and source_profile_kind in ('HEALTHROSTER_ACTUAL_ROWS','GENERIC_COMPLETE_SNAPSHOT')))"},
 {name:'all NONE invoice components carry native zero PAY in their own vector grain',before:"source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')",after:"(pay_effect<>'NONE' and source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION'))\n    or coalesce((pay_effect='NONE' and source_line_kind='SOURCE_FIXED_EXPENSE' and total_pay_ex_vat=0\n      and canonical_pay_vector_json->>'schema_version'='WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1'\n      and canonical_pay_vector_json->>'kind'='PAY' and (canonical_pay_vector_json->>'source_expense_pence')::bigint=0),false)"},
 {name:'two closed genuine components per actual Source snapshot',before:'on public.weekly_source_billing_movements(invoice_only_snapshot_line_id) where invoice_only_snapshot_line_id is not null;',after:'on public.weekly_source_billing_movements(invoice_only_snapshot_line_id,source_line_kind) where invoice_only_snapshot_line_id is not null;'}
]);
compose('forward/13_complete_invoice_admission.sqlinc','forward/09_complete_invoice_admission.sqlinc',[
 {name:'expense companion links only the actual same-snapshot HOURS presentation',before:"      case when v_is_expense then (\n        select pg_catalog.min(shift_line.id::text)::uuid",after:`      case when v_is_expense and v_movement.invoice_only_snapshot_line_id is not null then (
        select pg_catalog.min(shift_line.id::text)::uuid
        from public.weekly_source_invoice_presentation_lines shift_line
        join public.weekly_source_invoice_line_bindings hours_binding on hours_binding.presentation_line_id=shift_line.id
          and hours_binding.state='CURRENT' and hours_binding.invoice_id=v_invoice_id
        join public.weekly_source_billing_movements hours_movement on hours_movement.id=hours_binding.billing_movement_id
        where shift_line.client_manifest_id=v_manifest.id
          and hours_movement.invoice_only_snapshot_line_id=v_movement.invoice_only_snapshot_line_id
          and hours_movement.source_line_kind='SOURCE_REBILL_LINE' and hours_movement.pay_effect='NONE'
        having count(*)=1
      ) when v_is_expense then (
        select pg_catalog.min(shift_line.id::text)::uuid`}
]);
fs.writeFileSync(path.join(dir,'EXPENSE_WRITER_REVERSES.json'),JSON.stringify({source_only:true,records,
 unchanged_native_cash_and_reimbursement_authorities:true,new_payable_expense_generations_for_NONE:0,
 source_invoice_membership_and_origin_assertion_required:true,canonical_mounted:false,SQL_compiled:false,native_proof:false,
 full_pay_prepared_consumer_census_complete:false,Office_UI_and_OFF_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({output:r.output,hunks:r.hunks.length,sha256:r.after_sha256}))));
