import fs from 'node:fs';
import crypto from 'node:crypto';
const dir='docs/healthroster-build/proposals/native-financial-fixtures';
const input='docs/healthroster-build/proposals/protected-source-capability/SOURCE_CAS.sqlinc';
const bytes=fs.readFileSync(input);let sql=bytes.toString('utf8');
const old='    select root.timesheet_id,root.client_id,root.candidate_id,root.contract_id,root.family_booking_id\n      from public.timesheets root where root.timesheet_id in(select root_timesheet_id from pay_receipts)';
const replacement='    select root.timesheet_id,root.client_id,contract.candidate_id,root.contract_id,root.booking_id\n      from public.timesheets root join public.contracts contract on contract.id=root.contract_id\n      where root.timesheet_id in(select root_timesheet_id from pay_receipts)';
if(sql.split(old).length-1!==1)throw Error('CAPABILITY_SCHEMA_FORWARD_ANCHOR_CHANGED');
sql=sql.replaceAll(old,()=>replacement);
const oldFinancial='    select financial.id,financial.timesheet_id,financial.client_id,financial.candidate_id,financial.contract_id\n      from public.timesheets_financials financial where financial.id in(select published_timesheet_financial_id from pay_receipts)';
const newFinancial=`    select financial.id,financial.timesheet_id,financial.client_id,financial.candidate_id,
      (select min(m.contract_id::text)::uuid
        from pay_receipts receipt
        cross join lateral jsonb_array_elements(case when jsonb_typeof(receipt.source_unit_outcomes_json)='array'
          then receipt.source_unit_outcomes_json else '[]'::jsonb end) unit
        join public.weekly_source_billing_movements m on m.id=(unit->>'movement_id')::uuid
          and m.final_revision_id=receipt.final_revision_id and m.invoice_timesheet_id=receipt.root_timesheet_id
          and m.actual_client_id=receipt.client_id and m.candidate_id=financial.candidate_id
          and encode(m.movement_economic_hash,'hex')=unit->>'movement_economic_hash'
          and m.source_profile_kind<>'SELF_BILL_REPORT'
          and m.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')
        where receipt.published_timesheet_financial_id=financial.id
        having count(distinct m.contract_id)=1) as contract_id
      from public.timesheets_financials financial where financial.id in(select published_timesheet_financial_id from pay_receipts)`;
if(sql.split(oldFinancial).length-1!==1)throw Error('CAPABILITY_FINANCIAL_SCHEMA_FORWARD_ANCHOR_CHANGED');
sql=sql.replaceAll(oldFinancial,()=>newFinancial);
const oldGuard="  if (select count(*) from public.clients where id=any(clients))<>cardinality(clients)\n";
// CTEs are statement-local, so revalidate the immutable receipt set from the
// just-materialised facts JSON rather than referring to a stale CTE name.
const receiptGuard=`  if exists(select 1 from jsonb_array_elements(facts->'pay_receipts') receipt
    where receipt->>'published_timesheet_financial_id' is not null and (
      jsonb_typeof(receipt->'source_unit_outcomes_json') is distinct from 'array'
      or case when jsonb_typeof(receipt->'source_unit_outcomes_json')='array'
        then jsonb_array_length(receipt->'source_unit_outcomes_json')=0 else true end
      or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(receipt->'source_unit_outcomes_json')='array'
          then receipt->'source_unit_outcomes_json' else '[]'::jsonb end) unit
        where not exists(select 1 from public.weekly_source_billing_movements m
          where m.id=(unit->>'movement_id')::uuid and m.final_revision_id=(receipt->>'final_revision_id')::uuid
            and m.invoice_timesheet_id=(receipt->>'root_timesheet_id')::uuid
            and m.actual_client_id=(receipt->>'client_id')::uuid
            and exists(select 1 from jsonb_array_elements(facts->'financial_identities') financial
              where financial->>'id'=receipt->>'published_timesheet_financial_id'
                and financial->>'candidate_id'=m.candidate_id::text
                and financial->>'contract_id'=m.contract_id::text)
            and encode(m.movement_economic_hash,'hex')=unit->>'movement_economic_hash'
            and m.source_profile_kind<>'SELF_BILL_REPORT'
            and m.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION'))))) then
    raise exception 'WEEKLY_SOURCE_BULK_SOURCE_CAS_FINANCIAL_IDENTITY_INVALID' using errcode='55000'; end if;
`;
if(sql.split(oldGuard).length-1!==1)throw Error('CAPABILITY_FINANCIAL_GUARD_ANCHOR_CHANGED');
sql=sql.replaceAll(oldGuard,()=>receiptGuard+oldGuard);
fs.writeFileSync(dir+'/CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc','-- Complete forward schema correction; frozen predecessor remains evidence.\n'+sql);
fs.writeFileSync(dir+'/CAPABILITY_SCHEMA_FORWARD.json',JSON.stringify({schema_version:'CAPABILITY_SCHEMA_FORWARD_REVIEW_V1',
 input:{file:input,sha256:crypto.createHash('sha256').update(bytes).digest('hex')},
 reason:'Native Timesheet has booking_id/contract_id, no candidate_id/family_booking_id. TSFIN has no contract_id column. Candidate derives via actual root Contract; historical financial Contract derives only from the immutable pay receipt complete movement IDs/hashes with exact single Contract. Nullable TS client guard retained; missing/foreign/ambiguous immutable witnesses fail closed.',
 native_source_evidence:['supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql:4276',
 'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql:1196'],
 altered_schema:false,native_acceptance:false},null,2)+'\n');
