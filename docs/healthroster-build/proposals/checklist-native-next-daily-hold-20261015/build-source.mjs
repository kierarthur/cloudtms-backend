// Unmounted complete-owner source construction. No SQL/native/database calls.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const own='e7e0b572bc94ef65ad05899d755cd43d75a82b97',native='fa66183f01913e86651e82816f5e88f97e07123c';
const ordinary='docs/healthroster-build/proposals/checklist-native-ordinary-next-hold-20261015/forward/';
const action='docs/healthroster-build/proposals/checklist-invoice-decision-action-20261015/forward/';
const sha=b=>createHash('sha256').update(b).digest('hex'),records=[];
for(const d of ['input','forward'])fs.mkdirSync(path.join(here,d),{recursive:true});
function build(file,output,hunks,commit=own){const raw=execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:8e6});
 assert.deepEqual(raw,fs.readFileSync(path.join(repo,file)),'saved predecessor '+file);let after=raw.toString();
 for(const h of hunks){assert.equal(after.split(h.before).length,2,'unique forward '+h.label);after=after.replace(h.before,()=>h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,'unique reverse '+h.label);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,raw.toString(),'entire reverse');
 fs.writeFileSync(path.join(here,'input',output),raw);fs.writeFileSync(path.join(here,'forward',output),after);
 records.push({source_commit:commit,source_path:file,input:'input/'+output,output:'forward/'+output,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),whole_reverse_equal:true,hunks});}

// The native publisher already freezes the real Client in this exact revision.
// Preserve every contracted/Source/treatment/withdrawal branch and every reason,
// monetary split, write, caller, lock and ACL; fill only the real DAILY origin.
const beforeClient="  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;\n  select r.* into v_revision from private.bpay_next_work_revision r\n    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end\n      and r.work_id=v_work.id;";
const afterClient=beforeClient+`
  -- Contractless DAILY authority is the existing publisher's sealed revision
  -- and its exact physical/current TF, never a name, original-row fallback or
  -- an invented Contract. Incomplete/changed evidence leaves Client unknown.
  if v_work.contract_id is null and v_work.work_kind='ORDINARY' and v_work.origin_kind='DAILY'
    and v_revision.source_kind='ORDINARY' and v_revision.sealed_at_utc is not null
    and v_revision.daily_approval_evidence_json->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    and v_revision.daily_approval_evidence_json->>'timesheet_id'=v_revision.physical_timesheet_id::text
    and v_revision.daily_approval_evidence_json->>'timesheet_version'=v_revision.physical_timesheet_version::text
    and v_revision.daily_approval_evidence_json->>'financial_id'=v_revision.financial_snapshot_id::text
    and v_revision.daily_approval_evidence_json->>'booking_id'=v_work.booking_id
    and v_revision.daily_approval_evidence_json->>'candidate_id'=v_work.candidate_id::text
    and v_revision.daily_approval_evidence_json->>'work_date'=v_work.origin_work_date::text then
    select f.client_id into v_client from public.timesheets t join public.timesheets_financials f
      on f.id=v_revision.financial_snapshot_id and f.timesheet_id=t.timesheet_id
      where t.timesheet_id=v_revision.physical_timesheet_id and t.booking_id=v_work.booking_id
        and t.version=v_revision.physical_timesheet_version and t.is_current and t.contract_id is null
        and t.sheet_scope::text='DAILY' and t.line_type::text='HOURS'
        and t.week_ending_date=v_work.week_ending_date and t.authorised_at_server is not null
        and t.revoked_at is null and t.archived_at_utc is null and not t.is_adjustment
        and t.parent_timesheet_id is null and t.correction_id is null
        and f.is_current and not f.is_stale and f.timesheet_version=t.version and f.authorised_at_utc is not null
        and f.processed_at_utc is not null and f.basis::text='SELF_REPORTED' and f.candidate_id=v_work.candidate_id
        and f.client_id::text=v_revision.daily_approval_evidence_json->>'client_id'
        and (t.worked_start_iso at time zone 'Europe/London')::date=v_work.origin_work_date
        and (f.worked_start_iso,f.worked_end_iso,f.break_start_iso,f.break_end_iso,f.break_minutes)
          is not distinct from (t.worked_start_iso,t.worked_end_iso,t.break_start_iso,t.break_end_iso,t.break_minutes)
        and t.settings_authority_json->>'authority_version'='CONTRACT_SETTINGS_AUTHORITY_V1'
        and t.settings_authority_json->>'workflow'='DAILY'
        and t.settings_authority_json->>'client_id'=f.client_id::text
        and t.settings_authority_json->'contract_id'='null'::jsonb
        and t.settings_authority_json->>'relevant_date'=v_work.origin_work_date::text
        and (select count(*) from public.timesheets current_ts where current_ts.booking_id=t.booking_id and current_ts.is_current)=1
        and (select count(*) from public.timesheets_financials current_tf where current_tf.timesheet_id=t.timesheet_id and current_tf.is_current)=1;
  end if;`;
build('supabase/repeatable/12102026_2146_banking_pay_next_treatment_workbench_query_identity_v1.sql','15102026_0352_native_workbench_daily_client_v1.sql',[
 {label:'native contractless Daily Client comes only from its complete current publisher evidence',before:beforeClient,after:afterClient}
],native);

const edits=[];const edit=(label,before,after)=>edits.push({label,before,after});
edit('finite read includes the existing resolved contractless Daily origin',
'-- Finite ordinary WEEKLY Contract NEXT positive AVAILABLE hold certificate.',
'-- Finite ordinary WEEKLY Contract / resolved contractless DAILY NEXT AVAILABLE hold certificate.');
edit('record native origin without creating a second financial resolver',
'  frame jsonb; frame_after jsonb; source_amount numeric; pence numeric; positive_count bigint; proof text;',
'  frame jsonb; frame_after jsonb; source_amount numeric; pence numeric; positive_count bigint; proof text;\n  native_daily boolean:=false;v_identity_candidate uuid;v_identity_client uuid;daily_date date;daily_evidence jsonb;');
edit('keep Sunday refusal and every native authorisation/history guard',
`  -- Actual current weekly Contract arm only. Native Daily/null-Contract and
  -- own-calendar conversion need their separate complete identity proof.
  if root.sheet_scope::text is distinct from 'WEEKLY' or root.line_type::text is distinct from 'HOURS'
    or root.contract_id is null or route is distinct from 'ORDINARY_HOURS'`,
`  native_daily:=root.sheet_scope::text='DAILY' and root.contract_id is null and route='DAILY_HOURS';
  -- The real Daily publisher owns its Sunday work week. No GET calendar
  -- conversion, rate lookup or native projection refresh is introduced.
  if (not native_daily and (root.sheet_scope::text is distinct from 'WEEKLY' or root.contract_id is null or route is distinct from 'ORDINARY_HOURS'))
    or root.line_type::text is distinct from 'HOURS'`);
const contractBlock=`  select c.* into contract from public.contracts c where c.id=root.contract_id;
  if contract.id is null or contract.client_id::text is distinct from witness->>'client_id'
    or contract.candidate_id::text is distinct from witness->>'candidate_id'
    or contract.id::text is distinct from witness->>'contract_id' then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_CONTRACT_IDENTITY_UNAVAILABLE','native_facts',facts); end if;`;
edit('ordinary contract admission remains complete; Daily uses its actual submission identity',contractBlock,
`  if not native_daily then
${contractBlock}
    v_identity_candidate:=contract.candidate_id;v_identity_client:=contract.client_id;
  else
    v_identity_candidate:=(witness->>'candidate_id')::uuid;v_identity_client:=(witness->>'client_id')::uuid;
    daily_date:=(root.worked_start_iso at time zone 'Europe/London')::date;
    if witness->'contract_id' is distinct from 'null'::jsonb or witness->>'sheet_scope' is distinct from 'DAILY'
      or witness->>'work_date' is distinct from daily_date::text or daily_date is null
      or root.week_ending_date is distinct from daily_date+(7-extract(isodow from daily_date)::integer)
      or root.is_adjustment or root.parent_timesheet_id is not null or root.correction_id is not null then
      return jsonb_build_object('known',false,'reason','NATIVE_DAILY_SUBMISSION_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
  end if;`);
edit('current TF keeps exact native physical identity for either supported origin',
`    or financial.candidate_id is distinct from contract.candidate_id or financial.client_id is distinct from contract.client_id
    or financial.pay_on_hold is distinct from true then`,
`    or financial.candidate_id is distinct from v_identity_candidate or financial.client_id is distinct from v_identity_client
    or financial.pay_on_hold is distinct from true
    or (native_daily and (financial.basis::text is distinct from 'SELF_REPORTED' or financial.processed_at_utc is null
      or (financial.worked_start_iso,financial.worked_end_iso,financial.break_start_iso,financial.break_end_iso,financial.break_minutes)
        is distinct from (root.worked_start_iso,root.worked_end_iso,root.break_start_iso,root.break_end_iso,root.break_minutes))) then`);
edit('match exact native publisher Work origin without manufacturing a Contract',
`    where w.work_kind='ORDINARY' and w.origin_kind='CONTRACT' and w.origin_work_date is null
      and w.booking_id=root.booking_id and w.contract_id=contract.id and w.candidate_id=contract.candidate_id`,
`    where w.work_kind='ORDINARY'
      and ((not native_daily and w.origin_kind='CONTRACT' and w.origin_work_date is null)
        or (native_daily and w.origin_kind='DAILY' and w.origin_work_date=daily_date))
      and w.booking_id=root.booking_id and w.contract_id is not distinct from root.contract_id and w.candidate_id=v_identity_candidate`);
edit('Daily captured approval belongs to this exact sealed native revision',
`  facts:=facts||jsonb_build_object('work',to_jsonb(work),'revision',to_jsonb(revision));`,
`  daily_evidence:=revision.daily_approval_evidence_json;
  facts:=facts||jsonb_build_object('work',to_jsonb(work),'revision',to_jsonb(revision),'native_daily',native_daily);
  if native_daily and (daily_evidence->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    or daily_evidence->>'timesheet_id' is distinct from root.timesheet_id::text
    or daily_evidence->>'timesheet_version' is distinct from root.version::text
    or daily_evidence->>'financial_id' is distinct from financial.id::text
    or daily_evidence->>'booking_id' is distinct from root.booking_id
    or daily_evidence->>'candidate_id' is distinct from v_identity_candidate::text
    or daily_evidence->>'client_id' is distinct from v_identity_client::text
    or daily_evidence->>'work_date' is distinct from daily_date::text) then
    return jsonb_build_object('known',false,'reason','NATIVE_DAILY_APPROVAL_EVIDENCE_UNAVAILABLE','native_facts',facts); end if;`);
edit('projection identity matches genuine nullable Contract',
`    or group_row.candidate_id is distinct from contract.candidate_id or group_row.client_id is distinct from contract.client_id
    or group_row.contract_id is distinct from contract.id or group_row.week_ending_date is distinct from root.week_ending_date`,
`    or group_row.candidate_id is distinct from v_identity_candidate or group_row.client_id is distinct from v_identity_client
    or group_row.contract_id is distinct from root.contract_id or group_row.week_ending_date is distinct from root.week_ending_date`);
edit('every native monetary member uses the same current nullable identity',
`        or i.candidate_id is distinct from contract.candidate_id or i.client_id is distinct from contract.client_id or i.contract_id is distinct from contract.id`,
`        or i.candidate_id is distinct from v_identity_candidate or i.client_id is distinct from v_identity_client or i.contract_id is distinct from root.contract_id`);
edit('comparison frame has an explicit genuine absent Contract',
`  frame:=jsonb_build_object('root',to_jsonb(root),'financial',to_jsonb(financial),'contract',to_jsonb(contract),'work',to_jsonb(work),`,
`  frame:=jsonb_build_object('root',to_jsonb(root),'financial',to_jsonb(financial),'contract',case when root.contract_id is null then null else to_jsonb(contract) end,'work',to_jsonb(work),`);
edit('fresh frame can preserve that same actual NULL Contract',
`    from public.timesheets t join public.timesheets_financials f on f.id=financial.id join public.contracts c on c.id=t.contract_id`,
`    from public.timesheets t join public.timesheets_financials f on f.id=financial.id left join public.contracts c on c.id=t.contract_id`);
edit('fresh frame explicitly matches the absent Contract representation',
`  select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),'contract',to_jsonb(c),'work',to_jsonb(w),'revision',to_jsonb(r),`,
`  select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),'contract',case when t.contract_id is null then null else to_jsonb(c) end,'work',to_jsonb(w),'revision',to_jsonb(r),`);
edit('return actual Daily scope/date and distinguish its factual caption',
`    'worker',coalesce(nullif(btrim(metadata.candidate_sort_name),''),work.candidate_id::text),'work_date',root.week_ending_date,`,
`    'worker',coalesce(nullif(btrim(metadata.candidate_sort_name),''),work.candidate_id::text),
    'sheet_scope',root.sheet_scope::text,'work_date',case when native_daily then daily_date else root.week_ending_date end,`);
edit('caption retains source EX VAT and actual calendar authority',
`      ' ex VAT for w/e '||to_char(root.week_ending_date,'DD/MM/YYYY')||' · '||coalesce(financial.pay_on_hold_reason,'Pay on hold'), 'subtone','muted'),`,
`      case when native_daily then ' ex VAT for '||to_char(daily_date,'DD/MM/YYYY')
        else ' ex VAT for w/e '||to_char(root.week_ending_date,'DD/MM/YYYY') end||' · '||coalesce(financial.pay_on_hold_reason,'Pay on hold'), 'subtone','muted'),`);
edit('Daily literal transport witness binds the real worked date',
`      'revision_id',revision.id,'amount_basis','SOURCE_EX_VAT','grain','CURRENT_ORDINARY_WORK_AVAILABLE','counts_for_row_admission',false,'read_proof_hash',proof),`,
`      'revision_id',revision.id,'amount_basis','SOURCE_EX_VAT','grain','CURRENT_ORDINARY_WORK_AVAILABLE',
      'daily_work_date',case when native_daily then daily_date end,'counts_for_row_admission',false,'read_proof_hash',proof),`);
build(ordinary+'wp_u3_ordinary_next_hold.sqlinc','wp_u3_ordinary_next_hold.sqlinc',edits);
build(action+'wp_u3_native_checklist.sqlinc','wp_u3_native_checklist.sqlinc',[
 {label:'financial item retains genuine supported WEEKLY or DAILY scope',
 before:"          'sheet_scope','WEEKLY','reason_code','PAY_ON_HOLD','status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,",
 after:"          'sheet_scope',raw->>'sheet_scope','reason_code','PAY_ON_HOLD','status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,"}
]);
build(action+'healthroster-checklist-source.mjs','healthroster-checklist-source.mjs',[
 {label:'only the Daily native financial item accepts its extra literal work-date witness',
 before:"      'amount_basis','grain','counts_for_row_admission','read_proof_hash']);",
 after:"      'amount_basis','grain','daily_work_date','counts_for_row_admission','read_proof_hash']);"},
 {label:'Daily item needs matching literal day; weekly predecessor transport remains accepted',
 before:"      !['SIGNED_TIMESHEETS','AGENCY_INVOICE'].includes(item.route)||item.sheet_scope!=='WEEKLY'||",
 after:"      !['SIGNED_TIMESHEETS','AGENCY_INVOICE'].includes(item.route)||!['WEEKLY','DAILY'].includes(item.sheet_scope)||\n      (item.sheet_scope==='DAILY'&&(typeof witness.daily_work_date!=='string'||!DATE.test(witness.daily_work_date)||witness.daily_work_date!==item.work_date))||\n      (item.sheet_scope==='WEEKLY'&&witness.daily_work_date!=null)||"}
]);
build(action+'checklist-read.mjs','checklist-read.mjs',[]);
fs.writeFileSync(path.join(here,'COMPLETE_FORWARD_AND_REVERSE.json'),JSON.stringify({source_only:true,mounted:false,native:false,SQL_executed:false,predecessors:[own,native],records},null,2)+'\n');
console.log(JSON.stringify({source_only:true,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0),native:false,calendar_widened:false}));
