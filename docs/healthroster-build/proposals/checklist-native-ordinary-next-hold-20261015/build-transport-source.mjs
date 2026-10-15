// Finite ordinary native financial subject over the preserved decision source.
// Each complete owner reverses to exact Git; no frozen predecessor is edited.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='9b44412d7fdda7f8e099036e7c8a7297a8eeb795',records=[];
const predecessor='docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/forward/',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function full(file,output,hunks){const b=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6}),eol=b.includes(Buffer.from('\r\n'))?'\r\n':'\n';let next=b.toString();const applied=[];
 for(const h of hunks){const before=h.before.replaceAll('\n',eol),after=h.after.replaceAll('\n',eol);assert.equal(next.split(before).length,2,h.label);next=next.replace(before,()=>after);applied.push({...h,before,after});}
 let reverse=next;for(const h of [...applied].reverse()){assert.equal(reverse.split(h.after).length,2,h.label);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,b.toString());
 const saved='input/before_'+output;fs.writeFileSync(path.join(here,saved),b);fs.writeFileSync(path.join(here,'forward',output),next);
 records.push({source_commit:commit,source_path:file,saved,output:'forward/'+output,before_sha256:sha(b),after_sha256:sha(next),hunks:applied});}
full(predecessor+'healthroster-checklist-source.mjs','healthroster-checklist-source.mjs',[
 {label:'closed ordinary available EX-VAT authority',before:"'SOURCE_INVOICE_DECISION', 'NEXT'",after:"'SOURCE_INVOICE_DECISION', 'ORDINARY_NEXT_AVAILABLE_EX_VAT', 'NEXT'"},
 {label:'ordinary amount certifies only positive current native available whole-TS hold',before:'  return copy(value);\n}\n\nexport function unavailableChecklistHold()',after:`  if (value.authority.owner === 'ORDINARY_NEXT_AVAILABLE_EX_VAT' && (value.holds_pay !== true || value.holds_pay_scope !== 'TIMESHEET' ||
      value.holds_invoice !== null || value.pending_decision !== null || value.authority.withholding_known !== true)) {
    fail('CHECKLIST_ORDINARY_NEXT_MONEY_GRAIN_INVALID');
  }
  return copy(value);
}

export function unavailableChecklistHold()`},
 {label:'closed optional native financial witness',before:"'native_invoice_decision_witness']);",after:"'native_invoice_decision_witness', 'native_ordinary_money_witness']);"},
 {label:'finite independently native financial item kind',before:"'NATIVE_SOURCE', 'NATIVE_INVOICE'].includes(item.item_kind)",after:"'NATIVE_SOURCE', 'NATIVE_INVOICE', 'NATIVE_ORDINARY_FINANCIAL'].includes(item.item_kind)"},
 {label:'exact genuine current financial Timesheet identity and admission refusal',before:`  } else {
    uuid(item.counted_timesheet_id);
    if (item.source_information_witness != null)`,after:`  } else if (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') {
    const witness=item.native_ordinary_money_witness;
    closed(witness,['owner','actor_user_id','scope','timesheet_id','financial_id','work_id','revision_id',
      'amount_basis','grain','counts_for_row_admission','read_proof_hash']);
    uuid(witness.actor_user_id);bindScope(witness.scope);
    for(const key of ['timesheet_id','financial_id','work_id','revision_id'])uuid(witness[key]);uuid(item.counted_timesheet_id);
    if(witness.owner!=='private.bpay_next_workbench_refresh_work_core_v1'||!same(witness.actor_user_id,actor.actor_user_id)||
      !sameScope(witness.scope,item.scope)||!same(witness.timesheet_id,item.counted_timesheet_id)||
      item.item_id!=='native-financial:'+witness.work_id||witness.amount_basis!=='SOURCE_EX_VAT'||
      witness.grain!=='CURRENT_ORDINARY_WORK_AVAILABLE'||witness.counts_for_row_admission!==false||
      typeof witness.read_proof_hash!=='string'||!HASH.test(witness.read_proof_hash)||
      !['SIGNED_TIMESHEETS','AGENCY_INVOICE'].includes(item.route)||item.sheet_scope!=='WEEKLY'||
      item.subject!=='PAY'||item.permitted_pay_subject!==true||item.reason_code!=='PAY_ON_HOLD'||
      item.status?.text!=='Holds pay'||item.status?.tone!=='danger'||item.correction_binding!==null||item.submission_read!==null||
      item.source_information_witness!==null||item.native_source_witness!=null||item.native_invoice_witness!=null||
      item.native_invoice_decision_witness!=null||item.queries_parent_witness!=null||item.action===undefined||
      (item.action!==null&&(item.action.kind!=='TIMESHEET'||!same(item.action.target?.timesheet_id,witness.timesheet_id)))) {
      fail('CHECKLIST_NATIVE_ORDINARY_MONEY_INVALID');
    }
  } else {
    uuid(item.counted_timesheet_id);
    if (item.source_information_witness != null)`},
 {label:'financial witness cannot be borrowed by a query or information item',before:"  const action = link(item.action ?? null, item.scope);",after:"  if (item.item_kind !== 'NATIVE_ORDINARY_FINANCIAL' && item.native_ordinary_money_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');\n  const action = link(item.action ?? null, item.scope);"},
 {label:'native quantity binds only independently classified financial subject',before:"  if (correction != null) {",after:`  if ((hold.authority.owner === 'ORDINARY_NEXT_AVAILABLE_EX_VAT') !== (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') ||
      (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL' && hold.authority.hash !== item.native_ordinary_money_witness.read_proof_hash)) {
    fail('CHECKLIST_ORDINARY_NEXT_MONEY_GRAIN_INVALID');
  }
  if (correction != null) {`},
 {label:'whole row cannot claim finite available member sum',before:"  if (hold.authority.owner === 'SOURCE_QUERY_EX_VAT') fail('CHECKLIST_SOURCE_QUERY_MONEY_GRAIN_INVALID');",after:"  if (['SOURCE_QUERY_EX_VAT','ORDINARY_NEXT_AVAILABLE_EX_VAT'].includes(hold.authority.owner)) fail('CHECKLIST_SOURCE_QUERY_MONEY_GRAIN_INVALID');"}
]);
full(predecessor+'checklist-read.mjs','checklist-read.mjs',[]);
fs.writeFileSync(path.join(here,'COMPLETE_TRANSPORT_REVERSES.json'),JSON.stringify({status:'UNMOUNTED_FINITE_ORDINARY_NEXT_FINANCIAL_DTO_SOURCE_SUCCESSOR',native:false,records},null,2)+'\n');
console.log(JSON.stringify({source_only:true,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0),native:false}));
