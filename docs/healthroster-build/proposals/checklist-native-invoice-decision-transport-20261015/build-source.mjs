// Immutable whole producer/DTO successor over the frozen invoice-block source.
// No SQL/native action; no predecessor file is edited.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const commit='aea33728bb76c4d2c57a9e82abffa570e743a0f0',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const input=fs.mkdirSync(path.join(here,'input'),{recursive:true}),proofs=[];
function inputBytes(file){const b=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6});assert.ok(b.equals(fs.readFileSync(path.join(repo,file))),file+' frozen predecessor');return b;}
function successor(file,output,hunks){const raw=inputBytes(file),eol=raw.includes(Buffer.from('\r\n'))?'\r\n':'\n';let next=raw.toString();const applied=[];
 for(const h of hunks){const before=h.before.replaceAll('\n',eol),after=h.after.replaceAll('\n',eol);assert.equal(next.split(before).length,2,h.label);next=next.replace(before,()=>after);applied.push({...h,before,after});}
 let reverse=next;for(const h of [...applied].reverse()){assert.equal(reverse.split(h.after).length,2,h.label);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,raw.toString());
 const saved='input/'+output;fs.writeFileSync(path.join(here,saved),raw);fs.writeFileSync(path.join(here,'forward',output),next);
 proofs.push({source_commit:commit,source_path:file,saved,output:'forward/'+output,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(next),after_sha256:sha(next),hunks:applied});}
successor('broker/src/weekly-source/healthroster-checklist-source.mjs','healthroster-checklist-source.mjs',[
 {label:'finite native pending-decision authority',before:"'SOURCE_QUERY_EX_VAT', 'SOURCE_INVOICE_BLOCK', 'NEXT'",after:"'SOURCE_QUERY_EX_VAT', 'SOURCE_INVOICE_BLOCK', 'SOURCE_INVOICE_DECISION', 'NEXT'"},
 {label:'positive pending-decision holds no pay quantity',before:'  return copy(value);\n}\n\nexport function unavailableChecklistHold()',after:`  if (value.authority.owner === 'SOURCE_INVOICE_DECISION' && (value.holds_pay !== null || value.holds_pay_scope !== null ||
      value.withheld_amount_pence !== null || value.holds_invoice !== true || value.pending_decision !== true ||
      value.authority.withholding_known !== false)) fail('CHECKLIST_SOURCE_INVOICE_DECISION_GRAIN_INVALID');
  return copy(value);
}

export function unavailableChecklistHold()`},
 {label:'closed item adds exact optional decision witness',before:"'queries_parent_witness', 'native_invoice_witness']);",after:"'queries_parent_witness', 'native_invoice_witness', 'native_invoice_decision_witness']);"},
 {label:'bind complete actual decision witness to exact base invoice actor scope and member',before:`      fail('CHECKLIST_NATIVE_INVOICE_TARGET_INVALID');
    }
  } else {
    uuid(item.counted_timesheet_id);`,after:`      fail('CHECKLIST_NATIVE_INVOICE_TARGET_INVALID');
    }
    if (item.native_invoice_decision_witness != null) {
      const decision=item.native_invoice_decision_witness;
      closed(decision,['owner','actor_user_id','scope','billing_movement_id','client_manifest_id','upload_row_id',
        'invoice_block_read_proof_hash','source_row_hash','authority_scope_version','hold_kind','qualification_choice','read_proof_hash']);
      uuid(decision.actor_user_id);bindScope(decision.scope);
      for(const key of ['billing_movement_id','client_manifest_id','upload_row_id'])uuid(decision[key]);
      for(const key of ['invoice_block_read_proof_hash','source_row_hash','read_proof_hash']) {
        if(typeof decision[key]!=='string'||!HASH.test(decision[key]))fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
      }
      if(decision.owner!=='private.weekly_source_invoice_hold_release_context_core_v1'||
        !same(decision.actor_user_id,actor.actor_user_id)||!sameScope(decision.scope,item.scope)||
        !['billing_movement_id','client_manifest_id','upload_row_id'].every(k=>same(decision[k],witness[k]))||
        decision.invoice_block_read_proof_hash!==witness.read_proof_hash||
        !Number.isSafeInteger(decision.authority_scope_version)||decision.authority_scope_version<0||
        decision.hold_kind!=='VAT_FIT'||decision.qualification_choice!=='BILL_AT_NET_PLUS_20'||
        item.hold?.authority?.owner!=='SOURCE_INVOICE_DECISION'||item.hold.authority.hash!==decision.read_proof_hash) {
        fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
      }
    }
  } else {
    uuid(item.counted_timesheet_id);`},
 {label:'decision witness cannot escape invoice item grain',before:"  if (item.item_kind !== 'NATIVE_INVOICE' && item.native_invoice_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');",after:"  if (item.item_kind !== 'NATIVE_INVOICE' && (item.native_invoice_witness != null || item.native_invoice_decision_witness != null)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');"},
 {label:'separate exact base and qualified decision hold bindings',before:`  if ((hold.authority.owner === 'SOURCE_INVOICE_BLOCK') !== (item.item_kind === 'NATIVE_INVOICE') ||
      (item.item_kind === 'NATIVE_INVOICE' && hold.authority.hash !== item.native_invoice_witness.read_proof_hash)) {
    fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');
  }`,after:`  const invoiceOwner=['SOURCE_INVOICE_BLOCK','SOURCE_INVOICE_DECISION'].includes(hold.authority.owner);
  if (invoiceOwner !== (item.item_kind === 'NATIVE_INVOICE') ||
      (hold.authority.owner === 'SOURCE_INVOICE_BLOCK' && hold.authority.hash !== item.native_invoice_witness.read_proof_hash)) {
    fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');
  }
  if (hold.authority.owner === 'SOURCE_INVOICE_DECISION' && item.native_invoice_decision_witness == null) {
    fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
  }`},
 {label:'finite invoice decision cannot certify whole main row',before:"  if (hold.authority.owner === 'SOURCE_INVOICE_BLOCK') fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');",after:"  if (['SOURCE_INVOICE_BLOCK','SOURCE_INVOICE_DECISION'].includes(hold.authority.owner)) fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');"}
]);
successor('supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_native_checklist.sqlinc','wp_u3_native_checklist.sqlinc',[
 {label:'one scope-local invoice decision composition',before:'invoice_blocks:=private.weekly_source_checklist_source_invoice_block_items_v1(row_scope,actor.id);',after:'invoice_blocks:=private.weekly_source_checklist_source_invoice_decision_items_v1(row_scope,actor.id);'},
 {label:'native qualified decision gets literal pending text without action',before:"'subtext','Invoice held: source net and gross do not fit 20% VAT.','subtone','muted'),",after:"'subtext',case when raw#>>'{hold,pending_decision}'='true' then 'Office decision needed: native VAT_FIT release is qualified.'\n              else 'Invoice held: source net and gross do not fit 20% VAT.' end,'subtone','muted'),"}
]);
// The exact whole closed transport imports its adjacent successor DTO. Its own
// request/actor/scope/page/error behavior is unchanged byte-for-byte.
successor('broker/src/weekly-source/checklist-read.mjs','checklist-read.mjs',[]);
fs.writeFileSync(path.join(here,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({status:'UNMOUNTED_INVOICE_DECISION_TRANSPORT_SOURCE_SUCCESSOR',source_commit:commit,
 native:false,public_root_mounted:false,predecessor_mutated:false,proofs},null,2)+'\n');
console.log(JSON.stringify({source_only:true,whole_owners:proofs.length,hunks:proofs.reduce((n,p)=>n+p.hunks.length,0),whole_reverses_exact:true,native:false}));
