// New immutable purpose-bound action source; never edit the accepted predecessors.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026');
const ordinary='b0086dd9d6b5b6c1f20ef9bc57c2b73c7e669fe4',decision='9b44412d7fdda7f8e099036e7c8a7297a8eeb795',office='de90ab2134aca782a0f4b56154585fad65a81f12';
const op='docs/healthroster-build/proposals/checklist-native-ordinary-next-hold-20261015/forward/',dp='docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/';
const up='docs/healthroster-build/proposals/office-paid-native-caller-union-20261014/forward/',records=[],sha=b=>crypto.createHash('sha256').update(b).digest('hex');
for(const dir of ['input','forward','read-graph'])fs.mkdirSync(path.join(here,dir),{recursive:true});
function git(commit,file){return execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6});}
function full(commit,file,output,hunks,savedRoot=repo){const raw=git(commit,file);assert.ok(raw.equals(fs.readFileSync(path.join(savedRoot,file))),file+' exact saved predecessor');const eol=raw.includes(Buffer.from('\r\n'))?'\r\n':'\n';let next=raw.toString();const applied=[];
 for(const h of hunks){const before=h.before.replaceAll('\n',eol),after=h.after.replaceAll('\n',eol);assert.equal(next.split(before).length,2,h.label);next=next.replace(before,()=>after);applied.push({...h,before,after});}
 let reverse=next;for(const h of [...applied].reverse()){assert.equal(reverse.split(h.after).length,2,h.label+' reverse');reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,raw.toString());
 const saved='input/before_'+output;fs.writeFileSync(path.join(here,saved),raw);fs.writeFileSync(path.join(here,'forward',output),next);
 records.push({commit,file,saved,output:'forward/'+output,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(next),after_sha256:sha(next),hunks:applied});return next;}
const sql=full(decision,dp+'forward/wp_u3_source_invoice_decisions.sqlinc','wp_u3_source_invoice_decisions.sqlinc',[
 {label:'explicitly distinguish a native action descriptor from a release write',before:'-- and native release qualification. No release, money calculation or action.',after:'-- and native release qualification. No release write or money calculation.\n-- An action descriptor requires both literal native choices to qualify.'},
 {label:'same owner separate actual choice qualifications',before:"  decisions jsonb:='[]'::jsonb; witness jsonb; proof text; known boolean;",after:"  decisions jsonb:='[]'::jsonb; witness jsonb; proof text; known boolean; choices_known boolean;\n  file_request jsonb; file_context jsonb; file_context_again jsonb;"},
 {label:'unqualified base items retain no action',before:"    known:=false;context:=null;context_again:=null;request:=null;",after:"    known:=false;choices_known:=false;context:=null;context_again:=null;request:=null;\n    file_request:=null;file_context:=null;file_context_again:=null;item:=item||jsonb_build_object('action',null);"},
 {label:'qualify BILL_AS_FILE through same native owner and first-invoice assertion twice',before:"      fact:=fact||jsonb_build_object('decision_known',true,'pending_decision',true);",after:`      -- Choice eligibility is native-owned. Do not infer BILL_AS_FILE from the
      -- default qualification, calculate money or expose a public lock mode.
      file_request:=request||jsonb_build_object('action','BILL_AS_FILE');
      begin
        file_context:=private.weekly_source_invoice_hold_release_context_core_v1(file_request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        file_context_again:=private.weekly_source_invoice_hold_release_context_core_v1(file_request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        if file_context is distinct from file_context_again or (file_context-'choice') is distinct from (context-'choice')
          or file_context->>'choice' is distinct from 'BILL_AS_FILE' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
        choices_known:=true;
      exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023' then
        if sqlerrm='WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' then raise; end if;
        fact:=fact||jsonb_build_object('choice_qualification_sqlstate',sqlstate,'choice_qualification_code',sqlerrm);
      end;
      fact:=fact||jsonb_build_object('decision_known',true,'pending_decision',true,'native_release_choices_known',choices_known,
        'native_file_release_request',file_request,'native_file_release_context',file_context,'native_file_release_context_again',file_context_again);`},
 {label:'closed literal action binds both native choices to same qualified witness',before:"      item:=item||jsonb_build_object('native_invoice_decision_witness',witness,",after:`      item:=item||jsonb_build_object('action',case when choices_known then jsonb_build_object(
          'kind','INVOICE_HOLD_DECISION','label','Review invoice hold','native_allowed',true,
          'target',jsonb_build_object('contract','WEEKLY_SOURCE_CHECKLIST_INVOICE_HOLD_DECISION_ACTION_V1',
            'actor_user_id',p_actor_user_id,'permission_view','ADMIN','scope',p_scope,
            'billing_movement_id',witness->'billing_movement_id','client_manifest_id',witness->'client_manifest_id','upload_row_id',row_id,
            'expected_source_row_hash',witness->'source_row_hash','expected_authority_scope_version',cycle.version,
            'hold_kind','VAT_FIT','decision_read_proof_hash',proof,'choices',jsonb_build_array('BILL_AT_NET_PLUS_20','BILL_AS_FILE'))) end,
        'native_invoice_decision_witness',witness,`}
]);
full(ordinary,op+'healthroster-checklist-source.mjs','healthroster-checklist-source.mjs',[
 {label:'finite invoice action kind',before:"'DAILY_REVIEW', 'UPLOAD']);",after:"'DAILY_REVIEW', 'UPLOAD', 'INVOICE_HOLD_DECISION']);"},
 {label:'action requires the already validated invoice decision witness; cells and main rows have none',before:'function link(l, scope) {',after:'function link(l, scope, invoiceDecision = null) {'},
 {label:'closed actor scope native-row CAS and choice descriptor',before:'  text(l.label, 250); object(l.target);',after:`  text(l.label, 250); object(l.target);
  if(l.kind==='INVOICE_HOLD_DECISION') {
    const t=l.target,d=invoiceDecision;
    closed(t,['contract','actor_user_id','permission_view','scope','billing_movement_id','client_manifest_id','upload_row_id',
      'expected_source_row_hash','expected_authority_scope_version','hold_kind','decision_read_proof_hash','choices']);
    uuid(t.actor_user_id);bindScope(t.scope);
    for(const key of ['billing_movement_id','client_manifest_id','upload_row_id'])uuid(t[key]);
    if(!d||l.label!=='Review invoice hold'||t.contract!=='WEEKLY_SOURCE_CHECKLIST_INVOICE_HOLD_DECISION_ACTION_V1'||t.permission_view!=='ADMIN'||
      !same(t.actor_user_id,d.actor_user_id)||!sameScope(t.scope,scope)||!sameScope(t.scope,d.scope)||
      !['billing_movement_id','client_manifest_id','upload_row_id'].every(k=>same(t[k],d[k]))||
      typeof t.expected_source_row_hash!=='string'||!HASH.test(t.expected_source_row_hash)||t.expected_source_row_hash!==d.source_row_hash||
      !Number.isSafeInteger(t.expected_authority_scope_version)||t.expected_authority_scope_version<0||t.expected_authority_scope_version!==d.authority_scope_version||
      t.hold_kind!=='VAT_FIT'||typeof t.decision_read_proof_hash!=='string'||!HASH.test(t.decision_read_proof_hash)||t.decision_read_proof_hash!==d.read_proof_hash||
      !Array.isArray(t.choices)||t.choices.length!==2||t.choices[0]!=='BILL_AT_NET_PLUS_20'||t.choices[1]!=='BILL_AS_FILE') {
      fail('CHECKLIST_INVOICE_DECISION_ACTION_INVALID');
    }
    return copy(l);
  }`},
 {label:'invoice action has one native kind and only with qualified decision witness',before:"item.source_information_witness!==null||item.native_source_witness!==null||item.queries_parent_witness!=null||item.action!==null)",after:"item.source_information_witness!==null||item.native_source_witness!==null||item.queries_parent_witness!=null||item.action===undefined||\n      (item.action!==null&&(item.action.kind!=='INVOICE_HOLD_DECISION'||item.native_invoice_decision_witness==null)))"},
 {label:'other item kinds cannot borrow the invoice action descriptor',before:'  const action = link(item.action ?? null, item.scope);',after:"  if(item.action?.kind==='INVOICE_HOLD_DECISION'&&item.item_kind!=='NATIVE_INVOICE')fail('CHECKLIST_INVOICE_DECISION_ACTION_INVALID');\n  const action = link(item.action ?? null, item.scope,item.item_kind==='NATIVE_INVOICE'?item.native_invoice_decision_witness:null);"}
]);
full(ordinary,op+'checklist-read.mjs','checklist-read.mjs',[]);
full(ordinary,op+'wp_u3_native_checklist.sqlinc','wp_u3_native_checklist.sqlinc',[
 {label:'one native child action passes through without invented Finalise target',before:"'subject','INVOICE_ONLY','permitted_pay_subject',false,'action',null,'counted_timesheet_id',null,",after:"'subject','INVOICE_ONLY','permitted_pay_subject',false,'action',raw->'action','counted_timesheet_id',null,"}
]);
const fragment=fs.readFileSync(path.join(here,'office-invoice-decision.jsinc'),'utf8');
full(office,up+'workspace-actions.jsinc','workspace-actions.jsinc',[
 {label:'purpose-bound invoice child preserves complete current paid and R254 union',before:'  function handleAction(detailValue) {',after:fragment+'\n  function handleAction(detailValue) {'},
 {label:'only literal invoice descriptor dispatches the new purpose caller',before:"    const label = asText(detail.label);\n    if (detail.kind === 'CHECK_ONLY_D10_CANDIDATE_REMINDER')",after:"    const label = asText(detail.label);\n    if (detail.kind === 'CHECKLIST_INVOICE_HOLD_DECISION') return openChecklistInvoiceDecision(detail);\n    if (detail.kind === 'CHECK_ONLY_D10_CANDIDATE_REMINDER')"}
],root);
full(office,up+'import-workspace.jsinc','import-workspace.jsinc',[
 {label:'actual Checklist caller returns to same captured Checklist after native command',before:"      if(action.kind==='UPLOAD_DETAIL'&&root.CloudTMSWeeklySourceWorkspaceActionsV1)",after:`      if(action.kind==='INVOICE_HOLD_DECISION'&&root.CloudTMSWeeklySourceWorkspaceActionsV1){
        const invoiceWorkspace=session.workspace;
        const callerCurrent=()=>generation===session.openGeneration&&session.openCurrent?.()===true&&session.workspace===invoiceWorkspace;
        return root.CloudTMSWeeklySourceWorkspaceActionsV1.handleAction({kind:'CHECKLIST_INVOICE_HOLD_DECISION',label:'Review invoice hold',enabled:true,
          payload:target,context:{client:row.client},callerCurrent,returnTo:()=>current()?returnToChecklist():undefined});
      }
      if(action.kind==='UPLOAD_DETAIL'&&root.CloudTMSWeeklySourceWorkspaceActionsV1)`}
],root);
// Retain the complete exact qualification/read callee census; update only the
// changed purpose owner, which still calls the same four distinct routines.
const graph=JSON.parse(git(decision,dp+'NATIVE_READ_GRAPH.json'));
for(const entry of graph.routines){const raw=git(decision,dp+entry.saved_file);assert.equal(sha(raw),entry.definition_sha256);fs.writeFileSync(path.join(here,entry.saved_file),raw);
 if(entry.name==='private.weekly_source_checklist_source_invoice_decision_items_v1'){const event=functionEvents(sql).events.find(e=>e.definition);fs.writeFileSync(path.join(here,entry.saved_file),event.definition);entry.definition_sha256=sha(event.definition);entry.file='PROPOSED_INVOICE_DECISION_ACTION_SUCCESSOR';}}
graph.status='SOURCE_INVOICE_DECISION_ACTION_CALLEE_CENSUS_NATIVE_PURITY_UNRUN';graph.action_publication_native=false;
fs.writeFileSync(path.join(here,'NATIVE_READ_GRAPH.json'),JSON.stringify(graph,null,2)+'\n');
fs.writeFileSync(path.join(here,'COMPLETE_OWNER_REVERSES.json'),JSON.stringify({source_only:true,native:false,SQL_executed:false,canonical_mounted:false,records},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0),whole_reverses_exact:true,callee_bodies:graph.routines.length}));
