import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {projectChecklistItem,projectChecklistRow,unavailableChecklistHold,decodeChecklistHold} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../broker/src/weekly-source/checklist-read.mjs';
import {scanManagedSql,exactAtomicBody} from '../scripts/cloudtms-managed-release-sql.mjs';
import {currentOwner,forwardOwner,earlyTransitionHook,transitionFrozenHook,earlyFinalizeHook,finalizedHook}
  from '../docs/healthroster-build/proposals/checklist-native-20261014/build-owner-packet.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const scope={source_group_id:id(1),client_id:id(2),source_cycle_id:id(3),week_ending:'2026-10-11'};
const actor={actor_user_id:id(4),permission_view:'ADMIN',active:true};
const copy=x=>structuredClone(x), H='a'.repeat(64);
const read=name=>fs.readFileSync(new URL(`../supabase/repeatable/healthroster_wp_u3_source_proposals/${name}`,import.meta.url),'utf8');
function sourceItem(kind='incident'){
  const ids={incident:id(7),manual:id(8),missing:id(9)},key=`${kind}-${ids[kind]}`;
  return {item_id:`native-source:${key}`,item_kind:'NATIVE_SOURCE',scope:copy(scope),route:'SELF_BILL',worker:'Worker',work_date:'2026-10-07',sheet_scope:'WEEKLY',
    reason_code:kind==='manual'?'MANUAL_REVIEW':kind==='missing'?'CANDIDATE_TIMESHEET_MISSING':'SOURCE_HOURS_DIFFER',
    status:{text:'Needs action',tone:'warning',subtext:'Native reason',subtone:'muted'},hold:unavailableChecklistHold(),subject:'PAY',permitted_pay_subject:true,
    counted_timesheet_id:null,correction_binding:null,submission_read:null,source_information_witness:null,
    action:{kind:'QUERIES',label:'View item in Queries ›',native_allowed:true,target:{...scope,row_key:`qg_${H}`}},
    native_source_witness:{owner:'private.weekly_source_office_query_groups_v1',projection_publication_id:id(5),work_event_id:id(6),
      upload_row_id:kind!=='incident'?id(9):null,row_resolution_id:kind==='missing'?id(10):null,incident_id:kind==='incident'?id(7):null,
      comparison_revision_id:kind==='incident'?id(11):null,manual_review_id:kind==='manual'?id(8):null,native_row_key:key,parent_group_key:`qg_${H}`,evidence_fingerprint:H}};
}
function nativeItem(kind){const item=sourceItem(kind);delete item.action.target.week_ending;return item;}
test('native Source work event needs no invented submitted Timesheet and survives the whole closed read caller',()=>{
  for(const kind of ['incident','manual','missing']){
    const item=nativeItem(kind),result=projectChecklistItem(item,actor);
    assert.equal(result.counted_timesheet_id,null);assert.equal(result.hold.holds_pay,null);
    const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
    const page={ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:'ADMIN',
      binding:H,read_revision:H,rows:[],items:[item],has_more:false,next_cursor:null,refresh_required:false,anchor:null};
    assert.equal(decodeChecklistRead(page,request,{id:actor.actor_user_id,payView:false}).items[0].item_id,item.item_id);
    const reduced=copy(page);reduced.permission_view='PAY_READONLY';reduced.items[0].action=null;
    assert.equal(decodeChecklistRead(reduced,request,{id:actor.actor_user_id,payView:true}).items[0].action,null);
  }
});
test('native Source discriminant refuses invented Timesheet, foreign target, unsupported owner and incomplete native identity',()=>{
  for(const mutate of [x=>{x.counted_timesheet_id=id(20);},x=>{x.route='SIGNED_TIMESHEETS';},
    x=>{x.native_source_witness.owner='another.reader';},x=>{x.native_source_witness.comparison_revision_id=null;},
    x=>{x.action.target.row_key=`incident-${id(20)}`;},x=>{x.action.target.row_key=x.native_source_witness.native_row_key;},
    x=>{delete x.native_source_witness.parent_group_key;},x=>{x.action.target.client_id=id(20);},
    x=>{x.action={kind:'TIMESHEET',label:'Open',native_allowed:true,target:{timesheet_id:id(20)}};},
    x=>{x.correction_binding={};},x=>{x.native_source_witness.evidence_fingerprint='unproved';}]){
    const item=nativeItem('incident');mutate(item);assert.throws(()=>projectChecklistItem(item,actor));
  }
});
test('R254 native information permits only its exact existing Queries identity and never creates a parent',()=>{
  const i={...nativeItem('missing'),item_kind:'SOURCE_INFORMATION',item_id:`native-action:${H}`,route:'SIGNED_TIMESHEETS',
    reason_code:'WEEKLY_TIMESHEET_NOT_SUBMITTED',subject:'INFORMATION',permitted_pay_subject:false,native_source_witness:undefined,
    source_information_witness:{native_action_id:H,evidence_fingerprint:H,import_id:id(21),hr_row_id:id(22),existing_row_id:`d10g_${H}`,counts_for_row_admission:false},
    queries_parent_witness:{item_id:`native-action:${H}`,parent_group_key:`d10g_${H}`,owner:'private.weekly_source_check_only_query_groups_v1',
      actor_user_id:actor.actor_user_id,permission_view:'ADMIN',import_id:id(21),preview_generation:1,state_version:2,preview_fingerprint:H,proof_hash:H}};
  delete i.native_source_witness;i.action.target.row_key=i.source_information_witness.existing_row_id;
  assert.equal(projectChecklistItem(i,actor).action.kind,'QUERIES');
  const reduced=copy(i);reduced.action=null;reduced.queries_parent_witness=null;
  assert.equal(projectChecklistItem(reduced,{...actor,permission_view:'PAY_READONLY'}),null);
  for(const mutate of [x=>{x.reason_code='WEEKLY_TIMESHEET_COMPLETING';},x=>{x.action.target.row_key='neighbour';},x=>{x.source_information_witness.counts_for_row_admission=true;}]){
    const bad=copy(i);mutate(bad);assert.throws(()=>projectChecklistItem(bad,actor));
  }
});
test('all new producer pages are managed declarations with owner-only helpers and no read-side write/lock/cycle creation',()=>{
  for(const name of ['wp_u3_native_page.sqlinc','wp_u3_native_process.sqlinc','wp_u3_native_tracker.sqlinc','wp_u3_native_checklist.sqlinc']){
    const source=read(name),parsed=scanManagedSql(exactAtomicBody(source));
    assert.equal(parsed.meta.length,0,name);assert.equal(parsed.statements.length,name==='wp_u3_native_checklist.sqlinc'?4:3,name);
    const body=source.split('$function$')[1],tokens=scanManagedSql(body).statements.flatMap(s=>s.tokens);
    for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','LOCK','NOTIFY','CALL','EXECUTE'])assert.ok(!tokens.includes(forbidden),`${name}:${forbidden}`);
    assert.doesNotMatch(body,/pg_advisory|for\s+(?:update|share)|create\s+(?:temp|table)|set_config|pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  }
});
test('single owner applies native subject/retention before keyset slice and actor-wide revision is independent of request traversal',()=>{
  const source=read('wp_u3_native_checklist.sqlinc'),page=read('wp_u3_native_page.sqlinc');
  assert.match(source,/private\.weekly_source_pay_actor_allowed_v1\(actor\.id\)/);
  assert.match(source,/if view='PAY_READONLY' and pay_count=0 then continue/);
  assert.match(source,/if own_week<>latest_week and completed then continue/);
  assert.doesNotMatch(source,/interval\s+'\d+\s+(?:day|week|month)|generate_series|uuid_generate/);
  const revision=source.slice(source.indexOf("revision:=encode("),source.indexOf("records:='[]'::jsonb"));
  assert.match(revision,/'native_facts',fingerprints,'rows',rows,'items',items/);assert.doesNotMatch(revision,/request|command|cursor|sort|limit/);
  assert.match(page,/r->'_key'=token->'after'/);assert.match(page,/token->>'read_revision' is distinct from p_revision/);
  assert.doesNotMatch(page,/\boffset\b(?!,)/i);assert.match(page,/case when direction='ASC'/);assert.match(page,/case when direction='DESC'/);
});
test('tracker expression is exactly the effective native 0044 owner expression, with a finite pure call graph',()=>{
  const predecessor=fs.readFileSync(new URL('../supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql',import.meta.url),'utf8').replaceAll('\r\n','\n');
  const marker="  select pg_catalog.jsonb_build_object(\n    'title','Finalisation progress'",start=predecessor.indexOf(marker),end=predecessor.indexOf('  ) into v_tracker;',start)+'  ) into v_tracker;'.length;
  assert.ok(start>=0);const expression=predecessor.slice(start,end);assert.ok(read('wp_u3_native_tracker.sqlinc').includes(expression));
  const source=read('wp_u3_native_checklist.sqlinc');assert.doesNotMatch(source,/public\.weekly_source_office_workspace_v1\(/);
  assert.match(source,/private\.weekly_source_office_tracker_v1\(scope\.cycle_id,actor\.id\)/);
});
test('process proof binds native task/context/receipt/manifests and does not call proposal completion or payment proof complete',()=>{
  const source=read('wp_u3_native_process.sqlinc');
  for(const marker of ['WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_V1','WEEKLY_SOURCE_FINALISATION_PAY_RUN_V1',
    'weekly_source_report_projection_receipt_assert_v1(receipt)','receipt.final_manifest_hash=revision.manifest_hash',
    'receipt.outcome=task.state',"task.state in('PROPOSED','TARGET_MANAGED_SUPPRESSED') then tasks_proved:=false"]){assert.ok(source.includes(marker),marker);}
  assert.doesNotMatch(source,/'paid'\s*,true|'holds_pay'\s*,false|withheld_amount_pence.*'0'/i);
  assert.match(read('wp_u3_native_checklist.sqlinc'),/private\.weekly_source_agency_invoice_check_only_v1\(t\.timesheet_id\) is true/);
  assert.match(read('wp_u3_native_checklist.sqlinc'),/agency_native\.validation_status is distinct from 'VALIDATION_OK'/);
});
test('submission-cycle writer stays outside reads and creates only genuine Client-scoped native lifecycle metadata after witness reproof',()=>{
  const prepare=read('wp_u3_submission_cycle_prepare.sqlinc'),dispatch=read('wp_u3_submission_cycle_dispatch.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(prepare)).statements.length,3);
  assert.equal(scanManagedSql(exactAtomicBody(dispatch)).statements.length,3);
  assert.match(prepare,/private\.weekly_source_actual_timesheet_submission_v1\(p_timesheet_id\)/);
  assert.match(prepare,/WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTUAL_SUBMISSION_REQUIRED/);
  const write=prepare.indexOf('insert into public.weekly_source_cycles');
  for(const marker of ['WEEKLY_SOURCE_SUBMISSION_CYCLE_SIGNED_SCOPE_REQUIRED','weekly_source_lock_family_rows_v1',
    'weekly-source-cycle-lifecycle:',"for update",'WEEKLY_SOURCE_SUBMISSION_CYCLE_GROUP_STALE',
    'WEEKLY_SOURCE_SUBMISSION_CYCLE_CLIENT_WEEK_UNPROVED'])assert.ok(prepare.indexOf(marker)<write,marker);
  assert.match(prepare,/values\(p_source_group_id,client_id,cycle_week,cutoff_at,'OPEN',0,'NONE'\)/);
  assert.doesNotMatch(prepare,/insert into (?:public\.(?:timesheets|timesheets_financials|mail_outbox)|private\.)/i);
  assert.doesNotMatch(prepare,/created_at_utc\s*[,=]|p_now_utc\s*(?:default|:|,)|interval '7 days'/);
  assert.doesNotMatch(read('wp_u3_native_checklist.sqlinc'),/weekly_source_checklist_submission_cycle_prepare_v1|ensure_open_cycle_v1/);
  const verify=fs.readFileSync(new URL('../supabase/verification/support/wp_u3_submission_cycle_prepare_vectors.sqlinc',import.meta.url),'utf8');
  for(const kind of ['VALID_VACANT','VALID_REPLAY','PLANNED','LOCAL_DRAFT','EXPENSE_ONLY','WAITING_P0_ONLY','INACTIVE_ACTOR','WRONG_MEMBERSHIP'])assert.ok(verify.includes(kind),kind);
  assert.match(verify,/NATIVE_FIXTURES_REQUIRED/);assert.match(verify,/expected_new_cycle_count/);
});
test('complete current Candidate owner successors preserve every native byte outside their finite lifecycle admission/hooks',()=>{
  for(const kind of ['transition','finalize']){
    const forward=read(`wp_u3_candidate_${kind}.forward.sqlinc`).split('\n').slice(1).join('\n');
    const reverse=read(`wp_u3_candidate_${kind}.reverse.sqlinc`).split('\n').slice(1).join('\n');
    assert.equal(reverse,currentOwner(kind));assert.equal(forward,forwardOwner(kind));
    let restored=forward;
    if(kind==='transition'){
      restored=restored.replace('\n  v_checklist_cycle_admission jsonb;','').replace(earlyTransitionHook,'').replace(transitionFrozenHook,'');
      assert.ok(forward.indexOf(earlyTransitionHook)<forward.indexOf('  if v_is_service_action then\n'));
      assert.ok(forward.indexOf(transitionFrozenHook)>forward.indexOf("state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'"));
      assert.ok(forward.indexOf(transitionFrozenHook)<forward.indexOf("elsif v_action='REGISTER_REVIEW_COMPONENT'"));
    }else{
      restored=restored.replace('\n  v_checklist_candidate_id uuid;','').replace(earlyFinalizeHook,'').replace(finalizedHook,'');
      assert.ok(forward.indexOf(earlyFinalizeHook)<forward.indexOf('select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update'));
      assert.ok(forward.indexOf(finalizedHook)>forward.indexOf("'CANDIDATE_WORKFLOW_FINALISATION_COMPLETED'"));
      assert.ok(forward.indexOf(finalizedHook)>forward.lastIndexOf('private._candidate_workflow_mutation_receipt_v1('));
      assert.match(forward,/weekly_source_checklist_submission_cycle_prepare_v1\(v_hours_timesheet_id,null\)/);
    }
    assert.equal(restored,currentOwner(kind),`${kind}: all native permissions, signatures, money, Bank and A0 hooks remain byte-identical`);
    assert.equal(scanManagedSql(exactAtomicBody(forward)).statements.length,4);
  }
});
test('early admission is mutation-free and respects native Daily null-week/actual owner before I1/F1 and later lifecycle G1',()=>{
  const source=read('wp_u3_submission_cycle_admit.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,3);
  const tokens=scanManagedSql(source.split('$function$')[1]).statements.flatMap(s=>s.tokens);
  for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','NOTIFY','EXECUTE'])assert.ok(!tokens.includes(forbidden),forbidden);
  assert.match(source,/workflow\.scope='DAILY' and workflow\.week_ending_date is null and workflow\.contract_week_id is null/);
  assert.match(source,/p\.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p\.self_bill_enabled/);
  assert.ok(source.indexOf('weekly_source_pay_query_admit_v2()')<source.indexOf('weekly_source_lock_family_rows_v1'));
  assert.doesNotMatch(source,/ensure_open_cycle|insert into|weekly-source-cycle-lifecycle:|p_now_utc/i);
});
test('submitted witness follows actual Daily and finalisation artifact ABI, with no generation or Client guess',()=>{
  const source=read('wp_u3_submission_context.sqlinc');
  assert.match(source,/t\.sheet_scope::text='DAILY' and \(w\.week_ending_date is not null or w\.contract_week_id is not null\)/);
  assert.match(source,/object_type='candidate_workflow_finalisation_completion'/);
  assert.match(source,/a\.before_json->>'workflow_generation'=\(w\.generation-1\)::text/);
  assert.match(source,/a\.after_json->>'canonical_financial_sha256_hex'=encode\(w\.canonical_financial_sha256,'hex'\)/);
  assert.match(source,/component\.workflow_generation=v_artifact_generation/);
  assert.match(source,/NATIVE_ARTIFACT_GENERATION_MISMATCH/);
  assert.match(source,/t\.candidate_workflow_generation>v_artifact_generation/);
  assert.match(source,/canonical_result,hours_timesheet_id/);
  assert.match(source,/NULL_CONTRACT_DAILY_OWNER_UNAVAILABLE/);
  assert.doesNotMatch(source,/hospital_norm.*client|candidate_hint_text.*client|w\.generation-1.*(?:then true|coalesce)/);
  const fixture=fs.readFileSync(new URL('../supabase/verification/support/wp_u3_submission_witness_vectors.sqlinc',import.meta.url),'utf8');
  for(const name of ['DAILY_NULL_WORKFLOW_WEEK','FINALISED_ARTIFACT_GENERATION','COMBINED_SEPARATE_EXPENSE',
    'PENDING_RESUBMITTED_ARTIFACT','UNRESOLVED_DAILY_CLIENT','FALSIFIED_FINALISATION_RECEIPT','FOREIGN_ARTIFACT_GENERATION',
    'PLANNED','EXPENSE_ONLY','WAITING_P0_ONLY'])assert.ok(fixture.includes(name),name);
  assert.ok(fixture.includes('NATIVE_FIXTURES_REQUIRED'));
});
test('native hold adapter delegates exact admitted owners, certifies only no-subject advisory zero, and preserves unknown monetary quantities',()=>{
  const source=read('wp_u3_native_hold_facts.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,6);
  for(const body of source.split('$function$').filter((_,i)=>i%2)){
    const tokens=scanManagedSql(body).statements.flatMap(s=>s.tokens);
    for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','LOCK','NOTIFY','CALL','EXECUTE'])assert.ok(!tokens.includes(forbidden),forbidden);
    assert.doesNotMatch(body,/pg_advisory|for\s+(?:update|share)|create\s+(?:temp|table)|set_config/i);
  }
  for(const marker of ['private.weekly_source_pay_query_scope_v1(t.timesheet_id)','private.weekly_source_pay_query_facts_v1(t.timesheet_id)',
    'private.weekly_source_first_authorisation_context_v1(t.timesheet_id)','private.bpay_next_approval_route_v1(t.timesheet_id)',
    "d.action_kind='ADVISORY' and d.summary_json->>'authority_mode'='VALIDATION_ONLY'",
    'd.timesheet_id is null and d.is_current and not d.blocking',"'financial_subject','NONE'",
    "'withheld_amount_pence','0'",'NATIVE_WITHHELD_QUANTITY_CERTIFICATE_REQUIRED'])assert.ok(source.includes(marker),marker);
  assert.doesNotMatch(source,/public\.bpay_next_|paid_at_utc|held_source_ex_vat|total_pay_ex_vat|sum\(|d\.id\b/);
  const producer=read('wp_u3_native_checklist.sqlinc');
  assert.match(producer,/'native_financial_facts',financial_facts/);
  const sqlUnknown=JSON.parse(producer.match(/unknown_hold constant jsonb:='([^']+)'/)[1]);
  assert.deepEqual(sqlUnknown,unavailableChecklistHold());assert.deepEqual(decodeChecklistHold(sqlUnknown),sqlUnknown);
  for(const owner of ['SOURCE','NEXT','ORDINARY']){
    const unknown={...copy(sqlUnknown),authority:{owner,hash:H,withholding_known:false}};
    assert.deepEqual(decodeChecklistHold(unknown),unknown);
    const invented=copy(unknown);invented.withheld_amount_pence='0';assert.throws(()=>decodeChecklistHold(invented));
  }
  const noHold={holds_pay:false,holds_pay_scope:null,withheld_amount_pence:'0',holds_invoice:false,pending_decision:false,
    authority:{owner:'SOURCE',hash:H,withholding_known:true}};
  assert.deepEqual(decodeChecklistHold(noHold),noHold);
  const leaked=copy(noHold);leaked.authority.withholding_known=false;assert.throws(()=>decodeChecklistHold(leaked));
});
test('literal native Source parent/child census reaches the actual exact-target predicate while child-as-parent and foreign scopes fail',()=>{
  const root=new URL('../supabase/repeatable/',import.meta.url);
  const workspace=fs.readFileSync(new URL('13102026_1344_weekly_source_self_bill_report/13102026_1344_office_workspace.sqlinc',root),'utf8');
  const owner=fs.readFileSync(new URL('13102026_1438_weekly_source_protection_queries/22_query_groups.sqlinc',root),'utf8');
  assert.ok(workspace.includes('query.group_key=v_query_row_key and query.client_id=v_client_id'));
  assert.ok(owner.includes('named.group_key'));assert.ok(owner.includes("'row_key','manual-'||manual.id::text"));
  const producer=read('wp_u3_native_checklist.sqlinc');
  assert.ok(producer.includes('q.group_key parent_group_key'));assert.ok(producer.includes("'parent_group_key',source_native.parent_group_key"));
  assert.ok(producer.includes("jsonb_build_object('row_key',source_native.parent_group_key)"));
  const item=nativeItem('manual'),parent={group_key:item.native_source_witness.parent_group_key,client_id:scope.client_id,children:[{row_key:item.native_source_witness.native_row_key}]};
  const select=target=>[parent].filter(q=>q.group_key===target.row_key&&q.client_id===target.client_id);
  assert.equal(select(projectChecklistItem(item,actor).action.target).length,1);
  assert.equal(select({...item.action.target,row_key:item.native_source_witness.native_row_key}).length,0);
  assert.equal(select({...item.action.target,client_id:id(100)}).length,0);
  const wrong=copy(item);wrong.action.target.row_key=`qg_${'b'.repeat(64)}`;assert.throws(()=>projectChecklistItem(wrong,actor));
});
test('one check-only/D10 parent owner reuses current native decisions/CAS without publication, send/store or Source bulk action',()=>{
  const groups=read('wp_u3_check_only_query_groups.sqlinc'),raw=read('wp_u3_check_only_query_rows.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(groups)).statements.length,3);
  const tokens=scanManagedSql(groups.split('$function$')[1]).statements.flatMap(s=>s.tokens);
  for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','LOCK','NOTIFY','CALL','EXECUTE'])assert.ok(!tokens.includes(forbidden),forbidden);
  for(const marker of ['private.weekly_source_check_only_query_rows_v1(cycle.id,p_client_id',"parent_key:='d10g_'",
    'WEEKLY_SOURCE_CHECK_ONLY_QUERY_PARENT_V1','state.preview_generation','state.state_version','state.preview_fingerprint',
    "latest_decision.action_id",'WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE',"'source_outreach_eligible',false", "'bulk_actions','{}'::jsonb"]){assert.ok(groups.includes(marker),marker);}
  assert.doesNotMatch(groups,/current_projection_publication_id|weekly_source_office_query_groups_v1|ASK_CANDIDATES|SEND_MANAGER_NOW|REMIND_CANDIDATE|canonical_financial|submission_witness|holds_pay/);
  assert.ok(raw.includes("d.action_kind in('EMAIL_ISSUE','EMAIL_REMINDER')"));
  assert.doesNotMatch(raw,/EMAIL_WEEKLY_ISSUES|EMAIL_DAILY_ISSUES|EMAIL_MISSING_FROM_IMPORT/);
  assert.ok(raw.includes("route_state','NATIVE_AGENCY_INVOICE'"));assert.ok(raw.includes("c.source_group_id,p_client_id,v_week"));
});
test('D10 Queries target requires exact returned parent and fresh actor/import/CAS proof before crossing the DTO',()=>{
  const i=nativeItem('missing');delete i.native_source_witness;
  Object.assign(i,{item_kind:'ACTUAL_TIMESHEET',item_id:`timesheet:${id(30)}`,counted_timesheet_id:id(30),route:'SIGNED_TIMESHEETS',
    queries_parent_witness:{item_id:`timesheet:${id(30)}`,parent_group_key:`d10g_${H}`,owner:'private.weekly_source_check_only_query_groups_v1',
      actor_user_id:actor.actor_user_id,permission_view:'ADMIN',import_id:id(31),preview_generation:2,state_version:4,preview_fingerprint:H,proof_hash:H}});
  i.action.target.row_key=i.queries_parent_witness.parent_group_key;
  assert.equal(projectChecklistItem(i,actor).action.target.row_key,`d10g_${H}`);
  for(const mutate of [x=>{x.action.target.row_key=x.item_id;},x=>{x.queries_parent_witness.actor_user_id=id(32);},
    x=>{x.queries_parent_witness.permission_view='PAY_READONLY';},x=>{x.queries_parent_witness.preview_fingerprint='changed';},
    x=>{x.queries_parent_witness.state_version=-1;},x=>{x.queries_parent_witness.owner='other.owner';},x=>{delete x.queries_parent_witness;}]){
    const bad=copy(i);mutate(bad);assert.throws(()=>projectChecklistItem(bad,actor));
  }
});

test('new Source and D10 witness hashes reject coerced arrays through the actual full closed read decoder',()=>{
  const source=nativeItem('incident'),actual=nativeItem('missing');delete actual.native_source_witness;
  Object.assign(actual,{item_kind:'ACTUAL_TIMESHEET',item_id:`timesheet:${id(30)}`,counted_timesheet_id:id(30),route:'SIGNED_TIMESHEETS',
    queries_parent_witness:{item_id:`timesheet:${id(30)}`,parent_group_key:`d10g_${H}`,owner:'private.weekly_source_check_only_query_groups_v1',
      actor_user_id:actor.actor_user_id,permission_view:'ADMIN',import_id:id(31),preview_generation:2,state_version:4,preview_fingerprint:H,proof_hash:H}});
  actual.action.target.row_key=actual.queries_parent_witness.parent_group_key;
  const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
  const page=item=>({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:'ADMIN',
    binding:H,read_revision:H,rows:[],items:[item],has_more:false,next_cursor:null,refresh_required:false,anchor:null});
  for(const [item,witness,key] of [[source,'native_source_witness','evidence_fingerprint'],[actual,'queries_parent_witness','preview_fingerprint'],
    [actual,'queries_parent_witness','proof_hash']]){
    assert.equal(decodeChecklistRead(page(item),request,{id:actor.actor_user_id,payView:false}).items.length,1);
    for(const invalid of [[H],[],[H,H],null,123,{value:H}]){
      const bad=copy(item);bad[witness][key]=invalid;
      assert.throws(()=>projectChecklistItem(bad,actor));
      assert.throws(()=>decodeChecklistRead(page(bad),request,{id:actor.actor_user_id,payView:false}),
        error=>error.code==='WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID');
    }
  }
  const badParent=copy(actual);badParent.queries_parent_witness.parent_group_key=[`d10g_${H}`];
  assert.throws(()=>decodeChecklistRead(page(badParent),request,{id:actor.actor_user_id,payView:false}));
});

test('retention reads the one raw open fact before old-week admission, using real native email and unfinished Daily evidence',()=>{
  const leaf=read('wp_u3_client_week_context.sqlinc'),raw=read('wp_u3_check_only_query_rows.sqlinc'),producer=read('wp_u3_native_checklist.sqlinc');
  const native=fs.readFileSync(new URL('../supabase/repeatable/13102026_1205_healthroster_issued_signed_query_v1.sql',import.meta.url),'utf8');
  for(const kind of ['EMAIL_ISSUE','EMAIL_REMINDER']){
    assert.ok(native.includes(`'${kind}'`));assert.ok(raw.includes(`'${kind}'`));
  }
  assert.doesNotMatch(leaf,/EMAIL_WEEKLY_ISSUES|EMAIL_DAILY_ISSUES|EMAIL_MISSING_FROM_IMPORT|from public\.import_review_decisions/);
  const fact=raw.slice(raw.indexOf("'open_read_item',x.request_id"),raw.indexOf("'engineering_gate',",raw.indexOf("'open_read_item',x.request_id")));
  for(const pin of ['x.request_id is not null','SUBMITTED_PENDING_NATIVE_MATERIALISATION','x.authorised_at_server is null',
    'review.unfinished_daily_review',"jsonb_array_length(coalesce(issues.value,'[]'::jsonb))>0",'VALIDATION_OK','READY_FOR_HR'])assert.ok(fact.includes(pin),pin);
  assert.ok(raw.includes("'counts_for_row_admission',false,'open_read_item',false"));
  assert.ok(leaf.indexOf('v_page:=private.weekly_source_check_only_query_rows_v1(')<leaf.indexOf("if v_route='SIGNED_TIMESHEETS' and (v_count=0"));
  assert.match(leaf,/r->>'item_kind'='ACTUAL_TIMESHEET' and r#>'\{submission_witness,actual_submitted\}'='true'::jsonb/);
  assert.match(leaf,/v_scope_count>0 and v_scope_open>0 and v_read_scope\.week_ending_date<v_latest_week/);
  assert.match(leaf,/preferred\.scope_client_id=v_client/);assert.match(leaf,/v_revision is distinct from v_page->>'read_revision'/);
  assert.doesNotMatch(leaf,/generate_series|interval '\d+ (?:day|week|month)|insert into/i);
  assert.ok(producer.includes("d10_groups->>'source_read_revision' is distinct from leaf->>'native_query_read_revision'"));
  assert.ok(producer.includes("if raw->'open_read_item'='false'::jsonb then continue; end if;"));
  // These are native raw-frame boundary vectors, not SQL execution or a new
  // discrepancy classifier. Open truth is supplied by the existing raw owner.
  const actualRow=(open,extra={})=>({item_kind:'ACTUAL_TIMESHEET',submission_witness:{actual_submitted:true},open_read_item:open,...extra});
  const information={item_kind:'SOURCE_INFORMATION',counts_for_row_admission:false,open_read_item:false};
  const retention=(rows,latest)=>{
    const counted=rows.filter(row=>row.item_kind==='ACTUAL_TIMESHEET'&&row.submission_witness.actual_submitted===true);
    const open=counted.filter(row=>row.open_read_item===true).length;
    return {admitted:counted.length>0&&(latest||open>0),completed:open===0,actual:counted.length,open};
  };
  for(const kind of ['EMAIL_ISSUE','EMAIL_REMINDER'])assert.deepEqual(retention([actualRow(true,{native_issues:[{action_kind:kind}]})],false),{admitted:true,completed:false,actual:1,open:1});
  assert.equal(retention([actualRow(true,{unfinished_daily_review:true})],false).admitted,true);
  assert.equal(retention([actualRow(true,{correction_owns_read_item:true})],false).admitted,true);
  assert.equal(retention([actualRow(true,{submission_read_context:{route_state:'SUBMITTED_PENDING_NATIVE_MATERIALISATION'}})],false).admitted,true);
  assert.deepEqual(retention([actualRow(false),information],false),{admitted:false,completed:true,actual:1,open:0});
  assert.equal(retention([actualRow(false),information],true).admitted,true);
  assert.equal(retention([information],true).admitted,false);
  assert.equal(retention([{...actualRow(true),submission_witness:{actual_submitted:false}}],false).admitted,false);
});

test('retention native acceptance vectors require real old-week query/preview fixtures, with no absent-case or source-only qualification',()=>{
  const verifier=fs.readFileSync(new URL('../supabase/verification/support/wp_u3_retention_vectors.sqlinc',import.meta.url),'utf8');
  assert.equal(scanManagedSql(exactAtomicBody(verifier)).statements.length,1);
  for(const kind of ['OLDER_EMAIL_ISSUE','OLDER_EMAIL_REMINDER','OLDER_DAILY_PREVIEW','OLDER_PENDING_DAILY','OLDER_CORRECTION',
    'OLDER_CLOSED','OLDER_CLOSED_R254','LATEST_CLOSED','NO_ACTUAL_TIMESHEET'])assert.ok(verifier.includes(kind),kind);
  for(const owner of ['private.weekly_source_check_only_query_rows_v1','private.weekly_source_healthroster_checklist_leaf_v1',
    'public.weekly_source_checklist_v1'])assert.ok(verifier.includes(owner),owner);
  assert.match(verifier,/RETENTION_NATIVE_FIXTURES_REQUIRED/);assert.match(verifier,/RETENTION_NATIVE_FIXTURE_CASE_REQUIRED/);
});
