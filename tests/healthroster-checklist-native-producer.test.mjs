import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {projectChecklistItem,projectChecklistRow,unavailableChecklistHold} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../broker/src/weekly-source/checklist-read.mjs';
import {scanManagedSql,exactAtomicBody} from '../scripts/cloudtms-managed-release-sql.mjs';
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
    action:{kind:'QUERIES',label:'View item in Queries ›',native_allowed:true,target:{...scope,row_key:key}},
    native_source_witness:{owner:'private.weekly_source_office_query_groups_v1',projection_publication_id:id(5),work_event_id:id(6),
      upload_row_id:kind!=='incident'?id(9):null,row_resolution_id:kind==='missing'?id(10):null,incident_id:kind==='incident'?id(7):null,
      comparison_revision_id:kind==='incident'?id(11):null,manual_review_id:kind==='manual'?id(8):null,native_row_key:key,evidence_fingerprint:H}};
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
    x=>{x.action.target.row_key=`incident-${id(20)}`;},x=>{x.action.target.client_id=id(20);},
    x=>{x.action={kind:'TIMESHEET',label:'Open',native_allowed:true,target:{timesheet_id:id(20)}};},
    x=>{x.correction_binding={};},x=>{x.native_source_witness.evidence_fingerprint='unproved';}]){
    const item=nativeItem('incident');mutate(item);assert.throws(()=>projectChecklistItem(item,actor));
  }
});
test('R254 native information permits only its exact existing Queries identity and never creates a parent',()=>{
  const i={...nativeItem('missing'),item_kind:'SOURCE_INFORMATION',item_id:`native-action:${H}`,route:'SIGNED_TIMESHEETS',
    reason_code:'WEEKLY_TIMESHEET_NOT_SUBMITTED',subject:'INFORMATION',permitted_pay_subject:false,native_source_witness:undefined,
    source_information_witness:{native_action_id:H,evidence_fingerprint:H,import_id:id(21),hr_row_id:id(22),existing_row_id:`native-action:${H}`,counts_for_row_admission:false}};
  delete i.native_source_witness;i.action.target.row_key=i.source_information_witness.existing_row_id;
  assert.equal(projectChecklistItem(i,actor).action.kind,'QUERIES');
  assert.equal(projectChecklistItem(i,{...actor,permission_view:'PAY_READONLY'}),null);
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
