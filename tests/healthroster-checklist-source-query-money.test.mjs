import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {financialNativeCore} from '../docs/healthroster-build/proposals/checklist-native-20261014/build-financial-kernel-packet.mjs';
import {sqlTokens,functionEvents} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {decodeChecklistHold,projectChecklistItem,projectChecklistRow,unavailableChecklistHold} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../broker/src/weekly-source/checklist-read.mjs';
const packet=new URL('../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/',import.meta.url);
const sql=new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/',import.meta.url);
const read=p=>fs.readFileSync(new URL(p,packet),'utf8'),leaf=p=>fs.readFileSync(new URL(p,sql),'utf8');
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const pins=JSON.parse(read('NATIVE_KERNEL_INPUT_PINS.json')),graph=JSON.parse(read('NATIVE_READ_CALL_GRAPH.json'));
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64);
const scope={source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),week_ending:'2026-10-11'};
const actor={actor_user_id:id(4),permission_view:'ADMIN',active:true};
const hold=amount=>({holds_pay:true,holds_pay_scope:'SHIFT',withheld_amount_pence:amount,holds_invoice:null,pending_decision:null,
  authority:{owner:'SOURCE_QUERY_EX_VAT',hash:H,withholding_known:true}});
function item(){return {item_id:`native-source:incident-${id(7)}`,item_kind:'NATIVE_SOURCE',scope,route:'SELF_BILL',worker:'Worker',
  work_date:'2026-10-07',sheet_scope:'WEEKLY',reason_code:'SOURCE_HOURS_DIFFER',status:{text:'Needs action',tone:'warning',subtext:'Native reason',subtone:'muted'},
  hold:hold('12345'),subject:'PAY',permitted_pay_subject:true,counted_timesheet_id:null,correction_binding:null,submission_read:null,source_information_witness:null,
  action:{kind:'QUERIES',label:'View item in Queries ›',native_allowed:true,target:{source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),row_key:`qg_${H}`}},
  native_source_witness:{owner:'private.weekly_source_office_query_groups_v1',projection_publication_id:id(5),work_event_id:id(6),upload_row_id:null,
    row_resolution_id:null,incident_id:id(7),comparison_revision_id:id(8),manual_review_id:null,native_row_key:`incident-${id(7)}`,parent_group_key:`qg_${H}`,evidence_fingerprint:H}};}
const request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
const page=i=>({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:'ADMIN',
  binding:H,read_revision:H,rows:[],items:[i],has_more:false,next_cursor:null,refresh_required:false,anchor:null});
const copy=x=>structuredClone(x);
test('single native qualification kernel has exact mechanical full reverses and preserves scalar/table/void native ABI',()=>{
  const forward=leaf('wp_u3_financial_native_kernel.forward.sqlinc'),reverse=leaf('wp_u3_financial_native_kernel.reverse.sqlinc');
  assert.equal(hash(forward),pins.forward_sha256);assert.equal(hash(reverse),pins.reverse_sha256);
  assert.equal(scanManagedSql(forward).statements.length,24);assert.equal(scanManagedSql(reverse).statements.length,16);
  const definitions=new Map(functionEvents(forward).events.map(e=>[e.name,e.definition]));
  let locks=0;
  for(let n=0;n<pins.selected.length;n++){
    const selected=pins.selected[n],transform=pins.transformations[n],before=read('native/'+selected.name.replace('.','_')+'.sqlinc');
    assert.equal(hash(before),selected.definition_sha256);
    const nameChange=transform.changes[0],argChange=transform.changes[1];
    const lockChanges=transform.changes.filter(c=>c.after.startsWith('if p_native_locks then'));
    const calls=transform.changes.filter(c=>!c.after.startsWith('if p_native_locks then')&&c!==nameChange&&c!==argChange&&!c.before.startsWith('begin')).map(c=>[c.before,c.after]);
    const names=['p_binding_version_id','p_expected_book_revision'];
    const args=selected.name.includes('bound_source_snapshot')?names:selected.name.includes('work_members')?['p_work_id','p_revision_id']:
      selected.name.includes('bound_assert')?['p_binding_version_id']:['p_root_authorisation_id'];
    const rebuilt=financialNativeCore(before,{nativeName:selected.name,coreName:transform.coreName,args,lockStatements:lockChanges.map(c=>c.before),calls});
    assert.equal(definitions.get(selected.name),rebuilt.wrapper);assert.equal(definitions.get(transform.coreName),rebuilt.core);
    let restored=rebuilt.core;for(const c of [...transform.changes].reverse())restored=restored.replace(c.after,c.before);assert.equal(restored,before);
    assert.ok(reverse.includes(before));locks+=lockChanges.length;
    const currentFile=execFileSync('git',['show',pins.sourceCommit+':'+selected.file],{encoding:'utf8',maxBuffer:2e6}).replaceAll('\r\n','\n');
    assert.equal(hash(execFileSync('git',['show',pins.sourceCommit+':'+selected.file],{maxBuffer:2e6})),selected.source_file_sha256);
    const nativeAcl=scanManagedSql(currentFile).statements.map(s=>currentFile.slice(s.start,s.end).replaceAll(/\s/g,''));
    assert.ok(nativeAcl.some(s=>s.startsWith('revokeallonfunction')&&s.includes(`${selected.name}(${selected.inputTypes})`)
      &&s.endsWith('frompublic,anon,authenticated,service_role;')),selected.name);
    assert.ok(forward.includes(`revoke all on function ${transform.coreName}(${selected.inputTypes},boolean) from public,anon,authenticated,service_role;`));
    assert.ok(rebuilt.wrapper.includes(','+'true);'));
    if(/returns jsonb/.test(before)) {assert.match(rebuilt.wrapper,/return private\.weekly_source_checklist_native_initial_certificate_core_v1\(p_root_authorisation_id,true\);/);assert.doesNotMatch(rebuilt.wrapper,/return query/);}
  }
  assert.equal(locks,5);assert.equal(pins.canonicalBankBodyEdited,false);assert.equal(pins.nativeExecuted,false);
});
test('complete conservative source read graph retains all native qualifications and admits only literal read-mode lock seams',()=>{
  assert.equal(graph.nativeExecuted,false);assert.equal(graph.public_Bank_wrapper_used,false);assert.equal(graph.refresh_mutator_used,false);
  for(const r of graph.routines){const definition=read(r.file);assert.equal(hash(definition),r.definition_sha256);
    assert.ok(!/public\.bpay_next_|workbench_refresh|pg_advisory|for update|create temp/i.test(definition),r.id);
    for(const token of r.conservative_direct_write_or_lock_tokens)assert.equal(token,'execute',`${r.id}: ${token}`);
    if(r.direct_row_locks)assert.ok(['private.bpay_next_bound_source_snapshot_v1','private.bpay_next_initial_tsfin_certificate_v1'].includes(r.name));
    if(r.conservative_direct_write_or_lock_tokens.includes('execute')){
      const t=sqlTokens(definition.split('$function$')[1]),exec=t.filter(x=>x.kind==='word'&&x.value==='execute');assert.equal(exec.length,1);
      const index=t.indexOf(exec[0]);assert.equal(t[index+1].kind,'string');assert.match(t[index+1].value,/^select count\(\*\),jsonb_agg/);
      const dynamicTokens=sqlTokens(t[index+1].value).filter(t=>t.kind==='word').map(t=>t.value);
      for(const forbidden of ['insert','update','delete','lock','create','execute','call'])assert.ok(!dynamicTokens.includes(forbidden));
    }
    assert.ok(r.qualified_names_without_source.every(n=>n==='extensions.digest'));
    assert.ok(r.callees.every(id=>graph.routines.some(c=>c.id===id)));
  }
  const extraction=leaf('wp_u3_financial_native_kernel.forward.sqlinc');
  assert.equal((extraction.match(/if p_native_locks then/g)||[]).length,5);
  for(const transform of pins.transformations)for(const change of transform.changes.filter(c=>c.after.startsWith('if p_native_locks then'))){
    const falseStatement=change.after.split('\n  else\n  ')[1].split('\n  end if;')[0];
    assert.equal(falseStatement,change.before.replace(/ for share;$/i,';'));assert.doesNotMatch(falseStatement,/for share/i);
  }
  assert.match(extraction,/initial_bound_assert_core_v1\(v_version.id,p_native_locks\)/);
  assert.match(extraction,/initial_certificate_core_v1\(v.initial_root_authorisation_id,p_native_locks\)/);
  assert.match(extraction,/bound_snapshot_core_v1\(v.id,p.source_book_revision,p_native_locks\)/);
});
test('finite Source adapter reads original positive money, full membership/current proof and exact accepted-cut exclusion owner',()=>{
  const adapter=leaf('wp_u3_source_shift_query_money.sqlinc'),native=read('money30/input/private_bpay_next_workbench_refresh_work_core_v1.sqlinc');
  assert.equal(scanManagedSql(adapter).statements.length,3);
  const body=adapter.split('$function$')[1],tokens=sqlTokens(body).filter(t=>t.kind==='word').map(t=>t.value);
  for(const forbidden of ['insert','update','delete','lock','create','execute','notify','call','set_config','greatest','round'])assert.ok(!tokens.includes(forbidden),forbidden);
  assert.doesNotMatch(body,/for\s+(?:update|share)|pg_advisory|public\.bpay_next_|workbench_refresh|private\.bpay_next_bound_source_snapshot_v1/);
  assert.ok(native.includes('v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive'));
  assert.ok(native.includes("v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION'"));
  for(const marker of ['VIEW_PAY_WORK','private.weekly_source_checklist_native_members_core_v1(work.id,revision.id,false)',
    'private.bpay_next_source_query_segment_exclusions_v1(revision.id,gate)','totals.split_residual','actual_member_count=0','having count(*)<>1',
    'work.applied_revision_id is distinct from revision.id','revision.expected_line_count','metadata.continuation_pending is distinct from false',
    'i.projection_revision is distinct from group_row.projection_revision','i.financial_view_revision is distinct from worker_revision',
    'i.accepted_run_id is null and i.accepted_command_id is null','current_frame_after is distinct from current_frame','members_after is distinct from members',
    'pence:=source_amount*100',"'owner','SOURCE_QUERY_EX_VAT'","'holds_invoice',null,'pending_decision',null"]){assert.ok(body.includes(marker),marker);}
  assert.ok(body.indexOf('VIEW_PAY_WORK')<body.indexOf('private.bpay_next_workbench_item'));
  assert.doesNotMatch(body,/paid\s*(?:=|:=)\s*true|total_pay_ex_vat|native_held_.*as.*withheld|limit \d/i);
  const current=JSON.parse(read('money30/CURRENT_NATIVE_SOURCE_PINS.json'));
  assert.ok(JSON.stringify(current).includes('615f734caee9d73872deabc50685a82b1225997f17defad8634e3a35da194a47'));
});
test('real whole Checklist decoder preserves explicit SOURCE EX-VAT shift pence for Admin and text-only PAY without fake counted Timesheet',()=>{
  for(const amount of ['1','12345','999999999999999999']){
    const i=item();i.hold=hold(amount);assert.deepEqual(decodeChecklistHold(i.hold),i.hold);
    const admin=decodeChecklistRead(page(i),request,{id:actor.actor_user_id,payView:false});assert.equal(admin.items[0].hold.withheld_amount_pence,amount);
    assert.equal(admin.items[0].counted_timesheet_id,null);assert.equal(admin.items[0].hold.authority.owner,'SOURCE_QUERY_EX_VAT');
    const pay=page(i);pay.permission_view='PAY_READONLY';pay.items[0].action=null;
    const result=decodeChecklistRead(pay,request,{id:actor.actor_user_id,payView:true});assert.equal(result.items[0].action,null);
    assert.equal(result.items[0].hold.holds_invoice,null);assert.equal(result.items[0].hold.pending_decision,null);
  }
});
test('new money discriminant refuses arrays, rounding/overflow/zero, whole-TS, invented invoice/decision and non-Source subject',()=>{
  const mutations=[h=>{h.withheld_amount_pence='0';},h=>{h.withheld_amount_pence='1.5';},h=>{h.withheld_amount_pence='1000000000000000000';},
    h=>{h.withheld_amount_pence=12345;},h=>{h.withheld_amount_pence=['12345'];},h=>{h.authority.hash=[H];},h=>{h.authority.hash=null;},
    h=>{h.authority.withholding_known=false;},h=>{h.holds_pay=false;},h=>{h.holds_pay=null;},h=>{h.holds_pay_scope='TIMESHEET';},
    h=>{h.holds_invoice=false;},h=>{h.holds_invoice=true;},h=>{h.pending_decision=false;},h=>{h.pending_decision=true;},
    h=>{h.amount_basis='TARGET_INC_VAT';}];
  for(const mutate of mutations){const i=item();mutate(i.hold);assert.throws(()=>decodeChecklistHold(i.hold));assert.throws(()=>decodeChecklistRead(page(i),request,{id:actor.actor_user_id,payView:false}));}
  for(const mutate of [i=>{i.subject='INFORMATION';},i=>{i.permitted_pay_subject=false;},i=>{i.item_kind='ACTUAL_TIMESHEET';i.counted_timesheet_id=id(9);delete i.native_source_witness;}]){
    const i=item();mutate(i);assert.throws(()=>projectChecklistItem(i,actor));
  }
  const c={value:true,status:{text:'Ready',tone:'positive',subtext:'',subtone:'positive'},item_count:1,link:null};
  const row={row_id:'row',scope,client:'Client',group:'Source',route:'SELF_BILL',retention:{actual_timesheet_count:0,most_recent_ended_week:'2026-10-11',has_open_items:true,earlier_open_weeks:0},
    checked:c,completed:c,pay_completed:c,hold:hold('12345'),permitted_pay_item_count:1,admin_actions:[]};
  assert.throws(()=>projectChecklistRow(row,actor));
  assert.deepEqual(decodeChecklistHold(unavailableChecklistHold()),unavailableChecklistHold());
});
test('producer integrates through genuine existing native lineage before PAY filtering while row aggregate remains unknown',()=>{
  const producer=leaf('wp_u3_native_checklist.sqlinc'),start=producer.indexOf('if native_roots=1 then'),call=producer.indexOf('private.weekly_source_checklist_source_shift_query_money_v1(',start);
  assert.ok(start>producer.indexOf('from public.weekly_source_row_timesheet_lineages l'));assert.ok(call>start);
  const arm=producer.slice(start,producer.indexOf('if pay_subject then pay_count:=pay_count+1;',start));
  assert.doesNotMatch(arm,/actual_timesheet_submission|counted_timesheet_id|sum\(/);
  assert.ok(arm.includes("financial_facts:=financial_facts||jsonb_build_array(source_money->'native_facts')"));
  assert.ok(arm.includes("if source_money->'known'='true'::jsonb then item_hold:=source_money->'hold';pay_subject:=true; end if;"));
  assert.ok(call<producer.indexOf("if view='PAY_READONLY' and pay_subject is distinct from true then continue; end if;",start));
  assert.ok(producer.includes("'native_financial_facts',financial_facts"));assert.ok(producer.includes("'hold',unknown_hold"));
});
test('native vectors cannot qualify a missing case or a fake DTO financial frame',()=>{
  const source=fs.readFileSync(new URL('../supabase/verification/support/wp_u3_source_query_money_vectors.sqlinc',import.meta.url),'utf8');
  assert.equal(scanManagedSql(source).statements.length,1);
  for(const kind of ['SOURCE_SHIFT_POSITIVE','SOURCE_SHIFT_RESERVED_SPLIT','SOURCE_INITIAL_PUBLICATION_PROOF','SOURCE_BUNDLE_PUBLICATION_PROOF',
    'SOURCE_TREATMENT_REVISION','DIRTY_GROUP','WRONG_ITEM_REVISION','STALE_WORKER_CLOCK','PENDING_METADATA','INCOMPLETE_ATTRIBUTION','FOREIGN_MEMBER',
    'MISSING_MEMBER','ACCEPTED_IN_PREPARATION','PAY_ON_HOLD','RESERVED_ONLY','RECOVERY_ONLY','SNOOZED','QUERY_CLEAR','FOREIGN_SEGMENT',
    'FAKE_READY_NO_PUBLICATION','WRONG_BOOK_REVISION','WRONG_CLIENT_SCOPE','INACTIVE_ACTOR','NO_PAY_PERMISSION','LEGACY_OWNER','ORDINARY_WORK',
    'MINOR_UNIT_OVERFLOW'])assert.ok(source.includes(`'${kind}'`),kind);
  assert.match(source,/CHECKLIST_QUERY_MONEY_NATIVE_FIXTURES_REQUIRED/);assert.match(source,/CHECKLIST_QUERY_MONEY_NATIVE_CASE_REQUIRED/);
  assert.match(source,/private.weekly_source_checklist_source_shift_query_money_v1/);
  assert.doesNotMatch(source,/create table|insert into|set_config|update (?:public|private)\./i);
});
test('actual NEXT dirtying/refresh hooks are pinned as writer dependencies and never enter the GET graph',()=>{
  const boundary=JSON.parse(read('NATIVE_FRESHNESS_HOOK_BOUNDARY.json'));
  assert.equal(boundary.not_in_GET_graph,true);assert.equal(boundary.legacy_mark_candidate_dirty_is_not_NEXT_projection_coverage,true);
  assert.equal(boundary.nativeExecuted,false);assert.equal(boundary.hooks.length,2);
  for(const hook of boundary.hooks){const body=read(hook.file);assert.equal(hash(body),hook.definition_sha256);
    assert.ok(!graph.routines.some(r=>r.id===hook.id));assert.ok(hook.callers.length>0);
    if(hook.id.includes('source_query_refresh')){assert.match(body,/work_kind='SOURCE'/);assert.match(body,/private.bpay_next_workbench_refresh_work_v1\(v_id\)/);}
    else {assert.match(body,/complete=false,dirty=true/);assert.match(body,/for update/);}
  }
});
