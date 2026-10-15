import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {decodeChecklistHold,projectChecklistItem,projectChecklistRow} from '../docs/healthroster-build/proposals/checklist-native-ordinary-next-hold-20261015/forward/healthroster-checklist-source.mjs';
import {decodeChecklistRead} from '../docs/healthroster-build/proposals/checklist-native-ordinary-next-hold-20261015/forward/checklist-read.mjs';
const base=new URL('../docs/healthroster-build/proposals/checklist-native-ordinary-next-hold-20261015/',import.meta.url),read=p=>fs.readFileSync(new URL(p,base),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,H='a'.repeat(64),scope={source_group_id:id(1),source_cycle_id:id(2),client_id:id(3),week_ending:'2026-10-11'};
const actor={actor_user_id:id(4),permission_view:'ADMIN',active:true},request={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
function item(){return {item_id:'native-financial:'+id(7),item_kind:'NATIVE_ORDINARY_FINANCIAL',scope,route:'SIGNED_TIMESHEETS',worker:'Actual Worker',work_date:scope.week_ending,
 sheet_scope:'WEEKLY',reason_code:'PAY_ON_HOLD',status:{text:'Holds pay',tone:'danger',subtext:'Pay held: £321.45 ex VAT',subtone:'muted'},
 hold:{holds_pay:true,holds_pay_scope:'TIMESHEET',withheld_amount_pence:'32145',holds_invoice:null,pending_decision:null,authority:{owner:'ORDINARY_NEXT_AVAILABLE_EX_VAT',hash:H,withholding_known:true}},
 subject:'PAY',permitted_pay_subject:true,action:{kind:'TIMESHEET',label:'View pay hold in Timesheet ›',native_allowed:true,target:{timesheet_id:id(5)}},counted_timesheet_id:id(5),
 correction_binding:null,submission_read:null,source_information_witness:null,native_source_witness:null,native_invoice_witness:null,native_invoice_decision_witness:null,queries_parent_witness:null,
 native_ordinary_money_witness:{owner:'private.bpay_next_workbench_refresh_work_core_v1',actor_user_id:actor.actor_user_id,scope,timesheet_id:id(5),financial_id:id(6),work_id:id(7),revision_id:id(8),
 amount_basis:'SOURCE_EX_VAT',grain:'CURRENT_ORDINARY_WORK_AVAILABLE',counts_for_row_admission:false,read_proof_hash:H}};}
const envelope=(x,pay=false)=>({ok:true,contract:'WEEKLY_SOURCE_CHECKLIST_V1',command:request.command,actor_user_id:actor.actor_user_id,permission_view:pay?'PAY_READONLY':'ADMIN',
 binding:H,read_revision:H,rows:[],items:[x],has_more:false,next_cursor:null,refresh_required:false,anchor:null});
const call=(x,pay=false)=>decodeChecklistRead(envelope(x,pay),request,{id:actor.actor_user_id,payView:pay});
test('whole transport preserves genuine ordinary available EX-VAT pence with whole Timesheet scope and existing view target',()=>{
 const x=call(item()).items[0];assert.equal(x.hold.withheld_amount_pence,'32145');assert.equal(x.hold.holds_pay_scope,'TIMESHEET');assert.equal(x.hold.holds_invoice,null);
 assert.equal(x.native_ordinary_money_witness.counts_for_row_admission,false);assert.equal(x.action.target.timesheet_id,x.counted_timesheet_id);
});
test('actual reduced pay envelope is text-only and cannot hide a leaked action after paging',()=>{
 assert.throws(()=>call(item(),true),/PAY_ACTION_FORBIDDEN/);const x=item();x.action=null;const out=call(x,true).items[0];assert.equal(out.hold.withheld_amount_pence,'32145');assert.equal(out.action,null);
});
test('finite authority cannot claim known zero rounding target cash invoice or pending decision facts',()=>{
 const h=item().hold;for(const change of [{holds_pay:false,withheld_amount_pence:'0'},{withheld_amount_pence:'0'},{withheld_amount_pence:'321.45'},
 {withheld_amount_pence:'1000000000000000000'},{holds_pay_scope:'SHIFT'},{holds_invoice:true},{holds_invoice:false},{pending_decision:true},{pending_decision:false},
 {authority:{...h.authority,withholding_known:false}}])assert.throws(()=>decodeChecklistHold({...h,...change}));
});
test('all witness identities actor and exact scope bind real root financial Work and revision without original-root assumptions',()=>{
 for(const key of ['timesheet_id','financial_id','work_id','revision_id']){const x=item();x.native_ordinary_money_witness[key]=null;assert.throws(()=>call(x));}
 for(const change of [x=>x.native_ordinary_money_witness.actor_user_id=id(99),x=>x.native_ordinary_money_witness.timesheet_id=id(99),x=>x.native_ordinary_money_witness.work_id=id(99),
 x=>x.native_ordinary_money_witness.scope={...scope,client_id:id(99)},x=>x.counted_timesheet_id=id(99)]){const x=item();change(x);assert.throws(()=>call(x));}
});
test('new hash array coercions cannot cross the complete item transport',()=>{
 for(const value of [[H],[],{},null,42,true]){const x=item();x.native_ordinary_money_witness.read_proof_hash=value;assert.throws(()=>call(x));}
 const x=item();x.hold.authority.hash=[H];assert.throws(()=>call(x));
});
test('real native amount basis grain and nonadmission marker cannot be relabelled',()=>{
 for(const change of [{amount_basis:'TARGET_INC_VAT'},{grain:'TF_TOTAL'},{counts_for_row_admission:true},{owner:'private.invented'},
 {read_proof_hash:'b'.repeat(64)},{raw_financial_frame:{}}]){const x=item();Object.assign(x.native_ordinary_money_witness,change);assert.throws(()=>call(x));}
});
test('HealthRoster query, Source, invoice and no-Timesheet information items cannot borrow financial badge or witness',()=>{
 for(const change of [{item_kind:'ACTUAL_TIMESHEET'},{item_kind:'SOURCE_INFORMATION'},{item_kind:'NATIVE_SOURCE'},{item_kind:'NATIVE_INVOICE'},
 {route:'SELF_BILL'},{sheet_scope:'DAILY'},{reason_code:'HOURS_DIFFER'},{subject:'PAY_AND_INVOICE'},{subject:'INVOICE_ONLY'},
 {permitted_pay_subject:false},{queries_parent_witness:{}},{source_information_witness:{}}])assert.throws(()=>call({...item(),...change}));
});
test('financial action stays at its actual counted Timesheet and cannot acquire a fake Queries Finalise or correction target',()=>{
 for(const action of [{kind:'TIMESHEET',label:'View',native_allowed:true,target:{timesheet_id:id(99)}},{kind:'QUERIES',label:'Queries',native_allowed:true,target:{...scope,row_key:'d10g_'+H}},
 {kind:'FINALISE',label:'Finalise',native_allowed:true,target:scope}])assert.throws(()=>call({...item(),action}));
 const x=item();x.correction_binding={};assert.throws(()=>call(x));
});
test('finite native available quantity cannot certify a whole main row or a paid/completed status',()=>{
 const c={value:false,status:{text:'One item',tone:'muted'},item_count:1,link:null};assert.throws(()=>projectChecklistRow({row_id:'row',scope,client:'Client',group:'Group',route:'SIGNED_TIMESHEETS',
 retention:{actual_timesheet_count:1,most_recent_ended_week:scope.week_ending,has_open_items:true,earlier_open_weeks:0},checked:c,completed:c,pay_completed:c,hold:item().hold,permitted_pay_item_count:1,admin_actions:[]},actor));
 for(const status of [{text:'Paid',tone:'positive'},{text:'Holds pay',tone:'positive'},null])assert.throws(()=>call({...item(),status}));
});
test('all eight current literal native owners raw schemas controls and unchanged Native77 kernel pins verify exactly',()=>{
 const p=JSON.parse(read('CURRENT_NATIVE_INPUT_PINS.json'));assert.equal(p.selected.length,8);assert.deepEqual(p.census,{roots:1484,unique_inputs:1745,occurrences:2160});assert.equal(p.native,false);
 for(const r of p.selected){assert.equal(sha(read(r.saved)),r.definition_sha256);const raw=execFileSync('git',['cat-file','blob',p.source_commit+':'+r.file],{maxBuffer:4e6});assert.equal(sha(raw),r.source_file_sha256);
  assert.ok(functionEvents(raw.toString().replaceAll('\r\n','\n')).events.some(e=>e.id===r.id&&sha(e.definition)===r.definition_sha256));}
 for(const r of p.raw){const raw=execFileSync('git',['cat-file','blob',r.source_commit+':'+r.file],{maxBuffer:32e6});assert.equal(sha(raw),r.sha256);assert.equal(sha(fs.readFileSync(new URL(r.saved,base))),r.sha256);}
 assert.equal(sha(fs.readFileSync(new URL(p.kernel.saved,base))),p.kernel.sha256);
});
test('native classifier alone owns positive AVAILABLE and PAY_ON_HOLD; query qualification remains Source-only',()=>{
 const s=read('input/private_bpay_next_workbench_refresh_work_core_v1.sqlinc');
 assert.match(s,/included_source_ex_vat:=v_positive/);assert.match(s,/not v_item\.is_selected and v_item\.pay_hold and v_item\.reason_priority>40/);
 assert.match(s,/reason_code:='PAY_ON_HOLD';v_item\.reason_priority:=40;v_item\.section:='BLOCKED'/);
 assert.match(s,/query_qualified:=coalesce\(v_work\.work_kind='SOURCE'/);assert.match(s,/v_item\.portion_kind:='RESERVED'/);assert.match(s,/v_item\.portion_kind:='RECOVERY'/);
});
test('private financial reader contains no new native money or reason classifier and calls only literal read core',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc'),r=functionEvents(s).events.find(e=>e.definition),words=sqlTokens(r.body).filter(t=>t.kind==='word').map(t=>t.value);
 assert.deepEqual(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config','greatest','round'].includes(w)),[]);
 assert.equal((s.match(/weekly_source_checklist_native_members_core_v1\(work\.id,revision\.id,false\)/g)||[]).length,2);
 assert.doesNotMatch(s,/refresh_work_v1\(|refresh_work_core_v1\(|bound_source_snapshot_v1\(|public\.bpay_|financial\.total_pay|paid.*true/);
 assert.match(s,/sum\(i\.included_source_ex_vat\)/);assert.match(s,/pence:=source_amount\*100/);assert.match(s,/pence<>trunc\(pence\)/);assert.equal(scanManagedSql(s).statements.length,3);
});
test('fresh actual actor and scope permission precede native private finance and no actor substitute is permitted',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc');assert.ok(s.indexOf("'VIEW_PAY_WORK'")<s.indexOf('from private.bpay_next_module_control'));
 assert.match(s,/weekly_source_pay_actor_allowed_v1\(actor\.id\)/);assert.match(s,/weekly_source_actual_timesheet_submission_v1\(p_timesheet_id\)/);
 assert.match(s,/witness->'actual_submitted' is distinct from 'true'::jsonb/);assert.match(s,/between m\.valid_from and coalesce\(m\.valid_to/);
 assert.doesNotMatch(s,/admin_actor|fake_actor|grant execute|for update|for share/);
});
test('current raw booking and ALL-current TF are unique and exact native ordinary admission identities are conserved',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc'),native=read('input/private_bpay_next_publish_ordinary_from_financial_v1.sqlinc');
 assert.match(s,/where t\.booking_id=root\.booking_id and t\.is_current/);assert.match(s,/where f\.timesheet_id=root\.timesheet_id and f\.is_current;/);
 assert.match(s,/r\.physical_timesheet_id=root\.timesheet_id and r\.physical_timesheet_version=root\.version and r\.financial_snapshot_id=financial\.id/);
 assert.match(native,/v_ts\.timesheet_id,v_tf\.id,v_ts\.version/);assert.doesNotMatch(s,/original_timesheet_id\s*(?:=|is not distinct)/);
 assert.match(s,/root\.sheet_scope::text is distinct from 'WEEKLY'/);assert.match(s,/root\.contract_id is null/);assert.match(s,/route is distinct from 'ORDINARY_HOURS'/);
});
test('real current native hold-control tuple and worker projection clock are mandatory; missing control is unknown',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc'),native=read('input/private_bpay_next_pay_hold_control_trigger_v1.sqlinc');
 for(const field of ['pay_hold_control_version','pay_hold_changed_at_utc','pay_hold_physical_timesheet_id','pay_hold_financial_id','pay_hold_reason']){assert.ok(s.includes('work.'+field));assert.ok(native.includes(field));}
 assert.match(native,/financial_view_revision=financial_view_revision\+1/);assert.match(native,/bpay_next_workbench_refresh_work_v1/);
 assert.match(s,/work\.pay_hold_control_version is null or work\.pay_hold_control_version<=0/);assert.match(s,/group_row\.financial_view_revision is distinct from worker_revision/);
 assert.match(s,/NATIVE_CURRENT_SEALED_HOLD_CONTROL_UNAVAILABLE/);
});
test('all actual members are fully attributed conserved and exhaust the complete projection before the positive amount',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc');assert.match(s,/parent_complete' is distinct from 'true'/);assert.match(s,/segment_attribution_complete' is distinct from 'true'/);
 assert.match(s,/if exists\(with projected as/);assert.match(s,/sum\(i\.included_source_ex_vat\+i\.excluded_source_ex_vat-i\.recovery_source_ex_vat-i\.held_overhang_source_ex_vat\)/);
 assert.match(s,/NATIVE_FULL_MEMBER_CONSERVATION_UNAVAILABLE/);assert.ok(s.indexOf('NATIVE_FULL_MEMBER_CONSERVATION_UNAVAILABLE')<s.indexOf('into positive_count,source_amount'));
 assert.doesNotMatch(s,/for member in|\blimit\s|sample/i);
});
test('only native unselected AVAILABLE PAY_ON_HOLD quantity qualifies, with exact accepted and snooze owners and no H recovery zero shortcut',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc');assert.match(s,/workbench_accepted_work_v1\(work\.id,i\.component_key,i\.segment_key\)/);
 assert.match(s,/snooze_at_cut_v1\(work\.id,revision\.id,i\.segment_key,i\.component_key,native_sequence,native_today\)/);
 const arm=s.slice(s.indexOf('select count(*),sum(i.included_source_ex_vat)'),s.indexOf('if positive_count=0'));
 for(const text of ["i.portion_kind='AVAILABLE'","not i.is_selected","i.included_source_ex_vat>0","i.reason_code='PAY_ON_HOLD'","i.reason_priority=40","i.section='BLOCKED'","not i.selectable","i.direction='PAYMENT'","not i.query_qualified","i.query_state='NOT_APPLICABLE'"])assert.ok(arm.includes(text),text);
 assert.doesNotMatch(arm,/RESERVED|RECOVERY|TF_TOTAL|PAY_QUERY_OPEN/);assert.match(s,/if positive_count=0 or source_amount is null or source_amount<=0/);
});
test('full observation recheck binds native frames membership global cut fresh witness and permissions before positive certificate',()=>{
 const s=read('forward/wp_u3_ordinary_next_hold.sqlinc');assert.match(s,/frame is distinct from frame_after or members is distinct from members_after or native_sequence is distinct from sequence_after/);
 assert.match(s,/witness is distinct from private\.weekly_source_actual_timesheet_submission_v1\(root\.timesheet_id\)/);
 assert.match(s,/WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED/);assert.ok(s.lastIndexOf("'VIEW_PAY_WORK'")<s.indexOf('pence:=source_amount*100'));
});
test('four whole DTO transport leaf producer owners reverse fifteen exact hunks to immutable decision predecessor',()=>{
 const packets=['COMPLETE_TRANSPORT_REVERSES.json','COMPLETE_CALLER_REVERSES.json'];let count=0,hunks=0;
 for(const p of packets)for(const r of JSON.parse(read(p)).records){count++;hunks+=r.hunks.length;const b=fs.readFileSync(new URL(r.saved,base));assert.equal(sha(b),r.before_sha256);
  assert.ok(b.equals(execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6})));let s=read(r.output);assert.equal(sha(s),r.after_sha256);
  for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2,h.label);s=s.replace(h.after,()=>h.before);}assert.equal(s,b.toString());}
 assert.equal(count,4);assert.equal(hunks,15);
});
test('signed retention reuses actual raw open predicate plus same native financial fact before old-week filter; R254 cannot contribute',()=>{
 const s=read('forward/wp_u3_client_week_context.sqlinc'),money=s.indexOf('v_money:=private.weekly_source_checklist_ordinary_next_hold_money_v1');
 assert.ok(money>0&&money<s.indexOf("v_route='SIGNED_TIMESHEETS' and (v_count=0"));assert.match(s,/count\(\*\) filter\(where r->'open_read_item'='true'::jsonb\)/);
 const arm=s.slice(s.indexOf('for v_money_row in select r from jsonb_array_elements(v_rows)'),s.indexOf('if v_scope_count>0 then'));
 assert.match(arm,/item_kind'='ACTUAL_TIMESHEET'/);assert.match(arm,/submission_witness,actual_submitted/);assert.match(arm,/if v_money->'known'='true'::jsonb then/);assert.doesNotMatch(arm,/SOURCE_INFORMATION|R254.*true|v_count:=v_count\+1/);
 assert.match(s,/'native_financial_items',v_financial_items/);assert.match(s,/'native_financial_facts',v_scope_money_facts/);
});
test('public producer consumes exact read-local finance before revision and paging without relabelling a HealthRoster query',()=>{
 const s=read('forward/wp_u3_native_checklist.sqlinc'),begin=s.indexOf("for raw in select value from jsonb_array_elements(leaf->'native_financial_items')"),end=s.indexOf("fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts'",begin),arm=s.slice(begin,end);
 assert.ok(begin>0&&begin<s.indexOf("'WEEKLY_SOURCE_CHECKLIST_READ_REVISION_V1'"));assert.ok(begin<s.indexOf('weekly_source_checklist_page_v1('));
 assert.match(arm,/'NATIVE_ORDINARY_FINANCIAL'/);assert.match(arm,/'subject','PAY','permitted_pay_subject',true/);assert.match(arm,/View pay hold in Timesheet/);
 assert.match(arm,/completed:=false;pay_complete:=false/);assert.doesNotMatch(arm,/actual_count:=|queries_parent_witness'.*query_parent|hold_money_v1\(/);
 assert.match(arm,/'native-financial:'\|\|\(raw#>>/);assert.match(s,/'queries_parent_witness',null,'native_ordinary_money_witness'/);
});
test('source callee graph seals all full bodies and records conditional lock dynamic SELECT and opaque crypto native gates honestly',()=>{
 const graph=JSON.parse(read('NATIVE_READ_GRAPH.json'));assert.equal(graph.routines.length,46);assert.equal(graph.native,false);assert.equal(graph.FALSE_native_purity,false);
 for(const r of graph.routines){assert.equal(sha(read(r.saved_file)),r.definition_sha256);for(const id of r.callees)assert.ok(graph.routines.some(x=>x.id===id));}
 assert.equal(graph.routines.reduce((n,r)=>n+r.row_lock_sites,0),5);assert.equal(graph.dynamic_read_boundaries.length,1);assert.match(graph.dynamic_read_boundaries[0].literal_SQL,/^select count\(\*\)/);
 assert.ok(graph.external_boundaries.length>0);assert.ok(graph.external_boundaries.every(x=>x.name==='extensions.digest'&&!x.compiled_ABI&&!x.native_purity));
});
