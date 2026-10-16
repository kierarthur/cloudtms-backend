import test from 'node:test';
import assert from 'node:assert/strict';
import {validatePreviewRequest,validateAcceptRequest,validateReadRequest,validatePreview,validateResult,validateCurrentPlacement,validateAction,validateEffect,normalizeReason,ACTION_VARIANTS,VERSIONS,revision} from '../forward/contracts/contract-correction-v3.mjs';
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
const sha='a'.repeat(64),copy=v=>structuredClone(v);
const action=(kind='OPEN_TIMESHEET')=>({kind,target_type:'TIMESHEET',target_id:id(1),url:'/api/timesheets/'+id(1),caption:'Open Timesheet',enabled:true,disabled_reason:null});
const result=()=>({version:VERSIONS.CONTRACT,purpose:'CONTRACT_CORRECTION',operation_id:id(2),state:'COMPLETE',state_revision:'4',placement:{status:'COMMITTED',original_timesheet_id:id(1),current_timesheet_id:id(3),placement_revision:'1',receipt_id:id(4)},financial:{status:'REGISTERED',command_id:id(5),cut_id:id(6),summary:{visibility:'FULL',text:'Pay result registered.'}},billing:{status:'PREPARING_DRAFTS',revision:'1',group_id:id(7),visible_summary:{visibility:'FULL',text:'Invoice drafts are being prepared.'}},next_actions:[],links:[action()]});
const preview=()=>({version:VERSIONS.CONTRACT,operation_id:id(2),review_id:id(8),review_digest:sha,state_revision:'1',ready_to_confirm:true,current_timesheet_id:id(1),to_contract_id:id(9),summary:{worked_facts_preserved:true,pay_effect:{visibility:'REDACTED'},invoice_effect:{visibility:'REDACTED'}},known_blockers:[],confirm:{request_version:VERSIONS.CONTRACT,review_id:id(8),review_digest:sha,expected_state_revision:'1'},links:[]});
const rejects=(f,v)=>assert.throws(()=>f(v),/CORRECTION_CONTRACT_INVALID:/);
test('fresh request is exact V3 with server factual reason and no supplied economics',()=>{
 const v={version:VERSIONS.CONTRACT,request_id:id(1),timesheet_id:id(2),to_contract_id:id(3)};assert.equal(validatePreviewRequest(v),v);
 for(const [k,value] of Object.entries({actor_user_id:id(9),money:'10',rate:'10',component_resolutions:[],invoice_id:id(4),booking_id:'bk_abc',action:'PREVIEW'}))rejects(validatePreviewRequest,{...v,[k]:value});
 assert.equal(normalizeReason(),'Correcting the Contract');assert.equal(normalizeReason('  Wrong placement  '),'Correcting the Contract — Wrong placement');assert.equal(normalizeReason(''),'Correcting the Contract');assert.equal(normalizeReason('😀'.repeat(1969)).length,3964);
 assert.throws(()=>normalizeReason('😀'.repeat(1970)));assert.throws(()=>normalizeReason(null));
 rejects(validatePreviewRequest,{...v,version:'TIMESHEET_CONTRACT_MOVE_STAGE_V2'});rejects(validatePreviewRequest,{...v,to_contract_id:id(3).toUpperCase().replace('00000000','AAAAAAAA')});
});
test('accept and GET service envelopes cannot be exchanged or spoof the actor',()=>{
 const v={version:VERSIONS.CONTRACT,request_id:id(1),operation_id:id(2),review_id:id(3),review_digest:sha,expected_state_revision:'1'};assert.equal(validateAcceptRequest(v),v);
 rejects(validateAcceptRequest,{...v,reason:'changed accepted reason'});rejects(validateAcceptRequest,{...v,expected_state_revision:1});rejects(validateAcceptRequest,{...v,actor_user_id:id(5)});
 const read={version:VERSIONS.CONTRACT,action:'READ_RESULT',operation_id:id(2),request_id:id(1),actor_user_id:id(5)};assert.equal(validateReadRequest(read),read);rejects(validateReadRequest,{...read,review_id:id(3)});rejects(validateAcceptRequest,read);
});
test('readiness requires matching immutable review and no known blockers',()=>{
 const v=preview();assert.equal(validatePreview(v),v);
 for(const k of Object.keys(v)){const bad=copy(v);delete bad[k];rejects(validatePreview,bad);}
 const blocked=copy(v);blocked.ready_to_confirm=false;blocked.confirm=null;blocked.known_blockers=[{code:'NATIVE_RATE_MISSING',native_owner:'BANKING',field_key:'pay_day',permitted_action:null}];assert.equal(validatePreview(blocked),blocked);
 const contradictions=[{...v,summary:{...v.summary,worked_facts_preserved:false}},{...v,known_blockers:blocked.known_blockers},{...v,confirm:null},{...blocked,confirm:v.confirm},{...v,confirm:{...v.confirm,review_id:id(90)}},{...v,confirm:{...v.confirm,expected_state_revision:'2'}}];contradictions.forEach(x=>rejects(validatePreview,x));
});
test('every public nested object rejects unknown fields and missing fields',()=>{
 for(const path of [[],['placement'],['financial'],['billing'],['financial','summary'],['billing','visible_summary']]){
  const bad=result();let leaf=bad;for(const k of path)leaf=leaf[k];leaf.private_manifest={gross:'200'};rejects(validateResult,bad);
 }
 const v=result();assert.equal(validateResult(v),v);for(const k of Object.keys(v)){const b=copy(v);delete b[k];rejects(validateResult,b);}
});
test('REGISTERED financial work permits real completed placement without APPLIED or PDF issue',()=>{
 const v=result();assert.equal(validateResult(v),v);for(const state of ['REGISTERED','PROCESSING','APPLIED'])assert.equal(validateResult({...v,financial:{...v.financial,status:state}}).state,'COMPLETE');
 rejects(validateResult,{...v,placement:{...v.placement,status:'NOT_COMMITTED',receipt_id:null}});rejects(validateResult,{...v,placement:{...v.placement,receipt_id:null}});rejects(validateResult,{...v,placement:{...v.placement,placement_revision:'0'}});
 for(const forbidden of ['STAGED','READY_TO_PUBLISH','COMPLETE_FINANCIAL_PENDING'])rejects(validateResult,{...v,state:forbidden});
});
test('role redaction cannot hide money in secondary fields or native IDs',()=>{
 rejects(v=>validateResult(v,{financialVisible:false}),result());
 const v=result();v.financial.command_id=null;v.financial.cut_id=null;v.financial.summary={visibility:'REDACTED'};v.billing.group_id=null;v.billing.visible_summary={visibility:'REDACTED'};assert.equal(validateResult(v,{financialVisible:false,billingVisible:false}),v);
 for(const k of ['text','rate','amount','currency','operation_id','url'])rejects(validateEffect,{visibility:'REDACTED',[k]:'hidden'});
 const b=copy(v);b.financial.summary.text='£150';rejects(v=>validateResult(v,{financialVisible:false,billingVisible:false}),b);
});
test('NOT_REQUIRED is no fabricated command/invoice identity or summary',()=>{
 const v=result();v.financial={status:'NOT_REQUIRED',command_id:null,cut_id:null,summary:null};v.billing={status:'NOT_REQUIRED',revision:'1',group_id:null,visible_summary:null};assert.equal(validateResult(v),v);
 rejects(validateResult,{...v,financial:{...v.financial,command_id:id(8)}});rejects(validateResult,{...v,billing:{...v.billing,visible_summary:{visibility:'FULL',text:'A fake invoice'}}});
});
test('Daily has no Contract chooser, no Contract DTO field and the same physical identity',()=>{
 const req={version:VERSIONS.DAILY,request_id:id(2),timesheet_id:id(1)};assert.equal(validatePreviewRequest(req,{daily:true}),req);rejects(v=>validatePreviewRequest(v,{daily:true}),{...req,to_contract_id:id(3)});
 const p=preview();p.version=VERSIONS.DAILY;delete p.to_contract_id;p.confirm.request_version=VERSIONS.DAILY;assert.equal(validatePreview(p,{daily:true}),p);rejects(v=>validatePreview(v,{daily:true}),{...p,to_contract_id:id(3)});
 const v=result();v.version=VERSIONS.DAILY;v.purpose='DAILY_RATE_CORRECTION';v.placement.current_timesheet_id=v.placement.original_timesheet_id;assert.equal(validateResult(v),v);rejects(validateResult,{...v,placement:{...v.placement,current_timesheet_id:id(30)}});rejects(validateResult,{...v,purpose:'CONTRACT_CORRECTION'});
 assert.equal(normalizeReason(undefined,'DAILY_RATE_CORRECTION'),'Applying updated rates');
});
test('native uncertainty is explicit retained recovery only',()=>{
 const v=result();v.state='PENDING_PLACEMENT';v.placement={...v.placement,status:'NATIVE_PENDING',current_timesheet_id:null,receipt_id:null,placement_revision:'0'};rejects(validateResult,v);assert.equal(validateResult(v,{retainedNativePending:true}),v);
});
test('actions enforce exact correlations, dedupe native targets and reject retired repair wizard',()=>{
 for(const [kind,target_type,caption,extra] of ACTION_VARIANTS){const a={...action(),kind,target_type,caption};for(const k of extra)a[k]=k==='member_key'?'member1':k==='period_start'?'2026-10-01':k==='period_end'?'2026-10-31':id(4);if(kind==='WAIT_FOR_FINAL_REPORT')a.target_id=null;assert.equal(validateAction(a),a);rejects(validateAction,{...a,native_manifest:{}});}
 for(const url of ['https://evil.invalid/','//evil.invalid/','/a\\b','/a\nb'])rejects(validateAction,{...action(),url});
 for(const kind of ['REVIEW_INVOICE_MOVE','RESUME_INVOICE_MOVE'])rejects(validateAction,{...action(),kind});
 const v=result();v.next_actions=[action()];rejects(validateResult,v);
 for(const [kind,target_type,caption] of ACTION_VARIANTS.filter(x=>['REVIEW_RATES','RESUME_FINANCIAL','REVIEW_EARLIER_PAYMENT'].includes(x[0]))){const v=result();v.next_actions=[{...action(),kind,target_type,caption}];rejects(validateResult,v);}
});
test('canonical int64 decimal strings preserve values beyond JS safe integer',()=>{
 for(const value of ['0','1','9007199254740993','9223372036854775807'])assert.equal(revision(value),value);
 for(const value of [0,1,1n,'01','+1','1.0','1e5','9223372036854775808','-1',''])assert.throws(()=>revision(value));
});
test('direct placement requires a real receipt for every certified correction',()=>{
 const v={version:'CURRENT_CONTRACT_PLACEMENT_V1',requested_timesheet_id:id(1),family_root_id:id(1),current_timesheet_id:id(2),current_contract_id:id(3),placement_revision:'2',placement_receipt_id:id(4),legacy_origin:false};assert.equal(validateCurrentPlacement(v),v);
 rejects(validateCurrentPlacement,{...v,placement_receipt_id:null});rejects(validateCurrentPlacement,{...v,legacy_origin:true});rejects(validateCurrentPlacement,{...v,current_booking_id:'invented'});
 const native={...v,current_timesheet_id:id(1),placement_revision:'0',placement_receipt_id:null,legacy_origin:true};assert.equal(validateCurrentPlacement(native),native);
});
