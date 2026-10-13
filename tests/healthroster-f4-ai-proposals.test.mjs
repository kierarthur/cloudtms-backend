import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {loadAutomaticCorrectionCredits,startAutomaticCorrectionCredits} from '../docs/healthroster-build/proposals/f4-ai/agency-invoice-credit-consumer.mjs';
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),p=path.join(repo,'docs/healthroster-build/proposals/f4-ai');
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
function ui(file,key){const sandbox={};vm.runInNewContext(fs.readFileSync(path.join(p,file),'utf8'),sandbox);return sandbox[key];}
const adjustment=ui('source-invoice-adjustments.js','CloudTmsSourceInvoiceAdjustments');
const creditUI=ui('agency-invoice-credit-ui.js','CloudTmsAutomaticCreditNotes');
const source={is_weekly_source_invoice:true,editable:true,adjustments:{contract_version:'WEEKLY_SOURCE_INVOICE_ADJUSTMENTS_V1',allowed:true,credits_shift_available:false,default_vat_rate_pct:'20.00'}};
const H='a'.repeat(64),id=n=>`fc000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const row=n=>({request_id:id(n),root_timesheet_id:id(100),correction_id:`awc:${id(100)}:${id(n)}`,state_revision:'2',source_revision:H,
  original_invoice_id:id(200),reversal_timesheet_id:id(300+n),reversed_timesheet_id:id(400+n),replacement_timesheet_id:id(500+n),
  client_id:id(600),candidate_id:id(700+n),week_ending_date:'2026-10-11',candidate_name:`Worker ${n}`,invoice_number:'INV-TEST',
  blocker_code:null,selection_key:`healthroster-credit:${id(200)}:${id(300+n)}`,selectable:false,automatic:true});
const page=rows=>({contract_version:'TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1',rows,snapshot_hash:H,
  page:{returned_count:rows.length,has_more:false,next_request_id:null}});
const envelope=rows=>({contract_version:'TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1',rows,page_hashes:[H]});
const receipt=(op,extra={})=>({per_command_results:[{command_no:1,command_type:'GENERATE_CREDIT_NOTE',accepted:true,
  operation_id:id(op),operation_type:'GENERATE_CREDIT_NOTE',status:'QUEUED',phase:'SUBMITTED',change_seq:1,source_revision:H,
  created:true,reused_active:false,reused_ready:false,blocked:false,terminal_error:null,...extra}]});
const query={contract_version:'INVOICE_BATCH_QUERY_V2',action:'GENERATE',mode:'PAGE',snapshot:{},filters:{},sort:{},selection:{},page_size:100,cursor:null};
const esc=v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');

test('F4 capability is closed; absent/denied/unknown/linking claims never enable the form',()=>{
  assert.ok(adjustment.context(source));
  for(const value of [null,{}, {...source,editable:false},{...source,adjustments:{...source.adjustments,allowed:false}},
    {...source,adjustments:{...source.adjustments,credits_shift_available:true}},
    {...source,adjustments:{...source.adjustments,admin:true}}])assert.equal(adjustment.context(value),null);
});
test('F4 editable VAT default follows decimal PostgreSQL half-away rounding, including negative half pence',()=>{
  assert.equal(adjustment.defaultVat('-10.00','20.00'),'-2.00');
  assert.equal(adjustment.defaultVat('-0.03','50'),'-0.02');
  assert.equal(adjustment.defaultVat('-0.01','0'), '0.00');
  assert.equal(adjustment.defaultVat('-123.45','5.5'),'-6.79');
});
test('F4 staged default omits VAT authority; an explicit editable VAT amount survives exact wire compilation',()=>{
  const value={client_token:'test:credit',description:'Trust credit',amount_ex_vat:'-10.0'};
  assert.equal(adjustment.payload(value).amount_ex_vat,'-10.00');
  assert.equal(Object.hasOwn(adjustment.payload(value),'vat_amount'),false);
  assert.equal(adjustment.payload({...value,vat_amount:'-1.50'}).vat_amount,'-1.50');
  assert.equal(adjustment.payload({...value,vat_amount:'0.00'}).vat_amount,'0.00');
});
test('F4 refuses non-minus/precision/unsupported financial and credited-shift injection',()=>{
  const value={client_token:'valid',description:'Credit',amount_ex_vat:'-1.00'};
  for(const change of [{amount_ex_vat:'1'},{amount_ex_vat:'-0'},{amount_ex_vat:'-1.001'},
    {amount_ex_vat:NaN},{vat_amount:'1e3'},{credited_work_event_id:id(1)},{timesheet_id:id(1)}])assert.throws(()=>adjustment.payload({...value,...change}));
});
test('F4 renders exact unbound ADJ lines and escaped descriptions; existing Timesheet line is never removable credit',()=>{
  const valid={invoice_line_id:id(1),timesheet_id:null,booking_id:null,source_key:'ADJ:a',description:'<credit>',total_charge_ex_vat:-10,
    vat_amount:-2,meta_json:{weekly_source_adjustment_authority:{schema_version:'WEEKLY_SOURCE_INVOICE_ADJUSTMENT_AUTHORITY_V1'}}};
  const html=adjustment.rows([valid,{...valid,timesheet_id:id(3),description:'worker line'}],[],new Set(),source,true,esc,n=>n.toFixed(2));
  assert.match(html,/&lt;credit&gt;/);assert.doesNotMatch(html,/worker line/);assert.match(html,/inv-toggle-remove-line/);
});
test('F4 modal stages only after confirm and records user VAT overwrite distinctly',async()=>{
  const ids=['wsAdjDescription','wsAdjAmount','wsAdjVat','wsAdjError','wsAdjCancel','wsAdjAdd'];
  const nodes=Object.fromEntries(ids.map(id=>[id,{value:'',hidden:true}]));let closed=0,rendered=0;const state={};
  await adjustment.open(state,{invoiceData:{weekly_source_invoice:source},canEdit:true,showModal:()=>{},document:{getElementById:id=>nodes[id]},
    newToken:()=>id(1),close:()=>closed++,rerender:()=>rendered++});
  assert.equal(state.invoiceEdits,undefined);nodes.wsAdjDescription.value='Trust credit';nodes.wsAdjAmount.value='-10';nodes.wsAdjAmount.oninput();
  assert.equal(nodes.wsAdjVat.value,'-2.00');nodes.wsAdjVat.value='-1.50';nodes.wsAdjVat.oninput();nodes.wsAdjAdd.onclick();
  assert.equal(state.invoiceEdits.add_adjustments[0].vat_amount,'-1.50');assert.equal(closed,1);assert.equal(rendered,1);
});
test('AI missing/false producer capability preserves ordinary behavior without RPC',async()=>{
  let calls=0;for(const deps of [{rpc:()=>calls++},{rpc:()=>calls++,healthRosterCorrectionConsumersReady:async()=>false}])
    assert.equal(await loadAutomaticCorrectionCredits(deps,id(1),query),null);assert.equal(calls,0);
});
test('AI producer uses authenticated actor/query only and closed actual native TEXT identities',async()=>{
  let args;const loaded=await loadAutomaticCorrectionCredits({healthRosterCorrectionConsumersReady:async()=>true,rpc:async(name,input)=>{
    assert.equal(name,'timesheet_correction_credit_candidates_v1');args=input;return page([row(1)]);}},id(1),query);
  assert.equal(args.p_request.actor_user_id,id(1));assert.equal(loaded.rows[0].correction_id,`awc:${id(100)}:${id(1)}`);
  assert.equal(typeof loaded.rows[0].state_revision,'string');assert.equal(loaded.rows[0].selectable,false);
});
test('AI malformed/foreign/numeric producer facts refuse before any registration',async()=>{
  for(const change of [{correction_id:id(1)},{correction_id:`awc:${id(100)}:${id(2)}`},{root_timesheet_id:id(101)},{state_revision:2},
    {source_revision:2},{selectable:true},{automatic:false},{replacement_timesheet_id:id(301)}])
    await assert.rejects(()=>loadAutomaticCorrectionCredits({healthRosterCorrectionConsumersReady:async()=>true,rpc:async()=>page([{...row(1),...change}])},id(1),query),/PROJECTION_INVALID/);
});
test('AI separate commands against one I1 have distinct deterministic tokens and no selection root',async()=>{
  const commands=[];await startAutomaticCorrectionCredits(envelope([row(1),row(2)]),async command=>{commands.push(command);return receipt(1000+commands.length);});
  assert.equal(commands.length,2);assert.equal(commands[0].base_invoice_id,commands[1].base_invoice_id);
  assert.notEqual(commands[0].command_token,commands[1].command_token);assert.deepEqual(Object.keys(commands[0]).sort(),['base_invoice_id','command_token','command_type','credit_reason','credit_scope'].sort());
  assert.equal(Object.hasOwn(commands[0],'selection_contract'),false);
  const again=[];await startAutomaticCorrectionCredits(envelope([row(1)]),async command=>{again.push(command);return receipt(1001);});assert.equal(commands[0].command_token,again[0].command_token);
});
test('AI a failed credit does not prevent the next native registration; unknown error text is bounded',async()=>{
  let calls=0;const result=await startAutomaticCorrectionCredits(envelope([row(1),row(2)]),async()=>{if(++calls===1)throw new Error('private data');return receipt(1002);});
  assert.equal(calls,2);assert.equal(result[0].code,'CORRECTION_CREDIT_START_FAILED');assert.equal(result[1].accepted,true);assert.doesNotMatch(JSON.stringify(result),/private data/);
});
test('AI already-credited terminal is done, and a reused blocked operation keeps its real native state',async()=>{
  const done=await startAutomaticCorrectionCredits(envelope([row(1)]),async()=>({per_command_results:[{command_no:1,command_type:'GENERATE_CREDIT_NOTE',accepted:false,
    terminal_error:{code:'CREDIT_SCOPE_ALREADY_CREDITED'}}]}));assert.equal(done[0].already_credited,true);
  const blocked=await startAutomaticCorrectionCredits(envelope([row(1)]),async()=>receipt(1001,{status:'BLOCKED',phase:'VALIDATE',blocked:true,created:false,reused_active:true}));
  assert.equal(blocked[0].blocked,true);assert.equal(blocked[0].operation_receipt.status,'BLOCKED');assert.equal(blocked[0].operation_receipt.phase,'VALIDATE');
});
test('AI all rows validate before first mutation; duplicate units cannot register twice',async()=>{
  let calls=0;await assert.rejects(()=>startAutomaticCorrectionCredits(envelope([row(1),{...row(2),source_revision:3}]),async()=>{calls++;return receipt(1);}),/INVALID/);
  await assert.rejects(()=>startAutomaticCorrectionCredits(envelope([row(1),row(1)]),async()=>{calls++;return receipt(1);}),/INVALID/);assert.equal(calls,0);
});
test('AI list has no checkbox/manual tool; result rendering does not claim document completion or issued state',()=>{
  const html=creditUI.render(envelope([{...row(1),candidate_name:'<worker>'}]),esc);assert.match(html,/&lt;worker&gt;/);assert.doesNotMatch(html,/checkbox|data-batch-action/);
  const pending=creditUI.renderResults([{accepted:true,blocked:false,already_credited:false,code:null}],esc);assert.match(pending,/preparation started/);assert.doesNotMatch(pending,/issued|complete/i);
});
test('AI registers exact native operation receipts separately without making them the ordinary root',()=>{
  const native=receipt(1001).per_command_results[0];let seen;
  creditUI.register([{operation_receipt:native}],(value,options)=>{seen={value,options};return [];},{modal_identity:'m'});
  assert.equal(seen.value.per_command_results[0],native);assert.equal(Object.hasOwn(seen.value,'root_operation_id'),false);assert.equal(seen.options.purpose,'GENERATE_CREDIT_NOTE');
});
test('F4 complete native editor is identical after only declared guarded hunks are removed; Source move owner unchanged',()=>{
  const guard=JSON.parse(fs.readFileSync(path.join(p,'F4_SOURCE_GUARD.json')));assert.equal(guard.ordinary_invoice_apply_edits_body_after_removing_two_declared_hunks_is_identical,true);
  const move=fs.readFileSync(path.join(p,'source/04_invoice_move_atomic.sqlinc'),'utf8').trimEnd();assert.equal(sha(move),guard.unchanged_invoice_move_atomic_body_sha256);
  for(const output of guard.outputs)assert.equal(sha(fs.readFileSync(path.join(p,output.path))),output.sha256,output.path);
});
test('F4 all unbound exceptions require server-sealed exact ADJ proof; issue still requires a real Source binding',()=>{
  const allocation=fs.readFileSync(path.join(p,'source/02_invoice_allocation_assert.sqlinc'),'utf8');
  const issue=fs.readFileSync(path.join(p,'source/03_invoice_issue_validate.sqlinc'),'utf8');
  assert.equal((allocation.match(/adjustment_line_valid_v1/g)||[]).length,2);assert.equal((issue.match(/adjustment_line_valid_v1/g)||[]).length,1);
  assert.match(issue,/WEEKLY_SOURCE_ISSUE_NO_SOURCE_BINDING/);assert.match(issue,/WEEKLY_SOURCE_ISSUE_EMPTY_INVOICE/);
});
test('F4 line owner changes only invoice_lines/totals/audit and never produces finance, payment or placement',()=>{
  const source=fs.readFileSync(path.join(p,'f4-adjustment-owners.sqlinc'),'utf8');
  assert.doesNotMatch(source,/(?:insert into|update|delete from) public\.(timesheets|timesheets_financials|weekly_source_billing_movements|weekly_source_invoice_placements|pay_batches)/i);
  assert.match(source,/line\.timesheet_id is not null or line\.booking_id is not null/);assert.match(source,/CLIENT_SELF_BILL/);assert.match(source,/NEGATIVE_ADJUSTMENT_LINE/);
  assert.match(source,/rule_set_version_id/);assert.doesNotMatch(source,/weekly_source_effective_rule_set_v1/);assert.match(source,/CREDITED_SHIFT_AUTHORITY_UNAVAILABLE/);
});
test('AI source delegates the existing scoped authority/dispatcher and includes no new durable store',()=>{
  const sql=fs.readFileSync(path.join(p,'ai-credit-candidates.sqlinc'),'utf8');assert.match(sql,/invoice_credit_scope_context_v1/);assert.match(sql,/request\.state='APPLIED'/);
  assert.match(sql,/invoice\.status in\('ISSUED','PAID'\)/);assert.match(sql,/v_count=250/);assert.doesNotMatch(sql,/create table|insert into|update public|delete from/i);
  const guard=JSON.parse(fs.readFileSync(path.join(p,'AI_WORKER_SOURCE_GUARD.json')));assert.equal(guard.ordinary_confirm_after_name_restoration_is_identical,true);
  assert.equal(sha(fs.readFileSync(path.join(p,guard.output.path))),guard.output.sha256);
});
