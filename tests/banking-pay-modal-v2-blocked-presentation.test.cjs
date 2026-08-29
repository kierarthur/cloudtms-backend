const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const root=path.resolve(__dirname,'..');const query=require('./fixtures/banking-pay-local-query.cjs');
const oracle=require('./fixtures/banking-pay-legacy-oracle.cjs')();
const sqlFile='supabase/repeatable/28082026_2159_banking_pay_modal_blocked_presentation.sql';
const quote=v=>"'"+JSON.stringify(v).replaceAll("'","''")+"'::jsonb";
test('Blocked presentation is private and retains original financial and action owners',()=>{
 const sql=fs.readFileSync(path.join(root,sqlFile),'utf8');
 assert.match(sql,/private\.pay_workbench_modal_blocked_presentation_v2/);
 assert.match(sql,/IMMUTABLE SECURITY INVOKER SET search_path TO ''/);
 assert.doesNotMatch(sql,/GRANT EXECUTE|SECURITY DEFINER|UPDATE public\.|INSERT INTO|DELETE FROM/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 for(const id of ['MSG-031','MSG-032','MSG-090','MSG-091','MSG-093','MSG-096'])assert.ok(sql.includes(id),id);
});
test('actual Blocked amounts match the unchanged renderer for outstanding, scheduled and section amounts',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const rows=[
  {line_type:'TIMESHEET_PAYMENT',section_amount_display:'123.45'},
  {line_type:'DO_NOT_PAY',amount_display:'15.00'},
  {line_type:'LOAN_REPAYMENT',amount_ex_vat:'0',nominal_due_amount_ex_vat:'15'},
  {line_type:'MANUAL_DEBT_RECOVERY',amount_ex_vat:'0',nominal_due_amount_ex_vat:'20',blocked_reason_codes:['NO_PAY_HEADROOM']},
  {line_type:'OVERPAYMENT_RECOVERY',amount_ex_vat:'0',case_outstanding_amount:'120',blocked_reason_codes:['NO_PAY_HEADROOM']},
  {line_type:'OVERPAYMENT_RECOVERY',amount_ex_vat:'0',case_components:[
   {source_pay_ex_vat:'50',target_outstanding_ex_vat:'40'},{source_pay_ex_vat:'25',target_outstanding_ex_vat:'15'}],presentation_reason:'NO_PAY_HEADROOM'},
  {line_type:'OVERPAYMENT_RECOVERY',amount_ex_vat:'-10',nominal_due_amount_ex_vat:'40',blocked_reason_codes:['NO_PAY_HEADROOM']},
  {lineType:'OVERPAYMENT_RECOVERY',rowJson:{amountExVat:'0',caseOutstandingAmount:'22.35',presentationReason:'NO_PAY_HEADROOM'}},
  {line_type:'OVERPAYMENT_RECOVERY',amount_ex_vat:'0',nominal_due_amount_ex_vat:'30'},
  {line_type:'OVERPAYMENT_RECOVERY',amount_ex_vat:'0',case_components:[{targetOutstandingExVat:'-12.30'},{remainingSourceAmount:'8.40'}],
   case_resolution_summary:{blocked_reason_codes:['NO_PAY_HEADROOM']}}
 ];
 const expected=rows.map(row=>{
  const manual=oracle.getManualDebtRecoveryPresentation(row),over=oracle.getOverpaymentRecoveryPresentation(row);
  return manual?.no_available_funds?-Math.abs(manual.scheduled_due):
   over?.no_available_funds?over.outstanding_total:over?-Math.abs(over.recoverable_this_run):oracle.getLineSectionAmount(row);
 });
 const results=query(rows.map(row=>"SELECT private.pay_workbench_modal_blocked_presentation_v2("+quote(row)+",'{}'::jsonb);").join('\n'));
 results.forEach((value,i)=>assert.equal(Number(value.affected_display_amount),Number(expected[i])||0,'original Blocked amount '+i));
 assert.equal(results[2].nominal_due_amount,'15.00','retain published loan due separately, not a new calculation');
});
test('actual Blocked reasons retain exact known wording, dates, unknown diagnostics and no invented action',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const cases=[
  [{line_type:'MANUAL_DEBT_RECOVERY',amount_display:'0',nominal_due_amount_ex_vat:'10',blocked_reason_codes:['NO_PAY_HEADROOM']},{},'MSG-031','Insufficient funds to deduct'],
  [{line_type:'DO_NOT_PAY',amount_display:'22'},{},null,'This line is currently marked do not pay.'],
  [{line_type:'TIMESHEET_PAYMENT',amount_display:'22',snooze_state:{state:'SNOOZED',snooze_until_date:'2026-09-01'}},{},null,'Snoozed until 01/09/2026.'],
  [{line_type:'BLOCKED_TIMESHEET',amount_display:'22'},{},'MSG-093',null],
  [{line_type:'TIMESHEET_PAYMENT',amount_display:'22',presentation_reason:'UNFAMILIAR_PRIVATE_CODE'},{},'MSG-090','Payment issue not identified'],
  [{},{task_family:'SOURCE_PROGRESS',state:'BLOCKED',code:'SOURCE_REFRESH_FAILED',title:'Refresh failed'},null,'Refresh failed'],
  [{},{task_family:'BANK_ACCOUNT',state:'BLOCKED',code:'BANK_RESULT_CHANGED'},'MSG-096',null]
 ];
 const results=query(cases.map(([row,meta])=>"SELECT private.pay_workbench_modal_blocked_presentation_v2("+quote(row)+","+quote(meta)+");").join('\n'));
 results.forEach((r,i)=>{
  assert.equal(r.reason_message_id,cases[i][2],i);if(cases[i][3])assert.equal(r.reason,cases[i][3],i);
  assert.ok(r.reason&&r.clear_condition);assert.equal(r.action,undefined,'no action granted');
  assert.doesNotMatch(r.reason,/UNFAMILIAR_PRIVATE_CODE|SOURCE_REFRESH_FAILED|BANK_RESULT_CHANGED/);
 });
 assert.equal(results[5].affected_display_amount,null);assert.equal(results[6].affected_display_amount,null);
 assert.deepEqual(results[4].diagnostic_codes,['UNFAMILIAR_PRIVATE_CODE']);
});
test('private Blocked presentation excludes indefinite snoozes and rejects wrong state or corrupt display facts',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 assert.deepEqual(query("SELECT COALESCE(private.pay_workbench_modal_blocked_presentation_v2("+quote({
  line_type:'TIMESHEET_PAYMENT',amount_display:'10',snooze_state:{state:'SNOOZED'}})+",'{}'::jsonb),'null'::jsonb);"),[null]);
 for(const [row,meta,error] of [
  [{amount_display:'not an amount'},{},'BANKING_PAY_V2_INVALID_AMOUNT'],
  [{},{task_family:'SOURCE_PROGRESS',state:'UPDATING',title:'Refreshing…'},'BANKING_PAY_V2_INVALID_INPUT'],
  [{},{task_family:'SOURCE_PROGRESS'},'BANKING_PAY_V2_INVALID_INPUT'],
  [{},{task_family:'BANK_ACCOUNT',state:'ACTION_REQUIRED'},'BANKING_PAY_V2_INVALID_INPUT']
 ])assert.throws(()=>query("SELECT private.pay_workbench_modal_blocked_presentation_v2("+quote(row)+","+quote(meta)+");"),new RegExp(error));
});
