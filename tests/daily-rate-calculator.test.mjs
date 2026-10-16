import test from 'node:test';
import assert from 'node:assert/strict';
import {buildCanonicalDailyFinancialSnapshot} from '../broker/src/daily-canonical-authority.js';
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
function input(){
 const timesheet={timesheet_id:id(1),version:2,worked_start_iso:'2026-10-01T08:00:00Z',worked_end_iso:'2026-10-01T14:00:00Z',authorised_at_server:'2026-10-02T10:00:00Z'};
 const old={id:id(2),additional_units_json:{},travel_pay_ex_vat:10,travel_charge_ex_vat:15,mileage_units:0,
  paid_at_utc:'2026-10-03T10:00:00Z',locked_by_invoice_id:id(9)};
 const candidate={id:id(3),pay_method:'PAYE',mileage_pay_rate:0.45};
 const rates={source_kind:'CANDIDATE_OVERRIDE',override_id:id(4),default_id:id(5),rate_type:'PAYE',
  pay_day:0,pay_night:null,pay_sat:null,pay_sun:null,pay_bh:null,charge_day:20,charge_night:null,charge_sat:null,charge_sun:null,charge_bh:null};
 const context={version:'DAILY_RATE_CONTEXT_V1',identity:{timesheet_id:id(1),timesheet_version:'2',candidate_id:id(3),client_id:id(6),contract_id:null},
  current_financial_id:id(2),authority_selection_args:{role:'RN',band:null},rate_authority:{selected_rates:structuredClone(rates)},
  native_calculation_input:{out_timesheet:structuredClone(timesheet),out_cur_fin:structuredClone(old),out_candidate:structuredClone(candidate)},native_claim_charge_authority:{mileage_charge_rate:0.60}};
 return {env:{},timesheet,currentFinancials:old,candidate,candidateId:id(3),clientId:id(6),role:'RN',band:null,rateType:'PAYE',ratesRow:rates,
  policy:{apply_erni_to:'PAYE_ONLY',erni_pct:13.8},rateCorrectionContext:context,
  subtractBreak:s=>s,classifyMinutes:()=>({hours_day:6,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0}),resolveEffectivePayChannel:()=>({ok:true})};
}
test('actual zero override PAY and Client CHARGE preserve fixed reimbursement and old paid input',()=>{
 const v=input(),saved=structuredClone({old:v.currentFinancials,context:v.rateCorrectionContext,timesheet:v.timesheet});
 const s=buildCanonicalDailyFinancialSnapshot(v);
 assert.equal(s.pay_day,0);assert.equal(s.charge_day,20);assert.equal(s.total_pay_ex_vat,10);assert.equal(s.total_charge_ex_vat,135);assert.equal(s.margin_ex_vat,125);
 assert.equal(s.invoice_breakdown_json.base_hours.pay_ex_vat,0);assert.equal(s.invoice_breakdown_json.additional.pay_ex_vat,10);
 assert.deepEqual({old:v.currentFinancials,context:v.rateCorrectionContext,timesheet:v.timesheet},saved);
});
test('NULL used override PAY is missing and cannot be replaced by a Client PAY quote',()=>{
 const v=input();v.ratesRow.pay_day=null;v.rateCorrectionContext.rate_authority.selected_rates.pay_day=null;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_USED_RATE_MISSING/);
 v.ratesRow.pay_day=18;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_CALCULATION_AUTHORITY_MISMATCH/);
});
test('missing unused standard buckets and unused Additional zero do not block',()=>{
 const v=input();v.currentFinancials.additional_units_json={EXTRA:{units:0,pay_rate:null,charge_rate:null}};
 v.rateCorrectionContext.native_calculation_input.out_cur_fin.additional_units_json=structuredClone(v.currentFinancials.additional_units_json);
 assert.equal(buildCanonicalDailyFinancialSnapshot(v).has_rate_issue,false);
 v.currentFinancials.additional_units_json.EXTRA.units=1;
 v.rateCorrectionContext.native_calculation_input.out_cur_fin.additional_units_json.EXTRA.units=1;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_ADDITIONAL_AUTHORITY_REQUIRED/);
});
test('mileage uses genuine current Candidate PAY and Client CHARGE; zero and missing differ',()=>{
 const v=input();v.currentFinancials.mileage_units=10;v.rateCorrectionContext.native_calculation_input.out_cur_fin.mileage_units=10;
 let s=buildCanonicalDailyFinancialSnapshot(v);assert.equal(s.mileage_pay_ex_vat,4.5);assert.equal(s.mileage_charge_ex_vat,6);assert.equal(s.total_pay_ex_vat,14.5);assert.equal(s.total_charge_ex_vat,141);
 v.rateCorrectionContext.native_calculation_input.out_candidate.mileage_pay_rate=0;v.rateCorrectionContext.native_claim_charge_authority.mileage_charge_rate=0;
 s=buildCanonicalDailyFinancialSnapshot(v);assert.equal(s.mileage_pay_ex_vat,0);assert.equal(s.mileage_charge_ex_vat,0);
 v.rateCorrectionContext.native_claim_charge_authority.mileage_charge_rate=null;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_USED_MILEAGE_RATE_MISSING/);
});
test('current Candidate channel changes rate cells without changing original physical or paid input',()=>{
 const v=input();v.candidate.pay_method='UMBRELLA';v.rateCorrectionContext.native_calculation_input.out_candidate.pay_method='UMBRELLA';
 v.ratesRow.rate_type='UMBRELLA';v.ratesRow.pay_day=12;v.rateCorrectionContext.rate_authority.selected_rates=structuredClone(v.ratesRow);
 const s=buildCanonicalDailyFinancialSnapshot(v);assert.equal(s.pay_method,'UMBRELLA');assert.equal(s.total_pay_ex_vat,82);assert.equal(v.currentFinancials.paid_at_utc,'2026-10-03T10:00:00Z');assert.equal(s.timesheet_id,v.timesheet.timesheet_id);
});
test('Contract or changed server identity/context cannot enter Daily correction calculation',()=>{
 const v=input();v.rateCorrectionContext.identity.contract_id=id(7);assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_CALCULATION_CONTEXT_MISMATCH/);
 v.rateCorrectionContext.identity.contract_id=null;v.rateCorrectionContext.current_financial_id=id(8);assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_CALCULATION_CONTEXT_MISMATCH/);
});

test('native Client PAY fallback is used only when resolver selected CLIENT_DEFAULT',()=>{
 const v=input();v.ratesRow.source_kind='CLIENT_DEFAULT';v.ratesRow.override_id=null;v.ratesRow.pay_day=10;
 v.rateCorrectionContext.rate_authority.selected_rates=structuredClone(v.ratesRow);
 const s=buildCanonicalDailyFinancialSnapshot(v);
 assert.equal(s.rate_source_refs_json.kind,'CLIENT_DEFAULT');assert.equal(s.rate_source_refs_json.override_id,null);
 assert.equal(s.total_pay_ex_vat,70);assert.equal(s.total_charge_ex_vat,135);assert.equal(s.margin_ex_vat,56.72);
});
test('unused decimal zero Additional differs from missing units or nonfinite retained money',()=>{
 const v=input();v.rateCorrectionContext.native_calculation_input.out_cur_fin.additional_units_json={X:{units:'0.000'}};
 assert.equal(buildCanonicalDailyFinancialSnapshot(v).additional_pay_ex_vat,0);
 v.rateCorrectionContext.native_calculation_input.out_cur_fin.additional_units_json.X.units=null;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_ADDITIONAL_AUTHORITY_REQUIRED/);
 v.rateCorrectionContext.native_calculation_input.out_cur_fin.additional_units_json={};
 v.rateCorrectionContext.native_calculation_input.out_cur_fin.mileage_units='NaN';
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_NATIVE_ECONOMIC_BASIS_UNKNOWN/);
});
test('a used invalid rate and changed worked input cannot be admitted as zero',()=>{
 const v=input();v.ratesRow.pay_day=-1;v.rateCorrectionContext.rate_authority.selected_rates.pay_day=-1;
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_USED_RATE_INVALID/);
 v.ratesRow.pay_day=0;v.rateCorrectionContext.rate_authority.selected_rates.pay_day=0;
 v.timesheet.worked_end_iso='2026-10-01T16:00:00Z';
 assert.throws(()=>buildCanonicalDailyFinancialSnapshot(v),/DAILY_RATE_WORKED_FACTS_MISMATCH/);
});
