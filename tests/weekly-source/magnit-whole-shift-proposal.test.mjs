import test from 'node:test';
import assert from 'node:assert/strict';
import {canonicalWeeklyShiftFinancialSegment} from '../../broker/src/weekly-source/weekly-rate-owner.js';
import {compareWeeklySourceShiftPrice} from '../../broker/src/weekly-source/source-price-comparator.js';
import {parseWeeklySourceFile} from '../../broker/src/weekly-source/index.js';
import {adaptWeeklySourceParserOutput} from '../../broker/src/weekly-source/upload-staging-adapter.mjs';
import {ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS} from '../../broker/src/weekly-source/profiles.js';
import {assertMagnitContractPricing,assertMagnitWholeShiftResult,magnitRuleSetPreset} from '../../docs/healthroster-build/proposals/m1-whole-shift/magnit-contract-price-admission.js';
const payRates={day:10,night:11,sat:12,sun:13,bh:14},chargeRates={day:20,night:21,sat:22,sun:23,bh:24};
const whole=(extra={})=>canonicalWeeklyShiftFinancialSegment({mode:'WHOLE_SHIFT_START_DAY',startInstant:'2026-09-07T08:00:00Z',
  endInstant:'2026-09-07T16:00:00Z',timeZone:'Europe/London',breakEvidence:{durationMinutes:30},bankHolidayDates:[],payRates,chargeRates,...extra});
const cases=[
  ['day','2026-09-07T08:00:00Z','2026-09-07T16:00:00Z',[],7500,15000],
  ['night','2026-09-07T21:00:00Z','2026-09-08T05:00:00Z',[],8250,15750],
  ['sat','2026-09-05T21:00:00Z','2026-09-06T05:00:00Z',[],9000,16500],
  ['sun','2026-09-06T21:00:00Z','2026-09-07T05:00:00Z',[],9750,17250],
  ['bh','2026-09-06T21:00:00Z','2026-09-07T05:00:00Z',['2026-09-06'],10500,18000],
];
for(const [category,startInstant,endInstant,bankHolidayDates,pay,charge] of cases) test(`actual native whole-shift owner assigns all450 paid minutes to ${category}`,()=>{
  const result=whole({startInstant,endInstant,bankHolidayDates});
  assert.equal(result.category,category); assert.equal(result.paidMinutes,450);
  assert.equal(result.payPence,String(pay)); assert.equal(result.chargePence,String(charge));
  for(const [bucket,minutes] of Object.entries(result.bucketMinutes)) assert.equal(minutes,bucket===category?450:0);
});
test('holiday is chosen by actual start date, not end date or a source rate-family column',()=>{
  assert.equal(whole({startInstant:'2026-09-07T21:00:00Z',endInstant:'2026-09-08T05:00:00Z',bankHolidayDates:['2026-09-08']}).category,'night');
});
test('ordinary full negatives keep native bucket/rounding and never acquire a second calculator',()=>{
  const positive=whole(),negative=whole({sign:-1});
  assert.equal(negative.payPence,'-7500'); assert.equal(negative.chargePence,'-15000');
  assert.deepEqual(negative.bucketMinutes,positive.bucketMinutes);
  for(const bucket of Object.keys(positive.hours)) assert.equal(negative.hours[bucket],positive.hours[bucket]===0?0:-positive.hours[bucket]);
});
test('native summary comparator ignores arbitrary source Total Cost and all other source amounts',()=>{
  const input={sourceMode:'HEALTHROSTER_WEEKLY',financialInput:{mode:'WHOLE_SHIFT_START_DAY',startInstant:'2026-09-07T08:00:00Z',endInstant:'2026-09-07T16:00:00Z',
    timeZone:'Europe/London',breakEvidence:{durationMinutes:30},bankHolidayDates:[],payRates,chargeRates},effectiveRuleSet:{values:magnitRuleSetPreset()}};
  const result=compareWeeklySourceShiftPrice({...input,sourceChargePence:'999999999999'});
  assert.equal(result.invoicePresentationChargePence,'15000'); assert.equal(result.calculation.payPence,'7500');
  assert.equal(result.sourceValidationChargePence,null);
});
test('strict native positive Contract rates, invalid breaks and interval remain fail closed',()=>{
  assert.throws(()=>whole({chargeRates:{...chargeRates,bh:0}}),error=>error.code==='WEEKLY_SHIFT_RATE_INVALID');
  assert.throws(()=>whole({breakEvidence:{durationMinutes:481}}),error=>error.code==='WEEKLY_SHIFT_BREAK_INVALID');
  assert.throws(()=>whole({endInstant:'2026-09-07T07:00:00Z'}),error=>error.code==='WEEKLY_SHIFT_INTERVAL_INVALID');
});
test('preset output qualification rejects split or multiple-band claims without calculating another price',()=>{
  assert.equal(assertMagnitWholeShiftResult(magnitRuleSetPreset(),whole()),true);
  const result=whole();
  assert.throws(()=>assertMagnitWholeShiftResult(magnitRuleSetPreset(),{...result,bucketMinutes:{day:400,night:50,sat:0,sun:0,bh:0}}),error=>error.code==='WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID');
  assert.throws(()=>assertMagnitWholeShiftResult(magnitRuleSetPreset(),{...result,mode:'SPLIT_RATE_WINDOWS'}),error=>error.code==='WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID');
  assert.equal(assertMagnitWholeShiftResult(null,{}),false);
});
test('saved preset asserts actual dated policy method and keeps null/NHSP/agency old paths',()=>{
  assert.equal(assertMagnitContractPricing(magnitRuleSetPreset(),{weekly_rate_classification_method:'WHOLE_SHIFT_START_DAY'}),true);
  assert.throws(()=>assertMagnitContractPricing(magnitRuleSetPreset(),{weekly_rate_classification_method:'SPLIT_RATE_WINDOWS'}),error=>error.code==='WEEKLY_SOURCE_MAGNIT_WHOLE_SHIFT_REQUIRED');
  assert.throws(()=>assertMagnitContractPricing({...magnitRuleSetPreset(),source_amount_basis:'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'},{weekly_rate_classification_method:'WHOLE_SHIFT_START_DAY'}),error=>error.code==='WEEKLY_SOURCE_MAGNIT_PRESET_INVALID');
  assert.equal(assertMagnitContractPricing(null,{}),false);
  assert.equal(assertMagnitContractPricing({invoice_sender:'AGENCY_INVOICE',checking_profile_kind:'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'},{}),false);
  assert.equal(assertMagnitContractPricing({invoice_sender:'CLIENT_SELF_BILL',checking_profile_kind:'NHSP_FINAL_BACKING_V1'},{}),false);
});
function csvRow({hours='7.5',expense='12.34'}={}) {
  const record=Object.fromEntries(ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map(h=>[h,'']));
  Object.assign(record,{Candidate:'M1 Worker','Business Unit ID':'M1-UNIT','Line ID':'M1-001',Expenses:expense,'Total Hours':hours,
    'Total Cost':'THIS IS NOT MONEY','STD Sub Cost':'FORGED SOURCE BAND','OT Unit Cost':'999999999',
    'Booking Start':'2026-09-07 09:00:00','Booking End':'2026-09-07 17:00:00'});
  return ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map(h=>record[h]);
}
const bytes=rows=>new TextEncoder().encode([ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS,...rows].map(r=>r.join(',')).join('\r\n'));
test('real65-column CSV parser/staging preserve Total Hours, break and exact source expense while ignoring cost/band columns',async()=>{
  const parsed=await parseWeeklySourceFile(bytes([csvRow()]),{profileId:'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'});
  assert.equal(parsed.ok,true); const row=parsed.rows[0]; assert.equal(row.wholeShiftInputs.paidMinutes,450); assert.equal(row.wholeShiftInputs.breakMinutes,30);
  assert.equal(row.sourceFixedExpense.pence,'1234'); assert.equal(Object.hasOwn(row,'sourceTotalCost'),false);
  const staged=adaptWeeklySourceParserOutput(parsed,{actor_user_id:'f5530000-0000-4000-8000-000000000001',original_filename:'m1.csv',import_use:'CHECKING',
    coverage:{start_local_date:'2026-09-07',end_local_date:'2026-09-13',confirmed_at_utc:'2026-09-14T12:00:00Z'}},
    {environment:'TEST',agency_id:'f5530000-0000-4000-8000-000000000099',source_group_id:'f5530000-0000-4000-8000-000000000050',source_cycle_id:'f5530000-0000-4000-8000-000000000080',client_id:'f5530000-0000-4000-8000-000000000010'});
  assert.equal(staged.normalisedRows[0].source_total_cost_pence,null); assert.equal(staged.normalisedRows[0].source_shift_charge_pence,null);
  assert.equal(staged.normalisedRows[0].source_expense_pence,'1234'); assert.equal(staged.moneyEvidence.length,0); assert.equal(staged.expenseEvidence.length,1);
});
test('zero worked hours retain explicit source expense and a negative physical Total Hours is refused',async()=>{
  const zero=await parseWeeklySourceFile(bytes([csvRow({hours:'0'})]),{profileId:'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'});
  assert.equal(zero.ok,true); assert.equal(zero.rows[0].rowKind,'SOURCE_ZERO'); assert.equal(zero.rows[0].sourceFixedExpense.pence,'1234');
  const negative=await parseWeeklySourceFile(bytes([csvRow({hours:'-7.5'})]),{profileId:'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'});
  assert.equal(negative.ok,false);
});
