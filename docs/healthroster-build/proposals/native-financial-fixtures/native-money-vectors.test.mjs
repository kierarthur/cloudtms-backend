import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {FIXTURE_CONTRACT_RATES,NATIVE_CASES,nativeCase} from './native-casebook.mjs';
import {buildNativePhase} from './native-pure-bridge.mjs';
const root=path.resolve(process.env.NFX_REVIEW_SOURCE_ROOT??'../healthroster-v2-backend-13102026');
const load=x=>import(pathToFileURL(path.join(root,x)));
const money=await load('broker/src/weekly-source/source-invoice-money.js');
const rate=await load('broker/src/weekly-source/weekly-rate-owner.js');
const comparator=await load('broker/src/weekly-source/source-price-comparator.js');
const literal=x=>({kind:'STRING',text:x,rawToken:x,coordinate:'NFX2'});
const buckets=['day','night','sat','sun','bh'];
const pay=Object.fromEntries(buckets.map(b=>[b,FIXTURE_CONTRACT_RATES['paye_'+b]]));
const charge=Object.fromEntries(buckets.map(b=>[b,FIXTURE_CONTRACT_RATES['charge_'+b]]));
const id='00000000-0000-8000-8000-000000000001';
const input={contract:'NFX_NATIVE_CONTEXT_V1',case_id:'WS-NFX-NULL-HR-20261013',phase:'STAGE',actor_user_id:id,fixture_client_id:id,fixture_candidate_id:id,fixture_contract_id:id,external_source_key:'NFX-INDEPENDENT-CONTROL-101',database_observed_at_utc:'2026-10-13T12:00:00Z',context:{ok:true,environment:'TEST',agency_id:id,source_group_id:id,source_cycle_id:id,client_id:id,client_name:'NFX Fixture',source_family:'ROSTER',finalisation_week_ending:'2026-10-11',effective_rule_set:null}};

test('independent literal Contract oracle: eight weekday hours £20 pay / £30 charge',()=>{
 const x=rate.canonicalWeeklyShiftFinancialSegment({mode:'SPLIT_RATE_WINDOWS',bucketMinutes:{day:480,night:0,sat:0,sun:0,bh:0},payRates:pay,chargeRates:charge});
 assert.equal(x.payPence,'16000');assert.equal(x.chargePence,'24000');
 assert.deepEqual(money.deriveWeeklySourceInvoiceAmounts({netPence:x.chargePence,lineVatMethod:'ROUND_EACH_LINE_FROM_NET',vatRatePct:'20'}),{netPence:'24000',vatPence:'4800',grossPence:'28800'});
});
test('M1 whole overnight shift retains one native NIGHT category and ignores unrelated source Total Cost',()=>{
 const x=rate.canonicalWeeklyShiftFinancialSegment({mode:'WHOLE_SHIFT_START_DAY',startInstant:'2026-10-06T20:00:00Z',endInstant:'2026-10-07T00:00:00Z',timeZone:'Europe/London',breakEvidence:{durationMinutes:0},bankHolidayDates:[],payRates:pay,chargeRates:charge});
 assert.equal(x.category,'night');assert.equal(x.payPence,'12000');assert.equal(x.chargePence,'18000');
 assert.deepEqual(x.bucketMinutes,{day:0,night:240,sat:0,sun:0,bh:0});
 const financialInput={mode:'WHOLE_SHIFT_START_DAY',startInstant:'2026-10-06T20:00:00Z',endInstant:'2026-10-07T00:00:00Z',timeZone:'Europe/London',breakEvidence:{durationMinutes:0},bankHolidayDates:[],payRates:pay,chargeRates:charge};
 const values={invoice_sender:'CLIENT_SELF_BILL',invoice_amount_basis:'CONTRACT_CALCULATION',source_amount_basis:'NONE'};
 const normal=comparator.compareWeeklySourceShiftPrice({sourceMode:'HEALTHROSTER_WEEKLY',financialInput,effectiveRuleSet:{values}});
 const unrelated=comparator.compareWeeklySourceShiftPrice({sourceMode:'HEALTHROSTER_WEEKLY',financialInput,sourceChargePence:'999999',effectiveRuleSet:{values}});
 assert.deepEqual(unrelated,normal);
});
test('whole-shift weekend/BH precedence and reversal keep same Contract authority',()=>{
 const base={mode:'WHOLE_SHIFT_START_DAY',timeZone:'Europe/London',breakEvidence:{durationMinutes:30},bankHolidayDates:[],payRates:pay,chargeRates:charge};
 const sat=rate.canonicalWeeklyShiftFinancialSegment({...base,startInstant:'2026-10-10T20:00:00Z',endInstant:'2026-10-11T00:00:00Z'});
 assert.equal(sat.category,'sat');assert.equal(sat.paidMinutes,210);assert.equal(sat.payPence,'14000');assert.equal(sat.chargePence,'21000');
 const holiday=rate.canonicalWeeklyShiftFinancialSegment({...base,bankHolidayDates:['2026-10-10'],startInstant:'2026-10-10T20:00:00Z',endInstant:'2026-10-11T00:00:00Z',sign:-1});
 assert.equal(holiday.category,'bh');assert.equal(holiday.payPence,'-21000');assert.equal(holiday.chargePence,'-31500');
});
test('fractional stored minutes distinguish exact gross from rounded presentation hours without new pay',()=>{
 const x=rate.canonicalWeeklyShiftFinancialSegment({mode:'SPLIT_RATE_WINDOWS',bucketMinutes:{day:1,night:2,sat:0,sun:0,bh:0},payRates:{day:10,night:15,sat:1,sun:1,bh:1},chargeRates:{day:37.13,night:41.27,sat:1,sun:1,bh:1}});
 assert.equal(x.payPence,'65');assert.equal(x.chargePence,'198');
 const invoice=money.deriveWeeklySourceInvoiceAmounts({netPence:x.chargePence,lineVatMethod:'ROUND_TOTAL_FROM_EXACT_NET',vatRatePct:'20',bucketMinutes:x.bucketMinutes,chargeRates:{day:'37.13',night:'41.27',sat:'1',sun:'1',bh:'1'}});
 // Independent rational: (1*3713+2*4127)/60 pence * 6/5 = 239.34p.
 assert.deepEqual(invoice,{netPence:'198',vatPence:'41',grossPence:'239'});
});
test('report A HALF_UP cells and B supplied pence have independent literal oracles',()=>{
 assert.equal(money.parseReportLineVatCell(literal('6.602')).pence,'660');
 assert.equal(money.parseReportLineTotalCell(literal('39.612')).pence,'3961');
 assert.deepEqual(money.deriveWeeklySourceInvoiceAmounts({netPence:'3301',sourceVatPence:'661',sourceGrossPence:'3962',lineVatMethod:'LINE_VAT_FROM_REPORT',vatRatePct:'20'}),{netPence:'3301',vatPence:'661',grossPence:'3962'});
 assert.equal(money.parseReportLineVatCell(literal('-6.605')).pence,'-661');
});
test('opposite raw VAT errors plus mixed immutable releases require pending zero authority',()=>{
 const raw=[{netPence:'10000',vatPence:'2002'},{netPence:'10000',vatPence:'1998'}];
 assert.equal(money.invoiceVatRoundingPence(raw,'ONCE_ON_TOTAL_HALF_UP','20'),'0');
 const admitted=[{netPence:'10000',vatPence:'2000'},{netPence:'10000',vatPence:'1998'}];
 assert.equal(money.invoiceVatRoundingPence(admitted,'ONCE_ON_TOTAL_HALF_UP','20'),'2');
 assert.equal(20000+3998+2,24000);
 assert.equal(money.invoiceVatRoundingPence(admitted,'SUM_OF_LINES','20'),'0');
});
test('actual staging producer builds bounded SQL from source bytes, with no finance seed',async()=>{
 const result=await buildNativePhase({sourceRoot:root,input});
 assert.equal(result.fixture_file.byte_count,486);assert.equal(result.fixture_file.storage_status,'LOCAL_AUTHORED_BYTES_ONLY_NO_R2_CLAIM');
 assert.match(result.sql,/weekly_source_upload_stage_begin_atomic_v1/);assert.match(result.sql,/weekly_source_upload_seal_atomic_v1/);
 assert.doesNotMatch(result.sql,/insert into public\.(?:timesheets|invoices|weekly_source_final|weekly_source_billing)/i);
 assert.doesNotMatch(result.sql,/source_file_r2_key":"[^n]/);
 assert.ok(Buffer.byteLength(result.sql)<2_000_000);
});
test('foreign or early scope cannot enter fixture bridge',async()=>{
 await assert.rejects(buildNativePhase({sourceRoot:root,input:{...input,context:{...input.context,client_id:'00000000-0000-4000-8000-000000000099'}}}),/FOREIGN_CLIENT/);
 await assert.rejects(buildNativePhase({sourceRoot:root,input:{...input,context:{...input.context,finalisation_week_ending:'2026-10-18'}}}),/NULL_RULE_WEEK/);
 await assert.rejects(buildNativePhase({sourceRoot:root,input:{...input,context:{...input.context,effective_rule_set:{values:{}}}}}),/NULL_RULE_WEEK/);
});
test('two genuine source rows of one Contract/week are emitted through native parser/staging for lineage regression',async()=>{
 const r=await buildNativePhase({sourceRoot:root,input:{...input,case_id:'WS-NFX-MULTI-HR-20261013',fixture_candidate_name:'NFX October Multi Nurse',external_source_keys:['NFX-MULTI-ROW-001','NFX-MULTI-ROW-002']}});
 assert.match(r.sql,/NFX-MULTI-ROW-001/);assert.match(r.sql,/NFX-MULTI-ROW-002/);
 assert.match(r.sql,/2026-10-06/);assert.match(r.sql,/2026-10-07/);
 assert.doesNotMatch(r.sql,/insert into public\.(timesheets|contract_weeks|weekly_source_row_timesheet_lineages)/i);
 const m=JSON.parse(fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'multirow-lineage-fixture/NATIVE_FIXTURE_MANIFEST.json')));
 assert.equal(m.fixture_identities.contract.length,36);assert.equal(m.hosted_acceptance,false);
 const fin=fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'multirow-lineage-fixture/04_prepare_and_fin.sqlinc'),'utf8');
 assert.match(fin,/final_revision_id=s.revision_id\)<>2/);assert.match(fin,/count\(distinct invoice_timesheet_id\)/);
});
test('fixture declarations and emitted batches forbid result seeding, clock bypass and external dispatch',()=>{
 const folder=path.dirname(fileURLToPath(import.meta.url));
 const m=JSON.parse(fs.readFileSync(path.join(folder,'NATIVE_FIXTURE_MANIFEST.json')));
 assert.equal(m.hosted_acceptance,false);assert.equal(m.identity_namespace,'cloudtms.weekly-source.test.scenario.v1');
 for(const f of m.batch_files){const s=fs.readFileSync(path.join(folder,f.file),'utf8');assert.ok(f.bytes<2_000_000);assert.ok(f.statement_count_upper_bound<=250);assert.doesNotMatch(s,/\b(?:BEGIN|COMMIT|ROLLBACK)\s*;/i);assert.doesNotMatch(s,/p_now_utc|worker_drain|claim_v1|queue_emails|http_post|net\.http|dblink\s*\(/i);assert.doesNotMatch(s,/insert into public\.(?:timesheets|timesheet_financials|contract_weeks|weekly_source_final|weekly_source_billing|weekly_source_projection|weekly_source_row_resolutions|invoices|invoice_lines)/i);}
 assert.equal(nativeCase('WS-NFX-HISTORICAL-2025').execution,'STATIC_ORACLE_ONLY');
 assert.equal(NATIVE_CASES.find(x=>x.kind==='SURREY_SINGLE_CYCLE').expected.lateInThisCyclePay,false);
});
