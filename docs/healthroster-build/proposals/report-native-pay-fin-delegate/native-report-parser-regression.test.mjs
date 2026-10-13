import assert from 'node:assert/strict';
import test from 'node:test';
import * as XLSX from 'xlsx';
import {parseWeeklySourceFile,WEEKLY_SOURCE_PROFILE_IDS} from './worker/index.js';
const common=['Request','Date','Start','End','Staff','Requested On','Invoicenumber','Invoicedate'];
const header=layout=>[...common,...(layout==='A'?['Net','VAT','Total','MSP Fee','MSPVAT']:['Actual Cost','Net to Agency','VAT to Agency','Total','MSP Fee','ABI Fee'])];
const data=(layout,requested='2025-04-03')=>['000123','2025-04-01','08:00:00','16:00:00','Synthetic worker',requested,'XYOXLR030425','03/04/25',...(layout==='A'?[222.31,44.462,266.772,'ignored','ignored']:['999.99','263.4','52.68','316.08','ignored','ignored'])];
function bytes(layout,rows=[data(layout)],sheet='Allocate'){const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([header(layout),...rows]),sheet);return XLSX.write(book,{type:'buffer',bookType:'xlsx'});}
test('Layout A is automatically detected and unrounded line VAT/total half-up without pay hours',async()=>{
 const result=await parseWeeklySourceFile(bytes('A'));
 assert.equal(result.ok,true);assert.equal(result.profileId,WEEKLY_SOURCE_PROFILE_IDS.ACACIUM_SELF_BILL_REPORT_V1);assert.equal(result.rows[0].requestId,'000123');
 assert.deepEqual(result.report,{reportNumber:'XYOXLR030425',invoiceDate:'2025-04-03',layout:'A',lineCount:1,netPence:'22231',vatPence:'4446',grossPence:'26677'});
 assert.equal(Object.hasOwn(result.rows[0],'actual'),false);assert.equal(result.rows[0].rowKind,'SOURCE_REPORT_LINE');assert.equal(result.rows[0].pricingEvidence.components.gross.rounding,'HALF_UP_EACH_REPORT_LINE_TOTAL');
});
test('Layout B uses Net to Agency and literal text pence; Actual Cost and fees are not authorities',async()=>{
 const result=await parseWeeklySourceFile(bytes('B'));
 assert.equal(result.ok,true);assert.equal(result.report.netPence,'26340');assert.equal(result.report.vatPence,'5268');assert.equal(result.report.grossPence,'31608');
 assert.equal(Object.hasOwn(result.resolvedColumnMap,'actualCost'),false);assert.equal(Object.hasOwn(result.resolvedColumnMap,'mspFee'),false);
});
test('all three Requested On forms resolve only their literal calendar/time',async()=>{
 for(const [source,expected] of [['2025-04-03','2025-04-03T00:00:00'],['03-04-2025 13:07','2025-04-03T13:07:00'],['Apr 3 2025 1:07PM','2025-04-03T13:07:00']]){
  const result=await parseWeeklySourceFile(bytes('A',[data('A',source)]));assert.equal(result.ok,true);assert.equal(result.rows[0].requestedOn,expected);
 }
});
test('negative report lines preserve sign; arithmetic mismatches remain warnings',async()=>{
 const row=data('A');row[8]=-222.31;row[9]=-44.462;row[10]=-266.772;
 const negative=await parseWeeklySourceFile(bytes('A',[row]));assert.equal(negative.ok,true);assert.equal(negative.report.netPence,'-22231');assert.equal(negative.rowCounts.negativeLines,1);
 const bad=data('B');bad[11]='999.99';const warning=await parseWeeklySourceFile(bytes('B',[bad]));assert.equal(warning.ok,true);assert.ok(warning.warnings.some(v=>v.code==='SELF_BILL_REPORT_ARITHMETIC_WARNING'));
});
test('duplicate Request Id, invalid dates and conflicting invoice scope fail before staging',async()=>{
 const dup=await parseWeeklySourceFile(bytes('A',[data('A'),data('A')]));assert.equal(dup.ok,false);assert.ok(dup.fatalErrors.some(v=>v.code==='WEEKLY_SOURCE_SELF_BILL_REPORT_DUPLICATE_REQUEST'));
 const invalid=data('A');invalid[7]='31/02/25';assert.equal((await parseWeeklySourceFile(bytes('A',[invalid]))).ok,false);
 const second=data('A');second[0]='000124';second[6]='XYOXLR100425';second[7]='10/04/25';const conflict=await parseWeeklySourceFile(bytes('A',[data('A'),second]));assert.equal(conflict.ok,false);assert.ok(conflict.fatalErrors.some(v=>v.code==='SELF_BILL_REPORT_SINGLE_INVOICE_REQUIRED'));
});
test('exact sheet/header and explicit profile remain strict, and report layout VAT mismatch refuses',async()=>{
 await assert.rejects(parseWeeklySourceFile(bytes('A',undefined,'Other')),e=>e.code==='SOURCE_PROFILE_NOT_RECOGNISED');
 await assert.rejects(parseWeeklySourceFile(bytes('A'),{profileId:WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1}),e=>e.code==='SOURCE_PROFILE_MISMATCH');
 await assert.rejects(parseWeeklySourceFile(bytes('A'),{ruleValues:{invoice_vat_method:'ONCE_ON_TOTAL_TRUNCATE'}}),e=>e.code==='WEEKLY_SOURCE_RULE_R11');
 const book=XLSX.utils.book_new();for(const name of ['Allocate','Second'])XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([header('A'),data('A')]),name);
 // Two qualifying report sheets require the exact Allocate name; a duplicate
 // report+export profile on a qualifying worksheet must still be ambiguous.
 const mixed=[...header('A'),'Actual Cost','Net to Agency','VAT to Agency'];const mixedBook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(mixedBook,XLSX.utils.aoa_to_sheet([mixed,[...data('A'),'222.31','222.31','44.46']]),'Allocate');
 await assert.rejects(parseWeeklySourceFile(XLSX.write(mixedBook,{type:'buffer',bookType:'xlsx'})),e=>e.code==='SOURCE_PROFILE_AMBIGUOUS');
});
