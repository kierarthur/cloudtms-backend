import test from 'node:test';
import assert from 'node:assert/strict';
import * as XLSX from 'xlsx';
import { parseSourceMoneyCell } from '../../broker/src/weekly-source/money.js';
import { classifyExportSourceInvoiceMoney, parseReportLineVatCell, parseReportLineTotalCell,
  deriveWeeklySourceInvoiceAmounts, invoiceVatRoundingPence } from '../../broker/src/weekly-source/source-invoice-money.js';
import { parseSourceFinalisedTimeCell } from '../../broker/src/weekly-source/source-finalised-time.js';
import { compareWeeklySourceShiftPrice } from '../../broker/src/weekly-source/source-price-comparator.js';
import { buildWeeklySourceCanonicalEconomicSnapshot } from '../../broker/src/weekly-source/economic-snapshot.js';
import { parseWeeklySourceFile } from '../../broker/src/weekly-source/index.js';
import { adaptWeeklySourceParserOutput } from '../../broker/src/weekly-source/upload-staging-adapter.mjs';

const literal = (text) => ({ kind: 'STRING', text, rawToken: text, coordinate: 'A2' });
const rates = { day: 12, night: 12, sat: 12, sun: 12, bh: 12 };
const financialInput = { mode: 'SPLIT_RATE_WINDOWS', bucketMinutes: { day: 60, night: 0, sat: 0, sun: 0, bh: 0 }, payRates: rates, chargeRates: rates };
const enabled = { values: { invoice_sender: 'CLIENT_SELF_BILL', invoice_amount_basis: 'SOURCE_AMOUNT',
  source_amount_basis: 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY', invoice_membership: 'FINALISED_IN_CYCLE_WINDOW' } };

test('VAT-fit boundary is inclusive one penny and flags larger source disparity', () => {
  assert.equal(classifyExportSourceInvoiceMoney(literal('100'), literal('120.01')).vatFit, true);
  assert.equal(classifyExportSourceInvoiceMoney(literal('100'), literal('119.99')).vatFit, true);
  assert.equal(classifyExportSourceInvoiceMoney(literal('100'), literal('120.02')).vatFit, false);
  const row = classifyExportSourceInvoiceMoney(literal('263.4'), literal('316.08'));
  assert.equal(row.netPence, '26340'); assert.equal(row.vatPence, '5268'); assert.equal(row.grossPence, '31608');
});
test('report-only unrounded VAT and Total each HALF_UP and strict money remains unchanged', () => {
  for (const [token, expected] of [['2.345', '235'], ['-2.345', '-235'], ['2.3449', '234'], ['263.4', '26340'], ['£1,234.005', '123401']]) {
    assert.equal(parseReportLineVatCell(literal(token)).pence, expected);
    assert.equal(parseReportLineTotalCell(literal(token)).pence, expected);
  }
  assert.equal(parseReportLineTotalCell(literal('2.345')).rounding, 'HALF_UP_EACH_REPORT_LINE_TOTAL');
  assert.throws(() => parseSourceMoneyCell(literal('2.345')), { code: 'SOURCE_MONEY_EXCESS_PRECISION' });
  for (const kind of ['FORMULA','ERROR','BOOLEAN','DATE']) assert.throws(() => parseReportLineVatCell({ ...literal('1.01'), kind }));
  for (const token of ['-0.0001', '1e2', '(1.005)', '1 000.005', '10000000000']) assert.throws(() => parseReportLineTotalCell(literal(token)));
});
test('exact stored minutes determine gross without recalculating rounded presentation net or pay', () => {
  // Independently priced buckets: one minute at £37.13 plus two at £41.27.
  // Rounded display hours .02/.03 give £1.98; exact minutes give £1.9945.
  const output = deriveWeeklySourceInvoiceAmounts({ netPence: '198', lineVatMethod: 'ROUND_TOTAL_FROM_EXACT_NET', vatRatePct: '20',
    bucketMinutes: { day: 1, night: 2, sat: 0, sun: 0, bh: 0 },
    chargeRates: { day: '37.13', night: '41.27', sat: '1', sun: '1', bh: '1' } });
  assert.deepEqual(output, { netPence: '198', vatPence: '41', grossPence: '239' });
  assert.notEqual(output.grossPence, deriveWeeklySourceInvoiceAmounts({ netPence: '198', lineVatMethod: 'ROUND_EACH_LINE_FROM_NET', vatRatePct: '20' }).grossPence);
  assert.equal(BigInt(output.netPence) + BigInt(output.vatPence), BigInt(output.grossPence));
});
test('source VAT/gross are preserved and invoice-total rounding is one signed delta', () => {
  assert.deepEqual(deriveWeeklySourceInvoiceAmounts({ netPence: '10001', lineVatMethod: 'LINE_GROSS_MINUS_NET_FROM_SOURCE',
    sourceVatPence: '2001', sourceGrossPence: '12002', vatRatePct: '20' }), { netPence: '10001', vatPence: '2001', grossPence: '12002' });
  const lines = [{ netPence: '103', vatPence: '21' }, { netPence: '103', vatPence: '21' }];
  assert.equal(invoiceVatRoundingPence(lines, 'SUM_OF_LINES', '20'), '0');
  assert.equal(invoiceVatRoundingPence(lines, 'ONCE_ON_TOTAL_HALF_UP', '20'), '-1');
  assert.equal(invoiceVatRoundingPence(lines, 'ONCE_ON_TOTAL_TRUNCATE', '20'), '-1');
  assert.throws(() => deriveWeeklySourceInvoiceAmounts({ netPence:'100', lineVatMethod:'LINE_VAT_FROM_REPORT', sourceVatPence:'20', sourceGrossPence:'121', vatRatePct:'20' }));
});
test('SOURCE_AMOUNT difference is informational and canonical pay/economics are unchanged', () => {
  const legacy = compareWeeklySourceShiftPrice({ sourceMode:'HEALTHROSTER_WEEKLY', financialInput });
  const source = compareWeeklySourceShiftPrice({ sourceMode:'HEALTHROSTER_WEEKLY', financialInput, sourceChargePence:'1400', effectiveRuleSet:enabled });
  assert.equal(source.result, 'SOURCE_AMOUNT_DIFFERS'); assert.equal(source.requiresOfficeAcceptance, false);
  assert.equal(source.invoicePresentationChargePence, '1400'); assert.equal(source.canonicalPayPence, legacy.canonicalPayPence);
  const resolvedFacts = { sourceMode:'HEALTHROSTER_WEEKLY', rateMethod:'SPLIT_RATE_WINDOWS', payRates:rates, chargeRates:rates };
  assert.deepEqual(buildWeeklySourceCanonicalEconomicSnapshot({ resolvedFacts:{ ...resolvedFacts, effectiveRuleSet:enabled }, priceComparison:source }),
    buildWeeklySourceCanonicalEconomicSnapshot({ resolvedFacts, priceComparison:legacy }));
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({ resolvedFacts, priceComparison:source }));
  for (const rule of [null, { values:{ ...enabled.values, invoice_sender:'AGENCY_INVOICE' } }]) {
    assert.deepEqual(compareWeeklySourceShiftPrice({ sourceMode:'HEALTHROSTER_WEEKLY', financialInput, sourceChargePence:'1400', effectiveRuleSet:rule }), legacy);
  }
  for (const [pence, result] of [['1200','EXACT'],['1201','SOURCE_ROUNDING_EQUIVALENT'],['1199','SOURCE_ROUNDING_EQUIVALENT'],['1202','SOURCE_AMOUNT_DIFFERS']]) {
    assert.equal(compareWeeklySourceShiftPrice({ sourceMode:'HEALTHROSTER_WEEKLY', financialInput, sourceChargePence:pence, effectiveRuleSet:enabled }).result,result);
  }
});
test('Finalised Date maps a real London wall clock and refuses DST ambiguity/gaps', () => {
  assert.equal(parseSourceFinalisedTimeCell(literal('24/06/2026 23:59:00')).finalisedAtUtc, '2026-06-24T22:59:00.000Z');
  assert.equal(parseSourceFinalisedTimeCell(literal('2026-12-01 12:00')).finalisedAtUtc, '2026-12-01T12:00:00.000Z');
  assert.equal(parseSourceFinalisedTimeCell(literal('25/10/2026 01:30')).code, 'HEALTHROSTER_FINALISED_DATE_AMBIGUOUS');
  assert.equal(parseSourceFinalisedTimeCell(literal('29/03/2026 01:30')).code, 'HEALTHROSTER_FINALISED_DATE_NONEXISTENT');
  assert.equal(parseSourceFinalisedTimeCell(literal('31/02/2026 12:00')).ok, false);
  assert.equal(parseSourceFinalisedTimeCell(literal('2026-06-24')).ok, false);
});
test('physical export parser ignores all fees and enables new evidence only from resolved rules', async () => {
  const headers = ['Request Id','Staff','Date','From','To','Break','Start','End','Actual Break','Hours','Finalised Date','Timesheet Finalised By','Agency Worker Pay','Actual Cost','Agency Commission Amount'];
  const sheet = XLSX.utils.aoa_to_sheet([headers, ['00123','Worker','2026-06-20','09:00','17:00',0,'09:00','17:00',0,'8:00','24/06/2026 23:59','Manager','100.00','120.02','SECRET FEE IGNORE']]);
  sheet.P2 = { t:'n',f:'1/0',v:0 };
  const book = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(book,sheet,'Export');
  const bytes = XLSX.write(book,{ type:'buffer',bookType:'xlsx' });
  const options = { profileId:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1', expectedClient:'Client' };
  const legacy = await parseWeeklySourceFile(bytes,options);
  assert.equal(legacy.ok,true); assert.equal(legacy.rows[0].invoiceMoney,undefined);
  const parsed = await parseWeeklySourceFile(bytes,{ ...options,effectiveRuleSet:enabled });
  assert.equal(parsed.ok,true); assert.equal(parsed.rows[0].requestId,'00123');
  assert.equal(parsed.rows[0].invoiceMoney.vatFit,false); assert.equal(parsed.rows[0].finalisedAtUtc,'2026-06-24T22:59:00.000Z');
  assert.equal(parsed.resolvedColumnMap.sourceNet.column,'M'); assert.equal(parsed.resolvedColumnMap.sourceGross.column,'N');
  assert.equal(parsed.resolvedColumnMap.fee,undefined);
});
