import { parseSourceMoneyCell, SOURCE_PENCE_MIN, SOURCE_PENCE_MAX } from './money.js';
import { failParser } from './errors.js';

const BUCKETS = ['day', 'night', 'sat', 'sun', 'bh'];
function integer(value, label) {
  if (!/^-?\d+$/.test(String(value ?? ''))) throw new Error(`${label} must be integer pence.`);
  const result = BigInt(value);
  if (result < SOURCE_PENCE_MIN || result > SOURCE_PENCE_MAX) throw new Error(`${label} is outside the source range.`);
  return result;
}
function decimal(value, label) {
  const match = /^(\d+)(?:\.(\d+))?$/.exec(String(value ?? ''));
  if (!match || String(value).length > 128) throw new Error(`${label} must be a non-negative decimal.`);
  return [BigInt(match[1] + (match[2] ?? '')), 10n ** BigInt((match[2] ?? '').length)];
}
function halfUp(numerator, denominator) {
  const sign = numerator < 0n ? -1n : 1n;
  const magnitude = numerator * sign;
  return sign * ((magnitude + denominator / 2n) / denominator);
}

/** Layout A report VAT and Total are unrounded literals. Other money keeps the
 * existing strict penny parser, including its exact XLSX binary64 exception. */
function parseReportRoundedCell(cell, rounding, label) {
  if (!cell || ['BLANK', 'FORMULA', 'ERROR', 'BOOLEAN', 'DATE'].includes(cell.kind)) {
    failParser('SOURCE_MONEY_UNVERIFIABLE_CELL_TYPE', `Report ${label} must be a literal amount.`, { coordinate: cell?.coordinate ?? null });
  }
  let token = String(cell.kind === 'NUMBER' ? cell.rawToken : cell.text ?? '').replace(/^[ \u00a0]+|[ \u00a0]+$/g, '');
  if (token.length > 128) failParser('SOURCE_MONEY_TOKEN_TOO_LONG', `Report ${label} exceeds the permitted token length.`);
  const negative = token.startsWith('-');
  if (negative) token = token.slice(1);
  if (token.startsWith('£')) token = token.slice(1);
  const match = /^(\d+|(?:[1-9]\d{0,2})(?:,\d{3})+)(?:\.(\d+))?$/.exec(token);
  if (!match) failParser('SOURCE_MONEY_INVALID_LEXEME', `Report ${label} is not in the permitted literal format.`);
  const [n, d] = decimal(match[1].replaceAll(',', '') + (match[2] ? `.${match[2]}` : ''), `Report ${label}`);
  const pence = halfUp((negative ? -n : n) * 100n, d);
  if (negative && pence === 0n) failParser('SOURCE_MONEY_NEGATIVE_ZERO', `Negative zero is not permitted in report ${label}.`);
  if (pence < SOURCE_PENCE_MIN || pence > SOURCE_PENCE_MAX) failParser('SOURCE_MONEY_OUT_OF_RANGE', `Report ${label} is outside the permitted range.`);
  return { pence: pence.toString(), rounding };
}

export const parseReportLineVatCell = (cell) => parseReportRoundedCell(cell, 'HALF_UP_EACH_REPORT_LINE', 'VAT');
export const parseReportLineTotalCell = (cell) => parseReportRoundedCell(cell, 'HALF_UP_EACH_REPORT_LINE_TOTAL', 'Total');

export function classifyExportSourceInvoiceMoney(netCell, grossCell) {
  const net = parseSourceMoneyCell(netCell);
  const gross = parseSourceMoneyCell(grossCell);
  const netPence = BigInt(net.pence), grossPence = BigInt(gross.pence);
  if (netPence < 0n || grossPence < 0n) failParser('HEALTHROSTER_SOURCE_MONEY_SIGN_INVALID', 'A worked export row cannot contain a negative source amount.');
  const vatPence = integer(grossPence - netPence, 'Source VAT');
  const fit = 5n * grossPence - 6n * netPence;
  return {
    netPence: net.pence, grossPence: gross.pence, vatPence: vatPence.toString(),
    vatFit: (fit < 0n ? -fit : fit) <= 5n,
    normalisations: [net.normalisation, gross.normalisation].filter(Boolean),
  };
}

/** Invoice VAT only. Canonical pay and presentation net are supplied by the
 * existing Weekly owner; neither is recomputed here. Exact-net gross uses its
 * stored minute buckets and actual charge rates, never rounded display hours. */
export function deriveWeeklySourceInvoiceAmounts({ netPence, lineVatMethod, vatRatePct,
  sourceGrossPence, sourceVatPence, bucketMinutes, chargeRates, sign = 1 }) {
  const net = integer(netPence, 'Invoice net');
  const [rate, rateScale] = decimal(vatRatePct, 'VAT rate');
  if (rate > 100n * rateScale || ![1, -1].includes(sign)) throw new Error('Invoice VAT authority is invalid.');
  let vat, gross;
  if (['LINE_GROSS_MINUS_NET_FROM_SOURCE', 'LINE_VAT_FROM_REPORT'].includes(lineVatMethod)) {
    gross = integer(sourceGrossPence, 'Source gross');
    vat = integer(sourceVatPence, 'Source VAT');
    if (net + vat !== gross) throw new Error('Source net plus VAT must equal gross.');
  } else if (lineVatMethod === 'ROUND_EACH_LINE_FROM_NET') {
    vat = halfUp(net * rate, 100n * rateScale); gross = net + vat;
  } else if (lineVatMethod === 'ROUND_TOTAL_FROM_EXACT_NET') {
    let exactNumerator = 0n, exactDenominator = 1n;
    for (const bucket of BUCKETS) {
      const minutes = integer(bucketMinutes?.[bucket], 'Stored bucket minutes');
      const [charge, scale] = decimal(chargeRates?.[bucket], 'Stored charge rate');
      if (minutes < 0n) throw new Error('Stored bucket minutes cannot be negative.');
      const denominator = 60n * scale;
      exactNumerator = exactNumerator * denominator + minutes * charge * exactDenominator;
      exactDenominator *= denominator;
    }
    gross = halfUp(BigInt(sign) * exactNumerator * (100n * rateScale + rate), exactDenominator * rateScale);
    vat = gross - net;
  } else throw new Error('Invoice line VAT method is unavailable.');
  integer(vat, 'Invoice VAT'); integer(gross, 'Invoice gross');
  return Object.freeze({ netPence: net.toString(), vatPence: vat.toString(), grossPence: gross.toString() });
}

export function invoiceVatRoundingPence(lines, method, vatRatePct) {
  const [rate, scale] = decimal(vatRatePct, 'VAT rate');
  if (rate > 100n * scale) throw new Error('Invoice VAT rate is invalid.');
  let net = 0n, vat = 0n;
  for (const line of lines) { net += integer(line.netPence, 'Line net'); vat += integer(line.vatPence, 'Line VAT'); }
  if (method === 'SUM_OF_LINES') return '0';
  const numerator = net * rate, denominator = 100n * scale;
  const target = method === 'ONCE_ON_TOTAL_HALF_UP' ? halfUp(numerator, denominator)
    : method === 'ONCE_ON_TOTAL_TRUNCATE' ? numerator / denominator : null;
  if (target === null) throw new Error('Invoice VAT total method is unavailable.');
  return integer(target - vat, 'VAT rounding').toString();
}
