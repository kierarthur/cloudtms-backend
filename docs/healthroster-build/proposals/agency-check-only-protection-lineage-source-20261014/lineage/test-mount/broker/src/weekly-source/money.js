import { failParser } from './errors.js';

export const XLSX_BINARY64_SAME_VALUE_PENCE_V1 = 'XLSX_BINARY64_SAME_VALUE_PENCE_V1';
export const SOURCE_PENCE_MIN = -999_999_999_999n;
export const SOURCE_PENCE_MAX = 999_999_999_999n;

function trimOuterSourceSpaces(value) {
  return String(value ?? '').replace(/^[ \u00a0]+|[ \u00a0]+$/g, '');
}

function binary64Bits(value) {
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setFloat64(0, value, false);
  return `${view.getUint32(0, false).toString(16).padStart(8, '0')}${view.getUint32(4, false).toString(16).padStart(8, '0')}`;
}

function exactDecimalParts(token) {
  const match = /^(-)?(\d+)(?:\.(\d+))?$/.exec(token);
  if (!match) return null;
  return {
    negative: Boolean(match[1]),
    whole: match[2],
    fraction: match[3] ?? '',
  };
}

function enforceRange(pence, coordinate) {
  if (pence < SOURCE_PENCE_MIN || pence > SOURCE_PENCE_MAX) {
    failParser('SOURCE_MONEY_OUT_OF_RANGE', 'Source money is outside the permitted range.', { coordinate });
  }
}

function resolveBinary64Pence(rawNumericToken, parts) {
  const sign = parts.negative ? -1n : 1n;
  const scale = 10n ** BigInt(parts.fraction.length);
  const magnitude = (BigInt(parts.whole) * scale) + BigInt(parts.fraction || '0');
  const scaled = magnitude * 100n;
  const lowerMagnitude = scaled / scale;
  const remainder = scaled % scale;
  if (remainder === 0n) return null;

  const towardZero = sign * lowerMagnitude;
  const awayFromZero = sign * (lowerMagnitude + 1n);
  const rawBits = binary64Bits(Number(rawNumericToken));
  const candidates = [towardZero, awayFromZero].filter((candidate) => (
    binary64Bits(Number(candidate) / 100) === rawBits
  ));
  return candidates.length === 1 ? candidates[0] : null;
}

export function parseSourceMoneyCell(cell, { coordinate = cell?.coordinate ?? null } = {}) {
  if (!cell || cell.kind === 'BLANK') {
    failParser('SOURCE_MONEY_BLANK', 'A required source money cell is blank.', { coordinate });
  }
  if (['FORMULA', 'ERROR', 'BOOLEAN', 'DATE'].includes(cell.kind)) {
    failParser('SOURCE_MONEY_UNVERIFIABLE_CELL_TYPE', 'A required source money cell is not an auditable literal.', {
      coordinate,
      cellKind: cell.kind,
    });
  }

  const numericCell = cell.kind === 'NUMBER';
  let token = trimOuterSourceSpaces(numericCell ? cell.rawToken : cell.text);
  if (token === '') failParser('SOURCE_MONEY_BLANK', 'A required source money cell is blank.', { coordinate });
  if (token.length > 128) failParser('SOURCE_MONEY_TOKEN_TOO_LONG', 'Source money exceeds the permitted token length.', { coordinate });
  if (token.includes('\u2212') || token.includes('\u2013') || token.includes('\u2014')) {
    failParser('SOURCE_MONEY_INVALID_SIGN', 'Source money uses an unsupported sign.', { coordinate });
  }

  let negative = false;
  if (token.startsWith('-')) {
    negative = true;
    token = token.slice(1);
  }
  if (token.startsWith('£')) token = token.slice(1);
  if (!token || /[+()\s]/u.test(token) || /[eE]/.test(token)) {
    failParser('SOURCE_MONEY_INVALID_LEXEME', 'Source money is not in the permitted literal format.', { coordinate });
  }

  const parts = /^(\d+|(?:[1-9]\d{0,2})(?:,\d{3})+)(?:\.(\d+))?$/.exec(token);
  if (!parts) failParser('SOURCE_MONEY_INVALID_LEXEME', 'Source money is not in the permitted literal format.', { coordinate });

  const integerDigits = parts[1].replaceAll(',', '');
  const fraction = parts[2] ?? '';
  const canonicalNumericToken = `${negative ? '-' : ''}${integerDigits}${fraction ? `.${fraction}` : ''}`;
  const decimalParts = exactDecimalParts(canonicalNumericToken);
  let pence;
  let normalisation = null;

  if (fraction.length <= 2 || /^\d{0,2}0*$/.test(fraction)) {
    const firstTwo = `${fraction}00`.slice(0, 2);
    pence = BigInt(integerDigits) * 100n + BigInt(firstTwo);
    if (negative) pence = -pence;
  } else if (numericCell) {
    pence = resolveBinary64Pence(canonicalNumericToken, decimalParts);
    if (pence === null) {
      failParser('SOURCE_MONEY_EXCESS_PRECISION', 'Source money has unverifiable precision beyond pennies.', { coordinate });
    }
    normalisation = XLSX_BINARY64_SAME_VALUE_PENCE_V1;
  } else {
    failParser('SOURCE_MONEY_EXCESS_PRECISION', 'Source money has precision beyond pennies.', { coordinate });
  }

  if (negative && pence === 0n) {
    failParser('SOURCE_MONEY_NEGATIVE_ZERO', 'Negative zero is not permitted in source money.', { coordinate });
  }
  enforceRange(pence, coordinate);
  return {
    pence: pence.toString(),
    sign: pence < 0n ? 'NEGATIVE' : pence > 0n ? 'POSITIVE' : 'ZERO',
    ...(normalisation ? { normalisation } : {}),
  };
}

export function classifyNhspMoneyComponents(commissionCell, totalCostCell) {
  const commission = parseSourceMoneyCell(commissionCell);
  const totalCost = parseSourceMoneyCell(totalCostCell);
  const commissionPence = BigInt(commission.pence);
  const totalCostPence = BigInt(totalCost.pence);
  const sourceTotalPence = commissionPence + totalCostPence;

  if (sourceTotalPence === 0n && commissionPence === 0n && totalCostPence === 0n) {
    failParser('NHSP_ZERO_VALUE_ROW', 'An NHSP source row cannot have zero Commission and zero Total Cost.', {
      commissionCoordinate: commissionCell.coordinate,
      totalCostCoordinate: totalCostCell.coordinate,
    });
  }
  const hasPositive = commissionPence > 0n || totalCostPence > 0n;
  const hasNegative = commissionPence < 0n || totalCostPence < 0n;
  if (hasPositive && hasNegative) {
    failParser('NHSP_MIXED_SIGN_ROW', 'NHSP Commission and Total Cost must have a consistent sign.', {
      commissionCoordinate: commissionCell.coordinate,
      totalCostCoordinate: totalCostCell.coordinate,
    });
  }
  if (sourceTotalPence === 0n) {
    failParser('NHSP_ZERO_SUM_ROW', 'NHSP Commission and Total Cost cannot cancel to zero.', {
      commissionCoordinate: commissionCell.coordinate,
      totalCostCoordinate: totalCostCell.coordinate,
    });
  }
  enforceRange(sourceTotalPence, commissionCell.coordinate);
  return {
    commissionPence: commission.pence,
    totalCostPence: totalCost.pence,
    sourceTotalPence: sourceTotalPence.toString(),
    physicalSign: sourceTotalPence < 0n ? 'FULL_NEGATIVE' : 'POSITIVE',
    normalisations: [commission.normalisation, totalCost.normalisation].filter(Boolean),
    parsedComponents: { commission, totalCost },
  };
}

export function parseSourceFixedExpenseCell(cell, { coordinate = cell?.coordinate ?? null } = {}) {
  if (!cell || cell.kind === 'BLANK') return { pence: '0', state: 'OMITTED_ZERO' };
  if (cell.kind !== 'STRING') {
    failParser('SOURCE_EXPENSE_UNVERIFIABLE_CELL_TYPE', 'Source-fixed expense must be a literal CSV or spreadsheet text/number token.', {
      coordinate,
      cellKind: cell.kind,
    });
  }
  let token = String(cell.text ?? '').replace(/^[ \t\u00a0]+|[ \t\u00a0]+$/g, '');
  if (token === '') return { pence: '0', state: 'OMITTED_ZERO' };
  if (token.length > 128 || token.startsWith('-') || token.includes('\u2212')) {
    failParser('SOURCE_EXPENSE_INVALID_LEXEME', 'Source-fixed expense is not in the permitted non-negative format.', { coordinate });
  }
  if (token.startsWith('£')) token = token.slice(1);
  if (!token || /[+()\s\-]/u.test(token) || /[eE]/.test(token)) {
    failParser('SOURCE_EXPENSE_INVALID_LEXEME', 'Source-fixed expense is not in the permitted non-negative format.', { coordinate });
  }
  const parts = /^(\d+|(?:[1-9]\d{0,2})(?:,\d{3})+)(?:\.(\d{1,2}))?$/.exec(token);
  if (!parts) failParser('SOURCE_EXPENSE_INVALID_LEXEME', 'Source-fixed expense is not in the permitted non-negative format.', { coordinate });
  const pence = (BigInt(parts[1].replaceAll(',', '')) * 100n) + BigInt(`${parts[2] ?? ''}00`.slice(0, 2));
  enforceRange(pence, coordinate);
  return { pence: pence.toString(), state: 'PRESENT' };
}
