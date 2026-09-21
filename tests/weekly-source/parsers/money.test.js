import assert from 'node:assert/strict';
import test from 'node:test';

import {
  classifyNhspMoneyComponents,
  parseSourceMoneyCell,
  XLSX_BINARY64_SAME_VALUE_PENCE_V1,
} from '../../../broker/src/weekly-source/index.js';

function numeric(rawToken, coordinate = 'P4') {
  return { kind: 'NUMBER', rawToken, text: rawToken, coordinate, numericValue: Number(rawToken) };
}

function string(text, coordinate = 'P4') {
  return { kind: 'STRING', rawToken: text, text, coordinate, numericValue: null };
}

test('source money uses exact integer pence without binary floating-point authority', () => {
  assert.deepEqual(parseSourceMoneyCell(string(' £1,234.50\u00a0')), {
    pence: '123450',
    sign: 'POSITIVE',
  });
  assert.equal(parseSourceMoneyCell(string('1.2300')).pence, '123');
  assert.equal(parseSourceMoneyCell(numeric('-10.5')).pence, '-1050');
});

test('the named XLSX binary64 exception accepts only the one bit-identical penny value', () => {
  const result = parseSourceMoneyCell(numeric('69.069999999999993'));
  assert.equal(result.pence, '6907');
  assert.equal(result.normalisation, XLSX_BINARY64_SAME_VALUE_PENCE_V1);
  assert.throws(() => parseSourceMoneyCell(numeric('52.5001')), { code: 'SOURCE_MONEY_EXCESS_PRECISION' });
  assert.throws(() => parseSourceMoneyCell(string('69.069999999999993')), { code: 'SOURCE_MONEY_EXCESS_PRECISION' });
});

test('money parser refuses negative zero, formulas, exponents and malformed grouping', () => {
  assert.throws(() => parseSourceMoneyCell(string('-0.00')), { code: 'SOURCE_MONEY_NEGATIVE_ZERO' });
  assert.throws(() => parseSourceMoneyCell({ ...numeric('2'), kind: 'FORMULA' }), { code: 'SOURCE_MONEY_UNVERIFIABLE_CELL_TYPE' });
  assert.throws(() => parseSourceMoneyCell(string('1e2')), { code: 'SOURCE_MONEY_INVALID_LEXEME' });
  assert.throws(() => parseSourceMoneyCell(string('12,34.00')), { code: 'SOURCE_MONEY_INVALID_LEXEME' });
});

test('NHSP component signs classify only positive or full-negative rows', () => {
  assert.equal(classifyNhspMoneyComponents(numeric('52.50'), numeric('209.85', 'R4')).sourceTotalPence, '26235');
  assert.equal(classifyNhspMoneyComponents(numeric('-52.50'), numeric('-209.85', 'R4')).physicalSign, 'FULL_NEGATIVE');
  assert.throws(() => classifyNhspMoneyComponents(numeric('-1'), numeric('2', 'R4')), { code: 'NHSP_MIXED_SIGN_ROW' });
  assert.throws(() => classifyNhspMoneyComponents(numeric('0'), numeric('0', 'R4')), { code: 'NHSP_ZERO_VALUE_ROW' });
});

