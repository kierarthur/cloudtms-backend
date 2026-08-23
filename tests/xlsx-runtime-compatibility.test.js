import test from 'node:test';
import assert from 'node:assert/strict';
import * as XLSX from 'xlsx';

test('SheetJS 0.20.3 preserves CloudTMS workbook row and date parsing semantics', () => {
  const rows = [
    ['Worker', 'Date', 'Start', 'Break minutes'],
    ['Synthetic Worker', 46257, 0.3541666666666667, 30],
    ['Synthetic Worker', '23/08/2026', '08:30', '45']
  ];
  const sheet = XLSX.utils.aoa_to_sheet(rows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, sheet, 'Rota');
  const bytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });

  const parsed = XLSX.read(bytes, { type: 'array' });
  const parsedRows = XLSX.utils.sheet_to_json(parsed.Sheets.Rota, {
    header: 1,
    raw: true,
    defval: ''
  });

  assert.deepEqual(parsedRows, rows);
  assert.deepEqual(XLSX.SSF.parse_date_code(46257), {
    D: 46257,
    T: 0,
    u: 0,
    y: 2026,
    m: 8,
    d: 23,
    H: 0,
    M: 0,
    S: 0,
    q: 0
  });
  const time = XLSX.SSF.parse_date_code(0.3541666666666667);
  assert.equal(time.H, 8);
  assert.equal(time.M, 30);
});
