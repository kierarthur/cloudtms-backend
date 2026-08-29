const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const localQuery = require('./fixtures/banking-pay-local-query.cjs');
const legacyOracle = require('./fixtures/banking-pay-legacy-oracle.cjs');
const amountCases = require('./fixtures/banking-pay-modal-amount-cases.cjs');
const sourceOracle = require('./fixtures/banking-pay-legacy-display-oracle.json');
const enabled = Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const literal = value => `'${JSON.stringify(value).replaceAll("'", "''")}'::jsonb`;
const sqlSource = fs.readFileSync(path.resolve(__dirname, '../supabase/repeatable/28082026_1303_banking_pay_modal_display_projection.sql'), 'utf8');

test('display projection is private, read-only and independent of pay calculation owners', () => {
  assert.doesNotMatch(sqlSource, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(sqlSource, /\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE)\b/i);
  assert.doesNotMatch(sqlSource, /\b(?:pay_batch_items|pay_advances|pay_advance_reservations)\b/i);
  assert.match(sqlSource, /BANKING_PAY_V2_INVALID_AMOUNT/);
});

test('real PG17 display scalars match all 19 frozen legacy families and certified-cent recovery cases', { skip: !enabled }, () => {
  const oracle = legacyOracle();
  const rows = amountCases.flatMap(fixture => fixture.rows.map((row, index) => ({ id: `${fixture.id}:${index}`, row })));
  rows.push(
    { id: 'parent-section-precedence', row: { presentation_role: 'PARENT', amount_display: '25.00', section_amount_display: '75.00' } },
    { id: 'nested-camel-precedence', row: { amount_ex_vat: '2.00', rowJson: { amountDisplay: '12.34' } } },
    { id: 'nested-section-precedence', row: { amount_display: '9.00', row_json: { sectionAmountDisplay: '21.09' } } },
    { id: 'manual-blocked-display', row: { line_type: 'MANUAL_DEBT_RECOVERY', amount_display: '0.00', recoverable_this_pay_run_ex_vat: '0.00', nominal_due_amount_ex_vat: '25.00', blocked_reason_codes: ['NO_PAY_HEADROOM'] } }
  );
  // Deterministic cent-valued current-authority inputs, including multiple
  // components, empty components, original-only fallbacks and cap exhaustion.
  for (let n = 0; n < 160; n++) {
    const cap = (n * 37 % 10003) / 100;
    const due = (n * 71 % 15001) / 100;
    const outstanding = (n * 97 % 20003) / 100;
    rows.push({ id: `recovery-cent-${n}`, row: {
      line_type: 'OVERPAYMENT_RECOVERY', amount_display: (-cap).toFixed(2),
      recoverable_this_pay_run_ex_vat: cap.toFixed(2),
      case_components: n % 5 === 0 ? [] : [
        { source_amount: outstanding.toFixed(2), target_outstanding_ex_vat: outstanding.toFixed(2), preview_due_amount_ex_vat: due.toFixed(2) },
        { source_amount: '12.34', preview_due_amount_ex_vat: '3.21' },
        ...(n % 3 === 0 ? [{ preview_due_amount_ex_vat: '999.00' }] : [])
      ]
    } });
  }
  const [actual] = localQuery(`SELECT jsonb_agg(jsonb_build_object('id',fixture->>'id','amount',
    private.pay_workbench_modal_line_display_amount_v2(fixture->'row')::text) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY AS data(fixture,ord);`);
  assert.equal(actual.length, rows.length);
  for (let i = 0; i < rows.length; i++) {
    assert.equal(actual[i].id, rows[i].id);
    assert.equal(Number(actual[i].amount).toFixed(2), Number(oracle.getPreviewLineDisplayAmount(rows[i].row)).toFixed(2), rows[i].id);
  }
});

test('real PG17 filter predicate matches the complete existing route vocabulary and nested identity scope', { skip: !enabled }, () => {
  const fixtures = [];
  const add = (row, filters = {}) => {
    for (const section of ['canonical_preview_lines','cases_resolutions','blocked_for_pay']) {
      for (const channel of ['ALL','PAYE','UMBRELLA']) {
        const oracle = legacyOracle(filters, channel);
        fixtures.push({ row, filters, section, channel,
          expected: oracle.rowMatchesActivePayFilters(row, section === 'cases_resolutions' ? 'CASES_RESOLUTIONS' : section === 'blocked_for_pay' ? 'BLOCKED_FOR_PAY' : 'READY_TO_PAY') });
      }
    }
  };
  const routeKeys = sourceOracle.snippets.filter(item => /^workbench.*RouteKeys$/.test(item.name))
    .flatMap(item => [...item.source.matchAll(/'([A-Za-z_]+)'/g)].map(match => match[1]));
  for (const key of routeKeys) for (const value of ['PAYE','paye only','PAYE_CHANNEL','UMBRELLA','umbrella-only','NON PAYE','NONPAYE','unknown','']) add({ [key]: value });
  for (const row of [
    { pay_channel: 'PAYE', source_pay_method: 'PAYE', current_target_pay_method: 'UMBRELLA' },
    { pay_channel: 'UMBRELLA', source_pay_method: 'UMBRELLA', current_target_pay_method: 'PAYE' },
    { pay_channel: 'PAYE', row_json: { pay_channel: 'UMBRELLA' } },
    { case_components: [{ source_basis_json: { target_pay_method: 'UMBRELLA' } }] }
  ]) add(row);
  for (let depth = 0; depth <= 9; depth++) {
    let row = { pay_channel: 'PAYE', candidate_id: 'candidate-A', client_id: 'client-A' };
    for (let level = 0; level < depth; level++) row = level % 2 ? { child: row } : [row];
    add(row, { candidate_id: 'candidate-A', client_id: 'client-A' });
    add(row, { candidate_id: 'candidate-B', client_id: 'client-A' });
  }
  for (const row of [{}, { candidateId: 'candidate-A', clientId: 'client-A' }, { candidate_id: 'candidate-B', client_id: 'client-A' }]) {
    add(row, { candidate_id: 'candidate-A', client_id: 'client-A' });
    add(row, { candidateId: 'candidate-A', clientId: 'client-A' });
  }
  for (let start = 0; start < fixtures.length; start += 400) {
    const batch = fixtures.slice(start, start + 400);
    const [actual] = localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_row_matches_scope_v2(
      f->'row', f->'filters', f->>'channel', f->>'section') ORDER BY ord)
      FROM jsonb_array_elements(${literal(batch)}) WITH ORDINALITY AS data(f,ord);`);
    assert.deepEqual(actual, batch.map(item => item.expected), `filter parity batch starting ${start}`);
  }
  assert.ok(fixtures.length > 2000, 'All existing route keys must be exercised');
});

test('real PG17 exact Timesheet traversal preserves aliases, deduplication and original depth boundary', { skip: !enabled }, () => {
  const oracle = legacyOracle();
  const a = '00000000-0000-4000-8000-000000000011';
  const b = '00000000-0000-4000-8000-000000000012';
  const rows = [
    { timesheet_id:a, linked_timesheet_id:a, affected_timesheet_ids:[a,b,'not-an-id'] },
    { rowJson:{ economicKey:{ timesheetId:a }, sourceBasisJson:{ linkedTimesheetId:b } } },
    { case_components:[{ linked_timesheet_id:a },{ source_basis_json:{ timesheet_id:b } }] },
    { ignored_unrecognised_container:{ timesheet_id:a } },
    { timesheet_ids:[], originalTimesheetId:b }, {}, []
  ];
  for (let depth=0;depth<=10;depth++) {
    let row={ timesheet_id:a };
    for (let level=0;level<depth;level++) row=level%2 ? { row_json:row } : [row];
    rows.push(row);
  }
  const [actual] = localQuery(`SELECT jsonb_agg(to_jsonb(private.pay_workbench_modal_related_timesheets_v2(row)) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY AS data(row,ord);`);
  assert.deepEqual(actual,rows.map(row=>Array.from(oracle.getRelatedTimesheetIds(row)).sort()));
});
