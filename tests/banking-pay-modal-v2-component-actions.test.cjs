const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const localQuery = require('./fixtures/banking-pay-local-query.cjs');
const enabled = Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const sourcePath = path.resolve(__dirname, '../supabase/repeatable/28082026_1448_banking_pay_modal_component_action_facts.sql');
const entry = { candidate_id: '00000000-0000-4000-8000-000000000031', case_key: 'fixture-case',
  finance_case_id: '00000000-0000-4000-8000-000000000032', linked_timesheet_id: '00000000-0000-4000-8000-000000000033', resolution_family: 'BUCKETED' };
const literal = value => `'${JSON.stringify(value).replaceAll("'", "''")}'::jsonb`;

test('component action projection is a private read-only presentation helper', () => {
  const source = fs.readFileSync(sourcePath, 'utf8');
  assert.match(source, /FUNCTION private\.pay_workbench_modal_component_actions_v2/);
  assert.match(source, /IMMUTABLE SECURITY INVOKER SET search_path TO ''/);
  assert.doesNotMatch(source, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(source, /\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE)\b/i);
  for (const action of ['componentUseSuggested', 'componentManualRate', 'componentManualAmount', 'componentClearResolution']) assert.ok(source.includes(action));
});

test('every component button combination agrees with the complete frozen renderer', { skip: !enabled }, () => {
  const oracle = require('./fixtures/banking-pay-legacy-oracle.cjs')();
  const fixtures = [];
  for (let mask = 0; mask < 256; mask++) {
    for (const state of ['', 'STALE', 'RESOLVED', 'FIXED', 'NOT_REQUIRED']) {
      fixtures.push({ needs_action: Boolean(mask & 1), show_suggested_rate: Boolean(mask & 2),
        suggested_available: Boolean(mask & 4), show_manual_rate_control: Boolean(mask & 8),
        show_manual_amount_control: Boolean(mask & 16), has_operator_choice: Boolean(mask & 32),
        is_fixed_reimbursement: Boolean(mask & 64), is_fixed_no_action_taxable_row: Boolean(mask & 128), resolution_state: state });
    }
  }
  for (let start = 0; start < fixtures.length; start += 160) {
    const batch = fixtures.slice(start, start + 160);
    const [actual] = localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_component_actions_v2(value) ORDER BY ord)
      FROM jsonb_array_elements(${literal(batch)}) WITH ORDINALITY AS f(value,ord);`);
    batch.forEach((component, index) => {
      const html = oracle.renderComponentRows({ ...entry, components: [component] });
      const expectedActions = [...html.matchAll(/data-action="([^"]+)"/g)].map(match => match[1]);
      assert.deepEqual(actual[index].actions, expectedActions, `button combination ${start + index}`);
      assert.equal(actual[index].needs_action, html.includes('class="pill pill-bad">Needs action</span>'));
      assert.equal(actual[index].fixed_no_action, Boolean(component.is_fixed_reimbursement || component.is_fixed_no_action_taxable_row || ['FIXED', 'NOT_REQUIRED'].includes(component.resolution_state)));
    });
  }
  assert.equal(fixtures.length, 1280);
});

test('strict booleans and existing truthy clear/fixed flags are not silently normalised differently', { skip: !enabled }, () => {
  const oracle = require('./fixtures/banking-pay-legacy-oracle.cjs')();
  const fixtures = [];
  for (const flag of [undefined, null, false, true, 0, 1, '', 'false', 'true', ' true ', [], {}]) {
    for (const key of ['needs_action', 'show_suggested_rate', 'suggested_available', 'show_manual_rate_control', 'show_manual_amount_control', 'has_operator_choice', 'is_fixed_reimbursement', 'is_fixed_no_action_taxable_row']) {
      fixtures.push({ show_suggested_rate: true, suggested_available: true, [key]: flag });
    }
  }
  fixtures.push({ resolution_state: ' resolved ' }, { resolution_state: ' stale ' }, { resolution_state: ' fixed ' }, { resolution_state: ' not_required ' });
  const [actual] = localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_component_actions_v2(value) ORDER BY ord)
    FROM jsonb_array_elements(${literal(fixtures)}) WITH ORDINALITY AS f(value,ord);`);
  fixtures.forEach((component, index) => {
    const html = oracle.renderComponentRows({ ...entry, components: [component] });
    assert.deepEqual(actual[index].actions, [...html.matchAll(/data-action="([^"]+)"/g)].map(match => match[1]), `flag fixture ${index}`);
    assert.equal(actual[index].needs_action, component.needs_action === true);
    assert.equal(actual[index].fixed_no_action, Boolean(component.is_fixed_reimbursement || component.is_fixed_no_action_taxable_row || ['FIXED', 'NOT_REQUIRED'].includes(String(component.resolution_state || '').trim().toUpperCase())));
  });
  assert.equal(fixtures.length, 100);
});

test('non-object components are ignored exactly as in the existing table', { skip: !enabled }, () => {
  const [actual] = localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_component_actions_v2(value) ORDER BY ord)
    FROM jsonb_array_elements('[null,false,0,"bad",[]]'::jsonb) WITH ORDINALITY AS f(value,ord);`);
  assert.deepEqual(actual, [null, null, null, null, null]);
});
