import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { compareCompleteNativeComponents } from '../../docs/healthroster-build/proposals/automatic-native-delta/reference-comparison.mjs';

const lane = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const root = process.env.HEALTHROSTER_NATIVE_REVIEW_ROOT
  ? resolve(process.env.HEALTHROSTER_NATIVE_REVIEW_ROOT) : resolve(lane, '../healthroster-v2-backend-13102026');
const proposal = resolve(lane, 'docs/healthroster-build/proposals/automatic-native-delta');
const read = path => readFileSync(resolve(proposal, path), 'utf8');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const native = await import(pathToFileURL(resolve(root, 'broker/src/banking-pay/weekly-source-c1-components.mjs')));
const ids = ['00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000003', '00000000-0000-4000-8000-000000000004'];
function worked(id, pay = '80.00') {
  return { component_id: id, component_kind: 'WORKED_TIME', economic_key_type: 'SEGMENT',
    economic_key_value: `weekly-source-event:${id}`, component_member_identity: id,
    segment_id: `weekly-source-event:${id}`, segment_key: id, segment_stable_key: 'immutable-calc-before',
    work_date: '2026-10-12', reference_number: '000012', additional_code_raw: null, expense_code: null,
    exclude_from_pay: false, origin: 'WEEKLY_SOURCE', movement_id: null, movement_group_id: null,
    hours_day: '8.000000', hours_night: '0.000000', hours_sat: '0.000000', hours_sun: '0.000000', hours_bh: '0.000000',
    unit_count: null, unit_pay_rate: null, unit_charge_rate: null, pay_ex_vat: pay, charge_ex_vat: '160.00' };
}
function detail() {
  return { work_date: '2026-10-12', start: '08:00', end: '16:00', overnight: false,
    breaks: [], start_utc: '2026-10-12T07:00:00Z', end_utc: '2026-10-12T15:00:00Z',
    rates: { day: '10.000000', night: '12.000000', sat: '14.000000', sun: '16.000000', bh: '18.000000' },
    charge_rates: { day: '20.000000', night: '22.000000', sat: '24.000000', sun: '26.000000', bh: '28.000000' } };
}
function pair(id, before = detail(), next = structuredClone(before)) { return new Map([[id, { before, next }]]); }
function fail(code, fn) { assert.throws(fn, error => error.code === code); }
function syntheticAdditional(id, overrides = {}) {
  return { ...worked(id), component_kind: 'ADDITIONAL_UNIT', economic_key_type: 'ADDITIONAL_CODE', economic_key_value: 'ONCALL',
    component_member_identity: 'additional:bk-test:ONCALL', segment_id: null, segment_key: null, segment_stable_key: null,
    work_date: null, reference_number: null, additional_code_raw: 'ONCALL', origin: 'TIMESHEET_ADDITIONAL',
    hours_day: null, hours_night: null, hours_sat: null, hours_sun: null, hours_bh: null,
    unit_count: '2.000000', unit_pay_rate: '25.000000', unit_charge_rate: '40.000000', pay_ex_vat: '50.00', charge_ex_vat: '80.00', ...overrides };
}

test('complete native stable components accept unchanged financial facts independently of array order', () => {
  const before = [worked(ids[0]), worked(ids[1])];
  const details = new Map([...pair(ids[0]), ...pair(ids[1])]);
  assert.deepEqual(compareCompleteNativeComponents(before, structuredClone(before).reverse(), details), { component_count: 2, delta_pay_ex_vat: '0.00' });
});
test('equal total pay cannot conceal compensating line deltas', () => {
  const before = [worked(ids[0]), worked(ids[1])]; const next = structuredClone(before);
  next[0].pay_ex_vat = '79.99'; next[1].pay_ex_vat = '80.01';
  fail('COMPONENT_DELTA_NONZERO', () => compareCompleteNativeComponents(before, next, new Map([...pair(ids[0]), ...pair(ids[1])])));
});
test('equal elapsed hours cannot conceal a band redistribution', () => {
  const before = [worked(ids[0])]; const next = structuredClone(before);
  next[0].hours_day = '7.000000'; next[0].hours_night = '1.000000';
  fail('COMPONENT_DELTA_NONZERO', () => compareCompleteNativeComponents(before, next, pair(ids[0])));
});
test('equal pay and charge cannot conceal a changed approved pay rate', () => {
  const before = [worked(ids[0])]; const old = detail(); const next = structuredClone(old); next.rates.day = '10.000001';
  fail('APPROVED_RATE_CHANGED', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, next)));
});
test('an unused saved rate remains part of frozen rate proof', () => {
  const before = [worked(ids[0])]; const old = detail(); const next = structuredClone(old); next.rates.bh = '19.000000';
  fail('APPROVED_RATE_CHANGED', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, next)));
});
test('equal charge cannot conceal a changed historical charge rate', () => {
  const before = [worked(ids[0])]; const old = detail(); const next = structuredClone(old); next.charge_rates.day = '21.000000';
  fail('APPROVED_RATE_CHANGED', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, next)));
});
test('missing used rates are unknown, even if line totals look unchanged', () => {
  const before = [worked(ids[0])]; const old = detail(); old.rates.day = null;
  fail('APPROVED_RATE_UNAVAILABLE', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, old)));
});
test('clock changes with unchanged quantities still refuse', () => {
  const before = [worked(ids[0])]; const old = detail(); const next = structuredClone(old); next.start = '09:00'; next.end = '17:00';
  fail('APPROVED_DETAIL_CHANGED', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, next)));
});
test('a changed multipart break witness refuses', () => {
  const before = [worked(ids[0])]; const old = detail(); old.breaks = [{ start: '12:00', end: '12:30' }];
  const next = structuredClone(old); next.breaks[0].start = '13:00'; next.breaks[0].end = '13:30';
  fail('APPROVED_DETAIL_CHANGED', () => compareCompleteNativeComponents(before, before, pair(ids[0], old, next)));
});
test('economic identity is native component identity, never same-date matching', () => {
  const before = [worked(ids[0])]; const next = [worked(ids[1])];
  fail('COMPONENT_SET_CHANGED', () => compareCompleteNativeComponents(before, next, pair(ids[0])));
});
test('duplicate native component identities cannot become an apparently complete set', () => {
  fail('AMBIGUOUS_COMPONENT_ID', () => compareCompleteNativeComponents([worked(ids[0]), worked(ids[0])], [worked(ids[0])], pair(ids[0])));
});
test('nullable charge is not a guessed zero', () => {
  const before = [worked(ids[0])]; const next = structuredClone(before); next[0].charge_ex_vat = null;
  fail('COMPONENT_DELTA_NONZERO', () => compareCompleteNativeComponents(before, next, pair(ids[0])));
});
test('unknown decimal and precision-changing values fail closed', () => {
  for (const invalid of [undefined, 'NaN', 'Infinity', '80.001', '-0.00', 80]) {
    const before = [worked(ids[0])]; const next = structuredClone(before); next[0].pay_ex_vat = invalid;
    assert.throws(() => compareCompleteNativeComponents(before, next, pair(ids[0])));
  }
});
test('lexically different exact amounts do not need a financial calculator', () => {
  const before = [worked(ids[0])]; const next = structuredClone(before); next[0].pay_ex_vat = '80.000';
  assert.equal(compareCompleteNativeComponents(before, next, pair(ids[0])).delta_pay_ex_vat, '0.00');
});
test('additional quantities and saved rates are independently preserved', () => {
  const before = [syntheticAdditional(ids[2])];
  for (const field of ['unit_count', 'unit_pay_rate', 'unit_charge_rate']) {
    const next = structuredClone(before); next[0][field] = '999.000000';
    fail('COMPONENT_DELTA_NONZERO', () => compareCompleteNativeComponents(before, next, new Map()));
  }
});
test('charge-only zero-pay additional remains in complete I1 population', () => {
  const before = [syntheticAdditional(ids[2], { pay_ex_vat: '0.00', unit_pay_rate: '0.000000' })];
  assert.equal(compareCompleteNativeComponents(before, before, new Map()).component_count, 1);
  fail('COMPONENT_SET_CHANGED', () => compareCompleteNativeComponents(before, [], new Map()));
});
test('expense policy cannot change while the reimbursed amount stays the same', () => {
  const before = [syntheticAdditional(ids[2])];
  fail('EXPENSE_POLICY_CHANGED', () => compareCompleteNativeComponents(before, before, new Map(), [{
    before: { source_expense_vat_enabled: false, correction_presentation: 'NORMAL' },
    next: { source_expense_vat_enabled: true, correction_presentation: 'NORMAL' },
  }]));
});
test('movement ownership and references are not discarded for a zero-pay claim', () => {
  const before = [worked(ids[0])];
  for (const field of ['movement_id', 'reference_number', 'component_member_identity']) {
    const next = structuredClone(before); next[0][field] = ids[3];
    fail('COMPONENT_IDENTITY_CHANGED', () => compareCompleteNativeComponents(before, next, pair(ids[0])));
  }
});

test('real native C1 carrier uses a transport UUID and TS_DAY, distinct from I1 SEGMENT identity', () => {
  const snapshot = { timesheet_id: ids[3], invoice_breakdown_json: { mode: 'SEGMENTS', segments: [{
    date: '2026-10-12', segment_id: `weekly-source-event:${ids[0]}`, segment_key: ids[0], segment_stable_key: 'transport-calc',
    ref_num: '000012', hours_day: '8', hours_night: '0', hours_sat: '0', hours_sun: '0', hours_bh: '0',
    pay_amount: '80.00', charge_amount: '160.00', exclude_from_pay: false,
  }] }, additional_units_json: {} };
  const result = native.buildWeeklySourceC1Components({ root_timesheet_id: ids[3], financial_snapshot: snapshot, component_id_for_key: () => ids[1] });
  assert.equal(result[0].component.component_id, ids[1]); assert.notEqual(result[0].component.component_id, ids[0]);
  assert.equal(result[0].component.economic_key_type, 'TS_DAY'); assert.equal(result[0].component.economic_key_value, '2026-10-12');
  assert.equal(result[0].component.component_member_identity, 'transport-calc');
  assert.equal(worked(ids[0]).economic_key_type, 'SEGMENT'); assert.equal(worked(ids[0]).component_member_identity, ids[0]);
});
test('native carrier preserves the leading-zero reference and exact stored monetary facts', () => {
  const snapshot = { timesheet_id: ids[3], invoice_breakdown_json: { mode: 'SEGMENTS', segments: [{
    date: '2026-10-12', segment_id: `weekly-source-event:${ids[0]}`, ref_num: '000012',
    hours_day: '8', pay_amount: '80.01', charge_amount: '160.03',
  }] }, additional_units_json: {} };
  const row = native.buildWeeklySourceC1Components({ root_timesheet_id: ids[3], financial_snapshot: snapshot, component_id_for_key: () => ids[1] })[0].component;
  assert.equal(row.reference_number, '000012'); assert.equal(row.pay_ex_vat, '80.01'); assert.equal(row.charge_ex_vat, '160.03');
});

function definition(text, name) {
  const start = text.indexOf(`create or replace function ${name}(`);
  const end = text.indexOf('$function$;', text.indexOf('as $function$', start));
  assert.ok(start >= 0 && end > start); return text.slice(start, end + '$function$;'.length);
}
test('all exact current native inputs remain hash pinned', () => {
  for (const input of JSON.parse(read('NATIVE_INPUT_PINS.json')).inputs) {
    const bytes = readFileSync(resolve(root, input.path));
    assert.equal(bytes.length, input.bytes, input.path); assert.equal(sha(bytes), input.sha256, input.path);
  }
});
test('complete Stage and Source27 call-site copies are byte-exact current native inputs', () => {
  for (const [file, input] of [['10_complete_stage_request.sqlinc', '20_stage_request.sqlinc'], ['11_complete_automatic_reconcile.sqlinc', '27_automatic_reconcile.sqlinc']])
    assert.equal(read(file), readFileSync(resolve(root, 'supabase/repeatable/13102026_1438_weekly_source_protection_queries', input), 'utf8').replace(/\r\n/g, '\n'));
});
test('event-specific receipt cores preserve complete native predicates; null selector keeps ordinary latest-event behavior', () => {
  const proposed = read('04_native_receipt_successors.sqlinc');
  for (const [kind, path] of [['local', '05102026_0114_weekly_source_pay_query_evidence_v1.sql'], ['c1', '05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql'], ['next', '05102026_0258_weekly_source_pay_query_next_receipt_v1.sql']]) {
    const originalName = `private.weekly_source_pay_query_${kind}_receipt_v1`; const coreName = `private.weekly_source_pay_query_${kind}_receipt_event_v1`;
    const original = definition(readFileSync(resolve(root, 'supabase/repeatable', path), 'utf8').replace(/\r\n/g, '\n'), originalName);
    const restored = definition(proposed, coreName).replace(coreName, originalName)
      .replace(',p_family_event_id uuid', '').replace('\n      and (p_family_event_id is null or e.id=p_family_event_id)', '');
    assert.equal(restored, original, kind);
    assert.match(proposed, new RegExp(`${coreName.replaceAll('.', '\\.') }\\(p_root_timesheet_id,p_approval_id,p_generation_id,null\\)`));
  }
});
test('assertion happens after the real stage seal and before STAGED success, with no receipt fabrication', () => {
  const stage = read('10_complete_stage_request.sqlinc');
  assert.ok(stage.indexOf("'STAGE_C1_REQUEST',v_idempotency_key") < stage.indexOf('perform private.weekly_source_automatic_reconcile_certificate_v1'));
  assert.ok(stage.indexOf('perform private.weekly_source_automatic_reconcile_certificate_v1') < stage.lastIndexOf("'ok',true,'outcome','STAGED'"));
  const context = read('02_staged_publication_context.sqlinc');
  assert.match(context, /family\.bound_version is distinct from v_before_bound\+1/);
  assert.match(context, /stage\.allowlisted_owner_name is distinct from 'public\.weekly_exceptional_pay_stage_c1_request_v1'/);
  assert.match(context, /stage\.after_state_fingerprint is distinct from v_stage_hash/);
  assert.match(context, /v_inventory is distinct from v_before/);
  assert.doesNotMatch(context, /insert into|update public|set_config\(/i);
});
test('prior WAIT qualification permits only the exact own-stage successor and counts all three native transport lanes', () => {
  const prior = read('05_staged_prior_wait_receipt.sqlinc');
  assert.match(prior, /e\.id is distinct from \(v_stage->>'family_event_id'\)::uuid/);
  for (const kind of ['local', 'c1', 'next']) assert.match(prior, new RegExp(`weekly_source_pay_query_${kind}_receipt_event_v1`));
  assert.match(prior, /v_count<>1/); assert.match(prior, /v_local_count<>1/); assert.match(prior, /v_next_count<>0/);
  assert.match(prior, /not in \('COMPLETE','PUBLISHED'\)/);
  assert.match(read('08_automatic_reconcile_delta.sqlinc'), /v_prior_receipt is null/);
});
test('complete Local successor preserves ordinary producer and every native hook, and delegates automatic producer before writer', () => {
  const nativeBody = definition(readFileSync(resolve(root, 'supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql'), 'utf8').replace(/\r\n/g, '\n'), 'public.weekly_exceptional_pay_complete_local_v1');
  const proposed = definition(read('09_complete_local_successor.sqlinc'), 'public.weekly_exceptional_pay_complete_local_v1');
  const ordinaryStart = nativeBody.indexOf("    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,");
  const ordinaryEnd = nativeBody.indexOf('    -- Source expenses must be the same approved observations', ordinaryStart);
  assert.ok(proposed.includes(nativeBody.slice(ordinaryStart, ordinaryEnd)));
  assert.ok(proposed.indexOf('perform private.weekly_source_automatic_reconcile_delta_assert_v1') < proposed.indexOf('insert into private.weekly_source_local_protected_decision_receipts'));
  assert.ok(proposed.indexOf('weekly_source_staged_entitlement_components_v1') < proposed.indexOf('weekly_source_entitlement_publish_immediate_v1'));
  for (const owner of ['tsfin_write_current_snapshot_single_bounded', 'bpay_next_capture_local_source_detail_v1', 'bpay_next_legacy_callback_enabled_v1', 'weekly_source_entitlement_publish_immediate_v1'])
    assert.equal(proposed.split(owner).length, nativeBody.split(owner).length, owner);
});
test('native completion requires PUBLISHED before the sole cleared audit', () => {
  const atomic = read('11_complete_automatic_reconcile.sqlinc');
  const certificate = atomic.lastIndexOf('v_certificate:=private.weekly_source_automatic_reconcile_certificate_v1');
  const complete = atomic.lastIndexOf('v_result:=public.weekly_exceptional_pay_complete_local_v1');
  const refuse = atomic.lastIndexOf("v_result->>'outcome' is distinct from 'PUBLISHED'");
  const audit = atomic.lastIndexOf("perform public._audit_insert('weekly_source_automatic_reconcile'");
  assert.ok(certificate < complete && complete < refuse && refuse < audit);
});
test('all new proof helpers are read-only, owner-private and have an explicit closed certificate', () => {
  for (const file of ['01_staged_source_documents.sqlinc', '02_staged_publication_context.sqlinc', '03_approved_detail_evidence.sqlinc',
    '04_native_receipt_successors.sqlinc', '05_staged_prior_wait_receipt.sqlinc', '06_expense_evidence.sqlinc', '07_staged_entitlement_components.sqlinc', '08_automatic_reconcile_delta.sqlinc']) {
    const text = read(file);
    assert.doesNotMatch(text, /\b(insert\s+into|update\s+(?:public|private)\.|delete\s+from|create\s+table|set_config\(|for\s+update|for\s+share|grant\s+execute)\b/i, file);
    assert.match(text, /stable security definer/); assert.match(text, /from public,anon,authenticated,service_role/);
  }
  const delta = read('08_automatic_reconcile_delta.sqlinc');
  assert.match(delta, /weekly_source_automatic_reconcile_capability_assert_v1/);
  assert.match(delta, /v_cas is distinct from v_proof->'source_cas'/);
  assert.match(delta, /'hours_day','hours_night','hours_sat','hours_sun','hours_bh','unit_count','unit_pay_rate','unit_charge_rate','pay_ex_vat','charge_ex_vat'/);
  assert.doesNotMatch(delta, /automatic_reconcile_selection_v1\(|weekly_source_protected_wait_witness_v1\(/);
});

test('stored native numeric transport fields enter the closed codec as lossless decimal strings', () => {
  const producer = read('07_staged_entitlement_components.sqlinc');
  assert.match(producer, /v_stored:=v_stored\|\|jsonb_build_object\(v_key,to_jsonb\(t\)->>v_key\)/);
  assert.match(producer, /canonical_v1\(v_stored,'staged\.transport\.row'\)/);
  assert.match(producer, /coalesce\(v_raw->>\('hours_'\|\|v_key\),'0'\)/);
  assert.match(producer, /'hours_day','hours_night','hours_sat','hours_sun','hours_bh',\s*'unit_count','unit_pay_rate','unit_charge_rate','pay_ex_vat','charge_ex_vat'/);
});
test('decoded source documents retain exact native header and evidence identities', () => {
  const decoder = read('01_staged_source_documents.sqlinc');
  for (const key of ['source_id', 'source_system', 'external_identity', 'external_revision', 'root_timesheet_id', 'candidate_id', 'contract_id', 'week_ending_date', 'document_sha256'])
    assert.ok(decoder.includes(`v_document->>'${key}' is distinct from`), key);
  assert.match(decoder, /extensions\.digest\(part\.payload_utf8,'sha256'\) is distinct from part\.fragment_sha256/);
  assert.match(decoder, /string_agg\(encode\(part\.payload_utf8,'hex'\),'' order by part\.part_ordinal\)/);
});
test('proposal function bodies have balanced parentheses and stay within PostgreSQL JSON argument bounds', () => {
  // Static source sanity only. Actual PostgreSQL semantic compilation is held.
  for (const file of ['01_staged_source_documents.sqlinc', '02_staged_publication_context.sqlinc', '03_approved_detail_evidence.sqlinc',
    '04_native_receipt_successors.sqlinc', '05_staged_prior_wait_receipt.sqlinc', '06_expense_evidence.sqlinc',
    '07_staged_entitlement_components.sqlinc', '08_automatic_reconcile_delta.sqlinc', '09_complete_local_successor.sqlinc']) {
    for (const match of read(file).matchAll(/\bas\s+(\$[\w]*\$)([^]*?)\1\s*;/gi)) {
      const body = match[2].replace(/--[^\n]*/g, '').replace(/\/\*[^]*?\*\//g, '').replace(/'(?:''|[^'])*'/g, "''");
      const stack = []; let token = '';
      for (const char of body) {
        if (char === '(') { stack.push({ token: token.trim().match(/[\w.]+$/)?.[0], commas: 0 }); token = ''; }
        else if (char === ')') {
          assert.ok(stack.length > 0, file + ' extra close'); const call = stack.pop();
          if (['jsonb_build_object', 'pg_catalog.jsonb_build_object'].includes(call.token)) assert.ok(call.commas < 100, file + ' JSON argument bound');
          token = '';
        } else if (char === ',') { if (stack.length) stack.at(-1).commas++; token = ''; } else token += char;
      }
      assert.equal(stack.length, 0, file + ' unclosed parenthesis');
    }
  }
});
