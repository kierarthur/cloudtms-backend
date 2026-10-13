import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION as classification,
  classifyBpayNextLegacyEmail,
  isBpayNextLegacyFinancialEmail
} from '../broker/src/bpay-next-legacy-email.js';

test('retained remittance and cancellation types remain financial without companion metadata', () => {
  for (const type of ['REMITTANCE', 'PAYMENT_CANCELLATION', ' remittance ', ' payment_cancellation ']) {
    for (const fields of [{}, { context_id: null }, { context_kind: 1, reference: {} }]) {
      const row = { type, ...fields };
      assert.equal(classifyBpayNextLegacyEmail(row), classification.financial);
      assert.equal(isBpayNextLegacyFinancialEmail(row), true);
    }
  }
});

test('BROADCAST invite, scheduled, processing and completion contexts are financial', () => {
  for (const context_kind of ['pay_batch', 'pay_batches', 'PAY_BATCH_REMITTANCE', 'pay_payment_correction_requests']) {
    for (const type of ['BROADCAST', 'INVOICE', undefined, {}]) {
      assert.equal(classifyBpayNextLegacyEmail({ type, context_kind, context_id: null }), classification.financial);
      assert.equal(classifyBpayNextLegacyEmail({ type, context_kind: ` ${context_kind.toLowerCase()} ` }), classification.financial);
    }
  }
});

test('each exact retained reference family fences missing or misleading type/context', () => {
  for (const reference of ['remittance:one', 'remittance_one', 'remittanceAdvice', 'payout_notice:one',
    'pay_auth_invite:one', 'pay_batch_processing:one', 'pay_batch_scheduled:one',
    'pay_batch_completed:one', 'PAYMENT_CANCELLATION_NOTICE_V1:one']) {
    for (const fields of [{}, { type: 'BROADCAST' }, { type: 'INVOICE', context_kind: 'invoices' },
      { type: 1, context_kind: {} }]) {
      assert.equal(classifyBpayNextLegacyEmail({ ...fields, reference }), classification.financial, reference);
      assert.equal(isBpayNextLegacyFinancialEmail({ ...fields, reference: ` ${reference.toUpperCase()} ` }), true);
    }
  }
});

test('all exact current factual mail types survive absent optional context/reference', () => {
  for (const type of ['INVOICE', 'TSO_FAILURE', 'BROADCAST', 'TIMESHEET_QR',
    'TIMESHEET_REFUSAL', 'TIMESHEET_GENERAL', 'TIMESHEET_QUERY', 'MAILSHOT_EMAIL']) {
    for (const optional of [{}, { context_kind: null, reference: null },
      { context_kind: '', reference: '' }, { context_kind: undefined, reference: undefined }]) {
      assert.equal(classifyBpayNextLegacyEmail({ type, ...optional }), classification.factual, type);
      assert.equal(isBpayNextLegacyFinancialEmail({ type: type.toLowerCase(), ...optional }), false, type);
    }
  }
});

test('genuine Source completed copy, Candidate manager/paper and Invoice shapes remain factual', () => {
  const rows = [
    { type: 'TIMESHEET_GENERAL', context_kind: 'timesheets', reference: 'weekly_completed_pack:one',
      email_type: 'WEEKLY_COMPLETED_TIMESHEET_COPY', payment_scope_json: {
        completed_pack_copy_authority: 'WEEKLY_COMPLETED_PACK_COPY_V1', informational_only: true,
        changes_pay: false, changes_invoice: false } },
    { type: 'TIMESHEET_GENERAL', context_kind: 'CANDIDATE_WORKFLOW', reference: 'candidate_manager:one',
      payment_scope_json: { candidate_mail_authority: 'MANAGER_APPROVAL_V1',
        candidate_manager_mail_kind: 'CANCELLATION', candidate_approval_request_id: 'one' } },
    { type: 'TIMESHEET_QR', context_kind: 'timesheets', reference: 'candidate_paper:one',
      payment_scope_json: { candidate_workflow_id: 'one', paper_return_manifest_sha256: 'proof' } },
    { type: 'INVOICE', context_kind: 'invoices', reference: 'invoice:one',
      payment_scope_json: { invoice_id: 'one', mail_hold_reason: 'INVOICE_DOCUMENT_PENDING' } },
    { type: 'BROADCAST', context_kind: 'MYTMS_INVITATION', reference: 'mytms_invite:one',
      payment_scope_json: { candidate_id: 'one' } },
    { type: 'TIMESHEET_QUERY', context_kind: 'timesheets', reference: 'timesheet_query:one' }
  ];
  for (const row of rows) {
    assert.equal(classifyBpayNextLegacyEmail(row), classification.factual, row.reference);
    assert.equal(isBpayNextLegacyFinancialEmail(row), false, row.reference);
  }
});

test('scope JSON, subject, body, amount and status do not impersonate financial metadata', () => {
  for (const value of [null, [], {}, '{garbled', 'payment', { pay_batch_id: 'one' }]) {
    assert.equal(classifyBpayNextLegacyEmail({ type: 'BROADCAST', payment_scope_json: value,
      subject: 'Payment reminder', body_html: 'remittance', amount: 180, status: 'SENT' }), classification.factual);
  }
  const row = { type: 'INVOICE', context_kind: 'invoices', reference: 'invoice:one' };
  for (const name of ['payment_scope_json', 'body_html', 'subject', 'amount']) {
    Object.defineProperty(row, name, { get() { throw new Error('unrelated field was read'); } });
  }
  assert.equal(classifyBpayNextLegacyEmail(row), classification.factual);
});

test('unidentifiable metadata is held without coercion or thrown errors', () => {
  for (const row of [null, undefined, false, 1, '', [], {}, { type: null }, { type: '' }, { type: 'NEW_UNKNOWN_TYPE' },
    { type: 1 }, { type: ['INVOICE'] }, { type: { toString() { throw new Error('coercion'); } } },
    { type: 'INVOICE', context_kind: {} }, { type: 'INVOICE', reference: [] },
    Object.create({ type: 'REMITTANCE' })]) {
    assert.equal(classifyBpayNextLegacyEmail(row), classification.invalid);
    assert.equal(isBpayNextLegacyFinancialEmail(row), true);
  }
});

test('accessor metadata is not executed and cannot hide other financial evidence', () => {
  for (const type of ['INVOICE', 'REMITTANCE']) {
    let calls = 0;
    const row = { type };
    Object.defineProperty(row, 'reference', { get() { calls += 1; throw new Error('getter'); } });
    assert.equal(classifyBpayNextLegacyEmail(row), type === 'REMITTANCE' ? classification.financial : classification.invalid);
    assert.equal(calls, 0);
  }
  const row = new Proxy({}, { getOwnPropertyDescriptor() { throw new Error('descriptor'); } });
  assert.equal(classifyBpayNextLegacyEmail(row), classification.invalid);
});

test('classification is independent of claimed lease, body, delivery state and owner mode', () => {
  for (const status of ['QUEUED', 'SENT', 'FAILED']) {
    for (const owner of ['LEGACY', 'NEXT', 'DISABLED', undefined]) {
      const row = Object.freeze({ type: 'REMITTANCE', context_kind: 'pay_batches', reference: 'payout_notice:one',
        status, active_owner: owner, attempt_lease_token: 'one', next_attempt_at_utc: null });
      assert.equal(classifyBpayNextLegacyEmail(row), classification.financial);
      assert.equal(isBpayNextLegacyFinancialEmail(row), true);
      assert.equal(row.status, status);
      assert.equal(row.attempt_lease_token, 'one');
    }
  }
});

test('reference matches are anchored and do not block unrelated informational references', () => {
  for (const reference of ['invoice:remittance', 'weekly:pay_batch_completed:one', 'pay_batch',
    'pay_auth_invite', 'payout_notice', 'PAYMENT_CANCELLATION_NOTICE_V2:one', 'source_payment_advice:one']) {
    assert.equal(classifyBpayNextLegacyEmail({ type: 'TIMESHEET_GENERAL', context_kind: 'timesheets', reference }), classification.factual);
  }
  const row = Object.assign(Object.create(null), { type: 'INVOICE', context_kind: 'invoices' });
  assert.equal(classifyBpayNextLegacyEmail(row), classification.factual);
});

const savedSql = (path) => readFileSync(new URL(path, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const migration = savedSql('../supabase/migrations/04102026_0504_banking_pay_next_factual_email_claim_index.sql');
const successor = savedSql('../supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql');
const retained = savedSql('../supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql');
const originalQuery = retained.slice(retained.indexOf('  return query\n'), retained.indexOf('\nend;\n$function$;'));
function markedQuery(marker) {
  const start = successor.indexOf(`    -- BEGIN ${marker}\n`);
  const end = successor.indexOf(`    -- END ${marker}`, start);
  assert.ok(start >= 0 && end > start, marker);
  return successor.slice(start + `    -- BEGIN ${marker}\n`.length, end)
    .split('\n').filter((line, index, lines) => index !== lines.length - 1 || line !== '')
    .map((line) => line.slice(2)).join('\n');
}

test('both saved SQL queries preserve the complete original Candidate/Invoice/lease owner recipe', () => {
  assert.equal(markedQuery('RETAINED LEGACY CLAIM QUERY'), originalQuery);
  const factual = markedQuery('INDEXED FACTUAL CLAIM QUERY');
  const extra = '\n      and private.bpay_next_legacy_email_is_factual_v1(mo.type,mo.context_kind,mo.reference) is true';
  assert.equal(factual.split(extra).length, 2);
  assert.equal(factual.replace(extra, ''), originalQuery);
  assert.match(successor, /returns setof public\.mail_outbox\nlanguage plpgsql\nsecurity invoker/);
  assert.match(successor, /v_limit integer:=greatest\(0,least\(coalesce\(p_limit,0\),500\)\)/);
  assert.match(successor, /v_lease integer:=greatest\(1,least\(coalesce\(p_lease_minutes,5\),30\)\)/);
  assert.match(successor, /if v_legacy is true then/);
  assert.doesNotMatch(successor, /v_legacy\s+or|or\s+v_legacy/);
});

test('the constant factual branch implies exactly the ordered partial index predicate', () => {
  const query = markedQuery('INDEXED FACTUAL CLAIM QUERY');
  const indexed = migration.match(/create index bpay_next_factual_email_ready_idx[\s\S]*?\) where ([\s\S]*?);/);
  assert.ok(indexed);
  const normalized = (text) => text.replace(/\bmo\./g, '').replace(/\s+/g, ' ').trim();
  const wherePrefix = query.slice(query.indexOf('    where ') + 10, query.indexOf('\n      and coalesce('));
  assert.equal(normalized(indexed[1]), normalized(wherePrefix));
  assert.match(migration, /\(coalesce\(next_attempt_at_utc,scheduled_for_utc,created_at_utc\)\),created_at_utc,id/);
  assert.match(query, /order by coalesce\(mo\.next_attempt_at_utc,mo\.scheduled_for_utc,mo\.created_at_utc\),\n      mo\.created_at_utc,mo\.id/);
  assert.doesNotMatch(indexed[1], /now\(|clock_timestamp|active_owner|attempt_lease_expires/);
  assert.match(migration, /to_regprocedure\('private\.bpay_next_legacy_email_is_factual_v1\(text,text,text\)'\) is not null/);
  assert.match(migration, /to_regclass\('public\.bpay_next_factual_email_ready_idx'\) is not null/);
  assert.doesNotMatch(migration, /create or replace function|drop index|drop function|update public\.mail_outbox/i);
});

test('one fresh module SHARE lookup precedes mail row locks, without changing service-only public claim ACL', () => {
  assert.equal((successor.match(/v_legacy:=private\.bpay_next_legacy_email_claim_legacy_v1\(\)/g) || []).length, 1);
  assert.match(successor, /from private\.bpay_next_module_control m where m\.id=1 for share/);
  assert.match(successor, /coalesce\(found and v_owner='LEGACY' and v_epoch is not null and v_epoch>0,false\)/);
  assert.match(successor, /exception when others then[\s\S]*?return false;/);
  assert.ok(successor.indexOf('v_legacy:=') < successor.indexOf('-- BEGIN RETAINED LEGACY CLAIM QUERY'));
  assert.match(successor, /revoke all on function public\.email_outbox_claim_ready_batch\(integer,text,integer\)\n  from public,anon,authenticated/);
  assert.match(successor, /grant execute on function public\.email_outbox_claim_ready_batch\(integer,text,integer\)\n  to service_role/);
  for (const sql of [migration, successor]) {
    assert.doesNotMatch(sql, /grant .*to (anon|authenticated)|update private\.bpay_next_module_control|set_config|request\.jwt|http_[a-z_]+\s*\(|fetch\s*\(/i);
  }
});

test('SQL NULL-aware factual types align with the exact current stored type catalogue and JS', () => {
  const catalogue = savedSql('../supabase/migrations/07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql');
  const postimage = catalogue.match(/v_expected_postimage constant text := '([^\n]*)';/)[1];
  const storedTypes = [...postimage.matchAll(/''([A-Z_]+)''::text/g)].map((match) => match[1]).sort();
  const sqlTypes = [...migration.match(/select row_type=any\(array\[([\s\S]*?)\]::text\[\]\)/)[1]
    .matchAll(/'([A-Z_]+)'/g)].map((match) => match[1]);
  assert.deepEqual([...sqlTypes, 'REMITTANCE', 'PAYMENT_CANCELLATION'].sort(), storedTypes);
  for (const type of sqlTypes) {
    assert.equal(classifyBpayNextLegacyEmail({ type, context_kind: null, reference: null }), classification.factual);
  }
  assert.match(migration, /returns boolean language sql immutable parallel safe security invoker\ncalled on null input/);
  assert.doesNotMatch(migration, /payment_scope_json|body_html|subject|amount|security definer/i);
});

test('SQL and JS share exact trim characters and locale-independent ASCII reference matching', () => {
  const trims = [...migration.matchAll(/U&'([^']+)'/g)];
  assert.equal(trims.length, 3);
  assert.equal(new Set(trims.map((match) => match[1])).size, 1);
  const points = [...trims[0][1].matchAll(/\\([0-9A-F]{4})/g)].map((match) => parseInt(match[1], 16));
  assert.deepEqual(points, [9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197,
    8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288, 65279]);
  const sqlPrefixes = [...migration.matchAll(/starts_with\(row_reference,'([^']+)'\)/g)].map((match) => match[1]);
  for (const point of points) {
    const space = String.fromCodePoint(point);
    assert.equal(classifyBpayNextLegacyEmail({ type: `${space}invoice${space}` }), classification.factual);
    for (const prefix of sqlPrefixes) {
      assert.equal(classifyBpayNextLegacyEmail({ type: 'BROADCAST', reference: `${space}${prefix.toLowerCase()}one${space}` }), classification.financial);
    }
  }
  // Long-s would uppercase into S under Unicode folding, but is not part of
  // an exact ASCII authority marker in either implementation.
  assert.equal(classifyBpayNextLegacyEmail({ type: 'BROADCAST', reference: 'pay_batch_\u017fcheduled:one' }), classification.factual);
  assert.equal(classifyBpayNextLegacyEmail({ type: 'BROADCAST', context_kind: 'PAY_BATCHE\u017f' }), classification.factual);
  assert.equal((migration.match(/collate "C"/g) || []).length, 3);
});
