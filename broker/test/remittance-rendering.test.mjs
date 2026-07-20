import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';

const repoRoot = path.resolve(import.meta.dirname, '../..');
const workerPath = path.join(repoRoot, 'broker/src/index.js');
const worker = fs.readFileSync(workerPath, 'utf8');

function sliceBetween(start, end) {
  const from = worker.indexOf(start);
  const to = worker.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return worker.slice(from, to);
}

function createRenderingContext() {
  const classificationSource = sliceBetween(
    'function classifyFrozenRemittanceItemAmounts(item)',
    'function normaliseRemittanceJobForRendering(job)'
  );
  const normaliserSource = sliceBetween(
    'function normaliseRemittanceJobForRendering(job)',
    'async function handleBankingAlertAcknowledge(env, req, user)'
  );
  const rendererSource = sliceBetween(
    'function buildRemittanceEmailPayload(job, context = {})',
    'async function handleBankingPayBatchRetryBlockedFunds'
  );
  const timesheetRendererSource = sliceBetween(
    'function formatCloudTmsLondonDateTime(value)',
    'async function buildPayBatchDetailPdfFromRows(exportObj)'
  );
  const context = {
    escapeHtml(value) {
      return String(value == null ? '' : value).replace(/[&<>"']/g, (char) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[char]));
    },
    Intl,
    Date,
    Number,
    String,
    Math,
    Array,
    Object,
    JSON,
    Set
  };
  vm.runInNewContext(`${classificationSource}\n${normaliserSource}\n${timesheetRendererSource}\n${rendererSource}`, context, { filename: 'remittance-renderer.js' });
  return context;
}

test('frozen recovery items are deductions and do not reduce remittance gross totals', () => {
  const context = createRenderingContext();
  const earnings = context.classifyFrozenRemittanceItemAmounts({
    item_type: 'SEGMENT_DELTA',
    amount_ex_vat: 380,
    amount_vat: 0,
    amount_inc_vat: 380
  });
  const recovery = context.classifyFrozenRemittanceItemAmounts({
    item_type: 'OVERPAYMENT_RECOVERY',
    amount_ex_vat: -34.78,
    amount_vat: 0,
    amount_inc_vat: -34.78
  });

  assert.equal(earnings.is_deduction, false);
  assert.equal(earnings.gross_ex_vat, 380);
  assert.equal(recovery.is_deduction, true);
  assert.equal(recovery.gross_ex_vat, 0);
  assert.equal(recovery.deductions_inc_vat, 34.78);
});

test('candidate remittance renders the hydrated frozen candidate rather than a blank root candidate', () => {
  const context = createRenderingContext();
  const rendered = context.buildRemittanceEmailPayload({
    pay_batch_id: '973a8779-e853-4c1d-96b8-213bbac13508',
    pay_date: '2026-07-24',
    job_kind: 'CANDIDATE_REMITTANCE',
    scope: 'ALL',
    recipient: {
      entity_kind: 'CANDIDATE',
      candidate_id: '11111111-1111-4111-8111-111111111111',
      name: 'Kier Arthur'
    },
    candidates: [{
      candidate_id: '11111111-1111-4111-8111-111111111111',
      display_name: 'Kier Arthur',
      tms_ref: 'CCR-00835',
      timesheets: [{
        timesheet_id: '22222222-2222-4222-8222-222222222222',
        week_ending_date: '2026-03-15',
        client_name: 'Test client',
        unit_rows: [{
          pay_batch_item_id: '33333333-3333-4333-8333-333333333333',
          item_type: 'SEGMENT_DELTA',
          unit_name: 'Standard hours',
          quantity: 10,
          rate: 38,
          amount_ex_vat: 380,
          amount_vat: 0,
          amount_inc_vat: 380
        }],
        frozen_totals: { gross_ex_vat: 380, vat: 0, gross_inc_vat: 380 }
      }],
      non_timesheet_lines: [{
        pay_batch_item_id: '44444444-4444-4444-8444-444444444444',
        item_type: 'OVERPAYMENT_RECOVERY',
        amount_ex_vat: -34.78,
        amount_vat: 0,
        amount_inc_vat: -34.78
      }],
      frozen_totals: {
        gross_ex_vat: 380,
        vat: 0,
        gross_inc_vat: 380,
        deductions_recoveries_inc_vat: 34.78,
        final_payable: 0.05
      }
    }],
    summary: {
      candidate_count: 1,
      timesheet_count: 1,
      gross_ex_vat: 380,
      vat: 0,
      gross_inc_vat: 380,
      deductions_recoveries_inc_vat: 34.78,
      final_payable: 0.05
    }
  }, {
    jobKind: 'CANDIDATE_REMITTANCE',
    payBatchId: '973a8779-e853-4c1d-96b8-213bbac13508',
    payDate: '2026-07-24'
  });

  assert.match(rendered.body_text, /Kier Arthur\tCCR-00835\t1\t£380\.00\t£0\.00\t£380\.00\t£34\.78\t£0\.05/);
  assert.doesNotMatch(rendered.body_text, /\(unknown\)/);
  assert.equal(rendered.summary_json.recipient.name, 'Kier Arthur');
  assert.equal(rendered.summary_json.candidate_count, 1);
  assert.equal(rendered.summary_json.timesheet_count, 1);
  assert.equal(rendered.summary_json.gross_ex_vat, 380);
  assert.equal(rendered.summary_json.deductions, 34.78);
  assert.equal(rendered.summary_json.final_payable, 0.05);
  assert.equal(rendered.summary_json.render_incomplete, false);
});

test('remittance hydration remains limited to frozen pay-batch artifacts', () => {
  const hydration = sliceBetween(
    'const hydrateThinRemittanceJobFromFrozenBatchArtifacts = async (job, row) =>',
    'const buildRemittanceJobFromOutbox = (row) =>'
  );

  assert.match(hydration, /pay_batch_candidates/);
  assert.match(hydration, /pay_batch_items/);
  assert.match(hydration, /pay_batch_item_breakdowns/);
  assert.match(hydration, /pay_batch_timesheet_snapshots/);
  assert.doesNotMatch(hydration, /buildQueryUrl\('timesheets'/);
  assert.doesNotMatch(hydration, /buildQueryUrl\('finance_components'/);
  assert.doesNotMatch(hydration, /buildQueryUrl\('finance_cases'/);
});
