import assert from 'node:assert/strict';
import test from 'node:test';

import { weeklySourceReportInternals } from '../../broker/src/index.js';

const admin = async () => ({ id: '00000000-0000-4000-8000-000000000001', role: 'admin' });
const env = { SUPABASE_URL: 'https://test.invalid' };

function request(path) {
  return new Request(`https://worker.invalid${path}`, { headers: { origin: 'https://office.invalid' } });
}

async function payload(response) {
  assert.equal(response.status, 200);
  return response.json();
}

test('real Timesheets report keeps four Weekly Source hour facts separate in CSV', async () => {
  const rpcCalls = [];
  const response = await weeklySourceReportInternals.handleReportTimesheets(
    env,
    request('/reports/timesheets?format=csv'),
    {
      requireUser: admin,
      rpc: async (_env, name, args) => {
        rpcCalls.push({ name, args });
        return [{
          timesheet: { week_ending_date: '2026-09-06' }, client: { name: 'Trust A' },
          pay_method: 'PAYE', paid_at_utc: null, invoiced_any: true,
          total_pay_ex_vat: 100, total_charge_ex_vat: 200, margin_ex_vat: 100,
          expenses_charge_ex_vat: 0, mileage_charge_ex_vat: 0,
          weekly_source_hours: {
            weekly_source: true,
            submitted_hours: { state: 'AVAILABLE', total_hours: 10 },
            source_hours: { state: 'AVAILABLE', total_hours: 9 },
            approved_hours: { state: 'AVAILABLE', total_hours: 10 },
            paid_hours: { state: 'UNAVAILABLE', total_hours: null },
            invoice_movements: { movement_count: 2 },
          },
        }];
      },
    },
  );
  const body = await payload(response);
  assert.equal(rpcCalls.length, 1);
  assert.equal(rpcCalls[0].name, 'tsfin_report_timesheets_v2');
  assert.match(body.csv, /SubmittedHoursState,SubmittedHours,FinalSourceHoursState,FinalSourceHours,ApprovedHoursState,ApprovedHours,PaidHoursState,PaidHours,SourceInvoiceMovementCount/);
  assert.match(body.csv, /AVAILABLE,10\.00,AVAILABLE,9\.00,AVAILABLE,10\.00,UNAVAILABLE,,2/);
});

test('ordinary Timesheets report rows retain blank additive source columns', async () => {
  const response = await weeklySourceReportInternals.handleReportTimesheets(
    env,
    request('/reports/timesheets?format=csv'),
    {
      requireUser: admin,
      rpc: async () => [{
        timesheet: { week_ending_date: '2026-09-06' }, client: { name: 'Ordinary Client' },
        pay_method: 'PAYE', paid_at_utc: null, invoiced_any: false,
        total_pay_ex_vat: 10, total_charge_ex_vat: 20, margin_ex_vat: 10,
        expenses_charge_ex_vat: 0, mileage_charge_ex_vat: 0,
        weekly_source_hours: {},
      }],
    },
  );
  const body = await payload(response);
  assert.match(body.csv, /Ordinary Client,PAYE,N,N,10\.00,20\.00,10\.00,0\.00,0\.00,,,,,,,,,/);
});

test('real Invoices report exports immutable source movement truth and backing report identity', async () => {
  const rpcCalls = [];
  const response = await weeklySourceReportInternals.handleReportInvoices(
    env,
    request('/reports/invoices?format=csv'),
    {
      requireUser: admin,
      fetch: async (_env, url) => {
        if (url.includes('/rest/v1/invoices?')) return { rows: [{
          id: '00000000-0000-4000-8000-000000000101', invoice_no: 'SB-1', status: 'ISSUED',
          issued_at_utc: '2026-09-09T15:00:00Z', subtotal_ex_vat: 20,
          vat_amount: 4, total_inc_vat: 24,
        }] };
        assert.match(url, /select=invoice_id,timesheet_id,margin_ex_vat/);
        return { rows: [{ invoice_id: '00000000-0000-4000-8000-000000000101', margin_ex_vat: 10 }] };
      },
      rpc: async (_env, name, args) => {
        rpcCalls.push({ name, args });
        return { rows: [{
          invoice_id: '00000000-0000-4000-8000-000000000101', weekly_source: true,
          movement_count: 2, source_movement_ex_vat: 20,
          backing_report_numbers: ['BR-3'],
        }] };
      },
    },
  );
  const body = await payload(response);
  assert.equal(rpcCalls[0].name, 'weekly_source_invoice_report_rows_v1');
  assert.deepEqual(rpcCalls[0].args.p_request.invoice_ids,
    ['00000000-0000-4000-8000-000000000101']);
  assert.match(body.csv, /SourceMovementCount,SourceMovementExVAT,BackingReportNumbers/);
  assert.match(body.csv, /SB-1,ISSUED,2026-09-09T15:00:00Z,20\.00,4\.00,24\.00,10\.00,2,20\.00,BR-3/);
});

