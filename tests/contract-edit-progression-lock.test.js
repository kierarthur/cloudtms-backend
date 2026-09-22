import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const start = source.indexOf('async function contractHasProgressedTimesheetEvidence');
const end = source.indexOf('/** Numeric guard */', start);
assert.notEqual(start, -1);
assert.notEqual(end, -1);
const helperSource = source.slice(start, end);

function buildHelper({ timesheets = [], authorised = false, progressed = false } = {}) {
  const calls = [];
  const sbGetOne = async (_env, url) => {
    calls.push(url);
    if (url.includes('/timesheets?') && url.includes('authorised_at_server=not.is.null')) {
      return authorised ? { timesheet_id: timesheets[0]?.timesheet_id || 'authorised' } : null;
    }
    if (url.includes('/timesheets_financials?')) return progressed ? { id: 'progressed' } : null;
    return null;
  };
  const sbFetch = async (_env, url) => {
    calls.push(url);
    const offset = Number(new URL(url).searchParams.get('offset') || 0);
    return { rows: offset === 0 ? timesheets : [] };
  };
  const factory = new Function(
    'sbGetOne',
    'sbFetch',
    'enc',
    `${helperSource}; return contractHasProgressedTimesheetEvidence;`
  );
  return { helper: factory(sbGetOne, sbFetch, encodeURIComponent), calls };
}

const env = { SUPABASE_URL: 'https://test.invalid' };
const contractId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';

test('planned Contract weeks and a Contract start date are not part of the progression census', async () => {
  const { helper, calls } = buildHelper();
  assert.equal(await helper(env, contractId), false);
  assert.equal(calls.some((url) => url.includes('contract_weeks')), false);
  assert.equal(calls.some((url) => url.includes('start_date')), false);
});

test('merely-created and unprocessed Timesheet rows do not lock Contract changes', async () => {
  const { helper, calls } = buildHelper({
    timesheets: [{ timesheet_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }]
  });
  assert.equal(await helper(env, contractId), false);
  const financialCall = calls.find((url) => url.includes('/timesheets_financials?'));
  assert.ok(financialCall);
  assert.match(financialCall, /processed_at_utc\.not\.is\.null/);
  assert.match(financialCall, /authorised_at_utc\.not\.is\.null/);
  assert.match(financialCall, /locked_by_invoice_id\.not\.is\.null/);
  assert.match(financialCall, /paid_at_utc\.not\.is\.null/);
  assert.doesNotMatch(financialCall, /is_current/);
});

test('ordinary Timesheet authorisation locks Contract changes', async () => {
  const { helper } = buildHelper({
    timesheets: [{ timesheet_id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc' }],
    authorised: true
  });
  assert.equal(await helper(env, contractId), true);
});

test('any historical financial progression locks Contract changes', async () => {
  const { helper } = buildHelper({
    timesheets: [{ timesheet_id: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd' }],
    progressed: true
  });
  assert.equal(await helper(env, contractId), true);
});
