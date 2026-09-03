import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const start = source.indexOf('async function handleGetClient(env, req, clientId)');
const end = source.indexOf('async function handleCreateClient', start);
assert.ok(start >= 0 && end > start);
const handlerSource = source.slice(start, end);
const clientId = '11111111-1111-4111-8111-111111111111';

function harness({ enabled = false, authenticated = true, missingSettings = false } = {}) {
  const calls = [];
  const storedSettings = {
    id: '22222222-2222-4222-8222-222222222222', client_id: clientId,
    candidate_paper_submission_enabled: enabled, default_submission_mode: 'ELECTRONIC',
    invoice_consolidation_mode: 'BY_WEEK', is_nhsp: false, autoprocess_hr: false,
    no_timesheet_required: false, updated_at: '2026-08-28T12:00:00Z'
  };
  const context = vm.createContext({
    encodeURIComponent, console,
    requireUser: async (_env, _req, roles) => {
      assert.deepEqual(Array.from(roles), ['admin']);
      return authenticated ? { role: 'admin' } : null;
    },
    withCORS: (_env, _req, response) => response,
    ok: body => ({ status: 200, body }),
    unauthorized: () => ({ status: 401 }),
    notFound: () => ({ status: 404 }),
    serverError: () => ({ status: 500 }),
    sbFetch: async (_env, url, options) => {
      assert.equal(options?.method ?? 'GET', 'GET', 'details must remain read-only');
      const parsed = new URL(url);
      calls.push(parsed);
      switch (parsed.pathname) {
        case '/rest/v1/clients': return { rows: [{ id: clientId, name: 'TEST Client', rev: 7 }] };
        case '/rest/v1/client_settings': {
          assert.equal(parsed.searchParams.get('client_id'), `eq.${clientId}`);
          assert.equal(parsed.searchParams.get('order'), 'effective_from.desc,created_at.desc');
          assert.equal(parsed.searchParams.get('limit'), '1');
          // Model PostgREST's explicit projection: unselected fields do not come back.
          const columns = parsed.searchParams.get('select').split(',');
          return { rows: missingSettings ? [] : [Object.fromEntries(columns.map(key => [key, storedSettings[key] ?? null]))] };
        }
        case '/rest/v1/settings_defaults': return { rows: [] };
        case '/rest/v1/v_legacy_client_candidates': return { rows: [] };
        default: throw new Error('Unexpected read endpoint');
      }
    }
  });
  const handler = vm.runInContext(`${handlerSource}\nhandleGetClient`, context);
  return { calls, run: () => handler({ SUPABASE_URL: 'https://test-gateway.invalid' }, {}, clientId) };
}

for (const enabled of [true, false]) {
  test(`Client details returns stored Printed QR ${enabled} without changing other settings`, async () => {
    const { run, calls } = harness({ enabled });
    const result = await run();
    assert.equal(result.status, 200);
    assert.equal(result.body.client_settings.candidate_paper_submission_enabled, enabled);
    assert.equal(result.body.client_settings.default_submission_mode, 'ELECTRONIC');
    assert.equal(result.body.client_settings.invoice_consolidation_mode, 'BY_WEEK');
    assert.equal(result.body.client_settings.updated_at, '2026-08-28T12:00:00Z');
    assert.equal(Object.hasOwn(result.body, 'import_financial_policy'), false);
    assert.equal(result.body.has_e_history, false);
    assert.equal(calls.length, 3);
  });
}

test('Client details still requires Office admin authentication before reading', async () => {
  const { run, calls } = harness({ authenticated: false });
  assert.equal((await run()).status, 401);
  assert.equal(calls.length, 0);
});

test('Client without a settings record retains the existing null response', async () => {
  const { run } = harness({ missingSettings: true });
  const result = await run();
  assert.equal(result.status, 200);
  assert.equal(result.body.client_settings, null);
});
