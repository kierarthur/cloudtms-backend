import test from 'node:test';
import assert from 'node:assert/strict';
import { createBpayNextFrozenPageRpc } from '../broker/src/bpay-next-frozen-rpc.js';
import { handleBpayNextFrozenRead } from '../broker/src/bpay-next-frozen-read.js';
import { readFile } from 'node:fs/promises';

const actor = 'b8020000-0000-4000-8000-000000000001';
const line = 'b8020000-0000-4000-8000-000000000020';
test('frozen route binds verified actor and exact closed wrapper without buffering', async () => {
  let calls = 0;
  const raw = Response.json({ rows: [], complete: true });
  const rpc = createBpayNextFrozenPageRpc({
    baseUrl: 'https://agency-test.invalid/', headers: {
      Authorization: 'Bearer test-only', 'Content-Type': 'application/json', Accept: 'application/json'
    },
    fetchImpl: async (url, options) => {
      calls++;
      assert.equal(url, 'https://agency-test.invalid/rest/v1/rpc/bpay_next_frozen_detail_page_v1');
      assert.deepEqual(JSON.parse(options.body), {
        p_limit: 50, p_run_line_id: line, p_after_detail_no: null,
        p_kind: 'SHIFTS', p_actor_user_id: actor
      });
      assert.equal(options.redirect, 'error');
      const effective = new Request(url, options);
      assert.equal(effective.headers.get('authorization'), 'Bearer test-only');
      assert.equal(effective.headers.get('content-type'), 'application/json');
      assert.equal(effective.headers.get('accept'), 'application/json');
      assert.ok(options.signal instanceof AbortSignal);
      return raw;
    }
  });
  const response = await handleBpayNextFrozenRead(new Request('https://test.invalid/api/banking/pay/next/frozen-detail', {
    method: 'POST', body: JSON.stringify({ kind: 'SHIFTS', run_line_id: line })
  }), { requireOfficeUser: async () => ({ id: actor, role: 'admin' }), frozenPageRpc: rpc });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { ok: true, kind: 'SHIFTS', rows: [], complete: true, next_cursor: null });
  assert.equal(calls, 1);
});
test('fixed transport rejects caller-selected RPC kind and credential URL', async () => {
  assert.throws(() => createBpayNextFrozenPageRpc({ baseUrl: 'https://secret:password@test.invalid' }), TypeError);
  const rpc = createBpayNextFrozenPageRpc({ baseUrl: 'https://test.invalid', fetchImpl: () => { throw Error('must not fetch'); } });
  await assert.rejects(rpc('ARBITRARY_SQL', {}, { actorUserId: actor, signal: new AbortController().signal }), TypeError);
});
test('real broker dispatch imports the bounded reader before old Workbench contract dispatch', async () => {
  const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  const route = source.indexOf("if (req.method === 'POST' && nextPath === '/api/banking/pay/next/frozen-detail')");
  const guard = source.indexOf('const requiresBankingPayWorkbenchContract =', route);
  assert.ok(route > 0 && guard > route);
  assert.match(source.slice(route, guard), /requireOfficeUser: async \(\) => user/);
  assert.match(source.slice(route, guard), /baseUrl: env\.SUPABASE_URL, headers: sbHeaders\(env\)/);
});
