import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { runInNewContext } from 'node:vm';

const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const start = source.indexOf('async function handleContractsDuplicate');
const end = source.indexOf('async function generateContractWeeksInternal', start);
assert.ok(start >= 0 && end > start);
const handlerSource = source.slice(start, end);
const candidateId = '11111111-1111-4111-8111-111111111111';

async function duplicate({ channel = 'PAYE', candidateChannel = 'PAYE', assignments = [candidateId], missing = false, stale = false, authorised = true } = {}) {
  const writes = [], reads = [];
  const handler = runInNewContext(`${handlerSource}; handleContractsDuplicate`, {
    requireUser: async () => authorised ? { id: 'admin' } : null,
    parseJSONBody: async () => ({ count: assignments.length, assignments, expected_source_updated_at: stale ? 'old' : 'current' }),
    sbGetOne: async () => ({ id: 'source', pay_method_snapshot: channel, updated_at: 'current', client_id: 'client' }),
    sbFetch: async (env, url) => { reads.push(url); return { rows: missing ? [] : [{ id: candidateId, pay_method: candidateChannel }] }; },
    fetch: async (url, options) => {
      assert.equal(options.method, 'POST');
      const payload = JSON.parse(options.body);
      writes.push(payload);
      return { ok: true, json: async () => [{ ...payload, id: `copy-${writes.length}` }] };
    },
    withCORS: (env, req, result) => result,
    ok: body => ({ status: 200, body }), conflict: message => ({ status: 409, message }),
    badRequest: message => ({ status: 400, message }), unauthorized: () => ({ status: 401 }),
    notFound: message => ({ status: 404, message }), serverError: message => ({ status: 500, message }),
    sbHeaders: () => ({}), enc: encodeURIComponent, nowIso: () => '2026-10-09T13:00:00Z', console: { log() {}, warn() {}, error() {} }
  });
  const response = await handler({ SUPABASE_URL: 'https://test.invalid' }, {}, 'source');
  return { response, writes, reads };
}

for (const channel of ['PAYE', 'UMBRELLA']) {
  test(`${channel} duplicate accepts only the same current Candidate pay channel`, async () => {
    const matching = await duplicate({ channel, candidateChannel: channel });
    assert.equal(matching.response.status, 200);
    assert.equal(matching.writes[0].pay_method_snapshot, channel);
    assert.equal(matching.writes[0].candidate_id, candidateId);
    assert.match(matching.reads[0], /select=id,pay_method$/);
    for (const other of [channel === 'PAYE' ? 'UMBRELLA' : 'PAYE', null, '', 'UNKNOWN']) {
      const rejected = await duplicate({ channel, candidateChannel: other, assignments: [null, candidateId] });
      assert.equal(rejected.response.status, 409);
      assert.equal(rejected.writes.length, 0, 'all assignments must be validated before even a vacant copy is inserted');
      assert.match(rejected.response.message, /unassigned copy/);
    }
  });
  test(`${channel} duplicate permits unassigned copies without a Candidate read`, async () => {
    const result = await duplicate({ channel, assignments: [null, null] });
    assert.equal(result.response.status, 200);
    assert.equal(result.writes.length, 2);
    assert.equal(result.reads.length, 0);
    assert.ok(result.writes.every(row => row.candidate_id === null && row.pay_method_snapshot === channel));
  });
}

test('duplicate preserves source freshness, Candidate existence and admin checks', async () => {
  for (const [args, status] of [[{ stale: true }, 409], [{ missing: true }, 400], [{ authorised: false }, 401], [{ channel: 'UNKNOWN' }, 409]]) {
    const result = await duplicate(args);
    assert.equal(result.response.status, status);
    assert.equal(result.writes.length, 0);
  }
});
