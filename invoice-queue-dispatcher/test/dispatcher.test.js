import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

async function loadDispatcherModule() {
  const url = new URL('../src/index.js', import.meta.url);
  let source = await readFile(url, 'utf8');
  source = source.replace("import { DurableObject } from 'cloudflare:workers';", 'class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }');
  source += '\nexport { canonicalMessage, sign, verify, validatePayload, sha256 };\n';
  return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

function fakeSqlStorage() {
  const rows = new Map();
  return {
    rows,
    sql: {
      exec(sql, ...params) {
        if (sql.startsWith('CREATE TABLE')) return [];
        if (sql.startsWith('DELETE FROM')) {
          const before = Number(params[0]);
          for (const [key, expiry] of rows) if (expiry < before) rows.delete(key);
          return [];
        }
        if (sql.startsWith('SELECT nonce_hash')) return rows.has(params[0]) ? [{ nonce_hash: params[0] }] : [];
        if (sql.startsWith('INSERT INTO')) {
          if (rows.has(params[0])) throw new Error('UNIQUE');
          rows.set(params[0], Number(params[1]));
          return [];
        }
        throw new Error(`Unexpected SQL: ${sql}`);
      }
    }
  };
}

test('dispatcher validates lanes, depth and request age', async () => {
  const { validatePayload } = await loadDispatcherModule();
  const good = validatePayload({ timestamp: Date.now(), depth: 4, nonce: crypto.randomUUID(), lanes: ['database', 'PDF_MERGE'] });
  assert.equal(good.ok, true);
  assert.deepEqual(good.payload.lanes, ['DATABASE', 'PDF_MERGE']);
  assert.equal(validatePayload({ ...good.payload, depth: 5 }).code, 'INVOICE_DISPATCH_DEPTH_INVALID');
  assert.equal(validatePayload({ ...good.payload, lanes: ['PAYMENTS'] }).code, 'INVOICE_DISPATCH_LANES_INVALID');
  assert.equal(validatePayload({ ...good.payload, timestamp: Date.now() - 61000 }).code, 'INVOICE_DISPATCH_TIMESTAMP_INVALID');
});

test('dispatcher signatures bind canonical lane set and continuation identity', async () => {
  const { sign, verify } = await loadDispatcherModule();
  const secret = 'test-only-secret';
  const payload = { timestamp: Date.now(), depth: 2, nonce: crypto.randomUUID(), lanes: ['PDF_MERGE', 'DATABASE'], priority_class: 'VIEW_NOW' };
  const signature = await sign(secret, payload);
  assert.equal(await verify(secret, payload, signature), true);
  assert.equal(await verify(secret, { ...payload, depth: 3 }, signature), false);
  assert.equal(await verify(secret, { ...payload, lanes: ['DATABASE'] }, signature), false);
});

test('SQLite nonce gate consumes a nonce once', async () => {
  const { InvoiceDispatchNonceGate, sha256 } = await loadDispatcherModule();
  const storage = fakeSqlStorage();
  const gate = new InvoiceDispatchNonceGate({ storage }, {});
  const nonceHash = await sha256(crypto.randomUUID());
  const expiry = Date.now() + 60000;
  const first = await gate.fetch(new Request('https://gate/consume', { method: 'POST', body: JSON.stringify({ nonce_hash: nonceHash, expires_at: expiry }) }));
  const second = await gate.fetch(new Request('https://gate/consume', { method: 'POST', body: JSON.stringify({ nonce_hash: nonceHash, expires_at: expiry }) }));
  assert.equal(first.status, 200);
  assert.equal(second.status, 409);
  assert.equal((await second.json()).code, 'INVOICE_DISPATCH_REPLAY');
});

test('dispatcher rejects a public caller before reaching nonce or main bindings', async () => {
  const module = await loadDispatcherModule();
  const response = await module.default.fetch(new Request('https://dispatcher/dispatch', { method: 'POST', body: '{}' }), {}, { waitUntil() {} });
  assert.equal(response.status, 403);
  assert.equal((await response.json()).code, 'INVOICE_DISPATCH_CALLER_INVALID');
});
