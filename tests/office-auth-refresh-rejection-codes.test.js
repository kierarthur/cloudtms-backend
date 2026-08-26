import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import cloudTmsWorker from '../broker/src/index.js';

const source = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const allowedOrigin = 'https://testmode.arthur-rai.co.uk';
const env = {
  ALLOWED_ORIGINS: allowedOrigin,
  SESSION_TOKEN_SECRET: 'codex-test-session-secret-with-more-than-thirty-two-characters'
};
const ctx = { waitUntil() {} };

const refreshRequest = (cookie = '') => new Request('https://test.invalid/auth/refresh', {
  method: 'POST',
  headers: {
    Origin: allowedOrigin,
    ...(cookie ? { Cookie: cookie } : {})
  }
});

test('missing refresh cookie returns a stable invalid-session reason code', async () => {
  const response = await cloudTmsWorker.fetch(refreshRequest(), env, ctx);
  assert.equal(response.status, 401);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await response.json(), {
    error: 'No refresh cookie',
    code: 'REFRESH_COOKIE_MISSING'
  });
});

test('invalid refresh token returns a stable invalid-session reason code', async () => {
  const response = await cloudTmsWorker.fetch(refreshRequest('ctms_refresh=not-a-valid-token'), env, ctx);
  assert.equal(response.status, 401);
  assert.deepEqual(await response.json(), {
    error: 'Invalid refresh token',
    code: 'REFRESH_TOKEN_INVALID'
  });
});

test('every definitive Office refresh rejection has a UI-safe code', () => {
  const start = source.indexOf('const OFFICE_AUTH_REFRESH_REJECTION_CODES');
  const end = source.indexOf('const access = await mintAccessToken', start);
  assert.ok(start >= 0 && end > start);
  const authRefreshSource = source.slice(start, end);
  for (const code of [
    'REFRESH_COOKIE_MISSING',
    'REFRESH_TOKEN_INVALID',
    'REFRESH_CLAIMS_INVALID',
    'REFRESH_EXPIRED',
    'REFRESH_SESSION_MISSING',
    'REFRESH_USER_DISABLED',
    'REFRESH_SESSION_VERSION_CHANGED'
  ]) {
    assert.match(authRefreshSource, new RegExp(code));
  }
  const logLine = authRefreshSource.match(/console\.warn\([^\n]+/)?.[0] || '';
  assert.match(logLine, /office_auth_refresh_rejected/);
  assert.doesNotMatch(logLine, /authorization|cookie_value|refresh_token|user_id|sid|email/);
});
