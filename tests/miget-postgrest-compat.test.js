import assert from 'node:assert/strict';
import test from 'node:test';

import { normaliseMigetPostgrestRequest } from '../broker/src/miget-postgrest-compat.js';

test('rewrites Miget table and RPC routes without changing query parameters', () => {
  assert.equal(
    normaliseMigetPostgrestRequest('https://example.eu-east-1.migetapp.com/rest/v1/timesheets?select=id&limit=1'),
    'https://example.eu-east-1.migetapp.com/timesheets?select=id&limit=1'
  );
  assert.equal(
    normaliseMigetPostgrestRequest('https://example.eu-east-1.migetapp.com/rest/v1/rpc/show_limit'),
    'https://example.eu-east-1.migetapp.com/rpc/show_limit'
  );
  assert.equal(
    normaliseMigetPostgrestRequest('https://example.eu-east-1.migetapp.com/rest/v1'),
    'https://example.eu-east-1.migetapp.com/'
  );
});

test('leaves Supabase, non-Miget and already-native PostgREST routes untouched', () => {
  const supabase = 'https://example.supabase.co/rest/v1/timesheets?limit=1';
  const nativeMiget = 'https://example.eu-east-1.migetapp.com/timesheets?limit=1';
  const other = 'https://example.com/rest/v1/timesheets?limit=1';
  assert.equal(normaliseMigetPostgrestRequest(supabase), supabase);
  assert.equal(normaliseMigetPostgrestRequest(nativeMiget), nativeMiget);
  assert.equal(normaliseMigetPostgrestRequest(other), other);
});

test('preserves URL and Request types plus request method and headers', () => {
  const url = new URL('https://example.eu-east-1.migetapp.com/rest/v1/settings_defaults?id=eq.1');
  const rewrittenUrl = normaliseMigetPostgrestRequest(url);
  assert.ok(rewrittenUrl instanceof URL);
  assert.equal(rewrittenUrl.pathname, '/settings_defaults');
  assert.equal(rewrittenUrl.search, '?id=eq.1');

  const request = new Request('https://example.eu-east-1.migetapp.com/rest/v1/rpc/show_limit', {
    method: 'POST',
    headers: { 'content-type': 'application/json', prefer: 'return=representation' },
    body: '{}'
  });
  const rewrittenRequest = normaliseMigetPostgrestRequest(request);
  assert.ok(rewrittenRequest instanceof Request);
  assert.equal(new URL(rewrittenRequest.url).pathname, '/rpc/show_limit');
  assert.equal(rewrittenRequest.method, 'POST');
  assert.equal(rewrittenRequest.headers.get('prefer'), 'return=representation');
});
