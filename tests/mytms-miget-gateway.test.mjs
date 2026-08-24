import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const source = fs.readFileSync(
  new URL('../infra/miget/cloudtms-mytms-postgrest-gateway/src/index.ts', import.meta.url),
  'utf8',
);
const config = fs.readFileSync(
  new URL('../infra/miget/cloudtms-mytms-postgrest-gateway/wrangler.jsonc', import.meta.url),
  'utf8',
);
const generatedTypes = fs.readFileSync(
  new URL('../infra/miget/cloudtms-mytms-postgrest-gateway/worker-configuration.d.ts', import.meta.url),
  'utf8',
);

test('MyTMS gateway is permanent, isolated and generated-binding typed', () => {
  assert.match(config, /"name":\s*"cloudtms-mytms-miget-gateway"/);
  assert.match(config, /"compatibility_date":\s*"2026-08-24"/);
  assert.match(config, /"nodejs_compat"/);
  assert.match(generatedTypes, /MIGET_POSTGREST_ORIGIN/);
  assert.match(source, /reference path="\.\.\/worker-configuration\.d\.ts"/);
  assert.doesNotMatch(source, /interface Env/);
  assert.doesNotMatch(source, /MIGET_API_TOKEN|service_role_jwt|password|connection_string/i);
});
test('MyTMS gateway preserves rest-v1 and allows only control-plane RPC schemas', () => {
  assert.match(source, /const REST_PREFIX = "\/rest\/v1"/);
  assert.match(source, /new Set\(\["identity", "control", "google_control"\]\)/);
  assert.match(source, /contentProfile !== acceptProfile/);
  assert.match(source, /MYTMS_SCHEMA_NOT_ALLOWED/);
  assert.match(source, /origin\.protocol !== "https:"/);
  assert.match(source, /new Response\(response\.body/);
});

test('MyTMS gateway has credential-free health and explicit failure handling', () => {
  assert.match(source, /url\.pathname === "\/health"/);
  assert.match(source, /upstream_configured: configured/);
  assert.match(source, /configured \? 200 : 503/);
  assert.match(source, /MYTMS_GATEWAY_UNAVAILABLE/);
});
