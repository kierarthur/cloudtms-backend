import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const sourcePath = new URL(
  '../infra/miget/cloudtms-live-postgrest-gateway/src/index.ts',
  import.meta.url,
);
const configPath = new URL(
  '../infra/miget/cloudtms-live-postgrest-gateway/wrangler.jsonc',
  import.meta.url,
);
const source = fs.readFileSync(sourcePath, 'utf8');
const config = fs.readFileSync(configPath, 'utf8');
const rootWrangler = fs.readFileSync(new URL('../wrangler.toml', import.meta.url), 'utf8');

test('LIVE gateway is isolated from the TEST and MCP gateways', () => {
  assert.match(config, /"name":\s*"cloudtms-live-miget-gateway"/);
  assert.doesNotMatch(config, /codex-cloudtms-miget-gateway/);
  assert.doesNotMatch(source, /\/mcp|MIGET_API_TOKEN|agency_test|mytms_test/);
});

test('LIVE gateway preserves the existing rest-v1 contract and requires HTTPS upstream', () => {
  assert.match(source, /const REST_PREFIX = "\/rest\/v1"/);
  assert.match(source, /origin\.protocol !== "https:"/);
  assert.match(source, /headers\.delete\(name\)/);
  assert.match(source, /headers\.set\("x-cloudtms-miget-gateway", VERSION\)/);
  assert.match(source, /request\.method !== "GET" && request\.method !== "HEAD"/);
});

test('LIVE gateway health is credential-free and fail-closed when unconfigured', () => {
  assert.match(source, /url\.pathname === "\/health"/);
  assert.match(source, /upstream_configured: configured/);
  assert.match(source, /configured \? 200 : 503/);
  assert.doesNotMatch(source, /LIVE_POSTGREST_ORIGIN\s*[,}]/);
});

test('declared LIVE backend configuration routes through the isolated Miget gateway', () => {
  const production = rootWrangler.split('[env.production]')[1] ?? '';
  assert.match(production, /SUPABASE_URL\s*=\s*"https:\/\/cloudtms-live-miget-gateway\.kier-88a\.workers\.dev"/);
  assert.doesNotMatch(production, /SUPABASE_URL\s*=\s*"https:\/\/[^"\s]+\.supabase\.co"/);
});
