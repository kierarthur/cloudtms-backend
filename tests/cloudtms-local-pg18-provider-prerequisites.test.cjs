const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sql = fs.readFileSync(path.join(
  __dirname,
  'fixtures',
  '05092026_1100_cloudtms_local_pg18_provider_prerequisites.sql',
), 'utf8');

test('PG18 provider compatibility fixture is local, version-bounded and empty-database only', () => {
  assert.match(sql, /current_database\(\) !~ '\^banking_modal_v2_/);
  assert.match(sql, /server_version_num'\)::integer NOT BETWEEN 180000 AND 189999/);
  assert.match(sql, /inet_server_addr\(\) IS NULL/);
  assert.match(sql, /CLOUDTMS_LOCAL_PG18_PROVIDER_FIXTURE_TARGET_INVALID/);
  assert.match(sql, /CLOUDTMS_LOCAL_PG18_PROVIDER_FIXTURE_REQUIRES_EMPTY_DATABASE/);
  assert.match(sql, /n\.nspname IN \('public', 'private', 'auth', 'vault'\)/);
  assert.doesNotMatch(sql, /\b(?:DELETE|UPDATE|TRUNCATE|DROP)\b/i);
});

test('PG18 fixture supplies only the source-consumed auth and vault boundary', () => {
  assert.match(sql, /CREATE TABLE auth\.users \(id uuid PRIMARY KEY\)/);
  assert.match(sql, /CREATE FUNCTION auth\.uid\(\)/);
  assert.match(sql, /CREATE FUNCTION auth\.role\(\)/);
  assert.match(sql, /CREATE FUNCTION auth\.jwt\(\)/);
  assert.match(sql, /CREATE TABLE vault\.secrets/);
  assert.match(sql, /CREATE VIEW vault\.decrypted_secrets/);
  assert.match(sql, /CREATE FUNCTION vault\.create_secret\([\s\S]*new_key_id uuid DEFAULT NULL/);
  assert.match(sql, /SECURITY DEFINER[\s\S]*SET search_path TO ''/);
  assert.equal((sql.match(/^CREATE FUNCTION vault\./gm) || []).length, 1);
  assert.equal((sql.match(/^CREATE TABLE vault\./gm) || []).length, 1);
  assert.equal((sql.match(/^CREATE VIEW vault\./gm) || []).length, 1);
});
