import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const directory = path.dirname(fileURLToPath(import.meta.url));
const wrangler = fs.readFileSync(path.resolve(directory, '../wrangler.toml'), 'utf8');
const testEnvironment = wrangler.split('[env.test.vars]')[1]?.split('[[env.test.r2_buckets]]')[0] || '';

test('normal TEST Worker supplies the Candidate Office environment authority', () => {
  assert.match(wrangler, /\[env\.test\]\s*\r?\nname\s*=\s*"test-cloudtms-backend"/);
  assert.match(testEnvironment, /^CANDIDATE_APP_ENVIRONMENT\s*=\s*"TEST"\s*$/m);
  assert.equal((testEnvironment.match(/^CANDIDATE_APP_ENVIRONMENT\s*=/gm) || []).length, 1);
});
