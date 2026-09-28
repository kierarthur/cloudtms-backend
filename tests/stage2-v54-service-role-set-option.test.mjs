// Stage 2 v5.4: the hosted Miget release owner is a member of service_role WITHOUT the SET option
// (pg_has_role(current_user,'service_role','SET') = false), so no release verifier may switch role unguarded.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => fs.readFileSync(path.join(repoRoot, relative), 'utf8').replaceAll('\r\n', '\n');
const F8 = 'supabase/verification/17092026_0200_weekly_source_rotation_authority_v1.sql';
const roleSwitch = /\b(?:set\s+(?:local\s+|session\s+)?role|reset\s+role|set\s+session\s+authorization)\b|set_config\s*\(\s*'role'/gi;
const executable = text => text.split('\n').map(line => line.replace(/--.*$/, '')).join('\n');

test('every release verifier role switch is the guarded F8 block', () => {
  const release = JSON.parse(read('supabase/release/current-release.json'));
  const files = [...new Set([...release.verificationFiles, ...release.newVerificationFiles])];
  const switching = files.filter(file => (executable(read(file)).match(roleSwitch) ?? []).length > 0);
  assert.deepEqual(switching, [F8]);
});

test('F8 keeps the runtime SET ROLE proof only when SET is allowed and proves the same facts from the catalogue otherwise', () => {
  const text = executable(read(F8));
  const start = text.indexOf('do $f8_invoker$');
  const end = text.indexOf('$f8_invoker$;', start + 1);
  assert.ok(start > 0 && end > start);
  const block = text.slice(start, end);
  assert.equal((text.match(roleSwitch) ?? []).length, 2, 'only the one set/reset pair in the file');
  const guard = block.indexOf("if pg_catalog.pg_has_role(current_user,'service_role','SET') then");
  const set = block.indexOf('set local role service_role;');
  const reset = block.indexOf('reset role;');
  const otherwise = block.indexOf('else', reset);
  assert.ok(guard > 0 && guard < set && set < reset && reset < otherwise, 'SET ROLE only inside the SET-option branch');
  const fallback = block.slice(otherwise, block.indexOf('end if;\n  if not v_refused then'));
  assert.match(fallback, /v_refused:=not pg_catalog\.has_function_privilege\('service_role',v_guard,'EXECUTE'\);/);
  assert.match(fallback, /has_schema_privilege\('service_role','private','USAGE'\)/);
  assert.match(fallback, /has_function_privilege\('service_role',v_shim,'EXECUTE'\)/);
  assert.match(fallback, /proc\.prosecdef/);
  assert.match(fallback, /v_decision:=private\.weekly_source_managed_root_guard_decision_v1\(/);
  // the shared assertions still run after both branches, unchanged
  assert.match(block, /if not v_refused then\s+raise exception 'ASSERTION_FAILED: service_role could execute the guard directly';/);
  assert.match(block, /if v_decision is null or not \(v_decision \? 'managed'\) then/);
});
