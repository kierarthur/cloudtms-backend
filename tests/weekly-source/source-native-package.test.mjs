import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { exactAtomicBody, scanManagedSql } from '../../scripts/cloudtms-managed-release-sql.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const base = path.join(root, 'docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015');
const read = relative => readFileSync(path.join(base, relative), 'utf8');
const sha = text => createHash('sha256').update(text).digest('hex');
const manifest = JSON.parse(read('SOURCE_PACKAGE_RECEIPT.json'));
const receipt = JSON.parse(read('NATIVE_ENTRY_RECEIPT.json'));

test('Source package byte pins and atomic packaging classification match every saved SQL and runtime input', () => {
  let tables = 0;
  for (const stage of manifest.stages) {
    assert.ok(stage.requires.length);
    for (const file of stage.files) {
      const sql = read(file.path);
      assert.equal(sha(sql), file.sha256, file.path);
      assert.equal(Buffer.byteLength(sql), file.bytes);
      const parsed = scanManagedSql(exactAtomicBody(sql));
      assert.equal(parsed.statements.length, file.packaging_statements);
      for (const row of parsed.statements) assert.notEqual(row.unterminated, true);
      tables += [...sql.matchAll(/create table private\.weekly_source_pay_decision[a-z_]*\s*\(/g)].length;
      assert.doesNotMatch(sql, /create(?: or replace)? function private\.bpay_next_/i);
    }
  }
  assert.equal(tables, 11);
  for (const file of manifest.runtime) {
    const text = readFileSync(path.join(root, file.path), 'utf8');
    assert.equal(sha(text), file.sha256, file.path);
  }
  assert.equal(manifest.qualification, 'PARTIAL_SOURCE_NOT_INSTALLABLE_NATIVE_REQUIRED_NOT_RUN');
  assert.equal(manifest.native_tests, 'REQUIRED_NOT_RUN');
  assert.ok(manifest.open_health_owners.length >= 7);
});

for (const row of receipt.owners) test(`Complete ordinary native owner reverses exactly: ${row.name}`, () => {
  const output = read(row.output);
  assert.equal(sha(output), row.output_sha256);
  const name = row.name.slice('public.'.length, -'(jsonb)'.length);
  const parent = execFileSync('git', ['show', `${receipt.input_commit}:${row.input}`], {
    cwd: root, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
  });
  const start = parent.indexOf(`create or replace function public.${name}(`);
  assert.ok(start >= 0);
  const end = parent.indexOf('$function$;', parent.indexOf('as $function$', start));
  const original = parent.slice(start, end + '$function$;'.length) + '\n';
  assert.equal(sha(original), row.original_sha256);
  let reversed = output.replace('declare\n  v_source_gate jsonb;\n', 'declare\n');
  if (name === 'weekly_exceptional_pay_stage_c1_request_v1') {
    reversed = reversed.replace("  perform private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor_user_id,'STAGE_LOCAL');\n", '');
  } else if (name === 'weekly_exceptional_pay_complete_local_v1') {
    reversed = reversed.replace("  select * into strict v_request from public.weekly_exceptional_c1_publication_requests where id=v_request_id;\n  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(\n    v_request.family_id,v_request.orchestration_run_id,v_actor,'COMPLETE_LOCAL');\n  if v_source_gate is not null then return v_source_gate; end if;\n", '');
  } else {
    const phase = name === 'weekly_exceptional_pay_next_publication_status_v1' ? 'STATUS' : 'PUBLISH_NEXT';
    reversed = reversed.replace(`  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor,'${phase}');\n  if v_source_gate is not null then return v_source_gate; end if;\n`, '');
  }
  assert.equal(reversed, original);
});

test('Every new service RPC has a closed service ACL and private helpers revoke browser/service execute', () => {
  const sql = manifest.stages.find(s => s.id === 'HEALTH_OWNERS').files.map(f => read(f.path)).join('\n');
  const declared = [...sql.matchAll(/create or replace function\s+([a-z_0-9.]+)\s*\(([^)]*)\)/gi)];
  for (const [, name] of declared) {
    const escaped = name.replaceAll('.', '\\.');
    assert.match(sql, new RegExp(`revoke all on function ${escaped}\\([^;]*?from public,anon,authenticated${name.startsWith('private.') ? ',service_role' : ''};`, 's'));
    if (name.startsWith('public.')) assert.match(sql, new RegExp(`grant execute on function ${escaped}\\(jsonb\\) to service_role;`));
    else assert.doesNotMatch(sql, new RegExp(`grant execute on function ${escaped}\\(`));
  }
});
