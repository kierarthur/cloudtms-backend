import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import assert from 'node:assert/strict';
import { composeOwnerSource, sourceSha256 } from '../scripts/healthroster-owner-source-compose.mjs';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const folder = path.join(repo, 'docs/healthroster-build/proposals/office-report-d10-paid-match-union-20261014');
const read = file => fs.readFileSync(path.join(folder, file), 'utf8');
const before = read('reference/workspace.report-d10-before.sqlinc');
const after = read('forward/workspace.report-d10-paid-match.sqlinc');
const reverse = JSON.parse(read('COMPLETE_CURRENT_REVERSE.json'));

test('real six-hunk shared owner composition reverses exactly to the complete reviewed Report and D10 input', () => {
  assert.equal(reverse.hunks.length, 6);
  const composed = composeOwnerSource({ source: before, expectedSha256: reverse.before_sha256, hunks: reverse.hunks });
  assert.equal(composed.output, after);
  assert.equal(sourceSha256(after), '4528658d02b38f8b946e86751b2ec50f5c7b1c0b03d7d3ca3f9deaa1df889fb2');
  assert.equal(sourceSha256(read('reference/workspace.current-root-before.sqlinc')),
    '0693663d6e2bec8846e24d610e7a72d84b88f45fbc557595aecde78a733ece44');
});

test('complete Report query population, source selection proofs and PAY reduction remain unchanged by paid match', () => {
  const queriesStart = '  -- One shared native parent census.';
  const end = '  if v_tab=\'imports\' then';
  const part = (source, start, stop) => {
    const at = source.indexOf(start), to = stop ? source.indexOf(stop, at) : source.length;
    assert.ok(at >= 0 && to > at, 'Both exact retained block anchors must exist');
    return source.slice(at, to);
  };
  assert.equal(sourceSha256(part(after, queriesStart, end)), sourceSha256(part(before, queriesStart, end)));
  const population = "  if v_tab='queries' and v_publication_id is not null and not v_d10_target then";
  const populationEnd = '  v_queries:=v_queries||pg_catalog.jsonb_build_object';
  assert.equal(sourceSha256(part(after, population, populationEnd)), sourceSha256(part(before, population, populationEnd)));
  const pay = '  -- Reduce the existing Hours-question projection';
  assert.equal(sourceSha256(part(after, pay)), sourceSha256(part(before, pay)));
  assert.equal((after.match(/create or replace function /gi) ?? []).length, 1);
  assert.match(after, /create or replace function public\.weekly_source_office_workspace_v1\(/i);
});

test('each current paid-match question edit is present exactly once in the composed owner', () => {
  for (const hunk of reverse.hunks) assert.equal(after.split(hunk.after).length, 2, hunk.name);
  assert.match(after, /private\.weekly_source_paid_match_question_v1\(v_publication\.id,source_row\.id,v_actor\)/);
  assert.match(after, /'paid_match_question',paid_question\.value/);
});

test('supporting scope, question helper and ACL files retain exact independently reviewed source identities', () => {
  assert.equal(sourceSha256(read('forward/scopes.report-d10.sqlinc')),
    'ef29865225c8a487e4ae5e082fa6ce7afcc12d11e823c88da58a83b5e29a5fac');
  const manifest = JSON.parse(read('SOURCE_MANIFEST.json'));
  for (const entry of manifest.entries) {
    const raw = fs.readFileSync(path.join(folder, entry.path));
    assert.equal(raw.length, entry.bytes, entry.path);
    assert.equal(sourceSha256(raw), entry.sha256, entry.path);
  }
  assert.equal(manifest.native, false);
  assert.equal(manifest.independent_review_pending, true);
});

test('union introduces no schema-qualified PostgreSQL syntax constructs or parallel business store', () => {
  for (const file of ['forward/workspace.report-d10-paid-match.sqlinc', 'forward/scopes.report-d10.sqlinc',
    'forward/paid-match-question.sqlinc', 'forward/workspace-acl.sqlinc']) {
    const source = read(file);
    assert.doesNotMatch(source, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
    assert.doesNotMatch(source, /\bcreate\s+(?:unlogged\s+)?table\b|\balter\s+table\b/i);
  }
});
