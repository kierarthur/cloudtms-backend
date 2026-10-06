import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {repoRoot} from '../../scripts/cloudtms-db-release-lib.mjs';

const local = readFileSync(path.join(repoRoot,
  'supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql'), 'utf8');
const definition = name => {
  const matches = [...local.matchAll(new RegExp(`create or replace function (?:public|private)\\.${name}\\([\\s\\S]*?\\$function\\$;`, 'gi'))];
  assert.equal(matches.length, 1);
  return matches[0][0];
};

test('pending completion cannot retire contacts before real publication', () => {
  const sql = definition('weekly_exceptional_pay_complete_local_v1');
  assert.match(sql, /v_pending:=coalesce\(\(v_write->>'published'\)::boolean,false\) is not true;/);
  assert.match(sql, /if v_pending and v_write->>'code'<>'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'/);
  const guardedCall = /if not v_pending then\s+perform private\.weekly_source_protected_contact_retire_v1\(v_family\.id,v_approval\.work_event_id\);\s+end if;/g;
  assert.equal([...sql.matchAll(guardedCall)].length, 1);
  assert.doesNotMatch(sql.replace(guardedCall, ''), /protected_contact_retire_v1/,
    'no unconditional or second completion retirement call');
  assert(sql.indexOf('update private.weekly_source_local_protected_decision_receipts')
    < sql.search(guardedCall), 'completion receipt precedes guarded contact lifecycle');
});

test('existing deferred-release owner alone retires contacts after proved live receipt', () => {
  const sql = definition('weekly_source_local_protected_pending_released_v1');
  assert.match(sql, /if new\.state<>'RELEASED' or old\.state='RELEASED' then return new; end if;/);
  assert.match(sql, /WEEKLY_PROTECTED_LOCAL_DEFERRED_RECEIPT_INVALID/);
  assert.match(sql, /WEEKLY_PROTECTED_LOCAL_DEFERRED_RUN_INVALID/);
  const call = 'perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_work_event_id);';
  assert.equal(sql.split(call).length-1, 1);
  assert(sql.indexOf(call)>sql.indexOf('WEEKLY_PROTECTED_LOCAL_DEFERRED_RUN_INVALID'));
  assert(sql.indexOf(call)>sql.indexOf("jsonb_build_object('outcome','PUBLISHED','state','LIVE'"));
});

const nativeHarness = readFileSync(path.join(repoRoot,
  'tests/run-bp-source-local-native.mjs'), 'utf8');
const countStart = '  const bankContactCount=';
const countEnd = '  const sourceForBankComparison=';
assert.equal(nativeHarness.split(countStart).length, 2);
assert.equal(nativeHarness.split(countEnd).length, 2);
// Exercise the actual harness guard, not a second implementation of its count.
const classifyHooks = new Function('bankLocalMatches', 'contactRetirementHunk',
  'priorContactHunk', 'assert', nativeHarness.slice(nativeHarness.indexOf(countStart),
    nativeHarness.indexOf(countEnd)) + '\nreturn {bankContactCount,bankPriorContactCount};');
const priorHook = '  perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);\n';
const currentHook = '  -- Accepted pending approval is not publication. The existing release\n'+
  '  -- trigger retires its contacts only after the deferred receipt is proved.\n'+
  '  if not v_pending then\n'+
  '    perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);\n'+
  '  end if;\n';
const classify = body => classifyHooks([[body]], currentHook, priorHook, assert);

test('native guard counts the exact guarded hook once, not also as an old call', () => {
  assert.deepEqual(classify(currentHook), {bankContactCount:1,bankPriorContactCount:0});
  assert.deepEqual(classify(priorHook), {bankContactCount:0,bankPriorContactCount:1});
  assert.deepEqual(classify(''), {bankContactCount:0,bankPriorContactCount:0});
});

test('native guard still refuses mixed or duplicated contact retirement hooks', () => {
  assert.throws(() => classify(currentHook + priorHook), /never both contact hook revisions/);
  assert.throws(() => classify(currentHook + currentHook), /at most one exact accepted Banking contact hook/);
  assert.throws(() => classify(priorHook + priorHook), /at most one exact prior Banking contact hook/);
});
