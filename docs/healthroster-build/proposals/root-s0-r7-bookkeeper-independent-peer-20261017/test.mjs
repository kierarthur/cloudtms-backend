import fs from 'node:fs';
import vm from 'node:vm';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';

// Execute only the captured publication tail and parent predicate in a closed VM.
// The actual module, its imports, file writes, Git commands and native readers never run.
const source = fs.readFileSync(new URL('./reference/r7.mjs', import.meta.url), 'utf8');
const tail = source.slice(source.indexOf("g(i,'add','--',...files);"));
assert.ok(tail.startsWith("g(i,'add','--',...files);"));
const base = '1'.repeat(40), before = '2'.repeat(40), current = '3'.repeat(40);
function replay(phase, fault = null) {
  const calls = [], output = [], files = ['owned.json'];
  const saved = Buffer.from('exact owned evidence');
  const history = [base, before];
  const remote = [base];
  const g = (_root, ...args) => {
    calls.push(args);
    if (args[0] === 'show') return fault === 'index' ? Buffer.from('dirty') : saved;
    if (args[0] === 'diff' && args.includes('--check')) {
      if (fault === 'diff') throw Error('DIFF_CHECK');
      return Buffer.alloc(0);
    }
    if (args[0] === 'diff') return Buffer.from(fault === 'staged' ? 'unrelated.json\0owned.json\0' : 'owned.json\0');
    if (args[0] === 'commit') {
      if (fault === 'commit') throw Error('COMMIT');
      assert.deepEqual(args.slice(-3), ['--only', '--', 'owned.json']);
      history.push(current); return Buffer.alloc(0);
    }
    if (args[0] === 'rev-parse') return Buffer.from(current + '\n');
    if (args[0] === 'push') {
      if (fault === 'push') throw Error('PUSH');
      assert.equal(args[2], 'HEAD:refs/heads/codex/healthroster-install2-14102026');
      remote.splice(0, remote.length, ...history); return Buffer.alloc(0);
    }
    if (args[0] === 'ls-remote') return Buffer.from((fault === 'remote' ? base : remote.at(-1)) + '\trefs/heads/codex/healthroster-install2-14102026\n');
    assert.equal(args[0], 'add'); return Buffer.alloc(0);
  };
  const context = { g, i: 'closed-mock', files, phase, fs: {readFileSync: () => saved}, console: {log: x => output.push(JSON.parse(x))}, Buffer };
  let error;
  try { vm.runInNewContext(tail, context, {timeout: 1000}); } catch (e) {error = e.message;}
  return {calls, output, remote, history, error};
}

test('BEFORE commits exact owned paths locally; no push or remote proof is claimed', () => {
  const r = replay('BEFORE'); assert.equal(r.error, undefined);
  assert.deepEqual(r.remote, [base]); assert.equal(r.history.at(-1), current);
  assert.equal(r.calls.filter(x => ['push','ls-remote'].includes(x[0])).length, 0);
  assert.equal(r.output[0].status, 'ROOT_ACTUAL_BEFORE_COMMITTED');
});
for (const phase of ['SETUP', 'AFTER']) test(`${phase} pushes complete modeled ancestry and checks exact remote before success`, () => {
  const r = replay(phase); assert.equal(r.error, undefined);
  assert.deepEqual(r.remote, [base, before, current]);
  assert.deepEqual(r.calls.slice(-3).map(x => x[0]), ['rev-parse','push','ls-remote']);
  assert.equal(r.output.length, 1); assert.equal(r.output[0].commit, current);
  assert.equal(r.output[0].status, `ROOT_ACTUAL_${phase}_COMMITTED`);
});
for (const phase of ['SETUP','AFTER']) for (const fault of ['push','remote']) test(`${phase} ${fault} failure cannot emit committed status`, () => {
  const r = replay(phase, fault); assert.equal(r.error, fault.toUpperCase()); assert.equal(r.output.length, 0);
});
for (const [fault, error] of [['index','INDEX'],['staged','EXACT_OWNED_INDEX_REQUIRED'],['diff','DIFF_CHECK'],['commit','COMMIT']]) test(`exact ${fault} refusal precedes publication`, () => {
  const r = replay('SETUP', fault); assert.equal(r.error, error); assert.equal(r.output.length, 0);
  assert.equal(r.calls.some(x => x[0] === 'push'), false);
});

const start = source.indexOf('  if(sha(old)!==');
const stop = source.indexOf('\n  fs.writeFileSync', start);
assert.ok(start >= 0 && stop > start);
const predicate = source.slice(start, stop);
const exact = Buffer.from('closed synthetic parent');
const sha = x => crypto.createHash('sha256').update(x).digest('hex');
const fixedHash = source.match(/if\(sha\(old\)!=='([a-f0-9]{64})'/)[1];
function parentCheck(fault) {
  const calls = [];
  const g = (_i, cmd, spec) => {
    calls.push([cmd,spec]);
    return (fault === spec.split(':')[0]) ? Buffer.from('different') : exact;
  };
  const hash = x => fault === 'saved' ? sha(x) : fixedHash;
  let error;
  try {vm.runInNewContext(predicate, {old: exact, g, i:'closed', f:'effect.json', sha:hash}, {timeout:1000});} catch(e) {error = e.message;}
  return {error,calls};
}
test('replacement predicate retains exact saved/hash/index/HEAD/24805 body requirements', () => {
  const r = parentCheck(null); assert.equal(r.error, undefined);
  assert.deepEqual(r.calls.map(x => x[1]), ['HEAD:effect.json',':effect.json','24805aff1cf148b0cb3eefc47cbf04e2b67cac29:effect.json']);
  for (const fault of ['saved','HEAD','','24805aff1cf148b0cb3eefc47cbf04e2b67cac29']) assert.equal(parentCheck(fault).error, 'EXACT_R4_EFFECT_PARENT');
});
