import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// Source-only peer check. Does not write SQL, connect to a database, or install.
const out = path.dirname(fileURLToPath(import.meta.url));
const report = path.resolve(out, '../../../..');
const i2 = path.resolve(process.argv[2] ?? path.join(report, '../healthroster-install2-14102026'));
const oldCommit = 'a6739abd';
const oldBase = 'docs/healthroster-build/proposals/kept-pay-identical-rekey-current-root-forward-20261014';
const sha = value => createHash('sha256').update(value).digest('hex');
const git = file => execFileSync('git', ['--no-replace-objects', 'show', `${oldCommit}:${oldBase}/${file}`], { cwd: report, maxBuffer: 8 * 1024 * 1024 });
const parserFile = 'docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const parserBytes = execFileSync('git', ['--no-replace-objects', 'show', `${oldCommit}:${parserFile}`], { cwd: report, maxBuffer: 1024 * 1024 });
const { functionEvents } = await import('data:text/javascript;base64,' + parserBytes.toString('base64'));
const metadata = JSON.parse(git('KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json'));
const publicFile = 'supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc';
const privateFile = 'supabase/repeatable/13102026_1705_weekly_source_invoice_adjustments/00_adjustment_owners.sqlinc';
const oldUnlock = '        and tfu_lock.locked_by_invoice_id = p_invoice_id;';
const currentUnlock = `        and tfu_lock.locked_by_invoice_id = p_invoice_id
        and private.invoice_contract_move_selected_financial_authorise_v1(to_jsonb(tfu_lock),
          to_jsonb(tfu_lock)||jsonb_build_object('locked_by_invoice_id',null,'locked_at_utc',null,'updated_at',v_now),p_invoice_id);`;
const checks = [];
function check(label, fn) { fn(); checks.push(label); }
const currentRegistry = JSON.parse(fs.readFileSync(path.join(i2, 'docs/bpay-next/final-definer-closures.json'), 'utf8'));
check('Registered public final owner is the complete 0955 owner', () => {
  const owner = currentRegistry.chains.find(row => row.signature === 'public.invoice_apply_edits(uuid,jsonb,uuid)');
  assert.equal(owner.finalPhysical, publicFile);
  assert.equal(owner.finalRoot, 'supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority_v1.sql');
});
const owners = [];
for (const change of metadata.changes.filter(row => row.signature)) {
  const file = change.signature.startsWith('public.') ? publicFile : privateFile;
  const raw = fs.readFileSync(path.join(i2, file));
  const matches = functionEvents(raw.toString('utf8')).events.filter(event => event.kind === 'CREATE' && event.id === change.signature);
  check(`One complete current declaration: ${change.signature}`, () => assert.equal(matches.length, 1));
  const current = matches[0].definition;
  const beforeBytes = git(change.before_file);
  const afterBytes = git(change.forward_file);
  check(`Frozen old whole-leaf pins: ${change.signature}`, () => {
    assert.equal(sha(beforeBytes), change.before_sha256);
    assert.equal(sha(afterBytes), change.after_sha256);
  });
  const before = beforeBytes.toString('utf8').trimEnd();
  check(`Exact current/predecessor relation: ${change.signature}`, () => {
    if (change.signature.startsWith('public.')) {
      assert.equal(current.split(currentUnlock).length, 2);
      assert.equal(current.replace(currentUnlock, oldUnlock), before);
    } else assert.equal(current, before);
  });
  let mechanical = current;
  for (const hunk of change.hunks) {
    assert.equal(mechanical.split(hunk.before).length, 2);
    mechanical = mechanical.replace(hunk.before, hunk.after);
  }
  let reverse = mechanical;
  for (const hunk of change.hunks.toReversed()) {
    assert.equal(reverse.split(hunk.after).length, 2);
    reverse = reverse.replace(hunk.after, hunk.before);
  }
  check(`All old hunks have unique whole-current reverse: ${change.signature}`, () => {
    assert.equal(reverse, current);
    const declarations = functionEvents(mechanical).events;
    assert.equal(declarations.length, 1);
    assert.equal(declarations[0].id, change.signature);
    if (change.signature.startsWith('public.')) assert.ok(mechanical.includes(currentUnlock));
  });
  owners.push({ signature: change.signature, current_file: file, current_file_sha256: sha(raw),
    current_complete_definition_sha256: sha(current), current_definition_lines: current.split('\n').length,
    frozen_before_file: change.before_file, frozen_before_sha256: sha(beforeBytes),
    frozen_after_file: change.forward_file, frozen_after_sha256: sha(afterBytes),
    old_hunk_count: change.hunks.length, old_hunks_uniquely_reversible_on_current: true,
    mechanical_only_not_a_successor_or_semantic_approval: true });
}
check('Old context/facts helpers have no managed SQL declaration at this source observation', () => {
  let matches = '';
  try { matches = execFileSync('rg', ['-l', '-U', '-i', 'create\\s+(?:or\\s+replace\\s+)?function\\s+private[.]weekly_source_kept_as_paid_(?:facts|context)_v1\\s*[(]', 'supabase/repeatable', 'supabase/migrations', '-g', '*.sql', '-g', '*.sqlinc'], { cwd: i2, encoding: 'utf8' }); }
  catch (error) { if (error.status !== 1) throw error; }
  assert.equal(matches, '');
});
check('Historical context depends on its new decision table; no implicit new-store admission', () => {
  assert.ok(git('forward/06_kept_as_paid_context.sqlinc').toString('utf8').includes('public.weekly_source_kept_as_paid_decisions%rowtype'));
});
const receipt = { source_only: true, native_status: 'NOT_RUN', old_feature_commit: execFileSync('git', ['rev-parse', oldCommit], { cwd: report, encoding: 'utf8' }).trim(),
  parser: { commit: oldCommit, file: parserFile, sha256: sha(parserBytes) },
  old_selected_base: metadata.current_selected_commit, old_owner_receipt_sha256: sha(git('KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json')),
  current_i2_observed_commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: i2, encoding: 'utf8' }).trim(),
  current_registry_sha256: sha(fs.readFileSync(path.join(i2, 'docs/bpay-next/final-definer-closures.json'))), owners,
  old_credit_admit_sha256: sha(git('forward/08_credited_targets_admit.sqlinc')),
  managed_old_context_facts_declarations: 0,
  checks_passed: checks.length, checks };
console.log(JSON.stringify(receipt, null, 2));
