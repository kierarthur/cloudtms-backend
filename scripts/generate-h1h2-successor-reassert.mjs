#!/usr/bin/env node
// A resumed H1/H2 closure can replay historical Banking Pay function bodies
// after their later successors have already been ledgered. Copy only the exact
// current function definitions from those reviewed successor files.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const outputPath = 'supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql';
const owners = [
  ['29082026_0326_banking_pay_release_authority_repair_v1.sql', ['public.contract_week_manual_upsert_atomic']],
  ['04092026_2355_banking_pay_workbench_selection_owner_reassert_v1.sql', ['public.pay_workbench_session_set_selected_rows']],
  ['08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql', ['private.pay_workbench_candidate_dirty_cohort_stage_v1', 'public.pay_workbench_candidate_dirty_apply_job_process']],
  ['08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql', ['public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1']],
  ['26092026_0201_banking_pay_stage2_financial_readers_v1.sql', ['public._pay_active_settled_components']],
  ['26092026_0202_banking_pay_stage2_source_authorisation_v1.sql', [
    'public.pay_workbench_scope_change_tx_token_v1',
    'public.pay_workbench_contract_client_dirty_fanout_chunk',
    'public.timesheet_authorise_generic_atomic',
  ]],
  ['26092026_0203_banking_pay_stage2_workbench_draft_v1.sql', [
    'private.pay_sync_overpayments_from_workbench_workspace_v1',
    'public.pay_workbench_dirty_event_enqueue',
    'public.pay_workbench_mark_candidate_dirty',
    'public.pay_workbench_scope_blocker_state_v1',
    'public.pay_workbench_scope_reconcile_drain_one_v1',
  ]],
  ['26092026_0207_banking_pay_stage2_recovery_order_floor_v1.sql', [
    'private.pay_workbench_recovery_selection_overlay_apply_v1',
    'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1',
  ]],
];

function extract(source, identity) {
  const startRegex = new RegExp(`^CREATE OR REPLACE FUNCTION ${identity.replaceAll('.', '\\.')}\\(`, 'gm');
  const matches = [...source.matchAll(startRegex)];
  const overloadedRecovery = identity === 'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1';
  assert.equal(matches.length, overloadedRecovery ? 2 : 1,
    `${identity} must have the expected current owner definition count`);
  // The historical replay overwrites the three-argument overload, which is
  // the second definition in its current owner file.
  const start = matches[overloadedRecovery ? 1 : 0].index;
  const opening = /^AS (\$[A-Za-z0-9_]*\$)\s*$/gm;
  opening.lastIndex = start;
  const opener = opening.exec(source);
  assert.ok(opener, `${identity}: missing dollar-quoted function body`);
  const tag = opener[1];
  const closeRegex = new RegExp(`^${tag.replaceAll('$', '\\$')}\\s*;`, 'gm');
  closeRegex.lastIndex = opening.lastIndex;
  const closer = closeRegex.exec(source);
  assert.ok(closer, `${identity}: missing function terminator`);
  const definition = source.slice(start, closeRegex.lastIndex);
  assert.equal([...definition.matchAll(/^CREATE OR REPLACE FUNCTION /gm)].length, 1,
    `${identity}: extraction crossed another function boundary`);
  return definition;
}

const sections = [];
for (const [filename, identities] of owners) {
  const sourcePath = `supabase/repeatable/${filename}`;
  const source = fs.readFileSync(path.join(root, sourcePath), 'utf8').replaceAll('\r\n', '\n');
  // Retain the owner's own order in case one replacement depends on another.
  const definitions = identities.map(identity => ({identity, definition: extract(source, identity)}));
  definitions.sort((a, b) => source.indexOf(a.definition) - source.indexOf(b.definition));
  for (const {identity, definition} of definitions) {
    const digest = crypto.createHash('sha256').update(definition).digest('hex');
    sections.push(`-- ${identity} from ${sourcePath}; definition SHA-256 ${digest}.\n${definition}`);
  }
}
const output = [
  '-- Exact current successors after a resumed historical H1/H2 authority closure.',
  '-- This restores already-reviewed function bodies; it does not invoke payment, provider or external effects.',
  '\\set ON_ERROR_STOP on',
  'begin;',
  ...sections,
  'commit;',
  '',
].join('\n\n');
const target = path.join(root, outputPath);
if (process.argv[2] === '--check') {
  assert.equal(fs.readFileSync(target, 'utf8').replaceAll('\r\n', '\n'), output);
  console.log(`${sections.length} H1/H2 successor definitions match their reviewed owners.`);
} else if (process.argv[2] === '--write') {
  fs.writeFileSync(target, output, 'utf8');
  console.log(`Wrote ${outputPath} with ${sections.length} definitions.`);
} else {
  throw new Error('Use --check or --write');
}
