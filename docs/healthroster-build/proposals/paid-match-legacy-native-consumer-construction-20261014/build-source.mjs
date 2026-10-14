import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const dir = path.dirname(fileURLToPath(import.meta.url)), root = path.resolve(dir, '../../../..');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const commit = '65c82dcc6d8f2748fef7c4209d96a673f7ec8756';
const source = 'docs/healthroster-build/proposals/paid-match-source-expense-native-construction-20261014/forward/07_complete_fin4_expense.sqlinc';
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
for (const p of ['before', 'forward', 'input']) fs.mkdirSync(path.join(dir, p), { recursive: true });
const raw = fs.readFileSync(path.join(root, source));
assert.deepEqual(raw, execFileSync('git', ['show', commit + ':' + source], { cwd: root, maxBuffer: 3e6 }));
const before = raw.toString(), eol = before.includes('\r\n') ? '\r\n' : '\n';
const startMarker = '      with prior_states as (', endMarker = '      ), prior_lines as (';
assert.equal(before.split(startMarker).length, 3);
const start = before.indexOf(startMarker), end = before.indexOf(endMarker, start);
assert.ok(end > start);
const original = before.slice(start, end);
const selectPrefix = '        select distinct on (prior_transition.work_event_id)';
assert.ok(original.startsWith(startMarker + eol + selectPrefix));
let query = original.slice((startMarker + eol).length);
for (const [old, replacement] of [
  ['v_client_id', 'p_client_id'], ['v_source_profile_kind', 'p_source_profile_kind'],
  ['v_group.id', 'p_source_group_id'], ['v_cycle.finalisation_week_ending', 'p_before_week_ending']
]) query = query.replaceAll(old, replacement);
const oldSelect = '                 as new_snapshot_line_id';
assert.equal(query.split(oldSelect).length, 2);
query = query.replace(oldSelect, '                 as snapshot_line_id,' + eol + '               prior_transition.id as transition_id,prior_transition.final_revision_id');
const signature = 'private.weekly_source_roster_prior_states_v1(uuid,uuid,text,date)';
const helper = [
  '-- One extraction of the existing FIN4 prior-state selection. No new money rule.',
  'create or replace function private.weekly_source_roster_prior_states_v1(',
  '  p_source_group_id uuid,p_client_id uuid,p_source_profile_kind text,p_before_week_ending date',
  ') returns table(work_event_id uuid,snapshot_line_id uuid,transition_id uuid,final_revision_id uuid)',
  'language sql stable security definer',
  "set search_path to 'pg_catalog','pg_temp'",
  'as $function$', query, ';', '$function$;',
  'alter function ' + signature + ' owner to postgres;',
  'revoke all on function ' + signature + ' from public,anon,authenticated,service_role;', ''
].join(eol);
const replacement = [startMarker,
  '        select prior.work_event_id,prior.snapshot_line_id as new_snapshot_line_id',
  '        from private.weekly_source_roster_prior_states_v1(',
  '          v_group.id,v_client_id,v_source_profile_kind,v_cycle.finalisation_week_ending) prior'
].join(eol);
assert.equal(before.split(original).length, 3);
const hunks = ['v_missing_row', 'v_transition'].map(variable => ({
  name: 'same extracted prior-state owner for actual ' + variable + ' loop',
  before: '    for ' + variable + ' in' + eol + original,
  after: '    for ' + variable + ' in' + eol + replacement
}));
let after = before;
for (const h of hunks) { assert.equal(after.split(h.before).length, 2); after = after.replace(h.before, h.after); }
let reverse = after;
for (const h of [...hunks].reverse()) { assert.equal(reverse.split(h.after).length, 2); reverse = reverse.replace(h.after, h.before); }
assert.equal(reverse, before);
fs.writeFileSync(path.join(dir, 'forward/00_roster_prior_states.sqlinc'), helper);
fs.writeFileSync(path.join(dir, 'before/01_complete_fin4_prior_selector.sqlinc'), raw);
fs.writeFileSync(path.join(dir, 'forward/01_complete_fin4_prior_selector.sqlinc'), after);
const pins = [];
for (const file of [
  'supabase/repeatable/17092026_0400_weekly_source_freeze_census_v1.sql',
  'supabase/repeatable/17092026_1000_weekly_source_settlement_allocation_v1.sql',
  'supabase/repeatable/04092026_1440_banking_pay_draft_snapshot_pagination_v8.sql',
  'supabase/repeatable/26092026_0201_banking_pay_stage2_financial_readers_v1.sql',
  'supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql'
]) {
  const b = execFileSync('git', ['show', 'fa66183f01913e86651e82816f5e88f97e07123c:' + file], { cwd: root, maxBuffer: 6e6 });
  const saved = 'input/' + path.basename(file);
  fs.writeFileSync(path.join(dir, saved), b);
  pins.push({ commit: 'fa66183f01913e86651e82816f5e88f97e07123c', file, saved, bytes: b.length, sha256: sha(b) });
}
fs.writeFileSync(path.join(dir, 'CURRENT_SELECTOR_EXTRACTION_PROOF.json'), JSON.stringify({ source_only: true, unfinished: true, containing_checkpoint: commit, source, before_bytes: raw.length, before_sha256: sha(raw), after_bytes: Buffer.byteLength(after), after_sha256: sha(after), helper_sha256: sha(helper), signature, exact_original_query: original, original_identical_query_occurrences: 2, parameter_substitutions: ['v_client_id=p_client_id', 'v_source_profile_kind=p_source_profile_kind', 'v_group.id=p_source_group_id', 'v_cycle.finalisation_week_ending=p_before_week_ending'], extra_returned_immutable_identity_fields_only: ['transition_id', 'final_revision_id'], hunks, whole_reverse_verified: true, native_input_pins: pins, native_economic_prior_cutoff_order_and_excluded_CANCEL_unchanged: true, Legacy_paid_certificate_complete: false, Legacy_Same_Separate_admission_complete: false, SQL_compiled: false, canonical_mounted: false, native_proof: false }, null, 2) + '\n');
console.log(JSON.stringify({ whole_fin4_reverse: true, unique_hunks: hunks.length, helper_sha256: sha(helper), fin4_sha256: sha(after), native_inputs: pins.length, unfinished_Legacy_consumer: true }));

const coupled = [];
function compose(source, output, edits) {
  const b = fs.readFileSync(path.join(root, source));
  assert.deepEqual(b, execFileSync('git', ['show', commit + ':' + source], { cwd: root, maxBuffer: 3e6 }));
  const before = b.toString(), eol = before.includes('\r\n') ? '\r\n' : '\n';
  const hunks = edits.map(h => ({ ...h, before: h.before.replaceAll('\n', eol), after: h.after.replaceAll('\n', eol) }));
  let result = before;
  for (const h of hunks) { assert.equal(result.split(h.before).length, 2, h.name); result = result.replace(h.before, () => h.after); }
  let reverse = result;
  for (const h of [...hunks].reverse()) { assert.equal(reverse.split(h.after).length, 2, h.name); reverse = reverse.replace(h.after, () => h.before); }
  assert.equal(reverse, before);
  fs.writeFileSync(path.join(dir, 'before', output), b);
  fs.writeFileSync(path.join(dir, 'forward', output), result);
  coupled.push({ source, containing_commit: commit, file: output, before_bytes: b.length, before_sha256: sha(b), after_bytes: Buffer.byteLength(result), after_sha256: sha(result), hunks, whole_reverse_verified: true });
}
const legacyBefore = `      v_settlement:=private.weekly_source_settlement_allocation_v1(v_root);
      v_paid_basis:=v_paid_basis||jsonb_build_array(v_settlement);
      if v_settlement->>'state'='AVAILABLE' then
        for v_shift in select value from jsonb_array_elements(v_settlement->'shifts') loop
          if v_shift->>'segment_id'='weekly-source-event:'||v_event.id::text and (v_shift->>'hours')::numeric>0 then
            v_paid:=true; v_paid_state:='SETTLED_SOURCE_SEGMENT'; end if;
        end loop;
      elsif v_settlement->>'state'<>'NO_SETTLEMENT' then v_paid:=null; v_paid_state:='UNKNOWN'; v_unknown:=true; end if;`;
const legacyAfter = `      if p_actor_user_id is null then
        v_paid:=null;v_paid_state:='UNKNOWN';v_unknown:=true;
      else
        v_legacy:=private.weekly_source_legacy_signed_source_membership_v1(v_root,v_event.id,p_actor_user_id,v_cycle.id);
        v_settlement:=v_legacy->'native_allocation';
        v_paid_basis:=v_paid_basis||jsonb_build_array(v_legacy);
        if v_legacy->>'state'='MEMBERSHIP_PRESENT' and jsonb_array_length(v_legacy->'signed_native_memberships')=1 then
          v_legacy_member:=v_legacy#>'{signed_native_memberships,0}';
          if jsonb_array_length(v_legacy_member->'native_current_financial_components')=1
            and v_legacy_member#>>'{native_component,key_type}'='TS_DAY'
            and v_legacy_member#>>'{native_component,key_value}'=v_event.work_date::text
            and v_legacy_member#>>'{native_current_financial_components,0,timesheet_id}'=v_root::text
            and v_legacy_member#>>'{native_current_financial_components,0,key_type}'='TS_DAY'
            and v_legacy_member#>>'{native_current_financial_components,0,key_value}'=v_event.work_date::text
            and coalesce(v_legacy_member#>>'{native_component,target_amount_ex_vat}','') ~ '^[0-9]+(\\.[0-9]+)?$'
            and coalesce(v_legacy_member#>>'{native_current_financial_components,0,amount_ex_vat}','') ~ '^[0-9]+(\\.[0-9]+)?$'
            and coalesce(v_legacy_member#>>'{signed_member,pay_amount}','') ~ '^[0-9]+(\\.[0-9]+)?$'
            and (v_legacy_member#>>'{native_component,target_amount_ex_vat}')::numeric>0
            and (v_legacy_member#>>'{signed_member,pay_amount}')::numeric>0
            and (v_legacy_member#>>'{native_current_financial_components,0,amount_ex_vat}')::numeric
              =(v_legacy_member#>>'{native_component,target_amount_ex_vat}')::numeric then
            -- The native full day component is settled, its signed complete
            -- Source membership contains this event, and the existing native
            -- current financial position equals that full target. No cash is
            -- divided among shifts and no new amount is calculated.
            v_paid:=true;v_paid_state:='SETTLED_LEGACY_SOURCE_COMPONENT';
          else v_paid:=null;v_paid_state:='UNKNOWN';v_unknown:=true;end if;
        elsif v_settlement->>'state'='NO_SETTLEMENT'
          and v_legacy#>>'{native_module,active_owner}'='LEGACY'
          and v_legacy#>>'{native_terminal_census,result}'='RELEASABLE'
          and v_legacy#>'{native_terminal_census,class_counts,SETTLED_TERMINAL}'='0'::jsonb then
          -- Retain the existing native no-settlement disposition only when
          -- the complete terminal census also has no settled item.
          null;
        else v_paid:=null;v_paid_state:='UNKNOWN';v_unknown:=true;end if;
      end if;`;
compose('docs/healthroster-build/proposals/paid-shift-overlap-native-facts-construction-20261014/forward/01_work_event_payment_facts.sqlinc', '04_complete_payment_facts.sqlinc', [
  { name: 'private native Legacy member proof variables', before: '  v_matched_component_count integer;v_settlement jsonb;v_shift jsonb;', after: '  v_matched_component_count integer;v_settlement jsonb;v_shift jsonb;v_legacy jsonb;v_legacy_member jsonb;' },
  { name: 'single paid fact owner delegates complete native terminal signed-membership current-financial proof', before: legacyBefore, after: legacyAfter }
]);
compose('docs/healthroster-build/proposals/paid-match-retained-current-capture-construction-20261014/forward/02_complete_overlap_inputs.sqlinc', '05_complete_overlap_inputs.sqlinc', [
  { name: 'actual cycle selects existing NEXT or one extracted native Source Legacy prior owner', before: 'v_current_snapshot:=private.weekly_source_paid_overlap_current_snapshot_v1(v_event.id,v_movement.id,\n      p_candidate_id,p_client_id,v_movement.contract_id,v_cycle.source_group_id,\n      array(select value::uuid from jsonb_array_elements_text(v_payment->\'root_timesheet_ids\') value));', after: 'v_current_snapshot:=private.weekly_source_paid_overlap_current_snapshot_for_cycle_v1(v_event.id,v_movement.id,\n      p_candidate_id,p_client_id,v_movement.contract_id,v_cycle.source_group_id,\n      array(select value::uuid from jsonb_array_elements_text(v_payment->\'root_timesheet_ids\') value),v_cycle.id);' }
]);
for (const [input, output, expression] of [
  ['03_complete_choice_admit.sqlinc', '06_complete_choice_admit.sqlinc', "v_native_paid->>'paid_evidence_state'"],
  ['04_complete_choice_current.sqlinc', '07_complete_choice_current.sqlinc', "v_match#>>'{native_payment_facts,paid_evidence_state}'"],
  ['05_complete_office_question.sqlinc', '08_complete_office_question.sqlinc', "v_item#>>'{native_payment_facts,paid_evidence_state}'"],
  ['01_retained_paid_transition_assert.sqlinc', '09_complete_retained_transition_assert.sqlinc', "item#>>'{native_payment_facts,paid_evidence_state}'"]
]) compose('docs/healthroster-build/proposals/paid-match-retained-current-capture-construction-20261014/forward/' + input, output, [{
  name: 'existing actual paid fact owner may supply its complete native Legacy Source component state',
  before: expression + " is distinct from 'SETTLED_ORIGINAL'",
  after: '(' + expression + ") is null or (" + expression + ") not in ('SETTLED_ORIGINAL','SETTLED_LEGACY_SOURCE_COMPONENT')"
}]);
fs.writeFileSync(path.join(dir, 'COUPLED_LEGACY_CONSUMER_REVERSES.json'), JSON.stringify({ source_only: true, unfinished: true, records: coupled, single_existing_payment_fact_owner_retained: true, actual_Source_current_and_terminal_membership_delegated: true, complete_all_Legacy_grains: false, CorrectFinal_NHSP_protected_admission_complete: false, SQL_compiled: false, canonical_mounted: false, native_proof: false }, null, 2) + '\n');
console.log(JSON.stringify({ coupled_whole_owners: coupled.length, unique_hunks: coupled.reduce((n, r) => n + r.hunks.length, 0), records: coupled.map(r => ({ file: r.file, sha256: r.after_sha256 })), unfinished_Legacy_native_proof: true }));
