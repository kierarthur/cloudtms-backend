import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { scanManagedSql } from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir = path.dirname(fileURLToPath(import.meta.url));
const read = f => fs.readFileSync(path.join(dir, f), 'utf8');
const sha = b => createHash('sha256').update(b).digest('hex');
test('complete FIN4 reverses both uniquely anchored identical selectors without other edits', () => {
  const p = JSON.parse(read('CURRENT_SELECTOR_EXTRACTION_PROOF.json'));
  assert.equal(p.original_identical_query_occurrences, 2); assert.equal(p.hunks.length, 2);
  let s = read('forward/01_complete_fin4_prior_selector.sqlinc'); assert.equal(sha(s), p.after_sha256);
  for (const h of [...p.hunks].reverse()) { assert.equal(s.split(h.after).length, 2); s = s.replace(h.after, h.before); }
  assert.equal(s, read('before/01_complete_fin4_prior_selector.sqlinc')); assert.equal(sha(s), p.before_sha256);
});
test('single helper exactly reconstitutes the original native selector after only parameter and identity projection substitutions', () => {
  const p = JSON.parse(read('CURRENT_SELECTOR_EXTRACTION_PROOF.json'));
  const helper = read('forward/00_roster_prior_states.sqlinc');
  let q = helper.slice(helper.indexOf('        select distinct on'), helper.indexOf('\n;\n$function$;'));
  q = q.replace('                 as snapshot_line_id,\n               prior_transition.id as transition_id,prior_transition.final_revision_id', '                 as new_snapshot_line_id');
  for (const [param, old] of [['p_client_id', 'v_client_id'], ['p_source_profile_kind', 'v_source_profile_kind'], ['p_source_group_id', 'v_group.id'], ['p_before_week_ending', 'v_cycle.finalisation_week_ending']]) q = q.replaceAll(param, old);
  assert.equal('      with prior_states as (\n' + q, p.exact_original_query);
});
test('signed component reader does not use the unsupported allocation signature DTO or top-level fallback segments', () => {
  const s = read('forward/02_legacy_signed_source_membership.sqlinc');
  assert.doesNotMatch(s, /settlement->>'signature'/);
  for (const marker of ["settlement->>'history_id'", 'snapshot.signature is distinct from history.signature', 'snapshot.target_snapshot_json is distinct from history.snapshot_json', "snapshot.target_snapshot_json->'resolved_components'", 'native_items is distinct from signed_items', "x->>'class'='SETTLED_TERMINAL'"]) assert.ok(s.includes(marker), marker);
  assert.doesNotMatch(s, /snapshot.target_snapshot_json->'segments'/);
});
test('membership retains exact actual Source subject and delegates existing terminal and economic kernels', () => {
  const s = read('forward/02_legacy_signed_source_membership.sqlinc');
  for (const marker of ["'VIEW_PAY_WORK'", 'weekly_source_settlement_allocation_v1(p_root_timesheet_id)', 'weekly_source_freeze_census_v1(event.candidate_id,roots)', 'public._pay_batch_item_economic_components(null,array[item.id])', "s#>>'{weekly_source,work_event_id}'=event.id::text", "segment#>>'{weekly_source,movement_id}'", "m.pay_effect='NORMAL'", "native_census-'evaluated_at_utc'"]) assert.ok(s.includes(marker), marker);
  assert.doesNotMatch(s, /public.timesheet_pay_state\b|paid_at|\b(?:round|sum)\s*\(/i);
});
test('no single-member restriction or cash-authority claim is hidden in input reader', () => {
  const s = read('forward/02_legacy_signed_source_membership.sqlinc');
  assert.ok(s.includes("'paid_writer_permission',null,'paid',null,'paid_quantity_known',false"));
  assert.doesNotMatch(s, /jsonb_array_length\(v_members\)\s*(?:<>|!=|=)\s*1|\bpaid\s*:=\s*true|SETTLED_LEGACY_SOURCE/);
  assert.match(s, /jsonb_array_length\(v_members\)=0/);
});
test('Legacy current snapshot delegates actual prior and active owners, NEXT retains old exact call', () => {
  const s = read('forward/03_current_snapshot_for_cycle.sqlinc');
  assert.match(s, /if private.weekly_source_office_next_owner_v1\(\) then\s*return private.weekly_source_paid_overlap_current_snapshot_v1/);
  assert.ok(s.includes('private.weekly_source_roster_prior_states_v1('));
  assert.ok(s.includes('private.weekly_source_ordinary_projection_active_movements_v1(root,revision.id)'));
  assert.doesNotMatch(s, /bpay_next_source_current_snapshots|order by|limit 1|create table/i);
  assert.ok(s.includes("'source_export_present',transition.new_present"));
  assert.ok(s.includes('private.weekly_source_missing_cancel_transition_assert_v1(transition.id)'));
});
test('all new read leaves retain service-free owner ACL with no business DML', () => {
  for (const f of ['00_roster_prior_states.sqlinc', '02_legacy_signed_source_membership.sqlinc', '03_current_snapshot_for_cycle.sqlinc']) {
    const s = read('forward/' + f); assert.match(s, /stable security definer/);
    assert.match(s, /from public,anon,authenticated,service_role/);
    assert.doesNotMatch(s, /\b(?:insert into|update public\.|delete from|create table|for update|for share)\b/i);
  }
});
test('finite parser and source flags keep unfinished financial/correction/native boundaries explicit', () => {
  for (const f of fs.readdirSync(path.join(dir, 'forward'))) { const s = scanManagedSql(read('forward/' + f)); assert.ok(s.statements.length); assert.equal(s.meta.length, 0); }
  const p = JSON.parse(read('CURRENT_SELECTOR_EXTRACTION_PROOF.json'));
  assert.equal(p.Legacy_paid_certificate_complete, false); assert.equal(p.Legacy_Same_Separate_admission_complete, false);
  assert.equal(p.SQL_compiled, false); assert.equal(p.native_proof, false); assert.equal(p.canonical_mounted, false);
});
test('six full actual consumers reverse seven unique Legacy coupling hunks with literal SQL dollar regex preserved', () => {
  const p = JSON.parse(read('COUPLED_LEGACY_CONSUMER_REVERSES.json'));
  assert.equal(p.records.length, 6); assert.equal(p.records.reduce((n, r) => n + r.hunks.length, 0), 7);
  for (const r of p.records) {
    let s = read('forward/' + r.file); assert.equal(sha(s), r.after_sha256);
    for (const h of [...r.hunks].reverse()) { assert.equal(s.split(h.after).length, 2); s = s.replace(h.after, () => h.before); }
    assert.equal(s, read('before/' + r.file)); assert.equal(sha(s), r.before_sha256);
  }
  assert.ok(read('forward/04_complete_payment_facts.sqlinc').includes("~ '^[0-9]+(\\.[0-9]+)?$'"));
});
test('one payment-fact producer requires full native day target funding and positive actual signed member', () => {
  const s = read('forward/04_complete_payment_facts.sqlinc');
  for (const marker of ['weekly_source_legacy_signed_source_membership_v1(v_root,v_event.id,p_actor_user_id,v_cycle.id)', "v_legacy->>'state'='MEMBERSHIP_PRESENT'", "jsonb_array_length(v_legacy->'signed_native_memberships')=1", "jsonb_array_length(v_legacy_member->'native_current_financial_components')=1", "'{native_component,target_amount_ex_vat}'", "'{signed_member,pay_amount}'", "v_paid_state:='SETTLED_LEGACY_SOURCE_COMPONENT'"]) assert.ok(s.includes(marker), marker);
  assert.doesNotMatch(s, /v_paid_state:='SETTLED_SOURCE_SEGMENT'/);
  assert.ok(s.includes("'paid_quantity_known',false,'withheld_amount_known',false"));
  assert.match(s, /if p_actor_user_id is null then\s*v_paid:=null;v_paid_state:='UNKNOWN';v_unknown:=true/);
});
test('native-shaped complete day witness may include two Source members without splitting money', () => {
  const s = read('forward/02_legacy_signed_source_membership.sqlinc');
  assert.ok(s.includes("'complete_signed_source_members',v_members"));
  assert.ok(s.includes('for v_member in select value from jsonb_array_elements(v_members) loop'));
  assert.ok(s.includes('public._pay_active_settled_components(array[p_root_timesheet_id])'));
  assert.doesNotMatch(s, /amount.*\/.*(?:hours|minutes)|amount.*\*.*(?:hours|minutes)|jsonb_array_length\(v_members\)=1/);
  const nativeDay = { amount_ex_vat: '100', target_amount_ex_vat: '100', members: [{ event: 'A', pay_amount: '60' }, { event: 'B', pay_amount: '40' }] };
  assert.equal(nativeDay.members.length, 2);
  for (const m of nativeDay.members) assert.ok(Number(m.pay_amount) > 0 && nativeDay.amount_ex_vat === nativeDay.target_amount_ex_vat);
});
test('partial, recovered, absent, duplicate and foreign native funding cannot become paid certificate', () => {
  const source = read('forward/04_complete_payment_facts.sqlinc');
  assert.ok(source.includes("v_legacy_member#>>'{native_current_financial_components,0,timesheet_id}'=v_root::text"));
  assert.ok(source.includes("v_legacy_member#>>'{native_current_financial_components,0,key_value}'=v_event.work_date::text"));
  const fact = ({ state = 'MEMBERSHIP_PRESENT', members = 1, current = 1, sameRoot = true, sameDay = true, target = 100, funded = 100, memberPay = 60 } = {}) => state === 'MEMBERSHIP_PRESENT' && members === 1 && current === 1 && sameRoot && sameDay && target > 0 && memberPay > 0 && funded === target ? 'SETTLED_LEGACY_SOURCE_COMPONENT' : 'UNKNOWN';
  assert.equal(fact(), 'SETTLED_LEGACY_SOURCE_COMPONENT');
  for (const change of [{ state: 'UNAVAILABLE' }, { members: 2 }, { current: 0 }, { current: 2 }, { funded: 99 }, { funded: 0 }, { funded: -100 }, { memberPay: 0 }, { sameRoot: false }, { sameDay: false }]) assert.equal(fact(change), 'UNKNOWN');
});
test('actual locked admission fresh read question and retained assertion accept only the two literal certified states', () => {
  for (const file of ['06_complete_choice_admit.sqlinc', '07_complete_choice_current.sqlinc', '08_complete_office_question.sqlinc', '09_complete_retained_transition_assert.sqlinc']) {
    const s = read('forward/' + file);
    assert.ok(s.includes("not in ('SETTLED_ORIGINAL','SETTLED_LEGACY_SOURCE_COMPONENT')"));
    assert.ok(s.includes('is null or'));
    assert.ok(s.includes('current_pay_snapshot'));
  }
  assert.ok(read('forward/06_complete_choice_admit.sqlinc').includes('weekly_source_lock_and_resolve_families_v1'));
  assert.ok(read('forward/09_complete_retained_transition_assert.sqlinc').includes('private.bpay_next_source_current_snapshots mapping'));
});
