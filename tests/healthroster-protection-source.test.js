import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';

const root = resolve(import.meta.dirname, '..');
const dir = 'supabase/repeatable/13102026_1438_weekly_source_protection_queries';
const read = (path) => readFileSync(resolve(root, path), 'utf8').replaceAll('\r\n', '\n');
const page = (file) => read(`${dir}/${file}`);
function fn(source, name) {
  const start = source.indexOf(`create or replace function ${name}(`);
  assert.ok(start >= 0, name);
  const end = source.indexOf('$function$;', start);
  assert.ok(end >= start, name);
  return source.slice(start, end + '$function$;'.length);
}
const mutatingTables = (source) => [...source.matchAll(/\b(?:insert into|update|delete from)\s+(public|private)\.([a-z0-9_]+)/gi)]
  .map((match) => `${match[1]}.${match[2]}`).sort();
const calls = (source) => [...source.matchAll(/\b(private|public)\.([a-z0-9_]+)\s*\(/gi)]
  .map((match) => `${match[1]}.${match[2]}`).filter((name) => !new Set([
    'private.weekly_source_protected_family_lock_v1','private.weekly_source_protected_group_lock_v1',
    'private.weekly_source_bulk_stage_proof_v1','private.weekly_source_bulk_stage_replay_assert_v1',
    'private.weekly_source_automatic_reconcile_stage_proof_v1','private.weekly_source_automatic_reconcile_stage_replay_assert_v1',
    'private.weekly_source_automatic_reconcile_certificate_v1',
  ]).has(name)).sort();

test('accepted WAIT receipt qualification is the unchanged native proof block', () => {
  const native = read('supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc');
  const start = native.indexOf('      -- Choose the actual approval');
  const end = native.indexOf('\n    end if;\n    if v_row.issue_family', start);
  assert.ok(start >= 0 && end > start);
  assert.ok(page('01_wait_receipt.sqlinc').includes(native.slice(start, end)));
});

test('shared covered-hours reader remains byte-identical', () => {
  const name = 'private.weekly_source_covered_hours_incident_v1';
  assert.equal(fn(page('03_qualified_coverage.sqlinc'), name),
    fn(read('supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc'), name));
});

test('V3 facts hash shape and gate serialization are preserved', () => {
  const source = page('03_qualified_coverage.sqlinc');
  const native = read('supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc');
  assert.equal(source.slice(source.indexOf('    v_hours:=v_hours||'), source.indexOf('\nend;\n$function$;')),
    native.slice(native.indexOf('    v_hours:=v_hours||'), native.indexOf('\nend;\n$function$;')));
  assert.match(source, /issue_family in \('HEALTHROSTER_NOT_FINALISED','SOURCE_MISSING_OR_NOT_AUTHORISED'\)[\s\S]+weekly_source_is_import_authoritative_v1/);
  assert.match(source, /issue_family is distinct from 'CANDIDATE_TIMESHEET_MISSING'/);
  assert.doesNotMatch(source, /PAY_QUERY_STATE_V[4-9]/);
});

test('contact retirement has one shared native block and preserves protection guards', () => {
  const old = read('supabase/repeatable/05102026_0834_weekly_source_protected_contact_retirement_v1.sql');
  const start = old.indexOf('    update public.weekly_candidate_outreach_memberships membership');
  const end = old.indexOf('    v_count:=v_count+1;', start);
  assert.ok(page('04_contact_retirement.sqlinc').includes(old.slice(start, end)
    .replaceAll('v_incident.id', 'p_incident_id').replaceAll('v_incident.episode_number', 'v_episode')));
  const protectedOwner = fn(page('04_contact_retirement.sqlinc'), 'private.weekly_source_protected_contact_retire_v1');
  assert.match(protectedOwner, /weekly_source_covered_hours_incident_v1\(v_incident.id\)/);
  assert.match(protectedOwner, /weekly_source_incident_contact_retire_v1\(v_incident.id\)/);
  assert.doesNotMatch(protectedOwner, /update public.weekly_candidate_outreach_memberships/);
});

test('event witness rejects pending/ambiguous claims and uses exact current published membership', () => {
  const source = page('02_wait_witness.sqlinc');
  assert.match(source, /receipt_state}' not in \('COMPLETE','PUBLISHED'\)/);
  assert.match(source, /current_generation_id and family_id=v_family.id/);
  assert.match(source, /lifecycle_state is distinct from 'PUBLISHED'/);
  assert.match(source, /accepted_receipt,receipt_state}' is null/);
  assert.match(source, /generation_number is distinct from v_family.current_generation_number/);
  assert.match(source, /WEEKLY_PROTECTED_NEXT_TARGET_V1/);
  assert.match(source, /WEEKLY_PROTECTED_TARGET_VECTOR_V1/);
  assert.match(source, /work_event_id'=v_work.id::text/);
  assert.doesNotMatch(source, /c1_publication_state.*LIVE|paid[ _]?flag|limit 1/i);
  assert.match(source, /v_count<>1[\s\S]+WITNESS_UNAVAILABLE/);
});

test('automatic close uses exact report guard and distinct four facts without mail writes', () => {
  const source = page('05_unprotected_close.sqlinc');
  assert.match(source, /source_cycle_id=v_cycle.id and self_bill_report_id is not null/);
  assert.match(source, /weekly_source_current_publication_guard_v1\(v_cycle.id,'SELF_BILL_REPORT_SCOPE'/);
  assert.match(source, /s25_pay_selected'[\s\S]+on_accepted_report'/);
  assert.match(source, /set state='CANCELLED',resolved_at_utc=null,resolution_kind=null/);
  assert.match(source, /weekly_source_incident_contact_retire_v1/);
  assert.doesNotMatch(source, /insert into.*(?:mail_outbox|hr_issue_emails|weekly_message_intents)/);
});

test('an unaccepted reconcile event cannot erase WAIT from the published complete vector', () => {
  const source = page('02_wait_witness.sqlinc');
  const fence = source.slice(source.indexOf('if v_count=0 then'), source.indexOf('if v_count<>1 then'));
  assert.match(fence, /generation.lifecycle_state='PUBLISHED'/);
  assert.match(fence, /protected_target_state'='WAIT'/);
  assert.match(fence, /item->'protected'='true'::jsonb/);
  assert.ok(fence.indexOf('WITNESS_UNAVAILABLE') < fence.indexOf('return null'));
});

test('automatic reconciliation requires an approved complete inventory and exact final clocks without inferring money', () => {
  const source = page('27_automatic_reconcile.sqlinc');
  const selection = fn(source, 'private.weekly_source_automatic_reconcile_selection_v1');
  assert.match(selection, /private\.weekly_source_protected_wait_witness_v1/);
  assert.match(selection, /private\.weekly_source_pay_actor_allowed_v1\(v_actor\)/);
  assert.match(selection, /approval_basis,coverage_complete}' is distinct from 'true'/);
  assert.match(selection, /source_minutes}'\)::integer/);
  assert.match(selection, /'AUTOMATIC_ACTOR_UNAVAILABLE'/);
  assert.doesNotMatch(selection, /delta_pay|total_pay|from public.timesheets_financials/);
  assert.match(source, /weekly_source_automatic_reconcile_producer_ready_v1\(uuid\)/);
  assert.match(source, /v_ready is distinct from true/);
});

test('automatic stage seals proof before its vector hash and delta refusal occurs inside the same stage transaction', () => {
  const source = page('20_stage_request.sqlinc');
  const seal = source.indexOf("'automatic_reconcile_proof',v_automatic_reconcile_proof");
  assert.ok(seal > 0 && seal < source.indexOf('v_next_vector_hash:='));
  const assertion = source.lastIndexOf('perform private.weekly_source_automatic_reconcile_certificate_v1');
  assert.ok(assertion > source.indexOf('insert into public.weekly_exceptional_c1_component_records'));
  assert.ok(assertion < source.lastIndexOf("'ok',true,'outcome','STAGED','idempotent_replay',false"));
  assert.doesNotMatch(source.slice(assertion), /exception when others/);
  const complete = fn(page('27_automatic_reconcile.sqlinc'), 'public.weekly_source_automatic_reconcile_atomic_v1');
  assert.match(complete, /outcome' is distinct from 'PUBLISHED'/);
  assert.ok(complete.indexOf('weekly_source_candidate_serial_gate_v1') < complete.indexOf('weekly_source_lock_family_rows_v1'));
  assert.ok(complete.indexOf('weekly_source_lock_family_rows_v1') < complete.indexOf('id=v_source_group and active for update'));
});

test('staged optional context cannot be added, removed or changed on immutable replay', () => {
  const source = page('20_stage_request.sqlinc');
  assert.equal([...source.matchAll(/perform private\.weekly_source_bulk_stage_replay_assert_v1/g)].length, 2);
  assert.equal([...source.matchAll(/perform private\.weekly_source_automatic_reconcile_stage_replay_assert_v1/g)].length, 2);
  assert.match(page('25_bulk_stage_proof.sqlinc'), /not \(p_stage_request \? 'bulk_signed_selection_proof'\) or not \(v_vector \? 'bulk_signed_selection_proof'\)/);
});

test('new schema extends existing stores only and never assigns an automatic actor', () => {
  const source = read('supabase/migrations/13102026_1456_weekly_source_protection_query_support_v1.sql');
  assert.doesNotMatch(source, /create table|insert into|update public/i);
  assert.match(source, /automatic_reconcile_actor_user_id uuid references public.tms_users/);
  assert.match(source, /not_finalised_comparison_revision_id uuid/);
  assert.match(page('06_automatic_actor_settings.sqlinc'), /then v_automatic_actor else automatic_reconcile_actor_user_id end/);
});

test('isolated comparison failures reuse authoritative native audit with bounded errors', () => {
  const source = page('08_comparison_audit.sqlinc');
  assert.match(source, /public\._audit_insert/);
  assert.match(source, /audit.event_sequence_is_authoritative/);
  assert.match(source, /for update/);
  assert.match(source, /timesheet_revision[\s\S]+timesheet_hash/);
  assert.doesNotMatch(source, /create table|weekly_source_publication_comparison_receipts/);
  assert.match(page('10_publication_recheck.sqlinc'), /substring\(sqlerrm from '\^\[A-Z\]\[A-Z0-9_\]\{0,119\}\$'\)/);
  assert.match(page('10_publication_recheck.sqlinc'), /'NOT_COMPARED',v_refusal/);
});

test('unsigned materialiser keeps Candidate evidence absent and expected-week identity explicit', () => {
  const source = page('09_expected_week_materialiser.sqlinc');
  assert.match(source, /expected.status in \('PLANNED','OPEN','SUBMITTED'\)/);
  assert.match(source, /candidate_timesheet_id',null,'candidate_timesheet_revision',null,'candidate_shift_fingerprint',null/);
  assert.match(source, /'contract_week_id',v_week.id/);
  assert.match(source, /v_issue_family:='SOURCE_HOURS_DIFFER'; v_presence:='PRESENT'/);
  assert.doesNotMatch(source, /insert into public.timesheets\b|signed.*:=|r2_.*:=/);
});

test('Monday deferral writes no future mail intent and exact question scope excludes other questions', () => {
  const source = page('14_not_finalised_dispatch.sqlinc');
  const start = source.indexOf('if v_publication.published_at_utc<v_monday');
  const end = source.indexOf('v_context:=', start);
  assert.ok(end > start);
  assert.doesNotMatch(source.slice(start, end), /weekly_source_query_manager_intent_v1|insert into public.weekly_message_intents/);
  assert.match(source, /v_week\+\(8-extract\(isodow from v_week\)::integer\)/);
  assert.match(source, /at time zone 'Europe\/London'/);
  assert.match(source, /not_finalised_comparison_revision_id/);
  assert.match(page('11_manager_due_scope.sqlinc'), /event.not_finalised_comparison_revision_id=comparison.id/);
  assert.match(source, /route_mode in \('DISABLED','MANUAL_ONLY'\)/);
});

const owners = [
  ['17_prepare_family.sqlinc', 'supabase/repeatable/15092026_1534_weekly_source_protected_pay_publisher_v1.sql', 'public.weekly_exceptional_pay_prepare_family_v1'],
  ['18_prepare_action.sqlinc', 'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'public.weekly_exceptional_pay_prepare_action_v1'],
  ['19_action_context.sqlinc', 'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'public.weekly_exceptional_pay_action_context_v1'],
  ['20_stage_request.sqlinc', 'supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql', 'public.weekly_exceptional_pay_stage_c1_request_v1'],
];
for (const [file, input, name] of owners) {
  test(`${name} preserves every native mutation target and function call`, () => {
    const changed = fn(page(file), name); const original = fn(read(input), name);
    assert.deepEqual(mutatingTables(changed), mutatingTables(original));
    assert.deepEqual(calls(changed), calls(original));
    assert.ok(changed.indexOf('weekly_source_protected_group_lock_v1') < changed.indexOf('select cycle.* into strict v_cycle'));
    assert.match(changed, /WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED/);
  });
}

test('fresh stage locks native I-1 before Source row locks; immutable replay remains read-only', () => {
  const source = page('20_stage_request.sqlinc');
  const first = source.indexOf('where request.id=v_request_id;');
  const family = source.indexOf('v_lock_family:=private.weekly_source_protected_family_lock_v1');
  const row = source.indexOf('where request.id=v_request_id\n  for update;');
  assert.ok(first >= 0 && first < family && family < row);
  assert.ok(source.indexOf('v_lock_group:=') < source.indexOf('where cycle.id=v_source_cycle_id\n  for share;'));
});

test('publication family discovery precedes outer locks and refuses any set growth', () => {
  const source = page('21_publication_family_fence.sqlinc');
  assert.match(source, /array_agg\(distinct sheet.timesheet_id order by sheet.timesheet_id\)/);
  assert.match(source, /p_before is distinct from v_proof/);
  assert.match(source, /SIGNED_ROOT_SET_CHANGED' using errcode='40001'/);
  assert.match(source, /weekly_source_lock_family_rows_v1\(v_roots,null\)/);
  assert.doesNotMatch(source, /candidate_app_schedule|limit 1/);
});

test('every checking path obeys the declared upload or cycle Client scope', () => {
  const scope = page('00_checking_client_scope.sqlinc');
  assert.match(scope, /scope_client_id is distinct from v_client/);
  assert.match(scope, /return coalesce\(v_cycle.scope_client_id,v_client\)/);
  const compare = page('07_submission_comparison.sqlinc');
  assert.match(compare, /v_scope_client_id is not null and v_scope_client_id is distinct from v_contract.client_id/);
  assert.ok(compare.indexOf('v_scope_client_id:=') < compare.indexOf('for v_candidate_row in'));
  for (const file of ['09_expected_week_materialiser.sqlinc', '10_publication_recheck.sqlinc',
    '14_not_finalised_dispatch.sqlinc', '21_publication_family_fence.sqlinc']) {
    assert.match(page(file), /weekly_source_checking_client_scope_v1/, file);
  }
  assert.doesNotMatch(scope, /limit 1|min\(|display_name|like/i);
});

test('query reader carries OPEN import questions without rewriting comparison or qualifying bare WAIT flags', () => {
  const source = page('22_query_groups.sqlinc');
  assert.match(source, /authority_scope_kind='CYCLE'[\s\S]+weekly_source_is_import_authoritative_v1\(incident.client_id,comparison.contract_id,work_event.work_date\)/);
  assert.match(source, /profile_code='NHSP_PREFINAL_RELEASED_V1'/);
  assert.match(source, /not private\.weekly_source_covered_hours_incident_v1\(incident.id\)/);
  assert.match(source, /bool_or\(incident_facts.candidate_action_state<>'NOT_REQUIRED'\)/);
  assert.match(source, /checking_client_id is null or incident.client_id=cycle_context.checking_client_id/);
  assert.match(source, /weekly_source_expected_week_question_v1\(incident_facts.incident_id\)/);
  assert.deepEqual(mutatingTables(source), []);
  assert.doesNotMatch(source, /TARGET_MANAGED|withheld_amount_pence.*0/);
  const name = 'private.weekly_source_office_query_groups_v1';
  const before = fn(read('supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql'), name);
  assert.equal(source.slice(source.indexOf(') returns table('), source.indexOf('language sql')),
    before.slice(before.indexOf(') returns table('), before.indexOf('language sql')));
});

test('bulk eligibility binds the native immutable MyTMS signature and exact comparison fingerprint', () => {
  const source = page('23_bulk_signed_selection.sqlinc');
  assert.match(source, /weekly_exceptional_candidate_signed_evidence_v1/);
  assert.match(source, /signature_kind' is distinct from 'CANDIDATE_WORKFLOW'/);
  assert.match(source, /_candidate_sha256_jsonb_v1\(v_workflow.immutable_submission_json\)/);
  assert.match(source, /v_fingerprint=v_comparison.candidate_shift_fingerprint/);
  assert.match(source, /v_matches<>1/);
  assert.match(source, /WEEKLY_PROTECTED_EVIDENCE_UNSIGNED'[\s\S]+WEEKLY_PROTECTED_EVIDENCE_INVALID'/);
  assert.doesNotMatch(source.replace(/^--.*$/gm, ''), /scheduled_start_iso|booked|rota\./i);
});

test('own-stage exception requires native sealed proof, real scoped readiness and fresh root CAS', () => {
  const source = page('25_bulk_stage_proof.sqlinc');
  assert.match(source, /weekly_source_bulk_producer_ready_v1\(uuid\)/);
  assert.match(source, /v_ready is distinct from true/);
  assert.match(source, /a.approved_target_pay_components_json is distinct from g.complete_next_vector_json->'target_snapshot'/);
  assert.match(source, /g.complete_next_vector_hash/);
  assert.match(source, /v_cas is distinct from v_proof->'source_cas'/);
  assert.match(source, /WEEKLY_SOURCE_BULK_ROUND_ALREADY_STAGED/);
  assert.doesNotMatch(source, /set_config|create table|return true/);
  const stage = page('20_stage_request.sqlinc');
  assert.match(stage, /if p_request \? 'bulk_signed_selection_proof' then[\s\S]+v_bulk_signed_proof:=private.weekly_source_bulk_stage_proof_v1/);
  assert.ok(stage.indexOf("'bulk_signed_selection_proof',v_bulk_signed_proof") < stage.indexOf('v_next_vector_hash:='));
});

test('one-shift native round takes Candidate gate then complete I-1 before group/cycle and preserves savepoint rollback', () => {
  const source = page('26_bulk_command.sqlinc');
  const gate = source.indexOf('v_gate:=private.weekly_source_candidate_serial_gate_v1');
  const family = source.indexOf('v_lock:=private.weekly_source_lock_family_rows_v1');
  const group = source.indexOf('where id=v_group and active for update;');
  const cycle = source.indexOf('where id=v_cycle_id for update;', group);
  const complete = source.indexOf('v_result:=public.weekly_exceptional_pay_complete_local_v1');
  assert.ok(gate >= 0 && gate < family && family < group && group < cycle && cycle < complete);
  assert.match(source, /jsonb_array_length\(p_request->'staged'\)<>1/);
  assert.match(source, /exception when others then[\s\S]+PROTECTION_REFUSED/);
  assert.match(source, /whole_command_atomic',false/);
  assert.match(source, /WEEKLY_SOURCE_BULK_ORIGINAL_CLAIM_CHANGED/);
  assert.doesNotMatch(source, /create table|insert into public.timesheets_financials/);
});

test('new manager due scope is immutable and captured against the actual generation membership and Monday', () => {
  const source = page('24_due_scope_capture.sqlinc');
  assert.match(source, /OLD.not_finalised_comparison_revision_id is distinct from NEW.not_finalised_comparison_revision_id/);
  assert.match(source, /membership.comparison_revision_id=comparison.id/);
  assert.match(source, /NEW.due_at_utc<v_clock/);
  assert.match(source, /NEW.not_finalised_comparison_revision_id is null then return NEW/);
});

test('support pages are included by one canonical root and are not independent SQL roots', () => {
  const source = read('supabase/repeatable/13102026_1438_weekly_source_protection_queries_v1.sql');
  for (const file of readdirSync(resolve(root, dir))) {
    assert.ok(file.endsWith('.sqlinc'), file);
    assert.ok(source.includes(`${dir.split('/').at(-1)}/${file}`), file);
  }
});

test('portable first-use verifier includes every actual declared routine and exact V3 marker', () => {
  const verifier = read('supabase/verification/13102026_1438_weekly_source_protection_queries_v1.sql');
  for (const file of readdirSync(resolve(root, dir))) {
    const text = page(file);
    for (const match of text.matchAll(/create or replace function\s+([a-z0-9_.]+)\s*\(([\s\S]*?)\)\s*returns\s+/gi)) {
      const args = match[2].trim() ? match[2].split(',').map((parameter) =>
        parameter.trim().replace(/\s+default\s+[\s\S]*$/i, '').split(/\s+/)[1]) : [];
      assert.ok(verifier.includes(`'${match[1]}(${args.join(',')})'`), `${file}: ${match[1]}`);
    }
  }
  assert.match(verifier, /position\('PAY_QUERY_STATE_V3' in definition\)/);
  assert.doesNotMatch(verifier, /WEEKLY_SOURCE_PAY_QUERY_STATE_V3/);
});
