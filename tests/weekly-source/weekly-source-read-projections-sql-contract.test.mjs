import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const readSqlPath = new URL('../../supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql', import.meta.url);
const aclSqlPath = new URL('../../supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql', import.meta.url);
const verifySqlPath = new URL('../../supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql', import.meta.url);

const lower = (value) => value.toLowerCase();

test('Office Weekly source read and bulk owner is service-only and owns every exact public contract', async () => {
  const [sql, acl, verification] = await Promise.all([
    readFile(readSqlPath, 'utf8'), readFile(aclSqlPath, 'utf8'), readFile(verifySqlPath, 'utf8'),
  ]);
  const source = lower(sql);
  const signatures = [
    'weekly_source_office_workspace_v1',
    'weekly_source_office_timesheet_presentation_v1',
    'weekly_source_office_bulk_query_action_atomic_v1',
    'weekly_source_no_shifts_attest_atomic_v1',
  ];
  for (const name of signatures) {
    assert.match(source, new RegExp(`create or replace function public\\.${name}\\s*\\(`));
    assert.match(source, new RegExp(`revoke all on function public\\.${name}\\(jsonb\\)[\\s\\S]*?from public,anon,authenticated,service_role`));
    assert.match(source, new RegExp(`grant execute on function public\\.${name}\\(jsonb\\) to service_role`));
    assert.match(lower(acl), new RegExp(`public\\.${name}\\(jsonb\\)`));
    assert.match(lower(verification), new RegExp(`public\\.${name}\\(jsonb\\)`));
  }
  assert.ok((source.match(/perform private\.weekly_source_query_require_service_v1\(\)/g) || []).length >= 4);
});

test('workspace projects complete unloaded selections, plain detail/reminder actions and bounded History', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /weekly_source_bulk_filter_selection_v1/);
  assert.match(source, /'mode','all_filtered'/);
  assert.match(source, /'excluded_group_keys','\[\]'::jsonb/);
  assert.match(source, /'filtered_group_count',v_query_count,'selection_complete',true/);
  assert.match(source, /'label','open'.*?'detail'/s);
  assert.match(source, /'label','view details'/);
  assert.match(source, /'candidate_hours'/);
  assert.match(source, /'system_hours'/);
  assert.match(source, /'actual_hours'/);
  assert.match(source, /'movement',case charge\.row_sign_kind/);
  assert.match(source, /'commission'.*source_row\.source_commission_pence/s);
  assert.match(source, /'total_cost'.*source_row\.source_total_cost_pence/s);
  assert.match(source, /'invoice_charge'.*source_row\.source_shift_charge_pence/s);
  assert.match(source, /'label','remind candidate'.*?'candidate_generation_id'.*?'projection_publication_id'/s);
  assert.match(source, /candidate_manual_reminder_available_at_utc|manual_reminder_available_at_utc/);
  assert.match(source, /current_pay_cycle.*last_4_pay_cycles.*last_13_pay_cycles/s);
  assert.match(source, /offset v_offset limit v_limit/);
});

test('every policy-visible Finalise column has a server-side two-state sort contract', async () => {
  const [source, verification] = (await Promise.all([
    readFile(readSqlPath, 'utf8'), readFile(verifySqlPath, 'utf8'),
  ])).map(lower);
  const finaliseKeys = [
    'candidate', 'day_date', 'client', 'system_hours', 'actual_hours', 'movement',
    'commission', 'total_cost', 'invoice_charge', 'status', 'problem', 'job_role',
    'contract', 'outcome',
  ];
  const allowlist = source.match(/v_tab='finalise' and v_sort_key not in \(([\s\S]*?)\)\)/)?.[1] || '';
  for (const key of finaliseKeys) {
    assert.match(allowlist, new RegExp(`'${key}'`), `Finalise sort key ${key} is not admitted`);
    assert.match(source, new RegExp(`v_sort_key(?:=| in \\()([\\s\\S]{0,80})'${key}'|v_sort_key='${key}'`),
      `Finalise sort key ${key} has no ordering expression`);
  }
  assert.match(verification, /'sort_key','actual_hours','sort_direction','desc'/);
  assert.match(verification, /nhsp finalise did not accept and apply the approved actual hours sort/);
});

test('guarded accept proves complete groups, permits a selected shift subset and delegates one atomic union', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /v_action not in \('ask_candidates','send_manager_now','accept_system_hours'\)/);
  assert.match(source, /group_selection_proofs/);
  assert.match(source, /weekly_source_accept_group_selection_v1/);
  assert.match(source, /query\.accept_incident_ids/);
  assert.match(source, /not \(v_group\.accept_incident_ids && v_incident_ids\)/);
  assert.match(source, /having pg_catalog\.count\(eligible\.group_key\)<>1/);
  assert.doesNotMatch(source, /v_incident_ids is distinct from v_expected_incident_ids/);
  assert.match(source, /weekly_source_query_accept_system_hours_atomic_v1/);
  assert.match(source, /where id=v_cycle_id for update/);
  assert.match(source, /weekly_source_bulk_workspace_stale/);
});

test('Candidate and manager bulk actions preserve exact cohorts and mixed account availability', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /candidate_app_global_membership_links/);
  assert.match(source, /count\(\*\)=1 into v_candidate_available/);
  assert.match(source, /unavailable_no_active_app_account/);
  assert.match(source, /begin[\s\S]*?weekly_source_timesheet_submission_request_start_atomic_v1[\s\S]*?weekly_source_query_ask_candidate_atomic_v1[\s\S]*?exception when sqlstate '55000'/);
  assert.match(source, /weekly_source_office_missing_scope_fingerprint_v1/);
  assert.match(source, /normalised_row_hash/);
  assert.match(source, /group by query\.manager_recipient_route_key/);
  assert.match(source, /weekly_source_query_send_manager_now_atomic_v1/);
});

test('presentation bypasses Daily and ordinary rows, blocks unavailable source authority and exposes no rate/remittance model', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /return pg_catalog\.jsonb_build_object\('applicable',false,'scope','daily'\)/);
  assert.match(source, /return pg_catalog\.jsonb_build_object\('applicable',false,'scope','weekly'\)/);
  assert.match(source, /raise exception 'source_check_in_progress'/);
  assert.match(source, /'comparison'.*?'source_rows'.*?'submitted_rows'/s);
  assert.doesNotMatch(source, /'hourly_rate'|'pay_rate'|'charge_rate'|'remittance'/);
});

test('no-shifts attestation is exact, contradictory-source safe and never touches protected finance owners', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  for (const key of ['actor_user_id', 'source_cycle_id', 'source_group_id', 'client_id', 'expected_cycle_version', 'attestation_text']) {
    assert.match(source, new RegExp(`'${key}'`));
  }
  assert.match(source, /no_shifts_to_import/);
  assert.match(source, /weekly_source_no_shifts_client_source_exists/);
  assert.match(source, /weekly_source_no_shifts_unresolved_source_rows/);
  assert.match(source, /weekly_source_no_shifts_open_issue_exists/);
  assert.match(source, /for update/);
  assert.match(source, /'idempotent',true/);
  assert.match(source, /select not exists\([\s\S]*?\) into v_all_complete/);
  assert.match(source, /if v_all_complete then[\s\S]*?set state='finalised'[\s\S]*?else[\s\S]*?set state='finalisable'/);
  assert.doesNotMatch(source, /(insert into|update|delete from) public\.(timesheets_financials|invoices|invoice_lines|pay_workbench|banking_pay)/);
});

test('finalisation follow-up is durable, plain and can only continue or explicitly check one approved-hours task', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /weekly_source_finalisation_pay_runs/);
  assert.match(source, /weekly_source_finalisation_pay_tasks/);
  assert.match(source, /'approved_hours_follow_up',v_finalise_pay_follow_up/);
  assert.match(source, /'label','continue approved hours update','command','finalise_week'/);
  assert.match(source, /'label','check approved hours update','command','recover_finalised_pay'/);
  assert.match(source, /'confirm_retry',false/);
  assert.match(source, /the source is finalised and invoicing is not delayed/);
  assert.doesNotMatch(source, /'label','retry approved hours update'/);
});

test('the workspace cannot offer finalisation before the authoritative cutoff', async () => {
  const source = lower(await readFile(readSqlPath, 'utf8'));
  assert.match(source, /v_finalise_enabled:=v_blocker_count=0[\s\S]*?statement_timestamp\(\)>=coalesce\([\s\S]*?scope\.cutoff_at_utc[\s\S]*?v_cycle\.cutoff_at_utc[\s\S]*?v_cycle\.state not in \('finalising','finalised'\)/);
});

test('rollback verifier covers unloaded paging, stale failure, mixed eligibility, exact accept, no-shifts and ACL', async () => {
  const verification = lower(await readFile(verifySqlPath, 'utf8'));
  for (const evidence of [
    'paged query response does not prove unloaded groups',
    'mixed candidate availability did not preserve the eligible subset',
    'stale workspace replay did not fail before further action',
    'tampered complete-group proof did not roll back before acceptance',
    'all-client completion did not finalise the group cycle',
    'bounded history period filter or pagination is incomplete',
    'rollback',
  ]) assert.match(verification, new RegExp(evidence));
});
