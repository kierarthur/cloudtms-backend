import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const root = new URL('../../', import.meta.url);
const read = (path) => readFile(new URL(path, root), 'utf8');

test('Weekly source settings have one service-only durable owner and CAS saves', async () => {
  const sql = await read('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql');
  for (const name of [
    'weekly_source_client_settings_get_v1',
    'weekly_source_client_settings_save_atomic_v1',
    'weekly_source_contract_settings_get_v1',
    'weekly_source_contract_settings_save_atomic_v1',
    'weekly_source_global_settings_get_v1',
    'weekly_source_global_settings_save_atomic_v1',
    'weekly_source_source_groups_get_v1',
    'weekly_source_source_group_save_atomic_v1',
  ]) {
    assert.match(sql, new RegExp(`create or replace function public\\.${name}\\(`, 'i'));
    assert.match(sql, new RegExp(`'public\\.${name}\\(jsonb\\)'`, 'i'));
  }
  assert.match(sql, /execute 'revoke all on function '\|\|v_signature\|\|' from public,anon,authenticated'/i);
  assert.match(sql, /execute 'grant execute on function '\|\|v_signature\|\|' to service_role'/i);
  assert.match(sql, /execute 'alter function '\|\|v_signature\|\|' owner to current_user'/i);
  assert.doesNotMatch(sql, /execute[^;]*owner to postgres/i);
  assert.match(sql, /notify pgrst, 'reload schema';\s*commit;/i);
  assert.match(sql, /expected_settings_version/i);
  assert.match(sql, /WEEKLY_SOURCE_SETTINGS_STALE/);
  assert.match(sql, /WEEKLY_SOURCE_CLIENT_QUERY_SETTINGS_NOT_APPLICABLE/);
  assert.match(sql, /WEEKLY_SOURCE_CONTRACT_QUERY_SETTINGS_NOT_APPLICABLE/);
  assert.match(sql, /TIMESHEET_AUTHORITY[\s\S]*candidate_queries/i);
  assert.doesNotMatch(sql, /pay_workbench|banking_pay|create_draft|payment_execution|settlement/i);
});

test('Global secure-link lifetime replaces the hard-coded manager expiry', async () => {
  const schema = await read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
  const delivery = await read('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql');
  const verification = await read('supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql');
  assert.match(schema, /manager_secure_link_lifetime interval not null default interval '7 days'/i);
  for (const table of [
    'weekly_manager_review_batches',
    'weekly_manager_route_receipts',
    'weekly_manager_route_preparations',
  ]) {
    const start = schema.indexOf(`create table public.${table}`);
    const end = schema.indexOf(`alter table public.${table}`, start);
    const definition = schema.slice(start, end);
    assert.match(definition, /expires_at_utc>=issued_at_utc\+interval '1 day'[\s\S]*expires_at_utc<=issued_at_utc\+interval '30 days'/i);
    assert.doesNotMatch(definition, /expires_at_utc=issued_at_utc\+interval '7 days'/i);
  }
  assert.match(delivery, /select settings\.manager_secure_link_lifetime/i);
  assert.match(delivery, /v_issued_at\+v_secure_link_lifetime/i);
  const prepare = delivery.slice(
    delivery.indexOf('create or replace function public.weekly_source_manager_route_prepare_atomic_v1'),
    delivery.indexOf('create or replace function public.weekly_source_message_render_stage_atomic_v1'),
  );
  assert.doesNotMatch(prepare, /v_issued_at\+interval '7 days'/i);
  assert.match(verification, /manager_secure_link_lifetime=interval '5 days'/i);
  assert.match(verification, /expires_at_utc'[\s\S]*issued_at_utc'[\s\S]*interval '5 days'/i);
});

test('Effective-dated saves stale only open source projections', async () => {
  const sql = await read('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql');
  assert.match(sql, /state in \('OPEN','FINALISABLE'\)/);
  assert.match(sql, /from public\.weekly_source_report_scopes report_scope[\s\S]*report_scope\.state in \('FINALISING','CORRECTION_IN_PROGRESS'\)/);
  assert.match(sql, /projection_state='NONE',current_projection_publication_id=null/);
  assert.match(sql, /WEEKLY_SOURCE_SETTINGS_SCOPE_BUSY/);
  assert.doesNotMatch(sql, /where[^;]*state='FINALISED'[^;]*update/is);
});

test('Contract writes admit only effective_from and the declared override fields', async () => {
  const sql = await read('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql');
  const start = sql.indexOf('create or replace function public.weekly_source_contract_settings_save_atomic_v1');
  const end = sql.indexOf('do $weekly_source_settings_admin_acl$', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const save = sql.slice(start, end);
  const allowedStart = save.indexOf("v_input,array[");
  const allowedEnd = save.indexOf("],'WEEKLY_SOURCE_CONTRACT_SETTINGS_VALUES_INVALID'", allowedStart);
  assert.notEqual(allowedStart, -1);
  assert.notEqual(allowedEnd, -1);
  const allowed = save.slice(allowedStart, allowedEnd);
  for (const key of [
    'effective_from',
    'weekly_rate_classification_method_override',
    'duration_break_tie_rule_override',
    'source_fixed_expenses_enabled_override',
    'source_expense_vat_enabled_override',
    'candidate_queries_enabled_override',
    'manager_queries_enabled_override',
    'manager_query_recipient_override',
    'completed_pack_copy_enabled_override',
    'completed_pack_recipient_override',
  ]) assert.match(allowed, new RegExp(`'${key}'`));
  assert.doesNotMatch(allowed, /'(?:effective|client_settings)'/);
});

test('Weekly source verifier settings fixtures satisfy current mandatory semantic hashes', async () => {
  const paths = [
    'supabase/verification/15092026_1534_weekly_source_projection_build_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_finalisation_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_protected_action_orchestration_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_protected_pay_publisher_v1.sql',
    'supabase/verification/15092026_1534_weekly_source_upload_context_v1.sql',
    'supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql',
  ];
  for (const path of paths) {
    const verification = await read(path);
    assert.match(
      verification,
      /insert into public\.settings_defaults\([\s\S]*candidate_manager_email_templates_sha256[\s\S]*candidate_home_announcement_sha256[\s\S]*\)\s*values/i,
      path,
    );
  }
});

test('Settings verifier retains the explicit source and Timesheet contract authority flags', async () => {
  const verification = await read('supabase/verification/15092026_1534_weekly_source_settings_admin_v1.sql');
  assert.match(
    verification,
    /self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,\s*overrideclientsettings[\s\S]*true,'HEALTHROSTER',true,true,true,true[\s\S]*false,'HEALTHROSTER',false,true,false,true/i,
  );
});

test('Dedicated NHSP contracts derive source authority without the HealthRoster no-timesheet flag', async () => {
  const sql = await read('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql');
  const authorityDerivations = sql.match(
    /when contract\.weekly_timesheet_source::text='NHSP'\s+or coalesce\(contract\.no_timesheet_required,false\) then 'SOURCE_AUTHORITY'/gi,
  ) ?? [];
  assert.equal(authorityDerivations.length, 4);

  const verification = await read('supabase/verification/15092026_1534_weekly_source_settings_admin_v1.sql');
  assert.match(
    verification,
    /true,'NHSP',false,false,false,true[\s\S]*dedicated NHSP source-authority derivation proof failed[\s\S]*dedicated NHSP settings save proof failed/i,
  );
});

test('NHSP uses one server-owned source group and clients do not choose it', async () => {
  const sql = await read('supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql');
  assert.match(sql, /weekly-source-settings-nhsp-group:/i);
  assert.match(sql, /WEEKLY_SOURCE_NHSP_GROUP_ALREADY_EXISTS/);
  assert.match(sql, /WEEKLY_SOURCE_NHSP_GROUP_CARDINALITY_INVALID/);
  assert.match(sql, /if v_family='NHSP' then[\s\S]*WEEKLY_SOURCE_NHSP_GROUP_REQUIRED[\s\S]*select source_group\.id into strict v_group_id/i);
  assert.match(sql, /'source_group_id',case when v_derived_family='NHSP' then v_default_group_id else null end/i);
  assert.match(sql, /v_group_id:=nullif\(v_input->>'source_group_id',''\)::uuid/i);
});
