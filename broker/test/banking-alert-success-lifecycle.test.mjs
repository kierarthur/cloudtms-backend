import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const repoRoot = path.resolve(import.meta.dirname, '../..');
const migrationPath = path.join(repoRoot, 'supabase/migrations/20260718155608_banking_alert_success_lifecycle.sql');
const repeatablePath = path.join(repoRoot, 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const workerPath = path.join(repoRoot, 'broker/src/index.js');

const migration = fs.readFileSync(migrationPath, 'utf8');
const repeatable = fs.readFileSync(repeatablePath, 'utf8');
const worker = fs.readFileSync(workerPath, 'utf8');

function sliceBetween(source, start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return source.slice(from, to);
}

test('future scheduling and settlement are captured as separate persistent events', () => {
  const trigger = sliceBetween(
    migration,
    'CREATE OR REPLACE FUNCTION public.banking_alert_success_event_capture_pay_batch()',
    'DROP TRIGGER IF EXISTS trg_banking_alert_success_events_pay_batches_insert'
  );

  assert.match(trigger, /v_status = 'SCHEDULED'/);
  assert.match(trigger, /v_schedule_kind = 'SCHEDULED'/);
  assert.match(trigger, /NEW\.scheduled_at_utc > now\(\)/);
  assert.match(trigger, /v_is_csv_settlement IS NOT TRUE/);
  assert.match(trigger, /v_alert_kind := 'BATCH_SCHEDULED_SUCCESS'/);
  assert.match(trigger, /v_status = 'SETTLED'/);
  assert.match(trigger, /v_alert_kind := 'BATCH_SETTLED_SUCCESS'/);
  assert.match(trigger, /ON CONFLICT \(pay_batch_id, alert_kind, event_key\) DO NOTHING/);
});

test('immediate and CSV executions cannot create a scheduling alert', () => {
  const trigger = sliceBetween(
    migration,
    'CREATE OR REPLACE FUNCTION public.banking_alert_success_event_capture_pay_batch()',
    'DROP TRIGGER IF EXISTS trg_banking_alert_success_events_pay_batches_insert'
  );

  assert.match(trigger, /v_schedule_kind = 'SCHEDULED'/);
  assert.doesNotMatch(trigger, /v_schedule_kind IN \('SCHEDULED','IMMEDIATE'\)/);
  assert.match(trigger, /v_execution_mode LIKE 'CSV%'/);
  assert.match(trigger, /CSV settlement recorded successfully/);
  assert.match(trigger, /CloudTMS did not transfer the money/);
});

test('success messages use frozen payment artifacts and include the required details', () => {
  const trigger = sliceBetween(
    migration,
    'CREATE OR REPLACE FUNCTION public.banking_alert_success_event_capture_pay_batch()',
    'DROP TRIGGER IF EXISTS trg_banking_alert_success_events_pay_batches_insert'
  );

  assert.match(trigger, /NEW\.total_bank_out/);
  assert.match(trigger, /public\.pay_batch_display_summary/);
  assert.match(trigger, /public\.pay_bank_transfers/);
  assert.match(trigger, /will be paid across/);
  assert.match(trigger, /\(UK Time\)/);
  assert.match(trigger, /individual_payment_count/);
  assert.match(trigger, /'policy_x_source', 'FROZEN_BATCH_ARTIFACTS'/);
  assert.doesNotMatch(trigger, /finance_components|finance_cases|timesheets|timesheet_segments|live finance/i);
});

test('active alert computation includes latest schedule and settlement events and caches full detail', () => {
  assert.match(migration, /latest_success_events AS MATERIALIZED/);
  assert.match(migration, /DISTINCT ON \(success_event\.pay_batch_id, success_event\.alert_kind\)/);
  assert.match(migration, /SELECT \* FROM success_event_alerts/);
  assert.match(migration, /'BATCH_SCHEDULED_SUCCESS'/);
  assert.match(migration, /'BATCH_SETTLED_SUCCESS'/);
  assert.match(migration, /summary_json,\s*updated_at_utc[\s\S]*v_active_json,\s*now\(\)/);
  assert.match(repeatable, /summary_json = EXCLUDED\.summary_json/);
});

test('clear one, clear many, and clear all read the live alert set in alert-management context', () => {
  assert.match(migration, /ELSIF p_limit = 0 THEN\s*v_limit := NULL::integer/);
  const contextCalls = migration.match(/'ALERT_MANAGEMENT'/g) || [];
  assert.ok(contextCalls.length >= 7, 'expected explicit alert-management contexts on acknowledgement reads');
  assert.match(migration, /CREATE OR REPLACE FUNCTION public\.banking_alert_acknowledge\(/);
  assert.match(migration, /CREATE OR REPLACE FUNCTION public\.banking_alert_acknowledge_many\(/);
  assert.match(migration, /CREATE OR REPLACE FUNCTION public\.banking_alert_acknowledge_all_current\(/);
});

test('worker exposes explicit alert loading and keeps generic changes ping lightweight', () => {
  assert.match(worker, /async function handleBankingAlertsGet/);
  assert.match(worker, /banking_alerts_refresh_for_user/);
  assert.match(worker, /p_alert_context: 'ALERT_PANEL'/);
  assert.match(worker, /req\.method === 'GET' && p === '\/api\/banking\/alerts'/);
  const changesPing = sliceBetween(worker, 'async function handleChangesPing', 'async function handleRolesGlobal');
  assert.doesNotMatch(changesPing, /banking_alerts_active_for_user/);
  assert.match(changesPing, /banking_alert_summary_deferred = count > 0/);
});

test('successful lifecycle alerts are enabled and user-configurable', () => {
  assert.match(migration, /ALTER COLUMN include_success_alerts SET DEFAULT true/);
  assert.match(migration, /include_success_alerts = EXCLUDED\.include_success_alerts/);
  assert.match(migration, /RETURN COALESCE\(\(v_preferences ->> 'include_success_alerts'\)::boolean, true\)/);
  assert.match(worker, /'BATCH_SCHEDULED_SUCCESS'/);
  assert.match(worker, /'BATCH_SETTLED_SUCCESS'/);
  assert.doesNotMatch(worker, /preferencesPayload\.include_success_alerts = false/);
});

test('security-definer alert RPCs are restricted to the Worker service role', () => {
  const aclStart = migration.indexOf('-- Alert RPCs are Worker/service-role only.');
  assert.notEqual(aclStart, -1, 'missing alert RPC permission hardening');
  const acl = migration.slice(aclStart);

  assert.match(acl, /REVOKE ALL ON FUNCTION public\.banking_alerts_active_for_user\([^)]+\) FROM PUBLIC, anon, authenticated/);
  assert.match(acl, /REVOKE ALL ON FUNCTION public\.banking_alerts_refresh_for_user\([^)]+\) FROM PUBLIC, anon, authenticated/);
  assert.match(acl, /REVOKE ALL ON FUNCTION public\.banking_alert_acknowledge\([^)]+\) FROM PUBLIC, anon, authenticated/);
  assert.match(acl, /REVOKE ALL ON FUNCTION public\.banking_alert_preferences_update\([^)]+\) FROM PUBLIC, anon, authenticated/);
  assert.match(acl, /GRANT EXECUTE ON FUNCTION public\.banking_alerts_refresh_for_user\([^)]+\) TO service_role/);
  assert.match(acl, /GRANT EXECUTE ON FUNCTION public\.banking_alert_acknowledge_all_current\([^)]+\) TO service_role/);
  assert.equal(acl.includes('TO authenticated'), false);
  assert.match(repeatable, /-- Alert RPCs are Worker\/service-role only\.[\s\S]*GRANT EXECUTE ON FUNCTION public\.banking_alert_display_summary_refresh_for_user/);
});
