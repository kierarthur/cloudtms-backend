import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const lifecycle = readFileSync(new URL('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql', import.meta.url), 'utf8');
const retirements = readFileSync(new URL('../../supabase/repeatable/21072026_1820_99_import_review_hard_cutover_retirements.sql', import.meta.url), 'utf8');

function functionBody(source, name) {
  const marker = `create or replace function public.${name}(`;
  const start = source.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} must be defined`);
  const end = source.toLowerCase().indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} must have a complete body`);
  return source.slice(start, end + '$function$;'.length);
}

test('database contract exposes the component-aware follow-up capability', () => {
  const body = functionBody(lifecycle, 'import_review_contract_version_get_v1');
  assert.match(body, /follow_up_component_version','IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1/);
  assert.match(body, /legacy_contracts_supported',false/);
});

test('component update is request-hash bound and stores independent EMAIL and TSFIN state', () => {
  const body = functionBody(lifecycle, 'import_review_follow_up_component_update_v1');
  assert.match(body, /p_request_hash text/);
  assert.match(body, /o\.request_hash is distinct from lower\(btrim\(p_request_hash\)\)/);
  assert.match(body, /v_component not in \('EMAIL','TSFIN'\)/);
  assert.match(body, /review_email_follow_up_status/);
  assert.match(body, /review_tsfin_follow_up_status/);
  assert.match(body, /v_current='FAILED_RETRYABLE' and v_new='PENDING'/);
  assert.match(body, /v_current='PENDING' and v_new in \('COMPLETE','FAILED_RETRYABLE'\)/);
});

test('aggregate reconciliation derives bounded errors from the failed components', () => {
  const body = functionBody(lifecycle, '_import_review_follow_up_reconcile_core_v1');
  assert.match(body, /review_email_follow_up_error_code/);
  assert.match(body, /review_tsfin_follow_up_error_code/);
  assert.match(body, /MULTIPLE_FOLLOW_UP_COMPONENTS_FAILED/);
  assert.match(body, /follow_up_error_code=v_error_code/);
  assert.match(body, /follow_up_error_message=v_error_message/);
});

test('the new mutation RPC is service-role-only and the superseded TSFIN-only RPC is retired', () => {
  assert.match(lifecycle, /revoke all on function public\.import_review_follow_up_component_update_v1\([^)]+\) from public,anon,authenticated;/);
  assert.match(lifecycle, /grant execute on function public\.import_review_follow_up_component_update_v1\([^)]+\) to service_role;/);
  assert.doesNotMatch(lifecycle, /create or replace function public\.import_review_follow_up_update_v1\(/);
  assert.match(retirements, /drop function if exists public\.import_review_follow_up_update_v1\(/);
});

test('read contracts expose bounded aggregate follow-up diagnostics for the frontend', () => {
  for (const name of ['import_review_get_v1', 'import_review_apply_status_get_v1']) {
    const body = functionBody(lifecycle, name);
    assert.match(body, /follow_up_error_code/);
    assert.match(body, /follow_up_error_message/);
    assert.match(body, /follow_up_retry_count/);
  }
});

test('empty and exact-page review lists use typed cursors and emit a cursor only when more rows exist', () => {
  const body = functionBody(lifecycle, 'import_review_list_v1');
  assert.match(body, /v_last_updated_at timestamptz/);
  assert.match(body, /v_last_import_id uuid/);
  assert.match(body, /v_has_more boolean:=false/);
  assert.match(body, /select count\(\*\)>v_limit from page/);
  assert.match(body, /case when v_has_more then jsonb_build_object/);
  assert.doesNotMatch(body, /v_last record/);
});
