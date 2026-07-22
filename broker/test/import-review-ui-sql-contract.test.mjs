import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const uiSql = readFileSync(new URL('../../supabase/repeatable/22072026_0052_import_review_ui_contract_v1.sql', import.meta.url), 'utf8');
const emailSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql', import.meta.url), 'utf8');
const lifecycleSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql', import.meta.url), 'utf8');
const coreSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_00_import_review_internal_core.sql', import.meta.url), 'utf8');

function functionBody(source, name) {
  const marker = `create or replace function public.${name}(`;
  const start = source.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} must be defined`);
  const end = source.toLowerCase().indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} must have a complete body`);
  return source.slice(start, end + '$function$;'.length);
}

test('the fail-closed database contract exposes the bounded review UI and recipient grouping versions', () => {
  const body = functionBody(uiSql, 'import_review_contract_version_get_v1');
  assert.match(body, /review_ui_contract_version','IMPORT_REVIEW_UI_V2/);
  assert.match(body, /email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1/);
  assert.match(body, /legacy_contracts_supported',false/);
});

test('staged scope discovery is actor-bound, source-owned and bounded', () => {
  const body = functionBody(uiSql, 'import_review_staged_scope_get_v1');
  assert.match(body, /_import_review_assert_actor_v1/);
  assert.match(body, /v_row_count>500/);
  assert.match(body, /v_size not in \(25,50,75,100,500\)/);
  assert.match(body, /source_file_sha256/);
  assert.match(body, /coverage_start_date/);
  assert.match(body, /candidate_total_pages/);
  assert.match(body, /_import_review_overlap_preflight_core_v2/);
  assert.match(body, /overlapping_unfinished_reviews/);
});

test('action paging supports only the approved sizes and deterministic server-side sorts', () => {
  const body = functionBody(uiSql, 'import_review_actions_page_v1');
  assert.match(body, /v_size not in \(25,50,75,100\)/);
  assert.match(body, /candidate_surname_sort/);
  assert.match(body, /action_id asc/);
  assert.match(body, /'has_previous'/);
  assert.match(body, /'has_next'/);
  assert.match(body, /'view_counts'/);
});

test('overlap preflight is route, date and client aware', () => {
  const body = functionBody(coreSql, '_import_review_overlap_preflight_core_v2');
  assert.match(body, /hi\.source_system=p_source_system/);
  assert.match(body, /daterange\(hi\.coverage_start_date,hi\.coverage_end_date,'\[\]'\)/);
  assert.match(body, /current\.client_id is not null and other_client\.client_id=current\.client_id/);
  assert.match(body, /other_client\.source_client_key=current\.source_client_key/);
  assert.match(body, /limit 20/);
});

test('review get is authoritative for editability, commands, evidence and final confirmation', () => {
  const body = functionBody(lifecycleSql, 'import_review_get_v1');
  assert.match(body, /'editability'/);
  assert.match(body, /'allowed_commands'/);
  assert.match(body, /v_state\.status in \('APPLYING','APPLIED','ABANDONED','SUPERSEDED'\)/);
  assert.match(body, /'evidence'/);
  assert.match(body, /'confirmation_summary'/);
  assert.match(body, /selected_email_reminder_count/);
});

test('failed-before-commit recovery proves no commit before reopening the review', () => {
  const body = functionBody(lifecycleSql, 'import_review_apply_failed_before_commit_recover_v1');
  assert.match(body, /o\.state<>'FAILED_BEFORE_COMMIT'/);
  assert.match(body, /o\.committed_at_utc is not null/);
  assert.match(body, /status=v_status/);
  assert.match(body, /'source_committed',false/);
  assert.match(lifecycleSql, /grant execute on function public\.import_review_apply_failed_before_commit_recover_v1/);
});

test('query email enqueue consolidates one outbox message per normalised recipient address', () => {
  const body = functionBody(emailSql, 'timesheet_query_email_enqueue_v1');
  assert.match(body, /group by lower\(route->>'recipient_email'\)/);
  assert.match(body, /RECIPIENT_EMAIL:/);
  assert.match(body, /business_route_count/);
  assert.match(body, /Items are grouped by client and contract/);
  assert.match(body, /TIMESHEET_QUERY_EMAIL/);
  assert.doesNotMatch(body, /group by route->>'recipient_scope',route->>'recipient_scope_key'/);
});

test('new public read RPCs are service-role-only', () => {
  for (const signature of [
    'import_review_staged_scope_get_v1\\(uuid,uuid,integer,integer\\)',
    'import_review_actions_page_v1\\(uuid,uuid,integer,integer,text,text,text\\)'
  ]) {
    assert.match(uiSql, new RegExp(`revoke all on function public\\.${signature} from public,anon,authenticated;`));
    assert.match(uiSql, new RegExp(`grant execute on function public\\.${signature} to service_role;`));
  }
});
