import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const owner = (text, name) => { const from = text.indexOf(name); const end = text.indexOf('\n$function$;', from);
  assert.ok(from >= 0 && end > from); return text.slice(from, end + '\n$function$;'.length); };
const admission = read('supabase/repeatable/13102026_1359_weekly_source_timing_coverage_group_guards_v1.sql');
const prepare = read('supabase/repeatable/13102026_1404_weekly_source_import_preparation_timing_guard_v1.sql');
const move = read('supabase/repeatable/13102026_1404_weekly_source_client_group_move_guard_v1.sql');
const context = read('supabase/repeatable/13102026_1404_weekly_source_upload_coverage_context_v1.sql');
const stage = read('supabase/repeatable/13102026_1412_weekly_source_upload_stage_coverage_floor_v1.sql');

test('complete Prepare preserves exact native owner except an admission call before first write', () => {
  const name = 'create or replace function public.weekly_source_import_prepare_atomic_v1';
  assert.equal(owner(prepare, name).replace('  perform private.weekly_source_final_upload_assert_v1(v_upload.id);\n', ''),
    owner(read('supabase/repeatable/30092026_2052_weekly_source_import_preparation_v1.sql'), name));
  assert.ok(prepare.indexOf('weekly_source_current_publication_guard_v1') < prepare.indexOf('weekly_source_final_upload_assert_v1'));
  assert.ok(prepare.indexOf('weekly_source_final_upload_assert_v1') < prepare.indexOf('insert into private.weekly_source_import_preparations'));
});

test('complete Client Save retains every F1/native branch outside sorted group and CURRENT revision guard', () => {
  const name = 'CREATE OR REPLACE FUNCTION public.weekly_source_client_settings_save_atomic_v1';
  const stripped = owner(move, name).replace('  v_locked_old_group_id uuid;\n', '')
    .replace(/  -- F8\/G1:[\s\S]*?(?=  select \* into v_group from public\.weekly_source_groups source_group)/, '')
    .replace(/\n  if v_old_group_id is distinct from v_locked_old_group_id[\s\S]*?(?=\n\n  select pg_catalog\.count\(\*\) into v_membership_history_count)/, '');
  assert.equal(stripped, read('tests/weekly-source/fixtures/f8-client-save-f1-input.txt'));
  assert.ok(move.indexOf('weekly-source-settings-client:') < move.indexOf('order by source_group.id for update'));
  assert.ok(move.indexOf('order by source_group.id for update') < move.indexOf('where source_group.id=v_group_id for update'));
  assert.ok(move.indexOf('WEEKLY_SOURCE_CLIENT_GROUP_HAS_CURRENT_FINAL_REVISION') < move.indexOf('insert into public.weekly_source_group_clients'));
  const guard = move.slice(move.indexOf('if v_old_group_id is not null and v_old_group_id<>v_group_id and exists('), move.indexOf('select pg_catalog.count(*) into v_membership_history_count'));
  assert.match(guard, /final.state='CURRENT'/); assert.match(guard, /manifest.client_id=v_client/);
  assert.match(guard, /report.client_id=v_client/); assert.doesNotMatch(guard, /ROSTER|NHSP|self_bill|rule_set/);
});

test('complete Context preserves released eligibility and legacy JSON outside optional server facts', () => {
  const name = 'create or replace function public.weekly_source_upload_context_v1(';
  const stripped = owner(context, name).replace('  v_coverage_context jsonb;\n', '')
    .replace('    v_coverage_context:=private.weekly_source_upload_coverage_context_v1(v_client_id,v_cycle.id);\n', '')
    .replace(")||case when v_coverage_context is null then '{}'::jsonb\n      else jsonb_build_object('coverage_context',v_coverage_context) end;", ');');
  assert.equal(stripped, owner(read('supabase/repeatable/08102026_1635_weekly_source_released_client_eligibility_v1.sql'), name));
});

test('direct Stage floor cannot bypass authority and preserves existing source/upload/replay owners', () => {
  const name = 'create or replace function public.weekly_source_upload_stage_begin_atomic_v1(';
  const stripped = owner(stage, name).replace(/  -- WP-F8: complete authority[\s\S]*?v_client_id,v_cycle.id,v_profile.profile_code,v_confirmed_start\);\n\n/, '');
  assert.equal(stripped, owner(read('supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql'), name));
  assert.ok(stage.indexOf("v_actor,'UPLOAD_SOURCE'") < stage.indexOf('weekly_source_upload_coverage_floor_assert_v1'));
  assert.ok(stage.indexOf('weekly_source_upload_coverage_floor_assert_v1') < stage.indexOf('insert into public.weekly_source_uploads'));
});

test('S12 uses immutable saved upload time, current exact rules and three export profiles only', () => {
  assert.match(admission, /v_upload.uploaded_at_utc/); assert.match(admission, /p_uploaded_at_utc<v_earliest/);
  assert.match(admission, /p_week_ending\+v_weekday/); assert.match(admission, /v_earliest:=p_cutoff_at_utc/);
  assert.match(admission, /p_coverage_start>v_required_start or p_coverage_end<p_week_ending/);
  assert.doesNotMatch(admission, /update public.timesheets|insert into public.weekly_source_final|policy_sha256|rate_hash|NO_CHANGE/);
  assert.match(admission, /from public,anon,authenticated,service_role/);
});

test('client floor retains disabled activation receipts and excludes unrelated clients and never-enabled drafts', () => {
  const floor = owner(admission, 'create or replace function private.weekly_source_rule_coverage_floor_v1');
  assert.doesNotMatch(floor, /and a.feature_enabled/);
  assert.match(floor, /gc.client_id=p_client_id/); assert.match(floor, /feature_purpose='RULE_SET'/);
  assert.match(floor, /WEEKLY_SOURCE_RULE_FEATURE_RECEIPT_V1/); assert.match(floor, /final.rule_set_version_id/);
  assert.match(floor, /min\(first_week\)-6/); assert.match(floor, /WEEKLY_SOURCE_RULE_COVERAGE_HISTORY_UNPROVABLE/);
});
