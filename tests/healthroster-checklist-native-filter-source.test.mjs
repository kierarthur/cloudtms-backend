import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
const root=new URL('../supabase/repeatable/',import.meta.url),read=name=>fs.readFileSync(new URL(name,root),'utf8');
test('single D10 adapter filter keys project persisted native decision/comparison codes and never display text or recalculate differences',()=>{
  const groups=read('healthroster_wp_u3_source_proposals/wp_u3_check_only_query_groups.sqlinc'),native=read('13102026_1205_healthroster_issued_signed_query_v1.sql');
  const helper=groups.split('$function$')[1];
  for(const code of ['DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED','ACTUAL_HOURS_MISMATCH','START_END_MISMATCH',
    'BREAK_MINUTES_MISMATCH','WORKED_HOURS','MISSING_FROM_IMPORT']){assert.ok(native.includes(code),code);assert.ok(helper.includes(code),code);}
  const preview=read('21072026_1820_13_hr_weekly_validation_preview.sql');
  for(const status of ['MISMATCH','UNMATCHED','HR_ONLY']){assert.ok(preview.includes(`= '${status}'`),status);assert.ok(helper.includes(`'${status}'`),status);}
  for(const key of ['TIMESHEET_MISSING','HOURS_DIFFER','MISSING_SHIFT'])assert.ok(helper.includes(`jsonb_build_array('${key}')`),key);
  assert.doesNotMatch(helper,/outcome_label|like\s+'%|worked_start|worked_end|worked_minutes|hours_worked|paid|withheld|\babs\(/i);
  assert.ok(groups.includes('private.weekly_source_check_only_issue_filter_keys_v1(latest_decision.action_kind,native_summary)'));
  assert.ok(groups.includes("'issue_filter_keys',child_filter_keys"));assert.ok(groups.includes("'issue_filter_keys',group_filter_keys"));
  assert.ok(groups.includes('select distinct key from jsonb_array_elements_text(group_filter_keys||child_filter_keys)'));
  assert.ok(groups.includes("'children',children,'actions',jsonb_build_array(review_action)"));
  assert.ok(groups.includes("'groups',groups,'item_targets',targets"));
  // The existing literal parent recipe is unchanged; its read/proof now seals
  // the same projected native keys that Report consumes before COUNT/PAGE.
  assert.ok(groups.includes("parent_identity:=jsonb_build_object('scope',scope,'item_id',actual_item_id,'preview',preview,'actions',action_bindings)"));
});
test('accepted native upload default CHECKING survives absent metadata in both Checklist discovery and Client leaf without loosening acceptance/scope',()=>{
  const native=read('13102026_1412_weekly_source_upload_stage_coverage_floor_v1.sql'),begin=read('13102026_1344_weekly_source_self_bill_report/13102026_1344_upload_begin.sqlinc');
  for(const source of [native,begin])assert.ok(source.includes("coalesce(v_metadata->>'import_use','CHECKING') not in ('CHECKING','PREPARE_FINALISATION')"));
  for(const name of ['wp_u3_client_week_context.sqlinc','wp_u3_native_checklist.sqlinc']){
    const source=read('healthroster_wp_u3_source_proposals/'+name);
    assert.ok(source.includes("upper(coalesce(u.file_metadata_json->>'import_use','CHECKING'))='CHECKING'"));
    assert.ok(source.includes("a.result='ACCEPTED'"));assert.ok(source.includes("u.state in('CURRENT','SUPERSEDED')"));
    assert.doesNotMatch(source,/coalesce\(u\.file_metadata_json->>'import_use',''\)/);
  }
});
