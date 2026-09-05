import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const pageSql = fs.readFileSync(
  new URL('../supabase/repeatable/30082026_0125_candidate_submitted_weekly_card_linkage.sql', import.meta.url),
  'utf8'
);
const detailSql = fs.readFileSync(
  new URL('../supabase/repeatable/05092026_0420_candidate_timesheet_effective_pay_history_v1.sql', import.meta.url),
  'utf8'
);

test('Candidate Current and History use the established effective payment precedence', () => {
  for (const sql of [pageSql, detailSql]) {
    assert.match(sql, /timesheet_summary_pay_state_cache/i);
    assert.match(sql, /summary_state_applies/i);
    assert.match(sql, /timesheet_pay_state/i);
    assert.match(sql, /summary_pay_status_code/i);
    assert.match(sql, /summary_pay_paid_at_utc/i);
    assert.match(sql, /last_settled_at_utc/i);
  }
  assert.match(pageSql, /case when effective_pay\.pay_status_code='PAID' then effective_pay\.paid_at_utc else null end as paid_at_utc/i);
  assert.match(detailSql, /if upper\(coalesce\(v_effective_pay_status_code,'UNPAID'\)\)<>'PAID' then[\s\S]*v_effective_paid_at_utc:=null;/i);
});

test('Candidate effective payment readers retain least-privilege ACLs', () => {
  assert.match(pageSql, /revoke all on function public\.candidate_app_timesheet_page_v1\([^;]+from public,anon,authenticated;/i);
  assert.match(detailSql, /revoke all on function public\.candidate_app_timesheet_detail_v1\([\s\S]*from public,anon,authenticated;/i);
  assert.match(detailSql, /notify pgrst, 'reload schema';/i);
});
