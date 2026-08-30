import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql=readFileSync(
  new URL('../supabase/repeatable/30082026_0125_candidate_submitted_weekly_card_linkage.sql',import.meta.url),
  'utf8'
);

test('submitted Weekly workflows remain linked to their exact Contract Week before Timesheet materialisation',()=>{
  const fallback=sql.match(/select draft_week\.id[\s\S]*?limit 1\)/i)?.[0]??'';
  assert.ok(fallback,'expected the bounded Contract Week fallback');
  for(const state of [
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  ]) assert.match(fallback,new RegExp(`'${state}'`));
  for(const state of ['CANCELLED','SUPERSEDED','REJECTED'])
    assert.doesNotMatch(fallback,new RegExp(`'${state}'`));
  assert.match(fallback,/classified\.target_timesheet_id is null/i);
  assert.match(fallback,/classified\.anchor_timesheet_id is null/i);
  assert.match(fallback,/draft_week\.id=classified\.contract_week_id/i);
  assert.match(sql,/workflows\.display_timesheet_id=coalesce\(base\.timesheet_id,base\.id\)/i);
});

test('submitted Weekly cards show immutable hours and expenses before Timesheet materialisation',()=>{
  assert.match(sql,/\{hours_submission,canonical_tsfin_snapshot,total_hours\}/i);
  for(const field of [
    'expenses_pay_ex_vat','mileage_pay_ex_vat','travel_pay_ex_vat',
    'accommodation_pay_ex_vat','other_pay_ex_vat'
  ]) assert.match(sql,new RegExp(`\\{expense_submission,canonical_tsfin_snapshot,${field}\\}`,'i'));
  assert.match(sql,/case when base\.timesheet_id is null then coalesce\(workflows\.submitted_total_hours,base\.total_hours,0\)/i);
  assert.match(sql,/'total_hours',coalesce\(d\.overlay_total_hours,0\)/i);
  assert.match(sql,/case when base\.timesheet_id is null then coalesce\(workflows\.submitted_expenses_pay_ex_vat,/i);
  assert.match(sql,/else coalesce\(totals\.expenses_pay_ex_vat,base\.expenses_pay_ex_vat,0\) end as overlay_expenses_pay_ex_vat/i);
});

test('replacement preserves service-only paging authority and reloads PostgREST',()=>{
  assert.match(sql,/create or replace function public\.candidate_app_timesheet_page_v1\(/i);
  assert.doesNotMatch(sql,/create or replace function private\._candidate_daily_read_projection_v1\(/i);
  assert.match(sql,/alter function public\.candidate_app_timesheet_page_v1\([^;]+\) owner to postgres;/i);
  assert.match(sql,/revoke all on function public\.candidate_app_timesheet_page_v1\([^;]+\) from public,anon,authenticated;/i);
  assert.match(sql,/grant execute on function public\.candidate_app_timesheet_page_v1\([^;]+\) to service_role;/i);
  assert.match(sql,/notify pgrst, 'reload schema';/i);
});
