-- Data-free pure helper vectors. NOT_RUN. Root may compose the COMPLETE
-- forward/08_factual_snapshot.sqlinc under its authorised rollback route.
-- This is not CREATE, signature, Move, financial or API acceptance proof.
do $facts$
declare h jsonb; body jsonb; changed jsonb; fixture jsonb; rejected boolean; passed integer:=0;
begin
 h:='{"actual_schedule_json":[{"date":"2026-10-12","work_date":"2026-10-12","start_time":"19:00","end_time":"07:00","ref_num":null,"break_entry":{"kind":"NO_BREAK","no_break":true,"break_minutes":0}}],"additional_units_week":{"NIGHT":0},"additional_units_per_day":{"2026-10-12":{"NIGHT":0}}}'::jsonb;
 body:=h||jsonb_build_object('timesheet_patch_json',h);
 if private.candidate_workflow_factual_snapshot_assert_v1(body) is distinct from body then raise exception 'PURE_SNAPSHOT_VALID_FAILED';end if;passed:=passed+1;
 if private.candidate_workflow_factual_snapshot_assert_v1('{"expense_claim":{"mileage_units":0,"mileage_total_confirmed":false,"travel_amount":12.5,"description":""}}')
   is distinct from '{"expense_claim":{"mileage_units":0,"mileage_total_confirmed":false,"travel_amount":12.5,"description":""}}'::jsonb then raise exception 'PURE_EXPENSE_VALID_FAILED';end if;passed:=passed+1;
 foreach fixture in array array[
  'null'::jsonb,'[]'::jsonb,'{}'::jsonb,
  body-'additional_units_week',body-'timesheet_patch_json',
  jsonb_set(body,'{timesheet_patch_json,additional_units_week,NIGHT}','1'),
  jsonb_set(body,'{actual_schedule_json,0,date}','"2026-02-29"'),
  jsonb_set(body,'{actual_schedule_json,0,end_time}','"24:00"'),
  jsonb_set(body,'{additional_units_week,NIGHT}','"0"'),
  jsonb_set(body,'{additional_units_week,NIGHT}','-1'),
  jsonb_set(body,'{additional_units_week,NIGHT}','1000001'),
  jsonb_set(body,'{additional_units_week,pay_rate}','1'),
  body||'{"canonical_tsfin_snapshot":{"pay_total":1}}',
  body||'{"policy_snapshot_json":{}}',body||'{"day_off_dates":[]}',
  jsonb_set(body,'{actual_schedule_json,0,pay_rate}','1'),
  jsonb_set(body,'{timesheet_patch_json,contract_id}','"untrusted"'),
  jsonb_set(body,'{actual_schedule_json,0,break_entry}','{"kind":"DURATION_MINUTES","break_minutes":0}'),
  jsonb_set(body,'{actual_schedule_json,0,break_entry}','{"kind":"DURATION_MINUTES","break_minutes":1.5}'),
  jsonb_set(body,'{actual_schedule_json,0,break_entry}','{"kind":"START_END_TIMES","break_start":"23:45","break_end":"00:15","calculated_break_minutes":29}'),
  jsonb_set(body,'{actual_schedule_json,0,break_entry}','{"kind":"NO_BREAK","no_break":true,"break_minutes":0,"break_start":"12:00"}'),
  body||'{"break_entry_context":{"context_version":"CANDIDATE_BREAK_ENTRY_V1","context_token":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","mode":null}}',
  '{"expense_claim":{"travel_amount":0,"pay_rate":1}}'::jsonb,
  '{"expense_claim":{"other_amount":-1}}'::jsonb,
  '{"expense_category_resubmission":{"source_workflow_id":"internal-native"}}'::jsonb
 ] loop
  rejected:=false;
  begin perform private.candidate_workflow_factual_snapshot_assert_v1(fixture);
  exception when sqlstate '22023' then rejected:=true;end;
  if not rejected then raise exception 'PURE_SNAPSHOT_INVALID_ACCEPTED' using detail=fixture::text;end if;
  passed:=passed+1;
 end loop;
 changed:=jsonb_set(h,'{actual_schedule_json,0,break_entry}','{"kind":"START_END_TIMES","break_start":"23:45","break_end":"00:15","calculated_break_minutes":30}');
 body:=changed||jsonb_build_object('timesheet_patch_json',changed);
 if private.candidate_workflow_factual_snapshot_assert_v1(body) is distinct from body then raise exception 'PURE_BREAK_VALID_FAILED';end if;passed:=passed+1;
 if passed<>28 then raise exception 'PURE_SNAPSHOT_VECTOR_CENSUS_CHANGED' using detail=passed::text;end if;
 raise notice 'PASS pure factual snapshot vectors: %',passed;
end;
$facts$;
