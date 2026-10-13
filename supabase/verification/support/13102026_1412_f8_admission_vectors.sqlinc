-- Source-only helper vectors. Run after the exact new helpers are installed
-- within the owner's rollback transaction. This file creates no fixture rows,
-- release receipts, ledgers, email, payment or provider effects.
do $check$
declare
  v_rules jsonb:='{"checking_profile_kind":"HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1","earliest_final_weekday":4,"earliest_final_local_time":"00:00"}';
  v_summary jsonb:='{"checking_profile_kind":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1","earliest_final_weekday":null,"earliest_final_local_time":null}';
  v_result jsonb; v_case record; v_expected text;
begin
  for v_case in select * from (values
    ('2026-10-07 22:59:59+00'::timestamptz,'2026-09-28'::date,'2026-10-04'::date,'WEEKLY_SOURCE_FINAL_FILE_UPLOADED_TOO_EARLY'::text),
    ('2026-10-07 23:00:00+00','2026-09-28','2026-10-04',null),
    ('2026-10-08 11:00:00+00','2026-10-04','2026-10-08','WEEKLY_SOURCE_FINAL_FILE_COVERAGE_INCOMPLETE'),
    ('2026-10-08 11:00:00+00','2026-09-27','2026-10-04','WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET'),
    ('2026-10-08 11:00:00+00','2026-09-28','2026-10-03','WEEKLY_SOURCE_FINAL_FILE_COVERAGE_INCOMPLETE'),
    ('2026-10-08 11:00:00+00',null,'2026-10-04','WEEKLY_SOURCE_FINAL_FILE_COVERAGE_INCOMPLETE')
  ) cases(uploaded,coverage_start,coverage_end,code) loop
    v_result:=private.weekly_source_final_upload_facts_v1('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
      date '2026-10-04',timestamptz '2026-10-06 23:00:00+00',v_case.uploaded,'COMPLETE',
      v_case.coverage_start,v_case.coverage_end,v_rules,date '2026-09-28');
    if v_result->>'code' is distinct from v_case.code or (v_result->>'allowed')::boolean is distinct from (v_case.code is null) then
      raise exception 'F8 helper vector failed: %',v_result;
    end if;
  end loop;
  v_result:=private.weekly_source_final_upload_facts_v1('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
    date '2026-10-25',timestamptz '2026-10-28 00:00:00+00',timestamptz '2026-10-28 23:59:59+00',
    'COMPLETE',date '2026-10-19',date '2026-10-25',v_rules,date '2026-09-28');
  if v_result->>'code'<>'WEEKLY_SOURCE_FINAL_FILE_UPLOADED_TOO_EARLY'
    or v_result->>'message'<>'The final file can be uploaded from Thursday 00:00 (29/10/2026).' then
    raise exception 'F8 GMT clock/message vector failed';
  end if;
  v_result:=private.weekly_source_final_upload_facts_v1('ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
    date '2026-10-04',timestamptz '2026-10-07 10:00:00+00',timestamptz '2026-10-07 09:59:59+00',
    'COMPLETE',date '2026-09-28',date '2026-10-04',v_summary,date '2026-09-28');
  if v_result->>'code'<>'WEEKLY_SOURCE_FINAL_FILE_UPLOADED_TOO_EARLY' then raise exception 'F8 Magnit cutoff upload vector failed'; end if;
  v_result:=private.weekly_source_final_upload_facts_v1('ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
    date '2026-10-04',timestamptz '2026-10-07 10:00:00+00',timestamptz '2026-10-07 10:00:00+00',
    'COMPLETE',date '2026-09-28',date '2026-10-04',v_summary,date '2026-09-28');
  if v_result->'allowed' is distinct from 'true'::jsonb then raise exception 'F8 Magnit exact cutoff vector failed'; end if;
  foreach v_expected in array array['NHSP_FINAL_BACKING_V1','NHSP_PREFINAL_RELEASED_V1','MODE_A','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'] loop
    v_result:=private.weekly_source_final_upload_facts_v1(v_expected,null,null,null,null,null,null,null,null);
    if v_result->'allowed' is distinct from 'true'::jsonb then raise exception 'F8 legacy null-rule vector failed'; end if;
  end loop;
  v_result:=private.weekly_source_final_upload_facts_v1('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
    date '2026-10-04',timestamptz '2026-10-09 00:00:00+00',timestamptz '2026-10-10 00:00:00+00',
    'COMPLETE',date '2026-09-28',date '2026-10-04',v_rules,date '2026-09-28');
  if v_result->>'code'<>'WEEKLY_SOURCE_FINAL_UPLOAD_CONTEXT_INVALID' then raise exception 'F8 cutoff contradiction not held'; end if;
end;
$check$;
