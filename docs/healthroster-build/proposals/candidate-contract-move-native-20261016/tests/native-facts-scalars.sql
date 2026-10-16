-- Synthetic JSON ONLY: pure factual-domain assertions, no native business setup.
-- Root may include this inside the protected rollback rehearsal after the actual helpers.
do $test$
declare a jsonb; b jsonb; workflows jsonb; changed jsonb; original_digest bytea;
begin
 a:=jsonb_build_object('timesheet_id','00000000-0000-4000-8000-000000000001',
  'contract_id','00000000-0000-4000-8000-000000000002','booking_id','actual-original',
  'week_ending_date','2026-10-18','worked_minutes',480,'break_minutes',0,
  'actual_schedule_json',jsonb_build_array(jsonb_build_object('date','2026-10-12','hours',8)));
 workflows:=jsonb_build_array(jsonb_build_object('id','00000000-0000-4000-8000-000000000003',
  'generation',2,'state','AWAITING_MANAGER_APPROVAL','immutable_submission_json',jsonb_build_object('worked_minutes',480),
  'input_snapshot_json',jsonb_build_object('worked_minutes',480)));
 original_digest:=private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(a,workflows));
 b:=a||jsonb_build_object('timesheet_id','00000000-0000-4000-8000-000000000004',
  'contract_id','00000000-0000-4000-8000-000000000005','booking_id','actual-current');
 if original_digest is distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b,workflows)) then
  raise exception 'FACTS_DOMAIN_MOVED_IDENTITY_NOT_EXCLUDED';end if;
 changed:=jsonb_set(jsonb_set(workflows,'{0,generation}','3'),'{0,state}','"MANAGER_APPROVED"');
 if original_digest is distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b,changed)) then
  raise exception 'FACTS_DOMAIN_NATIVE_LIFECYCLE_NOT_SEPARATE';end if;
 if original_digest is not distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b||jsonb_build_object('worked_minutes',481),workflows)) then
  raise exception 'FACTS_DOMAIN_CHANGED_HOURS_NOT_DETECTED';end if;
 if original_digest is not distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b||jsonb_build_object('break_minutes',null),workflows)) then
  raise exception 'FACTS_DOMAIN_ZERO_BREAK_CONFUSED_WITH_UNKNOWN';end if;
 changed:=jsonb_set(workflows,'{0,immutable_submission_json,worked_minutes}','481');
 if original_digest is not distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b,changed)) then
  raise exception 'FACTS_DOMAIN_CHANGED_SIGNED_BODY_NOT_DETECTED';end if;
 changed:=jsonb_set(workflows,'{0,input_snapshot_json,worked_minutes}','481');
 if original_digest is not distinct from private.timesheet_contract_move_row_hash_v1(private.candidate_contract_move_facts_v1(b,changed)) then
  raise exception 'FACTS_DOMAIN_CHANGED_NATIVE_DRAFT_NOT_DETECTED';end if;
end;
$test$;
