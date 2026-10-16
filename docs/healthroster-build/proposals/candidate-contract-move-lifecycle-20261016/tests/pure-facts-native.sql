-- Data-free scalar proof. No business rows, workflow/audit fabrication, native
-- financial calculation, approval, publication, provider, or external effects.
-- NOT_RUN until root's protected rollback tool executes the exact complete unit.
do $facts_native$
declare raw jsonb; canonical jsonb; variant jsonb; original_t jsonb; before_facts jsonb; after_facts jsonb;
begin
 raw:='{"actual_schedule_json":[{"date":"2026-10-12","start":"09:00","end":"17:00","ref_num":"REF-1","break_entry":{"kind":"NO_BREAK","no_break":true,"break_minutes":0}}],"additional_units_week":{"ONCALL":2},"additional_units_per_day":{"2026-10-12":{"ONCALL":1}},"expense_claim":{"mileage_units":10,"travel_amount":5,"accommodation_amount":20,"other_amount":3,"description":"Claim"}}';
 canonical:=jsonb_build_object('contract_version','CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1',
  'timesheet_patch_json',jsonb_build_object('actual_schedule_json',jsonb_build_array(
   ((raw#>'{actual_schedule_json,0}')-'break_entry')||jsonb_build_object('no_break',true,'break_minutes',0,'break_mins',0,
    'breaks','[]'::jsonb,'break_start',null,'break_end',null,'break_start_iso',null,'break_end_iso',null)),
   'additional_units_week',raw->'additional_units_week','additional_units_per_day',raw->'additional_units_per_day'),
  'canonical_tsfin_snapshot',jsonb_build_object('mileage_units',10,'travel_pay_ex_vat',5,'accommodation_pay_ex_vat',20,'other_pay_ex_vat',3,
   'expenses_description','Claim','mileage_pay_rate',0.45,'mileage_charge_rate',0.65,'total_pay_ex_vat',100,'total_charge_ex_vat',200));
 if private.candidate_contract_move_signature_facts_v1(raw) is distinct from private.candidate_contract_move_signature_facts_v1(canonical) then
  raise exception 'FACTS_NATIVE_RAW_CANONICAL_MISMATCH';end if;
 variant:=jsonb_set(jsonb_set(canonical,'{canonical_tsfin_snapshot,total_pay_ex_vat}','999'),'{canonical_tsfin_snapshot,mileage_pay_rate}','0');
 if private.candidate_contract_move_signature_facts_v1(canonical) is distinct from private.candidate_contract_move_signature_facts_v1(variant) then
  raise exception 'FACTS_NATIVE_RATE_ONLY_MISMATCH';end if;
 if private.candidate_contract_move_signature_facts_v1(jsonb_build_object('hours_submission',canonical,'expense_submission',canonical))
   is distinct from private.candidate_contract_move_signature_facts_v1(raw) then raise exception 'FACTS_NATIVE_COMBINED_WRAPPER_MISMATCH';end if;
 foreach variant in array array[
  jsonb_set(raw,'{actual_schedule_json,0,end}','"18:00"'),
  jsonb_set(raw,'{additional_units_week,ONCALL}','3'),
  jsonb_set(raw,'{additional_units_per_day,2026-10-12,ONCALL}','2'),
  jsonb_set(raw,'{actual_schedule_json,0,ref_num}','"REF-2"'),
  jsonb_set(raw,'{expense_claim,mileage_units}','11'),
  jsonb_set(raw,'{expense_claim,travel_amount}','6'),
  jsonb_set(raw,'{expense_claim,accommodation_amount}','21'),
  jsonb_set(raw,'{expense_claim,other_amount}','4'),
  jsonb_set(raw,'{expense_claim,description}','"Changed claim"')
 ] loop
  if private.candidate_contract_move_signature_facts_v1(raw)=private.candidate_contract_move_signature_facts_v1(variant) then
   raise exception 'FACTS_NATIVE_CHANGED_FACT_OMITTED';end if;
 end loop;
 original_t:=jsonb_build_object('week_ending_date','2026-10-18','actual_schedule_json',raw->'actual_schedule_json',
  'worked_minutes',480,'reference_number','HEADER-1');
 before_facts:=private.candidate_contract_move_facts_v1(original_t,jsonb_build_array(jsonb_build_object('id','old','input_snapshot_json',raw)));
 after_facts:=private.candidate_contract_move_facts_v1(original_t,jsonb_build_array(
  jsonb_build_object('id','old','immutable_submission_json',canonical,'input_snapshot_json',canonical),
  jsonb_build_object('id','new-empty','generation',10,'state','WORKER_DRAFT','input_snapshot_json','{}'::jsonb)));
 if before_facts is distinct from after_facts then raise exception 'FACTS_NATIVE_EMPTY_ENROLMENT_OR_CANONICAL_CHURN';end if;
 after_facts:=private.candidate_contract_move_facts_v1(original_t,jsonb_build_array(jsonb_build_object('input_snapshot_json',raw),jsonb_build_object('input_snapshot_json',canonical)));
 if before_facts is distinct from after_facts then raise exception 'FACTS_NATIVE_EQUAL_BODY_DUPLICATE_CHURN';end if;
 foreach variant in array array[jsonb_set(original_t,'{worked_minutes}','481'),jsonb_set(original_t,'{reference_number}','"HEADER-2"'),
   jsonb_set(original_t,'{actual_schedule_json,0,end}','"19:00"'),jsonb_set(original_t,'{additional_units_week}','{"ONCALL":9}')] loop
  if before_facts=private.candidate_contract_move_facts_v1(variant,jsonb_build_array(jsonb_build_object('input_snapshot_json',raw))) then
   raise exception 'FACTS_NATIVE_LIVE_PHYSICAL_EDIT_OMITTED';end if;
 end loop;
 -- Native first-submit normalizers: explicit duration and midnight break window.
 raw:='{"actual_schedule_json":[{"date":"2026-10-12","start":"20:00","end":"08:00","break_entry":{"kind":"START_END_TIMES","break_start":"23:45","break_end":"00:15","calculated_break_minutes":30}}]}';
 canonical:='{"timesheet_patch_json":{"actual_schedule_json":[{"date":"2026-10-12","start":"20:00","end":"08:00","break_start":"23:45","break_end":"00:15","break_minutes":30}]}}';
 if private.candidate_contract_move_signature_facts_v1(raw) is distinct from private.candidate_contract_move_signature_facts_v1(canonical) then
  raise exception 'FACTS_NATIVE_MIDNIGHT_BREAK_MISMATCH';end if;
 raw:='{"actual_schedule_json":[{"date":"2026-10-12","start":"09:00","end":"17:00","break_entry":{"kind":"DURATION_MINUTES","break_minutes":30}}]}';
 canonical:='{"timesheet_patch_json":{"actual_schedule_json":[{"date":"2026-10-12","start":"09:00","end":"17:00","break_minutes":30}]}}';
 if private.candidate_contract_move_signature_facts_v1(raw) is distinct from private.candidate_contract_move_signature_facts_v1(canonical) then
  raise exception 'FACTS_NATIVE_DURATION_BREAK_MISMATCH';end if;
 if private.candidate_contract_move_signature_facts_v1('{"expense_claim":{"other_amount":1.005}}')->>'other_amount' <> '1.00000000000000000000' then
  -- Compare as numeric: JSON text scale is not itself an attested fact.
  if (private.candidate_contract_move_signature_facts_v1('{"expense_claim":{"other_amount":1.005}}')->>'other_amount')::numeric<>1 then
   raise exception 'FACTS_NATIVE_JS_AMOUNT_ROUNDING_MISMATCH';end if;
 end if;
 if private.candidate_contract_move_signature_facts_v1('{}') is distinct from private.candidate_contract_move_signature_facts_v1(
   '{"expense_claim":{"mileage_units":0,"travel_amount":0,"accommodation_amount":0,"other_amount":0,"description":""}}') then
  raise exception 'FACTS_NATIVE_MISSING_ZERO_CLAIM_MISMATCH';end if;
 begin perform private.candidate_contract_move_signature_facts_v1('{"expense_claim":{"other_amount":-1}}');
  raise exception 'FACTS_NATIVE_INVALID_AMOUNT_ACCEPTED';exception when invalid_parameter_value then null;end;
 begin perform private.candidate_contract_move_signature_facts_v1('{"actual_schedule_json":[{"break_entry":{"kind":"START_END_TIMES","break_start":"12:00","break_end":"12:30","calculated_break_minutes":29}}]}');
  raise exception 'FACTS_NATIVE_INVALID_BREAK_ACCEPTED';exception when invalid_parameter_value then null;end;
end;
$facts_native$;
select jsonb_build_object('version','CANDIDATE_MOVE_PURE_FACTS_NATIVE_V1','assertions',24,'native','PASS_IF_REACHED') as pure_facts_scalar_result;
