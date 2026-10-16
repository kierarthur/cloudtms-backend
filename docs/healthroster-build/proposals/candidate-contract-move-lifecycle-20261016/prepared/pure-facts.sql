-- Complete source-only pure facts subset; root owns SEND7 rollback execution.
create or replace function private._candidate_expense_number_v1(
  p_value jsonb,
  p_key text
)
returns numeric
language sql
immutable
parallel safe
set search_path = pg_catalog, pg_temp
as $function$
  select case
    when pg_catalog.jsonb_typeof(coalesce(p_value,'{}'::jsonb)->p_key)='number'
      then greatest(((coalesce(p_value,'{}'::jsonb)->>p_key)::numeric),0)
    when coalesce(p_value,'{}'::jsonb)->>p_key ~ '^[+]?[0-9]+(?:[.][0-9]+)?$'
      then greatest(((coalesce(p_value,'{}'::jsonb)->>p_key)::numeric),0)
    else 0
  end;
$function$;
alter function private._candidate_expense_number_v1(jsonb,text) owner to postgres;
revoke all on function private._candidate_expense_number_v1(jsonb,text) from public,anon,authenticated,service_role;

-- Worked/body fact projection, not a financial calculator. Exact native input
-- selection: buildCandidateWeeklySubmissionThroughCanonicalAuthority and native
-- normaliseAdaptiveBreakEntry/normaliseCandidateBreakInput. Current worked rows
-- and immutable source receipt checks remain separate.
create or replace function private.candidate_contract_move_signature_facts_v1(p_submission jsonb)
returns jsonb language plpgsql immutable security definer set search_path=pg_catalog,private,pg_temp
as $function$
declare
 h jsonb; e jsonb; schedule jsonb; segment jsonb; entry jsonb; normalized jsonb:='[]'::jsonb;
 amount_key text; amount_text text; kind text; start_value text; end_value text; start_minutes integer; end_minutes integer; minutes integer;
begin
 h:=coalesce(nullif(p_submission->'hours_submission','null'::jsonb),p_submission,'{}'::jsonb);
 h:=coalesce(nullif(h->'timesheet_patch_json','null'::jsonb),h);
 e:=coalesce(nullif(p_submission->'expense_submission','null'::jsonb),nullif(p_submission->'expense_claim','null'::jsonb),p_submission,'{}'::jsonb);
 e:=coalesce(nullif(e->'canonical_tsfin_snapshot','null'::jsonb),e);
 for amount_key in select unnest(array['mileage_units',case when e ? 'travel_amount' then 'travel_amount' else 'travel_pay_ex_vat' end,
  case when e ? 'accommodation_amount' then 'accommodation_amount' else 'accommodation_pay_ex_vat' end,
  case when e ? 'other_amount' then 'other_amount' else 'other_pay_ex_vat' end]) loop
  amount_text:=btrim(e->>amount_key);
  if amount_text is not null and amount_text<>'' and (amount_text !~ '^[+]?[0-9]+([.][0-9]+)?$'
   or jsonb_typeof(e->amount_key) not in('number','string')) then
   raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
 end loop;
 schedule:=coalesce(nullif(h->'actual_schedule_json','null'::jsonb),'[]'::jsonb);
 if jsonb_typeof(schedule)<>'array' then raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
 for segment in select value from jsonb_array_elements(schedule) loop
  if jsonb_typeof(segment)<>'object' then raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
  entry:=segment->'break_entry';
  if entry is not null then
   if jsonb_typeof(entry)<>'object' or exists(select 1 from jsonb_object_keys(entry) k
    where k not in('kind','break_start','break_end','calculated_break_minutes','break_minutes','no_break')) then
    raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
   kind:=upper(entry->>'kind');
   if kind='NO_BREAK' and entry->'no_break'='true'::jsonb and entry->>'break_minutes'='0'
     and not entry ?| array['break_start','break_end','calculated_break_minutes'] then
    segment:=(segment-'break_entry')||jsonb_build_object('no_break',true,'break_minutes',0);
   elsif kind='DURATION_MINUTES' and entry->>'break_minutes' ~ '^[0-9]+$'
     and (entry->>'break_minutes')::numeric between 1 and 1440
     and not entry ?| array['break_start','break_end','calculated_break_minutes','no_break'] then
    segment:=(segment-'break_entry')||jsonb_build_object('break_minutes',(entry->>'break_minutes')::integer);
   elsif kind='START_END_TIMES' and entry->>'break_start' ~ '^[0-2][0-9]:[0-5][0-9]$'
     and entry->>'break_end' ~ '^[0-2][0-9]:[0-5][0-9]$' and not entry ?| array['break_minutes','no_break'] then
    start_value:=entry->>'break_start';end_value:=entry->>'break_end';
    start_minutes:=split_part(start_value,':',1)::integer*60+split_part(start_value,':',2)::integer;
    end_minutes:=split_part(end_value,':',1)::integer*60+split_part(end_value,':',2)::integer;
    minutes:=case when end_minutes>start_minutes then end_minutes-start_minutes else end_minutes+1440-start_minutes end;
    if start_minutes>=1440 or end_minutes>=1440 or start_minutes=end_minutes
      or entry->>'calculated_break_minutes' is distinct from minutes::text then
     raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
    segment:=(segment-'break_entry')||jsonb_build_object('break_start',start_value,'break_end',end_value,'break_minutes',minutes);
   else raise exception 'CANDIDATE_MOVE_FACTS_INVALID' using errcode='22023';end if;
  end if;
  -- Exact native zero-break canonicalization. Keep every other segment field,
  -- including dates, worked start/end aliases, references, and additional facts.
  if segment->'no_break'='true'::jsonb or segment->'noBreak'='true'::jsonb
    or coalesce(nullif(segment->>'break_minutes',''),nullif(segment->>'break_mins','')) ~ '^[+]?0+([.]0+)?$' then
   segment:=segment||jsonb_build_object('no_break',true,'break_minutes',0,'break_mins',0,'breaks','[]'::jsonb,
    'break_start',null,'break_end',null,'break_start_iso',null,'break_end_iso',null);
  end if;
  normalized:=normalized||jsonb_build_array(segment);
 end loop;
 return jsonb_build_object('actual_schedule_json',normalized,
  'additional_units_week',coalesce(nullif(h->'additional_units_week','null'::jsonb),'{}'::jsonb),
  'additional_units_per_day',coalesce(nullif(h->'additional_units_per_day','null'::jsonb),'{}'::jsonb),
  'mileage_units',floor(private._candidate_expense_number_v1(e,'mileage_units')::double precision*100+0.5)::numeric/100,
  'travel_amount',floor(private._candidate_expense_number_v1(e,case when e ? 'travel_amount' then 'travel_amount' else 'travel_pay_ex_vat' end)::double precision*100+0.5)::numeric/100,
  'accommodation_amount',floor(private._candidate_expense_number_v1(e,case when e ? 'accommodation_amount' then 'accommodation_amount' else 'accommodation_pay_ex_vat' end)::double precision*100+0.5)::numeric/100,
  'other_amount',floor(private._candidate_expense_number_v1(e,case when e ? 'other_amount' then 'other_amount' else 'other_pay_ex_vat' end)::double precision*100+0.5)::numeric/100,
  'description',btrim(coalesce(e->>'description',e->>'expenses_description','')));
end;
$function$;
alter function private.candidate_contract_move_signature_facts_v1(jsonb) owner to postgres;
revoke all on function private.candidate_contract_move_signature_facts_v1(jsonb) from public,anon,authenticated,service_role;

-- Successor worked-fact domain. Identity/ancestry and accepted body proof are
-- independently checked by the native context and private workflow fact reader.
create or replace function private.candidate_contract_move_facts_v1(p_timesheet jsonb,p_workflows jsonb)
returns jsonb language sql immutable security definer set search_path=pg_catalog,private,pg_temp
as $function$
 select jsonb_build_object('version','CANDIDATE_MOVE_FACTS_V2',
  'timesheet',jsonb_build_object('week_ending_date',p_timesheet->'week_ending_date',
   'actual_schedule_json',p_timesheet->'actual_schedule_json','worked_start_iso',p_timesheet->'worked_start_iso',
   'worked_end_iso',p_timesheet->'worked_end_iso','worked_minutes',p_timesheet->'worked_minutes',
   'break_start_iso',p_timesheet->'break_start_iso','break_end_iso',p_timesheet->'break_end_iso','break_minutes',p_timesheet->'break_minutes',
   'additional_units_week',p_timesheet->'additional_units_week','additional_units_per_day',p_timesheet->'additional_units_per_day',
   'reference_number',p_timesheet->'reference_number','day_references_json',p_timesheet->'day_references_json'),
  'workflows',coalesce((select jsonb_agg(distinct facts order by facts) from (
    select coalesce(w->'worked_facts',private.candidate_contract_move_signature_facts_v1(
      coalesce(nullif(w->'input_snapshot_json','null'::jsonb),nullif(w->'immutable_submission_json','null'::jsonb),'{}'::jsonb))) facts
    from jsonb_array_elements(p_workflows) w
   ) projected where facts<>private.candidate_contract_move_signature_facts_v1('{}'::jsonb)
    and facts<>private.candidate_contract_move_signature_facts_v1(p_timesheet)),'[]'::jsonb));
$function$;
alter function private.candidate_contract_move_facts_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.candidate_contract_move_facts_v1(jsonb,jsonb) from public,anon,authenticated,service_role;

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
 -- An empty native draft can refer to an already-worked physical Timesheet.
 -- Adding the exact same worked body is not a new worked fact or signature.
 raw:='{"actual_schedule_json":[{"date":"2026-10-12","start":"09:00","end":"17:00","break_minutes":0}],"additional_units_week":{"ONCALL":1}}';
 original_t:=raw||jsonb_build_object('week_ending_date','2026-10-18','worked_minutes',480);
 canonical:=jsonb_build_object('timesheet_patch_json',raw,'canonical_tsfin_snapshot','{}'::jsonb);
 if private.candidate_contract_move_facts_v1(original_t,jsonb_build_array(jsonb_build_object('input_snapshot_json','{}'::jsonb)))
  is distinct from private.candidate_contract_move_facts_v1(original_t,jsonb_build_array(jsonb_build_object('input_snapshot_json',canonical))) then
  raise exception 'FACTS_NATIVE_EXISTING_PHYSICAL_BODY_DUPLICATE_CHURN';end if;
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
select jsonb_build_object('version','CANDIDATE_MOVE_PURE_FACTS_NATIVE_V1','assertions',25,'native','PASS_IF_REACHED') as pure_facts_scalar_result;
