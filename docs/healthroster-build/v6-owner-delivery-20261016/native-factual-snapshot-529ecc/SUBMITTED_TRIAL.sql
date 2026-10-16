set local pg_show_plans.is_enabled=off;
-- Closed factual snapshot for the additive existing CREATE input_snapshot.
-- New-field limits are explicit; this neither calculates nor accepts economics.
create or replace function private.candidate_workflow_factual_snapshot_assert_v1(p_snapshot jsonb)
returns jsonb language plpgsql immutable security definer
set search_path=pg_catalog,private,pg_temp
as $function$
declare s jsonb; e jsonb; c jsonb; m jsonb; n jsonb; k text; d text; hours boolean; lo integer; hi integer; duration integer;
begin
 if jsonb_typeof(p_snapshot) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
 if exists(select 1 from jsonb_object_keys(p_snapshot) key where key not in('actual_schedule_json','additional_units_week',
  'additional_units_per_day','timesheet_patch_json','break_entry_context','expense_claim')) then
  raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
 hours:=p_snapshot ? 'actual_schedule_json';
 if not hours and (not p_snapshot ? 'expense_claim' or p_snapshot ?| array['additional_units_week','additional_units_per_day','timesheet_patch_json','break_entry_context']) then
  raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
 if hours then
  if not p_snapshot ?& array['additional_units_week','additional_units_per_day','timesheet_patch_json']
   or jsonb_typeof(p_snapshot->'actual_schedule_json') is distinct from 'array' then
   raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  if jsonb_array_length(p_snapshot->'actual_schedule_json')>112 then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  for s in select value from jsonb_array_elements(p_snapshot->'actual_schedule_json') loop
   if jsonb_typeof(s) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   if not s ?& array['date','work_date','start_time','end_time'] or exists(select 1 from jsonb_object_keys(s) key
     where key not in('row_key','segment_id','date','work_date','start_time','end_time','ref_num','reference','booking_reference','band','break_entry')) then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   foreach k in array array['date','work_date'] loop
    d:=s->>k;
    if jsonb_typeof(s->k) is distinct from 'string' or d !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or (d::date)::text is distinct from d then
     raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   end loop;
   foreach k in array array['start_time','end_time'] loop
    if jsonb_typeof(s->k) is distinct from 'string' or s->>k !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
     raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   end loop;
   foreach k in array array['row_key','segment_id','ref_num','reference','booking_reference','band'] loop
    if s ? k and not (k in('ref_num','reference','booking_reference','band') and s->k='null'::jsonb)
     and (jsonb_typeof(s->k) is distinct from 'string' or length(s->>k)>200) then
     raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   end loop;
   if s ? 'break_entry' then
    e:=s->'break_entry';
    if jsonb_typeof(e) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
    if e->>'kind'='NO_BREAK' then
     if not e ?& array['kind','no_break','break_minutes'] or e->'no_break' is distinct from 'true'::jsonb or e->'break_minutes' is distinct from '0'::jsonb
      or exists(select 1 from jsonb_object_keys(e) key where key not in('kind','no_break','break_minutes')) then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
    elsif e->>'kind'='DURATION_MINUTES' then
     if not e ?& array['kind','break_minutes'] or jsonb_typeof(e->'break_minutes') is distinct from 'number'
      or (e->>'break_minutes')::numeric<>trunc((e->>'break_minutes')::numeric) or (e->>'break_minutes')::numeric not between 1 and 1440
      or exists(select 1 from jsonb_object_keys(e) key where key not in('kind','break_minutes')) then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
    elsif e->>'kind'='START_END_TIMES' then
     if not e ?& array['kind','break_start','break_end','calculated_break_minutes']
      or jsonb_typeof(e->'break_start') is distinct from 'string' or jsonb_typeof(e->'break_end') is distinct from 'string'
      or e->>'break_start' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' or e->>'break_end' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
      or jsonb_typeof(e->'calculated_break_minutes') is distinct from 'number'
      or exists(select 1 from jsonb_object_keys(e) key where key not in('kind','break_start','break_end','calculated_break_minutes')) then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
     lo:=split_part(e->>'break_start',':',1)::integer*60+split_part(e->>'break_start',':',2)::integer;
     hi:=split_part(e->>'break_end',':',1)::integer*60+split_part(e->>'break_end',':',2)::integer;
     duration:=(hi-lo+1440)%1440;
     if duration=0 or (e->>'calculated_break_minutes')::numeric<>duration then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
    else raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   end if;
  end loop;
  c:=p_snapshot->'additional_units_per_day';
  if jsonb_typeof(c) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  if (select count(*) from jsonb_object_keys(c))>7 then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  for d in select jsonb_object_keys(c) loop
   if d !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or (d::date)::text is distinct from d then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  end loop;
  for m in select p_snapshot->'additional_units_week' union all select value from jsonb_each(c) loop
   if jsonb_typeof(m) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   if (select count(*) from jsonb_object_keys(m))>1000 then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   for k,n in select key,value from jsonb_each(m) loop
    if k !~ '^[A-Z0-9_]{1,40}$' or jsonb_typeof(n) is distinct from 'number' or n::text::numeric not between 0 and 1000000 then
     raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   end loop;
  end loop;
  c:=p_snapshot->'timesheet_patch_json';
  if jsonb_typeof(c) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  if not c ?& array['actual_schedule_json','additional_units_week','additional_units_per_day']
   or exists(select 1 from jsonb_object_keys(c) key where key not in('actual_schedule_json','additional_units_week','additional_units_per_day')) then
   raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  foreach k in array array['actual_schedule_json','additional_units_week','additional_units_per_day'] loop
   if c->k is distinct from p_snapshot->k then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  end loop;
  if p_snapshot ? 'break_entry_context' then
   c:=p_snapshot->'break_entry_context';
   if jsonb_typeof(c) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
   if not c ?& array['context_version','context_token','mode'] or c->>'context_version' is distinct from 'CANDIDATE_BREAK_ENTRY_V1'
    or jsonb_typeof(c->'context_token') is distinct from 'string' or c->>'context_token' !~ '^[0-9a-f]{64}$'
    or jsonb_typeof(c->'mode') is distinct from 'string' or c->>'mode' not in('START_END_TIMES','DURATION_MINUTES')
    or exists(select 1 from jsonb_object_keys(c) key where key not in('context_version','context_token','mode')) then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  end if;
 end if;
 if p_snapshot ? 'expense_claim' then
  e:=p_snapshot->'expense_claim';
  if jsonb_typeof(e) is distinct from 'object' then raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  if exists(select 1 from jsonb_object_keys(e) key where key not in('mileage_units','mileage_total_confirmed','travel_amount','accommodation_amount','other_amount','description')) then
   raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  foreach k in array array['mileage_units','travel_amount','accommodation_amount','other_amount'] loop
   if e ? k and (jsonb_typeof(e->k) is distinct from 'number' or (e->>k)::numeric not between 0 and 1000000) then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
  end loop;
  if e ? 'mileage_total_confirmed' and jsonb_typeof(e->'mileage_total_confirmed') is distinct from 'boolean'
   or e ? 'description' and (jsonb_typeof(e->'description') is distinct from 'string' or length(e->>'description')>2000) then
   raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';end if;
 end if;
 return p_snapshot;
exception when invalid_datetime_format or datetime_field_overflow or invalid_text_representation or numeric_value_out_of_range then
 raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
end;
$function$;
alter function private.candidate_workflow_factual_snapshot_assert_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.candidate_workflow_factual_snapshot_assert_v1(jsonb) from public,anon,authenticated,service_role;

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
