-- Finite Health proposal only. Not an installed repeatable or sealed ABI.
-- Root installs only through the managed release and regenerated contract.
-- No operation, review, event capability, lock, calculation or financial write.
do $guard$
begin
 if to_regprocedure('private.timesheet_correction_entry_subject_v1(uuid,uuid)') is not null
  or to_regprocedure('private.timesheet_correction_entry_v1(uuid,uuid)') is not null
  or to_regprocedure('public.timesheet_correction_entry_v1(jsonb)') is not null then
  raise exception 'CORRECTION_ENTRY_ADDITIVE_SOURCE_CONFLICT';end if;
end;
$guard$;
create or replace function private.timesheet_correction_entry_subject_v1(
 p_timesheet_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare requested public.timesheets%rowtype; current_row public.timesheets%rowtype;
 requested_contract public.contracts%rowtype; current_contract public.contracts%rowtype;
 placement jsonb; evidence jsonb; schedule jsonb; day jsonb; workflow jsonb;
 financial public.timesheets_financials%rowtype; actual_day date; n integer;
begin
 -- Actor admission is global WP-PP. There is no invented actor/client ACL.
 if p_timesheet_id is null or p_actor_user_id is null
  or private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 select t.* into requested from public.timesheets t where t.timesheet_id=p_timesheet_id;
 if not found or requested.archived_at_utc is not null
  or requested.sheet_scope::text is distinct from 'WEEKLY' or requested.contract_id is null then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 select c.* into requested_contract from public.contracts c where c.id=requested.contract_id;
 if not found or requested_contract.candidate_id is null
  or not exists(select 1 from public.clients c where c.id=requested_contract.client_id)
  or not exists(select 1 from public.candidates c where c.id=requested_contract.candidate_id) then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 -- Final Candidate1003 owns rotations, accepted aliases and certified pending.
 -- It proves currentness only: actor and subject admission are independent here.
 placement:=private.timesheet_contract_current_placement_v1(p_timesheet_id);
 if placement->>'version' is distinct from 'CURRENT_CONTRACT_PLACEMENT_V1'
  or placement->>'requested_timesheet_id' is distinct from p_timesheet_id::text
  or placement->>'family_root_id' is null
  or coalesce(placement->>'placement_revision','') !~ '^(0|[1-9][0-9]*)$'
  or (placement->>'placement_revision')::numeric>9223372036854775807 then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 select c.* into current_contract from public.contracts c
  where c.id=(placement->>'current_contract_id')::uuid;
 if not found or current_contract.client_id is distinct from requested_contract.client_id
  or current_contract.candidate_id is distinct from requested_contract.candidate_id then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 if placement->'current_timesheet_id'='null'::jsonb then
  -- Only the exact resolver's certified pending branch permits this null.
  -- No current allocation is fabricated and no old record is offered to Move.
  return jsonb_build_object('version','TIMESHEET_CORRECTION_SUBJECT_V1',
   'requested_timesheet_id',p_timesheet_id,'current_timesheet_id',null,
   'placement_revision',placement->>'placement_revision','purpose','CONTRACT_CORRECTION',
   'pending',true);
 end if;
 select t.* into current_row from public.timesheets t
  where t.timesheet_id=(placement->>'current_timesheet_id')::uuid;
 if not found or not current_row.is_current or current_row.archived_at_utc is not null
  or current_row.revoked_at is not null or current_row.sheet_scope::text is distinct from 'WEEKLY'
  or current_row.contract_id is distinct from current_contract.id
  or current_row.week_ending_date is null then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 select count(*) into n from public.timesheets_financials f
  where f.timesheet_id=current_row.timesheet_id and f.is_current;
 if n>1 then raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 select f.* into financial from public.timesheets_financials f
  where f.timesheet_id=current_row.timesheet_id and f.is_current;
 if financial.id is not null and (financial.timesheet_version is distinct from current_row.version
  or financial.candidate_id is distinct from current_contract.candidate_id
  or financial.client_id is distinct from current_contract.client_id) then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 -- Native immutable evidence verifies accepted moved provenance and exact
 -- workflow/component ancestry. It is never returned across this read boundary.
 evidence:=private.timesheet_contract_move_evidence_v1(current_row.timesheet_id);
 if evidence is null or jsonb_typeof(evidence->'timesheet') is distinct from 'object'
  or jsonb_typeof(evidence->'workflows') is distinct from 'array'
  or jsonb_typeof(evidence->'components') is distinct from 'array' then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 -- Every exact attached workflow must stay in this native Candidate/Client
 -- scope. A retained A workflow may legitimately support a current B placement.
 for workflow in select value from jsonb_array_elements(evidence->'workflows') loop
  if workflow->>'candidate_id' is distinct from current_contract.candidate_id::text
   or coalesce(workflow->>'workflow_kind','') not in('CONTRACT_HOURS','CONTRACT_COMBINED','CONTRACT_EXPENSE')
   or not exists(select 1 from public.contracts c where c.id=(workflow->>'contract_id')::uuid
     and c.candidate_id=current_contract.candidate_id and c.client_id=current_contract.client_id)
   or not exists(select 1 from public.candidate_submission_workflows w
     where w.id=(workflow->>'id')::uuid and w.scope='WEEKLY'
      and w.week_ending_date=current_row.week_ending_date) then
   raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
  end if;
 end loop;
 if exists(select 1 from public.candidate_expense_components e
   left join public.candidate_submission_workflows w on w.id=e.workflow_id
   left join public.contracts c on c.id=w.contract_id
   where e.owning_timesheet_id=current_row.timesheet_id and
    (w.id is null or w.candidate_id is distinct from current_contract.candidate_id
     or c.candidate_id is distinct from current_contract.candidate_id
     or c.client_id is distinct from current_contract.client_id or w.scope is distinct from 'WEEKLY'
     or w.week_ending_date is distinct from current_row.week_ending_date
     or not exists(select 1 from jsonb_array_elements(evidence->'components') captured
       where captured->>'expense_component_id'=e.expense_component_id::text
        and captured->>'workflow_id'=w.id::text))) then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
 end if;
 -- Only actual native dates are inspected. This is subject admission, not
 -- destination/date/rate eligibility: that complete check belongs to Preview.
 schedule:=coalesce(current_row.actual_schedule_json,financial.actual_schedule_json);
 if schedule is not null and jsonb_typeof(schedule) is distinct from 'array' then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 for day in select value from jsonb_array_elements(coalesce(schedule,'[]'::jsonb)) loop
  if jsonb_typeof(day) is distinct from 'object' or coalesce(day->>'date','') !~ '^\d{4}-\d{2}-\d{2}$' then
   raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
  actual_day:=(day->>'date')::date;
  if to_char(actual_day,'YYYY-MM-DD') is distinct from day->>'date' then
   raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 end loop;
 if current_row.worked_end_iso is not null and (current_row.worked_start_iso is null
  or current_row.worked_end_iso<current_row.worked_start_iso) then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 -- An expense-only native claim has its exact workflow/week/component scope;
 -- no day is invented from week_ending_date and no HOURS/Source-only filter applies.
 if current_row.worked_start_iso is null and coalesce(jsonb_array_length(schedule),0)=0
  and not exists(select 1 from public.candidate_expense_components e
   where e.owning_timesheet_id=current_row.timesheet_id)
  and not (current_row.line_type::text='MILEAGE' or current_row.line_type::text='EXPENSES') then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 return jsonb_build_object('version','TIMESHEET_CORRECTION_SUBJECT_V1',
  'requested_timesheet_id',p_timesheet_id,'current_timesheet_id',current_row.timesheet_id,
  'placement_revision',placement->>'placement_revision','purpose','CONTRACT_CORRECTION','pending',false);
exception
 when sqlstate '42501' or sqlstate '22023' or sqlstate '55000' or sqlstate '23514'
  or sqlstate '22P02' or sqlstate '22007' or sqlstate '22008' or sqlstate '22003' then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
end;
$function$;

create or replace function private.timesheet_correction_entry_v1(p_timesheet_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare subject jsonb;
begin
 subject:=private.timesheet_correction_entry_subject_v1(p_timesheet_id,p_actor_user_id);
 return jsonb_build_object('version','TIMESHEET_CORRECTION_ENTRY_V1',
  'requested_timesheet_id',subject->'requested_timesheet_id','current_timesheet_id',subject->'current_timesheet_id',
  'placement_revision',subject->'placement_revision','purpose','CONTRACT_CORRECTION',
  'may_open_review',false,'disabled_reason',case when subject->'pending'='true'::jsonb
   then 'CURRENT_ALLOCATION_PENDING' else 'REVIEW_UNAVAILABLE' end);
end;
$function$;

create or replace function public.timesheet_correction_entry_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
begin
 if jsonb_typeof(p_request) is distinct from 'object' then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 if (select array_agg(key order by key) from jsonb_object_keys(p_request) key)
    is distinct from array['actor_user_id','timesheet_id','version']::text[]
  or p_request->>'version' is distinct from 'TIMESHEET_CORRECTION_ENTRY_REQUEST_V1'
  or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
  or jsonb_typeof(p_request->'timesheet_id') is distinct from 'string' then
  raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';end if;
 return private.timesheet_correction_entry_v1((p_request->>'timesheet_id')::uuid,(p_request->>'actor_user_id')::uuid);
exception when sqlstate '22P02' then
 raise exception 'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED' using errcode='42501';
end;
$function$;

revoke all on function private.timesheet_correction_entry_subject_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private.timesheet_correction_entry_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public.timesheet_correction_entry_v1(jsonb) from public,anon,authenticated;
grant execute on function public.timesheet_correction_entry_v1(jsonb) to service_role;
notify pgrst,'reload schema';
