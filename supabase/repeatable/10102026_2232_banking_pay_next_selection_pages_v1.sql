-- WB23 finite original accepted selection pages. Three-way successors1832/2026.
\set ON_ERROR_STOP on
begin;
-- Same original admin, module epoch, actor and run. Never a new accepted cut.
create or replace function private.bpay_next_selection_page_authority_v1(p_run_id uuid,p_actor_user_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if not exists(select 1 from private.bpay_next_pay_run r
   join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
   join private.bpay_next_command c on c.id=r.accepted_choice_command_id
   where r.id=p_run_id and r.actor_user_id=p_actor_user_id and r.accepted_choice_descriptor is not null
    and c.module_epoch=m.owner_epoch
    and r.accepted_choice_descriptor->'namespace'->>'actor_user_id'=p_actor_user_id::text
    and r.accepted_choice_descriptor->'namespace'->>'owner_epoch'=m.owner_epoch::text) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
end
$function$;
alter function private.bpay_next_selection_page_authority_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_selection_page_authority_v1(uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_exclusion_page_v1(p_run_id uuid,p_page_no bigint,p_request jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_exclusion_page%rowtype;
 v_work jsonb;v_candidates jsonb;v_value jsonb;v_result jsonb;v_n integer;v_total bigint;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id for update;
 if v_run.accepted_choice_descriptor is null then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_MISSING';end if;
 v_n:=jsonb_array_length(p_request->'excluded_work_ids')+jsonb_array_length(p_request->'excluded_candidate_ids');
 if v_n>100 then raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
 for v_value in select value from jsonb_array_elements((p_request->'excluded_work_ids')||(p_request->'excluded_candidate_ids')) loop
  if jsonb_typeof(v_value) is distinct from 'string' or v_value#>>'{}'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
 end loop;
 select src.* into v_page from private.bpay_next_selection_exclusion_page src where run_id=p_run_id and page_no=p_page_no;
 if found then
  if v_page.request_json is distinct from p_request or v_page.captured_result_json is null then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';end if;
  return v_page.captured_result_json||jsonb_build_object('replay',true);
 end if;
 if v_run.selection_state<>'OPEN' or exists(select 1 from private.bpay_next_selection_page
   where run_id=p_run_id and matching_request_json is not null) then
  raise exception using errcode='55000',message='BPAY_NEXT_EXCLUSIONS_CLOSED';end if;
 if v_run.status<>'PREPARING' or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_ALREADY_SEALED';end if;
 select greatest(1,(count(*)+99)/100) into v_total from private.bpay_next_run_selection_exclusion where run_id=p_run_id;
 select coalesce(jsonb_agg(work_id order by work_id) filter(where work_id is not null),'[]'),
   coalesce(jsonb_agg(candidate_id order by candidate_id) filter(where candidate_id is not null),'[]')
 into v_work,v_candidates from private.bpay_next_run_selection_exclusion where run_id=p_run_id and page_no=p_page_no;
 if p_page_no<>v_run.exclusion_page_count+1 or p_page_no>v_total
  or v_n<>(select count(*) from private.bpay_next_run_selection_exclusion where run_id=p_run_id and page_no=p_page_no)
  or (select coalesce(jsonb_agg((value#>>'{}')::uuid order by (value#>>'{}')::uuid),'[]') from jsonb_array_elements(p_request->'excluded_work_ids')) is distinct from v_work
  or (select coalesce(jsonb_agg((value#>>'{}')::uuid order by (value#>>'{}')::uuid),'[]') from jsonb_array_elements(p_request->'excluded_candidate_ids')) is distinct from v_candidates then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
 v_result:=jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
   'complete',p_page_no=v_total,'replay',false);
 insert into private.bpay_next_selection_exclusion_page(run_id,page_no,request_json,captured_result_json)
  values(p_run_id,p_page_no,p_request,v_result);
 update private.bpay_next_pay_run set exclusion_page_count=p_page_no where id=p_run_id;
 return v_result;
end
$function$;
alter function private.bpay_next_exclusion_page_v1(uuid,bigint,jsonb) owner to postgres;
revoke all on function private.bpay_next_exclusion_page_v1(uuid,bigint,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_matching_page_v1(p_run_id uuid,p_page_no bigint,p_request jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
 v_body jsonb;v_previous private.bpay_next_selection_page%rowtype;v_after bigint;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id for update;
 if v_run.accepted_choice_descriptor is null or v_run.accepted_choice_descriptor->'intent'->>'mode'<>'ALL_MATCHING' then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
 select src.* into v_page from private.bpay_next_selection_page src where run_id=p_run_id and page_no=p_page_no;
 if found then
  if v_page.matching_request_json is distinct from p_request or v_page.captured_result_json is null then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';end if;
  return v_page.captured_result_json||jsonb_build_object('replay',true);
 end if;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_ALREADY_SEALED';end if;
 if v_run.exclusion_page_count<>(select greatest(1,(count(*)+99)/100) from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
  or exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=p_run_id and member.member_kind='WORK'
    and member.selection_mode='SUBSET' and (member.member_no>v_run.uploaded_choice_work_count or
      (member.skip_code is null and not exists(select 1 from private.bpay_next_work_choice choice
       where choice.run_id=p_run_id and choice.work_id=member.work_id and choice.selection_state='SEALED' and choice.segment_sealed_count is not null)))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE';end if;
 select src.* into v_previous from private.bpay_next_selection_page src
  where run_id=p_run_id and matching_request_json is not null order by page_no desc limit 1;
 if found and (v_previous.captured_result_json->'complete'='true'::jsonb
  or v_previous.captured_result_json->>'next_after_member_no' is distinct from p_request->>'after_member_no') then
  raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';end if;
 v_after:=case when v_run.uploaded_choice_work_count=0 then null else v_run.uploaded_choice_work_count end;
 if (p_request->>'after_member_no')::bigint is distinct from v_after or p_page_no<>v_run.selection_page_count+1 then
  raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';end if;
 select coalesce(jsonb_agg(jsonb_build_object('work_id',work_id,'expected_revision_id',expected_revision_id,'mode',selection_mode)
  order by member_no),'[]') into v_body from (
  select work_id,expected_revision_id,selection_mode,member_no from private.bpay_next_run_choice_member
   where run_id=p_run_id and member_kind='WORK' and member_no>v_run.uploaded_choice_work_count
   order by member_no limit 100) page;
 if exists(select 1 from private.bpay_next_run_choice_member member
  where member.run_id=p_run_id and member.member_kind='WORK' and member.member_no>v_run.uploaded_choice_work_count
    and member.member_no<=v_run.uploaded_choice_work_count+jsonb_array_length(v_body)
    and (member.selection_mode<>'ALL' or exists(select 1 from private.bpay_next_run_selection_exclusion exclusion
      where exclusion.run_id=p_run_id and (exclusion.work_id=member.work_id or exclusion.candidate_id=member.candidate_id)))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
 return private.bpay_next_ack_choice_work_page_v2(p_run_id,p_page_no,v_body,true,p_request);
end
$function$;
alter function private.bpay_next_matching_page_v1(uuid,bigint,jsonb) owner to postgres;
revoke all on function private.bpay_next_matching_page_v1(uuid,bigint,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_choice_inputs_page_v1(p_run_id uuid,p_after bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_rows jsonb;v_n integer;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
 if p_limit not between 1 and 100 or p_after<0 or v_run.accepted_choice_descriptor is null then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 select coalesce(jsonb_agg(jsonb_build_object('member_no',member_no::text,'candidate_id',candidate_id,
  'work_id',work_id,'expected_revision_id',expected_revision_id,'selection_mode',selection_mode,'skip_code',skip_code)
  order by member_no),'[]'),count(*) into v_rows,v_n from (
   select member_no,candidate_id,work_id,expected_revision_id,selection_mode,skip_code
   from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'
    and member_no>coalesce(p_after,0) order by member_no limit p_limit) page;
 return jsonb_build_object('header',jsonb_build_object('run_id',p_run_id,
  'uploaded_choice_work_count',v_run.uploaded_choice_work_count::text,'selection_page_count',v_run.selection_page_count::text,
  'selection_count',v_run.selection_count::text,'exclusion_page_count',v_run.exclusion_page_count::text),
  'rows',v_rows,'complete',not exists(select 1 from private.bpay_next_run_choice_member
    where run_id=p_run_id and member_kind='WORK' and member_no>coalesce(p_after,0)+v_n));
end
$function$;
alter function private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer) from public,anon,authenticated,service_role;

-- Private immutable receipt metadata. No public table triggers.
create or replace function private.bpay_next_selection_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if tg_op in ('UPDATE','DELETE','TRUNCATE') then
  raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';end if;
 return new;
end
$function$;
alter function private.bpay_next_selection_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_selection_receipt_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_selection_receipt_guard on private.bpay_next_selection_page;
create trigger bpay_next_selection_receipt_guard before update or delete on private.bpay_next_selection_page
 for each row execute function private.bpay_next_selection_receipt_guard_v1();
drop trigger if exists bpay_next_selection_receipt_truncate_guard on private.bpay_next_selection_page;
create trigger bpay_next_selection_receipt_truncate_guard before truncate on private.bpay_next_selection_page
 for each statement execute function private.bpay_next_selection_receipt_guard_v1();


create or replace function private.bpay_next_ack_choice_work_page_v2(
 p_run_id uuid,p_page_no bigint,p_body jsonb,p_explicit boolean,p_matching_request jsonb
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_page private.bpay_next_selection_page%rowtype;
 v_member private.bpay_next_run_choice_member%rowtype;v_row jsonb;v_work uuid;v_n integer;
 v_result jsonb;v_complete boolean;v_last bigint;
 v_index integer:=0;v_accepted bigint:=0;v_skipped jsonb:='[]';v_ids jsonb:='[]';
begin
 if p_run_id is null or p_page_no is null or p_page_no<1 or p_explicit is null
   or jsonb_typeof(p_body) is distinct from 'array' or jsonb_array_length(p_body) not between 0 and 100
   or (jsonb_array_length(p_body)=0 and p_matching_request is null)
   or octet_length(p_body::text)>32768 then
  raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id for update;
 if v_run.accepted_choice_descriptor is null then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_MISSING';end if;
 v_n:=jsonb_array_length(p_body);
 for v_row in select value from jsonb_array_elements(p_body) loop
  if p_explicit then
   if jsonb_typeof(v_row) is distinct from 'object'
     or not(v_row ?& array['work_id','expected_revision_id','mode'])
     or v_row-array['work_id','expected_revision_id','mode']<>'{}'::jsonb
     or jsonb_typeof(v_row->'work_id') is distinct from 'string'
     or jsonb_typeof(v_row->'expected_revision_id') is distinct from 'string'
     or v_row->>'mode' not in ('ALL','SUBSET') then
    raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
   v_work:=(v_row->>'work_id')::uuid;
  else
   if jsonb_typeof(v_row) is distinct from 'string' then
    raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_WORK_ID_NOT_STRING';end if;
   v_work:=(v_row#>>'{}')::uuid;
  end if;
  if v_ids ? v_work::text then
   raise exception using errcode='22023',message='BPAY_NEXT_SELECTION_PAGE_INPUT_INVALID';end if;
  v_ids:=v_ids||jsonb_build_array(v_work);
 end loop;
 select src.* into v_page from private.bpay_next_selection_page src where run_id=p_run_id and page_no=p_page_no;
 if found then
  if v_page.matching_request_json is distinct from p_matching_request
    or v_page.work_ids_json is distinct from v_ids
    or (p_explicit and v_page.choices_request_json is distinct from p_body)
    or (not p_explicit and v_page.choices_request_json is not null) then
   raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_REPLAY_CONFLICT';end if;
  if v_page.captured_result_json is not null then
   return v_page.captured_result_json||jsonb_build_object('replay',true);end if;
  return jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
    'selection_count',v_run.selection_count::text,'selected_candidate_count',v_run.selected_candidate_count::text,
    'skipped',v_page.skipped_json,'replay',true);
 end if;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN'
   or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_ALREADY_SEALED';end if;
 if p_page_no<>v_run.selection_page_count+1 then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_PAGE_NOT_APPENDABLE';end if;
 for v_row in select value from jsonb_array_elements(p_body) loop
  v_index:=v_index+1;
  v_work:=case when p_explicit then (v_row->>'work_id')::uuid else (v_row#>>'{}')::uuid end;
  select src.* into v_member from private.bpay_next_run_choice_member src
    where run_id=p_run_id and member_kind='WORK' and member_no=v_run.uploaded_choice_work_count+v_index;
  if not found or v_member.work_id is distinct from v_work
    or (p_explicit and ((v_row->>'expected_revision_id')::uuid is distinct from v_member.expected_revision_id
      or v_row->>'mode' is distinct from v_member.selection_mode))
    or (not p_explicit and v_member.selection_mode<>'ALL') then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
  if v_member.skip_code is not null then
   v_skipped:=v_skipped||jsonb_build_array(jsonb_build_object('work_id',v_member.work_id,'code',v_member.skip_code));
  else
   if not exists(select 1 from private.bpay_next_run_selection s
     join private.bpay_next_work_choice choice on choice.run_id=s.run_id and choice.work_id=s.work_id
     where s.run_id=p_run_id and s.work_id=v_work and s.candidate_id=v_member.candidate_id
       and s.selection_no=v_run.uploaded_selection_count+v_accepted+1
       and choice.expected_revision_id=v_member.expected_revision_id and choice.selection_mode=v_member.selection_mode) then
    raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH';end if;
   if v_member.selection_mode='ALL' then perform private.bpay_next_materialise_choice_segments_v1(p_run_id,v_work);end if;
   v_accepted:=v_accepted+1;
  end if;
 end loop;
 v_result:=jsonb_build_object('run_id',p_run_id,'page_no',p_page_no::text,'item_count',v_n::text,
   'selection_count',v_run.selection_count::text,'selected_candidate_count',v_run.selected_candidate_count::text,
   'skipped',v_skipped,'replay',false);
 if p_matching_request is not null then
  v_last:=v_run.uploaded_choice_work_count+v_n;
  v_complete:=not exists(select 1 from private.bpay_next_run_choice_member
    where run_id=p_run_id and member_kind='WORK' and member_no>v_last);
  v_result:=v_result||jsonb_build_object('complete',v_complete,'next_after_member_no',
    case when v_complete then null else v_last::text end);
 end if;
 insert into private.bpay_next_selection_page(run_id,page_no,work_ids_json,first_selection_no,item_count,
   choices_request_json,skipped_json,matching_request_json,captured_result_json)
 values(p_run_id,p_page_no,v_ids,v_run.uploaded_selection_count+1,v_n,
   case when p_explicit then p_body else null end,v_skipped,p_matching_request,v_result);
 update private.bpay_next_pay_run set selection_page_count=p_page_no,
   uploaded_choice_work_count=uploaded_choice_work_count+v_n,
   uploaded_selection_count=uploaded_selection_count+v_accepted where id=p_run_id;
 return v_result;
end
$function$;

create or replace function private.bpay_next_ack_choice_work_page_v1(
 p_run_id uuid,p_page_no bigint,p_body jsonb,p_explicit boolean
) returns jsonb language sql security invoker set search_path=pg_catalog,private
as $function$
 select private.bpay_next_ack_choice_work_page_v2(p_run_id,p_page_no,p_body,p_explicit,null)
$function$;

create or replace function private.bpay_next_seal_selection_v1(
  p_run_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_selected record;
begin
  if p_run_id is null or p_expected_pages is null or p_expected_count is null
     or p_expected_pages<0 or p_expected_count<0
     or (p_expected_count>0 and p_expected_pages=0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SELECTION_SEAL_INPUT_INVALID';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.selection_page_count<>p_expected_pages
     or v_run.selection_count<>p_expected_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  if v_run.selection_state='SEALED' then
    return jsonb_build_object('run_id',p_run_id,'sealed',true,
      'selection_count',v_run.selection_count,'replay',true);
  end if;
  -- A case-only run contains genuine selected case components, not fictitious
  -- WORK rows. The root-locked seal counter is maintained once per Candidate
  -- by0330; checking it does not scan every Candidate at final confirmation.
  if p_expected_count=0 and (v_run.selected_candidate_count<1
     or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count) then
    raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_CASE_ONLY_NOT_SEALED';
  end if;
  if v_run.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_SEAL_STATUS_INVALID';
  end if;
  if v_run.accepted_choice_descriptor is not null then
    if exists(select 1 from private.bpay_next_work_choice where run_id=p_run_id and segment_sealed_count is null)
      or v_run.uploaded_selection_count<>v_run.selection_count
      or v_run.uploaded_choice_work_count<>(select count(*) from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK')
      or v_run.exclusion_page_count<>(select greatest(1,(count(*)+99)/100) from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
      or exists(select 1 from private.bpay_next_run_choice_member member
        join private.bpay_next_selection_candidate selected on selected.run_id=member.run_id and selected.candidate_id=member.candidate_id
        where member.run_id=p_run_id and member.member_kind='CASE_COMPONENT' and not exists(
          select 1 from private.bpay_next_case_selection_item item
          join private.bpay_next_case_selection header on header.run_id=item.run_id and header.candidate_id=item.candidate_id and header.selection_revision=item.selection_revision
          where item.run_id=member.run_id and item.candidate_id=member.candidate_id and header.status='SEALED'
            and item.case_component_id=member.case_component_id and item.is_selected=member.selected))
      or exists(select 1 from private.bpay_next_run_selection selected
        join private.bpay_next_work_choice choice on choice.run_id=selected.run_id and choice.work_id=selected.work_id
        join private.bpay_next_work_revision revision on revision.work_id=choice.work_id and revision.id=choice.expected_revision_id
        where selected.run_id=p_run_id and revision.source_kind in ('SOURCE','PROTECTED') and not exists(
          select 1 from private.bpay_next_run_work_query_snapshot snapshot where snapshot.run_id=p_run_id and snapshot.work_id=choice.work_id
            and snapshot.expected_revision_id=choice.expected_revision_id and snapshot.physical_timesheet_id=revision.physical_timesheet_id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE';
    end if;
  else
  -- Owner rule B: capture only the existing ticked expected revisions.
  -- Exact sealed replay above never refreshes settings or query state.
  perform private.weekly_source_pay_query_admit_v2();
  update private.bpay_next_selection_candidate selected
    set target_pay_channel=upper(candidate.pay_method),
        target_umbrella_id=candidate.umbrella_id,
        target_umbrella_vat_chargeable=umbrella.vat_chargeable,
        min_take_home_wtd=candidate.min_take_home_wtd
    from public.candidates candidate left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
    where selected.run_id=p_run_id and selected.candidate_id=candidate.id;
  for v_selected in
    select choice.work_id,choice.expected_revision_id,revision.physical_timesheet_id
      from private.bpay_next_run_selection selected
      join private.bpay_next_work_choice choice on choice.run_id=selected.run_id and choice.work_id=selected.work_id
      join private.bpay_next_work work on work.id=selected.work_id
      join private.bpay_next_work_revision revision on revision.work_id=choice.work_id and revision.id=choice.expected_revision_id
      where selected.run_id=p_run_id and work.work_kind='SOURCE' and revision.source_kind in ('SOURCE','PROTECTED')
      order by selected.selection_no
  loop
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_selected.work_id,v_selected.expected_revision_id,v_selected.physical_timesheet_id,
        private.weekly_source_pay_query_gate_v1(v_selected.physical_timesheet_id),pg_catalog.transaction_timestamp());
  end loop;
  end if;
  update private.bpay_next_pay_run
    set selection_state='SEALED',review_revision=review_revision+1
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'sealed',true,
    'selection_count',v_run.selection_count,'replay',false);
end
$function$;

create or replace function private.bpay_next_preparation_args_v1(p_args jsonb,p_keys text[])
returns void language plpgsql security invoker
set search_path=pg_catalog
as $function$
declare v_key text;v_type text;v_text text;
begin
  if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
    or pg_catalog.octet_length(p_args::text)>32768
    or (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.cardinality(p_keys)
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(p_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  foreach v_key in array p_keys loop
    v_type:=pg_catalog.jsonb_typeof(p_args->v_key);v_text:=p_args->>v_key;
    if v_key='descriptor' then
      if v_type is distinct from 'object' or octet_length((p_args->v_key)::text)>32768 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    elsif v_key='accepted_inputs' then
      if p_args->v_key is distinct from 'true'::jsonb then
       raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    elsif v_key in ('excluded_work_ids','excluded_candidate_ids') then
      if v_type is distinct from 'array' or jsonb_array_length(p_args->v_key)>100 then
       raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    elsif v_key in ('choices','approved_line_ids','component_ids','selected','segment_keys') then
      if v_type is distinct from 'array' or pg_catalog.jsonb_array_length(p_args->v_key) not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='limit' then
      if v_type is distinct from 'number' or v_text!~'^[0-9]{1,3}$' or v_text::integer not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key like 'after_%' and v_type='null' then
      continue;
    elsif v_type is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    elsif v_key like '%_id' then
      if v_text!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key in ('pay_date','after_week') then
      if v_text!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or not pg_catalog.isfinite(v_text::date)
        or v_text::date::text<>v_text then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='pay_channel' then
      if v_text not in ('PAYE','UMBRELLA') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    elsif v_key='after_bucket' then
      if v_text not in ('DAY','NIGHT','SAT','SUN','BH') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    else
      if v_text!~'^(0|[1-9][0-9]{0,18})$' or v_text::numeric>9223372036854775807
        or (v_key in ('page_no','selection_revision','review_revision') and v_text='0') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
    end if;
  end loop;
end
$function$;

create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  if p_action='CREATE' and p_args ? 'descriptor' then
    -- Authenticate before admission; preserve Source -> module lock order.
    if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users u
        where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin' for share) then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    perform private.bpay_next_preparation_args_v1(p_args,array['run_id','command_id','descriptor']);
    if not exists(select 1 from private.bpay_next_pay_run where id=(p_args->>'run_id')::uuid) then
      begin perform private.weekly_source_pay_query_admit_v2();
      exception when lock_not_available then null;end;end if;
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then case when p_args ? 'descriptor' then array['run_id','command_id','descriptor'] when p_args ? 'pay_date' then array['run_id','pay_date'] else array['run_id'] end
    when 'EXCLUSION_PAGE' then array['run_id','page_no','excluded_work_ids','excluded_candidate_ids']
    when 'MATCHING_PAGE' then array['run_id','page_no','after_member_no']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
    when 'SEGMENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','request_id','segment_keys']
    when 'SEGMENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'COMPONENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'CASE_PAGE' then array['run_id','candidate_id','selection_revision','page_no','request_id','component_ids','selected']
    when 'CASE_SEAL' then array['run_id','candidate_id','selection_revision','expected_pages','expected_items']
    when 'SEAL_ENQUEUE' then array['run_id','command_id','expected_pages','expected_count']
    when 'REVIEW' then array['run_id']
    when 'CONFIRM' then array['run_id','review_revision'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_run:=(p_args->>'run_id')::uuid;v_work:=(p_args->>'work_id')::uuid;
  v_revision:=(p_args->>'expected_revision_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  if p_action in ('EXCLUSION_PAGE','MATCHING_PAGE') then
    perform private.bpay_next_selection_page_authority_v1(v_run,p_actor_user_id);
  end if;
  if p_action in ('COMPONENT_PAGE','CASE_PAGE') then
    for v_entry in select value from pg_catalog.jsonb_array_elements(
      p_args->case when p_action='CASE_PAGE' then 'component_ids' else 'approved_line_ids' end) loop
      if pg_catalog.jsonb_typeof(v_entry) is distinct from 'string'
        or (v_entry#>>'{}')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      v_ids:=pg_catalog.array_append(v_ids,(v_entry#>>'{}')::uuid);
    end loop;
    if p_action='CASE_PAGE' then
      if pg_catalog.jsonb_array_length(p_args->'selected')<>pg_catalog.cardinality(v_ids) then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      for v_entry in select value from pg_catalog.jsonb_array_elements(p_args->'selected') loop
        if pg_catalog.jsonb_typeof(v_entry) is distinct from 'boolean' then
          raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
        end if;
        v_selected:=pg_catalog.array_append(v_selected,(v_entry#>>'{}')::boolean);
      end loop;
    end if;
  end if;
  case p_action
  when 'CREATE' then
    if p_args ? 'descriptor' then
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,null,p_args->'descriptor',(p_args->>'command_id')::uuid);
    else
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    end if;
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text,'official_pay_date',v_header.pay_date::text);
    if p_args ? 'descriptor' then
      v_output:=v_output||jsonb_build_object('command_id',v_result->>'command_id',
        'agency_sequence',v_result->'agency_sequence','choice_work_count',v_result->>'choice_work_count',
        'choice_case_count',v_result->>'choice_case_count','empty',v_result->'empty','replay',v_result->'replay');
    end if;
  when 'EXCLUSION_PAGE' then
    v_output:=private.bpay_next_exclusion_page_v1(v_run,(p_args->>'page_no')::bigint,p_args);
  when 'MATCHING_PAGE' then
    v_output:=private.bpay_next_matching_page_v1(v_run,(p_args->>'page_no')::bigint,p_args);
  when 'WORK_PAGE' then
    v_output:=private.bpay_next_append_work_choices_v1(v_run,(p_args->>'page_no')::bigint,p_args->'choices');
  when 'COMPONENT_PAGE' then
    v_output:=private.bpay_next_append_component_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,v_ids);
  when 'SEGMENT_PAGE' then
    if exists(select 1 from jsonb_array_elements(p_args->'segment_keys') item where jsonb_typeof(item)<>'string') then
      raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_SELECTION_KEY_INVALID';
    end if;
    v_output:=private.bpay_next_append_segment_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,
      (p_args->>'request_id')::uuid,array(select value#>>'{}' from jsonb_array_elements(p_args->'segment_keys')));
  when 'SEGMENT_SEAL' then
    v_output:=private.bpay_next_seal_segment_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'COMPONENT_SEAL' then
    v_output:=private.bpay_next_seal_component_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'CASE_PAGE' then
    v_result:=private.bpay_next_append_case_selection_page_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'page_no')::bigint,(p_args->>'request_id')::uuid,v_ids,v_selected);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no',
      'item_count',v_result->>'item_count','replay',v_result->'replay');
  when 'CASE_SEAL' then
    v_result:=private.bpay_next_seal_case_selection_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_items')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed',
      'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay');
  when 'SEAL_ENQUEUE' then
    perform private.bpay_next_seal_selection_v1(v_run,(p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
    v_result:=private.bpay_next_enqueue_preparation_v1(v_run,(p_args->>'command_id')::uuid);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id',
      'agency_sequence',v_result->>'agency_sequence','replay',v_result->'replay');
  when 'REVIEW' then
    v_result:=private.bpay_next_review_simple_run_v1(v_run);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason',
      'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready',
      'replay',coalesce(v_result->'replay','false'::jsonb));
  when 'CONFIRM' then
    v_result:=private.bpay_next_confirm_simple_run_v1(v_run,(p_args->>'review_revision')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase',
      'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts');
  end case;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';
  end if;
  return v_output;
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit'] else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;
alter function private.bpay_next_ack_choice_work_page_v2(uuid,bigint,jsonb,boolean,jsonb) owner to postgres;
revoke all on function private.bpay_next_ack_choice_work_page_v2(uuid,bigint,jsonb,boolean,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_ack_choice_work_page_v1(uuid,bigint,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_ack_choice_work_page_v1(uuid,bigint,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_args_v1(jsonb,text[]) owner to postgres;
revoke all on function private.bpay_next_preparation_args_v1(jsonb,text[]) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
