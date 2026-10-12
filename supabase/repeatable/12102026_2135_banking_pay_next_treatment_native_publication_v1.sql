-- WB-29 / BD155: complete native treatment publication; original earnings retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_treatment_approval_revision_v1(p_revision_id uuid)
returns uuid language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare r private.bpay_next_work_revision%rowtype; a private.bpay_next_work_revision%rowtype;
 pred private.bpay_next_work_revision%rowtype; pub private.bpay_next_publication%rowtype;
begin
 select x.* into r from private.bpay_next_work_revision x where x.id=p_revision_id;
 if r.id is null or r.approved_at_utc is null or r.sealed_at_utc is null then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_APPROVAL_UNAVAILABLE';end if;
 if r.payment_treatment_approval_revision_id is null then return r.id;end if;
 select x.* into a from private.bpay_next_work_revision x
  where x.id=r.payment_treatment_approval_revision_id and x.work_id=r.work_id;
 select x.* into pub from private.bpay_next_publication x where x.work_id=r.work_id and x.revision_id=r.id;
 select x.* into pred from private.bpay_next_work_revision x where x.id=pub.predecessor_revision_id and x.work_id=r.work_id;
 if a.id is null or a.id=r.id or a.payment_treatment_approval_revision_id is not null
  or a.approved_at_utc is null or a.sealed_at_utc is null or r.source_event_id is not null
  or (to_jsonb(r)-array['id','revision_no','source_event_id','approved_at_utc','sealed_at_utc','created_at_utc','payment_treatment_approval_revision_id']) is distinct from (to_jsonb(a)-array['id','revision_no','source_event_id','approved_at_utc','sealed_at_utc','created_at_utc','payment_treatment_approval_revision_id'])
  or pub.revision_no is distinct from r.revision_no
  or pred.id is null or pred.sealed_at_utc is null or pred.approved_at_utc is null
  or coalesce(pred.payment_treatment_approval_revision_id,pred.id) is distinct from a.id
  or pred.revision_no+1 is distinct from r.revision_no
  or not exists(select 1 from private.bpay_next_command c
   join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=pub.candidate_id
   join private.bpay_next_work w on w.id=pub.work_id and w.candidate_id=pub.candidate_id
   where c.id=pub.command_id and c.command_kind='POSITION_APPLY' and c.sealed_at_utc is not null
    and c.expected_member_count=1 and m.member_no=1) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_APPROVAL_UNAVAILABLE';end if;
 return a.id;
end
$function$;

create or replace function private.bpay_next_treatment_payload_assert_v1(p_revision_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare r private.bpay_next_work_revision%rowtype; a private.bpay_next_work_revision%rowtype;
begin
 select x.* into strict r from private.bpay_next_work_revision x where x.id=p_revision_id;
 if r.payment_treatment_approval_revision_id is null then return;end if;
 select x.* into a from private.bpay_next_work_revision x
  where x.id=r.payment_treatment_approval_revision_id and x.work_id=r.work_id;
 if a.id is null or a.id=r.id or a.payment_treatment_approval_revision_id is not null
  or a.sealed_at_utc is null or a.approved_at_utc is null or r.source_event_id is not null
  or r.revision_no<=a.revision_no
  or (to_jsonb(r)-array['id','revision_no','source_event_id','approved_at_utc','sealed_at_utc','created_at_utc','payment_treatment_approval_revision_id']) is distinct from (to_jsonb(a)-array['id','revision_no','source_event_id','approved_at_utc','sealed_at_utc','created_at_utc','payment_treatment_approval_revision_id']) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_SOURCE_CHANGED';end if;
 if exists(with original as (select to_jsonb(x)-array['id','revision_id','created_at_utc'] payload from private.bpay_next_approved_line x where x.revision_id=a.id), treatment as (select to_jsonb(x)-array['id','revision_id','created_at_utc'] payload from private.bpay_next_approved_line x where x.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_LINE_PAYLOAD_CHANGED';end if;
 if exists(with original as (select jsonb_build_array(l.component_key,to_jsonb(x)-array['id','approved_line_id']) payload from private.bpay_next_rate_detail x join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=a.id), treatment as (select jsonb_build_array(l.component_key,to_jsonb(x)-array['id','approved_line_id']) payload from private.bpay_next_rate_detail x join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_RATE_PAYLOAD_CHANGED';end if;
 if exists(with original as (select to_jsonb(x)-array['id','revision_id'] payload from private.bpay_next_rate_schedule x where x.revision_id=a.id), treatment as (select to_jsonb(x)-array['id','revision_id'] payload from private.bpay_next_rate_schedule x where x.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_SCHEDULE_PAYLOAD_CHANGED';end if;
 if exists(with original as (select jsonb_build_array(l.component_key,to_jsonb(x)-array['id','approved_line_id']) payload from private.bpay_next_shift_detail x join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=a.id), treatment as (select jsonb_build_array(l.component_key,to_jsonb(x)-array['id','approved_line_id']) payload from private.bpay_next_shift_detail x join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_SHIFT_PAYLOAD_CHANGED';end if;
 if exists(with original as (select jsonb_build_array(l.component_key,d.detail_no,to_jsonb(x)-array['id','shift_detail_id']) payload from private.bpay_next_break_detail x join private.bpay_next_shift_detail d on d.id=x.shift_detail_id join private.bpay_next_approved_line l on l.id=d.approved_line_id where l.revision_id=a.id), treatment as (select jsonb_build_array(l.component_key,d.detail_no,to_jsonb(x)-array['id','shift_detail_id']) payload from private.bpay_next_break_detail x join private.bpay_next_shift_detail d on d.id=x.shift_detail_id join private.bpay_next_approved_line l on l.id=d.approved_line_id where l.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_BREAK_PAYLOAD_CHANGED';end if;
 if r.sealed_at_utc is not null then
 if exists(with original as (select jsonb_build_array(l.component_key,d.detail_no,to_jsonb(x)-array['id','revision_id','approved_line_id','shift_detail_id']) payload from private.bpay_next_approved_segment x join private.bpay_next_approved_line l on l.id=x.approved_line_id join private.bpay_next_shift_detail d on d.id=x.shift_detail_id where x.revision_id=a.id), treatment as (select jsonb_build_array(l.component_key,d.detail_no,to_jsonb(x)-array['id','revision_id','approved_line_id','shift_detail_id']) payload from private.bpay_next_approved_segment x join private.bpay_next_approved_line l on l.id=x.approved_line_id join private.bpay_next_shift_detail d on d.id=x.shift_detail_id where x.revision_id=r.id)
  (select payload from original except all select payload from treatment)
  union all (select payload from treatment except all select payload from original)) then
  raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_SEGMENT_PAYLOAD_CHANGED';end if;
 end if;
end
$function$;

create or replace function private.bpay_next_stage_treatment_revision_v1(p_work_id uuid,p_expected_revision_id uuid)
returns uuid language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_work%rowtype; r private.bpay_next_work_revision%rowtype;
 v_root uuid;v_revision uuid:=gen_random_uuid();v_candidate uuid;
begin
 if p_work_id is null or p_expected_revision_id is null then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 -- W2 prelocks its entire finite Candidate/Work/Position scope before seed/clone.
 -- This helper stages only; original publisher owns seal, pointer and command.
 select x.candidate_id into strict v_candidate from private.bpay_next_work x where x.id=p_work_id;
 perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 select x.* into strict w from private.bpay_next_work x where x.id=p_work_id for update;
 select x.* into strict r from private.bpay_next_work_revision x where x.id=p_expected_revision_id and x.work_id=w.id for share;
 if w.approval_state is distinct from 'APPROVED' or w.current_revision_id is distinct from r.id
  or w.applied_revision_id is distinct from r.id or w.current_revision_no is distinct from r.revision_no
  or r.sealed_at_utc is null or r.approved_at_utc is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 v_root:=private.bpay_next_treatment_approval_revision_v1(r.id);
 insert into private.bpay_next_work_revision
 select (jsonb_populate_record(null::private.bpay_next_work_revision,to_jsonb(r)||jsonb_build_object(
  'id',v_revision,'revision_no',r.revision_no+1,'payment_treatment_approval_revision_id',v_root,
  'source_event_id',null,'approved_at_utc',null,'sealed_at_utc',null,'created_at_utc',transaction_timestamp()))).*;
 insert into private.bpay_next_approved_line
 select (jsonb_populate_record(null::private.bpay_next_approved_line,to_jsonb(x)||jsonb_build_object(
  'id',gen_random_uuid(),'revision_id',v_revision,'created_at_utc',transaction_timestamp()))).*
 from private.bpay_next_approved_line x where x.revision_id=r.id;
 insert into private.bpay_next_rate_detail
 select (jsonb_populate_record(null::private.bpay_next_rate_detail,to_jsonb(x)||jsonb_build_object(
  'id',gen_random_uuid(),'approved_line_id',n.id))).*
 from private.bpay_next_rate_detail x join private.bpay_next_approved_line o on o.id=x.approved_line_id
 join private.bpay_next_approved_line n on n.revision_id=v_revision and n.component_key=o.component_key where o.revision_id=r.id;
 insert into private.bpay_next_rate_schedule
 select (jsonb_populate_record(null::private.bpay_next_rate_schedule,to_jsonb(x)||jsonb_build_object(
  'id',gen_random_uuid(),'revision_id',v_revision))).* from private.bpay_next_rate_schedule x where x.revision_id=r.id;
 insert into private.bpay_next_shift_detail
 select (jsonb_populate_record(null::private.bpay_next_shift_detail,to_jsonb(x)||jsonb_build_object(
  'id',gen_random_uuid(),'approved_line_id',n.id))).*
 from private.bpay_next_shift_detail x join private.bpay_next_approved_line o on o.id=x.approved_line_id
 join private.bpay_next_approved_line n on n.revision_id=v_revision and n.component_key=o.component_key where o.revision_id=r.id;
 insert into private.bpay_next_break_detail
 select (jsonb_populate_record(null::private.bpay_next_break_detail,to_jsonb(x)||jsonb_build_object(
  'id',gen_random_uuid(),'shift_detail_id',nd.id))).*
 from private.bpay_next_break_detail x join private.bpay_next_shift_detail od on od.id=x.shift_detail_id
 join private.bpay_next_approved_line o on o.id=od.approved_line_id
 join private.bpay_next_approved_line n on n.revision_id=v_revision and n.component_key=o.component_key
 join private.bpay_next_shift_detail nd on nd.approved_line_id=n.id and nd.detail_no=od.detail_no where o.revision_id=r.id;
 perform private.bpay_next_treatment_payload_assert_v1(v_revision);
 return v_revision;
end
$function$;

alter function private.bpay_next_treatment_approval_revision_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_treatment_approval_revision_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_treatment_payload_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_treatment_payload_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_stage_treatment_revision_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_stage_treatment_revision_v1(uuid,uuid) from public,anon,authenticated,service_role;
commit;
