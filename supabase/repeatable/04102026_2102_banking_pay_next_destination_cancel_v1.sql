-- L22 all-original-leg cancellation certificate. CHECK completes before any
-- WORK/CASE hold release; FINAL marks original legs in bounded pages. Existing
--0460 owns Candidate/header/order/lease and the financial release arithmetic.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_destination_cancel_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_b private.bpay_next_case_cancel_binding%rowtype;v_g private.bpay_next_destination_group%rowtype;
 v_j private.bpay_next_job%rowtype;v_w private.bpay_next_run_worker%rowtype;v_t private.bpay_next_transfer%rowtype;
 v_l private.bpay_next_destination_group_leg%rowtype;v_n integer:=0;v_cursor integer;v_check boolean;
begin
 if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_DELETE_FORBIDDEN';end if;
 select * into strict v_b from private.bpay_next_case_cancel_binding where command_id=new.command_id;
 select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=new.anchor_transfer_id;
 select * into strict v_w from private.bpay_next_run_worker where id=v_b.run_worker_id;
 select * into strict v_j from private.bpay_next_job where command_id=new.command_id and candidate_id=v_b.candidate_id;
 if v_g.run_worker_id<>v_b.run_worker_id or v_g.candidate_id<>v_b.candidate_id or v_g.stage<>'COMPLETE'
   or new.expected_leg_count<>v_g.expected_leg_count or v_g.sealed_leg_count<>v_g.expected_leg_count
   or v_j.job_kind<>'SIMPLE_CANCEL' or v_j.status<>'LEASED' or v_j.lease_nonce is null
   or v_j.lease_until_utc<=pg_catalog.clock_timestamp()
   or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' and owner_epoch=v_j.module_epoch)
   or not exists(select 1 from private.bpay_next_worker_control where candidate_id=v_b.candidate_id
      and active_owner_epoch=v_j.owner_epoch and pending_outcome_count=0)
   or not exists(select 1 from private.bpay_next_pay_run where id=v_w.run_id and status='DRAFT' and confirmed_at_utc is not null)
   or v_w.realised_effect_count<>0 or v_w.financial_resolution_count<>0
   or exists(select 1 from private.bpay_next_job where candidate_id=v_b.candidate_id and command_sequence<v_j.command_sequence and status<>'DONE') then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_OWNER_INVALID';end if;
 if tg_op='INSERT' then
  if v_b.status<>'REQUESTED' or v_b.stage<>'INTENT' or v_w.status<>'READY' or v_j.phase<>'NEW'
    or new.checked_leg_count<>0 or new.cancelled_leg_count<>0 or new.check_cursor is not null or new.cancel_cursor is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_INITIAL_INVALID';end if;
  return new;
 end if;
 if (new.command_id,new.anchor_transfer_id,new.expected_leg_count) is distinct from (old.command_id,old.anchor_transfer_id,old.expected_leg_count)
   or old.cancelled_leg_count=old.expected_leg_count then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_IDENTITY_INVALID';end if;
 v_check:=new.checked_leg_count<>old.checked_leg_count;
 if v_check then
  if v_b.status<>'REQUESTED' or v_b.stage<>'INTENT' or v_w.status<>'READY' or v_j.phase<>'NEW'
    or old.checked_leg_count=old.expected_leg_count or new.checked_leg_count<=old.checked_leg_count
    or new.checked_leg_count-old.checked_leg_count>100 or new.check_cursor is null
    or new.check_cursor<=coalesce(old.check_cursor,0)
    or (new.cancelled_leg_count,new.cancel_cursor) is distinct from (old.cancelled_leg_count,old.cancel_cursor) then
   raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_CHECK_PROGRESS_INVALID';end if;
 else
  if v_b.status<>'CANCELLING' or v_b.stage<>'FINAL' or v_w.status<>'CANCELLING' or v_j.phase<>'RELEASE'
    or old.checked_leg_count<>old.expected_leg_count or new.check_cursor is distinct from old.check_cursor
    or new.cancelled_leg_count<=old.cancelled_leg_count or new.cancelled_leg_count-old.cancelled_leg_count>100
    or new.cancel_cursor is null or new.cancel_cursor<=coalesce(old.cancel_cursor,0)
    or v_w.active_case_hold_count<>0 then
   raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_FINAL_PROGRESS_INVALID';end if;
 end if;
 -- Certify the ACTUAL bounded indexed cursor range/count, not a trusted job
 -- payload or arbitrary counter jump. At most101 rows detect a forged jump.
 for v_t in select * from private.bpay_next_transfer where run_worker_id=v_w.id
   and transfer_no>case when v_check then coalesce(old.check_cursor,0) else coalesce(old.cancel_cursor,0) end
   and transfer_no<=case when v_check then new.check_cursor else new.cancel_cursor end
   order by transfer_no limit 101 loop
  v_n:=v_n+1;if v_n>100 then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_RANGE_TOO_LARGE';end if;
  v_l:=private.bpay_next_destination_execution_leg_v1(v_t.id);
  if v_l.transfer_id is null or v_l.anchor_transfer_id<>new.anchor_transfer_id
    or v_t.original_transfer_id is not null or v_t.return_cash_id is not null
    or v_t.status not in (case when v_check then 'MEMBERS_READY' else 'CANCELLED' end)
    or v_t.account_approval_ref is not null
    or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_internal_receipt where transfer_id=v_t.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_RANGE_PROTECTED';end if;
  v_cursor:=v_t.transfer_no;
 end loop;
 if v_n<>(case when v_check then new.checked_leg_count-old.checked_leg_count else new.cancelled_leg_count-old.cancelled_leg_count end)
   or v_cursor is distinct from (case when v_check then new.check_cursor else new.cancel_cursor end) then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_RANGE_COUNT_INVALID';end if;
 return new;
end
$function$;

create or replace function private.bpay_next_check_destination_cancel_page_v1(p_job_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_j private.bpay_next_job%rowtype;v_b private.bpay_next_case_cancel_binding%rowtype;
 v_g private.bpay_next_destination_group%rowtype;v_p private.bpay_next_destination_cancel%rowtype;
 v_t private.bpay_next_transfer%rowtype;v_l private.bpay_next_destination_group_leg%rowtype;
 v_seen integer:=0;v_cursor integer;v_block text;v_more boolean;
begin
 if p_job_id is null or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_CANCEL_PAGE_INPUT_INVALID';end if;
 select * into strict v_j from private.bpay_next_job where id=p_job_id;
 select * into strict v_b from private.bpay_next_case_cancel_binding where command_id=v_j.command_id;
 select * into strict v_g from private.bpay_next_destination_group where run_worker_id=v_b.run_worker_id;
 insert into private.bpay_next_destination_cancel(command_id,anchor_transfer_id,expected_leg_count)
   values(v_j.command_id,v_g.anchor_transfer_id,v_g.expected_leg_count) on conflict(command_id) do nothing;
 select * into strict v_p from private.bpay_next_destination_cancel where command_id=v_j.command_id for update;
 if v_p.anchor_transfer_id<>v_g.anchor_transfer_id or v_p.expected_leg_count<>v_g.expected_leg_count
   or v_p.cancelled_leg_count<>0 or v_b.status<>'REQUESTED' or v_b.stage<>'INTENT' then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_CHECK_SCOPE_INVALID';end if;
 if v_p.checked_leg_count=v_p.expected_leg_count then
  return pg_catalog.jsonb_build_object('blocked_code',null,'rows_visited',0,'complete',true);end if;
 for v_t in select * from private.bpay_next_transfer where run_worker_id=v_b.run_worker_id
   and transfer_no>coalesce(v_p.check_cursor,0) order by transfer_no limit p_limit for update loop
  v_seen:=v_seen+1;
  v_l:=private.bpay_next_destination_execution_leg_v1(v_t.id);
  if v_l.transfer_id is null or v_l.anchor_transfer_id<>v_g.anchor_transfer_id
    or v_t.original_transfer_id is not null or v_t.return_cash_id is not null then
   v_block:='BPAY_NEXT_CANCEL_REISSUE_OR_NON_CANDIDATE_UNSUPPORTED';exit;end if;
  if v_t.status<>'MEMBERS_READY' then v_block:='BPAY_NEXT_CANCEL_TRANSFER_PROTECTED';exit;end if;
  if v_t.account_approval_ref is not null
    or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_internal_receipt where transfer_id=v_t.id) then
   v_block:='BPAY_NEXT_CANCEL_INSTRUCTION_OR_OUTCOME_PROTECTED';exit;end if;
  v_cursor:=v_t.transfer_no;
 end loop;
 if v_block is not null then
  return pg_catalog.jsonb_build_object('blocked_code',v_block,'rows_visited',v_seen,'complete',false);end if;
 if v_seen=0 or v_p.checked_leg_count+v_seen>v_p.expected_leg_count then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_CHECK_COUNT_INVALID';end if;
 select exists(select 1 from private.bpay_next_transfer where run_worker_id=v_b.run_worker_id and transfer_no>v_cursor) into v_more;
 if (not v_more) is distinct from (v_p.checked_leg_count+v_seen=v_p.expected_leg_count) then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_CHECK_COUNT_INVALID';end if;
 update private.bpay_next_destination_cancel set checked_leg_count=checked_leg_count+v_seen,check_cursor=v_cursor
   where command_id=v_j.command_id returning * into v_p;
 return pg_catalog.jsonb_build_object('blocked_code',null,'rows_visited',v_seen,'complete',v_p.checked_leg_count=v_p.expected_leg_count);
end
$function$;

create or replace function private.bpay_next_finish_destination_cancel_page_v1(p_job_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_j private.bpay_next_job%rowtype;v_b private.bpay_next_case_cancel_binding%rowtype;
 v_p private.bpay_next_destination_cancel%rowtype;v_t private.bpay_next_transfer%rowtype;
 v_l private.bpay_next_destination_group_leg%rowtype;v_seen integer:=0;v_cursor integer;v_more boolean;
begin
 if p_job_id is null or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_CANCEL_PAGE_INPUT_INVALID';end if;
 select * into strict v_j from private.bpay_next_job where id=p_job_id;
 select * into strict v_b from private.bpay_next_case_cancel_binding where command_id=v_j.command_id;
 select * into strict v_p from private.bpay_next_destination_cancel where command_id=v_j.command_id for update;
 if v_b.status<>'CANCELLING' or v_b.stage<>'FINAL' or v_p.checked_leg_count<>v_p.expected_leg_count then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_FINAL_SCOPE_INVALID';end if;
 if v_p.cancelled_leg_count=v_p.expected_leg_count then
  return pg_catalog.jsonb_build_object('rows_visited',0,'complete',true);end if;
 for v_t in select * from private.bpay_next_transfer where run_worker_id=v_b.run_worker_id
   and transfer_no>coalesce(v_p.cancel_cursor,0) order by transfer_no limit p_limit for update loop
  v_l:=private.bpay_next_destination_execution_leg_v1(v_t.id);
  if v_l.transfer_id is null or v_l.anchor_transfer_id<>v_p.anchor_transfer_id or v_t.status<>'MEMBERS_READY'
    or v_t.original_transfer_id is not null or v_t.return_cash_id is not null or v_t.account_approval_ref is not null
    or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_t.id)
    or exists(select 1 from private.bpay_next_internal_receipt where transfer_id=v_t.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_FINAL_PROTECTED';end if;
  update private.bpay_next_transfer set status='CANCELLED' where id=v_t.id;
  v_seen:=v_seen+1;v_cursor:=v_t.transfer_no;
 end loop;
 if v_seen=0 or v_p.cancelled_leg_count+v_seen>v_p.expected_leg_count then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_FINAL_COUNT_INVALID';end if;
 select exists(select 1 from private.bpay_next_transfer where run_worker_id=v_b.run_worker_id and transfer_no>v_cursor) into v_more;
 if (not v_more) is distinct from (v_p.cancelled_leg_count+v_seen=v_p.expected_leg_count) then
  raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CANCEL_FINAL_COUNT_INVALID';end if;
 update private.bpay_next_destination_cancel set cancelled_leg_count=cancelled_leg_count+v_seen,cancel_cursor=v_cursor
   where command_id=v_j.command_id returning * into v_p;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen,'complete',v_p.cancelled_leg_count=v_p.expected_leg_count);
end
$function$;

drop trigger if exists bpay_next_destination_cancel_guard_v1 on private.bpay_next_destination_cancel;
create trigger bpay_next_destination_cancel_guard_v1 before insert or update or delete on private.bpay_next_destination_cancel
 for each row execute function private.bpay_next_destination_cancel_guard_v1();
alter function private.bpay_next_destination_cancel_guard_v1() owner to postgres;
alter function private.bpay_next_check_destination_cancel_page_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_finish_destination_cancel_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_destination_cancel_guard_v1(),private.bpay_next_check_destination_cancel_page_v1(uuid,integer),
 private.bpay_next_finish_destination_cancel_page_v1(uuid,integer) from public,anon,authenticated,service_role;

commit;
