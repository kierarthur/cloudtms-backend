-- Owner-only, first confirmed external CSV settlement. No provider call.
-- Receipt persists before bounded posting, using the existing command order.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_receive_simple_csv_settlement_v1(
  p_command_id uuid,p_transfer_id uuid,p_receipt_id text,
  p_amount numeric,p_occurred_at timestamptz,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_actor public.tms_users%rowtype;
  v_receipt private.bpay_next_transfer_outcome%rowtype;
  v_request private.bpay_next_outcome_request%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_transfer_id is null or p_receipt_id is null
     or pg_catalog.octet_length(p_receipt_id) not between 1 and 256
     or p_amount is null or p_amount<=0
     or p_amount<>pg_catalog.round(p_amount,2)
     or p_occurred_at is null or not pg_catalog.isfinite(p_occurred_at)
     or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_SETTLEMENT_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users
    where id=p_actor_user_id for share;
  if v_actor.is_active is not true
     or (v_actor.payment_authoriser is not true
         and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_SETTLEMENT_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_receipt from private.bpay_next_transfer_outcome
    where receipt_id=p_receipt_id;
  if found then
    if v_receipt.transfer_id<>p_transfer_id or v_receipt.outcome_kind<>'SETTLED'
       or v_receipt.whole_transfer_amount<>p_amount
       or v_receipt.occurred_at_utc<>p_occurred_at then
      raise exception using errcode='23514',message='BPAY_NEXT_SETTLEMENT_RECEIPT_CONFLICT';
    end if;
    select * into strict v_request from private.bpay_next_outcome_request
      where outcome_id=v_receipt.id;
    return pg_catalog.jsonb_build_object('command_id',v_request.command_id,
      'outcome_id',v_receipt.id,'replay',true,
      'posting_complete',v_request.posting_complete);
  end if;
  if exists(select 1 from private.bpay_next_outcome_request
            where command_id=p_command_id or transfer_id=p_transfer_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_SETTLEMENT_COMMAND_CONFLICT';
  end if;
  if v_transfer.status not in ('ISSUED_CSV','UNKNOWN')
     or v_transfer.destination_rail<>'CSV'
     or v_transfer.cash_amount<>p_amount
     or not exists(select 1 from private.bpay_next_csv_instruction i
                   where i.transfer_id=p_transfer_id and i.cash_amount=p_amount)
     or v_worker.target_pay_channel<>'PAYE' then
    raise exception using errcode='55000',message='BPAY_NEXT_SETTLEMENT_NOT_ELIGIBLE';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CSV_SETTLEMENT');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer_outcome
    (transfer_id,receipt_id,outcome_kind,whole_transfer_amount,occurred_at_utc)
    values(p_transfer_id,p_receipt_id,'SETTLED',p_amount,p_occurred_at)
    returning * into v_receipt;
  insert into private.bpay_next_outcome_request
    (command_id,outcome_id,transfer_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,v_receipt.id,p_transfer_id,v_worker.id,
      v_worker.candidate_id,p_actor_user_id);
  update private.bpay_next_transfer set status='SETTLED' where id=p_transfer_id;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,
    'outcome_id',v_receipt.id,'sequence',v_sequence,'replay',false,'posting_complete',false);
end
$function$;

create or replace function private.bpay_next_claim_simple_outcome_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_nonce uuid;
  v_owner bigint;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_OUTCOME_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j
    where j.id=p_job_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind not in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT')
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job j
               where j.candidate_id=v_candidate and j.command_sequence<v_job.command_sequence
                 and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1
    where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,
    lease_nonce=v_nonce,lease_until_utc=pg_catalog.clock_timestamp()+
      pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce; owner_epoch:=v_owner; return next;
end
$function$;

create or replace function private.bpay_next_post_simple_outcome_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_run uuid;
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_outcome_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_line private.bpay_next_run_line%rowtype;
  v_hold private.bpay_next_hold%rowtype;
  v_member record;
  v_cursor bigint;
  v_count bigint;
  v_seen integer:=0;
  v_original uuid;
  v_effect uuid;
  v_return_cash private.bpay_next_return_cash%rowtype;
  v_internal private.bpay_next_internal_receipt%rowtype;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_OUTCOME_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate
    from private.bpay_next_job j
    join private.bpay_next_outcome_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and r.candidate_id=j.candidate_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_outcome_request
    where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=v_request.transfer_id for update;
  if exists(select 1 from private.bpay_next_run_worker w
            where w.id=v_request.run_worker_id and w.status in ('CANCELLING','CANCELLED')) then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_CANCELLED_GROUP';
  end if;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.module_epoch<>v_epoch
     or v_transfer.candidate_id<>v_candidate
     or v_transfer.run_worker_id<>v_request.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.job_kind='INTERNAL_SETTLEMENT' then
    select * into strict v_internal from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
    if v_request.outcome_id is not null or v_internal.command_id<>v_job.command_id
       or v_internal.transfer_id<>v_transfer.id or v_internal.run_worker_id<>v_request.run_worker_id
       or v_internal.candidate_id<>v_candidate or v_internal.projection_id<>v_transfer.projection_id
       or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0
       or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_OUTCOME_SCOPE_INVALID';
    end if;
  elsif v_transfer.execution_kind<>'BANK' or v_request.outcome_id is null or v_request.internal_receipt_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_BANK_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.posting_complete then
    return pg_catalog.jsonb_build_object('done',true,'replay',true,
      'posted_members',v_request.posted_member_count);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'MEMBERS'
     or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_job.job_kind='CSV_SETTLEMENT' and v_transfer.status not in ('SETTLED','RETURNED'))
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and v_transfer.status<>'INTERNAL_PROCESSING')
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_worker_control
       where candidate_id=v_candidate and active_owner_epoch=p_owner_epoch))
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
               and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_POST_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('done',false,'replay',true,
      'cursor',v_job.cursor_key,'posted_members',v_request.posted_member_count);
  end if;
  v_cursor:=p_expected_cursor; v_count:=v_request.posted_member_count;
  if v_transfer.return_cash_id is not null then
    select * into strict v_return_cash from private.bpay_next_return_cash
      where id=v_transfer.return_cash_id for update;
    if v_transfer.member_count<>1 or v_return_cash.candidate_id<>v_candidate
       or v_return_cash.original_transfer_id<>v_transfer.original_transfer_id
       or v_return_cash.amount_held<v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CASH_HOLD_MISMATCH';
    end if;
  end if;
  for v_member in select * from private.bpay_next_transfer_member
    where transfer_id=v_transfer.id and (v_cursor is null or member_no>v_cursor)
    order by member_no limit p_limit
  loop
    if v_member.subject_kind='CASH_REISSUE' then
      if v_transfer.return_cash_id is null
         or v_member.signed_cash_contribution<>v_transfer.cash_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_MEMBER_MISMATCH';
      end if;
      update private.bpay_next_return_cash
        set amount_held=amount_held-v_transfer.cash_amount,
            amount_reissued_paid=amount_reissued_paid+v_transfer.cash_amount
        where id=v_return_cash.id;
    elsif v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_PAYROLL_MEMBER_FORBIDDEN';
    elsif v_member.subject_kind='WORK' then
      select * into strict v_line from private.bpay_next_run_line where id=v_member.run_line_id;
      if v_line.run_worker_id<>v_request.run_worker_id
         or v_line.target_pay_channel<>'PAYE'
         or v_line.frozen_inc_vat<>v_member.signed_cash_contribution then
        raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_FROZEN_MEMBER_MISMATCH';
      end if;
      if v_line.source_consumed_ex_vat>0 then
        select * into strict v_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
        if v_hold.status<>'ACTIVE' or v_hold.work_id<>v_line.work_id
           or v_hold.component_key<>v_line.component_key
           or v_hold.source_reserved_ex_vat<>v_line.source_consumed_ex_vat
           or v_hold.target_amount_ex_vat<>v_line.frozen_ex_vat
           or v_hold.target_amount_vat<>v_line.frozen_vat
           or v_hold.target_amount_inc_vat<>v_line.frozen_inc_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_HOLD_MISMATCH';
        end if;
        select original_timesheet_id into strict v_original from private.bpay_next_work where id=v_line.work_id;
        insert into private.bpay_next_financial_effect
          (operation_id,operation_item_id,effect_kind,candidate_id,work_id,component_key,
           original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
           target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_job.command_id,v_hold.id,'PAYROLL_SETTLED',v_candidate,
            v_line.work_id,v_line.component_key,v_original,v_transfer.id,
            v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,
            v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
          returning id into v_effect;
        update private.bpay_next_run_worker
          set realised_effect_count=realised_effect_count+1
          where id=v_request.run_worker_id;
        update private.bpay_next_position
          set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
              held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,
              held_target_vat=held_target_vat-v_hold.target_amount_vat,
              held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,
              realised_source_ex_vat=realised_source_ex_vat+v_hold.source_reserved_ex_vat,
              realised_target_ex_vat=realised_target_ex_vat+v_hold.target_amount_ex_vat,
              realised_target_vat=realised_target_vat+v_hold.target_amount_vat,
              realised_target_inc_vat=realised_target_inc_vat+v_hold.target_amount_inc_vat,
              updated_at_utc=pg_catalog.transaction_timestamp()
          where work_id=v_line.work_id and component_key=v_line.component_key
            and source_basis_channel=v_line.source_pay_channel
            and held_source_ex_vat>=v_hold.source_reserved_ex_vat;
        if not found then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_POSITION_MISMATCH';
        end if;
        update private.bpay_next_hold set status='REALISED',finished_at_utc=pg_catalog.transaction_timestamp()
          where id=v_hold.id;
        perform private.bpay_next_reconcile_work_collection_v1(
          p_job_id,v_line.work_id,v_line.component_key,v_effect);
      end if;
    elsif v_member.subject_kind='CASE' then
      perform private.bpay_next_post_case_transfer_member_v1(p_job_id,v_transfer.id,v_member.member_no);
    elsif v_member.subject_kind<>'NET_ADJUSTMENT' then
      raise exception using errcode='55000',message='BPAY_NEXT_SIMPLE_OUTCOME_CASE_NOT_SUPPORTED';
    end if;
    v_cursor:=v_member.member_no; v_count:=v_count+1; v_seen:=v_seen+1;
  end loop;
  if v_seen=0 or v_count>v_transfer.member_count then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_MEMBER_COUNT_INVALID';
  end if;
  update private.bpay_next_outcome_request
    set posted_member_count=v_count,posting_complete=(v_count=v_transfer.member_count)
    where command_id=v_job.command_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate;
  if v_count=v_transfer.member_count then
    if exists(select 1 from private.bpay_next_transfer_member
              where transfer_id=v_transfer.id and member_no>v_cursor) then
      raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_EXTRA_MEMBER';
    end if;
    if v_job.job_kind='INTERNAL_SETTLEMENT' then
      perform private.bpay_next_complete_internal_zero_v1(p_job_id,v_cursor);
    else
      perform private.bpay_next_apply_week_outcome_v1(p_job_id,null);
    end if;
    perform private.bpay_next_complete_destination_leg_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='COMPLETE',cursor_key=v_cursor::text,
      lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  else
    update private.bpay_next_job set cursor_key=v_cursor::text where id=p_job_id;
  end if;
  return pg_catalog.jsonb_build_object('done',v_count=v_transfer.member_count,
    'replay',false,'cursor',v_cursor,'posted_members',v_count,'rows_visited',v_seen);
end
$function$;

drop trigger if exists bpay_next_outcome_immutable_v1 on private.bpay_next_transfer_outcome;
create trigger bpay_next_outcome_immutable_v1 before update or delete
on private.bpay_next_transfer_outcome for each row
execute function private.bpay_next_effect_immutable_v1();
alter function private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamptz,uuid) owner to postgres;
alter function private.bpay_next_claim_simple_outcome_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamptz,uuid),
private.bpay_next_claim_simple_outcome_job_v1(uuid,integer),
private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)
from public,anon,authenticated,service_role;
commit;
