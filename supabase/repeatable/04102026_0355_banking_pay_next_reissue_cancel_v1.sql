-- Whole unpaid cash reissue cancellation. Original payroll/effects are not
-- cancelled. Exact returned-cash reservation is released to the same creditor.
-- Owner-only until closed service/caller integration and release proof.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_reissue_cancel_block_v1(p_transfer_id uuid)
returns text language plpgsql set search_path=pg_catalog,private
as $function$
declare v_transfer private.bpay_next_transfer%rowtype;
begin
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id;
  if v_transfer.original_transfer_id is null or v_transfer.return_cash_id is null
     or v_transfer.projection_id is not null or v_transfer.member_count<>1
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or v_transfer.beneficiary_kind<>'CANDIDATE' or v_transfer.beneficiary_id<>v_transfer.candidate_id
     or not exists(select 1 from private.bpay_next_transfer_member m
       where m.transfer_id=v_transfer.id and m.member_no=1 and m.subject_kind='CASH_REISSUE'
         and m.signed_cash_contribution=v_transfer.cash_amount) then
    return 'BPAY_NEXT_REISSUE_CANCEL_SCOPE_INVALID';
  end if;
  if v_transfer.status not in ('MEMBERS_READY','DRAFT','SCHEDULED')
     or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_transfer.id)
     or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_transfer.id) then
    return 'BPAY_NEXT_REISSUE_CANCEL_INSTRUCTION_PROTECTED';
  end if;
  return null;
end;
$function$;

create or replace function private.bpay_next_accept_reissue_cancel_v1(
  p_command_id uuid,p_transfer_id uuid,p_actor_user_id uuid
) returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_transfer private.bpay_next_transfer%rowtype;v_run uuid;v_actor public.tms_users%rowtype;
  v_prior private.bpay_next_reissue_cancel_request%rowtype;v_sequence bigint;v_block text;
begin
  if p_command_id is null or p_transfer_id is null or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_CANCEL_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where t.id=p_transfer_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_transfer.candidate_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_transfer.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users where id=p_actor_user_id for share;
  if v_actor.is_active is not true or
     (v_actor.payment_authoriser is not true and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_REISSUE_CANCEL_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_prior from private.bpay_next_reissue_cancel_request where command_id=p_command_id;
  if found then
    if v_prior.transfer_id<>p_transfer_id then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='REISSUE_CANCEL';
    return jsonb_build_object('sequence',v_sequence::text,'transfer_id',p_transfer_id,'phase',v_prior.status,'replay',true);
  end if;
  v_block:=private.bpay_next_reissue_cancel_block_v1(p_transfer_id);
  if v_block is not null then raise exception using errcode='55000',message=v_block;end if;
  if exists(select 1 from private.bpay_next_reissue_cancel_request where transfer_id=p_transfer_id and status='REQUESTED') then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_CANCEL_ALREADY_REQUESTED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'REISSUE_CANCEL');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_transfer.candidate_id,1);
  insert into private.bpay_next_reissue_cancel_request(command_id,transfer_id,return_cash_id,run_worker_id,candidate_id,actor_user_id,amount,status)
    values(p_command_id,v_transfer.id,v_transfer.return_cash_id,v_transfer.run_worker_id,v_transfer.candidate_id,p_actor_user_id,v_transfer.cash_amount,'REQUESTED');
  update private.bpay_next_command set status='SEALED',expected_member_count=1,sealed_at_utc=transaction_timestamp() where id=p_command_id;
  return jsonb_build_object('sequence',v_sequence::text,'transfer_id',p_transfer_id,'phase','REQUESTED','replay',false);
end;
$function$;

create or replace function private.bpay_next_claim_reissue_cancel_job_v1(p_job_id uuid,p_lease_seconds integer default 120)
returns table(lease_nonce uuid,owner_epoch bigint) language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_epoch bigint;v_run uuid;v_candidate uuid;v_nonce uuid;v_owner_epoch bigint;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_CANCEL_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate from private.bpay_next_job j
    join private.bpay_next_reissue_cancel_request r on r.command_id=j.command_id and r.candidate_id=j.candidate_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id where j.id=p_job_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for share;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  if v_job.job_kind<>'REISSUE_CANCEL' or v_job.module_epoch<>v_epoch or v_job.phase<>'NEW'
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED' and v_job.lease_until_utc>clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE';
  end if;
  if exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=v_candidate
     and earlier.command_sequence<v_job.command_sequence and earlier.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1
    where candidate_id=v_candidate returning active_owner_epoch into v_owner_epoch;
  v_nonce:=gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner_epoch,
    lease_nonce=v_nonce,lease_until_utc=clock_timestamp()+make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner_epoch;
  return next;
end;
$function$;

create or replace function private.bpay_next_apply_reissue_cancel_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_request private.bpay_next_reissue_cancel_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_cash private.bpay_next_return_cash%rowtype;
  v_run uuid;v_candidate uuid;v_epoch bigint;v_block text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_CANCEL_STEP_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate from private.bpay_next_job j
    join private.bpay_next_reissue_cancel_request r on r.command_id=j.command_id and r.candidate_id=j.candidate_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id where j.id=p_job_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_reissue_cancel_request where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  if v_job.job_kind<>'REISSUE_CANCEL' or v_job.module_epoch<>v_epoch then
    raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.status in ('CANCELLED','BLOCKED') then
    return jsonb_build_object('phase',v_request.status,'transfer_id',v_request.transfer_id,'blocked_code',v_request.blocked_code,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=clock_timestamp()
     or v_request.status<>'REQUESTED' then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_CANCEL_LEASE_STALE';
  end if;
  select * into strict v_transfer from private.bpay_next_transfer where id=v_request.transfer_id for update;
  select * into strict v_cash from private.bpay_next_return_cash where id=v_request.return_cash_id for update;
  if (v_transfer.return_cash_id,v_transfer.run_worker_id,v_transfer.candidate_id,v_transfer.cash_amount)
      is distinct from (v_request.return_cash_id,v_request.run_worker_id,v_candidate,v_request.amount)
     or v_transfer.original_transfer_id is distinct from v_cash.original_transfer_id then
    raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_FROZEN_SCOPE_CHANGED';
  end if;
  v_block:=private.bpay_next_reissue_cancel_block_v1(v_transfer.id);
  if v_block is null then
    if v_cash.amount_held<v_request.amount then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_RESERVATION_MISSING';
    end if;
    update private.bpay_next_return_cash set amount_held=amount_held-v_request.amount where id=v_cash.id;
    update private.bpay_next_transfer set status='CANCELLED' where id=v_transfer.id;
    update private.bpay_next_reissue_cancel_request set status='CANCELLED',completed_at_utc=clock_timestamp() where command_id=v_job.command_id;
    update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1 where candidate_id=v_candidate;
  else
    update private.bpay_next_reissue_cancel_request set status='BLOCKED',blocked_code=v_block,completed_at_utc=clock_timestamp() where command_id=v_job.command_id;
  end if;
  update private.bpay_next_job set status='DONE',phase=case when v_block is null then 'CANCELLED' else 'BLOCKED' end,
    lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  return jsonb_build_object('phase',case when v_block is null then 'CANCELLED' else 'BLOCKED' end,
    'transfer_id',v_request.transfer_id,'blocked_code',v_block,'replay',false);
end;
$function$;
alter function private.bpay_next_reissue_cancel_block_v1(uuid) owner to postgres;
alter function private.bpay_next_accept_reissue_cancel_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_claim_reissue_cancel_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_apply_reissue_cancel_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_reissue_cancel_block_v1(uuid),private.bpay_next_accept_reissue_cancel_v1(uuid,uuid,uuid),
  private.bpay_next_claim_reissue_cancel_job_v1(uuid,integer),private.bpay_next_apply_reissue_cancel_v1(uuid,uuid,bigint)
  from public,anon,authenticated,service_role;
commit;
