-- Genuine zero-cash accounting: an owned internal receipt, not a CSV or bank
-- outcome. Actual frozen member posting is the existing bounded0200 owner.
-- No history, provider, payroll reprice, principal inverse or cash-return row.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_internal_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_transfer private.bpay_next_transfer%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_command private.bpay_next_command%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
begin
  if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_RECEIPT_IMMUTABLE';end if;
  select * into strict v_transfer from private.bpay_next_transfer where id=new.transfer_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=new.run_worker_id;
  select * into strict v_projection from private.bpay_next_net_projection where id=new.projection_id;
  select * into strict v_command from private.bpay_next_command where id=new.command_id;
  v_leg:=private.bpay_next_destination_execution_leg_v1(new.transfer_id);
  if v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.cash_amount<>0 or v_transfer.member_cash_sum<>0 or v_transfer.member_count<1
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_transfer.run_worker_id<>v_worker.id or v_transfer.candidate_id<>v_worker.candidate_id
     or v_transfer.projection_id<>v_projection.id or v_projection.run_worker_id<>v_worker.id
     or new.candidate_id<>v_worker.candidate_id
     or (v_projection.cash_amount<>0 and (v_leg.transfer_id is null or v_leg.leg_kind<>'OWN'))
     or v_projection.retired_at_utc is not null or v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_worker.net_projection_revision<>v_projection.projection_no or v_worker.net_request_revision<>v_projection.projection_no
     or v_command.command_kind<>'INTERNAL_SETTLEMENT' or v_command.status<>'RECEIVED'
     or v_command.module_epoch is distinct from (select owner_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT')
     or new.occurred_at_utc<>pg_catalog.transaction_timestamp() or new.received_at_utc<>pg_catalog.transaction_timestamp()
     or not exists(select 1 from public.tms_users u where u.id=new.actor_user_id and u.is_active is true
       and u.role::text='admin' and (u.payment_authoriser is true or u.payment_golden_key is true))
     or not exists(select 1 from private.bpay_next_pay_run r where r.id=v_worker.run_id and r.status='DRAFT' and r.confirmed_at_utc is not null)
     or not exists(select 1 from private.bpay_next_job j where j.command_id=v_transfer.build_command_id
       and j.candidate_id=v_worker.candidate_id and j.job_kind='TRANSFER_BUILD' and j.status='DONE' and j.phase='MEMBERS_READY')
     or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_transfer.id)
     or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_transfer.id)
     or exists(select 1 from private.bpay_next_cancel_request where run_worker_id=v_worker.id and status in ('REQUESTED','CANCELLING')) then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_RECEIPT_BINDING_INVALID';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_internal_receipt_guard_v1 on private.bpay_next_internal_receipt;
create trigger bpay_next_internal_receipt_guard_v1 before insert or update or delete on private.bpay_next_internal_receipt
  for each row execute function private.bpay_next_internal_receipt_guard_v1();

create or replace function private.bpay_next_internal_request_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_receipt private.bpay_next_internal_receipt%rowtype;v_transfer private.bpay_next_transfer%rowtype;
begin
  if tg_op='DELETE' then
    if old.internal_receipt_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_REQUEST_IMMUTABLE';end if;
    return old;
  end if;
  if tg_op='UPDATE' and (old.internal_receipt_id is not null or new.internal_receipt_id is not null) then
    if (pg_catalog.to_jsonb(new)-array['posted_member_count','posting_complete'])
       is distinct from (pg_catalog.to_jsonb(old)-array['posted_member_count','posting_complete'])
       or old.posting_complete or new.posted_member_count<=old.posted_member_count then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_REQUEST_PROGRESS_INVALID';end if;
  end if;
  if new.internal_receipt_id is null then return new;end if;
  select * into strict v_receipt from private.bpay_next_internal_receipt where id=new.internal_receipt_id;
  select * into strict v_transfer from private.bpay_next_transfer where id=new.transfer_id;
  if new.outcome_id is not null or (new.command_id,new.transfer_id,new.run_worker_id,new.candidate_id,new.actor_user_id)
       is distinct from (v_receipt.command_id,v_receipt.transfer_id,v_receipt.run_worker_id,v_receipt.candidate_id,v_receipt.actor_user_id)
     or new.posted_member_count>v_transfer.member_count or new.posting_complete<>(new.posted_member_count=v_transfer.member_count)
     or (tg_op='INSERT' and (new.posted_member_count<>0 or new.posting_complete or v_transfer.status<>'MEMBERS_READY'))
     or (tg_op='UPDATE' and v_transfer.status<>'INTERNAL_PROCESSING') then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_REQUEST_BINDING_INVALID';end if;
  if tg_op='UPDATE' and not exists(select 1 from private.bpay_next_job j
       join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.command_id=new.command_id and j.candidate_id=new.candidate_id and j.job_kind='INTERNAL_SETTLEMENT'
         and j.status='LEASED' and j.phase='MEMBERS' and j.lease_nonce is not null
         and j.lease_until_utc>pg_catalog.clock_timestamp() and j.owner_epoch=c.active_owner_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_REQUEST_OWNER_INVALID';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_internal_request_guard_v1 on private.bpay_next_outcome_request;
create trigger bpay_next_internal_request_guard_v1 before insert or update or delete on private.bpay_next_outcome_request
  for each row execute function private.bpay_next_internal_request_guard_v1();

create or replace function private.bpay_next_receive_internal_settlement_v1(
  p_command_id uuid,p_transfer_id uuid,p_actor_user_id uuid
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_sequence bigint;
  v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_projection private.bpay_next_net_projection%rowtype;
  v_receipt private.bpay_next_internal_receipt%rowtype;v_request private.bpay_next_outcome_request%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
begin
  if p_command_id is null or p_transfer_id is null or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_INTERNAL_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-internal:'||p_command_id::text,0));
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id for update;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin'
      and (u.payment_authoriser is true or u.payment_golden_key is true) for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_INTERNAL_ACTOR_NOT_AUTHORISED';end if;
  select * into v_receipt from private.bpay_next_internal_receipt where command_id=p_command_id;
  if found then
    if v_receipt.transfer_id<>p_transfer_id or v_receipt.run_worker_id<>v_worker.id
       or v_receipt.candidate_id<>v_candidate or v_receipt.actor_user_id<>p_actor_user_id then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_RECEIPT_REPLAY_CONFLICT';end if;
    select * into strict v_request from private.bpay_next_outcome_request where command_id=p_command_id and internal_receipt_id=v_receipt.id;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id
      and command_kind='INTERNAL_SETTLEMENT' and module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'transfer_id',p_transfer_id,
      'internal_receipt_id',v_receipt.id,'sequence',v_sequence::text,'phase',case when v_request.posting_complete then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end,
      'posting_complete',v_request.posting_complete,'replay',true);
  end if;
  if exists(select 1 from private.bpay_next_internal_receipt where transfer_id=p_transfer_id)
     or exists(select 1 from private.bpay_next_outcome_request where command_id=p_command_id or transfer_id=p_transfer_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_RECEIPT_CONFLICT';end if;
  select * into strict v_projection from private.bpay_next_net_projection where id=v_transfer.projection_id;
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.status<>'MEMBERS_READY' or v_transfer.cash_amount<>0
     or v_transfer.member_count<1 or v_transfer.member_cash_sum<>0 or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or (v_projection.cash_amount<>0 and (v_leg.transfer_id is null or v_leg.leg_kind<>'OWN'))
     or v_projection.run_worker_id<>v_worker.id or v_projection.retired_at_utc is not null
     or v_worker.net_request_revision<>v_projection.projection_no or v_worker.net_projection_revision<>v_projection.projection_no
     or exists(select 1 from private.bpay_next_cancel_request where run_worker_id=v_worker.id and status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=p_transfer_id)
     or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=p_transfer_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_INTERNAL_NOT_ELIGIBLE';end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'INTERNAL_SETTLEMENT');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,1);
  insert into private.bpay_next_internal_receipt(command_id,transfer_id,run_worker_id,candidate_id,projection_id,actor_user_id)
    values(p_command_id,p_transfer_id,v_worker.id,v_candidate,v_projection.id,p_actor_user_id) returning * into v_receipt;
  insert into private.bpay_next_outcome_request(command_id,internal_receipt_id,transfer_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,v_receipt.id,p_transfer_id,v_worker.id,v_candidate,p_actor_user_id);
  update private.bpay_next_transfer set status='INTERNAL_PROCESSING' where id=p_transfer_id;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'transfer_id',p_transfer_id,
    'internal_receipt_id',v_receipt.id,'sequence',v_sequence::text,'phase','ACCEPTED_PENDING_POSTING','posting_complete',false,'replay',false);
end
$function$;

--0200 calls after its final real member and request progress update, in the
-- same transaction. It is not an alternative unbounded posting owner.
create or replace function private.bpay_next_complete_internal_zero_v1(p_job_id uuid,p_final_cursor bigint)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_last_member bigint;
  v_job private.bpay_next_job%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_request private.bpay_next_outcome_request%rowtype;
  v_receipt private.bpay_next_internal_receipt%rowtype;v_projection private.bpay_next_net_projection%rowtype;
  v_week private.bpay_next_worker_week%rowtype;v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
begin
  if p_job_id is null or p_final_cursor is null or p_final_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_INTERNAL_COMPLETE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,r.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_outcome_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id where j.id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_outcome_request where command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer where id=v_request.transfer_id for update;
  select * into strict v_receipt from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
  select * into strict v_projection from private.bpay_next_net_projection where id=v_receipt.projection_id;
  v_leg:=private.bpay_next_destination_execution_leg_v1(v_transfer.id);
  select member_no into strict v_last_member from private.bpay_next_transfer_member where transfer_id=v_transfer.id order by member_no desc limit 1;
  if v_job.job_kind<>'INTERNAL_SETTLEMENT' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_receipt.command_id<>v_job.command_id or v_receipt.transfer_id<>v_transfer.id
     or v_receipt.run_worker_id<>v_worker.id or v_receipt.candidate_id<>v_candidate or v_request.outcome_id is not null
     or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0 or v_transfer.member_cash_sum<>0
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_transfer.projection_id<>v_projection.id or v_projection.run_worker_id<>v_worker.id
     or (v_projection.cash_amount<>0 and (v_leg.transfer_id is null or v_leg.leg_kind<>'OWN'))
     or v_request.posting_complete is not true or v_request.posted_member_count<>v_transfer.member_count
     or v_last_member<>p_final_cursor or v_last_member<>v_transfer.member_count then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_COMPLETE_SCOPE_INVALID';end if;
  if v_job.status='DONE' then
    if v_job.phase<>'COMPLETE' or v_transfer.status<>'INTERNAL_SETTLED'
       or (v_leg.transfer_id is null and v_worker.status<>'COMPLETE')
       or (v_leg.transfer_id is not null and v_worker.status not in ('READY','COMPLETE')) then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_COMPLETE_REPLAY_INVALID';end if;
    return;
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'MEMBERS' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_transfer.status<>'INTERNAL_PROCESSING' or v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_projection.retired_at_utc is not null or v_worker.net_request_revision<>v_projection.projection_no
     or v_worker.net_projection_revision<>v_projection.projection_no
     or (v_leg.transfer_id is null and v_worker.active_case_hold_count<>0)
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_INTERNAL_COMPLETE_OWNER_INVALID';end if;
  select * into v_week from private.bpay_next_worker_week where candidate_id=v_candidate
    and pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select * into v_prior from private.bpay_next_worker_week_contribution where original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_worker.case_selection_revision<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_CASE_WEEK_MISSING';end if;
    -- A missing genuine no-case floor remains explicitly unbound; no backfill.
    raise notice 'BPAY_NEXT_INTERNAL_WEEK_CONTRIBUTION_UNBOUND';
  else
    if v_week.candidate_id is null or v_prior.candidate_id<>v_candidate or v_prior.pay_week_start<>v_week.pay_week_start
       or v_prior.original_pay_date<>v_run.pay_date or v_prior.original_created_at_utc<>v_run.created_at_utc
       or v_prior.original_gross_amount<>v_projection.gross_inc_vat
       or v_prior.original_payroll_net is distinct from v_projection.entered_paye_net
       or v_prior.accepted_projection_id is distinct from v_projection.id
       or v_prior.accepted_bank_cash is distinct from v_projection.cash_amount
       or v_prior.payment_state<>'ARRANGED' or v_prior.eligibility_state='UNBOUND'
       or v_prior.original_transfer_id is not null or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null
       or (v_projection.input_kind='CASE_PAYOUT' and (v_prior.basis_kind<>'GROSS_FALLBACK'
         or v_prior.original_gross_amount<>0 or v_prior.original_payroll_net is not null
         or v_prior.eligibility_state<>'EXCLUDED' or v_prior.eligible_arranged_amount is distinct from 0::numeric))
       or (v_projection.input_kind<>'CASE_PAYOUT' and (v_prior.basis_kind<>'PAYROLL_NET'
         or v_prior.eligibility_state not in ('ELIGIBLE','EXCLUDED') or v_prior.eligible_arranged_amount is distinct from 0::numeric)) then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_WEEK_NOT_EXACT';end if;
    update private.bpay_next_worker_week_contribution set payment_state='PAID',original_transfer_id=v_transfer.id,
      contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
    -- Its accepted zero cash was already included. Do not add payroll net or
    -- zero a shared weekly aggregate while another Candidate arrangement exists.
    update private.bpay_next_worker_week set period_revision=period_revision+1
      where candidate_id=v_candidate and pay_week_start=v_week.pay_week_start;
  end if;
  update private.bpay_next_transfer set status='INTERNAL_SETTLED' where id=v_transfer.id;
  -- Split OWN zero finishes only this leg. The original-leg counter in0200
  -- completes the worker after external CASE holds have actually posted.
  if v_leg.transfer_id is null then update private.bpay_next_run_worker set status='COMPLETE' where id=v_worker.id;end if;
end
$function$;

alter function private.bpay_next_internal_receipt_guard_v1() owner to postgres;
alter function private.bpay_next_internal_request_guard_v1() owner to postgres;
alter function private.bpay_next_receive_internal_settlement_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_complete_internal_zero_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_internal_receipt_guard_v1(),private.bpay_next_internal_request_guard_v1(),
  private.bpay_next_receive_internal_settlement_v1(uuid,uuid,uuid),private.bpay_next_complete_internal_zero_v1(uuid,bigint)
  from public,anon,authenticated,service_role;
commit;
