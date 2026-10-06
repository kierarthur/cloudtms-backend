-- Owner-only, one PAYE Candidate's exact returned-cash obligation.
-- No revaluation, new payroll, deduction, history scan or network action.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_receive_simple_reissue_v1(
  p_command_id uuid,p_return_cash_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run uuid;
  v_original private.bpay_next_transfer%rowtype;
  v_actor public.tms_users%rowtype;
  v_request private.bpay_next_reissue_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_return_cash_id is null or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where c.id=p_return_cash_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  select t.* into strict v_original from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id where c.id=p_return_cash_id;
  perform 1 from private.bpay_next_run_worker where id=v_original.run_worker_id for update;
  select * into strict v_actor from public.tms_users where id=p_actor_user_id for share;
  if v_actor.is_active is not true or
     (v_actor.payment_authoriser is not true and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_REISSUE_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_request from private.bpay_next_reissue_request where command_id=p_command_id;
  if found then
    if v_request.return_cash_id<>p_return_cash_id then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'transfer_id',v_request.transfer_id,'replay',true);
  end if;
  if v_original.status<>'RETURNED' or v_original.beneficiary_kind<>'CANDIDATE'
     or v_original.beneficiary_id<>v_original.candidate_id
     or not exists(select 1 from private.bpay_next_return_request r
                   where r.transfer_id=v_original.id and r.posting_complete) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_RETURN_NOT_COMPLETE';
  end if;
  -- Reject duplicate NEW requests before receiving a command. Otherwise an
  -- unfulfillable earlier job could block the actual settlement/return.
  -- Run lock serialises all supported cash writers; the pending index also
  -- prevents two accepted requests before either one builds its transfer.
  select * into strict v_cash from private.bpay_next_return_cash where id=p_return_cash_id for update;
  if v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid<=0
     or exists(select 1 from private.bpay_next_reissue_request
               where return_cash_id=p_return_cash_id and transfer_id is null) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_ALREADY_REQUESTED_OR_RESERVED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASH_REISSUE');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_original.candidate_id,1);
  insert into private.bpay_next_reissue_request(command_id,return_cash_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,p_return_cash_id,v_original.run_worker_id,v_original.candidate_id,p_actor_user_id);
  update private.bpay_next_command set status='SEALED',expected_member_count=1,
    sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'sequence',v_sequence,'replay',false);
end
$function$;

create or replace function private.bpay_next_build_simple_reissue_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_run uuid;
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_reissue_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_original private.bpay_next_transfer%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_amount numeric;
  v_number integer;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_BUILD_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate from private.bpay_next_job j
    join private.bpay_next_reissue_request r on r.command_id=j.command_id and r.candidate_id=j.candidate_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id where j.id=p_job_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_reissue_request where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_cash from private.bpay_next_return_cash where id=v_request.return_cash_id for update;
  select * into strict v_original from private.bpay_next_transfer where id=v_cash.original_transfer_id;
  if v_job.job_kind<>'CASH_REISSUE' or v_job.module_epoch<>v_epoch
     or v_cash.candidate_id<>v_candidate or v_original.run_worker_id<>v_request.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.transfer_id is not null then
    return pg_catalog.jsonb_build_object('transfer_id',v_request.transfer_id,'done',true,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
               and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_BUILD_NOT_ELIGIBLE';
  end if;
  v_amount:=v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid;
  if v_amount<=0 then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_CASH_ALREADY_RESERVED';
  end if;
  -- Last transfer number is one indexed lookup, not history reconstruction.
  select transfer_no+1 into strict v_number from private.bpay_next_transfer
    where run_worker_id=v_request.run_worker_id order by transfer_no desc limit 1;
  insert into private.bpay_next_transfer(run_worker_id,candidate_id,build_command_id,
    transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,original_transfer_id,return_cash_id)
    values(v_request.run_worker_id,v_candidate,v_job.command_id,v_number,
      v_original.beneficiary_kind,v_original.beneficiary_id,v_amount,'BUILDING',v_original.id,v_cash.id)
    returning * into v_transfer;
  insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,signed_cash_contribution)
    values(v_transfer.id,v_request.run_worker_id,1,'CASH_REISSUE',v_amount);
  update private.bpay_next_transfer set member_count=1,member_cash_sum=v_amount,status='MEMBERS_READY' where id=v_transfer.id;
  update private.bpay_next_return_cash set amount_held=amount_held+v_amount where id=v_cash.id;
  update private.bpay_next_reissue_request set transfer_id=v_transfer.id where command_id=v_job.command_id;
  update private.bpay_next_job set status='DONE',phase='COMPLETE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1 where candidate_id=v_candidate;
  return pg_catalog.jsonb_build_object('transfer_id',v_transfer.id,'done',true,'replay',false);
end
$function$;
alter function private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_build_simple_reissue_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid),
private.bpay_next_build_simple_reissue_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
commit;
