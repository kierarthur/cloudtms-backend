-- Approved reset policy: bank return reopens owned cash, retaining payroll.
-- Original, single-Candidate PAYE CSV only; no network or bank action.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_receive_simple_csv_return_v1(
  p_command_id uuid,p_transfer_id uuid,p_receipt_id text,
  p_amount numeric,p_occurred_at timestamptz,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run uuid;
  v_candidate_id uuid;
  v_transfer private.bpay_next_transfer%rowtype;
  v_actor public.tms_users%rowtype;
  v_receipt private.bpay_next_transfer_outcome%rowtype;
  v_prior private.bpay_next_return_request%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_transfer_id is null or p_receipt_id is null
     or pg_catalog.octet_length(p_receipt_id) not between 1 and 256
     or p_amount is null or p_amount<=0 or p_amount<>pg_catalog.round(p_amount,2)
     or p_occurred_at is null or not pg_catalog.isfinite(p_occurred_at)
     or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_RETURN_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run,v_candidate_id from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where t.id=p_transfer_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  perform 1 from private.bpay_next_run_worker w join private.bpay_next_transfer t
    on t.run_worker_id=w.id where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users where id=p_actor_user_id for share;
  if v_actor.is_active is not true or
     (v_actor.payment_authoriser is not true and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_RETURN_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_receipt from private.bpay_next_transfer_outcome where receipt_id=p_receipt_id;
  if found then
    if v_receipt.transfer_id<>p_transfer_id or v_receipt.outcome_kind<>'RETURNED'
       or v_receipt.whole_transfer_amount<>p_amount or v_receipt.occurred_at_utc<>p_occurred_at then
      raise exception using errcode='23514',message='BPAY_NEXT_RETURN_RECEIPT_CONFLICT';
    end if;
    select * into strict v_prior from private.bpay_next_return_request where outcome_id=v_receipt.id;
    return pg_catalog.jsonb_build_object('command_id',v_prior.command_id,
      'outcome_id',v_receipt.id,'posting_complete',v_prior.posting_complete,'replay',true);
  end if;
  if exists(select 1 from private.bpay_next_return_request
            where command_id=p_command_id or transfer_id=p_transfer_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_RETURN_COMMAND_CONFLICT';
  end if;
  if v_transfer.status<>'SETTLED'
     or v_transfer.destination_rail<>'CSV' or v_transfer.cash_amount<>p_amount
     or not exists(select 1 from private.bpay_next_outcome_request r
                   join private.bpay_next_transfer_outcome o on o.id=r.outcome_id
                   where r.transfer_id=p_transfer_id and o.outcome_kind='SETTLED'
                     and o.whole_transfer_amount=p_amount and o.occurred_at_utc<=p_occurred_at) then
    raise exception using errcode='55000',message='BPAY_NEXT_RETURN_PREDECESSOR_REQUIRED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CSV_RETURN');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_transfer.candidate_id,1);
  insert into private.bpay_next_transfer_outcome
    (transfer_id,receipt_id,outcome_kind,whole_transfer_amount,occurred_at_utc)
    values(p_transfer_id,p_receipt_id,'RETURNED',p_amount,p_occurred_at) returning * into v_receipt;
  insert into private.bpay_next_return_request
    (command_id,outcome_id,transfer_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,v_receipt.id,p_transfer_id,v_transfer.run_worker_id,v_transfer.candidate_id,p_actor_user_id);
  update private.bpay_next_transfer set status='RETURNED' where id=p_transfer_id;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',
    sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'outcome_id',v_receipt.id,
    'sequence',v_sequence,'posting_complete',false,'replay',false);
end
$function$;

create or replace function private.bpay_next_post_simple_return_v1(
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
  v_request private.bpay_next_return_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_cash uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',message='BPAY_NEXT_RETURN_POST_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate
    from private.bpay_next_job j join private.bpay_next_return_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id where j.id=p_job_id and r.candidate_id=j.candidate_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_return_request where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer where id=v_request.transfer_id for update;
  if v_job.job_kind<>'CSV_RETURN' or v_job.module_epoch<>v_epoch
     or v_transfer.candidate_id<>v_candidate or v_transfer.run_worker_id<>v_request.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_RETURN_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.posting_complete then
    if v_transfer.return_cash_id is null then
      select id into strict v_cash from private.bpay_next_return_cash where original_transfer_id=v_transfer.id;
    else
      v_cash:=v_transfer.return_cash_id;
    end if;
    return pg_catalog.jsonb_build_object('return_cash_id',v_cash,'done',true,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_transfer.status<>'RETURNED'
     or not exists(select 1 from private.bpay_next_outcome_request r
                   where r.transfer_id=v_transfer.id and r.posting_complete)
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
               and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_RETURN_POST_NOT_ELIGIBLE';
  end if;
  if v_transfer.return_cash_id is null then
    insert into private.bpay_next_return_cash(original_transfer_id,candidate_id,amount_owed)
      values(v_transfer.id,v_candidate,v_transfer.cash_amount) returning id into v_cash;
  else
    -- A returned reissue reopens the SAME obligation. It is never new wages
    -- or a duplicate credit, even after several different bank accounts.
    update private.bpay_next_return_cash
      set amount_reissued_paid=amount_reissued_paid-v_transfer.cash_amount
      where id=v_transfer.return_cash_id and candidate_id=v_candidate
        and original_transfer_id=v_transfer.original_transfer_id
        and amount_reissued_paid>=v_transfer.cash_amount
      returning id into v_cash;
    if not found then
      raise exception using errcode='23514',message='BPAY_NEXT_RETURN_REISSUE_CASH_MISMATCH';
    end if;
  end if;
  update private.bpay_next_return_request set posting_complete=true where command_id=v_job.command_id;
  perform private.bpay_next_apply_week_outcome_v1(p_job_id,v_cash);
  update private.bpay_next_job set status='DONE',phase='COMPLETE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  return pg_catalog.jsonb_build_object('return_cash_id',v_cash,'done',true,'replay',false);
end
$function$;
alter function private.bpay_next_receive_simple_csv_return_v1(uuid,uuid,text,numeric,timestamptz,uuid) owner to postgres;
alter function private.bpay_next_post_simple_return_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_receive_simple_csv_return_v1(uuid,uuid,text,numeric,timestamptz,uuid),
private.bpay_next_post_simple_return_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
commit;
