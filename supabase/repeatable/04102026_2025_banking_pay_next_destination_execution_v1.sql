-- L22 point-bound execution and original-leg posting conservation.
-- Current bank safety is not a financial reprice. Original money/identities stay frozen.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_destination_execution_leg_v1(p_transfer_id uuid)
returns private.bpay_next_destination_group_leg language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_t private.bpay_next_transfer%rowtype;v_o private.bpay_next_transfer%rowtype;
  v_l private.bpay_next_destination_group_leg%rowtype;v_g private.bpay_next_destination_group%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_p private.bpay_next_net_projection%rowtype;
  v_d private.bpay_next_net_destination_amount%rowtype;v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_bank public.pay_finance_case_oneoff_payout_bank_details%rowtype;
begin
  select * into strict v_t from private.bpay_next_transfer where id=p_transfer_id;
  select * into strict v_o from private.bpay_next_transfer where id=coalesce(v_t.original_transfer_id,v_t.id);
  select * into v_l from private.bpay_next_destination_group_leg where transfer_id=v_o.id;
  if not found then
    if exists(select 1 from private.bpay_next_destination_group where run_worker_id=v_o.run_worker_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_UNBOUND';end if;
    return null;
  end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_l.anchor_transfer_id;
  select * into strict v_n from private.bpay_next_net_destination_state where state_id=v_g.net_state_id;
  select * into strict v_p from private.bpay_next_net_projection where id=v_g.projection_id;
  if v_g.stage<>'COMPLETE' or v_g.completed_at_utc is null
     or v_g.sealed_leg_count<>v_g.expected_leg_count or v_g.created_leg_count<>v_g.expected_leg_count
     or v_g.sealed_cash_total<>v_p.cash_amount or v_n.projection_id<>v_p.id
     or v_n.run_worker_id<>v_g.run_worker_id or v_p.run_worker_id<>v_g.run_worker_id
     or v_n.completed_at_utc is null or v_n.own_amount+v_n.external_amount<>v_p.cash_amount
     or v_n.cash_amount<>v_p.cash_amount or v_g.expected_leg_count<>v_n.external_leg_count+1
     or v_o.run_worker_id<>v_g.run_worker_id or v_o.candidate_id<>v_g.candidate_id
     or v_o.projection_id<>v_p.id or v_o.original_transfer_id is not null or v_o.return_cash_id is not null
     or v_o.member_cash_sum<>v_o.cash_amount or v_o.member_count<1
     or v_o.beneficiary_kind<>'CANDIDATE' or v_o.beneficiary_id<>v_g.candidate_id
     or v_t.run_worker_id<>v_o.run_worker_id or v_t.candidate_id<>v_o.candidate_id
     or v_t.beneficiary_kind<>v_o.beneficiary_kind or v_t.beneficiary_id<>v_o.beneficiary_id
     or (v_t.original_transfer_id is not null and (v_t.return_cash_id is null or v_t.projection_id is not null))
     or not exists(select 1 from private.bpay_next_transfer a join private.bpay_next_command c on c.id=a.build_command_id
       join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=v_g.candidate_id
       where a.id=v_g.anchor_transfer_id and a.run_worker_id=v_g.run_worker_id and a.projection_id=v_p.id
         and c.command_kind='TRANSFER_BUILD' and c.status='COMPLETE' and j.job_kind='TRANSFER_BUILD'
         and j.status='DONE' and j.phase='MEMBERS_READY') then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_SCOPE_INVALID';end if;
  if v_l.leg_kind='OWN' then
    if v_o.id<>v_g.anchor_transfer_id or v_l.destination_id is not null or v_o.cash_amount<>v_n.own_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_OWN_INVALID';end if;
  else
    select * into strict v_d from private.bpay_next_net_destination_amount where destination_id=v_l.destination_id;
    select * into strict v_origin from private.bpay_next_stored_credit_origin where command_id=v_d.first_origin_command_id;
    select * into strict v_bank from public.pay_finance_case_oneoff_payout_bank_details
      where finance_case_id=v_origin.legacy_case_id and candidate_id=v_origin.candidate_id for share;
    if v_d.state_id<>v_n.state_id or v_d.amount<>v_o.cash_amount or v_o.execution_kind<>'BANK'
       or v_origin.candidate_id<>v_g.candidate_id or v_d.bank_details_hash<>v_origin.bank_details_hash
       or v_origin.original_source_pay_channel<>'UMBRELLA' or v_origin.original_tax_treatment<>'NON_TAXABLE'
       or v_origin.original_routing_kind<>'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
       or (v_bank.updated_at_utc,v_bank.bank_details_hash,v_bank.beneficiary_name,
           pg_catalog.replace(v_bank.sort_code,'-',''),v_bank.account_number)
         is distinct from (v_origin.bank_version_at_utc,v_origin.bank_details_hash,v_origin.beneficiary_name,
           v_origin.sort_code,v_origin.account_number)
       or public._bank_hash(v_bank.sort_code,v_bank.account_number,v_bank.beneficiary_name)
         is distinct from v_origin.bank_details_hash then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_ONEOFF_INVALID';end if;
  end if;
  return v_l;
end $function$;

create or replace function private.bpay_next_destination_posting_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_g private.bpay_next_destination_group%rowtype;v_r private.bpay_next_destination_posted_leg%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_DELETE_FORBIDDEN';end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=new.anchor_transfer_id;
  if v_g.stage<>'COMPLETE' or new.posted_leg_count>v_g.expected_leg_count then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_SCOPE_INVALID';end if;
  if tg_op='INSERT' then
    if new.posted_leg_count<>0 or new.last_posted_transfer_id is not null or new.completed_at_utc is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_INITIAL_INVALID';end if;
  else
    select * into strict v_r from private.bpay_next_destination_posted_leg
      where anchor_transfer_id=new.anchor_transfer_id and original_transfer_id=new.last_posted_transfer_id;
    if old.completed_at_utc is not null or new.anchor_transfer_id<>old.anchor_transfer_id
       or new.posted_leg_count<>old.posted_leg_count+1 or v_r.posting_no<>new.posted_leg_count
       or ((new.posted_leg_count=v_g.expected_leg_count) is distinct from (new.completed_at_utc is not null))
       or (new.completed_at_utc is not null and new.completed_at_utc<>pg_catalog.transaction_timestamp()) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_PROGRESS_INVALID';end if;
  end if;
  return new;
end $function$;

create or replace function private.bpay_next_destination_posted_leg_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_l private.bpay_next_destination_group_leg%rowtype;v_j private.bpay_next_job%rowtype;
  v_t private.bpay_next_transfer%rowtype;v_r private.bpay_next_outcome_request%rowtype;
  v_s private.bpay_next_destination_posting%rowtype;
begin
  if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_IMMUTABLE';end if;
  v_l:=private.bpay_next_destination_execution_leg_v1(new.original_transfer_id);
  select * into strict v_s from private.bpay_next_destination_posting where anchor_transfer_id=new.anchor_transfer_id;
  select * into strict v_j from private.bpay_next_job where id=new.job_id;
  select * into strict v_r from private.bpay_next_outcome_request where command_id=v_j.command_id;
  select * into strict v_t from private.bpay_next_transfer where id=new.original_transfer_id;
  if v_l.transfer_id is null or v_l.anchor_transfer_id<>new.anchor_transfer_id
     or v_s.completed_at_utc is not null or new.posting_no<>v_s.posted_leg_count+1
     or new.posted_at_utc<>pg_catalog.transaction_timestamp() or v_j.status<>'LEASED' or v_j.phase<>'MEMBERS'
     or v_j.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_j.lease_nonce is null
     or v_j.lease_until_utc<=pg_catalog.clock_timestamp()
     or not exists(select 1 from private.bpay_next_worker_control where candidate_id=v_j.candidate_id and active_owner_epoch=v_j.owner_epoch)
     or v_r.transfer_id<>v_t.id or v_r.run_worker_id<>v_t.run_worker_id or v_r.candidate_id<>v_j.candidate_id
     or v_r.posting_complete is not true or v_r.posted_member_count<>v_t.member_count
     or v_t.original_transfer_id is not null or v_t.return_cash_id is not null
     or (v_j.job_kind='CSV_SETTLEMENT' and (v_t.execution_kind<>'BANK' or v_t.status not in ('SETTLED','RETURNED')
       or v_r.internal_receipt_id is not null or not exists(select 1 from private.bpay_next_transfer_outcome o
         where o.id=v_r.outcome_id and o.transfer_id=v_t.id and o.outcome_kind='SETTLED' and o.whole_transfer_amount=v_t.cash_amount)))
     or (v_j.job_kind='INTERNAL_SETTLEMENT' and (v_l.leg_kind<>'OWN' or v_t.execution_kind<>'INTERNAL_ZERO'
       or v_t.status<>'INTERNAL_SETTLED' or v_t.cash_amount<>0 or v_r.outcome_id is not null
       or not exists(select 1 from private.bpay_next_internal_receipt i where i.id=v_r.internal_receipt_id
         and i.command_id=v_j.command_id and i.transfer_id=v_t.id and i.run_worker_id=v_t.run_worker_id))) then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_SCOPE_INVALID';end if;
  return new;
end $function$;

create or replace function private.bpay_next_complete_destination_leg_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_j private.bpay_next_job%rowtype;v_r private.bpay_next_outcome_request%rowtype;
  v_t private.bpay_next_transfer%rowtype;v_l private.bpay_next_destination_group_leg%rowtype;
  v_g private.bpay_next_destination_group%rowtype;v_s private.bpay_next_destination_posting%rowtype;
  v_old private.bpay_next_destination_posted_leg%rowtype;
begin
  select * into strict v_j from private.bpay_next_job where id=p_job_id;
  select * into strict v_r from private.bpay_next_outcome_request where command_id=v_j.command_id;
  select * into strict v_t from private.bpay_next_transfer where id=v_r.transfer_id;
  -- Reissue cash never posts original payroll/principal twice.
  if v_t.original_transfer_id is not null then return;end if;
  v_l:=private.bpay_next_destination_execution_leg_v1(v_t.id);
  if v_l.transfer_id is null then return;end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_l.anchor_transfer_id;
  select * into v_old from private.bpay_next_destination_posted_leg where original_transfer_id=v_t.id;
  if found then
    if v_old.job_id<>p_job_id then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_CONFLICT';end if;
    return;
  end if;
  -- Caller already holds header -> Candidate -> job -> worker -> exact transfer.
  -- The shared header serialises lazy counter creation; no sibling/history scan.
  insert into private.bpay_next_destination_posting(anchor_transfer_id) values(v_g.anchor_transfer_id) on conflict do nothing;
  select * into strict v_s from private.bpay_next_destination_posting where anchor_transfer_id=v_g.anchor_transfer_id for update;
  insert into private.bpay_next_destination_posted_leg(original_transfer_id,anchor_transfer_id,job_id,posting_no)
    values(v_t.id,v_g.anchor_transfer_id,p_job_id,v_s.posted_leg_count+1);
  update private.bpay_next_destination_posting set posted_leg_count=posted_leg_count+1,last_posted_transfer_id=v_t.id,
    completed_at_utc=case when posted_leg_count+1=v_g.expected_leg_count then pg_catalog.transaction_timestamp() else null end
    where anchor_transfer_id=v_g.anchor_transfer_id returning * into v_s;
  if v_s.completed_at_utc is not null then
    update private.bpay_next_run_worker set status='COMPLETE' where id=v_g.run_worker_id
      and status='READY' and active_case_hold_count=0;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_COMPLETION_HOLDS_OR_STATE_INVALID';end if;
  end if;
end $function$;

drop trigger if exists bpay_next_destination_posting_guard_v1 on private.bpay_next_destination_posting;
create trigger bpay_next_destination_posting_guard_v1 before insert or update or delete on private.bpay_next_destination_posting
  for each row execute function private.bpay_next_destination_posting_guard_v1();
drop trigger if exists bpay_next_destination_posted_leg_guard_v1 on private.bpay_next_destination_posted_leg;
create trigger bpay_next_destination_posted_leg_guard_v1 before insert or update or delete on private.bpay_next_destination_posted_leg
  for each row execute function private.bpay_next_destination_posted_leg_guard_v1();

alter function private.bpay_next_destination_execution_leg_v1(uuid) owner to postgres;
alter function private.bpay_next_destination_posting_guard_v1() owner to postgres;
alter function private.bpay_next_destination_posted_leg_guard_v1() owner to postgres;
alter function private.bpay_next_complete_destination_leg_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_destination_execution_leg_v1(uuid),private.bpay_next_destination_posting_guard_v1(),
  private.bpay_next_destination_posted_leg_guard_v1(),private.bpay_next_complete_destination_leg_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
