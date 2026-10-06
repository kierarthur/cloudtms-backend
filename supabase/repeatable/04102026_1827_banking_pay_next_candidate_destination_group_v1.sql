-- L22 ordinary PAYE destination owner. All money/bank inputs come from genuine
-- retained receipts; the public boundary accepts three identities only.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_bind_stored_credit_v1(
  p_command_id uuid,p_legacy_case_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_prior private.bpay_next_stored_credit_origin%rowtype;
  v_case public.pay_advances%rowtype;v_component public.pay_finance_case_components%rowtype;
  v_bank public.pay_finance_case_oneoff_payout_bank_details%rowtype;
  v_channel text;v_reply jsonb;v_count integer;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  if p_command_id is null or p_legacy_case_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_STORED_CREDIT_INPUT_INVALID';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-case-create:'||p_command_id::text,0));
  select * into v_prior from private.bpay_next_stored_credit_origin where command_id=p_command_id;
  if found then
    if v_prior.legacy_case_id<>p_legacy_case_id or v_prior.actor_user_id<>p_actor_user_id then
      raise exception using errcode='23514',message='BPAY_NEXT_STORED_CREDIT_REPLAY_CONFLICT';end if;
    return private.bpay_next_accept_case_create_v1(p_command_id,v_prior.candidate_id,p_actor_user_id,
      'CREDIT','MANUAL_CREDIT','NON_TAXABLE',v_prior.principal_source_ex_vat,null,null,null,null,null,null,
      'STORED_ONEOFF_CREDIT:'||p_legacy_case_id::text);
  end if;
  -- Candidate precedes the exact financial rows, matching the genuine creator.
  select * into strict v_case from public.pay_advances where id=p_legacy_case_id;
  select c.pay_method into strict v_channel from public.candidates c
    where c.id=v_case.candidate_id and c.active is true for share;
  select * into strict v_case from public.pay_advances where id=p_legacy_case_id for update;
  select * into strict v_bank from public.pay_finance_case_oneoff_payout_bank_details
    where finance_case_id=p_legacy_case_id and candidate_id=v_case.candidate_id for update;
  select count(*) into v_count from (select id from public.pay_finance_case_components
    where finance_case_id=p_legacy_case_id order by id limit 2) s;
  if v_count<>1 then raise exception using errcode='23514',message='BPAY_NEXT_STORED_CREDIT_COMPONENT_AMBIGUOUS';end if;
  select * into strict v_component from public.pay_finance_case_components where finance_case_id=p_legacy_case_id for update;
  if v_channel<>'PAYE' or v_case.case_type::text<>'MANUAL_CREDIT_ADJUSTMENT'
     or v_case.taxability::text<>'NON_TAXABLE' or v_case.routing_kind::text<>'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
     or not v_case.oneoff_bank_details_required or v_case.status::text<>'ACTIVE'
     or v_case.payout_status::text<>'PENDING' or v_case.payout_pay_batch_id is not null or v_case.payout_transfer_id is not null
     or v_case.original_amount<=0 or v_case.outstanding_amount<>v_case.original_amount
     or v_case.cleared_at_utc is not null or v_case.written_off_at_utc is not null
     or v_component.candidate_id<>v_case.candidate_id or v_component.source_pay_method<>'UMBRELLA'
     or v_component.classification::text<>'REIMBURSEMENT_GROSS_FIXED'
     or v_component.component_key_type<>'CASE_TOTAL' or v_component.component_key_value<>'TOTAL'
     or v_component.source_family_key<>'case:'||v_case.id::text
     or v_component.linked_timesheet_id is not null or v_component.closed_at_utc is not null
     or v_component.source_amount<>v_case.original_amount or v_component.remaining_source_amount<>v_component.source_amount
     or (v_component.source_basis_json->>'case_type') is distinct from 'MANUAL_CREDIT_ADJUSTMENT'
     or (v_component.source_basis_json->>'taxability') is distinct from 'NON_TAXABLE'
     or (v_component.source_basis_json->>'routing_kind') is distinct from 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
     or exists(select 1 from public.pay_advance_reservations r where r.finance_case_id=v_case.id
       and r.status::text in ('RESERVED','COMMITTED'))
     or not exists(select 1 from public.pay_finance_case_events e where e.finance_case_id=v_case.id
       and e.finance_component_id=v_component.id and e.event_type='COMPONENT_CREATED'
       and e.reason='MANUAL_CREDIT_ADJUSTMENT_CREATE')
     or public._bank_hash(v_bank.sort_code,v_bank.account_number,v_bank.beneficiary_name) is distinct from v_bank.bank_details_hash
     or not pg_catalog.isfinite(v_case.created_at) or not pg_catalog.isfinite(v_bank.updated_at_utc) then
    raise exception using errcode='55000',message='BPAY_NEXT_STORED_CREDIT_NOT_ELIGIBLE';end if;
  v_reply:=private.bpay_next_accept_case_create_v1(p_command_id,v_case.candidate_id,p_actor_user_id,
    'CREDIT','MANUAL_CREDIT','NON_TAXABLE',v_component.remaining_source_amount,null,null,null,null,null,null,
    'STORED_ONEOFF_CREDIT:'||p_legacy_case_id::text);
  -- Unique legacy identities reject a second command, including a concurrent bind.
  insert into private.bpay_next_stored_credit_origin(command_id,legacy_case_id,legacy_component_id,candidate_id,actor_user_id,
    original_source_pay_channel,original_tax_treatment,original_routing_kind,principal_source_ex_vat,original_created_at_utc,
    bank_version_at_utc,bank_details_hash,beneficiary_name,sort_code,account_number)
    values(p_command_id,v_case.id,v_component.id,v_case.candidate_id,p_actor_user_id,'UMBRELLA','NON_TAXABLE',
      'ONE_OFF_SPECIFIED_BANK_ACCOUNT',v_component.remaining_source_amount,v_case.created_at,v_bank.updated_at_utc,
      v_bank.bank_details_hash,v_bank.beneficiary_name,pg_catalog.replace(v_bank.sort_code,'-',''),v_bank.account_number);
  return v_reply;
end $function$;

create or replace function public.bpay_next_bind_stored_credit_v1(
  p_actor_user_id uuid,p_command_id uuid,p_legacy_case_id uuid)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  return private.bpay_next_bind_stored_credit_v1(p_command_id,p_legacy_case_id,p_actor_user_id);
end $function$;

-- A claimed legacy origin cannot subsequently be reserved/paid through LEGACY.
-- This is an exact identity guard, not an exemption from 0550 or a balance rewrite.
create or replace function private.bpay_next_stored_credit_legacy_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_old_id uuid;
begin
  if tg_table_name='pay_advances' then v_id:=old.id;
  elsif tg_op='DELETE' then v_id:=old.finance_case_id;
  else v_id:=new.finance_case_id;end if;
  if tg_op='UPDATE' and tg_table_name<>'pay_advances' then v_old_id:=old.finance_case_id;end if;
  if exists(select 1 from private.bpay_next_stored_credit_origin where legacy_case_id=v_id)
     or (v_old_id is not null and exists(select 1 from private.bpay_next_stored_credit_origin where legacy_case_id=v_old_id)) then
    raise exception using errcode='55000',message='BPAY_NEXT_STORED_CREDIT_ORIGIN_CLAIMED';end if;
  if tg_op='DELETE' then return old;else return new;end if;
end $function$;
drop trigger if exists bpay_next_stored_credit_legacy_guard_v1 on public.pay_advances;
create trigger bpay_next_stored_credit_legacy_guard_v1 before update or delete on public.pay_advances
  for each row execute function private.bpay_next_stored_credit_legacy_guard_v1();
drop trigger if exists bpay_next_stored_credit_legacy_guard_v1 on public.pay_finance_case_components;
create trigger bpay_next_stored_credit_legacy_guard_v1 before update or delete on public.pay_finance_case_components
  for each row execute function private.bpay_next_stored_credit_legacy_guard_v1();
drop trigger if exists bpay_next_stored_credit_legacy_guard_v1 on public.pay_finance_case_oneoff_payout_bank_details;
create trigger bpay_next_stored_credit_legacy_guard_v1 before update or delete on public.pay_finance_case_oneoff_payout_bank_details
  for each row execute function private.bpay_next_stored_credit_legacy_guard_v1();
drop trigger if exists bpay_next_stored_credit_legacy_guard_v1 on public.pay_advance_reservations;
create trigger bpay_next_stored_credit_legacy_guard_v1 before insert or update or delete on public.pay_advance_reservations
  for each row execute function private.bpay_next_stored_credit_legacy_guard_v1();

create or replace function private.bpay_next_capture_instruction_destination_v1(p_instruction_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_i private.bpay_next_run_case_instruction%rowtype;v_o private.bpay_next_stored_credit_origin%rowtype;
begin
  select * into strict v_i from private.bpay_next_run_case_instruction where id=p_instruction_id;
  select * into v_o from private.bpay_next_stored_credit_origin where command_id=v_i.case_component_id;
  if not found then return;end if;
  if v_i.candidate_id<>v_o.candidate_id or v_i.case_id<>v_o.command_id or v_i.case_kind<>'CREDIT'
     or v_i.case_subtype<>'MANUAL_CREDIT' or v_i.tax_treatment<>'NON_TAXABLE' or v_i.instruction_kind<>'CREDIT'
     or v_i.payroll_stage<>'NET_ADD' or v_i.direction<>'PAYMENT' or v_i.source_pay_channel<>'PAYE'
     or v_i.target_pay_channel<>'PAYE' or v_i.nominal_target_vat<>0
     or v_i.debt_age_at_utc<>v_o.original_created_at_utc then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CAPTURE_MISMATCH';end if;
  insert into private.bpay_next_instruction_destination(instruction_id,origin_command_id) values(v_i.id,v_o.command_id);
end $function$;

-- Called once for each already ordered NET instruction. Exact Draft addition
-- holds remain intact; CREDIT never enters the E/H allocation kernel.
create or replace function private.bpay_next_accumulate_net_destination_v1(p_state_id uuid,p_instruction_id uuid,p_result_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_o private.bpay_next_stored_credit_origin%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
  v_s private.bpay_next_case_allocation_state%rowtype;v_r private.bpay_next_case_allocation_result%rowtype;
  v_d private.bpay_next_net_destination_amount%rowtype;v_new boolean:=false;
begin
  select o.* into v_o from private.bpay_next_instruction_destination d
    join private.bpay_next_stored_credit_origin o on o.command_id=d.origin_command_id where d.instruction_id=p_instruction_id;
  if not found then return;end if;
  select * into strict v_s from private.bpay_next_case_allocation_state where id=p_state_id;
  select * into strict v_i from private.bpay_next_run_case_instruction where id=p_instruction_id;
  select * into strict v_r from private.bpay_next_case_allocation_result where id=p_result_id;
  if v_s.pass_kind<>'NET' or v_s.status<>'BUILDING' or v_s.net_stage<>'ALLOCATE'
     or v_i.run_worker_id<>v_s.run_worker_id or v_i.candidate_id<>v_o.candidate_id
     or v_i.preparation_revision<>v_s.preparation_revision or v_i.selection_revision<>v_s.selection_revision
     or v_i.case_component_id<>v_o.command_id or v_i.payroll_stage<>'NET_ADD' or v_i.tax_treatment<>'NON_TAXABLE'
     or v_r.instruction_id<>v_i.id or v_r.pass_kind<>'DRAFT'
     or v_r.allocated_target_vat<>0 or v_r.allocated_source_ex_vat<>v_r.allocated_target_inc_vat
     or not exists(select 1 from private.bpay_next_case_allocation_state d where d.id=v_r.state_id
       and d.run_worker_id=v_s.run_worker_id and d.preparation_revision=v_s.preparation_revision
       and d.selection_revision=v_s.selection_revision and d.pass_kind='DRAFT' and d.status='COMPLETE') then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_BASIS_INVALID';end if;
  if v_r.allocated_target_inc_vat=0 then return;end if;
  insert into private.bpay_next_net_destination_state(state_id,run_worker_id) values(v_s.id,v_s.run_worker_id)
    on conflict(state_id) do nothing;
  perform 1 from private.bpay_next_net_destination_state where state_id=v_s.id and projection_id is null for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_ALREADY_SEALED';end if;
  select * into v_d from private.bpay_next_net_destination_amount where state_id=v_s.id and bank_details_hash=v_o.bank_details_hash for update;
  if not found then
    insert into private.bpay_next_net_destination_amount(state_id,bank_details_hash,first_origin_command_id,credit_count,amount)
      values(v_s.id,v_o.bank_details_hash,v_o.command_id,1,v_r.allocated_target_inc_vat) returning * into v_d;
    v_new:=true;
  else
    update private.bpay_next_net_destination_amount set credit_count=credit_count+1,amount=amount+v_r.allocated_target_inc_vat
      where destination_id=v_d.destination_id;
  end if;
  insert into private.bpay_next_net_destination_credit(state_id,instruction_id,destination_id,allocation_result_id,amount)
    values(v_s.id,v_i.id,v_d.destination_id,v_r.id,v_r.allocated_target_inc_vat);
  update private.bpay_next_net_destination_state set external_credit_count=external_credit_count+1,
    external_leg_count=external_leg_count+case when v_new then 1 else 0 end,external_amount=external_amount+v_r.allocated_target_inc_vat
    where state_id=v_s.id;
end $function$;

create or replace function private.bpay_next_seal_net_destination_v1(p_state_id uuid,p_projection_id uuid)
returns numeric language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_p private.bpay_next_net_projection%rowtype;v_s private.bpay_next_net_destination_state%rowtype;
begin
  select * into strict v_p from private.bpay_next_net_projection where id=p_projection_id;
  select * into v_s from private.bpay_next_net_destination_state where state_id=p_state_id for update;
  if not found then return v_p.cash_amount;end if;
  if v_s.projection_id is not null or v_s.run_worker_id<>v_p.run_worker_id or v_s.external_leg_count<1
     or v_s.external_amount>v_p.accepted_net_additions or v_s.external_amount>v_p.cash_amount
     or not exists(select 1 from private.bpay_next_case_allocation_state s where s.id=p_state_id
       and s.run_worker_id=v_p.run_worker_id and s.pass_kind='NET' and s.status='BUILDING'
       and s.projection_no=v_p.projection_no and s.processed_instruction_count=s.expected_instruction_count) then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_CONSERVATION_INVALID';end if;
  update private.bpay_next_net_destination_state set projection_id=v_p.id,own_amount=v_p.cash_amount-external_amount,
    cash_amount=v_p.cash_amount,completed_at_utc=pg_catalog.transaction_timestamp() where state_id=p_state_id;
  return v_p.cash_amount-v_s.external_amount;
end $function$;

create or replace function private.bpay_next_destination_state_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_s private.bpay_next_case_allocation_state%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_o private.bpay_next_stored_credit_origin%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_DELETE_FORBIDDEN';end if;
  select * into strict v_s from private.bpay_next_case_allocation_state where id=new.state_id;
  if v_s.pass_kind<>'NET' or v_s.status<>'BUILDING' or v_s.net_stage<>'ALLOCATE'
     or not exists(select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.id=v_s.job_id and j.candidate_id=v_s.candidate_id and j.job_kind='PAYE_NET_ENTRY'
       and j.status='LEASED' and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_OWNER_INVALID';end if;
  if tg_table_name='bpay_next_net_destination_state' then
    if new.run_worker_id<>v_s.run_worker_id then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_SCOPE_INVALID';end if;
    if tg_op='INSERT' then
      if new.projection_id is not null or new.external_credit_count<>0 or new.external_leg_count<>0 or new.external_amount<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_INITIAL_INVALID';end if;
    elsif old.projection_id is not null or new.state_id<>old.state_id or new.run_worker_id<>old.run_worker_id
      or new.external_credit_count<old.external_credit_count or new.external_leg_count<old.external_leg_count
      or new.external_amount<old.external_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_TRANSITION_INVALID';end if;
  else
    select * into strict v_n from private.bpay_next_net_destination_state where state_id=new.state_id;
    select * into strict v_o from private.bpay_next_stored_credit_origin where command_id=new.first_origin_command_id;
    if v_n.projection_id is not null or v_o.candidate_id<>v_s.candidate_id or new.bank_details_hash<>v_o.bank_details_hash then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_AMOUNT_SCOPE_INVALID';end if;
    if tg_op='UPDATE' and ((new.destination_id,new.state_id,new.bank_details_hash,new.first_origin_command_id)
      is distinct from (old.destination_id,old.state_id,old.bank_details_hash,old.first_origin_command_id)
      or new.credit_count<>old.credit_count+1 or new.amount<=old.amount) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_AMOUNT_TRANSITION_INVALID';end if;
  end if;
  return new;
end $function$;
drop trigger if exists bpay_next_destination_state_guard_v1 on private.bpay_next_net_destination_state;
create trigger bpay_next_destination_state_guard_v1 before insert or update or delete on private.bpay_next_net_destination_state
  for each row execute function private.bpay_next_destination_state_guard_v1();
drop trigger if exists bpay_next_destination_state_guard_v1 on private.bpay_next_net_destination_amount;
create trigger bpay_next_destination_state_guard_v1 before insert or update or delete on private.bpay_next_net_destination_amount
  for each row execute function private.bpay_next_destination_state_guard_v1();

create or replace function private.bpay_next_destination_group_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_t private.bpay_next_transfer%rowtype;v_n private.bpay_next_net_destination_state%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_DELETE_FORBIDDEN';end if;
  if tg_op='INSERT' then
    select * into strict v_t from private.bpay_next_transfer where id=new.anchor_transfer_id;
    select * into strict v_n from private.bpay_next_net_destination_state where state_id=new.net_state_id;
    if v_t.run_worker_id<>new.run_worker_id or v_t.candidate_id<>new.candidate_id or v_t.projection_id<>new.projection_id
       or v_t.beneficiary_kind<>'CANDIDATE' or v_t.beneficiary_id<>new.candidate_id
       or v_t.build_command_id is null or v_t.transfer_no<>1 or v_t.status<>'BUILDING'
       or v_n.projection_id is distinct from new.projection_id or v_n.own_amount<>v_t.cash_amount
       or new.expected_leg_count<>v_n.external_leg_count+1 or new.stage<>'WORK' or new.checkpoint<>0
       or new.processed_work_count<>0 or new.processed_case_count<>0 or new.created_leg_count<>1
       or new.sealed_leg_count<>0 or new.member_cash_total<>0 or new.sealed_cash_total<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_INITIAL_INVALID';end if;
  else
    if old.stage='COMPLETE' or new.checkpoint<>old.checkpoint+1
       or (new.anchor_transfer_id,new.run_worker_id,new.candidate_id,new.projection_id,new.draft_state_id,new.net_state_id,
         new.preparation_revision,new.selection_revision,new.expected_work_count,new.expected_case_count,new.expected_leg_count)
       is distinct from (old.anchor_transfer_id,old.run_worker_id,old.candidate_id,old.projection_id,old.draft_state_id,old.net_state_id,
         old.preparation_revision,old.selection_revision,old.expected_work_count,old.expected_case_count,old.expected_leg_count)
       or new.processed_work_count<old.processed_work_count or new.processed_case_count<old.processed_case_count
       or new.created_leg_count<old.created_leg_count or new.sealed_leg_count<old.sealed_leg_count
       or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','ADJUST'))
       or (old.stage='ADJUST' and new.stage<>'SEAL') or (old.stage='SEAL' and new.stage not in ('SEAL','COMPLETE')) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_TRANSITION_INVALID';end if;
    select * into strict v_t from private.bpay_next_transfer where id=new.anchor_transfer_id;
    if not exists(select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.command_id=v_t.build_command_id and j.candidate_id=new.candidate_id and j.job_kind='TRANSFER_BUILD'
       and j.status='LEASED' and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_OWNER_INVALID';end if;
  end if;
  return new;
end $function$;
drop trigger if exists bpay_next_destination_group_guard_v1 on private.bpay_next_destination_group;
create trigger bpay_next_destination_group_guard_v1 before insert or update or delete on private.bpay_next_destination_group
  for each row execute function private.bpay_next_destination_group_guard_v1();

create or replace function private.bpay_next_destination_leg_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_t private.bpay_next_transfer%rowtype;v_g private.bpay_next_destination_group%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_d private.bpay_next_net_destination_amount%rowtype;
begin
  select * into strict v_t from private.bpay_next_transfer where id=new.transfer_id;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=new.anchor_transfer_id;
  select * into strict v_n from private.bpay_next_net_destination_state where state_id=v_g.net_state_id;
  if v_t.run_worker_id<>v_g.run_worker_id or v_t.candidate_id<>v_g.candidate_id or v_t.projection_id<>v_g.projection_id
     or v_t.beneficiary_kind<>'CANDIDATE' or v_t.beneficiary_id<>v_g.candidate_id or v_t.status<>'BUILDING'
     or v_t.original_transfer_id is not null or v_t.return_cash_id is not null
     or v_t.member_count<>0 or v_t.member_cash_sum<>0 or v_t.account_approval_ref is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_LEG_SCOPE_INVALID';end if;
  if new.leg_kind='OWN' then
    if v_g.stage<>'WORK' or v_g.checkpoint<>0 or v_t.id<>v_g.anchor_transfer_id or v_t.transfer_no<>1
       or v_t.build_command_id is null or v_t.cash_amount<>v_n.own_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_OWN_LEG_INVALID';end if;
  else
    select * into strict v_d from private.bpay_next_net_destination_amount where destination_id=new.destination_id;
    if v_g.stage<>'CASE' or v_t.build_command_id is not null or v_t.cash_amount<>v_d.amount
       or v_t.execution_kind<>'BANK' or v_d.state_id<>v_g.net_state_id then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_ONEOFF_LEG_INVALID';end if;
  end if;
  return new;
end $function$;
drop trigger if exists bpay_next_destination_leg_guard_v1 on private.bpay_next_destination_group_leg;
create trigger bpay_next_destination_leg_guard_v1 before insert on private.bpay_next_destination_group_leg
  for each row execute function private.bpay_next_destination_leg_guard_v1();

-- Immutable provenance and member identities use the existing closed guard.
create or replace function private.bpay_next_destination_build_binding_v1(p_transfer_id uuid)
returns private.bpay_next_case_transfer_build language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_g private.bpay_next_destination_group%rowtype;v_b private.bpay_next_case_transfer_build%rowtype;
begin
  select g.* into v_g from private.bpay_next_destination_group_leg l
    join private.bpay_next_destination_group g on g.anchor_transfer_id=l.anchor_transfer_id where l.transfer_id=p_transfer_id;
  if not found then return null;end if;
  v_b.transfer_id:=p_transfer_id;v_b.run_worker_id:=v_g.run_worker_id;v_b.candidate_id:=v_g.candidate_id;
  v_b.projection_id:=v_g.projection_id;v_b.draft_state_id:=v_g.draft_state_id;v_b.net_state_id:=v_g.net_state_id;
  v_b.preparation_revision:=v_g.preparation_revision;v_b.selection_revision:=v_g.selection_revision;
  v_b.stage:=case when v_g.stage='ADJUST' then 'FINAL' when v_g.stage='SEAL' then 'COMPLETE' else v_g.stage end;
  return v_b;
end $function$;

-- Called by the existing CASE member guard, in addition to all its exact
-- result/hold validations. One immutable subject can occur in only ONE leg.
create or replace function private.bpay_next_record_destination_member_v1(p_member private.bpay_next_transfer_member)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_l private.bpay_next_destination_group_leg%rowtype;v_g private.bpay_next_destination_group%rowtype;
  v_c private.bpay_next_net_destination_credit%rowtype;v_subject uuid;
begin
  select * into v_l from private.bpay_next_destination_group_leg where transfer_id=p_member.transfer_id;
  if not found then return;end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_l.anchor_transfer_id;
  if p_member.run_worker_id<>v_g.run_worker_id or v_g.stage in ('SEAL','COMPLETE') then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_MEMBER_SCOPE_INVALID';end if;
  if p_member.subject_kind='CASE' then
    select * into v_c from private.bpay_next_net_destination_credit
      where state_id=v_g.net_state_id and instruction_id=p_member.case_instruction_id;
    if (v_c.instruction_id is null and v_l.leg_kind<>'OWN')
       or (v_c.instruction_id is not null and (v_l.leg_kind<>'ONEOFF'
         or v_c.destination_id is distinct from v_l.destination_id
         or v_c.allocation_result_id is distinct from p_member.case_allocation_result_id
         or v_c.amount is distinct from p_member.signed_cash_contribution)) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_MEMBER_ROUTE_MISMATCH';end if;
    v_subject:=p_member.case_instruction_id;
  elsif p_member.subject_kind='WORK' then
    if v_l.leg_kind<>'OWN' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_WORK_NOT_OWN';end if;
    v_subject:=p_member.run_line_id;
  elsif p_member.subject_kind='NET_ADJUSTMENT' then
    if v_l.leg_kind<>'OWN' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_NOT_OWN';end if;
    v_subject:=v_g.run_worker_id;
  else raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_MEMBER_KIND_INVALID';end if;
  insert into private.bpay_next_destination_group_member(anchor_transfer_id,subject_kind,subject_id,transfer_id,member_no)
    values(v_g.anchor_transfer_id,p_member.subject_kind,v_subject,p_member.transfer_id,p_member.member_no);
end $function$;

create or replace function private.bpay_next_build_destination_group_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_cursor bigint;v_checkpoint bigint;
  v_run private.bpay_next_pay_run%rowtype;v_job private.bpay_next_job%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_anchor private.bpay_next_transfer%rowtype;v_leg private.bpay_next_transfer%rowtype;
  v_g private.bpay_next_destination_group%rowtype;v_p private.bpay_next_net_projection%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_draft private.bpay_next_case_allocation_state%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
  v_r private.bpay_next_case_allocation_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
  v_c private.bpay_next_net_destination_credit%rowtype;v_d private.bpay_next_net_destination_amount%rowtype;
  v_seen integer:=0;v_more boolean;v_signed numeric;v_leg_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or p_expected_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_PAGE_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_destination_group g on g.anchor_transfer_id=t.id
    join private.bpay_next_run_worker w on w.id=g.run_worker_id where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id where t.build_command_id=v_job.command_id for update of w;
  select * into strict v_anchor from private.bpay_next_transfer where build_command_id=v_job.command_id for update;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_anchor.id for update;
  select * into strict v_p from private.bpay_next_net_projection where id=v_g.projection_id;
  select * into strict v_n from private.bpay_next_net_destination_state where state_id=v_g.net_state_id;
  select * into strict v_draft from private.bpay_next_case_allocation_state where id=v_g.draft_state_id;
  if v_job.job_kind<>'TRANSFER_BUILD' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_g.candidate_id
     or v_anchor.run_worker_id<>v_g.run_worker_id or v_anchor.cash_amount<>v_n.own_amount
     or v_n.projection_id<>v_p.id or v_n.cash_amount<>v_p.cash_amount
     or v_anchor.beneficiary_kind<>'CANDIDATE' or v_anchor.beneficiary_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY' and v_g.stage='COMPLETE' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY','transfer_status',v_anchor.status,'transfer_id',v_anchor.id,
      'cursor',v_job.cursor_key,'member_count',v_anchor.member_count::text,'rows_visited','0','replay',true);end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.net_projection_revision<>v_p.projection_no or v_worker.net_request_revision<>v_p.projection_no
     or v_p.retired_at_utc is not null or v_anchor.account_approval_ref is not null
     or v_worker.preparation_revision<>v_g.preparation_revision or v_worker.case_selection_revision<>v_g.selection_revision
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_DESTINATION_BUILD_NOT_ELIGIBLE';end if;
  v_checkpoint:=nullif(v_g.checkpoint,0);v_cursor:=nullif(v_job.cursor_key,'')::bigint;
  if v_cursor is distinct from v_checkpoint then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CHECKPOINT_MISMATCH';end if;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)>coalesce(v_checkpoint,0) then
      raise exception using errcode='55000',message='BPAY_NEXT_DESTINATION_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase','BUILDING','transfer_id',v_anchor.id,'cursor',v_job.cursor_key,
      'member_count',v_anchor.member_count::text,'rows_visited','0','replay',true);end if;
  if v_g.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_g.work_cursor is null or line_no>v_g.work_cursor) order by line_no limit p_limit loop
      v_anchor.member_count:=v_anchor.member_count+1;v_anchor.member_cash_sum:=v_anchor.member_cash_sum+v_line.frozen_inc_vat;
      if v_line.source_pay_channel<>'PAYE' or v_line.target_pay_channel<>'PAYE' or v_line.frozen_vat<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_WORK_BASIS_INVALID';end if;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,run_line_id,signed_cash_contribution)
        values(v_anchor.id,v_worker.id,v_anchor.member_count,'WORK',v_line.id,v_line.frozen_inc_vat);
      v_g.processed_work_count:=v_g.processed_work_count+1;v_g.work_cash_total:=v_g.work_cash_total+v_line.frozen_inc_vat;
      v_g.member_cash_total:=v_g.member_cash_total+v_line.frozen_inc_vat;v_g.work_cursor:=v_line.line_no;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_g.work_cursor is null or line_no>v_g.work_cursor)) into v_more;
    if not v_more then
      if v_g.processed_work_count<>v_g.expected_work_count or v_g.work_cash_total<>v_draft.captured_work_gross then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_WORK_TOTAL_MISMATCH';end if;
      v_g.stage:='CASE';end if;
  elsif v_g.stage='CASE' then
    for v_i in select * from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_g.preparation_revision and selection_revision=v_g.selection_revision
      and (v_g.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_g.cursor_age_key,v_g.cursor_case_id,v_g.cursor_component_ordinal,v_g.cursor_case_component_id))
      order by age_key,case_id,component_ordinal,case_component_id limit p_limit loop
      select * into strict v_r from private.bpay_next_case_allocation_result where instruction_id=v_i.id
        and state_id=case when v_i.payroll_stage='NET_DEDUCT' then v_g.net_state_id else v_g.draft_state_id end;
      v_h:=null;
      if v_r.allocated_source_ex_vat>0 then
        select * into strict v_h from private.bpay_next_case_hold where allocation_result_id=v_r.id and status='ACTIVE' for share;
        if not exists(select 1 from private.bpay_next_case_capacity_use where case_hold_id=v_h.id and status='ACTIVE'
          and source_amount_ex_vat=v_r.allocated_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CAPACITY_MISMATCH';end if;
      end if;
      select * into v_c from private.bpay_next_net_destination_credit where state_id=v_g.net_state_id and instruction_id=v_i.id;
      if found then
        select * into strict v_d from private.bpay_next_net_destination_amount where destination_id=v_c.destination_id;
        select l.transfer_id into v_leg_id from private.bpay_next_destination_group_leg l where l.destination_id=v_d.destination_id;
        if not found then
          if v_g.created_leg_count>=2147483647 then raise exception using errcode='22023',message='BPAY_NEXT_DESTINATION_LEG_NUMBER_RANGE';end if;
          insert into private.bpay_next_transfer(run_worker_id,candidate_id,projection_id,transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
            values(v_worker.id,v_candidate,v_p.id,(v_g.created_leg_count+1)::integer,'CANDIDATE',v_candidate,v_d.amount,'BUILDING','BANK')
            returning id into v_leg_id;
          insert into private.bpay_next_destination_group_leg(transfer_id,anchor_transfer_id,destination_id,leg_kind)
            values(v_leg_id,v_anchor.id,v_d.destination_id,'ONEOFF');
          v_g.created_leg_count:=v_g.created_leg_count+1;
        end if;
        select * into strict v_leg from private.bpay_next_transfer where id=v_leg_id for update;
      else v_leg:=v_anchor;end if;
      v_signed:=case when v_i.direction='DEDUCTION' then -v_r.allocated_target_inc_vat else v_r.allocated_target_inc_vat end;
      v_leg.member_count:=v_leg.member_count+1;v_leg.member_cash_sum:=v_leg.member_cash_sum+v_signed;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,case_hold_id,
        case_instruction_id,case_allocation_result_id,signed_cash_contribution)
        values(v_leg.id,v_worker.id,v_leg.member_count,'CASE',v_h.id,v_i.id,v_r.id,v_signed);
      if v_leg.id=v_anchor.id then v_anchor:=v_leg;
      else update private.bpay_next_transfer set member_count=v_leg.member_count,member_cash_sum=v_leg.member_cash_sum where id=v_leg.id;end if;
      v_g.processed_case_count:=v_g.processed_case_count+1;v_g.member_cash_total:=v_g.member_cash_total+v_signed;
      if v_i.payroll_stage='GROSS_ADD' then v_g.gross_additions_total:=v_g.gross_additions_total+v_r.allocated_target_inc_vat;
      elsif v_i.payroll_stage='GROSS_DEDUCT' then v_g.gross_deductions_total:=v_g.gross_deductions_total+v_r.allocated_target_inc_vat;
      elsif v_i.payroll_stage='NET_ADD' then v_g.net_additions_total:=v_g.net_additions_total+v_r.allocated_target_inc_vat;
      else v_g.net_recoveries_total:=v_g.net_recoveries_total+v_r.allocated_target_inc_vat;end if;
      v_g.cursor_age_key:=v_i.age_key;v_g.cursor_case_id:=v_i.case_id;v_g.cursor_component_ordinal:=v_i.component_ordinal;
      v_g.cursor_case_component_id:=v_i.case_component_id;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_g.preparation_revision and selection_revision=v_g.selection_revision
      and (v_g.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_g.cursor_age_key,v_g.cursor_case_id,v_g.cursor_component_ordinal,v_g.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_g.processed_case_count<>v_g.expected_case_count or v_g.created_leg_count<>v_g.expected_leg_count
        or (v_g.gross_additions_total,v_g.gross_deductions_total,v_g.net_additions_total,v_g.net_recoveries_total)
          is distinct from (v_p.accepted_gross_additions,v_p.accepted_gross_deductions,v_p.accepted_net_additions,v_p.accepted_recoveries) then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CASE_TOTAL_MISMATCH';end if;
      v_g.stage:='ADJUST';end if;
  elsif v_g.stage='ADJUST' then
    if v_g.work_cash_total+v_g.gross_additions_total-v_g.gross_deductions_total<>v_p.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROSS_MISMATCH';end if;
    v_signed:=coalesce(v_p.entered_paye_net,0)-v_p.gross_inc_vat;
    v_anchor.member_count:=v_anchor.member_count+1;v_anchor.member_cash_sum:=v_anchor.member_cash_sum+v_signed;
    insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,signed_cash_contribution)
      values(v_anchor.id,v_worker.id,v_anchor.member_count,'NET_ADJUSTMENT',v_signed);
    v_g.member_cash_total:=v_g.member_cash_total+v_signed;
    if v_g.member_cash_total<>v_p.cash_amount or v_anchor.member_cash_sum<>v_n.own_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_CASH_MISMATCH';end if;
    v_g.stage:='SEAL';v_seen:=1;
  elsif v_g.stage='SEAL' then
    for v_leg in select t.* from private.bpay_next_transfer t where t.run_worker_id=v_worker.id
      and (v_g.seal_cursor is null or t.transfer_no>v_g.seal_cursor) order by t.transfer_no limit p_limit loop
      if v_leg.status<>'BUILDING' or v_leg.member_cash_sum<>v_leg.cash_amount
        or not exists(select 1 from private.bpay_next_destination_group_leg where transfer_id=v_leg.id and anchor_transfer_id=v_anchor.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_LEG_SEAL_MISMATCH';end if;
      update private.bpay_next_transfer set status='MEMBERS_READY' where id=v_leg.id;
      v_g.seal_cursor:=v_leg.transfer_no;v_g.sealed_leg_count:=v_g.sealed_leg_count+1;
      v_g.sealed_cash_total:=v_g.sealed_cash_total+v_leg.cash_amount;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id
      and (v_g.seal_cursor is null or transfer_no>v_g.seal_cursor)) into v_more;
    if not v_more then
      if v_g.sealed_leg_count<>v_g.expected_leg_count or v_g.sealed_cash_total<>v_p.cash_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_GROUP_SEAL_MISMATCH';end if;
      v_g.stage:='COMPLETE';v_g.completed_at_utc:=pg_catalog.transaction_timestamp();end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STAGE_INVALID';end if;
  -- Never turn a already sealed anchor back into BUILDING during leg sealing.
  if v_g.seal_cursor is null then
    update private.bpay_next_transfer set member_count=v_anchor.member_count,member_cash_sum=v_anchor.member_cash_sum where id=v_anchor.id;
  end if;
  v_cursor:=v_g.checkpoint+1;
  update private.bpay_next_destination_group set stage=v_g.stage,checkpoint=v_cursor,
    processed_work_count=v_g.processed_work_count,processed_case_count=v_g.processed_case_count,created_leg_count=v_g.created_leg_count,
    sealed_leg_count=v_g.sealed_leg_count,work_cursor=v_g.work_cursor,cursor_age_key=v_g.cursor_age_key,cursor_case_id=v_g.cursor_case_id,
    cursor_component_ordinal=v_g.cursor_component_ordinal,cursor_case_component_id=v_g.cursor_case_component_id,seal_cursor=v_g.seal_cursor,
    work_cash_total=v_g.work_cash_total,gross_additions_total=v_g.gross_additions_total,gross_deductions_total=v_g.gross_deductions_total,
    net_additions_total=v_g.net_additions_total,net_recoveries_total=v_g.net_recoveries_total,member_cash_total=v_g.member_cash_total,
    sealed_cash_total=v_g.sealed_cash_total,completed_at_utc=v_g.completed_at_utc where anchor_transfer_id=v_anchor.id;
  update private.bpay_next_job set cursor_key=v_cursor::text,status=case when v_g.stage='COMPLETE' then 'DONE' else 'LEASED' end,
    phase=case when v_g.stage='COMPLETE' then 'MEMBERS_READY' else 'WORK' end,
    lease_nonce=case when v_g.stage='COMPLETE' then null else lease_nonce end,
    lease_until_utc=case when v_g.stage='COMPLETE' then null else lease_until_utc end where id=p_job_id;
  if v_g.stage='COMPLETE' then update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;end if;
  return pg_catalog.jsonb_build_object('phase',case when v_g.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end,
    'transfer_id',v_anchor.id,'cursor',v_cursor::text,'member_count',v_anchor.member_count::text,'rows_visited',v_seen::text,'replay',false);
end $function$;

do $guards$
declare n text;
begin
  foreach n in array array['bpay_next_stored_credit_origin','bpay_next_instruction_destination',
    'bpay_next_net_destination_credit','bpay_next_destination_group_leg','bpay_next_destination_group_member'] loop
    execute pg_catalog.format('drop trigger if exists bpay_next_destination_immutable_v1 on private.%I',n);
    execute pg_catalog.format('create trigger bpay_next_destination_immutable_v1 before update or delete on private.%I '
      ||'for each row execute function private.bpay_next_effect_immutable_v1()',n);
  end loop;
end $guards$;

alter function public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid) from public,anon,authenticated;
grant execute on function public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid) to service_role;
revoke all on function private.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid),
  private.bpay_next_stored_credit_legacy_guard_v1(),private.bpay_next_capture_instruction_destination_v1(uuid),
  private.bpay_next_accumulate_net_destination_v1(uuid,uuid,uuid),private.bpay_next_seal_net_destination_v1(uuid,uuid)
  ,private.bpay_next_destination_build_binding_v1(uuid),private.bpay_next_record_destination_member_v1(private.bpay_next_transfer_member),
  private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)
  ,private.bpay_next_destination_state_guard_v1(),private.bpay_next_destination_group_guard_v1()
  ,private.bpay_next_destination_leg_guard_v1()
  from public,anon,authenticated,service_role;

do $owners$
declare s text;
begin
  foreach s in array array['private.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid)',
    'private.bpay_next_stored_credit_legacy_guard_v1()',
    'private.bpay_next_capture_instruction_destination_v1(uuid)',
    'private.bpay_next_accumulate_net_destination_v1(uuid,uuid,uuid)',
    'private.bpay_next_seal_net_destination_v1(uuid,uuid)',
    'private.bpay_next_destination_build_binding_v1(uuid)',
    'private.bpay_next_record_destination_member_v1(private.bpay_next_transfer_member)',
    'private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)',
    'private.bpay_next_destination_state_guard_v1()',
    'private.bpay_next_destination_group_guard_v1()',
    'private.bpay_next_destination_leg_guard_v1()'] loop
    execute pg_catalog.format('alter function %s owner to postgres',s);
  end loop;
end $owners$;

notify pgrst,'reload schema';
commit;
