-- Genuine ordered PAYE NET projection from the completed frozen case Draft.
-- The caller is the existing PAYE_NET_ENTRY lane, not a new calculator/job.
-- Gross instructions are unchanged; only own unpaid NET capacity is resized.
\set ON_ERROR_STOP on
begin;

drop trigger if exists bpay_next_net_request_immutable_v1 on private.bpay_next_paye_net_request;
create trigger bpay_next_net_request_immutable_v1 before update or delete
  on private.bpay_next_paye_net_request for each row
  execute function private.bpay_next_effect_immutable_v1();

create or replace function private.bpay_next_accept_case_net_request_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_input_kind text,
  p_entered_paye_net numeric,p_expected_projection_revision bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_sequence bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;
  v_prior private.bpay_next_paye_net_request%rowtype;
begin
  if p_command_id is null or p_run_worker_id is null or p_actor_user_id is null
     or p_input_kind is null or p_input_kind not in ('PAYE_MANUAL','CASE_PAYOUT')
     or p_expected_projection_revision is null or p_expected_projection_revision<0
     or p_expected_projection_revision=9223372036854775807
     or (p_input_kind='PAYE_MANUAL' and (p_entered_paye_net is null
       or p_entered_paye_net::text in ('NaN','Infinity','-Infinity') or p_entered_paye_net<0
       or p_entered_paye_net>=10000000000000000 or p_entered_paye_net<>pg_catalog.trunc(p_entered_paye_net,2)))
     or (p_input_kind='CASE_PAYOUT' and p_entered_paye_net is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
      and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_CASE_NET_ACTOR_INVALID';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-case-net:'||p_command_id::text,0));
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_run_worker where id=p_run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_worker from private.bpay_next_run_worker where id=p_run_worker_id for update;
  select * into v_prior from private.bpay_next_paye_net_request where command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>p_run_worker_id or v_prior.actor_user_id is distinct from p_actor_user_id
       or v_prior.input_kind<>p_input_kind or v_prior.entered_paye_net is distinct from p_entered_paye_net
       or v_prior.expected_projection_revision is distinct from p_expected_projection_revision
       or v_prior.case_draft_state_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='PAYE_NET_ENTRY';
    return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_prior.request_no::text,
      'replay',true,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id);
  end if;
  select s.* into strict v_draft from private.bpay_next_case_allocation_state s
    where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.preparation_revision
      and s.selection_revision=v_worker.case_selection_revision and s.pass_kind='DRAFT' and s.projection_no=0;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
     or v_worker.gross_ex_vat<>v_worker.gross_inc_vat or v_worker.gross_inc_vat<0
     or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or v_worker.case_pending_binding_count<>0
     or v_worker.net_projection_revision<>p_expected_projection_revision
     or v_worker.net_request_revision<>v_worker.net_projection_revision
     or v_worker.realised_effect_count<>0
     or v_draft.captured_work_gross+v_draft.allocated_gross_additions-v_draft.allocated_gross_deductions<>v_worker.gross_inc_vat
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_NOT_ELIGIBLE';
  end if;
  if p_input_kind='PAYE_MANUAL' then
    if p_entered_paye_net>v_worker.gross_inc_vat or exists(select 1 from private.bpay_next_case_payout_basis
      where run_worker_id=v_worker.id and preparation_revision=v_worker.preparation_revision) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_PAYROLL_BASIS_INVALID';
    end if;
  else
    if p_expected_projection_revision<>0 or v_worker.captured_line_count<>0 or v_worker.gross_inc_vat<>0
       or not exists(select 1 from private.bpay_next_case_payout_basis b where b.run_worker_id=v_worker.id
         and b.preparation_revision=v_worker.preparation_revision and b.status='SEALED'
         and b.projection_id is null and b.target_total_inc_vat>0
         and b.target_total_inc_vat=v_draft.allocated_net_additions and b.beneficiary_id=v_worker.candidate_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_PAYOUT_BASIS_INVALID';
    end if;
  end if;
  if p_expected_projection_revision>0 and not exists(select 1 from private.bpay_next_net_projection p
      where p.run_worker_id=v_worker.id and p.projection_no=p_expected_projection_revision
        and p.retired_at_utc is null and p.input_kind='PAYE_MANUAL') then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_PRIOR_PROJECTION_INVALID';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request(command_id,run_worker_id,candidate_id,request_no,
    entered_paye_net,frozen_gross_inc_vat,input_kind,actor_user_id,case_draft_state_id,
    preparation_revision,selection_revision,expected_projection_revision)
    values(p_command_id,v_worker.id,v_worker.candidate_id,p_expected_projection_revision+1,
      p_entered_paye_net,v_worker.gross_inc_vat,p_input_kind,p_actor_user_id,v_draft.id,
      v_worker.preparation_revision,v_worker.case_selection_revision,p_expected_projection_revision);
  update private.bpay_next_run_worker set net_request_revision=p_expected_projection_revision+1 where id=v_worker.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',(p_expected_projection_revision+1)::text,
    'replay',false,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id);
end
$function$;

create or replace function private.bpay_next_accept_case_paye_net_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_entered_paye_net numeric,
  p_expected_projection_revision bigint
) returns jsonb language sql security invoker set search_path=pg_catalog,private
as $function$
  select private.bpay_next_accept_case_net_request_v1(p_command_id,p_run_worker_id,p_actor_user_id,
    'PAYE_MANUAL',p_entered_paye_net,p_expected_projection_revision)
$function$;
create or replace function private.bpay_next_accept_case_payout_projection_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_expected_projection_revision bigint
) returns jsonb language sql security invoker set search_path=pg_catalog,private
as $function$
  select private.bpay_next_accept_case_net_request_v1(p_command_id,p_run_worker_id,p_actor_user_id,
    'CASE_PAYOUT',null,p_expected_projection_revision)
$function$;

-- One exact projection read protects the maintained contribution from mixing
-- entered payroll net, bank cash, another worker and payout-only earnings.
create or replace function private.bpay_next_week_projection_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_projection private.bpay_next_net_projection%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;v_eligible_cash numeric;
begin
  if new.accepted_projection_id is null then return new;end if;
  select * into strict v_projection from private.bpay_next_net_projection where id=new.accepted_projection_id;
  if v_projection.run_worker_id<>new.original_run_worker_id
     or not exists(select 1 from private.bpay_next_run_worker w
       where w.id=new.original_run_worker_id and w.candidate_id=new.candidate_id)
     or v_projection.cash_amount is distinct from new.accepted_bank_cash
     or v_projection.gross_inc_vat<>new.original_gross_amount
     or (v_projection.input_kind='CASE_PAYOUT' and (new.basis_kind<>'GROSS_FALLBACK'
       or new.original_payroll_net is not null or new.eligibility_state<>'EXCLUDED' or new.eligible_arranged_amount<>0))
     or (v_projection.input_kind<>'CASE_PAYOUT' and (new.basis_kind<>'PAYROLL_NET'
       or new.original_payroll_net is distinct from v_projection.entered_paye_net)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ACCEPTED_PROJECTION_MISMATCH';
  end if;
  -- Unique projection lookup, never a current WORK/Timesheet or historical
  -- reconstruction. 1827 seals this exact fact BEFORE the contribution switch.
  -- Its NET allocation owner can still be BUILDING in the same transaction.
  v_eligible_cash:=v_projection.cash_amount;
  select * into v_destination from private.bpay_next_net_destination_state
    where projection_id=v_projection.id;
  if found then
    if v_destination.run_worker_id<>new.original_run_worker_id
       or v_destination.projection_id is distinct from new.accepted_projection_id
       or v_destination.completed_at_utc is null
       or v_destination.external_leg_count<1 or v_destination.external_credit_count<1
       or v_destination.external_amount<=0 or v_destination.own_amount is null or v_destination.own_amount<0
       or v_destination.cash_amount is distinct from v_projection.cash_amount
       or v_destination.own_amount+v_destination.external_amount is distinct from v_destination.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_DESTINATION_BASIS_MISMATCH';
    end if;
    v_eligible_cash:=v_destination.own_amount;
  end if;
  if new.eligibility_state='ELIGIBLE'
     and new.eligible_arranged_amount is distinct from v_eligible_cash then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ELIGIBLE_DESTINATION_MISMATCH';
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_week_projection_guard_v1 on private.bpay_next_worker_week_contribution;
create trigger bpay_next_week_projection_guard_v1 before insert or update
  on private.bpay_next_worker_week_contribution for each row
  execute function private.bpay_next_week_projection_guard_v1();

create or replace function private.bpay_next_net_projection_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
begin
  if tg_op='DELETE' or old.retired_at_utc is not null or new.retired_at_utc is null
     or not pg_catalog.isfinite(new.retired_at_utc)
     or (pg_catalog.to_jsonb(new)-'retired_at_utc') is distinct from (pg_catalog.to_jsonb(old)-'retired_at_utc') then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_NET_PROJECTION_IMMUTABLE';
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_net_projection_guard_v1 on private.bpay_next_net_projection;
create trigger bpay_next_net_projection_guard_v1 before update or delete
  on private.bpay_next_net_projection for each row execute function private.bpay_next_net_projection_guard_v1();

create or replace function private.bpay_next_apply_case_paye_net_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint default null,
  p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_control_epoch bigint;v_checkpoint bigint;
  v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_paye_net_request%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_state private.bpay_next_case_allocation_state%rowtype;
  v_week private.bpay_next_worker_week%rowtype;v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_contribution private.bpay_next_worker_week_contribution%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_draft_result private.bpay_next_case_allocation_result%rowtype;
  v_own private.bpay_next_case_hold%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_period private.bpay_next_case_period%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_seen integer:=0;v_more boolean;v_issue text;v_terminal boolean:=false;v_requires_week boolean;
  v_e numeric;v_h numeric;v_own_amount numeric;v_capacity numeric;v_taken numeric;v_cash numeric;v_own_cash numeric;
  v_outstanding numeric;v_case_outstanding numeric;v_row record;v_result_id uuid;v_hold_id uuid;v_projection_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or (p_expected_cursor is not null and p_expected_cursor<1) or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_PAGE_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_paye_net_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.candidate_id=j.candidate_id
    where j.id=p_job_id and r.case_draft_state_id is not null;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select active_owner_epoch into strict v_control_epoch from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_paye_net_request where command_id=v_job.command_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_draft from private.bpay_next_case_allocation_state where id=v_request.case_draft_state_id;
  select * into v_state from private.bpay_next_case_allocation_state where job_id=p_job_id and pass_kind='NET' for update;
  if v_job.job_kind<>'PAYE_NET_ENTRY' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_request.candidate_id<>v_candidate or v_draft.run_worker_id<>v_worker.id or v_draft.pass_kind<>'DRAFT'
     or v_draft.preparation_revision<>v_request.preparation_revision or v_draft.selection_revision<>v_request.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_JOB_SCOPE_INVALID';
  end if;
  -- An immutable completed receipt survives later lane owners and net saves.
  if v_job.status='DONE' and v_state.id is not null and v_state.net_stage in ('COMPLETE','REVIEW') then
    return pg_catalog.jsonb_build_object('phase',case when v_state.net_stage='COMPLETE' then 'PROJECTED' else 'REVIEW' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_state.projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_state.net_issue_code);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_run.status<>'DRAFT'
     or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.preparation_revision<>v_request.preparation_revision or v_worker.case_selection_revision<>v_request.selection_revision
     or v_worker.net_request_revision<>v_request.request_no or v_worker.net_projection_revision<>v_request.expected_projection_revision
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0 or v_worker.gross_inc_vat<>v_request.frozen_gross_inc_vat
     or v_worker.realised_effect_count<>0 or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or exists(select 1 from private.bpay_next_cancel_request r join private.bpay_next_command c on c.id=r.command_id
       where r.run_worker_id=v_worker.id and (r.status='CANCELLING'
         or (r.status='REQUESTED' and c.agency_sequence<v_job.command_sequence)))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_OWNER_NOT_ELIGIBLE';
  end if;
  v_checkpoint:=v_job.cursor_key::bigint;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)<coalesce(v_checkpoint,0) then
      return pg_catalog.jsonb_build_object('phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,'rows_visited','0',
        'replay',true,'projection_id',null,'run_worker_id',v_worker.id,'issue_code',null);
    end if;
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_CURSOR_STALE';
  end if;

  <<net_page>>
  begin
    if v_state.id is null then
      select * into strict v_week from private.bpay_next_worker_week
        where candidate_id=v_candidate and pay_week_start=v_draft.pay_week_start for update;
      select exists(select 1 from private.bpay_next_run_case_instruction i
        join private.bpay_next_case_allocation_result a on a.instruction_id=i.id and a.state_id=v_draft.id
        where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
          and i.selection_revision=v_request.selection_revision and i.payroll_stage='NET_DEDUCT'
          and i.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and a.allocated_source_ex_vat>0) into v_requires_week;
      insert into private.bpay_next_case_allocation_state(run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,
        pass_kind,projection_no,command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
        expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,pay_week_start,captured_week_revision,
        captured_work_gross,net_stage,net_requires_week_binding)
        values(v_worker.id,v_run_id,v_candidate,v_request.selection_revision,v_request.preparation_revision,'NET',v_request.request_no,
          v_job.command_id,v_job.id,v_epoch,p_owner_epoch,
          (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),'BUILDING',
          case when v_requires_week then 'UNBOUND' else 'NOT_AFFECTED' end,v_draft.expected_instruction_count,
          coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0),v_draft.pay_week_start,v_week.period_revision,
          v_draft.captured_work_gross,case when v_requires_week then 'WEEK_CAPTURE' else 'ALLOCATE' end,v_requires_week)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0));
      -- Only arrangements BEFORE this run affect its floor. A later return
      -- cannot invalidate an earlier run merely through a whole-week counter.
      -- Exact prior UNBOUND rows are handled by the bounded week pages below.
      if v_requires_week and exists(
        select 1 from private.bpay_next_run_worker w join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_draft.pay_week_start and r.pay_date<v_draft.pay_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit net_page;
      end if;
    elsif v_state.status<>'BUILDING' or v_state.net_stage='NOT_CASE_NET' or v_state.projection_no<>v_request.request_no then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STATE_INVALID';
    end if;
    if v_state.owner_epoch<>p_owner_epoch then
      update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
    end if;
    select * into strict v_week from private.bpay_next_worker_week
      where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start for update;
    if v_state.net_requires_week_binding and v_week.period_revision<>v_state.captured_week_revision then
      v_issue:='CASE_WEEK_BASIS_UNBOUND';exit net_page;
    end if;
    if v_state.net_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID') then 'CASE_RETURN_FLOOR_UNBOUND'
            else 'CASE_WEEK_BASIS_UNBOUND' end;exit net_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_more then 'UNBOUND' else 'BOUND' end,
        net_stage=case when v_more then 'WEEK_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      -- Week capture and allocation never share a >limit step.
      exit net_page;
    end if;
    if v_state.net_stage<>'ALLOCATE' or v_state.weekly_binding_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_ALLOCATION_UNBOUND';
    end if;
    select * into strict v_channel from private.bpay_next_case_allocation_channel
      where state_id=v_state.id and allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
        and i.selection_revision=v_request.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      v_seen:=v_seen+1;
      select * into strict v_draft_result from private.bpay_next_case_allocation_result
        where state_id=v_draft.id and instruction_id=v_instruction.id;
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE'
         or v_instruction.allocation_channel<>'PAYE' or v_instruction.currency<>'GBP'
         or v_instruction.nominal_target_vat<>0 or v_draft_result.allocated_target_vat<>0
         or v_draft_result.allocated_source_ex_vat<>v_draft_result.allocated_target_ex_vat then
        v_issue:='CASE_INPUT_UNBOUND';exit net_page;
      end if;
      select * into v_own from private.bpay_next_case_hold where instruction_id=v_instruction.id and status='ACTIVE' for update;
      v_own_amount:=coalesce(v_own.source_reserved_ex_vat,0);
      if v_own.id is not null and (v_own.run_worker_id<>v_worker.id or v_own.purpose<>v_instruction.hold_purpose
          or v_own.source_reserved_ex_vat<>v_own.target_amount_ex_vat
          or v_own.target_amount_vat<>0 or not exists(select 1 from private.bpay_next_case_capacity_use u
            where u.case_hold_id=v_own.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own_amount)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_HOLD_INVALID';
      end if;
      if v_instruction.payroll_stage<>'NET_DEDUCT' then
        -- Gross deductions/additions and payout additions remain the exact
        -- Draft result/hold. They neither enter the kernel nor seed E/H.
        if v_own_amount<>v_draft_result.allocated_source_ex_vat
           or (v_own_amount>0 and (v_own.allocation_result_id<>v_draft_result.id or v_own.allocation_pass_kind<>'DRAFT')) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_GROSS_OR_ADDITION_CHANGED';
        end if;
        if v_instruction.payroll_stage='GROSS_ADD' then
          v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='GROSS_DEDUCT' then
          v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='NET_ADD' then
          v_state.allocated_net_additions:=v_state.allocated_net_additions+v_draft_result.allocated_target_inc_vat;
          perform private.bpay_next_accumulate_net_destination_v1(v_state.id,v_instruction.id,v_draft_result.id);
        else raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STAGE_INVALID';end if;
      else
        select * into strict v_case from private.bpay_next_finance_case where id=v_instruction.case_id for update;
        select * into strict v_component from private.bpay_next_case_component where id=v_instruction.case_component_id for update;
        select * into strict v_period from private.bpay_next_case_period
          where case_component_id=v_instruction.case_component_id and pay_week_start=v_instruction.pay_week_start for update;
        if v_own_amount>v_component.active_recovery_source_ex_vat
           or v_own_amount>v_case.active_recovery_hold_amount
           or v_own_amount>v_period.active_unrealised_recovery_source_ex_vat
           or (v_own.id is not null and not (
             (v_request.expected_projection_revision=0 and v_own.allocation_pass_kind='DRAFT'
               and v_own.allocation_result_id=v_draft_result.id)
             or (v_request.expected_projection_revision>0 and v_own.allocation_pass_kind='NET' and exists(
               select 1 from private.bpay_next_case_allocation_result a
                 join private.bpay_next_case_allocation_state s on s.id=a.state_id
               where a.id=v_own.allocation_result_id and s.run_worker_id=v_worker.id
                 and s.pass_kind='NET' and s.projection_no=v_request.expected_projection_revision
                 and s.status='COMPLETE' and s.net_stage='COMPLETE' and s.projection_id is not null)))) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_SURVIVING_CAPACITY_INVALID';
        end if;
        -- Current disjoint CAPACITY only, never a current rule, valuation,
        -- schedule, nominal, tax, channel or changed-case fallback.
        v_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off;
        v_capacity:=least(v_draft_result.allocated_source_ex_vat,
          greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_own_amount,0));
        if v_own_amount>v_draft_result.allocated_source_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_DRAFT_CAPACITY_EXCEEDED';
        end if;
        select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
          v_instruction.nominal_target_ex_vat,v_capacity,v_e,v_h,v_instruction.minimum_earnings_threshold,
          v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
        v_taken:=v_row.taken_amount;
        -- 0310 requires release/new exact rows, not an amount rewrite. Old
        -- reservation and all three active counters move in this same page.
        if v_own.id is not null then
          update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=pg_catalog.transaction_timestamp() where id=v_own.id;
          update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_own.id;
        end if;
        insert into private.bpay_next_case_allocation_result(state_id,instruction_id,run_worker_id,candidate_id,
          preparation_revision,selection_revision,result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,
          usable_capacity_target_ex_vat,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,
          shortfall_target_ex_vat,cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,
          worker_take_home_before,worker_take_home_after)
          values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_request.preparation_revision,v_request.selection_revision,
            v_state.processed_instruction_count+1,'NET','PAYE','NET_RECOVERY_CAPACITY',v_instruction.nominal_target_ex_vat,
            v_capacity,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,v_row.cap_reason,
            v_row.affordability_reason,v_row.capacity_reason,v_e,v_row.next_channel_headroom,v_h,v_row.next_worker_take_home)
          returning id into v_result_id;
        if v_taken>0 then
          insert into private.bpay_next_case_hold(run_worker_id,candidate_id,case_id,amount,status,instruction_id,
            allocation_result_id,allocation_pass_kind,case_component_id,purpose,pay_week_start,source_reserved_ex_vat,
            target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
            values(v_worker.id,v_candidate,v_instruction.case_id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'NET',
              v_instruction.case_component_id,'NET_RECOVERY_CAPACITY',v_instruction.pay_week_start,v_taken,v_taken,0,v_taken)
            returning id into v_hold_id;
          insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,pay_week_start,
            purpose,status,source_amount_ex_vat) values(v_hold_id,v_instruction.case_component_id,v_instruction.case_id,v_candidate,
              v_instruction.pay_week_start,'NET_RECOVERY_CAPACITY','ACTIVE',v_taken);
        end if;
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_own_amount+v_taken,
          component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_own_amount+v_taken,
          active_hold_amount=active_hold_amount-v_own_amount+v_taken,case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_own_amount+v_taken,
          period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count
          -case when v_own.id is null then 0 else 1 end+case when v_taken>0 then 1 else 0 end where id=v_worker.id;
        v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
        v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        v_e:=v_row.next_channel_headroom;v_h:=v_row.next_worker_take_home;
      end if;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_request.preparation_revision and i.selection_revision=v_request.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count
         or (v_state.allocated_gross_additions,v_state.allocated_gross_deductions,v_state.allocated_net_additions)
           is distinct from (v_draft.allocated_gross_additions,v_draft.allocated_gross_deductions,v_draft.allocated_net_additions) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_TOTALS_MISMATCH';
      end if;
      v_cash:=coalesce(v_request.entered_paye_net,0)+v_state.allocated_net_additions-v_state.recovered_target_total;
      if v_cash<0 or v_cash>=10000000000000000 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_CASH_RANGE_INVALID';
      end if;
      select * into strict v_contribution from private.bpay_next_worker_week_contribution where original_run_worker_id=v_worker.id for update;
      if v_contribution.eligibility_state='UNBOUND' or v_contribution.payment_state<>'ARRANGED'
         or v_contribution.original_gross_amount<>v_request.frozen_gross_inc_vat then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_WEEK_BASIS_INVALID';
      end if;
      insert into private.bpay_next_net_projection(run_worker_id,request_command_id,projection_no,input_kind,
        gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount,
        accepted_net_additions,accepted_gross_additions,accepted_gross_deductions)
        values(v_worker.id,v_job.command_id,v_request.request_no,v_request.input_kind,v_worker.gross_ex_vat,0,
          v_worker.gross_inc_vat,v_request.entered_paye_net,v_state.recovered_target_total,v_cash,
          v_state.allocated_net_additions,v_state.allocated_gross_additions,v_state.allocated_gross_deductions)
        returning id into v_projection_id;
      v_own_cash:=private.bpay_next_seal_net_destination_v1(v_state.id,v_projection_id);
      if v_request.expected_projection_revision>0 then
        update private.bpay_next_net_projection set retired_at_utc=pg_catalog.transaction_timestamp()
          where run_worker_id=v_worker.id and projection_no=v_request.expected_projection_revision and retired_at_utc is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_PRIOR_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_worker_week_contribution set
        basis_kind=case when v_request.input_kind='CASE_PAYOUT' then 'GROSS_FALLBACK' else 'PAYROLL_NET' end,
        original_payroll_net=v_request.entered_paye_net,accepted_projection_id=v_projection_id,accepted_bank_cash=v_cash,
        eligibility_state=case when v_request.input_kind='CASE_PAYOUT' then 'EXCLUDED' else 'ELIGIBLE' end,
        eligible_arranged_amount=case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1
        where id=v_contribution.id;
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home
        -v_contribution.eligible_arranged_amount+case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      if v_request.input_kind='CASE_PAYOUT' then
        update private.bpay_next_case_payout_basis set projection_id=v_projection_id
          where run_worker_id=v_worker.id and preparation_revision=v_request.preparation_revision and projection_id is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_PAYOUT_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_case_allocation_state set status='COMPLETE',net_stage='COMPLETE',projection_id=v_projection_id,
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set entered_paye_net=v_request.entered_paye_net,
        net_projection_revision=v_request.request_no where id=v_worker.id;
      update private.bpay_next_job set status='DONE',phase='PROJECTED',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      v_terminal:=true;
    end if;
  end net_page;
  if v_issue is not null then
    update private.bpay_next_case_allocation_state set net_stage='REVIEW',net_issue_code=v_issue,weekly_binding_state='UNBOUND',
      status='OUTDATED' where id=v_state.id;
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      case_pending_binding_count=1,financial_resolution_count=financial_resolution_count+1 where id=v_worker.id;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(coalesce(v_checkpoint,0)+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  return pg_catalog.jsonb_build_object('phase',case when v_issue is not null then 'REVIEW'
      when v_terminal then 'PROJECTED' else 'FINANCE_ALLOCATE' end,'cursor',v_job.cursor_key,
    'rows_visited',v_seen::text,'replay',false,'projection_id',v_projection_id,'run_worker_id',v_worker.id,'issue_code',v_issue);
end
$function$;

alter function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) owner to postgres;
alter function private.bpay_next_accept_case_paye_net_v1(uuid,uuid,uuid,numeric,bigint) owner to postgres;
alter function private.bpay_next_accept_case_payout_projection_v1(uuid,uuid,uuid,bigint) owner to postgres;
alter function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
alter function private.bpay_next_week_projection_guard_v1() owner to postgres;
alter function private.bpay_next_net_projection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint),
  private.bpay_next_accept_case_paye_net_v1(uuid,uuid,uuid,numeric,bigint),
  private.bpay_next_accept_case_payout_projection_v1(uuid,uuid,uuid,bigint),
  private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer),
  private.bpay_next_week_projection_guard_v1(),private.bpay_next_net_projection_guard_v1() from public,anon,authenticated,service_role;
commit;
