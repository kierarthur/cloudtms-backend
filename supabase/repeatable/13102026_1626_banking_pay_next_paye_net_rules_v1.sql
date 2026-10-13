-- DR23 exact latest-parent NET preview/audit/admission; no Source or producer changes.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_paye_net_entry_in_progress_v1(p_run_worker_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select coalesce((select w.target_pay_channel='PAYE' and
  (w.net_request_revision<>w.net_projection_revision or w.net_replacement_pending_id is not null
   or exists(select 1 from private.bpay_next_paye_net_request q
    join private.bpay_next_job j on j.command_id=q.command_id and j.job_kind='PAYE_NET_ENTRY'
    where q.run_worker_id=w.id and j.status<>'DONE'))
  from private.bpay_next_run_worker w where w.id=p_run_worker_id),false)
$function$;

create or replace function private.bpay_next_accept_simple_paye_net_v1(
  p_command_id uuid,p_run_worker_id uuid,p_entered_paye_net numeric,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_prior private.bpay_next_paye_net_request%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_run_worker_id is null
     or p_entered_paye_net is null
     or p_entered_paye_net<0
     or p_entered_paye_net<>pg_catalog.round(p_entered_paye_net,2) then
    raise exception using errcode='22023',message='BPAY_NEXT_PAYE_NET_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;

  if p_actor_user_id is not null and not exists(select 1 from public.tms_users u
    where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_PAYE_NET_ACTOR_INVALID';
  end if;
  -- The immutable worker identity locates its parent; lock the shared run
  -- header before this worker, as confirmation/cancellation owners do.
  select run_id into strict v_run_id from private.bpay_next_run_worker
    where id=p_run_worker_id;
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.id=p_run_worker_id for update;

  -- Exact retry wins even if an ensuing owner has changed the run state.
  select q.* into v_prior from private.bpay_next_paye_net_request q
    where q.command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>p_run_worker_id
       or v_prior.candidate_id<>v_worker.candidate_id
       or v_prior.case_draft_state_id is not null
       or v_prior.entered_paye_net<>p_entered_paye_net
       or (v_prior.actor_user_id is not null and v_prior.actor_user_id is distinct from p_actor_user_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PAYE_NET_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='PAYE_NET_ENTRY';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'request_no',v_prior.request_no,'replay',true,
      'phase','ACCEPTED_PENDING_PROJECTION');
  end if;

  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_worker.gross_vat<>0
     or v_worker.gross_inc_vat<0
     or p_entered_paye_net>v_worker.gross_inc_vat
     or v_worker.net_request_revision<>0
     or v_worker.net_projection_revision<>0
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_PAYE_NET_NOT_ELIGIBLE';
  end if;

  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request
    (command_id,run_worker_id,candidate_id,request_no,
     entered_paye_net,frozen_gross_inc_vat,actor_user_id)
    values(p_command_id,v_worker.id,v_worker.candidate_id,1,
           p_entered_paye_net,v_worker.gross_inc_vat,p_actor_user_id);
  update private.bpay_next_run_worker
    set net_request_revision=1 where id=v_worker.id;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'request_no',1,'replay',false,
    'phase','ACCEPTED_PENDING_PROJECTION');
end
$function$;

create or replace function private.bpay_next_accept_simple_paye_net_v1(
  p_command_id uuid,p_run_worker_id uuid,p_entered_paye_net numeric
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  -- Historical owner-only compatibility retains its original NULL actor.
  -- Authenticated intake always calls the four-argument audit owner.
  return private.bpay_next_accept_simple_paye_net_v1(p_command_id,p_run_worker_id,p_entered_paye_net,null);
end
$function$;

create or replace function private.bpay_next_accept_case_net_request_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_input_kind text,
  p_entered_paye_net numeric,p_expected_projection_revision bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
begin
  -- Existing CASE_PAYOUT and historical NULL-expectation replay stay exact.
  return private.bpay_next_accept_case_net_request_v1(p_command_id,p_run_worker_id,p_actor_user_id,p_input_kind,
    p_entered_paye_net,p_expected_projection_revision,null);
end
$function$;

create or replace function private.bpay_next_begin_simple_transfer_v1(
  p_command_id uuid,p_run_worker_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_sequence bigint;
  v_candidate_id uuid;
  v_case_selection_revision bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_BEGIN_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select run_id,candidate_id,case_selection_revision
    into strict v_run_id,v_candidate_id,v_case_selection_revision from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_case_selection_revision>0 then
    return private.bpay_next_begin_case_transfer_v1(p_command_id,p_run_worker_id);
  end if;
  select dr37_point_131.* into strict v_run from private.bpay_next_pay_run dr37_point_131
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select dr37_point_132.* into strict v_worker from private.bpay_next_run_worker dr37_point_132
    where id=p_run_worker_id for update;
  select dr37_point_133.* into v_transfer from private.bpay_next_transfer dr37_point_133
    where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>p_run_worker_id
       or v_transfer.candidate_id<>v_worker.candidate_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BEGIN_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence
      from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'transfer_id',v_transfer.id,'phase',v_transfer.status,'replay',true);
  end if;
  -- Exact replay precedes new admission. Maintained channel counts only.
  if v_worker.target_pay_channel='PAYE' then
    if private.bpay_next_paye_net_entry_in_progress_v1(v_worker.id)
      or exists(select 1 from private.bpay_next_run_channel_totals t
        where t.run_id=v_run.id and t.pay_channel='PAYE' and t.net_replacement_pending_count>0) then
      raise exception using errcode='55000',message='BPAY_NEXT_PAYE_NET_ENTRY_IN_PROGRESS';
    end if;
    if v_worker.net_projection_revision<1 or exists(select 1 from private.bpay_next_run_channel_totals t
      where t.run_id=v_run.id and t.pay_channel='PAYE' and t.applicable_net_missing_count>0) then
      raise exception using errcode='55000',message='BPAY_NEXT_PAYE_NET_REQUIRED';
    end if;
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY'
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or (v_worker.target_pay_channel='PAYE' and
         (v_worker.net_projection_revision<1 or v_worker.net_request_revision<>v_worker.net_projection_revision or v_worker.net_replacement_pending_id is not null))
     or (v_worker.target_pay_channel='UMBRELLA' and
         (v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
          or v_worker.entered_paye_net is not null
          or v_worker.target_umbrella_id is null or v_worker.target_umbrella_vat_chargeable is null
          or v_worker.gross_inc_vat<=0 or v_worker.captured_line_count<1
          or v_worker.financial_resolution_count<>0 or v_worker.expected_managed_position_count<>0
          or exists(select 1 from private.bpay_next_paye_net_request where run_worker_id=v_worker.id)))
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_TRANSFER_NOT_ELIGIBLE';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' then
    -- No payroll NET request/value, CASE adjustment or live financial lookup.
    -- Bounded member pages must certify every included line as same-channel
    -- TAXABLE WORK before the transfer can become issuable.
    insert into private.bpay_next_net_projection
      (run_worker_id,request_command_id,projection_no,input_kind,
       gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount)
      values(v_worker.id,null,1,'UMBRELLA',v_worker.gross_ex_vat,v_worker.gross_vat,
        v_worker.gross_inc_vat,null,0,v_worker.gross_inc_vat);
    update private.bpay_next_run_worker set net_projection_revision=1 where id=v_worker.id;
  end if;
  select dr37_point_134.* into strict v_projection from private.bpay_next_net_projection dr37_point_134
    where run_worker_id=v_worker.id and projection_no=case when v_worker.target_pay_channel='UMBRELLA' then 1 else v_worker.net_projection_revision end and retired_at_utc is null;
  if v_worker.target_pay_channel='PAYE' and (v_projection.input_kind<>'PAYE_MANUAL'
     or v_projection.gross_ex_vat<>v_worker.gross_ex_vat
     or v_projection.gross_vat<>0
     or v_projection.gross_inc_vat<>v_worker.gross_inc_vat
     or v_projection.entered_paye_net is distinct from
        v_worker.entered_paye_net
     or v_projection.accepted_recoveries<>0
     or v_projection.cash_amount<0
     or v_projection.cash_amount<>v_worker.entered_paye_net
     or v_projection.request_command_id is null) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' and
     ((v_projection.input_kind,v_projection.gross_ex_vat,v_projection.gross_vat,
       v_projection.gross_inc_vat,v_projection.cash_amount,v_projection.accepted_recoveries,
       v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,v_projection.accepted_net_additions)
       is distinct from ('UMBRELLA'::text,v_worker.gross_ex_vat,v_worker.gross_vat,
         v_worker.gross_inc_vat,v_worker.gross_inc_vat,0::numeric,0::numeric,0::numeric,0::numeric)
      or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null) then
    raise exception using errcode='23514',message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer
    (run_worker_id,candidate_id,projection_id,build_command_id,
     transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,
      1,case when v_worker.target_pay_channel='UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end,
      case when v_worker.target_pay_channel='UMBRELLA' then v_worker.target_umbrella_id else v_worker.candidate_id end,
      v_projection.cash_amount,'BUILDING',
      case when v_projection.cash_amount=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

create or replace function private.bpay_next_begin_case_transfer_v1(p_command_id uuid,p_run_worker_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_draft record;v_net private.bpay_next_case_allocation_state%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;v_cash numeric;
  v_sequence bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_TRANSFER_BEGIN_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_command_id::text,0));
  select run_id into strict v_run_id from private.bpay_next_run_worker where id=p_run_worker_id;
  select dr37_point_140.* into strict v_run from private.bpay_next_pay_run dr37_point_140 where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c join private.bpay_next_run_worker w on w.candidate_id=c.candidate_id
    where w.id=p_run_worker_id for update of c;
  select dr37_point_141.* into strict v_worker from private.bpay_next_run_worker dr37_point_141 where id=p_run_worker_id for update;
  select dr37_point_142.* into v_transfer from private.bpay_next_transfer dr37_point_142 where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>v_worker.id or v_transfer.candidate_id<>v_worker.candidate_id
       or (not exists(select 1 from private.bpay_next_case_transfer_build s where s.transfer_id=v_transfer.id)
         and not exists(select 1 from private.bpay_next_destination_group s where s.anchor_transfer_id=v_transfer.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_BEGIN_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD' and module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'transfer_id',v_transfer.id,
      'phase',v_transfer.status,'replay',true);
  end if;
  -- Exact replay precedes new admission. Maintained channel counts only.
  if v_worker.target_pay_channel='PAYE' then
    if private.bpay_next_paye_net_entry_in_progress_v1(v_worker.id)
      or exists(select 1 from private.bpay_next_run_channel_totals t
        where t.run_id=v_run.id and t.pay_channel='PAYE' and t.net_replacement_pending_count>0) then
      raise exception using errcode='55000',message='BPAY_NEXT_PAYE_NET_ENTRY_IN_PROGRESS';
    end if;
    if v_worker.net_projection_revision<1 or exists(select 1 from private.bpay_next_run_channel_totals t
      where t.run_id=v_run.id and t.pay_channel='PAYE' and t.applicable_net_missing_count>0) then
      raise exception using errcode='55000',message='BPAY_NEXT_PAYE_NET_REQUIRED';
    end if;
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.case_selection_revision<1
     or v_worker.case_pending_binding_count<>0 or v_worker.realised_effect_count<>0
     or v_worker.net_projection_revision<1 or v_worker.net_replacement_pending_id is not null or v_worker.net_request_revision<>v_worker.net_projection_revision
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_NOT_ELIGIBLE';end if;
  select dr37_point_143.* into strict v_projection from private.bpay_next_net_projection dr37_point_143
    where run_worker_id=v_worker.id and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
  select id,processed_instruction_count,expected_instruction_count,preparation_revision,selection_revision into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE';
  select dr37_point_144.* into strict v_net from private.bpay_next_case_allocation_state dr37_point_144 where projection_id=v_projection.id
    and run_worker_id=v_worker.id and pass_kind='NET' and status='COMPLETE' and net_stage='COMPLETE';
  if v_draft.processed_instruction_count<>v_worker.case_instruction_count
     or v_net.expected_instruction_count<>v_draft.expected_instruction_count
     or v_net.processed_instruction_count<>v_draft.processed_instruction_count
     or v_net.preparation_revision<>v_draft.preparation_revision or v_net.selection_revision<>v_draft.selection_revision
     or v_projection.input_kind not in ('PAYE_MANUAL','CASE_PAYOUT')
     or (v_projection.gross_ex_vat,v_projection.gross_vat,v_projection.gross_inc_vat,v_projection.entered_paye_net)
       is distinct from (v_worker.gross_ex_vat,v_worker.gross_vat,v_worker.gross_inc_vat,v_worker.entered_paye_net)
     or v_projection.cash_amount<0 or v_projection.request_command_id is null
     or not exists(select 1 from private.bpay_next_job j where j.id=v_net.job_id and j.status='DONE' and j.phase='PROJECTED') then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_PROJECTION_INVALID';end if;
  -- Zero cash is an internal financial envelope, never a bank instruction.
  select dr37_point_145.* into v_destination from private.bpay_next_net_destination_state dr37_point_145 where projection_id=v_projection.id;
  v_cash:=v_projection.cash_amount;
  if found then
    if v_destination.state_id<>v_net.id or v_destination.run_worker_id<>v_worker.id
       or v_destination.completed_at_utc is null or v_destination.cash_amount<>v_projection.cash_amount
       or v_destination.external_leg_count<1 or v_destination.external_amount<=0
       or v_destination.own_amount+v_destination.external_amount<>v_projection.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_TRANSFER_PROJECTION_INVALID';end if;
    v_cash:=v_destination.own_amount;
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer(run_worker_id,candidate_id,projection_id,build_command_id,transfer_no,
    beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,1,'CANDIDATE',v_worker.candidate_id,
      v_cash,'BUILDING',case when v_cash=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  if v_destination.state_id is not null then
    insert into private.bpay_next_destination_group(anchor_transfer_id,run_worker_id,candidate_id,projection_id,draft_state_id,
      net_state_id,preparation_revision,selection_revision,stage,expected_work_count,expected_case_count,expected_leg_count)
      values(v_transfer.id,v_worker.id,v_worker.candidate_id,v_projection.id,v_draft.id,v_net.id,
        v_worker.captured_preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,
        v_draft.expected_instruction_count,v_destination.external_leg_count+1);
    insert into private.bpay_next_destination_group_leg(transfer_id,anchor_transfer_id,leg_kind) values(v_transfer.id,v_transfer.id,'OWN');
  else
    insert into private.bpay_next_case_transfer_build(transfer_id,run_worker_id,candidate_id,projection_id,draft_state_id,
      net_state_id,preparation_revision,selection_revision,stage,expected_work_count,expected_case_count)
      values(v_transfer.id,v_worker.id,v_worker.candidate_id,v_projection.id,v_draft.id,v_net.id,
        v_worker.captured_preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,v_draft.expected_instruction_count);
  end if;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

create or replace function public.bpay_next_financial_intake_v1(
  p_actor_user_id uuid,p_action text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];
  v_key text;
  v_command_id uuid;
  v_worker_id uuid;
  v_transfer_id uuid;
  v_instruction_id uuid;
  v_return_cash_id uuid;
  v_run_id uuid;
  v_amount numeric;
  v_occurred_at timestamptz;
  v_result jsonb;
  v_output jsonb;
  v_sequence text;
  v_projection_revision bigint;v_preview_capacity_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
  v_proof_id uuid;
  v_reauth_scope jsonb;
  v_protected boolean;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
                   where u.id=p_actor_user_id and u.is_active is true
                     and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_action in ('CASE_PAUSE','CASE_RESUME') then
    return private.bpay_next_accept_case_pause_v1(p_actor_user_id,p_action,p_args);
  end if;
  if p_action='CASE_CHANNEL_RESOLVE' then
    return private.bpay_next_accept_case_channel_v1(p_actor_user_id,p_args);
  end if;
  if p_action='CASE_RESTRUCTURE' then
    return private.bpay_next_accept_case_restructure_v1(p_actor_user_id,p_args);
  end if;
  if p_action='CASE_EDIT' then
    return private.bpay_next_accept_case_edit_v1(p_actor_user_id,p_args);
  end if;
  if p_action='CASE_CREATE' then
    if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or (select count(*) from jsonb_object_keys(p_args))<>16
      or not(p_args ? 'command_id') or not(p_args ? 'preview_token')
      or jsonb_typeof(p_args->'command_id') is distinct from 'string'
      or (p_args->>'command_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->'preview_token' is null or p_args->'preview_token'='null'::jsonb
      or (p_args-'command_id'-'preview_token') is distinct from (p_args->'preview_token'->'input') then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    perform private.bpay_next_case_create_terms_v1(p_args-'command_id'-'preview_token');
    v_result:=private.bpay_next_accept_case_create_v1((p_args->>'command_id')::uuid,(p_args->>'candidate_id')::uuid,p_actor_user_id,
      p_args->>'case_kind',p_args->>'case_subtype',p_args->>'tax_treatment',(p_args->>'principal')::numeric,
      (p_args->>'due_date')::date,(p_args->>'start_monday')::date,(p_args->>'weekly_due')::numeric,(p_args->>'week_count')::bigint,
      (p_args->>'minimum_earnings_threshold')::numeric,(p_args->>'take_home_floor_override')::numeric,
      p_args->>'reason',p_args->>'note',p_args->>'original_liability_reference',p_args->'preview_token');
    v_output:=pg_catalog.jsonb_build_object('case_id',v_result->'case_id','command_id',v_result->'command_id',
      'sequence',v_result->'sequence','phase',v_result->'phase','replay',v_result->'replay');
    return v_output;
  end if;
  if p_action='RETRY_WORK_JOB' then
    return jsonb_build_object('action',p_action,'result',
      private.bpay_next_retry_work_job_v1(p_actor_user_id,p_args,false));
  end if;
  if p_action is null or p_action not in
      ('PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
       'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      || case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'PAYE_NET' then array['command_id','run_worker_id','entered_paye_net']
      || case when p_args ? 'expected_projection_revision' then array['expected_projection_revision'] else array[]::text[] end
      || case when p_args ? 'preview_capacity_revision' then array['preview_capacity_revision'] else array[]::text[] end
    when 'CASE_PAYOUT' then array['command_id','run_worker_id','expected_projection_revision']
    when 'TRANSFER_BEGIN' then array['command_id','run_worker_id']
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      || case when p_args ? 'run_id' or p_args ? 'pay_channel'
        then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    when 'INTERNAL_SETTLE' then array['command_id','transfer_id']
    when 'PREPARATION_EXPIRE' then array['command_id','run_id','expected_deadline']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  v_protected:=p_action in ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN');
  if v_protected then
    if p_args ? 'reauth_proof_id' then v_keys:=v_keys||array['reauth_proof_id'];
    elsif p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  end if;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id','reauth_proof_id')
       and (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key='pay_channel' and (p_args->>v_key) not in ('PAYE','UMBRELLA') then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end loop;
  v_command_id:=(p_args->>'command_id')::uuid;
  v_worker_id:=(p_args->>'run_worker_id')::uuid;
  v_transfer_id:=(p_args->>'transfer_id')::uuid;
  v_instruction_id:=(p_args->>'instruction_id')::uuid;
  v_return_cash_id:=(p_args->>'return_cash_id')::uuid;
  if p_action='WRITE_OFF' then
    v_case_component_id:=(p_args->>'case_component_id')::uuid;
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
       or (p_args->>'expected_component_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_component_revision:=(p_args->>'expected_component_revision')::bigint;
  end if;
  if p_action='PREPARATION_EXPIRE' then
    v_run_id:=(p_args->>'run_id')::uuid;
    if (p_args->>'expected_deadline')!~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'expected_deadline')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_args ? 'expected_projection_revision' then
    if (p_args->>'expected_projection_revision')!~'^(0|[1-9][0-9]{0,18})$'
       or (p_args->>'expected_projection_revision')::numeric>=9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_projection_revision:=(p_args->>'expected_projection_revision')::bigint;
  end if;
  if p_args ? 'preview_capacity_revision' then
    if p_action<>'PAYE_NET' or (p_args->>'preview_capacity_revision')!~'^(0|[1-9][0-9]{0,18})$'
      or (p_args->>'preview_capacity_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';end if;
    v_preview_capacity_revision:=(p_args->>'preview_capacity_revision')::bigint;
  end if;
  if p_action in ('PAYE_NET','CSV_SETTLEMENT','CSV_RETURN') then
    v_key:=case when p_action='PAYE_NET' then 'entered_paye_net' else 'amount' end;
    -- Current money columns are numeric(18,2). Explicit finite, ordinary
    -- decimal syntax prevents exponent/NaN, narrowing and JSON-number coercion.
    if (p_args->>v_key)!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_amount:=(p_args->>v_key)::numeric;
    if p_action<>'PAYE_NET' and v_amount<=0 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_action in ('CSV_SETTLEMENT','CSV_RETURN') then
    if pg_catalog.octet_length(p_args->>'receipt_id') not between 1 and 256
       or (p_args->>'occurred_at')!~
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'occurred_at')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;

  if v_protected then
    -- Shared finite resolver takes the native business locks before recomputing
    -- scope. Same original receipt qualifies consumed replay, never live repricing.
    v_proof_id:=(p_args->>'reauth_proof_id')::uuid;
    v_reauth_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,p_action,p_args-'reauth_proof_id',true);
    if not (p_action='CSV_ISSUE' and (v_reauth_scope->>'accepted')::boolean and v_proof_id is null) then
      perform private.bpay_next_reauth_consume_v1(v_proof_id,p_actor_user_id,p_action,
        v_reauth_scope->>'scope_sha256',coalesce(v_command_id,v_instruction_id));
    end if;
  end if;

  -- Fixed branches and exact typed IDs only. No dynamic identifier, query,
  -- caller actor, arbitrary RPC, live finance/history reconstruction or drain.
  case p_action
    when 'WRITE_OFF' then
      v_result:=private.bpay_next_accept_write_off_v1(v_command_id,v_case_component_id,p_actor_user_id,
        v_component_revision,p_args->>'scope',p_args->>'amount',p_args->>'reason');
      v_output:=pg_catalog.jsonb_build_object('command_id',v_result->>'command_id',
        'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope',
        'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
        'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id',
        'issue_code',v_result->>'issue_code','replay',v_result->'replay');
    when 'PREPARATION_EXPIRE' then
      -- NULL clock selects the real server clock AFTER acquiring the header.
      -- Office cannot supply SYSTEM authority, now, or a renewal duration.
      v_result:=private.bpay_next_accept_preparation_expiry_v1(
        v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','run_id',v_result->>'run_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'expected_candidates',v_result->>'expected_candidates',
        'completed_candidates',v_result->>'completed_candidates',
        'replay',v_result->'replay');
    when 'PAYE_NET' then
      -- Route by the worker's FROZEN selection, never by live Candidate cases.
      -- Explicit prior projection protects resaves from overwriting a newer one.
      select w.case_selection_revision into strict v_case_selection_revision
        from private.bpay_next_run_worker w where w.id=v_worker_id;
      if v_case_selection_revision>0 then
        if v_projection_revision is null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_case_net_request_v1(
          v_command_id,v_worker_id,p_actor_user_id,'PAYE_MANUAL',v_amount,v_projection_revision,v_preview_capacity_revision);
      else
        if v_projection_revision is not null or v_preview_capacity_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount,p_actor_user_id);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
      if v_case_selection_revision>0 then
        v_output:=v_output||pg_catalog.jsonb_build_object('replacement_id',v_result->>'replacement_id',
          'replacement_stage',v_result->>'replacement_stage','projection_id',v_result->>'projection_id','issue_code',v_result->>'issue_code');
      end if;
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
      v_output:=v_output||pg_catalog.jsonb_build_object('replacement_id',v_result->>'replacement_id',
        'replacement_stage',v_result->>'replacement_stage','projection_id',v_result->>'projection_id','issue_code',v_result->>'issue_code');
    when 'TRANSFER_BEGIN' then
      v_result:=private.bpay_next_begin_simple_transfer_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CANCEL_REQUEST' then
      -- Optional address is strictly paired and independently bound before accepting intent.
      if p_args ? 'run_id' and not exists(select 1 from private.bpay_next_run_worker w
        where w.id=v_worker_id and w.run_id=(p_args->>'run_id')::uuid
          and w.target_pay_channel=p_args->>'pay_channel') then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID';
      end if;
      v_result:=private.bpay_next_accept_simple_cancel_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count',
        'blocked_code',v_result->>'blocked_code','replay',v_result->'replay');
    when 'CASH_REISSUE' then
      v_result:=private.bpay_next_receive_simple_reissue_v1(v_command_id,v_return_cash_id,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c where c.id=v_command_id and c.command_kind='CASH_REISSUE';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'return_cash_id',v_return_cash_id,
        'sequence',v_sequence,'transfer_id',v_result->>'transfer_id',
        'phase',case when v_result->>'transfer_id' is null
          then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end,
        'replay',v_result->'replay');
    when 'REISSUE_CANCEL' then
      v_result:=private.bpay_next_accept_reissue_cancel_v1(v_command_id,v_transfer_id,p_actor_user_id);
      select r.blocked_code into v_key from private.bpay_next_reissue_cancel_request r where r.command_id=v_command_id;
      v_output:=pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay');
    when 'CSV_ISSUE' then
      -- A new CSV issue first binds its current approved destination using the
      -- existing owner. Already-issued replay never rebinds or changes bytes.
      -- Both calls use the same transaction and parent run mutex; any issue
      -- permission/state failure rolls the destination preparation back too.
      -- Acquire that mutex BEFORE checking instruction existence: concurrent
      -- same-ID retry must see the first committed issue and skip rebinding.
      select w.run_id into strict v_run_id
        from private.bpay_next_transfer t
        join private.bpay_next_run_worker w on w.id=t.run_worker_id
        where t.id=v_transfer_id;
      perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
      if not exists(select 1 from private.bpay_next_csv_instruction i where i.id=v_instruction_id) then
        perform private.bpay_next_bind_simple_csv_destination_v1(v_transfer_id);
      end if;
      v_result:=private.bpay_next_issue_simple_csv_v1(v_instruction_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id',
        'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256',
        'csv_text',v_result->>'csv_text','phase','ISSUED_CSV',
        'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay');
    when 'CSV_SETTLEMENT' then
      v_result:=private.bpay_next_receive_simple_csv_settlement_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_SETTLEMENT';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
    when 'INTERNAL_SETTLE' then
      -- The server creates an internal receipt for an exact zero-cash frozen
      -- envelope. There is no caller amount, bank receipt or CSV instruction.
      v_result:=private.bpay_next_receive_internal_settlement_v1(
        v_command_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id',
        'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence',
        'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay');
    when 'CSV_RETURN' then
      v_result:=private.bpay_next_receive_simple_csv_return_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_RETURN';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
  end case;
  if pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean'
     or pg_catalog.octet_length(v_output::text)>120000 then
    -- Exception aborts the whole request transaction; no partial issue or
    -- accepted financial request is committed without its bounded receipt.
    raise exception using errcode='54000',message='BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function private.bpay_next_paye_net_capacity_v1(
 p_draft_capacity numeric,p_period_available numeric,p_component_available numeric,
 p_case_outstanding numeric,p_case_active numeric,p_case_release numeric,p_case_reserve numeric,p_own numeric
) returns numeric language sql immutable parallel safe security invoker set search_path=pg_catalog
as $function$
 select least(p_draft_capacity,greatest(p_period_available+p_own,0),
  greatest(p_component_available+p_own,0),
  greatest(p_case_outstanding-p_case_active+p_case_release-p_case_reserve+p_own,0))
$function$;

create or replace function private.bpay_next_paye_net_cash_v1(p_entered_net numeric,p_net_additions numeric,p_recoveries numeric)
returns numeric language sql immutable parallel safe security invoker set search_path=pg_catalog
as $function$ select coalesce(p_entered_net,0)+p_net_additions-p_recoveries $function$;

create or replace function private.bpay_next_paye_net_preview_v1(
 p_actor_user_id uuid,p_run_worker_id uuid,p_entered_paye_net numeric,p_after jsonb,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
 v_w private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
 v_d private.bpay_next_case_allocation_state%rowtype;v_p private.bpay_next_net_projection%rowtype;
 v_i private.bpay_next_run_case_instruction%rowtype;v_a private.bpay_next_case_allocation_result%rowtype;
 v_h private.bpay_next_case_hold%rowtype;v_c private.bpay_next_finance_case%rowtype;
 v_cc private.bpay_next_case_component%rowtype;v_period private.bpay_next_case_period%rowtype;
 v_origin private.bpay_next_case_create_request%rowtype;v_collection private.bpay_next_work_collection%rowtype;
 v_prior_state uuid;v_prior_result private.bpay_next_case_allocation_result%rowtype;
 v_week_revision bigint;v_epoch bigint;v_financial_revision bigint;v_capacity_revision bigint;v_context jsonb;
 v_stage text:='PRECHECK';v_instruction_after jsonb:=null;v_run_after jsonb:=null;v_carry jsonb;
 v_e numeric:=p_entered_paye_net;v_t numeric:=p_entered_paye_net;v_add numeric:=0;
 v_gross_add numeric:=0;v_gross_deduct numeric:=0;v_recovered numeric:=0;
 v_before_advance numeric:=0;v_after_advance numeric:=0;v_before_overpayment numeric:=0;v_after_overpayment numeric:=0;
 v_before_debt numeric:=0;v_after_debt numeric:=0;v_prechecked bigint:=0;v_processed bigint:=0;
 v_requires_week boolean:=false;v_seen integer:=0;v_more boolean;v_complete boolean:=false;v_row record;v_k record;
 v_own numeric;v_capacity numeric;v_component_outstanding numeric;v_case_outstanding numeric;
 v_cash numeric;v_carry_keys text[];v_key text;v_recovery_rows jsonb:='[]'::jsonb;
begin
 if p_actor_user_id is null or p_run_worker_id is null or p_entered_paye_net is null
  or p_entered_paye_net::text in ('NaN','Infinity','-Infinity') or p_entered_paye_net<0
  or p_entered_paye_net>=10000000000000000 or p_entered_paye_net<>trunc(p_entered_paye_net,2)
  or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_INPUT_INVALID';end if;
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
 if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
 select w.* into strict v_w from private.bpay_next_run_worker w where w.id=p_run_worker_id;
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_w.run_id;
 if private.bpay_next_paye_net_entry_in_progress_v1(v_w.id) then
  raise exception using errcode='55000',message='BPAY_NEXT_PAYE_NET_ENTRY_IN_PROGRESS';end if;
 if exists(select 1 from private.bpay_next_job j where j.candidate_id=v_w.candidate_id and j.status<>'DONE')
  or exists(select 1 from private.bpay_next_candidate_week_commit_fence f where f.candidate_id=v_w.candidate_id and f.stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING')) then
  raise exception using errcode='55000',message='BPAY_NEXT_NET_PREVIEW_NOT_READY';end if;
 if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_w.status<>'READY'
  or v_w.target_pay_channel<>'PAYE' or v_w.gross_vat<>0 or p_entered_paye_net>v_w.gross_inc_vat
  or v_w.realised_effect_count<>0
  or exists(select 1 from private.bpay_next_case_payout_basis b where b.run_worker_id=v_w.id
    and b.preparation_revision=v_w.captured_preparation_revision)
  or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_w.id)
  or exists(select 1 from private.bpay_next_cancel_request q where q.run_worker_id=v_w.id and q.status in ('REQUESTED','CANCELLING')) then
  raise exception using errcode='55000',message='BPAY_NEXT_NET_PREVIEW_NOT_READY';end if;
 select financial_view_revision,net_capacity_revision into strict v_financial_revision,v_capacity_revision from private.bpay_next_worker_control where candidate_id=v_w.candidate_id;
 if v_w.case_selection_revision>0 then
  select s.* into strict v_d from private.bpay_next_case_allocation_state s
   where s.run_worker_id=v_w.id and s.preparation_revision=v_w.captured_preparation_revision
    and s.selection_revision=v_w.case_selection_revision and s.pass_kind='DRAFT' and s.projection_no=0;
  if v_d.status<>'COMPLETE' or v_d.prepare_stage<>'COMPLETE' or v_d.expected_instruction_count<>v_w.case_instruction_count then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
  select period_revision into strict v_week_revision from private.bpay_next_worker_week
   where candidate_id=v_w.candidate_id and pay_week_start=v_d.pay_week_start;
 end if;
 if v_w.net_projection_revision>0 then
  select p.* into strict v_p from private.bpay_next_net_projection p
   where p.run_worker_id=v_w.id and p.projection_no=v_w.net_projection_revision and p.retired_at_utc is null;
  if v_p.input_kind<>'PAYE_MANUAL' then raise exception using errcode='55000',message='BPAY_NEXT_NET_PREVIEW_NOT_READY';end if;
  if v_w.case_selection_revision>0 then
   select s.id into strict v_prior_state from private.bpay_next_case_allocation_state s
    where s.projection_id=v_p.id and s.run_worker_id=v_w.id and s.pass_kind='NET' and s.status='COMPLETE' and s.net_stage='COMPLETE';
  end if;
 end if;
 v_context:=jsonb_build_object('actor_user_id',p_actor_user_id,'owner_epoch',v_epoch::text,
  'run_id',v_run.id,'run_worker_id',v_w.id,'candidate_id',v_w.candidate_id,
  'preparation_revision',v_w.captured_preparation_revision::text,'selection_revision',v_w.case_selection_revision::text,
  'expected_projection_revision',v_w.net_projection_revision::text,'financial_view_revision',v_financial_revision::text,
  'net_request_revision',v_w.net_request_revision::text,'net_request_high_water',v_w.net_request_high_water::text,'capacity_revision',v_capacity_revision::text,
  'week_revision',v_week_revision::text,'draft_state_id',v_d.id,'projection_id',v_p.id,
  'entered_paye_net',p_entered_paye_net::numeric(18,2)::text);
 v_carry_keys:=array['requires_week','prechecked','processed','e','h','net_additions','gross_additions','gross_deductions','recoveries',
  'before_advance','after_advance','before_overpayment','after_overpayment','before_debt','after_debt'];
 if p_after is not null and p_after<>'null'::jsonb then
  if jsonb_typeof(p_after) is distinct from 'object' or octet_length(p_after::text)>8192
   or not(p_after?&array['context','stage','instruction_after','run_after','carry'])
   or p_after-array['context','stage','instruction_after','run_after','carry']<>'{}'::jsonb
   or jsonb_typeof(p_after->'stage') is distinct from 'string'
   or p_after->>'stage' not in ('PRECHECK','PRIOR_RUNS','WEEK_CAPTURE','ALLOCATE')
   or jsonb_typeof(p_after->'carry') is distinct from 'object'
   or not((p_after->'carry')?&v_carry_keys) or (p_after->'carry')-v_carry_keys<>'{}'::jsonb
   or jsonb_typeof(p_after#>'{carry,requires_week}') is distinct from 'boolean' then
   raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
  if p_after->'context' is distinct from v_context then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_CHANGED';end if;
  foreach v_key in array array['prechecked','processed'] loop
   if jsonb_typeof(p_after#>array['carry',v_key]) is distinct from 'string'
    or p_after#>>array['carry',v_key]!~'^(0|[1-9][0-9]{0,18})$'
    or (p_after#>>array['carry',v_key])::numeric>coalesce(v_d.expected_instruction_count,0) then
    raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
  end loop;
  foreach v_key in array v_carry_keys[4:15] loop
   if jsonb_typeof(p_after#>array['carry',v_key]) is distinct from 'string'
    or p_after#>>array['carry',v_key]!~'^-?(0|[1-9][0-9]{0,35})\.[0-9]{2}$'
    or (v_key<>'h' and (p_after#>>array['carry',v_key])::numeric<0) then
    raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
  end loop;
  v_carry:=p_after->'carry';v_stage:=p_after->>'stage';
  v_requires_week:=(v_carry->>'requires_week')::boolean;v_prechecked:=(v_carry->>'prechecked')::bigint;v_processed:=(v_carry->>'processed')::bigint;
  v_e:=(v_carry->>'e')::numeric;v_t:=(v_carry->>'h')::numeric;v_add:=(v_carry->>'net_additions')::numeric;
  v_gross_add:=(v_carry->>'gross_additions')::numeric;v_gross_deduct:=(v_carry->>'gross_deductions')::numeric;
  v_recovered:=(v_carry->>'recoveries')::numeric;
  v_before_advance:=(v_carry->>'before_advance')::numeric;v_after_advance:=(v_carry->>'after_advance')::numeric;
  v_before_overpayment:=(v_carry->>'before_overpayment')::numeric;v_after_overpayment:=(v_carry->>'after_overpayment')::numeric;
  v_before_debt:=(v_carry->>'before_debt')::numeric;v_after_debt:=(v_carry->>'after_debt')::numeric;
  v_instruction_after:=nullif(p_after->'instruction_after','null'::jsonb);v_run_after:=nullif(p_after->'run_after','null'::jsonb);
  if v_instruction_after is not null and (jsonb_typeof(v_instruction_after) is distinct from 'object'
   or not(v_instruction_after?&array['age_key','case_id','component_ordinal','case_component_id'])
   or v_instruction_after-array['age_key','case_id','component_ordinal','case_component_id']<>'{}'::jsonb
   or jsonb_typeof(v_instruction_after->'age_key') is distinct from 'string' or v_instruction_after->>'age_key'!~'^-?(0|[1-9][0-9]{0,18})$'
   or jsonb_typeof(v_instruction_after->'component_ordinal') is distinct from 'string' or v_instruction_after->>'component_ordinal'!~'^[1-9][0-9]{0,9}$'
   or jsonb_typeof(v_instruction_after->'case_id') is distinct from 'string' or v_instruction_after->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_instruction_after->'case_component_id') is distinct from 'string' or v_instruction_after->>'case_component_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
   raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
  if v_run_after is not null and (jsonb_typeof(v_run_after) is distinct from 'object'
   or not(v_run_after?&array['pay_date','created_at_utc','id']) or v_run_after-array['pay_date','created_at_utc','id']<>'{}'::jsonb
   or jsonb_typeof(v_run_after->'pay_date') is distinct from 'string' or v_run_after->>'pay_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   or jsonb_typeof(v_run_after->'created_at_utc') is distinct from 'string'
   or v_run_after->>'created_at_utc'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
   or jsonb_typeof(v_run_after->'id') is distinct from 'string' or v_run_after->>'id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
   raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
 end if;
 if v_w.case_selection_revision=0 then
  if p_after is not null and p_after<>'null'::jsonb then raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_CURSOR_INVALID';end if;
  v_complete:=true;
 elsif v_stage in ('PRECHECK','ALLOCATE') then
  for v_i in select i.* from private.bpay_next_run_case_instruction i
   where i.run_worker_id=v_w.id and i.preparation_revision=v_w.captured_preparation_revision and i.selection_revision=v_w.case_selection_revision
    and (v_instruction_after is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>
     ((v_instruction_after->>'age_key')::bigint,(v_instruction_after->>'case_id')::uuid,
      (v_instruction_after->>'component_ordinal')::integer,(v_instruction_after->>'case_component_id')::uuid))
   order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
  loop
   v_seen:=v_seen+1;
   select a.* into v_a from private.bpay_next_case_allocation_result a where a.state_id=v_d.id and a.instruction_id=v_i.id;
   if v_a.id is null or v_i.source_pay_channel<>'PAYE' or v_i.target_pay_channel<>'PAYE' or v_i.allocation_channel<>'PAYE'
    or v_i.currency<>'GBP' or v_i.nominal_target_vat<>0 or v_a.allocated_target_vat<>0
    or v_a.allocated_source_ex_vat<>v_a.allocated_target_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
   if v_stage='PRECHECK' then
    v_prechecked:=v_prechecked+1;
    if v_i.payroll_stage='NET_DEDUCT' and v_i.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and v_a.allocated_source_ex_vat>0 then
     v_requires_week:=true;end if;
   else
    select h.* into v_h from private.bpay_next_case_hold h where h.instruction_id=v_i.id and h.status='ACTIVE';
    v_own:=coalesce(v_h.source_reserved_ex_vat,0);
    if v_h.id is not null and (v_h.run_worker_id<>v_w.id or v_h.purpose<>v_i.hold_purpose
     or v_h.source_reserved_ex_vat<>v_h.target_amount_ex_vat or v_h.target_amount_vat<>0
     or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own)
     or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=v_h.allocation_result_id and a.instruction_id=v_i.id
      and a.state_id in (v_d.id,v_prior_state))) then
     raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
    if v_i.payroll_stage='NET_DEDUCT' then
     select c.* into strict v_c from private.bpay_next_finance_case c where c.id=v_i.case_id;
     select c.* into strict v_cc from private.bpay_next_case_component c where c.id=v_i.case_component_id;
     select p.* into strict v_period from private.bpay_next_case_period p where p.case_component_id=v_i.case_component_id and p.pay_week_start=v_i.pay_week_start;
     -- Native original producer admits one recovery component per supported Case.
     -- This is an identity proof, never a product instruction-count cap.
     if v_i.work_collection_id is null then
      select o.* into v_origin from private.bpay_next_case_create_request o where o.command_id=v_i.case_id;
      if v_origin.command_id is null or v_origin.candidate_id<>v_w.candidate_id
       or (v_i.case_kind in ('LOAN','ADVANCE') and v_origin.recovery_component_id is distinct from v_i.case_component_id)
       or (v_i.case_kind='MANUAL_DEBT' and v_origin.primary_component_id is distinct from v_i.case_component_id)
       or v_i.case_kind not in ('LOAN','ADVANCE','MANUAL_DEBT') then
       raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
     else
      select c.* into v_collection from private.bpay_next_work_collection c where c.id=v_i.work_collection_id;
      if v_i.case_kind<>'OVERPAYMENT' or v_collection.id is null or v_collection.candidate_id<>v_w.candidate_id
       or v_collection.id<>v_i.case_id or v_collection.id<>v_i.case_component_id
       or (v_collection.case_id,v_collection.case_component_id) is distinct from (v_i.case_id,v_i.case_component_id) then
       raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
     end if;
     if v_own>v_cc.active_recovery_source_ex_vat or v_own>v_c.active_recovery_hold_amount
      or v_own>v_period.active_unrealised_recovery_source_ex_vat or v_own>v_a.allocated_source_ex_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
     v_component_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_cc.funded_source_ex_vat else v_cc.approved_source_ex_vat end)
      -v_cc.recovered_source_ex_vat-v_cc.written_off_source_ex_vat;
     v_case_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)
      -v_c.principal_recovered-v_c.principal_written_off;
     v_capacity:=private.bpay_next_paye_net_capacity_v1(v_a.allocated_source_ex_vat,
      v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat-v_period.active_unrealised_recovery_source_ex_vat,
      v_component_outstanding-v_cc.active_recovery_source_ex_vat,
      v_case_outstanding,v_c.active_recovery_hold_amount,0,0,v_own);
     select k.* into strict v_k from private.bpay_next_case_allocation_row_v1(v_i.case_kind,v_i.nominal_target_ex_vat,v_capacity,
      v_e,v_t,v_i.minimum_earnings_threshold,v_i.take_home_floor_override,v_i.captured_default_floor) k;
     v_e:=v_k.next_channel_headroom;v_t:=v_k.next_worker_take_home;
     v_recovered:=v_recovered+v_k.taken_amount;
     if v_p.id is null then v_prior_result:=v_a;
     else
      select a.* into strict v_prior_result from private.bpay_next_case_allocation_result a where a.state_id=v_prior_state and a.instruction_id=v_i.id;
     end if;
     v_recovery_rows:=v_recovery_rows||jsonb_build_array(jsonb_build_object(
      'instruction_id',v_i.id,'case_id',v_i.case_id,'case_component_id',v_i.case_component_id,'case_kind',v_i.case_kind,
      'before',case when v_p.id is null then null else v_prior_result.allocated_target_ex_vat::numeric(18,2)::text end,
      'after',v_k.taken_amount::numeric(18,2)::text));
     if v_i.case_kind in ('LOAN','ADVANCE') then
      v_before_advance:=v_before_advance+v_prior_result.allocated_target_ex_vat;v_after_advance:=v_after_advance+v_k.taken_amount;
     elsif v_i.case_kind='OVERPAYMENT' then
      v_before_overpayment:=v_before_overpayment+v_prior_result.allocated_target_ex_vat;v_after_overpayment:=v_after_overpayment+v_k.taken_amount;
     else v_before_debt:=v_before_debt+v_prior_result.allocated_target_ex_vat;v_after_debt:=v_after_debt+v_k.taken_amount;end if;
    else
     if v_own<>v_a.allocated_source_ex_vat or v_own>0 and (v_h.allocation_result_id<>v_a.id or v_h.allocation_pass_kind<>'DRAFT') then
      raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
     if v_i.payroll_stage='NET_ADD' then v_add:=v_add+v_a.allocated_target_inc_vat;
     elsif v_i.payroll_stage='GROSS_ADD' then v_gross_add:=v_gross_add+v_a.allocated_target_ex_vat;
     elsif v_i.payroll_stage='GROSS_DEDUCT' then v_gross_deduct:=v_gross_deduct+v_a.allocated_target_ex_vat;
     else raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
    end if;
    v_processed:=v_processed+1;
   end if;
   v_instruction_after:=jsonb_build_object('age_key',v_i.age_key::text,'case_id',v_i.case_id,
    'component_ordinal',v_i.component_ordinal::text,'case_component_id',v_i.case_component_id);
  end loop;
  select exists(select 1 from private.bpay_next_run_case_instruction i
   where i.run_worker_id=v_w.id and i.preparation_revision=v_w.captured_preparation_revision and i.selection_revision=v_w.case_selection_revision
    and (v_instruction_after is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>
     ((v_instruction_after->>'age_key')::bigint,(v_instruction_after->>'case_id')::uuid,
      (v_instruction_after->>'component_ordinal')::integer,(v_instruction_after->>'case_component_id')::uuid))) into v_more;
  if not v_more then
   if v_stage='PRECHECK' then
    if v_prechecked<>v_d.expected_instruction_count then raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
    v_stage:=case when v_requires_week then 'PRIOR_RUNS' else 'ALLOCATE' end;v_instruction_after:=null;
   else
    if v_processed<>v_d.expected_instruction_count or (v_gross_add,v_gross_deduct,v_add)
     is distinct from (v_d.allocated_gross_additions,v_d.allocated_gross_deductions,v_d.allocated_net_additions) then
     raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
    v_complete:=true;
   end if;
  end if;
 elsif v_stage='PRIOR_RUNS' then
  for v_row in select r.id,r.pay_date,r.created_at_utc from private.bpay_next_pay_run r
   where r.pay_date>=v_d.pay_week_start and r.pay_date<v_d.pay_week_start+7
    and (r.pay_date,r.created_at_utc)<=(v_run.pay_date,v_run.created_at_utc)
    and (v_run_after is null or (r.pay_date,r.created_at_utc,r.id)>
     ((v_run_after->>'pay_date')::date,(v_run_after->>'created_at_utc')::timestamptz,(v_run_after->>'id')::uuid))
   order by r.pay_date,r.created_at_utc,r.id limit p_limit
  loop
   v_seen:=v_seen+1;
   if exists(select 1 from private.bpay_next_run_worker w where w.run_id=v_row.id and w.candidate_id=v_w.candidate_id
    and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
    and (v_row.pay_date,v_row.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_w.id)
    and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_WEEK_UNBOUND';end if;
   v_run_after:=jsonb_build_object('pay_date',v_row.pay_date,'created_at_utc',v_row.created_at_utc,'id',v_row.id);
  end loop;
  select exists(select 1 from private.bpay_next_pay_run r where r.pay_date>=v_d.pay_week_start and r.pay_date<v_d.pay_week_start+7
   and (r.pay_date,r.created_at_utc)<=(v_run.pay_date,v_run.created_at_utc)
   and (v_run_after is null or (r.pay_date,r.created_at_utc,r.id)>
    ((v_run_after->>'pay_date')::date,(v_run_after->>'created_at_utc')::timestamptz,(v_run_after->>'id')::uuid))) into v_more;
  if not v_more then v_stage:='WEEK_CAPTURE';v_run_after:=null;end if;
 else
  for v_row in select x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id,x.eligibility_state,x.eligible_arranged_amount
   from private.bpay_next_worker_week_contribution x where x.candidate_id=v_w.candidate_id and x.pay_week_start=v_d.pay_week_start
    and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_w.id)
    and (v_run_after is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)>
     ((v_run_after->>'pay_date')::date,(v_run_after->>'created_at_utc')::timestamptz,(v_run_after->>'id')::uuid))
   order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
  loop
   v_seen:=v_seen+1;
   if v_row.eligibility_state='UNBOUND' or v_row.eligible_arranged_amount is null then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_WEEK_UNBOUND';end if;
   v_t:=v_t+v_row.eligible_arranged_amount;
   v_run_after:=jsonb_build_object('pay_date',v_row.original_pay_date,'created_at_utc',v_row.original_created_at_utc,'id',v_row.original_run_worker_id);
  end loop;
  select exists(select 1 from private.bpay_next_worker_week_contribution x where x.candidate_id=v_w.candidate_id and x.pay_week_start=v_d.pay_week_start
   and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_w.id)
   and (v_run_after is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)>
    ((v_run_after->>'pay_date')::date,(v_run_after->>'created_at_utc')::timestamptz,(v_run_after->>'id')::uuid))) into v_more;
  if not v_more then v_stage:='ALLOCATE';v_run_after:=null;end if;
 end if;
 v_carry:=jsonb_build_object('requires_week',v_requires_week,'prechecked',v_prechecked::text,'processed',v_processed::text,
  'e',v_e::numeric(38,2)::text,'h',v_t::numeric(38,2)::text,'net_additions',v_add::numeric(38,2)::text,
  'gross_additions',v_gross_add::numeric(38,2)::text,'gross_deductions',v_gross_deduct::numeric(38,2)::text,'recoveries',v_recovered::numeric(38,2)::text,
  'before_advance',v_before_advance::numeric(38,2)::text,'after_advance',v_after_advance::numeric(38,2)::text,
  'before_overpayment',v_before_overpayment::numeric(38,2)::text,'after_overpayment',v_after_overpayment::numeric(38,2)::text,
  'before_debt',v_before_debt::numeric(38,2)::text,'after_debt',v_after_debt::numeric(38,2)::text);
 if v_complete then
  v_cash:=private.bpay_next_paye_net_cash_v1(p_entered_paye_net,v_add,v_recovered);
  if v_cash<0 or v_cash>=10000000000000000 then raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_INPUT_UNBOUND';end if;
 end if;
 return jsonb_build_object('context',v_context,'complete',v_complete,'rows_visited',v_seen::text,'recovery_rows',v_recovery_rows,
  'next_cursor',case when v_complete then null else jsonb_build_object('context',v_context,'stage',v_stage,
   'instruction_after',v_instruction_after,'run_after',v_run_after,'carry',v_carry) end,
  'summary',case when not v_complete then null else jsonb_build_object(
   'accepted_net',v_p.entered_paye_net::text,'accepted_recoveries',v_p.accepted_recoveries::text,'accepted_cash',v_p.cash_amount::text,
   'proposed_net',p_entered_paye_net::numeric(18,2)::text,'proposed_recoveries',v_recovered::numeric(18,2)::text,'proposed_cash',v_cash::numeric(18,2)::text,
   'net_additions',v_add::numeric(18,2)::text,'gross_additions',v_gross_add::numeric(18,2)::text,'gross_deductions',v_gross_deduct::numeric(18,2)::text,
   'advance_before',case when v_p.id is null then null else v_before_advance::numeric(18,2)::text end,'advance_after',v_after_advance::numeric(18,2)::text,
   'overpayment_before',case when v_p.id is null then null else v_before_overpayment::numeric(18,2)::text end,'overpayment_after',v_after_overpayment::numeric(18,2)::text,
   'debt_before',case when v_p.id is null then null else v_before_debt::numeric(18,2)::text end,'debt_after',v_after_debt::numeric(18,2)::text) end);
end
$function$;

create or replace function private.bpay_next_stage_case_net_page_v1(p_replacement_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
 v_a private.bpay_next_case_allocation_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
 v_c private.bpay_next_finance_case%rowtype;v_cc private.bpay_next_case_component%rowtype;v_p private.bpay_next_case_period%rowtype;
 v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_kernel record;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;
 v_own numeric;v_capacity numeric;v_outstanding numeric;v_case_outstanding numeric;v_e numeric;v_t numeric;
begin
 if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_NET_STAGING_INPUT_INVALID';end if;
 select dr37_point_38.* into strict v_rep from private.bpay_next_net_replacement dr37_point_38 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 if v_rep.stage<>'STAGING' or v_rep.staging_step<>'INSTRUCTIONS' then raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGING_STAGE_INVALID';end if;
 perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'STAGE');
 v_e:=v_rep.remaining_channel_headroom;v_t:=v_rep.remaining_take_home;
 for v_i in select i.* from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
  and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
  and (v_rep.cursor_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
   >(v_rep.cursor_age_key,v_rep.cursor_case_id,v_rep.cursor_component_ordinal,v_rep.cursor_component_id))
  order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
 loop
  select dr37_point_39.* into v_a from private.bpay_next_case_allocation_result dr37_point_39 where state_id=v_rep.draft_state_id and instruction_id=v_i.id;
  select dr37_point_40.* into v_h from private.bpay_next_case_hold dr37_point_40 where instruction_id=v_i.id and status='ACTIVE' for update nowait;
  select dr37_point_41.* into strict v_c from private.bpay_next_finance_case dr37_point_41 where id=v_i.case_id for update nowait;
  select dr37_point_42.* into strict v_cc from private.bpay_next_case_component dr37_point_42 where id=v_i.case_component_id for update nowait;
  select dr37_point_43.* into strict v_p from private.bpay_next_case_period dr37_point_43 where case_component_id=v_i.case_component_id and pay_week_start=v_i.pay_week_start for update nowait;
  v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_i),pg_catalog.to_jsonb(v_a),pg_catalog.to_jsonb(v_h),
   v_c.case_revision,v_c.principal_funded,v_c.principal_approved,v_c.principal_recovered,v_c.principal_written_off,v_c.active_recovery_hold_amount,
   v_cc.component_revision,v_cc.funded_source_ex_vat,v_cc.approved_source_ex_vat,v_cc.recovered_source_ex_vat,v_cc.written_off_source_ex_vat,
   v_cc.active_recovery_source_ex_vat,v_p.period_revision,v_p.opening_due_source_ex_vat,v_p.realised_recovery_source_ex_vat,v_p.active_unrealised_recovery_source_ex_vat)::text);
  if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
  if v_bytes+v_size>65536 then exit;end if;
  v_seen:=v_seen+1;v_bytes:=v_bytes+v_size;v_own:=coalesce(v_h.source_reserved_ex_vat,0);
  if v_a.id is null or v_i.source_pay_channel<>'PAYE' or v_i.target_pay_channel<>'PAYE' or v_i.allocation_channel<>'PAYE'
   or v_i.currency<>'GBP' or v_i.nominal_target_vat<>0 or v_a.allocated_target_vat<>0
   or v_a.allocated_source_ex_vat<>v_a.allocated_target_ex_vat then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  if v_h.id is not null and (v_h.run_worker_id<>v_rep.run_worker_id or v_h.purpose<>v_i.hold_purpose
   or v_h.source_reserved_ex_vat<>v_h.target_amount_ex_vat or v_h.target_amount_vat<>0
   or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own)
   or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=v_h.allocation_result_id
    and a.instruction_id=v_i.id and a.state_id in (v_rep.draft_state_id,v_rep.prior_state_id))) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  if v_i.payroll_stage<>'NET_DEDUCT' then
   if v_own<>v_a.allocated_source_ex_vat or v_own>0 and (v_h.allocation_result_id<>v_a.id or v_h.allocation_pass_kind<>'DRAFT') then
    v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   if v_i.payroll_stage='GROSS_ADD' then v_rep.gross_additions:=v_rep.gross_additions+v_a.allocated_target_ex_vat;
   elsif v_i.payroll_stage='GROSS_DEDUCT' then v_rep.gross_deductions:=v_rep.gross_deductions+v_a.allocated_target_ex_vat;
   elsif v_i.payroll_stage='NET_ADD' then v_rep.net_additions:=v_rep.net_additions+v_a.allocated_target_inc_vat;
   else v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  else
   if v_own>v_cc.active_recovery_source_ex_vat or v_own>v_c.active_recovery_hold_amount
    or v_own>v_p.active_unrealised_recovery_source_ex_vat or v_own>v_a.allocated_source_ex_vat then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   v_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_cc.funded_source_ex_vat else v_cc.approved_source_ex_vat end)
    -v_cc.recovered_source_ex_vat-v_cc.written_off_source_ex_vat;
   v_case_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)
    -v_c.principal_recovered-v_c.principal_written_off;
   insert into private.bpay_next_net_staged_case_capacity(replacement_id,case_id,captured_case_revision,captured_outstanding_source,captured_active_recovery_source)
    values(v_rep.id,v_i.case_id,v_c.case_revision,v_case_outstanding,v_c.active_recovery_hold_amount) on conflict(replacement_id,case_id) do nothing;
   select dr37_point_44.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_44 where replacement_id=v_rep.id and case_id=v_i.case_id for update;
   if (v_carry.captured_case_revision,v_carry.captured_outstanding_source,v_carry.captured_active_recovery_source)
    is distinct from (v_c.case_revision,v_case_outstanding,v_c.active_recovery_hold_amount) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   v_capacity:=private.bpay_next_paye_net_capacity_v1(v_a.allocated_source_ex_vat,
    v_p.opening_due_source_ex_vat-v_p.realised_recovery_source_ex_vat-v_p.active_unrealised_recovery_source_ex_vat,
    v_outstanding-v_cc.active_recovery_source_ex_vat,
    v_case_outstanding,v_c.active_recovery_hold_amount,v_carry.proposed_release_source,v_carry.proposed_reserve_source,v_own);
   select k.* into strict v_kernel from private.bpay_next_case_allocation_row_v1(v_i.case_kind,v_i.nominal_target_ex_vat,v_capacity,
    v_e,v_t,v_i.minimum_earnings_threshold,v_i.take_home_floor_override,v_i.captured_default_floor) k;
   v_a.usable_capacity_target_ex_vat:=v_capacity;v_a.allocated_source_ex_vat:=v_kernel.taken_amount;
   v_a.allocated_target_ex_vat:=v_kernel.taken_amount;v_a.allocated_target_vat:=0;v_a.allocated_target_inc_vat:=v_kernel.taken_amount;
   v_a.shortfall_target_ex_vat:=v_i.nominal_target_ex_vat-v_kernel.taken_amount;v_a.cap_reason:=v_kernel.cap_reason;
   v_a.affordability_reason:=v_kernel.affordability_reason;v_a.capacity_reason:=v_kernel.capacity_reason;
   v_a.channel_headroom_before:=v_e;v_a.channel_headroom_after:=v_kernel.next_channel_headroom;
   v_a.worker_take_home_before:=v_t;v_a.worker_take_home_after:=v_kernel.next_worker_take_home;
   v_rep.staged_deduction_count:=v_rep.staged_deduction_count+1;
   v_rep.staged_old_hold_count:=v_rep.staged_old_hold_count+case when v_h.id is null then 0 else 1 end;
   v_rep.staged_old_source_total:=v_rep.staged_old_source_total+v_own;
   v_rep.proposed_hold_count:=v_rep.proposed_hold_count+case when v_kernel.taken_amount>0 then 1 else 0 end;
   v_rep.proposed_source_total:=v_rep.proposed_source_total+v_kernel.taken_amount;v_rep.recoveries:=v_rep.recoveries+v_kernel.taken_amount;
   v_e:=v_kernel.next_channel_headroom;v_t:=v_kernel.next_worker_take_home;
  end if;
  insert into private.bpay_next_net_staged_result(state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,
   result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
   allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
   cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after,
   replacement_id,draft_result_id,prior_hold_id,prior_source_amount,case_id,case_component_id,pay_week_start,
   captured_case_revision,captured_component_revision,captured_period_revision,payroll_stage)
   values(v_rep.target_state_id,v_i.id,v_rep.run_worker_id,v_rep.candidate_id,v_rep.preparation_revision,v_rep.selection_revision,
    v_rep.staged_instruction_count+1,'NET',v_i.allocation_channel,v_i.hold_purpose,v_a.nominal_target_ex_vat,v_a.usable_capacity_target_ex_vat,
    v_a.allocated_source_ex_vat,v_a.allocated_target_ex_vat,v_a.allocated_target_vat,v_a.allocated_target_inc_vat,v_a.shortfall_target_ex_vat,
    v_a.cap_reason,v_a.affordability_reason,v_a.capacity_reason,v_a.channel_headroom_before,v_a.channel_headroom_after,v_a.worker_take_home_before,v_a.worker_take_home_after,
    v_rep.id,v_a.id,v_h.id,v_own,v_i.case_id,v_i.case_component_id,v_i.pay_week_start,v_c.case_revision,v_cc.component_revision,v_p.period_revision,v_i.payroll_stage);
  if v_i.payroll_stage='NET_DEDUCT' then
   update private.bpay_next_net_staged_case_capacity set proposed_release_source=proposed_release_source+v_own,
    proposed_reserve_source=proposed_reserve_source+v_kernel.taken_amount,staged_instruction_count=staged_instruction_count+1
    where replacement_id=v_rep.id and case_id=v_i.case_id;end if;
  v_rep.staged_instruction_count:=v_rep.staged_instruction_count+1;v_rep.cursor_age_key:=v_i.age_key;
  v_rep.cursor_case_id:=v_i.case_id;v_rep.cursor_component_ordinal:=v_i.component_ordinal;v_rep.cursor_component_id:=v_i.case_component_id;
 end loop;
 select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
  and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
  and (v_rep.cursor_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
   >(v_rep.cursor_age_key,v_rep.cursor_case_id,v_rep.cursor_component_ordinal,v_rep.cursor_component_id))) into v_more;
 if not v_more and v_issue is null and v_rep.staged_instruction_count<>v_rep.expected_instruction_count then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGING_COUNT_MISMATCH';end if;
 update private.bpay_next_net_replacement set staged_instruction_count=v_rep.staged_instruction_count,
  staged_deduction_count=v_rep.staged_deduction_count,staged_old_hold_count=v_rep.staged_old_hold_count,staged_old_source_total=v_rep.staged_old_source_total,
  proposed_hold_count=v_rep.proposed_hold_count,proposed_source_total=v_rep.proposed_source_total,recoveries=v_rep.recoveries,
  gross_additions=v_rep.gross_additions,gross_deductions=v_rep.gross_deductions,net_additions=v_rep.net_additions,
  remaining_channel_headroom=v_e,remaining_take_home=v_t,cursor_age_key=v_rep.cursor_age_key,cursor_case_id=v_rep.cursor_case_id,
  cursor_component_ordinal=v_rep.cursor_component_ordinal,cursor_component_id=v_rep.cursor_component_id,
  staging_step=case when not v_more and v_issue is null then 'VALIDATE' else staging_step end,
  updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'issue_code',v_issue);
end $function$;

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
  v_rep private.bpay_next_net_replacement%rowtype;v_staged private.bpay_next_net_staged_result%rowtype;
  v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_reply jsonb;v_cancel uuid;
  v_bytes integer:=0;v_size integer;
  v_seen integer:=0;v_more boolean;v_issue text;v_terminal boolean:=false;v_requires_week boolean;
  v_e numeric;v_h numeric;v_own_amount numeric;v_capacity numeric;v_taken numeric;v_cash numeric;v_own_cash numeric;
  v_outstanding numeric;v_case_outstanding numeric;v_row record;v_result_id uuid;v_hold_id uuid;v_projection_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or (p_expected_cursor is not null and p_expected_cursor<1) or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_PAGE_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_paye_net_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.candidate_id=j.candidate_id
    where j.id=p_job_id and r.case_draft_state_id is not null;
  select dr37_point_54.* into strict v_run from private.bpay_next_pay_run dr37_point_54 where id=v_run_id for update nowait;
  select active_owner_epoch into strict v_control_epoch from private.bpay_next_worker_control where candidate_id=v_candidate for update nowait;
  select dr37_point_55.* into strict v_job from private.bpay_next_job dr37_point_55 where id=p_job_id for update nowait;
  select dr37_point_56.* into strict v_request from private.bpay_next_paye_net_request dr37_point_56 where command_id=v_job.command_id;
  select dr37_point_57.* into strict v_worker from private.bpay_next_run_worker dr37_point_57 where id=v_request.run_worker_id for update nowait;
  select dr37_point_58.* into strict v_draft from private.bpay_next_case_allocation_state dr37_point_58 where id=v_request.case_draft_state_id;
  select dr37_point_59.* into v_state from private.bpay_next_case_allocation_state dr37_point_59 where job_id=p_job_id and pass_kind='NET' for update nowait;
  select dr37_point_60.* into v_rep from private.bpay_next_net_replacement dr37_point_60 where id=v_job.command_id for update nowait;
  if v_job.job_kind<>'PAYE_NET_ENTRY' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_request.candidate_id<>v_candidate or v_draft.run_worker_id<>v_worker.id or v_draft.pass_kind<>'DRAFT'
     or v_draft.preparation_revision<>v_request.preparation_revision or v_draft.selection_revision<>v_request.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_JOB_SCOPE_INVALID';
  end if;
  -- Retained terminal identity survives lease takeover and later deliberate requests.
  if v_job.status='DONE' and v_rep.id is not null and v_rep.stage in ('COMPLETE','REFUSED','CANCELLED') then
    return pg_catalog.jsonb_build_object('phase',case v_rep.stage when 'COMPLETE' then 'PROJECTED' when 'REFUSED' then 'REFUSED' else 'DONE' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_rep.accepted_projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_rep.refusal_code,'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,
      'request_no',v_rep.request_no::text,'staged_count',v_rep.staged_instruction_count::text,
      'validated_count',v_rep.validated_instruction_count::text,'committed_count',v_rep.committed_instruction_count::text,
      'expected_count',v_rep.expected_instruction_count::text);
  end if;
  if v_job.status='DONE' and v_rep.id is null and v_state.id is not null and v_state.net_stage in ('COMPLETE','REVIEW') then
    return pg_catalog.jsonb_build_object('phase',case when v_state.net_stage='COMPLETE' then 'PROJECTED' else 'REVIEW' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_state.projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_state.net_issue_code);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_run.status<>'DRAFT'
     or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.captured_preparation_revision<>v_request.preparation_revision or v_worker.case_selection_revision<>v_request.selection_revision
     or v_worker.net_request_revision<>v_request.request_no or v_worker.net_projection_revision<>v_request.expected_projection_revision
     or v_rep.id is null or v_worker.net_replacement_pending_id is distinct from v_rep.id
     or v_worker.net_request_high_water<v_request.request_no
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
        'replay',true,'projection_id',null,'run_worker_id',v_worker.id,'issue_code',null,
        'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,'request_no',v_rep.request_no::text,
        'staged_count',v_rep.staged_instruction_count::text,'validated_count',v_rep.validated_instruction_count::text,
        'committed_count',v_rep.committed_instruction_count::text,'expected_count',v_rep.expected_instruction_count::text);
    end if;
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_CURSOR_STALE';
  end if;

  <<net_page>>
  begin
    if v_state.id is null then
      select dr37_point_61.* into strict v_week from private.bpay_next_worker_week dr37_point_61
        where candidate_id=v_candidate and pay_week_start=v_draft.pay_week_start for update nowait;
      -- Derive protected-week dependence in finite PRECHECK instruction pages.
      v_requires_week:=false;
      insert into private.bpay_next_case_allocation_state(run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,
        pass_kind,projection_no,command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
        expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,pay_week_start,captured_week_revision,
        captured_work_gross,net_stage,net_requires_week_binding)
        values(v_worker.id,v_run_id,v_candidate,v_request.selection_revision,v_request.preparation_revision,'NET',v_request.request_no,
          v_job.command_id,v_job.id,v_epoch,p_owner_epoch,
          (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),'BUILDING',
          case when v_requires_week then 'UNBOUND' else 'NOT_AFFECTED' end,v_draft.expected_instruction_count,
          coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0),v_draft.pay_week_start,v_week.period_revision,
          v_draft.captured_work_gross,'PRECHECK',v_requires_week)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0));
      update private.bpay_next_net_replacement set original_job_id=p_job_id,target_state_id=v_state.id,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id returning * into v_rep;
    elsif v_state.status<>'BUILDING' or v_state.net_stage='NOT_CASE_NET' or v_state.projection_no<>v_request.request_no then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STATE_INVALID';
    end if;
    if v_state.owner_epoch<>p_owner_epoch then
      update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
    end if;
    select dr37_point_62.* into strict v_week from private.bpay_next_worker_week dr37_point_62
      where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start for update nowait;
    select q.command_id into v_cancel from private.bpay_next_cancel_request q
      join private.bpay_next_command c on c.id=q.command_id
      where q.run_worker_id=v_worker.id and q.status='REQUESTED' and c.agency_sequence>v_job.command_sequence
      order by c.agency_sequence limit 1;
    if v_cancel is not null or v_rep.stage in ('CANCEL_REQUESTED','CANCELLING') then
      v_reply:=private.bpay_next_cancel_net_replacement_page_v1(v_rep.id,coalesce(v_rep.cancel_command_id,v_cancel),p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_terminal:=(v_reply->>'complete')::boolean;exit net_page;
    end if;
    if v_state.net_stage='PRECHECK' then
      v_reply:=private.bpay_next_net_precheck_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';exit net_page;
    end if;
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
        v_size:=pg_catalog.octet_length(pg_catalog.to_jsonb(v_prior)::text);
        if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
        if v_bytes+v_size>65536 then exit;end if;
        v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
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
    if v_state.weekly_binding_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_ALLOCATION_UNBOUND';end if;
    if v_rep.stage='PRECHECK' and v_state.net_stage='ALLOCATE' then
      update private.bpay_next_net_replacement set stage='STAGING',staging_step='INSTRUCTIONS',
        initial_take_home=v_state.initial_worker_take_home,remaining_take_home=v_state.remaining_worker_take_home,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id returning * into v_rep;
      update private.bpay_next_case_allocation_state set net_stage='STAGING' where id=v_state.id;
    end if;
    if v_rep.stage='STAGING' and v_rep.staging_step='INSTRUCTIONS' then
      v_reply:=private.bpay_next_stage_case_net_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';exit net_page;
    elsif v_rep.stage='STAGING' and v_rep.staging_step='VALIDATE' then
      perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
      begin
        insert into private.bpay_next_candidate_week_commit_fence as f(candidate_id,pay_week_start,command_id,run_worker_id,
          original_job_id,module_epoch,allocation_state_id,stage,replacement_id)
          values(v_candidate,v_state.pay_week_start,v_rep.id,v_worker.id,p_job_id,v_epoch,v_state.id,'STAGING',v_rep.id)
          on conflict(candidate_id,pay_week_start) do update set command_id=excluded.command_id,run_worker_id=excluded.run_worker_id,
            original_job_id=excluded.original_job_id,module_epoch=excluded.module_epoch,allocation_state_id=excluded.allocation_state_id,
            stage='STAGING',replacement_id=excluded.replacement_id,updated_at_utc=pg_catalog.transaction_timestamp()
          where f.stage in ('COMPLETE','REFUSED','CANCELLED') or f.command_id=excluded.command_id;
        if not found then raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
      exception when unique_violation then
        raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end;
      v_reply:=private.bpay_next_validate_case_net_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';
      select dr37_point_63.* into strict v_rep from private.bpay_next_net_replacement dr37_point_63 where id=v_rep.id;
      if v_issue is null and v_rep.validation_step='DONE' then
        update private.bpay_next_net_replacement set stage='COMMITTING',staging_step='DONE',
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
        update private.bpay_next_case_allocation_state set net_stage='COMMITTING' where id=v_state.id;
        update private.bpay_next_candidate_week_commit_fence set stage='COMMITTING',updated_at_utc=pg_catalog.transaction_timestamp()
          where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      end if;
      exit net_page;
    end if;
    if v_rep.stage<>'COMMITTING' or v_state.net_stage<>'COMMITTING' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_STAGE_INVALID';end if;
    perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
    select dr37_point_64.* into strict v_channel from private.bpay_next_case_allocation_channel dr37_point_64
      where state_id=v_state.id and allocation_channel='PAYE' for update nowait;
    perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'COMMIT');
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
        and i.selection_revision=v_request.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      select dr37_point_65.* into strict v_staged from private.bpay_next_net_staged_result dr37_point_65
        where replacement_id=v_rep.id and instruction_id=v_instruction.id;
      v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_staged),pg_catalog.to_jsonb(v_instruction))::text);
      if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
      if v_bytes+v_size>65536 then exit;end if;
      v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
      if v_staged.result_no<>v_state.processed_instruction_count+1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_SEQUENCE_INVALID';end if;
      select dr37_point_66.* into strict v_draft_result from private.bpay_next_case_allocation_result dr37_point_66
        where state_id=v_draft.id and instruction_id=v_instruction.id;
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE'
         or v_instruction.allocation_channel<>'PAYE' or v_instruction.currency<>'GBP'
         or v_instruction.nominal_target_vat<>0 or v_draft_result.allocated_target_vat<>0
         or v_draft_result.allocated_source_ex_vat<>v_draft_result.allocated_target_ex_vat then
        v_issue:='CASE_INPUT_UNBOUND';exit net_page;
      end if;
      select dr37_point_67.* into v_own from private.bpay_next_case_hold dr37_point_67 where instruction_id=v_instruction.id and status='ACTIVE' for update nowait;
      v_own_amount:=coalesce(v_own.source_reserved_ex_vat,0);
      if (v_own.id,v_own_amount) is distinct from (v_staged.prior_hold_id,v_staged.prior_source_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_OWN_HOLD_CHANGED';end if;
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
        select dr37_point_68.* into strict v_case from private.bpay_next_finance_case dr37_point_68 where id=v_instruction.case_id for update nowait;
        select dr37_point_69.* into strict v_component from private.bpay_next_case_component dr37_point_69 where id=v_instruction.case_component_id for update nowait;
        select dr37_point_70.* into strict v_period from private.bpay_next_case_period dr37_point_70
          where case_component_id=v_instruction.case_component_id and pay_week_start=v_instruction.pay_week_start for update nowait;
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
        select dr37_point_71.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_71
          where replacement_id=v_rep.id and case_id=v_instruction.case_id for update nowait;
        if v_case.case_revision<>v_staged.captured_case_revision+v_carry.committed_instruction_count
          or v_component.component_revision<>v_staged.captured_component_revision or v_period.period_revision<>v_staged.captured_period_revision
          or v_case.active_recovery_hold_amount<>v_carry.captured_active_recovery_source-v_carry.committed_release_source+v_carry.committed_reserve_source then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_DEPENDENCY_CHANGED';end if;
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
        if v_capacity<>v_staged.usable_capacity_target_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_CAPACITY_CHANGED';end if;
        select v_staged.allocated_target_ex_vat taken_amount,v_staged.channel_headroom_after next_channel_headroom,
          v_staged.worker_take_home_after next_worker_take_home,v_staged.cap_reason cap_reason,
          v_staged.affordability_reason affordability_reason,v_staged.capacity_reason capacity_reason into v_row;
        if (v_e,v_h) is distinct from (v_staged.channel_headroom_before,v_staged.worker_take_home_before) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_CARRY_CHANGED';end if;
        v_taken:=v_row.taken_amount;
        -- 0310 requires release/new exact rows, not an amount rewrite. Old
        -- reservation and all three active counters move in this same page.
        if v_own.id is not null then
          update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=pg_catalog.transaction_timestamp() where id=v_own.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_own.id,'RELEASED');
          update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_own.id;
        end if;
        insert into private.bpay_next_case_allocation_result(id,state_id,instruction_id,run_worker_id,candidate_id,
          preparation_revision,selection_revision,result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,
          usable_capacity_target_ex_vat,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,
          shortfall_target_ex_vat,cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,
          worker_take_home_before,worker_take_home_after)
          values(v_staged.id,v_state.id,v_instruction.id,v_worker.id,v_candidate,v_request.preparation_revision,v_request.selection_revision,
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
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold_id,'ACTIVE');
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
        update private.bpay_next_net_staged_case_capacity set committed_instruction_count=committed_instruction_count+1,
          committed_release_source=committed_release_source+v_own_amount,committed_reserve_source=committed_reserve_source+v_taken
          where replacement_id=v_rep.id and case_id=v_instruction.case_id;
        v_rep.committed_deduction_count:=v_rep.committed_deduction_count+1;
        v_rep.committed_old_hold_count:=v_rep.committed_old_hold_count+case when v_own.id is null then 0 else 1 end;
        v_rep.committed_old_source_total:=v_rep.committed_old_source_total+v_own_amount;
        v_rep.committed_new_hold_count:=v_rep.committed_new_hold_count+case when v_taken>0 then 1 else 0 end;
        v_rep.committed_new_source_total:=v_rep.committed_new_source_total+v_taken;
        v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
        v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        v_e:=v_row.next_channel_headroom;v_h:=v_row.next_worker_take_home;
      end if;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_rep.committed_instruction_count:=v_rep.committed_instruction_count+1;v_rep.commit_cursor:=v_staged.result_no;
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
    update private.bpay_next_net_replacement set committed_instruction_count=v_rep.committed_instruction_count,commit_cursor=v_rep.commit_cursor,
      committed_deduction_count=v_rep.committed_deduction_count,committed_old_hold_count=v_rep.committed_old_hold_count,
      committed_old_source_total=v_rep.committed_old_source_total,committed_new_hold_count=v_rep.committed_new_hold_count,
      committed_new_source_total=v_rep.committed_new_source_total,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
    if not v_more then
      if (v_rep.staged_instruction_count,v_rep.validated_instruction_count,v_rep.committed_instruction_count)
        is distinct from (v_rep.expected_instruction_count,v_rep.expected_instruction_count,v_rep.expected_instruction_count)
        or (v_rep.committed_deduction_count,v_rep.committed_old_hold_count,v_rep.committed_old_source_total,v_rep.committed_new_hold_count,v_rep.committed_new_source_total)
          is distinct from (v_rep.staged_deduction_count,v_rep.staged_old_hold_count,v_rep.staged_old_source_total,v_rep.proposed_hold_count,v_rep.proposed_source_total)
        or v_state.recovered_source_total<>v_rep.recoveries or v_state.recovered_target_total<>v_rep.recoveries then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_TOTALS_MISMATCH';end if;
      if v_state.processed_instruction_count<>v_state.expected_instruction_count
         or (v_state.allocated_gross_additions,v_state.allocated_gross_deductions,v_state.allocated_net_additions)
           is distinct from (v_draft.allocated_gross_additions,v_draft.allocated_gross_deductions,v_draft.allocated_net_additions) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_TOTALS_MISMATCH';
      end if;
      v_cash:=private.bpay_next_paye_net_cash_v1(v_request.entered_paye_net,v_state.allocated_net_additions,v_state.recovered_target_total);
      if v_cash<0 or v_cash>=10000000000000000 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_CASH_RANGE_INVALID';
      end if;
      select dr37_point_72.* into strict v_contribution from private.bpay_next_worker_week_contribution dr37_point_72 where original_run_worker_id=v_worker.id for update nowait;
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
      update private.bpay_next_net_replacement set stage='COMPLETE',accepted_projection_id=v_projection_id,cash_amount=v_cash,
        finished_at_utc=pg_catalog.transaction_timestamp(),updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
      update private.bpay_next_candidate_week_commit_fence set stage='COMPLETE',updated_at_utc=pg_catalog.transaction_timestamp()
        where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_run_worker set entered_paye_net=v_request.entered_paye_net,net_replacement_pending_id=null,
        net_projection_revision=v_request.request_no where id=v_worker.id;
      update private.bpay_next_job set status='DONE',phase='PROJECTED',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      v_terminal:=true;
    end if;
  end net_page;
  select dr37_point_73.* into strict v_rep from private.bpay_next_net_replacement dr37_point_73 where id=v_rep.id;
  if v_issue is not null then
    if v_rep.stage not in ('PRECHECK','STAGING') or v_rep.committed_instruction_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_LATE_REFUSAL_FORBIDDEN';end if;
    update private.bpay_next_net_replacement set stage='REFUSED',refusal_code=v_issue,
      finished_at_utc=pg_catalog.transaction_timestamp(),updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
    update private.bpay_next_case_allocation_state set net_stage='REFUSED',net_issue_code=v_issue,weekly_binding_state='UNBOUND',status='OUTDATED' where id=v_state.id;
    update private.bpay_next_candidate_week_commit_fence set stage='REFUSED',updated_at_utc=pg_catalog.transaction_timestamp()
      where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
    update private.bpay_next_run_worker set net_request_revision=net_projection_revision,net_replacement_pending_id=null,review_issue_code=null,review_issue_work_id=null where id=v_worker.id;
    insert into private.bpay_next_paye_net_request_refusal(command_id,run_worker_id,candidate_id,request_no,refusal_code)
      values(v_rep.id,v_worker.id,v_candidate,v_rep.request_no,v_issue);
    update private.bpay_next_run_worker set review_issue_code=v_issue,review_issue_work_id=null where id=v_worker.id;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(coalesce(v_checkpoint,0)+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  select dr37_point_74.* into strict v_job from private.bpay_next_job dr37_point_74 where id=p_job_id;
  select dr37_point_75.* into strict v_rep from private.bpay_next_net_replacement dr37_point_75 where id=v_rep.id;
  v_reply:=pg_catalog.jsonb_build_object('phase',case v_rep.stage when 'REFUSED' then 'REFUSED' when 'COMPLETE' then 'PROJECTED'
      when 'CANCELLED' then 'DONE' else 'FINANCE_ALLOCATE' end,'cursor',v_job.cursor_key,
    'rows_visited',v_seen::text,'replay',false,'projection_id',v_rep.accepted_projection_id,'run_worker_id',v_worker.id,'issue_code',v_rep.refusal_code,
    'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,'request_no',v_rep.request_no::text,
    'staged_count',v_rep.staged_instruction_count::text,'validated_count',v_rep.validated_instruction_count::text,
    'committed_count',v_rep.committed_instruction_count::text,'expected_count',v_rep.expected_instruction_count::text);
  if pg_catalog.octet_length(v_reply::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_REPLY_BYTES_EXCEEDED';end if;
  return v_reply;
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_active_epoch bigint;v_active_agency uuid;v_active_scope text;v_active_context jsonb;
  v_active_after_time timestamptz;v_active_after_id uuid;v_active_last jsonb;
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
  v_issue_run uuid;v_issue_epoch bigint;v_issue_original_epoch bigint;v_issue_wb bigint;v_issue_adoption bigint;
  v_issue_run_row private.bpay_next_pay_run%rowtype;v_issue_context jsonb;v_issue_after jsonb;v_issue_reply jsonb;v_issue_sort text;v_issue_desc boolean;
  v_issue_reason_labels jsonb;v_issue_fresh_context jsonb;v_issue_final jsonb;
begin
  if p_kind='PAYE_NET_PREVIEW' then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or not(p_args?&array['run_worker_id','entered_paye_net','after','limit'])
      or p_args-array['run_worker_id','entered_paye_net','after','limit']<>'{}'::jsonb
      or jsonb_typeof(p_args->'run_worker_id') is distinct from 'string' or p_args->>'run_worker_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or jsonb_typeof(p_args->'entered_paye_net') is distinct from 'string' or p_args->>'entered_paye_net'!~'^(0|[1-9][0-9]{0,15})(\.[0-9]{1,2})?$'
      or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100 then
      raise exception using errcode='22023',message='BPAY_NEXT_NET_PREVIEW_INPUT_INVALID';end if;
    return private.bpay_next_paye_net_preview_v1(p_actor_user_id,(p_args->>'run_worker_id')::uuid,
      (p_args->>'entered_paye_net')::numeric,nullif(p_args->'after','null'::jsonb),(p_args->>'limit')::integer);
  end if;
  if p_kind in ('CHANNEL_DRAFT_LIST','CHANNEL_DRAFT_COUNT') then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    return private.bpay_next_channel_draft_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='ACTIVE_PREPARATIONS' then
    -- Read-only native directory, never a Workbench money freshness admission.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    select owner_epoch into v_active_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
    if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    select installation_id into strict v_active_agency from private.cloudtms_database_identity where singleton;
    if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or p_args-array['agency_id','owner_epoch','scope','after','limit']<>'{}'::jsonb
      or not(p_args?&array['agency_id','owner_epoch','scope','after','limit'])
      or jsonb_typeof(p_args->'agency_id') is distinct from 'string' or p_args->>'agency_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or jsonb_typeof(p_args->'owner_epoch') is distinct from 'string' or p_args->>'owner_epoch'!~'^[1-9][0-9]{0,18}$'
      or jsonb_typeof(p_args->'scope') is distinct from 'string' or p_args->>'scope' not in ('ALL','MINE')
      or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]{0,2}$'
      or (p_args->>'limit')::integer>100 then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    if (p_args->>'agency_id')::uuid<>v_active_agency or p_args->>'owner_epoch'<>v_active_epoch::text then
      raise exception using errcode='55000',message='BPAY_NEXT_ACTIVE_PREPARATIONS_CONTEXT_CHANGED';end if;
    v_limit:=(p_args->>'limit')::integer;v_active_scope:=p_args->>'scope';
    v_active_context:=jsonb_build_object('agency_id',v_active_agency,'actor_user_id',p_actor_user_id,'owner_epoch',v_active_epoch::text,'scope',v_active_scope);
    if p_args->'after'<>'null'::jsonb then
      if jsonb_typeof(p_args->'after') is distinct from 'object'
        or (p_args->'after')-array['context','created_at_utc','run_id']<>'{}'::jsonb
        or not((p_args->'after')?&array['context','created_at_utc','run_id'])
        or jsonb_typeof(p_args#>'{after,created_at_utc}') is distinct from 'string'
        or p_args#>>'{after,created_at_utc}'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
        or jsonb_typeof(p_args#>'{after,run_id}') is distinct from 'string'
        or p_args#>>'{after,run_id}'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      if p_args#>'{after,context}' is distinct from v_active_context then
        raise exception using errcode='55000',message='BPAY_NEXT_ACTIVE_PREPARATIONS_CONTEXT_CHANGED';end if;
      v_active_after_time:=(p_args#>>'{after,created_at_utc}')::timestamptz;v_active_after_id:=(p_args#>>'{after,run_id}')::uuid;
    end if;
    for v_row in
      with native_all as (
        select r.* from private.bpay_next_pay_run r
          where v_active_scope='ALL' and r.status in ('PREPARING','REVIEW') and r.confirmed_at_utc is null and r.preparation_expires_at_utc is not null
            and (v_active_after_time is null or (r.created_at_utc,r.id)<(v_active_after_time,v_active_after_id))
            and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id
              where rc.run_id=r.id and c.command_kind='PREPARE' and c.module_epoch=v_active_epoch)
          order by r.created_at_utc desc,r.id desc limit v_limit+1
      ), native_mine as (
        select r.* from private.bpay_next_pay_run r
          where v_active_scope='MINE' and r.actor_user_id=p_actor_user_id and r.status in ('PREPARING','REVIEW') and r.confirmed_at_utc is null and r.preparation_expires_at_utc is not null
            and (v_active_after_time is null or (r.created_at_utc,r.id)<(v_active_after_time,v_active_after_id))
            and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id
              where rc.run_id=r.id and c.command_kind='PREPARE' and c.module_epoch=v_active_epoch)
          order by r.created_at_utc desc,r.id desc limit v_limit+1
      ), native_page as (
        select * from native_all union all select * from native_mine
      )
      select jsonb_build_object('run_id',r.id,'actor_user_id',r.actor_user_id,
        'actor_display_name',left(coalesce(nullif(btrim(u.display_name),''),'Unknown user'),250),
        'status',r.status,'selection_state',r.selection_state,'started_at_utc',r.created_at_utc,'expires_at_utc',r.preparation_expires_at_utc,
        'selected_candidates',r.selected_candidate_count::text,
        'ready',(case when d.command_id is not null then 0 else r.ready_candidate_count end)::text,
        'review',(case when d.command_id is not null then d.blocked_candidate_count else r.review_candidate_count end)::text,
        'removed',(case when d.command_id is not null then d.removed_candidate_count+d.already_removed_candidate_count else r.cancelled_candidate_count end)::text,
        'pending',(case when d.command_id is not null then d.expected_candidate_count-d.completed_candidate_count
          else r.selected_candidate_count-r.ready_candidate_count-r.review_candidate_count-r.cancelled_candidate_count end)::text,
        'preparing',(case when d.command_id is not null then d.enrolled_candidate_count-d.completed_candidate_count else r.preparing_candidate_count end)::text,
        'safe_to_close',true) value
      from native_page r left join public.tms_users u on u.id=r.actor_user_id
      left join private.bpay_next_preparation_discard_request d on d.run_id=r.id
      order by r.created_at_utc desc,r.id desc
    loop
      v_n:=v_n+1;if v_n>v_limit then v_complete:=false;exit;end if;
      if (v_row->>'ready')::bigint+(v_row->>'review')::bigint+(v_row->>'pending')::bigint+(v_row->>'removed')::bigint<>(v_row->>'selected_candidates')::bigint
        or (v_row->>'pending')::bigint<0 or (v_row->>'preparing')::bigint<0 or (v_row->>'preparing')::bigint>(v_row->>'pending')::bigint then
        raise exception using errcode='23514',message='BPAY_NEXT_PROGRESS_COUNTERS_INVALID';end if;
      -- Leave ample space for wrapper/context/cursor; never skip an unreturned native row.
      if octet_length(v_rows::text)+octet_length(v_row::text)+8192>120*1024 then
        if jsonb_array_length(v_rows)=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
        v_complete:=false;exit;
      end if;
      v_rows:=v_rows||jsonb_build_array(v_row);v_active_last:=v_row;
    end loop;
    return jsonb_build_object('context',v_active_context,'rows',v_rows,'complete',v_complete,
      'next_cursor',case when v_complete then null else jsonb_build_object('context',v_active_context,
        'created_at_utc',v_active_last->'started_at_utc','run_id',v_active_last->'run_id') end);
  end if;
  if p_kind in ('REVIEW_TOTALS','REVIEW_CASES') then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    if p_kind='REVIEW_TOTALS' then
      if p_args-array['run_id']<>'{}'::jsonb or not(p_args?'run_id') then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      return private.bpay_next_run_review_totals_v1(p_actor_user_id,(p_args->>'run_id')::uuid);
    end if;
    if p_args-array['run_id','run_worker_id','sort','after','limit']<>'{}'::jsonb or not(p_args?&array['run_id','run_worker_id','sort','after','limit'])
      or jsonb_typeof(p_args->'run_worker_id') is distinct from 'string' or p_args->>'run_worker_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'limit') is distinct from 'number'
      or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100 then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_run_review_cases_v1(p_actor_user_id,(p_args->>'run_id')::uuid,(p_args->>'run_worker_id')::uuid,p_args->>'sort',p_args->'after',(p_args->>'limit')::integer);
  end if;
  if p_kind='PREPARE_ISSUES' then
    -- DR15 read-only original selection/diagnostic facts; no economic authority.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    select owner_epoch into v_issue_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
    if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or pg_catalog.octet_length(p_args::text)>32768 or p_args-array['run_id','view','sort','after','limit','row_key','waiting_only']<>'{}'::jsonb
      or not(p_args ?& array['run_id','view','sort','after','limit','row_key','waiting_only'])
      or jsonb_typeof(p_args->'waiting_only') is distinct from 'boolean' or (p_args->'waiting_only'='true'::jsonb and p_args->>'view'<>'CANDIDATES')
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->>'view' not in ('CANDIDATES','NOT_IN_DRAFT') or jsonb_typeof(p_args->'view') is distinct from 'string'
      or p_args->>'sort' not in ('CANDIDATE_ASC','CANDIDATE_DESC','PAYMENT_ASC','PAYMENT_DESC','REASON_ASC','REASON_DESC','GROSS_ASC','GROSS_DESC','STATE_ASC','STATE_DESC','CHANNEL_ASC','CHANNEL_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC')
      or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'limit') is distinct from 'number'
      or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100
      or (p_args->'row_key'<>'null'::jsonb and (jsonb_typeof(p_args->'row_key') is distinct from 'string' or length(p_args->>'row_key') not between 1 and 160)) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    v_issue_run:=(p_args->>'run_id')::uuid;v_limit:=(p_args->>'limit')::integer;v_issue_sort:=p_args->>'sort';v_issue_desc:=right(v_issue_sort,4)='DESC';
    select r.* into v_issue_run_row from private.bpay_next_pay_run r where r.id=v_issue_run;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_RUN_MISSING';end if;
    select c.module_epoch into v_issue_original_epoch from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=v_issue_run;
    v_issue_original_epoch:=coalesce(v_issue_original_epoch,(v_issue_run_row.accepted_choice_descriptor#>>'{namespace,owner_epoch}')::bigint);
    if v_issue_original_epoch is distinct from v_issue_epoch then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    select workbench_revision,adoption_revision into v_issue_wb,v_issue_adoption from private.bpay_next_workbench_revision where id=1;
    v_issue_context:=jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',v_issue_epoch::text,
      'review_revision',v_issue_run_row.review_revision::text,'run_status',v_issue_run_row.status,'selection_state',v_issue_run_row.selection_state,
      'selection_pages',v_issue_run_row.selection_page_count::text,'selected_candidates',v_issue_run_row.selected_candidate_count::text,
      'preparing',v_issue_run_row.preparing_candidate_count::text,'ready',v_issue_run_row.ready_candidate_count::text,
      'review',v_issue_run_row.review_candidate_count::text,'removed',v_issue_run_row.cancelled_candidate_count::text,
      'workbench_revision',v_issue_wb::text,'adoption_revision',v_issue_adoption::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key','waiting_only',p_args->'waiting_only','paye_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='PAYE'),'umbrella_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='UMBRELLA'));
    v_issue_after:=p_args->'after';
    if v_issue_after<>'null'::jsonb then
      if jsonb_typeof(v_issue_after) is distinct from 'object' or pg_catalog.octet_length(v_issue_after::text)>8192 or v_issue_after-array['context','key']<>'{}'::jsonb or not(v_issue_after ?& array['context','key'])
        or jsonb_typeof(v_issue_after->'key') is distinct from 'array' or jsonb_array_length(v_issue_after->'key')<>3
        or jsonb_typeof(v_issue_after#>'{key,0}') is distinct from 'boolean'
        or jsonb_typeof(v_issue_after#>'{key,2}') is distinct from 'string' or length(v_issue_after#>>'{key,2}') not between 1 and 160
        or (v_issue_after#>'{key,1}'<>'null'::jsonb and (jsonb_typeof(v_issue_after#>'{key,1}') is distinct from 'string' or length(v_issue_after#>>'{key,1}')>8192))
        or ((v_issue_after#>>'{key,0}')::boolean is distinct from (v_issue_after#>'{key,1}'='null'::jsonb))
        or (split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_after#>'{key,1}'<>'null'::jsonb and v_issue_after#>>'{key,1}'!~'^-?(0|[1-9][0-9]{0,15})(\.[0-9]{1,6})?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      if v_issue_after->'context' is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    end if;
    v_issue_reason_labels:=case when p_args->>'view'='NOT_IN_DRAFT' then '{"SOURCE_PAY_QUERY_OPEN":"Pay query open","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Could not check the pay query","SELECTED_WORK_NOT_APPROVED":"Changed after Create Draft — next Draft","SELECTED_WORK_REVISION_CHANGED":"Changed after Create Draft — next Draft","SELECTED_WORK_POSITION_PENDING":"Changed after Create Draft — next Draft","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb else '{"SOURCE_PAY_QUERY_OPEN":"Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Pay Query evidence could not be confirmed — check the Timesheet''s queries","SELECTED_WORK_NOT_APPROVED":"The Timesheet is no longer authorised.","SELECTED_WORK_REVISION_CHANGED":"The authorised Timesheet changed after Create Draft.","SELECTED_WORK_POSITION_PENDING":"The authorised Timesheet changed after Create Draft.","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb end;
    with choice_skips as (
      select 'SKIP:'||m.work_id::text row_key,'SKIPPED_WORK'::text origin,m.candidate_id,null::uuid worker_id,m.work_id,m.expected_revision_id revision_id,
        'NOT_IN_DRAFT'::text state,m.skip_code code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.selected and m.skip_code is not null
    ), page_skips as (
      select distinct on (w.id) 'PAGE:'||p.page_no::text||':'||(s.value->>'work_id') row_key,'SKIPPED_WORK'::text origin,w.candidate_id,null::uuid worker_id,w.id work_id,
        (ch.value->>'expected_revision_id')::uuid revision_id,'NOT_IN_DRAFT'::text state,s.value->>'code' code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_selection_page p cross join lateral jsonb_array_elements(p.skipped_json) s(value)
      join private.bpay_next_work w on w.id=(s.value->>'work_id')::uuid
      left join lateral (select value from jsonb_array_elements(coalesce(p.choices_request_json,'[]'::jsonb)) where value->>'work_id'=w.id::text limit 1) ch on true
      where p.run_id=v_issue_run and not exists(select 1 from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.work_id=w.id and m.selected and m.skip_code is not null)
      order by w.id,p.page_no
    ), releases as (
      select 'RELEASE:'||q.run_worker_id::text row_key,case when q.initiator_kind='SYSTEM' then 'SYSTEM_RELEASE' else 'OFFICE_RELEASE' end origin,
        q.candidate_id,q.run_worker_id worker_id,q.original_issue_work_id work_id,coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        'NOT_IN_DRAFT'::text state,case when q.initiator_kind='SYSTEM' then q.original_issue_code else 'DRAFT_CANDIDATE_REMOVED' end code,
        null::numeric gross,q.target_preparation_revision preparation_revision
      from private.bpay_next_worker_reprepare_request q
      left join private.bpay_next_run_work rw on rw.run_worker_id=q.run_worker_id and rw.work_id=q.original_issue_work_id and rw.candidate_id=q.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=q.original_issue_work_id and cm.candidate_id=q.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=q.original_issue_work_id
      where q.run_id=v_issue_run and q.action='RELEASE' and q.status='COMPLETE' and q.stage='COMPLETE' and q.finished_at_utc is not null
    ), expired as (
      select 'EXPIRED:'||x.candidate_id::text row_key,'EXPIRED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'PREPARATION_EXPIRED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_expiry_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='EXPIRED' and x.stage='COMPLETE' and x.finished_at_utc is not null
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), discarded as (
      select 'DISCARDED:'||x.candidate_id::text row_key,'DISCARDED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'DRAFT_DISCARDED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_discard_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='DISCARDED' and x.stage='COMPLETE' and x.finished_at_utc is not null and x.outcome in ('REMOVED','ALREADY_REMOVED')
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), candidates as (
      select 'CANDIDATE:'||sc.candidate_id::text row_key,'CANDIDATE'::text origin,sc.candidate_id,rw.id worker_id,rw.review_issue_work_id work_id,
        coalesce(cw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        coalesce(rw.status,case when cmd.status='FAILED' then 'FAILED' else 'PENDING' end) state,coalesce(rw.review_issue_code,case when rw.id is null and cmd.status='FAILED' then 'PREPARATION_ENROLMENT_FAILED' end) code,
        case when rw.status in ('READY','DRAFT','ISSUED','COMPLETE') or (rw.status='REVIEW' and exists(select 1 from private.bpay_next_case_allocation_state completed
        where completed.run_worker_id=rw.id and completed.preparation_revision=rw.captured_preparation_revision and completed.selection_revision=rw.case_selection_revision
          and completed.pass_kind='DRAFT' and completed.projection_no=0 and completed.completed_at_utc is not null)) then rw.gross_ex_vat end gross,rw.preparation_revision
      from private.bpay_next_selection_candidate sc
      left join private.bpay_next_run_worker rw on rw.run_id=sc.run_id and rw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_work cw on cw.run_worker_id=rw.id and cw.work_id=rw.review_issue_work_id and cw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=sc.run_id and cm.member_kind='WORK' and cm.work_id=rw.review_issue_work_id and cm.candidate_id=sc.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=sc.run_id and wc.work_id=rw.review_issue_work_id
      left join private.bpay_next_run_command rc on rc.run_id=sc.run_id left join private.bpay_next_command cmd on cmd.id=rc.command_id
      where sc.run_id=v_issue_run
    ), facts as (
      select * from choice_skips union all select * from page_skips union all select * from releases union all select * from expired union all select * from discarded union all select * from candidates
    ), labelled as (
      select f.*,
        case when f.origin='CANDIDATE' then coalesce(review_worker.target_pay_channel,selected_candidate.target_pay_channel) end pay_channel,
        case when f.origin='CANDIDATE' and contribution.frozen_offer_complete then contribution.gross_stage_deductions+contribution.net_recovery_reserved end deduction_total,
        f.origin='CANDIDATE' and coalesce(contribution.frozen_offer_complete,false) frozen_offer_complete,
        case when f.origin='CANDIDATE' then coalesce(case_selection.item_count,0) end case_count,
        wr.physical_timesheet_id,wr.physical_timesheet_version,wr.timesheet_reference,wr.week_ending_date,
        coalesce(wr.candidate_display_name,original_label.candidate_display_name,case when f.work_id is null then coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),'')) end) candidate_name,
        case when wr.id is not null then wr.candidate_reference when original_label.id is not null then original_label.candidate_reference else case when f.work_id is null then c.tms_ref end end candidate_reference,
        case when wr.id is not null or original_label.id is not null then 'CAPTURED_WORK' when f.work_id is null then 'CURRENT_CANDIDATE' else 'UNKNOWN_ORIGINAL' end label_origin,
        x.outcome discard_outcome,x.blocked_code discard_blocked_code,
        wait.waiting_run_id is not null waiting_on_other_run,wait.waiting_run_id,wait.waiting_actor_display_name,
        f.origin='CANDIDATE' and exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=f.worker_id
          and i.preparation_revision=(select own.captured_preparation_revision from private.bpay_next_run_worker own where own.id=f.worker_id)
          and i.other_run_capacity_reduction_target_ex_vat>0) capacity_reduced_by_other_run
      from facts f
      left join private.bpay_next_run_worker review_worker on review_worker.id=f.worker_id and review_worker.run_id=v_issue_run
      left join private.bpay_next_selection_candidate selected_candidate on selected_candidate.run_id=v_issue_run and selected_candidate.candidate_id=f.candidate_id
      left join private.bpay_next_case_selection case_selection on case_selection.run_id=v_issue_run and case_selection.candidate_id=f.candidate_id
        and case_selection.selection_revision=review_worker.case_selection_revision and case_selection.status='SEALED'
      left join lateral private.bpay_next_run_channel_contribution_v1(review_worker) contribution on f.origin='CANDIDATE'
      left join lateral private.bpay_next_preparation_wait_owner_v1(v_issue_run,f.candidate_id) wait on f.origin='CANDIDATE'
      left join private.bpay_next_work w on w.id=f.work_id and w.candidate_id=f.candidate_id
      left join private.bpay_next_work_revision wr on wr.work_id=w.id and wr.id=f.revision_id
      left join lateral (
        select r.id,r.candidate_display_name,r.candidate_reference from private.bpay_next_run_choice_member m
        join private.bpay_next_work_revision r on r.work_id=m.work_id and r.id=m.expected_revision_id
        where m.run_id=v_issue_run and m.candidate_id=f.candidate_id and m.member_kind='WORK' and m.selected order by m.member_no limit 1
      ) original_label on f.work_id is null and wr.id is null
      left join public.candidates c on c.id=f.candidate_id
      left join private.bpay_next_preparation_discard_worker x on x.run_id=v_issue_run and x.candidate_id=f.candidate_id and f.origin='CANDIDATE'
    ), scoped as (
      select l.*,case split_part(v_issue_sort,'_',1) when 'CANDIDATE' then lower(l.candidate_name) when 'PAYMENT' then case when l.timesheet_reference is not null then 'Timesheet '||l.timesheet_reference when l.work_id is null then 'Payments' else 'Timesheet' end
        when 'REASON' then case when l.waiting_on_other_run then 'Waiting for another Draft'||coalesce(' started by '||l.waiting_actor_display_name,'')||'.' else coalesce(v_issue_reason_labels->>l.code,case when l.state='PENDING' then 'Waiting to prepare' when l.state in ('PREPARING','CANCELLING') then 'Preparing' else 'Could not prepare this Candidate; contact support.' end) end
        when 'STATE' then case when l.waiting_on_other_run then '1:Waiting' else (case l.state when 'REVIEW' then '0' when 'FAILED' then '0' when 'PREPARING' then '1' when 'PENDING' then '2' when 'READY' then '3' when 'DRAFT' then '4' when 'ISSUED' then '5' when 'COMPLETE' then '6' when 'CANCELLING' then '7' else '8' end)||':'||case l.state when 'FAILED' then 'To review' when 'DRAFT' then 'Confirmed' when 'ISSUED' then 'Issued' when 'COMPLETE' then 'Complete' when 'NOT_IN_DRAFT' then 'Not in this Draft' when 'PENDING' then 'Waiting to prepare' when 'PREPARING' then 'Preparing' when 'REVIEW' then 'To review' when 'READY' then 'Ready' when 'CANCELLING' then 'Removing' when 'CANCELLED' then 'Not in this Draft' else l.state end end
        when 'GROSS' then l.gross::text when 'CHANNEL' then case l.pay_channel when 'UMBRELLA' then 'Umbrella' else l.pay_channel end when 'DEDUCTIONS' then l.deduction_total::text end sort_value
      from labelled l where ((p_args->>'view'='NOT_IN_DRAFT' and l.origin<>'CANDIDATE') or (p_args->>'view'='CANDIDATES' and l.origin='CANDIDATE')) and (p_args->'waiting_only'='false'::jsonb or l.waiting_on_other_run)
    ), tail as (
      select sort_value from scoped where row_key=v_issue_after#>>'{key,2}'
    ), counts as (
      select (select count(distinct work_id) from facts where origin='SKIPPED_WORK') skipped_timesheets,
        (select count(distinct candidate_id) from facts where origin in ('SYSTEM_RELEASE','OFFICE_RELEASE','EXPIRED','DISCARDED')) removed_candidates,
        (select count(*) from scoped) scope_count,
        (select count(*) from tail where sort_value is not distinct from v_issue_after#>>'{key,1}') tail_matches
    ), bounded as (
      select n.* from scoped n where (p_args->'row_key'='null'::jsonb or n.row_key=p_args->>'row_key')
        and (v_issue_after='null'::jsonb or case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') then
          case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C") end
          else case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C") end end)
      order by case when not v_issue_desc then n.sort_value is null end asc,case when v_issue_desc then n.sort_value is null end desc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and not v_issue_desc then n.sort_value::numeric end asc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_desc then n.sort_value::numeric end desc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and not v_issue_desc then n.sort_value end collate "C" asc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and v_issue_desc then n.sort_value end collate "C" desc,
        case when not v_issue_desc then n.row_key end collate "C" asc,case when v_issue_desc then n.row_key end collate "C" desc limit v_limit+1
    ) select jsonb_build_object('skipped_timesheets',counts.skipped_timesheets::text,'removed_candidates',counts.removed_candidates::text,'scope_count',counts.scope_count::text,'tail_matches',counts.tail_matches::text,
      'rows',coalesce((select jsonb_agg(jsonb_build_object('row_key',b.row_key,'origin',b.origin,'candidate_id',b.candidate_id,'run_worker_id',b.worker_id,'worker_status',b.state,'issue_code',b.code,
        'work_id',b.work_id,'expected_revision_id',b.revision_id,'physical_timesheet_id',b.physical_timesheet_id,'physical_timesheet_version',b.physical_timesheet_version::text,
        'candidate_display_name',b.candidate_name,'candidate_reference',b.candidate_reference,'label_origin',b.label_origin,'timesheet_reference',b.timesheet_reference,'week_ending_date',b.week_ending_date,
        'frozen_ex_vat',b.gross::text,'pay_channel',b.pay_channel,'deduction_total',b.deduction_total::text,'frozen_offer_complete',b.frozen_offer_complete,'case_count',b.case_count::text,'preparation_revision',b.preparation_revision::text,'discard_outcome',b.discard_outcome,'discard_blocked_code',b.discard_blocked_code,
        'waiting_on_other_run',b.waiting_on_other_run,'waiting_run_id',b.waiting_run_id,'waiting_actor_display_name',b.waiting_actor_display_name,'capacity_reduced_by_other_run',b.capacity_reduced_by_other_run,
        'sort_key',jsonb_build_array(b.sort_value is null,b.sort_value,b.row_key)) order by
        case when not v_issue_desc then b.sort_value is null end asc,case when v_issue_desc then b.sort_value is null end desc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and not v_issue_desc then b.sort_value::numeric end asc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_desc then b.sort_value::numeric end desc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and not v_issue_desc then b.sort_value end collate "C" asc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and v_issue_desc then b.sort_value end collate "C" desc,
        case when not v_issue_desc then b.row_key end collate "C" asc,case when v_issue_desc then b.row_key end collate "C" desc) from bounded b),'[]'::jsonb)) into v_issue_reply from counts;
    if v_issue_after<>'null'::jsonb and v_issue_reply->>'tail_matches'<>'1' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_complete:=jsonb_array_length(v_issue_reply->'rows')<=v_limit;v_rows:='[]'::jsonb;v_n:=0;
    for v_row in select value from jsonb_array_elements(v_issue_reply->'rows') limit v_limit loop
      if exists(select 1 from jsonb_each_text(v_row) e where e.key in ('candidate_display_name','candidate_reference','timesheet_reference') and pg_catalog.octet_length(e.value)>8192) then
        raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows||jsonb_build_array(v_row),'complete',false,
        'next_cursor',jsonb_build_object('context',v_issue_context,'key',v_row->'sort_key'),'counts',v_issue_reply-array['rows','tail_matches']);
      if pg_catalog.octet_length(v_issue_final::text)>120000 then
        if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
        v_complete:=false;exit;end if;
      v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
    end loop;
    -- The same native context closes a first-read/concurrent-commit gap.
    select jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
      'review_revision',r.review_revision::text,'run_status',r.status,'selection_state',r.selection_state,
      'selection_pages',r.selection_page_count::text,'selected_candidates',r.selected_candidate_count::text,
      'preparing',r.preparing_candidate_count::text,'ready',r.ready_candidate_count::text,
      'review',r.review_candidate_count::text,'removed',r.cancelled_candidate_count::text,
      'workbench_revision',b.workbench_revision::text,'adoption_revision',b.adoption_revision::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key','waiting_only',p_args->'waiting_only','paye_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='PAYE'),'umbrella_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='UMBRELLA')) into v_issue_fresh_context
      from private.bpay_next_pay_run r cross join private.bpay_next_module_control m cross join private.bpay_next_workbench_revision b
      where r.id=v_issue_run and m.id=1 and m.active_owner='NEXT' and b.id=1;
    if v_issue_fresh_context is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows,'complete',v_complete,
      'next_cursor',case when v_complete then null else jsonb_build_object('context',v_issue_context,'key',v_rows->-1->'sort_key') end,
      'counts',v_issue_reply-array['rows','tail_matches']);
    if pg_catalog.octet_length(v_issue_final::text)>120000 or (not v_complete and pg_catalog.octet_length((v_issue_final->'next_cursor')::text)>8192) then
      raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
    return v_issue_final;
  end if;
  if p_kind in ('CASE_AUDIT','CANDIDATE_CASE_REPORT') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_finance_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind in ('CASE_LIST','CASE_SUMMARY') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_list_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='CASE_CHANNEL_SUGGEST' then
    return private.bpay_next_case_channel_preview_v1(p_actor_user_id,p_args);
  end if;
  if p_kind='CASE_RESTRUCTURE_PREVIEW' then
    return private.bpay_next_case_restructure_preview_v1(p_actor_user_id,p_args);
  end if;
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args ? 'case_id' or p_args ? 'expected_case_revision' then
      if jsonb_typeof(p_args) is distinct from 'object' or p_args-array['input','case_id','expected_case_revision']<>'{}'::jsonb
        or not(p_args ?& array['input','case_id','expected_case_revision'])
        or jsonb_typeof(p_args->'case_id') is distinct from 'string'
        or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
        or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
        or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      return jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id,
        (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true);
    end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  if p_kind='LINES' and p_args?'worksheet_run_id' then
    perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
    perform private.bpay_next_preparation_args_v1(p_args,array['run_worker_id','worksheet_run_id','after_line_no','limit']);
    if jsonb_typeof(p_args->'run_worker_id') is distinct from 'string' or p_args->>'run_worker_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
     or jsonb_typeof(p_args->'worksheet_run_id') is distinct from 'string' or p_args->>'worksheet_run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
     or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100
     or (p_args->'after_line_no'<>'null'::jsonb and (jsonb_typeof(p_args->'after_line_no') is distinct from 'string' or p_args->>'after_line_no'!~'^[0-9]{1,18}$')) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_channel_drafts_page_v1((p_args->>'worksheet_run_id')::uuid);
    if not exists(select 1 from private.bpay_next_run_worker w where w.id=(p_args->>'run_worker_id')::uuid and w.run_id=(p_args->>'worksheet_run_id')::uuid and w.target_pay_channel='PAYE') then
      raise exception using errcode='55000',message='BPAY_NEXT_PAYE_WORKSHEET_CONTEXT_CHANGED';end if;
    v_limit:=(p_args->>'limit')::integer;
    for v_row in select jsonb_build_object('run_line_id',l.id,'line_no',l.line_no::text,
      'original_timesheet_id',work.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
      'component_kind',al.component_kind,'work_date',al.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
      'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text,
      'client_display_name',wr.client_display_name,'week_ending_date',wr.week_ending_date,'unit_label',al.unit_label,'approved_quantity',al.approved_quantity::text,'approved_unit_rate',al.approved_unit_rate::text)
      from private.bpay_next_run_line l join private.bpay_next_run_work rw on rw.id=l.run_work_id and rw.run_worker_id=l.run_worker_id and rw.captured_revision_id=l.captured_revision_id and rw.work_id=l.work_id
      join private.bpay_next_work_revision wr on wr.id=l.captured_revision_id and wr.work_id=l.work_id
      join private.bpay_next_work work on work.id=rw.work_id and work.candidate_id=wr.candidate_id
      join private.bpay_next_approved_line al on al.id=l.approved_line_id and al.revision_id=l.captured_revision_id and al.component_key=l.component_key
      where l.run_worker_id=(p_args->>'run_worker_id')::uuid and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
      order by l.line_no limit v_limit loop
      if octet_length(v_rows::text)+octet_length(v_row::text)>100000 then
        if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;v_complete:=false;exit;end if;
      v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
    end loop;
    v_complete:=v_complete and v_n<v_limit;
    return jsonb_build_object('rows',v_rows,'complete',v_complete,'next_cursor',case when v_complete then null else jsonb_build_object('after_line_no',v_rows->-1->>'line_no') end);
  end if;
  if p_kind='PAYE_WORKSHEET' then
    perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
    if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or p_args-array['run_id','pay_channel','candidate_id','query','sort','after','limit']<>'{}'::jsonb
      or not(p_args?&array['run_id','pay_channel','candidate_id','query','sort','after','limit'])
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->'pay_channel' is distinct from '"PAYE"'::jsonb
      or (p_args->'candidate_id'<>'null'::jsonb and (jsonb_typeof(p_args->'candidate_id') is distinct from 'string' or p_args->>'candidate_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
      or jsonb_typeof(p_args->'query') is distinct from 'string' or length(p_args->>'query')>120
      or jsonb_typeof(p_args->'sort') is distinct from 'string'
      or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100
      or jsonb_typeof(p_args->'after') not in ('null','object') then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_paye_worksheet_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,(p_args->>'candidate_id')::uuid,
      p_args->>'query',p_args->>'sort',p_args->'after',(p_args->>'limit')::integer);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  -- XD01/A3e: only this existing kind's optional sorted reader. The native
  -- common args owner remains unchanged and validates the original three keys.
  if p_kind='CURRENT_STORED_CREDITS' and p_args ? 'sort' then
    if pg_catalog.jsonb_typeof(p_args->'sort') is distinct from 'string'
      or p_args->>'sort' not in ('CREDIT_ASC','CREDIT_DESC','CREATED_ASC','CREATED_DESC','AMOUNT_ASC','AMOUNT_DESC','PAID_TO_ASC','PAID_TO_DESC') then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_preparation_args_v1(p_args-'sort',array['candidate_id','after_legacy_case_id','limit']);
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,(p_args->>'limit')::integer,p_args->>'sort');
  end if;
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']||case when p_args ? 'attention_only' then array['attention_only'] else array[]::text[] end
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
      ||case when p_args ? 'edit_command_id' then array['edit_command_id'] else array[]::text[] end
      ||case when p_args ? 'pause_command_id' then array['pause_command_id'] else array[]::text[] end
      ||case when p_args ? 'restructure_command_id' then array['restructure_command_id'] else array[]::text[] end
      ||case when p_args ? 'channel_command_id' then array['channel_command_id'] else array[]::text[] end
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='WORKERS' and p_args ? 'attention_only' then
    if p_args->'attention_only' is distinct from 'true'::jsonb then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_attention_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,(p_args->>'limit')::integer);
  end if;
  if p_kind='PROGRESS' then
    v_progress:=private.bpay_next_preparation_progress_v1((p_args->>'run_id')::uuid);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    v_progress:=private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
    if p_args ? 'edit_command_id' then
      if jsonb_typeof(p_args->'edit_command_id') is distinct from 'string'
        or p_args->>'edit_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_edit_receipt_v1((p_args->>'edit_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('edit_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if p_args ? 'pause_command_id' then
      if jsonb_typeof(p_args->'pause_command_id') is distinct from 'string'
         or p_args->>'pause_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_PAUSE_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_pause_receipt_v1((p_args->>'pause_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('pause_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if pg_catalog.octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    if p_args ? 'restructure_command_id' then
      if jsonb_typeof(p_args->'restructure_command_id') is distinct from 'string'
        or p_args->>'restructure_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_RESTRUCTURE_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_restructure_receipt_v1((p_args->>'restructure_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('restructure_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    if p_args ? 'channel_command_id' then
      if jsonb_typeof(p_args->'channel_command_id') is distinct from 'string'
        or p_args->>'channel_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_CHANNEL_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_channel_receipt_v1((p_args->>'channel_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('channel_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    return v_progress;
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code,
        'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        left join private.bpay_next_preparation_discard_worker x on x.run_id=(p_args->>'run_id')::uuid and x.candidate_id=w.candidate_id
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;

create or replace function private.bpay_next_accept_case_net_request_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_input_kind text,
  p_entered_paye_net numeric,p_expected_projection_revision bigint,p_preview_capacity_revision bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_sequence bigint;v_request_no bigint;v_latest_no bigint;
  v_replacement private.bpay_next_net_replacement%rowtype;v_prior_projection uuid;v_prior_state uuid;
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
  select dr37_point_16.* into strict v_run from private.bpay_next_pay_run dr37_point_16 where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select dr37_point_17.* into strict v_worker from private.bpay_next_run_worker dr37_point_17 where id=p_run_worker_id for update;
  select dr37_point_18.* into v_prior from private.bpay_next_paye_net_request dr37_point_18 where command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>p_run_worker_id or v_prior.actor_user_id is distinct from p_actor_user_id
       or v_prior.input_kind<>p_input_kind or v_prior.entered_paye_net is distinct from p_entered_paye_net
       or v_prior.expected_projection_revision is distinct from p_expected_projection_revision
       or v_prior.preview_capacity_revision is distinct from p_preview_capacity_revision
       or v_prior.case_draft_state_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='PAYE_NET_ENTRY';
    select dr37_point_19.* into v_replacement from private.bpay_next_net_replacement dr37_point_19 where id=p_command_id;
    if not found then
      -- Retain the historical 0559 receipt; never invent/backfill an operation.
      return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_prior.request_no::text,
        'replay',true,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id);
    end if;
    return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_prior.request_no::text,
      'replay',true,'phase',case v_replacement.stage when 'COMPLETE' then 'PROJECTED'
        when 'REFUSED' then 'REFUSED' when 'CANCELLED' then 'CANCELLED' else 'ACCEPTED_PENDING_PROJECTION' end,
      'run_worker_id',v_worker.id,'replacement_id',v_replacement.id,'replacement_stage',v_replacement.stage,
      'projection_id',v_replacement.accepted_projection_id,'issue_code',v_replacement.refusal_code);
  end if;
  -- Exact stored replay above precedes the fresh preview expectation.
  if p_input_kind='PAYE_MANUAL' and (p_preview_capacity_revision is null or p_preview_capacity_revision<0
    or p_preview_capacity_revision is distinct from (select c.net_capacity_revision from private.bpay_next_worker_control c where c.candidate_id=v_candidate)) then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_PREVIEW_CHANGED';end if;
  select s.* into strict v_draft from private.bpay_next_case_allocation_state s
    where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
      and s.selection_revision=v_worker.case_selection_revision and s.pass_kind='DRAFT' and s.projection_no=0;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
     or v_worker.gross_ex_vat<>v_worker.gross_inc_vat or v_worker.gross_inc_vat<0
     or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or v_worker.case_pending_binding_count<>0
     or v_worker.net_projection_revision<>p_expected_projection_revision
     or v_worker.net_request_revision<>v_worker.net_projection_revision
     or v_worker.net_replacement_pending_id is not null
     or v_worker.realised_effect_count<>0
     or v_draft.captured_work_gross+v_draft.allocated_gross_additions-v_draft.allocated_gross_deductions<>v_worker.gross_inc_vat
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_NOT_ELIGIBLE';
  end if;
  if p_input_kind='PAYE_MANUAL' then
    if p_entered_paye_net>v_worker.gross_inc_vat or exists(select 1 from private.bpay_next_case_payout_basis
      where run_worker_id=v_worker.id and preparation_revision=v_worker.captured_preparation_revision) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_PAYROLL_BASIS_INVALID';
    end if;
  else
    if p_expected_projection_revision<>0 or v_worker.captured_line_count<>0 or v_worker.gross_inc_vat<>0
       or not exists(select 1 from private.bpay_next_case_payout_basis b where b.run_worker_id=v_worker.id
         and b.preparation_revision=v_worker.captured_preparation_revision and b.status='SEALED'
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
  -- High-water is distinct from accepted projection and retains refused gaps.
  select r.request_no into v_latest_no from private.bpay_next_paye_net_request r
    where r.run_worker_id=v_worker.id order by r.request_no desc limit 1;
  v_request_no:=greatest(v_worker.net_request_high_water,coalesce(v_latest_no,0),v_worker.net_projection_revision);
  if v_request_no=9223372036854775807 then raise exception using errcode='22003',message='BPAY_NEXT_NET_REQUEST_NUMBER_EXHAUSTED';end if;
  v_request_no:=v_request_no+1;
  if p_expected_projection_revision>0 then
    select p.id,s.id into strict v_prior_projection,v_prior_state from private.bpay_next_net_projection p
      join private.bpay_next_case_allocation_state s on s.projection_id=p.id and s.status='COMPLETE' and s.net_stage='COMPLETE'
      where p.run_worker_id=v_worker.id and p.projection_no=p_expected_projection_revision and p.retired_at_utc is null
       and s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
       and s.selection_revision=v_worker.case_selection_revision and s.pass_kind='NET';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request(command_id,run_worker_id,candidate_id,request_no,
    entered_paye_net,frozen_gross_inc_vat,input_kind,actor_user_id,case_draft_state_id,
    preparation_revision,selection_revision,expected_projection_revision,prior_projection_id,prior_entered_paye_net,preview_capacity_revision)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_request_no,
      p_entered_paye_net,v_worker.gross_inc_vat,p_input_kind,p_actor_user_id,v_draft.id,
      v_worker.captured_preparation_revision,v_worker.case_selection_revision,p_expected_projection_revision,
      v_prior_projection,(select p.entered_paye_net from private.bpay_next_net_projection p where p.id=v_prior_projection),p_preview_capacity_revision);
  insert into private.bpay_next_net_replacement(id,run_worker_id,run_id,candidate_id,pay_week_start,request_no,
    prior_projection_no,prior_projection_id,draft_state_id,prior_state_id,module_epoch,preparation_revision,selection_revision,
    expected_instruction_count,initial_channel_headroom,remaining_channel_headroom,initial_take_home,remaining_take_home)
    values(p_command_id,v_worker.id,v_worker.run_id,v_worker.candidate_id,v_draft.pay_week_start,v_request_no,
      p_expected_projection_revision,v_prior_projection,v_draft.id,v_prior_state,v_epoch,v_worker.captured_preparation_revision,
      v_worker.case_selection_revision,v_draft.expected_instruction_count,coalesce(p_entered_paye_net,0),
      coalesce(p_entered_paye_net,0),coalesce(p_entered_paye_net,0),coalesce(p_entered_paye_net,0));
  update private.bpay_next_run_worker set net_request_revision=v_request_no,net_request_high_water=v_request_no,
    net_replacement_pending_id=p_command_id,
    review_issue_code=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_code end,
    review_issue_work_id=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_work_id end
    where id=v_worker.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_request_no::text,
    'replay',false,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id,
    'replacement_id',p_command_id,'replacement_stage','PRECHECK','projection_id',null,'issue_code',null);
end
$function$;

create or replace function private.bpay_next_validate_case_net_page_v1(p_replacement_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_s private.bpay_next_case_allocation_state%rowtype;
 v_d private.bpay_next_case_allocation_state%rowtype;v_q private.bpay_next_paye_net_request%rowtype;
 v_a private.bpay_next_net_staged_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
 v_c private.bpay_next_finance_case%rowtype;v_cc private.bpay_next_case_component%rowtype;v_p private.bpay_next_case_period%rowtype;
 v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_week private.bpay_next_worker_week%rowtype;
 v_contribution private.bpay_next_worker_week_contribution%rowtype;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;v_reply jsonb;
begin
 if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_NET_VALIDATION_INPUT_INVALID';end if;
 select dr37_point_24.* into strict v_rep from private.bpay_next_net_replacement dr37_point_24 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 select dr37_point_25.* into strict v_q from private.bpay_next_paye_net_request dr37_point_25 where command_id=v_rep.id;
 select dr37_point_26.* into strict v_s from private.bpay_next_case_allocation_state dr37_point_26 where id=v_rep.target_state_id for update;
 select dr37_point_27.* into strict v_d from private.bpay_next_case_allocation_state dr37_point_27 where id=v_rep.draft_state_id;
 if v_rep.stage<>'STAGING' or v_rep.staging_step<>'VALIDATE' or not exists(select 1 from private.bpay_next_candidate_week_commit_fence f
  where f.candidate_id=v_rep.candidate_id and f.pay_week_start=v_rep.pay_week_start and f.replacement_id=v_rep.id
   and f.command_id=v_rep.id and f.original_job_id=v_rep.original_job_id and f.allocation_state_id=v_rep.target_state_id and f.stage='STAGING') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_FENCE_INVALID';end if;
 select dr37_point_28.* into strict v_week from private.bpay_next_worker_week dr37_point_28 where candidate_id=v_rep.candidate_id and pay_week_start=v_rep.pay_week_start for update nowait;
 select dr37_point_29.* into v_contribution from private.bpay_next_worker_week_contribution dr37_point_29 where original_run_worker_id=v_rep.run_worker_id for update nowait;
 if v_s.net_requires_week_binding and v_week.period_revision<>v_s.captured_week_revision then v_issue:='CASE_WEEK_BASIS_UNBOUND';
 elsif v_contribution.id is null or v_contribution.eligibility_state='UNBOUND' or v_contribution.payment_state<>'ARRANGED'
  or v_contribution.original_gross_amount<>v_q.frozen_gross_inc_vat then v_issue:='CASE_WEEK_BASIS_UNBOUND';
 elsif (v_rep.gross_additions,v_rep.gross_deductions,v_rep.net_additions)
  is distinct from (v_d.allocated_gross_additions,v_d.allocated_gross_deductions,v_d.allocated_net_additions)
  or coalesce(v_q.entered_paye_net,0)+v_rep.net_additions-v_rep.recoveries<0
  or coalesce(v_q.entered_paye_net,0)+v_rep.net_additions-v_rep.recoveries>=10000000000000000 then v_issue:='CASE_INPUT_UNBOUND';end if;
 -- This check is STAGING-only under the original Candidate commit fence.
 -- No offer check is repeated after the first destructive COMMITTING page.
 if v_q.preview_capacity_revision is not null and v_q.preview_capacity_revision is distinct from
   (select c.net_capacity_revision from private.bpay_next_worker_control c where c.candidate_id=v_rep.candidate_id) then
  v_issue:='CASE_INPUT_UNBOUND';end if;
 if v_issue is null and v_rep.validation_step='INSTRUCTIONS' then
  perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'VALIDATE');
  for v_a in select a.* from private.bpay_next_net_staged_result a where a.replacement_id=v_rep.id and a.result_no>v_rep.validation_cursor
   order by a.result_no limit p_limit
  loop
   select dr37_point_30.* into v_h from private.bpay_next_case_hold dr37_point_30 where instruction_id=v_a.instruction_id and status='ACTIVE' for update nowait;
   select dr37_point_31.* into strict v_c from private.bpay_next_finance_case dr37_point_31 where id=v_a.case_id for update nowait;
   select dr37_point_32.* into strict v_cc from private.bpay_next_case_component dr37_point_32 where id=v_a.case_component_id for update nowait;
   select dr37_point_33.* into strict v_p from private.bpay_next_case_period dr37_point_33 where case_component_id=v_a.case_component_id and pay_week_start=v_a.pay_week_start for update nowait;
   v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_a),pg_catalog.to_jsonb(v_h),
    v_c.case_revision,v_c.active_recovery_hold_amount,v_c.principal_funded,v_c.principal_approved,v_c.principal_recovered,v_c.principal_written_off,
    v_cc.component_revision,v_p.period_revision)::text);
   if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
   if v_bytes+v_size>65536 then exit;end if;
   v_seen:=v_seen+1;v_bytes:=v_bytes+v_size;
   if (v_h.id,coalesce(v_h.source_reserved_ex_vat,0)) is distinct from (v_a.prior_hold_id,v_a.prior_source_amount)
    or (v_c.case_revision,v_cc.component_revision,v_p.period_revision)
     is distinct from (v_a.captured_case_revision,v_a.captured_component_revision,v_a.captured_period_revision)
    or v_h.id is not null and not exists(select 1 from private.bpay_next_case_capacity_use u
     where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_a.prior_source_amount) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   if v_a.payroll_stage='NET_DEDUCT' then
    select dr37_point_34.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_34 where replacement_id=v_rep.id and case_id=v_a.case_id;
    if v_carry.captured_case_revision<>v_c.case_revision or v_carry.captured_active_recovery_source<>v_c.active_recovery_hold_amount
     or v_carry.captured_outstanding_source<>(case when v_c.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)
      -v_c.principal_recovered-v_c.principal_written_off then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   end if;
   if v_a.result_no<>v_rep.validated_instruction_count+1 then raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_RESULT_SEQUENCE_INVALID';end if;
   v_rep.validated_instruction_count:=v_rep.validated_instruction_count+1;v_rep.validation_cursor:=v_a.result_no;
  end loop;
  select exists(select 1 from private.bpay_next_net_staged_result a where a.replacement_id=v_rep.id and a.result_no>v_rep.validation_cursor) into v_more;
  if not v_more and v_issue is null and v_rep.validated_instruction_count<>v_rep.expected_instruction_count then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_COUNT_MISMATCH';end if;
  update private.bpay_next_net_replacement set validated_instruction_count=v_rep.validated_instruction_count,validation_cursor=v_rep.validation_cursor,
   validation_step=case when not v_more and v_issue is null then case when v_s.net_requires_week_binding then 'PRIOR_RUNS' else 'DONE' end else validation_step end,
   updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 elsif v_issue is null and v_rep.validation_step='PRIOR_RUNS' then
  v_reply:=private.bpay_next_net_prior_run_page_v1(v_rep.id,p_limit,true);
  v_seen:=(v_reply->>'rows_visited')::integer;v_bytes:=(v_reply->>'encoded_bytes')::integer;v_issue:=v_reply->>'issue_code';
 end if;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'issue_code',v_issue);
end $function$;

create or replace function private.bpay_next_net_capacity_revision_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_candidate uuid;v_changed boolean:=true;
begin
 if tg_table_schema<>'private' or tg_table_name not in ('bpay_next_finance_case','bpay_next_case_component','bpay_next_case_period',
  'bpay_next_case_hold','bpay_next_case_capacity_use','bpay_next_worker_week_contribution') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_REVISION_SCOPE_INVALID';end if;
 if tg_op='UPDATE' then
  if new.candidate_id is distinct from old.candidate_id then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_REVISION_SCOPE_INVALID';end if;
  case tg_table_name
   when 'bpay_next_finance_case' then
    v_changed:=(new.principal_funded,new.principal_approved,new.principal_recovered,new.principal_written_off,new.active_recovery_hold_amount,new.active_payout_hold_amount)
     is distinct from (old.principal_funded,old.principal_approved,old.principal_recovered,old.principal_written_off,old.active_recovery_hold_amount,old.active_payout_hold_amount);
   when 'bpay_next_case_component' then
    v_changed:=(new.funded_source_ex_vat,new.approved_source_ex_vat,new.recovered_source_ex_vat,new.written_off_source_ex_vat,new.active_recovery_source_ex_vat,new.active_payout_source_ex_vat)
     is distinct from (old.funded_source_ex_vat,old.approved_source_ex_vat,old.recovered_source_ex_vat,old.written_off_source_ex_vat,old.active_recovery_source_ex_vat,old.active_payout_source_ex_vat);
   when 'bpay_next_case_period' then
    v_changed:=(new.opening_due_source_ex_vat,new.realised_recovery_source_ex_vat,new.active_unrealised_recovery_source_ex_vat,new.active_payout_source_ex_vat)
     is distinct from (old.opening_due_source_ex_vat,old.realised_recovery_source_ex_vat,old.active_unrealised_recovery_source_ex_vat,old.active_payout_source_ex_vat);
   when 'bpay_next_case_hold' then
    v_changed:=(new.status,new.source_reserved_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.run_worker_id,new.instruction_id,new.allocation_result_id)
     is distinct from (old.status,old.source_reserved_ex_vat,old.target_amount_ex_vat,old.target_amount_vat,old.run_worker_id,old.instruction_id,old.allocation_result_id);
   when 'bpay_next_case_capacity_use' then
    v_changed:=(new.status,new.source_amount_ex_vat,new.case_hold_id)
     is distinct from (old.status,old.source_amount_ex_vat,old.case_hold_id);
   when 'bpay_next_worker_week_contribution' then
    v_changed:=(new.eligibility_state,new.eligible_arranged_amount,new.payment_state,new.basis_kind,
     new.original_gross_amount,new.original_payroll_net,new.accepted_projection_id,new.accepted_bank_cash,
     new.original_pay_date,new.original_created_at_utc,new.original_run_worker_id,new.pay_week_start,new.primary_binding_revision)
     is distinct from (old.eligibility_state,old.eligible_arranged_amount,old.payment_state,old.basis_kind,
     old.original_gross_amount,old.original_payroll_net,old.accepted_projection_id,old.accepted_bank_cash,
     old.original_pay_date,old.original_created_at_utc,old.original_run_worker_id,old.pay_week_start,old.primary_binding_revision);
  end case;
 end if;
 if v_changed then
  v_candidate:=case when tg_op='DELETE' then old.candidate_id else new.candidate_id end;
  -- Native financial owners already hold Candidate control before Case rows.
  -- No public table, money value, financial_view_revision or owner lease changes.
  update private.bpay_next_worker_control set net_capacity_revision=net_capacity_revision+1 where candidate_id=v_candidate;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_CONTROL_MISSING';end if;
 end if;
 if tg_op='DELETE' then return old;else return new;end if;
end
$function$;

alter function private.bpay_next_paye_net_entry_in_progress_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_paye_net_entry_in_progress_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric,uuid) owner to postgres;
revoke all on function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric) owner to postgres;
revoke all on function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) owner to postgres;
revoke all on function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_simple_transfer_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_begin_simple_transfer_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_case_transfer_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_begin_case_transfer_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_paye_net_capacity_v1(numeric,numeric,numeric,numeric,numeric,numeric,numeric,numeric) owner to postgres;
revoke all on function private.bpay_next_paye_net_capacity_v1(numeric,numeric,numeric,numeric,numeric,numeric,numeric,numeric) from public,anon,authenticated,service_role;
alter function private.bpay_next_paye_net_cash_v1(numeric,numeric,numeric) owner to postgres;
revoke all on function private.bpay_next_paye_net_cash_v1(numeric,numeric,numeric) from public,anon,authenticated,service_role;
alter function private.bpay_next_paye_net_preview_v1(uuid,uuid,numeric,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_paye_net_preview_v1(uuid,uuid,numeric,jsonb,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_stage_case_net_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_stage_case_net_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_validate_case_net_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_validate_case_net_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_capacity_revision_v1() owner to postgres;
revoke all on function private.bpay_next_net_capacity_revision_v1() from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
