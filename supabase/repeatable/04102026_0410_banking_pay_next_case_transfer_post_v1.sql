-- Original case transfer/member/post owners. The shared 0120/0130/0200
-- owners call these exact seams; no provider, old calculator or history scan.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_begin_case_transfer_v1(p_command_id uuid,p_run_worker_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_net private.bpay_next_case_allocation_state%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;v_cash numeric;
  v_sequence bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_TRANSFER_BEGIN_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_command_id::text,0));
  select run_id into strict v_run_id from private.bpay_next_run_worker where id=p_run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c join private.bpay_next_run_worker w on w.candidate_id=c.candidate_id
    where w.id=p_run_worker_id for update of c;
  select * into strict v_worker from private.bpay_next_run_worker where id=p_run_worker_id for update;
  select * into v_transfer from private.bpay_next_transfer where build_command_id=p_command_id;
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
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.case_selection_revision<1
     or v_worker.case_pending_binding_count<>0 or v_worker.realised_effect_count<>0
     or v_worker.net_projection_revision<1 or v_worker.net_request_revision<>v_worker.net_projection_revision
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_NOT_ELIGIBLE';end if;
  select * into strict v_projection from private.bpay_next_net_projection
    where run_worker_id=v_worker.id and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
  select * into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.preparation_revision and selection_revision=v_worker.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE';
  select * into strict v_net from private.bpay_next_case_allocation_state where projection_id=v_projection.id
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
  select * into v_destination from private.bpay_next_net_destination_state where projection_id=v_projection.id;
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
        v_worker.preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,
        v_draft.expected_instruction_count,v_destination.external_leg_count+1);
    insert into private.bpay_next_destination_group_leg(transfer_id,anchor_transfer_id,leg_kind) values(v_transfer.id,v_transfer.id,'OWN');
  else
    insert into private.bpay_next_case_transfer_build(transfer_id,run_worker_id,candidate_id,projection_id,draft_state_id,
      net_state_id,preparation_revision,selection_revision,stage,expected_work_count,expected_case_count)
      values(v_transfer.id,v_worker.id,v_worker.candidate_id,v_projection.id,v_draft.id,v_net.id,
        v_worker.preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,v_draft.expected_instruction_count);
  end if;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

create or replace function private.bpay_next_case_transfer_build_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_transfer private.bpay_next_transfer%rowtype;v_draft private.bpay_next_case_allocation_state%rowtype;
  v_net private.bpay_next_case_allocation_state%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_BINDING_DELETE_FORBIDDEN';end if;
  if tg_op='INSERT' then
    select * into strict v_transfer from private.bpay_next_transfer where id=new.transfer_id;
    select * into strict v_draft from private.bpay_next_case_allocation_state where id=new.draft_state_id;
    select * into strict v_net from private.bpay_next_case_allocation_state where id=new.net_state_id;
    if v_transfer.status<>'BUILDING' or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
       or v_transfer.candidate_id<>new.candidate_id or v_transfer.projection_id<>new.projection_id
       or v_draft.pass_kind<>'DRAFT' or v_draft.status<>'COMPLETE' or v_net.pass_kind<>'NET'
       or v_net.status<>'COMPLETE' or v_net.net_stage<>'COMPLETE' or v_net.projection_id<>new.projection_id
       or v_draft.expected_instruction_count<>new.expected_case_count or v_net.expected_instruction_count<>new.expected_case_count
       or new.stage<>'WORK' or new.checkpoint<>0 or new.processed_work_count<>0 or new.processed_case_count<>0
       or new.work_cursor is not null or new.cursor_age_key is not null
       or new.work_cash_total<>0 or new.gross_additions_total<>0 or new.gross_deductions_total<>0
       or new.net_additions_total<>0 or new.net_recoveries_total<>0 or new.completed_at_utc is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_BINDING_INITIAL_INVALID';end if;
    return new;
  end if;
  if old.stage='COMPLETE'
     or (new.transfer_id,new.run_worker_id,new.candidate_id,new.projection_id,new.draft_state_id,new.net_state_id,
       new.preparation_revision,new.selection_revision,new.expected_work_count,new.expected_case_count,new.created_at_utc)
       is distinct from (old.transfer_id,old.run_worker_id,old.candidate_id,old.projection_id,old.draft_state_id,old.net_state_id,
         old.preparation_revision,old.selection_revision,old.expected_work_count,old.expected_case_count,old.created_at_utc)
     or new.checkpoint<>old.checkpoint+1 or new.processed_work_count<old.processed_work_count or new.processed_case_count<old.processed_case_count
     or new.work_cash_total<old.work_cash_total or new.gross_additions_total<old.gross_additions_total
     or new.gross_deductions_total<old.gross_deductions_total or new.net_additions_total<old.net_additions_total or new.net_recoveries_total<old.net_recoveries_total
     or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
     or (old.stage='CASE' and new.stage not in ('CASE','FINAL')) or (old.stage='FINAL' and new.stage<>'COMPLETE')
     or (new.stage='COMPLETE' and (new.completed_at_utc is null or not pg_catalog.isfinite(new.completed_at_utc)))
     or (new.stage<>'COMPLETE' and new.completed_at_utc is not null) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_BINDING_TRANSITION_INVALID';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_case_transfer_build_guard_v1 on private.bpay_next_case_transfer_build;
create trigger bpay_next_case_transfer_build_guard_v1 before insert or update or delete on private.bpay_next_case_transfer_build
  for each row execute function private.bpay_next_case_transfer_build_guard_v1();

create or replace function private.bpay_next_case_transfer_member_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_build private.bpay_next_case_transfer_build%rowtype;v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_result private.bpay_next_case_allocation_result%rowtype;v_hold private.bpay_next_case_hold%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_line private.bpay_next_run_line%rowtype;v_signed numeric;
begin
  select * into v_build from private.bpay_next_case_transfer_build where transfer_id=new.transfer_id for share;
  if not found then
    v_build:=private.bpay_next_destination_build_binding_v1(new.transfer_id);
    if v_build.transfer_id is not null then
      perform private.bpay_next_record_destination_member_v1(new);
    else
      if new.subject_kind='CASE' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_UNBOUND';end if;
      return new;
    end if;
  end if;
  if new.run_worker_id<>v_build.run_worker_id or v_build.stage='COMPLETE' or new.subject_kind='CASH_REISSUE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_SCOPE_INVALID';end if;
  if new.subject_kind='WORK' then
    select * into strict v_line from private.bpay_next_run_line where id=new.run_line_id;
    if v_build.stage<>'WORK' or v_line.run_worker_id<>v_build.run_worker_id or v_line.frozen_inc_vat<>new.signed_cash_contribution then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_WORK_MISMATCH';end if;
  elsif new.subject_kind='NET_ADJUSTMENT' then
    select * into strict v_projection from private.bpay_next_net_projection where id=v_build.projection_id;
    if v_build.stage<>'FINAL' or new.signed_cash_contribution is distinct from
      coalesce(v_projection.entered_paye_net,0)-v_projection.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_PAYROLL_ADJUSTMENT_MISMATCH';end if;
  elsif new.subject_kind='CASE' then
    select * into strict v_instruction from private.bpay_next_run_case_instruction where id=new.case_instruction_id;
    select * into strict v_result from private.bpay_next_case_allocation_result where id=new.case_allocation_result_id;
    v_signed:=case when v_instruction.direction='DEDUCTION' then -v_result.allocated_target_inc_vat else v_result.allocated_target_inc_vat end;
    if v_build.stage<>'CASE' or v_instruction.run_worker_id<>v_build.run_worker_id
       or v_instruction.preparation_revision<>v_build.preparation_revision or v_instruction.selection_revision<>v_build.selection_revision
       or v_result.instruction_id<>v_instruction.id or new.signed_cash_contribution<>v_signed
       or v_result.allocated_source_ex_vat<>v_result.allocated_target_ex_vat or v_result.allocated_target_vat<>0
       or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT'
         then v_build.net_state_id else v_build.draft_state_id end) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_FROZEN_RESULT_MISMATCH';end if;
    if v_result.allocated_source_ex_vat=0 then
      if new.case_hold_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_ZERO_HOLD_FORBIDDEN';end if;
    else
      select * into strict v_hold from private.bpay_next_case_hold where id=new.case_hold_id;
      if v_hold.status<>'ACTIVE' or v_hold.run_worker_id<>v_build.run_worker_id or v_hold.instruction_id<>v_instruction.id
         or v_hold.allocation_result_id<>v_result.id
         or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
           is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_MEMBER_HOLD_MISMATCH';end if;
    end if;
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_case_transfer_member_guard_v1 on private.bpay_next_transfer_member;
create trigger bpay_next_case_transfer_member_guard_v1 before insert on private.bpay_next_transfer_member
  for each row execute function private.bpay_next_case_transfer_member_guard_v1();

create or replace function private.bpay_next_build_case_transfer_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_checkpoint bigint;v_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;v_job private.bpay_next_job%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_build private.bpay_next_case_transfer_build%rowtype;v_projection private.bpay_next_net_projection%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_line private.bpay_next_run_line%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_seen integer:=0;v_more boolean;v_signed numeric;v_count bigint;v_sum numeric;v_adjustment numeric;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_TRANSFER_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_case_transfer_build b on b.transfer_id=t.id
    join private.bpay_next_run_worker w on w.id=b.run_worker_id where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id where t.build_command_id=v_job.command_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer where build_command_id=v_job.command_id for update;
  select * into strict v_build from private.bpay_next_case_transfer_build where transfer_id=v_transfer.id for update;
  select * into strict v_projection from private.bpay_next_net_projection where id=v_build.projection_id;
  select * into strict v_draft from private.bpay_next_case_allocation_state where id=v_build.draft_state_id;
  if v_job.job_kind<>'TRANSFER_BUILD' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_build.candidate_id
     or v_transfer.run_worker_id<>v_build.run_worker_id or v_transfer.projection_id<>v_build.projection_id
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_transfer.beneficiary_kind<>'CANDIDATE' or v_transfer.beneficiary_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY' and v_build.stage='COMPLETE' and v_transfer.status<>'BUILDING' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY','transfer_status',v_transfer.status,'transfer_id',v_transfer.id,
      'cursor',v_job.cursor_key,'member_count',v_transfer.member_count::text,'rows_visited','0','replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.net_projection_revision<>v_projection.projection_no or v_worker.net_request_revision<>v_projection.projection_no
     or v_projection.retired_at_utc is not null or v_transfer.status<>'BUILDING' or v_transfer.account_approval_ref is not null
     or v_worker.preparation_revision<>v_build.preparation_revision or v_worker.case_selection_revision<>v_build.selection_revision
     or v_transfer.cash_amount<>v_projection.cash_amount
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_BUILD_NOT_ELIGIBLE';end if;
  v_checkpoint:=nullif(v_build.checkpoint,0);v_cursor:=nullif(v_job.cursor_key,'')::bigint;
  if v_cursor is distinct from v_checkpoint then raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CHECKPOINT_MISMATCH';end if;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)>coalesce(v_checkpoint,0) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase','BUILDING','transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count::text,'rows_visited','0','replay',true);
  end if;
  v_count:=v_transfer.member_count;v_sum:=v_transfer.member_cash_sum;
  if v_build.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_build.work_cursor is null or line_no>v_build.work_cursor) order by line_no,id limit p_limit loop
      if v_line.source_pay_channel<>'PAYE' or v_line.target_pay_channel<>'PAYE' or v_line.frozen_vat<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_WORK_BASIS_INVALID';end if;
      v_count:=v_count+1;v_sum:=v_sum+v_line.frozen_inc_vat;v_seen:=v_seen+1;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,run_line_id,signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_count,'WORK',v_line.id,v_line.frozen_inc_vat);
      v_build.processed_work_count:=v_build.processed_work_count+1;v_build.work_cash_total:=v_build.work_cash_total+v_line.frozen_inc_vat;
      v_build.work_cursor:=v_line.line_no;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id
      and (v_build.work_cursor is null or line_no>v_build.work_cursor)) into v_more;
    if not v_more then
      if v_build.processed_work_count<>v_build.expected_work_count or v_build.work_cash_total<>v_draft.captured_work_gross then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_WORK_TOTAL_INVALID';end if;
      v_build.stage:='CASE';
    end if;
  elsif v_build.stage='CASE' then
    for v_instruction in select * from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_build.preparation_revision and selection_revision=v_build.selection_revision
      and (v_build.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_build.cursor_age_key,v_build.cursor_case_id,v_build.cursor_component_ordinal,v_build.cursor_case_component_id))
      order by age_key,case_id,component_ordinal,case_component_id limit p_limit loop
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE' or v_instruction.nominal_target_vat<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASE_BASIS_INVALID';end if;
      select * into strict v_result from private.bpay_next_case_allocation_result where instruction_id=v_instruction.id
        and state_id=case when v_instruction.payroll_stage='NET_DEDUCT' then v_build.net_state_id else v_build.draft_state_id end;
      v_hold:=null;
      if v_result.allocated_source_ex_vat>0 then
        select * into strict v_hold from private.bpay_next_case_hold where allocation_result_id=v_result.id and status='ACTIVE' for share;
        if not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_hold.id and u.status='ACTIVE'
            and u.source_amount_ex_vat=v_result.allocated_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CAPACITY_MISMATCH';end if;
      end if;
      v_signed:=case when v_instruction.direction='DEDUCTION' then -v_result.allocated_target_inc_vat else v_result.allocated_target_inc_vat end;
      v_count:=v_count+1;v_sum:=v_sum+v_signed;v_seen:=v_seen+1;
      insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,case_hold_id,
        case_instruction_id,case_allocation_result_id,signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_count,'CASE',v_hold.id,v_instruction.id,v_result.id,v_signed);
      v_build.processed_case_count:=v_build.processed_case_count+1;
      if v_instruction.payroll_stage='GROSS_ADD' then v_build.gross_additions_total:=v_build.gross_additions_total+v_result.allocated_target_inc_vat;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_build.gross_deductions_total:=v_build.gross_deductions_total+v_result.allocated_target_inc_vat;
      elsif v_instruction.payroll_stage='NET_ADD' then v_build.net_additions_total:=v_build.net_additions_total+v_result.allocated_target_inc_vat;
      else v_build.net_recoveries_total:=v_build.net_recoveries_total+v_result.allocated_target_inc_vat;end if;
      v_build.cursor_age_key:=v_instruction.age_key;v_build.cursor_case_id:=v_instruction.case_id;
      v_build.cursor_component_ordinal:=v_instruction.component_ordinal;v_build.cursor_case_component_id:=v_instruction.case_component_id;
    end loop;
    select exists(select 1 from private.bpay_next_run_case_instruction where run_worker_id=v_worker.id
      and preparation_revision=v_build.preparation_revision and selection_revision=v_build.selection_revision
      and (v_build.cursor_case_component_id is null or (age_key,case_id,component_ordinal,case_component_id)
        >(v_build.cursor_age_key,v_build.cursor_case_id,v_build.cursor_component_ordinal,v_build.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_build.processed_case_count<>v_build.expected_case_count
         or (v_build.gross_additions_total,v_build.gross_deductions_total,v_build.net_additions_total,v_build.net_recoveries_total)
           is distinct from (v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,
             v_projection.accepted_net_additions,v_projection.accepted_recoveries) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASE_TOTAL_INVALID';end if;
      v_build.stage:='FINAL';
    end if;
  elsif v_build.stage='FINAL' then
    if v_build.work_cash_total+v_build.gross_additions_total-v_build.gross_deductions_total<>v_projection.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_PAYROLL_GROSS_MISMATCH';end if;
    v_adjustment:=coalesce(v_projection.entered_paye_net,0)-v_projection.gross_inc_vat;
    v_count:=v_count+1;v_sum:=v_sum+v_adjustment;v_seen:=1;
    insert into private.bpay_next_transfer_member(transfer_id,run_worker_id,member_no,subject_kind,signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_count,'NET_ADJUSTMENT',v_adjustment);
    if v_count<>v_build.expected_work_count+v_build.expected_case_count+1 or v_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_CASH_MISMATCH';end if;
    v_build.stage:='COMPLETE';v_build.completed_at_utc:=pg_catalog.transaction_timestamp();
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_STAGE_INVALID';end if;
  update private.bpay_next_case_transfer_build set stage=v_build.stage,checkpoint=checkpoint+1,
    processed_work_count=v_build.processed_work_count,processed_case_count=v_build.processed_case_count,work_cursor=v_build.work_cursor,
    cursor_age_key=v_build.cursor_age_key,cursor_case_id=v_build.cursor_case_id,cursor_component_ordinal=v_build.cursor_component_ordinal,
    cursor_case_component_id=v_build.cursor_case_component_id,work_cash_total=v_build.work_cash_total,
    gross_additions_total=v_build.gross_additions_total,gross_deductions_total=v_build.gross_deductions_total,
    net_additions_total=v_build.net_additions_total,net_recoveries_total=v_build.net_recoveries_total,completed_at_utc=v_build.completed_at_utc
    where transfer_id=v_transfer.id returning checkpoint into v_cursor;
  update private.bpay_next_transfer set member_count=v_count,member_cash_sum=v_sum,
    status=case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end where id=v_transfer.id;
  update private.bpay_next_job set cursor_key=v_cursor::text,
    status=case when v_build.stage='COMPLETE' then 'DONE' else 'LEASED' end,
    phase=case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'WORK' end,
    lease_nonce=case when v_build.stage='COMPLETE' then null else lease_nonce end,
    lease_until_utc=case when v_build.stage='COMPLETE' then null else lease_until_utc end where id=p_job_id;
  if v_build.stage='COMPLETE' then update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;end if;
  return pg_catalog.jsonb_build_object('phase',case when v_build.stage='COMPLETE' then 'MEMBERS_READY' else 'BUILDING' end,
    'transfer_id',v_transfer.id,'cursor',v_cursor::text,'member_count',v_count::text,'rows_visited',v_seen::text,'replay',false);
end
$function$;

-- Called only for the actual immutable CASE member being posted by 0200.
-- Manual principal is not a WORK effect. A captured typed automatic origin
-- links genuine RECOVERED SOURCE back to its immutable original WORK only.
create or replace function private.bpay_next_post_case_transfer_member_v1(p_job_id uuid,p_transfer_id uuid,p_member_no bigint)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_job private.bpay_next_job%rowtype;v_request private.bpay_next_outcome_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_outcome private.bpay_next_transfer_outcome%rowtype;
  v_internal private.bpay_next_internal_receipt%rowtype;v_occurred_at timestamptz;
  v_member private.bpay_next_transfer_member%rowtype;v_build private.bpay_next_case_transfer_build%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_pair private.bpay_next_case_component%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_period_rule private.bpay_next_case_rule%rowtype;
  v_pair_period private.bpay_next_case_period%rowtype;v_origin private.bpay_next_case_create_request%rowtype;
  v_event_id uuid;v_new_rule_id uuid;v_kind text;v_amount numeric;v_signed numeric;
begin
  if p_job_id is null or p_transfer_id is null or p_member_no is null or p_member_no<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_POST_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  select * into strict v_request from private.bpay_next_outcome_request where command_id=v_job.command_id and transfer_id=p_transfer_id;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id;
  select * into v_build from private.bpay_next_case_transfer_build where transfer_id=p_transfer_id;
  if not found then
    v_build:=private.bpay_next_destination_build_binding_v1(p_transfer_id);
    if v_build.transfer_id is null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_TRANSFER_UNBOUND';end if;
  end if;
  select * into strict v_member from private.bpay_next_transfer_member where transfer_id=p_transfer_id and member_no=p_member_no;
  if v_job.job_kind='INTERNAL_SETTLEMENT' then
    select * into strict v_internal from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
    if v_request.outcome_id is not null or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0
       or v_transfer.status<>'INTERNAL_PROCESSING' or v_internal.command_id<>v_job.command_id
       or v_internal.transfer_id<>v_transfer.id or v_internal.run_worker_id<>v_build.run_worker_id
       or v_internal.candidate_id<>v_build.candidate_id or v_internal.projection_id<>v_build.projection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_INTERNAL_POST_SCOPE_INVALID';end if;
    v_occurred_at:=v_internal.occurred_at_utc;
  elsif v_job.job_kind='CSV_SETTLEMENT' then
    select * into strict v_outcome from private.bpay_next_transfer_outcome where id=v_request.outcome_id;
    if v_request.internal_receipt_id is not null or v_transfer.execution_kind<>'BANK'
       or v_transfer.status not in ('SETTLED','RETURNED') or v_outcome.transfer_id<>v_transfer.id
       or v_outcome.outcome_kind<>'SETTLED' or v_outcome.whole_transfer_amount<>v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_BANK_POST_SCOPE_INVALID';end if;
    v_occurred_at:=v_outcome.occurred_at_utc;
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_JOB_KIND_INVALID';end if;
  if v_job.module_epoch<>v_epoch or v_job.status<>'LEASED' or v_job.phase<>'MEMBERS'
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_job.candidate_id<>v_build.candidate_id
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_build.stage<>'COMPLETE' or v_member.subject_kind<>'CASE' or v_member.run_worker_id<>v_build.run_worker_id
     or v_occurred_at is null or not pg_catalog.isfinite(v_occurred_at)
     or not exists(select 1 from private.bpay_next_worker_control where candidate_id=v_job.candidate_id and active_owner_epoch=v_job.owner_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_SCOPE_INVALID';end if;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_build.run_worker_id for update;
  select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_member.case_instruction_id;
  select * into strict v_result from private.bpay_next_case_allocation_result where id=v_member.case_allocation_result_id;
  v_signed:=case when v_instruction.direction='DEDUCTION' then -v_result.allocated_target_inc_vat else v_result.allocated_target_inc_vat end;
  if v_instruction.run_worker_id<>v_worker.id or v_instruction.candidate_id<>v_worker.candidate_id
     or v_result.instruction_id<>v_instruction.id or v_member.signed_cash_contribution<>v_signed
     or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT' then v_build.net_state_id else v_build.draft_state_id end)
     or v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE' or v_result.allocated_target_vat<>0
     or v_result.allocated_source_ex_vat<>v_result.allocated_target_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_FROZEN_RESULT_MISMATCH';end if;
  select * into strict v_case from private.bpay_next_finance_case where id=v_instruction.case_id for update;
  select * into strict v_component from private.bpay_next_case_component where id=v_instruction.case_component_id for update;
  perform 1 from private.bpay_next_case_period where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start for update;
  v_amount:=v_result.allocated_source_ex_vat;
  if v_amount>0 then
    select * into strict v_hold from private.bpay_next_case_hold where id=v_member.case_hold_id for update;
    select * into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_hold.id for update;
    if v_hold.status<>'ACTIVE' or v_use.status<>'ACTIVE' or v_hold.allocation_result_id<>v_result.id
       or v_hold.instruction_id<>v_instruction.id or v_hold.run_worker_id<>v_worker.id
       or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
         is distinct from (v_amount,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat)
       or v_use.source_amount_ex_vat<>v_amount or v_worker.active_case_hold_count<1 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_HOLD_MISMATCH';end if;
    v_kind:=case when v_instruction.instruction_kind='PAYOUT' then 'FUNDED'
      when v_instruction.instruction_kind='CREDIT' then 'PAID' else 'RECOVERED' end;
    v_event_id:=pg_catalog.gen_random_uuid();
    insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,original_transfer_id,occurred_at_utc,
      funded_delta,recovered_delta,paid_credit_delta,instruction_id,allocation_result_id,run_worker_id,candidate_id,case_component_id,
      event_case_kind,hold_purpose,pay_week_start,source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,shortfall_source_ex_vat)
      values(v_event_id,v_case.id,v_job.command_id,v_hold.id,v_kind,v_transfer.id,v_occurred_at,
        case when v_kind='FUNDED' then v_amount else 0 end,case when v_kind='RECOVERED' then v_amount else 0 end,
        case when v_kind='PAID' then v_amount else 0 end,v_instruction.id,v_result.id,v_worker.id,v_worker.candidate_id,v_component.id,
        v_instruction.case_kind,v_instruction.hold_purpose,v_instruction.pay_week_start,v_amount,v_result.allocated_target_ex_vat,
        v_result.allocated_target_vat,v_result.allocated_target_inc_vat,0);
    if v_kind='FUNDED' then
      select * into strict v_origin from private.bpay_next_case_create_request where case_id=v_case.id
        and primary_component_id=v_component.id and recovery_component_id is not null;
      select * into strict v_pair from private.bpay_next_case_component where id=v_origin.recovery_component_id for update;
      select * into strict v_rule from private.bpay_next_case_rule where id=v_case.current_rule_id;
      if v_pair.case_id<>v_case.id or v_pair.instruction_kind<>'RECOVERY' or v_pair.case_kind<>v_instruction.case_kind
         or v_occurred_at<v_rule.case_created_at_utc then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_FUNDING_ORIGIN_INVALID';end if;
      v_new_rule_id:=v_rule.id;
      if v_rule.original_funding_event_id is null then
        v_new_rule_id:=pg_catalog.gen_random_uuid();
        insert into private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,
          case_created_at_utc,original_funded_at_utc,original_funding_event_id,minimum_earnings_threshold,take_home_floor_override,
          weekly_due_source_ex_vat,schedule_start_monday,next_due_monday,schedule_week_count)
          values(v_new_rule_id,v_case.id,v_worker.candidate_id,v_rule.rule_revision+1,v_rule.case_kind,v_rule.case_subtype,v_rule.tax_treatment,
            v_rule.case_created_at_utc,v_occurred_at,v_event_id,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
            v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count);
      end if;
      update private.bpay_next_case_component set funded_source_ex_vat=funded_source_ex_vat+v_amount,
        active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,rule_id=v_new_rule_id,
        component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_component.id and active_payout_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_FUNDING_COMPONENT_CAPACITY_INVALID';end if;
      -- Mirrored payout/recovery components describe ONE funded principal.
      update private.bpay_next_case_component set funded_source_ex_vat=funded_source_ex_vat+v_amount,
        rule_id=v_new_rule_id,component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_pair.id;
      -- Exact creation-owned zero period only; never rewrite consumed opening
      -- facts or scan previously captured pay weeks to manufacture due.
      select * into strict v_pair_period from private.bpay_next_case_period where case_component_id=v_pair.id
        and pay_week_start=v_origin.input_start_monday for update;
      select * into strict v_period_rule from private.bpay_next_case_rule where id=v_pair_period.rule_id;
      if v_period_rule.original_funding_event_id is null
         and (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
           v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
           v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
           v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
           is not distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
             v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
             v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
         and (v_pair_period.opening_outstanding_source_ex_vat,v_pair_period.opening_due_source_ex_vat,
           v_pair_period.realised_recovery_source_ex_vat,v_pair_period.active_unrealised_recovery_source_ex_vat,v_pair_period.active_payout_source_ex_vat)
           is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
        update private.bpay_next_case_period set rule_id=v_new_rule_id,
          opening_outstanding_source_ex_vat=v_pair.funded_source_ex_vat+v_amount,
          opening_due_source_ex_vat=least(v_rule.weekly_due_source_ex_vat,v_pair.funded_source_ex_vat+v_amount),period_revision=period_revision+1
          where case_component_id=v_pair.id and pay_week_start=v_origin.input_start_monday;
      end if;
      update private.bpay_next_finance_case set principal_funded=principal_funded+v_amount,
        active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        current_rule_id=v_new_rule_id,current_rule_revision=case when v_new_rule_id=v_rule.id then v_rule.rule_revision else v_rule.rule_revision+1 end,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
    elsif v_kind='PAID' then
      update private.bpay_next_case_component set paid_credit_source_ex_vat=paid_credit_source_ex_vat+v_amount,
        active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,component_revision=component_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id and active_payout_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREDIT_COMPONENT_CAPACITY_INVALID';end if;
      update private.bpay_next_finance_case set principal_paid_credit=principal_paid_credit+v_amount,
        active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
    else
      update private.bpay_next_case_component set recovered_source_ex_vat=recovered_source_ex_vat+v_amount,
        active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,component_revision=component_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id and active_recovery_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_RECOVERY_COMPONENT_CAPACITY_INVALID';end if;
      update private.bpay_next_finance_case set principal_recovered=principal_recovered+v_amount,
        active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_PRINCIPAL_CAPACITY_INVALID';end if;
    if v_kind in ('FUNDED','PAID') then
      update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
        period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start
          and active_payout_source_ex_vat>=v_amount;
    else
      update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,
        realised_recovery_source_ex_vat=realised_recovery_source_ex_vat+v_amount,period_revision=period_revision+1
        where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_PERIOD_CAPACITY_INVALID';end if;
    update private.bpay_next_case_hold set status='REALISED',finished_at_utc=pg_catalog.transaction_timestamp(),
      projection_id=case when purpose='NET_RECOVERY_CAPACITY' then v_build.projection_id else projection_id end where id=v_hold.id;
    update private.bpay_next_case_capacity_use set status='REALISED',realisation_event_id=v_event_id where case_hold_id=v_hold.id;
    update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1,realised_effect_count=realised_effect_count+1 where id=v_worker.id;
    if v_kind='RECOVERED' and v_instruction.work_collection_id is not null then
      perform private.bpay_next_apply_work_collection_recovery_v1(p_job_id,v_event_id);
    end if;
  elsif v_member.case_hold_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_ZERO_HOLD_FORBIDDEN';
  end if;
  if v_instruction.instruction_kind='RECOVERY' and v_result.shortfall_target_ex_vat>0 then
    insert into private.bpay_next_case_event(case_id,operation_id,operation_item_id,event_kind,original_transfer_id,occurred_at_utc,
      instruction_id,allocation_result_id,run_worker_id,candidate_id,case_component_id,event_case_kind,hold_purpose,pay_week_start,
      source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,shortfall_source_ex_vat,cap_reason)
      values(v_case.id,v_job.command_id,v_result.id,'SHORTFALL',v_transfer.id,v_occurred_at,v_instruction.id,v_result.id,
        v_worker.id,v_worker.candidate_id,v_component.id,v_instruction.case_kind,v_instruction.hold_purpose,v_instruction.pay_week_start,
        0,0,0,0,v_instruction.nominal_source_ex_vat-v_amount,v_result.cap_reason);
  end if;
end
$function$;

alter function private.bpay_next_begin_case_transfer_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_case_transfer_member_guard_v1() owner to postgres;
alter function private.bpay_next_case_transfer_build_guard_v1() owner to postgres;
alter function private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
alter function private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_begin_case_transfer_v1(uuid,uuid),private.bpay_next_case_transfer_member_guard_v1(),
  private.bpay_next_case_transfer_build_guard_v1(),
  private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer),private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint)
  from public,anon,authenticated,service_role;
commit;
