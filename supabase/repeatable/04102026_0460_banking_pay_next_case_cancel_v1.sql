-- Actual ordered original CASE cancellation. Main0260 dispatches here using
-- the worker's immutable case selection, not live Candidate cases. Shared
--0430 completes the maintained weekly contribution in the FINAL transaction.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_case_cancel_block_v1(p_run_worker_id uuid,p_allow_pending boolean default false)
returns text language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;v_count integer:=0;
 v_group private.bpay_next_destination_group%rowtype;
begin
  select * into strict v_worker from private.bpay_next_run_worker where id=p_run_worker_id;
  if v_worker.status<>'READY' or v_worker.case_selection_revision<1 or v_worker.target_pay_channel<>'PAYE'
     or v_worker.target_umbrella_id is not null or v_worker.gross_vat<>0 or v_worker.gross_inc_vat<0
     or v_worker.net_projection_revision>v_worker.net_request_revision or v_worker.case_pending_binding_count<>0 then
    return 'BPAY_NEXT_CANCEL_CASE_GROUP_NOT_DRAFT';end if;
  if v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0
     or exists(select 1 from private.bpay_next_worker_control where candidate_id=v_worker.candidate_id and pending_outcome_count<>0) then
    return 'BPAY_NEXT_CANCEL_GROUP_REALISED_OR_PENDING_OUTCOME';end if;
  if not exists(select 1 from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
      and preparation_revision=v_worker.preparation_revision and selection_revision=v_worker.case_selection_revision
      and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE' and prepare_stage='COMPLETE'
      and expected_instruction_count=v_worker.case_instruction_count and processed_instruction_count=v_worker.case_instruction_count) then
    return 'BPAY_NEXT_CANCEL_CASE_BINDING_INVALID';end if;
  if p_allow_pending is not true and v_worker.net_request_revision<>v_worker.net_projection_revision then
    return 'BPAY_NEXT_CANCEL_PROJECTION_PENDING';end if;
  select * into v_group from private.bpay_next_destination_group where run_worker_id=v_worker.id;
  if found then
    if v_group.candidate_id<>v_worker.candidate_id
      or (p_allow_pending is not true and v_group.stage<>'COMPLETE') then
      return 'BPAY_NEXT_CANCEL_DESTINATION_GROUP_NOT_READY';end if;
    -- Do not examine a whole transfer group here. The ordered job performs an
    -- indexed <=100-leg CHECK phase before changing worker state/releasing holds.
    return null;
  end if;
  if p_allow_pending is true and exists(select 1 from private.bpay_next_net_projection p
      join private.bpay_next_net_destination_state d on d.projection_id=p.id
      join private.bpay_next_transfer t on t.projection_id=p.id and t.run_worker_id=v_worker.id
      join private.bpay_next_command c on c.id=t.build_command_id
      where p.run_worker_id=v_worker.id and p.projection_no=v_worker.net_projection_revision and p.retired_at_utc is null
        and d.run_worker_id=v_worker.id and d.completed_at_utc is not null and d.external_leg_count>0
        and t.original_transfer_id is null and t.status='BUILDING' and c.command_kind='TRANSFER_BUILD'
        and c.status in ('SEALED','ENROLLING','COMPLETE')) then
    -- Genuine earlier accepted builder has not created its group yet. Existing
    -- Candidate command ordering finishes it before SIMPLE_CANCEL can claim.
    return null;
  end if;
  for v_transfer in select * from private.bpay_next_transfer where run_worker_id=v_worker.id order by transfer_no limit 2 loop
    v_count:=v_count+1;
    if v_count>1 then return 'BPAY_NEXT_CANCEL_MULTI_TRANSFER_UNSUPPORTED';end if;
    if v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
       or v_transfer.candidate_id<>v_worker.candidate_id or v_transfer.beneficiary_kind<>'CANDIDATE'
       or v_transfer.beneficiary_id<>v_worker.candidate_id
       or not exists(select 1 from private.bpay_next_case_transfer_build where transfer_id=v_transfer.id and run_worker_id=v_worker.id) then
      return 'BPAY_NEXT_CANCEL_REISSUE_OR_NON_CANDIDATE_UNSUPPORTED';end if;
    if v_transfer.status not in ('MEMBERS_READY','DRAFT','SCHEDULED')
       and not (p_allow_pending is true and v_transfer.status='BUILDING') then
      return 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED';end if;
    if v_transfer.account_approval_ref is not null
       or exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_transfer_outcome where transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_internal_receipt where transfer_id=v_transfer.id) then
      return 'BPAY_NEXT_CANCEL_INSTRUCTION_OR_OUTCOME_PROTECTED';end if;
  end loop;
  return null;
end
$function$;

create or replace function private.bpay_next_accept_case_cancel_v1(p_command_id uuid,p_run_worker_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;v_draft private.bpay_next_case_allocation_state%rowtype;
  v_sequence bigint;v_block text;
begin
  if p_command_id is null or p_run_worker_id is null then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CANCEL_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_command_id::text,0));
  select run_id into strict v_run_id from private.bpay_next_run_worker where id=p_run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c join private.bpay_next_run_worker w on w.candidate_id=c.candidate_id where w.id=p_run_worker_id for update of c;
  select * into strict v_worker from private.bpay_next_run_worker where id=p_run_worker_id for update;
  select * into v_request from private.bpay_next_cancel_request where command_id=p_command_id;
  if found then
    if v_request.run_worker_id<>v_worker.id or v_request.candidate_id<>v_worker.candidate_id
       or not exists(select 1 from private.bpay_next_case_cancel_binding where command_id=p_command_id and run_worker_id=v_worker.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id and command_kind='SIMPLE_CANCEL' and module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('sequence',v_sequence,'run_worker_id',v_worker.id,'phase',v_request.status,
      'cursor',v_request.cursor_line_no,'released_line_count',v_request.released_line_count,'blocked_code',v_request.blocked_code,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_run.selection_state<>'SEALED'
     or exists(select 1 from private.bpay_next_cancel_request where run_worker_id=v_worker.id and status in ('REQUESTED','CANCELLING')) then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_NOT_ELIGIBLE';end if;
  v_block:=private.bpay_next_case_cancel_block_v1(v_worker.id,true);
  if v_block is not null then raise exception using errcode='55000',message=v_block;end if;
  select * into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.preparation_revision and selection_revision=v_worker.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE';
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'SIMPLE_CANCEL');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_cancel_request(command_id,run_worker_id,candidate_id,expected_line_count,status)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_worker.captured_line_count,'REQUESTED');
  insert into private.bpay_next_case_cancel_binding(command_id,run_worker_id,candidate_id,draft_state_id,
    preparation_revision,selection_revision,status,stage,expected_work_count)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_draft.id,v_worker.preparation_revision,v_worker.case_selection_revision,
      'REQUESTED','INTENT',v_worker.captured_line_count);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,'run_worker_id',v_worker.id,'phase','REQUESTED','cursor',null,'released_line_count',0,'replay',false);
end
$function$;

create or replace function private.bpay_next_case_cancel_binding_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_request private.bpay_next_cancel_request%rowtype;v_draft private.bpay_next_case_allocation_state%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_BINDING_DELETE_FORBIDDEN';end if;
  if tg_op='INSERT' then
    select * into strict v_request from private.bpay_next_cancel_request where command_id=new.command_id;
    select * into strict v_draft from private.bpay_next_case_allocation_state where id=new.draft_state_id;
    if new.status<>'REQUESTED' or new.stage<>'INTENT' or new.checkpoint<>0 or new.released_work_count<>0
       or new.released_case_hold_count<>0 or new.work_cursor is not null or new.case_hold_cursor is not null
       or v_request.status<>'REQUESTED' or v_request.run_worker_id<>new.run_worker_id or v_request.candidate_id<>new.candidate_id
       or v_request.expected_line_count<>new.expected_work_count or v_draft.pass_kind<>'DRAFT'
       or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_BINDING_INITIAL_INVALID';end if;
    return new;
  end if;
  if old.status in ('CANCELLED','BLOCKED')
     or (new.command_id,new.run_worker_id,new.candidate_id,new.draft_state_id,new.preparation_revision,new.selection_revision,new.expected_work_count,new.created_at_utc)
       is distinct from (old.command_id,old.run_worker_id,old.candidate_id,old.draft_state_id,old.preparation_revision,old.selection_revision,old.expected_work_count,old.created_at_utc)
     or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
     or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
     or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
     or (old.stage='INTENT' and new.stage not in ('WORK','CASE','BLOCKED')
       and not (new.stage='INTENT' and new.status='REQUESTED' and exists(select 1 from private.bpay_next_destination_cancel p
         where p.command_id=new.command_id and p.checked_leg_count>0 and p.checked_leg_count<p.expected_leg_count)))
     or (old.stage='WORK' and new.stage not in ('WORK','CASE')) or (old.stage='CASE' and new.stage not in ('CASE','FINAL'))
     or (old.stage='FINAL' and new.stage<>'COMPLETE'
       and not (new.stage='FINAL' and new.status='CANCELLING' and exists(select 1 from private.bpay_next_destination_cancel p
         where p.command_id=new.command_id and p.checked_leg_count=p.expected_leg_count
           and p.cancelled_leg_count>0 and p.cancelled_leg_count<p.expected_leg_count)))
     or (old.status='CANCELLING' and (new.expected_case_hold_count,new.accepted_projection_revision,new.projection_id,new.net_state_id,new.transfer_id,new.started_at_utc)
       is distinct from (old.expected_case_hold_count,old.accepted_projection_revision,old.projection_id,old.net_state_id,old.transfer_id,old.started_at_utc)) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_BINDING_TRANSITION_INVALID';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_case_cancel_binding_guard_v1 on private.bpay_next_case_cancel_binding;
create trigger bpay_next_case_cancel_binding_guard_v1 before insert or update or delete on private.bpay_next_case_cancel_binding
  for each row execute function private.bpay_next_case_cancel_binding_guard_v1();

create or replace function private.bpay_next_cancel_case_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_checkpoint bigint;v_seen integer:=0;v_holds integer:=0;v_block text;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;v_worker private.bpay_next_run_worker%rowtype;v_binding private.bpay_next_case_cancel_binding%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_net private.bpay_next_case_allocation_state%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_line private.bpay_next_run_line%rowtype;v_work_hold private.bpay_next_hold%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_period private.bpay_next_case_period%rowtype;v_amount numeric;v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_group private.bpay_next_destination_group%rowtype;v_group_page jsonb;v_group_started boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 or p_limit is null or p_limit not between 1 and 100
     or (p_expected_cursor is not null and p_expected_cursor<1) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CANCEL_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_cancel_request r on r.command_id=j.command_id join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_cancel_request where command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_binding from private.bpay_next_case_cancel_binding where command_id=v_job.command_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_binding.candidate_id
     or v_request.run_worker_id<>v_binding.run_worker_id or v_worker.candidate_id<>v_binding.candidate_id
     or v_worker.preparation_revision<>v_binding.preparation_revision or v_worker.case_selection_revision<>v_binding.selection_revision
     or v_request.expected_line_count<>v_binding.expected_work_count or v_request.released_line_count<>v_binding.released_work_count
     or v_job.applied_line_count<>v_binding.released_work_count
     or coalesce(v_request.cursor_line_no,0)<>v_binding.checkpoint or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command where id=v_job.command_id and command_kind='SIMPLE_CANCEL'
       and agency_sequence=v_job.command_sequence and module_epoch=v_epoch and expected_member_count=1 and enrolled_member_count=1) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_request.status in ('CANCELLED','BLOCKED') and v_binding.status=v_request.status then
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'blocked_code',v_request.blocked_code,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_cursor,0)<>v_binding.checkpoint then
    if coalesce(p_expected_cursor,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_CASE_CANCEL_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  select * into v_group from private.bpay_next_destination_group where run_worker_id=v_worker.id;
  if v_binding.status='REQUESTED' then
    if v_job.phase<>'NEW' or v_request.status<>'REQUESTED' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_INTENT_INVALID';end if;
    v_block:=private.bpay_next_case_cancel_block_v1(v_worker.id,false);
    if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.captured_line_count<>v_binding.expected_work_count then
      v_block:='BPAY_NEXT_CANCEL_DRAFT_CHANGED';end if;
    if v_block is null and v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_check_destination_cancel_page_v1(p_job_id,p_limit);
      v_block:=v_group_page->>'blocked_code';v_seen:=(v_group_page->>'rows_visited')::integer;
      if v_block is null and (v_group_page->>'complete')::boolean is not true then
        update private.bpay_next_case_cancel_binding set checkpoint=checkpoint+1 where command_id=v_job.command_id
          returning checkpoint into v_checkpoint;
        update private.bpay_next_cancel_request set cursor_line_no=v_checkpoint where command_id=v_job.command_id;
        update private.bpay_next_job set cursor_key=v_checkpoint::text,
          lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
        return pg_catalog.jsonb_build_object('phase','REQUESTED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
          'released_line_count',0,'rows_visited',v_seen,'holds_released',0,'replay',false);
      end if;
      v_group_started:=true;
    end if;
    if v_block is not null then
      update private.bpay_next_case_cancel_binding set status='BLOCKED',stage='BLOCKED',checkpoint=checkpoint+1,blocked_code=v_block,finished_at_utc=v_now
        where command_id=v_job.command_id returning checkpoint into v_checkpoint;
      update private.bpay_next_cancel_request set status='BLOCKED',blocked_code=v_block,finished_at_utc=v_now,cursor_line_no=v_checkpoint where command_id=v_job.command_id;
      update private.bpay_next_job set status='DONE',phase='BLOCKED',cursor_key=v_checkpoint::text,last_error_code=v_block,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      return pg_catalog.jsonb_build_object('phase','BLOCKED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
        'released_line_count',0,'blocked_code',v_block,'rows_visited',v_seen,'holds_released',0,'replay',false);
    end if;
    v_binding.accepted_projection_revision:=v_worker.net_projection_revision;
    if v_worker.net_projection_revision>0 then
      select * into strict v_projection from private.bpay_next_net_projection where run_worker_id=v_worker.id
        and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
      select * into strict v_net from private.bpay_next_case_allocation_state where projection_id=v_projection.id and run_worker_id=v_worker.id
        and pass_kind='NET' and status='COMPLETE' and net_stage='COMPLETE';
      if v_net.preparation_revision<>v_binding.preparation_revision or v_net.selection_revision<>v_binding.selection_revision
         or v_projection.input_kind not in ('PAYE_MANUAL','CASE_PAYOUT') then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_NET_BINDING_INVALID';end if;
      v_binding.projection_id:=v_projection.id;v_binding.net_state_id:=v_net.id;
    end if;
    if v_group.anchor_transfer_id is not null then
      v_binding.transfer_id:=v_group.anchor_transfer_id;
    else
      select * into v_transfer from private.bpay_next_transfer where run_worker_id=v_worker.id for update;
      if found then v_binding.transfer_id:=v_transfer.id;update private.bpay_next_transfer set status='CANCELLED' where id=v_transfer.id;end if;
    end if;
    v_binding.expected_case_hold_count:=v_worker.active_case_hold_count;v_binding.started_at_utc:=v_now;
    v_binding.status:='CANCELLING';v_binding.stage:='WORK';
    update private.bpay_next_run_worker set status='CANCELLING' where id=v_worker.id;
    update private.bpay_next_cancel_request set status='CANCELLING',started_at_utc=v_now where command_id=v_job.command_id;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
    v_worker.status:='CANCELLING';v_request.status:='CANCELLING';
  end if;
  if v_binding.status<>'CANCELLING' or v_worker.status<>'CANCELLING' or v_request.status<>'CANCELLING'
     or v_run.status<>'DRAFT' or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0 or v_control.pending_outcome_count<>0
     or v_worker.net_request_revision<>v_binding.accepted_projection_revision or v_worker.net_projection_revision<>v_binding.accepted_projection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FENCE_BROKEN';end if;
  if v_group_started then
    -- This STEP examined only the CHECK page. WORK release starts next STEP;
    -- do not combine two independent <=100-row phases into a 200-row call.
    null;
  elsif v_binding.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)
      order by line_no,id limit p_limit loop
      select * into v_work_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
      if found then
        if v_work_hold.status<>'ACTIVE' or (v_work_hold.work_id,v_work_hold.component_key,v_work_hold.source_reserved_ex_vat,
          v_work_hold.target_amount_ex_vat,v_work_hold.target_amount_vat,v_work_hold.target_amount_inc_vat)
          is distinct from (v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_HOLD_NOT_EXACT_ACTIVE';end if;
        update private.bpay_next_position set held_source_ex_vat=held_source_ex_vat-v_work_hold.source_reserved_ex_vat,
          held_target_ex_vat=held_target_ex_vat-v_work_hold.target_amount_ex_vat,held_target_vat=held_target_vat-v_work_hold.target_amount_vat,
          held_target_inc_vat=held_target_inc_vat-v_work_hold.target_amount_inc_vat,updated_at_utc=v_now
          where work_id=v_work_hold.work_id and component_key=v_work_hold.component_key and held_source_ex_vat>=v_work_hold.source_reserved_ex_vat
            and held_target_ex_vat>=v_work_hold.target_amount_ex_vat and held_target_vat>=v_work_hold.target_amount_vat and held_target_inc_vat>=v_work_hold.target_amount_inc_vat;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITION_HOLD_MISMATCH';end if;
        update private.bpay_next_hold set status='RELEASED',finished_at_utc=v_now where id=v_work_hold.id;v_holds:=v_holds+1;
      elsif (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITIVE_HOLD_MISSING';end if;
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)) into v_more;
    if not v_more then
      if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORK_COUNT_INVALID';end if;
      v_binding.stage:='CASE';
    end if;
  elsif v_binding.stage='CASE' then
    for v_hold in select * from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_hold.instruction_id;
      select * into strict v_result from private.bpay_next_case_allocation_result where id=v_hold.allocation_result_id;
      select * into strict v_case from private.bpay_next_finance_case where id=v_hold.case_id for update;
      select * into strict v_component from private.bpay_next_case_component where id=v_hold.case_component_id for update;
      select * into strict v_period from private.bpay_next_case_period where case_component_id=v_hold.case_component_id and pay_week_start=v_hold.pay_week_start for update;
      perform 1 from private.bpay_next_case_hold where id=v_hold.id for update;
      select * into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_hold.id for update;
      if v_instruction.run_worker_id<>v_worker.id or v_instruction.preparation_revision<>v_binding.preparation_revision
         or v_instruction.selection_revision<>v_binding.selection_revision or v_result.instruction_id<>v_instruction.id
         or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT' and v_binding.accepted_projection_revision>0
           then v_binding.net_state_id else v_binding.draft_state_id end)
         or v_hold.allocation_pass_kind<>v_result.pass_kind or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
         or v_use.source_amount_ex_vat<>v_hold.source_reserved_ex_vat
         or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
           is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_HOLD_BINDING_INVALID';end if;
      v_amount:=v_hold.source_reserved_ex_vat;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_payout_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
      else
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_recovery_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CAPACITY_INVALID';end if;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
      else
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_PERIOD_CAPACITY_INVALID';end if;
      update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_hold.id;
      update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_hold.id;
      update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORKER_HOLD_COUNT_INVALID';end if;
      if v_instruction.work_collection_id is not null then
        perform private.bpay_next_reconcile_cancelled_work_collection_v1(p_job_id,v_hold.id);
      end if;
      v_binding.case_hold_cursor:=v_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;v_seen:=v_seen+1;v_holds:=v_holds+1;
    end loop;
    select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE') into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
         or (select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CURSOR_INVALID';end if;
  elsif v_binding.stage='FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
       or v_worker.active_case_hold_count<>0 or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE')
       or v_run.cancelled_candidate_count>=v_run.selected_candidate_count then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FINAL_COUNT_INVALID';end if;
    if v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_finish_destination_cancel_page_v1(p_job_id,p_limit);
      v_seen:=(v_group_page->>'rows_visited')::integer;
    end if;
    if v_group.anchor_transfer_id is null or (v_group_page->>'complete')::boolean is true then
      if v_binding.projection_id is not null then update private.bpay_next_net_projection set retired_at_utc=v_now where id=v_binding.projection_id and retired_at_utc is null;end if;
      update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;
      update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1,
        status=case when cancelled_candidate_count+1=selected_candidate_count then 'CANCELLED' else status end where id=v_run.id;
      v_binding.status:='CANCELLED';v_binding.stage:='COMPLETE';v_binding.finished_at_utc:=v_now;
      if v_group.anchor_transfer_id is null then v_seen:=1;end if;
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_STAGE_INVALID';end if;
  update private.bpay_next_case_cancel_binding set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,work_cursor=v_binding.work_cursor,expected_case_hold_count=v_binding.expected_case_hold_count,
    released_case_hold_count=v_binding.released_case_hold_count,case_hold_cursor=v_binding.case_hold_cursor,
    accepted_projection_revision=v_binding.accepted_projection_revision,projection_id=v_binding.projection_id,net_state_id=v_binding.net_state_id,
    transfer_id=v_binding.transfer_id,started_at_utc=v_binding.started_at_utc,finished_at_utc=v_binding.finished_at_utc
    where command_id=v_job.command_id returning checkpoint into v_checkpoint;
  update private.bpay_next_cancel_request set status=v_binding.status,cursor_line_no=v_checkpoint,released_line_count=v_binding.released_work_count,
    finished_at_utc=v_binding.finished_at_utc where command_id=v_job.command_id;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
  if v_binding.status='CANCELLED' then
    perform private.bpay_next_apply_cancelled_week_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='CANCELLED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  end if;
  return pg_catalog.jsonb_build_object('phase',v_binding.status,'run_worker_id',v_worker.id,'cursor',v_checkpoint,
    'released_line_count',v_binding.released_work_count,'rows_visited',v_seen,'holds_released',v_holds,'replay',false);
end
$function$;
alter function private.bpay_next_case_cancel_block_v1(uuid,boolean) owner to postgres;
alter function private.bpay_next_accept_case_cancel_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_case_cancel_binding_guard_v1() owner to postgres;
alter function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_case_cancel_block_v1(uuid,boolean),private.bpay_next_accept_case_cancel_v1(uuid,uuid),
  private.bpay_next_case_cancel_binding_guard_v1(),private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer)
  from public,anon,authenticated,service_role;
commit;
