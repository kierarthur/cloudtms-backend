-- Owner-only whole-Candidate cancellation of an unissued original PAYE Draft.
-- Requires the cancellation schema and shared ingress/enroller guards described
-- in tests/fixtures/bpay-next-simple-cancel-requirements.md. No public grant.
-- Intake records intent without disabling earlier accepted builders. Only the
-- ordered page owner starts CANCELLING, under the shared parent run mutex.
\set ON_ERROR_STOP on

begin;

-- Call only while holding the parent run, Candidate control and worker locks.
-- This is a bounded admission check, not an unbounded member/history scan.
-- realised_effect_count MUST be maintained by every realised posting owner.
create or replace function private.bpay_next_simple_cancel_block_v1(
  p_run_worker_id uuid,p_allow_building boolean default false
) returns text
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_count integer;
begin
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_worker.case_selection_revision>0 then
    return private.bpay_next_case_cancel_block_v1(p_run_worker_id,p_allow_building);
  end if;
  if v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_worker.target_umbrella_id is not null or v_worker.gross_vat<>0
     or v_worker.gross_inc_vat<0
     or v_worker.net_request_revision not between 0 and 1
     or v_worker.net_projection_revision not between 0 and 1
     or v_worker.net_projection_revision>v_worker.net_request_revision then
    return 'BPAY_NEXT_CANCEL_GROUP_NOT_SIMPLE_DRAFT';
  end if;
  if v_worker.realised_effect_count<>0
     or v_worker.financial_resolution_count<>0
     or exists(select 1 from private.bpay_next_worker_control c
               where c.candidate_id=v_worker.candidate_id
                 and c.pending_outcome_count<>0) then
    return 'BPAY_NEXT_CANCEL_GROUP_REALISED_OR_PENDING_OUTCOME';
  end if;
  if v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_case_hold h
               where h.run_worker_id=v_worker.id) then
    return 'BPAY_NEXT_CANCEL_CASE_GROUP_UNSUPPORTED';
  end if;
  if exists(select 1 from private.bpay_next_net_projection p
            where p.run_worker_id=v_worker.id
              and (p.projection_no<>1 or p.input_kind<>'PAYE_MANUAL'
                   or p.accepted_recoveries<>0
                   or p.retired_at_utc is not null)) then
    return 'BPAY_NEXT_CANCEL_PROJECTION_UNSUPPORTED';
  end if;
  if p_allow_building is not true
     and v_worker.net_request_revision<>v_worker.net_projection_revision then
    return 'BPAY_NEXT_CANCEL_PROJECTION_PENDING';
  end if;
  -- The existing original builder admits at most one original transfer.
  -- LIMIT 2 is deliberate: a broader/reissue group is not this owner's slice.
  select count(*) into v_count from
    (select 1 from private.bpay_next_transfer t
     where t.run_worker_id=v_worker.id order by t.transfer_no limit 2) t;
  if v_count>1 then return 'BPAY_NEXT_CANCEL_MULTI_TRANSFER_UNSUPPORTED'; end if;
  if v_count=1 then
    select * into strict v_transfer from private.bpay_next_transfer
      where run_worker_id=v_worker.id;
    if v_transfer.original_transfer_id is not null
       or v_transfer.return_cash_id is not null
       or v_transfer.projection_id is null
       or v_transfer.candidate_id<>v_worker.candidate_id
       or v_transfer.beneficiary_kind<>'CANDIDATE'
       or v_transfer.beneficiary_id<>v_worker.candidate_id then
      return 'BPAY_NEXT_CANCEL_REISSUE_OR_NON_CANDIDATE_UNSUPPORTED';
    end if;
    if v_transfer.status not in ('MEMBERS_READY','DRAFT','SCHEDULED')
       and not (p_allow_building is true and v_transfer.status='BUILDING') then
      return 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED';
    end if;
    if v_transfer.account_approval_ref is not null
       or exists(select 1 from private.bpay_next_csv_instruction i
                 where i.transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_transfer_outcome o
                 where o.transfer_id=v_transfer.id)
       or exists(select 1 from private.bpay_next_internal_receipt r
                 where r.transfer_id=v_transfer.id) then
      return 'BPAY_NEXT_CANCEL_INSTRUCTION_OR_OUTCOME_PROTECTED';
    end if;
  end if;
  return null;
end
$function$;

create or replace function private.bpay_next_accept_simple_cancel_v1(
  p_command_id uuid,p_run_worker_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_prior private.bpay_next_cancel_request%rowtype;
  v_sequence bigint;
  v_block text;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CANCEL_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- Frozen selection is routing information, not admission. The case owner
  -- rechecks the whole request under its command mutex BEFORE row locks,
  -- matching case NET/transfer intake and avoiding an inverted lock order.
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_worker.case_selection_revision>0 then
    return private.bpay_next_accept_case_cancel_v1(p_command_id,p_run_worker_id);
  end if;
  v_run_id:=v_worker.run_id;
  v_candidate_id:=v_worker.candidate_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id for update;
  select * into v_prior from private.bpay_next_cancel_request
    where command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>v_worker.id
       or v_prior.candidate_id<>v_worker.candidate_id then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='SIMPLE_CANCEL';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'run_worker_id',v_worker.id,'phase',v_prior.status,
      'cursor',v_prior.cursor_line_no,
      'released_line_count',v_prior.released_line_count,
      'blocked_code',v_prior.blocked_code,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or v_run.selection_state<>'SEALED'
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id
                 and r.status in ('REQUESTED','CANCELLING')) then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_NOT_ELIGIBLE';
  end if;
  -- BUILDING here is intentional. Its earlier accepted job must finish before
  -- the cancellation can claim; intake must not fence that job out of READY.
  v_block:=private.bpay_next_simple_cancel_block_v1(v_worker.id,true);
  if v_block is not null then
    raise exception using errcode='55000',message=v_block;
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'SIMPLE_CANCEL');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_cancel_request
    (command_id,run_worker_id,candidate_id,expected_line_count,status)
    values(p_command_id,v_worker.id,v_worker.candidate_id,
           v_worker.captured_line_count,'REQUESTED');
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'run_worker_id',v_worker.id,'phase','REQUESTED',
    'cursor',null,'released_line_count',0,'replay',false);
end
$function$;

create or replace function private.bpay_next_claim_simple_cancel_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_candidate_id uuid;
  v_job private.bpay_next_job%rowtype;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null
     or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_CANCEL_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate_id from private.bpay_next_job
    where id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'SIMPLE_CANCEL'
     or v_job.phase not in ('NEW','RELEASE')
     or v_job.status not in ('READY','LEASED')
     or v_job.available_at_utc>pg_catalog.clock_timestamp()
     or (v_job.status='LEASED'
         and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or not exists(select 1 from private.bpay_next_cancel_request r
                   where r.command_id=v_job.command_id
                     and r.candidate_id=v_candidate_id
                     and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=v_candidate_id
                 and earlier.command_sequence<v_job.command_sequence
                 and earlier.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()+
          pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce; owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

create or replace function private.bpay_next_cancel_simple_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_line record;
  v_hold private.bpay_next_hold%rowtype;
  v_block text;
  v_cursor bigint;
  v_released bigint;
  v_seen integer:=0;
  v_holds_released integer:=0;
  v_now timestamptz:=pg_catalog.transaction_timestamp();
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_owner_epoch<=0 or p_limit is null or p_limit not between 1 and 100
     or (p_expected_cursor is not null and p_expected_cursor<=0) then
    raise exception using errcode='22023',message='BPAY_NEXT_CANCEL_PAGE_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_job j
    join private.bpay_next_cancel_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  -- Same mutex as net projection, transfer build, destination/CSV issue and
  -- outcomes. No SOURCE/TARGET money moves before whole-group admission.
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_cancel_request
    where command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=v_request.run_worker_id for update;
  if v_worker.case_selection_revision>0 then
    return private.bpay_next_cancel_case_worker_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id
     or v_request.candidate_id<>v_worker.candidate_id
     or v_worker.run_id<>v_run.id
     or v_job.applied_line_count<>v_request.released_line_count
     or v_job.cursor_key is distinct from v_request.cursor_line_no::text
     or not exists(select 1 from private.bpay_next_command c
                   where c.id=v_job.command_id
                     and c.command_kind='SIMPLE_CANCEL'
                     and c.agency_sequence=v_job.command_sequence
                     and c.module_epoch=v_epoch
                     and c.expected_member_count=1
                     and c.enrolled_member_count=1) then
    raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.status in ('CANCELLED','BLOCKED') then
    return pg_catalog.jsonb_build_object('phase',v_request.status,
      'run_worker_id',v_worker.id,'cursor',v_request.cursor_line_no,
      'released_line_count',v_request.released_line_count,
      'blocked_code',v_request.blocked_code,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase not in ('NEW','RELEASE')
     or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=v_candidate_id
                 and earlier.command_sequence<v_job.command_sequence
                 and earlier.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID';
  end if;
  if v_request.cursor_line_no is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('phase',v_request.status,
      'run_worker_id',v_worker.id,'cursor',v_request.cursor_line_no,
      'released_line_count',v_request.released_line_count,'replay',true);
  end if;
  if v_request.status='REQUESTED' then
    if v_job.phase<>'NEW' or v_request.released_line_count<>0
       or v_request.cursor_line_no is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_STATE_INVALID';
    end if;
    v_block:=private.bpay_next_simple_cancel_block_v1(v_worker.id,false);
    if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
       or v_worker.captured_line_count<>v_request.expected_line_count then
      v_block:='BPAY_NEXT_CANCEL_DRAFT_CHANGED';
    end if;
    if v_block is not null then
      -- A newly protected group remains financially intact. Close this
      -- refused intent as DONE, not as an ordering job that stalls the queue.
      update private.bpay_next_cancel_request
        set status='BLOCKED',blocked_code=v_block,finished_at_utc=v_now
        where command_id=v_job.command_id;
      update private.bpay_next_job
        set status='DONE',phase='BLOCKED',last_error_code=v_block,
            lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE'
        where id=v_job.command_id;
      return pg_catalog.jsonb_build_object('phase','BLOCKED',
        'run_worker_id',v_worker.id,'cursor',null,
        'released_line_count',0,'blocked_code',v_block,'replay',false);
    end if;
    -- Header lock plus READY -> CANCELLING fences the whole group before
    -- cancelling its exact unissued original transfer or releasing any hold.
    update private.bpay_next_run_worker set status='CANCELLING'
      where id=v_worker.id;
    update private.bpay_next_transfer set status='CANCELLED'
      where run_worker_id=v_worker.id;
    update private.bpay_next_cancel_request
      set status='CANCELLING',started_at_utc=v_now
      where command_id=v_job.command_id;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
    v_worker.status:='CANCELLING';
    v_request.status:='CANCELLING';
  end if;
  if v_request.status<>'CANCELLING' or v_worker.status<>'CANCELLING'
     or v_worker.realised_effect_count<>0
     or v_worker.financial_resolution_count<>0
     or v_control.pending_outcome_count<>0
     or v_run.status<>'DRAFT'
     or v_worker.captured_line_count<>v_request.expected_line_count then
    raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_FENCE_BROKEN';
  end if;
  v_cursor:=v_request.cursor_line_no;
  v_released:=v_request.released_line_count;
  for v_line in
    select l.id,l.line_no,l.work_id,l.component_key,l.source_consumed_ex_vat,
           l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat
    from private.bpay_next_run_line l
    where l.run_worker_id=v_worker.id
      and l.line_no>coalesce(v_cursor,0)
    order by l.line_no,l.id limit p_limit
  loop
    select * into v_hold from private.bpay_next_hold
      where run_line_id=v_line.id for update;
    if found then
      if v_hold.status<>'ACTIVE'
         or v_hold.work_id<>v_line.work_id
         or v_hold.component_key<>v_line.component_key
         or v_hold.source_reserved_ex_vat<>v_line.source_consumed_ex_vat
         or v_hold.target_amount_ex_vat<>v_line.frozen_ex_vat
         or v_hold.target_amount_vat<>v_line.frozen_vat
         or v_hold.target_amount_inc_vat<>v_line.frozen_inc_vat then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_HOLD_NOT_EXACT_ACTIVE';
      end if;
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
            held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,
            held_target_vat=held_target_vat-v_hold.target_amount_vat,
            held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,
            updated_at_utc=v_now
        where work_id=v_hold.work_id and component_key=v_hold.component_key
          and held_source_ex_vat>=v_hold.source_reserved_ex_vat
          and held_target_ex_vat>=v_hold.target_amount_ex_vat
          and held_target_vat>=v_hold.target_amount_vat
          and held_target_inc_vat>=v_hold.target_amount_inc_vat;
      if not found then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITION_HOLD_MISMATCH';
      end if;
      update private.bpay_next_hold
        set status='RELEASED',finished_at_utc=v_now where id=v_hold.id;
      v_holds_released:=v_holds_released+1;
    elsif v_line.source_consumed_ex_vat<>0 or v_line.frozen_ex_vat<>0
          or v_line.frozen_vat<>0 or v_line.frozen_inc_vat<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITIVE_HOLD_MISSING';
    end if;
    v_seen:=v_seen+1;
    v_released:=v_released+1;
    v_cursor:=v_line.line_no;
  end loop;
  if v_released>v_request.expected_line_count
     or (v_seen=0 and v_released<v_request.expected_line_count) then
    raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_FROZEN_LINE_COUNT_MISMATCH';
  end if;
  update private.bpay_next_cancel_request
    set cursor_line_no=v_cursor,released_line_count=v_released
    where command_id=v_job.command_id;
  update private.bpay_next_job
    set cursor_key=v_cursor::text,applied_line_count=v_released,
        lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120)
    where id=p_job_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now
    where candidate_id=v_candidate_id;
  if v_released=v_request.expected_line_count then
    if exists(select 1 from private.bpay_next_run_line l
              where l.run_worker_id=v_worker.id
                and l.line_no>coalesce(v_cursor,0))
       or v_run.cancelled_candidate_count>=v_run.selected_candidate_count then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_COMPLETION_COUNT_MISMATCH';
    end if;
    -- Preserve projection money and every frozen/audit row. Retire only its
    -- execution eligibility; no reprice, deletion, case/effect or payment.
    update private.bpay_next_net_projection set retired_at_utc=v_now
      where run_worker_id=v_worker.id and retired_at_utc is null;
    update private.bpay_next_run_worker set status='CANCELLED'
      where id=v_worker.id;
    update private.bpay_next_pay_run
      set cancelled_candidate_count=cancelled_candidate_count+1,
          status=case when cancelled_candidate_count+1=selected_candidate_count
            then 'CANCELLED' else status end
      where id=v_run.id;
    update private.bpay_next_cancel_request
      set status='CANCELLED',finished_at_utc=v_now
      where command_id=v_job.command_id;
    perform private.bpay_next_apply_cancelled_week_v1(p_job_id);
    update private.bpay_next_job
      set status='DONE',phase='CANCELLED',lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE'
      where id=v_job.command_id;
    return pg_catalog.jsonb_build_object('phase','CANCELLED',
      'run_worker_id',v_worker.id,'cursor',v_cursor,
      'released_line_count',v_released,'rows_visited',v_seen,
      'holds_released',v_holds_released,'replay',false);
  end if;
  return pg_catalog.jsonb_build_object('phase','CANCELLING',
    'run_worker_id',v_worker.id,'cursor',v_cursor,
    'released_line_count',v_released,'rows_visited',v_seen,
    'holds_released',v_holds_released,'replay',false);
end
$function$;

alter function private.bpay_next_simple_cancel_block_v1(uuid,boolean) owner to postgres;
alter function private.bpay_next_accept_simple_cancel_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_claim_simple_cancel_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_cancel_simple_worker_page_v1(uuid,uuid,bigint,bigint,integer)
  owner to postgres;
revoke all on function private.bpay_next_simple_cancel_block_v1(uuid,boolean),
  private.bpay_next_accept_simple_cancel_v1(uuid,uuid),
  private.bpay_next_claim_simple_cancel_job_v1(uuid,integer),
  private.bpay_next_cancel_simple_worker_page_v1(uuid,uuid,bigint,bigint,integer)
  from public,anon,authenticated,service_role;

commit;
