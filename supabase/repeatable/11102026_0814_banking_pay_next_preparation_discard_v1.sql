-- DR09 separately typed whole-run discard. Native money/RuleB/due-expiry retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_preparation_discard_fenced_v1(p_run_id uuid,p_prepare_command_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_pay_run r
    join private.bpay_next_preparation_discard_request x on x.run_id=r.id
    where r.id=p_run_id and r.status in ('CANCELLING','CANCELLED','REVIEW') and r.confirmed_at_utc is null
      and x.status in ('CANCELLING','DISCARDED','PARTIAL') and x.original_prepare_command_id=p_prepare_command_id
      and x.captured_selection_state=r.selection_state
      and x.selection_count=r.selection_count and x.selection_page_count=r.selection_page_count
      and x.expected_candidate_count=r.selected_candidate_count
      and r.review_revision=x.selection_review_revision+1
      and (r.status='CANCELLING' or (x.status in ('DISCARDED','PARTIAL') and x.completed_candidate_count=x.expected_candidate_count
        and x.enrolled_candidate_count=x.expected_candidate_count)))
$function$;

create or replace function private.bpay_next_finish_discard_member_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_member private.bpay_next_preparation_discard_worker%rowtype;v_request private.bpay_next_preparation_discard_request%rowtype;
begin
 select x.* into strict v_member from private.bpay_next_preparation_discard_worker x where x.job_id=p_job_id;
 if v_member.stage<>'COMPLETE' or v_member.outcome is null or not exists(select 1 from private.bpay_next_job j
   where j.id=p_job_id and j.command_id=v_member.command_id and j.job_kind='PREPARATION_DISCARD' and j.status='DONE'
     and j.phase=case v_member.outcome when 'BLOCKED' then 'BLOCKED' else 'DISCARDED' end
     and j.cursor_key is not distinct from nullif(v_member.checkpoint,0)::text) then
  raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_MEMBER_NOT_TERMINAL';end if;
 update private.bpay_next_preparation_discard_request set completed_candidate_count=completed_candidate_count+1,
  removed_candidate_count=removed_candidate_count+case when v_member.outcome='REMOVED' then 1 else 0 end,
  already_removed_candidate_count=already_removed_candidate_count+case when v_member.outcome='ALREADY_REMOVED' then 1 else 0 end,
  blocked_candidate_count=blocked_candidate_count+case when v_member.outcome='BLOCKED' then 1 else 0 end,
  status=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count
    then case when blocked_candidate_count+case when v_member.outcome='BLOCKED' then 1 else 0 end>0 then 'PARTIAL' else 'DISCARDED' end else status end,
  finished_at_utc=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count
    then v_member.finished_at_utc else null end
  where command_id=v_member.command_id and status='CANCELLING' and completed_candidate_count<enrolled_candidate_count returning * into strict v_request;
 if v_request.status in ('PARTIAL','DISCARDED') then
  update private.bpay_next_pay_run set status=case v_request.status when 'DISCARDED' then 'CANCELLED' else 'REVIEW' end where id=v_request.run_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_request.command_id;
 end if;
end
$function$;

create or replace function private.bpay_next_accept_preparation_discard_v1(
  p_command_id uuid,p_run_id uuid,p_actor_user_id uuid,p_expected_preparation_revision bigint,p_reason text default null)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_prepare_command private.bpay_next_command%rowtype;
 v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_discard_request%rowtype;v_sequence bigint;v_prepare uuid;v_now timestamptz;
begin
  if p_command_id is null or p_run_id is null or p_actor_user_id is null or p_expected_preparation_revision is null
    or p_expected_preparation_revision<0 or(p_reason is not null and(pg_catalog.octet_length(p_reason) not between 1 and 500
      or pg_catalog.btrim(p_reason)='')) then raise exception using errcode='22023',message='BPAY_NEXT_DISCARD_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-preparation-expiry:'||p_command_id::text,0));
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select d.* into v_request from private.bpay_next_preparation_discard_request d where d.command_id=p_command_id;
  if found then
    if v_request.run_id<>p_run_id or v_request.actor_user_id is distinct from p_actor_user_id
       or v_request.selection_review_revision<>p_expected_preparation_revision or v_request.reason is distinct from p_reason
       or not exists(select 1 from private.bpay_next_command where id=p_command_id
         and command_kind='PREPARATION_DISCARD' and module_epoch=v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
      'phase',v_request.status,'expected_candidates',v_request.expected_candidate_count::text,
      'completed_candidates',v_request.completed_candidate_count::text,'removed_candidates',v_request.removed_candidate_count::text,
      'already_removed_candidates',v_request.already_removed_candidate_count::text,'blocked_candidates',v_request.blocked_candidate_count::text,'replay',true);
  end if;
  v_now:=pg_catalog.clock_timestamp();
  if p_actor_user_id is not null and not exists(select 1 from public.tms_users
      where id=p_actor_user_id and is_active is true and role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_DISCARD_ACTOR_FORBIDDEN';end if;
  if v_run.confirmed_at_utc is not null or v_run.status not in ('PREPARING','REVIEW') then
    raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_NOT_UNCONFIRMED';end if;
  if v_run.review_revision<>p_expected_preparation_revision then raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_REVISION_STALE';end if;
  if exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_EXPIRY_ALREADY_BOUND';end if;
  if v_run.selected_candidate_count=0 then
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',null,
      'phase','EMPTY','expected_candidates','0','completed_candidates','0','removed_candidates','0',
      'already_removed_candidates','0','blocked_candidates','0','replay',false);end if;
  if exists(select 1 from private.bpay_next_preparation_discard_request where run_id=p_run_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_RUN_ALREADY_BOUND';end if;
  if exists(select 1 from private.bpay_next_worker_reprepare_request where run_id=p_run_id and action='RELEASE' and status<>'COMPLETE') then
    raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_WORKER_RELEASE_PENDING';end if;
  select command_id into v_prepare from private.bpay_next_run_command where run_id=p_run_id;
if v_prepare is not null then
  select c.* into strict v_prepare_command
    from private.bpay_next_command c where c.id=v_prepare;
  if v_prepare_command.status='RECEIVED' then
    -- Original run UPDATE is already held. Enroller RECEIVED takes no run lock.
    select c.* into strict v_prepare_command
      from private.bpay_next_command c where c.id=v_prepare for update;
    if v_prepare_command.command_kind is distinct from 'PREPARE'
       or v_prepare_command.module_epoch is distinct from v_epoch
       or v_prepare_command.status is distinct from 'RECEIVED'
       or v_prepare_command.expected_member_count is not null
       or v_prepare_command.enrolled_member_count<>0
       or v_prepare_command.sealed_at_utc is not null
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_prepare
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=p_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_DISCARD_PREPARE_BINDING_INVALID';
    end if;
    update private.bpay_next_command
      set expected_member_count=0,status='COMPLETE',sealed_at_utc=v_now
      where id=v_prepare and status='RECEIVED';
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_DISCARD_PREPARE_BINDING_INVALID';
    end if;
  elsif not exists(select 1 from private.bpay_next_command
      where id=v_prepare and command_kind='PREPARE' and module_epoch=v_epoch
        and status in ('ENROLLING','SEALED')
        and expected_member_count=v_run.selected_candidate_count) then
    -- Original normal binding predicate, unchanged.
    raise exception using errcode='23514',
      message='BPAY_NEXT_DISCARD_PREPARE_BINDING_INVALID';
  end if;
end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARATION_DISCARD');
  insert into private.bpay_next_preparation_discard_request(command_id,run_id,original_prepare_command_id,
    captured_selection_state,reason,original_removed_count,selection_count,selection_page_count,selection_review_revision,expected_candidate_count,
    initiator_kind,actor_user_id,accepted_at_utc,status,finished_at_utc)
    values(p_command_id,p_run_id,v_prepare,v_run.selection_state,p_reason,v_run.cancelled_candidate_count,v_run.selection_count,v_run.selection_page_count,v_run.review_revision,
      v_run.selected_candidate_count,'OFFICE',p_actor_user_id,v_now,
      case when v_run.selected_candidate_count=0 then 'DISCARDED' else 'CANCELLING' end,
      case when v_run.selected_candidate_count=0 then v_now else null end);
  update private.bpay_next_pay_run set status=case when selected_candidate_count=0 then 'CANCELLED' else 'CANCELLING' end,
    review_revision=review_revision+1 where id=p_run_id;
  update private.bpay_next_command set expected_member_count=v_run.selected_candidate_count,
    status=case when v_run.selected_candidate_count=0 then 'COMPLETE' else 'ENROLLING' end,
    sealed_at_utc=case when v_run.selected_candidate_count=0 then v_now else null end where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
    'phase',case when v_run.selected_candidate_count=0 then 'DISCARDED' else 'CANCELLING' end,
    'expected_candidates',v_run.selected_candidate_count::text,'completed_candidates','0','removed_candidates','0','already_removed_candidates','0','blocked_candidates','0','replay',false);
end
$function$;

create or replace function private.bpay_next_enroll_preparation_discard_one_v1(p_command_id uuid)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;v_already_released boolean:=false;v_active private.bpay_next_job%rowtype;
 v_run_id uuid;v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_discard_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_original private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_candidate uuid;v_member bigint;v_job uuid;v_state uuid;
begin
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id into strict v_run_id from private.bpay_next_preparation_discard_request where command_id=p_command_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for share nowait;
  select * into strict v_request from private.bpay_next_preparation_discard_request where command_id=p_command_id for update;
  select * into strict v_command from private.bpay_next_command where id=p_command_id for update;
  if v_command.command_kind<>'PREPARATION_DISCARD' or v_command.module_epoch<>v_epoch
     or v_command.expected_member_count<>v_request.expected_candidate_count
     or v_command.enrolled_member_count<>v_request.enrolled_candidate_count
     or v_run.confirmed_at_utc is not null or v_run.selection_state<>v_request.captured_selection_state
     or v_run.selection_count<>v_request.selection_count or v_run.selection_page_count<>v_request.selection_page_count
     or v_run.selected_candidate_count<>v_request.expected_candidate_count
     or v_run.review_revision<>v_request.selection_review_revision+1 then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_ENROLLMENT_SCOPE_INVALID';end if;
  if v_request.expected_candidate_count=0 then
    if v_run.status<>'CANCELLED' or v_request.status<>'DISCARDED' or v_command.status<>'COMPLETE' then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_EMPTY_NOT_COMPLETE';end if;
    return null;
  end if;
  if v_run.status<>'CANCELLING' or v_request.status<>'CANCELLING' or v_command.status<>'ENROLLING'
     or v_request.enrolled_candidate_count>=v_request.expected_candidate_count then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_NOT_ENROLLABLE';end if;
  v_member:=v_request.enrolled_candidate_count+1;
  select candidate_id into strict v_candidate from private.bpay_next_selection_candidate where run_id=v_run_id and member_no=v_member;
if v_request.original_prepare_command_id is not null then
  v_original_unenrolled:=
    v_run.selection_state='OPEN'
    and v_run.accepted_choice_descriptor is not null
    and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
    and private.bpay_next_preparation_discard_fenced_v1(
      v_run_id,v_request.original_prepare_command_id)
    and exists(select 1 from private.bpay_next_command c
      where c.id=v_request.original_prepare_command_id
        and c.command_kind='PREPARE' and c.module_epoch=v_epoch
        and c.status='COMPLETE' and c.expected_member_count=0
        and c.enrolled_member_count=0 and c.sealed_at_utc is not null)
    and not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_job j
      where j.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_run_worker rw
      where rw.run_id=v_run_id);
  if not coalesce(v_original_unenrolled,false) then
    -- Complete original ordinary-job retirement block follows unchanged.
    select original_job.* into strict v_original from private.bpay_next_job original_job
      where command_id=v_request.original_prepare_command_id
        and candidate_id=v_candidate and job_kind='PREPARE' for update;
    if v_original.module_epoch<>v_epoch then
      raise exception using errcode='23514',
        message='BPAY_NEXT_DISCARD_ORIGINAL_JOB_INVALID';
    end if;
    if v_original.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='DISCARDED',
        lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_DISCARDED' where id=v_original.id;
    end if;
  end if;
end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run_id and candidate_id=v_candidate;
  if v_worker.id is not null then
    v_already_released:=private.bpay_next_worker_release_complete_v1(v_worker.id,v_request.accepted_at_utc);
    if v_original.id is null or v_worker.status='CANCELLING' then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_WORKER_NOT_PREPARATION';end if;
    select id into v_state from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
      and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
      and pass_kind='DRAFT' and projection_no=0;
  end if;
  if v_worker.id is not null and v_worker.active_prepare_job_id is not null and v_worker.active_prepare_job_id<>v_original.id then
    select j.* into strict v_active from private.bpay_next_job j where j.id=v_worker.active_prepare_job_id for update;
    if not exists(select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=v_active.id
      and x.command_id=v_active.command_id and x.run_worker_id=v_worker.id and x.run_id=v_run_id and x.candidate_id=v_candidate
      and x.original_prepare_command_id=v_request.original_prepare_command_id and x.input_prepare_job_id=v_original.id
      and x.module_epoch=v_epoch and x.target_preparation_revision=v_worker.preparation_revision
      and x.captured_preparation_revision=v_worker.captured_preparation_revision and x.selection_revision=v_worker.case_selection_revision
      and (x.action='REPREPARE' or (x.action='RELEASE' and v_already_released and x.status='COMPLETE')))
      or v_active.candidate_id<>v_candidate or v_active.module_epoch<>v_epoch or v_active.job_kind<>'PREPARE' then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_ACTIVE_RETRY_INVALID';end if;
    if v_active.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='DISCARDED',lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_DISCARDED' where id=v_active.id;
    end if;
  end if;
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,v_member);
  insert into private.bpay_next_job(command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
    values(p_command_id,v_command.agency_sequence,v_epoch,v_candidate,'PREPARATION_DISCARD','READY','NEW',1) returning id into v_job;
  insert into private.bpay_next_preparation_discard_worker(command_id,candidate_id,run_id,member_no,job_id,run_worker_id,
    original_job_id,original_job_status,original_job_phase,original_job_cursor,original_position_work_cursor,
    original_position_component_cursor,original_applied_line_count,original_worker_status,original_review_issue_code,
    original_review_issue_work_id,preparation_revision,selection_revision,draft_state_id,expected_work_count,expected_case_hold_count,status,stage,expected_execution_revision,already_worker_released)
    values(p_command_id,v_candidate,v_run_id,v_member,v_job,v_worker.id,v_original.id,v_original.status,v_original.phase,v_original.cursor_key,
      v_original.position_work_cursor,v_original.position_component_cursor,v_original.applied_line_count,v_worker.status,
      v_worker.review_issue_code,v_worker.review_issue_work_id,v_worker.captured_preparation_revision,v_worker.case_selection_revision,v_state,
      case when v_already_released then 0 else coalesce(v_worker.captured_line_count,0) end,
      case when v_already_released then 0 else coalesce(v_worker.active_case_hold_count,0) end,'READY','NEW',v_worker.preparation_revision,v_already_released);
  update private.bpay_next_preparation_discard_request set enrolled_candidate_count=v_member where command_id=p_command_id;
  update private.bpay_next_command set enrolled_member_count=v_member,
    status=case when v_member=expected_member_count then 'SEALED' else 'ENROLLING' end,
    sealed_at_utc=case when v_member=expected_member_count then pg_catalog.clock_timestamp() else null end where id=p_command_id;
  return v_job;
end
$function$;

create or replace function private.bpay_next_claim_preparation_discard_job_v1(p_job_id uuid,p_lease_seconds integer default 120)
returns table(lease_nonce uuid,owner_epoch bigint) language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_candidate uuid;v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;v_nonce uuid;v_owner bigint;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_DISCARD_LEASE_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_discard_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_job.job_kind<>'PREPARATION_DISCARD'
     or v_job.module_epoch<>v_epoch or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE';end if;
  if exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1,updated_at_utc=pg_catalog.clock_timestamp()
    where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,lease_nonce=v_nonce,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1 where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner;return next;
end
$function$;

create or replace function private.bpay_next_discard_preparation_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_checkpoint bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_candidate uuid;v_seen integer:=0;v_work_holds integer:=0;v_case_released integer:=0;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_binding private.bpay_next_preparation_discard_worker%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_hold private.bpay_next_hold%rowtype;
  v_case_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_amount numeric;v_now timestamptz;v_checkpoint bigint;
  v_ids uuid[];v_work_ids uuid[];v_case_ids uuid[];v_lock_cases uuid[];v_limit integer;v_cursor bigint;v_revision bigint;v_id uuid;
  v_task private.bpay_next_workbench_task%rowtype;v_work private.bpay_next_work%rowtype;
  v_wait boolean:=false;v_financial_changed boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or (p_expected_checkpoint is not null and p_expected_checkpoint<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_DISCARD_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_discard_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_binding from private.bpay_next_preparation_discard_worker where job_id=p_job_id for update;
  if v_binding.run_worker_id is not null then select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;end if;
  if v_job.job_kind<>'PREPARATION_DISCARD' or v_job.module_epoch<>v_epoch or v_job.command_id<>v_binding.command_id
     or v_job.candidate_id<>v_candidate or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command c join private.bpay_next_preparation_discard_request x on x.command_id=c.id
       where c.id=v_job.command_id and c.command_kind='PREPARATION_DISCARD' and c.agency_sequence=v_job.command_sequence
         and c.module_epoch=v_epoch and x.run_id=v_run_id and x.captured_selection_state=v_run.selection_state
         and x.expected_candidate_count=c.expected_member_count and x.enrolled_candidate_count=c.enrolled_member_count
         and v_run.review_revision=x.selection_review_revision+1) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_PAGE_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase in ('DISCARDED','BLOCKED') and v_binding.status in ('DISCARDED','BLOCKED') and v_binding.stage='COMPLETE' then
    return pg_catalog.jsonb_build_object('phase',v_job.phase,'cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_job.status<>'LEASED'
     or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_checkpoint,0)<>v_binding.checkpoint then
    if coalesce(p_expected_checkpoint,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_DISCARD_CURSOR_AHEAD';end if;
    return pg_catalog.jsonb_build_object('phase','RELEASE','cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  -- Never put completion before its accepted receipt, including labelled
  -- owner-only simulated-clock vectors. Public receipts use the real clock.
  v_now:=greatest(pg_catalog.clock_timestamp(),(select accepted_at_utc from private.bpay_next_preparation_discard_request where command_id=v_binding.command_id));
  if v_binding.status='READY' and v_worker.id is not null and not v_binding.already_worker_released
    and(v_worker.status not in ('PREPARING','REVIEW','READY') or v_worker.realised_effect_count<>0
      or v_worker.net_request_revision<>0 or v_worker.net_projection_revision<>0
      or exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id)
      or exists(select 1 from private.bpay_next_net_projection where run_worker_id=v_worker.id)) then
    if(v_worker.status,v_worker.captured_line_count,v_worker.active_case_hold_count,v_worker.captured_preparation_revision,
        v_worker.preparation_revision,v_worker.case_selection_revision)
      is distinct from(v_binding.original_worker_status,v_binding.expected_work_count,v_binding.expected_case_hold_count,
        v_binding.preparation_revision,v_binding.expected_execution_revision,v_binding.selection_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_BLOCKED_IDENTITY_CHANGED';end if;
    update private.bpay_next_preparation_discard_worker set status='BLOCKED',stage='COMPLETE',outcome='BLOCKED',
      blocked_code=case when v_worker.status not in ('PREPARING','REVIEW','READY') then 'PREPARATION_STATE_PROTECTED' else 'PAYMENT_ALREADY_PROTECTED' end,
      started_at_utc=v_now,finished_at_utc=v_now,checkpoint=checkpoint+1 where job_id=p_job_id returning checkpoint into v_checkpoint;
    update private.bpay_next_job set status='DONE',phase='BLOCKED',cursor_key=v_checkpoint::text,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    perform private.bpay_next_finish_discard_member_v1(p_job_id);
    return pg_catalog.jsonb_build_object('phase','BLOCKED','cursor',v_checkpoint::text,'rows_visited','1',
      'work_released','0','case_holds_released','0','run_worker_id',v_binding.run_worker_id,'replay',false);
  end if;
  if v_binding.status='READY' then
    if v_job.phase<>'NEW' or v_binding.stage<>'NEW' then raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_START_INVALID';end if;
    if v_worker.id is not null and not v_binding.already_worker_released then
      if v_worker.status<>v_binding.original_worker_status or v_worker.captured_line_count<>v_binding.expected_work_count
         or v_worker.active_case_hold_count<>v_binding.expected_case_hold_count or v_worker.captured_preparation_revision<>v_binding.preparation_revision
         or v_worker.preparation_revision<>coalesce(v_binding.expected_execution_revision,v_binding.preparation_revision)
         or v_worker.case_selection_revision<>v_binding.selection_revision or v_worker.realised_effect_count<>0
         or v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
         or exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id)
         or exists(select 1 from private.bpay_next_net_projection where run_worker_id=v_worker.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_GROUP_NOT_UNCONFIRMED';end if;
      -- The original issue is immutable in this exact expiry binding. Clear
      -- only the current status diagnostic to satisfy the existing REVIEW CK.
      -- Financial resolution counts, captured money and evidence stay intact.
      update private.bpay_next_run_worker set status='CANCELLING',review_issue_code=null,review_issue_work_id=null where id=v_worker.id;
      v_worker.status:='CANCELLING';
    end if;
    v_binding.status:='RELEASING';v_binding.stage:='WORK';v_binding.started_at_utc:=v_now;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
  end if;
  if v_binding.status<>'RELEASING' or (not v_binding.already_worker_released and v_worker.id is not null and (v_worker.status<>'CANCELLING'
      or v_worker.realised_effect_count<>0 or v_worker.captured_line_count<>v_binding.expected_work_count)) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_FENCE_BROKEN';end if;
  -- Every retained page's existing ORIGIN writer must finish before another
  -- expiry page changes the Candidate financial revision or claims completion.
  perform 1 from private.bpay_next_work where id=any(v_binding.pending_work_ids) order by id for update;
  for v_id in select unnest(v_binding.pending_work_ids) order by 1 loop
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_id and w.candidate_id=v_candidate;
    select t.* into strict v_task from private.bpay_next_workbench_task t where t.task_key='ORIGIN:'||v_id::text;
    if v_task.task_type<>'ORIGIN' or v_task.work_id is distinct from v_id or v_task.candidate_id is distinct from v_candidate then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_ORIGIN_SCOPE_INVALID';end if;
    if v_task.status='DONE' and(v_task.module_epoch,v_task.expected_revision_id,v_task.expected_applied_revision_id,
        v_task.expected_approval_state,v_task.financial_view_revision,v_task.expected_wait_state)
      is distinct from(v_epoch,v_work.current_revision_id,v_work.applied_revision_id,v_work.approval_state,
        v_control.financial_view_revision,private.bpay_next_work_wait_state_v1(v_id)) then
      perform private.bpay_next_workbench_queue_origin_v1(v_id);
    end if;
  end loop;
  v_wait:=exists(select 1 from unnest(v_binding.pending_work_ids) x(id)
    join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE');
  -- Clear a completed retained set in its own checkpoint. The next page may
  -- revisit the same origin and enqueue a new generation; its guard must not
  -- confuse that new task with this already-completed page.
  if not v_wait and v_binding.pending_work_ids is not null then
    v_binding.pending_work_ids:=null;v_wait:=true;
  end if;
  if not v_wait then
    v_binding.pending_work_ids:=null;
    if v_binding.already_worker_released and
      (not private.bpay_next_worker_release_complete_v1(v_binding.run_worker_id,
        (select accepted_at_utc from private.bpay_next_preparation_discard_request where command_id=v_binding.command_id))
       or v_binding.expected_work_count<>0 or v_binding.expected_case_hold_count<>0) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_RELEASE_RECEIPT_INVALID';end if;
    if v_binding.stage='WORK' then
      select array_agg(x.id order by x.line_no,x.id),array_agg(distinct x.work_id) into v_ids,v_work_ids
        from(select id,work_id,line_no from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id
          and line_no>coalesce(v_binding.work_cursor,0) and not v_binding.already_worker_released
          order by line_no,id limit p_limit) x;
      perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
      for v_line in select * from private.bpay_next_run_line where id=any(v_ids) order by line_no,id loop
        if private.bpay_next_release_prepare_work_hold_v1(p_job_id,v_line.id,v_now) then v_work_holds:=v_work_holds+1;end if;
        v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
      end loop;
      v_financial_changed:=v_seen>0;
      select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id
        and line_no>coalesce(v_binding.work_cursor,0) and not v_binding.already_worker_released) into v_more;
      if not v_more then
        if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_WORK_COUNT_INVALID';end if;
        v_binding.stage:='CASE';end if;
    elsif v_binding.stage='CASE' then
      v_limit:=least(p_limit,50);
      select array_agg(x.id order by x.id),array_agg(distinct x.case_id),array_agg(distinct wc.work_id) filter(where wc.work_id is not null)
        into v_ids,v_case_ids,v_work_ids from(select id,case_id,instruction_id from private.bpay_next_case_hold
          where run_worker_id=v_binding.run_worker_id and status='ACTIVE'
            and(v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit v_limit) x
        join private.bpay_next_run_case_instruction i on i.id=x.instruction_id and i.candidate_id=v_candidate
        left join private.bpay_next_work_collection wc on wc.id=i.work_collection_id and wc.candidate_id=v_candidate and wc.case_id=x.case_id;
      -- Mirror refresh_many's bounded related-Case preholds, before the first
      -- Case/period/hold mutation. Collection reconciliation sees held Work.
      select array_agg(x.id order by x.id) into v_lock_cases from(select distinct id from unnest(v_case_ids) id
        union select wc.case_id from private.bpay_next_work_collection wc where wc.work_id=any(v_work_ids) and wc.case_id is not null
        order by id limit 101) x;
      if coalesce(cardinality(v_work_ids),0)+coalesce(cardinality(v_lock_cases),0)>100 then v_lock_cases:=v_case_ids;end if;
      perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
      perform 1 from private.bpay_next_finance_case where id=any(v_lock_cases) order by id for update;
      for v_case_hold in select * from private.bpay_next_case_hold where id=any(v_ids) order by id loop
        perform private.bpay_next_release_prepare_case_hold_v1(p_job_id,v_case_hold.id,v_now);
        v_binding.case_hold_cursor:=v_case_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;
        v_seen:=v_seen+1;v_case_released:=v_case_released+1;
      end loop;
      v_financial_changed:=v_seen>0;
      select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE') into v_more;
      if not v_more then
        if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
          or(v_worker.id is not null and(select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0) then
          raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_CASE_COUNT_INVALID';end if;
        v_binding.stage:='PROJECT_WORK';
      elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_CASE_CURSOR_INVALID';end if;
    elsif v_binding.stage='PROJECT_WORK' then
      select array_agg(x.work_id order by x.selection_no),count(*)::integer,max(x.selection_no::bigint) into v_work_ids,v_seen,v_cursor
        from(select work_id,selection_no from private.bpay_next_run_selection
          where run_id=v_run_id and candidate_id=v_candidate and selection_no>coalesce(v_binding.projection_work_cursor,0)
          order by selection_no limit p_limit) x;
      if v_seen>0 then v_binding.projection_work_cursor:=v_cursor;end if;
      if v_seen<p_limit then v_binding.stage:='PROJECT_CASE';end if;
    elsif v_binding.stage='PROJECT_CASE' then
      v_limit:=least(p_limit,50);v_revision:=nullif(v_binding.selection_revision,0);
      if v_revision is null then
        select selection_revision into v_revision from private.bpay_next_case_selection where run_id=v_run_id and candidate_id=v_candidate
          and status='SEALED' order by selection_revision desc limit 1;
      elsif not exists(select 1 from private.bpay_next_case_selection where run_id=v_run_id and candidate_id=v_candidate
          and selection_revision=v_revision and status='SEALED') then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_REQUIRED';end if;
      select array_agg(distinct x.case_id),array_agg(distinct wc.work_id) filter(where wc.work_id is not null),count(*)::integer,max(x.selection_no::bigint)
        into v_case_ids,v_work_ids,v_seen,v_cursor from(select case_id,case_component_id,selection_no from private.bpay_next_case_selection_item
          where run_id=v_run_id and candidate_id=v_candidate and selection_revision=v_revision and is_selected
            and selection_no>coalesce(v_binding.projection_case_cursor,0) order by selection_no limit v_limit) x
        left join private.bpay_next_work_collection wc on wc.case_component_id=x.case_component_id and wc.case_id=x.case_id and wc.candidate_id=v_candidate;
      if v_seen>0 then v_binding.projection_case_cursor:=v_cursor;end if;
      if v_seen<v_limit then v_binding.stage:='FINAL';end if;
    elsif v_binding.stage='FINAL' then
      v_revision:=nullif(v_binding.selection_revision,0);
      if v_revision is null then select selection_revision into v_revision from private.bpay_next_case_selection
        where run_id=v_run_id and candidate_id=v_candidate and status='SEALED' order by selection_revision desc limit 1;end if;
      if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
        or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE')
        or exists(select 1 from private.bpay_next_run_selection where run_id=v_run_id and candidate_id=v_candidate
          and selection_no>coalesce(v_binding.projection_work_cursor,0))
        or exists(select 1 from private.bpay_next_case_selection_item where run_id=v_run_id and candidate_id=v_candidate
          and selection_revision=v_revision and is_selected and selection_no>coalesce(v_binding.projection_case_cursor,0)) then
        raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_FINAL_COUNT_INVALID';end if;
      if v_worker.id is not null and not v_binding.already_worker_released then update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;end if;
      -- Preserve the original final financial-view invalidation for native
      -- worker-week exclusion. All origin projection has finished; this is
      -- not a wait/selection page and changes no Ready offer inputs.
      v_financial_changed:=not v_binding.already_worker_released;
      v_binding.stage:='COMPLETE';v_binding.status:='DISCARDED';v_binding.outcome:=case when v_binding.already_worker_released then 'ALREADY_REMOVED' else 'REMOVED' end;v_binding.finished_at_utc:=v_now;v_seen:=1;
    else raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_STAGE_INVALID';end if;
    if v_financial_changed then
      update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
    end if;
    if coalesce(cardinality(v_work_ids),0)+coalesce(cardinality(v_case_ids),0)>0 then
      perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      select array_agg(x.id order by x.id) into v_binding.pending_work_ids from unnest(v_work_ids) x(id)
        join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE';
    end if;
  end if;
  update private.bpay_next_preparation_discard_worker set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,started_at_utc=v_binding.started_at_utc,
    finished_at_utc=v_binding.finished_at_utc,projection_work_cursor=v_binding.projection_work_cursor,
    projection_case_cursor=v_binding.projection_case_cursor,pending_work_ids=v_binding.pending_work_ids,outcome=v_binding.outcome
    where job_id=p_job_id returning checkpoint into v_checkpoint;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+interval '120 seconds' where id=p_job_id;
  if v_binding.status='DISCARDED' then
    if not v_binding.already_worker_released then perform private.bpay_next_exclude_preparation_week_v1(p_job_id,v_binding.run_worker_id);end if;
    update private.bpay_next_job set status='DONE',phase='DISCARDED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    if not v_binding.already_worker_released then
      update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1,
          preparing_candidate_count=preparing_candidate_count-case when v_binding.original_worker_status='PREPARING' then 1 else 0 end,
          ready_candidate_count=ready_candidate_count-case when v_binding.original_worker_status='READY' then 1 else 0 end,
          review_candidate_count=review_candidate_count-case when v_binding.original_worker_status='REVIEW' then 1 else 0 end
        where id=v_run_id and cancelled_candidate_count<selected_candidate_count;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_RUN_COUNT_INVALID';end if;
    end if;
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=v_now
      where run_worker_id=v_binding.run_worker_id and target_preparation_revision=v_binding.expected_execution_revision
        and action='REPREPARE' and status<>'COMPLETE';
    perform private.bpay_next_finish_discard_member_v1(p_job_id);
  end if;
  return pg_catalog.jsonb_build_object('phase',case when v_binding.status='DISCARDED' then 'DISCARDED' else 'RELEASE' end,
    'cursor',v_checkpoint::text,'rows_visited',v_seen::text,'work_released',v_work_holds::text,
    'case_holds_released',v_case_released::text,'run_worker_id',v_binding.run_worker_id,'replay',false);
end
$function$;

create or replace function private.bpay_next_discarded_work_collection_scope_v1(
  p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null
) returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_preparation_discard_worker x on x.job_id=j.id and x.command_id=j.command_id and x.candidate_id=j.candidate_id
    join private.bpay_next_preparation_discard_request q on q.command_id=x.command_id and q.run_id=x.run_id
    join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_DISCARD'
      and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
    join private.bpay_next_job original on original.id=x.original_job_id and original.command_id=q.original_prepare_command_id
      and original.candidate_id=x.candidate_id and original.module_epoch=j.module_epoch and original.job_kind='PREPARE' and original.status='DONE'
    join private.bpay_next_pay_run r on r.id=x.run_id
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.preparation_revision=x.preparation_revision
      and i.case_component_id=b.case_component_id and i.case_id=b.case_id and i.work_collection_id=b.id
      and i.selection_revision=x.selection_revision and i.valuation_policy_id=b.original_valuation_policy_id
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.candidate_id=x.candidate_id and s.preparation_revision=x.preparation_revision and s.selection_revision=x.selection_revision
      and private.bpay_next_prepare_job_run_v1(s.job_id)=x.run_id and s.pass_kind='DRAFT' and s.projection_no=0
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id
      and h.run_worker_id=w.id and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and j.job_kind='PREPARATION_DISCARD' and j.status='LEASED' and j.phase='RELEASE'
      and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      and x.status='RELEASING' and x.stage='CASE' and x.expected_case_hold_count>0
      and x.released_case_hold_count<x.expected_case_hold_count
      and q.status='CANCELLING' and q.captured_selection_state=r.selection_state
      and r.status='CANCELLING' and r.confirmed_at_utc is null and r.review_revision=q.selection_review_revision+1
      and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
        is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
      and (command.expected_member_count,command.enrolled_member_count)
        is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
      and q.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=r.id)
      and not x.already_worker_released and w.status='CANCELLING' and w.captured_preparation_revision=x.preparation_revision
      and w.preparation_revision=coalesce(x.expected_execution_revision,x.preparation_revision) and w.case_selection_revision=x.selection_revision
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and i.case_kind='OVERPAYMENT' and i.tax_treatment='TAXABLE' and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and i.payroll_stage='GROSS_DEDUCT' and i.hold_purpose='GROSS_RECOVERY'
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=q.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
        and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE'))
$function$;

create or replace function private.bpay_next_reconcile_discarded_work_collection_v1(p_job_id uuid,p_case_hold_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into strict v_basis from private.bpay_next_case_hold h
    join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
    join private.bpay_next_work_collection b on b.id=i.work_collection_id
      and b.case_id=h.case_id and b.case_component_id=h.case_component_id and b.candidate_id=h.candidate_id
    where h.id=p_case_hold_id;
  if not private.bpay_next_discarded_work_collection_scope_v1(p_job_id,v_basis.id,p_case_hold_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';end if;
  -- No financial disposition: the existing liability formula sees the exact
  -- released H and the maintained applied A/R/W. It never changes R, frozen
  -- evidence or an original cash receipt.1519 must explicitly admit this owner.
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,null);
end
$function$;

create or replace function private.bpay_next_discarded_prepare_receipt_v1(p_job_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_request record;v_worker private.bpay_next_run_worker%rowtype;
  v_binding private.bpay_next_preparation_discard_worker%rowtype;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id and job_kind='PREPARE';
  select r.* into strict v_run from private.bpay_next_pay_run r join private.bpay_next_run_command rc on rc.run_id=r.id
    where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
  if v_job.status='DONE' and v_job.phase='DISCARDED' and exists(select 1 from private.bpay_next_worker_reprepare_request x
      join private.bpay_next_command c on c.id=x.command_id and c.command_kind='PREPARE' and c.module_epoch=x.module_epoch
      join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id
        and w.preparation_revision>=x.target_preparation_revision and w.captured_preparation_revision=x.captured_preparation_revision
      join private.bpay_next_job input_job on input_job.id=x.input_prepare_job_id and input_job.command_id=x.original_prepare_command_id
        and input_job.candidate_id=x.candidate_id and input_job.module_epoch=x.module_epoch
      join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=x.module_epoch
      where x.previous_job_id=p_job_id and x.run_id=v_run.id and x.candidate_id=v_job.candidate_id
        and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'
        and x.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=v_run.id)) then
    select * into strict v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','DISCARDED','cursor',v_job.cursor_key,
      'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
      'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id);
  end if;
  select command_id,original_prepare_command_id,captured_selection_state into strict v_request from private.bpay_next_preparation_discard_request where run_id=v_run.id
    and original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=v_run.id);
  if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DISCARDING';end if;
  if v_run.status not in ('CANCELLING','CANCELLED','REVIEW') or v_run.confirmed_at_utc is not null
     or v_request.captured_selection_state<>v_run.selection_state
     or not private.bpay_next_preparation_discard_fenced_v1(v_run.id,v_request.original_prepare_command_id)
     or v_job.phase not in ('DISCARDED','READY','DONE','CANCELLED')
     or not exists(select 1 from private.bpay_next_command where id=v_request.command_id
       and command_kind='PREPARATION_DISCARD' and module_epoch=v_job.module_epoch)
     or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' and owner_epoch=v_job.module_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARDED_PREPARE_RECEIPT_SCOPE_INVALID';end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;
  select * into v_binding from private.bpay_next_preparation_discard_worker where command_id=v_request.command_id and candidate_id=v_job.candidate_id;
  if v_binding.job_id is not null and v_binding.original_job_id is distinct from v_job.id and not exists(
    select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=p_job_id and x.command_id=v_job.command_id
      and x.run_worker_id=v_binding.run_worker_id and x.run_id=v_run.id and x.candidate_id=v_job.candidate_id
      and x.input_prepare_job_id=v_binding.original_job_id and x.original_prepare_command_id=v_request.original_prepare_command_id
      and x.module_epoch=v_job.module_epoch and x.target_preparation_revision=v_binding.expected_execution_revision
      and x.captured_preparation_revision=v_binding.preparation_revision and x.selection_revision=v_binding.selection_revision) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARDED_PREPARE_RECEIPT_JOB_MISMATCH';end if;
  return pg_catalog.jsonb_build_object('done',true,'phase','DISCARDED','cursor',v_job.cursor_key,
    'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
    'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
    'issue_code',coalesce(v_worker.review_issue_code,v_binding.original_review_issue_code),
    'issue_work_id',coalesce(v_worker.review_issue_work_id,v_binding.original_review_issue_work_id));
end
$function$;

create or replace function private.bpay_next_prepare_release_scope_v1(p_job_id uuid,p_run_worker_id uuid,p_stage text)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_run_worker w on w.id=p_run_worker_id and w.candidate_id=j.candidate_id
    join private.bpay_next_pay_run r on r.id=w.run_id and r.confirmed_at_utc is null
    where j.id=p_job_id and j.status='LEASED' and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and w.status='CANCELLING' and w.realised_effect_count=0 and w.net_projection_revision=0 and w.net_request_revision=0
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
        and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE')
      and ((j.job_kind='PREPARE' and r.status='PREPARING' and private.bpay_next_prepare_job_active_v1(j.id)
        and exists(select 1 from private.bpay_next_worker_reprepare_request x
          where x.job_id=j.id and x.run_worker_id=w.id and x.run_id=r.id and x.action='RELEASE'
            and x.status='RUNNING' and x.target_preparation_revision=w.preparation_revision
            and x.captured_preparation_revision=w.captured_preparation_revision
            and x.selection_revision=w.case_selection_revision and x.stage=p_stage
            and j.phase=case p_stage when 'WORK' then 'WORKER_RELEASE_WORK' when 'CASE' then 'WORKER_RELEASE_CASE' else null end
            and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text))
        or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and r.status='CANCELLING'
          and exists(select 1 from private.bpay_next_preparation_expiry_worker x
            join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
            join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_EXPIRY'
              and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
            where x.job_id=j.id and x.run_worker_id=w.id and x.command_id=j.command_id and x.run_id=r.id
              and not x.already_worker_released and x.preparation_revision=w.captured_preparation_revision
              and coalesce(x.expected_execution_revision,x.preparation_revision)=w.preparation_revision
              and x.selection_revision=w.case_selection_revision and q.status='CANCELLING'
              and q.accepted_at_utc>=q.captured_deadline and q.captured_deadline=r.preparation_expires_at_utc
              and r.preparation_expires_at_utc=r.created_at_utc+interval '72 hours'
              and r.review_revision=q.selection_review_revision+1
              and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
                is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
              and (command.expected_member_count,command.enrolled_member_count)
                is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
              and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
              and ((x.status='RELEASING' and x.stage=p_stage) or (p_stage='WORK' and x.status='READY' and x.stage='NEW'))))        or (j.job_kind='PREPARATION_DISCARD' and j.phase='RELEASE' and r.status='CANCELLING'
          and exists(select 1 from private.bpay_next_preparation_discard_worker x
            join private.bpay_next_preparation_discard_request q on q.command_id=x.command_id and q.run_id=x.run_id
            join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_DISCARD'
              and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
            where x.job_id=j.id and x.run_worker_id=w.id and x.command_id=j.command_id and x.run_id=r.id
              and not x.already_worker_released and x.preparation_revision=w.captured_preparation_revision
              and coalesce(x.expected_execution_revision,x.preparation_revision)=w.preparation_revision
              and x.selection_revision=w.case_selection_revision and q.status='CANCELLING'
              and q.captured_selection_state=r.selection_state
              and r.review_revision=q.selection_review_revision+1
              and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
                is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
              and (command.expected_member_count,command.enrolled_member_count)
                is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
              and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
              and ((x.status='RELEASING' and x.stage=p_stage) or (p_stage='WORK' and x.status='READY' and x.stage='NEW'))))))
$function$;

create or replace function private.bpay_next_release_prepare_case_hold_v1(p_job_id uuid,p_case_hold_id uuid,p_now timestamptz)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_case_hold record;v_instruction record;v_result record;v_use record;v_binding record;v_worker record;
  v_candidate uuid;v_kind text;v_amount numeric;v_now timestamptz:=p_now;
begin
  select id,run_worker_id,instruction_id,allocation_result_id,case_id,case_component_id,candidate_id,
    pay_week_start,status,allocation_pass_kind,projection_id,purpose,source_reserved_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat
    into strict v_case_hold from private.bpay_next_case_hold where id=p_case_hold_id;
  if v_case_hold.status<>'ACTIVE' or p_now is null or not pg_catalog.isfinite(p_now)
    or not private.bpay_next_prepare_release_scope_v1(p_job_id,v_case_hold.run_worker_id,'CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_RELEASE_SCOPE_INVALID';end if;
  select job_kind,candidate_id into strict v_kind,v_candidate from private.bpay_next_job where id=p_job_id;
  select id into strict v_worker from private.bpay_next_run_worker where id=v_case_hold.run_worker_id;
  if v_kind='PREPARATION_EXPIRY' then
    select run_worker_id,preparation_revision,selection_revision,draft_state_id into strict v_binding
      from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  elsif v_kind='PREPARATION_DISCARD' then
    select run_worker_id,preparation_revision,selection_revision,draft_state_id into strict v_binding
      from private.bpay_next_preparation_discard_worker where job_id=p_job_id;
  else
    select x.run_worker_id,x.captured_preparation_revision as preparation_revision,x.selection_revision,x.draft_state_id
      into strict v_binding from private.bpay_next_worker_reprepare_request x
      join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
      where x.job_id=p_job_id and x.action='RELEASE' and x.status='RUNNING';
  end if;
  select id,run_worker_id,preparation_revision,selection_revision,work_collection_id into strict v_instruction from private.bpay_next_run_case_instruction where id=v_case_hold.instruction_id;
  select pass_kind,instruction_id,state_id,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat into strict v_result from private.bpay_next_case_allocation_result where id=v_case_hold.allocation_result_id;
  perform 1 from private.bpay_next_finance_case where id=v_case_hold.case_id for update;
  perform 1 from private.bpay_next_case_component where id=v_case_hold.case_component_id for update;
  perform 1 from private.bpay_next_case_period where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start for update;
  perform 1 from private.bpay_next_case_hold where id=v_case_hold.id for update;
  select status,realisation_event_id,source_amount_ex_vat into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_case_hold.id for update;
  if v_instruction.run_worker_id<>v_binding.run_worker_id or v_instruction.preparation_revision<>v_binding.preparation_revision
     or v_instruction.selection_revision<>v_binding.selection_revision or v_case_hold.candidate_id<>v_candidate
     or v_case_hold.allocation_pass_kind<>'DRAFT' or v_result.pass_kind<>'DRAFT'
     or v_result.instruction_id<>v_instruction.id or v_result.state_id is distinct from v_binding.draft_state_id
     or v_case_hold.projection_id is not null or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
     or v_use.source_amount_ex_vat<>v_case_hold.source_reserved_ex_vat
     or (v_case_hold.source_reserved_ex_vat,v_case_hold.target_amount_ex_vat,v_case_hold.target_amount_vat,v_case_hold.target_amount_inc_vat)
       is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_NOT_EXACT';end if;
  v_amount:=v_case_hold.source_reserved_ex_vat;
  if v_case_hold.purpose='PAYOUT' then
    update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_payout_source_ex_vat>=v_amount;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
    update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
  else
    update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_recovery_source_ex_vat>=v_amount;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
    update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
  end if;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CAPACITY_INVALID';end if;
  if v_case_hold.purpose='PAYOUT' then
    update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
  else
    update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
  end if;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_PERIOD_INVALID';end if;
  update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_case_hold.id;
  update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_case_hold.id;
  update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_COUNT_INVALID';end if;
  if v_instruction.work_collection_id is not null then
    if v_kind='PREPARATION_EXPIRY' then
      perform private.bpay_next_reconcile_expired_work_collection_v1(p_job_id,v_case_hold.id);
    elsif v_kind='PREPARATION_DISCARD' then
      perform private.bpay_next_reconcile_discarded_work_collection_v1(p_job_id,v_case_hold.id);
    else
      perform private.bpay_next_reconcile_worker_released_collection_v1(p_job_id,v_case_hold.id);
    end if;
  end if;
end
$function$;

create or replace function private.bpay_next_exclude_preparation_week_v1(p_job_id uuid,p_run_worker_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_binding record;v_run record;v_worker record;v_week record;v_prior record;
  v_kind text;v_old numeric;v_unbound bigint;
begin
  select job_kind into strict v_kind from private.bpay_next_job where id=p_job_id;
  if v_kind='PREPARATION_EXPIRY' then
    select x.run_worker_id,x.run_id,x.candidate_id,x.original_worker_status,x.preparation_revision,x.selection_revision,
      coalesce(x.expected_execution_revision,x.preparation_revision) as expected_execution_revision
      into strict v_binding from private.bpay_next_preparation_expiry_worker x
      join private.bpay_next_job j on j.id=x.job_id and j.command_id=x.command_id and j.status='LEASED' and j.phase='RELEASE'
      join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
      where j.id=p_job_id and x.status='EXPIRED' and x.stage='COMPLETE' and j.cursor_key=x.checkpoint::text
        and j.lease_until_utc>pg_catalog.clock_timestamp() and x.run_worker_id is not distinct from p_run_worker_id;
    select id,pay_date,created_at_utc,status,confirmed_at_utc into strict v_run from private.bpay_next_pay_run
      where id=v_binding.run_id and status='CANCELLING' and confirmed_at_utc is null;
  elsif v_kind='PREPARATION_DISCARD' then
    select x.run_worker_id,x.run_id,x.candidate_id,x.original_worker_status,x.preparation_revision,x.selection_revision,
      coalesce(x.expected_execution_revision,x.preparation_revision) as expected_execution_revision
      into strict v_binding from private.bpay_next_preparation_discard_worker x
      join private.bpay_next_job j on j.id=x.job_id and j.command_id=x.command_id and j.status='LEASED' and j.phase='RELEASE'
      join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
      where j.id=p_job_id and x.status='DISCARDED' and x.stage='COMPLETE' and j.cursor_key=x.checkpoint::text
        and j.lease_until_utc>pg_catalog.clock_timestamp() and x.run_worker_id is not distinct from p_run_worker_id;
    select id,pay_date,created_at_utc,status,confirmed_at_utc into strict v_run from private.bpay_next_pay_run
      where id=v_binding.run_id and status='CANCELLING' and confirmed_at_utc is null;
  elsif v_kind='PREPARE' then
    select x.run_worker_id,x.run_id,x.candidate_id,x.original_worker_status,x.captured_preparation_revision as preparation_revision,
      x.selection_revision,x.target_preparation_revision as expected_execution_revision
      into strict v_binding from private.bpay_next_worker_reprepare_request x
      join private.bpay_next_job j on j.id=x.job_id and j.command_id=x.command_id and j.status='LEASED' and j.phase='WORKER_RELEASE_FINAL'
      join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
      join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
      where j.id=p_job_id and x.action='RELEASE' and x.status='RUNNING' and x.stage='FINAL'
        and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text and j.lease_until_utc>pg_catalog.clock_timestamp()
        and x.run_worker_id=p_run_worker_id and private.bpay_next_prepare_job_active_v1(j.id);
    select id,pay_date,created_at_utc,status,confirmed_at_utc into strict v_run from private.bpay_next_pay_run
      where id=v_binding.run_id and status='PREPARING' and confirmed_at_utc is null;
  else raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_SCOPE_INVALID';end if;
  if v_binding.run_worker_id is null then return;end if;
  select id,status,realised_effect_count,active_case_hold_count,captured_preparation_revision,preparation_revision,case_selection_revision,gross_inc_vat into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id;
  if v_worker.status<>'CANCELLED' or v_worker.realised_effect_count<>0 or v_worker.active_case_hold_count<>0
     or v_worker.captured_preparation_revision<>v_binding.preparation_revision or v_worker.preparation_revision<>v_binding.expected_execution_revision or v_worker.case_selection_revision<>v_binding.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_WORKER_INVALID';end if;
  select candidate_id,pay_week_start into v_week from private.bpay_next_worker_week where candidate_id=v_binding.candidate_id
    and pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select id,candidate_id,pay_week_start,original_pay_date,original_created_at_utc,original_gross_amount,original_transfer_id,returned_cash_id,reissue_transfer_id,payment_state,eligibility_state,eligible_arranged_amount into v_prior from private.bpay_next_worker_week_contribution where original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_binding.original_worker_status='READY' and v_binding.selection_revision>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_READY_WEEK_FACT_MISSING';end if;
    -- A partial non-READY preparation never published an arrangement. The
    -- genuine no-case READY owner also intentionally preserves absence when
    -- PAYE floor input is unbound, or the target is not PAYE. Preserve that
    -- absence; no zero contribution, live-policy guess or backfill is made.
    return;
  end if;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_binding.candidate_id or v_prior.pay_week_start<>v_week.pay_week_start
     or v_prior.original_pay_date<>v_run.pay_date or v_prior.original_created_at_utc<>v_run.created_at_utc
     or v_prior.original_gross_amount<>v_worker.gross_inc_vat or v_prior.original_transfer_id is not null
     or exists(select 1 from private.bpay_next_internal_receipt where run_worker_id=v_worker.id)
     or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_FACT_NOT_EXACT';end if;
  if v_prior.payment_state='CANCELLED' and v_prior.eligibility_state='EXCLUDED' and v_prior.eligible_arranged_amount=0 then return;end if;
  if v_prior.payment_state<>'ARRANGED' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_NOT_UNPAID';end if;
  v_old:=coalesce(v_prior.eligible_arranged_amount,0);v_unbound:=case when v_prior.eligibility_state='UNBOUND' then 1 else 0 end;
  update private.bpay_next_worker_week_contribution set payment_state='CANCELLED',eligibility_state='EXCLUDED',eligible_arranged_amount=0,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old,
    unresolved_contribution_count=unresolved_contribution_count-v_unbound,period_revision=period_revision+1
    where candidate_id=v_binding.candidate_id and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old and unresolved_contribution_count>=v_unbound;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_DELTA_INVALID';end if;
end
$function$;

create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_segment_basis jsonb;v_segment_complete boolean;
  v_principal numeric;v_delta numeric;v_event text;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind='WORK'
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and v_approved.tax_treatment is not distinct from 'TAXABLE';
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARE' then
    if p_effect_id is not null or not exists(select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_worker_released_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_WORKER_RELEASE_SCOPE_INVALID';end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_DISCARD' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_discarded_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null or not v_capturable then return;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id)
      values(p_work_id,p_component_key,v_candidate,p_effect_id,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        v_approved.tax_treatment is not distinct from 'TAXABLE'
          and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not v_supported_paid then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_segment_basis:=private.bpay_next_segment_collection_basis_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_segment_complete:=coalesce((v_segment_basis->>'complete')::boolean,false);
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is distinct from 'TAXABLE' then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and not v_segment_complete and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind='WORK' and a.tax_treatment='TAXABLE'
            and (v_work.work_kind='ORDINARY'
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_position.applied_revision_id
        and v.head_id=v_revision.source_head_id and v_revision.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v.approval_revision_id
        and ((v_job.job_kind='POSITION_APPLY' and publication.command_id=v_job.command_id
              and publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and publication.status='APPLIED' and publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  if v_state='READY' then
    if v_basis.case_id is null then
      v_principal:=case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric
        else greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0) end;
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT','TAXABLE','OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT','TAXABLE',pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT','TAXABLE','RECOVERY','DEDUCTION','GROSS_DEDUCT','PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric else
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0) end;
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    segment_attribution_complete=v_segment_complete,
    observed_segment_negative_ex_vat=case when v_segment_complete then (v_segment_basis->>'negative_source_ex_vat')::numeric else 0 end,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
end
$function$;

create or replace function private.bpay_next_work_collection_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_DELETE_FORBIDDEN';
  end if;
  if tg_op='UPDATE' then
    if (new.id,new.work_id,new.component_key,new.candidate_id,new.original_effect_id,
        new.original_run_line_id,new.original_revision_id,new.original_approved_line_id,
        new.original_transfer_id,new.original_source_pay_channel,new.original_tax_treatment,
        new.original_valuation_policy_id,new.created_at_utc)
       is distinct from
       (old.id,old.work_id,old.component_key,old.candidate_id,old.original_effect_id,
        old.original_run_line_id,old.original_revision_id,old.original_approved_line_id,
        old.original_transfer_id,old.original_source_pay_channel,old.original_tax_treatment,
        old.original_valuation_policy_id,old.created_at_utc)
       or (old.case_id is not null and (new.case_id,new.case_component_id)
           is distinct from (old.case_id,old.case_component_id))
       or (not old.paid_basis_qualified and new.paid_basis_qualified)
       or new.reconcile_revision<>old.reconcile_revision+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BASIS_IMMUTABLE';
    end if;
  elsif new.reconcile_revision<>1 or new.case_id is not null
      or new.last_effect_id is distinct from new.original_effect_id then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_STATE_INVALID';
  end if;
  select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.original_run_line_id;
  if tg_op='INSERT' and not exists(select 1 from private.bpay_next_job j
      where j.id=new.last_job_id and j.command_id=v_effect.operation_id
        and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_OWNER_INVALID';
  end if;
  if v_effect.effect_kind<>'PAYROLL_SETTLED' or v_effect.source_disposition_ex_vat<=0
     or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id,v_effect.original_transfer_id)
       is distinct from (new.work_id,new.component_key,new.candidate_id,new.original_transfer_id)
     or (v_line.work_id,v_line.component_key,v_line.captured_revision_id,v_line.approved_line_id,
         v_line.source_pay_channel,v_line.valuation_policy_id)
       is distinct from (new.work_id,new.component_key,new.original_revision_id,new.original_approved_line_id,
         new.original_source_pay_channel,new.original_valuation_policy_id)
     or v_line.target_pay_channel<>'PAYE'
     or (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
       is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
     or not exists(select 1 from private.bpay_next_hold h
       where h.id=v_effect.operation_item_id and h.run_line_id=v_line.id and h.status='REALISED'
         and h.source_reserved_ex_vat=v_effect.source_disposition_ex_vat
         and h.source_reserved_ex_vat=v_line.source_consumed_ex_vat)
     or not exists(select 1 from private.bpay_next_approved_line a
       join private.bpay_next_work_revision r on r.id=a.revision_id
       join private.bpay_next_work w on w.id=r.work_id
       where a.id=new.original_approved_line_id and a.component_kind='WORK'
         and a.tax_treatment is not distinct from new.original_tax_treatment
         and w.id=new.work_id and a.component_key=new.component_key
         and ((r.source_kind='ORDINARY' and w.work_kind='ORDINARY')
           or (w.work_kind='SOURCE' and a.source_component_id is not null
             and a.component_key='SOURCE:'||a.source_component_id::text
             and ((r.source_kind='SOURCE' and r.source_event_id is not null
               and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
               and r.source_head_id is not null and r.source_event_id=r.source_head_id)))))
     or not exists(select 1 from private.bpay_next_transfer_member m
       where m.transfer_id=new.original_transfer_id and m.run_line_id=v_line.id and m.subject_kind='WORK'
         and m.signed_cash_contribution=v_line.frozen_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_ORIGINAL_BASIS_INVALID';
  end if;
  if new.case_id is not null and
     (new.case_id<>new.id or new.case_component_id<>new.id or not exists(
       select 1 from private.bpay_next_finance_case c
       join private.bpay_next_case_component x on x.id=new.case_component_id and x.case_id=c.id
       join private.bpay_next_case_rule r on r.id=x.rule_id and r.case_id=c.id
       where c.id=new.case_id and c.candidate_id=new.candidate_id
         and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
         and c.principal_approved=x.approved_source_ex_vat
         and c.principal_recovered=x.recovered_source_ex_vat
         and c.principal_written_off=x.written_off_source_ex_vat
         and c.active_recovery_hold_amount=x.active_recovery_source_ex_vat
         and x.candidate_id=c.candidate_id and x.component_key='RECOVERY' and x.component_ordinal=1
         and x.case_kind='OVERPAYMENT' and x.tax_treatment='TAXABLE' and x.source_pay_channel='PAYE'
         and x.instruction_kind='RECOVERY' and x.payroll_stage='GROSS_DEDUCT'
         and c.current_rule_id=r.id and c.current_rule_revision=r.rule_revision
         and r.id=new.id and r.case_kind='OVERPAYMENT' and r.weekly_due_source_ex_vat is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_OWNER_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_job j
     join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
     join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
     where j.id=new.last_job_id and j.candidate_id=new.candidate_id
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE')
       and j.status='LEASED' and j.module_epoch=m.owner_epoch
       and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()
       and ((j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null
         and exists(select 1 from private.bpay_next_publication p where p.command_id=j.command_id
           and p.work_id=new.work_id and p.candidate_id=new.candidate_id
           and p.revision_id=new.observed_revision_id and p.status in ('QUEUED','APPLYING')))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_outcome_request r on r.command_id=j.command_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='PAYROLL_SETTLED'
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=r.transfer_id and e.source_disposition_ex_vat>0))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_case_event ce on ce.id=e.operation_item_id and ce.operation_id=j.command_id
             join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
             join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ce.original_transfer_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='WORK_COLLECTION_RECOVERED'
               and ce.event_kind='RECOVERED' and ce.case_id=new.case_id and ce.case_component_id=new.case_component_id
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=ce.original_transfer_id
               and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
                 is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)))
         or (j.job_kind='PREPARE' and new.last_effect_id is null
           and private.bpay_next_worker_released_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_expired_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_DISCARD' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_discarded_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='SIMPLE_CANCEL' and j.phase='RELEASE' and new.last_effect_id is null
           and exists(select 1 from private.bpay_next_case_cancel_binding cb
             join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
             join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
             join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
             join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
             where cb.command_id=j.command_id and cb.candidate_id=new.candidate_id
               and cb.status='CANCELLING' and cb.stage='CASE'
               and i.case_id=new.case_id and i.case_component_id=new.case_component_id)))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  if not exists(select 1 from private.bpay_next_position p
      where p.work_id=new.work_id and p.component_key=new.component_key
        and (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
          is not distinct from (new.observed_revision_id,new.observed_approved_source_ex_vat,new.observed_realised_source_ex_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_OBSERVATION_INVALID';
  end if;
  if new.segment_attribution_complete is distinct from coalesce((private.bpay_next_segment_collection_basis_v1(
      new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
    or new.observed_segment_negative_ex_vat is distinct from (case when new.segment_attribution_complete then
      (private.bpay_next_segment_collection_basis_v1(new.work_id,new.component_key,new.observed_revision_id)->>'negative_source_ex_vat')::numeric else 0::numeric end) then
    -- The original INSERT records only its native paid anchor; the same-owner
    -- follow-up supplies the qualified observation, exactly as the predecessor.
    if tg_op<>'INSERT' or new.segment_attribution_complete or new.observed_segment_negative_ex_vat<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_SEGMENT_OBSERVATION_INVALID';
    end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_preparation_expiry_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;
 v_run private.bpay_next_pay_run%rowtype;v_request private.bpay_next_preparation_expiry_request%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EVIDENCE_DELETE_FORBIDDEN';end if;
  if tg_table_name='bpay_next_pay_run' then
    if tg_op='INSERT' then
      if new.preparation_expires_at_utc is null or new.preparation_expires_at_utc<>new.created_at_utc+interval '72 hours' then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CAPTURED_DEADLINE_REQUIRED';end if;
    elsif (new.created_at_utc,new.preparation_expires_at_utc) is distinct from (old.created_at_utc,old.preparation_expires_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_IMMUTABLE';end if;
    if (new.status='CANCELLING' or(tg_op='UPDATE' and old.status='CANCELLING'))
      and exists(select 1 from private.bpay_next_preparation_discard_request q where q.run_id=new.id) then
      if new.confirmed_at_utc is not null or not exists(select 1 from private.bpay_next_preparation_discard_request q
        join private.bpay_next_command c on c.id=q.command_id and c.command_kind='PREPARATION_DISCARD'
        join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
        where q.run_id=new.id and q.captured_selection_state=new.selection_state
          and(q.selection_count,q.selection_page_count,q.expected_candidate_count,q.selection_review_revision+1)
            is not distinct from(new.selection_count,new.selection_page_count,new.selected_candidate_count,new.review_revision)
          and(q.status='CANCELLING' or new.cancelled_candidate_count=q.original_removed_count+q.removed_candidate_count)
          and((new.status='CANCELLING' and q.status='CANCELLING')
            or(new.status='CANCELLED' and q.status='DISCARDED' and q.blocked_candidate_count=0)
            or(new.status='REVIEW' and q.status='PARTIAL' and q.blocked_candidate_count>0))
          and(q.status='CANCELLING' or(q.completed_candidate_count=q.expected_candidate_count
            and q.enrolled_candidate_count=q.expected_candidate_count and q.finished_at_utc is not null))) then
        raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_HEADER_FENCE_INVALID';end if;
    else
    if new.status='CANCELLING' or (tg_op='UPDATE' and old.status='CANCELLING') then
      if new.confirmed_at_utc is not null or new.status not in ('CANCELLING','CANCELLED')
         or not exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=new.id
           and captured_deadline=new.preparation_expires_at_utc and selection_count=new.selection_count
           and selection_page_count=new.selection_page_count and expected_candidate_count=new.selected_candidate_count
           and new.review_revision=selection_review_revision+1) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_HEADER_FENCE_INVALID';end if;
    end if;
    end if;
    return new;
  end if;
  if tg_op='INSERT' then
    if tg_table_name='bpay_next_preparation_expiry_request' then
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
      if v_run.status not in ('PREPARING','REVIEW') or v_run.confirmed_at_utc is not null
         or new.captured_deadline is distinct from v_run.preparation_expires_at_utc
         or (new.selection_count,new.selection_page_count,new.selection_review_revision,new.expected_candidate_count)
           is distinct from (v_run.selection_count,v_run.selection_page_count,v_run.review_revision,v_run.selected_candidate_count)
         or new.enrolled_candidate_count<>0 or new.completed_candidate_count<>0
         or not exists(select 1 from private.bpay_next_command where id=new.command_id and command_kind='PREPARATION_EXPIRY' and status='RECEIVED')
         or new.original_prepare_command_id is distinct from (select command_id from private.bpay_next_run_command where run_id=new.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_INITIAL_INVALID';end if;
    else
      select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=new.command_id;
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
v_original_unenrolled:=
  v_request.original_prepare_command_id is not null
  and new.original_job_id is null and new.run_worker_id is null
  and new.expected_work_count=0 and new.expected_case_hold_count=0
  and v_run.selection_state='OPEN'
  and v_run.accepted_choice_descriptor is not null
  and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
  and private.bpay_next_preparation_expiry_fenced_v1(
    new.run_id,v_request.original_prepare_command_id)
  and exists(select 1 from private.bpay_next_command c
    join private.bpay_next_job expiry on expiry.id=new.job_id
      and expiry.module_epoch=c.module_epoch
    where c.id=v_request.original_prepare_command_id
      and c.command_kind='PREPARE' and c.status='COMPLETE'
      and c.expected_member_count=0 and c.enrolled_member_count=0
      and c.sealed_at_utc is not null)
  and not exists(select 1 from private.bpay_next_command_member cm
    where cm.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_job j
    where j.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_run_worker rw
    where rw.run_id=new.run_id);
      if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_request.status<>'CANCELLING'
         or new.run_id<>v_request.run_id or new.member_no<>v_request.enrolled_candidate_count+1
         or new.projection_work_cursor is not null or new.projection_case_cursor is not null or new.pending_work_ids is not null
       or new.status<>'READY' or new.stage<>'NEW' or new.checkpoint<>0
         or new.released_work_count<>0 or new.released_case_hold_count<>0 or new.work_cursor is not null or new.case_hold_cursor is not null
         or not exists(select 1 from private.bpay_next_selection_candidate where run_id=new.run_id and candidate_id=new.candidate_id and member_no=new.member_no)
         or not exists(select 1 from private.bpay_next_job where id=new.job_id and command_id=new.command_id and candidate_id=new.candidate_id
           and job_kind='PREPARATION_EXPIRY' and status='READY' and phase='NEW')
         or (((v_request.original_prepare_command_id is null)
       is distinct from (new.original_job_id is null))
    and not coalesce(v_original_unenrolled,false))
         or (new.original_job_id is not null and not exists(select 1 from private.bpay_next_job where id=new.original_job_id
           and command_id=v_request.original_prepare_command_id and candidate_id=new.candidate_id and job_kind='PREPARE' and status='DONE'))
         or (new.run_worker_id is not null and not exists(select 1 from private.bpay_next_run_worker where id=new.run_worker_id
           and run_id=new.run_id and candidate_id=new.candidate_id and status=new.original_worker_status
           and captured_preparation_revision=new.preparation_revision and preparation_revision=coalesce(new.expected_execution_revision,new.preparation_revision)
           and case_selection_revision=new.selection_revision
           and ((not new.already_worker_released and captured_line_count=new.expected_work_count and active_case_hold_count=new.expected_case_hold_count)
             or (new.already_worker_released and new.expected_work_count=0 and new.expected_case_hold_count=0
               and private.bpay_next_worker_release_complete_v1(new.run_worker_id,v_request.accepted_at_utc)))
           and review_issue_code is not distinct from new.original_review_issue_code and review_issue_work_id is not distinct from new.original_review_issue_work_id)) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_INITIAL_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_preparation_expiry_request' then
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       or new.enrolled_candidate_count not between old.enrolled_candidate_count and old.enrolled_candidate_count+1
       or new.completed_candidate_count not between old.completed_candidate_count and old.completed_candidate_count+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_TRANSITION_INVALID';end if;
  else
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'projection_work_cursor'-'projection_case_cursor'-'pending_work_ids'-'started_at_utc'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'projection_work_cursor'-'projection_case_cursor'-'pending_work_ids'-'started_at_utc'-'finished_at_utc')
       or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
       or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
       or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
       or (old.started_at_utc is not null and new.started_at_utc is distinct from old.started_at_utc)
       or (old.stage='NEW' and new.stage not in ('WORK','CASE')) or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','PROJECT_WORK'))
       or (old.stage='PROJECT_WORK' and new.stage not in ('PROJECT_WORK','PROJECT_CASE'))
       or (old.stage='PROJECT_CASE' and new.stage not in ('PROJECT_CASE','FINAL'))
       or (old.projection_work_cursor is not null and(new.projection_work_cursor is null or new.projection_work_cursor<old.projection_work_cursor))
       or (old.projection_case_cursor is not null and(new.projection_case_cursor is null or new.projection_case_cursor<old.projection_case_cursor))
       or (new.projection_work_cursor is distinct from old.projection_work_cursor and old.stage<>'PROJECT_WORK')
       or (new.projection_case_cursor is distinct from old.projection_case_cursor and old.stage<>'PROJECT_CASE')
       or (exists(select 1 from unnest(old.pending_work_ids) x(id) join private.bpay_next_workbench_task t
            on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE')
         and(pg_catalog.to_jsonb(new)-'checkpoint') is distinct from(pg_catalog.to_jsonb(old)-'checkpoint')) or (old.stage='FINAL' and new.stage not in ('FINAL','COMPLETE')) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_TRANSITION_INVALID';end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_preparation_discard_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;
 v_run private.bpay_next_pay_run%rowtype;v_request private.bpay_next_preparation_discard_request%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_EVIDENCE_DELETE_FORBIDDEN';end if;
  if tg_op='INSERT' then
    if tg_table_name='bpay_next_preparation_discard_request' then
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
      if v_run.status not in ('PREPARING','REVIEW') or v_run.confirmed_at_utc is not null
         or new.captured_selection_state is distinct from v_run.selection_state
         or new.original_removed_count<>v_run.cancelled_candidate_count
         or new.removed_candidate_count<>0 or new.already_removed_candidate_count<>0 or new.blocked_candidate_count<>0
         or (new.selection_count,new.selection_page_count,new.selection_review_revision,new.expected_candidate_count)
           is distinct from (v_run.selection_count,v_run.selection_page_count,v_run.review_revision,v_run.selected_candidate_count)
         or new.enrolled_candidate_count<>0 or new.completed_candidate_count<>0
         or not exists(select 1 from private.bpay_next_command where id=new.command_id and command_kind='PREPARATION_DISCARD' and status='RECEIVED')
         or new.original_prepare_command_id is distinct from (select command_id from private.bpay_next_run_command where run_id=new.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_REQUEST_INITIAL_INVALID';end if;
    else
      select * into strict v_request from private.bpay_next_preparation_discard_request where command_id=new.command_id;
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
v_original_unenrolled:=
  v_request.original_prepare_command_id is not null
  and new.original_job_id is null and new.run_worker_id is null
  and new.expected_work_count=0 and new.expected_case_hold_count=0
  and v_run.selection_state='OPEN'
  and v_run.accepted_choice_descriptor is not null
  and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
  and private.bpay_next_preparation_discard_fenced_v1(
    new.run_id,v_request.original_prepare_command_id)
  and exists(select 1 from private.bpay_next_command c
    join private.bpay_next_job expiry on expiry.id=new.job_id
      and expiry.module_epoch=c.module_epoch
    where c.id=v_request.original_prepare_command_id
      and c.command_kind='PREPARE' and c.status='COMPLETE'
      and c.expected_member_count=0 and c.enrolled_member_count=0
      and c.sealed_at_utc is not null)
  and not exists(select 1 from private.bpay_next_command_member cm
    where cm.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_job j
    where j.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_run_worker rw
    where rw.run_id=new.run_id);
      if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_request.status<>'CANCELLING'
         or new.run_id<>v_request.run_id or new.member_no<>v_request.enrolled_candidate_count+1
         or new.projection_work_cursor is not null or new.projection_case_cursor is not null or new.pending_work_ids is not null
       or new.status<>'READY' or new.stage<>'NEW' or new.checkpoint<>0
         or new.released_work_count<>0 or new.released_case_hold_count<>0 or new.work_cursor is not null or new.case_hold_cursor is not null
         or not exists(select 1 from private.bpay_next_selection_candidate where run_id=new.run_id and candidate_id=new.candidate_id and member_no=new.member_no)
         or not exists(select 1 from private.bpay_next_job where id=new.job_id and command_id=new.command_id and candidate_id=new.candidate_id
           and job_kind='PREPARATION_DISCARD' and status='READY' and phase='NEW')
         or (((v_request.original_prepare_command_id is null)
       is distinct from (new.original_job_id is null))
    and not coalesce(v_original_unenrolled,false))
         or (new.original_job_id is not null and not exists(select 1 from private.bpay_next_job where id=new.original_job_id
           and command_id=v_request.original_prepare_command_id and candidate_id=new.candidate_id and job_kind='PREPARE' and status='DONE'))
         or (new.run_worker_id is not null and not exists(select 1 from private.bpay_next_run_worker where id=new.run_worker_id
           and run_id=new.run_id and candidate_id=new.candidate_id and status=new.original_worker_status
           and captured_preparation_revision=new.preparation_revision and preparation_revision=coalesce(new.expected_execution_revision,new.preparation_revision)
           and case_selection_revision=new.selection_revision
           and ((not new.already_worker_released and captured_line_count=new.expected_work_count and active_case_hold_count=new.expected_case_hold_count)
             or (new.already_worker_released and new.expected_work_count=0 and new.expected_case_hold_count=0
               and private.bpay_next_worker_release_complete_v1(new.run_worker_id,v_request.accepted_at_utc)))
           and review_issue_code is not distinct from new.original_review_issue_code and review_issue_work_id is not distinct from new.original_review_issue_work_id)) then
        raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_WORKER_INITIAL_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_preparation_discard_request' then
    if old.status in ('DISCARDED','PARTIAL') or (pg_catalog.to_jsonb(new)-'enrolled_candidate_count'-'completed_candidate_count'-'removed_candidate_count'-'already_removed_candidate_count'-'blocked_candidate_count'-'status'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'enrolled_candidate_count'-'completed_candidate_count'-'removed_candidate_count'-'already_removed_candidate_count'-'blocked_candidate_count'-'status'-'finished_at_utc')
       or new.enrolled_candidate_count not between old.enrolled_candidate_count and old.enrolled_candidate_count+1
       or new.completed_candidate_count not between old.completed_candidate_count and old.completed_candidate_count+1
       or new.removed_candidate_count not between old.removed_candidate_count and old.removed_candidate_count+1
       or new.already_removed_candidate_count not between old.already_removed_candidate_count and old.already_removed_candidate_count+1
       or new.blocked_candidate_count not between old.blocked_candidate_count and old.blocked_candidate_count+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_REQUEST_TRANSITION_INVALID';end if;
  else
    if old.status in ('DISCARDED','BLOCKED') or (pg_catalog.to_jsonb(new)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'projection_work_cursor'-'projection_case_cursor'-'pending_work_ids'-'started_at_utc'-'finished_at_utc'-'outcome'-'blocked_code')
       is distinct from (pg_catalog.to_jsonb(old)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'projection_work_cursor'-'projection_case_cursor'-'pending_work_ids'-'started_at_utc'-'finished_at_utc'-'outcome'-'blocked_code')
       or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
       or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
       or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
       or (old.started_at_utc is not null and new.started_at_utc is distinct from old.started_at_utc)
       or (old.stage='NEW' and new.stage not in ('WORK','CASE','COMPLETE')) or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','PROJECT_WORK'))
       or (old.stage='PROJECT_WORK' and new.stage not in ('PROJECT_WORK','PROJECT_CASE'))
       or (old.stage='PROJECT_CASE' and new.stage not in ('PROJECT_CASE','FINAL'))
       or (old.projection_work_cursor is not null and(new.projection_work_cursor is null or new.projection_work_cursor<old.projection_work_cursor))
       or (old.projection_case_cursor is not null and(new.projection_case_cursor is null or new.projection_case_cursor<old.projection_case_cursor))
       or (new.projection_work_cursor is distinct from old.projection_work_cursor and old.stage<>'PROJECT_WORK')
       or (new.projection_case_cursor is distinct from old.projection_case_cursor and old.stage<>'PROJECT_CASE')
       or (exists(select 1 from unnest(old.pending_work_ids) x(id) join private.bpay_next_workbench_task t
            on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE')
         and(pg_catalog.to_jsonb(new)-'checkpoint') is distinct from(pg_catalog.to_jsonb(old)-'checkpoint')) or (old.stage='FINAL' and new.stage not in ('FINAL','COMPLETE')) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_WORKER_TRANSITION_INVALID';end if;
  end if;
  if tg_op='UPDATE' and tg_table_name='bpay_next_preparation_discard_worker' then
    if new.status='BLOCKED' and(old.stage<>'NEW' or not exists(select 1 from private.bpay_next_run_worker w
      where w.id=new.run_worker_id and w.candidate_id=new.candidate_id and w.run_id=new.run_id
        and w.status=new.original_worker_status and w.preparation_revision=new.expected_execution_revision
        and w.captured_preparation_revision=new.preparation_revision and w.case_selection_revision=new.selection_revision
        and(w.status not in ('PREPARING','REVIEW','READY') or w.realised_effect_count<>0 or w.net_request_revision<>0
          or w.net_projection_revision<>0 or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=w.id)
          or exists(select 1 from private.bpay_next_net_projection n where n.run_worker_id=w.id)))) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_BLOCKED_SCOPE_INVALID';end if;
    if new.status='DISCARDED' and(old.stage<>'FINAL' or new.pending_work_ids is not null
      or new.released_work_count<>new.expected_work_count or new.released_case_hold_count<>new.expected_case_hold_count
      or exists(select 1 from private.bpay_next_hold h join private.bpay_next_run_line l on l.id=h.run_line_id
        where l.run_worker_id=new.run_worker_id and h.status='ACTIVE')
      or exists(select 1 from private.bpay_next_case_hold h where h.run_worker_id=new.run_worker_id and h.status='ACTIVE')) then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_FINAL_SCOPE_INVALID';end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_enroll_one_v1()
returns uuid
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_command private.bpay_next_command%rowtype;
  v_member private.bpay_next_command_member%rowtype;
  v_job_id uuid;
  v_epoch bigint;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_selected_candidate uuid;
  v_expired_preparation boolean;
  v_discarded_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select * into v_command from private.bpay_next_command
    where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  -- Native completed snooze controls own no financial job. Advance exactly
  -- the next accepted sequence only after its original immutable receipt.
  if v_command.command_kind in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')
    and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not (exists(select 1 from private.bpay_next_snooze_request r
        where r.request_id=v_command.id and r.action=v_command.command_kind
          and (r.request_body->>'request_id')::uuid=r.request_id)
        or private.bpay_next_snooze_due_receipt_valid_v1(v_command.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';
    end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  -- WB-38 parent records frozen native scope; original single child commands own all controls.
  if v_command.command_kind in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not exists(select 1 from private.bpay_next_snooze_many_request r
        join private.bpay_next_snooze_many_page p on p.request_id=r.request_id and p.page_no=1
        where r.request_id=v_command.id and r.action=v_command.command_kind and r.module_epoch=v_epoch
          and (r.request_body->>'request_id')::uuid=r.request_id and p.result_body->'received'='true'::jsonb) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  if v_command.command_kind='PREPARE' and exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.command_id=v_command.id and x.initiator_kind='OFFICE') then
    v_job_id:=private.bpay_next_enroll_worker_action_v1(v_command.id);
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARE' then
    select rc.run_id into strict v_run_id
      from private.bpay_next_run_command rc
      where rc.command_id=v_command.id;
if v_command.status in ('RECEIVED','COMPLETE') then
  -- NO FOR SHARE/UPDATE here. Seal/expiry acquire run then command;
  -- waiting/retired metadata must not add command -> run locking.
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=v_run_id;
  if v_command.status='RECEIVED' then
    if v_command.module_epoch is distinct from v_epoch
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.confirmed_at_utc is not null
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_command.id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=v_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    return null;
  end if;
  -- Only an exact expired, never-enrolled positive CREATE may retire0.
  if v_command.module_epoch is distinct from v_epoch
     or v_command.expected_member_count is distinct from 0::bigint
     or v_command.enrolled_member_count<>0
     or v_command.sealed_at_utc is null
     or v_run.selection_state is distinct from 'OPEN'
     or v_run.status not in ('CANCELLING','CANCELLED')
     or v_run.confirmed_at_utc is not null
     or v_run.accepted_choice_descriptor is null
     or v_run.accepted_choice_command_id is distinct from v_command.id
     or not(private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
       or private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id))
     or exists(select 1 from private.bpay_next_command_member cm
       where cm.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_job j
       where j.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_run_worker rw
       where rw.run_id=v_run_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence
    where id=1 and last_enrolled_sequence=v_last
      and v_command.agency_sequence=v_last+1;
  if not found then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  return null;
end if;
    select * into strict v_run from private.bpay_next_pay_run
      where id=v_run_id for share nowait;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    v_discarded_preparation:=private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and(v_expired_preparation or v_discarded_preparation)))
       or v_run.selection_state<>'SEALED'
       or v_command.expected_member_count<>v_run.selected_candidate_count
       or v_command.enrolled_member_count>=v_command.expected_member_count then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    select sc.candidate_id into strict v_selected_candidate
      from private.bpay_next_selection_candidate sc
      where sc.run_id=v_run_id
        and sc.member_no=v_command.enrolled_member_count+1;
    insert into private.bpay_next_command_member
      (command_id,candidate_id,member_no)
      values(v_command.id,v_selected_candidate,
             v_command.enrolled_member_count+1);
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_selected_candidate,'PREPARE',
             case when v_expired_preparation or v_discarded_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' when v_discarded_preparation then 'DISCARDED' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command
      set enrolled_member_count=enrolled_member_count+1,
          status=case when enrolled_member_count+1=expected_member_count
            then 'SEALED' else 'ENROLLING' end,
          sealed_at_utc=case when enrolled_member_count+1=expected_member_count
            then pg_catalog.transaction_timestamp() else null end
      where id=v_command.id;
    if v_command.enrolled_member_count+1=v_command.expected_member_count then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_EXPIRY' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_expiry_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_DISCARD' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_discard_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.status <> 'SEALED' then
    return null;
  end if;
  if v_command.command_kind='CASE_WRITE_OFF' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_write_off_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_WRITE_OFF','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_create_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_CREATE','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='REISSUE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_reissue_cancel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'REISSUE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT') then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if v_command.command_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is null and r.internal_receipt_id is not null)
       or v_command.command_kind='CSV_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is not null and r.internal_receipt_id is null)
       or v_command.command_kind='CSV_RETURN' and not exists(select 1 from private.bpay_next_return_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id)
       or v_command.command_kind='CASH_REISSUE' and not exists(select 1 from private.bpay_next_reissue_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,v_command.command_kind,'READY','MEMBERS',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='SIMPLE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_cancel_request r
      where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
        and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'SIMPLE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PAYE_NET_ENTRY' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_paye_net_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'PAYE_NET_ENTRY','READY',
             case when exists(select 1 from private.bpay_next_paye_net_request r
               where r.command_id=v_command.id and r.case_draft_state_id is not null)
               then 'FINANCE_ALLOCATE' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='TRANSFER_BUILD' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_transfer t
                  where t.build_command_id=v_command.id
                    and t.candidate_id=v_member.candidate_id
                    and t.status='BUILDING') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'TRANSFER_BUILD','READY','WORK',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.module_epoch<>v_epoch
     or v_command.command_kind<>'POSITION_APPLY'
     or v_command.expected_member_count<>1
     or v_command.enrolled_member_count<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_COMMAND_INVALID';
  end if;
  select * into strict v_member from private.bpay_next_command_member
    where command_id=v_command.id and member_no=1;
  if not exists (select 1 from private.bpay_next_publication p
                 where p.command_id=v_command.id and p.candidate_id=v_member.candidate_id) then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_PUBLICATION_MISSING';
  end if;
  insert into private.bpay_next_job
    (command_id,command_sequence,module_epoch,candidate_id,job_kind,
     status,phase,owner_epoch)
    values (v_command.id,v_command.agency_sequence,v_epoch,
            v_member.candidate_id,'POSITION_APPLY','READY','NEW',1)
    returning id into v_job_id;
  update private.bpay_next_command set enrolled_member_count=1
    where id=v_command.id;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence where id=1;
  return v_job_id;
end
$function$;

create or replace function private.bpay_next_enroll_worker_action_v1(p_command_id uuid)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run uuid;v_candidate uuid;v_worker uuid;v_revision bigint;v_action text;v_epoch bigint;v_sequence bigint;
  v_phase text;v_cursor text;v_work uuid;v_component text;v_job uuid;v_active_revision bigint;v_status text;
begin
  select run_id,candidate_id,run_worker_id,target_preparation_revision,action,module_epoch,
    resume_phase,resume_cursor,resume_work_cursor,resume_component_cursor
    into strict v_run,v_candidate,v_worker,v_revision,v_action,v_epoch,v_phase,v_cursor,v_work,v_component
    from private.bpay_next_worker_reprepare_request where command_id=p_command_id and initiator_kind='OFFICE' and status='ACCEPTED';
  perform 1 from private.bpay_next_pay_run where id=v_run and confirmed_at_utc is null for share nowait;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
  if v_action='REPREPARE' and exists(select 1 from private.bpay_next_worker_reprepare_request x
    where x.command_id=p_command_id and x.run_worker_id=v_worker and x.target_preparation_revision=v_revision
      and x.job_id is null and(private.bpay_next_preparation_expiry_fenced_v1(x.run_id,x.original_prepare_command_id)
        or private.bpay_next_preparation_discard_fenced_v1(x.run_id,x.original_prepare_command_id))) then
    -- Ordered enrollment retires an accepted retry fenced before its first job.
    update private.bpay_next_command set expected_member_count=0,status='COMPLETE',sealed_at_utc=pg_catalog.clock_timestamp()
      where id=p_command_id and status='ENROLLING' and expected_member_count=1 and enrolled_member_count=0;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ACTIVE_RETRY_INVALID';end if;
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=pg_catalog.clock_timestamp()
      where command_id=p_command_id and run_worker_id=v_worker and status='ACCEPTED';
    return null;
  end if;
  if not exists(select 1 from private.bpay_next_pay_run where id=v_run and status='PREPARING') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
  select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id and command_kind='PREPARE'
    and module_epoch=v_epoch and status='ENROLLING' and expected_member_count=1 and enrolled_member_count=0;
  select preparation_revision,status into strict v_active_revision,v_status from private.bpay_next_run_worker where id=v_worker;
  if v_active_revision<>v_revision or v_status<>(case when v_action='REPREPARE' then 'PREPARING' else 'CANCELLING' end) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,1);
  insert into private.bpay_next_job(command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch,
    cursor_key,position_work_cursor,position_component_cursor)
    values(p_command_id,v_sequence,v_epoch,v_candidate,'PREPARE','READY',
      case when v_action='REPREPARE' then v_phase else 'WORKER_RELEASE_WORK' end,1,
      case when v_action='REPREPARE' then v_cursor else null end,
      case when v_action='REPREPARE' then v_work else null end,
      case when v_action='REPREPARE' then v_component else null end) returning id into v_job;
  update private.bpay_next_worker_reprepare_request set job_id=v_job,status='RUNNING',
    stage=case when v_action='RELEASE' then 'WORK' else 'NEW' end where command_id=p_command_id and run_worker_id=v_worker;
  update private.bpay_next_run_worker set active_prepare_job_id=v_job where id=v_worker;
  update private.bpay_next_command set enrolled_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return v_job;
end
$function$;

create or replace function private.bpay_next_worker_action_receipt_v1(p_command_id uuid,p_run_worker_id uuid,p_expected_revision bigint,p_replay boolean)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select pg_catalog.jsonb_build_object('command_id',x.command_id,'run_id',x.run_id,'run_worker_id',x.run_worker_id,
    'action',x.action,'expected_preparation_revision',x.expected_preparation_revision::text,
    'preparation_revision',x.target_preparation_revision::text,'review_revision',x.accepted_review_revision::text,
    'agency_sequence',c.agency_sequence::text,'phase',case when private.bpay_next_preparation_expiry_fenced_v1(x.run_id,x.original_prepare_command_id) then 'EXPIRED' when private.bpay_next_preparation_discard_fenced_v1(x.run_id,x.original_prepare_command_id) then 'DISCARDED' else x.status end,'replay',p_replay)
    from private.bpay_next_worker_reprepare_request x join private.bpay_next_command c on c.id=x.command_id
    where x.command_id=p_command_id and x.run_worker_id=p_run_worker_id and x.expected_preparation_revision=p_expected_revision
      order by x.target_preparation_revision desc limit 1
$function$;

create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;v_seal jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  if p_action='CREATE' and p_args ? 'descriptor' then
    -- Authenticate before admission; preserve Source -> module lock order.
    if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users u
        where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin' for share) then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    perform private.bpay_next_preparation_args_v1(p_args,array['run_id','command_id','descriptor']);
    if not exists(select 1 from private.bpay_next_pay_run where id=(p_args->>'run_id')::uuid) then
      begin perform private.weekly_source_pay_query_admit_v2();
      exception when lock_not_available then null;end;end if;
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'PREPARATION_DISCARD' then array['command_id','run_id','expected_preparation_revision']||case when p_args ? 'reason' then array['reason'] else array[]::text[] end
    when 'WAKE' then array['run_id']
    when 'CREATE' then case when p_args ? 'descriptor' then array['run_id','command_id','descriptor'] when p_args ? 'pay_date' then array['run_id','pay_date'] else array['run_id'] end
    when 'EXCLUSION_PAGE' then array['run_id','page_no','excluded_work_ids','excluded_candidate_ids']
    when 'MATCHING_PAGE' then array['run_id','page_no','after_member_no']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
    when 'SEGMENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','request_id','segment_keys']
    when 'SEGMENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'COMPONENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'CASE_PAGE' then array['run_id','candidate_id','selection_revision','page_no','request_id','component_ids','selected']
    when 'CASE_SEAL' then array['run_id','candidate_id','selection_revision','expected_pages','expected_items']
    when 'SEAL_ENQUEUE' then array['run_id','command_id','expected_pages','expected_count']
    when 'WORKER_REPREPARE' then array['command_id','run_worker_id','expected_preparation_revision']
    when 'WORKER_RELEASE' then array['command_id','run_worker_id','expected_preparation_revision']
    when 'REVIEW' then array['run_id']
    when 'CONFIRM' then array['run_id','review_revision'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  if p_action='PREPARATION_DISCARD' then
    perform private.bpay_next_preparation_args_v1(p_args-array['expected_preparation_revision','reason'],array['command_id','run_id']);
    if p_args is null or pg_catalog.jsonb_typeof(p_args)<>'object' or pg_catalog.octet_length(p_args::text)>2048
      or(select count(*) from pg_catalog.jsonb_object_keys(p_args))<>cardinality(v_keys)
      or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys)))
      or pg_catalog.jsonb_typeof(p_args->'expected_preparation_revision') is distinct from 'string'
      or(p_args->>'expected_preparation_revision')!~'^(0|[1-9][0-9]{0,18})$'
      or(p_args->>'expected_preparation_revision')::numeric>9223372036854775807
      or(p_args ? 'reason' and(pg_catalog.jsonb_typeof(p_args->'reason') is distinct from 'string'
        or pg_catalog.octet_length(p_args->>'reason') not between 1 and 500 or btrim(p_args->>'reason')='')) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  else perform private.bpay_next_preparation_args_v1(p_args,v_keys);end if;
  v_run:=(p_args->>'run_id')::uuid;v_work:=(p_args->>'work_id')::uuid;
  v_revision:=(p_args->>'expected_revision_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  if p_action in ('EXCLUSION_PAGE','MATCHING_PAGE') then
    perform private.bpay_next_selection_page_authority_v1(v_run,p_actor_user_id);
  end if;
  if p_action in ('COMPONENT_PAGE','CASE_PAGE') then
    for v_entry in select value from pg_catalog.jsonb_array_elements(
      p_args->case when p_action='CASE_PAGE' then 'component_ids' else 'approved_line_ids' end) loop
      if pg_catalog.jsonb_typeof(v_entry) is distinct from 'string'
        or (v_entry#>>'{}')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      v_ids:=pg_catalog.array_append(v_ids,(v_entry#>>'{}')::uuid);
    end loop;
    if p_action='CASE_PAGE' then
      if pg_catalog.jsonb_array_length(p_args->'selected')<>pg_catalog.cardinality(v_ids) then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      for v_entry in select value from pg_catalog.jsonb_array_elements(p_args->'selected') loop
        if pg_catalog.jsonb_typeof(v_entry) is distinct from 'boolean' then
          raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
        end if;
        v_selected:=pg_catalog.array_append(v_selected,(v_entry#>>'{}')::boolean);
      end loop;
    end if;
  end if;
  case p_action
  when 'WAKE' then
    v_output:=private.bpay_next_preparation_wake_v1(v_run);
  when 'CREATE' then
    if p_args ? 'descriptor' then
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,null,p_args->'descriptor',(p_args->>'command_id')::uuid);
    else
      v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    end if;
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text,'official_pay_date',v_header.pay_date::text);
    if p_args ? 'descriptor' then
      v_output:=v_output||jsonb_build_object('command_id',v_result->>'command_id',
        'agency_sequence',v_result->'agency_sequence','choice_work_count',v_result->>'choice_work_count',
        'choice_case_count',v_result->>'choice_case_count','empty',v_result->'empty','replay',v_result->'replay');
    end if;
  when 'EXCLUSION_PAGE' then
    v_output:=private.bpay_next_exclusion_page_v1(v_run,(p_args->>'page_no')::bigint,p_args);
  when 'MATCHING_PAGE' then
    v_output:=private.bpay_next_matching_page_v1(v_run,(p_args->>'page_no')::bigint,p_args);
  when 'WORK_PAGE' then
    v_output:=private.bpay_next_append_work_choices_v1(v_run,(p_args->>'page_no')::bigint,p_args->'choices');
  when 'COMPONENT_PAGE' then
    v_output:=private.bpay_next_append_component_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,v_ids);
  when 'SEGMENT_PAGE' then
    if exists(select 1 from jsonb_array_elements(p_args->'segment_keys') item where jsonb_typeof(item)<>'string') then
      raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_SELECTION_KEY_INVALID';
    end if;
    v_output:=private.bpay_next_append_segment_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,
      (p_args->>'request_id')::uuid,array(select value#>>'{}' from jsonb_array_elements(p_args->'segment_keys')));
  when 'SEGMENT_SEAL' then
    v_output:=private.bpay_next_seal_segment_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'COMPONENT_SEAL' then
    v_output:=private.bpay_next_seal_component_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'CASE_PAGE' then
    v_result:=private.bpay_next_append_case_selection_page_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'page_no')::bigint,(p_args->>'request_id')::uuid,v_ids,v_selected);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no',
      'item_count',v_result->>'item_count','replay',v_result->'replay');
  when 'CASE_SEAL' then
    v_result:=private.bpay_next_seal_case_selection_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_items')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed',
      'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay');
  when 'SEAL_ENQUEUE' then
    v_seal:=private.bpay_next_seal_selection_v1(v_run,(p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
    v_result:=private.bpay_next_enqueue_preparation_v1(v_run,(p_args->>'command_id')::uuid);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id',
      'agency_sequence',v_result->>'agency_sequence','selection_count',v_seal->>'selection_count','replay',v_result->'replay');
  when 'WORKER_REPREPARE','WORKER_RELEASE' then
    v_output:=private.bpay_next_accept_worker_action_v1((p_args->>'command_id')::uuid,
      (p_args->>'run_worker_id')::uuid,case p_action when 'WORKER_REPREPARE' then 'REPREPARE' else 'RELEASE' end,
      p_actor_user_id,(p_args->>'expected_preparation_revision')::bigint);
  when 'PREPARATION_DISCARD' then
    v_output:=private.bpay_next_accept_preparation_discard_v1((p_args->>'command_id')::uuid,v_run,p_actor_user_id,(p_args->>'expected_preparation_revision')::bigint,p_args->>'reason');
  when 'REVIEW' then
    v_result:=private.bpay_next_review_simple_run_v1(v_run);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason',
      'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready',
      'replay',coalesce(v_result->'replay','false'::jsonb));
  when 'CONFIRM' then
    v_result:=private.bpay_next_confirm_simple_run_v1(v_run,(p_args->>'review_revision')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase',
      'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts');
  end case;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';
  end if;
  return v_output;
end
$function$;

create or replace function public.bpay_next_financial_job_v1(
  p_action text,p_job_id uuid,p_job_kind text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_keys text[];
  v_key text;
  v_paged boolean;
  v_integer_cursor boolean;
  v_phase text;
  v_lease_seconds integer;
  v_nonce uuid;
  v_owner_epoch bigint;
  v_cursor bigint;
  v_text_cursor text;
  v_work_cursor uuid;
  v_component_cursor text;
  v_limit integer;
  v_claim record;
  v_claim_state record;
  v_page record;
  v_worker private.bpay_next_run_worker%rowtype;
  v_result jsonb;
  v_output jsonb;
  v_lease_until text;
  v_claim_failure text;
  v_case_net boolean:=false;
  v_expired_prepare boolean:=false;
  v_discarded_prepare boolean:=false;
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_phase:=p_args->>'expected_phase';
  if p_action='STEP' and (p_job_kind in ('POSITION_APPLY','PREPARE')
      or (p_job_kind='PAYE_NET_ENTRY' and p_args ? 'expected_phase'))
     and (pg_catalog.jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
       or not(v_phase=any(case p_job_kind when 'POSITION_APPLY' then array['NEW','REMOVED']
         when 'PAYE_NET_ENTRY' then array['FINANCE_ALLOCATE']
         else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_keys:=case when p_action='CLAIM' then array['lease_seconds']
    when p_job_kind='POSITION_APPLY' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then array['lease_nonce','owner_epoch','expected_phase','expected_cursor']
    when p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then array['lease_nonce','owner_epoch','expected_phase','expected_work_cursor','expected_component_cursor','limit']
    when p_job_kind='PREPARE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when v_paged then array['lease_nonce','owner_epoch','expected_cursor','limit']
    else array['lease_nonce','owner_epoch'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='CLAIM' then
    if pg_catalog.jsonb_typeof(p_args->'lease_seconds') is distinct from 'number'
       or (p_args->>'lease_seconds')!~'^[1-9][0-9]{0,2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    v_lease_seconds:=(p_args->>'lease_seconds')::integer;
    if v_lease_seconds not between 1 and 120 then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  else
    if pg_catalog.jsonb_typeof(p_args->'lease_nonce') is distinct from 'string'
       or (p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    foreach v_key in array array['owner_epoch','expected_cursor'] loop
      if v_key='expected_cursor' and (not coalesce(v_integer_cursor,false) or p_args->v_key='null'::jsonb) then
        continue;
      end if;
      if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string'
         or (p_args->>v_key)!~'^[1-9][0-9]{0,18}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      if (p_args->>v_key)::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end loop;
    v_nonce:=(p_args->>'lease_nonce')::uuid;
    v_owner_epoch:=(p_args->>'owner_epoch')::bigint;
    if v_integer_cursor then v_cursor:=(p_args->>'expected_cursor')::bigint; end if;
    if p_job_kind='POSITION_APPLY' then
      if p_args->'expected_cursor'<>'null'::jsonb
         and (pg_catalog.jsonb_typeof(p_args->'expected_cursor') is distinct from 'string'
              or pg_catalog.char_length(p_args->>'expected_cursor') not between 1 and 256) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_text_cursor:=p_args->>'expected_cursor';
    elsif p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then
      -- A cursor is the complete (UUID work, opaque retained component TEXT)
      -- pair, or both NULL. Component bounds are CHARACTERS, not bytes.
      if (p_args->'expected_work_cursor'='null'::jsonb) is distinct from
          (p_args->'expected_component_cursor'='null'::jsonb)
         or (p_args->'expected_work_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_work_cursor') is distinct from 'string'
                  or (p_args->>'expected_work_cursor')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
         or (p_args->'expected_component_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_component_cursor') is distinct from 'string'
                  or pg_catalog.char_length(p_args->>'expected_component_cursor') not between 1 and 256)) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_work_cursor:=(p_args->>'expected_work_cursor')::uuid;
      v_component_cursor:=p_args->>'expected_component_cursor';
    end if;
    if v_paged then
      if pg_catalog.jsonb_typeof(p_args->'limit') is distinct from 'number'
         or (p_args->>'limit')!~'^[1-9][0-9]{0,2}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_limit:=(p_args->>'limit')::integer;
      if v_limit not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end if;
  end if;

  -- Immutable dispatch identity, without an outer job lock. Owners acquire
  -- Candidate -> job for CLAIM, parent run -> Candidate -> job for STEP.
  -- Taking job FOR UPDATE here would invert that established order.
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>p_job_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';
  end if;
  -- Query admission is outside even the public module SHARE. Plain dispatch
  -- discovery is not lease/module authority; every owner rechecks under locks.
  -- Definite terminal/advanced/cursor/stale replies must not newly contend on
  -- the query key. Their independent owner locks/validates the module before
  -- any write, and this boundary reasserts it immediately after that call.
  if p_action='STEP' and p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then
    if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
       and v_job.lease_nonce is not distinct from v_nonce
       and v_job.owner_epoch is not distinct from v_owner_epoch
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()
       and v_job.cursor_key::bigint is not distinct from v_cursor then
      perform private.weekly_source_pay_query_admit_v2();
    else
      v_defer_pin_module:=true;
    end if;
  end if;
  if not v_defer_pin_module and not exists(
      select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_job_kind='PAYE_NET_ENTRY' then
    select r.case_draft_state_id is not null into strict v_case_net
      from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id;
    if p_action='STEP' and v_case_net is distinct from (p_args ? 'expected_phase') then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  end if;
  if p_action='CLAIM' then
    if (p_job_kind='POSITION_APPLY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['DONE'] else array['NEW','REMOVED'] end)))
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED','DISCARDED','CANCELLED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind='PREPARATION_DISCARD' and not(v_job.phase=any(case when v_job.status='DONE' then array['DISCARDED','BLOCKED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind in ('CASE_CREATE','CASE_WRITE_OFF') and (v_job.phase is distinct from
          case when v_job.status='DONE' then 'DONE' else 'NEW' end or v_job.cursor_key is not null)) then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
    end if;
    -- Distinguish terminal and busy claims; never return the stored nonce.
    -- These read-only early checks are not ownership: owners recheck/lock.
    if v_job.status='DONE' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
    end if;
    if v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
    end if;
    begin
      case p_job_kind
        when 'POSITION_APPLY' then
          select c.* into strict v_claim from private.bpay_next_claim_position_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARE' then
          select c.* into strict v_claim from private.bpay_next_claim_prepare_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_EXPIRY' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_expiry_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_DISCARD' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_discard_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_CREATE' then
          select c.* into strict v_claim from private.bpay_next_claim_case_create_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_WRITE_OFF' then
          select c.* into strict v_claim from private.bpay_next_claim_write_off_job_v1(p_job_id,v_lease_seconds) c;
        when 'PAYE_NET_ENTRY' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_paye_net_job_v1(p_job_id,v_lease_seconds) c;
        when 'TRANSFER_BUILD' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_transfer_job_v1(p_job_id,v_lease_seconds) c;
        when 'SIMPLE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_cancel_job_v1(p_job_id,v_lease_seconds) c;
        when 'REISSUE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_reissue_cancel_job_v1(p_job_id,v_lease_seconds) c;
        else
          select c.* into strict v_claim from private.bpay_next_claim_simple_outcome_job_v1(p_job_id,v_lease_seconds) c;
      end case;
    exception when sqlstate '55000' then
      get stacked diagnostics v_claim_failure=message_text;
      if v_claim_failure not in
          ('BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE','BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE','BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD') then raise; end if;
      -- A loser may have read READY, then waited behind the winning owner.
      -- The failed subtransaction released its owner locks. Classify its
      -- now-visible exact job without acquiring job ahead of Candidate/run,
      -- retrying any owner or adopting the winner's stored lease nonce.
      -- Job and admission predicates share one fresh read snapshot. A
      -- predecessor finishing during separate reads must not manufacture a
      -- permanent eligibility failure for an already accepted financial job.
      select j.*,
        exists(select 1 from private.bpay_next_pay_run r
          where r.id=private.bpay_next_prepare_job_run_v1(j.id) and r.status='PREPARING' and r.selection_state='SEALED'
            and private.bpay_next_prepare_job_active_v1(j.id)
            and exists(select 1 from private.bpay_next_run_worker w where w.run_id=r.id and w.candidate_id=j.candidate_id
              and (w.status='PREPARING' or (j.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and w.status in ('READY','REVIEW','CANCELLED'))
                or (j.phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') and w.status='CANCELLING')))) as prepare_admissible,
        exists(select 1 from private.bpay_next_cancel_request r
               where r.command_id=j.command_id and r.candidate_id=j.candidate_id
                 and r.status in ('REQUESTED','CANCELLING')) as cancel_request_pending,
        exists(select 1 from private.bpay_next_preparation_expiry_request r
          join private.bpay_next_pay_run run on run.id=r.run_id
          where r.command_id=j.command_id and r.status='CANCELLING'
            and run.status='CANCELLING' and run.confirmed_at_utc is null) as expiry_request_pending,
        exists(select 1 from private.bpay_next_preparation_discard_request q join private.bpay_next_pay_run r on r.id=q.run_id
          where q.command_id=j.command_id and q.status='CANCELLING' and r.status='CANCELLING' and r.confirmed_at_utc is null) as discard_request_pending,
        exists(select 1 from private.bpay_next_case_create_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_create_request_present,
        exists(select 1 from private.bpay_next_write_off_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.status='REQUESTED') as write_off_request_pending,
        exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=j.candidate_id
                 and earlier.command_sequence<j.command_sequence
                 and earlier.status<>'DONE') as predecessor_pending
      into strict v_claim_state from private.bpay_next_job j where j.id=p_job_id;
      if v_claim_state.job_kind<>p_job_kind or v_claim_state.module_epoch is distinct from
          (select m.owner_epoch from private.bpay_next_module_control m
           where m.id=1 and m.active_owner='NEXT') then
        raise;
      end if;
      if v_claim_state.status='DONE' then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
      end if;
      if not(v_claim_state.phase=any(case p_job_kind
          when 'PAYE_NET_ENTRY' then array['NEW','FINANCE_ALLOCATE']
          when 'TRANSFER_BUILD' then array['WORK']
          when 'SIMPLE_CANCEL' then array['NEW','RELEASE']
          when 'REISSUE_CANCEL' then array['NEW']
          when 'POSITION_APPLY' then array['NEW','REMOVED']
          when 'PREPARE' then array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL']
          when 'PREPARATION_EXPIRY' then array['NEW','RELEASE']
          when 'PREPARATION_DISCARD' then array['NEW','RELEASE']
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='PREPARATION_DISCARD' and not v_claim_state.discard_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_WRITE_OFF' and (not v_claim_state.write_off_request_pending
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='SIMPLE_CANCEL'
             and (v_claim_state.available_at_utc>pg_catalog.clock_timestamp()
                  or not v_claim_state.cancel_request_pending)) then
        raise;
      end if;
      if v_claim_state.status='LEASED' and v_claim_state.lease_until_utc>pg_catalog.clock_timestamp() then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
      end if;
      if v_claim_state.status in ('READY','LEASED') and v_claim_state.predecessor_pending then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR';
      end if;
      if v_claim_state.status in ('READY','LEASED') and not v_claim_state.predecessor_pending
         and (v_claim_state.status='READY'
              or v_claim_state.lease_until_utc<=pg_catalog.clock_timestamp()) then
        -- The predecessor just completed or the short lease just expired.
        -- Only signal a fresh CLAIM; never invoke the owner a second time.
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_CLAIM_RETRY';
      end if;
      -- Other permanent scope/phase/request admission failures retain the
      -- exact original owner rejection, not a general automatic retry.
      raise;
    end;
    -- Only the owner-returned newly acquired token crosses this boundary.
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from v_claim.lease_nonce
       or v_job.owner_epoch is distinct from v_claim.owner_epoch then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_CLAIM_RESULT_INVALID';
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase,
      'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text,
      'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key);
    if p_job_kind='PREPARE' then
      v_output:=v_output||pg_catalog.jsonb_build_object(
        'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
        'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
        'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end);
    end if;
  else
    -- Do not reject cleared DONE leases here. The owners' scoped DONE replay
    -- branches are authoritative and return the immutable retained result.
    case p_job_kind
      when 'POSITION_APPLY' then
        select p.* into strict v_page from private.bpay_next_apply_position_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_phase,v_text_cursor,v_limit) p;
        v_result:=pg_catalog.jsonb_build_object('phase',v_page.next_phase,
          'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done);
      when 'PREPARE' then
        -- Terminal authority comes from the retained owner, never a fabricated
        -- READY row or an adopted active cursor/nonce. One owner per STEP.
        select private.bpay_next_preparation_expiry_fenced_v1(
          rc.run_id,rc.command_id) into v_expired_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        select private.bpay_next_preparation_discard_fenced_v1(rc.run_id,rc.command_id) into v_discarded_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        v_discarded_prepare:=coalesce(v_discarded_prepare,false) or(v_job.status='DONE' and v_job.phase='DISCARDED'
          and exists(select 1 from private.bpay_next_worker_reprepare_request x where x.previous_job_id=p_job_id
            and x.candidate_id=v_job.candidate_id and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'));
        if coalesce(v_discarded_prepare,false) then
          if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DISCARDING';end if;
          v_result:=private.bpay_next_discarded_prepare_receipt_v1(p_job_id);
        elsif coalesce(v_expired_prepare,false) then
          if v_job.status<>'DONE' then
            raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';
          end if;
          -- May have been retired before its first CLAIM: no worker is invented.
          -- Only the exact expiry owner validates and returns this old receipt.
          v_result:=private.bpay_next_expired_prepare_receipt_v1(p_job_id);
        elsif v_job.status='DONE' and v_job.phase='CANCELLED' then
          if not exists(select 1 from private.bpay_next_worker_reprepare_request x
            join private.bpay_next_run_worker w on w.id=x.run_worker_id
            where x.job_id=p_job_id and x.action='RELEASE' and x.status='COMPLETE'
              and x.target_preparation_revision=w.preparation_revision and w.status='CANCELLED'
              and x.module_epoch=v_job.module_epoch) then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
          v_result:=pg_catalog.jsonb_build_object('phase','CANCELLED','cursor',v_job.cursor_key,'rows_visited','0','replay',true);
        elsif v_job.status='DONE' and (exists(select 1 from private.bpay_next_case_allocation_state s
          join private.bpay_next_run_worker w on w.id=s.run_worker_id
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id
            and s.preparation_revision=w.captured_preparation_revision and s.pass_kind='DRAFT')
          or exists(select 1 from private.bpay_next_run_worker w
            where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id and w.status='REVIEW'
              and w.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
                'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT'))) then
          -- An original NEW/PINNING request has no page limit. Terminal
          -- read-back does no work, but the owner still requires a valid bound.
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce(v_limit,1));
        elsif v_job.status='DONE' and v_job.phase='READY' then
          v_result:=private.bpay_next_complete_no_case_worker_v1(p_job_id,v_nonce,v_owner_epoch);
        elsif v_job.status='DONE' and v_job.phase='DONE' then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,null);
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited','0');
        elsif v_phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then
          v_result:=private.bpay_next_worker_release_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
          v_result:=private.bpay_next_prepare_projection_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('NEW','PINNING') then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor);
          -- This owner examines at most one selected work. It exposes no page
          -- count, but its exact work/replay receipt identifies zero vs one.
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited',case
            when v_result->>'rows_visited' is not null then v_result->>'rows_visited'
            when v_result->>'replay'='true' then '0'
            when v_result->>'work_id' is not null then '1' else '0' end);
        elsif v_phase='RESERVE' then
          v_result:=private.bpay_next_capture_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='ALLOCATE' then
          v_result:=private.bpay_next_hold_fresh_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='FINANCE_ALLOCATE' then
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
        end if;
      when 'PAYE_NET_ENTRY' then
        if v_case_net then
          v_result:=private.bpay_next_apply_case_paye_net_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          v_result:=private.bpay_next_apply_simple_paye_net_v1(p_job_id,v_nonce,v_owner_epoch);
        end if;
      when 'TRANSFER_BUILD' then
        v_result:=private.bpay_next_build_simple_transfer_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'INTERNAL_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_RETURN' then
        v_result:=private.bpay_next_post_simple_return_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASH_REISSUE' then
        v_result:=private.bpay_next_build_simple_reissue_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'SIMPLE_CANCEL' then
        v_result:=private.bpay_next_cancel_simple_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'REISSUE_CANCEL' then
        v_result:=private.bpay_next_apply_reissue_cancel_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'PREPARATION_EXPIRY' then
        v_result:=private.bpay_next_expire_preparation_worker_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'PREPARATION_DISCARD' then
        v_result:=private.bpay_next_discard_preparation_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CASE_CREATE' then
        -- One exact original approval application. The retained 0330 owner
        -- alone governs lease validation and DONE/DONE immutable replay.
        v_result:=private.bpay_next_apply_case_create_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_WRITE_OFF' then
        v_result:=private.bpay_next_apply_write_off_v1(p_job_id,v_nonce,v_owner_epoch);
    end case;
    if v_defer_pin_module and not exists(
        select 1 from private.bpay_next_module_control m
        where m.id=1 and m.active_owner='NEXT' for share) then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
    end if;
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status not in ('LEASED','DONE')
       or (p_job_kind<>'POSITION_APPLY' and pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean')
       or (p_job_kind='POSITION_APPLY' and ((v_result->>'done')::boolean is distinct from (v_job.status='DONE')
            or v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
      or (p_job_kind='PREPARE' and v_result->>'phase' is distinct from
            case when v_discarded_prepare then 'DISCARDED' when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind in ('PREPARATION_EXPIRY','PREPARATION_DISCARD') and (v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
       or (p_job_kind='CASE_CREATE' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or v_result->>'phase' is distinct from 'CASE_CREATED'
            or not exists(select 1 from private.bpay_next_case_create_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.case_id::text=v_result->>'case_id')))
       or (p_job_kind='CASE_WRITE_OFF' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or not exists(select 1 from private.bpay_next_write_off_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW') and r.status=v_result->>'phase'
                and r.case_id::text=v_result->>'case_id' and r.case_component_id::text=v_result->>'case_component_id'
                and r.applied_amount is not distinct from (v_result->>'applied_amount')::numeric
                and r.remaining_amount is not distinct from (v_result->>'remaining_amount')::numeric
                and r.protected_amount is not distinct from (v_result->>'protected_amount')::numeric
                and r.event_id::text is not distinct from v_result->>'event_id'
                and r.issue_code is not distinct from v_result->>'issue_code')))
       or (v_case_net and (v_result->>'phase' is distinct from
            case when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end
          or v_result->>'cursor' is distinct from v_job.cursor_key
          or (v_result->>'run_worker_id')::uuid is distinct from
            (select r.run_worker_id from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id))) then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
    end if;
    -- Yield only this successfully validated page's own lease. The next ID
    -- notification takes a fresh CLAIM from the committed native checkpoint.
    if v_job.status='LEASED' then
      if v_job.lease_nonce is distinct from v_nonce or v_job.owner_epoch is distinct from v_owner_epoch then
        raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
      end if;
      update private.bpay_next_job set status='READY',lease_nonce=null,lease_until_utc=null
        where id=p_job_id and status='LEASED' and lease_nonce=v_nonce and owner_epoch=v_owner_epoch
        returning * into strict v_job;
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text,
      'lease_until_utc',v_lease_until,'done',v_job.status='DONE');
    if p_job_kind<>'POSITION_APPLY' then
      v_output:=v_output||pg_catalog.jsonb_build_object('replay',v_result->'replay');
    end if;
    case p_job_kind
      when 'POSITION_APPLY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited');
      when 'PREPARE' then
        if coalesce(v_expired_prepare,false) or coalesce(v_discarded_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase',case when v_discarded_prepare then 'DISCARDED' else 'EXPIRED' end,
            'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor',
            'component_cursor',v_result->>'component_cursor',
            'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status',
            'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id',
            'rows_visited',v_result->>'rows_visited');
        else
        select w.* into strict v_worker from private.bpay_next_run_worker w
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id;
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
          'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
          'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end,
          'run_worker_id',v_worker.id,'worker_status',v_worker.status,
          'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id,
          -- NULL means the exact owner did not report a count; do not fabricate
          -- zero for a REVIEW reached after earlier rows in the same page.
          'rows_visited',v_result->>'rows_visited');
        end if;
      when 'PAYE_NET_ENTRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','projection_id',v_result->>'projection_id');
        if v_case_net then
          v_output:=v_output||pg_catalog.jsonb_build_object(
            'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
            'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code');
        end if;
      when 'TRANSFER_BUILD' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id',
          'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'),
          'cursor',v_job.cursor_key,'member_count',v_result->>'member_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'INTERNAL_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_RETURN' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id');
      when 'CASH_REISSUE' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','transfer_id',v_result->>'transfer_id');
      when 'REISSUE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code');
      when 'SIMPLE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id',
          'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'),
          'holds_released',coalesce(v_result->>'holds_released','0'),
          'blocked_code',v_result->>'blocked_code');
      when 'PREPARATION_EXPIRY','PREPARATION_DISCARD' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
          'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released',
          'run_worker_id',v_result->>'run_worker_id');
      when 'CASE_CREATE' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id');
      when 'CASE_WRITE_OFF' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
          'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
          'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code');
    end case;
  end if;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_JOB_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function private.bpay_next_preparation_progress_v1(p_run_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_command private.bpay_next_command%rowtype;
 v_expiry private.bpay_next_preparation_expiry_request%rowtype;v_actor text;
 v_discard private.bpay_next_preparation_discard_request%rowtype;v_discard_actor text;
 v_pending bigint;v_preparing bigint;v_ready bigint;v_review bigint;v_removed bigint;
 v_enrolled bigint;v_enrolment_complete boolean;v_enrolment_failed boolean;
begin
 -- STABLE uses one calling snapshot for these point reads and never waits on
 -- the preparation writer's run lock. No status transition occurs on a read.
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id;
 select x.* into v_expiry from private.bpay_next_preparation_expiry_request x where x.run_id=p_run_id;
 select x.* into v_discard from private.bpay_next_preparation_discard_request x where x.run_id=p_run_id;
 select left(coalesce(nullif(btrim(u.display_name),''),'Unknown user'),250) into v_discard_actor from public.tms_users u where u.id=v_discard.actor_user_id;
 if (v_run.selection_state<>'SEALED' and not coalesce((v_run.selection_state='OPEN' and v_run.status='CANCELLED'
   and v_run.confirmed_at_utc is null and v_run.selected_candidate_count=0
   and v_expiry.status='EXPIRED' and v_expiry.expected_candidate_count=0 and v_expiry.enrolled_candidate_count=0
   and v_expiry.completed_candidate_count=0 and v_expiry.finished_at_utc is not null),false)
      and not coalesce((v_run.selection_state='OPEN' and v_run.confirmed_at_utc is null
        and v_discard.captured_selection_state='OPEN'
        and v_discard.selection_count=v_run.selection_count and v_discard.selection_page_count=v_run.selection_page_count
        and v_discard.expected_candidate_count=v_run.selected_candidate_count
        and v_run.review_revision=v_discard.selection_review_revision+1
        and ((v_discard.status='CANCELLING' and v_run.status='CANCELLING')
          or(v_discard.status in ('DISCARDED','PARTIAL') and v_discard.enrolled_candidate_count=v_discard.expected_candidate_count
            and v_discard.completed_candidate_count=v_discard.expected_candidate_count and v_discard.finished_at_utc is not null
            and v_run.status=case v_discard.status when 'DISCARDED' then 'CANCELLED' else 'REVIEW' end))
        and exists(select 1 from private.bpay_next_command c join private.bpay_next_module_control m
          on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
          where c.id=v_discard.command_id and c.command_kind='PREPARATION_DISCARD'
            and ((v_discard.status='CANCELLING' and c.status in ('ENROLLING','SEALED'))
              or(v_discard.status in ('DISCARDED','PARTIAL') and c.status='COMPLETE')))),false))
   or v_run.preparation_expires_at_utc is null then
  raise exception using errcode='55000',message='BPAY_NEXT_PROGRESS_NOT_ACCEPTED';end if;
 select c.* into v_command from private.bpay_next_run_command rc
  join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=p_run_id;
 if v_command.id is null and v_run.selection_state='OPEN' and not exists(select 1 from private.bpay_next_command c
   join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
   where(c.id=v_expiry.command_id and c.command_kind='PREPARATION_EXPIRY' or c.id=v_discard.command_id and c.command_kind='PREPARATION_DISCARD') and c.status='COMPLETE') then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if v_command.id is not null and not exists(select 1 from private.bpay_next_module_control m
   where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch) then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select coalesce(nullif(pg_catalog.btrim(u.display_name),''),'Unknown user') into v_actor
  from public.tms_users u where u.id=v_run.actor_user_id;
 v_actor:=pg_catalog.left(coalesce(v_actor,'Unknown user'),250);
 v_enrolled:=coalesce(v_command.enrolled_member_count,0);
 v_enrolment_complete:=coalesce(v_command.status in ('SEALED','COMPLETE')
   and v_command.expected_member_count=v_run.selected_candidate_count
   and v_enrolled=v_run.selected_candidate_count,false);
 v_enrolment_failed:=coalesce(v_command.status='FAILED' or v_command.enrollment_error_code is not null,false);
 v_ready:=v_run.ready_candidate_count;v_review:=v_run.review_candidate_count;
 v_removed:=v_run.cancelled_candidate_count;v_preparing:=v_run.preparing_candidate_count;
 if v_run.confirmed_at_utc is not null then
  v_pending:=0;v_preparing:=0;
 elsif v_discard.command_id is not null then
  v_ready:=0;v_review:=v_discard.blocked_candidate_count;
  v_pending:=v_discard.expected_candidate_count-v_discard.completed_candidate_count;
  v_preparing:=v_discard.enrolled_candidate_count-v_discard.completed_candidate_count;
  v_removed:=v_discard.removed_candidate_count+v_discard.already_removed_candidate_count;
  v_enrolled:=v_discard.enrolled_candidate_count;
  v_enrolment_complete:=v_discard.enrolled_candidate_count=v_discard.expected_candidate_count;
  v_enrolment_failed:=false;
 elsif v_run.status in ('CANCELLING','CANCELLED') then
  select x.* into v_expiry from private.bpay_next_preparation_expiry_request x where x.run_id=p_run_id;
  v_ready:=0;v_review:=0;
  if v_expiry.command_id is not null then
   v_pending:=v_expiry.expected_candidate_count-v_expiry.completed_candidate_count;
   v_preparing:=v_expiry.enrolled_candidate_count-v_expiry.completed_candidate_count;
   v_removed:=v_expiry.completed_candidate_count;
  else
   v_pending:=v_run.selected_candidate_count-v_removed;v_preparing:=0;
  end if;
 else
  v_pending:=v_run.selected_candidate_count-v_ready-v_review-v_removed;
 end if;
 if v_pending<0 or v_preparing<0 or v_preparing>v_pending or v_enrolled>v_run.selected_candidate_count
   or v_ready+v_review+v_removed+v_pending<>v_run.selected_candidate_count then
  raise exception using errcode='23514',message='BPAY_NEXT_PROGRESS_COUNTERS_INVALID';end if;
 return pg_catalog.jsonb_build_object('run_id',v_run.id,'status',v_run.status,
  'actor_display_name',v_actor,'started_at_utc',v_run.created_at_utc,'expires_at_utc',v_run.preparation_expires_at_utc,
  'selected_candidates',v_run.selected_candidate_count::text,'ready',v_ready::text,'review',v_review::text,
  'pending',v_pending::text,'preparing',v_preparing::text,'removed',v_removed::text,
  'enrolled_candidates',v_enrolled::text,'enrolment_complete',v_enrolment_complete,
  'enrolment_failed',v_enrolment_failed,'safe_to_close',true,
  'review_revision',v_run.review_revision::text,'discard_phase',v_discard.status,
  'discard_actor_display_name',v_discard_actor,'discarded_at_utc',v_discard.finished_at_utc,
  'discard_removed_candidates',coalesce(v_discard.removed_candidate_count,0)::text,
  'discard_already_removed_candidates',coalesce(v_discard.already_removed_candidate_count,0)::text,
  'discard_blocked_candidates',coalesce(v_discard.blocked_candidate_count,0)::text,
  'expired_at_utc',case when v_run.status='CANCELLED' and v_run.confirmed_at_utc is null and v_expiry.status='EXPIRED'
    and v_expiry.completed_candidate_count=v_expiry.expected_candidate_count and v_expiry.enrolled_candidate_count=v_expiry.expected_candidate_count
    then v_expiry.finished_at_utc else null end);
end
$function$;

create or replace function private.bpay_next_confirm_simple_run_v1(
  p_run_id uuid,p_review_revision bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_module_epoch bigint;
  v_command private.bpay_next_command%rowtype;
begin
  if p_run_id is null or p_review_revision is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CONFIRM_INPUT_INVALID';
  end if;
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.status='DRAFT' and v_run.review_revision=p_review_revision
     and v_run.confirmed_at_utc is not null then
    return pg_catalog.jsonb_build_object('phase','DRAFT','replay',true,
      'review_revision',v_run.review_revision,
      'drafts',private.bpay_next_channel_drafts_page_v1(p_run_id)->'rows');
  end if;
  if v_run.confirmed_at_utc is null and exists(select 1 from private.bpay_next_preparation_discard_request q where q.run_id=p_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CONFIRM_PREPARATION_DISCARDED_OR_BLOCKED';end if;
  if v_run.confirmed_at_utc is null and v_run.status in ('CANCELLING','CANCELLED')
    and exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND';end if;
  if v_run.status<>'REVIEW' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.review_revision<>p_review_revision
     or v_run.confirmed_at_utc is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_REVIEW_STATE_INVALID';
  end if;
  -- The real clock is read AFTER the header lock: a transaction started
  -- before the deadline cannot confirm late after waiting for that lock.
  -- The completed DRAFT replay above is permanent and never expires.
  if v_run.preparation_expires_at_utc is null
     or v_run.preparation_expires_at_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND';
  end if;
  select c.* into strict v_command
    from private.bpay_next_run_command rc
    join private.bpay_next_command c on c.id=rc.command_id
    where rc.run_id=p_run_id;
  if v_command.status<>'SEALED' or v_command.module_epoch<>v_module_epoch
     or v_command.expected_member_count<>v_run.selected_candidate_count
     or v_command.enrolled_member_count<>v_run.selected_candidate_count
     or exists (select 1 from private.bpay_next_job j
        where j.command_id=v_command.id and j.job_kind='PREPARE'
          and j.status<>'DONE')
     or exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_id=p_run_id and x.status<>'COMPLETE')
     or exists (
       select 1 from private.bpay_next_run_worker w
       where w.run_id=p_run_id and w.status<>'READY' and not (w.status='CANCELLED' and exists(select 1 from private.bpay_next_worker_reprepare_request x
        where x.run_worker_id=w.id and x.run_id=p_run_id and x.action='RELEASE' and x.status='COMPLETE'
          and x.target_preparation_revision=w.preparation_revision and x.captured_preparation_revision=w.captured_preparation_revision
          and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count))) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_UNRESOLVED_WORKER';
  end if;
  -- Each bounded holder transaction wrote its line, position and ACTIVE hold
  -- atomically before its worker became READY. Future cancellation/expiry
  -- owners must lock this parent and make a worker non-ready before release;
  -- no such owner is granted or activated in this narrow proof.
  update private.bpay_next_pay_run
    set status='DRAFT',confirmed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_run_id;
  return pg_catalog.jsonb_build_object('phase','DRAFT','replay',false,
    'review_revision',p_review_revision,
    'drafts',private.bpay_next_channel_drafts_page_v1(p_run_id)->'rows');
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
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
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
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

alter function private.bpay_next_preparation_discard_fenced_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_preparation_discard_fenced_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_finish_discard_member_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_finish_discard_member_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_preparation_discard_v1(uuid,uuid,uuid,bigint,text) owner to postgres;
revoke all on function private.bpay_next_accept_preparation_discard_v1(uuid,uuid,uuid,bigint,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_preparation_discard_one_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_preparation_discard_one_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_preparation_discard_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_preparation_discard_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_reconcile_discarded_work_collection_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_discarded_work_collection_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_discarded_prepare_receipt_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_discarded_prepare_receipt_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_release_scope_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_prepare_release_scope_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_exclude_preparation_week_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_exclude_preparation_week_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_expiry_guard_v1() owner to postgres;
revoke all on function private.bpay_next_preparation_expiry_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_discard_guard_v1() owner to postgres;
revoke all on function private.bpay_next_preparation_discard_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_worker_action_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_worker_action_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean) owner to postgres;
revoke all on function private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function private.bpay_next_preparation_progress_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_preparation_progress_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_confirm_simple_run_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_confirm_simple_run_v1(uuid,bigint) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
drop trigger if exists bpay_next_preparation_discard_guard_v1 on private.bpay_next_preparation_discard_request;
create trigger bpay_next_preparation_discard_guard_v1 before insert or update or delete
 on private.bpay_next_preparation_discard_request for each row execute function private.bpay_next_preparation_discard_guard_v1();
drop trigger if exists bpay_next_preparation_discard_guard_v1 on private.bpay_next_preparation_discard_worker;
create trigger bpay_next_preparation_discard_guard_v1 before insert or update or delete
 on private.bpay_next_preparation_discard_worker for each row execute function private.bpay_next_preparation_discard_guard_v1();


create or replace function public.bpay_next_preparation_expiry_due_v1(
  p_after_deadline timestamptz,p_after_run_id uuid,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_item record;
  v_items jsonb:='[]'::jsonb;
  v_last_deadline timestamptz;
  v_last_run_id uuid;
  v_now timestamptz:=pg_catalog.clock_timestamp();
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
      where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_limit is null or p_limit not between 1 and 100
     or (p_after_deadline is null) is distinct from (p_after_run_id is null)
     or (p_after_deadline is not null and not pg_catalog.isfinite(p_after_deadline)) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_SERVICE_INPUT_INVALID';
  end if;
  -- Partial due index + keyset cursor. At most 100 tiny identifiers are copied;
  -- never collect all due runs or lock their headers ahead of agency ordering.
  for v_item in
    select r.id,r.preparation_expires_at_utc
    from private.bpay_next_pay_run r
    where r.confirmed_at_utc is null and r.status in ('PREPARING','REVIEW')
      and r.preparation_expires_at_utc is not null
      and r.preparation_expires_at_utc<=v_now
      and (p_after_deadline is null or
        (r.preparation_expires_at_utc,r.id)>(p_after_deadline,p_after_run_id))
    order by r.preparation_expires_at_utc,r.id limit p_limit
  loop
    v_last_deadline:=v_item.preparation_expires_at_utc;
    v_last_run_id:=v_item.id;
    -- Filter only after the finite native due slice; cursor includes skipped rows.
    if exists(select 1 from private.bpay_next_preparation_discard_request q where q.run_id=v_item.id) then continue;end if;
    v_items:=v_items||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'run_id',v_item.id,'expected_deadline',pg_catalog.to_char(
        v_item.preparation_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
  end loop;
  return pg_catalog.jsonb_build_object('items',v_items,
    'cursor_deadline',pg_catalog.to_char(v_last_deadline at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'cursor_run_id',v_last_run_id);
end
$function$;

create or replace function private.bpay_next_accept_preparation_expiry_v1(
  p_command_id uuid,p_run_id uuid,p_actor_user_id uuid,p_expected_deadline timestamptz,
  p_observed_now timestamptz default null)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_prepare_command private.bpay_next_command%rowtype;
 v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_sequence bigint;v_prepare uuid;v_now timestamptz;
begin
  if p_command_id is null or p_run_id is null or p_expected_deadline is null
     or not pg_catalog.isfinite(p_expected_deadline)
     or (p_observed_now is not null and not pg_catalog.isfinite(p_observed_now)) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-preparation-expiry:'||p_command_id::text,0));
  select * into strict v_run from private.bpay_next_pay_run where id=p_run_id for update;
  select * into v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  if found then
    if v_request.run_id<>p_run_id or v_request.actor_user_id is distinct from p_actor_user_id
       or v_request.captured_deadline<>p_expected_deadline
       or not exists(select 1 from private.bpay_next_command where id=p_command_id
         and command_kind='PREPARATION_EXPIRY' and module_epoch=v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
      'phase',v_request.status,'expected_candidates',v_request.expected_candidate_count::text,
      'completed_candidates',v_request.completed_candidate_count::text,'replay',true);
  end if;
  -- Public wrappers MUST pass NULL, never a browser/request-supplied clock.
  -- Obtain real time after waiting for this same header as confirmation.
  -- The private ungranted operand permits labelled native 72h clock vectors.
  v_now:=coalesce(p_observed_now,pg_catalog.clock_timestamp());
  if p_actor_user_id is not null and not exists(select 1 from public.tms_users
      where id=p_actor_user_id and is_active is true and role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_ACTOR_FORBIDDEN';end if;
  if exists(select 1 from private.bpay_next_preparation_discard_request q where q.run_id=p_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_DISCARD_ALREADY_BOUND';end if;
  if v_run.confirmed_at_utc is not null or v_run.status not in ('PREPARING','REVIEW') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED';end if;
  if v_run.preparation_expires_at_utc is null then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND';end if;
  if v_run.preparation_expires_at_utc<>p_expected_deadline then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_CONFLICT';end if;
  if v_now<v_run.preparation_expires_at_utc then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_DUE';end if;
  if exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RUN_ALREADY_BOUND';end if;
  if exists(select 1 from private.bpay_next_worker_reprepare_request where run_id=p_run_id and action='RELEASE' and status<>'COMPLETE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_WORKER_RELEASE_PENDING';end if;
  select command_id into v_prepare from private.bpay_next_run_command where run_id=p_run_id;
if v_prepare is not null then
  select c.* into strict v_prepare_command
    from private.bpay_next_command c where c.id=v_prepare;
  if v_prepare_command.status='RECEIVED' then
    -- Original run UPDATE is already held. Enroller RECEIVED takes no run lock.
    select c.* into strict v_prepare_command
      from private.bpay_next_command c where c.id=v_prepare for update;
    if v_prepare_command.command_kind is distinct from 'PREPARE'
       or v_prepare_command.module_epoch is distinct from v_epoch
       or v_prepare_command.status is distinct from 'RECEIVED'
       or v_prepare_command.expected_member_count is not null
       or v_prepare_command.enrolled_member_count<>0
       or v_prepare_command.sealed_at_utc is not null
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_prepare
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=p_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
    update private.bpay_next_command
      set expected_member_count=0,status='COMPLETE',sealed_at_utc=v_now
      where id=v_prepare and status='RECEIVED';
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
  elsif not exists(select 1 from private.bpay_next_command
      where id=v_prepare and command_kind='PREPARE' and module_epoch=v_epoch
        and status in ('ENROLLING','SEALED')
        and expected_member_count=v_run.selected_candidate_count) then
    -- Original normal binding predicate, unchanged.
    raise exception using errcode='23514',
      message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
  end if;
end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARATION_EXPIRY');
  insert into private.bpay_next_preparation_expiry_request(command_id,run_id,original_prepare_command_id,
    captured_deadline,selection_count,selection_page_count,selection_review_revision,expected_candidate_count,
    initiator_kind,actor_user_id,accepted_at_utc,status,finished_at_utc)
    values(p_command_id,p_run_id,v_prepare,p_expected_deadline,v_run.selection_count,v_run.selection_page_count,v_run.review_revision,
      v_run.selected_candidate_count,case when p_actor_user_id is null then 'SYSTEM' else 'OFFICE' end,p_actor_user_id,v_now,
      case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
      case when v_run.selected_candidate_count=0 then v_now else null end);
  update private.bpay_next_pay_run set status=case when selected_candidate_count=0 then 'CANCELLED' else 'CANCELLING' end,
    review_revision=review_revision+1 where id=p_run_id;
  update private.bpay_next_command set expected_member_count=v_run.selected_candidate_count,
    status=case when v_run.selected_candidate_count=0 then 'COMPLETE' else 'ENROLLING' end,
    sealed_at_utc=case when v_run.selected_candidate_count=0 then v_now else null end where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
    'phase',case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
    'expected_candidates',v_run.selected_candidate_count::text,'completed_candidates','0','replay',false);
end
$function$;
alter function public.bpay_next_preparation_expiry_due_v1(timestamp with time zone,uuid,integer) owner to postgres;
revoke all on function public.bpay_next_preparation_expiry_due_v1(timestamp with time zone,uuid,integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_expiry_due_v1(timestamp with time zone,uuid,integer) to service_role;
alter function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone) from public,anon,authenticated,service_role;

create or replace function public.bpay_next_expiry_dispatch_page_v1(p_limit integer)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_cursor private.bpay_next_expiry_sweep_cursor%rowtype;
  v_page jsonb; v_item jsonb; v_items jsonb:='[]'::jsonb;
  v_deadline timestamptz; v_last_deadline timestamptz; v_last_run_id uuid;
  v_visited_deadline timestamptz;v_visited_run_id uuid;
  v_sweep_through timestamptz; v_finished boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_RUNTIME_SERVICE_INPUT_INVALID';
  end if;
  select c.* into strict v_cursor from private.bpay_next_expiry_sweep_cursor c where c.id=1 for update;
  -- One real-clock boundary per cycle, sampled after any lock wait. Newer
  -- arrivals cannot extend this cycle indefinitely and starve a lost page.
  v_sweep_through:=coalesce(v_cursor.sweep_through_utc,pg_catalog.clock_timestamp());
  v_page:=public.bpay_next_preparation_expiry_due_v1(v_cursor.after_deadline,v_cursor.after_run_id,p_limit);
  for v_item in select value from pg_catalog.jsonb_array_elements(v_page->'items') loop
    v_deadline:=(v_item->>'expected_deadline')::timestamptz;
    if v_deadline>v_sweep_through then
      v_finished:=true;
      exit;
    end if;
    v_items:=v_items||pg_catalog.jsonb_build_array(v_item);
    v_last_deadline:=v_deadline;
    v_last_run_id:=(v_item->>'run_id')::uuid;
  end loop;
  v_visited_deadline:=(v_page->>'cursor_deadline')::timestamptz;
  v_visited_run_id:=(v_page->>'cursor_run_id')::uuid;
  v_finished:=v_finished or v_visited_run_id is null or v_visited_deadline>v_sweep_through;
  -- Advance after this bounded read, not after every financial effect. If the
  -- caller stops before durable queue publication, the next completed sweep
  -- revisits the run. Older failed headers cannot starve later due headers.
  -- No financial evidence is marked received or completed by this cursor.
  update private.bpay_next_expiry_sweep_cursor set
    after_deadline=case when v_finished then null else v_visited_deadline end,
    after_run_id=case when v_finished then null else v_visited_run_id end,
    sweep_through_utc=case when v_finished then null else v_sweep_through end where id=1;
  -- Reply cursor describes only this returned page, even when the durable
  -- recovery cursor has restarted. No client controls that recovery cursor.
  return pg_catalog.jsonb_build_object('items',v_items,
    'cursor_deadline',pg_catalog.to_char(v_last_deadline at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'cursor_run_id',v_last_run_id);
end
$function$;
alter function public.bpay_next_expiry_dispatch_page_v1(integer) owner to postgres;
revoke all on function public.bpay_next_expiry_dispatch_page_v1(integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_expiry_dispatch_page_v1(integer) to service_role;

create or replace function private.bpay_next_project_worker_release_page_v1(
 p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_phase text,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_epoch bigint;v_candidate uuid;v_run_id uuid;v_job private.bpay_next_job%rowtype;
 v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
 v_binding private.bpay_next_worker_reprepare_request%rowtype;v_work private.bpay_next_work%rowtype;
 v_task private.bpay_next_workbench_task%rowtype;v_id uuid;v_wait boolean;v_work_ids uuid[];v_case_ids uuid[];
 v_seen integer:=0;v_cursor bigint;v_revision bigint;v_limit integer;v_phase text;v_done boolean:=false;
begin
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_worker_reprepare_request where job_id=p_job_id and action='RELEASE';
 select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
 perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
 select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
 select * into strict v_binding from private.bpay_next_worker_reprepare_request where job_id=p_job_id and action='RELEASE' and status='RUNNING' and stage='PROJECT' for update;
 select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;
 if p_limit is null or p_limit not between 1 and 100 or v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_run.confirmed_at_utc is not null
   or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
   or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
   or(select active_owner_epoch from private.bpay_next_worker_control where candidate_id=v_candidate) is distinct from p_owner_epoch
   or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_worker.status<>'CANCELLED'
   or v_worker.preparation_revision<>v_binding.target_preparation_revision or not private.bpay_next_prepare_job_active_v1(p_job_id) then
  raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
 if v_job.phase is distinct from p_expected_phase or v_job.cursor_key::bigint is distinct from p_expected_cursor then
  return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,'cursor',v_job.cursor_key,'rows_visited','0','replay',true);end if;
 v_phase:=v_job.phase;
  perform 1 from private.bpay_next_work where id=any(v_binding.pending_work_ids) order by id for update;
  for v_id in select unnest(v_binding.pending_work_ids) order by 1 loop
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_id and w.candidate_id=v_candidate;
    select t.* into strict v_task from private.bpay_next_workbench_task t where t.task_key='ORIGIN:'||v_id::text;
    if v_task.task_type<>'ORIGIN' or v_task.work_id is distinct from v_id or v_task.candidate_id is distinct from v_candidate then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_ORIGIN_SCOPE_INVALID';end if;
    if v_task.status='DONE' and(v_task.module_epoch,v_task.expected_revision_id,v_task.expected_applied_revision_id,
        v_task.expected_approval_state,v_task.financial_view_revision,v_task.expected_wait_state)
      is distinct from(v_epoch,v_work.current_revision_id,v_work.applied_revision_id,v_work.approval_state,
        (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),private.bpay_next_work_wait_state_v1(v_id)) then
      perform private.bpay_next_workbench_queue_origin_v1(v_id);
    end if;
  end loop;
  v_wait:=exists(select 1 from unnest(v_binding.pending_work_ids) x(id)
    join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE');
  -- Clear a completed retained set in its own checkpoint. The next page may
  -- revisit the same origin and enqueue a new generation; its guard must not
  -- confuse that new task with this already-completed page.
  if not v_wait and v_binding.pending_work_ids is not null then
    v_binding.pending_work_ids:=null;v_wait:=true;
  end if;
  if not v_wait then
    v_binding.pending_work_ids:=null;

  if v_phase='WORKBENCH_WORK' then
    select array_agg(x.work_id order by x.selection_no),count(*)::integer,max(x.selection_no::bigint) into v_work_ids,v_seen,v_cursor
      from(select work_id,selection_no from private.bpay_next_run_selection where run_id=v_run_id and candidate_id=v_candidate
        and selection_no>coalesce(v_binding.projection_work_cursor,0) order by selection_no limit p_limit) x;
    if v_seen>0 then v_binding.projection_work_cursor:=v_cursor;end if;
    if v_seen<p_limit then v_phase:='WORKBENCH_CASE';end if;
  elsif v_phase='WORKBENCH_CASE' then
    v_limit:=least(p_limit,50);v_revision:=nullif(v_binding.selection_revision,0);
    if v_revision is null then select selection_revision into v_revision from private.bpay_next_case_selection
      where run_id=v_run_id and candidate_id=v_candidate and status='SEALED' order by selection_revision desc limit 1;end if;
    select array_agg(distinct x.case_id),array_agg(distinct wc.work_id) filter(where wc.work_id is not null),count(*)::integer,max(x.selection_no::bigint)
      into v_case_ids,v_work_ids,v_seen,v_cursor from(select case_id,case_component_id,selection_no from private.bpay_next_case_selection_item
        where run_id=v_run_id and candidate_id=v_candidate and selection_revision=v_revision and is_selected
          and selection_no>coalesce(v_binding.projection_case_cursor,0) order by selection_no limit v_limit) x
      left join private.bpay_next_work_collection wc on wc.case_component_id=x.case_component_id and wc.case_id=x.case_id and wc.candidate_id=v_candidate;
    if v_seen>0 then v_binding.projection_case_cursor:=v_cursor;end if;
    if v_seen<v_limit then v_done:=true;end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_CHECKPOINT_INVALID';end if;
  if v_seen>0 then
    perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      select array_agg(x.id order by x.id) into v_binding.pending_work_ids from unnest(v_work_ids) x(id)
        join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE';
    if v_binding.pending_work_ids is not null then v_done:=false;end if;
  end if;
 end if;
 update private.bpay_next_worker_reprepare_request set checkpoint=checkpoint+1,
  projection_work_cursor=v_binding.projection_work_cursor,projection_case_cursor=v_binding.projection_case_cursor,pending_work_ids=v_binding.pending_work_ids,
  status=case when v_done then 'COMPLETE' else status end,stage=case when v_done then 'COMPLETE' else stage end,
  finished_at_utc=case when v_done then pg_catalog.clock_timestamp() else null end where job_id=p_job_id returning checkpoint into v_cursor;
 update private.bpay_next_job set phase=case when v_done then 'CANCELLED' else v_phase end,cursor_key=case when v_done then null else v_cursor::text end,
  status=case when v_done then 'DONE' else status end,lease_nonce=case when v_done then null else lease_nonce end,
  lease_until_utc=case when v_done then null else lease_until_utc end where id=p_job_id;
 if v_done then
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id and exists(select 1 from private.bpay_next_worker_reprepare_request
   where job_id=p_job_id and initiator_kind='OFFICE' and status='COMPLETE');
  perform private.bpay_next_review_simple_run_v1(v_run_id);
 end if;
 return pg_catalog.jsonb_build_object('done',v_done,'phase',case when v_done then 'CANCELLED' else v_phase end,
  'cursor',case when v_done then null else v_cursor::text end,'rows_visited',v_seen::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_accept_worker_action_v1(
  p_command_id uuid,p_run_worker_id uuid,p_action text,p_actor_user_id uuid,p_expected_revision bigint,p_system_job_id uuid default null
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_status text;v_confirmed timestamptz;v_deadline timestamptz;
  v_review bigint;v_selection text;v_selected bigint;v_revision bigint;v_data_revision bigint;v_worker_status text;
  v_input uuid;v_active uuid;v_prepare uuid;v_job_status text;v_job_kind text;v_job_command uuid;v_nonce uuid;v_owner bigint;
  v_job_module bigint;v_job_deadline timestamptz;v_control_owner bigint;v_phase text;v_cursor text;v_work_cursor uuid;v_component_cursor text;
  v_issue text;v_issue_work uuid;v_lines bigint;v_holds bigint;v_realised bigint;v_net_request bigint;v_net_projection bigint;
  v_selection_revision bigint;v_state uuid;v_sequence bigint;v_system boolean:=p_system_job_id is not null;
  v_old_actor uuid;v_old_action text;v_old_expected bigint;v_old_module bigint;v_found boolean;
begin
  if p_command_id is null or p_run_worker_id is null or p_expected_revision is null or p_expected_revision<1
    or p_expected_revision=9223372036854775807 or p_action is null or p_action not in ('REPREPARE','RELEASE')
    or (v_system and (p_action<>'RELEASE' or p_actor_user_id is not null)) or (not v_system and p_actor_user_id is null) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKER_ACTION_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_run_worker where id=p_run_worker_id;
  select status,confirmed_at_utc,preparation_expires_at_utc,review_revision,selection_state,selected_candidate_count
    into strict v_status,v_confirmed,v_deadline,v_review,v_selection,v_selected
    from private.bpay_next_pay_run where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-worker-action:'||p_command_id::text,0));
  select actor_user_id,action,expected_preparation_revision,module_epoch
    into v_old_actor,v_old_action,v_old_expected,v_old_module
    from private.bpay_next_worker_reprepare_request where command_id=p_command_id and run_worker_id=p_run_worker_id
      and expected_preparation_revision=p_expected_revision and initiator_kind=case when v_system then 'SYSTEM' else 'OFFICE' end;
  v_found:=found;
  if v_found then
    if (v_old_actor,v_old_action,v_old_expected,v_old_module) is distinct from
      (p_actor_user_id,p_action,p_expected_revision,v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_REPLAY_CONFLICT';end if;
    return private.bpay_next_worker_action_receipt_v1(p_command_id,p_run_worker_id,p_expected_revision,true);
  end if;
  if exists(select 1 from private.bpay_next_preparation_discard_request where run_id=v_run_id)
    or exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=v_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
  select command_id into strict v_prepare from private.bpay_next_run_command where run_id=v_run_id;
  select preparation_revision,captured_preparation_revision,status,input_prepare_job_id,active_prepare_job_id,
    review_issue_code,review_issue_work_id,captured_line_count,active_case_hold_count,realised_effect_count,
    net_request_revision,net_projection_revision,case_selection_revision
    into strict v_revision,v_data_revision,v_worker_status,v_input,v_active,v_issue,v_issue_work,v_lines,v_holds,v_realised,
      v_net_request,v_net_projection,v_selection_revision
    from private.bpay_next_run_worker where id=p_run_worker_id;
  if v_input is null then
    select id into strict v_input from private.bpay_next_job where command_id=v_prepare and candidate_id=v_candidate and job_kind='PREPARE';
  end if;
  v_active:=coalesce(v_active,v_input);
  select status,job_kind,command_id,lease_nonce,owner_epoch,module_epoch,lease_until_utc,
      retained_prepare_phase,retained_prepare_cursor,retained_prepare_work_cursor,retained_prepare_component_cursor
    into strict v_job_status,v_job_kind,v_job_command,v_nonce,v_owner,v_job_module,v_job_deadline,v_phase,v_cursor,v_work_cursor,v_component_cursor
    from private.bpay_next_job where id=v_active for update;
  perform 1 from private.bpay_next_run_worker where id=p_run_worker_id for update;
  select active_owner_epoch into strict v_control_owner from private.bpay_next_worker_control where candidate_id=v_candidate;
  if v_confirmed is not null or v_status not in ('PREPARING','REVIEW') or v_selection<>'SEALED'
    or v_revision<>p_expected_revision or v_realised<>0 or v_net_request<>0 or v_net_projection<>0
    or v_job_kind<>'PREPARE' or v_job_module<>v_epoch
    or exists(select 1 from private.bpay_next_transfer where run_worker_id=p_run_worker_id)
    or exists(select 1 from private.bpay_next_net_projection where run_worker_id=p_run_worker_id)
    or (not exists(select 1 from private.bpay_next_command c where c.id=v_prepare and c.command_kind='PREPARE'
      and c.module_epoch=v_epoch and c.status='SEALED' and c.expected_member_count=v_selected
      and c.enrolled_member_count=v_selected)
      and not(not v_system and p_action='RELEASE' and exists(select 1 from private.bpay_next_command c
        join private.bpay_next_command_member member on member.command_id=c.id and member.candidate_id=v_candidate
        join private.bpay_next_job j on j.id=v_input and j.command_id=c.id and j.candidate_id=member.candidate_id
          and j.job_kind='PREPARE' and j.module_epoch=c.module_epoch
        where c.id=v_prepare and c.command_kind='PREPARE' and c.module_epoch=v_epoch
          and c.status='ENROLLING' and c.expected_member_count=v_selected and member.member_no<=c.enrolled_member_count))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
  if v_system then
    if p_system_job_id<>v_active or p_command_id<>v_job_command or v_job_status<>'LEASED'
      or v_nonce is null or v_owner<>v_control_owner or v_job_deadline<=pg_catalog.clock_timestamp()
      or v_worker_status<>'REVIEW' or v_issue is null or v_issue not in ('SELECTED_WORK_NOT_APPROVED','SELECTED_WORK_REVISION_CHANGED',
        'SELECTED_WORK_POSITION_PENDING','SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE','CASE_NO_PAYABLE_AMOUNT') then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_SYSTEM_SCOPE_INVALID';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id;
  else
    if not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin' for share)
      or (p_action='REPREPARE' and v_job_status<>'DONE') or (p_action='REPREPARE' and (v_worker_status<>'REVIEW' or v_phase is null
        or v_deadline<=pg_catalog.clock_timestamp()))
      or (p_action='RELEASE' and (v_worker_status not in ('PREPARING','REVIEW','READY') or v_job_status not in ('READY','LEASED','BLOCKED','DONE')))
      or exists(select 1 from private.bpay_next_command where id=p_command_id)
      or exists(select 1 from private.bpay_next_worker_reprepare_request where run_worker_id=p_run_worker_id and status<>'COMPLETE') then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
    if p_action='RELEASE' and v_job_status<>'DONE' then
      if not private.bpay_next_prepare_job_active_v1(v_active) then
        raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
      update private.bpay_next_job set status='DONE',phase='DISCARDED',lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_CANDIDATE_PREPARATION_REMOVED' where id=v_active and status in ('READY','LEASED','BLOCKED');
      if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
    end if;
    v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  end if;
  select id into v_state from private.bpay_next_case_allocation_state where run_worker_id=p_run_worker_id
    and preparation_revision=v_data_revision and selection_revision=v_selection_revision and pass_kind='DRAFT' and projection_no=0;
  if v_system then
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=pg_catalog.clock_timestamp()
      where job_id=v_active and action='REPREPARE' and target_preparation_revision=v_revision and status<>'COMPLETE';
  end if;
  insert into private.bpay_next_worker_reprepare_request(command_id,run_worker_id,run_id,candidate_id,action,
    expected_preparation_revision,target_preparation_revision,captured_preparation_revision,original_prepare_command_id,
    input_prepare_job_id,previous_job_id,job_id,module_epoch,initiator_kind,actor_user_id,original_worker_status,
    original_issue_code,original_issue_work_id,accepted_review_revision,resume_phase,resume_cursor,resume_work_cursor,
    resume_component_cursor,selection_revision,draft_state_id,expected_work_count,expected_case_hold_count,status,stage)
  values(p_command_id,p_run_worker_id,v_run_id,v_candidate,p_action,v_revision,v_revision+1,v_data_revision,v_prepare,
    v_input,v_active,case when v_system then v_active else null end,v_epoch,case when v_system then 'SYSTEM' else 'OFFICE' end,
    p_actor_user_id,case when v_system then 'PREPARING' else v_worker_status end,v_issue,v_issue_work,v_review+1,
    case when p_action='REPREPARE' then v_phase else null end,case when p_action='REPREPARE' then v_cursor else null end,
    case when p_action='REPREPARE' then v_work_cursor else null end,case when p_action='REPREPARE' then v_component_cursor else null end,
    v_selection_revision,v_state,v_lines,v_holds,case when v_system then 'RUNNING' else 'ACCEPTED' end,
    case when v_system then 'WORK' else 'NEW' end);
  -- Readiness and target revision are invalidated before any hold mutation.
  update private.bpay_next_pay_run set status='PREPARING',review_revision=review_revision+1,
    preparing_candidate_count=preparing_candidate_count+case when v_system or v_worker_status='PREPARING' then 0 else 1 end,
    ready_candidate_count=ready_candidate_count-case when not v_system and v_worker_status='READY' then 1 else 0 end,
    review_candidate_count=review_candidate_count-case when not v_system and v_worker_status='REVIEW' then 1 else 0 end
    where id=v_run_id;
  update private.bpay_next_run_worker set preparation_revision=preparation_revision+1,input_prepare_job_id=v_input,
    active_prepare_job_id=case when v_system then v_active else null end,
    status=case when p_action='REPREPARE' then 'PREPARING' else 'CANCELLING' end,
    review_issue_code=null,review_issue_work_id=null,
    financial_resolution_count=financial_resolution_count-case when p_action='REPREPARE' and v_issue like 'CASE\_%' escape '\' then 1 else 0 end,
    case_pending_binding_count=case when p_action='REPREPARE' then 0 else case_pending_binding_count end
    where id=p_run_worker_id;
  if p_action='REPREPARE' and v_state is not null then
    update private.bpay_next_case_allocation_state set prepare_stage=retained_prepare_stage,
      weekly_binding_state=retained_weekly_binding_state where id=v_state and prepare_stage='REVIEW'
        and retained_prepare_stage is not null and retained_weekly_binding_state is not null;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RETRY_CHECKPOINT_MISSING';end if;
  end if;
  if v_system then
    update private.bpay_next_job set phase='WORKER_RELEASE_WORK',cursor_key=null,position_work_cursor=null,position_component_cursor=null where id=v_active;
  else
    update private.bpay_next_command set expected_member_count=1,status='ENROLLING' where id=p_command_id;
  end if;
  return private.bpay_next_worker_action_receipt_v1(p_command_id,p_run_worker_id,p_expected_revision,false);
end
$function$;

create or replace function private.bpay_next_worker_release_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_phase text,p_expected_checkpoint bigint,p_limit integer
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run uuid;v_candidate uuid;v_worker uuid;v_revision bigint;v_control bigint;
  v_job record;v_binding record;v_line record;v_case_hold record;v_ids uuid[];v_case_ids uuid[];
  v_work_ids uuid[];v_lock_cases uuid[];v_id uuid;v_work private.bpay_next_work%rowtype;v_task private.bpay_next_workbench_task%rowtype;v_wait boolean;v_limit integer;
  v_seen integer:=0;v_now timestamptz;v_stage text;v_cursor bigint;v_done boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
    or p_expected_phase is null or p_expected_phase not in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL')
    or p_limit is null or p_limit not between 1 and 100 or (p_expected_checkpoint is not null and p_expected_checkpoint<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKER_ACTION_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select x.run_id,x.candidate_id,x.run_worker_id into strict v_run,v_candidate,v_worker
    from private.bpay_next_worker_reprepare_request x join private.bpay_next_run_worker w
      on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
    where x.job_id=p_job_id and x.action='RELEASE' and x.status='RUNNING';
  perform 1 from private.bpay_next_pay_run where id=v_run and status='PREPARING' and confirmed_at_utc is null for update;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
  select active_owner_epoch into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select id,command_id,job_kind,status,phase,cursor_key,module_epoch,owner_epoch,lease_nonce,lease_until_utc
    into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select x.command_id,x.run_worker_id,x.target_preparation_revision,x.captured_preparation_revision,x.selection_revision,
    x.status,x.stage,x.expected_work_count,x.expected_case_hold_count,x.released_work_count,x.released_case_hold_count,
    x.work_cursor,x.case_hold_cursor,x.checkpoint,x.accepted_at_utc,x.original_worker_status,x.pending_work_ids
    into strict v_binding from private.bpay_next_worker_reprepare_request x
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
    where x.job_id=p_job_id and x.action='RELEASE' and x.status='RUNNING' for update of x;
  select preparation_revision into strict v_revision from private.bpay_next_run_worker where id=v_worker and status='CANCELLING' for update;
  if v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
    or v_job.owner_epoch<>p_owner_epoch or v_control<>p_owner_epoch or v_job.lease_nonce<>p_lease_nonce
    or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_revision<>v_binding.target_preparation_revision
    or not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_job.phase is distinct from p_expected_phase or v_binding.checkpoint is distinct from coalesce(p_expected_checkpoint,0) then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,'cursor',v_job.cursor_key,'rows_visited','0','replay',true);
  end if;
  if v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_CHECKPOINT_INVALID';end if;
  v_now:=greatest(pg_catalog.clock_timestamp(),v_binding.accepted_at_utc);v_stage:=v_binding.stage;
  perform 1 from private.bpay_next_work where id=any(v_binding.pending_work_ids) order by id for update;
  for v_id in select unnest(v_binding.pending_work_ids) order by 1 loop
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_id and w.candidate_id=v_candidate;
    select t.* into strict v_task from private.bpay_next_workbench_task t where t.task_key='ORIGIN:'||v_id::text;
    if v_task.task_type<>'ORIGIN' or v_task.work_id is distinct from v_id or v_task.candidate_id is distinct from v_candidate then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_ORIGIN_SCOPE_INVALID';end if;
    if v_task.status='DONE' and(v_task.module_epoch,v_task.expected_revision_id,v_task.expected_applied_revision_id,
        v_task.expected_approval_state,v_task.financial_view_revision,v_task.expected_wait_state)
      is distinct from(v_epoch,v_work.current_revision_id,v_work.applied_revision_id,v_work.approval_state,
        (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),private.bpay_next_work_wait_state_v1(v_id)) then
      perform private.bpay_next_workbench_queue_origin_v1(v_id);
    end if;
  end loop;
  v_wait:=exists(select 1 from unnest(v_binding.pending_work_ids) x(id)
    join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE');
  -- Clear a completed retained set in its own checkpoint. The next page may
  -- revisit the same origin and enqueue a new generation; its guard must not
  -- confuse that new task with this already-completed page.
  if not v_wait and v_binding.pending_work_ids is not null then
    v_binding.pending_work_ids:=null;v_wait:=true;
  end if;
  if not v_wait then
    v_binding.pending_work_ids:=null;
  if v_stage='WORK' and v_job.phase='WORKER_RELEASE_WORK' then
    select array_agg(distinct x.work_id) into v_work_ids from(select work_id from private.bpay_next_run_line
      where run_worker_id=v_worker and line_no>coalesce(v_binding.work_cursor,0) order by line_no,id limit p_limit) x;
    perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
    for v_line in select id,line_no,work_id from private.bpay_next_run_line where run_worker_id=v_worker
      and line_no>coalesce(v_binding.work_cursor,0) order by line_no,id limit p_limit loop
      perform private.bpay_next_release_prepare_work_hold_v1(p_job_id,v_line.id,v_now);
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
      v_ids:=pg_catalog.array_append(v_ids,v_line.work_id);
    end loop;
    if v_seen>0 then
      update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
      perform private.bpay_next_workbench_refresh_many_v1(v_ids,null);
      select array_agg(x.id order by x.id) into v_binding.pending_work_ids from unnest(v_work_ids) x(id)
        join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE';
    end if;
    if not exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker and line_no>coalesce(v_binding.work_cursor,0)) then
      if v_binding.released_work_count<>v_binding.expected_work_count then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_COUNT_INVALID';end if;
      v_stage:='CASE';
    end if;
  elsif v_stage='CASE' and v_job.phase='WORKER_RELEASE_CASE' then
    v_limit:=least(p_limit,50);
    select array_agg(x.id order by x.id),array_agg(distinct x.case_id),array_agg(distinct wc.work_id) filter(where wc.work_id is not null)
      into v_ids,v_case_ids,v_work_ids from(select id,case_id,instruction_id from private.bpay_next_case_hold
        where run_worker_id=v_worker and status='ACTIVE'
          and(v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit v_limit) x
      join private.bpay_next_run_case_instruction i on i.id=x.instruction_id and i.candidate_id=v_candidate
      left join private.bpay_next_work_collection wc on wc.id=i.work_collection_id and wc.candidate_id=v_candidate and wc.case_id=x.case_id;
    select array_agg(x.id order by x.id) into v_lock_cases from(select distinct id from unnest(v_case_ids) id
      union select wc.case_id from private.bpay_next_work_collection wc where wc.work_id=any(v_work_ids) and wc.case_id is not null
      order by id limit 101) x;
    if coalesce(cardinality(v_work_ids),0)+coalesce(cardinality(v_lock_cases),0)>100 then v_lock_cases:=v_case_ids;end if;
    perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
    perform 1 from private.bpay_next_finance_case where id=any(v_lock_cases) order by id for update;
    for v_case_hold in select id,case_id from private.bpay_next_case_hold where id=any(v_ids) order by id loop
      perform private.bpay_next_release_prepare_case_hold_v1(p_job_id,v_case_hold.id,v_now);
      v_binding.case_hold_cursor:=v_case_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;
      v_seen:=v_seen+1;
    end loop;
    if v_seen>0 then
      update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
      perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      select array_agg(x.id order by x.id) into v_binding.pending_work_ids from unnest(v_work_ids) x(id)
        join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE';
    end if;
    if not exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker and status='ACTIVE') then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
        or exists(select 1 from private.bpay_next_run_worker where id=v_worker and active_case_hold_count<>0) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COUNT_INVALID';end if;
      v_stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CURSOR_INVALID';end if;
  elsif v_stage='FINAL' and v_job.phase='WORKER_RELEASE_FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
      or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker and status='ACTIVE')
      or exists(select 1 from private.bpay_next_hold h join private.bpay_next_run_line l on l.id=h.run_line_id
        where l.run_worker_id=v_worker and h.status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FINAL_COUNT_INVALID';end if;
    update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker;
    update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
    perform private.bpay_next_exclude_preparation_week_v1(p_job_id,v_worker);
    update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count-1,
      cancelled_candidate_count=cancelled_candidate_count+1 where id=v_run;
    v_stage:='PROJECT';v_seen:=1;
  else raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_CHECKPOINT_INVALID';end if;
  end if;
  update private.bpay_next_worker_reprepare_request set stage=v_stage,checkpoint=checkpoint+1,pending_work_ids=v_binding.pending_work_ids,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count
    where command_id=v_binding.command_id and run_worker_id=v_worker and target_preparation_revision=v_revision
    returning checkpoint into v_cursor;
  update private.bpay_next_job set phase=case v_stage when 'WORK' then 'WORKER_RELEASE_WORK'
      when 'CASE' then 'WORKER_RELEASE_CASE' when 'FINAL' then 'WORKER_RELEASE_FINAL' else 'WORKBENCH_WORK' end,
    cursor_key=case when v_stage='PROJECT' then null else v_cursor::text end,
    position_work_cursor=null,position_component_cursor=null where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase',case v_stage when 'WORK' then 'WORKER_RELEASE_WORK'
    when 'CASE' then 'WORKER_RELEASE_CASE' when 'FINAL' then 'WORKER_RELEASE_FINAL' else 'WORKBENCH_WORK' end,
    'cursor',case when v_stage='PROJECT' then null else v_cursor::text end,'rows_visited',v_seen::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_worker_action_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_IMMUTABLE';end if;
  if tg_op='UPDATE' then
    if (pg_catalog.to_jsonb(new)-array['job_id','status','stage','released_work_count','released_case_hold_count',
        'work_cursor','case_hold_cursor','checkpoint','finished_at_utc','pending_work_ids','projection_work_cursor','projection_case_cursor']) is distinct from
       (pg_catalog.to_jsonb(old)-array['job_id','status','stage','released_work_count','released_case_hold_count',
        'work_cursor','case_hold_cursor','checkpoint','finished_at_utc','pending_work_ids','projection_work_cursor','projection_case_cursor'])
      or (old.job_id is not null and new.job_id is distinct from old.job_id)
      or old.status='COMPLETE' or new.released_work_count<old.released_work_count
      or new.released_case_hold_count<old.released_case_hold_count or new.checkpoint<old.checkpoint
      or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
      or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
      or (old.projection_work_cursor is not null and(new.projection_work_cursor is null or new.projection_work_cursor<old.projection_work_cursor))
      or (old.projection_case_cursor is not null and(new.projection_case_cursor is null or new.projection_case_cursor<old.projection_case_cursor))
      or (exists(select 1 from unnest(old.pending_work_ids) x(id) join private.bpay_next_workbench_task t
          on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE')
        and(pg_catalog.to_jsonb(new)-'checkpoint') is distinct from(pg_catalog.to_jsonb(old)-'checkpoint'))
      or (new.status='COMPLETE' and new.pending_work_ids is not null)
      or array_position(array['NEW','WORK','CASE','FINAL','PROJECT','COMPLETE'],new.stage)
        <array_position(array['NEW','WORK','CASE','FINAL','PROJECT','COMPLETE'],old.stage) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_IMMUTABLE';end if;
  end if;
  if not exists(select 1 from private.bpay_next_job input_job
      join private.bpay_next_run_command rc on rc.command_id=input_job.command_id and rc.run_id=new.run_id
      join private.bpay_next_command command on command.id=new.command_id and command.command_kind='PREPARE'
        and command.module_epoch=new.module_epoch
      where input_job.id=new.input_prepare_job_id and input_job.candidate_id=new.candidate_id
        and input_job.job_kind='PREPARE' and input_job.module_epoch=new.module_epoch
        and rc.command_id=new.original_prepare_command_id)
    or (new.job_id is not null and not exists(select 1 from private.bpay_next_job j
      where j.id=new.job_id and j.command_id=new.command_id and j.candidate_id=new.candidate_id
        and j.module_epoch=new.module_epoch and j.job_kind='PREPARE')) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_BINDING_INVALID';end if;
  return new;
end
$function$;

create or replace function private.bpay_next_prepare_projection_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_phase text,p_expected_cursor bigint,p_limit integer
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_module_epoch bigint;v_run_id uuid;
  v_ids uuid[];v_seen integer;v_cursor bigint;v_revision bigint;v_phase text;v_done boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
    or p_expected_phase is null or p_limit is null
    or p_expected_phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') or p_limit not between 1 and 100
    or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if exists(select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=p_job_id and x.action='RELEASE'
    and x.status='RUNNING' and x.stage='PROJECT') then
    return private.bpay_next_project_worker_release_page_v1(p_job_id,p_lease_nonce,p_owner_epoch,p_expected_phase,p_expected_cursor,p_limit);
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  select private.bpay_next_prepare_job_run_v1(p_job_id) into strict v_run_id;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_job.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_job.candidate_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
    or private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_job.command_id)
    or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED' or v_job.module_epoch<>v_module_epoch
    or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
    or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_worker.status not in ('READY','REVIEW','CANCELLED') then
    raise exception using errcode='55000',message='BPAY_NEXT_PIN_LEASE_STALE';
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id)
    or (v_worker.status='CANCELLED' and not exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.job_id=p_job_id and x.run_worker_id=v_worker.id and x.target_preparation_revision=v_worker.preparation_revision
        and x.action='RELEASE' and x.status='RUNNING' and x.stage='PROJECT')) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_job.phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_PIN_PHASE_INVALID';
  end if;
  if v_job.phase is distinct from p_expected_phase or v_job.cursor_key::bigint is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,'cursor',v_job.cursor_key,'rows_visited','0','replay',true);
  end if;
  v_phase:=v_job.phase;
  if v_phase='WORKBENCH_WORK' then
    select pg_catalog.array_agg(p.work_id order by p.selection_no),count(*)::integer,max(p.selection_no)
      into v_ids,v_seen,v_cursor from (select s.work_id,s.selection_no from private.bpay_next_run_selection s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id
          and s.selection_no>coalesce(p_expected_cursor,0) order by s.selection_no limit p_limit) p;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(v_ids,null);end if;
    if v_seen<p_limit then v_phase:='WORKBENCH_CASE';v_cursor:=null;end if;
  else
    v_revision:=nullif(v_worker.case_selection_revision,0);
    if v_revision is null then
      select s.selection_revision into v_revision from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id and s.status='SEALED'
        order by s.selection_revision desc limit 1;
    elsif not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id
        and s.candidate_id=v_job.candidate_id and s.selection_revision=v_revision and s.status='SEALED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_REQUIRED';
    end if;
    select pg_catalog.array_agg(distinct p.case_id),count(*)::integer,max(p.selection_no)
      into v_ids,v_seen,v_cursor from (select s.case_id,s.selection_no from private.bpay_next_case_selection_item s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id and s.selection_revision=v_revision
          and s.selection_no>coalesce(p_expected_cursor,0) order by s.selection_no limit p_limit) p;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(null,v_ids);end if;
    if v_seen<p_limit then
      v_done:=true;v_phase:=case when v_worker.status='READY' then 'READY' when v_worker.status='CANCELLED' then 'CANCELLED' else 'DONE' end;v_cursor:=null;
    end if;
  end if;
  update private.bpay_next_job set phase=v_phase,cursor_key=v_cursor::text,
    status=case when v_done then 'DONE' else 'LEASED' end,
    lease_nonce=case when v_done then null else lease_nonce end,
    lease_until_utc=case when v_done then null else lease_until_utc end where id=p_job_id;
  if v_done then
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=pg_catalog.clock_timestamp()
      where job_id=p_job_id and run_worker_id=v_worker.id and target_preparation_revision=v_worker.preparation_revision and status='RUNNING';
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id and exists(
      select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=p_job_id and x.initiator_kind='OFFICE' and x.status='COMPLETE');
    perform private.bpay_next_review_simple_run_v1(v_run_id);
  end if;
  return pg_catalog.jsonb_build_object('done',v_done,'phase',case when v_phase='DONE' then 'REVIEW' else v_phase end,
    'cursor',v_cursor::text,'rows_visited',v_seen::text,'replay',false);
end
$function$;
alter function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) owner to postgres;
revoke all on function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_action_guard_v1() owner to postgres;
revoke all on function private.bpay_next_worker_action_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
