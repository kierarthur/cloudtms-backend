-- DR09 source correction: explicit native row aliases only; installed predecessor retained.
\set ON_ERROR_STOP on
begin;
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
  select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for share nowait;
  select dr09_row.* into strict v_request from private.bpay_next_preparation_discard_request dr09_row where command_id=p_command_id for update;
  select dr09_row.* into strict v_command from private.bpay_next_command dr09_row where id=p_command_id for update;
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
  select dr09_row.* into v_worker from private.bpay_next_run_worker dr09_row where run_id=v_run_id and candidate_id=v_candidate;
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
  select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for update;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select dr09_row.* into strict v_job from private.bpay_next_job dr09_row where id=p_job_id for update;
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
  select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for update;
  select dr09_row.* into strict v_control from private.bpay_next_worker_control dr09_row where candidate_id=v_candidate for update;
  select dr09_row.* into strict v_job from private.bpay_next_job dr09_row where id=p_job_id for update;
  select dr09_row.* into strict v_binding from private.bpay_next_preparation_discard_worker dr09_row where job_id=p_job_id for update;
  if v_binding.run_worker_id is not null then select dr09_row.* into strict v_worker from private.bpay_next_run_worker dr09_row where id=v_binding.run_worker_id for update;end if;
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

create or replace function private.bpay_next_discarded_prepare_receipt_v1(p_job_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_request record;v_worker private.bpay_next_run_worker%rowtype;
  v_binding private.bpay_next_preparation_discard_worker%rowtype;
begin
  select dr09_row.* into strict v_job from private.bpay_next_job dr09_row where id=p_job_id and job_kind='PREPARE';
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
    select dr09_row.* into strict v_worker from private.bpay_next_run_worker dr09_row where run_id=v_run.id and candidate_id=v_job.candidate_id;
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
  select dr09_row.* into v_worker from private.bpay_next_run_worker dr09_row where run_id=v_run.id and candidate_id=v_job.candidate_id;
  select dr09_row.* into v_binding from private.bpay_next_preparation_discard_worker dr09_row where command_id=v_request.command_id and candidate_id=v_job.candidate_id;
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
      select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=new.run_id;
      if v_run.status not in ('PREPARING','REVIEW') or v_run.confirmed_at_utc is not null
         or new.captured_deadline is distinct from v_run.preparation_expires_at_utc
         or (new.selection_count,new.selection_page_count,new.selection_review_revision,new.expected_candidate_count)
           is distinct from (v_run.selection_count,v_run.selection_page_count,v_run.review_revision,v_run.selected_candidate_count)
         or new.enrolled_candidate_count<>0 or new.completed_candidate_count<>0
         or not exists(select 1 from private.bpay_next_command where id=new.command_id and command_kind='PREPARATION_EXPIRY' and status='RECEIVED')
         or new.original_prepare_command_id is distinct from (select command_id from private.bpay_next_run_command where run_id=new.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_INITIAL_INVALID';end if;
    else
      select dr09_row.* into strict v_request from private.bpay_next_preparation_expiry_request dr09_row where command_id=new.command_id;
      select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=new.run_id;
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
      select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=new.run_id;
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
      select dr09_row.* into strict v_request from private.bpay_next_preparation_discard_request dr09_row where command_id=new.command_id;
      select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=new.run_id;
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
  select dr09_row.* into v_command from private.bpay_next_command dr09_row where agency_sequence=v_last+1 for update;
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
    select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for share nowait;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
  select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
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
  select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=p_run_id for update;
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
  select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=p_run_id for update;
  select dr09_row.* into v_request from private.bpay_next_preparation_expiry_request dr09_row where command_id=p_command_id;
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
 select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for update;
 perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
 select dr09_row.* into strict v_job from private.bpay_next_job dr09_row where id=p_job_id for update;
 select dr09_row.* into strict v_binding from private.bpay_next_worker_reprepare_request dr09_row where job_id=p_job_id and action='RELEASE' and status='RUNNING' and stage='PROJECT' for update;
 select dr09_row.* into strict v_worker from private.bpay_next_run_worker dr09_row where id=v_binding.run_worker_id for update;
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
commit;
