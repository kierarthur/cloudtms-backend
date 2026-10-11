-- DR08 exact current0559/0708 merges; bounded expiry projection, original money/RuleB retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_expire_preparation_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_checkpoint bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_candidate uuid;v_seen integer:=0;v_work_holds integer:=0;v_case_released integer:=0;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_binding private.bpay_next_preparation_expiry_worker%rowtype;v_worker private.bpay_next_run_worker%rowtype;
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
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_binding from private.bpay_next_preparation_expiry_worker where job_id=p_job_id for update;
  if v_binding.run_worker_id is not null then select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;end if;
  if v_job.job_kind<>'PREPARATION_EXPIRY' or v_job.module_epoch<>v_epoch or v_job.command_id<>v_binding.command_id
     or v_job.candidate_id<>v_candidate or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command c join private.bpay_next_preparation_expiry_request x on x.command_id=c.id
       where c.id=v_job.command_id and c.command_kind='PREPARATION_EXPIRY' and c.agency_sequence=v_job.command_sequence
         and c.module_epoch=v_epoch and x.run_id=v_run_id and x.captured_deadline=v_run.preparation_expires_at_utc
         and x.expected_candidate_count=c.expected_member_count and x.enrolled_candidate_count=c.enrolled_member_count
         and v_run.review_revision=x.selection_review_revision+1) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_PAGE_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='EXPIRED' and v_binding.status='EXPIRED' and v_binding.stage='COMPLETE' then
    return pg_catalog.jsonb_build_object('phase','EXPIRED','cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_job.status<>'LEASED'
     or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_checkpoint,0)<>v_binding.checkpoint then
    if coalesce(p_expected_checkpoint,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_CURSOR_AHEAD';end if;
    return pg_catalog.jsonb_build_object('phase','RELEASE','cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  -- Never put completion before its accepted receipt, including labelled
  -- owner-only simulated-clock vectors. Public receipts use the real clock.
  v_now:=greatest(pg_catalog.clock_timestamp(),(select accepted_at_utc from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id));
  if v_binding.status='READY' then
    if v_job.phase<>'NEW' or v_binding.stage<>'NEW' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_START_INVALID';end if;
    if v_worker.id is not null and not v_binding.already_worker_released then
      if v_worker.status<>v_binding.original_worker_status or v_worker.captured_line_count<>v_binding.expected_work_count
         or v_worker.active_case_hold_count<>v_binding.expected_case_hold_count or v_worker.captured_preparation_revision<>v_binding.preparation_revision
         or v_worker.preparation_revision<>coalesce(v_binding.expected_execution_revision,v_binding.preparation_revision)
         or v_worker.case_selection_revision<>v_binding.selection_revision or v_worker.realised_effect_count<>0
         or v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
         or exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id)
         or exists(select 1 from private.bpay_next_net_projection where run_worker_id=v_worker.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_GROUP_NOT_UNCONFIRMED';end if;
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
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FENCE_BROKEN';end if;
  -- Every retained page's existing ORIGIN writer must finish before another
  -- expiry page changes the Candidate financial revision or claims completion.
  perform 1 from private.bpay_next_work where id=any(v_binding.pending_work_ids) order by id for update;
  for v_id in select unnest(v_binding.pending_work_ids) order by 1 loop
    select w.* into strict v_work from private.bpay_next_work w where w.id=v_id and w.candidate_id=v_candidate;
    select t.* into strict v_task from private.bpay_next_workbench_task t where t.task_key='ORIGIN:'||v_id::text;
    if v_task.task_type<>'ORIGIN' or v_task.work_id is distinct from v_id or v_task.candidate_id is distinct from v_candidate then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ORIGIN_SCOPE_INVALID';end if;
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
        (select accepted_at_utc from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id))
       or v_binding.expected_work_count<>0 or v_binding.expected_case_hold_count<>0) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RELEASE_RECEIPT_INVALID';end if;
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
        if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_COUNT_INVALID';end if;
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
          raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COUNT_INVALID';end if;
        v_binding.stage:='PROJECT_WORK';
      elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CURSOR_INVALID';end if;
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
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FINAL_COUNT_INVALID';end if;
      if v_worker.id is not null and not v_binding.already_worker_released then update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;end if;
      -- Preserve the original final financial-view invalidation for native
      -- worker-week exclusion. All origin projection has finished; this is
      -- not a wait/selection page and changes no Ready offer inputs.
      v_financial_changed:=not v_binding.already_worker_released;
      v_binding.stage:='COMPLETE';v_binding.status:='EXPIRED';v_binding.finished_at_utc:=v_now;v_seen:=1;
    else raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_STAGE_INVALID';end if;
    if v_financial_changed then
      update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
    end if;
    if coalesce(cardinality(v_work_ids),0)+coalesce(cardinality(v_case_ids),0)>0 then
      perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      select array_agg(x.id order by x.id) into v_binding.pending_work_ids from unnest(v_work_ids) x(id)
        join private.bpay_next_workbench_task t on t.task_key='ORIGIN:'||x.id::text where t.status<>'DONE';
    end if;
  end if;
  update private.bpay_next_preparation_expiry_worker set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,started_at_utc=v_binding.started_at_utc,
    finished_at_utc=v_binding.finished_at_utc,projection_work_cursor=v_binding.projection_work_cursor,
    projection_case_cursor=v_binding.projection_case_cursor,pending_work_ids=v_binding.pending_work_ids
    where job_id=p_job_id returning checkpoint into v_checkpoint;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+interval '120 seconds' where id=p_job_id;
  if v_binding.status='EXPIRED' then
    if not v_binding.already_worker_released then perform private.bpay_next_apply_expired_week_v1(p_job_id);end if;
    update private.bpay_next_job set status='DONE',phase='EXPIRED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_preparation_expiry_request set completed_candidate_count=completed_candidate_count+1,
      status=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count then 'EXPIRED' else status end,
      finished_at_utc=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count then v_now else null end
      where command_id=v_binding.command_id and completed_candidate_count<enrolled_candidate_count;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_PARENT_COUNT_INVALID';end if;
    if not v_binding.already_worker_released then
      update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1
        where id=v_run_id and cancelled_candidate_count<selected_candidate_count;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RUN_COUNT_INVALID';end if;
    end if;
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=v_now
      where run_worker_id=v_binding.run_worker_id and target_preparation_revision=v_binding.expected_execution_revision
        and action='REPREPARE' and status<>'COMPLETE';
    update private.bpay_next_pay_run set status=case when exists(select 1 from private.bpay_next_preparation_expiry_request
        where command_id=v_binding.command_id and status='EXPIRED') then 'CANCELLED' else 'CANCELLING' end where id=v_run_id;
    if exists(select 1 from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id and status='EXPIRED') then
      update private.bpay_next_command set status='COMPLETE' where id=v_binding.command_id;end if;
  end if;
  return pg_catalog.jsonb_build_object('phase',case when v_binding.status='EXPIRED' then 'EXPIRED' else 'RELEASE' end,
    'cursor',v_checkpoint::text,'rows_visited',v_seen::text,'work_released',v_work_holds::text,
    'case_holds_released',v_case_released::text,'run_worker_id',v_binding.run_worker_id,'replay',false);
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
    if new.status='CANCELLING' or (tg_op='UPDATE' and old.status='CANCELLING') then
      if new.confirmed_at_utc is not null or new.status not in ('CANCELLING','CANCELLED')
         or not exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=new.id
           and captured_deadline=new.preparation_expires_at_utc and selection_count=new.selection_count
           and selection_page_count=new.selection_page_count and expected_candidate_count=new.selected_candidate_count
           and new.review_revision=selection_review_revision+1) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_HEADER_FENCE_INVALID';end if;
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

create or replace function private.bpay_next_preparation_progress_v1(p_run_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_command private.bpay_next_command%rowtype;
 v_expiry private.bpay_next_preparation_expiry_request%rowtype;v_actor text;
 v_pending bigint;v_preparing bigint;v_ready bigint;v_review bigint;v_removed bigint;
 v_enrolled bigint;v_enrolment_complete boolean;v_enrolment_failed boolean;
begin
 -- STABLE uses one calling snapshot for these point reads and never waits on
 -- the preparation writer's run lock. No status transition occurs on a read.
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id;
 select x.* into v_expiry from private.bpay_next_preparation_expiry_request x where x.run_id=p_run_id;
 if (v_run.selection_state<>'SEALED' and not coalesce((v_run.selection_state='OPEN' and v_run.status='CANCELLED'
   and v_run.confirmed_at_utc is null and v_run.selected_candidate_count=0
   and v_expiry.status='EXPIRED' and v_expiry.expected_candidate_count=0 and v_expiry.enrolled_candidate_count=0
   and v_expiry.completed_candidate_count=0 and v_expiry.finished_at_utc is not null),false))
   or v_run.preparation_expires_at_utc is null then
  raise exception using errcode='55000',message='BPAY_NEXT_PROGRESS_NOT_ACCEPTED';end if;
 select c.* into v_command from private.bpay_next_run_command rc
  join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=p_run_id;
 if v_command.id is null and v_run.selection_state='OPEN' and not exists(select 1 from private.bpay_next_command c
   join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
   where c.id=v_expiry.command_id and c.command_kind='PREPARATION_EXPIRY' and c.status='COMPLETE') then
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
    else
      perform private.bpay_next_reconcile_worker_released_collection_v1(p_job_id,v_case_hold.id);
    end if;
  end if;
end
$function$;

alter function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_expiry_guard_v1() owner to postgres;
revoke all on function private.bpay_next_preparation_expiry_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_progress_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_preparation_progress_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_confirm_simple_run_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_confirm_simple_run_v1(uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
