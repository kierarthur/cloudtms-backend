-- Installed DR11 predecessor retained: five explicit expiry row aliases only.
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
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select dr11_row.* into strict v_run from private.bpay_next_pay_run dr11_row where id=v_run_id for update nowait;
  select dr11_row.* into strict v_control from private.bpay_next_worker_control dr11_row where candidate_id=v_candidate for update nowait;
  select dr11_row.* into strict v_job from private.bpay_next_job dr11_row where id=p_job_id for update nowait;
  select dr11_row.* into strict v_binding from private.bpay_next_preparation_expiry_worker dr11_row where job_id=p_job_id for update nowait;
  if v_binding.run_worker_id is not null then select dr11_row.* into strict v_worker from private.bpay_next_run_worker dr11_row where id=v_binding.run_worker_id for update nowait;end if;
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
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
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
commit;
