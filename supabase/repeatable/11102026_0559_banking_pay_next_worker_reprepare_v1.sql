-- DR05: exact retained-input retry, typed worker release, shared native hold mechanics.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_prepare_job_run_v1(p_job_id uuid)
returns uuid language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select coalesce(rc.run_id,x.run_id) from private.bpay_next_job j
    left join private.bpay_next_run_command rc on rc.command_id=j.command_id
    left join lateral (select q.run_id from private.bpay_next_worker_reprepare_request q
      where q.job_id=j.id and q.command_id=j.command_id and q.candidate_id=j.candidate_id
        and q.module_epoch=j.module_epoch order by q.target_preparation_revision desc limit 1) x on true
    where j.id=p_job_id and j.job_kind='PREPARE'
$function$;

create or replace function private.bpay_next_prepare_job_active_v1(p_job_id uuid)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_run_worker w on w.run_id=private.bpay_next_prepare_job_run_v1(j.id) and w.candidate_id=j.candidate_id
    left join private.bpay_next_worker_reprepare_request x on x.job_id=j.id and x.target_preparation_revision=w.preparation_revision
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where j.id=p_job_id and j.job_kind='PREPARE'
      and ((x.job_id is null and w.preparation_revision=1 and
          (w.active_prepare_job_id is null or w.active_prepare_job_id=j.id))
        or (x.job_id=j.id and x.run_worker_id=w.id and x.target_preparation_revision=w.preparation_revision
          and x.captured_preparation_revision=w.captured_preparation_revision and w.active_prepare_job_id=j.id
          and x.status<>'COMPLETE')))
$function$;

create or replace function private.bpay_next_worker_action_receipt_v1(p_command_id uuid,p_run_worker_id uuid,p_expected_revision bigint,p_replay boolean)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select pg_catalog.jsonb_build_object('command_id',x.command_id,'run_id',x.run_id,'run_worker_id',x.run_worker_id,
    'action',x.action,'expected_preparation_revision',x.expected_preparation_revision::text,
    'preparation_revision',x.target_preparation_revision::text,'review_revision',x.accepted_review_revision::text,
    'agency_sequence',c.agency_sequence::text,'phase',case when private.bpay_next_preparation_expiry_fenced_v1(x.run_id,x.original_prepare_command_id) then 'EXPIRED' else x.status end,'replay',p_replay)
    from private.bpay_next_worker_reprepare_request x join private.bpay_next_command c on c.id=x.command_id
    where x.command_id=p_command_id and x.run_worker_id=p_run_worker_id and x.expected_preparation_revision=p_expected_revision
      order by x.target_preparation_revision desc limit 1
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
    or not exists(select 1 from private.bpay_next_command c where c.id=v_prepare and c.command_kind='PREPARE'
      and c.module_epoch=v_epoch and c.status='SEALED' and c.expected_member_count=v_selected
      and c.enrolled_member_count=v_selected) then
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
      or v_job_status<>'DONE' or (p_action='REPREPARE' and (v_worker_status<>'REVIEW' or v_phase is null
        or v_deadline<=pg_catalog.clock_timestamp()))
      or (p_action='RELEASE' and v_worker_status not in ('REVIEW','READY'))
      or exists(select 1 from private.bpay_next_command where id=p_command_id)
      or exists(select 1 from private.bpay_next_worker_reprepare_request where run_worker_id=p_run_worker_id and status<>'COMPLETE') then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
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
    preparing_candidate_count=preparing_candidate_count+case when v_system then 0 else 1 end,
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
      and x.job_id is null and private.bpay_next_preparation_expiry_fenced_v1(x.run_id,x.original_prepare_command_id)) then
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


create or replace function private.bpay_next_worker_action_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_IMMUTABLE';end if;
  if tg_op='UPDATE' then
    if (pg_catalog.to_jsonb(new)-array['job_id','status','stage','released_work_count','released_case_hold_count',
        'work_cursor','case_hold_cursor','checkpoint','finished_at_utc']) is distinct from
       (pg_catalog.to_jsonb(old)-array['job_id','status','stage','released_work_count','released_case_hold_count',
        'work_cursor','case_hold_cursor','checkpoint','finished_at_utc'])
      or (old.job_id is not null and new.job_id is distinct from old.job_id)
      or old.status='COMPLETE' or new.released_work_count<old.released_work_count
      or new.released_case_hold_count<old.released_case_hold_count or new.checkpoint<old.checkpoint
      or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
      or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
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
drop trigger if exists bpay_next_worker_action_guard_v1 on private.bpay_next_worker_reprepare_request;
create trigger bpay_next_worker_action_guard_v1 before insert or update or delete
  on private.bpay_next_worker_reprepare_request for each row execute function private.bpay_next_worker_action_guard_v1();

-- Native typed admission common to the two release mechanics, not a due-date override.
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
              and ((x.status='RELEASING' and x.stage=p_stage) or (p_stage='WORK' and x.status='READY' and x.stage='NEW'))))))
$function$;

create or replace function private.bpay_next_release_prepare_work_hold_v1(p_job_id uuid,p_run_line_id uuid,p_now timestamptz)
returns boolean language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_line record;v_hold record;
begin
  select id,run_worker_id,work_id,component_key,source_consumed_ex_vat,frozen_ex_vat,frozen_vat,frozen_inc_vat
    into strict v_line from private.bpay_next_run_line where id=p_run_line_id;
  if p_now is null or not pg_catalog.isfinite(p_now)
    or not private.bpay_next_prepare_release_scope_v1(p_job_id,v_line.run_worker_id,'WORK') then
    raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_RELEASE_SCOPE_INVALID';end if;
  select id,status,work_id,component_key,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat
    into v_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
  if found then
    if v_hold.status<>'ACTIVE' or v_hold.work_id<>v_line.work_id or v_hold.component_key<>v_line.component_key
      or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
        is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_HOLD_NOT_EXACT';end if;
    update private.bpay_next_position set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
      held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,held_target_vat=held_target_vat-v_hold.target_amount_vat,
      held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,updated_at_utc=p_now
      where work_id=v_hold.work_id and component_key=v_hold.component_key and held_source_ex_vat>=v_hold.source_reserved_ex_vat
        and held_target_ex_vat>=v_hold.target_amount_ex_vat and held_target_vat>=v_hold.target_amount_vat
        and held_target_inc_vat>=v_hold.target_amount_inc_vat;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_POSITION_MISMATCH';end if;
    update private.bpay_next_hold set status='RELEASED',finished_at_utc=p_now where id=v_hold.id;
    return true;
  elsif (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
    is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_POSITIVE_HOLD_MISSING';end if;
  return false;
end
$function$;

create or replace function private.bpay_next_worker_released_collection_scope_v1(p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_worker_reprepare_request x on x.job_id=j.id and x.action='RELEASE' and x.status='RUNNING' and x.stage='CASE'
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.preparation_revision=x.captured_preparation_revision and s.selection_revision=x.selection_revision
      and s.pass_kind='DRAFT' and s.projection_no=0 and s.module_epoch=x.module_epoch
      and private.bpay_next_prepare_job_run_v1(s.job_id)=x.run_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.case_id=b.case_id and i.case_component_id=b.case_component_id
      and i.work_collection_id=b.id and i.preparation_revision=x.captured_preparation_revision and i.selection_revision=x.selection_revision
      and i.valuation_policy_id=b.original_valuation_policy_id
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id and h.run_worker_id=w.id
      and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and private.bpay_next_prepare_release_scope_v1(j.id,w.id,'CASE')
      and i.case_kind='OVERPAYMENT' and i.tax_treatment='TAXABLE' and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and i.payroll_stage='GROSS_DEDUCT' and i.hold_purpose='GROSS_RECOVERY'
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=x.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and x.released_case_hold_count<x.expected_case_hold_count and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat))
$function$;

create or replace function private.bpay_next_reconcile_worker_released_collection_v1(p_job_id uuid,p_case_hold_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_work uuid;v_component text;
begin
  select b.id,b.work_id,b.component_key into strict v_id,v_work,v_component from private.bpay_next_case_hold h
    join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
    join private.bpay_next_work_collection b on b.id=i.work_collection_id and b.case_id=h.case_id
      and b.case_component_id=h.case_component_id and b.candidate_id=h.candidate_id where h.id=p_case_hold_id;
  if not private.bpay_next_worker_released_collection_scope_v1(p_job_id,v_id,p_case_hold_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_WORKER_RELEASE_SCOPE_INVALID';end if;
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_work,v_component,null);
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
  select run_worker_id,preparation_revision,selection_revision,work_collection_id into strict v_instruction from private.bpay_next_run_case_instruction where id=v_case_hold.instruction_id;
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


create or replace function private.bpay_next_worker_release_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_phase text,p_expected_checkpoint bigint,p_limit integer
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run uuid;v_candidate uuid;v_worker uuid;v_revision bigint;v_control bigint;
  v_job record;v_binding record;v_line record;v_case_hold record;v_ids uuid[];v_case_ids uuid[];
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
    x.work_cursor,x.case_hold_cursor,x.checkpoint,x.accepted_at_utc,x.original_worker_status
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
  if v_stage='WORK' and v_job.phase='WORKER_RELEASE_WORK' then
    for v_line in select id,line_no,work_id from private.bpay_next_run_line where run_worker_id=v_worker
      and line_no>coalesce(v_binding.work_cursor,0) order by line_no,id limit p_limit loop
      perform private.bpay_next_release_prepare_work_hold_v1(p_job_id,v_line.id,v_now);
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
      v_ids:=pg_catalog.array_append(v_ids,v_line.work_id);
    end loop;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(v_ids,null);end if;
    if not exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker and line_no>coalesce(v_binding.work_cursor,0)) then
      if v_binding.released_work_count<>v_binding.expected_work_count then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_COUNT_INVALID';end if;
      v_stage:='CASE';
    end if;
  elsif v_stage='CASE' and v_job.phase='WORKER_RELEASE_CASE' then
    for v_case_hold in select id,case_id from private.bpay_next_case_hold where run_worker_id=v_worker and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      perform private.bpay_next_release_prepare_case_hold_v1(p_job_id,v_case_hold.id,v_now);
      v_binding.case_hold_cursor:=v_case_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;
      v_seen:=v_seen+1;v_case_ids:=pg_catalog.array_append(v_case_ids,v_case_hold.case_id);
    end loop;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(null,v_case_ids);end if;
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
    perform private.bpay_next_exclude_preparation_week_v1(p_job_id,v_worker);
    update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count-1,
      cancelled_candidate_count=cancelled_candidate_count+1 where id=v_run;
    v_stage:='PROJECT';v_seen:=1;
  else raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_CHECKPOINT_INVALID';end if;
  update private.bpay_next_worker_reprepare_request set stage=v_stage,checkpoint=checkpoint+1,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count
    where command_id=v_binding.command_id and run_worker_id=v_worker and target_preparation_revision=v_revision
    returning checkpoint into v_cursor;
  update private.bpay_next_job set phase=case v_stage when 'WORK' then 'WORKER_RELEASE_WORK'
      when 'CASE' then 'WORKER_RELEASE_CASE' when 'FINAL' then 'WORKER_RELEASE_FINAL' else 'WORKBENCH_WORK' end,
    cursor_key=case when v_stage='PROJECT' then null else v_cursor::text end,
    position_work_cursor=null,position_component_cursor=null where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
  return pg_catalog.jsonb_build_object('done',false,'phase',case v_stage when 'WORK' then 'WORKER_RELEASE_WORK'
    when 'CASE' then 'WORKER_RELEASE_CASE' when 'FINAL' then 'WORKER_RELEASE_FINAL' else 'WORKBENCH_WORK' end,
    'cursor',case when v_stage='PROJECT' then null else v_cursor::text end,'rows_visited',v_seen::text,'replay',false);
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


-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_claim_prepare_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run_id uuid;
  v_module_epoch bigint;
  v_channel text;
  v_umbrella_id uuid;
  v_umbrella_vat_chargeable boolean;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.candidate_id,private.bpay_next_prepare_job_run_v1(j.id) into strict v_job.candidate_id,v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_job.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.module_epoch<>v_module_epoch
     or not exists(select 1 from private.bpay_next_command command
       where command.id=v_job.command_id and command.agency_sequence=v_job.command_sequence
         and command.module_epoch=v_module_epoch and command.command_kind='PREPARE'
         and command.status='SEALED' and command.sealed_at_utc is not null
         and command.expected_member_count=case when exists(select 1 from private.bpay_next_worker_reprepare_request x
           where x.job_id=v_job.id and x.initiator_kind='OFFICE') then 1 else v_run.selected_candidate_count end
         and command.enrolled_member_count=command.expected_member_count)
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE';
  end if;
  if exists(select 1 from private.bpay_next_job earlier
      where earlier.candidate_id=v_job.candidate_id
        and earlier.command_sequence<v_job.command_sequence
        and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  select * into v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not found then
    if v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
    end if;
    select selected.target_pay_channel,selected.target_umbrella_id,selected.target_umbrella_vat_chargeable
      into strict v_channel,v_umbrella_id,v_umbrella_vat_chargeable
      from private.bpay_next_selection_candidate selected
      where selected.run_id=v_run_id and selected.candidate_id=v_job.candidate_id;
    if v_channel not in ('PAYE','UMBRELLA') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_PAY_CHANNEL_UNSUPPORTED';
    end if;
    insert into private.bpay_next_run_worker
      (run_id,candidate_id,target_pay_channel,target_umbrella_id,
       target_umbrella_vat_chargeable,status,input_prepare_job_id,active_prepare_job_id)
      values(v_run_id,v_job.candidate_id,v_channel,
        case when v_channel='UMBRELLA' then v_umbrella_id else null end,
        case when v_channel='UMBRELLA' then v_umbrella_vat_chargeable
          else null end,'PREPARING',p_job_id,p_job_id)
      returning * into v_worker;
    update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count+1 where id=v_run_id;
  elsif not ((v_job.phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE') and v_worker.status='PREPARING')
    or (v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and v_worker.status in ('READY','REVIEW','CANCELLED'))
    or (v_job.phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') and v_worker.status='CANCELLING')) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.preparation_revision=1 and v_worker.active_prepare_job_id is null then
    update private.bpay_next_run_worker set input_prepare_job_id=p_job_id,active_prepare_job_id=p_job_id where id=v_worker.id;
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()
          +pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_pin_selected_work_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker_id uuid;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_run_selection%rowtype;
  v_choice private.bpay_next_work_choice%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_current_cursor bigint;
  v_module_epoch bigint;
  v_query_admitted boolean:=false;
  v_source_classified boolean;
  v_source_origin record;
  v_query_gate jsonb;
  v_query_scope jsonb;
  v_query_disposition text;
  v_query_digest bytea;
  v_run_work_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PIN_INPUT_INVALID';
  end if;
  -- Authorised direct callers use the same outer entry as 0280. Discover
  -- without a job/module/run lock; do not newly admit retained replay/stale
  -- inputs. This read never substitutes for the original locked owner checks.
  select j.* into strict v_job from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select private.bpay_next_prepare_job_run_v1(p_job_id) into strict v_run_id;
  if v_run_id is null then raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_RUN_UNBOUND';end if;
  if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
     and v_job.lease_nonce is not distinct from p_lease_nonce
     and v_job.owner_epoch is not distinct from p_owner_epoch
     and v_job.lease_until_utc>pg_catalog.clock_timestamp()
     and v_job.cursor_key::bigint is not distinct from p_expected_cursor then
    perform private.weekly_source_pay_query_admit_v2();
    v_query_admitted:=true;
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.status='DONE' and v_job.phase='DONE'
     and v_job.job_kind='PREPARE' and v_job.owner_epoch=p_owner_epoch then
    select * into strict v_worker from private.bpay_next_run_worker
      where run_id=v_run_id and candidate_id=v_job.candidate_id;
    if v_worker.status='REVIEW' and v_worker.review_issue_code is not null then
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_worker.review_issue_code,
        'work_id',v_worker.review_issue_work_id,'replay',true);
    end if;
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.status<>'LEASED' or v_job.job_kind<>'PREPARE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_PIN_LEASE_STALE';
  end if;
  if v_job.phase='RESERVE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',true);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_job.phase not in ('NEW','PINNING') then
    raise exception using errcode='23514',message='BPAY_NEXT_PIN_PHASE_INVALID';
  end if;
  v_current_cursor:=v_job.cursor_key::bigint;
  if v_current_cursor is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,
      'next_cursor',v_current_cursor,'replay',true);
  end if;
  if not v_query_admitted then
    -- A changed discovery cannot become a fresh mutation after Bank locks.
    -- Roll back and retry the original STEP via the existing 40001 channel;
    -- never acquire the Source key late, fabricate REVIEW or adopt a nonce.
    raise exception using errcode='40001',message='BPAY_NEXT_PIN_ADMISSION_CHANGED';
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  v_worker_id:=v_worker.id;
  select * into v_selection from private.bpay_next_run_selection
    where run_id=v_run_id and candidate_id=v_job.candidate_id
      and selection_no>coalesce(v_current_cursor,0)
    order by selection_no limit 1;
  if not found then
    update private.bpay_next_job set phase='RESERVE',cursor_key=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',false);
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=v_selection.work_id for share;
  if v_work.candidate_id<>v_job.candidate_id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_CANDIDATE_MISMATCH';
  end if;
  select * into strict v_choice from private.bpay_next_work_choice
    where run_id=v_run_id and work_id=v_work.id;
  if v_choice.selection_state<>'SEALED' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  if v_work.approval_state<>'APPROVED'
     or v_work.current_revision_id is null
     or v_choice.expected_revision_id is null
     or v_choice.expected_revision_id is distinct from v_work.current_revision_id
     or v_work.current_revision_id is distinct from v_work.applied_revision_id then
    -- A newly published revision may be waiting behind this PREPARE command.
    -- End only this worker's non-executable attempt so its later position job
    -- can run. Do not silently omit the work or retain an executable offer.
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code=case
        when v_work.approval_state<>'APPROVED' or v_work.current_revision_id is null
          or v_choice.expected_revision_id is null
          then 'SELECTED_WORK_NOT_APPROVED'
        when v_choice.expected_revision_id is distinct from v_work.current_revision_id
          then 'SELECTED_WORK_REVISION_CHANGED'
        else 'SELECTED_WORK_POSITION_PENDING' end,
        review_issue_work_id=v_work.id
      where id=v_worker_id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,1);
  end if;
  select * into strict v_revision from private.bpay_next_work_revision
    where id=v_choice.expected_revision_id and work_id=v_work.id for share;
  if v_revision.sealed_at_utc is null or v_revision.approved_at_utc is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_APPROVAL_NOT_SEALED';
  end if;
  if v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED') then
    -- Exact point qualification of the approved captured origin. No I3
    -- financial-completion reader, inventory/history rebuild or live price.
    -- Initial authorised TSFIN, committed HEAD and genuine headless direct
    -- NEXT are distinct lanes; none may manufacture the other's authority.
    -- Evaluate the actual classifier as its own statement even for self-bill:
    -- OR/CASE short-circuiting must not hide its permission/integrity errors.
    -- Current domain facts qualify a returned Scope, not immutable provenance
    -- when Source explicitly reports that the current scope is unavailable.
    v_source_classified:=private._candidate_expense_source_family_v1(v_revision.physical_timesheet_id);
    select t.timesheet_id,t.version,t.booking_id,c.client_id,
           family.id as family_id,family.bound_version,
           (t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
            and t.contract_id=v_work.contract_id and c.candidate_id=v_work.candidate_id
            and t.week_ending_date=v_work.week_ending_date
            and t.version=v_revision.physical_timesheet_version
            and t.booking_id=v_work.booking_id and c.client_id is not null
            and v_revision.week_ending_date=v_work.week_ending_date) as root_exact,
           (t.line_type='HOURS'::public.timesheet_line_type_enum and not t.is_adjustment
            and (c.self_bill is true or v_source_classified is true)
            and cw.timesheet_id=t.timesheet_id and not cw.is_adjustment
            and (family.id is null or (family.root_timesheet_id=t.timesheet_id
              and family.root_family_booking_id=t.booking_id
              and family.candidate_id=v_work.candidate_id
              and family.contract_id=v_work.contract_id
              and family.week_ending_date=v_work.week_ending_date
              and family.bound_version>0))) as scope_exact,
           case when v_revision.source_head_id is not null then
             h.id=v_revision.source_event_id and h.root_timesheet_id=t.timesheet_id
             and h.root_timesheet_version=t.version and h.root_family_booking_id=t.booking_id
             and h.candidate_id=v_work.candidate_id and h.contract_id=v_work.contract_id
             and h.week_ending_date=v_work.week_ending_date and h.state in ('COMMITTED_CURRENT','SUPERSEDED')
             and h.committed_at_utc is not null and pg_catalog.isfinite(h.committed_at_utc)
             and h.authority_kind=case when v_revision.source_kind='PROTECTED'
               then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end
           when v_revision.source_kind='SOURCE' then
             auth.id=v_revision.source_event_id and auth.root_timesheet_id=t.timesheet_id
             and auth.family_booking_id=t.booking_id and auth.timesheet_version=t.version
             and auth.authorised_at_utc is not null and pg_catalog.isfinite(auth.authorised_at_utc)
             and tf.id=v_revision.financial_snapshot_id and tf.timesheet_id=t.timesheet_id
             and tf.timesheet_version=t.version and tf.candidate_id=v_work.candidate_id
             and tf.client_id=c.client_id and tf.authorised_at_utc is not null
             and pg_catalog.isfinite(tf.authorised_at_utc)
           else
             v_revision.source_head_id is null and v_revision.financial_snapshot_id is null
             and receipt.approval_id=v_revision.source_event_id
             and receipt.work_id=v_work.id and receipt.revision_id=v_revision.id
             and receipt.command_id=publication.command_id and receipt.family_id=family.id
             and receipt.agency_sequence=command.agency_sequence
             and receipt.accepted_family_bound_version<=family.bound_version
             and approval.creation_orchestration_run_id=receipt.orchestration_run_id
             and approval.pay_target_family_id=family.id and approval.candidate_id=v_work.candidate_id
             and approval.client_id=c.client_id and approval.contract_id=v_work.contract_id
             and approval.week_ending=v_work.week_ending_date
             and generation.family_id=family.id and generation.lifecycle_state in ('PUBLISHED','SUPERSEDED')
             and generation.published_at_utc is not null
             and generation.result_hash=receipt.publication_request_sha256
             and orchestration.family_id=family.id and orchestration.state='COMPLETE'
             and orchestration.requested_by_user_id=receipt.actor_user_id
             and orchestration.request_fingerprint=receipt.prepared_request_sha256
             and orchestration.after_state_fingerprint=receipt.publication_request_sha256
             and orchestration.completed_at_utc is not null
           end as origin_exact,
           case when v_revision.source_head_id is not null then
             h.state='COMMITTED_CURRENT' and live_auth.current_entitlement_head_id=h.id
           when v_revision.source_kind='SOURCE' then
             live_auth.id=auth.id and live_auth.current_entitlement_head_id is null
             and not exists(select 1 from public.weekly_source_entitlement_heads current_head
               where pg_catalog.btrim(current_head.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
                 and current_head.state='COMMITTED_CURRENT')
           else approval.withdrawn_at_utc is null end as financial_current,
           (publication.candidate_id=v_work.candidate_id
             and publication.revision_no=v_revision.revision_no
             and publication.status='APPLIED' and publication.phase='DONE'
             and command.command_kind='POSITION_APPLY' and command.module_epoch=v_module_epoch
             and command.id=private.bpay_next_source_command_id_v1(v_revision.source_event_id)
             and command.status='COMPLETE' and command.sealed_at_utc is not null
             and command.expected_member_count=command.enrolled_member_count
             and member.member_no is not null) as publication_exact
      into v_source_origin
      from public.timesheets t
      join public.contracts c on c.id=t.contract_id
      -- uq_contract_week makes this one indexed ordinary configured-week
      -- point; a same-contract/week row for another physical root is not scope.
      left join public.contract_weeks cw
        on cw.contract_id=t.contract_id and cw.week_ending_date=t.week_ending_date
          and cw.additional_seq=0
      left join public.weekly_exceptional_pay_target_families family
        on pg_catalog.btrim(family.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
      left join public.weekly_source_entitlement_heads h on h.id=v_revision.source_head_id
      left join public.weekly_source_root_authorisations auth
        on auth.id=v_revision.source_event_id and v_revision.source_head_id is null
          and v_revision.source_kind='SOURCE'
      left join public.weekly_source_root_authorisations live_auth
        on live_auth.root_timesheet_id=t.timesheet_id and live_auth.withdrawn_at_utc is null
      left join public.timesheets_financials tf on tf.id=v_revision.financial_snapshot_id
      left join private.bpay_next_protected_source_receipt receipt
        on receipt.revision_id=v_revision.id and receipt.work_id=v_work.id
      left join public.weekly_exceptional_payment_approvals approval on approval.id=receipt.approval_id
      left join public.weekly_exceptional_pay_generations generation on generation.id=receipt.generation_id
      left join public.weekly_exceptional_orchestration_runs orchestration on orchestration.id=receipt.orchestration_run_id
      left join private.bpay_next_publication publication
        on publication.work_id=v_work.id and publication.revision_id=v_revision.id
      left join private.bpay_next_command command on command.id=publication.command_id
      left join private.bpay_next_command_member member
        on member.command_id=command.id and member.candidate_id=v_work.candidate_id
      where t.timesheet_id=v_revision.physical_timesheet_id;
    if not found or v_source_origin.root_exact is not true
       or v_source_origin.origin_exact is not true or v_source_origin.publication_exact is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_ORIGIN_INVALID';
    end if;
    -- Consume the raw Source-owned gate captured for this exact ticked revision.
    select snapshot.gate_result into v_query_gate
      from private.bpay_next_run_work_query_snapshot snapshot
      where snapshot.run_id=v_run_id and snapshot.work_id=v_work.id
        and snapshot.expected_revision_id=v_revision.id
        and snapshot.physical_timesheet_id=v_revision.physical_timesheet_id;
    if not found then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_SNAPSHOT_MISSING';
    end if;
    if (pg_catalog.jsonb_typeof(v_query_gate)='object'
        and v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
        and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
        and pg_catalog.jsonb_typeof(v_query_gate->'ok')='boolean'
        and pg_catalog.jsonb_typeof(v_query_gate->'code')='string') is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
    v_query_scope:=nullif(v_query_gate->'scope','null'::jsonb);
    if v_query_scope is not null then
      if (v_source_origin.scope_exact is true
          and pg_catalog.jsonb_typeof(v_query_scope)='object'
          and v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
          and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
          and pg_catalog.jsonb_typeof(v_query_scope->'root_timesheet_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'family_booking_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'candidate_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'client_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'contract_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'week_ending_date')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'root_version')='string'
          and v_query_scope->>'root_timesheet_id'=v_revision.physical_timesheet_id::text
          and v_query_scope->>'root_version'=v_revision.physical_timesheet_version::text
          and v_query_scope->>'family_booking_id'=v_work.booking_id
          and v_query_scope->>'candidate_id'=v_work.candidate_id::text
          and v_query_scope->>'client_id'=v_source_origin.client_id::text
          and v_query_scope->>'contract_id'=v_work.contract_id::text
          and v_query_scope->>'week_ending_date'=v_work.week_ending_date::text
          and v_query_scope->'target_family_id'=coalesce(pg_catalog.to_jsonb(v_source_origin.family_id::text),'null'::jsonb)
          and v_query_scope->'family_bound_version'=coalesce(pg_catalog.to_jsonb(v_source_origin.bound_version::text),'null'::jsonb)) is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_SCOPE_MISMATCH';
      end if;
    end if;
    v_query_digest:=null;
    if v_query_gate->'ok'='true'::jsonb then
      if (v_query_gate->>'code'='OK' and v_query_scope is not null
          and pg_catalog.jsonb_typeof(v_query_gate->'blocked')='boolean'
          and pg_catalog.jsonb_typeof(v_query_gate->'query_state_sha256')='string'
          and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
      end if;
      -- The query Scope has no HEAD id. An available/current query root must
      -- never make a superseded or otherwise unmapped financial origin pay.
      -- Conversely an actual defined unavailable current scope retains its
      -- genuine immutable origin and reaches positive-WORK REVIEW below.
      if v_source_origin.financial_current is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_AUTHORITY_CHANGED';
      end if;
      v_query_digest:=pg_catalog.decode(v_query_gate->>'query_state_sha256','hex');
      v_query_disposition:=case when v_query_gate->'blocked'='true'::jsonb then 'BLOCKED' else 'CLEAR' end;
    elsif ((v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE' and v_query_scope is null)
        or (v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE' and v_query_scope is not null))
        and v_query_gate->'blocked'='null'::jsonb and v_query_gate->'query_state_sha256'='null'::jsonb then
      v_query_disposition:='UNAVAILABLE';
    else
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
  else
    v_query_disposition:=null;
  end if;
  insert into private.bpay_next_run_work
    (run_worker_id,candidate_id,work_id,captured_revision_id)
    values(v_worker_id,v_job.candidate_id,v_work.id,v_revision.id)
    returning id into v_run_work_id;
  if v_query_disposition is not null then
    insert into private.bpay_next_run_work_query_pin_v2
      (run_work_id,run_worker_id,candidate_id,work_id,captured_revision_id,
       prepare_job_id,module_epoch,gate_version,disposition,scope,query_state_sha256)
      values(v_run_work_id,v_worker_id,v_job.candidate_id,v_work.id,v_revision.id,
        p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3',v_query_disposition,v_query_scope,v_query_digest);
  end if;
  if v_choice.selection_mode='SUBSET' then
    update private.bpay_next_run_worker
      set expected_selected_component_count=
        expected_selected_component_count+v_choice.component_count
      where id=v_worker_id;
  end if;
  update private.bpay_next_job
    set phase='PINNING',cursor_key=v_selection.selection_no::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','PINNING',
    'next_cursor',v_selection.selection_no,'work_id',v_work.id,
    'captured_revision_id',v_revision.id,'replay',false);
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           p.approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      v_issue_work_id:=v_member.work_id;exit;
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical same-channel TAXABLE WORK.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.component_kind is distinct from 'WORK' or v_row.tax_treatment is distinct from 'TAXABLE' then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,'TAXABLE'::text,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (private.bpay_next_prepare_job_run_v1(v_query_pin.prepare_job_id) is distinct from v_run_id
        or (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        v_issue:='SOURCE_PAY_QUERY_OPEN';
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
  if v_issue is not null then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_issue_work_id where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_affected_work_ids uuid[]:=array[]::uuid[];
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select * into strict v_position from private.bpay_next_position
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,'TAXABLE'::text,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_affected_work_ids:=array_append(v_affected_work_ids,v_row.work_id);
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
    perform private.bpay_next_workbench_refresh_many_v1(v_affected_work_ids,null);
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_complete_no_case_worker_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;v_accepted_empty boolean;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_NO_CASE_COMPLETION_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_job.status='DONE' and v_job.phase='READY'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY',
      'worker_status',v_worker.status,
      'replay',true,'run_worker_id',v_worker.id);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_worker.status<>'PREPARING'
     or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_NO_CASE_COMPLETION_STATE_INVALID';
  end if;
  v_accepted_empty:=v_run.accepted_choice_descriptor is not null
    and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_job.candidate_id)
    and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
      and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_job.candidate_id)
    and not exists(select 1 from private.bpay_next_case_selection selection where selection.run_id=v_run_id and selection.candidate_id=v_job.candidate_id);
  if v_worker.captured_position_count=0
     or (not v_accepted_empty and v_worker.captured_position_count<>v_worker.captured_line_count)
     or (v_accepted_empty and (v_worker.captured_line_count=0
       or v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
       or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
       or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
         and work_collection_id is not null and not managed_capture_completed))) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_NO_CASE_COMPLETION_LINE_COUNT_MISMATCH';
  end if;
  -- Any case, including a paused or closed one, is conservatively deferred
  -- to the common case-allocation owner. This uses candidate_id index EXISTS,
  -- not a scan of payment or Timesheet history.
  if not v_accepted_empty and exists(select 1 from private.bpay_next_finance_case c
       where c.candidate_id=v_job.candidate_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CASE_ALLOCATION_REQUIRED';
  end if;
  update private.bpay_next_run_worker set status='READY'
    where id=v_worker.id;
  perform private.bpay_next_publish_no_case_week_fact_v1(v_worker.id,null);
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id;
  return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0542_banking_pay_next_prepare_case_visit_count_v1.sql; identity/support delta only.
create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_plan jsonb;v_plan_period jsonb;v_plan_row jsonb;v_plan_instructions jsonb;
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_saved_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_case_selection%rowtype;
  v_state private.bpay_next_case_allocation_state%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_period_rule private.bpay_next_case_rule%rowtype;
  v_origin private.bpay_next_case_create_request%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_due date;v_case_status text;
  v_automatic boolean;v_week_not_affected boolean;
  v_period private.bpay_next_case_period%rowtype;
  v_item record;v_prior record;v_value record;v_row record;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_floor numeric;v_outstanding numeric;v_case_outstanding numeric;
  v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_taken numeric;v_e numeric;v_h numeric;
  v_next_e numeric;v_next_h numeric;
  v_gross numeric;v_week_start date;v_age timestamptz;v_age_key bigint;
  v_purpose text;v_explanation text;v_issue text;v_seen bigint:=0;
  v_result_id uuid;v_hold_id uuid;v_instruction_id uuid;v_has_more boolean;v_terminal boolean:=false;
  v_cap_reason text;v_affordability_reason text;v_capacity_reason text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_expected_cursor<0 or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PREPARE_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id),j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update;
  if v_job.module_epoch<>v_epoch then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_OWNER_STALE';
  end if;
  if v_job.status='DONE' and v_job.phase='READY' and v_state.status='COMPLETE'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_worker.status='REVIEW'
     and v_worker.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_control.active_owner_epoch<>p_owner_epoch
     or v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_STATE_INVALID';
  end if;
  if v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
     or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
     or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
       and work_collection_id is not null and not managed_capture_completed) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_LINE_COUNT_MISMATCH';
  end if;
  if v_job.cursor_key is not null and v_job.cursor_key !~ '^[0-9]{1,19}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CURSOR_CORRUPT';
  end if;
  v_saved_cursor:=coalesce(v_job.cursor_key::bigint,0);
  if coalesce(p_expected_cursor,0)<v_saved_cursor then
    -- Lost STEP reply: read back the persisted checkpoint without a second
    -- capture, hold, cursor spend or financial-view bump. Ahead is not replay.
    return pg_catalog.jsonb_build_object('done',false,'phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,
      'rows_visited','0','replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if coalesce(p_expected_cursor,0)>v_saved_cursor then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_CURSOR_STALE';
  end if;
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate)
     and (not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
       or (v_run.accepted_choice_descriptor is not null
         and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_candidate)
         and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
           and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_candidate))) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    -- A resumed weekly/capture/allocation page uses its exact frozen selection
    -- revision. Refuse Source origins before any stage writes, including when
    -- the collection's current qualification reader would return no row.
    if v_state.id is not null and exists(
      select 1 from private.bpay_next_case_selection_item si
      join private.bpay_next_work_collection c on c.case_id=si.case_id
        and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
      where si.run_id=v_run_id and si.candidate_id=v_candidate
        and si.selection_revision=v_state.selection_revision and si.is_selected
        and w.work_kind='SOURCE') then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
    end if;
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      -- Comparison is owned by the later CM chain. Existing immutable Source
      -- origin identity is enough to refuse it; no absent pack column/type or
      -- fabricated financial certificate is consulted or created here.
      if exists(select 1 from private.bpay_next_case_selection_item si
        join private.bpay_next_work_collection c on c.case_id=si.case_id
          and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
        join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
        where si.run_id=v_run_id and si.candidate_id=v_candidate
          and si.selection_revision=v_selection.selection_revision and si.is_selected
          and w.work_kind='SOURCE') then
        raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
      end if;
      if v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
         or v_worker.gross_ex_vat<>v_worker.gross_inc_vat then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select selected.min_take_home_wtd into strict v_floor from private.bpay_next_selection_candidate selected
        where selected.run_id=v_run_id and selected.candidate_id=v_candidate;
      if v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity') or v_floor<0
         or v_floor<>pg_catalog.trunc(v_floor,2) then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      v_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
      -- OVERPAYMENT gross recovery does not consume the protected-debt H/floor
      -- operand (0290). Prove the exact nonempty SEALED selected set, not merely
      -- that no protected row happened to be found. Missing identities/counts
      -- remain week-dependent; instruction capture and allocation re-prove it.
      select count(*)=v_selection.selected_count and count(*)>0
        and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
        into v_week_not_affected
        from private.bpay_next_case_selection_item si
        left join private.bpay_next_case_component cc
          on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
        left join private.bpay_next_finance_case fc on fc.id=si.case_id and fc.candidate_id=si.candidate_id
        where si.run_id=v_selection.run_id and si.candidate_id=v_selection.candidate_id
          and si.selection_revision=v_selection.selection_revision and si.is_selected;
      -- Exact indexed metadata absence is required BEFORE establishing a new
      -- zero week. Missing prior NEW facts are unresolved, not zero/history SUM.
      if not v_week_not_affected and exists(select 1 from private.bpay_next_run_worker w
        join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_week_start and r.pay_date<v_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
      end if;
      insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
        default_floor,default_floor_revision,resolved_arranged_take_home)
        values(v_candidate,v_week_start,1,v_floor,1,0) on conflict(candidate_id,pay_week_start) do nothing;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_week_start for update;
      if not v_week_not_affected and v_week.unresolved_contribution_count<>0 then v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;end if;
      if v_week.default_floor<>v_floor then
        update private.bpay_next_worker_week set default_floor=v_floor,default_floor_revision=default_floor_revision+1,
          period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_week_start returning * into v_week;
      end if;
      update private.bpay_next_run_worker set case_selection_revision=v_selection.selection_revision where id=v_worker.id;
      v_worker.case_selection_revision:=v_selection.selection_revision;
      insert into private.bpay_next_case_allocation_state
        (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,pass_kind,projection_no,
         command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
         expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,prepare_stage,
         captured_default_floor,captured_default_floor_revision,pay_week_start,captured_week_revision,captured_work_gross)
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.captured_preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.captured_preparation_revision
         or v_state.selection_revision<>v_worker.case_selection_revision or v_state.prepare_stage='NOT_CASE_PREPARE' then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_BINDING_MISMATCH';
      end if;
      -- Lease takeover does not reset the captured economic cursors/headroom.
      if v_state.owner_epoch<>p_owner_epoch then
        update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
        v_state.owner_epoch:=p_owner_epoch;
      end if;
    end if;

    if v_state.prepare_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            <(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or
            (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
              >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID')
            then 'CASE_RETURN_FLOOR_UNBOUND' else 'CASE_WEEK_BASIS_UNBOUND' end;
          exit prepare_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;
        v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_has_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_has_more then 'UNBOUND' else 'BOUND' end,
        prepare_stage=case when v_has_more then 'WEEK_CAPTURE' else 'INSTRUCTION_CAPTURE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.prepare_stage='INSTRUCTION_CAPTURE' then
      for v_item in select i.* from private.bpay_next_case_selection_item i
        where i.run_id=v_run_id and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision
          and i.is_selected and i.selection_no>v_state.instruction_capture_cursor order by i.selection_no limit p_limit
      loop
        v_seen:=v_seen+1;
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        -- Original Case/component locks precede the original period lock.
        perform 1 from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        begin
          v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
            'candidate_id',v_candidate,'case_id',v_case.id,'case_component_id',v_component.id,
            'pay_date',v_run.pay_date,'pay_week_start',v_state.pay_week_start,
            'target_pay_channel',v_worker.target_pay_channel,'captured_default_floor',v_state.captured_default_floor,
            'captured_default_floor_revision',v_state.captured_default_floor_revision,
            'weekly_binding_state',v_state.weekly_binding_state,'accepted_run_id',v_run_id));
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_CASE_INPUT_UNBOUND','BPAY_NEXT_CASE_TARGET_UNSUPPORTED','BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND') then
            v_issue:=substring(sqlerrm from 11);exit prepare_page;
          else raise;end if;
        end;
        v_plan_period:=v_plan->'period';
        if v_plan->>'period_operation'='INSERT' then
          insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
            period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
            values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,(v_plan_period->>'rule_id')::uuid,
              (v_plan_period->>'period_revision')::bigint,(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
              (v_plan_period->>'opening_due_source_ex_vat')::numeric,(v_plan_period->>'work_collection_id')::uuid)
            on conflict(case_component_id,pay_week_start) do nothing;
        end if;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_plan->>'period_operation'='FUNDING_ZERO' then
          update private.bpay_next_case_period set rule_id=(v_plan_period->>'rule_id')::uuid,
            opening_outstanding_source_ex_vat=(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
            opening_due_source_ex_vat=(v_plan_period->>'opening_due_source_ex_vat')::numeric,period_revision=period_revision+1
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start
              and period_revision=(v_plan_period->>'period_revision')::bigint-1
              and (opening_outstanding_source_ex_vat,opening_due_source_ex_vat,realised_recovery_source_ex_vat,
                active_unrealised_recovery_source_ex_vat,active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) returning * into v_period;
          if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if to_jsonb(v_period) is distinct from v_plan_period then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=(v_plan->'resolution'->>'target_ex_vat')::numeric,target_vat=(v_plan->'resolution'->>'target_vat')::numeric,
          target_inc_vat=(v_plan->'resolution'->>'target_inc_vat')::numeric,
          valuation_policy_id=(v_plan->'resolution'->>'valuation_policy_id')::uuid,
          valuation_window_id=(v_plan->'resolution'->>'valuation_window_id')::uuid,
          resolution_ref=v_plan->'resolution'->>'resolution_ref',component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_plan:=jsonb_set(v_plan,'{instruction}',(v_plan->'instruction')||jsonb_build_object(
          'run_worker_id',v_worker.id,'run_id',v_run_id,'candidate_id',v_candidate,
          'selection_revision',v_state.selection_revision,'preparation_revision',v_worker.captured_preparation_revision,
          'captured_case_revision',v_case.case_revision,'captured_component_revision',v_component.component_revision,
          'captured_period_revision',v_period.period_revision));
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id)
          select planned.run_worker_id,planned.run_id,planned.candidate_id,planned.selection_revision,planned.preparation_revision,planned.case_component_id,planned.case_id,planned.captured_case_revision,planned.captured_component_revision,planned.captured_period_revision,planned.rule_id,planned.rule_revision,planned.case_kind,planned.case_subtype,planned.tax_treatment,planned.instruction_kind,planned.direction,planned.payroll_stage,planned.hold_purpose,planned.age_key,planned.debt_age_at_utc,planned.component_ordinal,planned.pay_week_start,planned.source_pay_channel,planned.target_pay_channel,planned.allocation_channel,planned.currency,planned.nominal_source_ex_vat,planned.nominal_target_ex_vat,planned.nominal_target_vat,planned.nominal_target_inc_vat,planned.capacity_source_ex_vat,planned.capacity_target_ex_vat,planned.capacity_target_vat,planned.capacity_target_inc_vat,planned.minimum_earnings_threshold,planned.take_home_floor_override,planned.captured_default_floor,planned.captured_default_floor_revision,planned.resolved_take_home_floor,planned.valuation_policy_id,planned.valuation_window_id,planned.resolution_ref,planned.explanation,planned.work_collection_id from jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan->'instruction') planned
          returning id into v_instruction_id;
        perform private.bpay_next_capture_instruction_destination_v1(v_instruction_id);
        v_state.instruction_capture_cursor:=v_item.selection_no;
        v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
        if v_component.payroll_stage='NET_ADD' then v_state.payout_instruction_count:=v_state.payout_instruction_count+1;end if;
        update private.bpay_next_run_worker set case_instruction_count=case_instruction_count+1 where id=v_worker.id;
      end loop;
      select exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=v_run_id
        and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision and i.is_selected
        and i.selection_no>v_state.instruction_capture_cursor) into v_has_more;
      if not v_has_more and v_state.captured_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CAPTURE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_case_allocation_state set instruction_capture_cursor=v_state.instruction_capture_cursor,
        captured_instruction_count=v_state.captured_instruction_count,payout_instruction_count=v_state.payout_instruction_count,
        prepare_stage=case when v_has_more then 'INSTRUCTION_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.weekly_binding_state='NOT_AFFECTED' then
      -- Only frozen instructions are authoritative from this point. This
      -- scalar metadata proof is scoped to this exact run worker/revisions;
      -- it never reconstructs money or reads prior payment history.
      select count(*)=v_state.expected_instruction_count and count(*)>0
        and coalesce(bool_and((i.case_kind,i.instruction_kind,i.payroll_stage,i.hold_purpose)
          is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
          and si.case_id is not null),false)
        into v_week_not_affected
        from private.bpay_next_run_case_instruction i
        left join private.bpay_next_case_selection_item si
          on si.run_id=i.run_id and si.candidate_id=i.candidate_id and si.selection_revision=i.selection_revision
            and si.case_id=i.case_id and si.case_component_id=i.case_component_id and si.is_selected
        where i.run_worker_id=v_worker.id and i.run_id=v_run_id and i.candidate_id=v_candidate
          and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision;
      if not v_week_not_affected
        or v_state.captured_instruction_count<>v_state.expected_instruction_count
        or v_worker.case_instruction_count<>v_state.expected_instruction_count
        or not exists(select 1 from private.bpay_next_case_selection cs where cs.run_id=v_run_id
          and cs.candidate_id=v_candidate and cs.selection_revision=v_state.selection_revision
          and cs.status='SEALED' and cs.selected_count=v_state.expected_instruction_count) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
      end if;
    end if;
    if v_state.prepare_stage<>'ALLOCATE' or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
    end if;
    select c.* into strict v_channel from private.bpay_next_case_allocation_channel c
      where c.state_id=v_state.id and c.allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    select coalesce(jsonb_agg(page.value),'[]'::jsonb) into v_plan_instructions from (
      select to_jsonb(i) as value from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.captured_preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    ) page;
    v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
      'instructions',v_plan_instructions,'channel_headroom',v_e,'worker_take_home',v_h));
    for v_plan_row in select value from jsonb_array_elements(v_plan->'rows') loop
      v_instruction:=jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan_row->'instruction');
      v_seen:=v_seen+1;
      v_taken:=(v_plan_row->>'taken_amount')::numeric;v_cap_reason:=v_plan_row->>'cap_reason';
      v_affordability_reason:=v_plan_row->>'affordability_reason';v_capacity_reason:=v_plan_row->>'capacity_reason';
      v_next_e:=(v_plan_row->>'channel_headroom_after')::numeric;v_next_h:=(v_plan_row->>'worker_take_home_after')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.captured_preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT','PAYE',v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,
         v_cap_reason,v_affordability_reason,v_capacity_reason,v_e,v_next_e,v_h,v_next_h)
        returning id into v_result_id;
      if v_taken>0 then
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_instruction.case_id for update;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_instruction.case_component_id for update;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_instruction.pay_week_start for update;
        insert into private.bpay_next_case_hold
          (run_worker_id,candidate_id,case_id,amount,status,instruction_id,allocation_result_id,allocation_pass_kind,
           case_component_id,purpose,pay_week_start,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_worker.id,v_candidate,v_case.id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_taken,v_taken,0,v_taken) returning id into v_hold_id;
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
      elsif v_instruction.payroll_stage='NET_ADD' then v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.captured_preparation_revision,'GBP','CANDIDATE',v_candidate,
            v_state.payout_instruction_count,v_state.payout_instruction_count,v_state.allocated_net_additions,
            v_state.allocated_net_additions,0,v_state.allocated_net_additions,'SEALED');
      end if;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_state.pay_week_start for update;
      insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,original_run_worker_id,
        original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,
        payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision)
        values(v_candidate,v_state.pay_week_start,v_worker.id,v_run.pay_date,v_run.created_at_utc,1,'GROSS_FALLBACK',
          v_gross,'ARRANGED',case when v_gross>0 then 'ELIGIBLE' else 'EXCLUDED' end,v_gross,1);
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home+v_gross,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_case_allocation_state set status='COMPLETE',prepare_stage='COMPLETE',
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=0,gross_inc_vat=v_gross,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;

      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set retained_prepare_stage=v_state.prepare_stage,retained_weekly_binding_state=v_state.weekly_binding_state,
        prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;

    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  if v_terminal then
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen::integer);
  end if;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
create or replace function private.bpay_next_begin_prepare_projection_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_rows_visited integer
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
begin
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id;
  if v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
    or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
    or v_worker.status not in ('READY','REVIEW') or p_rows_visited is null or p_rows_visited not between 0 and 100
    or v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.status='REVIEW' then
    update private.bpay_next_job set retained_prepare_phase=v_job.phase,retained_prepare_cursor=v_job.cursor_key,
      retained_prepare_work_cursor=v_job.position_work_cursor,retained_prepare_component_cursor=v_job.position_component_cursor where id=p_job_id;
    if v_worker.review_issue_code in ('SELECTED_WORK_NOT_APPROVED','SELECTED_WORK_REVISION_CHANGED','SELECTED_WORK_POSITION_PENDING',
      'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE','CASE_NO_PAYABLE_AMOUNT') then
      perform private.bpay_next_accept_worker_action_v1(v_job.command_id,v_worker.id,'RELEASE',null,v_worker.preparation_revision,p_job_id);
      return pg_catalog.jsonb_build_object('done',false,'phase','WORKER_RELEASE_WORK','cursor',null,
        'rows_visited',p_rows_visited::text,'replay',false,'run_worker_id',v_worker.id);
    end if;
  end if;
  update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count-1,
    ready_candidate_count=ready_candidate_count+case when v_worker.status='READY' then 1 else 0 end,
    review_candidate_count=review_candidate_count+case when v_worker.status='REVIEW' then 1 else 0 end
    where id=v_worker.run_id;
  update private.bpay_next_job set phase='WORKBENCH_WORK',cursor_key=null,
    position_work_cursor=null,position_component_cursor=null where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','WORKBENCH_WORK','cursor',null,
    'rows_visited',p_rows_visited::text,'replay',false,'run_worker_id',v_worker.id);
end
$function$;

-- Exact latest successor: supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql; identity/support delta only.
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

-- Exact latest successor: supabase/repeatable/11102026_0316_banking_pay_next_snooze_many_enrollment_v1.sql; identity/support delta only.
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
     or not private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
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
      where id=v_run_id for share;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and v_expired_preparation))
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
             case when v_expired_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' else 'NEW' end,1)
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

-- Exact latest successor: supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql; identity/support delta only.
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
    where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
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
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request(command_id,run_worker_id,candidate_id,request_no,
    entered_paye_net,frozen_gross_inc_vat,input_kind,actor_user_id,case_draft_state_id,
    preparation_revision,selection_revision,expected_projection_revision)
    values(p_command_id,v_worker.id,v_worker.candidate_id,p_expected_projection_revision+1,
      p_entered_paye_net,v_worker.gross_inc_vat,p_input_kind,p_actor_user_id,v_draft.id,
      v_worker.captured_preparation_revision,v_worker.case_selection_revision,p_expected_projection_revision);
  update private.bpay_next_run_worker set net_request_revision=p_expected_projection_revision+1 where id=v_worker.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',(p_expected_projection_revision+1)::text,
    'replay',false,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id);
end
$function$;

-- Exact latest successor: supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql; identity/support delta only.
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
     or v_worker.captured_preparation_revision<>v_request.preparation_revision or v_worker.case_selection_revision<>v_request.selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql; identity/support delta only.
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
  select id,processed_instruction_count,expected_instruction_count,preparation_revision,selection_revision into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql; identity/support delta only.
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
     or v_worker.captured_preparation_revision<>v_build.preparation_revision or v_worker.case_selection_revision<>v_build.selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_1827_banking_pay_next_candidate_destination_group_v1.sql; identity/support delta only.
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
     or v_worker.captured_preparation_revision<>v_g.preparation_revision or v_worker.case_selection_revision<>v_g.selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql; identity/support delta only.
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
      and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql; identity/support delta only.
create or replace function private.bpay_next_accept_case_cancel_v1(p_command_id uuid,p_run_worker_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;v_draft record;
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
  select id into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE';
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'SIMPLE_CANCEL');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_cancel_request(command_id,run_worker_id,candidate_id,expected_line_count,status)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_worker.captured_line_count,'REQUESTED');
  insert into private.bpay_next_case_cancel_binding(command_id,run_worker_id,candidate_id,draft_state_id,
    preparation_revision,selection_revision,status,stage,expected_work_count)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_draft.id,v_worker.captured_preparation_revision,v_worker.case_selection_revision,
      'REQUESTED','INTENT',v_worker.captured_line_count);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,'run_worker_id',v_worker.id,'phase','REQUESTED','cursor',null,'released_line_count',0,'replay',false);
end
$function$;

-- Exact latest successor: supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql; identity/support delta only.
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
     or v_worker.captured_preparation_revision<>v_binding.preparation_revision or v_worker.case_selection_revision<>v_binding.selection_revision
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

-- Exact latest successor: supabase/repeatable/04102026_0430_banking_pay_next_week_lifecycle_v1.sql; identity/support delta only.
create or replace function private.bpay_next_apply_cancelled_week_v1(p_job_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_cancel_request%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_case_cancel private.bpay_next_case_cancel_binding%rowtype;
  v_group private.bpay_next_destination_group%rowtype;v_group_cancel private.bpay_next_destination_cancel%rowtype;
  v_epoch bigint;v_transfer_count integer:=0;v_old_resolved numeric;v_old_unresolved integer;
begin
  if p_job_id is null then raise exception using errcode='22023',message='BPAY_NEXT_WEEK_CANCEL_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>'SIMPLE_CANCEL' then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_JOB_KIND_INVALID';
  end if;
  select r.* into strict v_request from private.bpay_next_cancel_request r where r.command_id=v_job.command_id;
  select r.* into strict v_run from private.bpay_next_pay_run r
    join private.bpay_next_run_worker w on w.run_id=r.id where w.id=v_request.run_worker_id for update of r;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_request.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_request.run_worker_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id or v_request.candidate_id<>v_worker.candidate_id
     or v_request.status<>'CANCELLED' or v_worker.status<>'CANCELLED'
     or v_request.released_line_count<>v_request.expected_line_count
     or v_job.applied_line_count<>v_request.released_line_count
     or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0
     then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_SCOPE_INVALID';
  end if;
  if v_worker.case_selection_revision>0 then
    select b.* into strict v_case_cancel from private.bpay_next_case_cancel_binding b
      where b.command_id=v_job.command_id;
    if v_case_cancel.run_worker_id<>v_worker.id or v_case_cancel.candidate_id<>v_worker.candidate_id
       or v_case_cancel.preparation_revision<>v_worker.captured_preparation_revision
       or v_case_cancel.selection_revision<>v_worker.case_selection_revision
       or v_case_cancel.status<>'CANCELLED' or v_case_cancel.stage<>'COMPLETE'
       or v_case_cancel.expected_work_count<>v_request.expected_line_count
       or v_case_cancel.released_work_count<>v_request.released_line_count
       or v_case_cancel.expected_case_hold_count is null
       or v_case_cancel.released_case_hold_count<>v_case_cancel.expected_case_hold_count
       or v_worker.active_case_hold_count<>0
       or v_request.cursor_line_no is distinct from v_case_cancel.checkpoint
       or v_job.cursor_key is distinct from v_case_cancel.checkpoint::text
       or exists(select 1 from private.bpay_next_case_hold h
           where h.run_worker_id=v_worker.id and h.status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CASE_CANCEL_BINDING_INVALID';
    end if;
  end if;
  if v_job.status='DONE' then return;end if;
  if v_job.status<>'LEASED' or v_job.phase<>'RELEASE' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_control.pending_outcome_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CANCEL_OWNER_INVALID';
  end if;
  select g.* into v_group from private.bpay_next_destination_group g where g.run_worker_id=v_worker.id;
  if found then
    -- The guarded certificate counts exact <=100-row CHECK and FINAL ranges.
    -- Do not scan every sibling again or use an arbitrary counter as authority.
    select c.* into strict v_group_cancel from private.bpay_next_destination_cancel c where c.command_id=v_job.command_id;
    if v_worker.case_selection_revision<1 or v_group.candidate_id<>v_worker.candidate_id
       or v_group.stage<>'COMPLETE' or v_group.sealed_leg_count<>v_group.expected_leg_count
       or v_group_cancel.anchor_transfer_id<>v_group.anchor_transfer_id
       or v_group_cancel.expected_leg_count<>v_group.expected_leg_count
       or v_group_cancel.checked_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.cancelled_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.check_cursor is null or v_group_cancel.cancel_cursor is null
       or v_group_cancel.check_cursor<>v_group_cancel.cancel_cursor
       or v_case_cancel.transfer_id is distinct from v_group.anchor_transfer_id
       or v_case_cancel.projection_id is distinct from v_group.projection_id
       or v_case_cancel.net_state_id is distinct from v_group.net_state_id then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_DESTINATION_CANCEL_CERTIFICATE_INVALID';
    end if;
  else
    -- Ordinary no-split scope retains its exact one-original-transfer guard.
    for v_transfer in select t.* from private.bpay_next_transfer t where t.run_worker_id=v_worker.id
      order by t.transfer_no limit 2 for update
    loop
      v_transfer_count:=v_transfer_count+1;
      if v_transfer_count>1 or v_transfer.status<>'CANCELLED' or v_transfer.original_transfer_id is not null
         or v_transfer.return_cash_id is not null or v_transfer.candidate_id<>v_worker.candidate_id
         or v_transfer.account_approval_ref is not null
         or exists(select 1 from private.bpay_next_csv_instruction i where i.transfer_id=v_transfer.id)
         or exists(select 1 from private.bpay_next_transfer_outcome o where o.transfer_id=v_transfer.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_TRANSFER_PROTECTED';
      end if;
    end loop;
  end if;
  select w.* into v_week from private.bpay_next_worker_week w where w.candidate_id=v_worker.candidate_id
    and w.pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select x.* into v_prior from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then raise notice 'BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_UNBOUND';return;end if;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_worker.candidate_id
     or v_prior.pay_week_start<>v_week.pay_week_start or v_prior.original_pay_date<>v_run.pay_date
     or v_prior.original_created_at_utc<>v_run.created_at_utc or v_prior.original_gross_amount<>v_worker.gross_inc_vat
     or v_prior.original_transfer_id is not null or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_NOT_EXACT';
  end if;
  if v_prior.payment_state='CANCELLED' and v_prior.eligibility_state='EXCLUDED' and v_prior.eligible_arranged_amount=0 then return;end if;
  if v_prior.payment_state<>'ARRANGED' then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_NOT_UNPAID';end if;
  v_old_resolved:=coalesce(v_prior.eligible_arranged_amount,0);
  v_old_unresolved:=case when v_prior.eligibility_state='UNBOUND' then 1 else 0 end;
  update private.bpay_next_worker_week_contribution set payment_state='CANCELLED',eligibility_state='EXCLUDED',eligible_arranged_amount=0,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old_resolved,
    unresolved_contribution_count=unresolved_contribution_count-v_old_unresolved,period_revision=period_revision+1
    where candidate_id=v_worker.candidate_id and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old_resolved and unresolved_contribution_count>=v_old_unresolved;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_DELTA_INVALID';end if;
end
$function$;

-- Exact latest successor: supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql; identity/support delta only.
create or replace function private.bpay_next_workbench_refresh_case_v1(p_case_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_snooze private.bpay_next_snooze%rowtype;v_accepted record;v_totals record;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_group text:='CASE:'||p_case_id::text;v_candidate uuid;v_client uuid;v_contract uuid;v_target text;
  v_financial bigint;v_projection bigint;v_count integer:=0;v_accepted_count integer;v_capacity numeric;
  v_available numeric;v_reserved numeric;v_realised numeric;v_balance numeric;v_due date;v_basis bytea;
  v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select c.candidate_id into strict v_candidate from private.bpay_next_finance_case c where c.id=p_case_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select w.contract_id,co.client_id into v_contract,v_client from private.bpay_next_work_collection wc
    join private.bpay_next_work w on w.id=wc.work_id left join public.contracts co on co.id=w.contract_id where wc.case_id=p_case_id;
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,case_id,candidate_id,client_id,contract_id,
    target_pay_channel,complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'CASE',v_case.id,v_candidate,v_client,v_contract,v_target,true,v_projection,v_financial,v_metadata.metadata_version,'CURRENT')
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    target_pay_channel=excluded.target_pay_channel,complete=true,projection_revision=excluded.projection_revision,
    financial_view_revision=excluded.financial_view_revision,metadata_version=excluded.metadata_version,
    progress_state='CURRENT',error_code=null,updated_at_utc=transaction_timestamp();
  for v_component in select c.* from private.bpay_next_case_component c where c.case_id=v_case.id order by c.component_ordinal,c.id loop
    v_count:=v_count+1;
    select r.* into strict v_rule from private.bpay_next_case_rule r where r.id=v_component.rule_id
      and r.case_id=v_case.id and r.candidate_id=v_candidate;
    if v_component.direction='DEDUCTION' then
      v_capacity:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end;
      v_realised:=v_component.recovered_source_ex_vat;v_reserved:=v_component.active_recovery_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    else
      v_capacity:=v_component.approved_source_ex_vat;
      v_realised:=v_component.funded_source_ex_vat+v_component.paid_credit_source_ex_vat;
      v_reserved:=v_component.active_payout_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    end if;
    if v_available<0 or v_balance<0 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_CAPACITY_INCONSISTENT';end if;
    if v_available=0 and v_reserved=0 then continue;end if;
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.case_id:=v_case.id;v_item.case_component_id:=v_component.id;v_item.rule_id:=v_rule.id;
    v_item.member_kind:='CASE_COMPONENT';v_item.member_key:=v_component.id::text;v_item.component_key:=v_component.component_key;
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.component_kind:=v_component.case_kind;v_item.tax_treatment:=v_component.tax_treatment;
    v_item.source_pay_channel:=v_component.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:=v_component.direction;v_item.payroll_stage:=v_component.payroll_stage;
    v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_available;v_item.approved_source_ex_vat:=v_capacity;
    v_item.realised_source_ex_vat:=v_realised;v_item.active_reserved_source_ex_vat:=0;
    v_item.written_off_source_ex_vat:=v_component.written_off_source_ex_vat;
    v_item.residual_source_ex_vat:=v_available;v_item.gross_due_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.included_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.excluded_source_ex_vat:=0;v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
    v_item.deduction_balance_source_ex_vat:=case when v_component.direction='DEDUCTION' then v_balance else 0 end;
    v_item.section:='READY';v_item.reason_code:='READY';v_item.reason_priority:=200;v_item.selectable:=true;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
    if v_target is null or v_case.current_rule_id is null then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_accepted_count:=0;
    for v_accepted in
      select r.id run_id,cmd.id command_id,cmd.module_epoch,ci.nominal_source_ex_vat
      from private.bpay_next_case_selection_item si
      join private.bpay_next_case_selection s on (s.run_id,s.candidate_id,s.selection_revision)=(si.run_id,si.candidate_id,si.selection_revision) and s.status='SEALED'
      join private.bpay_next_pay_run r on r.id=si.run_id and r.selection_state='SEALED' and r.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
      join private.bpay_next_run_command rc on rc.run_id=r.id join private.bpay_next_command cmd on cmd.id=rc.command_id and cmd.command_kind='PREPARE'
      left join private.bpay_next_run_worker rw on rw.run_id=r.id and rw.candidate_id=si.candidate_id
      left join private.bpay_next_run_case_instruction ci on ci.run_worker_id=rw.id and ci.preparation_revision=rw.captured_preparation_revision
        and ci.case_component_id=si.case_component_id
      where si.case_component_id=v_component.id and si.is_selected
        and ((rw.id is not null and rw.status in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','ISSUED')
          and si.selection_revision=case when rw.case_selection_revision>0 then rw.case_selection_revision
            else (select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
              where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED') end)
          or (rw.id is null and si.selection_revision=(select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
            where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED')))
    loop
      v_accepted_count:=v_accepted_count+1;
      if v_accepted_count>1 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_ACCEPTED_CASE_COLLISION';end if;
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.nominal_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.nominal_source_ex_vat is not null;
      if v_item.reason_priority>50 then v_item.section:='WAITING';v_item.reason_code:='IN_PREPARATION';v_item.reason_priority:=50;end if;
    end loop;
    if v_item.reason_priority>82 and v_component.resolution_state='REVIEW' then
      v_item.section:='ACTION';v_item.reason_code:='CASE_REVIEW_REQUIRED';v_item.reason_priority:=82;
    elsif v_item.reason_priority>90 and v_case.status='PAUSED' then
      v_item.section:='WAITING';v_item.reason_code:='CASE_PAUSED';v_item.reason_priority:=90;
    elsif v_case.status in ('CLOSED','WRITTEN_OFF') and v_available>0 then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_due:=case when v_component.direction='DEDUCTION' then coalesce(v_rule.next_due_monday,v_case.due_date) else v_case.due_date end;
    if v_item.reason_priority>100 and v_due>v_today then
      v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_due;
    end if;
    v_basis:=sha256(convert_to(jsonb_build_array(v_candidate,v_component.case_id,v_component.id,
      v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,v_component.direction,
      v_component.payroll_stage,v_component.source_pay_channel,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,
      v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_rule.weekly_due_source_ex_vat,
      v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)::text,'UTF8'));
    select s.* into v_snooze from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today) and s.worker_pay_basis=v_basis;
    if found then
      v_item.snooze_id:=v_snooze.id;v_item.snooze_version:=v_snooze.version;v_item.snooze_scope:=v_snooze.scope_kind;v_item.snooze_until_date:=v_snooze.until_date;
      if v_item.reason_priority>110 then v_item.section:='WAITING';v_item.reason_code:='SNOOZED';v_item.reason_priority:=110;end if;
      if v_component.direction='PAYMENT' then v_item.excluded_source_ex_vat:=v_item.included_source_ex_vat;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today)) then v_item.stale_snooze_code:='SNOOZE_BASIS_STALE';end if;
    v_item.selectable:=v_item.section='READY';v_base:=v_item;
    if v_available>0 then v_item.portion_kind:='AVAILABLE';insert into private.bpay_next_workbench_item select (v_item).*;end if;
    if v_reserved>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.nominal_source_ex_vat:=v_reserved;v_item.active_reserved_source_ex_vat:=v_reserved;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;
      if v_available>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;v_item.deduction_balance_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_item.reason_priority>111 then v_item.section:='WAITING';v_item.reason_code:='RESERVED';v_item.reason_priority:=111;end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
      sum(i.written_off_source_ex_vat) w,sum(i.residual_source_ex_vat) residual,
      sum(i.deduction_balance_source_ex_vat) debt into v_totals from private.bpay_next_workbench_item i
      where i.group_key=v_group and i.case_component_id=v_component.id;
    if (v_totals.a,v_totals.c,v_totals.h,v_totals.w,v_totals.residual,v_totals.debt)
      is distinct from (v_capacity,v_realised,v_reserved,v_component.written_off_source_ex_vat,v_available,
        case when v_component.direction='DEDUCTION' then v_balance else 0::numeric end) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_MEMBER_NOT_CONSERVED';end if;
  end loop;
  if v_count=0 and v_case.status in ('OPEN','PAUSED') then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.case_id:=v_case.id;
    v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_case.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selectable:=false;v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    v_item.target_pay_channel:=v_target;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='UNAVAILABLE';v_item.updated_at_utc:=transaction_timestamp();insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=x.unknown=0 from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,count(*) filter(where not amount_known) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;

-- Exact latest successor: supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql; identity/support delta only.
create or replace function private.bpay_next_case_instruction_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_worker_status text;v_run_status text;v_selection_status text;
  v_preparation_revision bigint;v_selection_revision bigint;
begin
  -- Serialize insertion with confirmation/cancellation and exact selection
  -- seal; no latest Timesheet/rate/history lookup or post-Draft enrichment.
  select status into strict v_run_status from private.bpay_next_pay_run
    where id=new.run_id for share;
  select status,captured_preparation_revision,case_selection_revision
    into strict v_worker_status,v_preparation_revision,v_selection_revision
    from private.bpay_next_run_worker
    where id=new.run_worker_id and candidate_id=new.candidate_id
      and run_id=new.run_id for share;
  select status into strict v_selection_status from private.bpay_next_case_selection
    where run_id=new.run_id and candidate_id=new.candidate_id
      and selection_revision=new.selection_revision for share;
  if not exists(select 1 from private.bpay_next_run_worker w where w.id=new.run_worker_id
      and private.bpay_next_prepare_job_active_v1(w.active_prepare_job_id))
     or v_worker_status<>'PREPARING' or v_run_status<>'PREPARING'
     or v_selection_status<>'SEALED' or v_preparation_revision<>new.preparation_revision
     or v_selection_revision<>new.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_INSTRUCTION_CAPTURE_STATE_INVALID';
  end if;
  return new;
end;
$function$;

-- Exact latest successor: supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql; identity/support delta only.
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
  if v_binding.already_worker_released then
    if not private.bpay_next_worker_release_complete_v1(v_binding.run_worker_id,(select accepted_at_utc from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id))
      or v_binding.expected_work_count<>0 or v_binding.expected_case_hold_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RELEASE_RECEIPT_INVALID';end if;
    -- Traverse the native legal stages without touching already released money.
    if v_binding.stage='WORK' then v_binding.stage:='CASE';
    elsif v_binding.stage='CASE' then v_binding.stage:='FINAL';
    elsif v_binding.stage='FINAL' then v_binding.stage:='COMPLETE';v_binding.status:='EXPIRED';v_binding.finished_at_utc:=v_now;
    else raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_STAGE_INVALID';end if;
    v_seen:=1;
  elsif v_binding.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id
      and line_no>coalesce(v_binding.work_cursor,0) order by line_no,id limit p_limit loop
      if private.bpay_next_release_prepare_work_hold_v1(p_job_id,v_line.id,v_now) then v_work_holds:=v_work_holds+1;end if;
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id and line_no>coalesce(v_binding.work_cursor,0)) into v_more;
    if not v_more then
      if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_COUNT_INVALID';end if;
      v_binding.stage:='CASE';end if;
  elsif v_binding.stage='CASE' then
    for v_case_hold in select * from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      perform private.bpay_next_release_prepare_case_hold_v1(p_job_id,v_case_hold.id,v_now);
      v_binding.case_hold_cursor:=v_case_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;v_seen:=v_seen+1;v_case_released:=v_case_released+1;
    end loop;
    select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE') into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
         or (v_worker.id is not null and (select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CURSOR_INVALID';end if;
  elsif v_binding.stage='FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
       or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FINAL_COUNT_INVALID';end if;
    if v_worker.id is not null then update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;end if;
    v_binding.stage:='COMPLETE';v_binding.status:='EXPIRED';v_binding.finished_at_utc:=v_now;v_seen:=1;
  else raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_STAGE_INVALID';end if;
  update private.bpay_next_preparation_expiry_worker set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,started_at_utc=v_binding.started_at_utc,
    finished_at_utc=v_binding.finished_at_utc where job_id=p_job_id returning checkpoint into v_checkpoint;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+interval '120 seconds' where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
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

-- Exact latest successor: supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql; identity/support delta only.
create or replace function private.bpay_next_apply_expired_week_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_binding private.bpay_next_preparation_expiry_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;v_old numeric;v_unbound bigint;
begin
  select * into strict v_binding from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_binding.run_id;
  if v_binding.status<>'EXPIRED' or v_binding.stage<>'COMPLETE' or v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null
     or not exists(select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.id=p_job_id and j.job_kind='PREPARATION_EXPIRY' and j.status='LEASED' and j.phase='RELEASE'
         and j.command_id=v_binding.command_id and j.cursor_key=v_binding.checkpoint::text
         and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_SCOPE_INVALID';end if;
  perform private.bpay_next_exclude_preparation_week_v1(p_job_id,v_binding.run_worker_id);
end
$function$;

-- DR-05 current-parent successor: public.bpay_next_financial_job_v1
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
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','CASE_CREATE','CASE_WRITE_OFF')
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
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY')
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
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED','CANCELLED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
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
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY') then raise; end if;
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
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
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
        if coalesce(v_expired_prepare,false) then
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
            case when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind='PREPARATION_EXPIRY' and (v_result->>'phase' is distinct from v_job.phase
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
        if coalesce(v_expired_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase','EXPIRED',
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
      when 'PREPARATION_EXPIRY' then
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

-- DR-05 current-parent successor: public.bpay_next_preparation_intake_v1
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
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
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

-- DR-05 current-parent successor: private.bpay_next_review_simple_run_v1
create or replace function private.bpay_next_review_simple_run_v1(
  p_run_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_command private.bpay_next_command%rowtype;
  v_all_ready boolean;
  v_module_epoch bigint;
begin
  if p_run_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_REVIEW_RUN_ID_REQUIRED';
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
  if v_run.status='REVIEW' then
    -- A lost successful response must remain confirmable on exact replay.
    -- This parent lock also serializes expiry; exceptional frozen worker
    -- states are probed through the same partial index as first review.
    v_all_ready:=not exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_id=p_run_id and x.status<>'COMPLETE') and not exists (
      select 1 from private.bpay_next_run_worker w
      where w.run_id=p_run_id and w.status<>'READY' and not (w.status='CANCELLED' and exists(select 1 from private.bpay_next_worker_reprepare_request x
        where x.run_worker_id=w.id and x.run_id=p_run_id and x.action='RELEASE' and x.status='COMPLETE'
          and x.target_preparation_revision=w.preparation_revision and x.captured_preparation_revision=w.captured_preparation_revision
          and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count)));
    return pg_catalog.jsonb_build_object('phase','REVIEW','replay',true,
      'review_revision',v_run.review_revision,
      'selected_candidates',v_run.selected_candidate_count,
      'all_ready',v_all_ready);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.selected_candidate_count<1 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_REVIEW_RUN_STATE_INVALID';
  end if;
  select c.* into strict v_command
    from private.bpay_next_run_command rc
    join private.bpay_next_command c on c.id=rc.command_id
    where rc.run_id=p_run_id;
  if v_command.status<>'SEALED' or v_command.module_epoch<>v_module_epoch
     or v_command.expected_member_count<>v_run.selected_candidate_count
     or v_command.enrolled_member_count<>v_run.selected_candidate_count then
    return pg_catalog.jsonb_build_object('phase','PREPARING',
      'reason','ENROLLMENT_INCOMPLETE');
  end if;
  if exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_id=p_run_id and x.status<>'COMPLETE') then
    return pg_catalog.jsonb_build_object('phase','PREPARING','reason','WORKERS_INCOMPLETE');end if;
  if exists (select 1 from private.bpay_next_job j
       where j.command_id=v_command.id and j.job_kind='PREPARE'
         and j.status<>'DONE') then
    return pg_catalog.jsonb_build_object('phase','PREPARING',
      'reason','WORKERS_INCOMPLETE');
  end if;
  -- The sealed command enrolled exactly the selected member count. Each
  -- PREPARE job can become DONE only through an owner that creates its worker.
  -- Probe the partial indexes for unfinished/non-ready rows instead of
  -- counting every selected Candidate or frozen line at the final boundary.
  v_all_ready:=not exists (
    select 1 from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.status<>'READY' and not (w.status='CANCELLED' and exists(select 1 from private.bpay_next_worker_reprepare_request x
        where x.run_worker_id=w.id and x.run_id=p_run_id and x.action='RELEASE' and x.status='COMPLETE'
          and x.target_preparation_revision=w.preparation_revision and x.captured_preparation_revision=w.captured_preparation_revision
          and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count)));
  update private.bpay_next_pay_run
    set status='REVIEW',review_revision=review_revision+1
    where id=p_run_id
    returning * into v_run;
  return pg_catalog.jsonb_build_object('phase','REVIEW','replay',false,
    'review_revision',v_run.review_revision,
    'selected_candidates',v_run.selected_candidate_count,
    'all_ready',v_all_ready);
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_confirm_simple_run_v1
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

-- DR-05 current-parent successor: private.bpay_next_segment_prepare_owner_v1
create or replace function private.bpay_next_segment_prepare_owner_v1(p_run_worker_id uuid,p_phase text)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_run_worker w
    join private.bpay_next_job j on j.candidate_id=w.candidate_id and private.bpay_next_prepare_job_run_v1(j.id)=w.run_id
      and private.bpay_next_prepare_job_active_v1(j.id)
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_pay_run r on r.id=w.run_id and r.status='PREPARING' and r.selection_state='SEALED'
    where w.id=p_run_worker_id and w.status='PREPARING' and j.job_kind='PREPARE' and j.phase=p_phase
      and j.status='LEASED' and j.lease_until_utc>clock_timestamp())
$function$;

-- DR-05 current-parent successor: private.bpay_next_run_work_query_pin_guard_v2
create or replace function private.bpay_next_run_work_query_pin_guard_v2()
returns trigger language plpgsql volatile security definer
set search_path=pg_catalog,private,public
as $function$
declare v_origin record;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PIN_IMMUTABLE';
  end if;
  -- These are exact indexed identities. The pin owner already holds the
  -- established module/run/Candidate/job/WORK/revision locks. This trigger
  -- acquires no admission/business lock and calls no live Source reader.
  select r.physical_timesheet_id,r.physical_timesheet_version,
      r.week_ending_date,w.booking_id,w.contract_id,c.client_id
    into v_origin
    from private.bpay_next_run_work captured
    join private.bpay_next_run_worker worker
      on worker.id=captured.run_worker_id and worker.candidate_id=captured.candidate_id
    join private.bpay_next_pay_run run on run.id=worker.run_id
    join private.bpay_next_job job on job.id=new.prepare_job_id
      and private.bpay_next_prepare_job_run_v1(job.id)=run.id and private.bpay_next_prepare_job_active_v1(job.id)
      and job.candidate_id=worker.candidate_id
    join private.bpay_next_command command on command.id=job.command_id
      and command.agency_sequence=job.command_sequence
    join private.bpay_next_worker_control owner on owner.candidate_id=worker.candidate_id
    join private.bpay_next_module_control module on module.id=1
    join private.bpay_next_run_selection selected on selected.run_id=run.id
      and selected.work_id=captured.work_id and selected.candidate_id=worker.candidate_id
    join private.bpay_next_work_choice choice on choice.run_id=run.id and choice.work_id=captured.work_id
    join private.bpay_next_work w on w.id=captured.work_id and w.candidate_id=worker.candidate_id
    join private.bpay_next_work_revision r on r.work_id=w.id and r.id=captured.captured_revision_id
    join public.contracts c on c.id=w.contract_id and c.candidate_id=worker.candidate_id
    join public.timesheets physical_root on physical_root.timesheet_id=r.physical_timesheet_id
      and physical_root.contract_id=w.contract_id and physical_root.booking_id=w.booking_id
      and physical_root.version=r.physical_timesheet_version
      and physical_root.week_ending_date=r.week_ending_date
    where captured.id=new.run_work_id and captured.run_worker_id=new.run_worker_id
      and captured.candidate_id=new.candidate_id and captured.work_id=new.work_id
      and captured.captured_revision_id=new.captured_revision_id
      and module.active_owner='NEXT' and module.owner_epoch=new.module_epoch
      and job.module_epoch=new.module_epoch and command.module_epoch=new.module_epoch
      and command.command_kind='PREPARE' and command.status='SEALED'
      and job.job_kind='PREPARE' and job.status='LEASED' and job.phase in ('NEW','PINNING')
      and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
      and job.owner_epoch=owner.active_owner_epoch
      and run.status='PREPARING' and run.selection_state='SEALED' and worker.status='PREPARING'
      and choice.selection_state='SEALED' and choice.expected_revision_id=r.id
      and w.work_kind='SOURCE' and w.approval_state='APPROVED'
      and w.current_revision_id=r.id and w.applied_revision_id=r.id
      and r.source_kind in ('SOURCE','PROTECTED') and r.source_event_id is not null
      and r.approved_at_utc is not null and r.sealed_at_utc is not null
      and r.week_ending_date=w.week_ending_date and c.client_id is not null;
  if not found then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_PIN_OWNER_MISMATCH';
  end if;
  if new.scope is not null and (
      (new.scope->>'root_timesheet_id')::uuid is distinct from v_origin.physical_timesheet_id
      or (new.scope->>'root_version')::bigint is distinct from v_origin.physical_timesheet_version
      -- QueryV4 preserves the stored raw root/WORK booking in Scope. Trimmed
      -- lookup/collision checks belong to the Source owner, never this identity.
      or new.scope->>'family_booking_id' is distinct from v_origin.booking_id
      or (new.scope->>'candidate_id')::uuid is distinct from new.candidate_id
      or (new.scope->>'contract_id')::uuid is distinct from v_origin.contract_id
      or (new.scope->>'client_id')::uuid is distinct from v_origin.client_id
      or (new.scope->>'week_ending_date')::date is distinct from v_origin.week_ending_date) then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_PIN_SCOPE_MISMATCH';
  end if;
  -- Server time belongs to the actual successful INSERT, not caller evidence
  -- or an earlier transaction-start time before a downstream lock wait.
  new.captured_at_utc:=pg_catalog.clock_timestamp();
  return new;
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_work_collection_capture_guard_v1
create or replace function private.bpay_next_work_collection_capture_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;v_totals record;
begin
  if tg_table_name='bpay_next_case_period' then
    if tg_op='UPDATE' then
      if new.work_collection_id is distinct from old.work_collection_id then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_IMMUTABLE';
      end if;
      if new.work_collection_id is not null and
         ((new.case_component_id,new.case_id,new.candidate_id,new.pay_week_start,new.rule_id,
           new.opening_outstanding_source_ex_vat,new.opening_due_source_ex_vat)
           is distinct from (old.case_component_id,old.case_id,old.candidate_id,old.pay_week_start,old.rule_id,
             old.opening_outstanding_source_ex_vat,old.opening_due_source_ex_vat)
          or new.period_revision<>old.period_revision+1
          or new.realised_recovery_source_ex_vat<old.realised_recovery_source_ex_vat
          or new.active_payout_source_ex_vat<>0
          or not exists(select 1 from private.bpay_next_case_component x
            join private.bpay_next_finance_case c on c.id=x.case_id and c.candidate_id=x.candidate_id
            where x.id=new.case_component_id and x.id=new.work_collection_id
              and x.case_id=new.case_id and x.candidate_id=new.candidate_id
              and x.recovered_source_ex_vat>=new.realised_recovery_source_ex_vat
              and x.active_recovery_source_ex_vat>=new.active_unrealised_recovery_source_ex_vat
              and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
                is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat))) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_COUNTER_INVALID';
      end if;
      return new;
    end if;
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
    if v_basis.id is distinct from new.work_collection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_case_instruction' then
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
    v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
    if v_basis.id is distinct from new.work_collection_id
       or new.valuation_policy_id is distinct from v_basis.original_valuation_policy_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  -- Required WB36 attribution counters are subordinate immutable observations.
  -- The old managed-capture transition remains exactly the ALLOCATE arm below.
  if tg_op='UPDATE' and (to_jsonb(new)-array['segment_member_count','segment_included_positive_ex_vat',
      'segment_excluded_positive_ex_vat','segment_recovery_ex_vat','segment_held_overhang_ex_vat','segment_attribution_complete',
      'needs_financial_resolution','work_collection_id','work_collection_revision'])
    is not distinct from (to_jsonb(old)-array['segment_member_count','segment_included_positive_ex_vat',
      'segment_excluded_positive_ex_vat','segment_recovery_ex_vat','segment_held_overhang_ex_vat','segment_attribution_complete',
      'needs_financial_resolution','work_collection_id','work_collection_revision']) then
    select count(*) n,coalesce(sum(p.included_positive_source_ex_vat),0) inc,
      coalesce(sum(p.excluded_positive_source_ex_vat),0) exc,coalesce(sum(p.recovery_source_ex_vat),0) rec,
      coalesce(sum(p.held_overhang_source_ex_vat),0) overhang,sum(p.approved_source_ex_vat) a,
      sum(p.realised_source_ex_vat) c,sum(p.held_source_ex_vat) h into v_totals
      from private.bpay_next_run_work_segment_parent p where p.run_work_id=new.run_work_id and p.component_key=new.component_key;
    if old.segment_attribution_complete or new.segment_member_count not between old.segment_member_count and old.segment_member_count+1
      or not private.bpay_next_segment_prepare_owner_v1(new.run_worker_id,'RESERVE')
      or (new.segment_member_count,new.segment_included_positive_ex_vat,new.segment_excluded_positive_ex_vat,
          new.segment_recovery_ex_vat,new.segment_held_overhang_ex_vat) is distinct from
        (v_totals.n,v_totals.inc,v_totals.exc,v_totals.rec,v_totals.overhang)
      or (new.segment_attribution_complete and (new.segment_member_count<>new.segment_expected_member_count or
        (new.approved_source_ex_vat,new.realised_source_ex_vat,new.held_source_ex_vat) is distinct from
          (v_totals.a,v_totals.c,v_totals.h))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_COUNTER_INVALID';
    end if;
    if (new.needs_financial_resolution,new.work_collection_id,new.work_collection_revision)
      is distinct from (old.needs_financial_resolution,old.work_collection_id,old.work_collection_revision) then
      if not new.segment_attribution_complete or new.segment_member_count<>old.segment_member_count then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CLASSIFICATION_BEFORE_SEAL';
      end if;
      if new.work_collection_id is not null then
        select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
        v_basis:=private.bpay_next_work_collection_origin_v1(v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id);
        if v_basis.id is distinct from new.work_collection_id or not v_basis.segment_attribution_complete
          or new.work_collection_revision is distinct from v_basis.reconcile_revision or new.needs_financial_resolution
          or new.segment_recovery_ex_vat<>v_basis.observed_segment_negative_ex_vat or new.segment_recovery_ex_vat<=0
          or new.segment_held_overhang_ex_vat<>0 or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
          or (new.work_id,new.component_key,new.captured_revision_id,new.approved_source_ex_vat,new.realised_source_ex_vat)
            is distinct from (v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_COLLECTION_CLASSIFICATION_INVALID';
        end if;
      elsif not new.needs_financial_resolution and not (new.approved_source_ex_vat=0 and new.realised_source_ex_vat=0 and new.held_source_ex_vat=0) then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_FINANCIAL_RESOLUTION_UNEXPLAINED';
      end if;
    end if;
    return new;
  end if;
  if tg_op<>'INSERT' then
    if tg_op<>'UPDATE' or old.work_collection_id is null or old.managed_capture_completed
       or not new.managed_capture_completed
       or (pg_catalog.to_jsonb(new)-'managed_capture_completed')
         is distinct from (pg_catalog.to_jsonb(old)-'managed_capture_completed') then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_IMMUTABLE';
    end if;
  elsif new.work_collection_id is null then
    return new;
  else
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
    v_basis:=private.bpay_next_work_collection_origin_v1(v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id);
    if v_basis.id is distinct from new.work_collection_id or new.managed_capture_completed
       or new.work_collection_revision is distinct from v_basis.reconcile_revision
       or (new.captured_revision_id,new.approved_source_ex_vat,new.realised_source_ex_vat)
         is distinct from (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat)
       or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
       or (new.held_source_ex_vat,new.held_target_ex_vat,new.held_target_vat,new.held_target_inc_vat)
         is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ORIGIN_INVALID';
    end if;
  end if;
  if not exists(select 1 from private.bpay_next_run_work rw
    join private.bpay_next_run_worker w on w.id=rw.run_worker_id and w.status='PREPARING'
    join private.bpay_next_job j on j.candidate_id=w.candidate_id
      and private.bpay_next_prepare_job_run_v1(j.id)=w.run_id and private.bpay_next_prepare_job_active_v1(j.id)
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where rw.id=new.run_work_id and rw.run_worker_id=new.run_worker_id and rw.work_id=new.work_id
      and rw.captured_revision_id=new.captured_revision_id and j.job_kind='PREPARE' and j.status='LEASED'
      and j.phase=case when tg_op='INSERT' then 'RESERVE' else 'ALLOCATE' end
      and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_COLLECTION_CAPTURE_OWNER_STALE';
  end if;
  return new;
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_reconcile_work_collection_v1
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
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARE') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
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

-- DR-05 current-parent successor: private.bpay_next_work_collection_guard_v1
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
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARE')
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

-- DR-05 current-parent successor: private.bpay_next_complete_retained_segment_parent_v1
create or replace function private.bpay_next_complete_retained_segment_parent_v1(
  p_run_worker_id uuid,p_run_work_id uuid,p_component_key text
) returns boolean language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_parent private.bpay_next_run_position%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_work_id uuid;v_totals record;v_target_totals record;v_collection private.bpay_next_work_collection%rowtype;v_needs_resolution boolean;
begin
  if not private.bpay_next_segment_prepare_owner_v1(p_run_worker_id,'RESERVE') then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=p_run_worker_id;
  select p.* into strict v_parent from private.bpay_next_run_position p
    where p.run_worker_id=p_run_worker_id and p.run_work_id=p_run_work_id and p.component_key=p_component_key for update;
  if v_parent.segment_attribution_complete then return true;end if;
  if v_parent.segment_member_count<>v_parent.segment_expected_member_count then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';end if;
  v_work_id:=v_parent.work_id;
      select sum(p.approved_source_ex_vat) a,sum(p.realised_source_ex_vat) c,sum(p.held_source_ex_vat) h
        into v_totals from private.bpay_next_run_work_segment_parent p
        where p.run_work_id=p_run_work_id and p.component_key=p_component_key;
      select (select row(coalesce(sum(e.target_ex_vat),0),coalesce(sum(e.target_vat),0),coalesce(sum(e.target_inc_vat),0))
          from private.bpay_next_work_segment_effect_share e where e.work_id=v_work_id and e.component_key=p_component_key) c,
        (select row(coalesce(sum(s.target_reserved_ex_vat),0),coalesce(sum(s.target_reserved_vat),0),coalesce(sum(s.target_reserved_inc_vat),0))
          from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
          where s.work_id=v_work_id and s.component_key=p_component_key) h into v_target_totals;
      if (v_totals.a,v_totals.c,v_totals.h) is distinct from
        (v_parent.approved_source_ex_vat,v_parent.realised_source_ex_vat,v_parent.held_source_ex_vat)
        or v_target_totals.c is distinct from row(v_parent.realised_target_ex_vat,v_parent.realised_target_vat,v_parent.realised_target_inc_vat)
        or v_target_totals.h is distinct from row(v_parent.held_target_ex_vat,v_parent.held_target_vat,v_parent.held_target_inc_vat) then
        return false;
      end if;
      select p.* into strict v_parent from private.bpay_next_run_position p
        where p.run_work_id=p_run_work_id and p.component_key=p_component_key;
      v_collection:=null;v_needs_resolution:=v_parent.needs_financial_resolution;
      if v_parent.segment_recovery_ex_vat>0 and v_worker.target_pay_channel='PAYE' then
        select b.* into v_collection from private.bpay_next_work_collection b
          where b.work_id=v_work_id and b.component_key=p_component_key and b.segment_attribution_complete;
        if v_collection.case_id is not null then
          v_collection:=private.bpay_next_work_collection_origin_v1(v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
        else v_collection:=null;end if;
        if v_collection.id is not null and v_parent.segment_held_overhang_ex_vat=0 then v_needs_resolution:=false;
        else v_needs_resolution:=true;end if;
      elsif v_parent.segment_recovery_ex_vat>0 or v_parent.segment_held_overhang_ex_vat>0 then v_needs_resolution:=true;
      elsif v_parent.approved_source_ex_vat=0 and v_parent.realised_source_ex_vat=0 and v_parent.held_source_ex_vat=0 then
        v_needs_resolution:=false;
      end if;
      update private.bpay_next_run_position set segment_attribution_complete=true,
        needs_financial_resolution=v_needs_resolution,work_collection_id=v_collection.id,work_collection_revision=v_collection.reconcile_revision
        where run_work_id=p_run_work_id and component_key=p_component_key;
      update private.bpay_next_run_worker set financial_resolution_count=financial_resolution_count
        +case when v_needs_resolution then 1 else 0 end-case when v_parent.needs_financial_resolution then 1 else 0 end,
        expected_managed_position_count=expected_managed_position_count+case when v_collection.id is not null then 1 else 0 end
          -case when v_parent.work_collection_id is not null then 1 else 0 end
        where id=p_run_worker_id;

  return true;
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_capture_segment_page_v1
create or replace function private.bpay_next_capture_segment_page_v1(p_run_worker_id uuid,p_limit integer)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_state private.bpay_next_run_segment_capture%rowtype;
  v_member record; v_current private.bpay_next_approved_segment%rowtype;
  v_parent private.bpay_next_run_position%rowtype; v_selected private.bpay_next_selected_segment%rowtype;
  v_worker private.bpay_next_run_worker%rowtype; v_segment_id uuid; v_disposition text;
  v_balance record; v_totals record;v_target_totals record;v_collection private.bpay_next_work_collection%rowtype;
  v_needs_resolution boolean;v_retry_seen integer:=0;v_retry record;v_seen integer:=0; v_a numeric;v_p numeric;v_r numeric;v_h numeric;
begin
  if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_SEGMENT_CAPTURE_LIMIT_INVALID';end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=p_run_worker_id for update;
  if not private.bpay_next_segment_prepare_owner_v1(p_run_worker_id,'RESERVE') then
    raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_CAPTURE_OWNER_STALE';end if;
  select s.* into v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  if not found then
    insert into private.bpay_next_run_segment_capture(run_worker_id,expected_count)
      select p_run_worker_id,coalesce(sum(p.segment_expected_member_count),0)
      from private.bpay_next_run_position p where p.run_worker_id=p_run_worker_id;
    select s.* into strict v_state from private.bpay_next_run_segment_capture s where s.run_worker_id=p_run_worker_id for update;
  end if;
  if v_state.complete then return jsonb_build_object('done',true,'rows_visited',0);end if;
  -- Recheck only spent incomplete parents, before advancing the original seek.
  for v_retry in select run_work_id,component_key,work_id from private.bpay_next_run_position
    where run_worker_id=p_run_worker_id and not segment_attribution_complete
      and segment_member_count=segment_expected_member_count and segment_member_count>0
    order by work_id,component_key limit p_limit loop
    v_retry_seen:=v_retry_seen+1;
    if not private.bpay_next_complete_retained_segment_parent_v1(p_run_worker_id,v_retry.run_work_id,v_retry.component_key) then
      update private.bpay_next_run_worker set status='REVIEW',review_issue_code='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE',
        review_issue_work_id=v_retry.work_id where id=p_run_worker_id;
      return jsonb_build_object('done',false,'rows_visited',v_retry_seen);
    end if;
  end loop;
  if v_retry_seen=p_limit then return jsonb_build_object('done',false,'rows_visited',v_retry_seen);end if;

  for v_member in select s.* from private.bpay_next_run_segment_members_v1(p_run_worker_id,
    v_state.cursor_work_id,v_state.cursor_component_key,v_state.cursor_segment_key,p_limit-v_retry_seen) s
  loop
    select p.* into strict v_parent from private.bpay_next_run_position p
      where p.run_work_id=v_member.run_work_id and p.component_key=v_member.component_key for update;
    select s.* into v_current from private.bpay_next_approved_segment s
      where s.revision_id=v_member.captured_revision_id and s.segment_key=v_member.segment_key;
    select s.* into v_selected from private.bpay_next_selected_segment s
      where s.run_id=v_worker.run_id and s.work_id=v_member.work_id and s.segment_key=v_member.segment_key;
    v_disposition:=case when v_current.id is null then 'REMOVED'
      when v_selected.approved_segment_id=v_current.id then v_selected.disposition else 'NOT_SELECTED' end;
    insert into private.bpay_next_run_work_segment(run_work_id,run_worker_id,work_id,captured_revision_id,
      segment_key,approved_segment_id,disposition)
    values(v_member.run_work_id,p_run_worker_id,v_member.work_id,v_member.captured_revision_id,
      v_member.segment_key,v_current.id,v_disposition) on conflict(run_work_id,segment_key) do nothing;
    select s.id into strict v_segment_id from private.bpay_next_run_work_segment s
      where s.run_work_id=v_member.run_work_id and s.segment_key=v_member.segment_key
        and s.approved_segment_id is not distinct from v_current.id and s.disposition=v_disposition;
    v_a:=case when v_current.component_key=v_member.component_key then v_current.approved_source_ex_vat else 0 end;
    select b.realised_source_ex_vat,b.active_held_source_ex_vat as held_source_ex_vat into strict v_balance
      from private.bpay_next_work_segment_gross_balance_v1(
      v_member.work_id,v_member.component_key,v_member.segment_key) b;
    v_p:=greatest(v_a-v_balance.realised_source_ex_vat-v_balance.held_source_ex_vat,0);
    v_r:=greatest(v_balance.realised_source_ex_vat-v_a,0);
    v_h:=greatest(v_balance.held_source_ex_vat-greatest(v_a-v_balance.realised_source_ex_vat,0),0);
    insert into private.bpay_next_run_work_segment_parent(run_work_segment_id,run_work_id,work_id,captured_revision_id,
      segment_key,component_key,approved_segment_id,approved_line_id,work_date,approved_source_ex_vat,
      realised_source_ex_vat,held_source_ex_vat,residual_source_ex_vat,included_positive_source_ex_vat,
      excluded_positive_source_ex_vat,recovery_source_ex_vat,held_overhang_source_ex_vat)
    values(v_segment_id,v_member.run_work_id,v_member.work_id,v_member.captured_revision_id,v_member.segment_key,
      v_member.component_key,case when v_current.component_key=v_member.component_key then v_current.id end,
      case when v_current.component_key=v_member.component_key then v_current.approved_line_id end,
      case when v_current.component_key=v_member.component_key then v_current.work_date end,v_a,
      v_balance.realised_source_ex_vat,v_balance.held_source_ex_vat,v_a-v_balance.realised_source_ex_vat-v_balance.held_source_ex_vat,
      case when v_disposition='INCLUDED' then v_p else 0 end,
      case when v_disposition='INCLUDED' then 0 else v_p end,v_r,v_h);
    update private.bpay_next_run_position set segment_member_count=segment_member_count+1,
      segment_included_positive_ex_vat=segment_included_positive_ex_vat+case when v_disposition='INCLUDED' then v_p else 0 end,
      segment_excluded_positive_ex_vat=segment_excluded_positive_ex_vat+case when v_disposition='INCLUDED' then 0 else v_p end,
      segment_recovery_ex_vat=segment_recovery_ex_vat+v_r,segment_held_overhang_ex_vat=segment_held_overhang_ex_vat+v_h
      where run_work_id=v_member.run_work_id and component_key=v_member.component_key;
    if v_parent.segment_member_count+1=v_parent.segment_expected_member_count then
      if not private.bpay_next_complete_retained_segment_parent_v1(p_run_worker_id,v_member.run_work_id,v_member.component_key) then
        update private.bpay_next_run_segment_capture set cursor_work_id=v_member.work_id,
          cursor_component_key=v_member.component_key,cursor_segment_key=v_member.segment_key,
          processed_count=v_state.processed_count+v_seen+1 where run_worker_id=p_run_worker_id;
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE',
          review_issue_work_id=v_member.work_id where id=p_run_worker_id;
        return jsonb_build_object('done',false,'rows_visited',v_retry_seen+v_seen+1);
      end if;
    end if;
    v_state.cursor_work_id:=v_member.work_id;v_state.cursor_component_key:=v_member.component_key;
    v_state.cursor_segment_key:=v_member.segment_key;v_seen:=v_seen+1;
  end loop;
  v_state.processed_count:=v_state.processed_count+v_seen;
  v_state.complete:=v_state.processed_count=v_state.expected_count;
  if v_state.complete and exists(select 1 from private.bpay_next_run_position p
    where p.run_worker_id=p_run_worker_id and not p.segment_attribution_complete) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
  end if;
  update private.bpay_next_run_segment_capture set cursor_work_id=v_state.cursor_work_id,
    cursor_component_key=v_state.cursor_component_key,cursor_segment_key=v_state.cursor_segment_key,
    processed_count=v_state.processed_count,complete=v_state.complete where run_worker_id=p_run_worker_id;
  return jsonb_build_object('done',v_state.complete,'rows_visited',v_retry_seen+v_seen);
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_worker_release_complete_v1
create or replace function private.bpay_next_worker_release_complete_v1(p_run_worker_id uuid,p_cutoff timestamptz)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_run_worker w
    join private.bpay_next_worker_reprepare_request x on x.run_worker_id=w.id and x.target_preparation_revision=w.preparation_revision
    join private.bpay_next_job j on j.id=x.job_id and j.id=w.active_prepare_job_id and j.command_id=x.command_id
      and j.candidate_id=w.candidate_id and j.module_epoch=x.module_epoch and j.job_kind='PREPARE' and j.status='DONE' and j.phase='CANCELLED'
    join private.bpay_next_run_command rc on rc.run_id=w.run_id and rc.command_id=x.original_prepare_command_id
    join private.bpay_next_job original on original.id=x.input_prepare_job_id and original.id=w.input_prepare_job_id
      and original.command_id=rc.command_id and original.candidate_id=w.candidate_id and original.job_kind='PREPARE' and original.status='DONE'
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=x.module_epoch
    where w.id=p_run_worker_id and w.status='CANCELLED' and x.run_id=w.run_id and x.candidate_id=w.candidate_id
      and x.action='RELEASE' and x.status='COMPLETE' and x.stage='COMPLETE' and x.finished_at_utc<=p_cutoff
      and x.captured_preparation_revision=w.captured_preparation_revision and x.selection_revision=w.case_selection_revision
      and x.expected_work_count=w.captured_line_count and x.released_work_count=x.expected_work_count
      and x.released_case_hold_count=x.expected_case_hold_count and w.active_case_hold_count=0
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and not exists(select 1 from private.bpay_next_case_hold h where h.run_worker_id=w.id and h.status='ACTIVE')
      and not exists(select 1 from private.bpay_next_hold h join private.bpay_next_run_line l on l.id=h.run_line_id where l.run_worker_id=w.id and h.status='ACTIVE')
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id))
$function$;

-- DR-05 current-parent successor: private.bpay_next_accept_preparation_expiry_v1
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

-- DR-05 current-parent successor: private.bpay_next_enroll_preparation_expiry_one_v1
create or replace function private.bpay_next_enroll_preparation_expiry_one_v1(p_command_id uuid)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_original_unenrolled boolean:=false;v_already_released boolean:=false;v_active private.bpay_next_job%rowtype;
 v_run_id uuid;v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_original private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_candidate uuid;v_member bigint;v_job uuid;v_state uuid;
begin
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id into strict v_run_id from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for share;
  select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id for update;
  select * into strict v_command from private.bpay_next_command where id=p_command_id for update;
  if v_command.command_kind<>'PREPARATION_EXPIRY' or v_command.module_epoch<>v_epoch
     or v_command.expected_member_count<>v_request.expected_candidate_count
     or v_command.enrolled_member_count<>v_request.enrolled_candidate_count
     or v_run.confirmed_at_utc is not null or v_run.preparation_expires_at_utc<>v_request.captured_deadline
     or v_run.selection_count<>v_request.selection_count or v_run.selection_page_count<>v_request.selection_page_count
     or v_run.selected_candidate_count<>v_request.expected_candidate_count
     or v_run.review_revision<>v_request.selection_review_revision+1 then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ENROLLMENT_SCOPE_INVALID';end if;
  if v_request.expected_candidate_count=0 then
    if v_run.status<>'CANCELLED' or v_request.status<>'EXPIRED' or v_command.status<>'COMPLETE' then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EMPTY_NOT_COMPLETE';end if;
    return null;
  end if;
  if v_run.status<>'CANCELLING' or v_request.status<>'CANCELLING' or v_command.status<>'ENROLLING'
     or v_request.enrolled_candidate_count>=v_request.expected_candidate_count then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_NOT_ENROLLABLE';end if;
  v_member:=v_request.enrolled_candidate_count+1;
  select candidate_id into strict v_candidate from private.bpay_next_selection_candidate where run_id=v_run_id and member_no=v_member;
if v_request.original_prepare_command_id is not null then
  v_original_unenrolled:=
    v_run.selection_state='OPEN'
    and v_run.accepted_choice_descriptor is not null
    and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
    and private.bpay_next_preparation_expiry_fenced_v1(
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
        message='BPAY_NEXT_EXPIRY_ORIGINAL_JOB_INVALID';
    end if;
    if v_original.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',
        lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_EXPIRED' where id=v_original.id;
    end if;
  end if;
end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run_id and candidate_id=v_candidate;
  if v_worker.id is not null then
    v_already_released:=private.bpay_next_worker_release_complete_v1(v_worker.id,v_request.accepted_at_utc);
    if v_original.id is null or (v_worker.status not in ('PREPARING','REVIEW','READY') and not v_already_released) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_NOT_PREPARATION';end if;
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
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ACTIVE_RETRY_INVALID';end if;
    if v_active.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_EXPIRED' where id=v_active.id;
    end if;
  end if;
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,v_member);
  insert into private.bpay_next_job(command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
    values(p_command_id,v_command.agency_sequence,v_epoch,v_candidate,'PREPARATION_EXPIRY','READY','NEW',1) returning id into v_job;
  insert into private.bpay_next_preparation_expiry_worker(command_id,candidate_id,run_id,member_no,job_id,run_worker_id,
    original_job_id,original_job_status,original_job_phase,original_job_cursor,original_position_work_cursor,
    original_position_component_cursor,original_applied_line_count,original_worker_status,original_review_issue_code,
    original_review_issue_work_id,preparation_revision,selection_revision,draft_state_id,expected_work_count,expected_case_hold_count,status,stage,expected_execution_revision,already_worker_released)
    values(p_command_id,v_candidate,v_run_id,v_member,v_job,v_worker.id,v_original.id,v_original.status,v_original.phase,v_original.cursor_key,
      v_original.position_work_cursor,v_original.position_component_cursor,v_original.applied_line_count,v_worker.status,
      v_worker.review_issue_code,v_worker.review_issue_work_id,v_worker.captured_preparation_revision,v_worker.case_selection_revision,v_state,
      case when v_already_released then 0 else coalesce(v_worker.captured_line_count,0) end,
      case when v_already_released then 0 else coalesce(v_worker.active_case_hold_count,0) end,'READY','NEW',v_worker.preparation_revision,v_already_released);
  update private.bpay_next_preparation_expiry_request set enrolled_candidate_count=v_member where command_id=p_command_id;
  update private.bpay_next_command set enrolled_member_count=v_member,
    status=case when v_member=expected_member_count then 'SEALED' else 'ENROLLING' end,
    sealed_at_utc=case when v_member=expected_member_count then pg_catalog.clock_timestamp() else null end where id=p_command_id;
  return v_job;
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_preparation_expiry_guard_v1
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
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
       or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
       or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
       or (old.started_at_utc is not null and new.started_at_utc is distinct from old.started_at_utc)
       or (old.stage='NEW' and new.stage not in ('WORK','CASE')) or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','FINAL')) or (old.stage='FINAL' and new.stage<>'COMPLETE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_TRANSITION_INVALID';end if;
  end if;
  return new;
end
$function$;

-- DR-05 current-parent successor: private.bpay_next_expired_work_collection_scope_v1
create or replace function private.bpay_next_expired_work_collection_scope_v1(
  p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null
) returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_preparation_expiry_worker x on x.job_id=j.id and x.command_id=j.command_id and x.candidate_id=j.candidate_id
    join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
    join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_EXPIRY'
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
    where j.id=p_job_id and j.job_kind='PREPARATION_EXPIRY' and j.status='LEASED' and j.phase='RELEASE'
      and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      and x.status='RELEASING' and x.stage='CASE' and x.expected_case_hold_count>0
      and x.released_case_hold_count<x.expected_case_hold_count
      and q.status='CANCELLING' and q.accepted_at_utc>=q.captured_deadline
      and q.captured_deadline=r.preparation_expires_at_utc and r.preparation_expires_at_utc=r.created_at_utc+interval '72 hours'
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

-- DR-05 current-parent successor: private.bpay_next_expired_prepare_receipt_v1
create or replace function private.bpay_next_expired_prepare_receipt_v1(p_job_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_request record;v_worker private.bpay_next_run_worker%rowtype;
  v_binding private.bpay_next_preparation_expiry_worker%rowtype;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id and job_kind='PREPARE';
  select r.* into strict v_run from private.bpay_next_pay_run r join private.bpay_next_run_command rc on rc.run_id=r.id
    where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
  select command_id,original_prepare_command_id,captured_deadline into strict v_request from private.bpay_next_preparation_expiry_request where run_id=v_run.id
    and original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=v_run.id);
  if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';end if;
  if v_run.status not in ('CANCELLING','CANCELLED') or v_run.confirmed_at_utc is not null
     or v_request.captured_deadline<>v_run.preparation_expires_at_utc
     or not private.bpay_next_preparation_expiry_fenced_v1(v_run.id,v_request.original_prepare_command_id)
     or v_job.phase not in ('EXPIRED','READY','DONE','CANCELLED')
     or not exists(select 1 from private.bpay_next_command where id=v_request.command_id
       and command_kind='PREPARATION_EXPIRY' and module_epoch=v_job.module_epoch)
     or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' and owner_epoch=v_job.module_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRED_PREPARE_RECEIPT_SCOPE_INVALID';end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;
  select * into v_binding from private.bpay_next_preparation_expiry_worker where command_id=v_request.command_id and candidate_id=v_job.candidate_id;
  if v_binding.job_id is not null and v_binding.original_job_id is distinct from v_job.id and not exists(
    select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=p_job_id and x.command_id=v_job.command_id
      and x.run_worker_id=v_binding.run_worker_id and x.run_id=v_run.id and x.candidate_id=v_job.candidate_id
      and x.input_prepare_job_id=v_binding.original_job_id and x.original_prepare_command_id=v_request.original_prepare_command_id
      and x.module_epoch=v_job.module_epoch and x.target_preparation_revision=v_binding.expected_execution_revision
      and x.captured_preparation_revision=v_binding.preparation_revision and x.selection_revision=v_binding.selection_revision) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRED_PREPARE_RECEIPT_JOB_MISMATCH';end if;
  return pg_catalog.jsonb_build_object('done',true,'phase','EXPIRED','cursor',v_job.cursor_key,
    'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
    'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
    'issue_code',coalesce(v_worker.review_issue_code,v_binding.original_review_issue_code),
    'issue_work_id',coalesce(v_worker.review_issue_work_id,v_binding.original_review_issue_work_id));
end
$function$;


-- Private worker identity fence: immutable economics, exact new execution only.
create or replace function private.bpay_next_worker_execution_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_WORKER_EXECUTION_IMMUTABLE';end if;
  if tg_op='UPDATE' then
    if new.captured_preparation_revision is distinct from old.captured_preparation_revision
      or (old.input_prepare_job_id is not null and new.input_prepare_job_id is distinct from old.input_prepare_job_id)
      or new.preparation_revision::numeric not between old.preparation_revision::numeric and old.preparation_revision::numeric+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_EXECUTION_IMMUTABLE';end if;
    if new.preparation_revision<>old.preparation_revision and not exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.run_worker_id=new.id and x.run_id=new.run_id and x.candidate_id=new.candidate_id
        and x.expected_preparation_revision=old.preparation_revision and x.target_preparation_revision=new.preparation_revision
        and x.captured_preparation_revision=new.captured_preparation_revision and x.status<>'COMPLETE'
        and x.input_prepare_job_id=new.input_prepare_job_id
        and ((x.initiator_kind='SYSTEM' and new.active_prepare_job_id=x.job_id)
          or (x.initiator_kind='OFFICE' and x.status='ACCEPTED' and new.active_prepare_job_id is null))) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  elsif new.preparation_revision<>1 or new.captured_preparation_revision<>1 then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_EXECUTION_INITIAL_INVALID';end if;
  if new.input_prepare_job_id is not null and not exists(select 1 from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id and rc.run_id=new.run_id
    where j.id=new.input_prepare_job_id and j.candidate_id=new.candidate_id and j.job_kind='PREPARE') then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_INPUT_JOB_INVALID';end if;
  if new.active_prepare_job_id is not null and (tg_op='INSERT' or new.active_prepare_job_id is distinct from old.active_prepare_job_id)
    and not ((new.preparation_revision=1 and new.active_prepare_job_id=new.input_prepare_job_id)
      or exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_worker_id=new.id
        and x.run_id=new.run_id and x.candidate_id=new.candidate_id and x.target_preparation_revision=new.preparation_revision
        and x.captured_preparation_revision=new.captured_preparation_revision and x.job_id=new.active_prepare_job_id
        and x.input_prepare_job_id=new.input_prepare_job_id and x.status='RUNNING')) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_ACTIVE_JOB_INVALID';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_worker_execution_guard_v1 on private.bpay_next_run_worker;
create trigger bpay_next_worker_execution_guard_v1 before insert or update or delete
  on private.bpay_next_run_worker for each row execute function private.bpay_next_worker_execution_guard_v1();

alter function private.bpay_next_prepare_job_run_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_prepare_job_run_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_job_active_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_prepare_job_active_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean) owner to postgres;
revoke all on function private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) owner to postgres;
revoke all on function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_worker_action_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_worker_action_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_action_guard_v1() owner to postgres;
revoke all on function private.bpay_next_worker_action_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_release_scope_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_prepare_release_scope_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_release_prepare_work_hold_v1(uuid,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_release_prepare_work_hold_v1(uuid,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_reconcile_worker_released_collection_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_worker_released_collection_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_exclude_preparation_week_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_exclude_preparation_week_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_prepare_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_prepare_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) owner to postgres;
revoke all on function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_case_transfer_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_begin_case_transfer_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_cancel_block_v1(uuid,boolean) owner to postgres;
revoke all on function private.bpay_next_case_cancel_block_v1(uuid,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_cancel_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_accept_case_cancel_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_cancelled_week_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_apply_cancelled_week_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_case_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_case_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_instruction_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_case_instruction_insert_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_expired_week_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_apply_expired_week_v1(uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_review_simple_run_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_review_simple_run_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_confirm_simple_run_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_confirm_simple_run_v1(uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_segment_prepare_owner_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_segment_prepare_owner_v1(uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_run_work_query_pin_guard_v2() owner to postgres;
revoke all on function private.bpay_next_run_work_query_pin_guard_v2() from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_capture_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_capture_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_complete_retained_segment_parent_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_complete_retained_segment_parent_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_segment_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_capture_segment_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_release_complete_v1(uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_worker_release_complete_v1(uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_expiry_guard_v1() owner to postgres;
revoke all on function private.bpay_next_preparation_expiry_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_expired_prepare_receipt_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_expired_prepare_receipt_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_execution_guard_v1() owner to postgres;
revoke all on function private.bpay_next_worker_execution_guard_v1() from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
