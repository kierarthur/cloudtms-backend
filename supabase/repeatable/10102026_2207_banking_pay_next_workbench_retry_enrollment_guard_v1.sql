-- WB19 nullable expected-member guard correction; installed2150 is immutable.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_retry_work_job_v1(p_actor_user_id uuid,p_args jsonb,p_readback boolean)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[]:=array['request_id','candidate_id','work_id','job_id','module_epoch','expected_owner_epoch','expected_phase','expected_cursor'];
  v_request uuid;v_candidate uuid;v_work_id uuid;v_job_id uuid;v_epoch bigint;
  v_saved private.bpay_next_workbench_command_receipt%rowtype;
  v_job private.bpay_next_job%rowtype;v_publication private.bpay_next_publication%rowtype;
  v_work private.bpay_next_work%rowtype;v_command private.bpay_next_command%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_projection private.bpay_next_workbench_revision%rowtype;
  v_repaired boolean:=false;v_result jsonb;v_now timestamptz;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_actor_user_id is null or p_readback is null or p_args is null or jsonb_typeof(p_args)<>'object'
    or pg_catalog.octet_length(p_args::text)>2048
    or (select count(*) from jsonb_object_keys(p_args))<>8
    or exists(select 1 from jsonb_object_keys(p_args) k where not(k=any(v_keys)))
    or exists(select 1 from unnest(array['request_id','candidate_id','work_id','job_id']) k
      where jsonb_typeof(p_args->k) is distinct from 'string'
        or (p_args->>k)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
    or exists(select 1 from unnest(array['module_epoch','expected_owner_epoch']) k
      where jsonb_typeof(p_args->k) is distinct from 'string'
        or case when (p_args->>k)~'^[1-9][0-9]{0,18}$'
          then (p_args->>k)::numeric>9223372036854775807 else true end)
    or jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
    or pg_catalog.octet_length(p_args->>'expected_phase') not between 1 and 64
    or jsonb_typeof(p_args->'expected_cursor') not in ('string','null')
    or pg_catalog.octet_length(p_args->>'expected_cursor')>512 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_request:=(p_args->>'request_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  v_work_id:=(p_args->>'work_id')::uuid;v_job_id:=(p_args->>'job_id')::uuid;
  select r.* into v_saved from private.bpay_next_workbench_command_receipt r where r.request_id=v_request;
  if found then
    if v_saved.actor_user_id<>p_actor_user_id or v_saved.request_body is distinct from p_args then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT';end if;
    return v_saved.result||jsonb_build_object('replay',true);
  end if;
  if p_readback then
    return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'work_id',v_work_id,
      'job_id',v_job_id,'received',false,'replay',false);
  end if;
  select m.owner_epoch into strict v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch<>(p_args->>'module_epoch')::bigint then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_COMMAND_EPOCH_STALE';end if;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  -- Recheck after the Candidate lock. Concurrent identical requests replay
  -- even when the first request has already made this job claimable.
  select r.* into v_saved from private.bpay_next_workbench_command_receipt r where r.request_id=v_request;
  if found then
    if v_saved.actor_user_id<>p_actor_user_id or v_saved.request_body is distinct from p_args then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT';end if;
    return v_saved.result||jsonb_build_object('replay',true);
  end if;
  select j.* into v_job from private.bpay_next_job j where j.id=v_job_id for update;
  if not found or v_job.candidate_id<>v_candidate or v_job.module_epoch<>v_epoch or v_job.job_kind<>'POSITION_APPLY'
    or v_job.owner_epoch<>(p_args->>'expected_owner_epoch')::bigint
    or v_job.phase is distinct from p_args->>'expected_phase'
    or v_job.cursor_key is distinct from p_args->>'expected_cursor' then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_STATE_CHANGED';end if;
  select c.* into v_command from private.bpay_next_command c where c.id=v_job.command_id;
  if not found or v_command.module_epoch<>v_epoch or v_command.agency_sequence<>v_job.command_sequence
    or v_command.command_kind<>'POSITION_APPLY'
    or v_command.expected_member_count is distinct from 1 or v_command.enrolled_member_count<>1
    or not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=v_job.command_id and cm.candidate_id=v_candidate and cm.member_no=1) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  select p.* into v_publication from private.bpay_next_publication p where p.command_id=v_job.command_id for update;
  if not found or v_publication.work_id<>v_work_id or v_publication.candidate_id<>v_candidate then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  select w.* into v_work from private.bpay_next_work w where w.id=v_work_id for update;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=v_publication.revision_id and r.work_id=v_work_id;
  if v_work.candidate_id is distinct from v_candidate or v_revision.id is null
    or v_revision.revision_no<>v_publication.revision_no or v_job.applied_line_count>v_revision.expected_line_count
    or (v_publication.phase in ('REMOVED','DONE') and v_job.applied_line_count<>v_revision.expected_line_count)
    or (v_publication.phase='NEW' and v_publication.cursor_key is null and v_job.applied_line_count<>0)
    or (v_publication.phase='NEW' and v_publication.cursor_key is not null and
      (v_job.applied_line_count=0 or not exists(select 1 from private.bpay_next_approved_line l
        join private.bpay_next_position p on p.work_id=v_work_id and p.component_key=l.component_key
        and p.applied_revision_id=v_revision.id
        where l.revision_id=v_revision.id and l.component_key=v_publication.cursor_key))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  if v_job.status='DONE' and v_publication.status='APPLIED' and v_publication.phase='DONE' then
    if v_job.phase<>'DONE' or v_job.cursor_key is not null or v_command.status<>'COMPLETE' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  elsif v_job.status<>'FAILED' or v_job.last_error_code is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_NOT_DEFINITELY_FAILED';
  elsif v_publication.status='APPLIED' and v_publication.phase='DONE' then
    if v_command.status<>'COMPLETE' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
    -- Proven canonical publication completion repairs only job metadata.
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,
      lease_nonce=null,lease_until_utc=null where id=v_job_id returning * into v_job;
    v_repaired:=true;
  else
    if v_publication.phase not in ('NEW','REMOVED') or v_publication.status not in ('QUEUED','APPLYING')
      or v_work.applied_revision_id is distinct from v_publication.predecessor_revision_id
      or pg_catalog.octet_length(v_publication.cursor_key)>512 then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
    -- The native publication is the atomic twin checkpoint, not a guessed
    -- default or latest Work revision. Counts and original inputs are retained.
    v_repaired:=v_job.phase is distinct from v_publication.phase
      or v_job.cursor_key is distinct from v_publication.cursor_key;
    v_now:=pg_catalog.clock_timestamp();
    update private.bpay_next_job set status='READY',phase=v_publication.phase,cursor_key=v_publication.cursor_key,
      lease_nonce=null,lease_until_utc=null,available_at_utc=v_now,queue_wake_after_utc=v_now
      where id=v_job_id returning * into v_job;
  end if;
  select r.* into strict v_projection from private.bpay_next_workbench_revision r where r.id=1;
  v_result:=jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'work_id',v_work_id,
    'job_id',v_job_id,'command_id',v_job.command_id,'command_sequence',v_job.command_sequence::text,
    'module_epoch',v_epoch::text,'owner_epoch',v_job.owner_epoch::text,'status',v_job.status,
    'phase',v_job.phase,'cursor',v_job.cursor_key,'first_recorded_error_code',v_job.first_recorded_error_code,
    'last_error_code',v_job.last_error_code,'workbench_revision',v_projection.workbench_revision::text,
    'adoption_revision',v_projection.adoption_revision::text,'repaired',v_repaired,'received',true,'replay',false,
    'captured_at_utc',to_char(clock_timestamp() at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
  insert into private.bpay_next_workbench_command_receipt(request_id,actor_user_id,candidate_id,work_id,job_id,
    module_epoch,request_body,result) values(v_request,p_actor_user_id,v_candidate,v_work_id,v_job_id,v_epoch,p_args,v_result);
  return v_result;
end
$function$;
alter function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
