-- WB19: exact administrative resume of the existing POSITION_APPLY job.
-- No new financial command/sequence, current-input expansion or owner drain.

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
    or v_command.expected_member_count<>1 or v_command.enrolled_member_count<>1
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

create or replace function public.bpay_next_workbench_command_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_actor uuid;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>4096
    or (select count(*) from jsonb_object_keys(p_request))<>4
    or exists(select 1 from jsonb_object_keys(p_request) k where k not in ('actor_user_id','action','args','readback'))
    or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or (p_request->>'actor_user_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or p_request->>'action' is distinct from 'RETRY_WORK_JOB'
    or jsonb_typeof(p_request->'readback') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if (p_request->>'readback')::boolean then
    return private.bpay_next_retry_work_job_v1(v_actor,p_request->'args',true);
  end if;
  return public.bpay_next_financial_intake_v1(v_actor,'RETRY_WORK_JOB',p_request->'args')->'result';
end
$function$;

create or replace function public.bpay_next_work_job_failure_service_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_phase text,p_expected_cursor text,p_error_code text)
returns boolean language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_candidate uuid;v_epoch bigint;v_job private.bpay_next_job%rowtype;v_now timestamptz;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';end if;
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_expected_phase is null
    or p_error_code is null or p_error_code!~'^BPAY_NEXT_[A-Z0-9_]{1,240}$'
    or p_error_code in ('BPAY_NEXT_POSITION_LEASE_STALE','BPAY_NEXT_POSITION_PREDECESSOR_MISMATCH',
      'BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_POSITION_STEP_INPUT_INVALID','BPAY_NEXT_JOB_KIND_MISMATCH',
      'BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING')
    or octet_length(p_expected_phase)>64 or octet_length(p_expected_cursor)>512 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into v_candidate from private.bpay_next_job j where j.id=p_job_id;
  if not found then return false;end if;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select j.* into v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'POSITION_APPLY' or v_job.owner_epoch<>p_owner_epoch
    or v_job.phase is distinct from p_expected_phase or v_job.cursor_key is distinct from p_expected_cursor then return false;end if;
  if v_job.status='FAILED' and v_job.failed_lease_nonce=p_lease_nonce then
    return v_job.last_error_code is not distinct from p_error_code;end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from p_lease_nonce then return false;end if;
  v_now:=clock_timestamp();
  update private.bpay_next_job set status='FAILED',failed_lease_nonce=p_lease_nonce,lease_nonce=null,lease_until_utc=null,
    first_recorded_error_code=coalesce(first_recorded_error_code,p_error_code),
    first_recorded_error_at_utc=coalesce(first_recorded_error_at_utc,v_now),last_error_code=p_error_code,last_error_at_utc=v_now
    where id=p_job_id;
  return true;
end
$function$;

alter function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_command_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_command_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_command_v1(jsonb) to service_role;
alter function public.bpay_next_work_job_failure_service_v1(uuid,uuid,bigint,text,text,text) owner to postgres;
revoke all on function public.bpay_next_work_job_failure_service_v1(uuid,uuid,bigint,text,text,text) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_work_job_failure_service_v1(uuid,uuid,bigint,text,text,text) to service_role;
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
  v_projection_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
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
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id')
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
        v_result:=private.bpay_next_accept_case_paye_net_v1(
          v_command_id,v_worker_id,p_actor_user_id,v_amount,v_projection_revision);
      else
        if v_projection_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
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
alter function public.bpay_next_financial_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) to service_role;
-- Existing W1 HEADER metadata seam; no intermediate financial-page adoption.
create or replace function private.bpay_next_workbench_job_progress_v1(p_context jsonb,p_after jsonb,p_limit integer)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog,private,public
as $function$
declare
 v_context jsonb:=jsonb_build_object('kind','WORK_JOB_PROGRESS','agency_id',p_context->'agency_id',
  'actor_user_id',p_context->'actor_user_id','owner_epoch',p_context->'owner_epoch','official_pay_date',p_context->'official_pay_date',
  'workbench_revision',p_context->'workbench_revision','adoption_revision',p_context->'adoption_revision',
  'scope',jsonb_build_object('candidate_id',p_context#>'{scope,candidate_id}',
   'client_id',p_context#>'{scope,client_id}','channel',p_context#>'{scope,channel}'));
 v_key jsonb;v_rows jsonb:='[]';v_row record;v_last jsonb;v_complete boolean:=true;
 v_candidate uuid:=(p_context#>>'{scope,candidate_id}')::uuid;
 v_client uuid:=(p_context#>>'{scope,client_id}')::uuid;
 v_channel text:=p_context#>>'{scope,channel}';v_epoch bigint:=(p_context->>'owner_epoch')::bigint;
 v_oldest timestamptz;v_unknown boolean;v_age text;
begin
 if p_after is not null and p_after<>'null'::jsonb then
  if jsonb_typeof(p_after)<>'object' or not(p_after ?& array['context','key'])
   or p_after-array['context','key']<>'{}'::jsonb or p_after->'context' is distinct from v_context then
   raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  v_key:=p_after->'key';
  if jsonb_typeof(v_key)<>'array' or jsonb_array_length(v_key)<>2
   or jsonb_typeof(v_key->0)<>'string' or v_key->>0!~'^[1-9][0-9]{0,18}$'
   or jsonb_typeof(v_key->1)<>'string' or v_key->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_key->>0)::numeric>9223372036854775807 then
   raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 end if;
 -- Nullable historical creation evidence remains unknown. Indexed native
 -- pending probe, never use a rotated wake clock as original creation.
 select j.created_at_utc into v_oldest from private.bpay_next_job j
  join private.bpay_next_publication p on p.command_id=j.command_id and p.candidate_id=j.candidate_id
  join private.bpay_next_work w on w.id=p.work_id and w.candidate_id=j.candidate_id
  join public.contracts c on c.id=w.contract_id join public.candidates ca on ca.id=j.candidate_id
  where j.module_epoch=v_epoch and j.job_kind='POSITION_APPLY' and j.status in ('READY','LEASED','BLOCKED')
   and (v_candidate is null or j.candidate_id=v_candidate) and (v_client is null or c.client_id=v_client)
   and (v_channel='ALL' or ca.pay_method::text=v_channel)
  order by j.created_at_utc nulls first,j.id limit 1;
 v_unknown:=found and v_oldest is null;
 v_age:=case when v_unknown or v_oldest is null then null
  else greatest(0,floor(extract(epoch from (statement_timestamp()-v_oldest))))::bigint::text end;
 for v_row in
  with seed as materialized (
   select j.id,j.command_sequence from private.bpay_next_job j
    join private.bpay_next_publication p on p.command_id=j.command_id and p.candidate_id=j.candidate_id
    join private.bpay_next_work w on w.id=p.work_id and w.candidate_id=j.candidate_id
    join public.contracts c on c.id=w.contract_id join public.candidates ca on ca.id=j.candidate_id
    where j.module_epoch=v_epoch and j.job_kind='POSITION_APPLY' and j.status<>'DONE'
     and (v_candidate is null or j.candidate_id=v_candidate) and (v_client is null or c.client_id=v_client)
     and (v_channel='ALL' or ca.pay_method::text=v_channel)
     and (v_key is null or (j.command_sequence,j.id)>((v_key->>0)::bigint,(v_key->>1)::uuid))
    order by j.command_sequence,j.id limit p_limit+1
  )
  select j.*,p.work_id,p.revision_id,r.candidate_display_name,r.timesheet_reference,
   r.physical_timesheet_id,r.client_display_name,
   (r.id is not null and c.id is not null and cm.command_id is not null) as evidence_available
  from seed s join private.bpay_next_job j on j.id=s.id
   join private.bpay_next_publication p on p.command_id=j.command_id and p.candidate_id=j.candidate_id
   left join private.bpay_next_work_revision r on r.id=p.revision_id and r.work_id=p.work_id
   left join private.bpay_next_command c on c.id=j.command_id and c.agency_sequence=j.command_sequence
    and c.module_epoch=j.module_epoch and c.command_kind='POSITION_APPLY'
   left join private.bpay_next_command_member cm on cm.command_id=j.command_id and cm.candidate_id=j.candidate_id
  order by s.command_sequence,s.id
 loop
  if jsonb_array_length(v_rows)=p_limit then v_complete:=false;exit;end if;
  v_rows:=v_rows||jsonb_build_array(jsonb_build_object('job_id',v_row.id,'work_id',v_row.work_id,
   'candidate_id',v_row.candidate_id,'revision_id',v_row.revision_id,'command_id',v_row.command_id,
   'command_sequence',v_row.command_sequence::text,'module_epoch',v_row.module_epoch::text,
   'owner_epoch',v_row.owner_epoch::text,'status',v_row.status,'phase',v_row.phase,'cursor',v_row.cursor_key,
   'attempt_count',v_row.attempt_count::text,'processing',v_row.status<>'FAILED',
   'failed_job_code',case when v_row.status='FAILED' then v_row.last_error_code else null end,
   'first_error_code',v_row.first_recorded_error_code,'last_error_code',v_row.last_error_code,
   'first_error_at_utc',v_row.first_recorded_error_at_utc,'last_error_at_utc',v_row.last_error_at_utc,
   'created_at_utc',v_row.created_at_utc,'started_at_utc',v_row.started_at_utc,'finished_at_utc',v_row.finished_at_utc,
   'candidate_display_name',v_row.candidate_display_name,'timesheet_reference',v_row.timesheet_reference,
   'client_display_name',v_row.client_display_name,'physical_timesheet_id',v_row.physical_timesheet_id,
   'evidence_available',v_row.evidence_available));
  v_last:=jsonb_build_array(v_row.command_sequence::text,v_row.id);
  if octet_length(v_rows::text)>100000 then
   raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 end loop;
 return jsonb_build_object('context',v_context,'rows',v_rows,'complete',v_complete,
  'next_cursor',case when v_complete then null else jsonb_build_object('context',v_context,'key',v_last) end,
  'oldest_pending_age_seconds',v_age);
end
$function$;
alter function private.bpay_next_workbench_job_progress_v1(jsonb,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_job_progress_v1(jsonb,jsonb,integer) from public,anon,authenticated,service_role;

create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;v_progress jsonb;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb),
    'progress_after',coalesce(p_request->'progress_after','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','limit','count_selection','progress_after'];
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or not(p_request ?& v_keys) or p_request-v_keys<>'{}'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if jsonb_typeof(p_request->'actor_user_id')<>'string'
    or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
  select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
  v_kind:=p_request->>'kind';v_sort:=p_request->>'sort';
  if jsonb_typeof(p_request->'kind')<>'string'
    or v_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL',
      'CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit not between 1 and 100 or (v_kind='CANDIDATE_GROUPS' and v_limit>10) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  foreach v_text in array array['candidate_id','client_id','item_id'] loop
    if p_request->v_text<>'null'::jsonb and (jsonb_typeof(p_request->v_text)<>'string'
      or p_request->>v_text!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end loop;
  if p_request->'group_key'<>'null'::jsonb and (jsonb_typeof(p_request->'group_key')<>'string'
    or p_request->>'group_key'!~'^(WORK|CASE|ROW):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->>'group_key' like 'ROW:%' and v_kind not in ('GROUP_LINES','CANDIDATE_GROUPS') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection<>'null'::jsonb then
    if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
      or not(v_count_selection ?& array['mode','candidate_ids'])
      or v_count_selection-array['mode','candidate_ids']<>'{}'::jsonb
      or jsonb_typeof(v_count_selection->'mode')<>'string' or v_count_selection->>'mode' not in ('ALL','NONE')
      or jsonb_typeof(v_count_selection->'candidate_ids')<>'array' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if exists(select 1 from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)
      where jsonb_typeof(a.value)<>'string' or (a.value#>>'{}')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (select count(*)<>count(distinct a.value) from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  if p_request->'progress_after'<>'null'::jsonb and v_kind<>'HEADER' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_request-array['actor_user_id','kind','after','limit','progress_after'];
  -- Fixed effective sections, explicit normalization; client cannot silently
  -- continue a cursor issued for a different list.
  if v_kind='CANDIDATES' then v_scope:=jsonb_set(v_scope,'{section}','"READY"'::jsonb);
  elsif v_kind='ACTION_REQUIRED' then v_scope:=jsonb_set(v_scope,'{section}','"ACTION"'::jsonb);
  elsif v_kind='BLOCKED' then v_scope:=jsonb_set(v_scope,'{section}','"BLOCKED"'::jsonb);end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_context:=jsonb_build_object('kind',v_kind,'agency_id',v_agency,'actor_user_id',v_actor,
    'owner_epoch',v_epoch::text,'official_pay_date',v_date_context->>'current_official_pay_date',
    'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,
    'scope',v_scope);
  if v_kind='HEADER' then
    v_progress:=private.bpay_next_workbench_job_progress_v1(v_context,p_request->'progress_after',v_limit);
  end if;
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
    v_response:=jsonb_build_object('ok',false,'error_code','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT',
      'retryable',true,'reload_required',false,'workbench_revision',v_revision.workbench_revision::text,
      'complete',false,'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
      'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
    if v_kind='HEADER' then v_response:=v_response||jsonb_build_object('job_progress',v_progress);end if;
    if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    return v_response;
  end if;
  v_cursor:=p_request->'after';
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or v_cursor-array['context','key']<>'{}'::jsonb or v_cursor->'context' is distinct from v_context then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    if v_kind='EXPORT_ROWS' then
      if jsonb_typeof(v_after)<>'array' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'string' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'boolean' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    else
    v_key_count:=case when v_kind in ('CANDIDATES','PICK_CANDIDATES') then case when v_sort like 'DEDUCTIONS_%' or v_sort like 'STATUS_%' then 5 when v_sort like 'AMOUNT_%' then 3 else 4 end
      when v_kind='CANDIDATE_GROUPS' then case when v_sort like 'DEDUCTIONS_%' then 3 else 4 end when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
    if jsonb_typeof(v_after)<>'array' or jsonb_array_length(v_after)<>v_key_count then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_kind in ('CANDIDATES','PICK_CANDIDATES') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string'
        or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'DEDUCTIONS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or jsonb_typeof(v_after->2)<>'boolean' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'STATUS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string')) then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind='CANDIDATE_GROUPS' then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(v_after->(v_key_count-2))<>'string' or v_after->>(v_key_count-2) not in ('WORK','CASE','ROW')
        or (v_sort like 'DEDUCTIONS_%' and jsonb_typeof(v_after->0)<>'boolean')
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort not like 'DEDUCTIONS_%' and v_sort not like 'REASON_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'))
        or ((v_sort='NAME_ASC' or v_sort like 'PAYMENT_%') and ((v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')))
        or (v_sort like 'AMOUNT_%' and v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^(0|[1-9][0-9]*)$'
          or (v_after->>0)::numeric>32767 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort like 'PAYMENT_%' and jsonb_typeof(v_after->0)<>'string') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif jsonb_typeof(v_after->0)<>'string' or (v_kind='CANDIDATE_GROUPS'
      and v_after->>0!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (v_kind<>'CANDIDATE_GROUPS' and v_after->>0!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    end if;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_scope,v_after,v_limit);
  end case;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if v_kind='HEADER' then
    v_response:=v_response||jsonb_build_object('job_progress',v_progress);
    v_response:=jsonb_set(v_response,'{rows,0}',(v_response#>'{rows,0}')||
      jsonb_build_object('oldest_pending_age_seconds',v_progress->'oldest_pending_age_seconds'));
  end if;
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  v_error:=case
    when sqlstate='42501' then 'BPAY_NEXT_WORKBENCH_FORBIDDEN'
    when sqlstate in ('22023','22P02','22003','22007','22008') then 'BPAY_NEXT_WORKBENCH_REQUEST_INVALID'
    when sqlstate='54000' then 'BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE'
    when sqlerrm in ('BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_CURSOR_STALE',
      'BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE') then sqlerrm
    else 'BPAY_NEXT_WORKBENCH_UNAVAILABLE' end;
  return jsonb_build_object('ok',false,'error_code',v_error,'retryable',v_error in
    ('BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'),
    'reload_required',v_error='BPAY_NEXT_WORKBENCH_CURSOR_STALE',
    'workbench_revision',v_revision.workbench_revision::text,'complete',false,
    'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
    'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
end
$function$;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';

commit;
