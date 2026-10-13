-- PF08 / BD124: bounded original-job preparation recovery.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_retry_execution_v1(p_job_id uuid,p_worker_id uuid,p_original_revision bigint)
returns bigint language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select case when j.retry_execution_revision is not null and exists(
  select 1 from private.bpay_next_workbench_command_receipt r
  where r.request_id=j.retry_request_id and r.job_id=j.id and r.candidate_id=j.candidate_id
   and r.module_epoch=j.module_epoch and r.retry_run_worker_id=w.id
   and r.retry_input_job_id=w.input_prepare_job_id and r.retry_captured_revision=w.captured_preparation_revision
   and r.retry_target_revision=j.retry_execution_revision and r.result->>'retry_kind'='PREPARE'
   and r.result->>'preparation_revision'=r.retry_target_revision::text
   and r.result->>'run_id'=w.run_id::text
   and ((p_original_revision=1 and j.id=w.input_prepare_job_id)
    or exists(select 1 from private.bpay_next_worker_reprepare_request x
     where x.job_id=j.id and x.run_worker_id=w.id and x.run_id=w.run_id and x.candidate_id=w.candidate_id
      and x.command_id=j.command_id and x.module_epoch=j.module_epoch and x.action='REPREPARE'
      and x.target_preparation_revision=p_original_revision and x.input_prepare_job_id=w.input_prepare_job_id
      and x.captured_preparation_revision=w.captured_preparation_revision)))
 then j.retry_execution_revision else p_original_revision end
 from private.bpay_next_job j join private.bpay_next_run_worker w on w.id=p_worker_id
  and w.run_id=private.bpay_next_prepare_job_run_v1(j.id) and w.candidate_id=j.candidate_id
 where j.id=p_job_id and j.job_kind='PREPARE'
$function$;

create or replace function private.bpay_next_prepare_attention_v1(p_job_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select exists(select 1 from private.bpay_next_job j join private.bpay_next_run_worker w
  on w.run_id=j.retry_run_id and w.candidate_id=j.candidate_id and w.active_prepare_job_id=j.id
  where j.id=p_job_id and j.job_kind='PREPARE' and j.needs_attention and j.retry_count=5
   and j.status='READY' and j.phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE')
   and w.status='REVIEW' and w.review_issue_code='PF08_NEEDS_ATTENTION'
   and private.bpay_next_prepare_job_active_v1(j.id))
$function$;

create or replace function private.bpay_next_prepare_retry_handoff_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare j private.bpay_next_job%rowtype;w private.bpay_next_run_worker%rowtype;
begin
 -- Caller owns module, Run, Candidate and job in that order. No new locks.
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id;
 if j.retry_count<>5 or j.needs_attention or j.status='DONE'
  or(j.status='LEASED' and j.lease_until_utc>clock_timestamp()) then return;end if;
 select pf08_bpay_next_run_worker.* into strict w from private.bpay_next_run_worker pf08_bpay_next_run_worker where run_id=j.retry_run_id and candidate_id=j.candidate_id;
 if j.phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE') then
  if w.status<>'PREPARING' then raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';end if;
  update private.bpay_next_job set retry_prior_issue_code=w.review_issue_code,
   retry_prior_issue_work_id=w.review_issue_work_id where id=j.id;
  update private.bpay_next_run_worker set status='REVIEW',review_issue_code='PF08_NEEDS_ATTENTION',review_issue_work_id=null where id=w.id;
  update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count-1,
   review_candidate_count=review_candidate_count+1,review_revision=review_revision+1 where id=w.run_id;
 elsif j.phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') or w.status not in ('READY','REVIEW') then
  raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
 else
  update private.bpay_next_pay_run set review_revision=review_revision+1 where id=w.run_id;
 end if;
 update private.bpay_next_job set needs_attention=true,status='READY',lease_nonce=null,lease_until_utc=null,next_retry_at_utc=null where id=j.id;
 perform private.bpay_next_review_simple_run_v1(w.run_id);
end
$function$;

create or replace function private.bpay_next_prepare_retry_admit_v1(p_job_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare j private.bpay_next_job%rowtype;r private.bpay_next_pay_run%rowtype;v_run uuid;v_epoch bigint;v_now timestamptz;
begin
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id;
 if j.job_kind<>'PREPARE' or j.status='DONE' or j.retry_count=0 then return null;end if;
 -- Admission for actual PIN never takes a business lock ahead of its query key.
 v_now:=clock_timestamp();
 if p_action='STEP' and j.status='LEASED' and j.lease_until_utc>v_now
  and j.owner_epoch::text=p_args->>'owner_epoch' and j.lease_nonce::text=p_args->>'lease_nonce' then return null;end if;
 if not(p_action='CLAIM' and j.retry_count=5 and not j.needs_attention and j.next_retry_at_utc<=v_now
   and(j.status='READY' or j.status='LEASED' and j.lease_until_utc<=v_now)) then
  if j.needs_attention or j.next_retry_at_utc>v_now or j.retry_count=5 then
   return jsonb_build_object('job_id',j.id,'job_kind','PREPARE','deferred',true,
    'needs_attention',j.needs_attention,'next_retry_at_utc',j.next_retry_at_utc);end if;
  return null;
 end if;
 v_run:=private.bpay_next_prepare_job_run_v1(j.id);
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 select pf08_bpay_next_pay_run.* into strict r from private.bpay_next_pay_run pf08_bpay_next_pay_run where id=v_run for update;
 perform 1 from private.bpay_next_worker_control where candidate_id=j.candidate_id for update;
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id for update;
 v_now:=clock_timestamp();
 if j.module_epoch is distinct from v_epoch then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if j.status='DONE' or exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_preparation_discard_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_worker_reprepare_request where job_id=j.id and action='RELEASE') then return null;end if;
 -- Preserve a direct original STEP replay while its uncertain invocation is live.
 if p_action='STEP' and j.status='LEASED' and j.lease_until_utc>v_now
  and j.owner_epoch::text=p_args->>'owner_epoch' and j.lease_nonce::text=p_args->>'lease_nonce' then return null;end if;
 if p_action='CLAIM' and j.retry_count=5 and not j.needs_attention and j.next_retry_at_utc<=v_now
  and (j.status='READY' or j.status='LEASED' and j.lease_until_utc<=v_now) then
  perform private.bpay_next_prepare_retry_handoff_v1(j.id);
  select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id;
 end if;
 if j.needs_attention or j.next_retry_at_utc>v_now or j.retry_count=5 then
  return jsonb_build_object('job_id',j.id,'job_kind','PREPARE','deferred',true,
   'needs_attention',j.needs_attention,'next_retry_at_utc',j.next_retry_at_utc);end if;
 return null;
end
$function$;

create or replace function private.bpay_next_prepare_failure_v1(p_job_id uuid,p_args jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare j private.bpay_next_job%rowtype;r private.bpay_next_pay_run%rowtype;v_run uuid;v_epoch bigint;v_key text;
 v_now timestamptz;v_count integer;v_due timestamptz;v_unknown boolean;v_match boolean;
begin
 foreach v_key in array array['lease_nonce','owner_epoch','expected_phase','error_code'] loop
  if jsonb_typeof(p_args->v_key) is distinct from 'string' then raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
 end loop;
 foreach v_key in array array['expected_cursor','expected_work_cursor','expected_component_cursor'] loop
  if jsonb_typeof(p_args->v_key) is null or jsonb_typeof(p_args->v_key) not in ('null','string')
   or octet_length(p_args->>v_key)>1024 then raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
 end loop;
 if p_args is null or jsonb_typeof(p_args)<>'object' or octet_length(p_args::text)>2048
  or(select count(*) from jsonb_object_keys(p_args))<>9
  or exists(select 1 from jsonb_object_keys(p_args) k where k not in
   ('lease_nonce','owner_epoch','expected_phase','expected_cursor','expected_work_cursor','expected_component_cursor','error_code','outcome_unknown','rollback_confirmed'))
  or(p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or(p_args->>'owner_epoch')!~'^[1-9][0-9]{0,18}$'
  or(p_args->>'owner_epoch')::numeric>9223372036854775807
  or(p_args->>'error_code')!~'^BPAY_NEXT_[A-Z0-9_]{1,240}$'
  or jsonb_typeof(p_args->'outcome_unknown') is distinct from 'boolean'
  or jsonb_typeof(p_args->'rollback_confirmed') is distinct from 'boolean'
  or(p_args->>'outcome_unknown')::boolean=(p_args->>'rollback_confirmed')::boolean
  or(p_args->>'expected_phase') not in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE') then
  raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id;
 v_run:=private.bpay_next_prepare_job_run_v1(j.id);
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 select pf08_bpay_next_pay_run.* into strict r from private.bpay_next_pay_run pf08_bpay_next_pay_run where id=v_run for update;
 perform 1 from private.bpay_next_worker_control where candidate_id=j.candidate_id for update;
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id for update;
 if j.job_kind<>'PREPARE' or j.module_epoch is distinct from v_epoch then raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';end if;
 if j.status='DONE' or j.owner_epoch::text is distinct from p_args->>'owner_epoch'
  or j.phase is distinct from p_args->>'expected_phase'
  or j.cursor_key is distinct from p_args->>'expected_cursor'
  or j.position_work_cursor::text is distinct from p_args->>'expected_work_cursor'
  or j.position_component_cursor is distinct from p_args->>'expected_component_cursor' then
  return jsonb_build_object('recorded',true,'job_id',j.id,'needs_attention',false,'next_retry_at_utc',null,'replay',true);end if;
 if j.last_retry_owner_epoch=j.owner_epoch then
  return jsonb_build_object('recorded',true,'job_id',j.id,'needs_attention',j.needs_attention,'next_retry_at_utc',j.next_retry_at_utc,'replay',true);end if;
 if j.status<>'LEASED' or j.lease_nonce::text is distinct from p_args->>'lease_nonce'
  or r.status<>'PREPARING' or r.confirmed_at_utc is not null or r.preparation_expires_at_utc<=clock_timestamp()
  or exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_preparation_discard_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_worker_reprepare_request where job_id=j.id and action='RELEASE')
  or not private.bpay_next_prepare_job_active_v1(j.id) then
  return jsonb_build_object('recorded',false,'job_id',j.id,'needs_attention',false,'next_retry_at_utc',null,'replay',false);end if;
 v_now:=clock_timestamp();v_unknown:=(p_args->>'outcome_unknown')::boolean;v_count:=least(j.retry_count+1,5);
 v_due:=v_now+make_interval(secs=>case v_count when 1 then 30 when 2 then 60 when 3 then 120 else 240 end);
 if v_unknown then v_due:=greatest(v_due,j.lease_until_utc);end if;
 update private.bpay_next_job set retry_count=v_count,next_retry_at_utc=v_due,retry_run_id=v_run,
  last_retry_owner_epoch=owner_epoch,first_recorded_error_code=coalesce(first_recorded_error_code,p_args->>'error_code'),
  first_recorded_error_at_utc=coalesce(first_recorded_error_at_utc,v_now),last_error_code=p_args->>'error_code',last_error_at_utc=v_now,
  status=case when v_unknown then status else 'READY' end,lease_nonce=case when v_unknown then lease_nonce else null end,
  lease_until_utc=case when v_unknown then lease_until_utc else null end,
  queue_wake_after_utc=v_due where id=j.id;
 if v_count=5 and not v_unknown then perform private.bpay_next_prepare_retry_handoff_v1(j.id);end if;
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=p_job_id;
 return jsonb_build_object('recorded',true,'job_id',j.id,'needs_attention',j.needs_attention,'next_retry_at_utc',j.next_retry_at_utc,'replay',false);
end
$function$;

create or replace function private.bpay_next_retry_prepare_job_v1(p_actor uuid,p_args jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare j private.bpay_next_job%rowtype;w private.bpay_next_run_worker%rowtype;r private.bpay_next_pay_run%rowtype;
 s private.bpay_next_workbench_command_receipt%rowtype;v_run uuid;v_epoch bigint;v_result jsonb;v_economic boolean;v_target bigint;
 v_projection private.bpay_next_workbench_revision%rowtype;v_now timestamptz;
begin
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=(p_args->>'job_id')::uuid;
 v_run:=private.bpay_next_prepare_job_run_v1(j.id);
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 select pf08_bpay_next_pay_run.* into strict r from private.bpay_next_pay_run pf08_bpay_next_pay_run where id=v_run for update;
 perform 1 from private.bpay_next_worker_control where candidate_id=j.candidate_id for update;
 select pf08_bpay_next_workbench_command_receipt.* into s from private.bpay_next_workbench_command_receipt pf08_bpay_next_workbench_command_receipt where request_id=(p_args->>'request_id')::uuid;
 if found then
  if s.actor_user_id<>p_actor or s.request_body is distinct from p_args then
   raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT';end if;
  return s.result||jsonb_build_object('replay',true);end if;
 select pf08_bpay_next_job.* into strict j from private.bpay_next_job pf08_bpay_next_job where id=(p_args->>'job_id')::uuid for update;
 select pf08_bpay_next_run_worker.* into strict w from private.bpay_next_run_worker pf08_bpay_next_run_worker where run_id=v_run and candidate_id=j.candidate_id for update;
 v_now:=clock_timestamp();v_economic:=j.phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE');
 if j.job_kind<>'PREPARE' or j.candidate_id::text is distinct from p_args->>'candidate_id'
  or j.module_epoch is distinct from v_epoch or v_epoch::text is distinct from p_args->>'module_epoch'
  or j.owner_epoch::text is distinct from p_args->>'expected_owner_epoch'
  or j.phase is distinct from p_args->>'expected_phase' or j.cursor_key is distinct from p_args->>'expected_cursor'
  or not j.needs_attention or j.status<>'READY' or j.lease_nonce is not null
  or r.status not in ('PREPARING','REVIEW') or r.selection_state<>'SEALED' or r.confirmed_at_utc is not null
  or r.preparation_expires_at_utc<=v_now or w.active_prepare_job_id is distinct from j.id
  or not private.bpay_next_prepare_job_active_v1(j.id)
  or exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_preparation_discard_request where run_id=v_run)
  or exists(select 1 from private.bpay_next_worker_reprepare_request where job_id=j.id and action='RELEASE')
  or(v_economic and (w.status<>'REVIEW' or w.review_issue_code is distinct from 'PF08_NEEDS_ATTENTION'
    or w.preparation_revision=9223372036854775807 or w.realised_effect_count<>0 or w.net_request_revision<>0
    or w.net_projection_revision<>0 or exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)))
  or(not v_economic and (j.phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') or w.status not in ('READY','REVIEW'))) then
  raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_STATE_CHANGED';end if;
 v_target:=w.preparation_revision+case when v_economic then 1 else 0 end;
 select pf08_bpay_next_workbench_revision.* into strict v_projection from private.bpay_next_workbench_revision pf08_bpay_next_workbench_revision where id=1;
 v_result:=jsonb_build_object('request_id',p_args->>'request_id','candidate_id',j.candidate_id,'work_id',null,
  'job_id',j.id,'command_id',j.command_id,'command_sequence',j.command_sequence::text,'module_epoch',j.module_epoch::text,
  'owner_epoch',j.owner_epoch::text,'status','READY','phase',j.phase,'cursor',j.cursor_key,
  'first_recorded_error_code',j.first_recorded_error_code,'last_error_code',j.last_error_code,
  'workbench_revision',v_projection.workbench_revision::text,'adoption_revision',v_projection.adoption_revision::text,
  'repaired',false,'received',true,'replay',false,'captured_at_utc',v_now,
  'retry_kind','PREPARE','run_id',r.id,'run_worker_id',w.id,'preparation_revision',v_target::text,
  'captured_preparation_revision',w.captured_preparation_revision::text);
 insert into private.bpay_next_workbench_command_receipt(request_id,actor_user_id,candidate_id,work_id,job_id,module_epoch,request_body,result,
  retry_run_worker_id,retry_input_job_id,retry_captured_revision,retry_expected_revision,retry_target_revision)
 values((p_args->>'request_id')::uuid,p_actor,j.candidate_id,null,j.id,j.module_epoch,p_args,v_result,
  case when v_economic then w.id end,case when v_economic then w.input_prepare_job_id end,
  case when v_economic then w.captured_preparation_revision end,case when v_economic then w.preparation_revision end,
  case when v_economic then v_target end);
 -- Immutable receipt exists before the worker revision guard can accept it.
 update private.bpay_next_job set retry_count=0,next_retry_at_utc=null,needs_attention=false,last_retry_owner_epoch=null,
  available_at_utc=v_now,queue_wake_after_utc=v_now,
  retry_request_id=case when v_economic then(p_args->>'request_id')::uuid else retry_request_id end,
  retry_execution_revision=case when v_economic then v_target else retry_execution_revision end where id=j.id;
 update private.bpay_next_pay_run set status='PREPARING',review_revision=review_revision+1,
  preparing_candidate_count=preparing_candidate_count+case when v_economic then 1 else 0 end,
  review_candidate_count=review_candidate_count-case when v_economic then 1 else 0 end where id=r.id;
 if v_economic then
  update private.bpay_next_run_worker set preparation_revision=v_target,status='PREPARING',
   review_issue_code=j.retry_prior_issue_code,review_issue_work_id=j.retry_prior_issue_work_id where id=w.id;
 end if;
 return v_result;
end
$function$;

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
    or exists(select 1 from unnest(array['request_id','candidate_id','job_id']) k
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
  if p_args->'work_id'<>'null'::jsonb and (jsonb_typeof(p_args->'work_id') is distinct from 'string'
    or(p_args->>'work_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
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
  if v_work_id is null then return private.bpay_next_retry_prepare_job_v1(p_actor_user_id,p_args);end if;
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

create or replace function private.bpay_next_prepare_job_active_v1(p_job_id uuid)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_run_worker w on w.run_id=private.bpay_next_prepare_job_run_v1(j.id) and w.candidate_id=j.candidate_id
    left join private.bpay_next_worker_reprepare_request x on x.job_id=j.id and private.bpay_next_retry_execution_v1(j.id,w.id,x.target_preparation_revision)=w.preparation_revision
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where j.id=p_job_id and j.job_kind='PREPARE'
      and ((x.job_id is null and w.preparation_revision=private.bpay_next_retry_execution_v1(j.id,w.id,1) and
          (w.active_prepare_job_id is null or w.active_prepare_job_id=j.id))
        or (x.job_id=j.id and x.run_worker_id=w.id and private.bpay_next_retry_execution_v1(j.id,w.id,x.target_preparation_revision)=w.preparation_revision
          and x.captured_preparation_revision=w.captured_preparation_revision and w.active_prepare_job_id=j.id
          and x.status<>'COMPLETE')))
$function$;

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
          or (x.initiator_kind='OFFICE' and x.status='ACCEPTED' and new.active_prepare_job_id is null)))
      and not exists(select 1 from private.bpay_next_job j join private.bpay_next_workbench_command_receipt r on r.request_id=j.retry_request_id
       where j.id=new.active_prepare_job_id and j.id=old.active_prepare_job_id and j.candidate_id=new.candidate_id
        and r.retry_run_worker_id=new.id and r.retry_input_job_id=new.input_prepare_job_id
        and r.retry_captured_revision=new.captured_preparation_revision and r.retry_expected_revision=old.preparation_revision
        and r.retry_target_revision=new.preparation_revision and j.retry_execution_revision=new.preparation_revision
        and private.bpay_next_retry_execution_v1(j.id,old.id,coalesce((select x.target_preparation_revision
          from private.bpay_next_worker_reprepare_request x where x.job_id=j.id and x.action='REPREPARE'),1))=new.preparation_revision)
      then
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
  if p_action is null or p_action not in ('CLAIM','STEP','FAILURE') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='FAILURE' then
    if p_job_kind<>'PREPARE' then raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_failure_v1(p_job_id,p_args);
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
  if p_job_kind='PREPARE' then
    v_output:=private.bpay_next_prepare_retry_admit_v1(p_job_id,p_action,p_args);
    if v_output is not null then return v_output;end if;
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
       or (p_job_kind in ('CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF') and (v_job.phase is distinct from
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
        when 'CASE_EDIT' then
          select c.* into strict v_claim from private.bpay_next_claim_case_edit_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_PAUSE','CASE_RESUME' then
          select c.* into strict v_claim from private.bpay_next_claim_case_pause_job_v1(p_job_id,v_lease_seconds) c;
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
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_EDIT_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_PAUSE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
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
        exists(select 1 from private.bpay_next_case_edit_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_edit_request_present,
        exists(select 1 from private.bpay_next_case_pause_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.action=j.job_kind
            and r.module_epoch=j.module_epoch and r.command_sequence=j.command_sequence) as case_pause_request_present,
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
          when 'CASE_EDIT' then array['NEW']
          when 'CASE_PAUSE' then array['NEW']
          when 'CASE_RESUME' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='PREPARATION_DISCARD' and not v_claim_state.discard_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_EDIT' and (not v_claim_state.case_edit_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (not v_claim_state.case_pause_request_present
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
      when 'CASE_EDIT' then
        v_result:=private.bpay_next_apply_case_edit_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_PAUSE','CASE_RESUME' then
        v_result:=private.bpay_next_apply_case_pause_v1(p_job_id,v_nonce,v_owner_epoch);
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
       or (p_job_kind='CASE_EDIT' and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or v_result->>'phase' is distinct from case when v_job.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end
          or v_result->>'issue_code' is distinct from v_job.last_error_code
          or not exists(select 1 from private.bpay_next_case_edit_request q where q.command_id=v_job.command_id
            and q.candidate_id=v_job.candidate_id and q.case_id::text=v_result->>'case_id'
            and q.new_rule_id::text=v_result->>'rule_id' and (q.expected_case_revision+1)::text=v_result->>'case_revision')))
       or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or not exists(select 1 from private.bpay_next_case_pause_request q where q.command_id=v_job.command_id
             and q.candidate_id=v_job.candidate_id and q.action=p_job_kind
             and private.bpay_next_case_pause_receipt_v1(q.command_id,q.case_id,q.candidate_id) is not distinct from(v_result-'replay'))))
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
    if p_job_kind='PREPARE' then
      update private.bpay_next_job set retry_count=0,next_retry_at_utc=null,needs_attention=false,last_retry_owner_epoch=null,
        retry_prior_issue_code=null,retry_prior_issue_work_id=null where id=p_job_id;
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
      when 'CASE_EDIT' then
        v_output:=v_output||jsonb_build_object('phase',v_result->>'phase','case_id',v_result->>'case_id',
          'case_revision',v_result->>'case_revision','rule_id',v_result->>'rule_id','issue_code',v_result->>'issue_code');
      when 'CASE_PAUSE','CASE_RESUME' then
        v_output:=v_output||(v_result-'replay');
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
  if v_job.needs_attention or v_job.next_retry_at_utc>pg_catalog.clock_timestamp() or v_job.retry_count=5 then
    raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_RETRY_DEFERRED';end if;
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

create or replace function public.bpay_next_job_wake_page_v1(p_lane text,p_limit integer)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare
  v_now timestamptz;
  v_job record; v_items jsonb:='[]'::jsonb;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_lane is null or p_lane not in ('READY','LEASE_EXPIRED')
    or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_RUNTIME_SERVICE_INPUT_INVALID';
  end if;
  -- Sample after any owner-lock wait; the rotation must remain in the future.
  v_now:=pg_catalog.clock_timestamp();
  -- Two explicit index lanes avoid filtering an unbounded mixed open list.
  -- Ready jobs may still wait for an earlier Candidate command; only CLAIM
  -- decides that. Rotating their wake time prevents one such job monopolising
  -- discovery. Never change available_at, status, owner epoch or lease tokens.
  if p_lane='READY' then
    for v_job in select j.id,j.job_kind,j.command_sequence from private.bpay_next_job j
      where j.status in ('READY','BLOCKED') and not j.needs_attention and greatest(j.queue_wake_after_utc,j.next_retry_at_utc)<=v_now
      order by greatest(j.queue_wake_after_utc,j.next_retry_at_utc),j.command_sequence,j.id
      limit p_limit for update skip locked
    loop
      update private.bpay_next_job set queue_wake_after_utc=v_now+interval '1 minute' where id=v_job.id;
      v_items:=v_items||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'job_id',v_job.id,'job_kind',v_job.job_kind,'command_sequence',v_job.command_sequence::text));
    end loop;
  else
    for v_job in select j.id,j.job_kind,j.command_sequence from private.bpay_next_job j
      where j.status='LEASED' and not j.needs_attention and greatest(j.queue_wake_after_utc,j.lease_until_utc,j.next_retry_at_utc)<=v_now
      order by greatest(j.queue_wake_after_utc,j.lease_until_utc,j.next_retry_at_utc),j.command_sequence,j.id
      limit p_limit for update skip locked
    loop
      update private.bpay_next_job set queue_wake_after_utc=v_now+interval '1 minute' where id=v_job.id;
      v_items:=v_items||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'job_id',v_job.id,'job_kind',v_job.job_kind,'command_sequence',v_job.command_sequence::text));
    end loop;
  end if;
  return pg_catalog.jsonb_build_object('lane',p_lane,'items',v_items);
end
$function$;

create or replace function private.bpay_next_prepare_attention_page_v1(p_run_id uuid,p_after_candidate uuid,p_limit integer)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare r private.bpay_next_pay_run%rowtype;v_rows jsonb:='[]'::jsonb;v_row jsonb;v_n integer:=0;v_complete boolean:=true;
begin
 if p_run_id is null or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 select pf08_bpay_next_pay_run.* into strict r from private.bpay_next_pay_run pf08_bpay_next_pay_run where id=p_run_id;
 if not exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id
  join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
  where rc.run_id=p_run_id) then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if r.status not in ('PREPARING','REVIEW') or r.selection_state<>'SEALED' then
  raise exception using errcode='55000',message='BPAY_NEXT_REVIEW_RUN_STATE_INVALID';end if;
 for v_row in select jsonb_build_object('run_worker_id',w.id,'candidate_id',j.candidate_id,
  'candidate_display_name',left(coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),'Unnamed Candidate'),250),
  'pay_channel',w.target_pay_channel,'worker_status',w.status,'job_id',j.id,'work_id',null,'module_epoch',j.module_epoch::text,
  'owner_epoch',j.owner_epoch::text,'phase',j.phase,'cursor',j.cursor_key,'retry_count',j.retry_count::text,
  'last_error_code',j.last_error_code,'retry_eligible',r.preparation_expires_at_utc>statement_timestamp()
    and private.bpay_next_prepare_job_active_v1(j.id)
    and w.active_prepare_job_id=j.id and not exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id)
    and not exists(select 1 from private.bpay_next_preparation_discard_request where run_id=p_run_id))
 from private.bpay_next_job j join private.bpay_next_run_worker w on w.run_id=p_run_id and w.candidate_id=j.candidate_id
 join public.candidates c on c.id=j.candidate_id
 where j.retry_run_id=p_run_id and j.job_kind='PREPARE' and j.needs_attention and w.active_prepare_job_id=j.id
  and (p_after_candidate is null or j.candidate_id>p_after_candidate)
 order by j.candidate_id,j.id limit p_limit
 loop
  if octet_length(jsonb_build_object('rows',v_rows||jsonb_build_array(v_row),'complete',false)::text)>120000 then v_complete:=false;exit;end if;
  v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
 end loop;
 if v_n=p_limit then v_complete:=false;end if;
 return jsonb_build_object('rows',v_rows,'complete',v_complete);
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
 return pg_catalog.jsonb_build_object('needs_attention',exists(select 1 from private.bpay_next_job j
  join private.bpay_next_run_worker w on w.run_id=v_run.id and w.candidate_id=j.candidate_id and w.active_prepare_job_id=j.id
  where j.retry_run_id=v_run.id and j.job_kind='PREPARE' and j.needs_attention),
  'run_id',v_run.id,'status',v_run.status,
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

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  if p_kind in ('CASE_AUDIT','CANDIDATE_CASE_REPORT') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_finance_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind in ('CASE_LIST','CASE_SUMMARY') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_list_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args ? 'case_id' or p_args ? 'expected_case_revision' then
      if jsonb_typeof(p_args) is distinct from 'object' or p_args-array['input','case_id','expected_case_revision']<>'{}'::jsonb
        or not(p_args ?& array['input','case_id','expected_case_revision'])
        or jsonb_typeof(p_args->'case_id') is distinct from 'string'
        or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
        or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
        or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      return jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id,
        (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true);
    end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']||case when p_args ? 'attention_only' then array['attention_only'] else array[]::text[] end
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
      ||case when p_args ? 'edit_command_id' then array['edit_command_id'] else array[]::text[] end
      ||case when p_args ? 'pause_command_id' then array['pause_command_id'] else array[]::text[] end
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='WORKERS' and p_args ? 'attention_only' then
    if p_args->'attention_only' is distinct from 'true'::jsonb then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_attention_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,(p_args->>'limit')::integer);
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
    v_progress:=private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
    if p_args ? 'edit_command_id' then
      if jsonb_typeof(p_args->'edit_command_id') is distinct from 'string'
        or p_args->>'edit_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_edit_receipt_v1((p_args->>'edit_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('edit_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if p_args ? 'pause_command_id' then
      if jsonb_typeof(p_args->'pause_command_id') is distinct from 'string'
         or p_args->>'pause_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_PAUSE_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_pause_receipt_v1((p_args->>'pause_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('pause_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if pg_catalog.octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    return v_progress;
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
  if exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_id=p_run_id and x.status<>'COMPLETE'
    and not(x.action='REPREPARE' and private.bpay_next_prepare_attention_v1(x.job_id))) then
    return pg_catalog.jsonb_build_object('phase','PREPARING','reason','WORKERS_INCOMPLETE');end if;
  if exists (select 1 from private.bpay_next_job j
       where j.command_id=v_command.id and j.job_kind='PREPARE'
         and j.status<>'DONE' and not private.bpay_next_prepare_attention_v1(j.id)) then
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
      where job_id=p_job_id and run_worker_id=v_worker.id and private.bpay_next_retry_execution_v1(p_job_id,v_worker.id,target_preparation_revision)=v_worker.preparation_revision and status='RUNNING';
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id and exists(
      select 1 from private.bpay_next_worker_reprepare_request x where x.job_id=p_job_id and x.initiator_kind='OFFICE' and x.status='COMPLETE');
    perform private.bpay_next_review_simple_run_v1(v_run_id);
  end if;
  return pg_catalog.jsonb_build_object('done',v_done,'phase',case when v_phase='DONE' then 'REVIEW' else v_phase end,
    'cursor',v_cursor::text,'rows_visited',v_seen::text,'replay',false);
end
$function$;

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
      update private.bpay_next_job set status='DONE',phase='EXPIRED',needs_attention=false,
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
      and x.module_epoch=v_epoch and private.bpay_next_retry_execution_v1(x.job_id,v_worker.id,x.target_preparation_revision)=v_worker.preparation_revision
      and x.captured_preparation_revision=v_worker.captured_preparation_revision and x.selection_revision=v_worker.case_selection_revision
      and (x.action='REPREPARE' or (x.action='RELEASE' and v_already_released and x.status='COMPLETE')))
      or v_active.candidate_id<>v_candidate or v_active.module_epoch<>v_epoch or v_active.job_kind<>'PREPARE' then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ACTIVE_RETRY_INVALID';end if;
    if v_active.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',needs_attention=false,lease_nonce=null,lease_until_utc=null,
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
      update private.bpay_next_job set status='DONE',phase='DISCARDED',needs_attention=false,
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
      and x.module_epoch=v_epoch and private.bpay_next_retry_execution_v1(x.job_id,v_worker.id,x.target_preparation_revision)=v_worker.preparation_revision
      and x.captured_preparation_revision=v_worker.captured_preparation_revision and x.selection_revision=v_worker.case_selection_revision
      and (x.action='REPREPARE' or (x.action='RELEASE' and v_already_released and x.status='COMPLETE')))
      or v_active.candidate_id<>v_candidate or v_active.module_epoch<>v_epoch or v_active.job_kind<>'PREPARE' then
      raise exception using errcode='23514',message='BPAY_NEXT_DISCARD_ACTIVE_RETRY_INVALID';end if;
    if v_active.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='DISCARDED',needs_attention=false,lease_nonce=null,lease_until_utc=null,
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
      and x.module_epoch=v_job.module_epoch and private.bpay_next_retry_execution_v1(x.job_id,x.run_worker_id,x.target_preparation_revision)=v_binding.expected_execution_revision
      and x.captured_preparation_revision=v_binding.preparation_revision and x.selection_revision=v_binding.selection_revision) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRED_PREPARE_RECEIPT_JOB_MISMATCH';end if;
  return pg_catalog.jsonb_build_object('done',true,'phase','EXPIRED','cursor',v_job.cursor_key,
    'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
    'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
    'issue_code',coalesce(v_worker.review_issue_code,v_binding.original_review_issue_code),
    'issue_work_id',coalesce(v_worker.review_issue_work_id,v_binding.original_review_issue_work_id));
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
      and x.module_epoch=v_job.module_epoch and private.bpay_next_retry_execution_v1(x.job_id,x.run_worker_id,x.target_preparation_revision)=v_binding.expected_execution_revision
      and x.captured_preparation_revision=v_binding.preparation_revision and x.selection_revision=v_binding.selection_revision) then
    raise exception using errcode='23514',message='BPAY_NEXT_DISCARDED_PREPARE_RECEIPT_JOB_MISMATCH';end if;
  return pg_catalog.jsonb_build_object('done',true,'phase','DISCARDED','cursor',v_job.cursor_key,
    'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
    'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
    'issue_code',coalesce(v_worker.review_issue_code,v_binding.original_review_issue_code),
    'issue_work_id',coalesce(v_worker.review_issue_work_id,v_binding.original_review_issue_work_id));
end
$function$;

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
      where run_worker_id=v_binding.run_worker_id and private.bpay_next_retry_execution_v1(job_id,run_worker_id,target_preparation_revision)=v_binding.expected_execution_revision
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
      where run_worker_id=v_binding.run_worker_id and private.bpay_next_retry_execution_v1(job_id,run_worker_id,target_preparation_revision)=v_binding.expected_execution_revision
        and action='REPREPARE' and status<>'COMPLETE';
    perform private.bpay_next_finish_discard_member_v1(p_job_id);
  end if;
  return pg_catalog.jsonb_build_object('phase',case when v_binding.status='DISCARDED' then 'DISCARDED' else 'RELEASE' end,
    'cursor',v_checkpoint::text,'rows_visited',v_seen::text,'work_released',v_work_holds::text,
    'case_holds_released',v_case_released::text,'run_worker_id',v_binding.run_worker_id,'replay',false);
end
$function$;

create or replace function private.bpay_next_record_case_hold_transition_v1(p_job_id uuid,p_hold_id uuid,p_transition text)
returns void language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_hold private.bpay_next_case_hold%rowtype;v_job private.bpay_next_job%rowtype;
 v_command uuid;v_kind text;v_actor uuid;v_actor_kind text:='UNKNOWN';v_reason text;v_time timestamptz;
 v_ordinal smallint;v_release record;v_prior private.bpay_next_case_hold_transition%rowtype;
begin
 select h.* into strict v_hold from private.bpay_next_case_hold h where h.id=p_hold_id;
 select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
 if v_job.candidate_id is distinct from v_hold.candidate_id or v_hold.status is distinct from p_transition
   or v_hold.instruction_id is null or v_hold.allocation_result_id is null
   or p_transition not in ('ACTIVE','RELEASED','REALISED') then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_SCOPE_INVALID';end if;
 v_command:=v_job.command_id;v_kind:=v_job.job_kind;
 -- A system RELEASE can reuse the original PREPARE job. Its exact running
 -- action must precede PREPARE attribution; an allocation state's old job is not a cause.
 select r.command_id,r.action,r.initiator_kind,r.actor_user_id,r.original_issue_code into v_release
  from private.bpay_next_worker_reprepare_request r
  join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.preparation_revision=private.bpay_next_retry_execution_v1(r.job_id,w.id,r.target_preparation_revision)
  where r.job_id=p_job_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id
   and r.status='RUNNING' and ((p_transition='RELEASED' and r.action='RELEASE') or(p_transition='ACTIVE' and r.action='REPREPARE'));
 if found then
  v_command:=v_release.command_id;v_kind:='WORKER_'||v_release.action;v_actor:=v_release.actor_user_id;
  v_actor_kind:=case v_release.initiator_kind when 'SYSTEM' then 'SYSTEM' when 'OFFICE' then 'HUMAN' else 'UNKNOWN' end;
  v_reason:=v_release.original_issue_code;
 elsif v_job.job_kind='PREPARATION_EXPIRY' then
  select r.actor_user_id,case r.initiator_kind when 'SYSTEM' then 'SYSTEM' else 'HUMAN' end into v_actor,v_actor_kind
   from private.bpay_next_preparation_expiry_worker w join private.bpay_next_preparation_expiry_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARATION_DISCARD' then
  select r.actor_user_id,'HUMAN',r.reason into v_actor,v_actor_kind,v_reason
   from private.bpay_next_preparation_discard_worker w join private.bpay_next_preparation_discard_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PAYE_NET_ENTRY' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_paye_net_request r
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='SIMPLE_CANCEL' then
  select p.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_reauth_proof p
   where p.consumed_command_id=v_job.command_id and p.action_kind='CANCEL_REQUEST';
 elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
  select coalesce(i.actor_user_id,r.actor_user_id),'HUMAN' into v_actor,v_actor_kind
   from private.bpay_next_outcome_request r left join private.bpay_next_internal_receipt i
    on i.id=r.internal_receipt_id and i.command_id=r.command_id and i.transfer_id=r.transfer_id
     and i.run_worker_id=r.run_worker_id and i.candidate_id=r.candidate_id
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARE' and p_transition='ACTIVE' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_run_worker w
   join private.bpay_next_run_command c on c.run_id=w.run_id and c.command_id=v_job.command_id
   join private.bpay_next_pay_run r on r.id=c.run_id
   where w.id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 end if;
 if v_actor_kind is null or(v_actor_kind='HUMAN' and v_actor is null) then v_actor_kind:='UNKNOWN';v_actor:=null;end if;
 v_time:=case when p_transition='ACTIVE' then v_hold.created_at_utc else v_hold.finished_at_utc end;
 v_ordinal:=case when p_transition='ACTIVE' then 1 else 2 end;
 if v_time is null or not isfinite(v_time) then raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_TIME_INVALID';end if;
 select t.* into v_prior from private.bpay_next_case_hold_transition t where t.hold_id=p_hold_id and t.event_ordinal=v_ordinal;
 if found then
  if (v_prior.transition,v_prior.cause_job_id,v_prior.command_id,v_prior.command_kind,v_prior.actor_kind,v_prior.actor_user_id,v_prior.reason,v_prior.occurred_at_utc)
   is distinct from(p_transition,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,v_time) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_REPLAY_MISMATCH';end if;
  return;
 end if;
 insert into private.bpay_next_case_hold_transition(hold_id,event_ordinal,transition,case_id,candidate_id,run_worker_id,
  case_component_id,instruction_id,allocation_result_id,cause_job_id,command_id,command_kind,actor_kind,actor_user_id,reason,
  source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc)
 values(v_hold.id,v_ordinal,p_transition,v_hold.case_id,v_hold.candidate_id,v_hold.run_worker_id,v_hold.case_component_id,
  v_hold.instruction_id,v_hold.allocation_result_id,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,
  v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat,v_time);
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
      or exists(select 1 from private.bpay_next_worker_reprepare_request x where x.run_worker_id=p_run_worker_id and x.status<>'COMPLETE'
        and not(p_action='RELEASE' and x.action='REPREPARE' and x.job_id=v_active and private.bpay_next_prepare_attention_v1(x.job_id))) then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_STATE_INVALID';end if;
    if p_action='RELEASE' and v_job_status<>'DONE' then
      if not private.bpay_next_prepare_job_active_v1(v_active) then
        raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
      update private.bpay_next_job set status='DONE',phase='DISCARDED',lease_nonce=null,lease_until_utc=null,needs_attention=false,
        last_error_code='BPAY_NEXT_CANDIDATE_PREPARATION_REMOVED' where id=v_active and status in ('READY','LEASED','BLOCKED');
      if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
    end if;
    v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  end if;
  select id into v_state from private.bpay_next_case_allocation_state where run_worker_id=p_run_worker_id
    and preparation_revision=v_data_revision and selection_revision=v_selection_revision and pass_kind='DRAFT' and projection_no=0;
  if v_system or p_action='RELEASE' then
    update private.bpay_next_worker_reprepare_request set status='COMPLETE',stage='COMPLETE',finished_at_utc=pg_catalog.clock_timestamp()
      where job_id=v_active and action='REPREPARE' and private.bpay_next_retry_execution_v1(job_id,run_worker_id,target_preparation_revision)=v_revision and status<>'COMPLETE';
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
  -- The new native action owns its own current target; retire the old operational point.
  update private.bpay_next_job set retry_count=0,next_retry_at_utc=null,needs_attention=false,
    retry_execution_revision=null,retry_request_id=null where id=v_active;
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
alter function private.bpay_next_retry_execution_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_retry_execution_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_attention_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_prepare_attention_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_retry_handoff_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_prepare_retry_handoff_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_retry_admit_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_prepare_retry_admit_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_failure_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_prepare_failure_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_retry_prepare_job_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_retry_prepare_job_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_job_active_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_prepare_job_active_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_execution_guard_v1() owner to postgres;
revoke all on function private.bpay_next_worker_execution_guard_v1() from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function private.bpay_next_claim_prepare_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_prepare_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_job_wake_page_v1(text,integer) owner to postgres;
revoke all on function public.bpay_next_job_wake_page_v1(text,integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_job_wake_page_v1(text,integer) to service_role;
alter function private.bpay_next_prepare_attention_page_v1(uuid,uuid,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_attention_page_v1(uuid,uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_preparation_progress_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_preparation_progress_v1(uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_review_simple_run_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_review_simple_run_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_preparation_discard_one_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_enroll_preparation_discard_one_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_expired_prepare_receipt_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_expired_prepare_receipt_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_discarded_prepare_receipt_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_discarded_prepare_receipt_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) owner to postgres;
revoke all on function private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
