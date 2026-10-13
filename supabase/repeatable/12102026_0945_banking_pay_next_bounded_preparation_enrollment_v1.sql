-- DR20 / BD140: at most20 native metadata jobs for ONE original sealed PREPARE.
-- Full0541 successor. Private2041, financial owners, original ordering/locks unchanged.

\set ON_ERROR_STOP on

begin;

create or replace function public.bpay_next_enroll_service_v1()
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare
  v_before bigint; v_after bigint; v_job_id uuid;
  v_job private.bpay_next_job%rowtype;
  v_error text;v_blocked_id uuid;v_module_epoch bigint;
  v_first_command uuid;v_batch boolean;v_i integer;
  v_jobs jsonb:='[]'::jsonb;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select e.last_enrolled_sequence into strict v_before
    from private.bpay_next_enrollment_clock e where e.id=1 for update;
  if exists(select 1 from private.bpay_next_command c where c.agency_sequence=v_before+1
      and c.module_epoch is distinct from v_module_epoch) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select c.id into v_first_command from private.bpay_next_command c
    where c.agency_sequence=v_before+1;
  -- Lock-free qualification only; the unchanged owner still takes command then
  -- Run SHARE NOWAIT and validates its exact current state on EVERY iteration.
  -- Never hold Candidate business/Work/Case locks or enter another command.
  v_batch:=exists(select 1 from private.bpay_next_command c
    join private.bpay_next_run_command rc on rc.command_id=c.id
    join private.bpay_next_pay_run r on r.id=rc.run_id
    where c.id=v_first_command and c.command_kind='PREPARE'
      and c.module_epoch=v_module_epoch and c.status='ENROLLING'
      and r.accepted_choice_command_id=c.id and r.selection_state='SEALED'
      and r.status='PREPARING' and r.confirmed_at_utc is null
      and not exists(select 1 from private.bpay_next_worker_reprepare_request x
        where x.command_id=c.id and x.initiator_kind='OFFICE')
      and not private.bpay_next_preparation_expiry_fenced_v1(r.id,c.id)
      and not private.bpay_next_preparation_discard_fenced_v1(r.id,c.id));
  begin
    -- One protected page: a failure rolls back every new member/job/count.
    for v_i in 1..case when v_batch then 20 else 1 end loop
      v_job_id:=private.bpay_next_enroll_one_v1();
      if v_job_id is not null then
        select j.* into strict v_job from private.bpay_next_job j where j.id=v_job_id;
        -- Retirement may commit between qualification and the owner's NOWAIT
        -- Run lock. Keep that exact DONE receipt one-call, never a batch.
        if v_batch and (v_job.job_kind is distinct from 'PREPARE'
          or v_job.status is distinct from 'READY') then v_batch:=false;end if;
        v_jobs:=v_jobs||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'job_id',v_job.id,'job_kind',v_job.job_kind,'job_status',v_job.status));
      end if;
      select e.last_enrolled_sequence into strict v_after
        from private.bpay_next_enrollment_clock e where e.id=1;
      exit when not v_batch or v_after>v_before or v_job_id is null;
    end loop;
  exception when others then
    get stacked diagnostics v_error=returned_sqlstate;
    -- PL/pgSQL variables survive rollback; never expose rolled-back UUIDs.
    v_jobs:='[]'::jsonb;v_job_id:=null;v_job:=null;
    update private.bpay_next_command c set enrollment_error_code=v_error,
      enrollment_error_at_utc=pg_catalog.transaction_timestamp()
      where c.agency_sequence=v_before+1 and c.module_epoch=v_module_epoch returning c.id into v_blocked_id;
    if v_blocked_id is null then raise;end if;
    return pg_catalog.jsonb_build_object('progressed',false,'last_enrolled_sequence',v_before::text,
      'job_id',null,'job_kind',null,'job_status',null,'jobs',v_jobs,
      'blocked_command_id',v_blocked_id,'enrollment_error_code',v_error);
  end;
  update private.bpay_next_command c set enrollment_error_code=null,enrollment_error_at_utc=null
    where c.agency_sequence=v_before+1 and c.module_epoch=v_module_epoch and c.enrollment_error_code is not null;
  select e.last_enrolled_sequence into strict v_after
    from private.bpay_next_enrollment_clock e where e.id=1;
  return pg_catalog.jsonb_build_object(
    'progressed',v_job_id is not null or v_after>v_before,
    'last_enrolled_sequence',v_after::text,
    'job_id',v_job_id,'job_kind',v_job.job_kind,'job_status',v_job.status,'jobs',v_jobs,
    'blocked_command_id',null,'enrollment_error_code',null);
end
$function$;
alter function public.bpay_next_enroll_service_v1() owner to postgres;
revoke all on function public.bpay_next_enroll_service_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_enroll_service_v1() to service_role;
notify pgrst, 'reload schema';

commit;
