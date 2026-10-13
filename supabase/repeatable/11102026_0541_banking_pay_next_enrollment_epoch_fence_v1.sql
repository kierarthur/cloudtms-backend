-- DR-04 independent-review correction: failed enrollment stays in exact NEXT epoch.
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
  begin
    v_job_id:=private.bpay_next_enroll_one_v1();
  exception when others then
    get stacked diagnostics v_error=returned_sqlstate;
    update private.bpay_next_command c set enrollment_error_code=v_error,
      enrollment_error_at_utc=pg_catalog.transaction_timestamp()
      where c.agency_sequence=v_before+1 and c.module_epoch=v_module_epoch returning c.id into v_blocked_id;
    if v_blocked_id is null then raise;end if;
    return pg_catalog.jsonb_build_object('progressed',false,'last_enrolled_sequence',v_before::text,
      'job_id',null,'job_kind',null,'job_status',null,'blocked_command_id',v_blocked_id,'enrollment_error_code',v_error);
  end;
  update private.bpay_next_command c set enrollment_error_code=null,enrollment_error_at_utc=null
    where c.agency_sequence=v_before+1 and c.module_epoch=v_module_epoch and c.enrollment_error_code is not null;
  select e.last_enrolled_sequence into strict v_after
    from private.bpay_next_enrollment_clock e where e.id=1;
  if v_job_id is not null then
    select j.* into strict v_job from private.bpay_next_job j where j.id=v_job_id;
  end if;
  return pg_catalog.jsonb_build_object(
    'progressed',v_job_id is not null or v_after>v_before,
    'last_enrolled_sequence',v_after::text,
    'job_id',v_job_id,'job_kind',v_job.job_kind,'job_status',v_job.status,
    'blocked_command_id',null,'enrollment_error_code',null);
end
$function$;
alter function public.bpay_next_enroll_service_v1() owner to postgres;
revoke all on function public.bpay_next_enroll_service_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_enroll_service_v1() to service_role;
notify pgrst, 'reload schema';
commit;
