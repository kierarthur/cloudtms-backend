-- One metadata enrollment per call and <=100 recoverable job identifiers.
-- No Candidate/Timesheet/history discovery and no claim, lease adoption or
-- financial worker call. Existing owners retain ordering and exact replay.
\set ON_ERROR_STOP on
begin;

create or replace function public.bpay_next_enroll_service_v1()
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare
  v_before bigint; v_after bigint; v_job_id uuid;
  v_job private.bpay_next_job%rowtype;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select e.last_enrolled_sequence into strict v_before
    from private.bpay_next_enrollment_clock e where e.id=1 for update;
  v_job_id:=private.bpay_next_enroll_one_v1();
  select e.last_enrolled_sequence into strict v_after
    from private.bpay_next_enrollment_clock e where e.id=1;
  if v_job_id is not null then
    select j.* into strict v_job from private.bpay_next_job j where j.id=v_job_id;
  end if;
  return pg_catalog.jsonb_build_object(
    'progressed',v_job_id is not null or v_after>v_before,
    'last_enrolled_sequence',v_after::text,
    'job_id',v_job_id,'job_kind',v_job.job_kind,'job_status',v_job.status);
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
      where j.status in ('READY','BLOCKED') and j.queue_wake_after_utc<=v_now
      order by j.queue_wake_after_utc,j.command_sequence,j.id
      limit p_limit for update skip locked
    loop
      update private.bpay_next_job set queue_wake_after_utc=v_now+interval '1 minute' where id=v_job.id;
      v_items:=v_items||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'job_id',v_job.id,'job_kind',v_job.job_kind,'command_sequence',v_job.command_sequence::text));
    end loop;
  else
    for v_job in select j.id,j.job_kind,j.command_sequence from private.bpay_next_job j
      where j.status='LEASED' and greatest(j.queue_wake_after_utc,j.lease_until_utc)<=v_now
      order by greatest(j.queue_wake_after_utc,j.lease_until_utc),j.command_sequence,j.id
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

alter function public.bpay_next_enroll_service_v1() owner to postgres;
create or replace function public.bpay_next_expiry_dispatch_page_v1(p_limit integer)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_cursor private.bpay_next_expiry_sweep_cursor%rowtype;
  v_page jsonb; v_item jsonb; v_items jsonb:='[]'::jsonb;
  v_deadline timestamptz; v_last_deadline timestamptz; v_last_run_id uuid;
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
  v_finished:=v_finished or v_last_run_id is null;
  -- Advance after this bounded read, not after every financial effect. If the
  -- caller stops before durable queue publication, the next completed sweep
  -- revisits the run. Older failed headers cannot starve later due headers.
  -- No financial evidence is marked received or completed by this cursor.
  update private.bpay_next_expiry_sweep_cursor set
    after_deadline=case when v_finished then null else v_last_deadline end,
    after_run_id=case when v_finished then null else v_last_run_id end,
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
alter function public.bpay_next_job_wake_page_v1(text,integer) owner to postgres;
revoke all on function public.bpay_next_enroll_service_v1() from public,anon,authenticated,service_role;
revoke all on function public.bpay_next_job_wake_page_v1(text,integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_enroll_service_v1() to service_role;
grant execute on function public.bpay_next_job_wake_page_v1(text,integer) to service_role;
notify pgrst,'reload schema';
commit;
