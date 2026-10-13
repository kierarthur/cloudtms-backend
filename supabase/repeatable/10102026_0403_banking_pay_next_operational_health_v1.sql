-- PF-04 D02: on-demand read-only operational measures. Never admission authority.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_operational_health_v1()
returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_now timestamptz := pg_catalog.statement_timestamp();
  v_queue jsonb;
  v_errors jsonb;
  v_oldest_hold numeric;
  v_latency jsonb;
  v_sizes jsonb;
begin
  select pg_catalog.jsonb_build_object(
    'pending_jobs',count(*)::text,
    'oldest_created_age_seconds',case when min(j.created_at_utc::timestamptz) is null then null
      else greatest(0,extract(epoch from (v_now-min(j.created_at_utc::timestamptz))))::text end,
    'unknown_created_jobs',count(*) filter(where j.created_at_utc is null)::text)
  into v_queue from private.bpay_next_job j where j.status in ('READY','LEASED','BLOCKED');

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'code',e.code,'jobs',e.jobs::text) order by e.code),'[]'::jsonb)
  into v_errors from (
    select case when j.last_error_code ~ '^BPAY_NEXT_[A-Z0-9_]{1,120}$'
      then j.last_error_code else 'UNCLASSIFIED' end as code,count(*) as jobs
    from private.bpay_next_job j where j.last_error_code is not null
    group by 1
  ) e;

  select case when min(h.created_at_utc::timestamptz) is null then null
    else greatest(0,extract(epoch from (v_now-min(h.created_at_utc::timestamptz)))) end
  into v_oldest_hold from (
    select wh.created_at_utc from private.bpay_next_hold wh where wh.status='ACTIVE'
    union all
    select ch.created_at_utc from private.bpay_next_case_hold ch where ch.status='ACTIVE'
  ) h;

  -- PF06 captures job lifecycle, not a persistent per-RPC stopwatch. This is
  -- job elapsed time grouped by the stored kind/terminal step. Individual RPC
  -- timings are the six-field Worker logs; never label this isolated DB time.
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'kind',l.job_kind,'step',l.phase,'measurement','JOB_FIRST_START_TO_FINISH',
    'completed_samples',l.samples::text,'average_ms',l.average_ms::text,
    'maximum_ms',l.maximum_ms::text) order by l.job_kind,l.phase),'[]'::jsonb)
  into v_latency from (
    select j.job_kind,j.phase,count(*) as samples,
      avg(extract(epoch from (j.finished_at_utc-j.started_at_utc))*1000) as average_ms,
      max((extract(epoch from (j.finished_at_utc-j.started_at_utc))*1000)::numeric) as maximum_ms
    from private.bpay_next_job j
    where j.status in ('DONE','FAILED') and j.started_at_utc is not null
      and j.finished_at_utc is not null and j.finished_at_utc>=j.started_at_utc
    group by j.job_kind,j.phase
  ) l;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'table',c.relname,'total_bytes',pg_catalog.pg_total_relation_size(c.oid)::text)
    order by c.relname),'[]'::jsonb)
  into v_sizes from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relkind in ('r','p')
    and c.relname like 'bpay\_next\_%' escape '\';

  return pg_catalog.jsonb_build_object('queue_age',v_queue,'error_counts',v_errors,
    'oldest_active_hold_age',v_oldest_hold::text,'step_latency',v_latency,'table_sizes',v_sizes);
end
$function$;

create or replace function public.bpay_next_operational_health_service_v1()
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private
as $function$
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  return private.bpay_next_operational_health_v1();
end
$function$;

alter function private.bpay_next_operational_health_v1() owner to postgres;
revoke all on function private.bpay_next_operational_health_v1() from public,anon,authenticated,service_role;
alter function public.bpay_next_operational_health_service_v1() owner to postgres;
revoke all on function public.bpay_next_operational_health_service_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_operational_health_service_v1() to service_role;
notify pgrst, 'reload schema';
commit;
