-- Service-only bounded due discovery and ONE exact expiry receipt.
-- No browser/Office clock or SYSTEM actor input; no reservation drain here.
-- Actual expiry and release belong to the ordered 0510 owners. Policy X:
-- no financial recalculation, current Timesheet enrichment or history fallback.
\set ON_ERROR_STOP on
begin;

create or replace function public.bpay_next_preparation_expiry_due_v1(
  p_after_deadline timestamptz,p_after_run_id uuid,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_item record;
  v_items jsonb:='[]'::jsonb;
  v_last_deadline timestamptz;
  v_last_run_id uuid;
  v_now timestamptz:=pg_catalog.clock_timestamp();
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
      where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_limit is null or p_limit not between 1 and 100
     or (p_after_deadline is null) is distinct from (p_after_run_id is null)
     or (p_after_deadline is not null and not pg_catalog.isfinite(p_after_deadline)) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_SERVICE_INPUT_INVALID';
  end if;
  -- Partial due index + keyset cursor. At most 100 tiny identifiers are copied;
  -- never collect all due runs or lock their headers ahead of agency ordering.
  for v_item in
    select r.id,r.preparation_expires_at_utc
    from private.bpay_next_pay_run r
    where r.confirmed_at_utc is null and r.status in ('PREPARING','REVIEW')
      and r.preparation_expires_at_utc is not null
      and r.preparation_expires_at_utc<=v_now
      and (p_after_deadline is null or
        (r.preparation_expires_at_utc,r.id)>(p_after_deadline,p_after_run_id))
    order by r.preparation_expires_at_utc,r.id limit p_limit
  loop
    v_last_deadline:=v_item.preparation_expires_at_utc;
    v_last_run_id:=v_item.id;
    v_items:=v_items||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'run_id',v_item.id,'expected_deadline',pg_catalog.to_char(
        v_item.preparation_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
  end loop;
  return pg_catalog.jsonb_build_object('items',v_items,
    'cursor_deadline',pg_catalog.to_char(v_last_deadline at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'cursor_run_id',v_last_run_id);
end
$function$;

create or replace function public.bpay_next_preparation_expiry_receive_v1(p_run_id uuid)
returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_deadline timestamptz;
  v_command_id uuid;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
      where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_run_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_SERVICE_INPUT_INVALID';
  end if;
  select r.preparation_expires_at_utc into strict v_deadline
    from private.bpay_next_pay_run r where r.id=p_run_id;
  if v_deadline is null then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND';
  end if;
  -- Namespace-derived idempotency identity, not an authority token. Same run
  -- always reaches the same expiry request; no caller picks a new clock/id.
  v_command_id:=pg_catalog.md5('bpay-next:preparation-expiry:v1:'||p_run_id::text)::uuid;
  return private.bpay_next_accept_preparation_expiry_v1(
    v_command_id,p_run_id,null,v_deadline,null);
end
$function$;

alter function public.bpay_next_preparation_expiry_due_v1(timestamptz,uuid,integer) owner to postgres;
alter function public.bpay_next_preparation_expiry_receive_v1(uuid) owner to postgres;
revoke all on function public.bpay_next_preparation_expiry_due_v1(timestamptz,uuid,integer)
  from public,anon,authenticated,service_role;
revoke all on function public.bpay_next_preparation_expiry_receive_v1(uuid)
  from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_expiry_due_v1(timestamptz,uuid,integer) to service_role;
grant execute on function public.bpay_next_preparation_expiry_receive_v1(uuid) to service_role;
notify pgrst,'reload schema';
commit;
