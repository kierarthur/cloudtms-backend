-- Repeatable CloudTMS authority: weekly_source_private_classifiers_v1
-- Central server-only identity and policy primitives for Weekly Source.
-- These helpers create no Timesheet, financial, invoice, Workbench or Banking
-- Pay record.
-- The _00 sort key intentionally installs these shared primitives before the
-- dependent Weekly Source authorities in the same release minute.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_sha256_text_v1(
  p_domain text,
  p_value text
) returns bytea
language sql immutable
set search_path to 'pg_catalog','extensions','pg_temp'
as $function$
  select extensions.digest(
    pg_catalog.convert_to(
      coalesce(p_domain,'') || pg_catalog.chr(31) || coalesce(p_value,''),
      'UTF8'
    ),
    'sha256'
  );
$function$;

create or replace function private.weekly_source_sha256_jsonb_v1(
  p_domain text,
  p_value jsonb
) returns bytea
language sql immutable
set search_path to 'pg_catalog','private','extensions','pg_temp'
as $function$
  select private.weekly_source_sha256_text_v1(
    p_domain,
    coalesce(p_value,'null'::jsonb)::text
  );
$function$;

create or replace function private.weekly_source_breaks_equivalent_v1(
  p_source_break_minutes integer,
  p_submitted_break_minutes integer
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select (p_source_break_minutes is null or p_source_break_minutes>=0)
     and (p_submitted_break_minutes is null or p_submitted_break_minutes>=0)
     and coalesce(p_source_break_minutes,0)=coalesce(p_submitted_break_minutes,0);
$function$;

comment on function private.weekly_source_breaks_equivalent_v1(integer,integer) is
  'Weekly comparison primitive: only total break duration is compared; a clock-time break and a duration-only break are equivalent when their minute lengths are equal.';

create or replace function private.weekly_source_scope_fingerprint_v1(
  p_environment text,
  p_agency_id uuid,
  p_source_group_id uuid,
  p_source_cycle_id uuid,
  p_report_scope_id uuid default null,
  p_client_id uuid default null
) returns bytea
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_payload jsonb;
begin
  if p_environment not in ('TEST','LIVE')
     or p_agency_id is null
     or p_source_group_id is null
     or p_source_cycle_id is null then
    raise exception 'WEEKLY_SOURCE_SCOPE_INPUT_INVALID' using errcode='22023';
  end if;

  select * into v_group
  from public.weekly_source_groups
  where id=p_source_group_id;
  if not found
     or v_group.environment is distinct from p_environment
     or v_group.agency_id is distinct from p_agency_id then
    raise exception 'WEEKLY_SOURCE_GROUP_SCOPE_MISMATCH' using errcode='22023';
  end if;

  select * into v_cycle
  from public.weekly_source_cycles
  where id=p_source_cycle_id;
  if not found or v_cycle.source_group_id is distinct from p_source_group_id then
    raise exception 'WEEKLY_SOURCE_CYCLE_SCOPE_MISMATCH' using errcode='22023';
  end if;

  if p_report_scope_id is not null then
    select * into v_scope
    from public.weekly_source_report_scopes
    where id=p_report_scope_id;
    if not found
       or v_scope.source_cycle_id is distinct from p_source_cycle_id
       or v_scope.environment is distinct from p_environment
       or v_scope.agency_id is distinct from p_agency_id
       or v_scope.source_group_id is distinct from p_source_group_id
       or (p_client_id is not null and v_scope.client_id is distinct from p_client_id) then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='22023';
    end if;
  elsif p_client_id is not null and not exists(
    select 1
    from public.weekly_source_group_clients membership
    where membership.source_group_id=p_source_group_id
      and membership.client_id=p_client_id
      and v_cycle.finalisation_week_ending between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date)
  ) then
    raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_MISMATCH' using errcode='22023';
  end if;

  v_payload:=pg_catalog.jsonb_build_object(
    'environment',p_environment,
    'agency_id',p_agency_id,
    'source_group_id',p_source_group_id,
    'source_cycle_id',p_source_cycle_id,
    'report_scope_id',p_report_scope_id,
    'client_id',coalesce(p_client_id,v_scope.client_id),
    'finalisation_week_ending',v_cycle.finalisation_week_ending,
    'cutoff_at_utc',coalesce(v_scope.cutoff_at_utc,v_cycle.cutoff_at_utc),
    'source_family',v_group.source_family
  );
  return private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SCOPE_V1',v_payload);
end;
$function$;

create or replace function private.weekly_source_office_authority_v1(
  p_actor_user_id uuid,
  p_operation text,
  p_source_group_id uuid default null,
  p_client_id uuid default null,
  p_scope_date date default null
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_actor public.tms_users%rowtype;
  v_operation text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_operation,'')));
  v_payment_operation boolean;
  v_scope_date date:=coalesce(p_scope_date,current_date);
  v_payload jsonb;
begin
  if p_actor_user_id is null or v_operation not in (
    'OPEN_QUERY_NOTICE','VIEW_SOURCE_PROGRESS','UPLOAD_SOURCE','SEAL_SOURCE',
    'SUPERSEDE_SOURCE','RECHECK_SOURCE','ASK_CANDIDATES','SEND_MANAGER',
    'RESEND_MANAGER','ACCEPT_SYSTEM_HOURS','FINALISE_WEEK',
    'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE',
    'APPROVE_PROTECTED_PAY','AMEND_PROTECTED_PAY','WITHDRAW_PROTECTED_PAY',
    'RECORD_NOT_WORKED','RECONCILE_PROTECTED_PAY','CORRECT_FINAL_SOURCE'
  ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_OPERATION_INVALID' using errcode='22023';
  end if;

  select * into v_actor
  from public.tms_users
  where id=p_actor_user_id;
  if not found or not v_actor.is_active then
    raise exception 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' using errcode='42501';
  end if;

  if v_operation<>'OPEN_QUERY_NOTICE'
     and pg_catalog.lower(pg_catalog.btrim(coalesce(v_actor.role,'')))<>'admin' then
    raise exception 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' using errcode='42501';
  end if;

  v_payment_operation:=v_operation in (
    'APPROVE_PROTECTED_PAY','AMEND_PROTECTED_PAY','WITHDRAW_PROTECTED_PAY',
    'RECORD_NOT_WORKED','RECONCILE_PROTECTED_PAY'
  );
  if v_payment_operation
     and not (coalesce(v_actor.payment_authoriser,false)
              or coalesce(v_actor.payment_golden_key,false)) then
    raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';
  end if;

  if p_source_group_id is not null and not exists(
    select 1 from public.weekly_source_groups source_group
    where source_group.id=p_source_group_id and source_group.active
  ) then
    raise exception 'WEEKLY_SOURCE_GROUP_NOT_ACTIVE' using errcode='22023';
  end if;

  if p_client_id is not null and (
    p_source_group_id is null or not exists(
      select 1
      from public.weekly_source_group_clients membership
      where membership.source_group_id=p_source_group_id
        and membership.client_id=p_client_id
        and v_scope_date between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
    )
  ) then
    raise exception 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP' using errcode='22023';
  end if;

  v_payload:=pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor.id,
    'actor_active',v_actor.is_active,
    'actor_role',pg_catalog.lower(pg_catalog.btrim(coalesce(v_actor.role,''))),
    'payment_authoriser',coalesce(v_actor.payment_authoriser,false),
    'payment_golden_key',coalesce(v_actor.payment_golden_key,false),
    'operation',v_operation,
    'source_group_id',p_source_group_id,
    'client_id',p_client_id,
    'scope_date',v_scope_date
  );
  return v_payload || pg_catalog.jsonb_build_object(
    'allowed',true,
    'decision_fingerprint',pg_catalog.encode(
      private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_AUTHORITY_V1',v_payload),
      'hex'
    )
  );
end;
$function$;

alter function private.weekly_source_sha256_text_v1(text,text) owner to postgres;
alter function private.weekly_source_sha256_jsonb_v1(text,jsonb) owner to postgres;
alter function private.weekly_source_breaks_equivalent_v1(integer,integer) owner to postgres;
alter function private.weekly_source_scope_fingerprint_v1(text,uuid,uuid,uuid,uuid,uuid) owner to postgres;
alter function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date) owner to postgres;

revoke all on function private.weekly_source_sha256_text_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_sha256_jsonb_v1(text,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_breaks_equivalent_v1(integer,integer)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_scope_fingerprint_v1(text,uuid,uuid,uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)
  from public,anon,authenticated;
grant execute on function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)
  to service_role;

notify pgrst, 'reload schema';

commit;
