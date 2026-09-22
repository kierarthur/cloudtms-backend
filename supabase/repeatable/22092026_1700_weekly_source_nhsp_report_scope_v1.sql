-- Repeatable CloudTMS authority: NHSP report-scope resolution.
-- A final NHSP backing report identifies one Trust inside the file.  The
-- browser cannot choose that Trust or manufacture a report scope: this
-- service-only owner resolves the exact saved Client membership and creates
-- the one idempotent weekly report scope needed by the existing upload owner.
-- Its service grant is sealed by the Weekly Source ACL contract and generated
-- database contract shipped in the same release.

\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_nhsp_report_scope_resolve_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_allowed constant text[]:=array[
    'actor_user_id','source_group_id','source_cycle_id','trust_name'
  ]::text[];
  v_actor uuid;
  v_group_id uuid;
  v_cycle_id uuid;
  v_trust_name text;
  v_unknown text;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_client_id uuid;
  v_client_name text;
  v_match_count integer;
  v_inserted_count integer:=0;
  v_scope public.weekly_source_report_scopes%rowtype;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown
  from pg_catalog.jsonb_object_keys(p_request) key
  where not key=any(v_allowed)
  order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_group_id:=(p_request->>'source_group_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_trust_name:=nullif(pg_catalog.btrim(p_request->>'trust_name'),'');
  exception when others then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_group_id is null or v_cycle_id is null
     or v_trust_name is null or pg_catalog.char_length(v_trust_name)>200 then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;

  select * into v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_group_id
  for update;
  if not found or not v_group.active or v_group.source_family<>'NHSP' then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_GROUP_INVALID' using errcode='22023';
  end if;
  select * into v_cycle
  from public.weekly_source_cycles cycle
  where cycle.id=v_cycle_id
    and cycle.source_group_id=v_group.id
  for update;
  if not found or v_cycle.state not in ('OPEN','FINALISABLE') then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_CYCLE_INVALID' using errcode='55000';
  end if;

  select pg_catalog.count(*),pg_catalog.min(client.id::text)::uuid,
         pg_catalog.min(client.name)
    into v_match_count,v_client_id,v_client_name
  from public.weekly_source_group_clients membership
  join public.clients client on client.id=membership.client_id
  where membership.source_group_id=v_group.id
    and v_cycle.finalisation_week_ending between membership.valid_from
      and coalesce(membership.valid_to,'infinity'::date)
    and pg_catalog.lower(pg_catalog.btrim(client.name))=
      pg_catalog.lower(v_trust_name);
  if v_match_count=0 then
    raise exception 'WEEKLY_SOURCE_NHSP_TRUST_NOT_CONFIGURED' using errcode='22023';
  elsif v_match_count>1 then
    raise exception 'WEEKLY_SOURCE_NHSP_TRUST_AMBIGUOUS' using errcode='55000';
  end if;

  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-nhsp-report-scope:'||v_cycle.id::text||':'||v_client_id::text,0
  ));
  insert into public.weekly_source_report_scopes(
    source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc
  ) values (
    v_cycle.id,v_group.environment,v_group.agency_id,v_group.id,v_client_id,v_cycle.cutoff_at_utc
  ) on conflict (environment,agency_id,source_group_id,client_id,cutoff_at_utc)
    do nothing;
  get diagnostics v_inserted_count=row_count;
  select * into strict v_scope
  from public.weekly_source_report_scopes report_scope
  where report_scope.environment=v_group.environment
    and report_scope.agency_id=v_group.agency_id
    and report_scope.source_group_id=v_group.id
    and report_scope.client_id=v_client_id
    and report_scope.cutoff_at_utc=v_cycle.cutoff_at_utc;
  if v_scope.source_cycle_id is distinct from v_cycle.id then
    raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_CYCLE_MISMATCH' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'report_scope_id',v_scope.id,
    'source_group_id',v_group.id,
    'source_cycle_id',v_cycle.id,
    'client_id',v_client_id,
    'client_name',v_client_name,
    'cutoff_at_utc',v_scope.cutoff_at_utc,
    'authority_scope_version',v_scope.version,
    'idempotent',v_inserted_count=0
  );
end;
$function$;

alter function public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb) is
  'Service-only idempotent NHSP Trust-to-Client weekly report-scope owner. Browser identities are not accepted.';

commit;
