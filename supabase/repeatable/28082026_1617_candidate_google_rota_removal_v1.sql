-- Repeatable CloudTMS function/view authority: candidate_google_rota_removal_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Only the exact registered MASTER, authenticated by the control plane and
-- bound into the signed private route, can reach this operation. No account,
-- membership, Contract, Timesheet, document, availability-history or emergency
-- record is deleted. Unfinished work fails closed before any durable change.
create or replace function public.candidate_google_rota_remove_v1(
  p_internal_context jsonb,
  p_request jsonb,
  p_correlation_id text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_environment text:=p_internal_context->>'environment';
  v_integration uuid;
  v_operation uuid;
  v_code text:=pg_catalog.upper(pg_catalog.btrim(p_request->>'candidate_code'));
  v_source text:=p_request->>'candidate_source_hmac';
  v_key integer;
  v_hash text;
  v_code_hash text;
  v_candidate_id uuid;
  v_count integer;
  v_candidate public.candidates%rowtype;
  v_link private.candidate_daily_source_links%rowtype;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_receipt private.candidate_google_rota_removal_receipts%rowtype;
  v_outcome text:='UNLINKED';
  v_lock text;
begin
  if pg_catalog.jsonb_typeof(p_internal_context) is distinct from 'object'
    or p_internal_context->>'route_context_verified' is distinct from 'true'
    or p_internal_context->>'audience' is distinct from 'GOOGLE_ROTA_REMOVE'
    or p_internal_context->>'project_role' is distinct from 'MASTER'
    or coalesce(v_environment,'') not in ('TEST','LIVE')
    or pg_catalog.jsonb_typeof(p_request) is distinct from 'object'
    or not (p_request ?& array['operation_id','candidate_code','candidate_source_hmac',
      'source_hmac_key_version','surname','email','mobile','row_fingerprint'])
    or (select count(*) from pg_catalog.jsonb_object_keys(p_request))<>8
    or coalesce(v_code,'') !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
    or coalesce(v_source,'') !~ '^[a-f0-9]{64}$'
    or coalesce(p_request->>'row_fingerprint','') !~ '^[a-f0-9]{64}$'
    or coalesce(p_request->>'source_hmac_key_version','') !~ '^[1-9][0-9]{0,8}$'
    or coalesce(p_request->>'operation_id','') !~ '^[0-9a-fA-F-]{36}$'
    or p_internal_context->>'operation_id' is distinct from p_request->>'operation_id'
    or coalesce(p_internal_context->>'integration_id','') !~ '^[0-9a-fA-F-]{36}$'
    or coalesce(length(btrim(p_correlation_id)),0) not between 1 and 200
    or p_now_utc is null or not isfinite(p_now_utc)
    or abs(extract(epoch from p_now_utc-clock_timestamp()))>120 then
    raise exception using errcode='22023',message='GOOGLE_ROTA_REMOVAL_INVALID';
  end if;
  v_operation:=(p_request->>'operation_id')::uuid;
  v_integration:=(p_internal_context->>'integration_id')::uuid;
  v_key:=(p_request->>'source_hmac_key_version')::integer;
  v_hash:=private._candidate_daily_json_sha256_v1(p_request);
  v_code_hash:=encode(extensions.digest(convert_to(v_code,'UTF8'),'sha256'),'hex');

  -- Same global/source ordering as generation publication and source identity.
  for v_lock in select x from unnest(array['GLOBAL:'||v_code,
      'SOURCE:'||v_key::text||':'||v_source])x order by x loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(v_environment||':'||v_lock,0));
  end loop;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    v_environment||':ROTA_REMOVE:'||v_integration::text||':'||v_operation::text,0));
  select * into v_receipt from private.candidate_google_rota_removal_receipts
    where environment=v_environment and integration_id=v_integration and operation_id=v_operation;
  if found then
    if v_receipt.request_sha256<>v_hash then
      raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
    end if;
    -- Crucially, replay is read-only even if this person has since re-enrolled.
    return jsonb_build_object('ok',true,'state',v_receipt.outcome,
      'operation_id',v_operation,'idempotent_replay',true);
  end if;

  perform 1 from public.candidates where upper(btrim(key_norm))=v_code order by id for update;
  select count(*),min(id::text)::uuid into v_count,v_candidate_id
    from public.candidates where upper(btrim(key_norm))=v_code;
  if v_count>1 then
    raise exception using errcode='23505',message='GOOGLE_ROTA_IDENTITY_AMBIGUOUS';
  end if;
  select * into v_link from private.candidate_daily_source_links
    where environment=v_environment and source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
      and hmac_key_version=v_key and identifier_hmac=v_source;
  if v_link.link_id is not null and (v_link.state='REJECTED'
      or (v_candidate_id is not null and v_candidate_id<>v_link.candidate_id)
      or (v_candidate_id is null and v_link.state in ('PRIMARY','OVERLAP'))) then
    raise exception using errcode='40001',message='GOOGLE_ROTA_IDENTITY_CHANGED';
  end if;
  if v_candidate_id is not null then
    select * into strict v_candidate from public.candidates where id=v_candidate_id;
    -- A first-generation link may not yet exist. In that case require the same
    -- exact three-field match used by Create; a CID alone is insufficient.
    if v_link.link_id is null and (
        lower(btrim(coalesce(v_candidate.last_name,''))) is distinct from lower(btrim(p_request->>'surname'))
        or private._candidate_normalize_email(v_candidate.email) is distinct from private._candidate_normalize_email(p_request->>'email')
        or private._candidate_google_mobile_normalize_v1(v_candidate.phone) is distinct from private._candidate_google_mobile_normalize_v1(p_request->>'mobile')
        or nullif(btrim(p_request->>'surname'),'') is null
        or nullif(btrim(p_request->>'email'),'') is null
        or nullif(btrim(p_request->>'mobile'),'') is null) then
      raise exception using errcode='40001',message='GOOGLE_ROTA_IDENTITY_CHANGED';
    end if;
    select * into v_scope from private.candidate_daily_authority_scopes
      where environment=v_environment and candidate_id=v_candidate_id for update;
    if coalesce(v_scope.transition_in_progress,false) then
      return jsonb_build_object('ok',false,'state','BUSY','error_code','GOOGLE_ROTA_UPDATES_PENDING');
    end if;
    if exists(select 1 from public.candidate_daily_rota_generations g
      join public.candidate_daily_rota_days d using(generation_id)
      where g.environment=v_environment and g.candidate_id=v_candidate_id and g.state='ACTIVE'
        and d.booked and (d.shift_ends_at>p_now_utc or
          (d.shift_ends_at is null and d.rota_date >= (p_now_utc at time zone 'Europe/London')::date))) then
      raise exception using errcode='55000',message='GOOGLE_ROTA_BOOKINGS_EXIST';
    end if;
    perform 1 from public.candidate_daily_sheet_projection_outbox
      where environment=v_environment and candidate_id=v_candidate_id order by outbox_id for update;
    perform 1 from private.candidate_daily_external_effect_receipts
      where environment=v_environment and candidate_id=v_candidate_id order by effect_receipt_id for update;
    if exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where environment=v_environment and candidate_id=v_candidate_id and state not in ('DELIVERED','TERMINAL'))
      or exists(select 1 from private.candidate_daily_external_effect_receipts
        where environment=v_environment and candidate_id=v_candidate_id and state in ('IN_PROGRESS','UNKNOWN')) then
      return jsonb_build_object('ok',false,'state','BUSY','error_code','GOOGLE_ROTA_UPDATES_PENDING');
    end if;
    update private.candidate_daily_entitlements set enabled=false,
      reason='Removed from the Google Rota',evidence_sha256=v_hash,updated_at_utc=p_now_utc
      where environment=v_environment and candidate_id=v_candidate_id;
    update private.candidate_daily_source_links set state='RETIRED',
      valid_to_utc=greatest(p_now_utc,valid_from_utc+interval '1 microsecond'),updated_at_utc=p_now_utc
      where environment=v_environment and candidate_id=v_candidate_id
        and source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID' and hmac_key_version=v_key
        and identifier_hmac=v_source and state in ('PRIMARY','OVERLAP');
    update public.candidate_daily_rota_generations set state='SUPERSEDED',updated_at_utc=p_now_utc
      where environment=v_environment and candidate_id=v_candidate_id and state='ACTIVE';
    update private.candidate_daily_authority_scopes set active_generation_id=null,updated_at_utc=p_now_utc
      where environment=v_environment and candidate_id=v_candidate_id;
    update public.candidates set key_norm=null,updated_at=p_now_utc
      where id=v_candidate_id and upper(btrim(key_norm))=v_code;
    if not found then raise exception using errcode='40001',message='GOOGLE_ROTA_IDENTITY_CHANGED'; end if;
    v_outcome:='REMOVED';
  end if;
  insert into private.candidate_google_rota_removal_receipts(environment,integration_id,operation_id,
    candidate_id,candidate_code_sha256,source_hmac_key_version,candidate_source_hmac,
    request_sha256,outcome,removed_at_utc,correlation_id)
  values(v_environment,v_integration,v_operation,v_candidate_id,v_code_hash,v_key,v_source,
    v_hash,v_outcome,p_now_utc,p_correlation_id);
  return jsonb_build_object('ok',true,'state',v_outcome,'operation_id',v_operation,'idempotent_replay',false);
end;
$function$;

alter function public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz) owner to postgres;
revoke all on function public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz) to service_role;
notify pgrst, 'reload schema';

commit;
