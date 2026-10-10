-- SP-03 compatibility correction: existing shared qualified WAIT, original manual/core scope.
\set ON_ERROR_STOP on
begin;
CREATE OR REPLACE FUNCTION private.weekly_source_first_authorise_core_v1(p_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid, p_lock_result jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_family jsonb;

  v_elsewhere uuid;
  v_actor public.tms_users%rowtype;
  v_authorise jsonb;
  v_root public.timesheets%rowtype;
  v_contract_week_id uuid;
  v_signature text;
  v_signature_json jsonb;
  v_generation integer;
  v_agency_id uuid;
  v_protected_hashes text[];
  v_evidence_reference jsonb;
  v_decision_digest bytea;
  v_id uuid;
begin
  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  select * into v_actor from public.tms_users where id=p_actor_user_id;
  if not found or not coalesce(v_actor.is_active,false) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_ACTOR_INVALID',
      'retryable',false,'reason','ACTOR_NOT_FOUND_OR_INACTIVE');
  end if;

  -- proof/34 section 5 steps 2 and 3: the lock result is the proof that the
  -- deadlock-free lock set is held and the only source of the canonical answer.
  if coalesce((p_lock_result->>'ok')::boolean,false) is not true
     or coalesce(p_lock_result->>'gate','')<>'GRANTED' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_LOCK_RESULT_INVALID',
      'retryable',false,'reason','LOCK_RESULT_NOT_GRANTED');
  end if;

  select family_element.value
    into v_family
  from pg_catalog.jsonb_array_elements(coalesce(p_lock_result->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if v_family is null
     or coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
     or coalesce((v_family->>'family_is_current')::boolean,false) is not true
     or (v_family->>'canonical_timesheet_id')::uuid is distinct from p_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_family->>'canonical_timesheet_id');
  end if;

  -- The outer owner still holds its accepted family locks. This core
  -- addresses the live financial authorisation directly by frozen work key;
  -- it never treats obsolete Timesheet hours as current entitlement.
  -- WP-07b finding F2: a live generation anywhere ELSE in the family means the
  -- authorised root was rotated after its first authorisation.  proof/34
  -- section 6: "the operation stops with WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE,
  -- no financial change is made, and Office review is required."  Refused
  -- before the ordinary Authorise owner is called, so nothing is written.
  select live_row.root_timesheet_id
    into v_elsewhere
  from public.weekly_source_root_authorisations as live_row
  where live_row.family_booking_id=v_family->>'family_booking_id'
    and live_row.root_timesheet_id is distinct from p_timesheet_id
    and live_row.withdrawn_at_utc is null
  limit 1;
  if v_elsewhere is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'retryable',false,'reason','LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW',
      'timesheet_id',p_timesheet_id,
      'live_authorisation_root_timesheet_id',v_elsewhere,
      'family_booking_id',v_family->>'family_booking_id');
  end if;

  -- A live generation on the requested row means the root is already an
  -- authorised managed root; re-authorisation is only ever the path after a
  -- withdrawal.
  if exists (select 1 from public.weekly_source_root_authorisations as live_row
             where live_row.root_timesheet_id=p_timesheet_id
               and live_row.withdrawn_at_utc is null) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED',
      'retryable',false,'timesheet_id',p_timesheet_id);
  end if;

  -- The Office projection has always described this as a first-authorisation
  -- hold. Enforce it in the command while the canonical family locks are held,
  -- using the Timesheet's work week rather than the source cutoff period.
  select * into v_root from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_TIMESHEET_MISSING','retryable',false);
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_QUERY_FAMILY:'||v_root.contract_id::text||':'||v_root.week_ending_date::text,0));
  -- SP-03: retain ordinary/new-B scope while qualifying actual query exceptions.
  perform private.weekly_source_office_authority_v1(p_actor_user_id,'APPROVE_PROTECTED_PAY');
  if exists (
    select 1 from private.weekly_source_manual_reviews manual_review
    where manual_review.contract_id=v_root.contract_id
      and manual_review.work_date between v_root.week_ending_date-6 and v_root.week_ending_date
      and manual_review.state='OPEN'
  ) then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_QUERY_OPEN','retryable',false,
      'reason','RESOLVE_MANUAL_QUERY');
  end if;
  if exists (
    select 1 from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.state='OPEN'
      and comparison.contract_id=v_root.contract_id
      and work_event.work_date between v_root.week_ending_date-6 and v_root.week_ending_date
      and comparison.issue_family is distinct from 'CANDIDATE_TIMESHEET_MISSING'
      and private.weekly_source_covered_hours_incident_v1(incident.id) is not true
  ) then
    return pg_catalog.jsonb_build_object('ok',false,
      'code','WEEKLY_SOURCE_FIRST_AUTHORISE_QUERY_OPEN','retryable',false,
      'reason','RESOLVE_OPEN_HOURS_QUERY');
  end if;

  -- The UNCHANGED ordinary Authorise owner, exactly once, through its current
  -- contract.  No timestamp is passed: the owner keeps its own default.
  v_evidence_reference:=private.bpay_protected_reference_for_root_v1(p_timesheet_id);

  v_authorise:=public.timesheet_authorise_generic_atomic(
    p_timesheet_id=>p_timesheet_id,
    p_expected_timesheet_id=>p_timesheet_id,
    p_actor_user_id=>p_actor_user_id,
    p_expected_row_signature=>nullif(pg_catalog.btrim(coalesce(p_expected_row_signature,'')),''));

  if coalesce((v_authorise->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_FIRST_AUTHORISE_OWNER_REFUSED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_FIRST_AUTHORISE_OWNER_REFUSED',
              'timesheet_id',p_timesheet_id,
              'authorise_result',v_authorise)::text;
  end if;

  select * into v_root from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
              'reason','CANONICAL_ROW_MISSING_AFTER_AUTHORISE',
              'timesheet_id',p_timesheet_id)::text;
  end if;

  -- proof/34 section 4: the row signature AT authorisation.  Read from the
  -- owner's own return where it gives one, else recomputed; blank fails closed.
  v_signature:=nullif(pg_catalog.btrim(coalesce(
    v_authorise->>'backend_row_signature',v_authorise->>'row_signature','')),'');
  if v_signature is null then
    select contract_week_row.id into v_contract_week_id
    from public.contract_weeks as contract_week_row
    where contract_week_row.timesheet_id=p_timesheet_id;
    v_signature_json:=public.timesheet_lifecycle_guard_signature_v1(
      p_timesheet_id,v_contract_week_id,false);
    v_signature:=nullif(pg_catalog.btrim(coalesce(
      v_signature_json->>'backend_row_signature',
      v_signature_json->>'row_signature','')),'');
  end if;
  if v_signature is null or pg_catalog.char_length(v_signature)>512 then
    raise exception 'WEEKLY_SOURCE_ROOT_SIGNATURE_UNAVAILABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_SIGNATURE_UNAVAILABLE',
              'timesheet_id',p_timesheet_id)::text;
  end if;

  select coalesce(pg_catalog.max(prior_row.authorisation_generation),0)+1
    into v_generation
  from public.weekly_source_root_authorisations as prior_row
  where prior_row.root_timesheet_id=p_timesheet_id;

  -- ROUND 5 Part E, the rotation/signature ruling: the authorisation row binds
  -- all five required fields, and the canonical digest of them is stored on the
  -- row so that a later reader can prove the row is the one that was written
  -- rather than trusting the columns individually.  Bound over the WHOLE
  -- family: the protected decisions and the agency evidence belong to the
  -- family, not to whichever physical row is current.
  SELECT agency_id INTO v_agency_id
  FROM private.bpay_protected_family_for_current_v1(p_timesheet_id);
  v_protected_hashes:=NULL; -- V2 uses the frozen reference; never pretend this is a V1 empty set.
  v_decision_digest:=private.weekly_source_publication_request_digest_v1(
    private.bpay_protected_reference_authorisation_signature_v1(
      v_agency_id,p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
      v_signature,v_protected_hashes,2::smallint,v_evidence_reference));

  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,current_entitlement_head_id,authorised_by_user_id,
    agency_id,protected_decision_hashes,decision_digest,bpay_evidence_codec_v1,bpay_evidence_reference_v1
  ) values (
    p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
    v_signature,null,p_actor_user_id,
    v_agency_id,v_protected_hashes,v_decision_digest,2,v_evidence_reference
  ) returning id into v_id;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'timesheet_id',p_timesheet_id,
    'root_authorisation_id',v_id,
    'authorisation_generation',v_generation,
    'family_booking_id',v_root.booking_id,
    'timesheet_version',v_root.version,
    'authorised_row_signature',v_signature,
    'agency_id',v_agency_id,
    'decision_digest',pg_catalog.encode(v_decision_digest,'hex'),
    'authorise_result',pg_catalog.jsonb_build_object(
      'ok',true,
      'operation',v_authorise->>'operation',
      'timesheet_id',v_authorise->>'timesheet_id',
      'contract_week_id',v_authorise->>'contract_week_id',
      'processing_status',v_authorise->>'processing_status'));
end;
$function$;

alter function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
