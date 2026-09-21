-- Repeatable CloudTMS function/view authority: weekly_source_rotation_authority_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.
--
-- Plan 6.2 Gate 6 rotation authority (interface I-1, G6-10, G6-12).
--
-- One shared owner for every Weekly Source path that must find, lock and trust
-- the genuinely current Timesheet version:
--
--   * private.weekly_source_candidate_serial_gate_v1     proof/32 section 6 step 1
--   * private.weekly_source_lock_family_rows_v1          proof/32 section 6 step 2, proof/34 section 5
--   * private.weekly_source_lock_and_resolve_families_v1 interface I-1 (the fixed signature)
--   * private.weekly_source_resolve_root_identity_v1     proof/34 section 7 (read only)
--   * private.weekly_source_root_integrity_assert_v1     proof/34 section 6 (G6-12)
--   * private.weekly_source_root_authorisation_state_v1  decision D8 root-authorisation reader
--   * private.weekly_source_managed_root_guard_v1        proof/34 section 3 "Guard owner" (G6-10)
--
-- This owner performs no business write.  It takes only the existing rotation
-- lock set and the existing Candidate serial gate; it adds no lock of its own on
-- any Banking Pay table and never creates, rotates, replaces or repoints a
-- Timesheet.  It is not literally write-free: the INSTALLED serial gate audits
-- itself, so every call through it writes one public.audit_events row on GRANTED
-- and two on BLOCKED (review F13).  That is the call-only owner's behaviour, not
-- Weekly Source state.

\set ON_ERROR_STOP on

begin;

-- proof/32 section 6 step 1.  Every Weekly Source path pins its own job type so
-- the installed helper's WORKBENCH_CANDIDATE\_% rule applies.  GRANTED is
-- required; BLOCKED is a retryable refusal; BYPASSED (or anything else) is a
-- failure.  No write has happened when this returns, so refusals are returned
-- rather than raised.  The INSTALLED gate does audit itself, so a call writes
-- one public.audit_events row on GRANTED and two on BLOCKED (review F13).
create or replace function private.weekly_source_candidate_serial_gate_v1(
  p_candidate_id uuid,
  p_job_type text,
  p_job_id uuid,
  p_reason text
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_job_types constant text[]:=array[
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',
    'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    'WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE'
  ];
  v_job_type text:=nullif(pg_catalog.btrim(coalesce(p_job_type,'')),'');
  v_gate jsonb;
  v_decision text;
begin
  -- Any job type outside the four pinned Weekly Source types is refused by this
  -- helper itself, before the installed gate is consulted, because the installed
  -- helper would return BYPASSED for it and take no lock at all.  The refusal
  -- carries the same code as a real BYPASSED so no caller has to learn a second
  -- one; the reason distinguishes the two.
  if p_candidate_id is null
     or v_job_type is null
     or not (v_job_type=any(v_allowed_job_types)) then
    return pg_catalog.jsonb_build_object(
      'ok',false,
      'code','WEEKLY_SOURCE_SERIAL_GATE_BYPASSED',
      'retryable',false,
      'gate','BYPASSED',
      'reason',case when p_candidate_id is null
        then 'CANDIDATE_REQUIRED' else 'JOB_TYPE_NOT_PINNED' end,
      'job_type',v_job_type,
      'candidate_id',p_candidate_id
    );
  end if;

  v_gate:=public._pay_workbench_candidate_serial_try_gate(
    p_job_id:=p_job_id,
    p_candidate_id:=p_candidate_id,
    p_job_type:=v_job_type,
    p_payload_json:=pg_catalog.jsonb_build_object('candidate_id',p_candidate_id::text),
    p_reason:=coalesce(nullif(pg_catalog.btrim(coalesce(p_reason,'')),''),'WEEKLY_SOURCE_ROTATION_AUTHORITY')
  );
  v_decision:=coalesce(nullif(pg_catalog.btrim(coalesce(
    v_gate->>'candidate_serial_gate_decision',''
  )),''),'UNKNOWN');

  -- A GRANTED decision is only trusted when the installed gate actually
  -- attempted and obtained the transaction-scoped advisory lock; anything else
  -- is the BYPASSED failure, never a silent pass.
  if v_decision='GRANTED'
     and coalesce((v_gate->>'advisory_lock_attempted')::boolean,false)
     and coalesce((v_gate->>'advisory_lock_granted')::boolean,false) then
    return pg_catalog.jsonb_build_object(
      'ok',true,'gate','GRANTED','candidate_id',p_candidate_id,'job_type',v_job_type
    );
  elsif v_decision='BLOCKED' then
    return pg_catalog.jsonb_build_object(
      'ok',false,
      'code','WEEKLY_SOURCE_CANDIDATE_BUSY',
      'retryable',true,
      'gate','BLOCKED',
      'candidate_id',p_candidate_id,
      'job_type',v_job_type
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',false,
    'code','WEEKLY_SOURCE_SERIAL_GATE_BYPASSED',
    'retryable',false,
    'gate',v_decision,
    'reason','GATE_DECISION_NOT_GRANTED',
    'candidate_id',p_candidate_id,
    'job_type',v_job_type
  );
end;
$function$;

-- proof/32 section 6 step 2 and proof/34 section 5: the deadlock-free lock set.
-- Families ordered by (btrim(booking_id), booking_id); trimmed advisory key
-- first, then the raw key only when it differs, then the exact raw-booking
-- family rows FOR UPDATE.  After the locks every requested row's booking_id is
-- re-read, because installed import owners can re-point booking_id (census 3.2).
-- Resolution is the installed Workbench resolver, never a second identity
-- system (proof/34 rule 12).
create or replace function private.weekly_source_lock_family_rows_v1(
  p_requested_timesheet_ids uuid[],
  p_candidate_id uuid default null
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_ids uuid[];
  v_before jsonb:='{}'::jsonb;
  v_after jsonb:='{}'::jsonb;
  v_missing_count integer;
  v_foreign_count integer;
  v_split_count integer;
  v_unresolved_count integer;
  v_family record;
  v_families jsonb;
  v_failure text;
begin
  -- Review F6: a NULL element or a duplicate must fail closed, never be dropped
  -- silently.  A caller that builds the array from a nullable variable would
  -- otherwise get ok:true and believe every root it named is locked.
  if p_requested_timesheet_ids is null
     or pg_catalog.cardinality(p_requested_timesheet_ids)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','NO_REQUESTED_ROOTS'
    );
  end if;
  if pg_catalog.array_position(p_requested_timesheet_ids,null::uuid) is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','REQUESTED_ROOT_NULL_ELEMENT'
    );
  end if;
  if (select pg_catalog.count(distinct requested.timesheet_id)
      from pg_catalog.unnest(p_requested_timesheet_ids) as requested(timesheet_id))
     <>pg_catalog.cardinality(p_requested_timesheet_ids) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','REQUESTED_ROOT_DUPLICATE'
    );
  end if;

  select pg_catalog.array_agg(requested.timesheet_id order by requested.ord)
  into v_ids
  from pg_catalog.unnest(p_requested_timesheet_ids)
    with ordinality as requested(timesheet_id,ord);

  -- Step 2 (first half): the booking identity of every requested row, read
  -- before any lock is taken so the re-read in step 3 has something to compare.
  select
    coalesce(pg_catalog.jsonb_object_agg(
      timesheet.timesheet_id::text,timesheet.booking_id
    ),'{}'::jsonb),
    pg_catalog.count(*) filter (
      where timesheet.booking_id is null
         or pg_catalog.btrim(timesheet.booking_id)=''
    )
  into v_before,v_missing_count
  from public.timesheets timesheet
  where timesheet.timesheet_id=any(v_ids);

  -- WP-08a review F1: the installed resolver returns NO row at all for a
  -- Timesheet that exists but whose booking_id is blank or whitespace-only, so
  -- a guard that only inspects returned rows never fires.  Every requested id is
  -- therefore accounted for here, explicitly, before any lock is taken.
  if v_missing_count>0
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_before))
        <>pg_catalog.array_length(v_ids,1) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','BOOKING_IDENTITY_MISSING'
    );
  end if;

  -- Every requested root must belong to the Candidate the caller pinned at the
  -- serial gate.  A root from another Candidate would be serialised under the
  -- wrong Candidate key, so it fails closed rather than proceeding.
  if p_candidate_id is not null then
    select pg_catalog.count(*)
    into v_foreign_count
    from public.timesheets timesheet
    left join public.contracts contract on contract.id=timesheet.contract_id
    where timesheet.timesheet_id=any(v_ids)
      and contract.candidate_id is distinct from p_candidate_id;
    if v_foreign_count>0 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'reason','ROOT_CANDIDATE_MISMATCH'
      );
    end if;
  end if;

  -- Step 2 (second half): the exact lock set, in the exact order.
  for v_family in
    select distinct_family.trimmed_booking_id,distinct_family.booking_id
    from (
      select distinct
        pg_catalog.btrim(timesheet.booking_id) as trimmed_booking_id,
        timesheet.booking_id as booking_id
      from public.timesheets timesheet
      where timesheet.timesheet_id=any(v_ids)
    ) distinct_family
    order by distinct_family.trimmed_booking_id,distinct_family.booking_id
  loop
    perform pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtext(v_family.trimmed_booking_id)
    );
    if v_family.booking_id<>v_family.trimmed_booking_id then
      perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(v_family.booking_id));
    end if;
    perform 1
    from public.timesheets timesheet
    where timesheet.booking_id=v_family.booking_id
    for update;
  end loop;

  -- Step 3: the booking identity must be unchanged since step 2.
  select coalesce(pg_catalog.jsonb_object_agg(
    timesheet.timesheet_id::text,timesheet.booking_id
  ),'{}'::jsonb)
  into v_after
  from public.timesheets timesheet
  where timesheet.timesheet_id=any(v_ids);

  if v_after is distinct from v_before then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','BOOKING_REPOINTED_DURING_LOCK'
    );
  end if;

  -- WP-08a review F3: the confirmed rotation route serialises on the trimmed
  -- key and the schema package keys the one-current-head index on the trimmed
  -- family booking id, but family membership in the installed resolver is by
  -- the exact raw value.  A sibling row whose btrim() matches but whose raw
  -- value differs therefore splits one logical family in two.  That is
  -- ambiguous, not resolvable, so it fails closed.  A single padded booking id
  -- with no such sibling is unaffected (the R37 case).
  --
  -- HANDOVER 2 round-5 Part E, "Rotation readings": this case is a canonical
  -- booking-reference collision and must be named as one, never described
  -- merely as a non-unique family (WP-09b handoff N2, WP-03 handoff N20).  It
  -- is a change of NAME only: the case keeps its own outcome and is never
  -- merged with blank, null, unknown or all-revoked, which keep
  -- BOOKING_IDENTITY_MISSING, ROOT_ID_REQUIRED and CANONICAL_AMBIGUOUS.
  select pg_catalog.count(*)
  into v_split_count
  from (
    select distinct requested.booking_id
    from public.timesheets requested
    where requested.timesheet_id=any(v_ids)
  ) family
  where exists(
    select 1
    from public.timesheets sibling
    where pg_catalog.btrim(sibling.booking_id)=pg_catalog.btrim(family.booking_id)
      and sibling.booking_id<>family.booking_id
  );
  if v_split_count>0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','BOOKING_REFERENCE_CANONICAL_COLLISION'
    );
  end if;

  -- Step 4 (completeness, WP-08a review F1): the resolver must return at least
  -- one row for every requested id.  Never return ok:true with fewer families
  -- than the request implies.
  select pg_catalog.count(*)
  into v_unresolved_count
  from pg_catalog.unnest(v_ids) as requested(timesheet_id)
  where not exists(
    select 1
    from public._pay_timesheet_rotation_scope(v_ids) resolved
    where resolved.requested_timesheet_id=requested.timesheet_id
  );
  if v_unresolved_count>0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','ROOT_NOT_RETURNED_BY_RESOLVER'
    );
  end if;

  -- Step 4: resolve through the installed Workbench rotation resolver only.
  with requested as (
    select input.timesheet_id,input.ord
    from pg_catalog.unnest(v_ids) with ordinality as input(timesheet_id,ord)
  ),
  scope as (
    select * from public._pay_timesheet_rotation_scope(v_ids)
  ),
  per_request as (
    select
      requested.ord as ord,
      requested.timesheet_id as requested_timesheet_id,
      pg_catalog.min(resolved.booking_id) as family_booking_id,
      pg_catalog.count(distinct resolved.booking_id) as booking_count,
      pg_catalog.min(resolved.canonical_timesheet_id::text)::uuid as canonical_timesheet_id,
      pg_catalog.count(distinct resolved.canonical_timesheet_id::text) as canonical_count,
      pg_catalog.bool_or(resolved.requested_is_canonical) as requested_is_canonical,
      pg_catalog.count(*) as member_count,
      pg_catalog.count(*) filter (where resolved.family_is_current) as current_count,
      pg_catalog.count(distinct resolved.family_version) as distinct_version_count,
      pg_catalog.count(*) filter (where resolved.family_version is null) as null_version_count,
      pg_catalog.count(*) filter (
        where resolved.family_timesheet_id=requested.timesheet_id
      ) as requested_member_count,
      pg_catalog.bool_or(
        resolved.family_timesheet_id=requested.timesheet_id
        and resolved.family_is_current
      ) as family_is_current,
      pg_catalog.count(*) filter (
        where resolved.family_timesheet_id=resolved.canonical_timesheet_id
      ) as canonical_member_count,
      pg_catalog.min(resolved.family_version) filter (
        where resolved.family_timesheet_id=resolved.canonical_timesheet_id
      ) as canonical_version,
      pg_catalog.array_agg(
        resolved.family_timesheet_id
        order by resolved.family_version,resolved.family_timesheet_id
      ) as member_timesheet_ids
    from requested
    left join scope resolved
      on resolved.requested_timesheet_id=requested.timesheet_id
    group by requested.ord,requested.timesheet_id
  ),
  checked as (
    select
      per_request.*,
      case
        when per_request.family_booking_id is null
          or per_request.booking_count<>1
          or per_request.member_count=0
          or per_request.requested_member_count<>1
          then 'FAMILY_UNRESOLVED'
        when per_request.family_booking_id is distinct from (v_before->>per_request.requested_timesheet_id::text)
          then 'BOOKING_REPOINTED_DURING_LOCK'
        when per_request.canonical_timesheet_id is null
          or per_request.canonical_count<>1
          or per_request.canonical_member_count<>1
          then 'CANONICAL_AMBIGUOUS'
        when per_request.current_count<>1 then 'CURRENT_ROW_CARDINALITY'
        when per_request.null_version_count>0
          or per_request.distinct_version_count<>per_request.member_count
          then 'VERSION_NOT_UNIQUE'
        else null
      end as failure_reason
    from per_request
  )
  select
    pg_catalog.min(checked.failure_reason),
    coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id',checked.requested_timesheet_id,
        'family_booking_id',checked.family_booking_id,
        'canonical_timesheet_id',checked.canonical_timesheet_id,
        'canonical_version',checked.canonical_version,
        'requested_is_canonical',coalesce(checked.requested_is_canonical,false),
        'family_is_current',coalesce(checked.family_is_current,false),
        'member_timesheet_ids',pg_catalog.to_jsonb(checked.member_timesheet_ids)
      ) order by checked.ord
    ) filter (where checked.failure_reason is null),'[]'::jsonb)
  into v_failure,v_families
  from checked;

  if v_failure is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason',v_failure
    );
  end if;

  -- Final completeness assertion: one family per distinct requested id, always.
  if pg_catalog.jsonb_array_length(v_families)
     is distinct from pg_catalog.array_length(v_ids,1) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','FAMILY_RESULT_INCOMPLETE'
    );
  end if;

  return pg_catalog.jsonb_build_object('ok',true,'families',v_families);
end;
$function$;

-- Interface I-1.  The fixed signature three other packages code against.
create or replace function private.weekly_source_lock_and_resolve_families_v1(
  p_candidate_id uuid,
  p_requested_timesheet_ids uuid[],
  p_job_type text,
  p_job_id uuid,
  p_reason text
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_gate jsonb;
  v_locked jsonb;
begin
  v_gate:=private.weekly_source_candidate_serial_gate_v1(
    p_candidate_id,p_job_type,p_job_id,p_reason
  );
  if coalesce((v_gate->>'ok')::boolean,false) is not true then
    return v_gate;
  end if;

  v_locked:=private.weekly_source_lock_family_rows_v1(
    p_requested_timesheet_ids,p_candidate_id
  );
  if coalesce((v_locked->>'ok')::boolean,false) is not true then
    return v_locked;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'gate','GRANTED','families',v_locked->'families'
  );
end;
$function$;

-- proof/34 section 7.  Read-only resolution of one stored or browser-supplied
-- id through the installed resolver.  Takes no lock, so it is safe inside a
-- STABLE assert.  It reports the facts; the caller decides what they mean.
create or replace function private.weekly_source_resolve_root_identity_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_result jsonb;
begin
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'reason','ROOT_ID_REQUIRED','timesheet_id',null,
      -- A null id is a "cannot tell", so bound fails CLOSED here.
      'weekly_source_bound',true,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  with scope as (
    select * from public._pay_timesheet_rotation_scope(array[p_timesheet_id]::uuid[])
  ),
  split_family as (
    -- WP-08a review F3: a sibling whose btrim(booking_id) matches but whose raw
    -- value differs splits one logical family in two.  HANDOVER 2 round-5
    -- Part E names this a canonical booking-reference collision.
    select pg_catalog.count(*) as split_count
    from public.timesheets requested
    where requested.timesheet_id=p_timesheet_id
      and exists(
        select 1
        from public.timesheets sibling
        where pg_catalog.btrim(sibling.booking_id)=pg_catalog.btrim(requested.booking_id)
          and sibling.booking_id<>requested.booking_id
      )
  ),
  aggregated as (
    select
      (select split_family.split_count from split_family) as split_count,
      pg_catalog.min(resolved.booking_id) as family_booking_id,
      pg_catalog.count(distinct resolved.booking_id) as booking_count,
      pg_catalog.min(resolved.canonical_timesheet_id::text)::uuid as canonical_timesheet_id,
      pg_catalog.count(distinct resolved.canonical_timesheet_id::text) as canonical_count,
      pg_catalog.bool_or(resolved.requested_is_canonical) as requested_is_canonical,
      pg_catalog.count(*) as member_count,
      pg_catalog.count(*) filter (where resolved.family_is_current) as current_count,
      pg_catalog.count(distinct resolved.family_version) as distinct_version_count,
      pg_catalog.count(*) filter (where resolved.family_version is null) as null_version_count,
      pg_catalog.count(*) filter (
        where resolved.family_timesheet_id=p_timesheet_id
      ) as requested_member_count,
      pg_catalog.bool_or(
        resolved.family_timesheet_id=p_timesheet_id and resolved.family_is_current
      ) as family_is_current,
      pg_catalog.min(resolved.family_version) filter (
        where resolved.family_timesheet_id=p_timesheet_id
      ) as requested_version,
      pg_catalog.count(*) filter (
        where resolved.family_timesheet_id=resolved.canonical_timesheet_id
      ) as canonical_member_count,
      pg_catalog.min(resolved.family_version) filter (
        where resolved.family_timesheet_id=resolved.canonical_timesheet_id
      ) as canonical_version,
      pg_catalog.array_agg(
        resolved.family_timesheet_id
        order by resolved.family_version,resolved.family_timesheet_id
      ) as member_timesheet_ids
    from scope resolved
  )
  select
    case
      when aggregated.family_booking_id is null
        or pg_catalog.btrim(aggregated.family_booking_id)=''
        or aggregated.booking_count<>1
        or aggregated.member_count=0
        or aggregated.requested_member_count<>1
        then pg_catalog.jsonb_build_object(
          'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','BOOKING_IDENTITY_MISSING','timesheet_id',p_timesheet_id
        )
      when coalesce(aggregated.split_count,0)>0
        then pg_catalog.jsonb_build_object(
          'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','BOOKING_REFERENCE_CANONICAL_COLLISION','timesheet_id',p_timesheet_id,
          'family_booking_id',aggregated.family_booking_id
        )
      when aggregated.canonical_timesheet_id is null
        or aggregated.canonical_count<>1
        or aggregated.canonical_member_count<>1
        then pg_catalog.jsonb_build_object(
          'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','CANONICAL_AMBIGUOUS','timesheet_id',p_timesheet_id,
          'family_booking_id',aggregated.family_booking_id
        )
      when aggregated.current_count<>1
        then pg_catalog.jsonb_build_object(
          'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','CURRENT_ROW_CARDINALITY','timesheet_id',p_timesheet_id,
          'family_booking_id',aggregated.family_booking_id
        )
      when aggregated.null_version_count>0
        or aggregated.distinct_version_count<>aggregated.member_count
        then pg_catalog.jsonb_build_object(
          'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','VERSION_NOT_UNIQUE','timesheet_id',p_timesheet_id,
          'family_booking_id',aggregated.family_booking_id
        )
      else pg_catalog.jsonb_build_object(
        'ok',true,
        'timesheet_id',p_timesheet_id,
        'family_booking_id',aggregated.family_booking_id,
        'canonical_timesheet_id',aggregated.canonical_timesheet_id,
        'canonical_version',aggregated.canonical_version,
        'requested_version',aggregated.requested_version,
        'requested_is_canonical',coalesce(aggregated.requested_is_canonical,false),
        'family_is_current',coalesce(aggregated.family_is_current,false),
        'member_timesheet_ids',pg_catalog.to_jsonb(aggregated.member_timesheet_ids)
      )
    end
  into v_result
  from aggregated;

  return coalesce(v_result,pg_catalog.jsonb_build_object(
    'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'reason','BOOKING_IDENTITY_MISSING','timesheet_id',p_timesheet_id
  ));
end;
$function$;

-- G6-12 / proof/34 section 6.  After first authorisation an unexpected rotation
-- is never a stale rebuild: it is an integrity failure that stops the operation
-- and makes no financial change.  The release owner maps this to MANUAL_REVIEW
-- (proof/32 section 4.0); every other path raises it.
create or replace function private.weekly_source_root_integrity_assert_v1(
  p_timesheet_id uuid,
  p_expected_family_booking_id text,
  p_expected_timesheet_version integer
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution jsonb;
  v_reason text;
begin
  v_resolution:=private.weekly_source_resolve_root_identity_v1(p_timesheet_id);
  if coalesce((v_resolution->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason',v_resolution->>'reason',
          'timesheet_id',p_timesheet_id
        )::text;
  end if;

  v_reason:=case
    when coalesce((v_resolution->>'requested_is_canonical')::boolean,false) is not true
      then 'ROOT_NOT_CANONICAL'
    when coalesce((v_resolution->>'family_is_current')::boolean,false) is not true
      then 'ROOT_NOT_CURRENT'
    when p_expected_family_booking_id is not null
      and v_resolution->>'family_booking_id' is distinct from p_expected_family_booking_id
      then 'FAMILY_BOOKING_CHANGED'
    when p_expected_timesheet_version is not null
      and (v_resolution->>'canonical_version')::integer is distinct from p_expected_timesheet_version
      then 'ROOT_VERSION_CHANGED'
    else null
  end;

  if v_reason is not null then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason',v_reason,
          'timesheet_id',p_timesheet_id,
          'family_booking_id',v_resolution->>'family_booking_id',
          'canonical_timesheet_id',v_resolution->>'canonical_timesheet_id',
          'canonical_version',v_resolution->>'canonical_version'
        )::text;
  end if;

  return v_resolution;
end;
$function$;

-- Decision D8 (owner, 17 September 2026).  The authorisation record is per
-- ROOT, in public.weekly_source_root_authorisations, written only by the
-- first-authorisation owner (interface I-6) after the ordinary Authorise
-- succeeds, and marked withdrawn only by the withdrawal owner.  The Weekly
-- Source lineage row is a BINDING record and carries no authorisation.
--
-- This reader is the single source of truth for "is this family's canonical
-- root authorised", used by the guard and by the lineage ensure owner.  It
-- couples to two column names only (`root_timesheet_id`, `withdrawn_at_utc`) and
-- returns the whole record as jsonb, so the relation can gain columns without
-- touching this file.  If the relation is absent the reader reports
-- `relation_present = false` and every caller fails closed.
--
-- "Currently authorised" is the INSTALLED predicate, not a new one:
-- `timesheets.authorised_at_server is not null`
--   or current `timesheets_financials.authorised_at_utc is not null`
--   or `contract_weeks.status = 'AUTHORISED'`
-- (`supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql:1708-1710`).
create or replace function private.weekly_source_root_authorisation_state_v1(
  p_canonical_timesheet_id uuid,
  p_member_timesheet_ids uuid[]
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_canonical jsonb;
  v_canonical_count integer:=0;
  v_other_count integer:=0;
  v_other_timesheet_id uuid;
  v_authorised boolean:=false;
begin
  if p_canonical_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'relation_present',false,'reason','CANONICAL_ROOT_REQUIRED'
    );
  end if;

  begin
    -- Review G6: never express safety through a LIMIT.  The live-generation
    -- partial unique index should make this exactly one row, but the reader
    -- proves it with an explicit cardinality check so a missing or disabled
    -- index cannot hand the guard an arbitrary generation.
    select pg_catalog.count(*)
    into v_canonical_count
    from public.weekly_source_root_authorisations root_authorisation
    where root_authorisation.root_timesheet_id=p_canonical_timesheet_id
      and root_authorisation.withdrawn_at_utc is null;

    if coalesce(v_canonical_count,0)>1 then
      return pg_catalog.jsonb_build_object(
        'relation_present',true,'reason','ROOT_AUTHORISATION_CARDINALITY',
        'live_on_canonical',true,'live_on_canonical_count',v_canonical_count,
        'live_on_other_member_count',0,'timesheet_currently_authorised',false,
        'authorisation',null
      );
    end if;

    select pg_catalog.to_jsonb(root_authorisation)
    into v_canonical
    from public.weekly_source_root_authorisations root_authorisation
    where root_authorisation.root_timesheet_id=p_canonical_timesheet_id
      and root_authorisation.withdrawn_at_utc is null;

    select
      pg_catalog.count(*),
      pg_catalog.min(root_authorisation.root_timesheet_id::text)::uuid
    into v_other_count,v_other_timesheet_id
    from public.weekly_source_root_authorisations root_authorisation
    where root_authorisation.root_timesheet_id=any(
            coalesce(p_member_timesheet_ids,array[]::uuid[]))
      and root_authorisation.root_timesheet_id is distinct from p_canonical_timesheet_id
      and root_authorisation.withdrawn_at_utc is null;
  exception
    when undefined_table or undefined_column then
      return pg_catalog.jsonb_build_object(
        'relation_present',false,'reason','ROOT_AUTHORISATION_RELATION_MISSING'
      );
  end;

  select
    coalesce(timesheet.authorised_at_server is not null,false)
    or coalesce(exists(
      select 1
      from public.timesheets_financials financial
      where financial.timesheet_id=timesheet.timesheet_id
        and financial.is_current
        and financial.authorised_at_utc is not null
    ),false)
    or coalesce(exists(
      select 1
      from public.contract_weeks contract_week
      -- Review G7: the installed owner takes the Contract Week by
      -- timesheet_id with NO additional_seq restriction
      -- (08082026_2035_...:1456-1458), so this reader must not add one.
      where contract_week.timesheet_id=timesheet.timesheet_id
        and pg_catalog.upper(pg_catalog.btrim(coalesce(contract_week.status::text,'')))
            ='AUTHORISED'
    ),false)
  into v_authorised
  from public.timesheets timesheet
  where timesheet.timesheet_id=p_canonical_timesheet_id;

  return pg_catalog.jsonb_build_object(
    'relation_present',true,
    'live_on_canonical',v_canonical is not null,
    'live_on_other_member_count',coalesce(v_other_count,0),
    'live_on_other_member_timesheet_id',v_other_timesheet_id,
    'timesheet_currently_authorised',coalesce(v_authorised,false),
    'authorisation',v_canonical
  );
end;
$function$;

-- G6-10 / proof/34 section 3 "Guard owner", under decision D8.
--
-- managed = true exactly when BOTH hold: a LIVE (not withdrawn) row of
-- public.weekly_source_root_authorisations exists for the family's CANONICAL
-- root, and that Timesheet is currently authorised by the installed predicate.
-- Owner ruling of 18 September 2026 (review G5): D8 governs, so a live record
-- on a Timesheet that is NOT currently authorised is managed = FALSE and the
-- contradiction is returned as evidence, never as managedness.
-- A bound-but-never-authorised root, a withdrawn root and a root with no record
-- are NOT managed, so first authorisation and re-authorisation are never
-- blocked (review F3).  A protected-pay root is covered by the same record with
-- no special case (review F7).  Requesting the guard with an OLD physical
-- member of a managed family still answers managed, because the family is
-- resolved to its canonical row first.
--
-- Where the guard cannot tell, it fails closed with managed = true and
-- ok = false, as proof/34 section 3 (HANDOVER 2 round 2, Q9) requires: an
-- unresolvable or ambiguous family, a missing authorisation relation, a live
-- record sitting on a non-canonical member (the section 6 integrity failure),
-- and more than one live record for the canonical root (review G6).
--
-- HANDOVER 2 round-8 correction C5 NARROWS that: "cannot tell" now means
-- "a relevant managed, bound or protected identity genuinely exists but its
-- family cannot be safely resolved".  An identifier that matches NOTHING --
-- no Timesheet row, no binding, no authorisation record and no protected
-- target family, which includes a NULL id -- is not a rotation collision and
-- keeps its former typed not-found / no-op outcome.  Review F9's "both cannot
-- tell cases fail in the same direction" is superseded by that ruling: both
-- now PERMIT, and the owner's own not-found handling answers.
-- weekly_source_bound is reported as evidence, never as the decision, so the
-- ROT-010 differential can tell a Weekly Source family from a structurally
-- broken ordinary one; after WP-06's change S8 it is keyed on the FAMILY
-- identity and fails closed, because a permissive failure inside a rotation
-- guard would let a rotation through that should have been refused
-- (review G9 and WP-06 review F5).
create or replace function private.weekly_source_managed_root_guard_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution jsonb;
  v_canonical_timesheet_id uuid;
  v_members uuid[];
  v_authorisation jsonb;
  v_weekly_source_bound boolean:=false;
  v_protected_state text;
  v_protected_state_count integer:=0;
  v_managed boolean;
  v_authorised boolean;
  v_contradictory boolean:=false;
  v_relevant_identity_count integer:=0;
  v_relevant_identity_present boolean:=true;
begin
  -- HANDOVER 2 round-8 correction C5 (finance approver, 18 September 2026),
  -- word for word: "For an identifier that matches no managed, bound or
  -- protected root, preserve the former typed not-found/no-op outcome; it is
  -- not a rotation collision.  Fail closed only where a relevant
  -- managed/bound/protected identity exists but its family cannot be safely
  -- resolved."
  --
  -- This is a STATE test and nothing else.  Four existence probes on the
  -- requested identifier: the Timesheet itself, a Weekly Source binding on it,
  -- a root authorisation record naming it, and a protected-pay target family
  -- naming it.  There is no caller test, no session setting, no allowlist and
  -- no current_user anywhere in this decision, so it holds identically for a
  -- direct SQL call and under `set local role service_role`.
  --
  -- A NULL identifier satisfies none of the four, so the two "matches nothing"
  -- cases -- an unknown id and a null id -- take the same branch and the
  -- calling owner then produces its own typed "Timesheet not found" /
  -- "timesheet_id is required" / no-op answer, exactly as it did before this
  -- feature was installed.  That is the correction: the guard must not convert
  -- a not-found into a rotation collision.
  --
  -- Cardinality is an explicit count, never a LIMIT and never "the unique index
  -- makes this impossible" (Part 1 addendum rule 5).  If any of the four
  -- relations cannot be read the probe fails CLOSED, because the guard then
  -- genuinely cannot tell whether a relevant identity exists.
  begin
    select pg_catalog.count(*)
    into v_relevant_identity_count
    from (
      select 1 as probe
      from public.timesheets requested
      where requested.timesheet_id=p_timesheet_id
      union all
      select 1
      from public.weekly_source_row_timesheet_lineages lineage
      where lineage.timesheet_id=p_timesheet_id
      union all
      select 1
      from public.weekly_source_root_authorisations root_authorisation
      where root_authorisation.root_timesheet_id=p_timesheet_id
      union all
      select 1
      from public.weekly_exceptional_pay_target_families family
      where family.root_timesheet_id=p_timesheet_id
    ) relevant_identity;
    v_relevant_identity_present:=coalesce(v_relevant_identity_count,0)>0;
  exception
    when undefined_table or undefined_column then
      v_relevant_identity_present:=true;
  end;

  if v_relevant_identity_present is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,
      'managed',false,
      'code','WEEKLY_SOURCE_ROOT_IDENTIFIER_MATCHES_NOTHING',
      'refusal_code',null,
      'reason',case when p_timesheet_id is null
                 then 'ROOT_ID_REQUIRED' else 'ROOT_NOT_FOUND' end,
      'timesheet_id',p_timesheet_id,
      'weekly_source_bound',false,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  -- Correction C5 leaves nothing for the former NULL-id branch to do: a null
  -- identifier matches none of the four probes above and has already returned
  -- the preserved typed outcome.  Everything from here on is an identifier for
  -- which a relevant managed, bound or protected identity genuinely exists.

  v_resolution:=private.weekly_source_resolve_root_identity_v1(p_timesheet_id);

  -- Review G9 and WP-06 review F5.  After WP-06's schema change S8 the
  -- protected-pay family is keyed on btrim(root_family_booking_id), and
  -- root_timesheet_id still names the OLD physical id after a rotation.  The
  -- rule, once: after S8 nothing may be keyed on the physical root id alone.
  -- Both probes below therefore key on the FAMILY identity, and this one is
  -- evidence that must fail CLOSED - a permissive failure inside a rotation
  -- guard would let a rotation through that should have been refused.
  -- A blank booking reference has no family identity to match on, so the two
  -- family probes below are restricted to a non-empty trimmed key and such a
  -- row can only be bound through itself.  Without that, one blank-booking row
  -- would inherit another blank-booking row's binding and an unrelated unbound
  -- ordinary family would acquire a refusal, which ruling B3 forbids.
  select exists(
    select 1
    from public.timesheets member
    join public.timesheets requested
      on requested.timesheet_id=p_timesheet_id
     and pg_catalog.btrim(requested.booking_id)<>''
     and pg_catalog.btrim(member.booking_id)=pg_catalog.btrim(requested.booking_id)
    where exists(
      select 1
      from public.weekly_source_row_timesheet_lineages lineage
      where lineage.timesheet_id=member.timesheet_id
    )
  ) or exists(
    select 1
    from public.weekly_source_row_timesheet_lineages lineage
    where lineage.timesheet_id=p_timesheet_id
  ) or exists(
    select 1
    from public.weekly_exceptional_pay_target_families family
    join public.timesheets requested
      on requested.timesheet_id=p_timesheet_id
     and pg_catalog.btrim(requested.booking_id)<>''
     and pg_catalog.btrim(family.root_family_booking_id)
         =pg_catalog.btrim(requested.booking_id)
  ) or not exists(
    -- The requested id names no Timesheet at all.  After correction C5 this
    -- limb is reached ONLY when one of the other three probes above already
    -- proved a relevant managed, bound or protected identity for that exact
    -- identifier -- a binding row, an authorisation record or a protected
    -- target family whose Timesheet is gone -- so the family identity cannot be
    -- established for something that genuinely exists.  That is precisely the
    -- case the ruling still requires to fail closed, so it reports bound.
    select 1 from public.timesheets requested
    where requested.timesheet_id=p_timesheet_id
  )
  into v_weekly_source_bound;
  v_weekly_source_bound:=coalesce(v_weekly_source_bound,true);

  if coalesce((v_resolution->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,
      'managed',true,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'refusal_code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'reason',v_resolution->>'reason',
      'timesheet_id',p_timesheet_id,
      'weekly_source_bound',v_weekly_source_bound,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  v_canonical_timesheet_id:=(v_resolution->>'canonical_timesheet_id')::uuid;
  select pg_catalog.array_agg(member.value::uuid)
  into v_members
  from pg_catalog.jsonb_array_elements_text(
    v_resolution->'member_timesheet_ids'
  ) member(value);

  v_authorisation:=private.weekly_source_root_authorisation_state_v1(
    v_canonical_timesheet_id,v_members
  );

  -- Same rule: key the protected-pay family on the family identity, and report
  -- an ambiguous or unreadable state as AMBIGUOUS rather than picking one with
  -- an aggregate.
  select
    pg_catalog.count(*),
    pg_catalog.min(family.ownership_state)
  into v_protected_state_count,v_protected_state
  from public.weekly_exceptional_pay_target_families family
  where pg_catalog.btrim(family.root_family_booking_id)
        =pg_catalog.btrim(coalesce(v_resolution->>'family_booking_id',''));
  if coalesce(v_protected_state_count,0)>1
     and (select pg_catalog.count(distinct family.ownership_state)
          from public.weekly_exceptional_pay_target_families family
          where pg_catalog.btrim(family.root_family_booking_id)
                =pg_catalog.btrim(coalesce(v_resolution->>'family_booking_id','')))>1 then
    v_protected_state:='AMBIGUOUS';
  end if;

  -- Review G6: more than one live record for the canonical root is
  -- unresolvable, so it joins the fail-closed branch below.
  if v_authorisation->>'reason'='ROOT_AUTHORISATION_CARDINALITY' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'managed',true,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'refusal_code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'reason','ROOT_AUTHORISATION_CARDINALITY',
      'timesheet_id',p_timesheet_id,
      'family_booking_id',v_resolution->>'family_booking_id',
      'canonical_timesheet_id',v_canonical_timesheet_id,
      'live_on_canonical_count',v_authorisation->'live_on_canonical_count',
      'weekly_source_bound',true,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  -- The relation WP-01c owns is not installed: nothing can be decided.
  if coalesce((v_authorisation->>'relation_present')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,'managed',true,
      'code','WEEKLY_SOURCE_ROOT_AUTHORISATION_UNAVAILABLE',
      'refusal_code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'reason',coalesce(v_authorisation->>'reason','ROOT_AUTHORISATION_RELATION_MISSING'),
      'timesheet_id',p_timesheet_id,
      'family_booking_id',v_resolution->>'family_booking_id',
      'canonical_timesheet_id',v_canonical_timesheet_id,
      'weekly_source_bound',v_weekly_source_bound,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  -- proof/34 section 6: a live authorisation on a historical member means the
  -- authorised root was rotated after first authorisation.
  if coalesce((v_authorisation->>'live_on_other_member_count')::integer,0)>0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'managed',true,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'refusal_code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
      'reason','AUTHORISED_ROOT_NOT_CANONICAL',
      'timesheet_id',p_timesheet_id,
      'family_booking_id',v_resolution->>'family_booking_id',
      'canonical_timesheet_id',v_canonical_timesheet_id,
      'authorised_timesheet_id',v_authorisation->>'live_on_other_member_timesheet_id',
      'weekly_source_bound',true,
      'authorisation_record_without_authorised_timesheet',false,
      'protected_target_ownership_state',null
    );
  end if;

  -- Decision D8, as the owner ruled on 18 September 2026 (review G5): managed is
  -- the CONJUNCTION - a live record for the canonical root AND that Timesheet
  -- currently authorised.  A live record on a Timesheet that is not currently
  -- authorised is therefore managed = FALSE, and the contradiction is reported
  -- as evidence (authorisation_record_without_authorised_timesheet), never as
  -- managedness.  The ordering rule this exposes belongs to the withdrawal
  -- owner: it must mark the record withdrawn BEFORE, or in the same statement
  -- as, anything that unauthorises the Timesheet, so the pair is never
  -- observable in the contradictory order.
  v_authorised:=coalesce(
    (v_authorisation->>'timesheet_currently_authorised')::boolean,false);
  v_contradictory:=coalesce((v_authorisation->>'live_on_canonical')::boolean,false)
    and v_authorised is not true;
  v_managed:=coalesce((v_authorisation->>'live_on_canonical')::boolean,false)
    and v_authorised;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'managed',v_managed,
    'timesheet_id',p_timesheet_id,
    'family_booking_id',v_resolution->>'family_booking_id',
    'canonical_timesheet_id',v_canonical_timesheet_id,
    'canonical_version',(v_resolution->>'canonical_version')::integer,
    'requested_is_canonical',(v_resolution->>'requested_is_canonical')::boolean,
    'family_is_current',(v_resolution->>'family_is_current')::boolean,
    'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
    'weekly_source_bound',v_weekly_source_bound,
    'protected_target_ownership_state',v_protected_state,
    'timesheet_currently_authorised',v_authorised,
    'authorisation_record_without_authorised_timesheet',v_contradictory,
    'refusal_code',case when v_managed
      then 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED' else null end,
    'authorisation',v_authorisation->'authorisation'
  );
end;
$function$;

-- Review F8.  The guard itself is owner-only, but entry point E7
-- (`public.tsfin_prepare_write`, `public.tsfin_mark_revoked`) is
-- SECURITY INVOKER and executable by `service_role`, so a guard call placed
-- inside it would raise `permission denied` for EVERY Timesheet, managed or
-- not, and break the ROT-010 differential.
--
-- This is the narrowest seam that fixes it: a definer-rights shim, owned by the
-- release owner, granted EXECUTE to `service_role` ONLY, that returns just the
-- refusal decision.  It deliberately drops `member_timesheet_ids`,
-- `canonical_timesheet_id`, `family_booking_id`, `authorisation` and every
-- other projection the guard returns, so an invoker-rights caller learns
-- exactly one thing: whether it must refuse, and why.  `anon` and
-- `authenticated` keep no execute at all, and the shim reads nothing they
-- could not already reach through the entry point they are calling.
--
-- WP-09: call THIS from `public.tsfin_prepare_write` and
-- `public.tsfin_mark_revoked`; call `private.weekly_source_managed_root_guard_v1`
-- directly from every SECURITY DEFINER entry point.
create or replace function private.weekly_source_managed_root_guard_decision_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_guard jsonb;
begin
  v_guard:=private.weekly_source_managed_root_guard_v1(p_timesheet_id);
  -- WP-09b handoff N1, under HANDOVER 2 round-5 ruling B3.  The narrowed
  -- refusal predicate every call site now uses reads three more decision inputs
  -- than the first five keys carried:
  --
  --   (coalesce(managed,true) and (coalesce(ok,true) or coalesce(weekly_source_bound,true)))
  --   or coalesce(authorisation_record_without_authorised_timesheet,false)
  --   or protected_target_ownership_state is not null
  --
  -- Entry point E7 is the only SECURITY INVOKER owner in the census, so it is
  -- the only site that reads the decision through this shim.  While the shim
  -- omitted them the predicate took the unsafe value for each, and E7 refused a
  -- malformed but UNBOUND ordinary family that every definer-rights site
  -- permits -- nine of the nineteen remaining ordinary-outcome changes.  The
  -- ruling is explicit that an unrelated unbound ordinary family must not
  -- acquire a new refusal merely because this feature was installed.
  --
  -- These are decision INPUTS, not identities: no member ids, no canonical id,
  -- no booking reference and no authorisation row cross this boundary, so the
  -- shim still discloses only the refusal decision.  weekly_source_bound is
  -- therefore load-bearing for a money-adjacent decision now, not evidence, and
  -- the guard derives it from the FAMILY identity and fails CLOSED when that
  -- identity cannot be established (review G9, WP-06 review F5).
  return pg_catalog.jsonb_build_object(
    'ok',(v_guard->>'ok')::boolean,
    'managed',(v_guard->>'managed')::boolean,
    'refusal_code',v_guard->>'refusal_code',
    'reason',v_guard->>'reason',
    'timesheet_id',p_timesheet_id,
    'weekly_source_bound',(v_guard->>'weekly_source_bound')::boolean,
    'authorisation_record_without_authorised_timesheet',
      (v_guard->>'authorisation_record_without_authorised_timesheet')::boolean,
    'protected_target_ownership_state',v_guard->>'protected_target_ownership_state'
  );
end;
$function$;

alter function private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)
  owner to postgres;
alter function private.weekly_source_managed_root_guard_decision_v1(uuid)
  owner to postgres;
alter function private.weekly_source_lock_family_rows_v1(uuid[],uuid) owner to postgres;
alter function private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)
  owner to postgres;
alter function private.weekly_source_resolve_root_identity_v1(uuid) owner to postgres;
alter function private.weekly_source_root_integrity_assert_v1(uuid,text,integer)
  owner to postgres;
alter function private.weekly_source_root_authorisation_state_v1(uuid,uuid[])
  owner to postgres;
alter function private.weekly_source_managed_root_guard_v1(uuid) owner to postgres;

revoke all on function private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_lock_family_rows_v1(uuid[],uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_resolve_root_identity_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_root_integrity_assert_v1(uuid,text,integer)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_root_authorisation_state_v1(uuid,uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_managed_root_guard_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_managed_root_guard_decision_v1(uuid)
  from public,anon,authenticated,service_role;
-- Review F8: the ONLY rotation-authority routine `service_role` may execute, so
-- the SECURITY INVOKER entry point E7 can refuse a managed root.
grant execute on function private.weekly_source_managed_root_guard_decision_v1(uuid)
  to service_role;

comment on function private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text) is
  'Plan 6.2 interface I-1. Callers must match families by requested_timesheet_id, never by array index; a NULL element or a duplicate in p_requested_timesheet_ids is refused, not dropped. Candidate serial gate with a pinned Weekly Source job type, then the deadlock-free rotation lock set (trimmed key, raw key when different, raw-booking family rows FOR UPDATE, families ordered by (btrim(booking_id), booking_id)), then a booking re-point check and resolution through public._pay_timesheet_rotation_scope. Writes nothing and takes no Banking Pay lock.';
comment on function private.weekly_source_managed_root_guard_v1(uuid) is
  'Plan 6.2 G6-10 rotation guard (proof/34 section 3, decision D8), as corrected by HANDOVER 2 round-8 correction C5. managed = true exactly when BOTH hold: a live public.weekly_source_root_authorisations row exists for the family canonical root, and that Timesheet is currently authorised. A live record on a Timesheet that is not currently authorised is managed = false, and the contradiction is returned as the evidence field authorisation_record_without_authorised_timesheet. Correction C5: an identifier that matches NO managed, bound or protected root - no Timesheet row, no Weekly Source binding, no root authorisation record and no protected target family, which includes a NULL identifier - is NOT a rotation collision; it returns managed = false with refusal_code null so the calling owner produces its former typed not-found/no-op outcome. The guard fails closed - managed = true with ok = false, so the entry point refuses - only where a relevant managed, bound or protected identity genuinely exists but its family cannot be safely resolved: an unresolvable or ambiguous family, an unreadable authorisation relation, a live record on a non-canonical member, more than one live record, or a live record whose Timesheet is not authorised. weekly_source_bound is evidence, never the decision.';

commit;
