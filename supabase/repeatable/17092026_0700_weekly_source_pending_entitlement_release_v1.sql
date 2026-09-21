-- Repeatable CloudTMS authority: weekly_source_pending_entitlement_release_v1
--
-- The Weekly Source pending-publication owners (Gate 5, items G5-4 to G5-7):
-- one PENDING bundle per accepted Office decision that could not activate
-- because a member root is frozen by Banking Pay, the bounded claim page the
-- delivery tick uses, and the apply owner that revalidates and then calls the
-- SAME head-publication coordinator the immediate path calls.
--
-- Pack authority, read word for word before this file was written:
--   P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md
--     sections 2 (owner identity), 3 (wake-up contract), 7 (revalidation),
--     8 (release transaction), 10 (bounds and terminal states) and
--     11 (forbidden).
--   P:\24_CROSS_SYSTEM_SOURCE_PAY_INVOICE_AMENDMENT_AUTHORITY.md section 4.4
--     (the pending decision, the unchanged Draft and the one bounded stale
--     warning through the existing banking_pay_batch_signal_touch owner).
--   HANDOVER 2 round-4 rulings, preserved at
--   ..\plan6-pack-audit-20260916\HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R4.md:
--     ruling 3 (a `WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` item is an ordinary
--     FROZEN result and consumes no technical-failure budget), ruling 4 (a
--     `VOIDED` transfer stays CENSUS_ERROR and reaches Office manual review;
--     Weekly Source adds no interpretation of its own) and ruling 6 point 7
--     (a rolled-back release transaction is a technical failure for that bundle
--     and leaves it PENDING or reclaimable, never half-released).
--
-- Interfaces:
--   I-5  private.weekly_source_pending_entitlement_bundle_save_v1 - built here,
--        called by WP-02's immediate entry point when the census is FROZEN.
--   I-1  private.weekly_source_lock_and_resolve_families_v1       - WP-03.
--   I-2  private.weekly_source_freeze_census_v1                   - WP-08a.
--   I-4  private.weekly_source_entitlement_publish_core_v1        - WP-02.
--   I-3  the request shape, IMPL\interfaces\PUBLICATION_REQUEST_SHAPE.md.
-- plpgsql resolves called functions at run time, so this file compiles before
-- any of them exists.
--
-- What this owner never does (proof/32 section 11, asserted by the verifier
-- against pg_get_functiondef of every function defined here): it calculates no
-- residual; it creates no pay, recovery, Draft, Case, provider or settlement
-- row; it reads no C1 staging or checkpoint table as authority; it never
-- mutates, rotates, replaces, unauthorises, reauthorises or rebuilds a public
-- Timesheet or current TSFIN; it never trusts a stored timesheet_id without
-- resolving its family through I-1; it never infers an unknown provider result;
-- it never releases by elapsed time or attempt count; it adds and edits no
-- Banking Pay definition; it holds no lock on a Banking Pay table; and it
-- accepts no actor and no timestamp from the caller.
--
-- The only Banking Pay call anywhere in this file is the informational
-- `public.banking_pay_batch_signal_touch`, which 24 section 4.4 permits by name
-- and which writes only `public.banking_pay_batch_change_signals`.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- 0. Bounds and small helpers
-- ---------------------------------------------------------------------------
-- proof/32 section 10.  The numbers live in one place so the claim page, the
-- apply owner and the verifier cannot drift apart.
--   page size      <= 25 bundles per tick
--   lease          30..120 seconds
--   ordinary retry 60 seconds (one delivery tick)
--   backoff        exponential from the ordinary retry, capped at 15 minutes
--   manual review  ten CONSECUTIVE technical failures
create or replace function private.weekly_source_pending_release_backoff_v1(
  p_failure_count integer
) returns interval
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select least(
    pg_catalog.make_interval(mins=>15),
    pg_catalog.make_interval(
      secs=>60::double precision
            * pg_catalog.power(
                2::double precision,
                (least(greatest(coalesce(p_failure_count,1),1),10)-1)
                  ::double precision)));
$function$;

-- ---------------------------------------------------------------------------
-- 0a. HANDOVER 2 round-5 ruling B4.1 - what may spend the retry budget
-- ---------------------------------------------------------------------------
-- "Every permanent refusal goes immediately to Office manual review. This
-- includes digest disagreement and any fixed structural/integrity refusal. Only
-- genuinely transient technical errors consume the retry budget."
--
-- This reverses the behaviour WP-08b shipped and WP-08b's review measured as
-- finding F4: three named codes escalated immediately and EVERY other refusal
-- spent ten attempts and about ninety minutes of exponential backoff on its way
-- to a human, including refusals that could never succeed
-- (`APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION`,
-- `WEEK_OR_CONTRACT_DISAGREES_WITH_ACCEPTED_DECISION`, every `CENSUS_ERROR`).
--
-- The default is now the opposite: a refusal this function cannot PROVE is
-- transient is permanent, and permanent means a human sees it on this tick.
-- Nothing is released either way; the budget only decides how long a decision
-- that needs Office attention sits in a queue instead of on a screen.
--
-- Transient is proved by exactly two things and nothing else:
--
--   1. the refusing owner stamped `retryable` BOOLEAN TRUE on its own refusal.
--      Read three-valued (Part 1 review rule 4): absent, JSON null, the STRING
--      "true" and any non-boolean are all "not proved transient".  In the
--      installed owners only the serial gate's BLOCKED return carries it, and
--      that one is handled earlier as a no-budget skip, so this arm is a
--      forward-compatible contract rather than a live path today;
--   2. a SQLSTATE, or a bounded worker-observed failure kind, from the list
--      below.  These arrive only through the round-4 ruling 6 point 7 recorder,
--      because a transient database fault inside the release transaction RAISES
--      and rolls the whole transaction back rather than returning a code.
--
-- The worker reports the SQLSTATE and the failure kind as FACTS; it classifies
-- nothing (`proof/32` section 2: "this module decides nothing").  A rollback
-- whose SQLSTATE is absent or unlisted - `23514`, `42883`, `P0001`, anything -
-- is permanent, which is the ruling's stated default.
create or replace function private.weekly_source_pending_release_transient_v1(
  p_code text,
  p_detail jsonb,
  p_sqlstate text
) returns boolean
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  -- Exact SQLSTATEs that PostgreSQL itself defines as a condition that can
  -- clear on its own: serialisation, deadlock, lock/object contention, a
  -- cancelled statement, shutdown and "cannot connect now".
  v_transient_sqlstates constant text[]:=array[
    '40001',  -- serialization_failure
    '40P01',  -- deadlock_detected
    '55006',  -- object_in_use
    '55P03',  -- lock_not_available
    '57014',  -- query_canceled
    '57P01',  -- admin_shutdown
    '57P02',  -- crash_shutdown
    '57P03',  -- cannot_connect_now
    '58030'   -- io_error
  ];
  -- Whole SQLSTATE classes that are transient by definition: 08 connection
  -- exception, 53 insufficient resources.
  v_transient_classes constant text[]:=array['08','53'];
  -- The bounded facts the Worker may report about a failure it saw from
  -- outside the database.  A client-side timeout and a dropped connection are
  -- real transient conditions that carry no SQLSTATE at all, and WP-08b's
  -- review finding F6 is explicit that a 30-second client timeout is not proof
  -- that the server transaction rolled back - so the bundle must be retried,
  -- not sent to a human.
  v_transient_kinds constant text[]:=array['TIMEOUT','NETWORK'];
  v_sqlstate text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_sqlstate,'')));
  v_kind text;
begin
  -- 1. the refusing owner's own three-valued retryability stamp.
  if pg_catalog.jsonb_typeof(coalesce(p_detail->'retryable','null'::jsonb))='boolean'
     and coalesce((p_detail->>'retryable')::boolean,false) is true then
    return true;
  end if;

  -- 2a. a bounded worker-observed failure kind.
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(
    (pg_catalog.regexp_match(coalesce(p_sqlstate,'')||' '||coalesce(p_code,''),
                             'KIND=([A-Za-z_]+)','i'))[1],'')));
  if v_kind=any(v_transient_kinds) then
    return true;
  end if;

  -- 2b. the SQLSTATE itself.  A five-character code, or the same code carried
  -- inside a bounded `sqlstate=XXXXX` token.
  if v_sqlstate !~ '^[0-9A-Z]{5}$' then
    v_sqlstate:=pg_catalog.upper(pg_catalog.btrim(coalesce(
      (pg_catalog.regexp_match(coalesce(p_sqlstate,''),
                               'SQLSTATE=([0-9A-Za-z]{5})','i'))[1],'')));
  end if;
  if v_sqlstate ~ '^[0-9A-Z]{5}$' then
    if v_sqlstate=any(v_transient_sqlstates)
       or pg_catalog.left(v_sqlstate,2)=any(v_transient_classes) then
      return true;
    end if;
  end if;

  -- Everything else, including an absent SQLSTATE, is permanent.  Ruling B4.1:
  -- "the default for a refusal you cannot prove is transient must be immediate
  -- escalation, not the budget."
  return false;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 0b. HANDOVER 2 round-5 ruling B4.2 - the material census signature
-- ---------------------------------------------------------------------------
-- "Do not append an audit row for every frozen tick. Record state transitions
-- and maintain bounded current-state counters/timestamps. Repeated unchanged
-- frozen polls may update one bounded status record or operational metric, but
-- may not grow an audit table forever."
--
-- To know that a poll is UNCHANGED, the owner needs a value that is equal for
-- two censuses that say the same thing and different for two that do not.  The
-- census's own `evaluated_at_utc` moves on every call, so the whole blob cannot
-- be compared; and `proof` and `predicates` carry evidence FOR the verdict
-- rather than the verdict itself.  The signature is therefore built from the
-- material facts only, each one explicitly named:
--
--   * `result`   - RELEASABLE / FROZEN / CENSUS_ERROR;
--   * `reason`   - the census's own reason for that result;
--   * `class_counts` - the per-class totals;
--   * every item's (class, reason, pay_batch_item_id, timesheet_id), sorted;
--   * every error's (scope, code), sorted;
--   * the expanded member set the census actually evaluated, sorted.
--
-- Anything not in this list cannot make an unchanged freeze look changed, and
-- nothing in this list can change without the signature changing.  The sort
-- makes it independent of the census's own row order.  It decides only whether
-- to keep waiting or to run a full attempt; it never decides a release.
create or replace function private.weekly_source_pending_release_census_signature_v1(
  p_census jsonb
) returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'result',coalesce(p_census->>'result',''),
      'reason',coalesce(p_census->>'reason',''),
      'class_counts',coalesce(p_census->'class_counts','{}'::jsonb),
      'items',coalesce((
        select pg_catalog.jsonb_agg(item.line order by item.line)
          from (
            select pg_catalog.jsonb_build_array(
                     coalesce(element.value->>'class',''),
                     coalesce(element.value->>'reason',''),
                     coalesce(element.value->>'pay_batch_item_id',''),
                     coalesce(element.value->>'timesheet_id','')) as line
              from pg_catalog.jsonb_array_elements(
                     case when pg_catalog.jsonb_typeof(coalesce(p_census->'items','[]'::jsonb))='array'
                          then p_census->'items' else '[]'::jsonb end) as element(value)
          ) as item),'[]'::jsonb),
      'errors',coalesce((
        select pg_catalog.jsonb_agg(err.line order by err.line)
          from (
            select pg_catalog.jsonb_build_array(
                     coalesce(element.value->>'scope',''),
                     coalesce(element.value->>'code','')) as line
              from pg_catalog.jsonb_array_elements(
                     case when pg_catalog.jsonb_typeof(coalesce(p_census->'errors','[]'::jsonb))='array'
                          then p_census->'errors' else '[]'::jsonb end) as element(value)
          ) as err),'[]'::jsonb),
      'members',coalesce((
        select pg_catalog.jsonb_agg(member.value order by member.value)
          from pg_catalog.jsonb_array_elements_text(
                 case when pg_catalog.jsonb_typeof(
                             coalesce(p_census->'expanded_member_timesheet_ids','[]'::jsonb))='array'
                      then p_census->'expanded_member_timesheet_ids' else '[]'::jsonb end)
               as member(value)),'[]'::jsonb)
    )::text,'UTF8')),'hex');
$function$;

-- ---------------------------------------------------------------------------
-- 1. Lock, integrity gate and census, in the proof/32 section 6 order
-- ---------------------------------------------------------------------------
-- Used by the apply owner.  Steps, exactly:
--   1. interface I-1 - the Candidate serial gate with the PINNED job type
--      WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE (section 6 step 1), then
--      the rotation lock set (trimmed key, then the raw key only when it
--      differs, then the family rows FOR UPDATE) and the family resolution;
--   2. the section 4.0 integrity gate: EVERY stored root must still be the
--      canonical current row of its family at the stored version, and its
--      family booking identity must still be the stored one.  Every stored id
--      is accounted for explicitly - a family returned for an id we did not ask
--      about, or a stored id with no family, is an integrity failure, never a
--      silently dropped member;
--   3. interface I-2 over EVERY physical member of EVERY family (never only the
--      canonical rows), which takes no row lock on any Banking Pay table.
create or replace function private.weekly_source_pending_release_lock_and_census_v1(
  p_candidate_id uuid,
  p_member_root_ids uuid[],
  p_member_family_booking_ids text[],
  p_member_root_versions integer[],
  p_worker_run_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_lock_result jsonb;
  v_family jsonb;
  v_family_count integer;
  v_index integer;
  v_count integer;
  v_member_timesheet_ids uuid[]:=array[]::uuid[];
  v_census jsonb;
  v_failures jsonb:='[]'::jsonb;
begin
  if p_candidate_id is null
     or coalesce(pg_catalog.cardinality(p_member_root_ids),0)<1
     or pg_catalog.cardinality(p_member_root_ids)
        <>pg_catalog.cardinality(p_member_family_booking_ids)
     or pg_catalog.cardinality(p_member_root_ids)
        <>pg_catalog.cardinality(p_member_root_versions) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'stage','REQUEST','retryable',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      'reason','MEMBER_ARRAYS_MISALIGNED');
  end if;

  -- proof/32 section 6 step 1 and interface I-1.  The job id is derived from
  -- the Worker run, exactly as section 6 requires; no actor and no timestamp is
  -- taken from the caller.
  v_lock_result:=private.weekly_source_lock_and_resolve_families_v1(
    p_candidate_id,
    p_member_root_ids,
    'WORKBENCH_CANDIDATE_PENDING_ENTITLEMENT_RELEASE',
    coalesce(p_worker_run_id,pg_catalog.gen_random_uuid()),
    'WEEKLY_SOURCE_PENDING_RELEASE');
  if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
    return v_lock_result||pg_catalog.jsonb_build_object('stage','LOCK');
  end if;

  -- Section 4.0 integrity gate, id by id.  Both directions are checked so that
  -- neither a missing family nor an unexpected extra family can pass.
  v_count:=pg_catalog.jsonb_array_length(coalesce(v_lock_result->'families','[]'::jsonb));
  if v_count is distinct from pg_catalog.cardinality(p_member_root_ids) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'stage','INTEGRITY','retryable',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'reason','FAMILY_COUNT_MISMATCH',
      'expected',pg_catalog.cardinality(p_member_root_ids),'actual',v_count);
  end if;

  for v_index in 1..pg_catalog.cardinality(p_member_root_ids) loop
    select pg_catalog.count(*),pg_catalog.min(family_element.value::text)::jsonb
      into v_family_count,v_family
      from pg_catalog.jsonb_array_elements(v_lock_result->'families') as family_element(value)
     where (family_element.value->>'requested_timesheet_id')::uuid=p_member_root_ids[v_index];
    if coalesce(v_family_count,0)<>1 or v_family is null then
      -- Every stored id is accounted for explicitly, in both directions: no
      -- family, or more than one family, for one stored root is a failure.
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','FAMILY_NOT_RETURNED_EXACTLY_ONCE',
        'family_count',coalesce(v_family_count,0)));
      continue;
    end if;
    -- Three-valued logic: every one of these comparisons is written so that a
    -- NULL on either side FAILS the gate rather than passing it.
    if coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true then
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','ROOT_NOT_CANONICAL'));
    elsif coalesce((v_family->>'family_is_current')::boolean,false) is not true then
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','FAMILY_NOT_CURRENT'));
    elsif (v_family->>'canonical_timesheet_id')::uuid is distinct from p_member_root_ids[v_index] then
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','CANONICAL_ROOT_CHANGED',
        'canonical_timesheet_id',v_family->'canonical_timesheet_id'));
    elsif (v_family->>'canonical_version')::integer is distinct from p_member_root_versions[v_index] then
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','ROOT_VERSION_CHANGED',
        'stored_version',p_member_root_versions[v_index],
        'canonical_version',v_family->'canonical_version'));
    elsif pg_catalog.btrim(coalesce(v_family->>'family_booking_id',pg_catalog.chr(1)))
          is distinct from pg_catalog.btrim(coalesce(p_member_family_booking_ids[v_index],pg_catalog.chr(2))) then
      -- H2-035 compares the trimmed form, because the trimmed value is the
      -- advisory-lock key that protects the uniqueness rule.
      v_failures:=v_failures||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'member_ordinal',v_index,'member_root_id',p_member_root_ids[v_index],
        'reason','FAMILY_BOOKING_CHANGED'));
    end if;
  end loop;

  if pg_catalog.jsonb_array_length(v_failures)>0 then
    -- proof/34 section 6 and proof/32 section 4.0: after first authorisation an
    -- unexpected rotation is an INTEGRITY FAILURE, never a stale rebuild.
    return pg_catalog.jsonb_build_object(
      'ok',false,'stage','INTEGRITY','retryable',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'reason','ROOT_ROTATED_AFTER_AUTHORISATION',
      'failures',v_failures);
  end if;

  -- Interface I-2 takes EVERY physical member of EVERY family, never only the
  -- canonical rows (proof/32 section 4.0; I-3 section 7.3 step 3).
  select coalesce(pg_catalog.array_agg(distinct member_element.value::uuid),
                             array[]::uuid[])
    into v_member_timesheet_ids
    from pg_catalog.jsonb_array_elements(v_lock_result->'families') as family_element(value)
   cross join lateral pg_catalog.jsonb_array_elements_text(
           coalesce(family_element.value->'member_timesheet_ids','[]'::jsonb))
           as member_element(value);

  if coalesce(pg_catalog.cardinality(v_member_timesheet_ids),0)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'stage','INTEGRITY','retryable',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'reason','NO_FAMILY_MEMBERS_RESOLVED');
  end if;

  v_census:=private.weekly_source_freeze_census_v1(p_candidate_id,v_member_timesheet_ids);

  return pg_catalog.jsonb_build_object(
    'ok',true,'stage','CENSUS',
    'lock_result',v_lock_result,
    'member_timesheet_ids',pg_catalog.to_jsonb(v_member_timesheet_ids),
    'census',v_census);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 2. Interface I-5 - save exactly one PENDING bundle
-- ---------------------------------------------------------------------------
-- 24 section 4.4: the decision is saved as pending, the previous effective
-- entitlement remains current, and the existing Draft and all frozen evidence
-- remain unchanged.  Saving may send ONLY the existing bounded stale warning to
-- an affected active Draft through the existing banking_pay_batch_signal_touch
-- owner; that signal is informational and must not alter frozen items, amounts,
-- reservations or payment state.
--
-- Idempotent on the request digest: a second save for the same
-- (decision_bundle_id, bundle_revision) recomputes the digest against the row
-- that already exists and returns it unchanged when they agree.  The digest
-- covers publication_mode = 'DEFERRED' and the pending bundle's own id
-- (I-3 section 0), which is why the id is allocated before the digest and why
-- the comparison is done against the stored row's id, never against a fresh one.
create or replace function private.weekly_source_pending_entitlement_bundle_save_v1(
  p_request jsonb,
  p_lock_result jsonb,
  p_census jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_pending_bundle_id uuid:=pg_catalog.gen_random_uuid();
  v_canonical jsonb;
  v_digest bytea;
  v_existing_canonical jsonb;
  v_existing_digest bytea;
  v_source_revision_digest bytea;
  v_contract_choice_digest bytea;
  v_before_inventory_digest bytea;
  v_acceptance_digest bytea;
  v_err_message text;
  v_err_detail text;
  v_bundle record;
  v_existing record;
  v_live record;
  v_decision_bundle_id uuid;
  v_bundle_revision bigint;
  v_candidate_id uuid;
  v_decision_id uuid;
  v_member_root_ids uuid[];
  v_member_family_booking_ids text[];
  v_member_root_versions integer[];
  v_head_ids uuid[];
  v_now timestamptz;
  v_signalled jsonb:='[]'::jsonb;
  v_signal_batch uuid;
  v_signal_error text;
  v_signal_count integer:=0;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  -- The save is correct for exactly one census outcome.  A RELEASABLE census
  -- should have published; a CENSUS_ERROR must not be parked as a decision.
  if pg_catalog.jsonb_typeof(coalesce(p_census,'null'::jsonb))<>'object'
     or coalesce(p_census->>'result','')<>'FROZEN' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_PENDING_BUNDLE_CENSUS_NOT_FROZEN',
      'detail',pg_catalog.jsonb_build_object(
        'census_result',coalesce(p_census->>'result','<null>')));
  end if;
  if coalesce((p_lock_result->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'detail',pg_catalog.jsonb_build_object('reason','LOCK_RESULT_NOT_OK'));
  end if;

  -- The one canonical encoder, with the coordinator's own mode and this
  -- bundle's own id (I-3 section 0; proof/32 section 9).
  begin
    v_canonical:=private.weekly_source_publication_request_canonical_v1(
      p_request,'DEFERRED',v_pending_bundle_id);
    v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical);
  exception when invalid_parameter_value then
    get stacked diagnostics v_err_message=message_text, v_err_detail=pg_exception_detail;
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code',v_err_message,
      'detail',case when coalesce(v_err_detail,'') ~ '^\{' then v_err_detail::jsonb
                    else pg_catalog.to_jsonb(v_err_detail) end);
  end;

  v_decision_bundle_id:=(v_canonical->>'decision_bundle_id')::uuid;
  v_bundle_revision:=(v_canonical->>'bundle_revision')::bigint;
  v_candidate_id:=(v_canonical->>'candidate_id')::uuid;
  v_decision_id:=(v_canonical->>'decision_id')::uuid;
  select pg_catalog.array_agg(element.value::uuid order by element.ordinality)
    into v_member_root_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_ids')
         with ordinality as element(value,ordinality);
  select pg_catalog.array_agg(element.value order by element.ordinality)
    into v_member_family_booking_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_family_booking_ids')
         with ordinality as element(value,ordinality);
  select pg_catalog.array_agg(element.value::integer order by element.ordinality)
    into v_member_root_versions
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_versions')
         with ordinality as element(value,ordinality);
  select pg_catalog.array_agg(element.value::uuid order by element.ordinality)
    into v_head_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'head_ids')
         with ordinality as element(value,ordinality);

  -- The actor is taken from the immutable accepted decision, never from the
  -- caller (proof/32 section 2).
  select * into v_bundle
    from public.weekly_source_entitlement_decision_bundles as bundle_row
   where bundle_row.decision_bundle_id=v_decision_bundle_id
     and bundle_row.bundle_revision=v_bundle_revision
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_FOUND',
        'decision_bundle_id',v_decision_bundle_id,'bundle_revision',v_bundle_revision));
  end if;
  if v_bundle.state not in ('PROPOSED','COMMITTED') then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_LIVE',
        'state',v_bundle.state));
  end if;
  if v_bundle.candidate_id<>v_candidate_id
     or v_bundle.decision_id<>v_decision_id
     or v_bundle.proposed_head_ids is distinct from v_head_ids
     or (v_bundle.bundle_kind='SINGLE_ROOT')<>(pg_catalog.cardinality(v_member_root_ids)=1)
     or (v_bundle.bundle_kind='CROSS_CONTRACT_A_B')<>(pg_catalog.cardinality(v_member_root_ids)=2) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_DISAGREES_WITH_REQUEST'));
  end if;

  -- The four approval digests the accepted decision carries, recomputed with
  -- the ONE canonical encoder and compared exactly as the coordinator compares
  -- them.  This is the coordinator's own rule, applied earlier and only to
  -- refuse: a request whose approval digests already disagree can never be
  -- released, so parking it would spend ten technical failures to reach a
  -- MANUAL_REVIEW that is knowable now.  It can never cause a release.
  v_source_revision_digest:=private.weekly_source_publication_request_digest_v1(
    coalesce(v_canonical->'financial_request'->'source_revision','null'::jsonb));
  v_contract_choice_digest:=private.weekly_source_publication_request_digest_v1(
    coalesce(v_canonical->'financial_request'->'contract_choices','null'::jsonb));
  v_before_inventory_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_before_inventory_v1(
      coalesce(p_request->'control','{}'::jsonb),pg_catalog.cardinality(v_member_root_ids)));
  -- The acceptance digest is the canonical request in IMMEDIATE mode with no
  -- pending bundle: publication_mode and pending_bundle_id are digest fields,
  -- so the accepted decision cannot carry the deferred value.
  v_acceptance_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(p_request,'IMMEDIATE',null::uuid));
  if v_bundle.source_revision_digest is distinct from v_source_revision_digest
     or v_bundle.contract_choice_digest is distinct from v_contract_choice_digest
     or v_bundle.before_inventory_digest is distinct from v_before_inventory_digest
     or v_bundle.request_digest is distinct from v_acceptance_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'created',false,'retryable',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      'detail',pg_catalog.jsonb_build_object(
        'reason','APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION',
        'request_digest_matches',v_bundle.request_digest=v_acceptance_digest,
        'source_revision_digest_matches',v_bundle.source_revision_digest=v_source_revision_digest,
        'contract_choice_digest_matches',v_bundle.contract_choice_digest=v_contract_choice_digest,
        'before_inventory_digest_matches',
          v_bundle.before_inventory_digest=v_before_inventory_digest));
  end if;

  -- Idempotence, on the request digest, against the row that already exists.
  select * into v_existing
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.decision_bundle_id=v_decision_bundle_id
     and pending_row.bundle_revision=v_bundle_revision
   for update;
  if found then
    v_existing_canonical:=private.weekly_source_publication_request_canonical_v1(
      p_request,'DEFERRED',v_existing.id);
    v_existing_digest:=private.weekly_source_publication_request_digest_v1(v_existing_canonical);
    if v_existing_digest is distinct from v_existing.request_digest then
      return pg_catalog.jsonb_build_object(
        'ok',false,'created',false,'retryable',false,
        'code','WEEKLY_SOURCE_PENDING_BUNDLE_CONFLICT',
        'detail',pg_catalog.jsonb_build_object(
          'pending_bundle_id',v_existing.id,
          'stored_request_digest',pg_catalog.encode(v_existing.request_digest,'hex'),
          'recomputed_request_digest',pg_catalog.encode(v_existing_digest,'hex')));
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'created',false,'replayed',true,
      'pending_bundle_id',v_existing.id,
      'state',v_existing.state,
      'pending_revision',v_existing.pending_revision,
      'request_digest',pg_catalog.encode(v_existing.request_digest,'hex'),
      'next_check_at_utc',v_existing.next_check_at_utc,
      'released_receipt_id',v_existing.released_receipt_id);
  end if;

  -- proof/32 section 10: a newer Office decision or source revision supersedes
  -- the bundle.  An older revision is never resurrected behind a newer one.
  select * into v_live
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.decision_bundle_id=v_decision_bundle_id
     and pending_row.state in ('PENDING','RELEASING')
   for update;
  if found then
    if v_live.bundle_revision>=v_bundle_revision then
      return pg_catalog.jsonb_build_object(
        'ok',false,'created',false,'retryable',false,
        'code','WEEKLY_SOURCE_PENDING_BUNDLE_SUPERSEDED',
        'detail',pg_catalog.jsonb_build_object(
          'live_pending_bundle_id',v_live.id,
          'live_bundle_revision',v_live.bundle_revision,
          'requested_bundle_revision',v_bundle_revision));
    end if;
    update public.weekly_source_pending_entitlement_bundles
       set state='SUPERSEDED',
           pending_revision=pending_revision+1,
           lease_owner=null,lease_token=null,lease_worker_run_id=null,
           lease_expires_at_utc=null,
           next_check_at_utc=pg_catalog.clock_timestamp(),
           updated_at_utc=pg_catalog.clock_timestamp()
     where id=v_live.id;
  end if;

  v_now:=pg_catalog.clock_timestamp();
  insert into public.weekly_source_pending_entitlement_bundles(
    id,decision_bundle_id,bundle_revision,candidate_id,
    member_root_ids,member_family_booking_ids,member_root_versions,
    request_digest,source_revision_digest,contract_choice_digest,
    decision_id,decided_by_user_id,proposed_head_ids,
    pending_revision,state,next_check_at_utc,technical_failure_count,
    last_census_json,request_json,created_at_utc,updated_at_utc
  ) values (
    v_pending_bundle_id,v_decision_bundle_id,v_bundle_revision,v_candidate_id,
    v_member_root_ids,v_member_family_booking_ids,v_member_root_versions,
    v_digest,v_source_revision_digest,v_contract_choice_digest,
    v_decision_id,v_bundle.decided_by_user_id,v_head_ids,
    1,'PENDING',v_now+pg_catalog.make_interval(secs=>60),0,
    p_census,p_request,v_now,v_now);

  -- 24 section 4.4: the one bounded, informational stale warning, to the
  -- affected active Drafts only.  Bounded to the distinct DRAFT batches that
  -- actually hold an ACTIVE family item in this census, capped, and never
  -- allowed to lose the decision if the Banking Pay signal owner refuses.
  for v_signal_batch in
    select distinct (item_element.value->>'pay_batch_id')::uuid as pay_batch_id
      from pg_catalog.jsonb_array_elements(
             coalesce(p_census->'items','[]'::jsonb)) as item_element(value)
      join public.pay_batches as batch_row
        on batch_row.id=(item_element.value->>'pay_batch_id')::uuid
     where item_element.value->>'class'='ACTIVE'
       and item_element.value->>'pay_batch_id' is not null
       and batch_row.status='DRAFT'
     order by 1
     limit 25
  loop
    begin
      perform public.banking_pay_batch_signal_touch(
        p_pay_batch_id=>v_signal_batch,
        p_change_reason=>'WEEKLY_SOURCE_PENDING_ENTITLEMENT_DECISION_SAVED',
        p_change_source=>'WEEKLY_SOURCE',
        p_change_scope_json=>pg_catalog.jsonb_build_object(
          'weekly_source_pending_bundle_id',v_pending_bundle_id,
          'candidate_ids',pg_catalog.jsonb_build_array(v_candidate_id)),
        p_touch_payment_status=>false,
        p_touch_correction_progress=>false,
        p_touch_alerts=>false,
        p_touch_overview=>true);
      v_signalled:=v_signalled||pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('pay_batch_id',v_signal_batch,'signalled',true));
      v_signal_count:=v_signal_count+1;
    exception when others then
      get stacked diagnostics v_signal_error=message_text;
      v_signalled:=v_signalled||pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('pay_batch_id',v_signal_batch,'signalled',false,
          'error',pg_catalog.left(coalesce(v_signal_error,''),200)));
    end;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'created',true,'replayed',false,
    'pending_bundle_id',v_pending_bundle_id,
    'state','PENDING',
    'pending_revision',1,
    'request_digest',pg_catalog.encode(v_digest,'hex'),
    'next_check_at_utc',v_now+pg_catalog.make_interval(secs=>60),
    'superseded_pending_bundle_id',case when v_live.id is null then null else v_live.id end,
    'stale_warning',pg_catalog.jsonb_build_object(
      'signalled_batch_count',v_signal_count,'batches',v_signalled));
end;
$function$;

-- ---------------------------------------------------------------------------
-- 3. G5-4 - the bounded claim page
-- ---------------------------------------------------------------------------
-- proof/32 section 2: clamps INSIDE the function to LEAST(GREATEST(p_limit,1),25)
-- bundles and LEAST(GREATEST(p_lease_seconds,30),120) seconds regardless of the
-- caller's values; claims PENDING bundles whose next_check_at_utc is due AND
-- RELEASING bundles whose lease has expired (expired-lease reclaim; apply keeps
-- requiring an unexpired lease), in `next_check_at_utc, id` order with
-- FOR UPDATE SKIP LOCKED.
--
-- Every writer of this relation lives in this file and always sets
-- next_check_at_utc, so the ordering key is never NULL.  The ORDER BY is
-- fairness only: no safety rule is expressed through it, and the two claim arms
-- are two explicit predicates, not an ordering trick.
-- HANDOVER 2 round-5 ruling B4.2 - the bounded frozen watch.
--
-- "Do not append an audit row for every frozen tick. Record state transitions
-- and maintain bounded current-state counters/timestamps. A repeated unchanged
-- frozen poll may update one bounded status record or operational metric, but
-- may not grow an audit table without limit."
--
-- WHAT WROTE THE ROWS.  A frozen bundle was re-proved by a full claim/apply
-- cycle every sixty seconds, and that cycle writes audit rows in two places
-- that this package cannot change:
--
--   * interface I-1 takes the installed Candidate serial gate, which appends
--     one `CANDIDATE_SERIAL_GATE_GRANTED` row to `public.audit_events` per
--     grant (two on BLOCKED).  The gate is call-only under contract section 2.
--     WP-08b's review finding F5 measured about 1,400 rows a day from this
--     alone for ONE waiting bundle;
--   * WP-12's `weekly_source_audit_pending_bundle` trigger records the claim
--     (`PENDING -> RELEASING`) as `…RELEASE_ATTEMPT_STARTED` and the frozen
--     landing (`RELEASING -> PENDING` with a moved `pending_revision`) as
--     `…ENTITLEMENT_PUBLICATION_FROZEN`, once per member root each.
--
-- So the fix cannot be "write fewer audit rows" - it has to be "stop performing
-- a state transition that nothing has changed".  That is what this owner does.
--
-- It runs BEFORE the claim, in the claim's own transaction, over bundles that a
-- previous full attempt already proved FROZEN and left a signature on.  For
-- each one it re-reads the truth with NO serial gate, NO row lock on any
-- Timesheet and NO state change:
--
--   1. `…_resolve_root_identity_v1` - the read-only resolver proof/34 section 7
--      provides for exactly this ("Takes no lock, so it is safe inside a STABLE
--      assert") - for every stored root, compared against the stored canonical
--      root, version and trimmed family booking id, in the same three-valued
--      way the locked integrity gate compares them;
--   2. the accepted Office decision, read without a lock, against the stored
--      decision id, Candidate, state and proposed heads;
--   3. interface I-2 over every physical member of every family, which is
--      `stable` and writes nothing.
--
-- If ALL of that still says FROZEN with the SAME material signature, the bundle
-- has not changed: the owner moves `next_check_at_utc` and the bounded watch
-- counters on `last_census_json` and NOTHING ELSE.  `state` and
-- `pending_revision` do not move, so WP-12's trigger falls through to its
-- `return null`, and no serial gate was taken, so the gate wrote nothing
-- either.  A long frozen wait therefore costs a fixed number of audit rows, not
-- one per tick.
--
-- If ANYTHING else is true - the census changed, the root identity changed, the
-- decision no longer stands, or the poll raised at all - the owner writes NO
-- verdict of its own.  It drops the watch marker and sets `next_check_at_utc`
-- to now, which makes the bundle claimable in the SAME transaction, and the
-- ordinary claim/apply path then does the whole thing again properly, under the
-- serial gate and the family locks, and audits the real transition.
--
-- SAFETY.  This owner cannot release anything: it calls no coordinator, writes
-- no head, no receipt and no Banking Pay row, and its only possible outcomes
-- are "wait another minute" and "let the real path look at this".  Its reads
-- are unlocked, so a torn read is possible; both of its outcomes are safe under
-- one - a wrong "changed" costs one full attempt that re-proves everything
-- under the locks, and a wrong "unchanged" costs one minute of delay.  Nothing
-- is released by elapsed time, by observation count or by inference.
create or replace function private.weekly_source_pending_release_watch_page_v1(
  p_worker_id text,
  p_worker_run_id uuid,
  p_limit integer
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_limit integer:=least(greatest(coalesce(p_limit,25),1),25);
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_row record;
  v_decision record;
  v_identity jsonb;
  v_index integer;
  v_escalate boolean;
  v_escalate_reason text;
  v_members uuid[];
  v_new_members uuid[];
  v_census jsonb;
  v_signature text;
  v_stored_signature text;
  v_watch jsonb;
  v_observations bigint;
  v_polled integer:=0;
  v_unchanged integer:=0;
  v_escalated integer:=0;
  v_results jsonb:='[]'::jsonb;
begin
  for v_row in
    select pending_row.id,pending_row.candidate_id,pending_row.decision_bundle_id,
           pending_row.bundle_revision,pending_row.decision_id,
           pending_row.member_root_ids,pending_row.member_family_booking_ids,
           pending_row.member_root_versions,pending_row.proposed_head_ids,
           pending_row.last_census_json,pending_row.technical_failure_count
      from public.weekly_source_pending_entitlement_bundles as pending_row
     where pending_row.state='PENDING'
       and pending_row.next_check_at_utc is not null
       and pending_row.next_check_at_utc<=v_now
       and coalesce(pending_row.last_census_json,'{}'::jsonb) ? 'watch'
       and coalesce(pending_row.last_census_json->>'result','')='FROZEN'
     order by pending_row.next_check_at_utc,pending_row.id
     limit v_limit
     for update skip locked
  loop
    v_polled:=v_polled+1;
    v_escalate:=false;
    v_escalate_reason:=null;
    v_members:=array[]::uuid[];
    begin
      -- 1. read-only root identity, id by id, exactly the facts the locked
      --    integrity gate compares.  Every comparison fails CLOSED on NULL.
      for v_index in 1..pg_catalog.cardinality(v_row.member_root_ids) loop
        v_identity:=private.weekly_source_resolve_root_identity_v1(v_row.member_root_ids[v_index]);
        if coalesce((v_identity->>'ok')::boolean,false) is not true then
          v_escalate:=true;
          v_escalate_reason:=coalesce(v_identity->>'reason','ROOT_IDENTITY_UNRESOLVED');
          exit;
        end if;
        if coalesce((v_identity->>'requested_is_canonical')::boolean,false) is not true
           or coalesce((v_identity->>'family_is_current')::boolean,false) is not true
           or (v_identity->>'canonical_timesheet_id')::uuid
               is distinct from v_row.member_root_ids[v_index]
           or (v_identity->>'canonical_version')::integer
               is distinct from v_row.member_root_versions[v_index]
           or pg_catalog.btrim(coalesce(v_identity->>'family_booking_id',pg_catalog.chr(1)))
               is distinct from pg_catalog.btrim(coalesce(
                    v_row.member_family_booking_ids[v_index],pg_catalog.chr(2))) then
          v_escalate:=true;
          v_escalate_reason:='ROOT_IDENTITY_CHANGED';
          exit;
        end if;
        select coalesce(pg_catalog.array_agg(distinct member_element.value::uuid),
                        array[]::uuid[])
          into v_new_members
          from pg_catalog.jsonb_array_elements_text(
                 coalesce(v_identity->'member_timesheet_ids','[]'::jsonb))
               as member_element(value);
        v_members:=v_members||v_new_members;
      end loop;
      -- Every physical member of every family, once (I-3 section 7.3 step 3).
      if not v_escalate then
        select coalesce(pg_catalog.array_agg(distinct member_element.value),
                        array[]::uuid[])
          into v_members
          from pg_catalog.unnest(v_members) as member_element(value);
      end if;

      -- 2. the accepted Office decision, read without a lock.
      if not v_escalate then
        select bundle_row.state as state,bundle_row.decision_id as decision_id,
               bundle_row.candidate_id as candidate_id,
               bundle_row.proposed_head_ids as proposed_head_ids
          into v_decision
          from public.weekly_source_entitlement_decision_bundles as bundle_row
         where bundle_row.decision_bundle_id=v_row.decision_bundle_id
           and bundle_row.bundle_revision=v_row.bundle_revision;
        if not found
           or v_decision.state not in ('PROPOSED','COMMITTED')
           or v_decision.decision_id is distinct from v_row.decision_id
           or v_decision.candidate_id is distinct from v_row.candidate_id
           or v_decision.proposed_head_ids is distinct from v_row.proposed_head_ids then
          v_escalate:=true;
          v_escalate_reason:='DECISION_NO_LONGER_STANDS';
        end if;
      end if;

      -- 3. interface I-2 over every physical member of every family.
      if not v_escalate then
        if coalesce(pg_catalog.cardinality(v_members),0)=0 then
          v_escalate:=true;
          v_escalate_reason:='NO_FAMILY_MEMBERS_RESOLVED';
        else
          v_census:=private.weekly_source_freeze_census_v1(v_row.candidate_id,v_members);
          v_signature:=private.weekly_source_pending_release_census_signature_v1(v_census);
          v_stored_signature:=v_row.last_census_json->'watch'->>'signature';
          if coalesce(v_census->>'result','')<>'FROZEN'
             or v_signature is null
             or v_stored_signature is null
             or v_signature<>v_stored_signature then
            v_escalate:=true;
            v_escalate_reason:=case when coalesce(v_census->>'result','')<>'FROZEN'
              then 'CENSUS_RESULT_CHANGED' else 'CENSUS_SIGNATURE_CHANGED' end;
          end if;
        end if;
      end if;
    exception when others then
      -- A poll that raised proves nothing, so it never keeps the bundle
      -- waiting: the full path looks at it instead.
      v_escalate:=true;
      v_escalate_reason:='WATCH_POLL_FAILED';
    end;

    if v_escalate then
      -- `next_check_at_utc` is deliberately NOT moved.  The row was selected
      -- because it is already due, so leaving its due time alone is what makes
      -- it claimable by the claim query that runs immediately after this pass
      -- in the same transaction; writing `clock_timestamp()` here would set a
      -- due time LATER than the claim's own snapshot and silently cost a tick.
      -- Dropping the watch marker is the whole escalation.
      update public.weekly_source_pending_entitlement_bundles
         set last_census_json=coalesce(last_census_json,'{}'::jsonb)-'watch',
             updated_at_utc=v_now
       where id=v_row.id;
      v_escalated:=v_escalated+1;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'pending_bundle_id',v_row.id,'outcome','ESCALATED_TO_CLAIM',
        'reason',v_escalate_reason));
    else
      v_watch:=coalesce(v_row.last_census_json->'watch','{}'::jsonb);
      v_observations:=coalesce((v_watch->>'observation_count')::bigint,0)+1;
      -- The ONE bounded status record this ruling permits: a count and two
      -- timestamps, updated in place, on a column that is already a registered
      -- lifecycle column.  `state` and `pending_revision` are deliberately NOT
      -- in this statement.
      update public.weekly_source_pending_entitlement_bundles
         set next_check_at_utc=v_now+pg_catalog.make_interval(secs=>60),
             last_census_json=v_census||pg_catalog.jsonb_build_object(
               'watch',v_watch||pg_catalog.jsonb_build_object(
                 'signature',v_signature,
                 'observation_count',v_observations,
                 'last_observed_at_utc',v_now,
                 'last_observed_by_worker_id',
                   pg_catalog.left(pg_catalog.btrim(coalesce(p_worker_id,'')),128),
                 'last_observed_by_worker_run_id',p_worker_run_id)),
             updated_at_utc=v_now
       where id=v_row.id;
      v_unchanged:=v_unchanged+1;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'pending_bundle_id',v_row.id,'outcome','UNCHANGED_FROZEN',
        'observation_count',v_observations));
    end if;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'polled',v_polled,'unchanged_frozen',v_unchanged,
    'escalated_to_claim',v_escalated,'limit',v_limit,'bundles',v_results);
end;
$function$;

create or replace function private.weekly_source_pending_entitlement_release_claim_page_v1(
  p_worker_id text,
  p_worker_run_id uuid,
  p_lease_seconds integer,
  p_limit integer
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_worker text:=pg_catalog.btrim(coalesce(p_worker_id,''));
  v_limit integer;
  v_lease_seconds integer;
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_bundles jsonb;
  v_watch jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if v_worker='' or v_worker !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' or p_worker_run_id is null then
    raise exception 'WEEKLY_SOURCE_RELEASE_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;

  v_limit:=least(greatest(coalesce(p_limit,25),1),25);
  v_lease_seconds:=least(greatest(coalesce(p_lease_seconds,60),30),120);

  -- HANDOVER 2 round-5 ruling B4.2.  The bounded frozen watch runs FIRST, in
  -- this transaction, so a bundle whose freeze it proves unchanged never
  -- reaches the claim below and performs no state transition at all; and a
  -- bundle whose situation it finds CHANGED has had its watch marker dropped
  -- and its `next_check_at_utc` set to now, so the claim in the same statement
  -- picks it up on this very tick and does the full, audited work.
  v_watch:=private.weekly_source_pending_release_watch_page_v1(
    v_worker,p_worker_run_id,v_limit);

  with due as (
    select pending_row.id
      from public.weekly_source_pending_entitlement_bundles as pending_row
     where (pending_row.state='PENDING'
              and pending_row.next_check_at_utc is not null
              and pending_row.next_check_at_utc<=v_now
              -- A bundle still carrying a watch marker is the watch's, not the
              -- claim's.  Only the watch owner, the Office reopen or a terminal
              -- transition clears the marker, so a waiting bundle can never be
              -- dragged back into a full claim/apply cycle - and the audit-row
              -- bound holds even when more bundles are waiting than one page.
              and not (coalesce(pending_row.last_census_json,'{}'::jsonb) ? 'watch'))
        or (pending_row.state='RELEASING'
              and pending_row.lease_expires_at_utc is not null
              and pending_row.lease_expires_at_utc<=v_now)
     order by pending_row.next_check_at_utc,pending_row.id
     limit v_limit
     for update skip locked
  ),
  claimed as (
    update public.weekly_source_pending_entitlement_bundles as pending_row
       set state='RELEASING',
           pending_revision=pending_row.pending_revision+1,
           lease_owner=v_worker,
           lease_token=pg_catalog.gen_random_uuid(),
           lease_worker_run_id=p_worker_run_id,
           lease_expires_at_utc=v_now+pg_catalog.make_interval(secs=>v_lease_seconds),
           next_check_at_utc=v_now+pg_catalog.make_interval(secs=>v_lease_seconds),
           updated_at_utc=v_now
      from due
     where pending_row.id=due.id
     returning pending_row.id,pending_row.decision_bundle_id,pending_row.bundle_revision,
               pending_row.candidate_id,pending_row.pending_revision,
               pending_row.request_digest,pending_row.lease_token,
               pending_row.lease_worker_run_id,pending_row.lease_expires_at_utc,
               pending_row.technical_failure_count
  )
  select coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'pending_bundle_id',claimed.id,
             'decision_bundle_id',claimed.decision_bundle_id,
             'bundle_revision',claimed.bundle_revision,
             'candidate_id',claimed.candidate_id,
             'pending_revision',claimed.pending_revision,
             'request_digest',pg_catalog.encode(claimed.request_digest,'hex'),
             'lease_token',claimed.lease_token,
             'worker_run_id',claimed.lease_worker_run_id,
             'lease_expires_at_utc',claimed.lease_expires_at_utc,
             'technical_failure_count',claimed.technical_failure_count)
           order by claimed.id),'[]'::jsonb)
    into v_bundles
    from claimed;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'worker_id',v_worker,
    'worker_run_id',p_worker_run_id,
    'limit',v_limit,
    'lease_seconds',v_lease_seconds,
    'claimed_count',pg_catalog.jsonb_array_length(v_bundles),
    'bundles',v_bundles,
    -- The bounded operational metric for the tick: how many waiting bundles
    -- were re-proved frozen without a state transition, and how many the watch
    -- handed back to the claim.
    'watch',v_watch);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 4. G5-5 - the apply owner
-- ---------------------------------------------------------------------------
-- Order, and the order is the whole point (proof/32 sections 2, 7 and 8):
--   1. exact receipt replay, BEFORE the lease checks, so a committed release
--      whose response was lost is returned even after its lease has expired
--      (R33).  A digest hit whose immutable fields differ is a replay conflict;
--   2. the lease checks: RELEASING, this worker, this token, this Worker RUN
--      id, unexpired.  Any mismatch is WEEKLY_SOURCE_RELEASE_LEASE_INVALID with
--      nothing written (R32);
--   3. the section 6 locks through I-1, then the section 4.0 integrity gate,
--      then the section 4 census;
--   4. the section 7 revalidation under the locks;
--   5. the SAME coordinator (interface I-4) in DEFERRED mode - never a copy.
--
-- FROZEN is a normal outcome: next_check_at_utc advances by the ordinary
-- interval and technical_failure_count is NEVER touched.  Round-4 ruling 3
-- makes this explicit for the census's WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED
-- item, which is an ordinary ACTIVE/FROZEN result and consumes no budget.
-- Only technical database or census errors and invariant failures increment the
-- counter; ten CONSECUTIVE ones move the bundle to MANUAL_REVIEW.  Nothing is
-- ever released by elapsed time, by attempt count or by inference.
create or replace function private.weekly_source_pending_entitlement_release_apply_v1(
  p_pending_bundle_id uuid,
  p_expected_pending_revision bigint,
  p_expected_request_digest bytea,
  p_worker_id text,
  p_lease_token uuid,
  p_worker_run_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_bundle record;
  v_locked record;
  v_receipt record;
  v_decision record;
  v_now timestamptz;
  v_lock_census jsonb;
  v_lock_result jsonb;
  v_census jsonb;
  v_census_result text;
  v_core jsonb;
  v_code text;
  v_proof jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  select * into v_bundle
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=p_pending_bundle_id;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'replayed',false,'retryable',false,
      'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;

  -- ---- 1. exact receipt replay, before the lease checks --------------------
  -- proof/32 section 8 step 1: the digest is only an index lookup; every
  -- immutable receipt field is compared before a replay may be returned.
  if p_expected_request_digest is not null then
    select * into v_receipt
      from private.weekly_source_entitlement_publication_receipts as receipt_row
     where receipt_row.request_digest=p_expected_request_digest
       and receipt_row.decision_bundle_id=v_bundle.decision_bundle_id
       and receipt_row.bundle_revision=v_bundle.bundle_revision;
    if found then
      if v_receipt.publication_mode<>'DEFERRED'
         or v_receipt.pending_bundle_id is distinct from p_pending_bundle_id
         or v_receipt.candidate_id<>v_bundle.candidate_id
         or v_receipt.member_root_ids is distinct from v_bundle.member_root_ids
         or v_receipt.member_family_booking_ids is distinct from v_bundle.member_family_booking_ids
         or v_receipt.member_root_versions is distinct from v_bundle.member_root_versions
         or v_receipt.head_ids is distinct from v_bundle.proposed_head_ids
         or v_receipt.decision_id<>v_bundle.decision_id then
        return pg_catalog.jsonb_build_object(
          'ok',false,'released',false,'replayed',false,'retryable',false,
          'code','WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
          'pending_bundle_id',p_pending_bundle_id,
          'detail',pg_catalog.jsonb_build_object('receipt_id',v_receipt.id));
      end if;
      return pg_catalog.jsonb_build_object(
        'ok',true,'released',true,'replayed',true,
        'pending_bundle_id',p_pending_bundle_id,
        'state',v_bundle.state,
        'receipt',private.weekly_source_publication_receipt_json_v1(v_receipt.id));
    end if;
  end if;

  -- ---- 2. the lease checks -------------------------------------------------
  v_now:=pg_catalog.clock_timestamp();
  if v_bundle.state<>'RELEASING'
     or v_bundle.lease_owner is distinct from pg_catalog.btrim(coalesce(p_worker_id,''))
     or v_bundle.lease_token is distinct from p_lease_token
     or v_bundle.lease_worker_run_id is distinct from p_worker_run_id
     or v_bundle.lease_expires_at_utc is null
     or v_bundle.lease_expires_at_utc<=v_now
     or v_bundle.pending_revision is distinct from p_expected_pending_revision
     or v_bundle.request_digest is distinct from p_expected_request_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'replayed',false,'retryable',false,
      'code','WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
      'pending_bundle_id',p_pending_bundle_id,
      'detail',pg_catalog.jsonb_build_object('state',v_bundle.state));
  end if;

  -- ---- 3. the section 6 locks, the integrity gate and the census -----------
  v_lock_census:=private.weekly_source_pending_release_lock_and_census_v1(
    v_bundle.candidate_id,
    v_bundle.member_root_ids,
    v_bundle.member_family_booking_ids,
    v_bundle.member_root_versions,
    v_bundle.lease_worker_run_id);

  -- ---- 4. revalidation under the locks, BEFORE ANY WRITE (proof/32 §7) ----
  -- Review finding F1.  This block used to sit after the three step-3 refusal
  -- transitions, so a BUSY, MANUAL_REVIEW or TECHNICAL_FAILURE write could land
  -- on a row whose lease had already been taken over - stopped only by the
  -- relation's own CHECK constraints, which is the schema saving the owner
  -- rather than the owner being right.  "Under the locks, BEFORE ANY WRITE"
  -- means exactly that, so the re-check now precedes every branch below and no
  -- path in steps 3 to 5 writes without a verified lease held under the row
  -- lock.  Lock order is unchanged: the I-1 locks are already held here, so the
  -- pending row is still the last lock taken (§6 step 4).
  select * into v_locked
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=p_pending_bundle_id
   for update;
  if not found
     or v_locked.state<>'RELEASING'
     or v_locked.lease_token is distinct from p_lease_token
     or v_locked.lease_worker_run_id is distinct from p_worker_run_id
     or v_locked.lease_owner is distinct from pg_catalog.btrim(coalesce(p_worker_id,''))
     or v_locked.lease_expires_at_utc is null
     or v_locked.lease_expires_at_utc<=pg_catalog.clock_timestamp()
     or v_locked.pending_revision is distinct from p_expected_pending_revision
     or v_locked.request_digest is distinct from p_expected_request_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'replayed',false,'retryable',false,
      'code','WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
      'pending_bundle_id',p_pending_bundle_id,
      'detail',pg_catalog.jsonb_build_object('stage','UNDER_LOCK'));
  end if;

  if coalesce((v_lock_census->>'ok')::boolean,false) is not true then
    v_code:=coalesce(v_lock_census->>'code','WEEKLY_SOURCE_PENDING_RELEASE_UNKNOWN');
    if v_code='WEEKLY_SOURCE_CANDIDATE_BUSY' then
      -- proof/32 section 6 step 1: BLOCKED means skip this bundle for this tick
      -- and retry next tick.  Not a failure; the counter is not touched.
      update public.weekly_source_pending_entitlement_bundles
         set state='PENDING',
             pending_revision=pending_revision+1,
             lease_owner=null,lease_token=null,lease_worker_run_id=null,
             lease_expires_at_utc=null,
             next_check_at_utc=v_now+pg_catalog.make_interval(secs=>60),
             updated_at_utc=v_now
       where id=p_pending_bundle_id;
      return pg_catalog.jsonb_build_object(
        'ok',false,'released',false,'replayed',false,'retryable',true,
        'code','WEEKLY_SOURCE_CANDIDATE_BUSY','outcome','SKIPPED_THIS_TICK',
        'pending_bundle_id',p_pending_bundle_id);
    end if;
    if v_code='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' then
      -- proof/32 sections 4.0, 5.3 and 7: a rotated, ambiguous or absent stored
      -- root is MANUAL_REVIEW.  Never released, never silently rebuilt.
      --
      -- =====================================================================
      -- HANDOVER 2 round-7 ruling A6 - THE COLLISION NAME, AND ITS LIMIT
      -- =====================================================================
      -- "§4.0 remains fail-closed for bound bundles.  Where the ambiguous
      --  family is specifically the trim/whitespace-equivalent canonical
      --  booking-reference split, the authoritative outcome is
      --  `BOOKING_REFERENCE_CANONICAL_COLLISION` everywhere, including R24.
      --  Other non-canonical shapes retain their own precise reason; do not
      --  collapse every integrity failure into the collision code."
      --
      -- §4.0 is unchanged and still fails closed.  What changes is the NAME the
      -- Office sees.  Before this ruling was implemented the collision reached
      -- the Office as `WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION` with the
      -- real cause buried in a text suffix, because
      -- `…_manual_review_v1` takes the Office-visible code from
      -- `split_part(reason,':',1)`.  Measured on a build from empty, both the
      -- trim/whitespace split and the zero-current-row family produced the same
      -- Office-visible code.  That is the "soft form of merging" the approver
      -- named.
      --
      -- The narrowing is the whole point and is written as a WHITELIST of ONE
      -- reason.  Making the collision code universal would be the easy and
      -- wrong reading: every other non-canonical shape - `CANONICAL_AMBIGUOUS`
      -- (the zero-or-many canonical-row family R24 actually drives),
      -- `CURRENT_ROW_CARDINALITY`, `VERSION_NOT_UNIQUE`, `FAMILY_UNRESOLVED`,
      -- `BOOKING_IDENTITY_MISSING`, `BOOKING_REPOINTED_DURING_LOCK`,
      -- `ROOT_CANDIDATE_MISMATCH`, `ROOT_NOT_RETURNED_BY_RESOLVER`,
      -- `FAMILY_RESULT_INCOMPLETE` and every §4.0 per-member reason - keeps
      -- `WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION` and its own precise
      -- reason, exactly as it does today.
      --
      -- The reason is read from the interface I-1 result, which is the ONE
      -- place that detects the split (`weekly_source_lock_family_rows_v1`,
      -- WP-08a review F3); this owner re-derives nothing.
      return private.weekly_source_pending_release_manual_review_v1(
        p_pending_bundle_id,
        case when pg_catalog.upper(pg_catalog.btrim(coalesce(v_lock_census->>'reason','')))
                  ='BOOKING_REFERENCE_CANONICAL_COLLISION'
             then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
             else 'WEEKLY_SOURCE_ROOT_ROTATED_AFTER_AUTHORISATION' end
          ||': '
          ||pg_catalog.left(coalesce(v_lock_census->>'reason','')
             ||' '||coalesce((v_lock_census->'failures')::text,''),400),
        v_lock_census);
    end if;
    -- HANDOVER 2 round-5 ruling B4.1.  This used to send EVERY remaining lock
    -- and integrity refusal - `WEEKLY_SOURCE_SERIAL_GATE_BYPASSED`,
    -- `WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID` and anything unnamed - round
    -- the ten-attempt budget first.  None of them clears with time, so the
    -- disposition owner now escalates them on this tick unless it can PROVE
    -- they are transient.
    return private.weekly_source_pending_release_refuse_v1(
      p_pending_bundle_id,v_code,v_lock_census);
  end if;

  v_lock_result:=v_lock_census->'lock_result';
  v_census:=v_lock_census->'census';
  v_census_result:=coalesce(v_census->>'result','');

  -- ---- 5. the accepted decision still stands ------------------------------
  select * into v_decision
    from public.weekly_source_entitlement_decision_bundles as bundle_row
   where bundle_row.decision_bundle_id=v_locked.decision_bundle_id
     and bundle_row.bundle_revision=v_locked.bundle_revision
   for update;
  if not found then
    -- B4.1: a missing accepted decision is a fixed integrity refusal.  It can
    -- never clear on a retry, so it goes to a human on this tick.
    return private.weekly_source_pending_release_refuse_v1(
      p_pending_bundle_id,'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
      pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_FOUND',
                                    'integrity_failure',true));
  end if;
  if v_decision.state not in ('PROPOSED','COMMITTED')
     or v_decision.decision_id<>v_locked.decision_id
     or v_decision.candidate_id<>v_locked.candidate_id
     or v_decision.proposed_head_ids is distinct from v_locked.proposed_head_ids then
    -- The immutable accepted Office decision no longer stands (section 7).
    return private.weekly_source_pending_release_superseded_v1(
      p_pending_bundle_id,'WEEKLY_SOURCE_PENDING_BUNDLE_DECISION_SUPERSEDED',
      pg_catalog.jsonb_build_object('decision_bundle_state',v_decision.state));
  end if;

  -- ---- 5. the census verdict ----------------------------------------------
  if v_census_result='FROZEN' then
    -- A normal result.  Round-4 ruling 3: this includes the census's
    -- WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED item.
    --
    -- HANDOVER 2 round-5 ruling B4.3, confirmed rather than changed: "A frozen
    -- result does not reset the technical-failure counter. It neither
    -- increments nor clears it."  `technical_failure_count` is absent from this
    -- statement for exactly that reason, and the executed proof drives it with
    -- a non-zero counter so its absence is measured rather than asserted.
    --
    -- HANDOVER 2 round-5 ruling B4.2: this is also where the frozen WATCH is
    -- established.  The signature of the census this attempt proved is stored
    -- beside it, so the next tick can prove "nothing has changed" with a
    -- read-only poll instead of another full claim/apply cycle - and a full
    -- cycle is what writes the audit rows (WP-08b review F5 measured about
    -- 1,400 CANDIDATE_SERIAL_GATE_GRANTED rows a day for one waiting bundle,
    -- and WP-12's bundle trigger adds an ATTEMPT_STARTED and a
    -- PUBLICATION_FROZEN row per member root on top of that).
    update public.weekly_source_pending_entitlement_bundles
       set state='PENDING',
           pending_revision=pending_revision+1,
           lease_owner=null,lease_token=null,lease_worker_run_id=null,
           lease_expires_at_utc=null,
           next_check_at_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>60),
           last_census_json=v_census||pg_catalog.jsonb_build_object(
             'watch',pg_catalog.jsonb_build_object(
               'signature',private.weekly_source_pending_release_census_signature_v1(v_census),
               'observation_count',1,
               'established_by','RELEASE_ATTEMPT',
               'first_observed_at_utc',pg_catalog.clock_timestamp(),
               'last_observed_at_utc',pg_catalog.clock_timestamp())),
           updated_at_utc=pg_catalog.clock_timestamp()
     where id=p_pending_bundle_id;
    return pg_catalog.jsonb_build_object(
      'ok',true,'released',false,'replayed',false,'retryable',true,
      'outcome','FROZEN','state','PENDING',
      'pending_bundle_id',p_pending_bundle_id,
      'census_result','FROZEN',
      'census_reason',v_census->>'reason',
      'watch_signature',private.weekly_source_pending_release_census_signature_v1(v_census),
      'technical_failure_count',v_locked.technical_failure_count);
  end if;

  if v_census_result<>'RELEASABLE' then
    -- CENSUS_ERROR, and anything the census does not name.
    --
    -- Round-4 ruling 4: an unmarked VOIDED transfer reaches here and must stay
    -- here - Weekly Source adds no interpretation of its own.  HANDOVER 2
    -- round-5 ruling B4.1 changes only WHEN the Office sees it: a census that
    -- cannot classify a Banking Pay item is a fixed structural refusal, not a
    -- transient technical error, so it no longer spends ten attempts and about
    -- ninety minutes of backoff first.  The disposition owner escalates it on
    -- this tick, and the exact item identifiers go to the child relation.
    return private.weekly_source_pending_release_refuse_v1(
      p_pending_bundle_id,
      'WEEKLY_SOURCE_CENSUS_ERROR',
      pg_catalog.jsonb_build_object(
        'census_result',v_census_result,
        'census_reason',v_census->>'reason',
        'integrity_failure',true,
        'census_errors',coalesce(v_census->'errors','[]'::jsonb),
        'census_error_items',coalesce((
          select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                   'class',item_element.value->>'class',
                   'pay_batch_item_id',item_element.value->'pay_batch_item_id',
                   'timesheet_id',item_element.value->'timesheet_id',
                   'reason',item_element.value->'reason')
                 order by item_element.value->>'pay_batch_item_id')
            from pg_catalog.jsonb_array_elements(
                   coalesce(v_census->'items','[]'::jsonb)) as item_element(value)
           where item_element.value->>'class'='CENSUS_ERROR'),'[]'::jsonb)),
      v_census);
  end if;

  -- ---- 6. the SAME coordinator, in DEFERRED mode (proof/32 section 8) ------
  v_proof:=pg_catalog.jsonb_build_object(
    'cancellation',coalesce((
      select pg_catalog.jsonb_agg(proof_element.value order by proof_element.ordinality)
        from pg_catalog.jsonb_array_elements(
               coalesce(v_census->'proof','[]'::jsonb))
             with ordinality as proof_element(value,ordinality)
       where proof_element.value->>'section'='5.1'),'[]'::jsonb),
    'settlement',coalesce((
      select pg_catalog.jsonb_agg(proof_element.value order by proof_element.ordinality)
        from pg_catalog.jsonb_array_elements(
               coalesce(v_census->'proof','[]'::jsonb))
             with ordinality as proof_element(value,ordinality)
       where proof_element.value->>'section'='5.2'),'[]'::jsonb),
    'class_counts',coalesce(v_census->'class_counts','{}'::jsonb));

  v_core:=private.weekly_source_entitlement_publish_core_v1(
    p_request=>v_locked.request_json,
    p_publication_mode=>'DEFERRED',
    p_lock_result=>v_lock_result,
    p_pending_bundle_id=>p_pending_bundle_id,
    p_worker_id=>v_locked.lease_owner,
    p_worker_run_id=>v_locked.lease_worker_run_id,
    p_census=>v_census,
    p_proof=>v_proof);

  if coalesce((v_core->>'ok')::boolean,false) is not true then
    -- The core returns only pre-write refusals; everything after its first
    -- write raises and rolls the whole transaction back (round-4 ruling 6
    -- point 7), which the Worker records through the failure owner below.
    v_code:=coalesce(v_core->>'code','WEEKLY_SOURCE_PENDING_RELEASE_UNKNOWN');
    if v_code in ('WEEKLY_SOURCE_PUBLICATION_HEAD_CAS_CONFLICT',
                  'WEEKLY_SOURCE_PUBLICATION_SOURCE_REVISION_STALE') then
      -- proof/32 section 10: a newer committed head or a newer source revision
      -- has replaced this bundle.  R8, and R7 read through section 10.
      return private.weekly_source_pending_release_superseded_v1(
        p_pending_bundle_id,v_code,v_core);
    end if;
    -- HANDOVER 2 round-5 ruling A1 control 5, delivered through WP-02b handoff
    -- N1: "Conflicting replay and a tampered row are permanent integrity
    -- failures and go directly to manual review; they are not retried ten
    -- times."  WP-02b now LABELS those outcomes on the refusal itself, before
    -- any write, with `detail.disposition='MANUAL_REVIEW'` and
    -- `detail.integrity_failure=true`.  The disposition owner honours that
    -- label rather than re-deriving the decision from the code, which is why
    -- the hand-kept list below is now only a backstop: it is reached when the
    -- coordinator did not label the refusal, and it no longer decides anything
    -- the label already decided.
    if v_code in (
         -- A digest hit whose immutable fields differ is never retried silently.
         'WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
         -- proof/34 section 6 and the WP-02 mapping table (WP-02_NEEDS N4): a
         -- root integrity failure is MANUAL_REVIEW wherever it is raised, not
         -- only when interface I-1 raises it.  Interface I-7 revision 2 REFUSES
         -- a family whose head names a different physical root instead of
         -- falling back to a TSFIN authority, and the refusal arrives here with
         -- detail.reason COMMITTED_HEAD_EXISTS_FOR_THE_PHYSICAL_ROOT_BUT_NOT_THE
         -- _DECLARED_FAMILY or INTERFACE_I7_REPORTS_AN_AUTHORITY_THE_FAMILY_KEY
         -- _DOES_NOT.  A refusal is treated as a refusal: nothing is published
         -- and nothing is rebuilt.
         'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
         -- Decision D10 (WP-02_NEEDS N12): the coordinator verifies the stored
         -- request against the stored digest under the lock.  Both reasons -
         -- THE_STORED_REQUEST_DOES_NOT_MATCH_ITS_STORED_DIGEST and
         -- THE_REQUEST_BEING_RELEASED_IS_NOT_THE_ONE_THAT_WAS_SAVED - mean the
         -- bundle needs Office attention, not another tick.
         'WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID',
         -- A second, different request under an already COMMITTED bundle
         -- revision.  Permanent by construction, so it never spends the
         -- technical-failure budget on its way to Office review.
         'WEEKLY_SOURCE_PUBLICATION_BUNDLE_REVISION_ALREADY_PUBLISHED')
    then
      return private.weekly_source_pending_release_manual_review_v1(
        p_pending_bundle_id,
        v_code||': '||coalesce(v_core->'detail'->>'reason','<no reason>'),
        v_core||pg_catalog.jsonb_build_object('census',v_census));
    end if;
    -- B4.1 and review finding F4.  Every remaining coordinator refusal used to
    -- come here and spend the whole budget, including refusals the review
    -- executed and proved could never succeed - `REQUEST_INVALID /
    -- APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION` and
    -- `WEEK_OR_CONTRACT_DISAGREES_WITH_ACCEPTED_DECISION`.  The disposition
    -- owner now decides, and its default is immediate escalation.
    return private.weekly_source_pending_release_refuse_v1(
      p_pending_bundle_id,v_code,v_core,v_census);
  end if;

  if coalesce((v_core->>'replayed')::boolean,false) then
    -- The coordinator found its own committed receipt for this exact request.
    -- Record the release facts and publish nothing a second time.
    update public.weekly_source_pending_entitlement_bundles
       set state='RELEASED',
           pending_revision=pending_revision+1,
           released_receipt_id=(v_core->'receipt'->>'id')::uuid,
           released_receipt_digest=pg_catalog.decode(v_core->'receipt'->>'request_digest','hex'),
           released_by_worker_id=coalesce(
             v_core->'receipt'->>'released_by_worker_id',v_locked.lease_owner),
           released_by_worker_run_id=coalesce(
             (v_core->'receipt'->>'released_by_worker_run_id')::uuid,v_locked.lease_worker_run_id),
           released_at_utc=pg_catalog.clock_timestamp(),
           lease_owner=null,lease_token=null,lease_worker_run_id=null,
           lease_expires_at_utc=null,
           next_check_at_utc=pg_catalog.clock_timestamp(),
           last_census_json=v_census,
           -- HANDOVER 2 round-5 ruling B4.3: "Reset is allowed only after
           -- positively proved successful progress."  A replayed release IS
           -- proved progress - the coordinator found its own committed receipt
           -- for this exact request - so this path now resets the counter, the
           -- same as the first-release path below.  Leaving the two paths
           -- disagreeing was the inconsistency B4.3 asked to be checked.
           technical_failure_count=0,
           updated_at_utc=pg_catalog.clock_timestamp()
     where id=p_pending_bundle_id;
    return v_core||pg_catalog.jsonb_build_object(
      'released',true,'pending_bundle_id',p_pending_bundle_id,'state','RELEASED');
  end if;

  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASED',
         pending_revision=pending_revision+1,
         released_receipt_id=(v_core->'receipt'->>'id')::uuid,
         released_receipt_digest=pg_catalog.decode(v_core->'receipt'->>'request_digest','hex'),
         released_by_worker_id=v_locked.lease_owner,
         released_by_worker_run_id=v_locked.lease_worker_run_id,
         released_at_utc=pg_catalog.clock_timestamp(),
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=pg_catalog.clock_timestamp(),
         last_census_json=v_census,
         technical_failure_count=0,
         updated_at_utc=pg_catalog.clock_timestamp()
   where id=p_pending_bundle_id;

  return v_core||pg_catalog.jsonb_build_object(
    'released',true,
    'pending_bundle_id',p_pending_bundle_id,
    'state','RELEASED',
    'census_result','RELEASABLE',
    'census',v_census);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 5. The three terminal transitions, in one place each
-- ---------------------------------------------------------------------------
-- proof/32 section 10.  None of them publishes, and none of them touches any
-- Banking Pay row.
-- The ONE builder of the Office-visible manual-review reason.
--
-- Review finding F3.  The reason used to be prose with the whole detail object
-- appended and cut at 1,000 characters, which silently dropped the census item
-- identifiers from the fifth item onward.  HANDOVER 2 round-4 ruling 4 requires
-- the exact item identifiers to reach the Office, so the reason is now a
-- STRUCTURED object rendered as text:
--
--   * `message` is the short human sentence an Office screen prints;
--   * `items` carries the exact identifiers, one object per census error item;
--   * `item_count` is the TRUE count, whether or not every item is listed;
--   * `items_truncated` says so explicitly when the cap bites, and
--     `complete_detail_in` names where the complete list always survives.
--
-- The structured form was chosen over simply raising the cap because a prose
-- string that happens to be long enough today is not a contract: an Office
-- screen has to be able to FIND the identifiers, not scrape them out of a
-- sentence, and a future census with more items would silently regress.
--
-- The cap is a deliberate, stated 8,000 characters (the column itself is
-- unbounded `text`; the cap exists so one bad bundle cannot put an unbounded
-- blob on an Office screen).  It is applied by DROPPING WHOLE ITEMS, never by
-- cutting the text, so the value is always valid JSON and any loss is declared.
--
-- HANDOVER 2 round-5 ruling B4.4 goes further than that fix: "The Office
-- reason's 1,000-character summary may remain bounded, but the exact census
-- item identifiers must be retained losslessly in bounded, pageable child
-- records and shown through the Office detail view. Never concatenate an
-- unbounded list into one field and never silently drop the fifth or later
-- identifier."
--
-- So the reason is now only a SUMMARY.  It is no longer the carrier of the
-- identifiers: every item, including the fifth and every later one, is written
-- losslessly to `private.weekly_source_pending_release_review_items` by
-- `…_record_review_items_v1` below, one row per item with its identifiers in
-- their own typed columns, and read back a page at a time through
-- `public.weekly_source_pending_release_review_items_page_v1`.  `complete_detail_in`
-- names that relation and that reader so an Office screen can FIND the
-- identifiers rather than scrape them, and `items_truncated` now says only that
-- the SUMMARY is short, never that evidence was lost.
create or replace function private.weekly_source_pending_release_review_reason_v1(
  p_code text,
  p_message text,
  p_detail jsonb,
  p_failure_count integer default null
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_cap constant integer:=8000;
  v_items jsonb:=coalesce(p_detail->'census_error_items','[]'::jsonb);
  v_item_count integer;
  v_keep integer;
  v_reason jsonb;
  v_text text;
begin
  if pg_catalog.jsonb_typeof(v_items)<>'array' then
    v_items:='[]'::jsonb;
  end if;
  v_item_count:=pg_catalog.jsonb_array_length(v_items);
  v_keep:=v_item_count;
  loop
    v_reason:=pg_catalog.jsonb_build_object(
      'message',pg_catalog.left(
        coalesce(nullif(pg_catalog.btrim(coalesce(p_message,'')),''),
                 'This decision needs Office review. No financial change has been made.'),500),
      'code',coalesce(nullif(pg_catalog.btrim(coalesce(p_code,'')),''),
                      'WEEKLY_SOURCE_PENDING_RELEASE_MANUAL_REVIEW'),
      'consecutive_technical_failures',p_failure_count,
      'census_reason',p_detail->>'census_reason',
      'reasons',(select pg_catalog.jsonb_agg(distinct item_row.value->>'reason')
                   from pg_catalog.jsonb_array_elements(v_items) as item_row(value)
                  where item_row.value->>'reason' is not null),
      'item_count',v_item_count,
      'items',(select coalesce(pg_catalog.jsonb_agg(listed.value order by listed.ordinality),
                               '[]'::jsonb)
                 from pg_catalog.jsonb_array_elements(v_items)
                      with ordinality as listed(value,ordinality)
                where listed.ordinality<=v_keep),
      -- B4.4: this says the SUMMARY is short, not that anything was lost.  The
      -- complete list is in the child relation below, always.
      'items_truncated',v_keep<v_item_count,
      'complete_detail_in',pg_catalog.jsonb_build_object(
        'relation','private.weekly_source_pending_release_review_items',
        'reader','public.weekly_source_pending_release_review_items_page_v1',
        'census_blob','public.weekly_source_pending_entitlement_bundles.last_census_json'));
    v_text:=v_reason::text;
    exit when v_keep=0 or pg_catalog.length(v_text)<=v_cap;
    -- Drop whole items, never characters, so the value stays valid JSON and the
    -- loss is always declared by `items_truncated` and `item_count`.
    v_keep:=(v_keep*3)/4;
  end loop;
  return pg_catalog.left(v_text,v_cap);
end;
$function$;

-- HANDOVER 2 round-5 ruling B4.4, the lossless half.
--
-- One row per census item, in the census's own order, with the identifiers in
-- their own typed columns.  Nothing is concatenated and nothing is dropped:
-- the row count written is returned and the caller asserts it equals the item
-- count it was handed, so a silent loss is impossible rather than merely
-- unlikely.
--
-- The items are taken from `detail.census_error_items` when the refusal carries
-- one (the CENSUS_ERROR shape the apply owner builds), and otherwise from the
-- census's own `items` array restricted to the CENSUS_ERROR class, so a manual
-- review reached by any route carries the same evidence.
--
-- It is append-only and idempotent by construction: `(pending_bundle_id,
-- pending_revision, item_ordinal)` is unique and the write is
-- `on conflict do nothing`, so a replayed recorder cannot duplicate a
-- generation and a new attempt writes its own generation beside the old one.
create or replace function private.weekly_source_pending_release_record_review_items_v1(
  p_pending_bundle_id uuid,
  p_pending_revision bigint,
  p_code text,
  p_detail jsonb,
  p_census jsonb default null
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_items jsonb;
  v_item_count integer;
  v_written integer;
  v_existing integer;
begin
  if p_pending_bundle_id is null or coalesce(p_pending_revision,0)<1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'item_count',0,'rows_written',0,
      'reason','REVIEW_ITEM_TARGET_INVALID');
  end if;

  v_items:=coalesce(p_detail->'census_error_items','null'::jsonb);
  if pg_catalog.jsonb_typeof(v_items)<>'array' then
    v_items:=coalesce((
      select pg_catalog.jsonb_agg(element.value order by element.ordinality)
        from pg_catalog.jsonb_array_elements(
               case when pg_catalog.jsonb_typeof(coalesce(p_census->'items','[]'::jsonb))='array'
                    then p_census->'items' else '[]'::jsonb end)
             with ordinality as element(value,ordinality)
       where element.value->>'class'='CENSUS_ERROR'),'[]'::jsonb);
  end if;
  v_item_count:=pg_catalog.jsonb_array_length(v_items);

  insert into private.weekly_source_pending_release_review_items(
    pending_bundle_id,pending_revision,item_ordinal,
    refusal_code,census_result,item_class,item_reason,
    pay_batch_item_id,timesheet_id,item_json)
  select
    p_pending_bundle_id,
    p_pending_revision,
    element.ordinality::integer,
    pg_catalog.left(coalesce(nullif(pg_catalog.btrim(coalesce(p_code,'')),''),
                             'WEEKLY_SOURCE_PENDING_RELEASE_MANUAL_REVIEW'),200),
    nullif(pg_catalog.btrim(coalesce(p_census->>'result','')),''),
    coalesce(nullif(pg_catalog.btrim(coalesce(element.value->>'class','')),''),'UNCLASSIFIED'),
    nullif(pg_catalog.btrim(coalesce(element.value->>'reason','')),''),
    -- A malformed identifier is preserved in `item_json` and left null in the
    -- typed column rather than failing the write; losing the row would be the
    -- exact silent drop the ruling forbids.
    case when coalesce(element.value->>'pay_batch_item_id','')
              ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
         then (element.value->>'pay_batch_item_id')::uuid end,
    case when coalesce(element.value->>'timesheet_id','')
              ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
         then (element.value->>'timesheet_id')::uuid end,
    case when pg_catalog.jsonb_typeof(element.value)='object' then element.value
         else pg_catalog.jsonb_build_object('value',element.value) end
    from pg_catalog.jsonb_array_elements(v_items) with ordinality as element(value,ordinality)
  on conflict (pending_bundle_id,pending_revision,item_ordinal) do nothing;
  get diagnostics v_written=row_count;

  select pg_catalog.count(*) into v_existing
    from private.weekly_source_pending_release_review_items as stored
   where stored.pending_bundle_id=p_pending_bundle_id
     and stored.pending_revision=p_pending_revision;

  return pg_catalog.jsonb_build_object(
    'ok',v_existing>=v_item_count,
    'item_count',v_item_count,
    'rows_written',v_written,
    'rows_retained',v_existing,
    'relation','private.weekly_source_pending_release_review_items',
    'reader','public.weekly_source_pending_release_review_items_page_v1');
end;
$function$;

create or replace function private.weekly_source_pending_release_technical_failure_v1(
  p_pending_bundle_id uuid,
  p_code text,
  p_detail jsonb,
  p_census jsonb default null
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_count integer;
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_reason text;
  v_next timestamptz;
  v_revision bigint;
  v_items jsonb;
begin
  -- HANDOVER 2 round-7 ruling A5.  A frozen result "neither increments nor
  -- resets the technical-failure counter".  This owner is the ONLY writer that
  -- increments it, so the guard belongs here as well as in the classifier: a
  -- frozen census reaching the budget would spend a retry a waiting state must
  -- not spend, and after ten of them would reach a human.  Fail closed and
  -- raise, exactly as the classifier does, so the whole tick rolls back and the
  -- bundle is left untouched.
  if pg_catalog.upper(pg_catalog.btrim(coalesce(p_census->>'result','')))='FROZEN' then
    raise exception 'WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL',
              'reason','A_FROZEN_CENSUS_RESULT_MUST_NEVER_SPEND_THE_TECHNICAL_FAILURE_BUDGET',
              'ruling','HANDOVER 2 round 7, A5',
              'pending_bundle_id',p_pending_bundle_id,
              'refusal_code',p_code)::text;
  end if;

  select pending_row.technical_failure_count+1 into v_count
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=p_pending_bundle_id
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'retryable',false,
      'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;

  if v_count>=10 then
    -- Ten CONSECUTIVE technical failures.  The old head stays current and no
    -- Banking state is touched.  Round-4 ruling 4 requires the Office-visible
    -- reason to be clear and specific, because the unmarked VOIDED transfer
    -- case will be common until Banking Pay ships its classifier change.
    v_reason:=private.weekly_source_pending_release_review_reason_v1(
      p_code,
      p_code||' after '||v_count::text||' consecutive technical failures. '
        ||'The previous entitlement is still current and no Banking Pay row was '
        ||'touched. Office review is needed before this decision can publish.',
      p_detail,
      v_count);
    update public.weekly_source_pending_entitlement_bundles
       set state='MANUAL_REVIEW',
           pending_revision=pending_revision+1,
           technical_failure_count=v_count,
           manual_review_reason=v_reason,
           lease_owner=null,lease_token=null,lease_worker_run_id=null,
           lease_expires_at_utc=null,
           next_check_at_utc=v_now,
           last_census_json=coalesce(p_census,last_census_json)-'watch',
           updated_at_utc=v_now
     where id=p_pending_bundle_id
    returning pending_revision into v_revision;
    -- B4.4: a manual review reached through the budget carries the same
    -- lossless child records as one reached directly.
    v_items:=private.weekly_source_pending_release_record_review_items_v1(
      p_pending_bundle_id,v_revision,p_code,p_detail,p_census);
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'retryable',false,
      'code',p_code,'outcome','MANUAL_REVIEW','state','MANUAL_REVIEW',
      'pending_bundle_id',p_pending_bundle_id,
      'technical_failure_count',v_count,
      'manual_review_reason',v_reason,
      'review_items',v_items,
      'detail',p_detail);
  end if;

  v_next:=v_now+private.weekly_source_pending_release_backoff_v1(v_count);
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',
         pending_revision=pending_revision+1,
         technical_failure_count=v_count,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=v_next,
         -- B4.2: a technical failure is not a proved frozen wait, so the bundle
         -- stops being watch-eligible and the next tick takes the full
         -- claim/apply path again.
         last_census_json=coalesce(p_census,last_census_json)-'watch',
         updated_at_utc=v_now
   where id=p_pending_bundle_id;
  return pg_catalog.jsonb_build_object(
    'ok',false,'released',false,'retryable',true,
    'code',p_code,'outcome','TECHNICAL_FAILURE','state','PENDING',
    'pending_bundle_id',p_pending_bundle_id,
    'technical_failure_count',v_count,
    'next_check_at_utc',v_next,
    'detail',p_detail);
end;
$function$;

create or replace function private.weekly_source_pending_release_manual_review_v1(
  p_pending_bundle_id uuid,
  p_reason text,
  p_detail jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_revision bigint;
  v_items jsonb;
  -- Review finding F3: one builder for the Office-visible reason, so a manual
  -- review reached this way carries the same structured item identifiers as one
  -- reached through the technical-failure threshold.
  v_reason text:=private.weekly_source_pending_release_review_reason_v1(
    pg_catalog.split_part(coalesce(p_reason,''),':',1),
    coalesce(nullif(pg_catalog.btrim(coalesce(p_reason,'')),''),
             'WEEKLY_SOURCE_PENDING_RELEASE_MANUAL_REVIEW'),
    p_detail,
    null::integer);
begin
  update public.weekly_source_pending_entitlement_bundles
     set state='MANUAL_REVIEW',
         pending_revision=pending_revision+1,
         manual_review_reason=v_reason,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=v_now,
         -- B4.2: a bundle that has left PENDING is no longer a frozen watch.
         last_census_json=last_census_json-'watch',
         updated_at_utc=v_now
   where id=p_pending_bundle_id
  returning pending_revision into v_revision;
  if v_revision is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'retryable',false,
      'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;
  -- B4.4: the exact census item identifiers are written losslessly to the
  -- child relation here, for EVERY manual review, however it was reached.
  v_items:=private.weekly_source_pending_release_record_review_items_v1(
    p_pending_bundle_id,v_revision,
    pg_catalog.split_part(coalesce(p_reason,''),':',1),
    p_detail,p_detail->'census');
  return pg_catalog.jsonb_build_object(
    'ok',false,'released',false,'retryable',false,
    'code','WEEKLY_SOURCE_PENDING_BUNDLE_MANUAL_REVIEW',
    'outcome','MANUAL_REVIEW','state','MANUAL_REVIEW',
    'pending_bundle_id',p_pending_bundle_id,
    'manual_review_reason',v_reason,
    'review_items',v_items,
    'detail',p_detail);
end;
$function$;

create or replace function private.weekly_source_pending_release_superseded_v1(
  p_pending_bundle_id uuid,
  p_code text,
  p_detail jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_now timestamptz:=pg_catalog.clock_timestamp();
begin
  update public.weekly_source_pending_entitlement_bundles
     set state='SUPERSEDED',
         pending_revision=pending_revision+1,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=v_now,
         updated_at_utc=v_now
   where id=p_pending_bundle_id;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'released',false,'retryable',false,
      'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',false,'released',false,'retryable',false,
    'code',coalesce(p_code,'WEEKLY_SOURCE_PENDING_BUNDLE_SUPERSEDED'),
    'outcome','SUPERSEDED','state','SUPERSEDED',
    'pending_bundle_id',p_pending_bundle_id,
    'detail',p_detail);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 5a. HANDOVER 2 round-5 ruling B4.1 - the ONE place a refusal is dispositioned
-- ---------------------------------------------------------------------------
-- Every refusal on the release path now arrives here, and the answer is one of
-- two things:
--
--   * TRANSIENT - `…_transient_v1` PROVED it can clear on its own, so it takes
--     the ordinary technical-failure budget and the exponential backoff, and a
--     human sees it only after ten consecutive ones;
--   * PERMANENT - anything else, and that includes every structural refusal,
--     every integrity refusal and every digest disagreement.  It reaches Office
--     manual review on THIS tick.
--
-- Ruling A1 control 5 (and WP-02b handoff N1) is honoured before the classifier
-- is consulted at all: the coordinator now labels a conflicting replay and a
-- tampered row machine-readably, with `detail.disposition='MANUAL_REVIEW'` and
-- `detail.integrity_failure=true`, and this owner ACTS ON THAT LABEL rather
-- than re-deriving the decision from the code.  Both are read three-valued
-- (Part 1 review rule 4): absent, JSON null, a string and any non-boolean all
-- mean "the coordinator did not say so", and the hand-kept code list that
-- follows is then only a backstop for owners that have not adopted the label.
--
-- Nothing here publishes, and the disposition never changes what is released -
-- only whether a decision that cannot publish waits in a queue or appears on a
-- screen.
--
-- ===========================================================================
-- HANDOVER 2 round-7 ruling A5 - A FROZEN STATE IS NOT A REFUSAL
-- ===========================================================================
-- "`FROZEN / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` is not a refusal and
--  must never enter the permanence classifier.  It neither increments nor
--  resets the technical-failure counter, publishes a head, writes a completion
--  receipt nor causes immediate manual review.  The permanence classifier
--  begins only after a genuine refused attempt has been produced."
--
-- Before this ruling was implemented the classifier accepted a frozen state and
-- was NOT harmless: handed `FROZEN / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED`
-- it returned `refusal_disposition = PERMANENT` and moved the bundle straight
-- to `MANUAL_REVIEW` on that tick - the exact escalation round-4 ruling 3
-- forbids for an ordinary waiting state.  Measured on a build from empty, the
-- bundle went `PENDING -> MANUAL_REVIEW` with the counter left at 3.
--
-- The classifier now refuses to classify a frozen state at all.  It RAISES
-- rather than returning a disposition, because:
--
--   * there is no correct disposition for a non-refusal.  Returning TRANSIENT
--     would spend the retry budget a frozen wait must not spend; returning
--     PERMANENT is the defect above.  Both are wrong answers to a question that
--     must not have been asked;
--   * raising rolls the whole tick back, so the frozen bundle is left exactly
--     as it was - no head, no receipt, no transition, no counter movement,
--     which is precisely what ruling A5 requires of a frozen result;
--   * a defect that raises is found.  A defect that returns "something
--     harmless" is the failure mode this programme has already met twice.
--
-- The test is on the CENSUS VERDICT, never on the mere presence of a
-- `WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` item: a census carrying one such
-- ACTIVE item AND a genuine `CENSUS_ERROR` item is a real refusal whose verdict
-- is `CENSUS_ERROR`, and it must still escalate.  Only a census whose own
-- `result` is `FROZEN`, or a refusal code that names the frozen class itself,
-- is refused entry here.
create or replace function private.weekly_source_pending_release_refuse_v1(
  p_pending_bundle_id uuid,
  p_code text,
  p_detail jsonb,
  p_census jsonb default null,
  p_sqlstate text default null
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_code text:=coalesce(nullif(pg_catalog.btrim(coalesce(p_code,'')),''),
                        'WEEKLY_SOURCE_PENDING_RELEASE_UNKNOWN');
  v_detail jsonb:=case when pg_catalog.jsonb_typeof(coalesce(p_detail,'null'::jsonb))='object'
                       then p_detail else pg_catalog.jsonb_build_object('detail',p_detail) end;
  v_coordinator_says_review boolean;
  v_transient boolean;
  v_census_result text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_census->>'result','')));
  v_frozen_codes constant text[]:=array[
    'FROZEN',
    'WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED',
    'WEEKLY_SOURCE_PENDING_RELEASE_FROZEN',
    'WEEKLY_SOURCE_CENSUS_FROZEN'
  ];
begin
  -- Round-7 ruling A5.  The permanence classifier begins only after a genuine
  -- refused attempt has been produced.  A frozen state reaching here is a
  -- defect in the CALLER, so it is reported as one and nothing is written.
  if v_census_result='FROZEN'
     or pg_catalog.upper(pg_catalog.btrim(v_code))=any(v_frozen_codes) then
    raise exception 'WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_FROZEN_STATE_IS_NOT_A_REFUSAL',
              'reason','A_FROZEN_CENSUS_RESULT_MUST_NEVER_ENTER_THE_PERMANENCE_CLASSIFIER',
              'ruling','HANDOVER 2 round 7, A5',
              'pending_bundle_id',p_pending_bundle_id,
              'refusal_code',v_code,
              'census_result',nullif(v_census_result,''))::text,
            hint='A FROZEN census is an ordinary waiting state: advance '
                 ||'next_check_at_utc and leave the technical-failure counter '
                 ||'alone. It is neither a refusal nor a technical failure.';
  end if;

  -- Ruling A1 control 5 / WP-02b N1, read three-valued.
  v_coordinator_says_review:=
       (v_detail->>'disposition')='MANUAL_REVIEW'
    or (pg_catalog.jsonb_typeof(coalesce(v_detail->'integrity_failure','null'::jsonb))='boolean'
        and coalesce((v_detail->>'integrity_failure')::boolean,false) is true)
    or (v_detail->'detail'->>'disposition')='MANUAL_REVIEW'
    or (pg_catalog.jsonb_typeof(coalesce(v_detail->'detail'->'integrity_failure','null'::jsonb))='boolean'
        and coalesce((v_detail->'detail'->>'integrity_failure')::boolean,false) is true);

  v_transient:=case when coalesce(v_coordinator_says_review,false) then false
                    else private.weekly_source_pending_release_transient_v1(
                           v_code,v_detail,p_sqlstate) end;

  if coalesce(v_transient,false) then
    return private.weekly_source_pending_release_technical_failure_v1(
             p_pending_bundle_id,v_code,
             v_detail||pg_catalog.jsonb_build_object(
               'refusal_disposition','TRANSIENT',
               'refusal_sqlstate',nullif(pg_catalog.btrim(coalesce(p_sqlstate,'')),'')),
             p_census)
           ||pg_catalog.jsonb_build_object('refusal_disposition','TRANSIENT');
  end if;

  return private.weekly_source_pending_release_manual_review_v1(
           p_pending_bundle_id,
           v_code||': '||coalesce(
             nullif(pg_catalog.btrim(coalesce(v_detail->'detail'->>'reason','')),''),
             nullif(pg_catalog.btrim(coalesce(v_detail->>'reason','')),''),
             '<no reason>'),
           v_detail
             ||pg_catalog.jsonb_build_object(
                 'refusal_disposition','PERMANENT',
                 'refusal_sqlstate',nullif(pg_catalog.btrim(coalesce(p_sqlstate,'')),''),
                 'escalated_immediately',true)
             ||case when p_census is null then '{}'::jsonb
                    else pg_catalog.jsonb_build_object('census',p_census) end)
         ||pg_catalog.jsonb_build_object(
             'refusal_disposition','PERMANENT','code',v_code);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 5b. HANDOVER 2 round-5 ruling B4.4 - the bounded, pageable reader
-- ---------------------------------------------------------------------------
-- "…retained losslessly in bounded, pageable child records and shown through
-- the Office detail view."  This is the pageable half.  The page size is
-- clamped INSIDE the function (1..200, default 50) exactly as the claim page
-- clamps its own bounds, so no caller can ask for an unbounded page; the true
-- total is always returned beside the page, so the Office can page to the last
-- identifier and know that it has.
--
-- `p_pending_revision` null means "the latest generation that has evidence",
-- which is what an Office screen showing the current manual review wants.
--
-- NOT YET REACHABLE FROM POSTGREST.  The Weekly Source ACL contract's service
-- RPC allowlist is a closed set verified in both directions
-- (`supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql`),
-- and it lives in a file this package does not own, so a new granted
-- `public.` wrapper would fail that verifier for every package's build.  The
-- exact wrapper and its contract entry are in `IMPL\handoffs\WP-08c_NEEDS.md`.
create or replace function private.weekly_source_pending_release_review_items_page_v1(
  p_pending_bundle_id uuid,
  p_pending_revision bigint default null,
  p_offset integer default 0,
  p_limit integer default 50
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_limit integer:=least(greatest(coalesce(p_limit,50),1),200);
  v_offset integer:=greatest(coalesce(p_offset,0),0);
  v_revision bigint;
  v_total integer;
  v_items jsonb;
begin
  if p_pending_bundle_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'items','[]'::jsonb,'total_count',0,'offset',v_offset,'limit',v_limit,
      'has_more',false);
  end if;

  v_revision:=p_pending_revision;
  if v_revision is null then
    select pg_catalog.max(stored.pending_revision) into v_revision
      from private.weekly_source_pending_release_review_items as stored
     where stored.pending_bundle_id=p_pending_bundle_id;
  end if;

  select pg_catalog.count(*) into v_total
    from private.weekly_source_pending_release_review_items as stored
   where stored.pending_bundle_id=p_pending_bundle_id
     and stored.pending_revision=v_revision;

  select coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'item_ordinal',page_row.item_ordinal,
             'item_class',page_row.item_class,
             'item_reason',page_row.item_reason,
             'pay_batch_item_id',page_row.pay_batch_item_id,
             'timesheet_id',page_row.timesheet_id,
             'refusal_code',page_row.refusal_code,
             'census_result',page_row.census_result,
             'item_json',page_row.item_json,
             'recorded_at_utc',page_row.recorded_at_utc)
           order by page_row.item_ordinal),'[]'::jsonb)
    into v_items
    from (
      select stored.*
        from private.weekly_source_pending_release_review_items as stored
       where stored.pending_bundle_id=p_pending_bundle_id
         and stored.pending_revision=v_revision
       order by stored.item_ordinal
       offset v_offset
       limit v_limit
    ) as page_row;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'pending_bundle_id',p_pending_bundle_id,
    'pending_revision',v_revision,
    'offset',v_offset,
    'limit',v_limit,
    'total_count',coalesce(v_total,0),
    'returned_count',pg_catalog.jsonb_array_length(v_items),
    'has_more',v_offset+pg_catalog.jsonb_array_length(v_items)<coalesce(v_total,0),
    'next_offset',case when v_offset+pg_catalog.jsonb_array_length(v_items)<coalesce(v_total,0)
                       then v_offset+pg_catalog.jsonb_array_length(v_items) end,
    'items',v_items);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 6. Round-4 ruling 6 point 7 - recording a rolled-back release transaction
-- ---------------------------------------------------------------------------
-- "Any token disagreement, unexpected scope, invalidation failure, receipt
-- failure or timeout - including retryable 55P03 - rolls back the entire
-- release transaction."  A rollback also discards any counter increment made
-- inside it, so the Worker records the failure through this owner in a FRESH
-- transaction, on that bundle only.  It publishes nothing and never inspects
-- the coordinator's work; it moves only the bundle's own lifecycle columns.
--
-- The lease is accepted while it is this Worker run's lease, expired or not:
-- the transaction that raised held a valid lease, and the round trip that
-- records the failure must not be defeated by the clock.  A different worker,
-- token or run id is refused.
create or replace function private.weekly_source_pending_entitlement_release_record_failure_v1(
  p_pending_bundle_id uuid,
  p_lease_token uuid,
  p_worker_id text,
  p_worker_run_id uuid,
  p_code text,
  p_detail text
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_bundle record;
  v_code text:=pg_catalog.left(
    coalesce(nullif(pg_catalog.btrim(coalesce(p_code,'')),''),
                        'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK'),200);
  v_detail text:=pg_catalog.left(coalesce(p_detail,''),1000);
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  select * into v_bundle
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=p_pending_bundle_id
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;

  -- A bundle that reached RELEASED before the response was lost is never
  -- turned back into a failure.
  if v_bundle.state='RELEASED' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'recorded',false,'state','RELEASED',
      'pending_bundle_id',p_pending_bundle_id);
  end if;
  if v_bundle.state<>'RELEASING'
     or v_bundle.lease_token is distinct from p_lease_token
     or v_bundle.lease_worker_run_id is distinct from p_worker_run_id
     or v_bundle.lease_owner is distinct from pg_catalog.btrim(coalesce(p_worker_id,'')) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'recorded',false,
      'code','WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
      'pending_bundle_id',p_pending_bundle_id,
      'state',v_bundle.state);
  end if;

  -- HANDOVER 2 round-5 ruling B4.1.  A rolled-back release transaction used to
  -- be a technical failure unconditionally, so a `23514`, a `42883` or a raised
  -- `P0001` - none of which can ever clear - spent ten attempts and about
  -- ninety minutes before a human saw it.  The disposition owner now decides,
  -- and only a PROVED transient condition spends the budget.
  --
  -- The Worker reports two FACTS and classifies nothing (`proof/32` section 2):
  -- the SQLSTATE PostgreSQL returned, and the bounded kind of failure it
  -- observed from outside the database (`TIMEOUT`, `NETWORK`,
  -- `DATABASE_ERROR`).  They arrive inside the bounded detail string in the
  -- fixed `SQLSTATE=xxxxx; KIND=xxxx;` form the public wrapper composes, which
  -- is why the private signature is unchanged; the classifier reads them and
  -- the Worker never sees the answer.  An absent or unlisted SQLSTATE is
  -- permanent, which is the ruling's stated default.
  return private.weekly_source_pending_release_refuse_v1(
    p_pending_bundle_id,v_code,
    pg_catalog.jsonb_build_object(
      'source','WORKER_TRANSACTION_ROLLED_BACK',
      'worker_id',v_bundle.lease_owner,
      'worker_run_id',v_bundle.lease_worker_run_id,
      'detail',v_detail),
    null::jsonb,
    v_detail)
    ||pg_catalog.jsonb_build_object('recorded',true);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 6a. HANDOVER 2 round-7 ruling A1 - WHICH KIND OF REOPEN MAY RESET THE COUNTER
-- ---------------------------------------------------------------------------
-- "An audited Office reopen may reset the counter only when it creates a new
--  immutable superseding decision/generation with its own actor, reason and
--  receipt.  A status-only reopen must not reset it.  Add this as the third
--  named permitted cause beside the two proved RELEASED paths."
--
-- Round-5 ruling B4.3 named two permitted causes - positively proved successful
-- progress, and a new superseding decision/generation.  Round 7 adds the third
-- and, more importantly, LIMITS it: "that it was a reopen" is no longer enough;
-- the reopen has to have created the superseding generation.
--
-- Before this ruling was implemented, measured on a build from empty:
--
--   * the audited Office reopen reset `technical_failure_count` 7 -> 0
--     unconditionally, carrying no generation of its own and no receipt beyond
--     an ordinary audit row; and
--   * a STATUS-ONLY write - `update … set state='PENDING',
--     technical_failure_count=0` straight out of MANUAL_REVIEW, with no actor,
--     no reason, no generation and no receipt - was ACCEPTED by the database,
--     taking the counter 9 -> 0.
--
-- Nothing distinguished the two.  The old proof of this rule was a TEXT COUNT
-- over `pg_get_functiondef` asserting that the string `technical_failure_count=0`
-- appeared exactly three times, which cannot tell one kind of reopen from
-- another and would have passed unchanged through both defects above.
--
-- This guard is the executed replacement.  It sits on the relation, so it binds
-- every writer - the owners in this file, a hand-written UPDATE, a future
-- package and an Office script alike - and it names the three permitted causes
-- explicitly:
--
--   1 and 2. the two proved RELEASED paths.  `state` becomes `RELEASED`, the
--      four proof/32 section 8 step 5 release facts are all present, and the
--      named publication receipt EXISTS - checked by explicit cardinality, not
--      by `limit 1` and not by trusting the id;
--   3. the audited Office reopen that created a new immutable superseding
--      generation.  Proved by exactly one reopen receipt naming this bundle AND
--      this exact new `pending_revision` as its reopen generation, carrying its
--      own actor and its own reason.  A status-only reopen writes no such
--      receipt and is therefore refused.
--
-- Anything else raises.  The guard fires on any DECREASE of the counter, not
-- only on a write of literal zero: a partial rewind is a reset this ruling does
-- not name either, and a guard that only saw `=0` would be a text rule wearing
-- a trigger's clothes.
--
-- Safety-property discipline (Part 1 rule 5): every cardinality here is an
-- explicit `count(*)` compared to 1.  No `limit`, no `order by`, and no appeal
-- to a unique index.  Two candidate receipts route to the fail-closed branch
-- exactly as zero does.
create or replace function private._weekly_source_pending_counter_reset_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_receipts integer;
  v_reopen_receipts integer;
begin
  -- Only a DECREASE is governed.  An increment is the technical-failure owner's
  -- own write and B4.3 does not restrict it; an unchanged counter is not a
  -- reset at all.
  if coalesce(new.technical_failure_count,0)>=coalesce(old.technical_failure_count,0) then
    return new;
  end if;

  -- Causes 1 and 2: a proved RELEASED path.
  if new.state='RELEASED'
     and new.released_receipt_id is not null
     and new.released_receipt_digest is not null
     and new.released_by_worker_id is not null
     and new.released_by_worker_run_id is not null
     and new.released_at_utc is not null then
    select pg_catalog.count(*) into v_receipts
      from private.weekly_source_entitlement_publication_receipts as receipt_row
     where receipt_row.id=new.released_receipt_id;
    if v_receipts=1 then
      return new;
    end if;
    raise exception 'WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED',
              'reason','A_RELEASED_RESET_MUST_NAME_EXACTLY_ONE_INSTALLED_PUBLICATION_RECEIPT',
              'ruling','HANDOVER 2 round 5 B4.3 as limited by round 7 A1',
              'pending_bundle_id',new.id,
              'released_receipt_id',new.released_receipt_id,
              'matching_receipts',v_receipts)::text;
  end if;

  -- Cause 3: the audited Office reopen that created a new immutable
  -- superseding generation with its own actor, reason and receipt.  The
  -- receipt must name THIS bundle and THIS new pending_revision as its reopen
  -- generation, and must carry a real actor and a non-empty reason.
  select pg_catalog.count(*) into v_reopen_receipts
    from public.audit_events as audit_row
   where audit_row.object_type='weekly_source_pending_entitlement_bundles'
     and audit_row.object_id_text=new.id::text
     and audit_row.action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'
     and audit_row.actor_user_id is not null
     and pg_catalog.btrim(coalesce(audit_row.reason,''))<>''
     and pg_catalog.jsonb_typeof(coalesce(audit_row.after_json,'null'::jsonb))='object'
     and audit_row.after_json->>'reopen_generation'=new.pending_revision::text
     and audit_row.after_json->>'supersedes_pending_revision'=old.pending_revision::text;
  if v_reopen_receipts=1 then
    return new;
  end if;

  raise exception 'WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'code','WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED',
            'reason','ONLY_A_PROVED_RELEASE_OR_A_SUPERSEDING_AUDITED_REOPEN_MAY_RESET_THE_COUNTER',
            'ruling','HANDOVER 2 round 7, A1',
            'pending_bundle_id',new.id,
            'state_before',old.state,'state_after',new.state,
            'technical_failure_count_before',old.technical_failure_count,
            'technical_failure_count_after',new.technical_failure_count,
            'matching_reopen_receipts',v_reopen_receipts)::text,
          hint='A status-only reopen must carry the failure count forward. '
               ||'Use public.weekly_source_pending_entitlement_bundle_reopen_v1, '
               ||'which creates the immutable superseding reopen generation, '
               ||'its actor, its reason and its receipt.';
end;
$function$;

alter function private._weekly_source_pending_counter_reset_guard_v1() owner to postgres;
revoke all on function private._weekly_source_pending_counter_reset_guard_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists weekly_source_pending_counter_reset_guard
  on public.weekly_source_pending_entitlement_bundles;
create trigger weekly_source_pending_counter_reset_guard
before update on public.weekly_source_pending_entitlement_bundles
for each row
when (new.technical_failure_count < old.technical_failure_count)
execute function private._weekly_source_pending_counter_reset_guard_v1();

-- ---------------------------------------------------------------------------
-- 7. G5-6 - the audited Office reopen
-- ---------------------------------------------------------------------------
-- proof/32 sections 2 and 10: MANUAL_REVIEW -> PENDING, writes one audit row
-- through the existing Weekly Source audit relation, and NEVER releases.  The
-- next claim reruns the complete current census from the beginning.
--
-- HANDOVER 2 round-7 ruling A1.  The reset is no longer a side effect of the
-- status change.  This owner now, in one transaction and in this order:
--
--   1. locks the bundle row and reads the guarded state under that lock;
--   2. allocates the new reopen GENERATION - the bundle's next
--      `pending_revision` - and records its predecessor;
--   3. writes the immutable superseding receipt FIRST, carrying its own actor,
--      its own reason, the generation and the predecessor link, and proves by
--      explicit cardinality that exactly one such receipt now exists;
--   4. only then writes the status change AND the reset, which the relation's
--      own guard re-proves independently of this owner.
--
-- If step 3 does not produce exactly one receipt the owner refuses and nothing
-- is written: the fail-closed direction is a reopen that did not happen, never
-- a reopen whose counter was silently reset.
create or replace function public.weekly_source_pending_entitlement_bundle_reopen_v1(
  p_pending_bundle_id uuid,
  p_reason text,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_bundle record;
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_reason text:=pg_catalog.left(pg_catalog.btrim(coalesce(p_reason,'')),1000);
  v_previous_reason text;
  v_previous_count integer;
  -- Round-7 ruling A1: the superseding reopen generation and its receipt.
  v_reopen_generation bigint;
  v_predecessor_generation bigint;
  v_receipt_id uuid;
  v_receipt_count integer;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_pending_bundle_id is null or p_actor_user_id is null or v_reason='' then
    raise exception 'WEEKLY_SOURCE_PENDING_BUNDLE_REOPEN_REQUEST_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.tms_users actor
                 where actor.id=p_actor_user_id and actor.is_active) then
    raise exception 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' using errcode='42501';
  end if;

  select * into v_bundle
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=p_pending_bundle_id
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_FOUND',
      'pending_bundle_id',p_pending_bundle_id);
  end if;
  if v_bundle.state<>'MANUAL_REVIEW' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_PENDING_BUNDLE_NOT_IN_MANUAL_REVIEW',
      'pending_bundle_id',p_pending_bundle_id,'state',v_bundle.state);
  end if;

  v_previous_reason:=v_bundle.manual_review_reason;
  v_previous_count:=v_bundle.technical_failure_count;

  -- ---- round-7 ruling A1, step 2: allocate the superseding generation -----
  -- The row is held FOR UPDATE, so no other writer can move `pending_revision`
  -- between this read and the update below.  The new generation IS the next
  -- pending_revision, and the predecessor link is the one it supersedes.
  v_predecessor_generation:=v_bundle.pending_revision;
  v_reopen_generation:=v_bundle.pending_revision+1;

  -- ---- round-7 ruling A1, step 3: the immutable superseding receipt -------
  -- Written BEFORE the status change and the reset, so the relation's own guard
  -- can prove the reset was authorised without trusting this owner.  It carries
  -- its own actor, its own reason, the generation and the predecessor link.
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,
    object_type,object_id_text,action,before_json,after_json,reason
  )
  select
    pg_catalog.statement_timestamp(),p_actor_user_id,actor.display_name,actor.role,
    'weekly_source_pending_entitlement_bundles',p_pending_bundle_id::text,
    'WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED',
    pg_catalog.jsonb_build_object(
      'state','MANUAL_REVIEW',
      'technical_failure_count',v_previous_count,
      'manual_review_reason',v_previous_reason,
      'pending_revision',v_predecessor_generation),
    pg_catalog.jsonb_build_object(
      'state','PENDING','technical_failure_count',0,
      'decision_bundle_id',v_bundle.decision_bundle_id,
      'bundle_revision',v_bundle.bundle_revision,
      'candidate_id',v_bundle.candidate_id,
      -- Round-7 ruling A1.  These three members ARE the superseding
      -- decision/generation: the generation this reopen creates, the generation
      -- it supersedes, and the explicit statement that this reopen is the
      -- third named permitted cause of a counter reset rather than a
      -- status-only flip.  The relation guard reads exactly these.
      'reopen_generation',v_reopen_generation,
      'supersedes_pending_revision',v_predecessor_generation,
      'counter_reset_cause','SUPERSEDING_AUDITED_OFFICE_REOPEN',
      'technical_failure_count_before',v_previous_count),
    v_reason
  from public.tms_users actor
  where actor.id=p_actor_user_id
  returning id into v_receipt_id;
  if v_receipt_id is null then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;

  -- Explicit cardinality, never `limit 1` and never a unique index (Part 1
  -- rule 5).  Zero and two both route to the fail-closed branch.
  select pg_catalog.count(*) into v_receipt_count
    from public.audit_events as audit_row
   where audit_row.object_type='weekly_source_pending_entitlement_bundles'
     and audit_row.object_id_text=p_pending_bundle_id::text
     and audit_row.action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'
     and audit_row.actor_user_id is not null
     and pg_catalog.btrim(coalesce(audit_row.reason,''))<>''
     and pg_catalog.jsonb_typeof(coalesce(audit_row.after_json,'null'::jsonb))='object'
     and audit_row.after_json->>'reopen_generation'=v_reopen_generation::text
     and audit_row.after_json->>'supersedes_pending_revision'=v_predecessor_generation::text;
  if v_receipt_count<>1 then
    -- Fail closed: a reopen that could not create exactly one superseding
    -- generation is not a superseding reopen, so it must not reset the counter
    -- and must not happen at all.
    raise exception 'WEEKLY_SOURCE_PENDING_BUNDLE_REOPEN_RECEIPT_NOT_UNIQUE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PENDING_BUNDLE_REOPEN_RECEIPT_NOT_UNIQUE',
              'reason','A_REOPEN_MUST_CREATE_EXACTLY_ONE_SUPERSEDING_GENERATION_RECEIPT',
              'ruling','HANDOVER 2 round 7, A1',
              'pending_bundle_id',p_pending_bundle_id,
              'reopen_generation',v_reopen_generation,
              'matching_receipts',v_receipt_count)::text;
  end if;

  -- ---- round-7 ruling A1, step 4: the status change AND the reset ---------
  update public.weekly_source_pending_entitlement_bundles
     set state='PENDING',
         pending_revision=pending_revision+1,
         -- The reset is permitted here, and ONLY here, because step 3 created
         -- the superseding generation above.  The relation's own
         -- `weekly_source_pending_counter_reset_guard` re-proves that
         -- independently: if the receipt were missing this statement would
         -- raise `WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED`
         -- and the whole reopen would roll back.
         technical_failure_count=0,
         manual_review_reason=null,
         lease_owner=null,lease_token=null,lease_worker_run_id=null,
         lease_expires_at_utc=null,
         next_check_at_utc=v_now,
         -- HANDOVER 2 round-5 ruling B4.2 and B4.3.  The reopen is the Office's
         -- superseding decision to try again, so it resets the counter (above)
         -- AND drops any frozen watch marker: a reopened bundle always gets one
         -- full claim/apply cycle, under the serial gate and the family locks,
         -- with its real transition audited, before it may go back to being
         -- watched.  Without this a reopened bundle would resume the cheap poll
         -- and the Office would get no record that its reopen was acted on.
         last_census_json=coalesce(last_census_json,'{}'::jsonb)-'watch',
         updated_at_utc=v_now
   where id=p_pending_bundle_id
     and pending_revision=v_predecessor_generation;
  if not found then
    raise exception 'WEEKLY_SOURCE_PENDING_BUNDLE_REOPEN_GENERATION_CONFLICT'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PENDING_BUNDLE_REOPEN_GENERATION_CONFLICT',
              'reason','THE_PREDECESSOR_GENERATION_MOVED_UNDER_THE_LOCK',
              'pending_bundle_id',p_pending_bundle_id,
              'expected_pending_revision',v_predecessor_generation)::text;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'released',false,'state','PENDING',
    'pending_bundle_id',p_pending_bundle_id,
    'technical_failure_count',0,
    'previous_technical_failure_count',v_previous_count,
    'previous_manual_review_reason',v_previous_reason,
    -- Round-7 ruling A1: the superseding generation and its receipt are part of
    -- the answer, so a caller (and the Office) can see WHICH kind of reopen
    -- this was.
    'reopen_generation',v_reopen_generation,
    'supersedes_pending_revision',v_predecessor_generation,
    'reopen_receipt_id',v_receipt_id,
    'counter_reset_cause','SUPERSEDING_AUDITED_OFFICE_REOPEN',
    'next_check_at_utc',v_now);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 8. PostgREST transport for the delivery Worker
-- ---------------------------------------------------------------------------
-- The broker reaches the database only through PostgREST in the `public`
-- schema (`broker/src/index.js` -> `sbRpc`), so the two private owners the
-- contract names get the established Weekly Source transport shape, exactly as
-- `public.weekly_source_query_scheduler_tick_v1(jsonb)` and
-- `public.weekly_source_message_dispatch_target_claim_v1(jsonb)` already have.
-- Each wrapper validates the request keys strictly and adds no logic.
create or replace function public.weekly_source_pending_entitlement_release_claim_page_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) as key
                where key not in ('worker_id','worker_run_id','lease_seconds','limit')) then
    raise exception 'WEEKLY_SOURCE_RELEASE_CLAIM_REQUEST_INVALID' using errcode='22023';
  end if;
  return private.weekly_source_pending_entitlement_release_claim_page_v1(
    p_request->>'worker_id',
    nullif(p_request->>'worker_run_id','')::uuid,
    nullif(p_request->>'lease_seconds','')::integer,
    nullif(p_request->>'limit','')::integer);
end;
$function$;

create or replace function public.weekly_source_pending_entitlement_release_apply_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) as key
                where key not in ('pending_bundle_id','expected_pending_revision',
                                  'expected_request_digest','worker_id',
                                  'lease_token','worker_run_id')) then
    raise exception 'WEEKLY_SOURCE_RELEASE_APPLY_REQUEST_INVALID' using errcode='22023';
  end if;
  if coalesce(p_request->>'expected_request_digest','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_RELEASE_APPLY_REQUEST_INVALID' using errcode='22023';
  end if;
  return private.weekly_source_pending_entitlement_release_apply_v1(
    nullif(p_request->>'pending_bundle_id','')::uuid,
    nullif(p_request->>'expected_pending_revision','')::bigint,
    pg_catalog.decode(p_request->>'expected_request_digest','hex'),
    p_request->>'worker_id',
    nullif(p_request->>'lease_token','')::uuid,
    nullif(p_request->>'worker_run_id','')::uuid);
end;
$function$;

create or replace function public.weekly_source_pending_entitlement_release_record_failure_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  -- HANDOVER 2 round-5 ruling B4.1.  `sqlstate` and `failure_kind` are the two
  -- FACTS the Worker may report about a rolled-back release transaction; the
  -- database classifies them and the Worker never does.  They are normalised
  -- into the fixed `SQLSTATE=xxxxx; KIND=xxxx;` prefix of the bounded detail
  -- string so the private owner's registered signature does not change: adding
  -- a defaulted seventh parameter would leave the six-argument form ambiguous
  -- on any database that already has the old function installed.
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) as key
                where key not in ('pending_bundle_id','lease_token','worker_id',
                                  'worker_run_id','code','detail',
                                  'sqlstate','failure_kind')) then
    raise exception 'WEEKLY_SOURCE_RELEASE_FAILURE_REQUEST_INVALID' using errcode='22023';
  end if;
  if coalesce(p_request->>'sqlstate','') <> ''
     and coalesce(p_request->>'sqlstate','') !~ '^[0-9A-Za-z]{5}$' then
    raise exception 'WEEKLY_SOURCE_RELEASE_FAILURE_REQUEST_INVALID' using errcode='22023';
  end if;
  if coalesce(p_request->>'failure_kind','') <> ''
     and coalesce(p_request->>'failure_kind','')
         not in ('TIMEOUT','NETWORK','DATABASE_ERROR','UNKNOWN') then
    raise exception 'WEEKLY_SOURCE_RELEASE_FAILURE_REQUEST_INVALID' using errcode='22023';
  end if;
  return private.weekly_source_pending_entitlement_release_record_failure_v1(
    nullif(p_request->>'pending_bundle_id','')::uuid,
    nullif(p_request->>'lease_token','')::uuid,
    p_request->>'worker_id',
    nullif(p_request->>'worker_run_id','')::uuid,
    p_request->>'code',
    'SQLSTATE='||coalesce(nullif(p_request->>'sqlstate',''),'-----')
      ||'; KIND='||coalesce(nullif(p_request->>'failure_kind',''),'UNKNOWN')
      ||'; '||coalesce(p_request->>'detail',''));
end;
$function$;

-- ---------------------------------------------------------------------------
-- 9. Ownership and privileges
-- ---------------------------------------------------------------------------
alter function private.weekly_source_pending_release_backoff_v1(integer) owner to postgres;
alter function private.weekly_source_pending_release_transient_v1(text,jsonb,text) owner to postgres;
alter function private.weekly_source_pending_release_census_signature_v1(jsonb) owner to postgres;
alter function private.weekly_source_pending_release_record_review_items_v1(uuid,bigint,text,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_pending_release_refuse_v1(uuid,text,jsonb,jsonb,text) owner to postgres;
alter function private.weekly_source_pending_release_review_items_page_v1(uuid,bigint,integer,integer) owner to postgres;
alter function private.weekly_source_pending_release_watch_page_v1(text,uuid,integer) owner to postgres;
alter function private.weekly_source_pending_release_lock_and_census_v1(uuid,uuid[],text[],integer[],uuid) owner to postgres;
alter function private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_pending_entitlement_release_claim_page_v1(text,uuid,integer,integer) owner to postgres;
alter function private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid) owner to postgres;
alter function private.weekly_source_pending_release_review_reason_v1(text,text,jsonb,integer) owner to postgres;
alter function private.weekly_source_pending_release_technical_failure_v1(uuid,text,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_pending_release_manual_review_v1(uuid,text,jsonb) owner to postgres;
alter function private.weekly_source_pending_release_superseded_v1(uuid,text,jsonb) owner to postgres;
alter function private.weekly_source_pending_entitlement_release_record_failure_v1(uuid,uuid,text,uuid,text,text) owner to postgres;
alter function public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid) owner to postgres;
alter function public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb) owner to postgres;
alter function public.weekly_source_pending_entitlement_release_apply_v1(jsonb) owner to postgres;
alter function public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_pending_release_backoff_v1(integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_transient_v1(text,jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_census_signature_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_record_review_items_v1(uuid,bigint,text,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_refuse_v1(uuid,text,jsonb,jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_review_items_page_v1(uuid,bigint,integer,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_watch_page_v1(text,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_lock_and_census_v1(uuid,uuid[],text[],integer[],uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_entitlement_release_claim_page_v1(text,uuid,integer,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_review_reason_v1(text,text,jsonb,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_technical_failure_v1(uuid,text,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_manual_review_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_release_superseded_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_pending_entitlement_release_record_failure_v1(uuid,uuid,text,uuid,text,text) from public,anon,authenticated,service_role;

revoke all on function public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid) to service_role;
revoke all on function public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb) to service_role;
revoke all on function public.weekly_source_pending_entitlement_release_apply_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_pending_entitlement_release_apply_v1(jsonb) to service_role;
revoke all on function public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb) to service_role;

comment on function private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb) is
  'Interface I-5. Persists exactly one PENDING Weekly Source entitlement bundle per decision bundle revision, idempotent on the request digest, leaves the old head current, and issues only the bounded stale warning through the existing banking_pay_batch_signal_touch (24 section 4.4; proof/32 section 2).';
comment on function private.weekly_source_pending_entitlement_release_claim_page_v1(text,uuid,integer,integer) is
  'Bounded Weekly Source pending-release claim page: clamps to 25 bundles and 30 to 120 seconds inside the function, claims due PENDING and expired RELEASING bundles with FOR UPDATE SKIP LOCKED (proof/32 section 2).';
comment on function private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid) is
  'Weekly Source deferred release: exact receipt replay before the lease checks, then the section 6 locks, the section 4.0 integrity gate, the section 4 census and the SAME publication coordinator in DEFERRED mode. FROZEN never touches the failure counter; nothing is released by timeout (proof/32 sections 2, 7, 8, 10).';
comment on function private.weekly_source_pending_entitlement_release_record_failure_v1(uuid,uuid,text,uuid,text,text) is
  'Records a rolled-back Weekly Source release transaction as a technical failure on that bundle only, in a fresh transaction (HANDOVER 2 round-4 ruling 6 point 7). It never publishes.';
comment on function public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid) is
  'Audited Office reopen of a Weekly Source pending entitlement bundle: MANUAL_REVIEW to PENDING, drops any frozen watch marker so the next tick runs a full audited attempt, and never releases (proof/32 sections 2 and 10; HANDOVER 2 round-5 rulings B4.2 and B4.3). HANDOVER 2 round-7 ruling A1: it resets the technical failure counter only because it first creates a new immutable superseding reopen generation with its own actor, reason and receipt, proved by explicit cardinality and re-proved at the relation by weekly_source_pending_counter_reset_guard. A status-only reopen creates no such generation and is refused a reset.';
comment on function private._weekly_source_pending_counter_reset_guard_v1() is
  'HANDOVER 2 round-7 ruling A1, enforced at the relation: the technical failure counter of a Weekly Source pending entitlement bundle may fall only through one of three named causes - the two proved RELEASED paths, each naming exactly one installed publication receipt, or an audited Office reopen that created a new immutable superseding generation with its own actor, reason and receipt. A status-only reopen, and any other writer, is refused with WEEKLY_SOURCE_TECHNICAL_FAILURE_COUNT_RESET_UNAUTHORISED.';
comment on function private.weekly_source_pending_release_transient_v1(text,jsonb,text) is
  'HANDOVER 2 round-5 ruling B4.1: the only place a Weekly Source release refusal may be called transient. Proof is the refusing owner''s three-valued retryable=true, a listed transient SQLSTATE or class, or a bounded worker-observed TIMEOUT/NETWORK fact. Everything else, including an absent SQLSTATE, is permanent.';
comment on function private.weekly_source_pending_release_refuse_v1(uuid,text,jsonb,jsonb,text) is
  'HANDOVER 2 round-5 ruling B4.1 and ruling A1 control 5: dispositions every Weekly Source release refusal. It honours the coordinator''s own detail.disposition/integrity_failure label first, then the transient classifier; a permanent refusal reaches Office manual review on this tick instead of spending the ten-attempt budget. It publishes nothing.';
comment on function private.weekly_source_pending_release_watch_page_v1(text,uuid,integer) is
  'HANDOVER 2 round-5 ruling B4.2: the bounded frozen watch. Re-proves an already-FROZEN pending bundle with no serial gate, no lock and no state transition, so a long frozen wait writes a fixed number of audit rows instead of one per tick. It can only wait or hand the bundle back to the ordinary claim path; it never releases.';
comment on function private.weekly_source_pending_release_record_review_items_v1(uuid,bigint,text,jsonb,jsonb) is
  'HANDOVER 2 round-5 ruling B4.4: writes every census item identifier of a manual review losslessly to private.weekly_source_pending_release_review_items, one append-only row per item, and returns the counts so a silent drop is impossible.';
comment on function private.weekly_source_pending_release_review_items_page_v1(uuid,bigint,integer,integer) is
  'HANDOVER 2 round-5 ruling B4.4: the bounded, pageable reader for the Office detail view over a pending bundle''s manual-review census item identifiers. The page size is clamped inside the function and the true total is always returned.';

notify pgrst,'reload schema';

commit;
