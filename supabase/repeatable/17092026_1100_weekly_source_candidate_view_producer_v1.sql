-- Repeatable CloudTMS authority: weekly_source_candidate_view_producer_v1
--
-- Gate 9 producers owned by WP-11a (Plan 6.2 contract section 13).
--
-- G9-6: the MyTMS payload producer.
--   `CandidateWeeklySourceView` exists in the MyTMS API contract
--   (`<mytms>\contracts\candidate-api.openapi.json` `components.schemas`) and is
--   consumed as the nullable `weekly_source_candidate_view` member of
--   `TimesheetDetail`, but nothing produced it: the broker is a straight
--   pass-through of `public.candidate_app_timesheet_detail_v2`
--   (`<backend>\broker\src\candidate-app-backend.js:6122-6138`), so the payload
--   is database-owned.  Until this producer existed every MyTMS Weekly Source
--   branch was inert at runtime (reconciliation report 04 section 6.3).
--
--   Hours only.  `24 section 17` and `16 section 3.3`: MyTMS shows the
--   Candidate's own submitted Timesheet, and `Approved hours to be paid` only
--   when the approved hours differ from it.  It never shows money, rates,
--   payment or recovery history, remittance links, invoice movements, or the
--   words source, protected, exceptional or reconciliation.  Nothing in this
--   file reads an amount, a rate or a Banking Pay relation.
--
-- G9-4 support: the Office `action_state` bridge.
--   `public.weekly_source_office_timesheet_presentation_v1` may only be edited
--   inside its `action_state` object, which is a single jsonb expression, so the
--   Unauthorise verdict it now carries is produced by one call to the bridge
--   below.  The bridge NEVER computes the W1 to W9 checks: it calls the
--   withdrawal owner's own read-only availability function and reports what it
--   returns.  The availability owner is late-bound (it installs in a later
--   repeatable), so an absent owner is reported as an explicit unavailable
--   verdict instead of breaking the whole Office projection.
--
-- Both producers are read-only, `STABLE`, and write nothing anywhere.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- Is this Timesheet a Weekly Source week at all?
--
-- Same test the Office presentation applies: a current WEEKLY HOURS Timesheet
-- on a Contract whose Client is in an active Weekly Source group for that week
-- (`15092026_1534_weekly_source_read_projections_v1.sql`, the applicability
-- prologue).  Anything else is an ordinary Timesheet and produces NULL, so the
-- MyTMS payload is byte-identical to today for every ordinary record.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_week_context_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
begin
  if p_timesheet_id is null then return null; end if;

  select * into v_timesheet from public.timesheets
  where timesheet_id=p_timesheet_id and revoked_at is null;
  if not found then return null; end if;
  if v_timesheet.sheet_scope is distinct from 'WEEKLY'
     or v_timesheet.line_type is distinct from 'HOURS'
     or v_timesheet.contract_id is null then
    return null;
  end if;

  select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  if not found then return null; end if;

  select source_group.* into v_group
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group
    on source_group.id=membership.source_group_id
  where membership.client_id=v_contract.client_id
    and source_group.active
    and v_timesheet.week_ending_date
        between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  if not found then return null; end if;

  -- WP-11d F4.  By this line the week is ALREADY CONFIRMED to be a Weekly
  -- Source week: a current WEEKLY HOURS Timesheet on a Contract whose Client is
  -- in an ACTIVE Weekly Source group covering the week.  A policy that cannot be
  -- resolved for such a week is a CONFIGURATION FAILURE, not an ordinary
  -- non-Weekly-Source week, and the two must not look alike to the caller.
  --
  -- Previously this was `exception when others then return null`, which made an
  -- integrity or configuration failure indistinguishable from "not a Weekly
  -- Source week": MyTMS silently rendered the ordinary Timesheet and the
  -- Candidate lost the `Approved hours to be paid` card and the UI-021 sentence
  -- with no signal anywhere.  Contract section 13's exit is that a malformed or
  -- contradictory projection FAILS CLOSED; that was fail-open to the ordinary
  -- presentation.  The frozen MyTMS contract has no error member, so the only
  -- honest fail-closed route is to let the failure propagate and have the caller
  -- fail visibly.  Ordinary Timesheets never reach this line, so their payload
  -- is unaffected.
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_timesheet.week_ending_date);

  return pg_catalog.jsonb_build_object(
    'timesheet_id',v_timesheet.timesheet_id,
    'candidate_id',v_contract.candidate_id,
    'client_id',v_contract.client_id,
    'contract_id',v_contract.id,
    'week_ending_date',pg_catalog.to_char(v_timesheet.week_ending_date,'YYYY-MM-DD'),
    'source_group_id',v_group.id,
    'authority_mode',v_policy->>'authority_mode',
    'source_fixed_expenses_enabled',
      coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false),
    -- The installed server test for "the Candidate submitted evidence for this
    -- week" (`…read_projections_v1.sql`, `v_submitted_available`).  `24 §18`:
    -- where no Candidate submission exists the submitted fact stays empty and is
    -- never filled from source.
    'candidate_submitted',
      v_timesheet.r2_nurse_key is not null and v_timesheet.img_sha256_nurse is not null,
    'authorised_for_pay',v_timesheet.authorised_at_server is not null);
end;
$function$;

-- ---------------------------------------------------------------------------
-- The Candidate's live Weekly Source request for this exact Timesheet, if any.
--
-- `request_id`, `scope_id` and `request_kind` are required members of
-- `CandidateWeeklySourceView` and are what
-- `apps/candidate-app/src/features/timesheets/weekly-source.ts`
-- `canUnlockSourceHoursEditor` matches against, so they must be the same
-- identities the request projection publishes
-- (`15092026_2203_weekly_source_candidate_app_contract_v1.sql`):
--   CHECK_HOURS      scope_id = min(membership.id::text)::uuid over the
--                    outreach memberships that group to this Timesheet;
--   SUBMIT_TIMESHEET scope_id = the submission-request membership id for this
--                    Contract and week.
-- All three are null when no live request covers this Timesheet.
--
-- PostgreSQL has no min(uuid); the text form is aggregated and cast back,
-- exactly as the installed request projection does.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_view_request_v1(
  p_context jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet_id uuid:=(p_context->>'timesheet_id')::uuid;
  v_candidate_id uuid:=(p_context->>'candidate_id')::uuid;
  v_contract_id uuid:=(p_context->>'contract_id')::uuid;
  v_week date:=(p_context->>'week_ending_date')::date;
  v_request_id uuid;
  v_scope_id uuid;
  v_kind text;
begin
  select generation.id,
         pg_catalog.min(membership.id::text)::uuid
    into v_request_id,v_scope_id
  from public.weekly_candidate_outreach_generations generation
  join public.weekly_candidate_outreach_memberships membership
    on membership.candidate_generation_id=generation.id
  join public.weekly_discrepancy_incidents incident
    on incident.id=membership.incident_id
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  where generation.candidate_id=v_candidate_id
    and generation.request_kind='CHECK_HOURS'
    and generation.state not in ('SUPERSEDED','CANCELLED')
    and membership.state<>'SUPERSEDED'
    and membership.comparison_revision_id=incident.current_comparison_revision_id
    and comparison.candidate_timesheet_id=v_timesheet_id
  group by generation.id
  order by generation.id
  limit 1;
  if v_request_id is not null then
    return pg_catalog.jsonb_build_object(
      'request_id',v_request_id,'scope_id',v_scope_id,'request_kind','CHECK_HOURS');
  end if;

  select generation.id,membership.id,'SUBMIT_TIMESHEET'
    into v_request_id,v_scope_id,v_kind
  from public.weekly_timesheet_submission_requests submission
  join public.weekly_candidate_outreach_generations generation
    on generation.candidate_cohort_id=submission.candidate_cohort_id
   and generation.source_cycle_id=submission.source_cycle_id
   and generation.candidate_id=submission.candidate_id
   and generation.generation_number=submission.request_generation
   and generation.request_kind='SUBMIT_TIMESHEET'
   and generation.state not in ('SUPERSEDED','CANCELLED')
  join public.weekly_timesheet_submission_request_memberships membership
    on membership.submission_request_id=submission.id
  where submission.candidate_id=v_candidate_id
    and submission.state not in ('SUPERSEDED','CANCELLED')
    and membership.contract_id=v_contract_id
    and membership.week_ending=v_week
  order by generation.generation_number desc,generation.id desc,membership.id
  limit 1;
  if v_request_id is not null then
    return pg_catalog.jsonb_build_object(
      'request_id',v_request_id,'scope_id',v_scope_id,'request_kind',v_kind);
  end if;

  return pg_catalog.jsonb_build_object(
    'request_id',null,'scope_id',null,'request_kind',null);
end;
$function$;

-- ---------------------------------------------------------------------------
-- The approved hours for the week, in the Candidate contract's own shape.
--
-- **The authority is the committed entitlement head, never the source**
-- (WP-11d F10, from the independent review of WP-14, finding F1).
--
-- This function previously read the CURRENT projection publication's resolved
-- source rows and the family's latest `WAIT` events, and never read
-- `public.weekly_source_entitlement_heads` at all.  That is source-derived, and
-- the Office's decision is what makes hours approved.  The executed consequence
-- (WP-14 review, probe A9/A9b) was that a certified-zero head still told the
-- Candidate about two shifts, and a head whose approved hours later became four
-- hours pushed nothing at all, because the deduplication key is a digest of this
-- payload and the unchanged source made it unchanged.  A worker was told about
-- hours that were never approved, and told nothing when the approved hours
-- changed.
--
-- The head is therefore the authority for WHICH work is approved and for HOW
-- MANY hours.  The head carries `work_date` and the five hour buckets but no
-- clock times, and the frozen Candidate contract is a times shape, so the times
-- are recovered for presentation from the durable work event the head component
-- names (`component_member_identity`), preferring the Office's own latest
-- protected family event over the resolved source row.  The recovered times are
-- then RECONCILED against the head's own hours; where they cannot be, or where
-- the head cannot be resolved or contradicts itself, the result is an explicit
-- UNAVAILABLE state with a reason.  **It never falls back to a source figure**,
-- because falling back to source is precisely what produced the defect.
--
-- The rows are built with the installed Candidate-app hours helper so the shape
-- is the contract's `CandidateWeeklySourceHours` by construction.  No amount,
-- rate or money column is read here or anywhere in this file: the components'
-- `pay_ex_vat` and `charge_ex_vat` columns are never selected.
--
-- States:
--   AVAILABLE               `rows` is the approved schedule.  A certified-zero
--                           head is AVAILABLE with zero rows: the Office has
--                           approved nothing, which is a decided fact.
--   NO_APPROVED_ENTITLEMENT nothing is approved yet.  `rows` is empty and the
--                           Candidate sees no approved card.  A recoverable,
--                           ordinary state.
--   UNAVAILABLE             the head cannot be resolved or contradicts itself.
--                           `rows` is empty and `reason` names it.  No schedule
--                           is stated, and never a source-derived one.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_approved_entitlement_v1(
  p_context jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_members uuid[];
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_head_count integer;
  v_stored_components integer;
  v_hour_components integer;
  v_rows jsonb:='[]'::jsonb;
  v_row_count integer;
  v_unresolved integer;
  v_irreconcilable integer;
  v_total numeric;
begin
  if coalesce((p_context->>'authorised_for_pay')::boolean,false) is not true then
    -- `24 section 17` names the card `Approved hours to be paid`.  Showing a
    -- not-yet-approved proposal under that label would be a false statement to
    -- the Candidate.
    return pg_catalog.jsonb_build_object(
      'state','NO_APPROVED_ENTITLEMENT','reason','NOT_AUTHORISED_FOR_PAY',
      'authority',null,'head_id',null,'rows','[]'::jsonb);
  end if;

  select * into v_timesheet from public.timesheets
  where timesheet_id=(p_context->>'timesheet_id')::uuid;
  if not found then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','TIMESHEET_NOT_FOUND',
      'authority',null,'head_id',null,'rows','[]'::jsonb);
  end if;

  -- Every physical member of the family, because a head is keyed to the root
  -- Timesheet version that was current when it was committed and rotation must
  -- never hide it (`proof/34 section 8`).  The rotation resolver is call-only.
  select pg_catalog.array_agg(distinct scope_row.family_timesheet_id)
    into v_members
  from public._pay_timesheet_rotation_scope(array[v_timesheet.timesheet_id])
    as scope_row;
  if v_members is null then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','FAMILY_UNRESOLVED',
      'authority',null,'head_id',null,'rows','[]'::jsonb);
  end if;

  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads as head_row
  where head_row.state='COMMITTED_CURRENT'
    and head_row.root_timesheet_id=any(v_members);

  -- An explicit cardinality check, never `limit 1`: two committed heads for one
  -- family is contradictory evidence and is reported, never resolved by picking
  -- one (`24 section 4.3`; Part 1 standing rule 5).
  if v_head_count>1 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','MULTIPLE_COMMITTED_HEADS_FOR_FAMILY',
      'authority',null,'head_id',null,'head_count',v_head_count,
      'rows','[]'::jsonb);
  end if;

  if v_head_count=0 then
    -- No head.  Under TIMESHEET authority the Timesheet's own schedule IS the
    -- approved fact, and that is not a source figure, so it stands.  Under
    -- SOURCE authority nothing has been approved through the entitlement route
    -- and the Candidate is told nothing rather than told the source.
    if p_context->>'authority_mode' is distinct from 'SOURCE_AUTHORITY' then
      return pg_catalog.jsonb_build_object(
        'state','AVAILABLE','reason',null,
        'authority','TIMESHEET','head_id',null,
        'rows',private.weekly_source_candidate_app_schedule_v1(
          v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day));
    end if;
    return pg_catalog.jsonb_build_object(
      'state','NO_APPROVED_ENTITLEMENT','reason','NO_COMMITTED_HEAD',
      'authority',null,'head_id',null,'rows','[]'::jsonb);
  end if;

  select head_row.* into v_head
  from public.weekly_source_entitlement_heads as head_row
  where head_row.state='COMMITTED_CURRENT'
    and head_row.root_timesheet_id=any(v_members);

  -- The head's own declared component count must match what is stored for it.
  select pg_catalog.count(*)::integer into v_stored_components
  from public.weekly_source_entitlement_head_components as component
  where component.head_id=v_head.id;
  if v_stored_components is distinct from v_head.component_count then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','HEAD_COMPONENT_COUNT_MISMATCH',
      'authority','HEAD','head_id',v_head.id,
      'head_component_count',v_head.component_count,
      'stored_component_count',v_stored_components,'rows','[]'::jsonb);
  end if;

  if v_head.certified_zero then
    -- A decided zero.  The Office has approved no hours, so the Candidate is
    -- shown no approved rows - and, critically, is not shown the source.
    return pg_catalog.jsonb_build_object(
      'state','AVAILABLE','reason',null,
      'authority','HEAD','head_id',v_head.id,
      'head_revision',v_head.head_revision,'certified_zero',true,
      'component_count',0,'total_hours',0::numeric,'rows','[]'::jsonb);
  end if;

  -- The hour-bearing, payable components.  An expense component carries all five
  -- hour buckets null and is not an hours row; an `exclude_from_pay` component
  -- is not paid.  Money columns are never selected.
  with payable as (
    select component.component_id,
           component.component_member_identity,
           component.work_date,
           coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
           +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
           +coalesce(component.hours_bh,0) as component_hours
    from public.weekly_source_entitlement_head_components as component
    where component.head_id=v_head.id
      and component.exclude_from_pay is not true
      and (component.hours_day is not null or component.hours_night is not null
        or component.hours_sat is not null or component.hours_sun is not null
        or component.hours_bh is not null)
  ), timed as (
    select payable.*,
           resolved.start_at_local,
           resolved.end_at_local,
           resolved.break_minutes,
           resolved.time_source,
           resolved.match_count
    from payable
    left join lateral (
      -- The Office's own latest protected statement for this durable work event
      -- is preferred, because when the Office takes an event over it states the
      -- times and the head's buckets were computed from them.  Only when no such
      -- statement exists are the resolved source row's times used, and then only
      -- as PRESENTATION for hours the head has already decided.
      select event_times.start_at_local,event_times.end_at_local,
             event_times.break_minutes,event_times.time_source,
             event_times.match_count
      from (
        select pg_catalog.min(event.start_at_local) as start_at_local,
               pg_catalog.min(event.end_at_local) as end_at_local,
               pg_catalog.min(event.break_minutes) as break_minutes,
               'OFFICE_EVENT'::text as time_source,
               pg_catalog.count(*)::integer as match_count
        from public.weekly_exceptional_pay_family_events as event
        join public.weekly_exceptional_pay_target_families as target_family
          on target_family.id=event.family_id
        where target_family.root_timesheet_id=any(v_members)
          and event.durable_work_event_id::text=payable.component_member_identity
          and event.state='WAIT'
          and event.event_sequence=(
            select pg_catalog.max(latest.event_sequence)
            from public.weekly_exceptional_pay_family_events as latest
            where latest.family_id=event.family_id
              and latest.durable_work_event_id=event.durable_work_event_id)
        having pg_catalog.count(*)>0
        union all
        -- WP-11e G3.  TWO restrictions, both of which were missing and which
        -- together made an ORDINARY source re-upload block the whole week.
        --
        -- 1. THE AUTHORITATIVE UPLOAD.  `weekly_source_uploads.state` has
        --    `SUPERSEDED` as a designed state and the resolver has
        --    `EXACT_DURABLE_LINEAGE` precisely so that a corrected re-upload of
        --    the same shift resolves to the SAME durable work event.  Counting
        --    resolutions across every upload therefore found two matches for
        --    every re-uploaded shift and made the head unresolvable.  The
        --    authority is the cycle's own current complete upload, which is the
        --    same authority the pre-WP-11d predicate took from the current
        --    projection publication.
        -- 2. DISTINCT TIMES, not rows.  Two rows that STATE THE SAME TIMES are
        --    one statement, whatever they are counted from.  Rows that state
        --    DIFFERENT times are a real contradiction and still fail closed,
        --    because the distinct count is then greater than one.
        select pg_catalog.min(source_row.start_at_local),
               pg_catalog.min(source_row.end_at_local),
               pg_catalog.min(source_row.break_minutes),
               'SOURCE_ROW'::text,
               pg_catalog.count(distinct (source_row.start_at_local,
                                          source_row.end_at_local,
                                          source_row.break_minutes))::integer
        from public.weekly_source_row_resolutions as resolution
        join public.weekly_source_upload_rows as source_row
          on source_row.id=resolution.upload_row_id
        join public.weekly_source_uploads as upload_row
          on upload_row.id=source_row.upload_id
        join public.weekly_source_cycles as cycle_row
          on cycle_row.id=upload_row.source_cycle_id
         and cycle_row.current_complete_upload_id=upload_row.id
        where resolution.work_event_id::text=payable.component_member_identity
          and resolution.mapping_state='RESOLVED'
          and resolution.generation=(
            select pg_catalog.max(latest.generation)
            from public.weekly_source_row_resolutions as latest
            where latest.upload_row_id=resolution.upload_row_id)
        having pg_catalog.count(*)>0
      ) as event_times
      order by case when event_times.time_source='OFFICE_EVENT' then 1 else 2 end
      limit 1
    ) as resolved on true
  )
  select
    pg_catalog.count(*) filter (
      where timed.start_at_local is null or timed.end_at_local is null
         or timed.match_count is distinct from 1)::integer,
    -- The recovered times must describe the hours the head decided.  A
    -- disagreement means the times do not belong to those hours, and presenting
    -- them would tell the worker a schedule the Office never approved.
    pg_catalog.count(*) filter (
      where timed.start_at_local is not null and timed.end_at_local is not null
        -- `extract` is a PostgreSQL syntax construct, not a pg_catalog
        -- function, and must never be schema-qualified (the same class of
        -- defect as a qualified COALESCE/NULLIF/LEAST/GREATEST, which compiles
        -- and then fails with 42883 on first execution).
        and pg_catalog.abs(
              (extract(epoch from
                 (timed.end_at_local-timed.start_at_local))/3600.0)
              -(coalesce(timed.break_minutes,0)/60.0)
              -timed.component_hours)>0.01)::integer,
    pg_catalog.count(*)::integer,
    coalesce(pg_catalog.sum(timed.component_hours),0)
    into v_unresolved,v_irreconcilable,v_hour_components,v_total
  from timed;

  if v_unresolved>0 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','APPROVED_COMPONENT_TIMES_UNRESOLVED',
      'authority','HEAD','head_id',v_head.id,
      'head_revision',v_head.head_revision,
      'component_count',v_head.component_count,
      'unresolved_components',v_unresolved,'rows','[]'::jsonb);
  end if;
  if v_irreconcilable>0 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','APPROVED_ENTITLEMENT_NOT_RECONCILED',
      'authority','HEAD','head_id',v_head.id,
      'head_revision',v_head.head_revision,
      'component_count',v_head.component_count,
      'irreconcilable_components',v_irreconcilable,'rows','[]'::jsonb);
  end if;

  with payable as (
    select component.component_id,
           component.component_member_identity,
           component.work_date,
           coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
           +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
           +coalesce(component.hours_bh,0) as component_hours
    from public.weekly_source_entitlement_head_components as component
    where component.head_id=v_head.id
      and component.exclude_from_pay is not true
      and (component.hours_day is not null or component.hours_night is not null
        or component.hours_sat is not null or component.hours_sun is not null
        or component.hours_bh is not null)
  ), timed as (
    select payable.*,resolved.start_at_local,resolved.end_at_local,
           resolved.break_minutes
    from payable
    left join lateral (
      select event_times.start_at_local,event_times.end_at_local,
             event_times.break_minutes,event_times.time_source
      from (
        select pg_catalog.min(event.start_at_local) as start_at_local,
               pg_catalog.min(event.end_at_local) as end_at_local,
               pg_catalog.min(event.break_minutes) as break_minutes,
               'OFFICE_EVENT'::text as time_source
        from public.weekly_exceptional_pay_family_events as event
        join public.weekly_exceptional_pay_target_families as target_family
          on target_family.id=event.family_id
        where target_family.root_timesheet_id=any(v_members)
          and event.durable_work_event_id::text=payable.component_member_identity
          and event.state='WAIT'
          and event.event_sequence=(
            select pg_catalog.max(latest.event_sequence)
            from public.weekly_exceptional_pay_family_events as latest
            where latest.family_id=event.family_id
              and latest.durable_work_event_id=event.durable_work_event_id)
        having pg_catalog.count(*)>0
        union all
        -- WP-11e G3.  The SAME two restrictions as the cardinality pass above.
        -- The two branches must agree exactly or the presentation would be
        -- drawn from rows the cardinality check never proved unique.
        select pg_catalog.min(source_row.start_at_local),
               pg_catalog.min(source_row.end_at_local),
               pg_catalog.min(source_row.break_minutes),
               'SOURCE_ROW'::text
        from public.weekly_source_row_resolutions as resolution
        join public.weekly_source_upload_rows as source_row
          on source_row.id=resolution.upload_row_id
        join public.weekly_source_uploads as upload_row
          on upload_row.id=source_row.upload_id
        join public.weekly_source_cycles as cycle_row
          on cycle_row.id=upload_row.source_cycle_id
         and cycle_row.current_complete_upload_id=upload_row.id
        where resolution.work_event_id::text=payable.component_member_identity
          and resolution.mapping_state='RESOLVED'
          and resolution.generation=(
            select pg_catalog.max(latest.generation)
            from public.weekly_source_row_resolutions as latest
            where latest.upload_row_id=resolution.upload_row_id)
        having pg_catalog.count(*)>0
      ) as event_times
      order by case when event_times.time_source='OFFICE_EVENT' then 1 else 2 end
      limit 1
    ) as resolved on true
  )
  select coalesce(pg_catalog.jsonb_agg(hours_row order by
           hours_row->>'date',
           coalesce(hours_row->>'start','') collate "C",
           coalesce(hours_row->>'row_key','') collate "C"),'[]'::jsonb)
    into v_rows
  from (
    select private.weekly_source_candidate_app_issue_hours_v1(
             -- The head's own work date is the authority; the recovered record
             -- supplies only the clock times.
             timed.work_date,timed.start_at_local,timed.end_at_local,
             timed.break_minutes,
             -- The row key carries no internal vocabulary: MyTMS must never see
             -- the words source, protected, exceptional or reconciliation, and
             -- must not be told which approved row the Office replaced.  A
             -- component id is a bare uuid.
             'approved-'||timed.component_id::text,'[]'::jsonb
           ) as hours_row
    from timed
  ) as approved;

  v_row_count:=pg_catalog.jsonb_array_length(v_rows);
  if v_row_count is distinct from v_hour_components then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','APPROVED_ENTITLEMENT_NOT_RECONCILED',
      'authority','HEAD','head_id',v_head.id,
      'head_revision',v_head.head_revision,
      'component_count',v_head.component_count,
      'rows','[]'::jsonb);
  end if;

  return pg_catalog.jsonb_build_object(
    'state','AVAILABLE','reason',null,
    'authority','HEAD','head_id',v_head.id,
    'head_revision',v_head.head_revision,'certified_zero',false,
    'component_count',v_head.component_count,
    'total_hours',v_total,'rows',v_rows);
end;
$function$;

-- ---------------------------------------------------------------------------
-- The approved rows themselves, for callers that can only carry an array.
--
-- `AVAILABLE` and `NO_APPROVED_ENTITLEMENT` return the rows (empty for the
-- latter).  `UNAVAILABLE` RAISES, because the only alternative within an array
-- return type is an empty array, which is indistinguishable from "nothing is
-- approved" - and that indistinguishability is the WP-11d F4 defect in a
-- different place.  The existing callers already treat a raise here as
-- fail-closed: WP-14's push payload builder catches it and withholds the push
-- with `APPROVED_HOURS_NOT_DERIVABLE`, which is the required behaviour.
--
-- WP-11e G3: UNCHANGED, deliberately.  Withholding a push is not the same act
-- as refusing to render a worker's own week, and the two callers now differ.
-- The MyTMS payload producer no longer calls this wrapper; it reads
-- `weekly_source_candidate_approved_entitlement_v1` directly and degrades to a
-- stated unavailable result, because propagating this raise through
-- `public.candidate_app_timesheet_detail_v2` stopped the worker opening the
-- week at all.  Any NEW caller that can carry a state should read the resolver
-- directly rather than this wrapper.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_approved_hours_v1(
  p_context jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_entitlement jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(p_context);
  if v_entitlement->>'state'='UNAVAILABLE' then
    raise exception 'WEEKLY_SOURCE_APPROVED_ENTITLEMENT_UNAVAILABLE: %',
      coalesce(v_entitlement->>'reason','UNKNOWN') using errcode='55000';
  end if;
  return coalesce(v_entitlement->'rows','[]'::jsonb);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Do the approved hours differ from the Candidate's submitted Timesheet?
--
-- `24 section 17`: `Approved hours to be paid` is shown ONLY when those hours
-- differ.  The comparison is on the hours themselves - worked, date, start, end
-- and break length - and not on row keys or additional units, so a re-keyed but
-- identical schedule is not a difference.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_hours_shape_v1(
  p_hours jsonb
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(pg_catalog.jsonb_agg(shape order by shape::text collate "C"),'[]'::jsonb)
  from (
    select pg_catalog.jsonb_build_object(
      'worked',coalesce((element.value->>'worked')::boolean,false),
      'date',element.value->>'date',
      'start',element.value->>'start',
      'end',element.value->>'end',
      'break_minutes',coalesce(
        (element.value#>>'{break_entry,break_minutes}')::integer,
        (element.value#>>'{break_entry,calculated_break_minutes}')::integer,
        0)
    ) as shape
    from pg_catalog.jsonb_array_elements(
      case when pg_catalog.jsonb_typeof(coalesce(p_hours,'null'::jsonb))='array'
        then p_hours else '[]'::jsonb end) as element(value)
  ) as shaped;
$function$;

create or replace function private.weekly_source_candidate_hours_differ_v1(
  p_submitted jsonb,
  p_approved jsonb
) returns boolean
language sql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
  select private.weekly_source_candidate_hours_shape_v1(p_approved)
       is distinct from private.weekly_source_candidate_hours_shape_v1(p_submitted);
$function$;

-- ---------------------------------------------------------------------------
-- G9-6: the payload itself.
--
-- Returns the exact `CandidateWeeklySourceView` object of the MyTMS contract
-- (`additionalProperties: false`, all nine members required), or NULL when the
-- Timesheet is not a Weekly Source week.
--
-- `UI-021` - a source-authority week with no Candidate submission after Office
-- authorisation - is expressed within the frozen contract as an empty
-- `submitted_timesheet` beside a non-empty `approved_hours_to_be_paid`.  The
-- contract admits no extra member, and that pair is unambiguous: it is the only
-- shape in which approved hours exist with no submission, which is what MyTMS
-- renders as `You did not submit a Timesheet for this week.`  The hours are
-- never labelled as a submission because they are never placed in
-- `submitted_timesheet`.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_view_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_timesheet public.timesheets%rowtype;
  v_request jsonb;
  v_submitted jsonb:='[]'::jsonb;
  v_units_week jsonb:='[]'::jsonb;
  v_units_day jsonb:='[]'::jsonb;
  v_entitlement jsonb;
  v_approved jsonb:='[]'::jsonb;
  v_differ boolean:=false;
begin
  v_context:=private.weekly_source_candidate_week_context_v1(p_timesheet_id);
  if v_context is null then return null; end if;

  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then return null; end if;

  -- The Candidate's own evidence, untouched.  Where no Candidate submission
  -- exists the submitted fact stays empty and is never filled from source
  -- (`24 section 18`).
  -- WP-11d F4.  These paths run only for a CONFIRMED Weekly Source week, so a
  -- failure in them is an integrity failure, not an ordinary week.  They were
  -- previously `exception when others then return null`, which made the
  -- Candidate's week silently indistinguishable from an ordinary Timesheet: the
  -- app rendered a blank week that should have shown hours, and nothing anywhere
  -- signalled the failure.  NULL therefore still means exactly one thing - "not
  -- a Weekly Source week" - and a configuration or infrastructure failure still
  -- PROPAGATES so the caller fails visibly.
  if coalesce((v_context->>'candidate_submitted')::boolean,false) then
    v_submitted:=private.weekly_source_candidate_app_schedule_v1(
      v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day);
    v_units_week:=private.weekly_source_candidate_app_units_week_v1(
      v_timesheet.additional_units_week);
    v_units_day:=private.weekly_source_candidate_app_units_day_v1(
      v_timesheet.additional_units_per_day,null);
  end if;

  -- WP-11e G3.  THE APPROVED HOURS ARE READ FROM THE ENTITLEMENT RESOLVER
  -- DIRECTLY, not through the array wrapper that raises.
  --
  -- The wrapper `weekly_source_candidate_approved_hours_v1` raises
  -- `WEEKLY_SOURCE_APPROVED_ENTITLEMENT_UNAVAILABLE` on an unavailable head,
  -- because its return type is an array and an empty array would be
  -- indistinguishable from "nothing is approved".  That is right FOR THAT
  -- CALLER: WP-14's push builder catches it and withholds the push, and it
  -- keeps its behaviour unchanged.  It was wrong here.  Propagated through
  -- `public.candidate_app_timesheet_detail_v2`, it made the whole Timesheet
  -- detail error, so the WORKER COULD NOT OPEN THEIR OWN WEEK - not the
  -- approved card, the week - whenever the Office's head could not be resolved.
  -- After an ordinary source re-upload that was every source-timed component
  -- (WP-11e G3, executed).
  --
  -- An unresolvable head is a statement about the OFFICE'S APPROVAL, not about
  -- the Candidate's own submitted evidence, which is sound and which the worker
  -- is entitled to see.  So it degrades to a STATED UNAVAILABLE RESULT: the
  -- resolver's own explicit `state='UNAVAILABLE'` with its reason, which the
  -- Office projection, the audit and WP-14 all continue to read, and here an
  -- empty approved schedule and `approved_hours_differ=false`.
  --
  -- WHAT IS AND IS NOT PRESERVED, stated plainly:
  --   * `approved_hours_to_be_paid` NEVER falls back to a source-derived figure
  --     (WP-11d F10).  Empty means empty.
  --   * NULL still means only "not a Weekly Source week" (WP-11d F4): this
  --     returns a payload, not NULL, so the week renders as a Weekly Source
  --     week.
  --   * A configuration or infrastructure failure - an unresolvable client
  --     policy, a rotation resolver that raises - still propagates and still
  --     fails the call, because those mean the week itself cannot be read.
  --   * WHAT IS LOST: the frozen MyTMS contract has nine members and admits no
  --     error member, so the Candidate screen cannot say WHY no approved card
  --     is shown, and this state is not distinguishable in the payload from
  --     "nothing has been approved yet".  That is recorded for WP-13 and the
  --     approver in `IMPL\handoffs\WP-11e_NEEDS.md` D4.  A silent missing card
  --     is recoverable; a week the worker cannot open is not.
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(v_context);
  if v_entitlement->>'state'='UNAVAILABLE' then
    v_approved:='[]'::jsonb;
  else
    v_approved:=coalesce(v_entitlement->'rows','[]'::jsonb);
  end if;

  -- An empty approved schedule is not a difference: it means no approved
  -- statement exists yet, which is the state before Office authorisation
  -- (`NAI-MYT-001`: "before authorisation no Candidate-facing detail exists").
  -- A non-empty approved schedule that equals the submission is not a
  -- difference either, and `24 section 17` then shows no approved card at all.
  v_differ:=pg_catalog.jsonb_array_length(v_approved)>0
    and private.weekly_source_candidate_hours_differ_v1(v_submitted,v_approved);
  if not v_differ then
    v_approved:='[]'::jsonb;
  end if;

  v_request:=private.weekly_source_candidate_view_request_v1(v_context);

  return pg_catalog.jsonb_build_object(
    'request_id',v_request->'request_id',
    'scope_id',v_request->'scope_id',
    'request_kind',v_request->'request_kind',
    'submitted_timesheet',v_submitted,
    'submitted_additional_units_week',v_units_week,
    'submitted_additional_units_per_day',v_units_day,
    'approved_hours_to_be_paid',v_approved,
    'approved_hours_differ',v_differ,
    'expense_entry_mode',case
      when coalesce((v_context->>'source_fixed_expenses_enabled')::boolean,false)
        then 'NOT_AVAILABLE' else 'SEPARATE_TIMESHEET' end);
end;
$function$;

-- ---------------------------------------------------------------------------
-- The single additive call site's payload.
--
-- `{}` for every ordinary Timesheet, so merging it into the Candidate detail
-- result is a no-op and the ordinary payload is byte-identical to today; the
-- one-member object only for a Weekly Source week.  The Candidate detail owner
-- therefore needs exactly one additive call and no new local variable.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_candidate_view_merge_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_view jsonb;
begin
  v_view:=private.weekly_source_candidate_view_v1(p_timesheet_id,p_now_utc);
  if v_view is null then return '{}'::jsonb; end if;
  return pg_catalog.jsonb_build_object('weekly_source_candidate_view',v_view);
end;
$function$;

-- A first Candidate submission is contract-week owned until its signed
-- Timesheet is materialised.  The request projection deliberately targets that
-- contract-week, so it must be able to publish the same server-owned request
-- and scope identities before a Timesheet id exists.  This is not a general
-- import-authoritative edit bypass: without an active SUBMIT_TIMESHEET request
-- for this exact Candidate, Contract and week the merge remains an empty no-op.
create or replace function private.weekly_source_candidate_contract_week_view_merge_v1(
  p_contract_week_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_request jsonb;
begin
  if p_contract_week_id is null then return '{}'::jsonb; end if;

  select * into v_week
  from public.contract_weeks
  where id=p_contract_week_id;
  if not found or v_week.timesheet_id is not null then return '{}'::jsonb; end if;

  select * into v_contract
  from public.contracts
  where id=v_week.contract_id;
  if not found then return '{}'::jsonb; end if;

  if not exists(
    select 1
    from public.weekly_source_group_clients membership
    join public.weekly_source_groups source_group
      on source_group.id=membership.source_group_id
    where membership.client_id=v_contract.client_id
      and source_group.active
      and v_week.week_ending_date
          between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
  ) then
    return '{}'::jsonb;
  end if;

  v_request:=private.weekly_source_candidate_view_request_v1(
    pg_catalog.jsonb_build_object(
      'timesheet_id',null,
      'candidate_id',v_contract.candidate_id,
      'client_id',v_contract.client_id,
      'contract_id',v_contract.id,
      'week_ending_date',pg_catalog.to_char(v_week.week_ending_date,'YYYY-MM-DD')
    )
  );
  if v_request->>'request_kind' is distinct from 'SUBMIT_TIMESHEET'
     or v_request->>'request_id' is null
     or v_request->>'scope_id' is null then
    return '{}'::jsonb;
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_week.week_ending_date);

  return pg_catalog.jsonb_build_object(
    'weekly_source_candidate_view',pg_catalog.jsonb_build_object(
      'request_id',v_request->'request_id',
      'scope_id',v_request->'scope_id',
      'request_kind',v_request->'request_kind',
      'submitted_timesheet','[]'::jsonb,
      'submitted_additional_units_week','[]'::jsonb,
      'submitted_additional_units_per_day','[]'::jsonb,
      'approved_hours_to_be_paid','[]'::jsonb,
      'approved_hours_differ',false,
      'expense_entry_mode',case
        when coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false)
          then 'NOT_AVAILABLE' else 'SEPARATE_TIMESHEET' end
    )
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- G9-4: the Office Unauthorise action-state bridge.
--
-- The Office presentation's `action_state` object is a single jsonb expression,
-- so the whole verdict is produced here.  This bridge computes NO check of its
-- own: `available`, the refusal code, its permanent/temporary nature and
-- `retryable` are whatever the withdrawal owner's read-only availability
-- function returns (`proof/36 section 3`, Gate 9 item G9-4: "computed by the
-- withdrawal owner's own checks").
--
-- Availability owner: `public.weekly_source_first_authorisation_withdraw_available_v1(uuid)`,
-- the name WP-07 published for its G3-4 function.  It is installed by a LATER
-- repeatable than the Office projection, and plpgsql resolves callees at run
-- time, so an absent owner is reported as an explicit unavailable verdict with
-- `availability_source = 'OWNER_ABSENT'` rather than breaking the projection.
--
-- `withdrawn` is a plain lifecycle fact on the root-authorisation relation, not
-- one of the nine checks: a family that carries a withdrawn generation and no
-- live one is WITHDRAWN (the `UI-022` precondition), a live generation is
-- AUTHORISED, and no generation at all is NEVER_AUTHORISED.  Every member of
-- the returned object is non-null so that `jsonb_strip_nulls` at the call site
-- cannot remove one.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_authorisation_state_v1(
  p_timesheet_id uuid
) returns text
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_members uuid[];
  v_live integer;
  v_withdrawn integer;
begin
  select pg_catalog.array_agg(distinct scope_row.family_timesheet_id)
    into v_members
  from public._pay_timesheet_rotation_scope(array[p_timesheet_id]) as scope_row;
  if v_members is null then return 'UNKNOWN'; end if;

  select pg_catalog.count(*) filter (where authorisation_row.withdrawn_at_utc is null)::integer,
         pg_catalog.count(*) filter (where authorisation_row.withdrawn_at_utc is not null)::integer
    into v_live,v_withdrawn
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=any(v_members);

  if v_live>0 then return 'AUTHORISED'; end if;
  if v_withdrawn>0 then return 'WITHDRAWN'; end if;
  return 'NEVER_AUTHORISED';
exception
  when undefined_table or undefined_function or insufficient_privilege then
    return 'UNKNOWN';
end;
$function$;

create or replace function private.weekly_source_office_unauthorise_action_state_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_verdict jsonb;
  v_source text:='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAW_AVAILABLE_V1';
  v_state text;
  v_owner_sqlstate text:='NONE';
  v_owner_message text:='NONE';
  v_available boolean;
begin
  v_state:=private.weekly_source_office_authorisation_state_v1(p_timesheet_id);

  begin
    v_verdict:=public.weekly_source_first_authorisation_withdraw_available_v1(p_timesheet_id);
  exception
    when undefined_function or undefined_table then
      v_verdict:=null;
      v_source:='OWNER_ABSENT';
      -- WP-11d F7.  The cause is preserved, not discarded.
      get stacked diagnostics v_owner_sqlstate=returned_sqlstate,
                              v_owner_message=message_text;
    when others then
      v_verdict:=null;
      v_source:='OWNER_ERROR';
      get stacked diagnostics v_owner_sqlstate=returned_sqlstate,
                              v_owner_message=message_text;
  end;

  if v_verdict is null or pg_catalog.jsonb_typeof(v_verdict)<>'object' then
    return pg_catalog.jsonb_build_object(
      'unauthorise_allowed',false,
      'unauthorise',pg_catalog.jsonb_build_object(
        'available',false,
        'refusal_code','WEEKLY_SOURCE_WITHDRAW_AVAILABILITY_UNAVAILABLE',
        'refusal_nature','INTEGRITY',
        'permanent',false,
        'retryable',false,
        'reason','THE_WITHDRAWAL_AVAILABILITY_OWNER_DID_NOT_ANSWER',
        'authorisation_state',v_state,
        'withdrawn',v_state='WITHDRAWN',
        'availability_source',v_source,
        -- WP-11d F7: an Office user and an operator can now see WHY the owner
        -- did not answer.  The refusal code itself is unchanged.  The message is
        -- bounded so a long owner error cannot bloat the projection.
        'owner_sqlstate',coalesce(v_owner_sqlstate,'NONE'),
        'owner_error',pg_catalog.left(
          coalesce(nullif(pg_catalog.btrim(v_owner_message),''),'NONE'),200)));
  end if;

  -- WP-11d F3, Part 1 standing rule 4: a boolean read from JSON is three-valued,
  -- and absent, null AND NON-BOOLEAN must all take the unsafe branch.  A bare
  -- `(v_verdict->>'available')::boolean` handled absent and JSON null (both give
  -- false) but CAST a string, and PostgreSQL accepts 'yes', 'on', '1' and 't' as
  -- true - so a malformed verdict granted the Unauthorise control.
  v_available:=case
    when pg_catalog.jsonb_typeof(v_verdict->'available')='boolean'
      then (v_verdict->>'available')::boolean
    else false end;

  return pg_catalog.jsonb_build_object(
    'unauthorise_allowed',v_available,
    'unauthorise',pg_catalog.jsonb_build_object(
      'available',v_available,
      'refusal_code',coalesce(v_verdict->>'code','NONE'),
      'refusal_nature',coalesce(v_verdict->>'refusal_nature','NONE'),
      -- proof/36 section 6: a PERMANENT refusal never clears, so the control is
      -- not shown at all; a TEMPORARY one clears when the existing Banking Pay
      -- and invoice owners finish their own work.
      'permanent',coalesce(v_verdict->>'refusal_nature','NONE')='PERMANENT',
      -- `retryable` is the same three-valued JSON boolean.
      'retryable',case
        when pg_catalog.jsonb_typeof(v_verdict->'retryable')='boolean'
          then (v_verdict->>'retryable')::boolean
        else false end,
      'reason',coalesce(v_verdict->>'reason','NONE'),
      'authorisation_state',v_state,
      'withdrawn',v_state='WITHDRAWN',
      'availability_source',v_source,
      'owner_sqlstate','NONE',
      'owner_error','NONE'));
end;
$function$;

alter function private.weekly_source_candidate_week_context_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_view_request_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_approved_entitlement_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_approved_hours_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_hours_shape_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_hours_differ_v1(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_candidate_view_v1(uuid,timestamptz) owner to postgres;
alter function private.weekly_source_candidate_view_merge_v1(uuid,timestamptz) owner to postgres;
alter function private.weekly_source_candidate_contract_week_view_merge_v1(uuid,timestamptz) owner to postgres;
alter function private.weekly_source_office_authorisation_state_v1(uuid) owner to postgres;
alter function private.weekly_source_office_unauthorise_action_state_v1(uuid) owner to postgres;

revoke all on function private.weekly_source_candidate_week_context_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_view_request_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_approved_entitlement_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_approved_hours_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_shape_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_differ_v1(jsonb,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_view_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_contract_week_view_merge_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_authorisation_state_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_unauthorise_action_state_v1(uuid)
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_candidate_approved_entitlement_v1(jsonb) is
  'Gate 9 G9-6, WP-11d F10. The approved hours for a Weekly Source week, whose authority is the ONE committed entitlement head for the family - never the projection''s source rows. The head decides which work is approved and how many hours; clock times are recovered for presentation from the durable work event the component names, preferring the Office''s own latest protected family event, and are then reconciled against the head''s hours. A certified-zero head is AVAILABLE with zero rows. Where the head cannot be resolved or contradicts itself the state is UNAVAILABLE with a reason and no rows; it never falls back to a source-derived schedule. WP-11e G3: the source clock times are read ONLY from the cycle''s current complete upload and counted as DISTINCT (start, end, break) tuples, so an ordinary corrected re-upload that lineage-resolves to the same durable work event is one statement rather than two, while rows stating different times are still a contradiction and still fail closed. Reads no amount, rate or money column.';

comment on function private.weekly_source_candidate_approved_hours_v1(jsonb) is
  'Gate 9 G9-6. The approved rows array for callers that cannot carry a state. AVAILABLE and NO_APPROVED_ENTITLEMENT return the rows; UNAVAILABLE raises WEEKLY_SOURCE_APPROVED_ENTITLEMENT_UNAVAILABLE (55000), because an empty array would be indistinguishable from "nothing is approved". Callers must treat the raise as fail-closed and withhold, never substitute a source figure.';

comment on function private.weekly_source_candidate_view_v1(uuid,timestamptz) is
  'Gate 9 G9-6. Produces the MyTMS CandidateWeeklySourceView payload for a Weekly Source week, or NULL for any other Timesheet. Hours only: submitted Candidate evidence untouched, Approved hours to be paid from the committed entitlement head and only when they differ from the submission and the week is authorised for pay, and the expense mode. Never money, rates, payment or recovery history, remittance links, or the words source, protected, exceptional or reconciliation. NULL means "not a Weekly Source week" and nothing else: for a confirmed Weekly Source week a configuration or infrastructure failure - an unresolvable client policy, a rotation resolver that raises - still propagates to the caller rather than being swallowed (WP-11d F4). WP-11e G3: an UNAVAILABLE entitlement head no longer propagates as an error, because that stopped the worker opening their own week at all after an ordinary source re-upload. It degrades to the resolver''s stated unavailable result and an empty approved schedule here, never to a source-derived figure. The frozen contract has no error member, so the Candidate screen cannot state the reason; the Office projection, the audit and WP-14 still read it in full.';

comment on function private.weekly_source_office_unauthorise_action_state_v1(uuid) is
  'Gate 9 G9-4. The Office action_state Unauthorise verdict. Computes no check of its own: it calls the withdrawal owner''s read-only availability function (proof/36 section 3) and reports its verdict, plus the plain withdrawn/authorised lifecycle fact. A late-bound or failing availability owner yields an explicit unavailable verdict, never a broken projection.';

notify pgrst, 'reload schema';

commit;
