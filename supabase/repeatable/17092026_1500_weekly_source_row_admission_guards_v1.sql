-- Repeatable CloudTMS authority: weekly_source_row_admission_guards_v1
--
-- Package WP-54.  Two source-row admission rules that the pack states and that
-- no installed owner enforced, held in one file so that the owners which must
-- apply them each gain a single call rather than a copy of the predicate.
--
-- ===========================================================================
-- RULE 1 -- NHSP-BR-006, the cutoff cannot predate a row's Actual finish
-- ===========================================================================
--
-- Pack 14 section 4.1.3, second sentence, word for word:
--
--   "The service refuses a cutoff earlier than any row's real `Actual`
--    finishing instant, including an overnight finish.  File-created, modified
--    and upload timestamps are audit only."
--
-- Pack 14 section 10, acceptance row NHSP-BR-006:
--
--   "Cutoff earlier than a row's Actual finish, including overnight finish,
--    blocks."
--
-- Traceability row SRC-NHSP-BR-006 names the required proof mechanism as a
-- "cutoff validator" and the refuted alternative as "File timestamp authority
-- or impossible cutoff".
--
-- The rule belongs to the NHSP FINAL backing report.  Section 4.1 is the scope
-- and finalisation section of "4. NHSP final backing-report authority", and it
-- is the final report that carries a confirmed cutoff occurrence and a report
-- scope.  Section 3 makes the NHSP pre-final export "a checking aid only" which
-- "never creates final ordinary-pay or invoice authority", so it has no cutoff
-- occurrence to contradict, and the HealthRoster and generic profiles are
-- governed by confirmed coverage rather than by a cutoff instant.  This guard
-- therefore applies to NHSP_FINAL_BACKING_V1 and to nothing else.
--
-- "Including an overnight finish" needs no special case here and is the reason
-- the guard reads `end_at_local` rather than `work_date` plus a time of day.
-- `public.weekly_source_upload_rows` already stores the finish as a full local
-- timestamp constrained to be strictly later than the start
-- (`check (... end_at_local>start_at_local)` in the Plan 6 schema), so a shift
-- that starts 22:00 on the 15th and ends 06:00 on the 16th is already carried
-- as 2026-09-16 06:00 and compares correctly without reconstructing the date.
--
-- The comparison is made in the source's own local zone.  Every Weekly Source
-- coverage instant in this system is 'Europe/London' (the seal refuses any
-- other `coverage_timezone`), and `cutoff_at_utc` is a `timestamptz`, so the
-- guard converts the local finishing instant to an absolute instant with
-- `at time zone 'Europe/London'` and compares absolute to absolute.  A British
-- clock change therefore cannot move a row across the cutoff by an hour.
--
-- The refusal is strictly "earlier than".  A row whose Actual finish is exactly
-- the cutoff instant is admitted: the cutoff is not earlier than that finish.
--
-- ===========================================================================
-- RULE 2 -- overlapping worked rows for one person
-- ===========================================================================
--
-- Pack 03 section 7, last line, word for word:
--
--   "Overlapping candidate work events are checked before finalisation.  The
--    system does not assume two overlapping records are valid because their
--    references differ."
--
-- Pack 14 section 4.2.8, word for word:
--
--   "Every physical row must be accounted for as a header, trailer, proved
--    non-economic continuation or accepted economic line.  An unexplained,
--    malformed, truncated or duplicate economic row blocks finalisation."
--
-- Pack 14 section 4.1.7, word for word:
--
--   "Finalisation is all or nothing.  A blocker cannot be removed, unticked or
--    ignored to make the file final.  The Blocked tab explains the exact
--    correction needed.  When corrected source or configuration removes a
--    blocker, it disappears from that tab."
--
-- Those three sentences settle the disposition and this guard does not choose
-- one of its own.  The pack says the condition BLOCKS, that a blocker cannot be
-- ignored, and that finalisation is all or nothing -- so the answer is neither
-- "drop the offending row" (a report may not be made final by removing part of
-- it) nor "flag it for review" (a blocker cannot be unticked).  The report is
-- refused as a whole and the Trust supplies a corrected report.
--
-- WHAT COUNTS AS AN OVERLAP.  Two rows of the same upload, for the same source
-- candidate identity, both claiming worked time, whose worked intervals
-- intersect.  Half-open intervals: a row finishing 13:00 and a row starting
-- 13:00 do not overlap, so an ordinary split shift is admitted unchanged.
--
-- WHAT IS DELIBERATELY NOT AN OVERLAP.  A full-negative NHSP row.  Pack 14
-- section 4.2.4 says a negative physical line "is the full reversal supplied by
-- NHSP", and section 4.2.5 that "Row order has no financial meaning.  A
-- negative and a later positive may appear in different reports and cycles" --
-- so a reversal and the re-issue it is paired with legitimately cover the same
-- instants and must keep sealing.  A reversal is not a claim that the person
-- worked; it withdraws one.  The guard therefore compares claims of worked time
-- only, which for NHSP means rows whose signed `Commission + Total Cost` is
-- positive, and for every other profile means every accepted worked row,
-- because those profiles carry no source money at all
-- (`source_money_parse_state='NOT_APPLICABLE'`, enforced by the seal).
--
-- The identity used is the SOURCE's own candidate identity, because that is the
-- only identity that exists at the seal, before any CloudTMS resolution has
-- run.  That makes this a structural report check.  It is deliberately NOT a
-- substitute for an overlap check over RESOLVED work events across reports and
-- cycles, which is a larger rule; WP-54's report states precisely what that
-- would take and does not pretend this covers it.
--
-- ===========================================================================
-- WHAT THIS FILE DOES NOT DO
-- ===========================================================================
--
--   * It creates, reads and writes nothing.  Both routines are read-only over
--     two Weekly Source evidence relations and either return normally or raise.
--   * It touches no Timesheet, TSFIN, Workbench, Banking Pay, invoice,
--     payment, settlement or remittance relation, and calls no owner in any of
--     them.
--   * It makes no pricing decision, no identity decision and no pay decision.
--     It does not classify a row, does not change a count and does not write a
--     blocker record; the callers already refuse on the counts the normaliser
--     produced and these are two further refusals beside them.
--   * It expresses no safety through `limit`, `order by` or a unique index.
--     Each rule is an explicit set predicate over the whole row set, and the
--     detail that accompanies a refusal is aggregated over every offending row,
--     never sampled.
--
-- Prerequisites: the Plan 6 schema (public.weekly_source_upload_rows,
-- public.weekly_source_uploads, public.weekly_source_format_profiles).

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- NHSP-BR-006.  Raises WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH when any
-- row of the upload finishes after the confirmed cutoff occurrence.
--
-- The caller passes the profile code and the cutoff it has already read under
-- its own lock rather than having this routine re-read them, so that the
-- instant checked here is exactly the instant the caller is about to make
-- authoritative and no second read can drift from it.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_cutoff_admission_assert_v1(
  p_upload_id uuid,
  p_profile_code text,
  p_cutoff_at_utc timestamptz
) returns void
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_breaching integer;
  v_latest timestamp without time zone;
begin
  if p_upload_id is null then
    raise exception 'WEEKLY_SOURCE_CUTOFF_ASSERT_INPUT_INVALID' using errcode='22023';
  end if;
  if p_profile_code is distinct from 'NHSP_FINAL_BACKING_V1' then
    return;
  end if;
  -- Fail closed.  An NHSP final backing report always has a confirmed cutoff
  -- occurrence; a missing one is not "no rule to apply", it is an unverifiable
  -- report.
  if p_cutoff_at_utc is null then
    raise exception 'WEEKLY_SOURCE_NHSP_CUTOFF_OCCURRENCE_REQUIRED' using errcode='55000';
  end if;

  select pg_catalog.count(*)::integer,pg_catalog.max(source_row.end_at_local)
    into v_breaching,v_latest
  from public.weekly_source_upload_rows source_row
  where source_row.upload_id=p_upload_id
    and source_row.end_at_local is not null
    and (source_row.end_at_local at time zone 'Europe/London')>p_cutoff_at_utc;

  if v_breaching>0 then
    raise exception 'WEEKLY_SOURCE_NHSP_CUTOFF_BEFORE_ACTUAL_FINISH'
      using errcode='55000',
        detail=pg_catalog.format(
          '%s row(s) finish after the confirmed cutoff %s; latest Actual finish %s Europe/London',
          v_breaching,p_cutoff_at_utc,v_latest
        );
  end if;
end;
$function$;

-- ---------------------------------------------------------------------------
-- Pack 03 section 7 and pack 14 sections 4.2.8 and 4.1.7.  Raises
-- WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS when one person is claimed as working
-- two intersecting intervals in the same report.
--
-- `b.source_row_ordinal>a.source_row_ordinal` is pair de-duplication only -- it
-- visits each unordered pair once -- and carries no part of the decision.  The
-- decision is the count of such pairs, and any non-zero count refuses.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_overlap_admission_assert_v1(
  p_upload_id uuid
) returns void
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_pairs integer;
  v_detail text;
begin
  if p_upload_id is null then
    raise exception 'WEEKLY_SOURCE_OVERLAP_ASSERT_INPUT_INVALID' using errcode='22023';
  end if;

  with worked as (
    select source_row.source_row_ordinal,
           source_row.source_candidate_identity,
           source_row.work_date,
           source_row.start_at_local,
           source_row.end_at_local
    from public.weekly_source_upload_rows source_row
    where source_row.upload_id=p_upload_id
      and source_row.row_finalisation_state='SOURCE_WORKED'
      and source_row.start_at_local is not null
      and source_row.end_at_local is not null
      -- A NULL source charge is a profile that carries no source money at all
      -- and every such accepted row is a claim of worked time.  A NHSP
      -- full-negative row is a reversal, not a claim, and is excluded.  A
      -- structurally valid £0 NHSP row is still a claim of worked time and must
      -- participate in the overlap census.
      and coalesce(source_row.source_shift_charge_pence,1)>=0
  ), overlapping_pairs as (
    select earlier.source_candidate_identity,
           earlier.source_row_ordinal earlier_ordinal,
           later.source_row_ordinal later_ordinal,
           earlier.work_date
    from worked earlier
    join worked later
      on later.source_candidate_identity=earlier.source_candidate_identity
     and later.source_row_ordinal>earlier.source_row_ordinal
     and earlier.start_at_local<later.end_at_local
     and later.start_at_local<earlier.end_at_local
  )
  select pg_catalog.count(*)::integer,
         pg_catalog.string_agg(
           pg_catalog.format('%s rows %s and %s on %s',
             source_candidate_identity,earlier_ordinal,later_ordinal,work_date),
           '; ' order by earlier_ordinal,later_ordinal
         )
    into v_pairs,v_detail
  from overlapping_pairs;

  if v_pairs>0 then
    raise exception 'WEEKLY_SOURCE_OVERLAPPING_WORKED_ROWS'
      using errcode='55000',
        detail=pg_catalog.format('%s overlapping worked pair(s): %s',v_pairs,v_detail);
  end if;
end;
$function$;

alter function private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamptz)
  owner to postgres;
alter function private.weekly_source_overlap_admission_assert_v1(uuid)
  owner to postgres;

revoke all on function private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_overlap_admission_assert_v1(uuid)
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamptz) is
  'WP-54, pack 14 section 4.1.3 and acceptance row NHSP-BR-006. Refuses an NHSP final backing report whose confirmed cutoff occurrence is earlier than any row real Actual finishing instant, overnight finishes included, comparing the local finish converted through Europe/London against the cutoff timestamptz. Exactly equal is admitted. Applies to NHSP_FINAL_BACKING_V1 only; every other profile returns without a check. Read-only: it writes nothing and decides no price, identity or pay.';
comment on function private.weekly_source_overlap_admission_assert_v1(uuid) is
  'WP-54, pack 03 section 7 with pack 14 sections 4.2.8 and 4.1.7. Refuses an upload in which one source candidate identity is claimed as working two intersecting intervals. Half-open intervals, so a split shift that abuts is admitted. A NHSP full-negative row is a reversal under pack 14 section 4.2.4 and is excluded, so a reversal and its re-issue still seal. The whole report is refused because a blocker cannot be ignored and finalisation is all or nothing. Structural, on the source identity at the seal; it is not an overlap check over resolved work events across reports.';

commit;
