-- Repeatable CloudTMS authority: weekly_source_settlement_allocation_v1
--
-- Gate 9 item G9-2 (Plan 6.2 contract section 13; gap row XSG-019).
--
-- The single server-owned source of `Hours paid`, `Hours paid to date` and
-- `current paid hours` for a Weekly Source root.
--
--   * Hours come ONLY from the immutable per-root, per-shift settlement
--     allocation: the signed `public.timesheet_pay_state_history` row for a
--     `(timesheet_id, pay_batch_id)` pair and the `public.pay_batch_timesheet_snapshots`
--     row it was copied from (`proof/32 section 5.2`).
--   * NEVER from a currency-to-hours calculation.  This authority reads no
--     amount, rate, net, gross or VAT column anywhere, and its verifier asserts
--     that by inspecting the installed definition.
--   * NEVER from the one-row-per-Timesheet cache
--     `public.timesheet_pay_state.last_settled_signature` /
--     `last_settled_pay_batch_id`, which holds only the LAST settled batch and
--     would make a root settled in two batches unreadable
--     (`proof/32 section 5.2`, replacement closure review 1 R1-001).
--   * Evidence is read across EVERY physical member of the Timesheet family,
--     because settlement history is keyed by the physical Timesheet version and
--     rotation must never hide prior payment activity
--     (`proof/34 section 8`, `proof/32 section 4.0`).
--   * Malformed or contradictory evidence returns an explicit `UNAVAILABLE`
--     state with a reason.  It never guesses, and the browser then shows no
--     figure (contract section 13 exit: the projection fails closed and the
--     browser performs no financial inference).
--
-- Read-only.  `STABLE`.  It takes no row lock on any Banking Pay table, writes
-- nothing anywhere, and is bounded (family size and evidence-row caps below).
-- It adds no rule: it is listed for HANDOVER 2 awareness only (`XSG-019`).
--
-- SCOPE OF THE AUTHORITY CHAIN (WP-11d F6 - stated exactly, not overstated).
-- `proof/32 section 5.2` lists SEVEN conditions for a terminal settlement.  This
-- reader is a paid-HOURS reader, not the release census, and implements the four
-- that bear on whether the hours evidence is sound:
--
--   IMPLEMENTED  1. `COUNT(*) = 1` over `timesheet_pay_state_history` for the
--                   `(timesheet_id, pay_batch_id)` pair;
--                2. a non-empty signature equal to the signature of the
--                   `pay_batch_timesheet_snapshots` row chosen exactly as the
--                   settle rail chooses it, with a missing row, an empty
--                   signature or a conflicting target snapshot refused - and,
--                   added by WP-11d F2, that signature re-checked against the
--                   content it attests;
--                3. a terminal container: `SETTLED`, or `FAILED` with
--                   `completed_at_utc`, with `execution_commit_state =
--                   'COMMITTED'` and `execution_committed_at_utc`;
--                4. the `pay_batch_candidates` row for the snapshot's own
--                   candidate is `SETTLED` with `settled_at_utc`.
--
--   NOT IMPLEMENTED (the three money-movement conditions, which belong to the
--   release census in `proof/32 section 4`, not to an hours reader, and which
--   this file could not evaluate without reading reservation, transfer and
--   operation state):
--                5. every reservation for the item is `SETTLED` or `RELEASED`;
--                6. every transfer bound to the item is `is_final_money_moved`;
--                7. every `banking_pay_operations` row for the batch is terminal
--                   with both lease forms null or expired.
--
-- A consumer that needs the full seven-condition proof must use the release
-- census, not this reader.
--
-- This authority is CALL-ONLY with respect to Banking Pay: it defines, wraps,
-- re-creates and re-points nothing that Banking Pay owns.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- Bounds.  A Weekly Source root's family and its settlement evidence are both
-- small in every real shape; these caps exist so a damaged or adversarial row
-- set can never turn a read projection into an unbounded scan.  Every cap
-- declared here is applied, and exceeding one is reported as UNAVAILABLE, never
-- truncated silently:
--
--   max_family_members  applied at `:member_count` against the rotation scope;
--   max_history_rows    applied at `:history_count` against the family's
--                       `timesheet_pay_state_history` rows;
--   max_shift_rows      applied in the per-snapshot shift derivation.
--
-- WP-11d F5: a fourth member, `max_snapshot_rows`, was declared here and never
-- referenced anywhere, so the sentence above was not true of it.  The
-- per-pair snapshot lookup is keyed on the unique index
-- `ux_pay_batch_timesheet_snapshots_key (pay_batch_id, timesheet_id,
-- pay_channel)` and is bounded by that key, not by a row cap, so the member is
-- removed rather than given an unreachable branch.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_bounds_v1()
returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'max_family_members',200,
    'max_history_rows',500,
    'max_shift_rows',1000
  );
$function$;

-- ---------------------------------------------------------------------------
-- Per-shift hours from one frozen settlement snapshot.
--
-- The hours-bearing structure of a frozen Banking Pay snapshot is its
-- `segments` array.  Each element carries the shift identity (`segment_id`,
-- `date`, `start_utc`, `end_utc`, `break_mins`, `breaks`) and the five CloudTMS
-- hour buckets (`hours_day`, `hours_night`, `hours_sat`, `hours_sun`,
-- `hours_bh`).  The installed Umbrella/Workbench readers use exactly that
-- structure and exactly those five buckets to decide whether a segment carries
-- paid hours at all (`supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql`
-- lines 88631-88678, the `ts_schedule_rows_all` closure), and the same five
-- buckets are the Weekly Source economic vocabulary
-- (`15092026_1534_weekly_source_finalisation_v1.sql:59`, `:227`, `:233`).
--
-- `exclude_from_pay = true` segments are not paid and are excluded, exactly as
-- the installed reader excludes them.
--
-- Anything that is not a well-formed numeric bucket makes the whole snapshot
-- UNDERIVABLE.  A partially readable snapshot is contradictory evidence, not a
-- smaller number.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_snapshot_shifts_v1(
  p_snapshot jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_segments jsonb;
  v_shifts jsonb;
  v_bad integer;
  v_bounds jsonb:=private.weekly_source_settlement_bounds_v1();
begin
  if p_snapshot is null or pg_catalog.jsonb_typeof(p_snapshot)<>'object' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_NOT_AN_OBJECT','shifts','[]'::jsonb);
  end if;

  v_segments:=p_snapshot->'segments';
  if v_segments is null or pg_catalog.jsonb_typeof(v_segments)<>'array' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_SEGMENTS_ABSENT','shifts','[]'::jsonb);
  end if;
  if pg_catalog.jsonb_array_length(v_segments)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_SEGMENTS_EMPTY','shifts','[]'::jsonb);
  end if;
  if pg_catalog.jsonb_array_length(v_segments)
     >(v_bounds->>'max_shift_rows')::integer then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_SEGMENTS_EXCEED_BOUND','shifts','[]'::jsonb);
  end if;

  -- Any element that is not an object, or that carries a bucket value which is
  -- not a plain signed decimal, or a date that is not an ISO date, makes the
  -- snapshot underivable.
  --
  -- WP-11e G2 corrects the sentence that stood here.  It said "Adjustment
  -- snapshots legitimately carry negative bucket values", which contradicts the
  -- model this file implements: an adjustment Timesheet is its OWN family (it
  -- gets its own hashed `booking_id`, `hr_weekly_phase3_apply_adjustment_truth`
  -- :734-748), so it is never a member of a Weekly Source root's family, and
  -- every snapshot this reader sees is a RESTATEMENT of a whole position.  A
  -- whole position is never negative.  The leading minus is still accepted HERE,
  -- so that a negative value is parsed rather than mis-reported as unreadable,
  -- and the aggregated position is then rejected explicitly as
  -- `SNAPSHOT_POSITION_NEGATIVE` by the reader below.  Rejecting at the parse
  -- step would report "the hours cannot be read", which is not what is wrong.
  select pg_catalog.count(*)::integer
    into v_bad
  from pg_catalog.jsonb_array_elements(v_segments) as segment_element(value)
  where pg_catalog.jsonb_typeof(segment_element.value)<>'object'
     or nullif(pg_catalog.btrim(
          coalesce(segment_element.value->>'date','')),'') is null
     or nullif(pg_catalog.btrim(
          coalesce(segment_element.value->>'date','')),'') !~ '^\d{4}-\d{2}-\d{2}$'
     or exists (
          select 1
          from pg_catalog.unnest(array[
            'hours_day','hours_night','hours_sat','hours_sun','hours_bh'
          ]::text[]) as bucket(name)
          where coalesce(segment_element.value->>bucket.name,'0') !~ '^-?\d+(\.\d+)?$'
        );
  if v_bad>0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','shifts','[]'::jsonb);
  end if;

  select coalesce(pg_catalog.jsonb_agg(shift_row order by
           shift_row->>'date',
           coalesce(shift_row->>'start_utc','') collate "C",
           coalesce(shift_row->>'segment_id','') collate "C"),'[]'::jsonb)
    into v_shifts
  from (
    select pg_catalog.jsonb_build_object(
             'segment_id',nullif(pg_catalog.btrim(
               coalesce(segment_element.value->>'segment_id','')),''),
             'date',pg_catalog.btrim(segment_element.value->>'date'),
             'start_utc',nullif(pg_catalog.btrim(
               coalesce(segment_element.value->>'start_utc','')),''),
             'end_utc',nullif(pg_catalog.btrim(
               coalesce(segment_element.value->>'end_utc','')),''),
             'break_mins',case
               when coalesce(segment_element.value->>'break_mins','0') ~ '^-?\d+(\.\d+)?$'
                 then (segment_element.value->>'break_mins')::numeric
               else 0::numeric end,
             'hours_day',(coalesce(segment_element.value->>'hours_day','0'))::numeric,
             'hours_night',(coalesce(segment_element.value->>'hours_night','0'))::numeric,
             'hours_sat',(coalesce(segment_element.value->>'hours_sat','0'))::numeric,
             'hours_sun',(coalesce(segment_element.value->>'hours_sun','0'))::numeric,
             'hours_bh',(coalesce(segment_element.value->>'hours_bh','0'))::numeric,
             'hours',(coalesce(segment_element.value->>'hours_day','0'))::numeric
                    +(coalesce(segment_element.value->>'hours_night','0'))::numeric
                    +(coalesce(segment_element.value->>'hours_sat','0'))::numeric
                    +(coalesce(segment_element.value->>'hours_sun','0'))::numeric
                    +(coalesce(segment_element.value->>'hours_bh','0'))::numeric
           ) as shift_row
    from pg_catalog.jsonb_array_elements(v_segments) as segment_element(value)
    where coalesce(
            nullif(segment_element.value->>'exclude_from_pay','')::boolean,
            false)=false
  ) as kept;

  return pg_catalog.jsonb_build_object(
    'ok',true,'reason',null,'shifts',v_shifts,
    'excluded_from_pay',(
      select pg_catalog.count(*)::integer
      from pg_catalog.jsonb_array_elements(v_segments) as segment_element(value)
      where coalesce(
              nullif(segment_element.value->>'exclude_from_pay','')::boolean,
              false)=true));
exception
  when invalid_text_representation or numeric_value_out_of_range then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','shifts','[]'::jsonb);
end;
$function$;

-- ---------------------------------------------------------------------------
-- WHY A FIGURE IS ABSENT: the two cases a consumer must be able to tell apart.
--
-- This is an INTERFACE, agreed with WP-11b (18 September 2026).  Its lifecycle
-- resolver was treating every unavailable allocation as a contradiction, which
-- would have turned this reader's deliberate fail-closed gate into an ERROR on
-- a perfectly legitimate week.  The two cases are therefore distinguished in
-- what this reader RETURNS, not only in its internal logic:
--
--   `unavailable_class = 'POSITION_WITHHELD'`
--       The evidence is SOUND.  Every settlement proved individually.  What
--       cannot be stated is the POSITION, either because no installed settlement
--       sequence can prove which settlement states the current position
--       (WP-11e G7) or because the latest settlement is not decidable.  The week
--       is legitimate: the phase still resolves, the heading stands, and the
--       paid schedules are marked unavailable carrying the reason below.  This
--       is NOT a contradiction.
--
--   `unavailable_class = 'EVIDENCE_DAMAGED'`
--       The evidence itself is missing, malformed or contradictory - or the
--       subject could not be identified at all.  This IS a contradiction and
--       the projection fails closed (contract section 13 exit).
--
-- `reason_detail` carries a plain-English sentence for each reason, so an Office
-- user reads something true rather than a code.  (This reader never feeds the
-- MyTMS payload - the Candidate view producer does not call it - so ordinary
-- words like "paid" are safe here.)
--
-- `settlement_count` and `batch_count` are present on every result where the
-- family resolved, including both unavailable classes, because WP-11b resolves
-- the phase from those COUNTS without ever taking a figure.  They are counts of
-- financial events.  They must never be replaced by, or conflated with, an
-- amount or an hours figure.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_reason_class_v1(
  p_reason text
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'unavailable_class',
      case p_reason
        when 'SETTLEMENT_SEQUENCE_UNPROVABLE' then 'POSITION_WITHHELD'
        when 'SETTLEMENT_ORDER_AMBIGUOUS' then 'POSITION_WITHHELD'
        -- WP-11e G7.  The reason this branch replaces is retained here so that
        -- a caller still holding the old literal is classed correctly rather
        -- than falling through to EVIDENCE_DAMAGED.  NOTHING IN THIS FILE
        -- PRODUCES IT any more; it is a compatibility mapping only.
        when 'SETTLEMENT_POSITION_SEMANTICS_UNRULED' then 'POSITION_WITHHELD'
        else 'EVIDENCE_DAMAGED'
      end,
    'reason_detail',
      case p_reason
        -- WP-11e G7.  The finance approver HAS ruled (HANDOVER 2 response R5
        -- B1a): the latest authoritative settlement restates the complete paid
        -- position, and it must be selected by the installed settlement
        -- sequence/revision contract, not by a timestamp.  No such contract
        -- exists on any installed evidence relation, so the ordering the ruling
        -- requires cannot be proved and the figure is still withheld - but for
        -- the TRUE reason, which is not "the semantics are unruled".
        when 'SETTLEMENT_SEQUENCE_UNPROVABLE' then
          'This week has been paid more than once. The payment records are '
          ||'complete and consistent, but which of the payments states the '
          ||'current position cannot be proved from an installed settlement '
          ||'sequence, so no figure is shown.'
        when 'SETTLEMENT_POSITION_SEMANTICS_UNRULED' then
          'This week has been paid more than once and no paid figure is being '
          ||'shown. (Superseded reason, retained only so that an older caller '
          ||'reads something true; this projection no longer produces it.)'
        when 'SNAPSHOT_POSITION_NEGATIVE' then
          'The payment record for this week states a negative number of hours, '
          ||'which cannot be a paid position. No paid figure can be stated.'
        when 'SETTLEMENT_ORDER_AMBIGUOUS' then
          'This week has been paid more than once and the payments carry the '
          ||'same completion time, so which one states the current position '
          ||'cannot be decided. The paid figure is being withheld.'
        when 'ROOT_NOT_FOUND' then
          'This Timesheet could not be found.'
        when 'FAMILY_UNRESOLVED' then
          'The versions of this Timesheet could not be resolved, so its payment '
          ||'records cannot be read.'
        when 'FAMILY_EXCEEDS_BOUND' then
          'This Timesheet has more versions than this projection will read.'
        when 'EVIDENCE_EXCEEDS_BOUND' then
          'This week has more payment records than this projection will read.'
        when 'SETTLEMENT_HISTORY_CONFLICT' then
          'This week has more than one payment record for the same payment run, '
          ||'which is contradictory. No paid figure can be stated.'
        when 'SETTLEMENT_SNAPSHOT_CONFLICT' then
          'The payment record for this week does not match the signed record it '
          ||'was taken from. No paid figure can be stated.'
        when 'SETTLEMENT_STATUS_CONTRADICTS_HISTORY' then
          'This week has a payment record, but the payment run it belongs to '
          ||'does not show as completed and settled. No paid figure can be '
          ||'stated.'
        when 'SNAPSHOT_SEGMENTS_ABSENT' then
          'The signed payment record for this week carries no shifts, so paid '
          ||'hours cannot be read from it.'
        when 'SNAPSHOT_SEGMENTS_EMPTY' then
          'The signed payment record for this week carries an empty list of '
          ||'shifts, so paid hours cannot be read from it.'
        when 'SNAPSHOT_SEGMENTS_EXCEED_BOUND' then
          'The signed payment record for this week carries more shifts than '
          ||'this projection will read.'
        when 'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE' then
          'The hours on the signed payment record for this week cannot be read, '
          ||'so no paid figure can be stated. A partly readable record is never '
          ||'shown as a smaller number.'
        when 'SNAPSHOT_NOT_AN_OBJECT' then
          'The signed payment record for this week is not in a readable form.'
        else
          'The paid figure cannot be stated from the payment records for this '
          ||'week.'
      end);
$function$;

-- ---------------------------------------------------------------------------
-- THE SETTLEMENT-POSITION GATE.
--
-- WP-11e G7 - READ THIS BEFORE TOUCHING THE GATE.
--
-- The finance approver HAS now ruled (HANDOVER 2 implementation rulings
-- response R5, section B1a, word for word):
--
--   "APPROVED: the latest authoritative settlement restates the complete paid
--    position for that root and shift.  Do not sum successive restatements. ...
--    Select the latest authoritative settlement by the installed settlement
--    sequence/revision contract, not by an arbitrary timestamp.  If two rows
--    contend for currentness or the ordering cannot be proved, show no figure
--    and the explicit reason."
--
-- The restatement half of that ruling is what this file implements.  The
-- SELECTION half cannot be satisfied today, and that is why this gate stays
-- CLOSED:
--
--   * no installed evidence relation carries a settlement sequence or revision.
--     `timesheet_pay_state_history` has exactly six columns (`id`,
--     `timesheet_id`, `pay_batch_id`, `settled_at_utc`, `snapshot_json`,
--     `signature`) and its `id` defaults to `gen_random_uuid()`; `pay_batches`
--     carries creation, commit and completion times, a Workbench
--     `source_session_version` and a bank `bulk_ref_num` that no installed
--     routine assigns.  Executed census, PostgreSQL 17.11, release 15 clone;
--     exactly one installed routine (`pay_settle_rail`) inserts settlement
--     history;
--   * `settled_at_utc` is NOT a sequence.  It is the settle rail's transaction
--     start time (`pay_settle_rail`: `v_now := now()`), one single value stamped
--     on every history row of a whole batch, unrelated to commit order or to the
--     order the Drafts were built in.  Two batches one microsecond apart are
--     resolved by that microsecond - executed, and the batch created FIRST won.
--
-- So under B1a "the ordering cannot be proved", and the ruled outcome is no
-- figure and the explicit reason.  THAT IS WHAT THIS GATE DOES.  It is not a
-- placeholder for an unmade decision any more; it is the ruled behaviour.
--
-- **DO NOT REMOVE THIS BRANCH TO "APPLY THE RULING".**  Removing it makes the
-- reader select by `settled_at_utc`, which is precisely the arbitrary timestamp
-- the ruling forbids.  The branch may be removed only when BOTH of these are
-- true, and they are stated in full in `IMPL\handoffs\WP-11e_NEEDS.md` D1:
--
--   1. an installed settlement sequence/revision contract exists - a column the
--      settle rail writes monotonically per settlement, total per root family
--      and never re-used - which HANDOVER 2 must name or Banking Pay must
--      install (contract section 2 forbids THIS project adding it); and
--   2. the position pick and the tie test below both order by that column
--      instead of `settled_at_utc`.
--
-- Until then `settled_at_utc` may remain ONLY as the audit ordering of the
-- `settlements` array, never as the thing that chooses a figure.
--
-- The installed Banking Pay owner RESTATES the whole position in every
-- settlement snapshot and moves only the money residual:
--
--   * `public.pay_preview_candidate_build_timesheet_snapshots` builds
--     `target_snapshot_json` from the CURRENT `timesheets_financials` row - the
--     whole current position, `segments` and all five hour buckets (installed
--     definition, the `page_rows_with_json` block) - and sets
--     `base_snapshot_json` to `COALESCE(last_settled_snapshot_json,'{}')`;
--   * `public.pay_batch_create_timesheet_snapshots` stores that target and signs
--     it `md5(target_snapshot_json::text)` (installed definition line 329);
--   * `public.pay_settle_rail` copies the chosen row's `target_snapshot_json`
--     verbatim into `timesheet_pay_state_history.snapshot_json` (installed
--     definition `:5293-5326`), computes the money as
--     `sum(truth_ex_vat - baseline_ex_vat)` and upserts
--     `timesheet_pay_state.last_settled_snapshot_json = target_snapshot_json`
--     as the new base (`:5340-5414`).
--
-- So the paid position after N settlements is the LATEST settlement's restated
-- position, not the sum of N snapshots.  Summing reports a week paid 8 hours and
-- then adjusted to 9 as 17, and one paid 8 and then recovered to 7 as 15.
--
-- The restatement reading is implemented below.  Because no installed sequence
-- can prove WHICH restatement is current, this gate refuses to state a figure
-- for any family carrying more than one settlement: a missing figure is
-- recoverable, a wrong figure shown to a worker is not.
--
-- WP-11e G1 - THE EXACT CHANGE, FOR THE DAY CONDITION 1 AND 2 ABOVE ARE MET.
-- WP-11d's report and handoff said "delete these two lines ... leaving
-- `return null;` as the fall-through".  There is no `return null;` after the
-- branch; the only one is the `v_count<=1` early return above it.  Performing
-- that deletion literally leaves a plpgsql function that falls off its end, and
-- EVERY multi-settlement root then raises `2F005 control reached end of function
-- without RETURN` instead of stating a figure.  Executed on the installed text
-- (WP-11e report, G1).  The change is a ONE-LINE SUBSTITUTION, not a deletion:
--
--     replace   return 'SETTLEMENT_SEQUENCE_UNPROVABLE';
--     with      return null;
--
-- and, in the same change, re-point the tie test below and the position pick in
-- the reader (`order by ... settled_at_utc desc limit 1`) at the installed
-- sequence column.  The `SETTLEMENT_ORDER_AMBIGUOUS` branch must survive it:
-- two settlements that cannot be separated by the ordering contract are an
-- integrity fault independent of the semantics, and a contention is never
-- resolved by sort order.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_position_gate_v1(
  p_settlements jsonb
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_count integer;
  v_unordered integer;
  v_at_max integer;
begin
  v_count:=pg_catalog.jsonb_array_length(coalesce(p_settlements,'[]'::jsonb));
  if v_count<=1 then
    return null;
  end if;

  -- A settlement with no settled_at_utc cannot be ordered against the others.
  select pg_catalog.count(*)::integer
    into v_unordered
  from pg_catalog.jsonb_array_elements(p_settlements) as element(value)
  where nullif(pg_catalog.btrim(
          coalesce(element.value->>'settled_at_utc','')),'') is null;
  if v_unordered>0 then
    return 'SETTLEMENT_ORDER_AMBIGUOUS';
  end if;

  -- Two settlements sharing the maximum settled_at_utc: which one restates the
  -- position is not decidable from the evidence.  Never resolved by sort order.
  -- WP-11e G7: when the installed sequence contract exists this test moves to
  -- that column; until then it is kept because it is strictly stronger than
  -- nothing, and it is NOT what decides the withholding below.
  select pg_catalog.count(*)::integer
    into v_at_max
  from pg_catalog.jsonb_array_elements(p_settlements) as element(value)
  where (element.value->>'settled_at_utc')::timestamptz
      = (select pg_catalog.max((inner_element.value->>'settled_at_utc')::timestamptz)
         from pg_catalog.jsonb_array_elements(p_settlements) as inner_element(value));
  if v_at_max>1 then
    return 'SETTLEMENT_ORDER_AMBIGUOUS';
  end if;

  -- === WP-11e G7.  Ruling B1a requires selection by an installed settlement
  -- === sequence/revision contract.  No such contract exists on any installed
  -- === evidence relation, so the ordering the ruling requires CANNOT BE
  -- === PROVED, and B1a's own answer to that is: no figure and the explicit
  -- === reason.  Substitute `return null;` here ONLY together with the two
  -- === conditions in the header block above.  Do NOT delete this line.
  return 'SETTLEMENT_SEQUENCE_UNPROVABLE';
end;
$function$;

-- ---------------------------------------------------------------------------
-- WP-11e G8.  THE POSITION UNIT.  One function, one literal, one edit.
--
-- Ruling B1a says "the latest authoritative settlement restates the complete
-- paid position **for that root and shift**", and separately that "a wider total
-- across different roots or shifts may sum each root/shift's latest restated
-- position once".  Its unit of restatement is therefore the pair (root, shift),
-- and this reader implements that:
--
--   'ROOT_AND_SHIFT'  for each shift identity, the position is the hours stated
--                     by the LATEST settlement that restates THAT shift.  A
--                     shift restated once and then absent from a later
--                     settlement keeps the hours of the settlement that stated
--                     it.  This is the ruling's own words and is what ships.
--
--   'ROOT'            the position is the whole of the latest settlement's
--                     snapshot, so a shift absent from that snapshot is read as
--                     no longer paid.  This matches what the installed writer
--                     PRODUCES - its snapshot is the Timesheet's complete
--                     current position, and a shift removed from the Timesheet
--                     is absent from it with its money recovered as the residual
--                     - and it is what the WP-11d reviewer recommended.
--
-- THE TWO READINGS DISAGREE ON A REAL FIGURE.  Settlement 1 restating shift A =
-- 8 h and shift B = 4 h, followed by settlement 2 restating shift A = 9 h only,
-- reads 13 hours under 'ROOT_AND_SHIFT' and 9 hours under 'ROOT'.  Both are
-- executed in the verifier.  The approver must decide which is intended: the
-- question is recorded in `IMPL\handoffs\WP-11e_NEEDS.md` D2.  Changing the
-- answer is changing the literal below and nothing else.
--
-- This choice is UNOBSERVABLE while the G7 gate is closed, because the two
-- readings can only differ across more than one settlement and every
-- multi-settlement root is withheld.  It is proved identical on every
-- single-settlement shape by the verifier.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_position_unit_v1()
returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select 'ROOT_AND_SHIFT'::text;
$function$;

-- ---------------------------------------------------------------------------
-- The settlement allocation reader.
--
-- Returns the per-shift settled position for the whole Timesheet family of
-- `p_root_timesheet_id`, or an explicit UNAVAILABLE state.
--
-- **The position is a RESTATEMENT, not a sum** (WP-11d F1).  Every settlement
-- snapshot restates the Timesheet's whole position and Banking Pay moves only
-- the residual, so the paid position is the LATEST settlement's restated
-- position, per the unit that `weekly_source_settlement_position_unit_v1`
-- names (WP-11e G8).  The full `settlements` array is retained for audit and no
-- settlement event is collapsed.  See the gate function above for the installed
-- writer citations and for why the fail-closed gate stays closed.
--
-- **WHAT THIS READER CAN AND CANNOT PROVE ABOUT THE SNAPSHOT** (WP-11e G5, the
-- third caveat, which WP-11d's report did not state).  What this reader reads is
-- `pay_batch_timesheet_snapshots.target_snapshot_json -> 'segments'` of the row
-- the settle rail chose, and it can prove exactly that much.  It CANNOT prove
-- which branch produced those segments.  The one installed writer of that
-- relation, `public.pay_batch_create_timesheet_snapshots`, composes
-- `display_metadata_json.segments` from a SIX-WAY COALESCE (executed dump,
-- release 15 clone, PostgreSQL 17.11):
--
--     allocation_target_json.segments                      <- the whole position
--     allocation_line_json.segments
--     allocation_base_json.segments                        <- the PREVIOUS position
--     frozen_source_basis_json.segments
--     frozen_source_basis_json.target_snapshot_json.segments
--     frozen_component_snapshot_json.segments               <- ONE component only
--
-- and takes them from the FIRST allocation row of the operation for that
-- (timesheet, channel) (`LIMIT 1`).  Only the first branch is the whole restated
-- position.  Where a fallback fed a row, "the latest restatement" would be
-- reading a previous or partial position as the whole one, and nothing in the
-- stored row distinguishes them.  The fail-closed gate above is what makes this
-- safe today, because no figure is stated for a root settled more than once.
-- Before the gate is opened the finance approver must confirm the model against
-- real TEST snapshot rows - in particular that `OVERPAYMENT_RECOVERY` and
-- `UNDERPAYMENT_PAYMENT` lines carrying a `timesheet_id` never feed a
-- component-only `segments` array.  That is handoff D3.
--
-- **Hours are derived from the SIGNED snapshot, not from the history copy**
-- (WP-11d F2).  The history row's own `snapshot_json` is compared to the chosen
-- `pay_batch_timesheet_snapshots.target_snapshot_json`, and that row's signature
-- is re-computed as the installed writer computes it
-- (`md5(target_snapshot_json::text)`, `pay_batch_create_timesheet_snapshots`
-- line 329 - this file invents no signing scheme of its own).  A divergence in
-- either direction is contradictory evidence and fails closed.
--
-- States:
--   AVAILABLE    every settlement in the family is provable, its hours are
--                derivable, and the position is unambiguous; `shifts`,
--                `total_hours` and `hours_by_bucket` are the restated position.
--   NO_SETTLEMENT no settlement history exists for any family member.  Nothing
--                has been paid.  This is a normal result, not an error, and it
--                carries NO numeric member at all (WP-11d F8): a week that has
--                not been paid has no paid figure, it does not have zero.
--   UNAVAILABLE  the evidence is missing, malformed or contradictory, or the
--                settlement that states the current position cannot be proved.
--                `reason` names it.  No figure is produced, not even a partial
--                one.
--
-- Every result carries `settlement_count` and `batch_count` as JSON NUMBERS,
-- and `evidence_counts_basis` naming what they were counted from (WP-11e G4).
--
-- Reasons (all from `proof/32 section 5.2` unless marked):
--   ROOT_NOT_FOUND                     the requested Timesheet does not exist
--   FAMILY_UNRESOLVED                  the rotation resolver returned no family
--   FAMILY_EXCEEDS_BOUND               bound
--   EVIDENCE_EXCEEDS_BOUND             bound
--   SETTLEMENT_HISTORY_CONFLICT        more than one history row for a pair
--   SETTLEMENT_SNAPSHOT_CONFLICT       missing / empty-signature / ambiguous /
--                                      signature-mismatch snapshot, a history
--                                      copy that has diverged from the signed
--                                      snapshot, a signature that does not bind
--                                      the content it attests (WP-11d F2), or a
--                                      snapshot settled under a Candidate who is
--                                      not the Timesheet's contract Candidate
--                                      (WP-11e G6)
--   SETTLEMENT_STATUS_CONTRADICTS_HISTORY  a history row exists but the batch or
--                                      its Candidate row does not say settled
--   SETTLEMENT_ORDER_AMBIGUOUS         two settlements share the maximum
--                                      `settled_at_utc`, or one cannot be
--                                      ordered at all, so which one restates the
--                                      position is not decidable (WP-11d F1)
--   SETTLEMENT_SEQUENCE_UNPROVABLE     more than one settlement, and ruling B1a
--                                      requires the latest to be chosen by an
--                                      installed settlement sequence/revision
--                                      contract, which does not exist, so the
--                                      ordering cannot be proved (WP-11e G7)
--   SNAPSHOT_POSITION_NEGATIVE         the stated position, or one of its shifts
--                                      or buckets, is negative.  A whole
--                                      restated position can never be negative
--                                      (WP-11e G2)
--   SNAPSHOT_SEGMENTS_ABSENT           the snapshot carries no shift structure
--   SNAPSHOT_SEGMENTS_EMPTY            it carries an empty one
--   SNAPSHOT_SEGMENTS_EXCEED_BOUND     bound
--   SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE hours not readable from the snapshot
--   SNAPSHOT_NOT_AN_OBJECT             the stored snapshot is not an object
--
-- RETIRED, and produced by nothing in this file (WP-11e G7):
--   SETTLEMENT_POSITION_SEMANTICS_UNRULED  the semantics ARE ruled now.  The
--                                      reason class function still maps the
--                                      literal so that an older caller is not
--                                      handed an unclassified reason.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_settlement_allocation_v1(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_bounds jsonb:=private.weekly_source_settlement_bounds_v1();
  v_family text;
  v_canonical uuid;
  v_members uuid[];
  v_member_count integer;
  v_history_count integer;
  v_settlements jsonb:='[]'::jsonb;
  v_failure jsonb;
  v_shifts jsonb;
  v_total numeric;
  v_day numeric; v_night numeric; v_sat numeric; v_sun numeric; v_bh numeric;
  v_first timestamptz;
  v_last timestamptz;
  v_batch_count integer;
  v_gate text;
  v_position jsonb;
  v_unit text:=private.weekly_source_settlement_position_unit_v1();
  v_position_settlements integer;
  v_negative integer;
  -- WP-11e G4.  `settlement_count` and `batch_count` are load-bearing for
  -- WP-11b's phase resolution, so they are present as JSON NUMBERS on EVERY
  -- result of this function, including the paths where no evidence could be
  -- read at all.  `evidence_counts_basis` says what they were counted from, so
  -- a consumer can tell "no settlements exist" from "no evidence could be
  -- read".  It is never a substitute for branching on `state`.
  v_no_evidence jsonb:=pg_catalog.jsonb_build_object(
    'settlement_count',0,'batch_count',0,
    'evidence_counts_basis','NO_EVIDENCE_READ');
begin
  if p_root_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','ROOT_NOT_FOUND',
      'root_timesheet_id',null)
      ||v_no_evidence
      ||private.weekly_source_settlement_reason_class_v1('ROOT_NOT_FOUND');
  end if;

  if not exists (
    select 1 from public.timesheets as root_row
    where root_row.timesheet_id=p_root_timesheet_id) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','ROOT_NOT_FOUND',
      'root_timesheet_id',p_root_timesheet_id)
      ||v_no_evidence
      ||private.weekly_source_settlement_reason_class_v1('ROOT_NOT_FOUND');
  end if;

  -- Every physical member of the family, current and historical.  The installed
  -- rotation resolver is CALL-ONLY (contract section 2).
  select pg_catalog.min(scope_row.booking_id),
         pg_catalog.min(scope_row.canonical_timesheet_id::text)::uuid,
         pg_catalog.array_agg(distinct scope_row.family_timesheet_id),
         pg_catalog.count(distinct scope_row.family_timesheet_id)::integer
    into v_family,v_canonical,v_members,v_member_count
  from public._pay_timesheet_rotation_scope(array[p_root_timesheet_id]) as scope_row;

  if v_members is null or v_member_count=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','FAMILY_UNRESOLVED',
      'root_timesheet_id',p_root_timesheet_id)
      ||v_no_evidence
      ||private.weekly_source_settlement_reason_class_v1('FAMILY_UNRESOLVED');
  end if;
  if v_member_count>(v_bounds->>'max_family_members')::integer then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','FAMILY_EXCEEDS_BOUND',
      'root_timesheet_id',p_root_timesheet_id,'member_count',v_member_count)
      ||v_no_evidence
      ||private.weekly_source_settlement_reason_class_v1('FAMILY_EXCEEDS_BOUND');
  end if;

  select pg_catalog.count(*)::integer
    into v_history_count
  from public.timesheet_pay_state_history as history_row
  where history_row.timesheet_id=any(v_members);

  if v_history_count=0 then
    -- WP-11d F8.  Nothing has been paid, so there is no paid figure.  This
    -- result carries NO `total_hours`, `hours_by_bucket` or `shifts` member at
    -- all, exactly as the UNAVAILABLE results do, so a consumer that reads a
    -- figure without branching on `state` cannot print "0 hours paid" for a week
    -- the lifecycle matrix never shows as paid.  `batch_count` and
    -- `settlement_count` are counts of financial EVENTS, not hour figures, and
    -- are retained.
    return pg_catalog.jsonb_build_object(
      'ok',true,'state','NO_SETTLEMENT','reason',null,
      -- Present and null, so a consumer can read them unconditionally.
      'unavailable_class',null,'reason_detail',null,
      'root_timesheet_id',p_root_timesheet_id,
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
      'batch_count',0,'settlement_count',0,
      'evidence_counts_basis','FAMILY_EVIDENCE',
      'first_settled_at_utc',null,'last_settled_at_utc',null,
      'settlements','[]'::jsonb);
  end if;
  if v_history_count>(v_bounds->>'max_history_rows')::integer then
    -- WP-11e G4.  `settlement_count` here used to be the RAW history-row count,
    -- which is not a count of settlements: 501 duplicate rows for one settled
    -- pair reported 501.  The raw count keeps its own name, and the two
    -- published counts are counted the way they are counted everywhere else -
    -- distinct (family member, batch) pairs and distinct batches.  Both are
    -- bounded by the family, which is already capped above.
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','EVIDENCE_EXCEEDS_BOUND',
      'root_timesheet_id',p_root_timesheet_id,
      'history_row_count',v_history_count,
      'settlement_count',(
        select pg_catalog.count(*)::integer
        from (select distinct history_row.timesheet_id,history_row.pay_batch_id
              from public.timesheet_pay_state_history as history_row
              where history_row.timesheet_id=any(v_members)) as distinct_pairs),
      'batch_count',(
        select pg_catalog.count(distinct history_row.pay_batch_id)::integer
        from public.timesheet_pay_state_history as history_row
        where history_row.timesheet_id=any(v_members)),
      'evidence_counts_basis','FAMILY_EVIDENCE',
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members))
      ||private.weekly_source_settlement_reason_class_v1('EVIDENCE_EXCEEDS_BOUND');
  end if;

  -- One row per (family member, batch) pair, with every proof `proof/32
  -- section 5.2` requires attached to it.  Nothing is aggregated before the
  -- proofs are complete.
  with pairs as (
    select history_row.timesheet_id,
           history_row.pay_batch_id,
           pg_catalog.count(*)::integer as history_rows,
           pg_catalog.min(history_row.id::text)::uuid as history_id,
           pg_catalog.min(history_row.settled_at_utc) as settled_at_utc,
           pg_catalog.min(history_row.signature) as signature,
           pg_catalog.min(history_row.snapshot_json::text) as snapshot_text
    from public.timesheet_pay_state_history as history_row
    where history_row.timesheet_id=any(v_members)
    group by history_row.timesheet_id,history_row.pay_batch_id
  ), chosen_snapshot as (
    select pairs.timesheet_id,pairs.pay_batch_id,
           snapshot_pick.id as snapshot_id,
           snapshot_pick.signature as snapshot_signature,
           snapshot_pick.target_snapshot_json,
           snapshot_pick.candidate_id,
           snapshot_stats.snapshot_rows,
           snapshot_stats.distinct_targets
    from pairs
    left join lateral (
      select snapshot_row.id,snapshot_row.signature,snapshot_row.target_snapshot_json,
             snapshot_row.candidate_id
      from public.pay_batch_timesheet_snapshots as snapshot_row
      where snapshot_row.pay_batch_id=pairs.pay_batch_id
        and snapshot_row.timesheet_id=pairs.timesheet_id
      order by snapshot_row.created_at_utc desc,snapshot_row.id
      limit 1
    ) as snapshot_pick on true
    left join lateral (
      select pg_catalog.count(*)::integer as snapshot_rows,
             pg_catalog.count(distinct snapshot_row.target_snapshot_json::text)::integer
               as distinct_targets
      from public.pay_batch_timesheet_snapshots as snapshot_row
      where snapshot_row.pay_batch_id=pairs.pay_batch_id
        and snapshot_row.timesheet_id=pairs.timesheet_id
    ) as snapshot_stats on true
  ), container as (
    select chosen_snapshot.*,
           batch_row.status as batch_status,
           batch_row.completed_at_utc,
           batch_row.execution_commit_state,
           batch_row.execution_committed_at_utc,
           candidate_row.settlement_status,
           candidate_row.settled_at_utc as candidate_settled_at_utc,
           -- WP-11e G6.  The Candidate the Timesheet is CONTRACTED to, carried
           -- through `timesheets.contract_id`.  The snapshot names its own
           -- `candidate_id` and everything below is reached through it, so
           -- without this the reader will happily state hours for a Timesheet
           -- that was settled under somebody else.  That is contradictory
           -- evidence and the pack's direction for contradictory evidence is
           -- fail-closed.  Three-valued: a Timesheet with no contract, or a
           -- snapshot with no Candidate, is not a match.
           contract_row.candidate_id as contract_candidate_id
    from chosen_snapshot
    left join public.timesheets as member_row
      on member_row.timesheet_id=chosen_snapshot.timesheet_id
    left join public.contracts as contract_row
      on contract_row.id=member_row.contract_id
    left join public.pay_batches as batch_row
      on batch_row.id=chosen_snapshot.pay_batch_id
    -- The Candidate row is reached through the frozen snapshot's own
    -- `candidate_id`, which the settle rail scopes by, so the join never
    -- depends on a live `pay_batch_items` row still existing.
    left join lateral (
      select pay_candidate.settlement_status,pay_candidate.settled_at_utc
      from public.pay_batch_candidates as pay_candidate
      where pay_candidate.pay_batch_id=chosen_snapshot.pay_batch_id
        and pay_candidate.candidate_id=chosen_snapshot.candidate_id
      order by pay_candidate.id
      limit 1
    ) as candidate_row on true
  )
  select coalesce(pg_catalog.jsonb_agg(settlement_row order by
           settlement_row->>'settled_at_utc',
           settlement_row->>'pay_batch_id',
           settlement_row->>'timesheet_id'),'[]'::jsonb)
    into v_settlements
  from (
    select pg_catalog.jsonb_build_object(
             'timesheet_id',pairs.timesheet_id,
             'pay_batch_id',pairs.pay_batch_id,
             'history_id',pairs.history_id,
             'history_rows',pairs.history_rows,
             'settled_at_utc',pairs.settled_at_utc,
             'signature_present',
               nullif(pg_catalog.btrim(coalesce(pairs.signature,'')),'')
                 is not null,
             'snapshot_id',container.snapshot_id,
             'snapshot_rows',coalesce(container.snapshot_rows,0),
             'snapshot_distinct_targets',coalesce(container.distinct_targets,0),
             'signature_matches',
               nullif(pg_catalog.btrim(coalesce(pairs.signature,'')),'')
                 is not distinct from
               nullif(pg_catalog.btrim(coalesce(container.snapshot_signature,'')),''),
             -- WP-11d F2.  A signature exists to bind an attestation to the
             -- content it attests.  Two facts are carried and both are checked
             -- in the failure scan below:
             --   content_matches  the history row's own `snapshot_json` is the
             --                    chosen signed `target_snapshot_json`;
             --   signature_binds_content
             --                    that signature re-computes from that content
             --                    by the INSTALLED writer's own scheme,
             --                    `md5(target_snapshot_json::text)`
             --                    (`pay_batch_create_timesheet_snapshots`
             --                    line 329).  No new scheme is invented here;
             --                    exactly one installed routine writes this
             --                    relation and this is how it signs.
             'content_matches',
               pairs.snapshot_text::jsonb
                 is not distinct from container.target_snapshot_json,
             'signature_binds_content',
               pg_catalog.md5(container.target_snapshot_json::text)
                 is not distinct from
               nullif(pg_catalog.btrim(
                 coalesce(container.snapshot_signature,'')),''),
             -- WP-11e G6.  The snapshot's Candidate is the Timesheet's contract
             -- Candidate.  Both sides are compared as non-null values, so an
             -- absent contract or an absent snapshot Candidate is `false`, not
             -- null, and routes to the fail-closed branch either way.
             'snapshot_candidate_id',container.candidate_id,
             'contract_candidate_id',container.contract_candidate_id,
             'candidate_matches_contract',
               container.candidate_id is not null
               and container.contract_candidate_id is not null
               and container.candidate_id=container.contract_candidate_id,
             'batch_status',container.batch_status,
             'batch_terminal',
               container.batch_status='SETTLED'
               or (container.batch_status='FAILED'
                   and container.completed_at_utc is not null),
             'execution_committed',
               container.execution_commit_state='COMMITTED'
               and container.execution_committed_at_utc is not null,
             'candidate_settlement_status',container.settlement_status,
             'candidate_settled',
               container.settlement_status='SETTLED'
               and container.candidate_settled_at_utc is not null,
             -- Hours come from the CHOSEN SIGNED row, never from the history
             -- copy.  The two are proved equal by `content_matches` before any
             -- figure is produced (WP-11d F2).
             'derived',private.weekly_source_settlement_snapshot_shifts_v1(
               case when pg_catalog.jsonb_typeof(
                      coalesce(container.target_snapshot_json,'null'::jsonb))='object'
                 then container.target_snapshot_json end)
           ) as settlement_row
    from pairs
    join container
      on container.timesheet_id=pairs.timesheet_id
     and container.pay_batch_id=pairs.pay_batch_id
  ) as settlement_rows;

  -- First failure wins, in a fixed order, so the same damaged evidence always
  -- reports the same reason.
  select pg_catalog.jsonb_build_object(
           'reason',failure.reason,
           'timesheet_id',failure.value->>'timesheet_id',
           'pay_batch_id',failure.value->>'pay_batch_id')
    into v_failure
  from pg_catalog.jsonb_array_elements(v_settlements) as element(value)
  cross join lateral (
    select element.value,
           case
             when (element.value->>'history_rows')::integer>1
               then 'SETTLEMENT_HISTORY_CONFLICT'
             when (element.value->>'signature_present')::boolean is not true
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             when element.value->>'snapshot_id' is null
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             when (element.value->>'snapshot_distinct_targets')::integer>1
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             when (element.value->>'signature_matches')::boolean is not true
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             -- WP-11d F2, in this order: the history copy must BE the signed
             -- content, and the signature must re-compute from that content.
             -- Both are three-valued, so a null routes to the fail-closed
             -- branch (`is not true`).
             when (element.value->>'content_matches')::boolean is not true
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             when (element.value->>'signature_binds_content')::boolean is not true
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             -- WP-11e G6.  A settlement recorded against this Timesheet under a
             -- Candidate who is not the Timesheet's contract Candidate.
             when (element.value->>'candidate_matches_contract')::boolean
                    is not true
               then 'SETTLEMENT_SNAPSHOT_CONFLICT'
             when (element.value->>'batch_terminal')::boolean is not true
               or (element.value->>'execution_committed')::boolean is not true
               or (element.value->>'candidate_settled')::boolean is not true
               then 'SETTLEMENT_STATUS_CONTRADICTS_HISTORY'
             when (element.value#>>'{derived,ok}')::boolean is not true
               then coalesce(element.value#>>'{derived,reason}',
                             'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE')
             else null
           end as reason
  ) as failure
  where failure.reason is not null
  order by case failure.reason
             when 'SETTLEMENT_HISTORY_CONFLICT' then 1
             when 'SETTLEMENT_SNAPSHOT_CONFLICT' then 2
             when 'SETTLEMENT_STATUS_CONTRADICTS_HISTORY' then 3
             else 4 end,
           element.value->>'settled_at_utc',
           element.value->>'pay_batch_id',
           element.value->>'timesheet_id'
  limit 1;

  if v_failure is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason',v_failure->>'reason',
      'root_timesheet_id',p_root_timesheet_id,
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
      'settlement_count',pg_catalog.jsonb_array_length(v_settlements),
      -- WP-11b interface: the evidence COUNTS are load-bearing for the
      -- paid-hours screen, so they are present on every unavailable result where
      -- the family resolved.  They are counts of financial events, never
      -- amounts and never hours.
      'batch_count',(
        select pg_catalog.count(distinct element.value->>'pay_batch_id')::integer
        from pg_catalog.jsonb_array_elements(v_settlements) as element(value)),
      'evidence_counts_basis','FAMILY_EVIDENCE',
      'failed_timesheet_id',v_failure->>'timesheet_id',
      'failed_pay_batch_id',v_failure->>'pay_batch_id',
      'settlements',v_settlements)
      ||private.weekly_source_settlement_reason_class_v1(v_failure->>'reason');
  end if;

  -- WP-11d F1 / WP-11e G7.  The settlement-position gate.  Everything above this
  -- point has already proved each settlement individually; this decides whether a
  -- POSITION can be stated from them at all.  See the gate function for ruling
  -- B1a, for why the branch stays closed, and for the exact one-line change that
  -- opens it once an installed settlement sequence contract exists.
  v_gate:=private.weekly_source_settlement_position_gate_v1(v_settlements);
  if v_gate is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason',v_gate,
      'root_timesheet_id',p_root_timesheet_id,
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
      'settlement_count',pg_catalog.jsonb_array_length(v_settlements),
      'batch_count',(
        select pg_catalog.count(distinct element.value->>'pay_batch_id')::integer
        from pg_catalog.jsonb_array_elements(v_settlements) as element(value)),
      'evidence_counts_basis','FAMILY_EVIDENCE',
      'settlements',v_settlements)
      ||private.weekly_source_settlement_reason_class_v1(v_gate);
  end if;

  -- The RESTATED position.  Each settlement snapshot restates the whole position
  -- of its unit (the installed writer citations are on the gate function), so
  -- the paid position is the LATEST restatement of each unit - never a sum
  -- across settlements.  Every settlement stays in `settlements` for audit; no
  -- financial event is collapsed (`proof/32 section 5.2`).
  --
  -- WP-11e G7.  `settled_at_utc` is used as the ordering below ONLY because no
  -- installed settlement sequence exists.  While the gate is closed this
  -- ordering can never separate two settlements - the gate has already refused
  -- every root carrying more than one - so it decides nothing today.  It becomes
  -- load-bearing the moment the gate is opened, which is why the gate may only
  -- be opened together with re-pointing THIS `order by` and the gate's tie test
  -- at the installed sequence column.
  select element.value
    into v_position
  from pg_catalog.jsonb_array_elements(v_settlements) as element(value)
  order by (element.value->>'settled_at_utc')::timestamptz desc,
           element.value->>'pay_batch_id',
           element.value->>'timesheet_id'
  limit 1;

  -- WP-11e G8.  The unit of restatement, from the one function that names it.
  --   'ROOT_AND_SHIFT' - ruling B1a's own words: the latest settlement that
  --                      restates THAT shift states that shift's position;
  --   'ROOT'           - the latest settlement's whole snapshot is the position,
  --                      so a shift absent from it is no longer paid.
  -- `dense_rank` partitions by the shift identity and orders by the SETTLEMENT,
  -- so every row of the winning settlement for a shift ties at rank 1 and a
  -- split segment is still summed WITHIN that one restatement.
  select coalesce(pg_catalog.jsonb_agg(shift_row order by
           shift_row->>'date',
           coalesce(shift_row->>'start_utc','') collate "C",
           coalesce(shift_row->>'segment_id','') collate "C"),'[]'::jsonb),
         coalesce(pg_catalog.sum((shift_row->>'hours')::numeric),0),
         coalesce(pg_catalog.sum((shift_row->>'hours_day')::numeric),0),
         coalesce(pg_catalog.sum((shift_row->>'hours_night')::numeric),0),
         coalesce(pg_catalog.sum((shift_row->>'hours_sat')::numeric),0),
         coalesce(pg_catalog.sum((shift_row->>'hours_sun')::numeric),0),
         coalesce(pg_catalog.sum((shift_row->>'hours_bh')::numeric),0),
         pg_catalog.count(distinct shift_row#>>'{settlements,0,pay_batch_id}')
           ::integer
    into v_shifts,v_total,v_day,v_night,v_sat,v_sun,v_bh,v_position_settlements
  from (
    select pg_catalog.jsonb_build_object(
             'date',ranked.k_date,
             'segment_id',ranked.k_segment,
             'start_utc',ranked.k_start,
             'end_utc',ranked.k_end,
             'break_mins',pg_catalog.max((ranked.shift->>'break_mins')::numeric),
             'hours',pg_catalog.sum((ranked.shift->>'hours')::numeric),
             'hours_day',pg_catalog.sum((ranked.shift->>'hours_day')::numeric),
             'hours_night',pg_catalog.sum((ranked.shift->>'hours_night')::numeric),
             'hours_sat',pg_catalog.sum((ranked.shift->>'hours_sat')::numeric),
             'hours_sun',pg_catalog.sum((ranked.shift->>'hours_sun')::numeric),
             'hours_bh',pg_catalog.sum((ranked.shift->>'hours_bh')::numeric),
             -- The single settlement that restated THIS shift's position.
             'settlements',pg_catalog.jsonb_build_array(
               pg_catalog.jsonb_build_object(
                 'timesheet_id',pg_catalog.max(ranked.settlement->>'timesheet_id'),
                 'pay_batch_id',pg_catalog.max(ranked.settlement->>'pay_batch_id'),
                 'settled_at_utc',
                   pg_catalog.max(ranked.settlement->>'settled_at_utc')))
           ) as shift_row
    from (
      select exploded.k_date,exploded.k_segment,exploded.k_start,exploded.k_end,
             exploded.settlement,exploded.shift,
             pg_catalog.dense_rank() over (
               partition by exploded.k_date,exploded.k_segment,
                            exploded.k_start,exploded.k_end
               order by (exploded.settlement->>'settled_at_utc')::timestamptz desc,
                        exploded.settlement->>'pay_batch_id',
                        exploded.settlement->>'timesheet_id') as shift_rank
      from (
        select element.value as settlement,
               shift.value as shift,
               shift.value->>'date' as k_date,
               shift.value->>'segment_id' as k_segment,
               shift.value->>'start_utc' as k_start,
               shift.value->>'end_utc' as k_end
        from pg_catalog.jsonb_array_elements(v_settlements) as element(value)
        cross join lateral pg_catalog.jsonb_array_elements(
          element.value#>'{derived,shifts}') as shift(value)
        where v_unit='ROOT_AND_SHIFT'
           or (element.value->>'pay_batch_id'
                 is not distinct from v_position->>'pay_batch_id'
               and element.value->>'timesheet_id'
                 is not distinct from v_position->>'timesheet_id')
      ) as exploded
    ) as ranked
    where ranked.shift_rank=1
    group by ranked.k_date,ranked.k_segment,ranked.k_start,ranked.k_end
  ) as aggregated;

  -- WP-11e G2.  A whole restated position can never be negative, in the total,
  -- in any bucket or in any single shift.  Negative bucket values are the
  -- fingerprint of the delta model this reader rejects, so a negative figure is
  -- contradictory evidence and is refused rather than shown as hours paid.
  select pg_catalog.count(*)::integer
    into v_negative
  from pg_catalog.jsonb_array_elements(v_shifts) as shift_element(value)
  where (shift_element.value->>'hours')::numeric<0
     or (shift_element.value->>'hours_day')::numeric<0
     or (shift_element.value->>'hours_night')::numeric<0
     or (shift_element.value->>'hours_sat')::numeric<0
     or (shift_element.value->>'hours_sun')::numeric<0
     or (shift_element.value->>'hours_bh')::numeric<0;

  if v_total<0 or v_day<0 or v_night<0 or v_sat<0 or v_sun<0 or v_bh<0
     or v_negative>0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','SNAPSHOT_POSITION_NEGATIVE',
      'root_timesheet_id',p_root_timesheet_id,
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
      'settlement_count',pg_catalog.jsonb_array_length(v_settlements),
      'batch_count',(
        select pg_catalog.count(distinct element.value->>'pay_batch_id')::integer
        from pg_catalog.jsonb_array_elements(v_settlements) as element(value)),
      'evidence_counts_basis','FAMILY_EVIDENCE',
      'negative_shift_count',v_negative,
      'settlements',v_settlements)
      ||private.weekly_source_settlement_reason_class_v1(
          'SNAPSHOT_POSITION_NEGATIVE');
  end if;

  select pg_catalog.min((element.value->>'settled_at_utc')::timestamptz),
         pg_catalog.max((element.value->>'settled_at_utc')::timestamptz),
         pg_catalog.count(distinct element.value->>'pay_batch_id')::integer
    into v_first,v_last,v_batch_count
  from pg_catalog.jsonb_array_elements(v_settlements) as element(value);

  return pg_catalog.jsonb_build_object(
    'ok',true,'state','AVAILABLE','reason',null,
    'unavailable_class',null,'reason_detail',null,
    'root_timesheet_id',p_root_timesheet_id,
    'family_booking_id',v_family,'canonical_timesheet_id',v_canonical,
    'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
    'batch_count',v_batch_count,
    'settlement_count',pg_catalog.jsonb_array_length(v_settlements),
    'evidence_counts_basis','FAMILY_EVIDENCE',
    'first_settled_at_utc',v_first,'last_settled_at_utc',v_last,
    -- Which settlement the stated position was restated by, so a consumer can
    -- show the figure's own provenance (WP-11d F1).  `position_basis` keeps its
    -- published value; WP-11e G8 adds `position_unit`, which names the unit the
    -- restatement was taken per, and `position_settlement_count`, which is how
    -- many distinct settlements the stated shifts were drawn from (always 1
    -- while the G7 gate is closed).  Each shift also names its own restating
    -- settlement in its `settlements` array.
    'position_basis','LATEST_RESTATEMENT',
    'position_unit',v_unit,
    'position_settlement_count',coalesce(v_position_settlements,0),
    'position_timesheet_id',v_position->'timesheet_id',
    'position_pay_batch_id',v_position->'pay_batch_id',
    'position_settled_at_utc',v_position->'settled_at_utc',
    'total_hours',v_total,
    'hours_by_bucket',pg_catalog.jsonb_build_object(
      'day',v_day,'night',v_night,'sat',v_sat,'sun',v_sun,'bh',v_bh),
    'shifts',v_shifts,
    'settlements',v_settlements);
end;
$function$;

alter function private.weekly_source_settlement_bounds_v1() owner to postgres;
alter function private.weekly_source_settlement_snapshot_shifts_v1(jsonb) owner to postgres;
alter function private.weekly_source_settlement_reason_class_v1(text) owner to postgres;
alter function private.weekly_source_settlement_position_gate_v1(jsonb) owner to postgres;
alter function private.weekly_source_settlement_position_unit_v1() owner to postgres;
alter function private.weekly_source_settlement_allocation_v1(uuid) owner to postgres;

revoke all on function private.weekly_source_settlement_bounds_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_settlement_snapshot_shifts_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_settlement_reason_class_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_settlement_position_gate_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_settlement_position_unit_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_settlement_allocation_v1(uuid)
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_settlement_reason_class_v1(text) is
  'Gate 9 G9-2. The WP-11b interface: classifies why a paid figure is absent as POSITION_WITHHELD (the evidence is sound but the position cannot be stated, so the phase still resolves and the heading stands) or EVIDENCE_DAMAGED (contradictory or unreadable evidence, which is a fail-closed contradiction), and supplies a plain-English reason_detail an Office user can read.';

comment on function private.weekly_source_settlement_position_gate_v1(jsonb) is
  'Gate 9 G9-2, WP-11d F1 and WP-11e G7. The single settlement-position gate. Returns null when a position can be stated, SETTLEMENT_ORDER_AMBIGUOUS when two settlements cannot be separated at all, and SETTLEMENT_SEQUENCE_UNPROVABLE for every root carrying more than one settlement. Ruling B1a (HANDOVER 2 response R5) requires the latest authoritative settlement to be selected by the installed settlement sequence/revision contract and NOT by a timestamp, and says that where the ordering cannot be proved no figure is shown with the explicit reason. No installed evidence relation carries such a sequence: timesheet_pay_state_history has six columns and a random-uuid id, and settled_at_utc is the settle rail''s transaction start time stamped once per batch. So the withholding IS the ruled behaviour, not a placeholder. Opening it is a one-line substitution of `return null;` for the final return TOGETHER WITH re-pointing this tie test and the reader''s position pick at an installed sequence column; the two-line deletion WP-11d prescribed leaves a function with no RETURN and raises 2F005 on every multi-settlement root.';

comment on function private.weekly_source_settlement_position_unit_v1() is
  'Gate 9 G9-2, WP-11e G8. Names the unit the settled position is restated per, and is the ONE place that choice lives. ROOT_AND_SHIFT (shipped) reads ruling B1a''s words "the complete paid position for that root and shift" literally: each shift''s position is the latest settlement that restated THAT shift. ROOT reads the latest settlement''s whole snapshot as the position, so a shift absent from it is no longer paid, which is what the installed writer produces. The two disagree on a real figure (8+4 then 9 reads 13 one way and 9 the other) and the approver must confirm which is intended; the choice is unobservable while the settlement-position gate withholds every multi-settlement root.';

comment on function private.weekly_source_settlement_allocation_v1(uuid) is
  'Gate 9 G9-2. Read-only, family-aware, bounded settlement-allocation reader. Supplies Hours paid, Hours paid to date and current paid hours from the signed timesheet_pay_state_history rows and their pay_batch_timesheet_snapshots across every physical Timesheet version (proof/32 section 5.2, proof/34 section 8). The paid position is the LATEST settlement''s RESTATED position, never a sum across settlements, because the installed Banking Pay writer restates the whole position in every snapshot and moves only the money residual (WP-11d F1); hours are derived from the signed snapshot and the signature is re-checked against that content by the installed writer''s own md5 scheme (WP-11d F2). WP-11e: the restatement unit is named by weekly_source_settlement_position_unit_v1 (G8); a negative stated position is refused as SNAPSHOT_POSITION_NEGATIVE (G2); a snapshot settled under a Candidate who is not the Timesheet''s contract Candidate is a SETTLEMENT_SNAPSHOT_CONFLICT (G6); settlement_count and batch_count are JSON numbers on every result with evidence_counts_basis naming what they were counted from (G4); and every root settled more than once is withheld as SETTLEMENT_SEQUENCE_UNPROVABLE because ruling B1a requires selection by an installed settlement sequence contract that does not exist (G7). Never a currency-to-hours calculation, never the timesheet_pay_state last-settled cache, no lock on any Banking Pay table, no write anywhere. Malformed, contradictory or unprovably-ordered evidence returns state UNAVAILABLE with a reason and no figure; NO_SETTLEMENT carries no numeric member at all.';

notify pgrst, 'reload schema';

commit;
