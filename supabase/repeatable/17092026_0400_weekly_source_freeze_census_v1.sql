-- Repeatable CloudTMS authority: weekly_source_freeze_census_v1
--
-- Read-only Banking Pay freeze census for Weekly Source entitlement
-- publication (interface I-2; pack authority `P:\proof\32_PENDING_PUBLICATION_
-- OWNER_SPECIFICATION_20260917.md` sections 4.0 to 4.3, 5.1 to 5.4 and the
-- HANDOVER 2 round-3 binding amendments quoted verbatim in section 14).
--
-- The census reads existing Banking Pay evidence with plain SELECT only.  It
-- takes no row lock on any Banking Pay table, writes nothing anywhere, creates
-- no Draft, payment, recovery, provider, settlement, reservation or Case row,
-- and edits, wraps or re-creates no Banking Pay definition.  Transfers are
-- classified only by the installed adapter
-- `public._pay_rail_state_money_movement_classify`, called with exactly the
-- arguments the installed cancellation owner uses
-- (`public.pay_no_money_unwind_apply_work_item`, classifier call site).
--
-- The Umbrella rule reads only the frozen `public.pay_batch_items.umbrella_id`
-- and `public.pay_batch_items.pay_channel`.  The Candidate's current Umbrella is
-- never consulted (proof/32 section 14.1, condition 3.1).
--
-- A batch that is `FAILED` with `completed_at_utc` is a terminal container and
-- settles nobody; settlement is decided per Candidate (proof/32 section 14.6).
--
-- ---------------------------------------------------------------------------
-- HANDOVER 2 round-4 rulings (17 September 2026), preserved byte for byte as
-- `plan6-pack-audit-20260916\HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R4.md`.
-- That response outranks `proof/32` revision 4 wherever it amends it, until the
-- sealed pack is corrected at Gate 13.  The two readings that were open when
-- this file was first written are now ruled, and the rules below are no longer
-- interim: there is no "literal variant" to restore.
--
--   Ruling 1 (OR-10), Binding C - "positively corroborated PAYE-net
--   reprojection void".  An enumerated voided item is proved under Binding C
--   only when all common terminal conditions hold and at least one durable
--   `pay_advance_reservations` row linked by `pay_batch_item_id = i.id` has
--   `status = 'RELEASED'` and `released_reason = 'PAYE_NET_REPROJECTION'`;
--   every other reservation linked to the item is terminal under the accepted
--   rule; the item has no money-moved or ambiguous transfer and no live C4
--   operation.  Batch terminality alone, the absence of a reservation, or the
--   absence of contradictory evidence is not proof of why the item was voided.
--   Absent the artefact the item is
--   `CENSUS_ERROR / WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE`, never
--   `VOIDED_TERMINAL/C`.  Implemented at "RULING 1".
--
--   Ruling 2 (OR-9), born-voided dormant recovery template.  The structural
--   class `NON_ENUMERATED_BORN_VOIDED_TEMPLATE` is registered by the installed
--   writer census (WP-18), not here.  Its runtime half is this census's rule
--   that a born-voided row which ever becomes enumerable is `CENSUS_ERROR`,
--   not Binding C - which follows from ruling 1 and is proved by the verifier.
--
--   Ruling 3 (OR-8), mid-flight.  A voided item in a terminal batch is not yet
--   an unbindable-terminal error while evidence guaranteed to resolve through
--   an already live Banking Pay lifecycle remains in flight.  The permitted
--   in-flight cases are limited to exactly two: (a) a live C4 correction
--   operation for the batch, or (b) the item's bound transfer classifying
--   `PENDING_NON_FINAL`.  Such an item is
--   `ACTIVE / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED` and keeps the root
--   FROZEN without consuming the technical-failure budget.  An outstanding
--   `RESERVED` or `COMMITTED` reservation in an already terminal batch, and an
--   `UNKNOWN` or contradictory transfer, are NOT self-healing and remain
--   `CENSUS_ERROR`.  This changes escalation timing only; it cannot make an
--   item or root releasable.  Implemented at "RULING 3".
--
--   Ruling 4 (OR-11), a cancellation-owned `VOIDED` transfer.  Owned by the
--   Banking Pay money-movement classifier, NOT by a Weekly Source exception.
--   `VOIDED` alone stays ambiguous.  This census adds no private interpretation
--   of it and keeps classifying only through the installed
--   `public._pay_rail_state_money_movement_classify`, so an ordinary
--   whole-transfer pre-bank cancellation that had a bound transfer currently
--   ends in `CENSUS_ERROR` and Office review.  No code change here; the
--   acceptance scenario is in the verifier.
--
--   Ruling 5 (OR-3), writer inventory, item 5.  A reservation released with
--   `released_reason = 'WRITE_OFF'` is not proof of cancellation, settlement,
--   reprojection or safe source release and must not satisfy the positive
--   reservation evidence a binding requires.  Applied at "RULING 5" to every
--   place reservations are tested as positive evidence.
--
--   HANDOVER 2 round-7 ruling A4 RATIFIES the wider reading applied here - the
--   §5.1 common condition AND the §5.2 settlement condition, not only each
--   binding's own evidence - and adds that it must hold in EVERY place a
--   write-off could satisfy one of the three.  The reservation-terminality test
--   is therefore normalised with `upper(btrim(...))`, closing a spelling gap
--   that let a write-off stored as `' write_off '` or `'Write_Off'` release a
--   source position.  Predicate C2 stays untouched: ruling A4 also says a
--   write-off is "neither positive release evidence nor a freeze by itself".
--   See "HANDOVER 2 round-7 ruling A4" at `item_reservation_state`.
--
--   Extra dispositions.  Several correction requests naming one item take the
--   worst applicable disposition (see "G1"); the family-split read is bounded
--   with a fixed per-call limit and a positive completion proof (see
--   "BOUNDED READ"); the settlement snapshot selection's `candidate_id`
--   predicate is accepted because it exactly reproduces the installed
--   settlement owner's selection and fails closed on mismatch (see
--   "SNAPSHOT SELECTION"); `§4.3` C2 stays literal.
--
-- One drafting note that the rulings do not change: `§5.1`'s "exactly one of
-- the three bindings" is implemented as the first binding that proves the item,
-- evaluated A then B then C, because each binding's heading scopes it to a
-- distinct writer family and an ordinary full cancellation satisfies A and B
-- together.

\set ON_ERROR_STOP on

begin;

-- Extracts the exact UUID item identities carried by an installed Banking Pay
-- selection/blocker JSON array.  Values that are not exact UUIDs carry no item
-- identity and are deliberately dropped, so the caller's "no exact item
-- identity" rule (proof/32 section 5.1, Binding A) fails closed.
create or replace function private.weekly_source_freeze_census_uuid_array_v1(
  p_value jsonb
) returns uuid[]
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(
    pg_catalog.array_agg(distinct candidate_values.value_text::uuid),
    array[]::uuid[]
  )
  from pg_catalog.jsonb_array_elements_text(
    case
      when pg_catalog.jsonb_typeof(p_value)='array' then p_value
      else '[]'::jsonb
    end
  ) as candidate_values(value_text)
  where candidate_values.value_text
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
$function$;

-- Interface I-2.  Returns
--   {result, reason, class_counts, items, proof, families, predicates, errors,
--    member_timesheet_ids, expanded_member_timesheet_ids, evaluated_at_utc}
-- where `result` is one of RELEASABLE, FROZEN, CENSUS_ERROR.
create or replace function private.weekly_source_freeze_census_v1(
  p_candidate_id uuid,
  p_member_timesheet_ids uuid[]
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_requested uuid[];
  -- BOUNDED READ: the fixed per-call sibling-probe bound.  See the header and
  -- the WP-08a report for the evidence behind the value.
  v_split_scan_bound constant integer:=256;
  v_result jsonb;
begin
  if p_candidate_id is null then
    return pg_catalog.jsonb_build_object(
      'result','CENSUS_ERROR',
      'reason','WEEKLY_SOURCE_CENSUS_CANDIDATE_REQUIRED',
      'class_counts',pg_catalog.jsonb_build_object(
        'CENSUS_ERROR',0,'VOIDED_TERMINAL',0,'SETTLED_TERMINAL',0,'ACTIVE',0),
      'items','[]'::jsonb,'proof','[]'::jsonb,'families','[]'::jsonb,
      'predicates','[]'::jsonb,
      'errors',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'scope','REQUEST','code','WEEKLY_SOURCE_CENSUS_CANDIDATE_REQUIRED')),
      'member_timesheet_ids','[]'::jsonb,
      'expanded_member_timesheet_ids','[]'::jsonb,
      'evaluated_at_utc',v_now
    );
  end if;

  select coalesce(pg_catalog.array_agg(distinct requested_values.timesheet_id),array[]::uuid[])
  into v_requested
  from pg_catalog.unnest(coalesce(p_member_timesheet_ids,array[]::uuid[]))
    as requested_values(timesheet_id)
  where requested_values.timesheet_id is not null;

  if pg_catalog.cardinality(v_requested)=0 then
    return pg_catalog.jsonb_build_object(
      'result','CENSUS_ERROR',
      'reason','WEEKLY_SOURCE_CENSUS_NO_MEMBERS',
      'class_counts',pg_catalog.jsonb_build_object(
        'CENSUS_ERROR',0,'VOIDED_TERMINAL',0,'SETTLED_TERMINAL',0,'ACTIVE',0),
      'items','[]'::jsonb,'proof','[]'::jsonb,'families','[]'::jsonb,
      'predicates','[]'::jsonb,
      'errors',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'scope','REQUEST','code','WEEKLY_SOURCE_CENSUS_NO_MEMBERS')),
      'member_timesheet_ids','[]'::jsonb,
      'expanded_member_timesheet_ids','[]'::jsonb,
      'evaluated_at_utc',v_now
    );
  end if;

  with
  -- Section 4.0 step 0.  The complete Timesheet family of every supplied member
  -- is resolved through the installed Workbench rotation resolver; the stored
  -- id is never trusted alone and the family key is `timesheets.booking_id`.
  rotation_scope as (
    select
      scope_rows.requested_timesheet_id,
      scope_rows.booking_id,
      scope_rows.canonical_timesheet_id,
      scope_rows.family_timesheet_id,
      scope_rows.family_is_current,
      scope_rows.family_version
    from public._pay_timesheet_rotation_scope(v_requested) as scope_rows
  ),
  unresolved_member as (
    select distinct rotation_scope.requested_timesheet_id
    from rotation_scope
    where rotation_scope.booking_id is null
  ),
  -- Review finding F1.  `public._pay_timesheet_rotation_scope` returns its
  -- defensive NULL-booking row only when the Timesheet id does not exist.  A
  -- Timesheet that DOES exist but whose `btrim(booking_id)` is empty is excluded
  -- by the resolver's own `BTRIM(...) <> ''` filter and produces no row at all,
  -- so the member, its family and all of its Banking Pay evidence would vanish
  -- and the bundle could report RELEASABLE with a live Draft open.  Every id in
  -- the input must therefore be accounted for in the resolver output; anything
  -- the resolver drops, for any present or future reason, fails closed here.
  unaccounted_member as (
    select requested_values.timesheet_id
    from pg_catalog.unnest(v_requested) as requested_values(timesheet_id)
    where not exists (
      select 1 from rotation_scope
      where rotation_scope.requested_timesheet_id=requested_values.timesheet_id
    )
  ),
  blank_booking_member as (
    select member_row.timesheet_id
    from pg_catalog.unnest(v_requested) as requested_values(timesheet_id)
    join public.timesheets as member_row
      on member_row.timesheet_id=requested_values.timesheet_id
    where coalesce(pg_catalog.btrim(member_row.booking_id),'')=''
  ),
  family_member as (
    select distinct
      rotation_scope.booking_id,
      rotation_scope.family_timesheet_id,
      rotation_scope.family_is_current,
      rotation_scope.family_version
    from rotation_scope
    where rotation_scope.booking_id is not null
  ),
  family_stat as (
    select
      family_member.booking_id,
      pg_catalog.count(*)::integer as member_count,
      pg_catalog.count(*) filter (where family_member.family_is_current)::integer
        as current_count
    from family_member
    group by family_member.booking_id
  ),
  -- Section 4.0 integrity gate as it applies to the family itself: zero or more
  -- than one current row is an ambiguous family (R24) and fails closed.
  family_error as (
    select
      family_stat.booking_id,
      'WEEKLY_SOURCE_CENSUS_FAMILY_AMBIGUOUS'::text as code,
      family_stat.current_count
    from family_stat
    where family_stat.current_count<>1
  ),
  -- Review finding F3.  The installed resolver matches `booking_id` exactly, so a
  -- sibling row whose booking identity differs only by surrounding whitespace is
  -- a different family to the resolver, to this census and to the I-1 lock
  -- helper, and its Banking Pay evidence would be invisible.  The rotation lock
  -- order of `proof/32 §6` step 2 exists because padded booking identities are
  -- real.  A split family is ambiguous and fails closed here; I-1 enforces the
  -- same rule independently and this census does not rely on it.
  -- Review finding G2.  The sibling search must stay bounded: `proof/32 §14.7`
  -- forbids adding an unbounded scan, no installed index covers
  -- `btrim(booking_id)`, and this census adds no index to a table it does not
  -- own.  A rotation family is created within one Contract and one week, so the
  -- search is restricted to that Contract-week and is served by the installed
  -- `idx_timesheets_contract_weekending (contract_id, week_ending_date)`.  A
  -- member whose `contract_id` is null - the column is nullable - falls to the
  -- second branch, served by `idx_timesheets_weekending (week_ending_date)`.
  -- Both are index-backed and bounded; neither scans `public.timesheets`.
  family_member_row as (
    select
      family_member.booking_id,
      family_member.family_timesheet_id,
      member_row.contract_id,
      member_row.week_ending_date
    from family_member
    join public.timesheets as member_row
      on member_row.timesheet_id=family_member.family_timesheet_id
  ),
  -- BOUNDED READ.  HANDOVER 2 round 4 ruled the earlier whole-table form a
  -- release blocker and required "a bounded indexed/keyset path with fixed
  -- per-call work and byte limits, continuation identity, positive completion
  -- proof and no business-population ceiling".
  --
  -- Fixed per-call bound: at most `v_split_scan_bound + 1` index rows are read
  -- per family member, through `idx_timesheets_contract_weekending` (or
  -- `idx_timesheets_weekending` when the member carries no Contract).  The
  -- continuation identity is `(contract_id, week_ending_date, booking_id,
  -- timesheet_id)`, the index's own order.  Reading the bound plus one is what
  -- makes completion positive rather than assumed: fewer than or exactly the
  -- bound means the Contract-week was read to its end and the answer is
  -- complete; the extra row means it was not, and that is
  -- `WEEKLY_SOURCE_CENSUS_FAMILY_SPLIT_SCAN_BOUND_EXCEEDED`, never a silent
  -- pass.  Only `booking_id` and `timesheet_id` are projected, so the byte
  -- limit is the bound times two small columns.
  --
  -- The bound is a property of a Contract-week, not of the business population,
  -- so raising the number of Timesheets, Candidates, Contracts or weeks cannot
  -- approach it.  See the WP-08a report for the measured evidence behind the
  -- chosen value.
  family_split_probe as (
    select
      family_member_row.booking_id,
      family_member_row.family_timesheet_id,
      sibling.sibling_booking_id,
      sibling.probe_ordinal
    from family_member_row
    cross join lateral (
      select
        probe_row.booking_id as sibling_booking_id,
        pg_catalog.row_number() over (
          order by probe_row.booking_id, probe_row.timesheet_id
        ) as probe_ordinal
      from public.timesheets as probe_row
      where (
              (family_member_row.contract_id is not null
               and probe_row.contract_id=family_member_row.contract_id
               and probe_row.week_ending_date=family_member_row.week_ending_date)
              or
              (family_member_row.contract_id is null
               and probe_row.week_ending_date=family_member_row.week_ending_date)
            )
      order by probe_row.booking_id, probe_row.timesheet_id
      limit v_split_scan_bound+1
    ) as sibling
  ),
  family_split_error as (
    select distinct
      family_split_probe.booking_id,
      'WEEKLY_SOURCE_CENSUS_FAMILY_BOOKING_SPLIT'::text as code,
      family_split_probe.sibling_booking_id
    from family_split_probe
    where pg_catalog.btrim(family_split_probe.sibling_booking_id)
          =pg_catalog.btrim(family_split_probe.booking_id)
      and family_split_probe.sibling_booking_id
          is distinct from family_split_probe.booking_id
  ),
  -- Positive completion proof, per family member.
  family_split_completion as (
    select
      family_member_row.booking_id,
      family_member_row.family_timesheet_id,
      coalesce(pg_catalog.max(family_split_probe.probe_ordinal),0)::integer
        as rows_examined,
      coalesce(pg_catalog.max(family_split_probe.probe_ordinal),0)
        <=v_split_scan_bound as completed
    from family_member_row
    left join family_split_probe
      on family_split_probe.family_timesheet_id
         =family_member_row.family_timesheet_id
    group by family_member_row.booking_id, family_member_row.family_timesheet_id
  ),
  family_split_bound_error as (
    select
      family_split_completion.booking_id,
      family_split_completion.family_timesheet_id,
      family_split_completion.rows_examined
    from family_split_completion
    where not family_split_completion.completed
  ),
  -- Review finding F1 / F6.  A member Timesheet whose Contract belongs to a
  -- DIFFERENT Candidate is not this Candidate's evidence.  Without this test a
  -- family that happens to carry no `pay_batch_items` row accepts any Candidate
  -- id, because the foreign-join test of §4.2 class 1 has nothing to compare.
  --
  -- Review finding G3.  Two installed sources can bind a Timesheet to a
  -- Candidate, and BOTH are consulted: the Contract chain
  -- (`timesheets.contract_id` -> `contracts.candidate_id`) and the Timesheet's
  -- own current financial row (`timesheets_financials.candidate_id`, reached
  -- through the installed `idx_tsfin_current_timesheet`, which the
  -- `trg_tsfin_ai` trigger creates for every Timesheet).  A non-null
  -- disagreement from EITHER source is a mismatch and fails closed.
  --
  -- Only a POSITIVE mismatch is an error.  `public.timesheets.contract_id`,
  -- `public.contracts.candidate_id` and `public.timesheets_financials.candidate_id`
  -- are all nullable in the installed schema, so an unresolvable ownership chain
  -- is an ordinary state, not evidence of anything.  An earlier build treated it
  -- as technical and turned all 29 WP-16a fixture states into CENSUS_ERROR - the
  -- "never paid" direction, and the normal shape of a root that has no Banking
  -- Pay evidence yet.  The unresolvable case is therefore reported as a
  -- diagnostic, and the authoritative Candidate binding for Banking Pay evidence
  -- remains the §4.2 class 1 foreign-join test on
  -- `pay_batch_candidates.candidate_id`, applied to every enumerated item.
  member_owner as (
    select
      family_member.booking_id,
      family_member.family_timesheet_id,
      member_row.contract_id,
      owner_contract.candidate_id as contract_candidate_id,
      financial_row.candidate_id as financial_candidate_id,
      coalesce(owner_contract.candidate_id,financial_row.candidate_id)
        as candidate_id
    from family_member
    join public.timesheets as member_row
      on member_row.timesheet_id=family_member.family_timesheet_id
    left join public.contracts as owner_contract
      on owner_contract.id=member_row.contract_id
    left join lateral (
      select current_financial.candidate_id
      from public.timesheets_financials as current_financial
      where current_financial.timesheet_id=family_member.family_timesheet_id
        and current_financial.is_current
        and current_financial.candidate_id is not null
      limit 1
    ) as financial_row on true
  ),
  member_owner_error as (
    select
      member_owner.booking_id,
      member_owner.family_timesheet_id,
      'WEEKLY_SOURCE_CENSUS_MEMBER_CANDIDATE_MISMATCH'::text as code
    from member_owner
    where (member_owner.contract_candidate_id is not null
           and member_owner.contract_candidate_id<>p_candidate_id)
       or (member_owner.financial_candidate_id is not null
           and member_owner.financial_candidate_id<>p_candidate_id)
  ),
  member_owner_unknown as (
    select
      member_owner.booking_id,
      member_owner.family_timesheet_id
    from member_owner
    where member_owner.candidate_id is null
  ),

  -- Section 4.2.  Every `pay_batch_items` row of every family member, joined to
  -- its candidate row and that row's batch.
  census_item as (
    select
      family_member.booking_id,
      family_member.family_timesheet_id as timesheet_id,
      batch_item.id as pay_batch_item_id,
      batch_item.pay_batch_candidate_id,
      batch_item.is_voided,
      batch_item.pay_bank_transfer_id,
      batch_item.reservation_id,
      batch_item.umbrella_id,
      batch_item.pay_channel,
      batch_item.item_type,
      batch_candidate.id as candidate_row_id,
      batch_candidate.candidate_id as candidate_row_candidate_id,
      batch_candidate.settlement_status,
      batch_candidate.settled_at_utc,
      batch.id as pay_batch_id,
      batch.status as batch_status,
      batch.cancelled_at_utc,
      batch.completed_at_utc,
      batch.execution_commit_state,
      batch.execution_committed_at_utc,
      batch.execution_commit_ref
    from family_member
    join public.pay_batch_items as batch_item
      on batch_item.timesheet_id=family_member.family_timesheet_id
    left join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id=batch_item.pay_batch_candidate_id
    left join public.pay_batches as batch
      on batch.id=batch_candidate.pay_batch_id
  ),
  item_flag as (
    select
      census_item.*,
      (
        census_item.candidate_row_id is null
        or census_item.pay_batch_id is null
        or census_item.candidate_row_candidate_id is distinct from p_candidate_id
        or (
          census_item.pay_bank_transfer_id is not null
          and not exists (
            select 1 from public.pay_bank_transfers as bound_transfer
            where bound_transfer.id=census_item.pay_bank_transfer_id
          )
        )
      ) as join_error,
      -- Section 4.1 terminal container.
      (
        census_item.batch_status in ('SETTLED','CANCELLED')
        or (census_item.batch_status='FAILED'
            and census_item.completed_at_utc is not null)
      ) as batch_terminal,
      -- Section 4.2 class 1: the terminal-batch qualifier used for the
      -- unbindable-void rule.
      (
        (census_item.batch_status='CANCELLED'
         and census_item.cancelled_at_utc is not null)
        or (census_item.batch_status='SETTLED'
            and census_item.execution_commit_state='COMMITTED')
        or (census_item.batch_status='FAILED'
            and census_item.completed_at_utc is not null
            and census_item.execution_commit_state='COMMITTED')
      ) as void_terminal_container
    from census_item
  ),
  family_batch as (
    select distinct item_flag.booking_id, item_flag.pay_batch_id
    from item_flag
    where item_flag.pay_batch_id is not null
  ),
  family_batch_state as (
    select
      item_flag.booking_id,
      item_flag.pay_batch_id,
      pg_catalog.bool_or(not item_flag.is_voided) as has_non_voided,
      pg_catalog.bool_or(
        not item_flag.is_voided
        and item_flag.candidate_row_candidate_id=p_candidate_id
      ) as has_non_voided_bundle_candidate
    from item_flag
    where item_flag.pay_batch_id is not null
    group by item_flag.booking_id, item_flag.pay_batch_id
  ),

  -- Predicate C4 (live operation) and the Binding B / section 5.2 requirement
  -- that every operation of the batch is terminal with both lease forms null or
  -- expired.  Both lease forms are kept by the schema.
  batch_operation as (
    select
      family_batch.booking_id,
      family_batch.pay_batch_id,
      operation.id as operation_id,
      operation.status as operation_status,
      operation.lease_expires_at_utc,
      operation.lock_expires_at_utc
    from family_batch
    join public.banking_pay_operations as operation
      on operation.pay_batch_id=family_batch.pay_batch_id
      or (
        operation.operation_type='PAYMENT_CORRECTION'
        and operation.input_json ? 'correction_request_id'
        and exists (
          select 1
          from public.pay_payment_correction_requests as batch_request
          where batch_request.pay_batch_id=family_batch.pay_batch_id
            and operation.input_json->>'correction_request_id'
                =batch_request.id::text
        )
      )
  ),
  batch_operation_state as (
    select
      family_batch.booking_id,
      family_batch.pay_batch_id,
      coalesce(pg_catalog.bool_or(
        batch_operation.operation_status in (
          'QUEUED','RUNNING','WAITING','WAITING_AUTHORISATION',
          'WAITING_PROVIDER','REVIEW_REQUIRED')
        or batch_operation.lease_expires_at_utc>v_now
        or batch_operation.lock_expires_at_utc>v_now
      ),false) as has_live_operation,
      coalesce(pg_catalog.bool_and(
        batch_operation.operation_status in ('COMPLETE','FAILED','CANCELLED')
        and (batch_operation.lease_expires_at_utc is null
             or batch_operation.lease_expires_at_utc<=v_now)
        and (batch_operation.lock_expires_at_utc is null
             or batch_operation.lock_expires_at_utc<=v_now)
      ),true) as all_operations_terminal
    from family_batch
    left join batch_operation
      on batch_operation.booking_id=family_batch.booking_id
     and batch_operation.pay_batch_id=family_batch.pay_batch_id
    group by family_batch.booking_id, family_batch.pay_batch_id
  ),

  -- Section 5.1 Binding A core evidence.  The installed cancellation owners are
  -- `pay_payment_correction_process_chunk` and
  -- `pay_payment_cancel_finalise_metadata_v1` together with the three apply
  -- owners (contract section 3.1); all of them record the void through the
  -- correction request, its candidate membership row, an APPLIED work item and
  -- the durable `pay_payment_correction_items` row.
  binding_a_candidate as (
    select
      item_flag.pay_batch_item_id,
      item_flag.booking_id,
      correction_request.id as correction_request_id,
      correction_request.status as correction_request_status,
      work_item.id as work_item_id,
      correction_operation.id as operation_id,
      (correction_request.status='APPLIED_WITH_BLOCKERS') as with_blockers
    from item_flag
    join public.pay_payment_correction_requests as correction_request
      on correction_request.pay_batch_id=item_flag.pay_batch_id
     and correction_request.status in ('APPLIED','APPLIED_WITH_BLOCKERS')
    join public.pay_payment_correction_request_candidates as request_candidate
      on request_candidate.correction_request_id=correction_request.id
     and request_candidate.pay_batch_candidate_id=item_flag.pay_batch_candidate_id
     and item_flag.pay_batch_item_id
         =any(coalesce(request_candidate.pay_batch_item_ids,array[]::uuid[]))
    join public.pay_payment_correction_work_items as work_item
      on work_item.correction_request_id=correction_request.id
     and work_item.pay_batch_id=item_flag.pay_batch_id
     and work_item.pay_batch_candidate_id=item_flag.pay_batch_candidate_id
     and work_item.status='APPLIED'
     and (
       coalesce(work_item.result_json#>'{changed_scope_json,changed_pay_batch_item_ids}',
                '[]'::jsonb) ? item_flag.pay_batch_item_id::text
       or coalesce(work_item.result_json->'changed_pay_batch_item_ids','[]'::jsonb)
          ? item_flag.pay_batch_item_id::text
       or exists (
         select 1
         from public.pay_payment_correction_items as correction_item
         where correction_item.correction_request_id=correction_request.id
           and correction_item.pay_batch_id=item_flag.pay_batch_id
           and correction_item.pay_batch_item_id=item_flag.pay_batch_item_id
           and correction_item.status='APPLIED'
       )
     )
    join public.banking_pay_operations as correction_operation
      on correction_operation.operation_type='PAYMENT_CORRECTION'
     and correction_operation.input_json->>'correction_request_id'
         =correction_request.id::text
     and correction_operation.status='COMPLETE'
     and correction_operation.phase='COMPLETE'
    where item_flag.is_voided
  ),
  binding_a_core as (
    select distinct on (binding_a_candidate.pay_batch_item_id, binding_a_candidate.correction_request_id)
      binding_a_candidate.*
    from binding_a_candidate
    order by binding_a_candidate.pay_batch_item_id,
             binding_a_candidate.correction_request_id,
             binding_a_candidate.work_item_id
  ),
  -- A blocker is every work item of the same request that did not apply.  Its
  -- exact item identity is the work item's own expected selection, otherwise the
  -- request's candidate membership row.  No identity means the blocker cannot be
  -- proved outside the family, which keeps the root FROZEN.
  blocker_work as (
    select
      binding_a_core.pay_batch_item_id,
      binding_a_core.correction_request_id,
      binding_a_core.booking_id,
      blocker_item.id as blocker_work_item_id,
      case
        when private.weekly_source_freeze_census_uuid_array_v1(
               blocker_item.selection_json->'expected_pay_batch_item_ids')
             <>array[]::uuid[]
          then private.weekly_source_freeze_census_uuid_array_v1(
                 blocker_item.selection_json->'expected_pay_batch_item_ids')
        else coalesce(blocker_candidate.pay_batch_item_ids,array[]::uuid[])
      end as blocker_item_ids
    from binding_a_core
    join public.pay_payment_correction_work_items as blocker_item
      on blocker_item.correction_request_id=binding_a_core.correction_request_id
     and coalesce(blocker_item.status,'') not in ('APPLIED','SKIPPED')
    left join public.pay_payment_correction_request_candidates as blocker_candidate
      on blocker_candidate.correction_request_id=binding_a_core.correction_request_id
     and blocker_candidate.pay_batch_candidate_id=blocker_item.pay_batch_candidate_id
    where binding_a_core.with_blockers
  ),
  blocker_eval as (
    select
      blocker_work.pay_batch_item_id,
      blocker_work.correction_request_id,
      pg_catalog.count(*)::integer as blocker_count,
      pg_catalog.bool_or(
        coalesce(pg_catalog.cardinality(blocker_work.blocker_item_ids),0)=0
      ) as any_without_identity,
      pg_catalog.bool_or(exists (
        select 1 from item_flag as family_item
        where family_item.booking_id=blocker_work.booking_id
          and family_item.pay_batch_item_id=any(blocker_work.blocker_item_ids)
      )) as any_blocker_in_family
    from blocker_work
    group by blocker_work.pay_batch_item_id, blocker_work.correction_request_id
  ),
  binding_a_evaluated as (
    select
      binding_a_core.*,
      case
        when not binding_a_core.with_blockers then true
        when coalesce(blocker_eval.blocker_count,0)=0 then false
        when coalesce(blocker_eval.any_without_identity,false) then false
        when coalesce(blocker_eval.any_blocker_in_family,false) then false
        else true
      end as blocker_ok
    from binding_a_core
    left join blocker_eval
      on blocker_eval.pay_batch_item_id=binding_a_core.pay_batch_item_id
     and blocker_eval.correction_request_id=binding_a_core.correction_request_id
  ),
  -- Review findings F7 and G1.  More than one terminal correction request can
  -- name the same item.  Each is evaluated with its own blocker set, and the
  -- safety rule is stated explicitly rather than expressed as a sort order:
  --
  --   an item is disqualified when ANY terminal request touching it carries a
  --   blocker that names a family item or that carries no exact item identity.
  --
  -- `proof/32 §5.1` Binding A says such a blocker "cannot be proved outside the
  -- family", and `R38` requires the root to stay FROZEN.  Producing a second,
  -- cleaner request does not make that blocker go away, so no ordering of the
  -- candidate requests may be allowed to step around it.  An earlier build
  -- ordered by `blocker_ok desc` and therefore preferred the permissive request:
  -- adding one clean request flipped a correctly FROZEN root to RELEASABLE
  -- while the un-identified blocker still stood.
  binding_a_disqualified as (
    select distinct binding_a_evaluated.pay_batch_item_id
    from binding_a_evaluated
    where not binding_a_evaluated.blocker_ok
  ),
  -- The surviving pick is only about which request the receipt tuple names, so
  -- it is ordered by request id alone and carries no safety meaning.
  binding_a as (
    select distinct on (binding_a_evaluated.pay_batch_item_id)
      binding_a_evaluated.pay_batch_item_id,
      binding_a_evaluated.booking_id,
      binding_a_evaluated.correction_request_id,
      binding_a_evaluated.correction_request_status,
      binding_a_evaluated.work_item_id,
      binding_a_evaluated.operation_id,
      binding_a_evaluated.with_blockers,
      not exists (
        select 1 from binding_a_disqualified
        where binding_a_disqualified.pay_batch_item_id
              =binding_a_evaluated.pay_batch_item_id
      ) as blocker_ok
    from binding_a_evaluated
    order by binding_a_evaluated.pay_batch_item_id,
             binding_a_evaluated.correction_request_id
  ),

  -- Section 5.1 common conditions, per voided item, plus the section 5.2
  -- per-item transfer condition.
  --
  -- RULING 5 (HANDOVER 2 round 4, OR-3 item 5).  `public.pay_finance_case_write_off`
  -- is not a Binding A/B/C release authority:
  --
  --   "A reservation with `released_reason = 'WRITE_OFF'` is not proof of
  --   cancellation, settlement, reprojection or safe source release and must not
  --   satisfy the positive reservation evidence required by a binding."
  --
  -- The installed writer releases every RESERVED/COMMITTED reservation of a
  -- finance case across all batches, on a finance-case-wide predicate with no
  -- batch-state guard, while leaving the items active.  It therefore silences
  -- predicate C2 without resolving anything.  The fail-closed reading is taken
  -- everywhere a reservation is read as POSITIVE evidence - the §5.1 common
  -- condition and the §5.2 settlement condition both use the flag below - so a
  -- write-off release never stands in for a settled or properly released
  -- reservation.  Predicate C2 is untouched: it tests RESERVED/COMMITTED and a
  -- written-off row is neither.
  -- =====================================================================
  -- HANDOVER 2 round-7 ruling A4 - THE WIDER, CONSERVATIVE READING, AND THE
  -- SPELLING GAP IT CLOSES
  -- =====================================================================
  -- "APPROVED.  The wider conservative interpretation is required.  `WRITE_OFF`
  --  must not satisfy the common condition, a binding's positive reservation
  --  evidence or §5.2 settlement evidence.  It is neither positive release
  --  evidence nor a freeze by itself."
  --
  -- The wider reading was already applied to both the §5.1 common condition and
  -- the §5.2 settlement condition, and ruling A4 ratifies it.  What ruling A4
  -- also requires is "check every place a write-off could currently satisfy any
  -- of those three and close each" - and one was open.
  --
  -- `pay_advance_reservations.released_reason` is plain `text` with NO check
  -- constraint (the only constraint on the relation is `status IN
  -- ('RESERVED','COMMITTED','SETTLED','RELEASED')`).  The test below compared
  -- the RAW literal, so any write-off whose stored reason differed by case or
  -- by surrounding whitespace passed straight through as a terminal
  -- reservation.  The withdrawal owner in
  -- `17092026_0600_weekly_source_first_authorisation_v1.sql` already normalises
  -- the same fact with `upper(btrim(...))`, so the two authorities disagreed
  -- about what a write-off IS, and the census was the permissive one.
  --
  -- Measured on a build from empty, before this change:
  --
  --   * a Binding A void carrying a reservation released with reason
  --     `' write_off '` classified `VOIDED_TERMINAL / A` and the family came
  --     back `RELEASABLE` - the write-off satisfied the §5.1 common condition;
  --   * a settled item carrying a reservation released with reason
  --     `'Write_Off'` classified `SETTLED_TERMINAL` and the family came back
  --     `RELEASABLE` - the write-off satisfied §5.2 settlement evidence.
  --
  -- Both are release-direction: a write-off released a source position.  The
  -- comparison is therefore normalised here to the same `upper(btrim(...))`
  -- form the withdrawal owner uses.  It is a NARROWING of what counts as
  -- terminal and can only make the census more conservative.
  --
  -- What is deliberately NOT changed, because ruling A4 says a write-off is
  -- "neither positive release evidence NOR A FREEZE BY ITSELF": predicate C2
  -- still tests `RESERVED`/`COMMITTED` only, so a written-off row - which is
  -- `RELEASED` - cannot create a freeze either.  It simply stops being proof.
  item_reservation_state as (
    select
      item_flag.pay_batch_item_id,
      not exists (
        select 1 from public.pay_advance_reservations as reservation
        where (
                reservation.pay_batch_item_id=item_flag.pay_batch_item_id
                or (item_flag.reservation_id is not null
                    and reservation.id=item_flag.reservation_id)
              )
          and not (
            coalesce(reservation.status,'')='SETTLED'
            or (coalesce(reservation.status,'')='RELEASED'
                and pg_catalog.upper(pg_catalog.btrim(
                      coalesce(reservation.released_reason,'')))<>'WRITE_OFF')
          )
      ) as reservations_terminal,
      exists (
        select 1 from public.pay_advance_reservations as written_off
        where (
                written_off.pay_batch_item_id=item_flag.pay_batch_item_id
                or (item_flag.reservation_id is not null
                    and written_off.id=item_flag.reservation_id)
              )
          and coalesce(written_off.status,'')='RELEASED'
          and pg_catalog.upper(pg_catalog.btrim(
                coalesce(written_off.released_reason,'')))='WRITE_OFF'
      ) as has_write_off_release
    from item_flag
  ),
  item_transfer_state as (
    select
      item_flag.pay_batch_item_id,
      item_flag.pay_bank_transfer_id,
      coalesce(bound_state.is_final_money_moved,false) as bound_is_final_money_moved,
      coalesce(bound_state.is_terminal_no_money,false) as bound_is_terminal_no_money,
      bound_state.cash_state as bound_cash_state
    from item_flag
    left join lateral (
      select movement.cash_state, movement.is_final_money_moved,
             movement.is_terminal_no_money
      from public.pay_bank_transfers as bound_transfer
      cross join lateral public._pay_rail_state_money_movement_classify(
        bound_transfer.status,
        bound_transfer.rail_state,
        coalesce(bound_transfer.rail_meta_json,'{}'::jsonb),
        coalesce(bound_transfer.rail_meta_json,'{}'::jsonb)
      ) as movement
      where bound_transfer.id=item_flag.pay_bank_transfer_id
    ) as bound_state on true
  ),

  -- Section 4.3 C5 family transfer scope.
  direct_scope_transfer as (
    select
      item_flag.booking_id,
      item_flag.pay_batch_id,
      item_flag.pay_bank_transfer_id as transfer_id
    from item_flag
    where item_flag.pay_bank_transfer_id is not null
      and item_flag.pay_batch_id is not null
  ),
  scope_transfer as (
    select distinct
      direct_scope_transfer.booking_id,
      direct_scope_transfer.pay_batch_id,
      direct_scope_transfer.transfer_id,
      'DIRECT_ITEM'::text as scope_kind
    from direct_scope_transfer
    union
    select distinct
      family_batch_state.booking_id,
      family_batch_state.pay_batch_id,
      batch_transfer.id,
      'BATCH_CANDIDATE'::text
    from family_batch_state
    join public.pay_bank_transfers as batch_transfer
      on batch_transfer.pay_batch_id=family_batch_state.pay_batch_id
    where family_batch_state.has_non_voided
      and batch_transfer.candidate_id=p_candidate_id
      and not exists (
        select 1 from direct_scope_transfer
        where direct_scope_transfer.booking_id=family_batch_state.booking_id
          and direct_scope_transfer.transfer_id=batch_transfer.id
      )
    union
    -- The six-condition Umbrella rule (proof/32 section 14.1, condition 3.1).
    select distinct
      family_batch_state.booking_id,
      family_batch_state.pay_batch_id,
      batch_transfer.id,
      'BATCH_UMBRELLA'::text
    from family_batch_state
    join public.pay_bank_transfers as batch_transfer
      on batch_transfer.pay_batch_id=family_batch_state.pay_batch_id
    where family_batch_state.has_non_voided_bundle_candidate
      and batch_transfer.candidate_id is null
      and batch_transfer.umbrella_id is not null
      and exists (
        select 1 from item_flag as frozen_item
        where frozen_item.booking_id=family_batch_state.booking_id
          and frozen_item.pay_batch_id=family_batch_state.pay_batch_id
          and not frozen_item.is_voided
          and frozen_item.candidate_row_candidate_id=p_candidate_id
          and frozen_item.umbrella_id=batch_transfer.umbrella_id
          and frozen_item.pay_channel=batch_transfer.pay_channel
      )
      and not exists (
        select 1 from direct_scope_transfer
        where direct_scope_transfer.booking_id=family_batch_state.booking_id
          and direct_scope_transfer.transfer_id=batch_transfer.id
      )
  ),
  umbrella_evidence_error as (
    select
      family_batch_state.booking_id,
      family_batch_state.pay_batch_id,
      batch_transfer.id as transfer_id,
      case
        when exists (
          select 1 from item_flag as frozen_item
          where frozen_item.booking_id=family_batch_state.booking_id
            and frozen_item.pay_batch_id=family_batch_state.pay_batch_id
            and not frozen_item.is_voided
            and frozen_item.candidate_row_candidate_id=p_candidate_id
            and frozen_item.pay_channel='UMBRELLA'
            and frozen_item.umbrella_id is null
        ) then 'WEEKLY_SOURCE_CENSUS_UMBRELLA_EVIDENCE_MISSING'
        when exists (
          select 1 from item_flag as frozen_item
          where frozen_item.booking_id=family_batch_state.booking_id
            and frozen_item.pay_batch_id=family_batch_state.pay_batch_id
            and not frozen_item.is_voided
            and frozen_item.candidate_row_candidate_id=p_candidate_id
            and frozen_item.umbrella_id=batch_transfer.umbrella_id
            and frozen_item.pay_channel is distinct from batch_transfer.pay_channel
        ) then 'WEEKLY_SOURCE_CENSUS_UMBRELLA_EVIDENCE_CONTRADICTORY'
        else null
      end as code
    from family_batch_state
    join public.pay_bank_transfers as batch_transfer
      on batch_transfer.pay_batch_id=family_batch_state.pay_batch_id
    where family_batch_state.has_non_voided_bundle_candidate
      and batch_transfer.candidate_id is null
      and batch_transfer.umbrella_id is not null
  ),
  scope_transfer_class as (
    select
      scope_transfer.booking_id,
      scope_transfer.pay_batch_id,
      scope_transfer.transfer_id,
      scope_transfer.scope_kind,
      movement.cash_state,
      movement.is_final_money_moved,
      movement.is_terminal_no_money,
      movement.is_pending_non_final
    from scope_transfer
    join public.pay_bank_transfers as scoped_transfer
      on scoped_transfer.id=scope_transfer.transfer_id
    cross join lateral public._pay_rail_state_money_movement_classify(
      scoped_transfer.status,
      scoped_transfer.rail_state,
      coalesce(scoped_transfer.rail_meta_json,'{}'::jsonb),
      coalesce(scoped_transfer.rail_meta_json,'{}'::jsonb)
    ) as movement
  ),
  -- Section 5.2: every other in-scope transfer of the batch is final-money-moved
  -- or terminal-no-money bound to a voided family item.
  other_transfer_state as (
    select
      family_batch.booking_id,
      family_batch.pay_batch_id,
      not exists (
        select 1 from scope_transfer_class as other_transfer
        where other_transfer.booking_id=family_batch.booking_id
          and other_transfer.pay_batch_id=family_batch.pay_batch_id
          and not other_transfer.is_final_money_moved
          and not (
            other_transfer.is_terminal_no_money
            and exists (
              select 1 from item_flag as voided_item
              where voided_item.booking_id=family_batch.booking_id
                and voided_item.pay_batch_id=family_batch.pay_batch_id
                and voided_item.is_voided
                and voided_item.pay_bank_transfer_id=other_transfer.transfer_id
            )
          )
      ) as other_transfers_consistent
    from family_batch
  ),

  -- Predicate C2, `proof/32 §4.3` **literally**: a reservation "whose
  -- `pay_batch_item_id` is one of the family's items (or whose
  -- `pay_batch_id`/`pay_batch_candidate_id` is one of the family's
  -- batches/candidate rows)" with `status IN ('RESERVED','COMMITTED')`.
  --
  -- The batch clause is deliberately at full width.  An earlier build narrowed it
  -- to reservations that name neither an item nor a candidate row, so that
  -- another Candidate's live reservation in a shared Draft could not freeze this
  -- root; the independent review recorded that as a release-direction deviation
  -- from an approved money rule (finding F4) and it has been reverted.  The
  -- consequence is deliberate and is the conservative direction: any
  -- `RESERVED`/`COMMITTED` reservation anywhere in a batch that holds a family
  -- item freezes the root until that reservation settles or is released.
  reservation_scope as (
    select distinct
      item_flag.booking_id,
      reservation.id as reservation_id,
      reservation.status as reservation_status,
      reservation.pay_batch_item_id,
      reservation.pay_batch_candidate_id,
      reservation.pay_batch_id
    from item_flag
    join public.pay_advance_reservations as reservation
      on reservation.pay_batch_item_id=item_flag.pay_batch_item_id
      or reservation.pay_batch_candidate_id=item_flag.candidate_row_id
      or reservation.pay_batch_id=item_flag.pay_batch_id
  ),
  reservation_error as (
    select
      reservation_scope.booking_id,
      reservation_scope.reservation_id,
      'WEEKLY_SOURCE_CENSUS_RESERVATION_JOIN_MISSING'::text as code
    from reservation_scope
    where (reservation_scope.pay_batch_item_id is not null
           and not exists (select 1 from public.pay_batch_items as joined_item
                           where joined_item.id=reservation_scope.pay_batch_item_id))
       or (reservation_scope.pay_batch_candidate_id is not null
           and not exists (select 1 from public.pay_batch_candidates as joined_candidate
                           where joined_candidate.id=reservation_scope.pay_batch_candidate_id))
       or (reservation_scope.pay_batch_id is not null
           and not exists (select 1 from public.pay_batches as joined_batch
                           where joined_batch.id=reservation_scope.pay_batch_id))
  ),

  -- Section 5.2 settlement evidence, per (family member, batch).
  settle_pair as (
    select distinct item_flag.booking_id, item_flag.timesheet_id, item_flag.pay_batch_id
    from item_flag
    where item_flag.pay_batch_id is not null
  ),
  history_stat as (
    select
      settle_pair.booking_id,
      settle_pair.timesheet_id,
      settle_pair.pay_batch_id,
      pg_catalog.count(pay_state_history.id)::integer as history_count,
      (pg_catalog.min(pay_state_history.id::text))::uuid as history_id,
      pg_catalog.min(pay_state_history.settled_at_utc) as history_settled_at_utc,
      pg_catalog.min(pay_state_history.signature) as history_signature
    from settle_pair
    left join public.timesheet_pay_state_history as pay_state_history
      on pay_state_history.timesheet_id=settle_pair.timesheet_id
     and pay_state_history.pay_batch_id=settle_pair.pay_batch_id
    group by settle_pair.booking_id, settle_pair.timesheet_id, settle_pair.pay_batch_id
  ),
  snapshot_stat as (
    select
      settle_pair.booking_id,
      settle_pair.timesheet_id,
      settle_pair.pay_batch_id,
      pg_catalog.count(batch_snapshot.id)::integer as snapshot_count,
      pg_catalog.count(distinct batch_snapshot.target_snapshot_json)::integer
        as distinct_target_count,
      pg_catalog.count(distinct batch_snapshot.signature)::integer
        as distinct_signature_count
    from settle_pair
    left join public.pay_batch_timesheet_snapshots as batch_snapshot
      on batch_snapshot.pay_batch_id=settle_pair.pay_batch_id
     and batch_snapshot.timesheet_id=settle_pair.timesheet_id
     and batch_snapshot.candidate_id=p_candidate_id
    group by settle_pair.booking_id, settle_pair.timesheet_id, settle_pair.pay_batch_id
  ),
  -- SNAPSHOT SELECTION.  The authoritative snapshot row, selected with the
  -- installed settle rail's own ordering AND its own candidate filter.  The
  -- HANDOVER 2 round-4 response accepts the additional `candidate_id` predicate
  -- "only where it exactly reproduces the installed settlement owner's selection
  -- and fails closed on mismatch".  The installed `public.pay_settle_rail`
  -- selects, for the snapshot it records and signs:
  --
  --   distinct on (pbs.timesheet_id) ...
  --   from public.pay_batch_timesheet_snapshots pbs
  --   where pbs.pay_batch_id = p_pay_batch_id
  --     and pbs.candidate_id in (select t.candidate_id
  --                              from _tmp_settle_cache_candidates t)
  --   order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  --
  -- and the CTE below is that selection narrowed to the one bundle Candidate.
  -- A snapshot belonging to another Candidate therefore never silently becomes
  -- this root's proof: it is not selected, and the resulting absence is
  -- SETTLEMENT_SNAPSHOT_CONFLICT.  The verifier proves both halves - equality
  -- with the settle rail's own expression over the same rows, and the
  -- fail-closed result on a foreign-Candidate snapshot.
  snapshot_chosen as (
    select distinct on (settle_pair.booking_id, settle_pair.timesheet_id, settle_pair.pay_batch_id)
      settle_pair.booking_id,
      settle_pair.timesheet_id,
      settle_pair.pay_batch_id,
      batch_snapshot.id as snapshot_id,
      batch_snapshot.signature as snapshot_signature
    from settle_pair
    join public.pay_batch_timesheet_snapshots as batch_snapshot
      on batch_snapshot.pay_batch_id=settle_pair.pay_batch_id
     and batch_snapshot.timesheet_id=settle_pair.timesheet_id
     and batch_snapshot.candidate_id=p_candidate_id
    order by settle_pair.booking_id, settle_pair.timesheet_id, settle_pair.pay_batch_id,
             batch_snapshot.created_at_utc desc, batch_snapshot.id
  ),
  settle_state as (
    select
      history_stat.booking_id,
      history_stat.timesheet_id,
      history_stat.pay_batch_id,
      history_stat.history_count,
      history_stat.history_id,
      history_stat.history_settled_at_utc,
      history_stat.history_signature,
      snapshot_chosen.snapshot_id,
      snapshot_chosen.snapshot_signature,
      case
        when history_stat.history_count>1 then 'SETTLEMENT_HISTORY_CONFLICT'
        when history_stat.history_count=1 and (
               coalesce(snapshot_stat.snapshot_count,0)=0
               or coalesce(snapshot_stat.distinct_target_count,0)>1
               or coalesce(snapshot_stat.distinct_signature_count,0)>1
               or coalesce(pg_catalog.btrim(snapshot_chosen.snapshot_signature),'')=''
               or coalesce(pg_catalog.btrim(history_stat.history_signature),'')=''
               or history_stat.history_signature
                  is distinct from snapshot_chosen.snapshot_signature
             ) then 'SETTLEMENT_SNAPSHOT_CONFLICT'
        else null
      end as conflict_code
    from history_stat
    join snapshot_stat
      on snapshot_stat.booking_id=history_stat.booking_id
     and snapshot_stat.timesheet_id=history_stat.timesheet_id
     and snapshot_stat.pay_batch_id=history_stat.pay_batch_id
    left join snapshot_chosen
      on snapshot_chosen.booking_id=history_stat.booking_id
     and snapshot_chosen.timesheet_id=history_stat.timesheet_id
     and snapshot_chosen.pay_batch_id=history_stat.pay_batch_id
  ),

  -- Section 5.1 bindings, evaluated in the order A, B, C.  Binding B is the
  -- whole-batch cancellation "without APPLIED correction work items for the
  -- voided items", and Binding C is intra-batch supersession, so an item proved
  -- by Binding A is never re-proved by B or C.
  item_binding as (
    select
      item_flag.pay_batch_item_id,
      binding_a.pay_batch_item_id is not null as has_a_core,
      coalesce(binding_a.blocker_ok,false) as a_blocker_ok,
      binding_a.correction_request_id,
      binding_a.correction_request_status,
      binding_a.work_item_id,
      binding_a.operation_id,
      (
        binding_a.pay_batch_item_id is null
        and item_flag.batch_status='CANCELLED'
        and item_flag.cancelled_at_utc is not null
        and not exists (
          select 1 from item_flag as sibling_item
          where sibling_item.booking_id=item_flag.booking_id
            and sibling_item.pay_batch_id=item_flag.pay_batch_id
            and not sibling_item.is_voided
        )
        and coalesce(batch_operation_state.all_operations_terminal,true)
        and not exists (
          select 1 from public.pay_payment_correction_requests as batch_request
          where batch_request.pay_batch_id=item_flag.pay_batch_id
            and coalesce(batch_request.status,'') not in
                ('APPLIED','APPLIED_WITH_BLOCKERS','REJECTED','CANCELLED')
        )
      ) as b_ok,
      (
        binding_a.pay_batch_item_id is null
        and coalesce(item_reservation_state.reservations_terminal,false)
        and (
          (item_flag.batch_status='SETTLED'
           and item_flag.execution_commit_state='COMMITTED')
          or (item_flag.batch_status='FAILED'
              and item_flag.completed_at_utc is not null
              and item_flag.execution_commit_state='COMMITTED')
        )
        -- =================================================================
        -- RULING 1 (HANDOVER 2 round 4, OR-10).  Binding C is the
        -- "positively corroborated PAYE-net reprojection void":
        --
        --   "An enumerated voided item is proved under Binding C only when all
        --   common terminal conditions hold and at least one durable
        --   `pay_advance_reservations` row linked by `pay_batch_item_id = i.id`
        --   has `status = 'RELEASED'` and
        --   `released_reason = 'PAYE_NET_REPROJECTION'`.  Every other
        --   reservation linked to the item must also be terminal under the
        --   accepted rule.  The item must have no money-moved or ambiguous
        --   transfer and no live C4 operation.  Batch terminality alone, the
        --   absence of a reservation, or the absence of contradictory evidence
        --   is not proof of why the item was voided.  If the positive
        --   item-linked reprojection artefact is absent, the item does not fit
        --   Binding C and an otherwise terminal void is
        --   `CENSUS_ERROR / WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE`, never
        --   `VOIDED_TERMINAL/C`."
        --
        -- The artefact is the one the installed `public.pay_set_paye_net_manual`
        -- really leaves: it sets the item's own reservation to
        -- `status = 'RELEASED'`, `released_reason = 'PAYE_NET_REPROJECTION'`
        -- (the two `update public.pay_advance_reservations` statements
        -- immediately before its two `is_voided = true` updates) and then nulls
        -- `pay_batch_items.reservation_id`, so the surviving link is
        -- `pay_advance_reservations.pay_batch_item_id`.
        --
        -- The three conjuncts below are, in order: the positive artefact; every
        -- other linked reservation terminal (the `common_ok` flag already
        -- carries the accepted rule, including ruling 5's WRITE_OFF exclusion);
        -- and no money-moved or ambiguous transfer.  The last is stated
        -- explicitly although `common_ok` is stricter still, because the ruling
        -- names it as a condition of Binding C itself.
        -- =================================================================
        and exists (
          select 1 from public.pay_advance_reservations as reprojection_release
          where reprojection_release.pay_batch_item_id=item_flag.pay_batch_item_id
            and reprojection_release.status='RELEASED'
            and reprojection_release.released_reason='PAYE_NET_REPROJECTION'
        )
        and (
          item_flag.pay_bank_transfer_id is null
          or (not coalesce(item_transfer_state.bound_is_final_money_moved,false)
              and coalesce(item_transfer_state.bound_cash_state,'')<>'UNKNOWN')
        )
      ) as c_candidate,
      (
        item_flag.is_voided
        and coalesce(item_reservation_state.reservations_terminal,false)
        and (item_flag.pay_bank_transfer_id is null
             or item_transfer_state.bound_is_terminal_no_money)
        and not coalesce(batch_operation_state.has_live_operation,false)
      ) as common_ok
    from item_flag
    left join binding_a
      on binding_a.pay_batch_item_id=item_flag.pay_batch_item_id
    left join batch_operation_state
      on batch_operation_state.booking_id=item_flag.booking_id
     and batch_operation_state.pay_batch_id=item_flag.pay_batch_id
    left join item_reservation_state
      on item_reservation_state.pay_batch_item_id=item_flag.pay_batch_item_id
    left join item_transfer_state
      on item_transfer_state.pay_batch_item_id=item_flag.pay_batch_item_id
  ),
  item_binding_letter as (
    select
      item_binding.*,
      case
        when item_binding.has_a_core and item_binding.a_blocker_ok then 'A'
        when item_binding.b_ok then 'B'
        when (not item_binding.b_ok) and item_binding.c_candidate then 'C'
        else null
      end as binding_letter
    from item_binding
  ),

  -- Section 4.2 classification, first match wins.
  classified_item as (
    select
      item_flag.booking_id,
      item_flag.timesheet_id,
      item_flag.pay_batch_item_id,
      item_flag.pay_batch_id,
      item_flag.is_voided,
      item_flag.batch_status,
      item_flag.execution_commit_ref,
      item_binding_letter.binding_letter,
      item_binding_letter.correction_request_id,
      item_binding_letter.correction_request_status,
      item_binding_letter.work_item_id,
      item_binding_letter.operation_id,
      item_flag.cancelled_at_utc,
      settle_state.history_id,
      settle_state.history_settled_at_utc,
      settle_state.history_signature,
      settle_state.snapshot_id,
      settle_state.conflict_code,
      (
        not item_flag.is_voided
        and item_flag.batch_terminal
        and item_flag.batch_status in ('SETTLED','FAILED')
        and item_flag.execution_commit_state='COMMITTED'
        and item_flag.execution_committed_at_utc is not null
        and item_flag.settlement_status='SETTLED'
        and item_flag.settled_at_utc is not null
        and coalesce(settle_state.history_count,0)=1
        and settle_state.conflict_code is null
        and coalesce(item_reservation_state.reservations_terminal,false)
        and (item_flag.pay_bank_transfer_id is null
             or item_transfer_state.bound_is_final_money_moved)
        and coalesce(other_transfer_state.other_transfers_consistent,true)
        and coalesce(batch_operation_state.all_operations_terminal,true)
      ) as settlement_proved,
      case
        when item_flag.join_error then 'CENSUS_ERROR'
        when settle_state.conflict_code is not null then 'CENSUS_ERROR'
        when item_flag.is_voided
             and item_binding_letter.has_a_core
             and not item_binding_letter.a_blocker_ok then 'ACTIVE'
        when item_flag.is_voided
             and item_flag.void_terminal_container
             and not (item_binding_letter.common_ok
                      and item_binding_letter.binding_letter is not null)
             and not (
               -- RULING 3 (HANDOVER 2 round 4, OR-8).  "The permitted in-flight
               -- cases are limited to: (a) a live C4 correction operation for
               -- the batch, or (b) the item's bound transfer classifying
               -- `PENDING_NON_FINAL`.  Such an item is
               -- `ACTIVE / WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED`, keeps the
               -- root FROZEN and is retried without consuming the
               -- technical-failure budget.  Once neither permitted in-flight
               -- condition exists, a voided terminal item that no binding proves
               -- is `CENSUS_ERROR` and follows the technical-failure/
               -- manual-review path."
               --
               -- Exactly those two cases appear below, and nothing else.
               -- Checked case by case against the ruling: an outstanding
               -- RESERVED or COMMITTED reservation in an already terminal batch
               -- is NOT self-healing evidence and is not listed; an UNKNOWN or
               -- contradictory transfer is NOT self-healing evidence and is not
               -- listed.  Both remain CENSUS_ERROR where no binding proves the
               -- void.  This changes escalation timing only; it cannot make an
               -- item or root releasable.
               coalesce(batch_operation_state.has_live_operation,false)
               or coalesce(item_transfer_state.bound_cash_state,'')='PENDING_NON_FINAL'
             )
          then 'CENSUS_ERROR'
        when (not item_flag.is_voided) and item_flag.batch_status='CANCELLED'
          then 'CENSUS_ERROR'
        when item_flag.is_voided
             and item_binding_letter.common_ok
             and item_binding_letter.binding_letter is not null
          then 'VOIDED_TERMINAL'
        when (not item_flag.is_voided)
             and item_flag.batch_terminal
             and item_flag.batch_status in ('SETTLED','FAILED')
             and item_flag.execution_commit_state='COMMITTED'
             and item_flag.execution_committed_at_utc is not null
             and item_flag.settlement_status='SETTLED'
             and item_flag.settled_at_utc is not null
             and coalesce(settle_state.history_count,0)=1
             and settle_state.conflict_code is null
             and coalesce(item_reservation_state.reservations_terminal,false)
             and (item_flag.pay_bank_transfer_id is null
                  or item_transfer_state.bound_is_final_money_moved)
             and coalesce(other_transfer_state.other_transfers_consistent,true)
             and coalesce(batch_operation_state.all_operations_terminal,true)
          then 'SETTLED_TERMINAL'
        else 'ACTIVE'
      end as item_class,
      case
        when item_flag.join_error then 'WEEKLY_SOURCE_CENSUS_ITEM_JOIN_INVALID'
        when settle_state.conflict_code is not null then settle_state.conflict_code
        when item_flag.is_voided
             and item_binding_letter.has_a_core
             and not item_binding_letter.a_blocker_ok
          then 'WEEKLY_SOURCE_CENSUS_APPLIED_WITH_BLOCKERS_UNPROVED'
        when item_flag.is_voided
             and item_flag.void_terminal_container
             and not (item_binding_letter.common_ok
                      and item_binding_letter.binding_letter is not null)
             and not (
               coalesce(batch_operation_state.has_live_operation,false)
               or coalesce(item_transfer_state.bound_cash_state,'')='PENDING_NON_FINAL'
             )
          then 'WEEKLY_SOURCE_CENSUS_VOID_UNBINDABLE'
        when item_flag.is_voided
             and not (item_binding_letter.common_ok
                      and item_binding_letter.binding_letter is not null)
          then 'WEEKLY_SOURCE_CENSUS_VOID_NOT_YET_PROVED'
        when (not item_flag.is_voided) and item_flag.batch_status='CANCELLED'
          then 'WEEKLY_SOURCE_CENSUS_ACTIVE_ITEM_IN_CANCELLED_BATCH'
        else null
      end as item_reason
    from item_flag
    join item_binding_letter
      on item_binding_letter.pay_batch_item_id=item_flag.pay_batch_item_id
    left join settle_state
      on settle_state.booking_id=item_flag.booking_id
     and settle_state.timesheet_id=item_flag.timesheet_id
     and settle_state.pay_batch_id=item_flag.pay_batch_id
    left join item_reservation_state
      on item_reservation_state.pay_batch_item_id=item_flag.pay_batch_item_id
    left join item_transfer_state
      on item_transfer_state.pay_batch_item_id=item_flag.pay_batch_item_id
    left join other_transfer_state
      on other_transfer_state.booking_id=item_flag.booking_id
     and other_transfer_state.pay_batch_id=item_flag.pay_batch_id
    left join batch_operation_state
      on batch_operation_state.booking_id=item_flag.booking_id
     and batch_operation_state.pay_batch_id=item_flag.pay_batch_id
  ),

  -- Section 4.3 predicates C1 to C6, recorded per family.
  predicate_row as (
    select classified_item.booking_id,'C1'::text as predicate,
           pg_catalog.jsonb_build_object(
             'pay_batch_item_id',classified_item.pay_batch_item_id,
             'timesheet_id',classified_item.timesheet_id,
             'pay_batch_id',classified_item.pay_batch_id,
             'reason',coalesce(classified_item.item_reason,'ACTIVE_ITEM')) as detail
    from classified_item where classified_item.item_class='ACTIVE'
    union all
    select reservation_scope.booking_id,'C2',
           pg_catalog.jsonb_build_object(
             'reservation_id',reservation_scope.reservation_id,
             'status',reservation_scope.reservation_status)
    from reservation_scope
    where reservation_scope.reservation_status in ('RESERVED','COMMITTED')
    union all
    select family_batch_state.booking_id,'C3',
           pg_catalog.jsonb_build_object('pay_batch_id',family_batch_state.pay_batch_id,
                                         'batch_status',batch_row.status)
    from family_batch_state
    join public.pay_batches as batch_row on batch_row.id=family_batch_state.pay_batch_id
    where family_batch_state.has_non_voided
      and not (
        batch_row.status in ('SETTLED','CANCELLED')
        or (batch_row.status='FAILED' and batch_row.completed_at_utc is not null)
      )
    union all
    select batch_operation.booking_id,'C4',
           pg_catalog.jsonb_build_object(
             'pay_batch_id',batch_operation.pay_batch_id,
             'operation_id',batch_operation.operation_id,
             'status',batch_operation.operation_status)
    from batch_operation
    where batch_operation.operation_status in (
            'QUEUED','RUNNING','WAITING','WAITING_AUTHORISATION',
            'WAITING_PROVIDER','REVIEW_REQUIRED')
       or batch_operation.lease_expires_at_utc>v_now
       or batch_operation.lock_expires_at_utc>v_now
    union all
    select scope_transfer_class.booking_id,'C5',
           pg_catalog.jsonb_build_object(
             'pay_batch_id',scope_transfer_class.pay_batch_id,
             'pay_bank_transfer_id',scope_transfer_class.transfer_id,
             'scope_kind',scope_transfer_class.scope_kind,
             'cash_state',scope_transfer_class.cash_state)
    from scope_transfer_class
    where scope_transfer_class.is_pending_non_final
       or (not scope_transfer_class.is_final_money_moved
           and not scope_transfer_class.is_terminal_no_money)
    union all
    select family_batch_state.booking_id,'C6',
           pg_catalog.jsonb_build_object('pay_batch_id',family_batch_state.pay_batch_id,
                                         'batch_status','DRAFT')
    from family_batch_state
    join public.pay_batches as draft_batch on draft_batch.id=family_batch_state.pay_batch_id
    where family_batch_state.has_non_voided and draft_batch.status='DRAFT'
  ),

  -- Every technical (CENSUS_ERROR) finding that is not attached to one item.
  scope_error as (
    select 'MEMBER'::text as scope,
           'WEEKLY_SOURCE_CENSUS_BOOKING_IDENTITY_MISSING'::text as code,
           pg_catalog.jsonb_build_object(
             'requested_timesheet_id',unresolved_member.requested_timesheet_id) as detail
    from unresolved_member
    union all
    select 'MEMBER','WEEKLY_SOURCE_CENSUS_MEMBER_NOT_RESOLVED',
           pg_catalog.jsonb_build_object(
             'requested_timesheet_id',unaccounted_member.timesheet_id)
    from unaccounted_member
    union all
    select 'MEMBER','WEEKLY_SOURCE_CENSUS_BOOKING_IDENTITY_BLANK',
           pg_catalog.jsonb_build_object(
             'requested_timesheet_id',blank_booking_member.timesheet_id)
    from blank_booking_member
    union all
    select 'MEMBER',member_owner_error.code,
           pg_catalog.jsonb_build_object(
             'family_booking_id',member_owner_error.booking_id,
             'timesheet_id',member_owner_error.family_timesheet_id)
    from member_owner_error
    union all
    select 'FAMILY',family_error.code,
           pg_catalog.jsonb_build_object('family_booking_id',family_error.booking_id,
                                         'current_count',family_error.current_count)
    from family_error
    union all
    select 'FAMILY',family_split_error.code,
           pg_catalog.jsonb_build_object(
             'family_booking_id',family_split_error.booking_id,
             'sibling_booking_id',family_split_error.sibling_booking_id)
    from family_split_error
    union all
    select 'FAMILY','WEEKLY_SOURCE_CENSUS_FAMILY_SPLIT_SCAN_BOUND_EXCEEDED',
           pg_catalog.jsonb_build_object(
             'family_booking_id',family_split_bound_error.booking_id,
             'timesheet_id',family_split_bound_error.family_timesheet_id,
             'rows_examined',family_split_bound_error.rows_examined,
             'bound',v_split_scan_bound)
    from family_split_bound_error
    union all
    select 'RESERVATION',reservation_error.code,
           pg_catalog.jsonb_build_object('family_booking_id',reservation_error.booking_id,
                                         'reservation_id',reservation_error.reservation_id)
    from reservation_error
    union all
    select 'TRANSFER',umbrella_evidence_error.code,
           pg_catalog.jsonb_build_object(
             'family_booking_id',umbrella_evidence_error.booking_id,
             'pay_batch_id',umbrella_evidence_error.pay_batch_id,
             'pay_bank_transfer_id',umbrella_evidence_error.transfer_id)
    from umbrella_evidence_error
    where umbrella_evidence_error.code is not null
  ),

  aggregate_counts as (
    select
      pg_catalog.count(*) filter (where classified_item.item_class='CENSUS_ERROR')::integer
        as census_error_count,
      pg_catalog.count(*) filter (where classified_item.item_class='VOIDED_TERMINAL')::integer
        as voided_terminal_count,
      pg_catalog.count(*) filter (where classified_item.item_class='SETTLED_TERMINAL')::integer
        as settled_terminal_count,
      pg_catalog.count(*) filter (where classified_item.item_class='ACTIVE')::integer
        as active_count
    from classified_item
  )

  select pg_catalog.jsonb_build_object(
    'result',
      case
        when (select aggregate_counts.census_error_count from aggregate_counts)>0
          or exists (select 1 from scope_error) then 'CENSUS_ERROR'
        when (select aggregate_counts.active_count from aggregate_counts)>0
          or exists (select 1 from predicate_row) then 'FROZEN'
        else 'RELEASABLE'
      end,
    'reason',
      case
        when (select aggregate_counts.census_error_count from aggregate_counts)>0
          or exists (select 1 from scope_error)
          then 'WEEKLY_SOURCE_CENSUS_TECHNICAL_ERROR'
        when (select aggregate_counts.active_count from aggregate_counts)>0
          or exists (select 1 from predicate_row)
          then 'WEEKLY_SOURCE_CENSUS_FROZEN'
        else 'WEEKLY_SOURCE_CENSUS_RELEASABLE'
      end,
    'class_counts',pg_catalog.jsonb_build_object(
      'CENSUS_ERROR',(select aggregate_counts.census_error_count from aggregate_counts),
      'VOIDED_TERMINAL',(select aggregate_counts.voided_terminal_count from aggregate_counts),
      'SETTLED_TERMINAL',(select aggregate_counts.settled_terminal_count from aggregate_counts),
      'ACTIVE',(select aggregate_counts.active_count from aggregate_counts)),
    'items',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'pay_batch_item_id',classified_item.pay_batch_item_id,
        'timesheet_id',classified_item.timesheet_id,
        'pay_batch_id',classified_item.pay_batch_id,
        'family_booking_id',classified_item.booking_id,
        'class',classified_item.item_class,
        'binding',case when classified_item.item_class='VOIDED_TERMINAL'
                       then classified_item.binding_letter else null end,
        'predicate',case when classified_item.item_class='ACTIVE' then 'C1' else null end,
        'reason',classified_item.item_reason
      ) order by classified_item.booking_id, classified_item.timesheet_id,
                 classified_item.pay_batch_id, classified_item.pay_batch_item_id)
      from classified_item),'[]'::jsonb),
    'proof',coalesce((
      select pg_catalog.jsonb_agg(proof_rows.proof_json
             order by proof_rows.sort_class, proof_rows.sort_item)
      from (
        select 1 as sort_class, classified_item.pay_batch_item_id as sort_item,
          pg_catalog.jsonb_build_object(
            'section','5.1',
            'timesheet_id',classified_item.timesheet_id,
            'pay_batch_item_id',classified_item.pay_batch_item_id,
            'pay_batch_id',classified_item.pay_batch_id,
            'binding',classified_item.binding_letter,
            'correction_request_id',classified_item.correction_request_id,
            'correction_request_status',classified_item.correction_request_status,
            'work_item_id',classified_item.work_item_id,
            'operation_id',classified_item.operation_id,
            'cancelled_at_utc',classified_item.cancelled_at_utc) as proof_json
        from classified_item where classified_item.item_class='VOIDED_TERMINAL'
        union all
        select 2, classified_item.pay_batch_item_id,
          pg_catalog.jsonb_build_object(
            'section','5.2',
            'timesheet_id',classified_item.timesheet_id,
            'pay_batch_item_id',classified_item.pay_batch_item_id,
            'pay_batch_id',classified_item.pay_batch_id,
            'timesheet_pay_state_history_id',classified_item.history_id,
            'settled_at_utc',classified_item.history_settled_at_utc,
            'signature',classified_item.history_signature,
            'pay_batch_timesheet_snapshot_id',classified_item.snapshot_id,
            'execution_commit_ref',classified_item.execution_commit_ref)
        from classified_item where classified_item.item_class='SETTLED_TERMINAL'
      ) as proof_rows),'[]'::jsonb),
    'families',coalesce((
      select pg_catalog.jsonb_agg(family_rows.family_json
             order by family_rows.family_booking_id)
      from (
        select family_stat.booking_id as family_booking_id,
          pg_catalog.jsonb_build_object(
            'family_booking_id',family_stat.booking_id,
            'member_count',family_stat.member_count,
            'current_count',family_stat.current_count,
            'member_timesheet_ids',coalesce((
              select pg_catalog.jsonb_agg(family_member.family_timesheet_id
                     order by family_member.family_timesheet_id)
              from family_member
              where family_member.booking_id=family_stat.booking_id),'[]'::jsonb),
            'class_counts',pg_catalog.jsonb_build_object(
              'CENSUS_ERROR',(select pg_catalog.count(*) filter
                 (where classified_item.item_class='CENSUS_ERROR')::integer
                 from classified_item where classified_item.booking_id=family_stat.booking_id),
              'VOIDED_TERMINAL',(select pg_catalog.count(*) filter
                 (where classified_item.item_class='VOIDED_TERMINAL')::integer
                 from classified_item where classified_item.booking_id=family_stat.booking_id),
              'SETTLED_TERMINAL',(select pg_catalog.count(*) filter
                 (where classified_item.item_class='SETTLED_TERMINAL')::integer
                 from classified_item where classified_item.booking_id=family_stat.booking_id),
              'ACTIVE',(select pg_catalog.count(*) filter
                 (where classified_item.item_class='ACTIVE')::integer
                 from classified_item where classified_item.booking_id=family_stat.booking_id))
          ) as family_json
        from family_stat
      ) as family_rows),'[]'::jsonb),
    'predicates',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'family_booking_id',predicate_row.booking_id,
        'predicate',predicate_row.predicate,
        'detail',predicate_row.detail)
        order by predicate_row.booking_id, predicate_row.predicate,
                 predicate_row.detail::text)
      from predicate_row),'[]'::jsonb),
    'errors',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'scope',scope_error.scope,'code',scope_error.code,'detail',scope_error.detail)
        order by scope_error.scope, scope_error.code, scope_error.detail::text)
      from scope_error),'[]'::jsonb),
    'member_timesheet_ids',coalesce((
      select pg_catalog.jsonb_agg(family_member.family_timesheet_id
             order by family_member.family_timesheet_id)
      from family_member),'[]'::jsonb),
    'expanded_member_timesheet_ids',coalesce((
      select pg_catalog.jsonb_agg(family_member.family_timesheet_id
             order by family_member.family_timesheet_id)
      from family_member
      where not (family_member.family_timesheet_id=any(v_requested))),'[]'::jsonb),
    -- Members whose Contract-to-Candidate chain is unresolvable.  Not an error
    -- (both columns are nullable), reported so it is never silent.
    'member_candidate_unknown_timesheet_ids',coalesce((
      select pg_catalog.jsonb_agg(member_owner_unknown.family_timesheet_id
             order by member_owner_unknown.family_timesheet_id)
      from member_owner_unknown),'[]'::jsonb),
    -- BOUNDED READ: the positive completion proof for the family-split probe.
    'family_split_scan',pg_catalog.jsonb_build_object(
      'bound',v_split_scan_bound,
      'members_probed',(select pg_catalog.count(*)::integer
                        from family_split_completion),
      'max_rows_examined',coalesce((
        select pg_catalog.max(family_split_completion.rows_examined)
        from family_split_completion),0),
      'completed',coalesce((
        select pg_catalog.bool_and(family_split_completion.completed)
        from family_split_completion),true)),
    'evaluated_at_utc',v_now
  )
  into v_result;

  return v_result;
end;
$function$;

alter function private.weekly_source_freeze_census_uuid_array_v1(jsonb) owner to postgres;
alter function private.weekly_source_freeze_census_v1(uuid,uuid[]) owner to postgres;

revoke all on function private.weekly_source_freeze_census_uuid_array_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_freeze_census_v1(uuid,uuid[])
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_freeze_census_v1(uuid,uuid[]) is
  'Read-only Weekly Source Banking Pay freeze census (proof/32 sections 4 and 5). Takes no row lock on any Banking Pay table and writes nothing. Returns RELEASABLE, FROZEN or CENSUS_ERROR with every item class, binding and proof tuple.';

commit;
