-- Repeatable CloudTMS authority: weekly_source_first_authorisation_v1
--
-- Gate 3 (contract section 7).  Two owners and their read-only companions:
--
--   * interface I-6, `private.weekly_source_first_authorise_core_v1`, called by
--     the Office first-authorisation entry point and by the Gate 5 head
--     publication coordinator for a genuinely new B root;
--   * the Office first-authorisation entry point
--     `public.weekly_source_first_authorise_v1`;
--   * G3-3 `public.weekly_source_first_authorisation_withdraw_v1`, the Office
--     change-of-mind route, implemented to `P:\proof\36_FIRST_AUTHORISATION_
--     WITHDRAWAL_UNAUTHORISE_20260917.md` section 3 order, section 4 checks
--     W1 to W9 with their exact refusal codes and permanent/temporary nature,
--     and section 5 effects 1 to 9;
--   * G3-4 `public.weekly_source_first_authorisation_withdraw_available_v1`,
--     the read-only availability verdict the screens use, computed from the
--     same W1 to W9.
--
-- Pack authority, read word for word and not paraphrased here:
--   proof/36 sections 3, 4, 5, 6, 7 and 8;
--   proof/34 sections 4 to 7 (retained identities, lock order, integrity);
--   proof/32 sections 4.2, 4.3 C5, 5.1, 6 and 8 step 3;
--   24 sections 4.1 and 4.1A;  H2-037, H2-039, H2-036.
--   HANDOVER 2 round 4 (`HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R4.md`)
--   rulings 3, 4, 5 item 5 and 6.  Ruling 6 restates R25; ruling 4 forbids any
--   private Weekly Source interpretation of a `VOIDED` transfer, so W2 consumes
--   the installed classifier only; ruling 5 item 5 makes a reservation released
--   with `released_reason = 'WRITE_OFF'` no evidence of a completed
--   cancellation, so W4 and W6 never accept it as such.
--   HANDOVER 2 round 5 (`HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R5.md`)
--   section A3, section A5 and the rotation/signature paragraph of Part E.
--
-- ROUND 5, SECTION A3 — THE RULE THAT REPLACED WP-07b's PERMANENT REFUSAL.
-- The finance approver REJECTED option (a) as a product rule:
--
--   "A user must not become permanently unable to withdraw an authorisation
--    merely because its entitlement head has been committed. ... Therefore the
--    withdrawal owner must supersede the committed head atomically with the
--    withdrawal.  The withdrawal is permitted only when authoritative checks
--    prove that no payment work or ambiguous money effect has crossed the
--    boundary. ... A missing or ambiguous result refuses."
--
-- and its five numbered steps are implemented as one transaction in
-- `public.weekly_source_first_authorisation_withdraw_v1` below.  Package WP-07c
-- owns that change.  The Gate 4 Workbench selector is NOT touched: ruling A3
-- ends "The Workbench selector remains unchanged and continues to consume only
-- current committed heads", and it continues to do exactly that — the head it
-- would have read is no longer current, which is the whole point.
--
-- ROUND 5, SECTION A5 — the `WRITE_OFF` reservation release.  Confirmed as the
-- current fail-closed rule, with a wording requirement this file must meet:
-- "The message must identify the finance-case write-off as unresolved Banking
-- Pay evidence and route it to review.  Do not label it paid, settled or safely
-- cancelled."  Declared addition D4 therefore gained its own refusal code and
-- its own message (see W4).
--
-- Boundary.  Nothing here defines, wraps, re-creates, re-points or triggers a
-- Banking Pay owner.  `public.timesheet_unauthorise_atomic`,
-- `public.timesheet_authorise_generic_atomic`,
-- `private.pay_workbench_scope_invalidate_v1`,
-- `public.pay_workbench_scope_change_tx_token_v1`,
-- `public._pay_workbench_candidate_serial_try_gate`,
-- `public._pay_timesheet_rotation_scope`,
-- `public._pay_workbench_normalise_timesheet_rotation_scope_payload` and
-- `public._pay_rail_state_money_movement_classify` are CALL-ONLY and unchanged.
-- No Workbench session setting is ever read as authority, set or pre-seeded.
-- The withdrawal owner writes no Draft, item, reservation, transfer, Case,
-- recovery or settlement row, and touches no invoice object (DEC-061 Option A).
--
-- Interfaces consumed: I-1 `private.weekly_source_lock_and_resolve_families_v1`
-- (WP-03) and I-2 `private.weekly_source_freeze_census_v1` (WP-08a).  The
-- withdrawal checks reuse I-2's item classes; every other predicate is read
-- from the evidence proof/36 section 4 names.
--
-- Decision D8: the authorisation record is per ROOT, in
-- `public.weekly_source_root_authorisations`.  I-6 inserts the generation after
-- the ordinary Authorise succeeds; the withdrawal owner sets the two withdrawal
-- columns and clears the head pointer on the live generation and nothing else.
--
-- Declared additions beyond the pack's literal text, each fail-closed and each
-- recorded in the WP-07 report:
--   D1. `WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT` — this owner is the route
--       for a Weekly-Source-managed root only (proof/36 section 3).  A root with
--       no live authorisation generation is refused here and belongs to the
--       unchanged ordinary owner.  It is not one of the nine W refusals.
--   D2. W6 also refuses permanently for a `ts_pay_adjustments` row carrying
--       `paid_at_utc`: that is Candidate money already moved.
--   D3. A batch-level transfer classified terminal-no-money has no bound item,
--       so W2's "permitted only when its item is VOIDED_TERMINAL" cannot be
--       satisfied and it refuses temporarily.  Such a transfer is only in scope
--       while a non-voided family item survives, which W3 already refuses.
--   D4. W4 refuses outright when any family item carries a reservation
--       `RELEASED` with `released_reason = 'WRITE_OFF'`.  This is STRICTER than
--       both `proof/36 section 4` W4, whose literal predicate is
--       `status IN ('RESERVED','COMMITTED')`, and HANDOVER 2 round 4 ruling 5
--       item 5, which forbids a write-off release from SATISFYING the positive
--       reservation evidence a binding needs but does not make it a refusal in
--       its own right (W6, below, implements exactly that evidence rule).  It
--       is kept as a refusal because `pay_finance_case_write_off` is recorded
--       by ruling 5 item 5 as no release authority at all, with a
--       finance-case-wide predicate and no batch-state guard, so a family item
--       whose reservation it released has no proved disposition; refusing is
--       fail-closed and can never cause a wrong payment.  It CAN refuse a
--       withdrawal the pack would permit.  Reversing it is a one-branch change:
--       delete the W4 `RESERVATION_RELEASED_BY_WRITE_OFF` block and leave the
--       rule to W6 alone.  ROUND 5 SECTION A5 CONFIRMED IT as the current
--       fail-closed rule and added the wording requirement, so it now carries
--       its own code `WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED` and its
--       own message, which names it as unresolved Banking Pay evidence routed
--       to review and never calls it paid, settled or safely cancelled.
--       (WP-07b finding F5; WP-07c under ruling A5.)
--   D5. WITHDRAWN BY ROUND 5 SECTION A3.  WP-07b's
--       `WEEKLY_SOURCE_UNAUTHORISE_ENTITLEMENT_HEAD_COMMITTED` made a committed
--       head a PERMANENT W1 refusal.  The finance approver rejected that as a
--       product rule.  A committed current head is now SUPERSEDED atomically
--       with the withdrawal (W11 and section 5 step 3); the code and its
--       message are gone, and nothing returns them.
--   D6. W10, "other committed financial effect" — the ruling A3 list is wider
--       than proof/36's nine checks: it also names a provider ATTEMPT, a
--       transfer EVENT, a REMITTANCE and an advance against the root.  W10 adds
--       exactly those, each bound to a family pay item and never to a batch at
--       large (proof/32 section 4.3 C5), and each fail-closed.  Weekly Source
--       reads Banking Pay evidence and never reinterprets a marker (round 4
--       ruling 4).
--   D7. W11, head supersession safety — the checks that must pass before this
--       owner may write a head row at all: exactly one committed current head
--       for the family, proved by an explicit cardinality test and not by the
--       unique index; its physical identity equal to the canonical root's; the
--       live generation pointing at it; nothing built on top of it; and no
--       proposed or pending bundle naming it.  Anything else is an integrity
--       failure and refuses.
--
\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- Signature drift, removed explicitly rather than left as an overload.
--
-- WP-07c gave the exact-replay reader a third argument (the presented physical
-- Timesheet expectation, which is what makes a CONFLICTING replay detectable).
-- `CREATE OR REPLACE FUNCTION` cannot change a signature, so on any database
-- that already carries the two-argument form this repeatable would leave BOTH
-- installed: the old one keeps whatever ACL it had, and a two-argument call
-- would silently reach the version that cannot detect a conflict.  That is
-- exactly the latent defect the seals review found in another package's file,
-- so it is closed here by construction.
-- ---------------------------------------------------------------------------
drop function if exists private.weekly_source_first_authorisation_withdrawal_recorded_v1(uuid,text);

-- ---------------------------------------------------------------------------
-- Exact UUID identities carried by an installed Banking Pay selection or
-- blocker JSON array.  A value that is not an exact UUID carries no item
-- identity and is dropped, so proof/36 section 4 W6's "carries no exact item
-- identity" rule fails closed.  Same shape as the census helper; kept local so
-- neither package silently changes the other's meaning.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_withdrawal_uuid_array_v1(
  p_value jsonb
) returns uuid[]
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(
    pg_catalog.array_agg(distinct element.value_text::uuid),
    array[]::uuid[]
  )
  from pg_catalog.jsonb_array_elements_text(
    case when pg_catalog.jsonb_typeof(p_value)='array' then p_value else '[]'::jsonb end
  ) as element(value_text)
  where element.value_text
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
$function$;

-- ---------------------------------------------------------------------------
-- The family transfer scope of proof/32 section 4.3 C5, classified with the
-- exact installed adapter the cancellation owner uses.  Read-only.
--
-- HANDOVER 2 round 4 ruling 4: Weekly Source adds no private interpretation of
-- a transfer status.  Every verdict below is the installed classifier's own
-- `is_final_money_moved` / `is_pending_non_final` / `is_terminal_no_money`.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_withdrawal_transfer_scope_v1(
  p_candidate_id uuid,
  p_member_timesheet_ids uuid[]
) returns jsonb
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with member as (
    select distinct member_id.value as timesheet_id
    from pg_catalog.unnest(coalesce(p_member_timesheet_ids,array[]::uuid[]))
      as member_id(value)
    where member_id.value is not null
  ),
  family_item as (
    select
      item_row.id as pay_batch_item_id,
      item_row.timesheet_id,
      item_row.pay_bank_transfer_id,
      item_row.umbrella_id,
      item_row.pay_channel,
      item_row.is_voided,
      candidate_row.pay_batch_id,
      candidate_row.candidate_id
    from public.pay_batch_items as item_row
    join member on member.timesheet_id=item_row.timesheet_id
    left join public.pay_batch_candidates as candidate_row
      on candidate_row.id=item_row.pay_batch_candidate_id
  ),
  -- C5 condition 3: the batch still contains a non-voided family item
  -- belonging, through pay_batch_candidates, to the bundle Candidate.
  batch_with_live_family_item as (
    select distinct family_item.pay_batch_id
    from family_item
    where family_item.pay_batch_id is not null
      and family_item.is_voided=false
      and family_item.candidate_id=p_candidate_id
  ),
  -- The stronger direct binding (C5 condition 6 excludes these from every
  -- batch-level rule).
  direct_transfer as (
    select
      transfer_row.id as transfer_id,
      transfer_row.pay_batch_id,
      'ITEM_BOUND'::text as scope_kind,
      family_item.pay_batch_item_id
    from family_item
    join public.pay_bank_transfers as transfer_row
      on transfer_row.id=family_item.pay_bank_transfer_id
  ),
  -- A transfer id bound to a family item that names no pay_bank_transfers row.
  unjoinable_transfer as (
    select family_item.pay_batch_item_id, family_item.pay_bank_transfer_id as transfer_id
    from family_item
    where family_item.pay_bank_transfer_id is not null
      and not exists (
        select 1 from public.pay_bank_transfers as transfer_row
        where transfer_row.id=family_item.pay_bank_transfer_id
      )
  ),
  batch_candidate_transfer as (
    select distinct
      transfer_row.id as transfer_id,
      transfer_row.pay_batch_id,
      'BATCH_CANDIDATE'::text as scope_kind,
      null::uuid as pay_batch_item_id
    from public.pay_bank_transfers as transfer_row
    join batch_with_live_family_item
      on batch_with_live_family_item.pay_batch_id=transfer_row.pay_batch_id
    where transfer_row.candidate_id=p_candidate_id
      and not exists (
        select 1 from direct_transfer
        where direct_transfer.transfer_id=transfer_row.id
      )
  ),
  -- The six-condition null-Candidate Umbrella rule, read only from the frozen
  -- Draft item.  The Candidate's current Umbrella is never consulted.
  batch_umbrella_transfer as (
    select distinct
      transfer_row.id as transfer_id,
      transfer_row.pay_batch_id,
      'BATCH_UMBRELLA'::text as scope_kind,
      null::uuid as pay_batch_item_id
    from public.pay_bank_transfers as transfer_row
    join batch_with_live_family_item
      on batch_with_live_family_item.pay_batch_id=transfer_row.pay_batch_id
    where transfer_row.candidate_id is null
      and transfer_row.umbrella_id is not null
      and exists (
        select 1 from family_item
        where family_item.pay_batch_id=transfer_row.pay_batch_id
          and family_item.is_voided=false
          and family_item.candidate_id=p_candidate_id
          and family_item.umbrella_id=transfer_row.umbrella_id
          and family_item.pay_channel=transfer_row.pay_channel
      )
      and not exists (
        select 1 from direct_transfer
        where direct_transfer.transfer_id=transfer_row.id
      )
  ),
  -- Missing, contradictory or ambiguous frozen item evidence for a
  -- null-Candidate transfer in a live family batch.  Never a fallback to
  -- current Candidate data: it is an error, and W2 refuses temporarily.
  umbrella_evidence_error as (
    select distinct
      transfer_row.id as transfer_id,
      transfer_row.pay_batch_id
    from public.pay_bank_transfers as transfer_row
    join batch_with_live_family_item
      on batch_with_live_family_item.pay_batch_id=transfer_row.pay_batch_id
    where transfer_row.candidate_id is null
      and not exists (
        select 1 from direct_transfer
        where direct_transfer.transfer_id=transfer_row.id
      )
      and not exists (
        select 1 from batch_umbrella_transfer
        where batch_umbrella_transfer.transfer_id=transfer_row.id
      )
      and (
        transfer_row.umbrella_id is null
        or exists (
          select 1 from family_item
          where family_item.pay_batch_id=transfer_row.pay_batch_id
            and family_item.is_voided=false
            and family_item.candidate_id=p_candidate_id
            and (family_item.umbrella_id is null or family_item.pay_channel is null)
        )
      )
  ),
  in_scope as (
    select transfer_id,pay_batch_id,scope_kind,pay_batch_item_id from direct_transfer
    union all
    select transfer_id,pay_batch_id,scope_kind,pay_batch_item_id from batch_candidate_transfer
    union all
    select transfer_id,pay_batch_id,scope_kind,pay_batch_item_id from batch_umbrella_transfer
  ),
  classified as (
    select
      in_scope.transfer_id,
      in_scope.pay_batch_id,
      in_scope.scope_kind,
      in_scope.pay_batch_item_id,
      classification.cash_state,
      coalesce(classification.is_final_money_moved,false) as is_final_money_moved,
      coalesce(classification.is_terminal_no_money,false) as is_terminal_no_money,
      coalesce(classification.is_pending_non_final,false) as is_pending_non_final
    from in_scope
    join public.pay_bank_transfers as transfer_row on transfer_row.id=in_scope.transfer_id
    cross join lateral public._pay_rail_state_money_movement_classify(
      transfer_row.status,
      transfer_row.rail_state,
      coalesce(transfer_row.rail_meta_json,'{}'::jsonb),
      coalesce(transfer_row.rail_meta_json,'{}'::jsonb)
    ) as classification
  )
  select pg_catalog.jsonb_build_object(
    'transfers',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'pay_bank_transfer_id',classified.transfer_id,
        'pay_batch_id',classified.pay_batch_id,
        'scope_kind',classified.scope_kind,
        'pay_batch_item_id',classified.pay_batch_item_id,
        'cash_state',classified.cash_state,
        'is_final_money_moved',classified.is_final_money_moved,
        'is_terminal_no_money',classified.is_terminal_no_money,
        'is_pending_non_final',classified.is_pending_non_final,
        'is_ambiguous',
          (not classified.is_final_money_moved
           and not classified.is_terminal_no_money
           and not classified.is_pending_non_final)
      ) order by classified.transfer_id, classified.pay_batch_item_id)
      from classified),'[]'::jsonb),
    'errors',coalesce((
      select pg_catalog.jsonb_agg(error_row.error_json order by error_row.sort_key)
      from (
        select 1 as sort_key, pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_WITHDRAWAL_TRANSFER_UNJOINABLE',
          'pay_bank_transfer_id',unjoinable_transfer.transfer_id,
          'pay_batch_item_id',unjoinable_transfer.pay_batch_item_id) as error_json
        from unjoinable_transfer
        union all
        select 2, pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_WITHDRAWAL_UMBRELLA_EVIDENCE_AMBIGUOUS',
          'pay_bank_transfer_id',umbrella_evidence_error.transfer_id,
          'pay_batch_id',umbrella_evidence_error.pay_batch_id)
        from umbrella_evidence_error
      ) as error_row),'[]'::jsonb)
  );
$function$;

-- ---------------------------------------------------------------------------
-- Read-only root context.  Resolves the family through the installed Workbench
-- rotation authority (proof/34 section 7: a stored or browser-supplied id is
-- only a lookup key), reads the live root-authorisation generation and the
-- current row signature.  No lock, no write.  The caller re-runs it under the
-- I-1 locks before acting.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_first_authorisation_context_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_identity jsonb;
  v_members uuid[];
  v_canonical uuid;
  v_family text;
  v_root public.timesheets%rowtype;
  v_candidate_id uuid;
  v_contract_candidate_id uuid;
  v_tsfin_owner_count integer;
  v_contract_week_id uuid;
  v_contract_week_count integer;
  v_signature_json jsonb;
  v_signature text;
  v_live public.weekly_source_root_authorisations%rowtype;
  v_live_count integer;
  v_family_count integer;
  v_live_elsewhere integer;
  v_max_generation integer;
begin
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','ROOT_ID_REQUIRED');
  end if;

  v_identity:=private.weekly_source_resolve_root_identity_v1(p_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true then
    return v_identity||pg_catalog.jsonb_build_object('retryable',false);
  end if;

  v_family:=v_identity->>'family_booking_id';
  v_canonical:=(v_identity->>'canonical_timesheet_id')::uuid;
  select pg_catalog.array_agg(member_element.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_identity->'member_timesheet_ids')
    as member_element(value);

  select * into v_root from public.timesheets where timesheet_id=v_canonical;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','CANONICAL_ROW_MISSING');
  end if;

  -- The Candidate the serial gate is pinned to.  contracts.candidate_id is
  -- nullable, so a null or disagreeing owner fails closed rather than
  -- serialising the withdrawal under the wrong Candidate key.
  select contract_row.candidate_id
    into v_contract_candidate_id
  from public.contracts as contract_row
  where contract_row.id=v_root.contract_id;

  -- The current TSFIN owner, read as a set so a second current row owned by
  -- another Candidate is a conflict rather than an arbitrary pick.  The
  -- installed invalidator refuses on exactly that shape
  -- (PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH), so refusing here
  -- keeps the withdrawal from writing anything at all.
  select pg_catalog.count(distinct financial_row.candidate_id)::integer,
         pg_catalog.min(financial_row.candidate_id::text)::uuid
    into v_tsfin_owner_count, v_candidate_id
  from public.timesheets_financials as financial_row
  where financial_row.timesheet_id=v_canonical
    and financial_row.is_current=true
    and financial_row.candidate_id is not null;

  if v_contract_candidate_id is null
     or v_tsfin_owner_count>1
     or (v_candidate_id is not null and v_candidate_id is distinct from v_contract_candidate_id) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','CANDIDATE_UNRESOLVED',
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical);
  end if;
  v_candidate_id:=v_contract_candidate_id;

  -- The Contract Week the signature helper is given.  More than one week row
  -- pointing at one physical Timesheet is not a shape the installed owners
  -- produce, so the cardinality is checked rather than assumed.
  select pg_catalog.count(*)::integer,
         pg_catalog.min(contract_week_row.id::text)::uuid
    into v_contract_week_count, v_contract_week_id
  from public.contract_weeks as contract_week_row
  where contract_week_row.timesheet_id=v_canonical;
  if v_contract_week_count>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','CONTRACT_WEEK_CARDINALITY',
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical);
  end if;

  v_signature_json:=public.timesheet_lifecycle_guard_signature_v1(
    v_canonical,v_contract_week_id,false);
  v_signature:=nullif(pg_catalog.btrim(coalesce(
    v_signature_json->>'backend_row_signature',
    v_signature_json->>'row_signature','')),'');

  -- Live and historical root-authorisation generations for the family.
  select pg_catalog.count(*)::integer
    into v_family_count
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=any(v_members);

  select pg_catalog.count(*)::integer
    into v_live_count
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=any(v_members)
    and authorisation_row.withdrawn_at_utc is null;

  select pg_catalog.count(*)::integer
    into v_live_elsewhere
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=any(v_members)
    and authorisation_row.root_timesheet_id is distinct from v_canonical
    and authorisation_row.withdrawn_at_utc is null;

  select coalesce(pg_catalog.max(authorisation_row.authorisation_generation),0)
    into v_max_generation
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=v_canonical;

  if v_live_count>1 or v_live_elsewhere>0 then
    -- A live generation on a historical member means the authorised root was
    -- rotated after first authorisation: proof/34 section 6 integrity failure.
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason',case when v_live_elsewhere>0
                    then 'LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW'
                    else 'LIVE_AUTHORISATION_CARDINALITY' end,
      'family_booking_id',v_family,'canonical_timesheet_id',v_canonical);
  end if;

  select * into v_live
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=v_canonical
    and authorisation_row.withdrawn_at_utc is null;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'timesheet_id',p_timesheet_id,
    'family_booking_id',v_family,
    'canonical_timesheet_id',v_canonical,
    'canonical_version',(v_identity->>'canonical_version')::integer,
    'requested_version',(v_identity->>'requested_version')::integer,
    'requested_is_canonical',coalesce((v_identity->>'requested_is_canonical')::boolean,false),
    'family_is_current',coalesce((v_identity->>'family_is_current')::boolean,false),
    'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
    'candidate_id',v_candidate_id,
    'contract_id',v_root.contract_id,
    'contract_week_id',v_contract_week_id,
    'current_row_signature',v_signature,
    'root',pg_catalog.jsonb_build_object(
      'timesheet_id',v_root.timesheet_id,
      'booking_id',v_root.booking_id,
      'version',v_root.version,
      'is_current',v_root.is_current,
      'authorised_at_server',v_root.authorised_at_server,
      'archived_at_utc',v_root.archived_at_utc),
    'live_authorisation_count',v_live_count,
    'family_authorisation_count',v_family_count,
    'max_generation',v_max_generation,
    'authorisation',case when v_live.id is null then null else pg_catalog.jsonb_build_object(
      'id',v_live.id,
      'root_timesheet_id',v_live.root_timesheet_id,
      'family_booking_id',v_live.family_booking_id,
      'timesheet_version',v_live.timesheet_version,
      'authorisation_generation',v_live.authorisation_generation,
      'authorised_row_signature',v_live.authorised_row_signature,
      'current_entitlement_head_id',v_live.current_entitlement_head_id,
      'authorised_by_user_id',v_live.authorised_by_user_id,
      'authorised_at_utc',v_live.authorised_at_utc,
      -- ROUND 5 Part E: the five-field signature binding, and the digest
      -- REBUILT from the row's own bindings so W9 can compare the two.  A row
      -- written before the binding existed carries a null stored digest and is
      -- refused by name; it never passes by accident.
      'agency_id',v_live.agency_id,
      'protected_decision_hashes',pg_catalog.to_jsonb(
        coalesce(v_live.protected_decision_hashes,array[]::text[])),
      'decision_digest',case when v_live.decision_digest is null then null
                        else pg_catalog.encode(v_live.decision_digest,'hex') end,
      'decision_digest_rebuilt',pg_catalog.encode(
        private.weekly_source_publication_request_digest_v1(
          private.weekly_source_root_authorisation_signature_v1(
            v_live.agency_id,v_live.root_timesheet_id,v_live.family_booking_id,
            v_live.timesheet_version,v_live.authorisation_generation,
            v_live.authorised_row_signature,
            coalesce(v_live.protected_decision_hashes,array[]::text[]))),
        'hex')) end
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- proof/36 section 4: the nine server checks, family-wide and read-only.
--
-- Every check is evaluated; none short-circuits another, because the screens
-- need the whole verdict and because a temporary refusal must never hide a
-- permanent one.  The refusal returned is chosen by severity, not by evaluation
-- or sort order: W9's root-identity failure first, then any permanent refusal,
-- then an integrity failure raised by the census (W3's CENSUS_ERROR class),
-- then any temporary refusal, and within one severity the lowest check number.
-- The block comment above the `order by` below states the same order and the
-- reason for it; the two are kept in step deliberately (WP-07b, finding F7).
--
-- W3's item classes come from interface I-2 (the freeze census); its ACTIVE
-- class is a temporary refusal and its CENSUS_ERROR class is
-- WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE (HANDOVER 2 round 4 ruling 3 changes the
-- census's own escalation timing and reason code, not this mapping).
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_first_authorisation_withdraw_checks_v1(
  p_context jsonb,
  p_census jsonb,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_members uuid[];
  v_candidate_id uuid;
  v_family text;
  v_canonical uuid;
  v_authorisation jsonb;
  v_first_authorised_at timestamptz;
  v_transfer_scope jsonb;
  v_invoice_ids uuid[];
  v_now timestamptz:=pg_catalog.clock_timestamp();

  v_checks jsonb:='[]'::jsonb;
  v_reasons text[];

  v_w1 boolean; v_w2 boolean; v_w3 boolean; v_w4 boolean; v_w5 boolean;
  v_w6 boolean; v_w7 boolean; v_w8 boolean; v_w9 boolean;
  v_w10 boolean; v_w11 boolean;
  v_w1_reasons text[]:=array[]::text[];
  v_w2_reasons text[]:=array[]::text[];
  v_w3_reasons text[]:=array[]::text[];
  v_w4_reasons text[]:=array[]::text[];
  v_w5_reasons text[]:=array[]::text[];
  v_w6_reasons text[]:=array[]::text[];
  v_w7_reasons text[]:=array[]::text[];
  v_w8_reasons text[]:=array[]::text[];
  v_w9_reasons text[]:=array[]::text[];
  v_w10_reasons text[]:=array[]::text[];
  v_w11_reasons text[]:=array[]::text[];
  v_w2_permanent boolean:=false;
  v_w6_permanent boolean:=false;
  v_w10_permanent boolean:=false;
  v_w4_write_off boolean:=false;

  v_n bigint;
  v_n2 bigint;
  v_n3 bigint;
  v_n4 bigint;
  v_head_count bigint;
  v_head_staged bigint;
  v_head_current bigint;
  v_head_revision bigint;
  v_head_id uuid;
  v_current_head public.weekly_source_entitlement_heads%rowtype;
  v_family_items uuid[];
  v_code text;
  v_nature text;
  v_message text;
begin
  if coalesce((p_context->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,'available',false,
      'code',coalesce(p_context->>'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'),
      'refusal_nature','INTEGRITY','retryable',false,
      'reason',p_context->>'reason','checks','[]'::jsonb);
  end if;

  select pg_catalog.array_agg(member_element.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(p_context->'member_timesheet_ids')
    as member_element(value);
  v_members:=coalesce(v_members,array[]::uuid[]);
  v_candidate_id:=(p_context->>'candidate_id')::uuid;
  v_family:=p_context->>'family_booking_id';
  v_canonical:=(p_context->>'canonical_timesheet_id')::uuid;
  v_authorisation:=case when pg_catalog.jsonb_typeof(coalesce(p_context->'authorisation','null'::jsonb))='object'
                        then p_context->'authorisation' end;

  if v_authorisation is null then
    -- Declared addition D1: not a Weekly-Source-managed root, so this owner is
    -- not the route (proof/36 section 3).  Permanent, and never one of the nine.
    return pg_catalog.jsonb_build_object(
      'ok',false,'available',false,
      'code','WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
      'refusal_nature','PERMANENT','retryable',false,
      'refusal_message','This Timesheet is not a Weekly-Source-managed root, so '
        ||'this route does not apply to it. Use the ordinary Unauthorise control.',
      'reason','NO_LIVE_ROOT_AUTHORISATION','checks','[]'::jsonb);
  end if;

  -- The moment the first authorisation happened.  least() ignores nulls, so the
  -- earliest evidence wins and a later decision is never missed.
  v_first_authorised_at:=least(
    (v_authorisation->>'authorised_at_utc')::timestamptz,
    (p_context#>>'{root,authorised_at_server}')::timestamptz);

  v_transfer_scope:=private.weekly_source_withdrawal_transfer_scope_v1(
    v_candidate_id,v_members);

  select coalesce(pg_catalog.array_agg(distinct invoice_id.id),array[]::uuid[])
    into v_invoice_ids
  from (
    select line_row.invoice_id as id
    from public.invoice_lines as line_row
    where line_row.timesheet_id=any(v_members)
      and line_row.invoice_id is not null
    union
    select binding_row.invoice_id
    from public.weekly_source_invoice_line_bindings as binding_row
    join public.weekly_source_billing_movements as movement_row
      on movement_row.id=binding_row.billing_movement_id
    where movement_row.invoice_timesheet_id=any(v_members)
      and binding_row.invoice_id is not null
  ) as invoice_id;

  -- ---------------- W1 -----------------------------------------------------
  -- First authorisation is the only authorisation event; no later Weekly Source
  -- decision depends on the root.
  select pg_catalog.count(*) into v_n
  from public.weekly_exceptional_payment_approvals as approval_row
  join public.weekly_exceptional_pay_target_families as family_row
    on family_row.id=approval_row.pay_target_family_id
  where family_row.root_timesheet_id=any(v_members)
    and approval_row.withdrawn_at_utc is null
    and v_first_authorised_at is not null
    and approval_row.approved_at_utc>v_first_authorised_at;
  if v_n>0 then
    v_w1_reasons:=v_w1_reasons||'LATER_PROTECTED_HOURS_APPROVAL'::text;
  end if;

  -- "no current entitlement head beyond the first" (24 section 4).
  --
  -- ROUND 5 SECTION A3 REPLACED WP-07b's RULE HERE.  WP-07b made ANY head that
  -- had ever been committed for the family a PERMANENT W1 refusal (its declared
  -- addition D5).  The finance approver rejected that as a product rule:
  --
  --   "Option (a) is rejected as a permanent product rule ... A user must not
  --    become permanently unable to withdraw an authorisation merely because
  --    its entitlement head has been committed."
  --
  -- What W1 now does with heads:
  --
  --   * a head that is COMMITTED_CURRENT for this family is NOT a W1 refusal.
  --     It is a head to be SUPERSEDED atomically with the withdrawal, and W11
  --     below decides whether that is safe to do.  W2 to W10 decide whether any
  --     money work has crossed the boundary; if any of them refuses, nothing is
  --     superseded and nothing is written.
  --   * a head that has been committed and is already SUPERSEDED is history.
  --     It never refused before WP-07b and it does not refuse now.
  --   * a STAGED head for the family still refuses here, unchanged: the
  --     coordinator stages and activates in one transaction, so a STAGED row
  --     seen under these locks is either a concurrent publication or a residue,
  --     and neither is a safe state to withdraw in.
  --
  -- The wrong-payment path WP-07b found is closed by the supersession itself,
  -- not by the refusal.  The Gate 4 selector
  -- `private.pay_workbench_unit_economic_occurrence_page_v1` resolves the head
  -- by physical root and COMMITTED_CURRENT state and deliberately never reads
  -- this authorisation record; after this owner runs there IS no
  -- COMMITTED_CURRENT head for the root, so the selector cannot pay from the
  -- pre-withdrawal head.  The selector is unchanged, exactly as ruling A3
  -- requires.
  --
  -- proof/36 section 5.6's "clears `current_entitlement_head_id`" is reachable
  -- again: a root that carries a head is now withdrawable, so the owner really
  -- does clear the pointer, in the same statement as the withdrawal marks
  -- (decision D8 and the relation's own check constraint).
  v_head_id:=(v_authorisation->>'current_entitlement_head_id')::uuid;
  select pg_catalog.count(*) filter (where head_row.committed_at_utc is not null),
         pg_catalog.count(*) filter (where head_row.state='STAGED'),
         pg_catalog.count(*) filter (where head_row.state='COMMITTED_CURRENT'),
         pg_catalog.max(head_row.head_revision)
    into v_head_count, v_head_staged, v_head_current, v_head_revision
  from public.weekly_source_entitlement_heads as head_row
  where pg_catalog.btrim(head_row.root_family_booking_id)=pg_catalog.btrim(v_family)
     or head_row.root_timesheet_id=any(v_members);
  if v_head_staged>0 then
    v_w1_reasons:=v_w1_reasons||'ENTITLEMENT_HEAD_STAGED_FOR_THE_ROOT'::text;
  end if;
  -- A pointer that names no live committed head of this family is an integrity
  -- fault in its own right and still fails closed.
  if v_head_id is not null
     and not exists (
       select 1 from public.weekly_source_entitlement_heads as pointed_head
       where pointed_head.id=v_head_id
         and pointed_head.state='COMMITTED_CURRENT'
         and pg_catalog.btrim(pointed_head.root_family_booking_id)
             =pg_catalog.btrim(v_family)) then
    v_w1_reasons:=v_w1_reasons||'ENTITLEMENT_HEAD_POINTER_INVALID'::text;
  end if;

  select pg_catalog.count(*) filter (where bundle_row.state='PROPOSED'),
         pg_catalog.count(*) filter (where bundle_row.state='COMMITTED')
    into v_n, v_n2
  from public.weekly_source_entitlement_decision_bundles as bundle_row
  where pg_catalog.btrim(bundle_row.source_root_family_booking_id)=pg_catalog.btrim(v_family)
     or pg_catalog.btrim(coalesce(bundle_row.target_root_family_booking_id,''))
        =pg_catalog.btrim(v_family);
  if v_n>0 then
    v_w1_reasons:=v_w1_reasons||'ACCEPTED_OR_PROPOSED_LATER_DECISION'::text;
  end if;
  if v_n2>1 then
    v_w1_reasons:=v_w1_reasons||'MORE_THAN_ONE_COMMITTED_DECISION_BUNDLE'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.weekly_source_pending_entitlement_bundles as pending_row
  where pending_row.state in ('PENDING','RELEASING','MANUAL_REVIEW')
    and (
      exists (select 1 from pg_catalog.unnest(pending_row.member_root_ids) as root_id(value)
              where root_id.value=any(v_members))
      or exists (select 1 from pg_catalog.unnest(pending_row.member_family_booking_ids)
                   as booking_id(value)
                 where pg_catalog.btrim(booking_id.value)=pg_catalog.btrim(v_family))
    );
  if v_n>0 then
    v_w1_reasons:=v_w1_reasons||'PENDING_ENTITLEMENT_BUNDLE_NAMES_THE_ROOT'::text;
  end if;
  v_w1:=coalesce(pg_catalog.array_length(v_w1_reasons,1),0)=0;

  -- ---------------- W2 -----------------------------------------------------
  -- No Candidate money has moved or may be moving.
  select pg_catalog.count(*) into v_n
  from public.timesheet_pay_state_history as history_row
  where history_row.timesheet_id=any(v_members);
  if v_n>0 then
    v_w2_reasons:=v_w2_reasons||'SETTLEMENT_HISTORY_EXISTS'::text;
    v_w2_permanent:=true;
  end if;

  select pg_catalog.count(*) into v_n
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row
    on candidate_row.id=item_row.pay_batch_candidate_id
  where item_row.timesheet_id=any(v_members)
    and candidate_row.settlement_status='SETTLED';
  if v_n>0 then
    v_w2_reasons:=v_w2_reasons||'CANDIDATE_ROW_SETTLED'::text;
    v_w2_permanent:=true;
  end if;

  select pg_catalog.count(*) into v_n
  from public.timesheets_financials as financial_row
  where financial_row.timesheet_id=any(v_members)
    and financial_row.paid_at_utc is not null;
  if v_n>0 then
    v_w2_reasons:=v_w2_reasons||'TIMESHEET_FINANCIALS_PAID'::text;
    v_w2_permanent:=true;
  end if;

  select
    pg_catalog.count(*) filter (
      where (transfer_element.value->>'is_final_money_moved')::boolean),
    pg_catalog.count(*) filter (
      where (transfer_element.value->>'is_pending_non_final')::boolean
         or (transfer_element.value->>'is_ambiguous')::boolean),
    pg_catalog.count(*) filter (
      where (transfer_element.value->>'is_terminal_no_money')::boolean
        and not exists (
          select 1
          from pg_catalog.jsonb_array_elements(coalesce(p_census->'items','[]'::jsonb))
            as census_item(value)
          where census_item.value->>'pay_batch_item_id'
                =transfer_element.value->>'pay_batch_item_id'
            and census_item.value->>'class'='VOIDED_TERMINAL'
            and census_item.value->>'binding' in ('A','B')))
    into v_n, v_n2, v_n3
  from pg_catalog.jsonb_array_elements(coalesce(v_transfer_scope->'transfers','[]'::jsonb))
    as transfer_element(value);
  if v_n>0 then
    v_w2_reasons:=v_w2_reasons||'TRANSFER_FINAL_MONEY_MOVED'::text;
    v_w2_permanent:=true;
  end if;
  if v_n2>0 then
    v_w2_reasons:=v_w2_reasons||'TRANSFER_PENDING_OR_UNKNOWN'::text;
  end if;
  if v_n3>0 then
    v_w2_reasons:=v_w2_reasons||'TERMINAL_NO_MONEY_TRANSFER_WITHOUT_BINDING_A_OR_B'::text;
  end if;
  if pg_catalog.jsonb_array_length(coalesce(v_transfer_scope->'errors','[]'::jsonb))>0 then
    v_w2_reasons:=v_w2_reasons||'TRANSFER_SCOPE_CONTRADICTORY_OR_UNJOINABLE'::text;
  end if;
  v_w2:=coalesce(pg_catalog.array_length(v_w2_reasons,1),0)=0;

  -- ---------------- W3 -----------------------------------------------------
  -- Not in a Banking Pay Draft, and every earlier void proved.
  select pg_catalog.count(*) into v_n
  from public.pay_batch_items as item_row
  where item_row.timesheet_id=any(v_members)
    and item_row.is_voided is distinct from true;
  if v_n>0 then
    v_w3_reasons:=v_w3_reasons||'NON_VOIDED_FAMILY_ITEM'::text;
  end if;

  select
    pg_catalog.count(*) filter (where census_item.value->>'class'='ACTIVE'),
    pg_catalog.count(*) filter (where census_item.value->>'class'='CENSUS_ERROR')
    into v_n, v_n2
  from pg_catalog.jsonb_array_elements(coalesce(p_census->'items','[]'::jsonb))
    as census_item(value);
  if v_n>0 then
    v_w3_reasons:=v_w3_reasons||'CENSUS_ACTIVE_ITEM'::text;
  end if;
  if v_n2>0
     or coalesce(p_census->>'result','CENSUS_ERROR')='CENSUS_ERROR'
     or pg_catalog.jsonb_array_length(coalesce(p_census->'errors','[]'::jsonb))>0 then
    v_w3_reasons:=v_w3_reasons||'CENSUS_ERROR'::text;
  end if;
  v_w3:=coalesce(pg_catalog.array_length(v_w3_reasons,1),0)=0;

  -- ---------------- W4 -----------------------------------------------------
  -- Not reserved or scheduled.
  select pg_catalog.count(*) into v_n
  from public.pay_advance_reservations as reservation_row
  join public.pay_batch_items as item_row on item_row.id=reservation_row.pay_batch_item_id
  where item_row.timesheet_id=any(v_members)
    and reservation_row.status in ('RESERVED','COMMITTED');
  if v_n>0 then
    v_w4_reasons:=v_w4_reasons||'ACTIVE_RESERVATION'::text;
  end if;

  -- DECLARED ADDITION D4 (see the header block; WP-07b finding F5).  This is
  -- STRICTER than proof/36 section 4 W4, whose literal predicate is
  -- `status IN ('RESERVED','COMMITTED')`, and stricter than HANDOVER 2 round 4
  -- ruling 5 item 5, which says a reservation released with reason WRITE_OFF
  -- "must not satisfy the positive reservation evidence required by a binding" —
  -- an evidence rule, which W6 below implements exactly, not a refusal in its
  -- own right.  It is kept as an outright W4 refusal because ruling 5 item 5
  -- also records `pay_finance_case_write_off` as no Binding A/B/C release
  -- authority at all, with a finance-case-wide predicate and no batch-state
  -- guard, so an item whose reservation it released has no proved disposition.
  -- Fail-closed: it can refuse a withdrawal the pack would permit, and can
  -- never cause a wrong payment.  Reversing it is deleting this one block.
  select pg_catalog.count(*) into v_n
  from public.pay_advance_reservations as reservation_row
  join public.pay_batch_items as item_row on item_row.id=reservation_row.pay_batch_item_id
  where item_row.timesheet_id=any(v_members)
    and reservation_row.status='RELEASED'
    and pg_catalog.upper(pg_catalog.btrim(coalesce(reservation_row.released_reason,'')))
        ='WRITE_OFF';
  if v_n>0 then
    v_w4_reasons:=v_w4_reasons||'RESERVATION_RELEASED_BY_WRITE_OFF'::text;
    -- ROUND 5 SECTION A5, the wording requirement: this refusal gets its own
    -- code and its own message, which "identify the finance-case write-off as
    -- unresolved Banking Pay evidence and route it to review" and never call it
    -- paid, settled or safely cancelled.  Nature INTEGRITY, because INTEGRITY is
    -- this package's existing "needs Office review before it can be changed"
    -- disposition: it is not retryable, and it is not the permanent "the control
    -- is never shown again" of proof/36 section 6 either, because ruling A5 says
    -- the position holds only "until the later Banking Pay cancellation/security
    -- reconciliation assigns an authoritative disposition".
    v_w4_write_off:=true;
  end if;

  select pg_catalog.count(distinct batch_row.id) into v_n
  from public.pay_batches as batch_row
  where exists (
      select 1
      from public.pay_batch_items as item_row
      join public.pay_batch_candidates as candidate_row
        on candidate_row.id=item_row.pay_batch_candidate_id
      where candidate_row.pay_batch_id=batch_row.id
        and item_row.timesheet_id=any(v_members)
        and item_row.is_voided=false)
    and not (
      batch_row.status in ('SETTLED','CANCELLED')
      or (batch_row.status='FAILED' and batch_row.completed_at_utc is not null));
  if v_n>0 then
    v_w4_reasons:=v_w4_reasons||'NON_TERMINAL_BATCH_HOLDS_A_LIVE_FAMILY_ITEM'::text;
  end if;
  v_w4:=coalesce(pg_catalog.array_length(v_w4_reasons,1),0)=0;

  -- ---------------- W5 -----------------------------------------------------
  -- Payment execution has not started.
  select pg_catalog.count(distinct batch_row.id) into v_n
  from public.pay_batches as batch_row
  where exists (
      select 1
      from public.pay_batch_items as item_row
      join public.pay_batch_candidates as candidate_row
        on candidate_row.id=item_row.pay_batch_candidate_id
      where candidate_row.pay_batch_id=batch_row.id
        and item_row.timesheet_id=any(v_members)
        and item_row.is_voided=false)
    and (
      batch_row.status in ('EXECUTING','SCHEDULED','READY','AUTHORISED_FOR_PAYMENT',
                           'AWAITING_AUTHORISATION','WAITING_BANK_CONFIRM',
                           'BLOCKED_FUNDS','PARTIAL')
      or batch_row.execution_commit_state is distinct from 'NOT_SUBMITTED');
  if v_n>0 then
    v_w5_reasons:=v_w5_reasons||'EXECUTION_STARTED_OR_COMMITTED'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.banking_pay_operations as operation_row
  where operation_row.pay_batch_id in (
      select candidate_row.pay_batch_id
      from public.pay_batch_items as item_row
      join public.pay_batch_candidates as candidate_row
        on candidate_row.id=item_row.pay_batch_candidate_id
      where item_row.timesheet_id=any(v_members)
        and item_row.is_voided=false)
    and (
      operation_row.status in ('QUEUED','RUNNING','WAITING','WAITING_AUTHORISATION',
                               'WAITING_PROVIDER','REVIEW_REQUIRED')
      or operation_row.lease_expires_at_utc>v_now
      or operation_row.lock_expires_at_utc>v_now);
  if v_n>0 then
    v_w5_reasons:=v_w5_reasons||'LIVE_BANKING_PAY_OPERATION'::text;
  end if;
  v_w5:=coalesce(pg_catalog.array_length(v_w5_reasons,1),0)=0;

  -- ---------------- W6 -----------------------------------------------------
  -- No recovery, and no correction other than a completed no-money Draft
  -- cancellation.
  select pg_catalog.count(*) into v_n
  from public.ts_pay_adjustments as adjustment_row
  where adjustment_row.timesheet_id=any(v_members)
    and (coalesce(adjustment_row.as_advance,false)=true
         or adjustment_row.advance_reason is not null
         or coalesce(adjustment_row.delta_pay_ex_vat,0)<0
         or adjustment_row.paid_at_utc is not null);
  if v_n>0 then
    v_w6_reasons:=v_w6_reasons||'RECOVERY_OR_OVERPAYMENT_ADJUSTMENT'::text;
    v_w6_permanent:=true;
  end if;

  with family_item as (
    select item_row.id as pay_batch_item_id,
           candidate_row.pay_batch_id,
           item_row.pay_bank_transfer_id
    from public.pay_batch_items as item_row
    left join public.pay_batch_candidates as candidate_row
      on candidate_row.id=item_row.pay_batch_candidate_id
    where item_row.timesheet_id=any(v_members)
  ),
  touching_request as (
    select distinct request_row.id, request_row.status, request_row.correction_kind,
           request_row.pay_batch_id
    from public.pay_payment_correction_requests as request_row
    where exists (select 1 from family_item
                  where family_item.pay_batch_id=request_row.pay_batch_id)
       or exists (
            select 1
            from public.pay_payment_correction_request_candidates as request_candidate
            join family_item
              on family_item.pay_batch_item_id=any(request_candidate.pay_batch_item_ids)
            where request_candidate.correction_request_id=request_row.id)
  )
  select
    pg_catalog.count(*) filter (
      where touching_request.status not in
            ('APPLIED','APPLIED_WITH_BLOCKERS','FAILED','REJECTED','CANCELLED')),
    pg_catalog.count(*) filter (
      where touching_request.correction_kind
            not in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND')),
    pg_catalog.count(*) filter (
      where touching_request.status='APPLIED_WITH_BLOCKERS'
        and exists (
          -- proof/32 section 5.1 Binding A: a blocker is a work item of the same
          -- request that did not apply; its identity comes from its own
          -- selection_json or from the request's candidate membership row.
          select 1
          from public.pay_payment_correction_work_items as blocker_row
          left join public.pay_payment_correction_request_candidates as blocker_candidate
            on blocker_candidate.correction_request_id=touching_request.id
           and blocker_candidate.pay_batch_candidate_id=blocker_row.pay_batch_candidate_id
          where blocker_row.correction_request_id=touching_request.id
            and coalesce(blocker_row.status,'') not in ('APPLIED','SKIPPED')
            and (
              coalesce(pg_catalog.cardinality(
                case when pg_catalog.jsonb_typeof(
                          blocker_row.selection_json->'pay_batch_item_ids')='array'
                     then private.weekly_source_withdrawal_uuid_array_v1(
                            blocker_row.selection_json->'pay_batch_item_ids')
                     when pg_catalog.jsonb_typeof(
                          blocker_row.selection_json->'expected_pay_batch_item_ids')='array'
                     then private.weekly_source_withdrawal_uuid_array_v1(
                            blocker_row.selection_json->'expected_pay_batch_item_ids')
                     else coalesce(blocker_candidate.pay_batch_item_ids,array[]::uuid[])
                end),0)=0
              or exists (
                select 1 from family_item
                where family_item.pay_batch_item_id=any(
                  case when pg_catalog.jsonb_typeof(
                            blocker_row.selection_json->'pay_batch_item_ids')='array'
                       then private.weekly_source_withdrawal_uuid_array_v1(
                              blocker_row.selection_json->'pay_batch_item_ids')
                       when pg_catalog.jsonb_typeof(
                            blocker_row.selection_json->'expected_pay_batch_item_ids')='array'
                       then private.weekly_source_withdrawal_uuid_array_v1(
                              blocker_row.selection_json->'expected_pay_batch_item_ids')
                       else coalesce(blocker_candidate.pay_batch_item_ids,array[]::uuid[])
                  end))
            ))),
    pg_catalog.count(*) filter (
      where touching_request.status in ('APPLIED','APPLIED_WITH_BLOCKERS')
        and exists (
          select 1
          from public.banking_pay_operations as operation_row
          where operation_row.operation_type='PAYMENT_CORRECTION'
            and operation_row.input_json->>'correction_request_id'
                =touching_request.id::text
            and (operation_row.status is distinct from 'COMPLETE'
                 or operation_row.phase is distinct from 'COMPLETE'
                 or operation_row.lease_expires_at_utc>v_now
                 or operation_row.lock_expires_at_utc>v_now)))
    into v_n, v_n2, v_n3, v_head_count
  from touching_request;

  if v_n>0 then
    v_w6_reasons:=v_w6_reasons||'NON_TERMINAL_CORRECTION_REQUEST'::text;
  end if;
  if v_n2>0 then
    v_w6_reasons:=v_w6_reasons||'CORRECTION_KIND_NOT_A_NO_MONEY_CANCELLATION'::text;
    v_w6_permanent:=true;
  end if;
  if v_n3>0 then
    v_w6_reasons:=v_w6_reasons||'BLOCKER_NAMES_FAMILY_ITEM_OR_CARRIES_NO_ITEM_IDENTITY'::text;
    v_w6_permanent:=true;
  end if;
  if v_head_count>0 then
    v_w6_reasons:=v_w6_reasons||'CORRECTION_OPERATION_NOT_TERMINAL'::text;
  end if;

  -- The terminal request is permitted only when every family item it touched is
  -- VOIDED_TERMINAL under Binding A (or its batch under Binding B), every
  -- reservation is RELEASED (not by write-off) or SETTLED, and every bound
  -- transfer is terminal-no-money.
  select pg_catalog.count(*) into v_n
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row
    on candidate_row.id=item_row.pay_batch_candidate_id
  where item_row.timesheet_id=any(v_members)
    and exists (
      select 1 from public.pay_payment_correction_requests as request_row
      where request_row.pay_batch_id=candidate_row.pay_batch_id)
    and not exists (
      select 1
      from pg_catalog.jsonb_array_elements(coalesce(p_census->'items','[]'::jsonb))
        as census_item(value)
      where (census_item.value->>'pay_batch_item_id')::uuid=item_row.id
        and census_item.value->>'class'='VOIDED_TERMINAL'
        and census_item.value->>'binding' in ('A','B'));
  if v_n>0 then
    v_w6_reasons:=v_w6_reasons||'CORRECTION_TOUCHED_ITEM_NOT_YET_VOIDED_TERMINAL'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.pay_advance_reservations as reservation_row
  join public.pay_batch_items as item_row on item_row.id=reservation_row.pay_batch_item_id
  where item_row.timesheet_id=any(v_members)
    and (reservation_row.status not in ('RELEASED','SETTLED')
         or (reservation_row.status='RELEASED'
             and pg_catalog.upper(pg_catalog.btrim(coalesce(reservation_row.released_reason,'')))
                 ='WRITE_OFF'))
    and exists (
      select 1
      from public.pay_batch_candidates as candidate_row
      join public.pay_payment_correction_requests as request_row
        on request_row.pay_batch_id=candidate_row.pay_batch_id
      where candidate_row.id=item_row.pay_batch_candidate_id);
  if v_n>0 then
    v_w6_reasons:=v_w6_reasons||'CORRECTION_TOUCHED_RESERVATION_NOT_TERMINAL'::text;
  end if;
  v_w6:=coalesce(pg_catalog.array_length(v_w6_reasons,1),0)=0;

  -- ---------------- W7 -----------------------------------------------------
  -- No invoice line exists, including on an unissued (DRAFT) invoice.
  select pg_catalog.count(*) into v_n
  from public.invoice_lines as line_row
  where line_row.timesheet_id=any(v_members);
  if v_n>0 then
    v_w7_reasons:=v_w7_reasons||'INVOICE_LINE_EXISTS'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.weekly_source_invoice_line_bindings as binding_row
  join public.weekly_source_billing_movements as movement_row
    on movement_row.id=binding_row.billing_movement_id
  where movement_row.invoice_timesheet_id=any(v_members)
    and binding_row.state='CURRENT';
  if v_n>0 then
    v_w7_reasons:=v_w7_reasons||'CURRENT_SOURCE_INVOICE_BINDING'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.timesheets_financials as financial_row
  where financial_row.timesheet_id=any(v_members)
    and financial_row.locked_by_invoice_id is not null;
  if v_n>0 then
    v_w7_reasons:=v_w7_reasons||'LOCKED_BY_INVOICE'::text;
  end if;

  -- The same segment scan the unchanged owner's TIMESHEET_LOCKED_BY_INVOICE
  -- guard performs, re-run family-wide under the lock.
  select pg_catalog.count(*) into v_n
  from public.timesheets_financials as financial_row
  cross join lateral pg_catalog.jsonb_array_elements(
    case
      when financial_row.invoice_breakdown_json is null then '[]'::jsonb
      when pg_catalog.jsonb_typeof(financial_row.invoice_breakdown_json)='array'
        then financial_row.invoice_breakdown_json
      when pg_catalog.jsonb_typeof(financial_row.invoice_breakdown_json)='object'
       and pg_catalog.jsonb_typeof(financial_row.invoice_breakdown_json->'segments')='array'
        then financial_row.invoice_breakdown_json->'segments'
      else '[]'::jsonb
    end) as segment(value)
  where financial_row.timesheet_id=any(v_members)
    and nullif(pg_catalog.btrim(coalesce(
          segment.value->>'invoice_locked_invoice_id','')),'') is not null;
  if v_n>0 then
    v_w7_reasons:=v_w7_reasons||'INVOICE_LOCKED_SEGMENT'::text;
  end if;
  v_w7:=coalesce(pg_catalog.array_length(v_w7_reasons,1),0)=0;

  -- ---------------- W8 -----------------------------------------------------
  -- No invoice generation or issue operation is using it.
  select pg_catalog.count(*) into v_n
  from public.invoice_operations as operation_row
  where operation_row.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and (
      operation_row.entity_id=any(v_invoice_ids)
      or exists (select 1 from pg_catalog.unnest(v_invoice_ids) as invoice_id(value)
                 where operation_row.input_json->>'invoice_id'=invoice_id.value::text
                    or coalesce(operation_row.input_json->'invoice_ids','[]'::jsonb)
                       @> pg_catalog.to_jsonb(invoice_id.value::text))
      or exists (select 1 from pg_catalog.unnest(v_members) as member_id(value)
                 where operation_row.input_json->>'timesheet_id'=member_id.value::text
                    or coalesce(operation_row.input_json->'timesheet_ids','[]'::jsonb)
                       @> pg_catalog.to_jsonb(member_id.value::text)));
  if v_n>0 then
    v_w8_reasons:=v_w8_reasons||'LIVE_INVOICE_OPERATION'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.invoice_pdf_outbox as outbox_row
  where outbox_row.invoice_id=any(v_invoice_ids);
  if v_n>0 then
    v_w8_reasons:=v_w8_reasons||'QUEUED_INVOICE_PDF_OUTBOX'::text;
  end if;

  select pg_catalog.count(*) into v_n
  from public.invoice_jobs_outbox as outbox_row
  where exists (select 1 from pg_catalog.unnest(v_invoice_ids) as invoice_id(value)
                where outbox_row.payload->>'invoice_id'=invoice_id.value::text
                   or coalesce(outbox_row.payload->'invoice_ids','[]'::jsonb)
                      @> pg_catalog.to_jsonb(invoice_id.value::text));
  if v_n>0 then
    v_w8_reasons:=v_w8_reasons||'QUEUED_INVOICE_JOB_OUTBOX'::text;
  end if;
  v_w8:=coalesce(pg_catalog.array_length(v_w8_reasons,1),0)=0;

  -- ---------------- W9 -----------------------------------------------------
  -- Root identity (proof/34 section 7).
  if coalesce((p_context->>'requested_is_canonical')::boolean,false) is not true then
    v_w9_reasons:=v_w9_reasons||'REQUESTED_ROW_NOT_CANONICAL'::text;
  end if;
  if coalesce((p_context->>'family_is_current')::boolean,false) is not true then
    v_w9_reasons:=v_w9_reasons||'REQUESTED_ROW_NOT_CURRENT'::text;
  end if;
  if (v_authorisation->>'timesheet_version')::integer
     is distinct from (p_context->>'canonical_version')::integer then
    v_w9_reasons:=v_w9_reasons||'STORED_VERSION_CHANGED'::text;
  end if;
  if pg_catalog.btrim(coalesce(v_authorisation->>'family_booking_id',''))
     is distinct from pg_catalog.btrim(coalesce(v_family,'')) then
    v_w9_reasons:=v_w9_reasons||'STORED_FAMILY_CHANGED'::text;
  end if;
  if (p_context#>>'{root,authorised_at_server}') is null then
    v_w9_reasons:=v_w9_reasons||'ROOT_NOT_AUTHORISED'::text;
  end if;
  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_canonical then
    v_w9_reasons:=v_w9_reasons||'EXPECTED_TIMESHEET_MISMATCH'::text;
  end if;
  -- ROUND 5, Part E rotation readings: the authorisation row signature must
  -- bind tenant and agency, the canonical root, the exact authorised physical
  -- Timesheet identity, the source generation and revision, and the canonical
  -- digest of the protected decision fields.  W9 is where the stored binding is
  -- PROVED rather than assumed, by rebuilding it from the row's own five
  -- bindings and comparing.
  --
  -- Both failures are named, and neither can pass silently:
  --   a row written before the binding existed carries a NULL digest, which is
  --   MISSING, not "no objection";
  --   a row whose stored digest no longer rebuilds has had one of the five
  --   bindings changed underneath it, which is a MISMATCH.
  -- Either way the withdrawal refuses with WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE
  -- and writes nothing.
  if (v_authorisation->>'decision_digest') is null then
    v_w9_reasons:=v_w9_reasons||'ROOT_AUTHORISATION_DECISION_DIGEST_MISSING'::text;
  elsif pg_catalog.lower(v_authorisation->>'decision_digest')
        is distinct from pg_catalog.lower(coalesce(
          v_authorisation->>'decision_digest_rebuilt','')) then
    v_w9_reasons:=v_w9_reasons||'ROOT_AUTHORISATION_DECISION_DIGEST_MISMATCH'::text;
  end if;
  if p_expected_row_signature is not null then
    if nullif(pg_catalog.btrim(p_expected_row_signature),'') is null then
      v_w9_reasons:=v_w9_reasons||'EXPECTED_ROW_SIGNATURE_BLANK'::text;
    elsif (p_context->>'current_row_signature') is null then
      v_w9_reasons:=v_w9_reasons||'ROOT_SIGNATURE_UNAVAILABLE'::text;
    elsif pg_catalog.btrim(p_expected_row_signature)
          is distinct from (p_context->>'current_row_signature') then
      v_w9_reasons:=v_w9_reasons||'ROW_SIGNATURE_MISMATCH'::text;
    end if;
  end if;
  v_w9:=coalesce(pg_catalog.array_length(v_w9_reasons,1),0)=0;

  -- ---------------- W10 (declared addition D6) -----------------------------
  -- ROUND 5 SECTION A3: "it must prove there is no active or published Draft
  -- dependency, payment item, reservation, bank transfer, execution/provider
  -- attempt, settlement, remittance or other committed financial effect against
  -- the head/root.  A missing or ambiguous result refuses."
  --
  -- proof/36's nine checks already cover the Draft dependency (W3), the payment
  -- item (W3), the reservation (W4), the bank transfer (W2, through the
  -- installed classifier), the execution (W5), the settlement history (W2) and
  -- the recovery/correction (W6).  W10 adds the four the ruling names that
  -- proof/36 did not: a PROVIDER ATTEMPT, a TRANSFER EVENT, a REMITTANCE and a
  -- settlement-scope row, plus an advance standing against the root.
  --
  -- Scope discipline, proof/32 section 4.3 C5: every predicate below is bound to
  -- a family PAY ITEM or to the Candidate row that owns one, never to a batch at
  -- large.  Another Candidate's provider attempt in the same batch is never this
  -- root's evidence.
  --
  -- Interpretation discipline, round 4 ruling 4: Weekly Source reads Banking Pay
  -- evidence and never reinterprets a marker.  The only status values acted on
  -- are the ones the installed CHECK constraints define, and the only value
  -- treated as "authoritatively nothing happened" is the explicit SKIPPED.
  select coalesce(pg_catalog.array_agg(item_row.id),array[]::uuid[])
    into v_family_items
  from public.pay_batch_items as item_row
  where item_row.timesheet_id=any(v_members);

  -- Provider attempt: an operation transfer scope that carries one of this
  -- family's pay items, and an attempt row against it.
  select pg_catalog.count(*) into v_n
  from public.banking_pay_operation_provider_attempts as attempt_row
  where exists (
    select 1
    from public.banking_pay_operation_transfer_scope_items as scope_item
    where scope_item.transfer_scope_id=attempt_row.transfer_scope_id
      and scope_item.pay_batch_item_id=any(v_family_items));
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'PROVIDER_ATTEMPT_AGAINST_A_FAMILY_ITEM'::text;
  end if;

  -- Provider submission recorded on the transfer scope itself, whether or not an
  -- attempt row survives.  Any one of the six provider marks is enough.
  select pg_catalog.count(*) into v_n
  from public.banking_pay_operation_transfer_scope as scope_row
  where exists (
      select 1
      from public.banking_pay_operation_transfer_scope_items as scope_item
      where scope_item.transfer_scope_id=scope_row.id
        and scope_item.pay_batch_item_id=any(v_family_items))
    and (scope_row.provider_request_prepared_at_utc is not null
         or scope_row.provider_request_sending_at_utc is not null
         or scope_row.provider_request_sent_at_utc is not null
         or scope_row.provider_response_at_utc is not null
         or scope_row.provider_transaction_id is not null
         or scope_row.provider_request_id is not null);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'PROVIDER_SUBMISSION_AGAINST_A_FAMILY_ITEM'::text;
  end if;

  -- Transfer event: the Banking Pay classifier's own terminal-no-money proof
  -- requires that NO transfer event exists, so one here contradicts it.
  select pg_catalog.count(*) into v_n
  from public.pay_bank_transfer_events as event_row
  where exists (
    select 1
    from public.pay_batch_items as item_row
    where item_row.id=any(v_family_items)
      and item_row.pay_bank_transfer_id=event_row.pay_bank_transfer_id);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'BANK_TRANSFER_EVENT_AGAINST_A_FAMILY_ITEM'::text;
  end if;

  -- Settlement scope.  SETTLED is money: permanent.  PENDING or FAILED is
  -- unresolved: temporary.  SKIPPED is the authoritative "no settlement for this
  -- row" and is not a refusal.
  select pg_catalog.count(*) filter (where scope_row.status='SETTLED'),
         pg_catalog.count(*) filter (where scope_row.status in ('PENDING','FAILED'))
    into v_n, v_n2
  from public.banking_pay_operation_settlement_scope as scope_row
  where exists (
    select 1
    from public.pay_batch_items as item_row
    where item_row.id=any(v_family_items)
      and item_row.pay_batch_candidate_id=scope_row.pay_batch_candidate_id);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'SETTLEMENT_SCOPE_SETTLED'::text;
    v_w10_permanent:=true;
  end if;
  if v_n2>0 then
    v_w10_reasons:=v_w10_reasons||'SETTLEMENT_SCOPE_UNRESOLVED'::text;
  end if;

  -- Remittance scope.  A remittance is an outward statement of payment; PENDING,
  -- QUEUED or FAILED all leave the position unresolved.  SKIPPED is not a
  -- refusal.
  select pg_catalog.count(*) into v_n
  from public.banking_pay_operation_remittance_scope as scope_row
  where scope_row.status in ('PENDING','QUEUED','FAILED')
    and exists (
      select 1
      from public.pay_batch_items as item_row
      where item_row.id=any(v_family_items)
        and item_row.pay_batch_candidate_id=scope_row.pay_batch_candidate_id);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'REMITTANCE_SCOPE_UNRESOLVED'::text;
  end if;

  -- An allocation row that names a family pay item is money apportioned to this
  -- root inside an operation.
  select pg_catalog.count(*) into v_n
  from public.banking_pay_operation_candidate_allocation_rows as allocation_row
  where allocation_row.pay_batch_item_id=any(v_family_items);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'OPERATION_ALLOCATION_AGAINST_A_FAMILY_ITEM'::text;
  end if;

  -- An advance standing against this physical Timesheet, or paid out through a
  -- batch that holds one of this family's items.  W6 covers ts_pay_adjustments;
  -- this covers the advance relation the ruling's "other committed financial
  -- effect against the root" names.  A cleared or written-off advance is still
  -- unresolved evidence here: see the write-off rule in W4 and ruling A5.
  select pg_catalog.count(*) into v_n
  from public.pay_advances as advance_row
  where advance_row.linked_timesheet_id=any(v_members)
     or exists (
       select 1
       from public.pay_batch_items as item_row
       join public.pay_batch_candidates as candidate_row
         on candidate_row.id=item_row.pay_batch_candidate_id
       where item_row.id=any(v_family_items)
         and candidate_row.pay_batch_id=advance_row.payout_pay_batch_id);
  if v_n>0 then
    v_w10_reasons:=v_w10_reasons||'ADVANCE_AGAINST_THE_ROOT'::text;
  end if;
  v_w10:=coalesce(pg_catalog.array_length(v_w10_reasons,1),0)=0;

  -- ---------------- W11 (declared addition D7) -----------------------------
  -- Head supersession safety.  This is the only check in this file whose
  -- failure means "this owner must not write a head row", and it is deliberately
  -- an INTEGRITY refusal rather than a money refusal: every shape it catches is
  -- a contradiction in the head relation itself.
  --
  -- ROUND 5, Part E, "Rotation readings": the authorisation row signature binds
  -- tenant/agency, canonical root, EXACT AUTHORISED PHYSICAL TIMESHEET IDENTITY,
  -- source generation/revision and the canonical digest of the protected
  -- decision fields; "booking reference alone is not a signature key" and "a
  -- decision must never migrate silently between physical Timesheet IDs".  So
  -- the head being retired must name the canonical root PHYSICALLY as well as by
  -- family, and must carry that row's version.
  --
  -- Rule 5 of the executed-review rules: cardinality is checked explicitly and
  -- never inferred from the unique index.
  if v_head_current>1 then
    v_w11_reasons:=v_w11_reasons||'MORE_THAN_ONE_COMMITTED_CURRENT_HEAD'::text;
  elsif v_head_current=1 then
    select * into v_current_head
    from public.weekly_source_entitlement_heads as head_row
    where head_row.state='COMMITTED_CURRENT'
      and (pg_catalog.btrim(head_row.root_family_booking_id)=pg_catalog.btrim(v_family)
           or head_row.root_timesheet_id=any(v_members));

    if v_current_head.id is null then
      v_w11_reasons:=v_w11_reasons||'COMMITTED_CURRENT_HEAD_UNREADABLE'::text;
    else
      if v_current_head.root_timesheet_id is distinct from v_canonical then
        v_w11_reasons:=v_w11_reasons||'HEAD_PHYSICAL_ROOT_IS_NOT_THE_CANONICAL_ROW'::text;
      end if;
      if pg_catalog.btrim(coalesce(v_current_head.root_family_booking_id,''))
         is distinct from pg_catalog.btrim(coalesce(v_family,'')) then
        v_w11_reasons:=v_w11_reasons||'HEAD_FAMILY_IS_NOT_THE_ROOT_FAMILY'::text;
      end if;
      if v_current_head.root_timesheet_version
         is distinct from (p_context->>'canonical_version')::integer then
        v_w11_reasons:=v_w11_reasons||'HEAD_VERSION_IS_NOT_THE_CANONICAL_VERSION'::text;
      end if;
      -- The live generation must be the one that owns this head.  A committed
      -- current head with no live generation pointing at it is exactly the
      -- orphan shape the WP-07 review proved pays from a stale head, so it
      -- refuses rather than being silently retired.
      if v_head_id is distinct from v_current_head.id then
        v_w11_reasons:=v_w11_reasons||'LIVE_GENERATION_DOES_NOT_OWN_THE_CURRENT_HEAD'::text;
      end if;
      -- Nothing may already be built on top of it.
      select pg_catalog.count(*) into v_n
      from public.weekly_source_entitlement_heads as child_row
      where child_row.prior_head_id=v_current_head.id;
      if v_n>0 then
        v_w11_reasons:=v_w11_reasons||'A_LATER_HEAD_IS_BUILT_ON_THIS_HEAD'::text;
      end if;
      -- No proposed decision bundle and no pending release bundle may name it.
      select pg_catalog.count(*) into v_n
      from public.weekly_source_entitlement_decision_bundles as bundle_row
      where bundle_row.state='PROPOSED'
        and v_current_head.id=any(bundle_row.proposed_head_ids);
      select pg_catalog.count(*) into v_n2
      from public.weekly_source_pending_entitlement_bundles as pending_row
      where pending_row.state in ('PENDING','RELEASING','MANUAL_REVIEW')
        and v_current_head.id=any(pending_row.proposed_head_ids);
      if v_n>0 or v_n2>0 then
        v_w11_reasons:=v_w11_reasons||'A_PROPOSED_OR_PENDING_BUNDLE_NAMES_THIS_HEAD'::text;
      end if;
      -- And it must not already carry a withdrawal supersession.  The check
      -- constraint makes that impossible while it is COMMITTED_CURRENT; it is
      -- tested anyway because this is the row the owner is about to write.
      if v_current_head.superseded_by_withdrawal_id is not null
         or v_current_head.superseded_at_utc is not null then
        v_w11_reasons:=v_w11_reasons||'HEAD_ALREADY_SUPERSEDED'::text;
      end if;
    end if;
  end if;
  v_w11:=coalesce(pg_catalog.array_length(v_w11_reasons,1),0)=0;

  -- ---------------- verdict ------------------------------------------------
  v_checks:=pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object('check','W1','passed',v_w1,
      'code','WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS',
      'nature','PERMANENT',
      'reasons',pg_catalog.to_jsonb(v_w1_reasons)),
    pg_catalog.jsonb_build_object('check','W2','passed',v_w2,
      'code',case when v_w2_permanent then 'WEEKLY_SOURCE_UNAUTHORISE_PAID'
                  else 'WEEKLY_SOURCE_UNAUTHORISE_MONEY_UNRESOLVED' end,
      'nature',case when v_w2_permanent then 'PERMANENT' else 'TEMPORARY' end,
      'reasons',pg_catalog.to_jsonb(v_w2_reasons)),
    pg_catalog.jsonb_build_object('check','W3','passed',v_w3,
      'code',case when 'CENSUS_ERROR'=any(v_w3_reasons)
                  then 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
                  else 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE' end,
      'nature',case when 'CENSUS_ERROR'=any(v_w3_reasons) then 'INTEGRITY'
                    else 'TEMPORARY' end,
      'reasons',pg_catalog.to_jsonb(v_w3_reasons)),
    pg_catalog.jsonb_build_object('check','W4','passed',v_w4,
      -- Round 5 section A5: the finance-case write-off has its own code and its
      -- own review disposition; every other W4 reason stays the temporary
      -- Banking-Pay-active refusal.
      'code',case when v_w4_write_off
                  then 'WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED'
                  else 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE' end,
      'nature',case when v_w4_write_off then 'INTEGRITY' else 'TEMPORARY' end,
      'reasons',pg_catalog.to_jsonb(v_w4_reasons)),
    pg_catalog.jsonb_build_object('check','W5','passed',v_w5,
      'code','WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE','nature','TEMPORARY',
      'reasons',pg_catalog.to_jsonb(v_w5_reasons)),
    pg_catalog.jsonb_build_object('check','W6','passed',v_w6,
      'code',case when v_w6_permanent then 'WEEKLY_SOURCE_UNAUTHORISE_RECOVERY_EXISTS'
                  else 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE' end,
      'nature',case when v_w6_permanent then 'PERMANENT' else 'TEMPORARY' end,
      'reasons',pg_catalog.to_jsonb(v_w6_reasons)),
    pg_catalog.jsonb_build_object('check','W7','passed',v_w7,
      'code','WEEKLY_SOURCE_UNAUTHORISE_INVOICED','nature','PERMANENT',
      'reasons',pg_catalog.to_jsonb(v_w7_reasons)),
    pg_catalog.jsonb_build_object('check','W8','passed',v_w8,
      'code','WEEKLY_SOURCE_UNAUTHORISE_INVOICE_OPERATION_ACTIVE','nature','TEMPORARY',
      'reasons',pg_catalog.to_jsonb(v_w8_reasons)),
    pg_catalog.jsonb_build_object('check','W9','passed',v_w9,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','nature','INTEGRITY',
      'reasons',pg_catalog.to_jsonb(v_w9_reasons)),
    pg_catalog.jsonb_build_object('check','W10','passed',v_w10,
      'code',case when v_w10_permanent then 'WEEKLY_SOURCE_UNAUTHORISE_PAID'
                  else 'WEEKLY_SOURCE_UNAUTHORISE_MONEY_UNRESOLVED' end,
      'nature',case when v_w10_permanent then 'PERMANENT' else 'TEMPORARY' end,
      'reasons',pg_catalog.to_jsonb(v_w10_reasons)),
    pg_catalog.jsonb_build_object('check','W11','passed',v_w11,
      'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','nature','INTEGRITY',
      'reasons',pg_catalog.to_jsonb(v_w11_reasons))
  );

  -- Which refusal is returned is decided by severity, never by evaluation or
  -- sort order, and every failing check is returned alongside it:
  --
  --   1. W9, root identity.  If the identity is wrong then W1 to W8 may have
  --      read the wrong family, so nothing they found can be reported as the
  --      verdict (proof/34 section 6, proof/36 section 4 W9).
  --   2. any PERMANENT refusal.  proof/36 section 6's permanence rule: once the
  --      root is paid, invoiced or used by a later decision the control is not
  --      shown at all, and that must never be softened into "review required"
  --      or "try again later".
  --   3. an INTEGRITY failure: the census error class (W3), the round-5
  --      section A5 finance-case write-off (W4) and the head-supersession
  --      integrity checks (W11).  All three mean "Office review", not "try
  --      again later".
  --   4. a TEMPORARY refusal.
  --
  -- Every ordering above refuses; none of them releases, so the choice only
  -- decides which true reason Office is shown.  Within one severity the check
  -- NUMBER decides, taken numerically so that W2 precedes W10 (a lexicographic
  -- sort would put 'W10' before 'W2').
  select failed.value->>'code', failed.value->>'nature'
    into v_code, v_nature
  from pg_catalog.jsonb_array_elements(v_checks) as failed(value)
  where (failed.value->>'passed')::boolean is not true
  order by case
             when failed.value->>'check'='W9' then 1
             when failed.value->>'nature'='PERMANENT' then 2
             when failed.value->>'nature'='INTEGRITY' then 3
             else 4 end,
           pg_catalog.substr(failed.value->>'check',2)::integer
  limit 1;

  -- The plain-English sentence for the refusal Office is shown.  The code is
  -- the machine fact; this is the human one, and it states permanence in words
  -- because proof/36 section 6 turns permanence into "the control is not shown"
  -- rather than "try again later".
  v_message:=case v_code
    when 'WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS' then
      'A later Weekly Source decision already depends on this Timesheet, so the '
      ||'first authorisation can no longer be withdrawn. This is permanent. Use '
      ||'the Weekly Source reconciliation process instead.'
    when 'WEEKLY_SOURCE_UNAUTHORISE_PAID' then
      'This Timesheet has been paid, so the first authorisation can no longer be '
      ||'withdrawn. This is permanent.'
    -- ROUND 5 SECTION A3: "If any payment is already in flight or any effect is
    -- ambiguous, withdrawal is refused with a plain-English instruction to use
    -- the proper Banking Pay cancellation/correction path."
    when 'WEEKLY_SOURCE_UNAUTHORISE_MONEY_UNRESOLVED' then
      'A payment for this Timesheet is in flight, or its outcome is not yet known. '
      ||'Nothing has been changed here. Use the Banking Pay cancellation or '
      ||'correction process to deal with the payment, and withdraw the '
      ||'authorisation once that has finished.'
    when 'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE' then
      'This Timesheet is in a Banking Pay Draft, reservation or payment run. '
      ||'Use the Banking Pay cancellation or correction process to deal with the '
      ||'payment first, then withdraw the authorisation.'
    -- ROUND 5 SECTION A5, word for word: "The message must identify the
    -- finance-case write-off as unresolved Banking Pay evidence and route it to
    -- review.  Do not label it paid, settled or safely cancelled."  The sentence
    -- below says unresolved, says Banking Pay, says review, and says nothing
    -- about the money having moved or not moved, because nothing here proves
    -- either.
    when 'WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED' then
      'A Banking Pay reservation for this Timesheet was released by a finance-case '
      ||'write-off. That is unresolved Banking Pay evidence: it does not show '
      ||'whether the payment was made or cancelled, so the authorisation cannot be '
      ||'withdrawn yet. This has been sent for Banking Pay review, which will '
      ||'record what actually happened to the payment.'
    when 'WEEKLY_SOURCE_UNAUTHORISE_RECOVERY_EXISTS' then
      'A recovery or a payment correction other than a completed no-money '
      ||'cancellation exists for this Timesheet, so the first authorisation can '
      ||'no longer be withdrawn. This is permanent.'
    when 'WEEKLY_SOURCE_UNAUTHORISE_INVOICED' then
      'This Timesheet is on an invoice, so the first authorisation can no longer '
      ||'be withdrawn. This is permanent.'
    when 'WEEKLY_SOURCE_UNAUTHORISE_INVOICE_OPERATION_ACTIVE' then
      'An invoice operation is using this Timesheet. Try again once it has '
      ||'finished.'
    when 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' then
      'This Timesheet''s record needs Office review before it can be changed. No '
      ||'financial change has been made.'
    else null end;

  return pg_catalog.jsonb_build_object(
    'ok',v_code is null,
    'available',v_code is null,
    'code',v_code,
    'refusal_nature',v_nature,
    'refusal_message',v_message,
    'retryable',coalesce(v_nature='TEMPORARY',false),
    'checks',v_checks,
    'failed_checks',coalesce((
      select pg_catalog.jsonb_agg(failed.value order by failed.ordinality)
      from pg_catalog.jsonb_array_elements(v_checks) with ordinality as failed(value,ordinality)
      where (failed.value->>'passed')::boolean is not true),'[]'::jsonb),
    'census_result',p_census->>'result',
    'census_class_counts',p_census->'class_counts',
    'transfer_scope',v_transfer_scope,
    -- ROUND 5 SECTION A3 step 3: the head this withdrawal would supersede, and
    -- its immutable predecessor evidence, decided HERE under the locks and
    -- handed to the writer rather than re-derived by it.  Null on the ordinary
    -- no-head withdrawal, which retires nothing.
    'head_supersession',case
      when v_current_head.id is null then null
      else pg_catalog.jsonb_build_object(
        'head_id',v_current_head.id,
        'head_revision',v_current_head.head_revision,
        'state_before',v_current_head.state,
        'certified_zero',v_current_head.certified_zero,
        'component_count',v_current_head.component_count,
        'agency_id',v_current_head.agency_id,
        'authority_kind',v_current_head.authority_kind,
        'root_timesheet_id',v_current_head.root_timesheet_id,
        'root_timesheet_version',v_current_head.root_timesheet_version,
        'publication_scope_change_tx_token',v_current_head.scope_change_tx_token)
      end,
    'family_booking_id',v_family,
    'canonical_timesheet_id',v_canonical,
    'candidate_id',v_candidate_id,
    'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
    'evaluated_at_utc',v_now);
end;
$function$;

-- ---------------------------------------------------------------------------
-- THE AUTHORISATION ROW SIGNATURE — the five bindings ROUND 5 Part E requires.
--
-- "The authorisation row signature binds tenant/agency, canonical root, exact
--  authorised physical Timesheet identity, source generation/revision and the
--  canonical digest of the protected decision fields.  Booking reference alone
--  is not a signature key. ... a decision must never migrate silently between
--  physical Timesheet IDs."
--
-- The audit of what the row bound before WP-07c, and why two of the five were
-- missing, is written out in full at the head of
-- `18092026_0900_weekly_source_withdrawal_supersession.sql` section 4.  This is
-- the function that now binds all five at once.
--
-- Read it as five slots, every one of which is always present in the object:
--
--   agency_id                  tenant and agency.  NULL is a VALUE here, not an
--                              omission: an ordinary root with no protected pay
--                              target family has no authoritative agency
--                              evidence at first authorisation, and binding the
--                              absence is what stops a row written without
--                              agency evidence ever matching one written with
--                              it.
--   root_family_booking_id     the canonical root -- and NEVER on its own.
--   root_timesheet_id          the exact authorised PHYSICAL Timesheet
--   timesheet_version          identity and its revision.  These two are why a
--                              decision cannot migrate between physical ids:
--                              the digest of a row naming one physical id can
--                              never equal the digest of a row naming another.
--   authorisation_generation   the source generation.
--   protected_decision_hashes  the canonical digest of the protected decision
--   lifecycle_row_signature    fields: every live protected-hours approval hash
--                              for the root's pay target family, sorted and
--                              lower-cased, plus the ordinary CloudTMS lifecycle
--                              row signature at authorisation.
--
-- The hash itself is the INSTALLED proof/32 section 9 encoder.  This package
-- does not get a second digest mechanism, and the ordinary lifecycle signature
-- keeps its own separate column and its own separate meaning.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_root_authorisation_signature_v1(
  p_agency_id uuid,
  p_root_timesheet_id uuid,
  p_root_family_booking_id text,
  p_timesheet_version integer,
  p_authorisation_generation integer,
  p_lifecycle_row_signature text,
  p_protected_decision_hashes text[]
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'binding','WEEKLY_SOURCE_ROOT_AUTHORISATION_SIGNATURE_V1',
    'agency_id',p_agency_id,
    'root_timesheet_id',p_root_timesheet_id,
    'root_family_booking_id',pg_catalog.btrim(coalesce(p_root_family_booking_id,'')),
    'timesheet_version',p_timesheet_version,
    'authorisation_generation',p_authorisation_generation,
    'lifecycle_row_signature',pg_catalog.btrim(coalesce(p_lifecycle_row_signature,'')),
    'protected_decision_hashes',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.lower(hash_element.value)
                                  order by pg_catalog.lower(hash_element.value))
      from pg_catalog.unnest(coalesce(p_protected_decision_hashes,array[]::text[]))
        as hash_element(value)
      where nullif(pg_catalog.btrim(coalesce(hash_element.value,'')),'') is not null
    ),'[]'::jsonb));
$function$;

-- The live protected-decision hashes for a root's pay target family, in the
-- exact shape the signature binds.  Read-only.  A withdrawn approval is not a
-- live protected decision and is deliberately excluded, which is why a
-- withdrawal that marks approvals withdrawn changes what a LATER authorisation
-- of the same root would bind -- correctly, because it is a different decision.
create or replace function private.weekly_source_root_protected_decision_hashes_v1(
  p_member_timesheet_ids uuid[]
) returns text[]
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select coalesce(
    pg_catalog.array_agg(distinct pg_catalog.encode(approval_row.approval_hash,'hex')),
    array[]::text[])
  from public.weekly_exceptional_payment_approvals as approval_row
  join public.weekly_exceptional_pay_target_families as family_row
    on family_row.id=approval_row.pay_target_family_id
  where family_row.root_timesheet_id=any(coalesce(p_member_timesheet_ids,array[]::uuid[]))
    and approval_row.withdrawn_at_utc is null
    and approval_row.approval_hash is not null;
$function$;

-- The agency evidence for a root, or NULL when this schema holds none.  Read
-- only, and explicitly cardinality-checked: two pay target families disagreeing
-- about the agency of one root is not a shape from which an arbitrary row may
-- be picked, so it yields NULL and the caller's identity checks then refuse.
create or replace function private.weekly_source_root_agency_id_v1(
  p_member_timesheet_ids uuid[]
) returns uuid
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select case when pg_catalog.count(distinct family_row.agency_id)=1
              then pg_catalog.min(family_row.agency_id::text)::uuid end
  from public.weekly_exceptional_pay_target_families as family_row
  where family_row.root_timesheet_id=any(coalesce(p_member_timesheet_ids,array[]::uuid[]))
    and family_row.agency_id is not null;
$function$;

-- ---------------------------------------------------------------------------
-- The canonical withdrawal request and its digest.
--
-- ROUND 5, Part E, "Rotation readings", word for word: "The authorisation row
-- signature binds tenant/agency, canonical root, exact authorised physical
-- Timesheet identity, source generation/revision and the canonical digest of
-- the protected decision fields.  Booking reference alone is not a signature
-- key.  A rotation or re-point that changes the physical source requires the
-- accepted rotation/normalisation path and a new current signature; a decision
-- must never migrate silently between physical Timesheet IDs."
--
-- Every one of those five bindings is a key of the object below:
--   tenant and agency              -> agency_id (the retired head's agency)
--   canonical root                 -> root_family_booking_id AND root_timesheet_id
--   exact physical Timesheet identity
--                                  -> root_timesheet_id, requested_timesheet_id,
--                                     expected_timesheet_id, root_timesheet_version
--   source generation and revision -> authorisation_generation,
--                                     predecessor_head_revision
--   protected decision fields      -> predecessor_head_id and the row signature
--
-- The digest is produced by the INSTALLED canonical encoder WP-02 built for
-- proof/32 section 9 (`private.weekly_source_publication_request_digest_v1`:
-- sorted keys, no insignificant whitespace, base-10 integers, lower-case UUIDs,
-- SHA-256 of the UTF-8 bytes).  Reusing it is deliberate: this package does not
-- get a second digest mechanism.
--
-- The object is built from receipt-shaped FIELDS, not from a live read, so the
-- identical function serves two callers: the writer, which builds it from the
-- context under the locks, and the replay path, which rebuilds it from the
-- stored receipt row and compares.  That comparison is what makes a tampered
-- receipt detectable (round 5 section A1 control 5: a tampered row is a
-- permanent integrity failure that goes straight to manual review).
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_withdrawal_canonical_request_v1(
  p_agency_id uuid,
  p_candidate_id uuid,
  p_contract_id uuid,
  p_root_timesheet_id uuid,
  p_root_family_booking_id text,
  p_root_timesheet_version integer,
  p_requested_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_root_authorisation_id uuid,
  p_authorisation_generation integer,
  p_expected_row_signature text,
  p_predecessor_head_id uuid,
  p_predecessor_head_revision bigint
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'operation','WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',
    'agency_id',p_agency_id,
    'candidate_id',p_candidate_id,
    'contract_id',p_contract_id,
    'root_timesheet_id',p_root_timesheet_id,
    'root_family_booking_id',pg_catalog.btrim(coalesce(p_root_family_booking_id,'')),
    'root_timesheet_version',p_root_timesheet_version,
    'requested_timesheet_id',p_requested_timesheet_id,
    'expected_timesheet_id',p_expected_timesheet_id,
    'root_authorisation_id',p_root_authorisation_id,
    'authorisation_generation',p_authorisation_generation,
    'expected_row_signature',pg_catalog.btrim(coalesce(p_expected_row_signature,'')),
    'predecessor_head_id',p_predecessor_head_id,
    'predecessor_head_revision',p_predecessor_head_revision);
$function$;

-- ---------------------------------------------------------------------------
-- proof/36 section 5 step 1, as ROUND 5 SECTION A3 step 5 now makes it: "Exact
-- replay returns the existing receipt; conflicting replay refuses."
--
-- WP-07b's finding F4 is CLOSED as a consequence.  The recorded result used to
-- live only in `public.audit_events.after_json->'result'`, so an audit-retention
-- rule could silently turn a recorded success into a refusal.  Ruling A3 step 5
-- puts a durable replay receipt in the atomic write set, so the replay now reads
-- `private.weekly_source_first_authorisation_withdrawal_receipts`, which no
-- retention rule touches and which cannot be updated or deleted at all (its
-- immutability trigger raises WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE).
--
-- Three outcomes, and no fourth:
--   * no receipt for (root, signature)            -> null, the caller proceeds;
--   * a receipt whose stored request rebuilds to its own digest AND whose
--     recorded request equals the presented one  -> the recorded result;
--   * anything else -> a structured refusal.  A presented request that differs
--     from the recorded one is WEEKLY_SOURCE_WITHDRAWAL_REPLAY_CONFLICT; a row
--     that no longer hashes to its own digest is
--     WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_TAMPERED.  Both are permanent integrity
--     outcomes for manual review and neither is retried (round 5 section A1
--     control 5).
--
-- The lookup is keyed on the PHYSICAL root id the caller passed, which is what
-- preserves UNA-018: an old physical id of a rotated family matches no receipt,
-- falls through to the checks and is refused.  The decision is never received by
-- or moved to it.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_first_authorisation_withdrawal_recorded_v1(
  p_timesheet_id uuid,
  p_expected_row_signature text,
  p_expected_timesheet_id uuid default null
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_signature text;
  v_count integer;
  v_generation integer;
  v_receipt private.weekly_source_first_authorisation_withdrawal_receipts%rowtype;
  v_rebuilt bytea;
begin
  v_signature:=nullif(pg_catalog.btrim(coalesce(p_expected_row_signature,'')),'');
  if p_timesheet_id is null or v_signature is null then
    return null;
  end if;

  -- A REPLAY IS ONLY EVER A REPLAY WHILE THE ROOT IS STILL WITHDRAWN, and it is
  -- always a replay of the LATEST WITHDRAWN GENERATION.  Both halves are load
  -- bearing, and the second was proved necessary by execution rather than
  -- reasoned about:
  --
  --   `public.timesheet_lifecycle_guard_signature_v1` is a function of the
  --   Timesheet's own state, so withdrawing a root and then authorising it
  --   again produces the SAME row signature on the new generation.  On the
  --   WP-07 fixture, generation 1 and generation 2 of root WP07-BK-06 carry a
  --   byte-identical signature.  Keyed on (root, signature) alone, a genuine
  --   second withdrawal of generation 2 was MISTAKEN FOR A REPLAY of
  --   generation 1 and returned the earlier receipt -- reporting no head
  --   superseded while the committed head stayed current.  That is the same
  --   class of defect as the one ruling A3 exists to close, so the generation
  --   is part of the key and a live generation ends the replay question before
  --   it is asked.
  --
  -- This also preserves UNA-011 and UNA-018 exactly as WP-07 built them: a
  -- re-authorised root can never be withdrawn again by presenting the old
  -- signature, and an old physical id of a rotated family matches nothing.
  if exists (
    select 1
    from public.weekly_source_root_authorisations as live_row
    where live_row.root_timesheet_id=p_timesheet_id
      and live_row.withdrawn_at_utc is null
  ) then
    return null;
  end if;

  select pg_catalog.max(authorisation_row.authorisation_generation)
    into v_generation
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=p_timesheet_id
    and authorisation_row.withdrawn_at_utc is not null;
  if v_generation is null then
    return null;
  end if;

  -- Explicit cardinality, never `limit 1` (executed-review rule 5).  The unique
  -- index makes two rows impossible; if one ever existed this returns the
  -- fail-closed branch instead of an arbitrary row.
  select pg_catalog.count(*)::integer into v_count
  from private.weekly_source_first_authorisation_withdrawal_receipts as receipt_row
  where receipt_row.root_timesheet_id=p_timesheet_id
    and receipt_row.expected_row_signature=v_signature
    and receipt_row.authorisation_generation=v_generation;
  if v_count=0 then
    return null;
  end if;
  if v_count>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_TAMPERED',
      'refusal_nature','INTEGRITY','retryable',false,
      'refusal_message','This withdrawal record needs Office review before the '
        ||'Timesheet can be changed. No financial change has been made.',
      'reason','MORE_THAN_ONE_RECEIPT_FOR_THE_REPLAY_IDENTITY',
      'review_required',true);
  end if;

  select * into v_receipt
  from private.weekly_source_first_authorisation_withdrawal_receipts as receipt_row
  where receipt_row.root_timesheet_id=p_timesheet_id
    and receipt_row.expected_row_signature=v_signature
    and receipt_row.authorisation_generation=v_generation;

  v_rebuilt:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_withdrawal_canonical_request_v1(
      v_receipt.agency_id,v_receipt.candidate_id,v_receipt.contract_id,
      v_receipt.root_timesheet_id,v_receipt.root_family_booking_id,
      v_receipt.root_timesheet_version,v_receipt.requested_timesheet_id,
      v_receipt.expected_timesheet_id,v_receipt.root_authorisation_id,
      v_receipt.authorisation_generation,v_receipt.expected_row_signature,
      v_receipt.predecessor_head_id,v_receipt.predecessor_head_revision));
  if v_rebuilt is distinct from v_receipt.request_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_TAMPERED',
      'refusal_nature','INTEGRITY','retryable',false,
      'refusal_message','This withdrawal record needs Office review before the '
        ||'Timesheet can be changed. No financial change has been made.',
      'reason','RECEIPT_DOES_NOT_REBUILD_TO_ITS_OWN_DIGEST',
      'receipt_id',v_receipt.id,
      'review_required',true);
  end if;

  -- Conflicting replay.  The only caller-supplied field beyond the replay
  -- identity is the expected physical Timesheet id, and presenting a different
  -- one is exactly the "decision migrating between physical Timesheet IDs" the
  -- round-5 rotation ruling forbids.  A null expectation is the caller declining
  -- to state one and is not a conflict.
  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_receipt.expected_timesheet_id
     and p_expected_timesheet_id is distinct from v_receipt.root_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_WITHDRAWAL_REPLAY_CONFLICT',
      'refusal_nature','INTEGRITY','retryable',false,
      'refusal_message','This withdrawal has already been recorded against a '
        ||'different Timesheet record, so it cannot be repeated against this one. '
        ||'Office review is needed. No financial change has been made.',
      'reason','EXPECTED_TIMESHEET_ID_DIFFERS_FROM_THE_RECORDED_REQUEST',
      'receipt_id',v_receipt.id,
      'recorded_expected_timesheet_id',v_receipt.expected_timesheet_id,
      'presented_expected_timesheet_id',p_expected_timesheet_id,
      'review_required',true);
  end if;

  return v_receipt.result_json;
end;
$function$;

-- ---------------------------------------------------------------------------
-- HANDOVER 2 round 4 ruling 6 (R25, restated).  Verified inside the
-- transaction, after the single explicit invalidation and before the caller
-- commits anything that depends on it:
--
--   1. every invalidation produced in this transaction carries the SAME token
--      UUID — the explicit one, the ordinary Authorise/Unauthorise trigger
--      route, and every registered DIRTY_TRIGGER:<table>:<op> path;
--   5. every non-complete job, normalised through the INSTALLED rotation
--      normaliser, is an exact subset of the declared complete aligned scope
--      for that Candidate; a different Candidate or a root outside the declared
--      scope is a contract failure, not harmless extra work;
--   6. exactly one effective complete-scope result is adopted per Candidate;
--   7. any token disagreement or unexpected scope rolls the transaction back.
--
-- Jobs produced by this transaction are identified by their own xmin, not by a
-- timestamp: a concurrently committed job must never be mistaken for ours.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_invalidation_contract_assert_v1(
  p_candidate_id uuid,
  p_declared_root_ids uuid[],
  p_token uuid,
  p_context text,
  p_pre_existing_job_ids uuid[] default array[]::uuid[]
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_declared uuid[];
  v_jobs jsonb:='[]'::jsonb;
  v_complete integer:=0;
  v_whole_candidate integer:=0;
  v_tokens integer;
  v_failure text;
  v_job record;
  v_scope uuid[];
  v_targeted uuid[];
  v_pre uuid[]:=coalesce(p_pre_existing_job_ids,array[]::uuid[]);
begin
  if p_token is null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
              'context',p_context)::text;
  end if;

  -- The declared complete aligned scope, expanded through the installed
  -- normaliser exactly as the Workbench expands a queued job.
  select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
           public._pay_workbench_normalise_timesheet_rotation_scope_payload(
             coalesce(p_declared_root_ids,array[]::uuid[]),array[]::uuid[]
           )->'family_timesheet_ids'),array[]::uuid[])
    into v_declared;

  -- The controlling token row must exist and still be open.
  select pg_catalog.count(*)::integer into v_tokens
  from public.banking_pay_scope_change_transactions as token_row
  where token_row.tx_token=p_token and token_row.state='PENDING';
  if v_tokens<>1 then
    v_failure:='CONTROLLING_TOKEN_NOT_OPEN';
  end if;

  -- Which jobs belong to this operation: every job that is QUEUED or RUNNING
  -- now and was not already so when the operation started.  Those are exactly
  -- the jobs the Workbench will run because of it, whatever Candidate and
  -- whatever token they carry, so a job for the wrong Candidate or under a
  -- different token is caught.  The installed dedupe index is partial on
  -- (QUEUED, RUNNING), so a coalesced job is always one of these rows.
  --
  -- A transaction id cannot be used for this: a plpgsql block with an EXCEPTION
  -- handler runs in a subtransaction, so rows written by the owner carry a
  -- SUBtransaction xid in xmin whenever any caller wraps it, and an `xmin =
  -- pg_current_xact_id()` test then silently sees nothing at all.  That was
  -- observed, not assumed (WP-07 report, finding F3).
  for v_job in
    select job_row.id, job_row.candidate_id, job_row.job_type, job_row.dedupe_key,
           job_row.scope_change_tx_token, job_row.scope_change_generation,
           job_row.payload_json
    from public.banking_pay_workbench_jobs as job_row
    where job_row.status in ('QUEUED','RUNNING')
      and not (job_row.id=any(v_pre))
    order by job_row.id
  loop
    -- Point 1: every invalidation produced by this operation carries the same
    -- token, whatever registered path produced it — including the
    -- CONTRACT_CLIENT_DIRTY_FANOUT and finance-case paths, which carry no
    -- Candidate at all.
    if v_job.scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'JOB_CARRIES_A_DIFFERENT_TOKEN');
    end if;

    v_targeted:=private.weekly_source_withdrawal_uuid_array_v1(
      v_job.payload_json->'targeted_timesheet_ids');

    select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
             public._pay_workbench_normalise_timesheet_rotation_scope_payload(
               v_targeted,
               private.weekly_source_withdrawal_uuid_array_v1(
                 v_job.payload_json->'linked_timesheet_ids')
             )->'family_timesheet_ids'),array[]::uuid[])
      into v_scope;

    -- Point 5 is a rule about CANDIDATE jobs: a job queued for a different
    -- Candidate, or naming a root outside the declared aligned scope, is a
    -- contract failure.  A job with no Candidate is a registered non-Candidate
    -- fanout path; it is recorded and its token is still required to match.
    if v_job.candidate_id is not null then
      if v_job.candidate_id is distinct from p_candidate_id then
        v_failure:=coalesce(v_failure,'JOB_FOR_A_DIFFERENT_CANDIDATE');
      end if;

      if exists (select 1 from pg_catalog.unnest(v_scope) as scope_id(value)
                 where not (scope_id.value=any(v_declared))) then
        v_failure:=coalesce(v_failure,'JOB_SCOPE_OUTSIDE_THE_DECLARED_SCOPE');
      end if;

      if coalesce(pg_catalog.cardinality(v_targeted),0)=0 then
        -- An empty target list is the Workbench's whole-Candidate job.  It
        -- cannot name a root outside the Candidate and it can only widen the
        -- refresh, never narrow it, so it is neither a subset violation nor the
        -- declared complete-scope job.  It is counted and reported separately.
        v_whole_candidate:=v_whole_candidate+1;
      elsif coalesce(pg_catalog.cardinality(v_scope),0)
            =coalesce(pg_catalog.cardinality(v_declared),0)
        and not exists (select 1 from pg_catalog.unnest(v_declared) as declared_id(value)
                        where not (declared_id.value=any(v_scope))) then
        v_complete:=v_complete+1;
      end if;
    end if;

    v_jobs:=v_jobs||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'job_id',v_job.id,
      'candidate_id',v_job.candidate_id,
      'job_type',v_job.job_type,
      'dedupe_key',v_job.dedupe_key,
      'scope_change_tx_token',v_job.scope_change_tx_token,
      'scope_change_generation',v_job.scope_change_generation,
      'targeted_timesheet_ids',pg_catalog.to_jsonb(v_targeted),
      'normalised_scope',pg_catalog.to_jsonb(v_scope),
      'whole_candidate',v_job.candidate_id is not null
        and coalesce(pg_catalog.cardinality(v_targeted),0)=0));
  end loop;

  -- Point 6: exactly one effective complete-scope dirty result per Candidate.
  if v_complete<>1 then
    v_failure:=coalesce(v_failure,
      case when v_complete=0 then 'NO_COMPLETE_SCOPE_JOB'
           else 'MORE_THAN_ONE_COMPLETE_SCOPE_JOB' end);
  end if;

  if v_failure is not null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
              'reason',v_failure,
              'context',p_context,
              'scope_change_tx_token',p_token,
              'declared_scope',pg_catalog.to_jsonb(v_declared),
              'jobs',v_jobs)::text;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'scope_change_tx_token',p_token,
    'declared_scope',pg_catalog.to_jsonb(v_declared),
    'complete_scope_job_count',v_complete,
    'whole_candidate_job_count',v_whole_candidate,
    'jobs',v_jobs);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Interface I-6 — first-authorisation core.
--
-- Assumes the I-1 locks are already held in this transaction and receives the
-- helper result as proof of it.  Confirms from p_lock_result that the requested
-- row is the canonical current version (otherwise the stale refusal of
-- proof/34 section 5 step 3, with no write), calls the UNCHANGED ordinary
-- Authorise owner exactly once, requires success, and inserts the root
-- authorisation generation of proof/34 section 4 (generation 1, or N+1 after a
-- withdrawal).
--
-- It calls NO invalidator: the ordinary Authorise trigger route does the
-- wake-up, and inside a bundle the coordinator makes the single explicit call
-- afterwards (proof/32 section 8 step 3).
--
-- WP-07b, finding F2.  The already-authorised test here is FAMILY-WIDE, not per
-- physical root.  The Office entry point reaches I-6 through
-- `private.weekly_source_first_authorisation_context_v1`, which already refuses
-- `LIVE_AUTHORISATION_NOT_ON_CANONICAL_ROW`; WP-02's Gate 5 coordinator calls
-- I-6 DIRECTLY for a genuinely new B root with only an I-1 lock result and no
-- context read.  With a per-root test a family that had rotated after its first
-- authorisation — the state proof/34 section 6 calls an integrity failure —
-- could gain a SECOND live generation, on the canonical row, while the first
-- stayed on a historical member.  The root was then authorised, payable, and
-- permanently un-withdrawable, because the withdrawal owner's context read
-- refuses that same family shape and the `Unauthorise` control is never shown.
-- The member list is taken from `p_lock_result`'s own `families` entry, so no
-- relation outside the I-1 lock set is read.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_first_authorise_core_v1(
  p_timesheet_id uuid,
  p_expected_row_signature text,
  p_actor_user_id uuid,
  p_lock_result jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_family jsonb;
  v_members uuid[];
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

  -- The complete physical family, from the lock result the caller already holds
  -- the locks for.  The requested row is always a member of its own family; it
  -- is added defensively so a helper that omitted it could never widen the test
  -- into a no-op.
  select coalesce(pg_catalog.array_agg(distinct member_element.value::uuid),
                  array[]::uuid[])
    into v_members
  from pg_catalog.jsonb_array_elements_text(
         case when pg_catalog.jsonb_typeof(coalesce(v_family->'member_timesheet_ids','null'::jsonb))='array'
              then v_family->'member_timesheet_ids' else '[]'::jsonb end)
    as member_element(value);
  if not (p_timesheet_id=any(v_members)) then
    v_members:=v_members||p_timesheet_id;
  end if;

  -- WP-07b finding F2: a live generation anywhere ELSE in the family means the
  -- authorised root was rotated after its first authorisation.  proof/34
  -- section 6: "the operation stops with WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE,
  -- no financial change is made, and Office review is required."  Refused
  -- before the ordinary Authorise owner is called, so nothing is written.
  select live_row.root_timesheet_id
    into v_elsewhere
  from public.weekly_source_root_authorisations as live_row
  where live_row.root_timesheet_id=any(v_members)
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

  -- The UNCHANGED ordinary Authorise owner, exactly once, through its current
  -- contract.  No timestamp is passed: the owner keeps its own default.
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
  v_agency_id:=private.weekly_source_root_agency_id_v1(v_members);
  v_protected_hashes:=private.weekly_source_root_protected_decision_hashes_v1(v_members);
  v_decision_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_root_authorisation_signature_v1(
      v_agency_id,p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
      v_signature,v_protected_hashes));

  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,current_entitlement_head_id,authorised_by_user_id,
    agency_id,protected_decision_hashes,decision_digest
  ) values (
    p_timesheet_id,v_root.booking_id,v_root.version,v_generation,
    v_signature,null,p_actor_user_id,
    v_agency_id,v_protected_hashes,v_decision_digest
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

-- ---------------------------------------------------------------------------
-- The Office first-authorisation entry point: interface I-1 with the pinned job
-- type WORKBENCH_CANDIDATE_FIRST_AUTHORISATION, then interface I-6.
-- It calls no invalidator (24 section 4.1: the existing ordinary authorisation
-- owner remains the only first-authorisation mechanism and its trigger route
-- does the wake-up).
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_first_authorise_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_lock jsonb;
  v_core jsonb;
  v_canonical uuid;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  -- One plain read before the gate, because the gate is pinned to a Candidate.
  -- Nothing is written by it and the identity is re-resolved under the locks.
  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    return v_context;
  end if;

  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    (v_context->>'candidate_id')::uuid,
    array[p_timesheet_id]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),
    'WEEKLY_SOURCE_FIRST_AUTHORISATION');
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock;
  end if;

  select (family_element.value->>'canonical_timesheet_id')::uuid
    into v_canonical
  from pg_catalog.jsonb_array_elements(coalesce(v_lock->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_canonical then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical);
  end if;

  v_core:=private.weekly_source_first_authorise_core_v1(
    p_timesheet_id,p_expected_row_signature,p_actor_user_id,v_lock);
  return v_core||pg_catalog.jsonb_build_object('gate',v_lock->>'gate');
end;
$function$;

-- ---------------------------------------------------------------------------
-- G3-4: the read-only availability verdict the Simple Timesheet and Bulk
-- Authorise screens use.  Computed from the same W1 to W9 as the write path, so
-- the control is shown if and only if the owner would accept the call.  STABLE:
-- it can write nothing.
--
-- WP-07b, finding F3 — the meaning of `ok` in this package, stated once and
-- uniform in every function here:
--
--   `ok = true`  means THE UNAUTHORISE ACTION IS PERMITTED.  In this read-only
--                function that is "the control may be shown"; in
--                `public.weekly_source_first_authorisation_withdraw_v1` it is
--                "the withdrawal happened (or its exact replay was returned)".
--   `ok = false` means refused, and `code`, `refusal_nature`,
--                `refusal_message` and `retryable` say why.
--
-- `available` is kept as the explicit alias the screens read and is always
-- equal to `ok` here.  The earlier shape forced `ok := true` on every return,
-- including refusals, so a screen or broker reading `ok` would have shown the
-- `Unauthorise` control on a permanently refused root — the write path would
-- then have refused, so no money could move, but proof/36 sections 3 and 6
-- require the control not to be shown at all.
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_census jsonb;
  v_members uuid[];
  v_verdict jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,'available',false,
      'code',v_context->>'code',
      'refusal_nature','INTEGRITY','retryable',false,
      'refusal_message','This Timesheet''s record needs Office review before it '
        ||'can be changed. No financial change has been made.',
      'reason',v_context->>'reason','checks','[]'::jsonb,
      'timesheet_id',p_timesheet_id);
  end if;

  select pg_catalog.array_agg(member_element.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_context->'member_timesheet_ids')
    as member_element(value);

  v_census:=private.weekly_source_freeze_census_v1(
    (v_context->>'candidate_id')::uuid,coalesce(v_members,array[]::uuid[]));

  v_verdict:=private.weekly_source_first_authorisation_withdraw_checks_v1(
    v_context,v_census,null,null);

  -- `ok` is the verdict's own `available`, never an unconditional true.
  return v_verdict||pg_catalog.jsonb_build_object(
    'ok',coalesce((v_verdict->>'available')::boolean,false),
    'timesheet_id',p_timesheet_id);
end;
$function$;

-- ---------------------------------------------------------------------------
-- proof/36 section 5 step 7: where protected/exceptional hours formed part of
-- the first authorisation, the approved protected-hours decision is marked
-- withdrawn.  The row is never deleted; its history stays visible.
--
-- The step also names "plus a weekly_exceptional_pay_family_events row".  The
-- installed state check on that relation admits only WAIT, ACCEPTED_SOURCE and
-- NOT_WORKED, none of which means "withdrawn", and the Office read projections
-- interpret the latest event.  Appending a mislabelled event would change what
-- the screens show, so the row is written only when the installed constraint
-- admits FIRST_AUTHORISATION_WITHDRAWN.  Until the schema owner adds that value
-- (handoff WP-07 N2) the omission is reported, never silent.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_first_authorisation_withdraw_protected_v1(
  p_member_timesheet_ids uuid[],
  p_actor_user_id uuid,
  p_now timestamptz
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_state_admitted boolean;
  v_approval_ids uuid[]:=array[]::uuid[];
  v_event_ids uuid[]:=array[]::uuid[];
  v_approval record;
  v_sequence bigint;
  v_prior_hash bytea;
  v_event_hash bytea;
  v_event_id uuid;
begin
  select exists (
    select 1
    from pg_catalog.pg_constraint as constraint_row
    where constraint_row.conrelid='public.weekly_exceptional_pay_family_events'::pg_catalog.regclass
      and constraint_row.contype='c'
      and pg_catalog.pg_get_constraintdef(constraint_row.oid)
          like '%FIRST_AUTHORISATION_WITHDRAWN%')
    into v_state_admitted;

  for v_approval in
    select approval_row.id, approval_row.pay_target_family_id,
           approval_row.work_event_id, approval_row.protected_work_date,
           approval_row.protected_start_at_local, approval_row.protected_end_at_local,
           approval_row.protected_break_minutes
    from public.weekly_exceptional_payment_approvals as approval_row
    join public.weekly_exceptional_pay_target_families as family_row
      on family_row.id=approval_row.pay_target_family_id
    where family_row.root_timesheet_id=any(coalesce(p_member_timesheet_ids,array[]::uuid[]))
      and approval_row.withdrawn_at_utc is null
    order by approval_row.id
  loop
    update public.weekly_exceptional_payment_approvals
       set withdrawn_at_utc=p_now,
           withdrawn_by_user_id=p_actor_user_id,
           withdrawal_kind='FIRST_AUTHORISATION_WITHDRAWN'
     where id=v_approval.id;
    v_approval_ids:=v_approval_ids||v_approval.id;

    if v_state_admitted then
      select coalesce(pg_catalog.max(event_row.event_sequence),0)+1
        into v_sequence
      from public.weekly_exceptional_pay_family_events as event_row
      where event_row.family_id=v_approval.pay_target_family_id;

      select prior_row.event_hash into v_prior_hash
      from public.weekly_exceptional_pay_family_events as prior_row
      where prior_row.family_id=v_approval.pay_target_family_id
        and prior_row.event_sequence=v_sequence-1;

      v_event_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_FAMILY_EVENT_V1',
        pg_catalog.jsonb_build_object(
          'family_id',v_approval.pay_target_family_id,
          'event_sequence',v_sequence,
          'work_event_id',v_approval.work_event_id,
          'approval_id',v_approval.id,
          'state','FIRST_AUTHORISATION_WITHDRAWN',
          'prior_event_hash',case when v_prior_hash is null then null
                                  else pg_catalog.encode(v_prior_hash,'hex') end));

      insert into public.weekly_exceptional_pay_family_events(
        family_id,event_sequence,durable_work_event_id,evidence_approval_id,
        work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
        source_proposal_snapshot_json,source_proposal_hash,
        fixed_office_target_snapshot_json,fixed_office_target_hash,state,
        office_actor_user_id,office_reason,prior_event_hash,event_hash
      ) values (
        v_approval.pay_target_family_id,v_sequence,v_approval.work_event_id,v_approval.id,
        v_approval.protected_work_date,v_approval.protected_start_at_local,
        v_approval.protected_end_at_local,v_approval.protected_break_minutes,
        '{}'::jsonb,'{}'::jsonb,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1','{}'::jsonb),
        '{}'::jsonb,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_FIXED_OFFICE_TARGET_V1','{}'::jsonb),
        'FIRST_AUTHORISATION_WITHDRAWN',
        p_actor_user_id,'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN',
        v_prior_hash,v_event_hash
      ) returning id into v_event_id;
      v_event_ids:=v_event_ids||v_event_id;
    end if;
  end loop;

  return pg_catalog.jsonb_build_object(
    'approvals_withdrawn',pg_catalog.to_jsonb(v_approval_ids),
    'family_events_written',pg_catalog.to_jsonb(v_event_ids),
    'family_event_state_admitted',v_state_admitted,
    'family_event_omitted',
      (not v_state_admitted) and coalesce(pg_catalog.cardinality(v_approval_ids),0)>0,
    'family_event_omitted_reason',
      case when (not v_state_admitted)
            and coalesce(pg_catalog.cardinality(v_approval_ids),0)>0
           then 'WEEKLY_SOURCE_FAMILY_EVENT_STATE_NOT_ADMITTED' end);
end;
$function$;

-- ---------------------------------------------------------------------------
-- G3-3 — the first-authorisation withdrawal owner.
--
-- proof/36 section 3 order, and HANDOVER 2 ROUND 5 SECTION A3's five numbered
-- steps, which replace WP-07b's permanent refusal.  Ruling A3, word for word:
--
--   "When safe, one transaction must:
--      1. lock the authorisation, canonical root, current head and relevant
--         payment/currentness guards;
--      2. mark the authorisation withdrawn;
--      3. supersede the committed head with an explicit withdrawal reason and
--         immutable predecessor link;
--      4. invalidate the aligned Candidate/canonical-root scope using the same
--         transaction token and generation required by R25;
--      5. commit the withdrawal, supersession, invalidation and durable replay
--         receipt together."
--
-- Where each step is below:
--
--   step 1  the Candidate serial gate and the rotation lock set through
--           interface I-1 (job type WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_
--           WITHDRAWAL), then FOR UPDATE on the live root-authorisation row and
--           on any COMMITTED_CURRENT head of the family, then the context and
--           the freeze census re-read UNDER those locks, then W1 to W11.
--           Heads are locked AFTER the family and Timesheet locks, which is the
--           order the publication coordinator uses too, so the two owners
--           cannot invert on each other.
--   step 2  `public.timesheet_unauthorise_atomic` exactly once, requiring
--           {ok:true}, and the withdrawal marks on the live generation.
--   step 3  the head supersession, with `superseded_reason =
--           'FIRST_AUTHORISATION_WITHDRAWN'` and `superseded_by_withdrawal_id`
--           naming the receipt, whose own `predecessor_head_id` is the
--           immutable link back.  Both are immutable once written: the relation
--           refuses to change them and refuses to revive the head.
--   step 4  one aligned invalidation for the pair (Candidate, canonical root)
--           with the single transaction token, then the R25 contract assert.
--   step 5  the receipt row, and everything above, in ONE transaction with no
--           EXCEPTION handler anywhere, so it all commits or none of it does.
--
-- WHAT THIS OWNER STILL NEVER DOES.  It writes no Draft, item, reservation,
-- transfer, Case, recovery, settlement or remittance row; it touches no invoice
-- object (DEC-061 option A); it does not cancel, reinterpret or reclassify any
-- payment; and it does not change the Gate 4 Workbench selector, which goes on
-- consuming only current committed heads.  Where any payment work or ambiguous
-- money effect has crossed the boundary, W2 to W10 refuse and NOTHING here runs
-- at all -- in particular the head is not superseded.
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_first_authorisation_withdraw_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_signature text;
  v_actor public.tms_users%rowtype;
  v_recorded jsonb;
  v_context jsonb;
  v_candidate_id uuid;
  v_lock jsonb;
  v_census jsonb;
  v_members uuid[];
  v_verdict jsonb;
  v_canonical uuid;
  v_authorisation jsonb;
  v_authorisation_id uuid;
  v_token uuid;
  v_token_again uuid;
  v_pre_jobs uuid[];
  v_unauthorise jsonb;
  v_invalidation jsonb;
  v_contract jsonb;
  v_protected jsonb;
  v_head jsonb;
  v_head_id uuid;
  v_head_locked uuid;
  v_head_lock_count integer;
  v_receipt_id uuid;
  v_canonical_request jsonb;
  v_digest bytea;
  v_now timestamptz;
  v_rows integer;
  v_result jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  v_signature:=nullif(pg_catalog.btrim(coalesce(p_expected_row_signature,'')),'');
  if p_timesheet_id is null or p_actor_user_id is null or v_signature is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID','retryable',false,
      'reason','TIMESHEET_ACTOR_AND_EXPECTED_ROW_SIGNATURE_REQUIRED');
  end if;

  select * into v_actor from public.tms_users where id=p_actor_user_id;
  if not found or not coalesce(v_actor.is_active,false) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_UNAUTHORISE_ACTOR_INVALID','retryable',false,
      'reason','ACTOR_NOT_FOUND_OR_INACTIVE');
  end if;

  -- ---- section 5 step 1 / ruling A3 step 5: exact replay first, before the
  -- gate and the locks.  An exact replay returns the durable receipt's own
  -- recorded result and calls nothing.  A CONFLICTING replay -- the same replay
  -- identity presented with a different physical Timesheet expectation -- and a
  -- receipt that no longer rebuilds to its own digest both refuse, permanently,
  -- for manual review, and write no lifecycle row.
  v_recorded:=private.weekly_source_first_authorisation_withdrawal_recorded_v1(
    p_timesheet_id,v_signature,p_expected_timesheet_id);
  if v_recorded is not null then
    if coalesce((v_recorded->>'ok')::boolean,false) is not true then
      perform public._audit_insert(
        'timesheets',p_timesheet_id::text,
        'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
        null,
        pg_catalog.jsonb_build_object(
          'expected_row_signature',v_signature,
          'expected_timesheet_id',p_expected_timesheet_id,
          'code',v_recorded->>'code','reason',v_recorded->>'reason',
          'receipt_id',v_recorded->>'receipt_id',
          'stage','REPLAY'),
        'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
      return v_recorded;
    end if;
    return v_recorded||pg_catalog.jsonb_build_object('replayed',true);
  end if;

  -- One plain read so the gate can be pinned to a Candidate.  No write.
  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'expected_timesheet_id',p_expected_timesheet_id,
        'code',v_context->>'code','reason',v_context->>'reason',
        'stage','IDENTITY'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return v_context||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;
  v_candidate_id:=(v_context->>'candidate_id')::uuid;

  -- ---- ruling A3 step 1, first half: the Candidate serial gate, then the
  -- rotation lock set, both through interface I-1 in the deadlock-free order of
  -- proof/34 section 5.
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate_id,
    array[p_timesheet_id]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',
    pg_catalog.gen_random_uuid(),
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL');
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;

  -- ---- ruling A3 step 1, second half: the currentness guards.  The live
  -- root-authorisation row and any committed current head of the family are
  -- taken FOR UPDATE, by PREDICATE rather than by an id read earlier, so a
  -- publication that committed while this call was resolving identity is seen
  -- and locked rather than missed.  Heads are locked after the family and
  -- Timesheet locks, matching the publication coordinator's own order.
  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'code',v_context->>'code','reason',v_context->>'reason',
        'stage','IDENTITY_UNDER_LOCK'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return v_context||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;

  v_canonical:=(v_context->>'canonical_timesheet_id')::uuid;
  v_authorisation:=v_context->'authorisation';
  select pg_catalog.array_agg(member_element.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_context->'member_timesheet_ids')
    as member_element(value);
  v_members:=coalesce(v_members,array[]::uuid[]);

  if pg_catalog.jsonb_typeof(coalesce(v_authorisation,'null'::jsonb))='object' then
    v_authorisation_id:=(v_authorisation->>'id')::uuid;
    perform 1
    from public.weekly_source_root_authorisations as authorisation_row
    where authorisation_row.id=v_authorisation_id
      and authorisation_row.withdrawn_at_utc is null
    for update;
  end if;

  -- Explicit cardinality, never `limit 1`: two committed current heads for one
  -- family is impossible under the partial unique index, and if it ever happened
  -- W11 refuses on the count rather than this statement picking one.
  -- `FOR UPDATE` cannot appear in the same query level as an aggregate, so the
  -- lock is taken in a CTE and counted outside it.
  with locked_head as (
    select head_row.id
    from public.weekly_source_entitlement_heads as head_row
    where head_row.state='COMMITTED_CURRENT'
      and (pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(coalesce(v_context->>'family_booking_id',''))
           or head_row.root_timesheet_id=any(v_members))
    for update
  )
  select pg_catalog.count(*)::integer,pg_catalog.min(locked_head.id::text)::uuid
    into v_head_lock_count,v_head_locked
  from locked_head;

  -- W1 to W11 under those locks, over the complete family.
  v_census:=private.weekly_source_freeze_census_v1(v_candidate_id,v_members);
  v_verdict:=private.weekly_source_first_authorisation_withdraw_checks_v1(
    v_context,v_census,p_expected_timesheet_id,v_signature);

  if coalesce((v_verdict->>'ok')::boolean,false) is not true then
    -- UNA-010: refused with no lifecycle write and an audit row.
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'expected_timesheet_id',p_expected_timesheet_id,
        'candidate_id',v_candidate_id,
        'family_booking_id',v_context->>'family_booking_id',
        'canonical_timesheet_id',v_canonical,
        'code',v_verdict->>'code',
        'refusal_nature',v_verdict->>'refusal_nature',
        'refusal_message',v_verdict->>'refusal_message',
        'failed_checks',v_verdict->'failed_checks',
        'census_result',v_verdict->>'census_result',
        'stage','CHECKS'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code',v_verdict->>'code',
      'refusal_nature',v_verdict->>'refusal_nature',
      'refusal_message',v_verdict->>'refusal_message',
      'retryable',coalesce((v_verdict->>'retryable')::boolean,false),
      -- An INTEGRITY refusal is the package's "Office review" disposition, and
      -- round 5 section A5 requires the finance-case write-off in particular to
      -- be routed to review rather than presented as a retry.
      'review_required',coalesce(v_verdict->>'refusal_nature','')='INTEGRITY',
      'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical,
      'family_booking_id',v_context->>'family_booking_id',
      'checks',v_verdict->'checks',
      'failed_checks',v_verdict->'failed_checks',
      'census_result',v_verdict->>'census_result',
      'census_class_counts',v_verdict->'census_class_counts');
  end if;

  -- The head the verdict decided may be superseded, re-proved against the row
  -- this transaction actually holds FOR UPDATE.  A disagreement means the head
  -- moved between the lock and the verdict, which cannot happen under these
  -- locks; it fails closed rather than being written through.
  v_head:=case when pg_catalog.jsonb_typeof(coalesce(v_verdict->'head_supersession','null'::jsonb))='object'
               then v_verdict->'head_supersession' end;
  v_head_id:=(v_head->>'head_id')::uuid;
  if v_head_id is distinct from v_head_locked
     or v_head_lock_count is distinct from (case when v_head_id is null then 0 else 1 end) then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_LOCK_DISAGREEMENT'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_LOCK_DISAGREEMENT',
              'locked_head_id',v_head_locked,
              'locked_head_count',v_head_lock_count,
              'verdict_head_id',v_head_id)::text;
  end if;

  -- ---- ruling A3 step 4: the controlling transaction token is established
  -- before the unauthorisation so that the ordinary Unauthorise trigger route
  -- and every registered DIRTY_TRIGGER path share it (round 4 ruling 6 point 1).
  v_token:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token is null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
              'context','WITHDRAWAL')::text;
  end if;

  -- Every Workbench job already live when the withdrawal starts.  In production
  -- the Candidate serial gate means there are none for this Candidate; in a
  -- rollback-contained proof the fixture's own writes create some.  Bounded by
  -- the live queue, never by the whole table.
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[])
    into v_pre_jobs
  from public.banking_pay_workbench_jobs as job_row
  where job_row.status in ('QUEUED','RUNNING');

  -- ---------------------------------------------------------------------
  -- ORDERING RULE — WHY THE WRITES BELOW MUST NOT BE SEPARATED
  -- (WP-03 handoff N16, carried into WP-07b and kept by WP-07c.)
  --
  -- The rule, in plain English.  The Timesheet must never be observable as
  -- UNAUTHORISED while its Weekly Source authorisation record is still LIVE,
  -- and the entitlement head must never be observable as SUPERSEDED while the
  -- Timesheet is still authorised.  Those facts are what
  -- `private.weekly_source_managed_root_guard_v1` and the Gate 4 Workbench
  -- selector read, and a reader that saw a contradictory pair would be told
  -- something untrue about a root.  So all of them must be written in ONE
  -- transaction -- unless they are provably unobservable apart, which is the
  -- case here.
  --
  -- Why the present order is SAFE, and it is a property of PostgreSQL rather
  -- than of this code:
  --
  --   * every write below is in ONE transaction, and this owner has NO
  --     exception handler anywhere, so there is no sub-transaction that could
  --     commit one without the others;
  --   * PostgreSQL offers no dirty read at any isolation level — READ
  --     UNCOMMITTED is mapped to READ COMMITTED — so no concurrent session can
  --     see the order of statements inside this transaction.  Proved at READ
  --     COMMITTED and REPEATABLE READ, in both directions;
  --   * the one remaining way the window could be seen is an IN-TRANSACTION
  --     reader: a trigger that calls the managed-root guard or the selector
  --     while this owner runs.  Every non-internal trigger on
  --     `public.timesheets`, `public.timesheets_financials`,
  --     `public.contract_weeks` and `public.weekly_source_entitlement_heads`
  --     was enumerated and NONE calls
  --     `private.weekly_source_managed_root_guard_v1`,
  --     `…_guard_decision_v1` or the Gate 4 selector.  That matches open ruling
  --     OR-2, which explicitly does NOT build a trigger-level backstop on those
  --     tables.
  --
  -- WHAT WOULD BREAK IT, and therefore what must not be done without reordering
  -- first:
  --
  --   1. splitting this sequence across two transactions, or introducing an
  --      intermediate COMMIT, an autonomous transaction or a dblink/background
  --      call between any two of the writes;
  --   2. adding an `EXCEPTION` handler around any of them, which puts it in a
  --      sub-transaction that can roll back on its own;
  --   3. OPEN RULING OR-2 being decided the other way — that is, a guard
  --      trigger being attached to `public.timesheets` or
  --      `public.timesheets_financials`.  The order would then be observable
  --      INSIDE this transaction and reordering becomes MANDATORY, not merely
  --      prudent: the withdrawal marks would have to be written before the call
  --      to `public.timesheet_unauthorise_atomic` below.
  --
  -- Section 16 of this package's verifier is the executed guard on points 1 and
  -- 2: it fails if these writes stop sharing one transaction, if an exception
  -- handler appears, or if a concurrent reader can ever see a live
  -- authorisation record on an unauthorised Timesheet.
  -- ---------------------------------------------------------------------
  -- WP-24, 18 September 2026.  POINT 3 ABOVE HAS NOW HAPPENED, so the
  -- reordering this note calls MANDATORY is done here rather than left prudent.
  --
  -- Gate 13 hostile review finding F1 (CRITICAL) proved that the ORDINARY
  -- Unauthorise owner, which the Office's own `/unauthorise` route calls, left a
  -- committed entitlement head current on a managed root and paid it in place of
  -- what the Office authorised next: GBP 90.00 / 9 h paid against GBP 140.00 /
  -- 14 h authorised, executed with the real Gate 4 selector.  The fix is a
  -- BEFORE UPDATE guard trigger on `public.timesheets.authorised_at_server`
  -- (`supabase/repeatable/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql`),
  -- which HANDOVER 2 round-5 Part D authorises when it rules OR-2 CONFIRMED:
  -- "If the guard attaches to Timesheet, financial or contract-week writes, the
  -- withdrawal owner obeys the same canonical lock order as Part A3."
  --
  -- That trigger IS the in-transaction reader point 3 names, so the order below
  -- became observable INSIDE this transaction and the withdrawal marks now go
  -- FIRST.  Nothing else about this sequence changes: it is still one
  -- transaction, still has no exception handler anywhere, and the two invariants
  -- at the head of this note are both STRENGTHENED rather than weakened - the
  -- Timesheet is now never observable as unauthorised while its authorisation
  -- record is live even to an in-transaction reader, and the head is still
  -- superseded only after the Timesheet has been unauthorised.
  --
  -- It is also what ruling A3 itself describes: "marks the authorisation
  -- withdrawn" is step 2 and the head supersession is step 3, so the two halves
  -- of step 2 are now adjacent and both precede step 3.
  --
  -- `v_now` moves up with the marks it stamps.  It is read after this point by
  -- the protected-hours withdrawal, the replay receipt and the supersession, and
  -- by nothing before it.
  -- ---------------------------------------------------------------------
  v_now:=pg_catalog.clock_timestamp();

  -- Ruling A3 step 2, second half (now written FIRST): the withdrawal marks on
  -- the live root-authorisation generation and nothing else on it; the head
  -- pointer is cleared in the SAME statement.
  --
  -- Decision D8 and proof/36 section 5.6.  The check constraint
  -- `withdrawn_at_utc IS NULL OR current_entitlement_head_id IS NULL` is
  -- evaluated on the finished row, so a two-statement version fails; this one
  -- does not.
  --
  -- WP-24: this is also the STATE that tells this owner apart from an ordinary
  -- Unauthorise at the guard trigger below.  Decision D8 makes `managed` the
  -- conjunction of a LIVE generation and a currently authorised Timesheet, so
  -- once this statement has run the root is not managed and the guard permits
  -- the unauthorisation that follows.  An ordinary Unauthorise never runs this
  -- statement, so its generation is still live and it is refused.  The guard
  -- asks nothing about who is calling.
  update public.weekly_source_root_authorisations
     set withdrawn_at_utc=v_now,
         withdrawn_by_user_id=p_actor_user_id,
         current_entitlement_head_id=null
   where id=v_authorisation_id
     and withdrawn_at_utc is null;
  get diagnostics v_rows=row_count;
  if v_rows<>1 then
    raise exception 'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_FAILED',
              'root_authorisation_id',v_authorisation_id,
              'rows',v_rows)::text;
  end if;

  -- Ruling A3 step 2, first half: the UNCHANGED owner, exactly once, requiring
  -- {ok:true}.
  v_unauthorise:=public.timesheet_unauthorise_atomic(
    p_timesheet_id=>v_canonical,
    p_expected_timesheet_id=>v_canonical,
    p_actor_user_id=>p_actor_user_id,
    p_expected_row_signature=>v_signature);

  if coalesce((v_unauthorise->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED',
              'timesheet_id',v_canonical,
              'owner_result',v_unauthorise)::text;
  end if;

  -- Ruling A3 step 4: one aligned invalidation for the pair (Candidate,
  -- canonical root) only, and success required.  HANDOVER 2 round 3 answer 10:
  -- not every physical family member; the installed normaliser expands the pair
  -- to the full physical rotation family.
  v_invalidation:=private.pay_workbench_scope_invalidate_v1(
    p_candidate_ids=>array[v_candidate_id]::uuid[],
    p_timesheet_ids=>array[v_canonical]::uuid[],
    p_reason=>'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',
    p_scope_change_tx_token=>v_token,
    p_payload_json=>pg_catalog.jsonb_build_object(
      'weekly_source_first_authorisation_withdrawal',pg_catalog.jsonb_build_object(
        'timesheet_id',v_canonical,
        'family_booking_id',v_context->>'family_booking_id',
        'authorisation_generation',v_authorisation->>'authorisation_generation',
        'superseded_head_id',v_head_id,
        'expected_row_signature',v_signature)));

  if coalesce((v_invalidation->>'ok')::boolean,false) is not true
     or (v_invalidation->>'candidate_count')::integer<>1
     or (v_invalidation->>'scope_change_tx_token')::uuid is distinct from v_token
     or coalesce((v_invalidation->>'job_inserted_count')::integer,0)
        +coalesce((v_invalidation->>'job_coalesced_count')::integer,0)<1 then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_INVALIDATION_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_INVALIDATION_FAILED',
              'invalidation',v_invalidation)::text;
  end if;

  v_token_again:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token_again is distinct from v_token then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
              'reason','TRANSACTION_TOKEN_CHANGED',
              'context','WITHDRAWAL')::text;
  end if;

  v_contract:=private.weekly_source_invalidation_contract_assert_v1(
    v_candidate_id,array[v_canonical]::uuid[],v_token,'WITHDRAWAL',v_pre_jobs);

  -- WP-24: ruling A3 step 2's withdrawal marks USED TO SIT HERE.  They are now
  -- written before the call to `public.timesheet_unauthorise_atomic` above, for
  -- the reason given in full at that point: the guard trigger on
  -- `public.timesheets.authorised_at_server` is the in-transaction reader that
  -- the ORDERING RULE note lists as making this reordering mandatory.  Nothing
  -- was added or removed - the same single statement, with the same three
  -- assignments and the same one-row assertion, in a different place.

  -- proof/36 section 5 step 7: the approved protected-hours decision, marked
  -- withdrawn and never deleted.
  v_protected:=private.weekly_source_first_authorisation_withdraw_protected_v1(
    v_members,p_actor_user_id,v_now);

  -- ---- ruling A3 step 5: the durable replay receipt, and step 3: the head
  -- supersession that points at it.
  --
  -- The receipt id is generated first so that the recorded result and the
  -- returned result are the SAME object: an exact replay hands back exactly
  -- what this call returned, receipt id and supersession included.
  v_receipt_id:=pg_catalog.gen_random_uuid();
  v_canonical_request:=private.weekly_source_withdrawal_canonical_request_v1(
    (v_head->>'agency_id')::uuid,
    v_candidate_id,
    (v_context->>'contract_id')::uuid,
    v_canonical,
    v_context->>'family_booking_id',
    (v_context->>'canonical_version')::integer,
    p_timesheet_id,
    p_expected_timesheet_id,
    v_authorisation_id,
    (v_authorisation->>'authorisation_generation')::integer,
    v_signature,
    v_head_id,
    (v_head->>'head_revision')::bigint);
  v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical_request);

  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'withdrawn',true,'replayed',false,
    -- WP-12 handoff N2 reads the response as
    -- `{ ok, withdrawn, code, refusal_nature, refusal_message, retryable }`, so
    -- the SUCCESS shape carries those keys as explicit nulls rather than
    -- omitting them.  An absent key and a null key are different things to a
    -- browser, and the screen should never have to tell them apart.
    'code',null::text,
    'refusal_nature',null::text,
    'refusal_message',null::text,
    'retryable',false,
    'review_required',false,
    'timesheet_id',p_timesheet_id,
    'canonical_timesheet_id',v_canonical,
    'family_booking_id',v_context->>'family_booking_id',
    'timesheet_version',(v_context->>'canonical_version')::integer,
    'candidate_id',v_candidate_id,
    'expected_row_signature',v_signature,
    'root_authorisation_id',v_authorisation_id,
    'authorisation_generation',(v_authorisation->>'authorisation_generation')::integer,
    'entitlement_head_cleared',(v_authorisation->>'current_entitlement_head_id')::uuid,
    'withdrawn_at_utc',v_now,
    'withdrawn_by_user_id',p_actor_user_id,
    'scope_change_tx_token',v_token,
    'withdrawal_receipt_id',v_receipt_id,
    'request_digest',pg_catalog.encode(v_digest,'hex'),
    'head_superseded',v_head_id is not null,
    'head_supersession',case when v_head_id is null then null
      else pg_catalog.jsonb_build_object(
        'head_id',v_head_id,
        'head_revision',(v_head->>'head_revision')::bigint,
        'state_before',v_head->>'state_before',
        'state_after','SUPERSEDED',
        'superseded_reason','FIRST_AUTHORISATION_WITHDRAWN',
        'certified_zero',(v_head->>'certified_zero')::boolean,
        'component_count',(v_head->>'component_count')::integer,
        'predecessor_link_receipt_id',v_receipt_id) end,
    'invalidation',pg_catalog.jsonb_build_object(
      'ok',true,
      'candidate_count',(v_invalidation->>'candidate_count')::integer,
      'job_inserted_count',coalesce((v_invalidation->>'job_inserted_count')::integer,0),
      'job_coalesced_count',coalesce((v_invalidation->>'job_coalesced_count')::integer,0)),
    'invalidation_contract',v_contract,
    'protected_hours',v_protected,
    'unauthorise_result',pg_catalog.jsonb_build_object(
      'ok',true,
      'operation',v_unauthorise->>'operation',
      'timesheet_id',v_unauthorise->>'timesheet_id',
      'contract_week_id',v_unauthorise->>'contract_week_id',
      'processing_status',v_unauthorise->>'processing_status'),
    'checks',v_verdict->'checks',
    'census_result',v_verdict->>'census_result',
    'census_class_counts',v_verdict->'census_class_counts');

  insert into private.weekly_source_first_authorisation_withdrawal_receipts(
    id,agency_id,candidate_id,contract_id,root_timesheet_id,root_family_booking_id,
    root_timesheet_version,requested_timesheet_id,expected_timesheet_id,
    root_authorisation_id,authorisation_generation,expected_row_signature,
    request_digest,predecessor_head_id,predecessor_head_revision,
    predecessor_head_state_before,predecessor_head_certified_zero,head_superseded,
    scope_change_tx_token,withdrawn_at_utc,withdrawn_by_user_id,checks_json,result_json
  ) values (
    v_receipt_id,
    (v_head->>'agency_id')::uuid,
    v_candidate_id,
    (v_context->>'contract_id')::uuid,
    v_canonical,
    v_context->>'family_booking_id',
    (v_context->>'canonical_version')::integer,
    p_timesheet_id,
    p_expected_timesheet_id,
    v_authorisation_id,
    (v_authorisation->>'authorisation_generation')::integer,
    v_signature,
    v_digest,
    v_head_id,
    (v_head->>'head_revision')::bigint,
    case when v_head_id is null then null else v_head->>'state_before' end,
    case when v_head_id is null then null else (v_head->>'certified_zero')::boolean end,
    v_head_id is not null,
    v_token,
    v_now,
    p_actor_user_id,
    coalesce(v_verdict->'checks','[]'::jsonb),
    v_result);

  -- Ruling A3 step 3.  This is the ONLY statement in the Weekly Source
  -- withdrawal path that writes a head row, and it can only be reached when
  -- W1 to W11 have all passed, which means no Draft dependency, payment item,
  -- reservation, bank transfer, execution, provider attempt, settlement,
  -- remittance, recovery, invoice or other committed financial effect stands
  -- against the head or the root.  It sets the explicit reason and the link to
  -- the receipt whose `predecessor_head_id` is the immutable link back; both are
  -- frozen from here on by
  -- `private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1`,
  -- which also makes reviving the head impossible.
  --
  -- The head's own `scope_change_tx_token` and `publication_receipt_digest` are
  -- deliberately NOT touched: they are the publication's evidence, and the
  -- deferred `weekly_source_entitlement_head_receipt_assert` re-runs on this
  -- update and would refuse if they moved.
  if v_head_id is not null then
    update public.weekly_source_entitlement_heads
       set state='SUPERSEDED',
           superseded_at_utc=v_now,
           superseded_reason='FIRST_AUTHORISATION_WITHDRAWN',
           superseded_by_withdrawal_id=v_receipt_id
     where id=v_head_id
       and state='COMMITTED_CURRENT';
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_SUPERSESSION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_SUPERSESSION_FAILED',
                'head_id',v_head_id,'rows',v_rows)::text;
    end if;

    -- Prove the post-state inside the transaction rather than asserting it in
    -- prose: no committed current head may survive for this family.
    select pg_catalog.count(*)::integer into v_rows
    from public.weekly_source_entitlement_heads as head_row
    where head_row.state='COMMITTED_CURRENT'
      and (pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(coalesce(v_context->>'family_booking_id',''))
           or head_row.root_timesheet_id=any(v_members));
    if v_rows<>0 then
      raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_STILL_CURRENT'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_STILL_CURRENT',
                'head_id',v_head_id,'remaining',v_rows)::text;
    end if;
  end if;

  -- proof/36 section 5 step 4: the withdrawal recorded through the existing
  -- audit owner as well, so the previous authorisation and the withdrawal are
  -- both visible in Audit.  The durable replay copy is the receipt above; this
  -- is the human record and no longer the replay source (WP-07b finding F4).
  perform public._audit_insert(
    'timesheets',v_canonical::text,
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN',
    pg_catalog.jsonb_build_object(
      'authorised_at_server',v_context#>>'{root,authorised_at_server}',
      'current_entitlement_head_id',v_authorisation->>'current_entitlement_head_id',
      'authorisation_generation',(v_authorisation->>'authorisation_generation')::integer),
    pg_catalog.jsonb_build_object(
      'expected_row_signature',v_signature,
      'expected_timesheet_id',p_expected_timesheet_id,
      'requested_timesheet_id',p_timesheet_id,
      'withdrawal_receipt_id',v_receipt_id,
      'result',v_result),
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);

  return v_result;
end;
$function$;

-- ---------------------------------------------------------------------------
-- THE SERVICE ENTRY POINT THE OFFICE SCREEN CALLS.
--
-- WP-12 handoff N2: the Office `Unauthorise` control on a Weekly-Source-managed
-- root has nothing behind it.  The withdrawal owner exists and works, but it
-- takes POSITIONAL arguments, where every routed Weekly Source owner takes one
-- `jsonb` request, so the broker's own `callRpc` (which always sends
-- `{ p_request: … }`) could not dispatch it even if it were listed.
--
-- This wrapper is that missing entry point, coded to the FIXED shape WP-12 has
-- already coded against (briefs Part 3: code to the fixed shape, report the
-- problem, do not silently change it):
--
--   POST /api/weekly-source/v1/commands
--   { "action": "WITHDRAW_FIRST_AUTHORISATION",
--     "payload": { "timesheet_id": "<uuid>",
--                  "expected_timesheet_id": "<uuid>",
--                  "expected_row_signature": "<string>" } }
--
-- `actor_user_id` is injected by the broker and is therefore an ALLOWED key of
-- the request but never a browser-supplied one; the broker overwrites whatever
-- the browser sent (`routes.js` builds `{ ...payload, actor_user_id: user.id }`).
--
-- Permission: open ruling OR-5, confirmed in round 5 — "Reuse the ordinary
-- Timesheet-authorise permission, with the same actor, tenant and ownership
-- checks.  Do not introduce a broader Weekly Source Office permission."  That
-- permission is enforced where every other Weekly Source owner enforces it: the
-- function is `SECURITY DEFINER`, revoked from `PUBLIC`, `anon` and
-- `authenticated`, granted ONLY to `service_role`, and refuses any caller whose
-- request role is not `service_role`; the actor is checked for existence and
-- activity by the owner below, and the ordinary Authorise owner's own actor,
-- tenant and ownership guards run inside `public.timesheet_unauthorise_atomic`.
-- This wrapper adds no new privilege of any kind.
--
-- It is a THIN adapter: it validates the request shape, rejects unknown fields,
-- and calls the owner.  It contains no check, no lock and no money rule of its
-- own, so the owner stays the single place the rules live.
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_first_authorisation_withdraw_request_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','timesheet_id','expected_timesheet_id','expected_row_signature'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_timesheet uuid;
  v_expected uuid;
  v_signature text;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=nullif(pg_catalog.btrim(coalesce(p_request->>'actor_user_id','')),'')::uuid;
    v_timesheet:=nullif(pg_catalog.btrim(coalesce(p_request->>'timesheet_id','')),'')::uuid;
    v_expected:=nullif(pg_catalog.btrim(coalesce(p_request->>'expected_timesheet_id','')),'')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_VALUE_INVALID' using errcode='22023';
  end;
  v_signature:=nullif(pg_catalog.btrim(coalesce(p_request->>'expected_row_signature','')),'');

  return public.weekly_source_first_authorisation_withdraw_v1(
    v_timesheet,v_expected,v_signature,v_actor);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Ownership, privileges and comments.
-- ---------------------------------------------------------------------------
alter function private.weekly_source_withdrawal_uuid_array_v1(jsonb) owner to postgres;
alter function private.weekly_source_withdrawal_transfer_scope_v1(uuid,uuid[]) owner to postgres;
alter function private.weekly_source_first_authorisation_context_v1(uuid) owner to postgres;
alter function private.weekly_source_first_authorisation_withdraw_checks_v1(jsonb,jsonb,uuid,text)
  owner to postgres;
alter function private.weekly_source_root_authorisation_signature_v1(
  uuid,uuid,text,integer,integer,text,text[]) owner to postgres;
alter function private.weekly_source_root_protected_decision_hashes_v1(uuid[]) owner to postgres;
alter function private.weekly_source_root_agency_id_v1(uuid[]) owner to postgres;
alter function private.weekly_source_withdrawal_canonical_request_v1(
  uuid,uuid,uuid,uuid,text,integer,uuid,uuid,uuid,integer,text,uuid,bigint) owner to postgres;
alter function private.weekly_source_first_authorisation_withdrawal_recorded_v1(uuid,text,uuid)
  owner to postgres;
alter function private.weekly_source_invalidation_contract_assert_v1(uuid,uuid[],uuid,text,uuid[])
  owner to postgres;
alter function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) owner to postgres;
alter function private.weekly_source_first_authorisation_withdraw_protected_v1(uuid[],uuid,timestamptz)
  owner to postgres;
alter function public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid) owner to postgres;
alter function public.weekly_source_first_authorisation_withdraw_available_v1(uuid) owner to postgres;
alter function public.weekly_source_first_authorisation_withdraw_request_v1(jsonb) owner to postgres;
alter function public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)
  owner to postgres;

revoke all on function private.weekly_source_withdrawal_uuid_array_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_withdrawal_transfer_scope_v1(uuid,uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_first_authorisation_context_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_first_authorisation_withdraw_checks_v1(jsonb,jsonb,uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_root_authorisation_signature_v1(
  uuid,uuid,text,integer,integer,text,text[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_root_protected_decision_hashes_v1(uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_root_agency_id_v1(uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_withdrawal_canonical_request_v1(
  uuid,uuid,uuid,uuid,text,integer,uuid,uuid,uuid,integer,text,uuid,bigint)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_first_authorisation_withdrawal_recorded_v1(uuid,text,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invalidation_contract_assert_v1(uuid,uuid[],uuid,text,uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_first_authorisation_withdraw_protected_v1(uuid[],uuid,timestamptz)
  from public,anon,authenticated,service_role;

revoke all on function public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_first_authorisation_withdraw_available_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)
  from public,anon,authenticated,service_role;

grant execute on function public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)
  to service_role;
grant execute on function public.weekly_source_first_authorisation_withdraw_available_v1(uuid)
  to service_role;
grant execute on function public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)
  to service_role;

comment on function private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb) is
  'Interface I-6: Weekly Source first-authorisation core. Assumes the I-1 rotation locks are already held, calls the unchanged ordinary Authorise owner exactly once and inserts the root authorisation generation of proof/34 section 4. Calls no invalidator.';
comment on function public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid) is
  'Service-only Office first authorisation of a Weekly Source root: the Candidate serial gate and rotation lock set (interface I-1, job type WORKBENCH_CANDIDATE_FIRST_AUTHORISATION), then interface I-6.';
comment on function public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid) is
  'Service-only first-authorisation withdrawal (proof/36 and HANDOVER 2 round-5 ruling A3). Exact replay from the durable receipt, Candidate serial gate, rotation lock set, FOR UPDATE on the live authorisation generation and on any committed current head, W1 to W11 over the complete family, one call to the unchanged public.timesheet_unauthorise_atomic, one aligned invalidation for the pair (Candidate, canonical root) under one transaction token, the root-authorisation withdrawal marks with the head pointer cleared, the protected-hours withdrawal, the durable replay receipt and the atomic supersession of the committed head with an explicit withdrawal reason and an immutable predecessor link, all in one transaction. A committed head is superseded ONLY when every check proves no payment work and no ambiguous money effect crossed the boundary; a missing or ambiguous result refuses. Writes nothing in Banking Pay, cancels and reinterprets no payment, touches no invoice object and does not change the Workbench selector.';
comment on function public.weekly_source_first_authorisation_withdraw_request_v1(jsonb) is
  'Service-only jsonb-request entry point for the Office Unauthorise control on a Weekly-Source-managed root (WP-12 handoff N2). Accepts { actor_user_id, timesheet_id, expected_timesheet_id, expected_row_signature }, refuses any other field, and calls public.weekly_source_first_authorisation_withdraw_v1. It holds no check, no lock and no money rule of its own and grants no new privilege: open ruling OR-5, the ordinary Timesheet-authorise permission.';
comment on function public.weekly_source_first_authorisation_withdraw_available_v1(uuid) is
  'Service-only read-only availability verdict for the Unauthorise control, computed from the same checks W1 to W11 as the withdrawal owner. ok has the same meaning as everywhere else in this package: true means the Unauthorise action is permitted, and it always equals available. A refusal carries code, refusal_nature, refusal_message and retryable.';
comment on function private.weekly_source_first_authorisation_withdrawal_recorded_v1(uuid,text,uuid) is
  'HANDOVER 2 round-5 ruling A3 step 5: the exact replay reads the durable withdrawal receipt, not the audit trail. Returns the recorded result for an exact replay, a permanent integrity refusal for a conflicting replay or a receipt that no longer rebuilds to its own digest, and NULL when there is no receipt.';
comment on function private.weekly_source_withdrawal_canonical_request_v1(
  uuid,uuid,uuid,uuid,text,integer,uuid,uuid,uuid,integer,text,uuid,bigint) is
  'The canonical withdrawal request whose digest binds tenant and agency, the canonical root, the exact authorised physical Timesheet identity, the source generation and revision, and the protected decision fields (HANDOVER 2 round 5, Part E rotation readings). Hashed by the installed proof/32 section 9 encoder; the booking reference alone is never the key.';

notify pgrst, 'reload schema';

commit;
