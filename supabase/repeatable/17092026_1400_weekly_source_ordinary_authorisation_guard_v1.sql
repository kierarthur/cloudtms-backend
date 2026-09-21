-- Repeatable CloudTMS function/trigger authority:
-- weekly_source_ordinary_authorisation_guard_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path and ACL
-- contracts.
--
-- Plan 6.2, Gate 13 hostile review finding F1 (CRITICAL), package WP-24.
--
-- WHAT WAS WRONG.  `public.timesheet_unauthorise_atomic`,
-- `public.timesheet_unauthorise_bulk_atomic`,
-- `public.timesheet_authorise_generic_atomic` and
-- `public.timesheet_authorise_bulk_atomic` are the owners the Office's ordinary
-- Authorise and Unauthorise controls actually call (`broker/src/index.js`
-- `/unauthorise` and the Bulk route).  None of them knows anything about Weekly
-- Source, and contract section 2 freezes all four as CALL-ONLY, so the guard
-- cannot be put inside them.  The result, executed on a build from empty:
--
--   * a managed root with a committed entitlement head of 9 h / GBP 90 is
--     unauthorised by the ordinary owner;
--   * the head stays `COMMITTED_CURRENT` and the authorisation generation stays
--     live, which is exactly the pair the rotation guard reports as
--     `authorisation_record_without_authorised_timesheet`;
--   * the Office restates the week at 14 h / GBP 140 and re-authorises with the
--     ordinary owner, which succeeds;
--   * the Gate 4 Workbench selector still resolves the STALE head, so the Pay
--     Workbench is fed GBP 90.00 / 9 h against GBP 140.00 / 14 h authorised.
--
-- The withdrawal owner refuses to recover that state
-- (`WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE` / W9 `ROOT_NOT_AUTHORISED`), so it is
-- not self-healing: it is a wrong amount paid to a worker.
--
-- WHAT THIS FILE DOES, AND WHY THIS SHAPE.
--
-- `proof/36 section 3`, word for word: the withdrawal owner "is the **only**
-- route by which a Weekly-Source-managed root is unauthorised", and "a direct
-- call while the control is unavailable is refused with no write" (UNA-010,
-- contract G3-4).  So the answer is (b) FAIL CLOSED, not (a) silently re-route:
-- the pack names refusal, and refusal is also what the existing rotation guard
-- already does at all 28 entry points.  No third behaviour is invented here.
--
-- The guard therefore attaches BESIDE the four frozen owners, as a BEFORE
-- UPDATE trigger on the one column that carries the authorisation boundary,
-- `public.timesheets.authorised_at_server`.  HANDOVER 2 round-5 Part D rules
-- open point OR-2 CONFIRMED - "Guard every entry point" - and says in terms:
-- "If the guard attaches to Timesheet, financial or contract-week writes, the
-- withdrawal owner obeys the same canonical lock order as Part A3 (section 1.1,
-- SD-1)."  That is the authority for both halves of WP-24: this trigger, and
-- the matching ordering change in
-- `supabase/repeatable/17092026_0600_weekly_source_first_authorisation_v1.sql`,
-- which that file's own ORDERING RULE note already specifies as MANDATORY once
-- a guard trigger is attached to `public.timesheets`.
--
-- IT IS A STATE TEST, NEVER A CALLER EXEMPTION.  Nothing here asks who is
-- calling.  There is no transaction-local setting, no session GUC, no allowlist
-- of owners and no `current_user` test - a previous fix in this programme was
-- rejected for exempting a caller, and the accepted pattern is the one
-- `public.tsfin_write_current_snapshot_single_bounded` carries at entry point
-- E25 (WP-09b / WP-17b finding F1):
--
--     (guard->>'protected_target_ownership_state') is not null
--     and (prev.id is not null or v_current_ts.authorised_at_server is not null)
--
-- - a property of the ROOT conjoined with a property of what there is to
-- displace.  This file uses the same form and the same narrowed refusal
-- predicate that every call site now uses under HANDOVER 2 round-5 ruling B3.
--
-- WHY THE WITHDRAWAL OWNER IS NOT REFUSED BY IT.  Decision D8: `managed` is the
-- CONJUNCTION of a live `public.weekly_source_root_authorisations` generation
-- for the family's canonical root AND that Timesheet currently being
-- authorised.  With the ordering change in `…_0600_…`, the withdrawal owner
-- marks its generation withdrawn BEFORE it calls
-- `public.timesheet_unauthorise_atomic`, so at the moment of the timesheets
-- write there is no live generation: `managed` is false and the contradiction
-- limb is false, and the write proceeds.  An ORDINARY unauthorise leaves the
-- generation live, so `managed` is true and it refuses.  The two are told apart
-- by the state of the authorisation record, which is the thing the pack says
-- governs, and by nothing else.
--
-- WHAT IS DELIBERATELY NOT CARRIED ACROSS.  E25's third limb, the
-- protected-pay clause, is NOT reproduced here, and that is a decision rather
-- than an oversight.  At E25 "something to displace" is a current
-- `timesheets_financials` row; at the AUTHORISATION boundary it would have to be
-- "the root is already authorised", and that limb would then refuse two
-- legitimate paths this package must not break: the publication coordinator's
-- first authorisation of a genuinely new B root (interface I-6, which runs on a
-- protected family that already has a current financial row), and the
-- withdrawal owner's own unauthorise of a protected managed root.  F1 is a
-- stale ENTITLEMENT HEAD defect; the protected-pay limb belongs to the Part E
-- rotation ruling and to the packages that own it.  This is recorded in
-- `IMPL\reports\WP-24_MANAGED_ROOT_UNAUTHORISE_GUARD.md` as a stated scope
-- boundary, not as a proved property.
--
-- LOCKS BEFORE READS.  An identical guard in this programme was defeated by a
-- race at nine owners because it read the state it was guarding on before it
-- locked it.  `private.weekly_source_ordinary_authorisation_guard_v1` takes
-- `FOR UPDATE` on every `public.weekly_source_root_authorisations` row of the
-- booking family FIRST, and only then calls the STABLE, lock-free
-- `private.weekly_source_managed_root_guard_v1` to read it.  The row of
-- `public.timesheets` being changed is already locked by the UPDATE that fired
-- this trigger, and it is that lock - taken before the trigger body runs - that
-- serialises a concurrent first authorisation, which must authorise the same
-- root before it may insert a generation.
--
-- THIS GUARD WRITES NOTHING.  Contract D11 and D13, carried in G6-11: "The
-- refusal itself writes zero rows in its own transaction: no audit row is
-- written here; where a durable caller exists it records the refusal afterward
-- in a separate transaction."  The refusal raises
-- `WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED` with `errcode 55000`, which is
-- the exact sqlstate and message `broker/src/weekly-source/guard-refusal-record.mjs`
-- already recognises, so the durable caller WP-23 built records this refusal
-- with no further change.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner, and nothing
-- here sets or pre-seeds a Workbench session setting.

\set ON_ERROR_STOP on

begin;

-- Part 1 addendum rule 4: a boolean read from JSON is THREE-valued.  An absent
-- key, a JSON null and a non-boolean all make `not value` evaluate to NULL,
-- which passes an `if not value then refuse` guard - the defect that let WP-05's
-- cross-week invoice move commit without its confirmation.  Every flag this
-- package reads goes through this reader, which gates on `jsonb_typeof` and
-- returns the caller's stated UNSAFE value for absent, null and non-boolean
-- alike.  It never casts a JSON string, so a `"true"` cannot masquerade as a
-- boolean and a `"yes"` cannot raise `22P02` from inside a guard.
create or replace function private.weekly_source_guard_flag_v1(
  p_decision jsonb,
  p_key text,
  p_unsafe_value boolean
) returns boolean
language sql
immutable
as $function$
  select case
    when p_decision is null then p_unsafe_value
    when p_key is null then p_unsafe_value
    when pg_catalog.jsonb_typeof(p_decision->p_key)='boolean'
      then (p_decision->p_key)::boolean
    else p_unsafe_value
  end;
$function$;

-- The decision owner.  Locks first, reads second, decides third, writes never.
--
-- The refusal predicate is HANDOVER 2 round-5 ruling B3's narrowed one, copied
-- from the installed call sites rather than paraphrased:
--
--   (managed and (ok or weekly_source_bound)) or contradiction
--
-- with `managed`, `ok` and `weekly_source_bound` taking TRUE as their unsafe
-- value and `contradiction` taking FALSE.  Read it limb by limb:
--
--   * a Weekly-Source MANAGED root (managed, ok)                -> refuse;
--   * a root the guard CANNOT resolve that is bound or protected
--     (managed = true because it fails closed, ok = false,
--      weekly_source_bound = true)                              -> refuse;
--   * an unrelated UNBOUND ordinary family, including a malformed one
--     (ok = false, weekly_source_bound = false)                 -> PERMIT,
--     which is the whole point of ruling B3: such a family must not acquire a
--     new refusal merely because this feature was installed;
--   * a live authorisation generation on a Timesheet that is not currently
--     authorised                                                -> refuse, and
--     this limb is F1's own end state, so it is what stops an ordinary
--     re-authorise reviving a stale head on a root that is already in the
--     broken pair.
create or replace function private.weekly_source_ordinary_authorisation_guard_v1(
  p_timesheet_id uuid,
  p_booking_id text
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_family_key text:=pg_catalog.btrim(coalesce(p_booking_id,''));
  v_guard jsonb;
  v_managed boolean;
  v_ok boolean;
  v_bound boolean;
  v_contradiction boolean;
  v_refuse boolean;
  v_basis text;
begin
  -- A null id cannot be resolved and cannot be locked: fail closed without
  -- reading anything, the same direction the rotation guard takes for a null id
  -- (review F9).
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'refuse',true,
      'refusal_basis','WEEKLY_SOURCE_MANAGED_ROOT',
      'reason','ROOT_ID_REQUIRED',
      'timesheet_id',null,
      'managed',true,'ok',false,'weekly_source_bound',true,
      'authorisation_record_without_authorised_timesheet',false);
  end if;

  -- ---------------------------------------------------------------------
  -- LOCKS FIRST.  Every authorisation row of the booking family, plus the
  -- requested root's own, are taken FOR UPDATE before a single one of them is
  -- read.  The family is keyed on the TRIMMED booking reference, which is the
  -- family identity the rotation authority uses after schema change S8; a blank
  -- reference has no family identity, so such a row can only be locked through
  -- itself.  No LIMIT and no ORDER BY appears anywhere in this file: cardinality
  -- is decided by the guard's own explicit counts, never by taking whichever row
  -- an ordering happened to return.
  -- ---------------------------------------------------------------------
  perform 1
  from public.weekly_source_root_authorisations authorisation_row
  where authorisation_row.root_timesheet_id in (
          select family_member.timesheet_id
          from public.timesheets family_member
          where v_family_key<>''
            and pg_catalog.btrim(coalesce(family_member.booking_id,''))=v_family_key
          union
          select p_timesheet_id
        )
  for update of authorisation_row;

  -- THEN READ.  `private.weekly_source_managed_root_guard_v1` is STABLE and
  -- takes no lock of its own; the locks above are the ones that make what it
  -- reads stable for the rest of this transaction.
  v_guard:=private.weekly_source_managed_root_guard_v1(p_timesheet_id);

  v_managed:=private.weekly_source_guard_flag_v1(v_guard,'managed',true);
  v_ok:=private.weekly_source_guard_flag_v1(v_guard,'ok',true);
  v_bound:=private.weekly_source_guard_flag_v1(v_guard,'weekly_source_bound',true);
  v_contradiction:=private.weekly_source_guard_flag_v1(
    v_guard,'authorisation_record_without_authorised_timesheet',false);

  v_refuse:=(v_managed and (v_ok or v_bound)) or v_contradiction;

  -- The named basis, in the same order and with the same tokens the installed
  -- call sites use, so Office sees one vocabulary.  HANDOVER 2 round-5 Part E:
  -- the trim-equivalent split family is a canonical booking-reference collision
  -- and must be named as one; WP-03 handoff N20 keeps BOTH the installed token
  -- and the ruled name accepted for one release.
  v_basis:=case
    when v_guard->>'reason' in (
           'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
      then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
    when v_managed and v_ok then 'WEEKLY_SOURCE_MANAGED_ROOT'
    when v_managed then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
    when v_contradiction then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
    else 'WEEKLY_SOURCE_MANAGED_ROOT'
  end;

  return pg_catalog.jsonb_build_object(
    'refuse',v_refuse,
    'refusal_basis',case when v_refuse then v_basis else null end,
    'reason',v_guard->>'reason',
    'timesheet_id',p_timesheet_id,
    'canonical_timesheet_id',v_guard->>'canonical_timesheet_id',
    'family_booking_id',v_guard->>'family_booking_id',
    'managed',v_managed,
    'ok',v_ok,
    'weekly_source_bound',v_bound,
    'authorisation_record_without_authorised_timesheet',v_contradiction);
end;
$function$;

-- The trigger body.  It classifies the transition for the refusal message only:
-- the DECISION above does not read the transition at all, so an ordinary
-- Authorise, an ordinary Unauthorise and an ordinary re-stamp of the
-- authorisation time are all held to the same state test and none of them has a
-- direction-specific exemption.
create or replace function private.weekly_source_ordinary_authorisation_guard_tg_v1()
returns trigger
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_decision jsonb;
  v_transition text;
begin
  v_transition:=case
    when old.authorised_at_server is not null and new.authorised_at_server is null
      then 'UNAUTHORISE'
    when old.authorised_at_server is null and new.authorised_at_server is not null
      then 'AUTHORISE'
    else 'AUTHORISATION_RESTAMP'
  end;

  v_decision:=private.weekly_source_ordinary_authorisation_guard_v1(
    new.timesheet_id,new.booking_id);

  -- Absent, null and non-boolean all take the UNSAFE value, which for `refuse`
  -- is TRUE: a decision this trigger cannot read is a decision it refuses on.
  if private.weekly_source_guard_flag_v1(v_decision,'refuse',true) then
    raise exception 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
              'entry_point','E29:public.timesheets.authorised_at_server',
              'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
              'refusal_basis',v_decision->>'refusal_basis',
              'reason',v_decision->>'reason',
              'transition',v_transition,
              'timesheet_id',new.timesheet_id,
              -- proof/36 section 3 and contract G3-4 / UNA-010: the withdrawal
              -- owner is the ONLY route by which a Weekly-Source-managed root is
              -- unauthorised, so the refusal names it rather than leaving Office
              -- with a bare code.
              'required_route',case when v_transition='UNAUTHORISE'
                then 'public.weekly_source_first_authorisation_withdraw_v1'
                else 'public.weekly_source_first_authorise_v1' end,
              'office_message',case when v_transition='UNAUTHORISE'
                then 'This week is managed by Weekly Source. Withdraw the first '
                     ||'authorisation instead; the ordinary Unauthorise cannot be '
                     ||'used, because it would leave the published entitlement in '
                     ||'place and pay it.'
                else 'This week is managed by Weekly Source and its authorisation '
                     ||'record is not in a state the ordinary Authorise may change. '
                     ||'Office review is required.' end,
              'guard',v_decision)::text;
  end if;

  return new;
end;
$function$;

-- The attachment.  Narrow on purpose:
--
--   * BEFORE UPDATE, so the refusal happens before this owner's first write and
--     while the UPDATE already holds the row lock on the Timesheet;
--   * FOR EACH ROW with a WHEN clause on the authorisation boundary alone, so
--     the guard body never runs for the many other updates of this hot table.
--     The WHEN clause is a property of the row transition, not of the caller;
--   * `public.timesheets` only.  The matching `timesheets_financials`
--     `authorised_at_utc` write is already guarded at entry points E7
--     (`public.tsfin_prepare_write`, `public.tsfin_mark_revoked`) and E25
--     (`public.tsfin_write_current_snapshot_single_bounded`) by the G6-11 work,
--     and in both ordinary owners the `public.timesheets` write shares the
--     transaction with it, so a second trigger would add no coverage and would
--     put a guard on a second hot table for nothing.
drop trigger if exists weekly_source_managed_root_authorisation_guard_bu
  on public.timesheets;
create trigger weekly_source_managed_root_authorisation_guard_bu
  before update on public.timesheets
  for each row
  when (old.authorised_at_server is distinct from new.authorised_at_server)
  execute function private.weekly_source_ordinary_authorisation_guard_tg_v1();

alter function private.weekly_source_guard_flag_v1(jsonb,text,boolean) owner to postgres;
alter function private.weekly_source_ordinary_authorisation_guard_v1(uuid,text) owner to postgres;
alter function private.weekly_source_ordinary_authorisation_guard_tg_v1() owner to postgres;

-- The Weekly Source ACL contract (`15092026_1534_weekly_source_acl_contract_v1.sql`)
-- revokes every `weekly_source_%` routine in `public` and `private` from
-- PUBLIC, `anon`, `authenticated` and `service_role`, and its verifier RAISES if
-- any grantee other than the owner survives on a routine that is not in the
-- service-RPC allowlist.  None of these three is a service RPC, so all three are
-- owner-only and NOTHING is granted.  A trigger function needs no EXECUTE grant
-- at fire time - PostgreSQL checks that privilege when the trigger is created,
-- not when it fires - and in every path that matters the caller is already
-- inside a SECURITY DEFINER owner running as `postgres`.
revoke all on function private.weekly_source_guard_flag_v1(jsonb,text,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_authorisation_guard_v1(uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_authorisation_guard_tg_v1()
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_ordinary_authorisation_guard_v1(uuid,text) is
  'Plan 6.2 WP-24, Gate 13 finding F1. The state test behind the authorisation-boundary trigger on public.timesheets. Takes FOR UPDATE on every weekly_source_root_authorisations row of the booking family FIRST, then reads private.weekly_source_managed_root_guard_v1. Refuses on the HANDOVER 2 round-5 ruling B3 predicate (managed and (ok or weekly_source_bound)) or the live-record-on-an-unauthorised-Timesheet contradiction, with absent, null and non-boolean taking the unsafe value at every read. It asks nothing about the caller: the ordinary Unauthorise refuses because the authorisation generation is still live, and the withdrawal owner proceeds because it has already marked that generation withdrawn. Writes nothing.';
comment on function private.weekly_source_ordinary_authorisation_guard_tg_v1() is
  'Plan 6.2 WP-24, Gate 13 finding F1. BEFORE UPDATE trigger body on public.timesheets, armed only when authorised_at_server changes. Raises WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED (errcode 55000) - the sqlstate and message the broker durable refusal recorder already reads - with the refusal basis, the transition, and the route Office must use instead (proof/36 section 3, UNA-010). Writes zero rows (contract D11, D13).';

commit;
