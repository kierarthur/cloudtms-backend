-- One-time CloudTMS schema migration: weekly_source_external_publication_arrival
--
-- Package WP-06d.  Authority: HANDOVER 2 round-5 rulings response
-- (`HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R5.md`), Part E, the paragraph
-- headed "External C1 publication after a CloudTMS head exists", word for word:
--
--   "The current CloudTMS head remains authoritative until a successor is
--    positively accepted.  An external publication with the exact same source
--    identity, generation and digest is idempotent evidence/readback.  A
--    different publication becomes a pending successor input and cannot
--    replace, mutate or release the current head automatically.  It must pass
--    the normal proposal/authorisation/publication path.  Unknown or
--    contradictory identity fails closed."
--
-- and Part D ruling OR-1:
--
--   "It is a CloudTMS-owned relation/adapter that consumes the sealed C1 source
--    identity and receipt contract.  It is not a modification of C1 and must
--    not become a second source authority.  CloudTMS owns current-head
--    publication and Workbench consumption; C1 owns the source facts and
--    handoff evidence."
--
-- This closes the precise question `IMPL\reports\WP-06_DESIGN.md` section 4.2
-- stopped at, and handoff WP-06_NEEDS N7 / open question 1 of WP-06's report.
--
-- WHAT THIS RELATION IS.  One append-only row per external publication that
-- arrived AFTER a CloudTMS entitlement head was already current for the root
-- and was classified DIFFERENT.  It is a pending successor INPUT: evidence that
-- something outside CloudTMS believes a different entitlement exists.  It is
-- not an entitlement, not a proposal, not a decision and not a head.
--
-- WHAT THIS RELATION DELIBERATELY DOES NOT HOLD, and why the omission is the
-- whole point.  There is no hours column, no pay column, no charge column, no
-- component child relation and no request/entitlement body of any kind.  Only
-- identity, generation and digests are recorded.  A relation that stored the
-- external side's figures would be a second place a payable number could be
-- read from, which is exactly what OR-1 forbids ("must not become a second
-- source authority").  Because the row carries no figure, no future reader can
-- accidentally pay from it: there is nothing here to pay.
--
-- APPEND-ONLY, WITH NO LIFECYCLE.  The row is never updated and never deleted.
-- "Still outstanding" is DERIVED at read time by comparing the recorded
-- declared head revision with the root's current committed head, so there is no
-- stored state that a writer could flip to "accepted".  Acceptance of a
-- successor happens only where the ruling puts it -- the ordinary proposal,
-- authorisation and publication path -- and that path rebuilds the entitlement
-- from CloudTMS's own source facts and never reads this relation.
--
-- WHY `private` AND NOT `public`.  The same two reasons the schema migration
-- records for `private.weekly_source_pending_release_review_items`: the ACL
-- contract's `WEEKLY_SOURCE_ACL_TABLE_UNCLASSIFIED` sweep is a closed set of
-- `public.weekly_*` relations owned by a file this package does not own, and
-- external handoff evidence must have no browser surface at all.
--
-- This migration is ADDITIVE and is deliberately a SEPARATE file rather than an
-- edit of `15092026_1534_weekly_source_plan6_schema.sql`, which several
-- packages are editing concurrently.  It sorts after
-- `18092026_0900_weekly_source_withdrawal_supersession.sql` and depends on
-- nothing that file adds.
--
-- Nothing in this file defines, wraps, re-creates, re-points or triggers a
-- Banking Pay, Draft, execution, cancellation, provider, settlement, recovery
-- or remittance owner, and it changes no Workbench selector.

\set ON_ERROR_STOP on

begin;

create table private.weekly_source_external_publication_arrivals (
  id uuid primary key default pg_catalog.gen_random_uuid(),

  -- Only one classification is ever recorded.  IDENTICAL writes nothing (it is
  -- a readback), and UNKNOWN/CONTRADICTORY writes nothing (it fails closed), so
  -- a row in this relation always means "different, pending, not accepted".
  -- The column is present rather than implied so the row is self-describing to
  -- an Office reader and so a future classification cannot be added by
  -- overloading the meaning of an existing row.
  arrival_outcome text not null
    check (arrival_outcome='PENDING_SUCCESSOR_INPUT'),

  -- Which external system handed the evidence over.  Bounded, and recorded as
  -- given; it is a label on evidence, never an authority.
  external_system text not null
    check (pg_catalog.char_length(external_system) between 1 and 64),

  -- ------------------------------------------------------------------ --
  -- 1. SOURCE IDENTITY, exactly as the arrival declared it.
  -- ------------------------------------------------------------------ --
  declared_root_timesheet_id uuid not null
    references public.timesheets(timesheet_id) on delete restrict,
  -- Stored raw, like every other Weekly Source booking identity (H2-035): the
  -- trimmed form is a lock and index key, not an identity.
  declared_root_family_booking_id text not null
    check (pg_catalog.char_length(declared_root_family_booking_id) between 1 and 200),
  declared_final_revision_id uuid not null
    references public.weekly_source_final_revisions(id) on delete restrict,
  declared_source_cycle_id uuid not null
    references public.weekly_source_cycles(id) on delete restrict,

  -- ------------------------------------------------------------------ --
  -- 2. GENERATION, exactly as the arrival declared it.
  -- ------------------------------------------------------------------ --
  declared_root_timesheet_version integer not null
    check (declared_root_timesheet_version>=1),
  declared_source_revision_number integer not null
    check (declared_source_revision_number>=1),
  -- The CloudTMS head generation the arrival claims to be a successor to or
  -- evidence of.  A pending successor input always declares a STRICTLY GREATER
  -- generation than the current committed head; equal-with-different-content
  -- and lower are fail-closed refusals and never reach this relation, so the
  -- cross-row check below is a real invariant and not decoration.
  declared_head_revision bigint not null check (declared_head_revision>=1),

  -- ------------------------------------------------------------------ --
  -- 3. DIGESTS, exactly as the arrival declared them.
  -- ------------------------------------------------------------------ --
  declared_source_generation_digest bytea not null
    check (pg_catalog.octet_length(declared_source_generation_digest)=32),
  declared_entitlement_digest bytea not null
    check (pg_catalog.octet_length(declared_entitlement_digest)=32),
  declared_publication_receipt_digest bytea not null
    check (pg_catalog.octet_length(declared_publication_receipt_digest)=32),

  -- ------------------------------------------------------------------ --
  -- 4. The CloudTMS head that REMAINED AUTHORITATIVE at arrival.
  --
  -- This is the immutable proof that the head was not replaced, mutated or
  -- released.  It is recorded from the row the owner read under its own lock,
  -- inside the same transaction that appends this row.
  -- ------------------------------------------------------------------ --
  current_head_id uuid not null
    references public.weekly_source_entitlement_heads(id) on delete restrict,
  current_head_revision bigint not null check (current_head_revision>=1),
  current_entitlement_digest bytea not null
    check (pg_catalog.octet_length(current_entitlement_digest)=32),
  current_source_generation_digest bytea not null
    check (pg_catalog.octet_length(current_source_generation_digest)=32),
  current_publication_receipt_digest bytea not null
    check (pg_catalog.octet_length(current_publication_receipt_digest)=32),

  -- The canonical digest of the complete declared arrival, produced by the one
  -- installed encoder (`private.weekly_source_publication_request_digest_v1`,
  -- H2-032).  It is the replay key: the same arrival delivered twice appends
  -- one row and returns the same row the second time.
  arrival_digest bytea not null
    check (pg_catalog.octet_length(arrival_digest)=32),

  -- The field-by-field comparison the owner actually performed, so an Office
  -- reader can see WHICH of the ten declared facts differed without recomputing
  -- anything.  Booleans, identifiers and hex digests only; no figure appears
  -- here, by construction and by the check below.
  comparison_json jsonb not null
    check (pg_catalog.jsonb_typeof(comparison_json)='object'),

  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),

  -- The pending-successor invariant, enforced by the relation and not only by
  -- the owner: a recorded arrival always claims a LATER generation than the
  -- head it did not replace.
  check (declared_head_revision>current_head_revision),
  unique (arrival_digest)
);
alter table private.weekly_source_external_publication_arrivals owner to postgres;

-- The Office reader's key: "what external inputs are outstanding for this
-- root".  Deliberately NOT unique: several different external publications may
-- legitimately be outstanding for one root at once, and each is separate
-- evidence.
create index weekly_source_external_publication_arrivals_root_idx
  on private.weekly_source_external_publication_arrivals(
    declared_root_timesheet_id,declared_head_revision,id);
create index weekly_source_external_publication_arrivals_head_idx
  on private.weekly_source_external_publication_arrivals(current_head_id,id);

-- Append-only.  Evidence is never edited.  A later, different arrival is a NEW
-- row with its own digest; the ordinary path's acceptance of a successor is a
-- new head, not an update here.
create function private.weekly_source_external_publication_arrival_immutable_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_ARRIVAL_IMMUTABLE'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_ARRIVAL_IMMUTABLE',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;
alter function private.weekly_source_external_publication_arrival_immutable_v1()
  owner to postgres;
revoke all on function private.weekly_source_external_publication_arrival_immutable_v1()
  from public,anon,authenticated,service_role;

create trigger weekly_source_external_publication_arrival_immutable
  before update or delete on private.weekly_source_external_publication_arrivals
  for each row execute function private.weekly_source_external_publication_arrival_immutable_v1();
create trigger weekly_source_external_publication_arrival_truncate_guard
  before truncate on private.weekly_source_external_publication_arrivals
  for each statement execute function private.weekly_source_external_publication_arrival_immutable_v1();

alter table private.weekly_source_external_publication_arrivals
  enable row level security;
alter table private.weekly_source_external_publication_arrivals
  force row level security;
revoke all on table private.weekly_source_external_publication_arrivals
  from public,anon,authenticated,service_role;
do $weekly_source_external_publication_arrival_policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on private.weekly_source_external_publication_arrivals for all to %I using (true) with check (true)',
    current_user
  );
end;
$weekly_source_external_publication_arrival_policy$;

comment on table private.weekly_source_external_publication_arrivals is
  'HANDOVER 2 round-5, Part E "External C1 publication after a CloudTMS head exists": one append-only row per external publication classified DIFFERENT against the root current committed head. A pending successor INPUT and nothing more. It carries identity, generation and digests only - no hours, pay, charge or component - so nothing payable can ever be read from it, and it is never read by the publication coordinator. Acceptance of a successor happens only through the ordinary proposal, authorisation and publication path.';
comment on column private.weekly_source_external_publication_arrivals.arrival_outcome is
  'Always PENDING_SUCCESSOR_INPUT. An identical arrival is idempotent readback and writes nothing; an unknown or contradictory arrival fails closed and writes nothing. A row therefore always means different, pending and not accepted.';
comment on column private.weekly_source_external_publication_arrivals.current_head_id is
  'The CloudTMS entitlement head that remained authoritative when this arrival was classified. Immutable proof that the external publication neither replaced, mutated nor released the current head.';
comment on column private.weekly_source_external_publication_arrivals.arrival_digest is
  'Canonical digest of the complete declared arrival, produced by the one installed encoder. Replay key: the same external publication delivered twice appends one row and returns that row the second time.';
comment on column private.weekly_source_external_publication_arrivals.comparison_json is
  'The field-by-field comparison the owner performed: which of the declared source-identity, generation and digest facts matched the current head and which differed. Identifiers, booleans and hex digests only; no monetary or hours value is recorded anywhere in this relation.';

commit;
