-- One-time CloudTMS schema/data migration: weekly_source_withdrawal_supersession
--
-- Package WP-07c.  Authority: HANDOVER 2 round-5 rulings response
-- (`HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R5.md`) section A3, which REJECTS
-- the permanent refusal WP-07b installed and selects option (b):
--
--   "A user must not become permanently unable to withdraw an authorisation
--    merely because its entitlement head has been committed. ... the withdrawal
--    owner must supersede the committed head atomically with the withdrawal."
--
-- and its five numbered steps, of which steps 3 and 5 need durable structure
-- that Gate 1 did not create:
--
--   3. "supersede the committed head with an explicit withdrawal reason and
--       immutable predecessor link";
--   5. "commit the withdrawal, supersession, invalidation and durable replay
--       receipt together".
--
-- This migration is ADDITIVE and is deliberately a SEPARATE file rather than an
-- edit of `15092026_1534_weekly_source_plan6_schema.sql`: several packages are
-- editing that file concurrently (WP-02b holds it for handoff N9), and an
-- ALTER-based migration that sorts after it is both collision-free and
-- UPGRADE-safe.  If the final seals pass prefers one Plan 6.2 migration it may
-- fold this file into that one; nothing here depends on being separate.
--
-- Nothing in this file defines, wraps, re-creates, re-points or triggers a
-- Banking Pay, Draft, execution, cancellation, provider, settlement, recovery
-- or remittance owner, and it changes no Workbench selector.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- 1. The durable replay receipt of ruling A3 step 5.
--
-- Until now the exact replay of `proof/36 section 5 step 1` read the recorded
-- result out of `public.audit_events.after_json->'result'`, which WP-07b
-- declared as finding F4: replay depended on audit retention and there was no
-- second durable copy.  Ruling A3 step 5 makes the receipt part of the atomic
-- write set, so it becomes a relation of its own and F4 closes as a
-- consequence.
--
-- Shape follows `private.weekly_source_entitlement_publication_receipts`
-- deliberately: same schema, same immutability trigger pattern, same RLS and
-- privilege pattern, same `request_digest` replay contract (exact replay
-- returns the stored receipt; a conflicting replay refuses).  Reusing the
-- installed mechanism is the instruction; inventing a second one is not.
--
-- ROUND-5 ROTATION/SIGNATURE RULING (Part E, "Rotation readings"):
--   "The authorisation row signature binds tenant/agency, canonical root, exact
--    authorised physical Timesheet identity, source generation/revision and the
--    canonical digest of the protected decision fields.  Booking reference
--    alone is not a signature key. ... a decision must never migrate silently
--    between physical Timesheet IDs."
-- Every one of those five bindings is a column here AND a field of
-- `request_digest`, and `root_timesheet_id` is a real physical identity, so a
-- withdrawal decision cannot be presented against a different physical row.
-- ---------------------------------------------------------------------------
create table private.weekly_source_first_authorisation_withdrawal_receipts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  -- Tenant binding.  There is no agencies relation in this schema; agency_id is
  -- a deployment identity carried by the decision bundle and copied onto the
  -- head.  A withdrawal that retires a head therefore binds that head's agency;
  -- a withdrawal with no head to retire has no agency evidence to bind and
  -- records NULL rather than inventing one.
  agency_id uuid,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  root_family_booking_id text not null
    check (pg_catalog.char_length(root_family_booking_id) between 1 and 200),
  root_timesheet_version integer not null check (root_timesheet_version>=1),
  requested_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  -- The caller's `p_expected_timesheet_id`, stored so the canonical request can
  -- be rebuilt EXACTLY from this row at replay time.  Nullable because the owner
  -- accepts a null expectation.
  expected_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  root_authorisation_id uuid not null
    references public.weekly_source_root_authorisations(id) on delete restrict,
  authorisation_generation integer not null check (authorisation_generation>=1),
  expected_row_signature text not null
    check (pg_catalog.char_length(expected_row_signature) between 1 and 512),
  -- The canonical digest of the protected decision fields, produced by the
  -- INSTALLED encoder private.weekly_source_publication_request_digest_v1.
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  -- The immutable predecessor link of ruling A3 step 3: the head this
  -- withdrawal retired, its revision and the state it was in beforehand.  NULL
  -- on the ordinary no-head withdrawal, which retires nothing.
  predecessor_head_id uuid references public.weekly_source_entitlement_heads(id) on delete restrict,
  predecessor_head_revision bigint check (predecessor_head_revision>=1),
  predecessor_head_state_before text
    check (predecessor_head_state_before is null or predecessor_head_state_before='COMMITTED_CURRENT'),
  predecessor_head_certified_zero boolean,
  head_superseded boolean not null,
  scope_change_tx_token uuid not null,
  withdrawn_at_utc timestamptz not null,
  withdrawn_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  checks_json jsonb not null check (pg_catalog.jsonb_typeof(checks_json)='array'),
  result_json jsonb not null check (pg_catalog.jsonb_typeof(result_json)='object'),
  created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  check (head_superseded=(predecessor_head_id is not null)),
  check ((predecessor_head_id is null)=(predecessor_head_revision is null)),
  check ((predecessor_head_id is null)=(predecessor_head_state_before is null)),
  check ((predecessor_head_id is null)=(predecessor_head_certified_zero is null)),
  -- One withdrawal per authorisation generation, for ever.
  unique (root_authorisation_id),
  -- A head is retired by at most one withdrawal, for ever.  NULLs repeat
  -- freely, so an ordinary no-head withdrawal is unaffected.
  unique (predecessor_head_id),
  -- The replay identity of `proof/36 section 5 step 1`, read by (timesheet,
  -- expected row signature) exactly as the owner is called -- AND BY
  -- GENERATION, which is not decoration.
  --
  -- `public.timesheet_lifecycle_guard_signature_v1` is a function of the
  -- Timesheet's own state, so withdrawing a root and then authorising it again
  -- can and does produce the SAME row signature on the new generation: proved
  -- by execution on the WP-07 fixture, where generation 1 and generation 2 of
  -- root WP07-BK-06 carry an identical signature.  Keyed on (root, signature)
  -- alone, the second, genuinely different withdrawal would either be mistaken
  -- for a replay of the first or collide on this index.  The generation is what
  -- separates them, and the replay reader additionally refuses to treat
  -- anything as a replay while a LIVE generation exists.
  unique (root_timesheet_id,expected_row_signature,authorisation_generation)
);
alter table private.weekly_source_first_authorisation_withdrawal_receipts owner to postgres;
create index weekly_source_first_authorisation_withdrawal_receipts_family_idx
  on private.weekly_source_first_authorisation_withdrawal_receipts(
    root_family_booking_id,authorisation_generation,id);
create index weekly_source_first_authorisation_withdrawal_receipts_digest_idx
  on private.weekly_source_first_authorisation_withdrawal_receipts(request_digest,id);

create function private.weekly_source_withdrawal_receipt_immutable_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;
alter function private.weekly_source_withdrawal_receipt_immutable_v1() owner to postgres;
revoke all on function private.weekly_source_withdrawal_receipt_immutable_v1()
  from public,anon,authenticated,service_role;

create trigger weekly_source_withdrawal_receipt_immutable
  before update or delete on private.weekly_source_first_authorisation_withdrawal_receipts
  for each row execute function private.weekly_source_withdrawal_receipt_immutable_v1();
create trigger weekly_source_withdrawal_receipt_truncate_guard
  before truncate on private.weekly_source_first_authorisation_withdrawal_receipts
  for each statement execute function private.weekly_source_withdrawal_receipt_immutable_v1();

alter table private.weekly_source_first_authorisation_withdrawal_receipts
  enable row level security;
alter table private.weekly_source_first_authorisation_withdrawal_receipts
  force row level security;
revoke all on table private.weekly_source_first_authorisation_withdrawal_receipts
  from public,anon,authenticated,service_role;
do $weekly_source_withdrawal_receipt_policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on private.weekly_source_first_authorisation_withdrawal_receipts for all to %I using (true) with check (true)',
    current_user
  );
end;
$weekly_source_withdrawal_receipt_policy$;

-- ---------------------------------------------------------------------------
-- 2. The head gains a supersession REASON and a withdrawal authority.
--
-- Before this migration the only way to leave `state='SUPERSEDED'` was through
-- `superseded_by_head_id`, because of the CHECK
--     ((superseded_at_utc IS NULL) = (superseded_by_head_id IS NULL))
-- which makes a successor head mandatory.  A withdrawal has no successor head
-- — that is the whole point of ruling A3: the week stops being current and
-- nothing replaces it — so the constraint is replaced by one that admits
-- EXACTLY TWO supersession authorities and no third:
--
--   * publication, which sets `superseded_by_head_id` (the mechanism WP-02's
--     coordinator already uses at `17092026_0300_…:2489-2491`, unchanged);
--   * first-authorisation withdrawal, which sets `superseded_by_withdrawal_id`
--     and must state the reason explicitly.
--
-- A head can never be superseded by both, and can never be superseded by
-- neither.  `superseded_reason` stays nullable on the publication branch so
-- that WP-02's installed coordinator needs no change to keep working; setting
-- it to 'ENTITLEMENT_HEAD_PUBLICATION' there is a one-line improvement handed
-- off rather than made (WP-07c handoff N1).
-- ---------------------------------------------------------------------------
alter table public.weekly_source_entitlement_heads
  add column superseded_reason text,
  add column superseded_by_withdrawal_id uuid;

alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_entitlement_heads_superseded_withdrawal_fk
  foreign key (superseded_by_withdrawal_id)
  references private.weekly_source_first_authorisation_withdrawal_receipts(id)
  on delete restrict;

-- The constraint being replaced is anonymous, so its generated name depends on
-- the creation order of every other CHECK on the relation and would silently
-- move if Gate 1 gained one more.  It is located by its DEFINITION and the
-- cardinality is checked explicitly: zero or two matches fail closed rather
-- than dropping an arbitrary one.
do $weekly_source_head_supersession_constraint$
declare
  v_name text;
  v_count integer;
begin
  select pg_catalog.count(*)::integer,pg_catalog.min(constraint_row.conname)
    into v_count,v_name
  from pg_catalog.pg_constraint as constraint_row
  where constraint_row.conrelid='public.weekly_source_entitlement_heads'::pg_catalog.regclass
    and constraint_row.contype='c'
    and pg_catalog.pg_get_constraintdef(constraint_row.oid)
        ='CHECK (((superseded_at_utc IS NULL) = (superseded_by_head_id IS NULL)))';
  if v_count<>1 then
    raise exception
      'WEEKLY_SOURCE_HEAD_SUPERSESSION_CONSTRAINT_NOT_LOCATED: expected exactly one, found %',
      v_count
      using errcode='55000';
  end if;
  execute pg_catalog.format(
    'alter table public.weekly_source_entitlement_heads drop constraint %I',v_name);
end;
$weekly_source_head_supersession_constraint$;

alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_entitlement_heads_supersession_authority_check
  -- Written as an AND chain rather than a CASE so that `pg_get_constraintdef`
  -- renders it on ONE line: the Gate 1 schema verifier compares CHECK
  -- definitions as exact normalised strings, and a pretty-printed CASE cannot be
  -- matched that way.  The five clauses are, in order: a supersession has
  -- exactly the authorities its timestamp implies; never both authorities; only
  -- the two named reasons exist; the withdrawal authority must state its reason;
  -- the publication authority must never claim the withdrawal reason; and an
  -- unsuperseded head carries no reason.
  check (
    (superseded_at_utc is null)
      =(superseded_by_head_id is null and superseded_by_withdrawal_id is null)
    and not (superseded_by_head_id is not null and superseded_by_withdrawal_id is not null)
    and (superseded_reason is null
         or superseded_reason in ('ENTITLEMENT_HEAD_PUBLICATION','FIRST_AUTHORISATION_WITHDRAWN'))
    and (superseded_by_withdrawal_id is null
         or superseded_reason='FIRST_AUTHORISATION_WITHDRAWN')
    and (superseded_by_head_id is null
         or superseded_reason is distinct from 'FIRST_AUTHORISATION_WITHDRAWN')
    and (superseded_at_utc is not null or superseded_reason is null)
  );

-- ---------------------------------------------------------------------------
-- 3. "never revives the superseded head" (ruling A3), enforced by the database.
--
-- Ruling A3: "Reauthorisation creates a new generation/head and never revives
-- the superseded head."  Reauthorisation is an ordinary Authorise, and a later
-- publication stages a NEW head; neither has any reason to touch this row.  The
-- guarantee is nevertheless written down here rather than left to the callers,
-- because it is the property the whole ruling rests on: once a withdrawal has
-- retired a head, that head's state, its supersession time, its reason and its
-- predecessor link are immutable and it can never return to COMMITTED_CURRENT.
-- ---------------------------------------------------------------------------
create function private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if old.superseded_by_withdrawal_id is null then
    return new;
  end if;
  if new.superseded_by_withdrawal_id is distinct from old.superseded_by_withdrawal_id
     or new.superseded_reason is distinct from old.superseded_reason
     or new.superseded_at_utc is distinct from old.superseded_at_utc
     or new.state is distinct from old.state
     or new.superseded_by_head_id is distinct from old.superseded_by_head_id then
    raise exception 'WEEKLY_SOURCE_HEAD_WITHDRAWAL_SUPERSESSION_IMMUTABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_HEAD_WITHDRAWAL_SUPERSESSION_IMMUTABLE',
              'head_id',old.id,
              'superseded_by_withdrawal_id',old.superseded_by_withdrawal_id
            )::text;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1()
  owner to postgres;
revoke all on function private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_entitlement_head_withdrawal_supersession_guard
  before update on public.weekly_source_entitlement_heads
  for each row
  execute function private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1();

-- ---------------------------------------------------------------------------
-- 4. THE AUTHORISATION ROW SIGNATURE, AUDITED AGAINST ROUND 5 PART E.
--
-- The rotation paragraphs of Part E rule that "the authorisation row signature
-- binds tenant/agency, canonical root, exact authorised physical Timesheet
-- identity, source generation/revision and the canonical digest of the
-- protected decision fields", that "booking reference alone is not a signature
-- key", and that "a decision must never migrate silently between physical
-- Timesheet IDs".
--
-- AUDIT OF WHAT THE ROW BOUND BEFORE THIS MIGRATION, field by field:
--
--   canonical root                    BOUND   family_booking_id, and never on
--                                             its own: the insert-time guard
--                                             `weekly_source_root_authorisation_identity_v1`
--                                             refuses a row whose stored family
--                                             is not the PHYSICAL row's own.
--   exact physical Timesheet identity BOUND   root_timesheet_id, a real FK to
--                                             public.timesheets, and the same
--                                             guard binds it to the family and
--                                             the version together.
--   source generation and revision    BOUND   authorisation_generation and
--                                             timesheet_version.
--   tenant and agency                 MISSING there was no agency column and no
--                                             agency evidence on the row.
--   canonical digest of the protected
--   decision fields                   MISSING `authorised_row_signature` holds
--                                             the ORDINARY CloudTMS Timesheet
--                                             lifecycle guard signature, which
--                                             is a function of the Timesheet's
--                                             own state and is shared with the
--                                             unchanged ordinary Authorise and
--                                             Unauthorise owners.  It is not a
--                                             Weekly Source construct and this
--                                             package may not redefine it.
--
-- So three of the five were bound and two were not.  The two missing ones are
-- added here as columns of the authorisation row, carrying a canonical digest
-- computed by the INSTALLED proof/32 section 9 encoder over all five bindings
-- at once.  The ordinary lifecycle signature keeps its own meaning and its own
-- column; nothing about the ordinary owners changes.
--
-- WHAT HAPPENS TO A ROW WRITTEN BEFORE THIS CHANGE.  `decision_digest` is
-- NULLABLE precisely so that this question has a deliberate answer instead of a
-- migration-time failure.  A pre-existing row carries NULL, and the withdrawal
-- owner's check W9 turns that into the NAMED refusal
-- `WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE` with the reason
-- `ROOT_AUTHORISATION_DECISION_DIGEST_MISSING` -- never a silent mismatch and
-- never an accidental pass.  A row whose stored digest does not rebuild from its
-- own five bindings refuses the same way, with
-- `ROOT_AUTHORISATION_DECISION_DIGEST_MISMATCH`.  Nothing is installed anywhere,
-- so no real row needs migrating; the behaviour is stated because it must be
-- deliberate.
--
-- `agency_id` is nullable for an honest reason and not a convenient one: at
-- FIRST authorisation a root that carries no protected pay target family and no
-- decision bundle has no authoritative agency evidence anywhere in this schema,
-- and inventing a deployment constant would be a binding that binds nothing.
-- The digest binds the ABSENCE as a value, so a row written with no agency
-- evidence can never later match a row that has some.
-- ---------------------------------------------------------------------------
-- Every input of the digest is a STORED column of the same row.  That is what
-- makes the digest able to detect that one of the row's own bindings was changed
-- underneath it; a digest rebuilt from live evidence elsewhere would instead
-- report a mismatch every time that evidence legitimately moved, and would
-- refuse withdrawals the pack permits.  So the protected-decision hashes AS THEY
-- STOOD AT AUTHORISATION are recorded here, and a genuinely LATER protected
-- decision is caught by check W1, which is where the pack puts it, not by a
-- digest comparison.
alter table public.weekly_source_root_authorisations
  add column agency_id uuid,
  add column protected_decision_hashes text[],
  add column decision_digest bytea
    check (decision_digest is null or pg_catalog.octet_length(decision_digest)=32);
alter table public.weekly_source_root_authorisations
  add constraint weekly_source_root_authorisations_protected_hashes_check
  check (protected_decision_hashes is null
         or pg_catalog.array_position(protected_decision_hashes,null::text) is null);

comment on column public.weekly_source_root_authorisations.agency_id is
  'Tenant and agency binding of HANDOVER 2 round 5, Part E rotation readings. Copied from the root protected pay target family where one exists, otherwise NULL because no authoritative agency evidence exists for an ordinary root at first authorisation. Bound into decision_digest either way, absence included.';
comment on column public.weekly_source_root_authorisations.protected_decision_hashes is
  'The protected decision fields bound by the authorisation row signature: every live protected-hours approval hash for the root family AT AUTHORISATION, hex and sorted. Recorded so the digest rebuilds from the row own stored bindings; a genuinely later protected decision is refused by check W1, not by a digest comparison.';
comment on column public.weekly_source_root_authorisations.decision_digest is
  'The canonical digest of the five bindings HANDOVER 2 round 5 Part E requires of the authorisation row signature: tenant and agency, canonical root, exact authorised physical Timesheet identity, source generation and revision, and the protected decision fields. Produced by the installed proof/32 section 9 encoder. NULL only on a row written before the binding existed, which the withdrawal owner refuses by name rather than passing.';

comment on table private.weekly_source_first_authorisation_withdrawal_receipts is
  'Durable replay receipt of HANDOVER 2 round-5 ruling A3 step 5. One immutable row per completed first-authorisation withdrawal, carrying the canonical request digest, the immutable predecessor link to the entitlement head the withdrawal retired (NULL when none existed), the single scope-change transaction token and the complete recorded result. An exact replay returns this row; a conflicting replay refuses.';
comment on column public.weekly_source_entitlement_heads.superseded_reason is
  'Why a head stopped being current: NULL or ENTITLEMENT_HEAD_PUBLICATION for the ordinary publication supersession, FIRST_AUTHORISATION_WITHDRAWN for a head retired by the Office change-of-mind withdrawal (HANDOVER 2 round-5 ruling A3 step 3).';
comment on column public.weekly_source_entitlement_heads.superseded_by_withdrawal_id is
  'The withdrawal receipt that retired this head, exactly one supersession authority alongside superseded_by_head_id and never both (HANDOVER 2 round-5 ruling A3 step 3). Immutable once set.';

commit;
