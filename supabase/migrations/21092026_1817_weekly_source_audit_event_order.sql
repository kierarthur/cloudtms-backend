-- One-time CloudTMS schema/data migration: weekly_source_audit_event_order
--
-- STEP 6 / hostile-review finding F-02.  `public.audit_events.ts_utc` uses the
-- transaction timestamp and `id` is a random UUID.  They cannot prove the
-- order of two audit facts written by one transaction.  This migration adds a
-- database-owned, global insertion sequence.  The sequence is the ordering
-- authority for every audit event written after this migration.
--
-- Existing rows are deliberately marked non-authoritative: PostgreSQL can
-- assign them a deterministic backfill sequence, but it cannot reconstruct an
-- order the old schema never recorded.  New rows default to authoritative only
-- after the identity column exists.  The whole migration is transactional, so
-- no session can observe an intermediate default.
--
-- This changes no audit writer, financial owner, Workbench route, Timesheet
-- status, invoice or Candidate-pay decision.  Existing INSERT statements omit
-- the new columns and therefore receive their values from PostgreSQL.

\set ON_ERROR_STOP on

begin;

alter table public.audit_events
  add column event_sequence_is_authoritative boolean not null default false;

alter table public.audit_events
  add column event_sequence bigint generated always as identity;

alter table public.audit_events
  alter column event_sequence_is_authoritative set default true;

create unique index audit_events_event_sequence_uq
  on public.audit_events(event_sequence);

create index audit_events_object_sequence_idx
  on public.audit_events(object_type,object_id_text,event_sequence);

comment on column public.audit_events.event_sequence is
  'Database-owned global insertion order. New audit events use this value as the durable chronology authority.';

comment on column public.audit_events.event_sequence_is_authoritative is
  'TRUE only when event_sequence was allocated when the audit event was written. Rows predating this migration are FALSE because their original order cannot be reconstructed.';

commit;
