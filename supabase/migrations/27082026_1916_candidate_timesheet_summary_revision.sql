-- One-time CloudTMS schema/data migration: candidate_timesheet_summary_revision
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

create sequence if not exists private.candidate_timesheet_summary_revision_seq
  as bigint increment by 1 minvalue 1 start with 1 cache 32;

create table if not exists private.candidate_timesheet_summary_revisions (
  identity_kind text not null,
  identity_id uuid not null,
  current_timesheet_id uuid,
  contract_week_id uuid,
  revision_seq bigint not null default nextval('private.candidate_timesheet_summary_revision_seq'::regclass),
  changed_at_utc timestamptz not null default clock_timestamp(),
  constraint candidate_timesheet_summary_revisions_pk primary key(identity_kind,identity_id),
  constraint candidate_timesheet_summary_revisions_kind_ck
    check(identity_kind in ('TIMESHEET','CONTRACT_WEEK')),
  constraint candidate_timesheet_summary_revisions_shape_ck
    check((identity_kind='TIMESHEET' and current_timesheet_id is not null) or identity_kind='CONTRACT_WEEK')
);

create unique index if not exists candidate_timesheet_summary_revisions_seq_uq
  on private.candidate_timesheet_summary_revisions(revision_seq);

alter sequence private.candidate_timesheet_summary_revision_seq owner to postgres;
alter table private.candidate_timesheet_summary_revisions owner to postgres;

revoke all on sequence private.candidate_timesheet_summary_revision_seq
  from public,anon,authenticated,service_role;
revoke all on table private.candidate_timesheet_summary_revisions
  from public,anon,authenticated,service_role;

comment on table private.candidate_timesheet_summary_revisions is
  'Bounded latest-revision index for Candidate-driven Timesheet Summary cell refresh; one row per logical identity, never a business-data history log.';

commit;
