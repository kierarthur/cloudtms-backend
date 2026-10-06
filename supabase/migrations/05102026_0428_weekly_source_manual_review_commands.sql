-- One-time CloudTMS schema/data migration: weekly_source_manual_review_commands
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Immutable request replies and the actually observed opening authority only.
-- No pay, recovery, invoice, archive or frozen Draft state is stored or changed.
-- Existing unanchored reviews are deliberately not backfilled from today's report.
create table private.weekly_source_manual_review_commands (
  command_id uuid primary key,
  command_kind text not null check (command_kind in ('OPEN','RESOLVE')),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  request_sha256 bytea not null check (octet_length(request_sha256)=32),
  review_id uuid not null references private.weekly_source_manual_reviews(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  week_ending_date date not null,
  opening_final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  opening_projection_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
  opening_target_family_id uuid references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  opening_event_sequence bigint check (opening_event_sequence>=0),
  result_json jsonb not null check (jsonb_typeof(result_json)='object'),
  completed_at_utc timestamptz not null default statement_timestamp()
    check (isfinite(completed_at_utc)),
  check (not (opening_final_revision_id is not null and opening_projection_publication_id is not null)),
  check (command_kind='OPEN' or (opening_final_revision_id is null and opening_projection_publication_id is null)),
  check ((opening_final_revision_id is not null or opening_projection_publication_id is not null)=
    (opening_event_sequence is not null)),
  check (opening_event_sequence is not null or opening_target_family_id is null),
  check ((command_kind='OPEN' and result_json->>'already_open'='false')=
    (opening_final_revision_id is not null or opening_projection_publication_id is not null))
);
alter table private.weekly_source_manual_review_commands owner to postgres;
create unique index weekly_source_manual_review_opening_command_uq
  on private.weekly_source_manual_review_commands(review_id)
  where opening_final_revision_id is not null or opening_projection_publication_id is not null;
revoke all on private.weekly_source_manual_review_commands from public,anon,authenticated,service_role;

create function private.weekly_source_manual_review_command_immutable_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_IMMUTABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_manual_review_command_immutable_v1() owner to postgres;
revoke all on function private.weekly_source_manual_review_command_immutable_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_manual_review_command_immutable
  before update or delete on private.weekly_source_manual_review_commands
  for each row execute function private.weekly_source_manual_review_command_immutable_v1();
create trigger weekly_source_manual_review_command_no_truncate
  before truncate on private.weekly_source_manual_review_commands
  for each statement execute function private.weekly_source_manual_review_command_immutable_v1();

commit;
