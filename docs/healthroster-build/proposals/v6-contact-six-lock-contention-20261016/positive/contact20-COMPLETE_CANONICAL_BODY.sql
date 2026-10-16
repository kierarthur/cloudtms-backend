-- One-time CloudTMS migration: complete frozen Contact one-time/02_automatic_followup_tranche.sqlinc.
-- Source-only proposal; exact current prerequisites/native contract qualification required.




-- Hold the exact prerequisite objects before every original assertion.
-- Existing native owners acquire these relations in differing paths; NOWAIT
-- refuses an active incompatible owner before checks instead of waiting with
-- a partial witness set. All locks remain in this existing atomic unit.
lock table public.weekly_manager_recipient_generations in access exclusive mode nowait;
lock table public.weekly_message_dispatch_commands in access exclusive mode nowait;

-- Root reviewed additive tranche; all eleven original values retain meaning.
-- A temporary NOT VALID check lets PostgreSQL compare its own canonical parsed
-- definition against the actual prior constraint. It is removed in this same
-- atomic migration; an unexpected owner/schema/tree aborts the whole migration.
do $prior$
begin
 if not exists(select 1 from pg_class relation join pg_namespace namespace on namespace.oid=relation.relnamespace
  join pg_attribute attribute on attribute.attrelid=relation.oid and attribute.attname='tranche_kind'
  join pg_constraint constraint_row on constraint_row.conrelid=relation.oid
   and constraint_row.conname='weekly_message_dispatch_commands_tranche_kind_check'
  where namespace.nspname='public' and relation.relname='weekly_message_dispatch_commands' and relation.relkind='r'
   and relation.relowner=(select oid from pg_roles where rolname=CURRENT_USER) and attribute.atttypid='text'::regtype
   and attribute.attnotnull and not attribute.attisdropped and constraint_row.contype='c' and constraint_row.convalidated
   and constraint_row.conkey=array[attribute.attnum]::smallint[]) then
  raise exception 'CONTACT_SOURCE_AUTOMATIC_TRANCHE_PRIOR_SCHEMA_MISMATCH' using errcode='55000';end if;
end;
$prior$;
alter table public.weekly_message_dispatch_commands add constraint weekly_contact_expected_original_tranche_ck
 check(tranche_kind in('CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
 'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H','MANAGER_EARLY_ALL','MANAGER_T6_RESPONDED',
 'MANAGER_T12_REMAINDER','MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND','COMPLETED_PACK_COPY')) not valid;
do $exact_prior$
declare actual text;expected text;
begin
 select pg_get_constraintdef(oid,false) into strict actual from pg_constraint
  where conrelid='public.weekly_message_dispatch_commands'::regclass and conname='weekly_message_dispatch_commands_tranche_kind_check';
 select regexp_replace(pg_get_constraintdef(oid,false),' NOT VALID$','') into strict expected from pg_constraint
  where conrelid='public.weekly_message_dispatch_commands'::regclass and conname='weekly_contact_expected_original_tranche_ck';
 if actual is distinct from expected then raise exception 'CONTACT_SOURCE_AUTOMATIC_TRANCHE_PRIOR_ENUM_MISMATCH' using errcode='55000';end if;
end;
$exact_prior$;
alter table public.weekly_message_dispatch_commands drop constraint weekly_contact_expected_original_tranche_ck;
alter table public.weekly_message_dispatch_commands drop constraint weekly_message_dispatch_commands_tranche_kind_check;
alter table public.weekly_message_dispatch_commands add constraint weekly_message_dispatch_commands_tranche_kind_check
 check(tranche_kind in('CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
 'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H','MANAGER_EARLY_ALL','MANAGER_T6_RESPONDED',
 'MANAGER_T12_REMAINDER','MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND','COMPLETED_PACK_COPY','MANAGER_AUTOMATIC_FOLLOWUP'));

do $generation_prior$
begin
 if not exists(select 1 from pg_class relation join pg_namespace namespace on namespace.oid=relation.relnamespace
  join pg_attribute attribute on attribute.attrelid=relation.oid and attribute.attname='trigger_kind'
  join pg_constraint constraint_row on constraint_row.conrelid=relation.oid
   and constraint_row.conname='weekly_manager_recipient_generations_trigger_kind_check'
  where namespace.nspname='public' and relation.relname='weekly_manager_recipient_generations' and relation.relkind='r'
   and relation.relowner=(select oid from pg_roles where rolname=CURRENT_USER) and attribute.atttypid='text'::regtype
   and attribute.attnotnull and not attribute.attisdropped and constraint_row.contype='c' and constraint_row.convalidated
   and constraint_row.conkey=array[attribute.attnum]::smallint[]) then
  raise exception 'CONTACT_SOURCE_COMPOSITION_PRIOR_SCHEMA_MISMATCH' using errcode='55000';end if;
end;
$generation_prior$;
alter table public.weekly_manager_recipient_generations add constraint weekly_contact_expected_original_generation_ck
 check(trigger_kind in('OFFICE_DIRECT','EARLY_ALL','T6_RESPONDED','T12_REMAINDER','NEW_INCIDENT','MANUAL_RESEND')) not valid;
do $exact_generation_prior$
declare actual text;expected text;
begin
 select pg_get_constraintdef(oid,false) into strict actual from pg_constraint
  where conrelid='public.weekly_manager_recipient_generations'::regclass and conname='weekly_manager_recipient_generations_trigger_kind_check';
 select regexp_replace(pg_get_constraintdef(oid,false),' NOT VALID$','') into strict expected from pg_constraint
  where conrelid='public.weekly_manager_recipient_generations'::regclass and conname='weekly_contact_expected_original_generation_ck';
 if actual is distinct from expected then raise exception 'CONTACT_SOURCE_COMPOSITION_PRIOR_ENUM_MISMATCH' using errcode='55000';end if;
end;
$exact_generation_prior$;
alter table public.weekly_manager_recipient_generations drop constraint weekly_contact_expected_original_generation_ck;
alter table public.weekly_manager_recipient_generations drop constraint weekly_manager_recipient_generations_trigger_kind_check;
alter table public.weekly_manager_recipient_generations add constraint weekly_manager_recipient_generations_trigger_kind_check
 check(trigger_kind in('OFFICE_DIRECT','EARLY_ALL','T6_RESPONDED','T12_REMAINDER','NEW_INCIDENT','MANUAL_RESEND','CONTACT_COMPOSITION'));


