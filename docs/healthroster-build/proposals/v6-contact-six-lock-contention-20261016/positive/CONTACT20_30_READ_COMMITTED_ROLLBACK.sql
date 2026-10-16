begin isolation level read committed;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
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



-- One-time CloudTMS migration: complete frozen Contact one-time/03_submission_cohort_identity.sqlinc.
-- Source-only proposal; exact current prerequisites/native contract qualification required.




-- Hold the exact prerequisite objects before every original assertion.
-- Existing native owners acquire these relations in differing paths; NOWAIT
-- refuses an active incompatible owner before checks instead of waiting with
-- a partial witness set. All locks remain in this existing atomic unit.
lock table public.weekly_candidate_cohorts in share mode nowait;
lock table public.weekly_timesheet_submission_requests in access exclusive mode nowait;
lock table public.weekly_candidate_outreach_generations in share mode nowait;
lock table public.weekly_timesheet_submission_request_memberships in share mode nowait;

-- Guarded existing-table identity successor. No history is rewritten.
-- Source-only: Root must review/export exact contract and qualify PostgreSQL.
do $block$
declare relation oid:='public.weekly_timesheet_submission_requests'::regclass;
 cohort_relation oid:='public.weekly_candidate_cohorts'::regclass; prior_constraint name;prior_index oid;
 actual smallint[];expected smallint[];predicate text;
begin
 if(select relowner from pg_class where oid=relation)<>(select oid from pg_roles where rolname=current_user)
  or(select relowner from pg_class where oid=cohort_relation)<>(select oid from pg_roles where rolname=current_user) then
  raise exception 'CONTACT_SUBMISSION_OWNER_DRIFT' using errcode='55000';end if;
 if not exists(select 1 from pg_attribute where attrelid=relation and attname='candidate_cohort_id'
   and atttypid='uuid'::regtype and attnotnull and not attisdropped)
  or not exists(select 1 from pg_attribute where attrelid=relation and attname='request_generation'
   and atttypid='integer'::regtype and attnotnull and not attisdropped)
  or not exists(select 1 from pg_attribute where attrelid=cohort_relation and attname='current_submission_generation_id'
   and atttypid='uuid'::regtype and not attisdropped) then
  raise exception 'CONTACT_SUBMISSION_NATIVE_SCHEMA_DRIFT' using errcode='55000';end if;
 select array_agg(attribute.attnum order by operand.ordinality) into expected
  from unnest(array['source_cycle_id','candidate_id','request_generation'])with ordinality operand(name,ordinality)
  join pg_attribute attribute on attribute.attrelid=relation and attribute.attname=operand.name and not attribute.attisdropped;
 select conname into strict prior_constraint from pg_constraint where conrelid=relation and contype='u' and conkey=expected
  and convalidated and not condeferrable and not condeferred;
 if exists(select 1 from unnest(array['source_cycle_id','candidate_id','candidate_cohort_id'])operand(name)
  where not exists(select 1 from pg_attribute attribute where attribute.attrelid=relation and attribute.attname=operand.name
   and attribute.atttypid='uuid'::regtype and attribute.attnotnull and not attribute.attisdropped))
  or not exists(select 1 from pg_index index_row where index_row.indexrelid='public.weekly_candidate_outreach_generations_active_uq'::regclass
   and index_row.indrelid='public.weekly_candidate_outreach_generations'::regclass and index_row.indisunique and index_row.indisvalid
   and index_row.indisready and index_row.indnkeyatts=2 and index_row.indnatts=2
   and array(select unnest(index_row.indkey::smallint[]))=(select array_agg(attribute.attnum order by operand.ordinality)
    from unnest(array['candidate_cohort_id','request_kind'])with ordinality operand(name,ordinality)
    join pg_attribute attribute on attribute.attrelid=index_row.indrelid and attribute.attname=operand.name and not attribute.attisdropped)
   and pg_get_expr(index_row.indpred,index_row.indrelid)='(state = ''ACTIVE''::text)') then
  raise exception 'CONTACT_SUBMISSION_NATIVE_GENERATION_SCHEMA_DRIFT' using errcode='55000';end if;
 prior_index:=to_regclass('public.weekly_timesheet_submission_requests_active_uq');
 select indkey::smallint[],pg_get_expr(indpred,indrelid) into actual,predicate from pg_index
  where indexrelid=prior_index and indrelid=relation and indisunique and indisvalid and indisready and indnkeyatts=2 and indnatts=2;
 select array_agg(attribute.attnum order by operand.ordinality) into expected
  from unnest(array['source_cycle_id','candidate_id'])with ordinality operand(name,ordinality)
  join pg_attribute attribute on attribute.attrelid=relation and attribute.attname=operand.name and not attribute.attisdropped;
 if actual is null or array(select unnest(actual)) is distinct from expected
  or predicate is distinct from '(state = ANY (ARRAY[''READY''::text, ''ACTIVE''::text, ''OVERDUE''::text, ''PARTLY_SUBMITTED''::text]))' then
  raise exception 'CONTACT_SUBMISSION_NATIVE_INDEX_DRIFT' using errcode='55000';end if;
 -- This is a one-time complete retained identity proof, never a lifetime
 -- count or repeated historical scan imposed on a business mutation.
 if exists(select 1 from public.weekly_timesheet_submission_requests request
  left join public.weekly_candidate_cohorts cohort on cohort.id=request.candidate_cohort_id
  left join public.weekly_candidate_outreach_generations generation
   on generation.candidate_cohort_id=request.candidate_cohort_id and generation.generation_number=request.request_generation
    and generation.request_kind='SUBMIT_TIMESHEET'
  where(cohort.source_cycle_id,cohort.candidate_id) is distinct from(request.source_cycle_id,request.candidate_id)
   or generation.id is null or(generation.source_cycle_id,generation.candidate_id,generation.membership_hash,
    generation.started_at_utc,generation.reminder_due_at_utc,generation.deadline_at_utc) is distinct from
    (request.source_cycle_id,request.candidate_id,request.membership_hash,request.started_at_utc,request.reminder_due_at_utc,request.deadline_at_utc)
   or(request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED') and
    (generation.state<>'ACTIVE' or cohort.current_submission_generation_id is distinct from generation.id)))
  or exists(select 1 from public.weekly_timesheet_submission_requests request
   join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
   join public.weekly_timesheet_submission_requests other on other.source_cycle_id=request.source_cycle_id
    and other.candidate_id=request.candidate_id and other.id<>request.id and other.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
   join public.weekly_timesheet_submission_request_memberships overlap on overlap.submission_request_id=other.id
    and(overlap.client_id,overlap.contract_id,overlap.week_ending)=(member.client_id,member.contract_id,member.week_ending)
   where request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')) then
  raise exception 'CONTACT_SUBMISSION_RETAINED_IDENTITY_UNRESOLVED' using errcode='55000';end if;
 execute format('alter table public.weekly_timesheet_submission_requests drop constraint %I',prior_constraint);
 alter table public.weekly_timesheet_submission_requests add constraint weekly_timesheet_submission_requests_cohort_generation_uq
  unique(source_cycle_id,candidate_id,candidate_cohort_id,request_generation);
 drop index public.weekly_timesheet_submission_requests_active_uq;
 create unique index weekly_timesheet_submission_requests_active_uq on public.weekly_timesheet_submission_requests
  (source_cycle_id,candidate_id,candidate_cohort_id) where state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED');
end;
$block$;



rollback;