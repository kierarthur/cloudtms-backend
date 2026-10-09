-- F-25 private true-root Query producer identity and immutable association.
-- No history backfill, business rows, financial job, agency switch or clock.
-- Design: docs/bpay-next/design/F-25-query-transaction-protocol.md.
\set ON_ERROR_STOP on
begin;

create table private.bpay_next_query_producer (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 preparing_transaction_id xid8 not null unique references private.bpay_next_input_transaction(preparing_transaction_id) on delete restrict,
 module_epoch bigint not null check(module_epoch>0),
 owner_kind text not null check(owner_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),
 cause_kind text not null check(cause_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),
 cause_key text not null check(pg_catalog.octet_length(cause_key) between 1 and 256),
 request_sha256 bytea not null check(pg_catalog.octet_length(request_sha256)=32),
 mode text not null check(mode in ('STANDALONE_QUERY','SOURCE_PUBLICATION')),
 phase text not null check(phase in ('BUSINESS_OPEN','TAIL_PREPARED','FINISHING','FINISHED')),
 owner_stack text[] not null check(pg_catalog.cardinality(owner_stack) between 1 and 16),
 source_publication_receipt_id uuid references private.weekly_source_entitlement_publication_receipts(id) on delete restrict,
 metadata_command_id uuid references private.bpay_next_command(id) on delete restrict deferrable initially deferred,
 reserved_range_start bigint,reserved_range_end bigint,next_reserved_ordinal bigint,
 completion_witness_kind text check(completion_witness_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),
 completion_witness_id uuid,
 completion_witness_sha256 bytea check(pg_catalog.octet_length(completion_witness_sha256)=32),
 created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
 finished_at_utc timestamptz,
 unique(id,module_epoch),unique(id,preparing_transaction_id,module_epoch),
 check(pg_catalog.array_ndims(owner_stack)=1 and pg_catalog.array_lower(owner_stack,1)=1 and (owner_stack[1]=owner_kind) is true and owner_stack <@ array['MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE']::text[] and pg_catalog.array_position(owner_stack,null) is null),
 check((reserved_range_start is null and reserved_range_end is null and next_reserved_ordinal is null)
   or (reserved_range_start is not null and reserved_range_end is not null and next_reserved_ordinal is not null and reserved_range_start between 1 and 1000 and reserved_range_end between reserved_range_start and 1000
     and next_reserved_ordinal between reserved_range_start and reserved_range_end+1)),
 check((phase='FINISHED')=(finished_at_utc is not null)),
 check((completion_witness_kind is null and completion_witness_id is null and completion_witness_sha256 is null)
   or (completion_witness_kind is not null and completion_witness_id is not null and completion_witness_sha256 is not null))
);

-- Exact current-transaction event ownership; original event stays immutable.
create table private.bpay_next_query_event_owner (
 event_id uuid primary key references private.bpay_next_query_input_version(event_id) on delete restrict,
 producer_id uuid not null references private.bpay_next_query_producer(id) on delete restrict,
 unique(event_id,producer_id)
);
create index bpay_next_query_event_owner_producer_idx on private.bpay_next_query_event_owner(producer_id,event_id);

create table private.bpay_next_query_version_command (
 command_id uuid primary key,
 agency_sequence bigint not null check(agency_sequence>0),module_epoch bigint not null check(module_epoch>0),
 producer_id uuid not null unique,preparing_transaction_id xid8 not null unique,
 owner_kind text not null check(owner_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),cause_kind text not null check(cause_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),
 cause_key text not null check(pg_catalog.octet_length(cause_key) between 1 and 256),
 request_sha256 bytea not null check(pg_catalog.octet_length(request_sha256)=32),
 actual_event_count bigint not null check(actual_event_count between 1 and 1000),
 first_event_ordinal bigint not null check(first_event_ordinal between 1 and 1000),
 last_event_ordinal bigint not null check(last_event_ordinal between first_event_ordinal and 1000),
 completion_witness_kind text not null check(completion_witness_kind in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE')),
 completion_witness_id uuid not null,completion_witness_sha256 bytea not null check(pg_catalog.octet_length(completion_witness_sha256)=32),
 ordered_events_sha256 bytea not null check(pg_catalog.octet_length(ordered_events_sha256)=32),
 header_sha256 bytea not null check(pg_catalog.octet_length(header_sha256)=32),
 foreign key(command_id,agency_sequence,module_epoch) references private.bpay_next_command(id,agency_sequence,module_epoch) on delete restrict,
 foreign key(producer_id,preparing_transaction_id,module_epoch) references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 check(actual_event_count<=last_event_ordinal-first_event_ordinal+1)
);

-- A complete actual original Source group, never an invented pair clock.
create table private.bpay_next_query_source_group (
 id uuid primary key,producer_id uuid not null unique,preparing_transaction_id xid8 not null,
 module_epoch bigint not null check(module_epoch>0),
 origin_kind text not null check(origin_kind in ('CURRENT_HEAD','DIRECT_NEXT_PROTECTED','INITIAL_TSFIN')),
 source_receipt_id uuid references private.weekly_source_entitlement_publication_receipts(id) on delete restrict,
 member_count integer not null check(member_count between 1 and 2),is_fresh_receive boolean not null,
 certificate_sha256 bytea not null check(pg_catalog.octet_length(certificate_sha256)=32),
 outer_completion_witness_id uuid not null,outer_completion_witness_sha256 bytea not null check(pg_catalog.octet_length(outer_completion_witness_sha256)=32),
 unique(id,module_epoch),
 foreign key(producer_id,preparing_transaction_id,module_epoch) references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 check((origin_kind='CURRENT_HEAD')=(source_receipt_id is not null))
);
create table private.bpay_next_query_source_member (
 group_id uuid not null,member_no integer not null check(member_no between 1 and 2),
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
 root_timesheet_version bigint not null check(root_timesheet_version>0),
 work_id uuid not null references private.bpay_next_work(id) on delete restrict,
 revision_id uuid not null references private.bpay_next_work_revision(id) on delete restrict,
 source_event_id uuid not null,command_id uuid not null,agency_sequence bigint not null check(agency_sequence>0),
 module_epoch bigint not null check(module_epoch>0),
 primary key(group_id,member_no),unique(group_id,work_id),unique(group_id,root_timesheet_id),
 foreign key(command_id,work_id,revision_id,candidate_id) references private.bpay_next_publication(command_id,work_id,revision_id,candidate_id) on delete restrict,
 unique(group_id,command_id),unique(group_id,source_event_id),
 foreign key(group_id,module_epoch) references private.bpay_next_query_source_group(id,module_epoch) on delete restrict,
 foreign key(command_id,agency_sequence,module_epoch) references private.bpay_next_command(id,agency_sequence,module_epoch) on delete restrict
);
create index bpay_next_query_source_member_command_idx on private.bpay_next_query_source_member(command_id,group_id);
create table private.bpay_next_query_event_source_member (
 event_id uuid not null references private.bpay_next_query_input_version(event_id) on delete restrict,
 group_id uuid not null,member_no integer not null,
 old_scope_match boolean not null,new_scope_match boolean not null,
 primary key(event_id,group_id,member_no),
 foreign key(group_id,member_no) references private.bpay_next_query_source_member(group_id,member_no) on delete restrict,
 check(old_scope_match or new_scope_match)
);

-- Optional finite postclock completion targets are operational preheld scope,
-- never a full row snapshot or proof that a mere registration took a lock.
create table private.bpay_next_query_tail_target (
 producer_id uuid not null references private.bpay_next_query_producer(id) on delete restrict,
 owner_relation text not null check(owner_relation in (
 'private.weekly_source_manual_reviews','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions',
 'public.weekly_exceptional_pay_target_families','public.weekly_exceptional_pay_family_events','public.weekly_exceptional_pay_target_events',
 'public.weekly_exceptional_payment_approvals','public.weekly_exceptional_c1_publication_requests','private.weekly_source_local_protected_decision_receipts',
 'private.bpay_next_protected_source_receipt','public.weekly_exceptional_pay_generations','public.weekly_exceptional_orchestration_runs',
 'public.weekly_exceptional_orchestration_steps','public.weekly_exceptional_c1_publication_checkpoints','public.weekly_source_pending_entitlement_bundles',
 'private.weekly_source_entitlement_publication_receipts','public.weekly_source_entitlement_heads','public.weekly_source_entitlement_head_components',
 'public.weekly_source_root_authorisations')),
 owner_row_id uuid not null,operation text not null check(operation in ('INSERT','UPDATE','DELETE')),
 candidate_id uuid references public.candidates(id) on delete restrict,
 root_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
 scope_sha256 bytea not null check(pg_catalog.octet_length(scope_sha256)=32),
 primary key(producer_id,owner_relation,owner_row_id,operation)
);

create or replace function private.bpay_next_query_producer_operational_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if tg_op='TRUNCATE' or tg_op='DELETE' then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PRODUCER_RETAINED';end if;
 if tg_op='INSERT' then
   if new.preparing_transaction_id<>pg_catalog.pg_current_xact_id() or new.phase<>'BUSINESS_OPEN'
     or pg_catalog.cardinality(new.owner_stack)<>1 or new.metadata_command_id is not null
     or new.source_publication_receipt_id is not null or new.reserved_range_start is not null
     or new.reserved_range_end is not null or new.next_reserved_ordinal is not null
     or new.completion_witness_kind is not null or new.completion_witness_id is not null or new.completion_witness_sha256 is not null
     or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=new.preparing_transaction_id
       and t.module_epoch=new.module_epoch and t.phase='OPEN') then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PRODUCER_OWNER_INVALID';end if;
   return new;
 end if;
 if (old.reserved_range_start is null and new.reserved_range_start is not null and
      (old.phase<>'BUSINESS_OPEN' or new.phase<>'TAIL_PREPARED' or not exists(
        select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=old.preparing_transaction_id
          and t.module_epoch=old.module_epoch and t.phase='OPEN')))
    or (old.reserved_range_start is not null and
      ((new.reserved_range_start,new.reserved_range_end) is distinct from (old.reserved_range_start,old.reserved_range_end)
        or new.next_reserved_ordinal is null or new.next_reserved_ordinal<old.next_reserved_ordinal)) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_RESERVED_RANGE_IMMUTABLE_OR_REGRESSION';end if;
 if old.preparing_transaction_id<>pg_catalog.pg_current_xact_id() or old.phase='FINISHED'
   or (new.id,new.preparing_transaction_id,new.module_epoch,new.owner_kind,new.cause_kind,new.cause_key,new.request_sha256,new.mode,new.created_at_utc)
     is distinct from (old.id,old.preparing_transaction_id,old.module_epoch,old.owner_kind,old.cause_kind,old.cause_key,old.request_sha256,old.mode,old.created_at_utc)
   or (old.phase='BUSINESS_OPEN' and new.phase not in ('BUSINESS_OPEN','TAIL_PREPARED','FINISHING','FINISHED'))
   or (old.phase='TAIL_PREPARED' and new.phase not in ('TAIL_PREPARED','FINISHING','FINISHED'))
   or (old.phase='FINISHING' and new.phase not in ('FINISHING','FINISHED')) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PRODUCER_IMMUTABLE_OR_REGRESSION';end if;
 return new;
end
$function$;
alter function private.bpay_next_query_producer_operational_guard_v1() owner to postgres;
revoke all on function private.bpay_next_query_producer_operational_guard_v1() from public,anon,authenticated,service_role;
create trigger bpay_next_query_producer_operational_guard_v1 before insert or update or delete on private.bpay_next_query_producer
 for each row execute function private.bpay_next_query_producer_operational_guard_v1();
create trigger bpay_next_query_producer_no_truncate_v1 before truncate on private.bpay_next_query_producer
 for each statement execute function private.bpay_next_query_producer_operational_guard_v1();
alter table private.bpay_next_query_producer owner to postgres;
alter table private.bpay_next_query_producer enable row level security;
revoke all on private.bpay_next_query_producer from public,anon,authenticated,service_role;
alter table private.bpay_next_query_event_owner owner to postgres;
alter table private.bpay_next_query_event_owner enable row level security;
revoke all on private.bpay_next_query_event_owner from public,anon,authenticated,service_role;
create trigger bpay_next_query_event_owner_immutable_v1 before update or delete on private.bpay_next_query_event_owner for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_event_owner_no_truncate_v1 before truncate on private.bpay_next_query_event_owner for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
alter table private.bpay_next_query_version_command owner to postgres;
alter table private.bpay_next_query_version_command enable row level security;
revoke all on private.bpay_next_query_version_command from public,anon,authenticated,service_role;
create trigger bpay_next_query_version_command_immutable_v1 before update or delete on private.bpay_next_query_version_command for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_version_command_no_truncate_v1 before truncate on private.bpay_next_query_version_command for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
alter table private.bpay_next_query_source_group owner to postgres;
alter table private.bpay_next_query_source_group enable row level security;
revoke all on private.bpay_next_query_source_group from public,anon,authenticated,service_role;
create trigger bpay_next_query_source_group_immutable_v1 before update or delete on private.bpay_next_query_source_group for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_source_group_no_truncate_v1 before truncate on private.bpay_next_query_source_group for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
alter table private.bpay_next_query_source_member owner to postgres;
alter table private.bpay_next_query_source_member enable row level security;
revoke all on private.bpay_next_query_source_member from public,anon,authenticated,service_role;
create trigger bpay_next_query_source_member_immutable_v1 before update or delete on private.bpay_next_query_source_member for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_source_member_no_truncate_v1 before truncate on private.bpay_next_query_source_member for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
alter table private.bpay_next_query_event_source_member owner to postgres;
alter table private.bpay_next_query_event_source_member enable row level security;
revoke all on private.bpay_next_query_event_source_member from public,anon,authenticated,service_role;
create trigger bpay_next_query_event_source_member_immutable_v1 before update or delete on private.bpay_next_query_event_source_member for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_event_source_member_no_truncate_v1 before truncate on private.bpay_next_query_event_source_member for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
alter table private.bpay_next_query_tail_target owner to postgres;
alter table private.bpay_next_query_tail_target enable row level security;
revoke all on private.bpay_next_query_tail_target from public,anon,authenticated,service_role;
create trigger bpay_next_query_tail_target_immutable_v1 before update or delete on private.bpay_next_query_tail_target for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_tail_target_no_truncate_v1 before truncate on private.bpay_next_query_tail_target for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

commit;
