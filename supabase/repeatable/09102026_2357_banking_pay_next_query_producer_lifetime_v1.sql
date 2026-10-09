-- F-25 actual true-root transaction lifetime; no caller-resettable GUC.
-- No stage callback acquires an agency clock. Finite outer owners finish.
-- Initial enter/leave owns BUSINESS_OPEN only. Registered postclock nested
-- callback entry is deliberately unavailable until its actual Source lockset
-- and callback manifest successor is installed; no Source hooks attach here.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_producer_enter_v1(
 p_owner_kind text,p_cause_kind text,p_cause_key text,p_request_sha256 bytea,p_mode text
) returns table(producer_id uuid,is_outer boolean)
language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;epoch bigint;current_phase text;parent text;
begin
 if p_owner_kind is null or p_owner_kind not in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE') or p_cause_kind is distinct from p_owner_kind
   or p_cause_key is null or pg_catalog.octet_length(p_cause_key) not between 1 and 256
   or p_request_sha256 is null or pg_catalog.octet_length(p_request_sha256)<>32
   or p_mode is null or p_mode not in ('STANDALONE_QUERY','SOURCE_PUBLICATION') then
   raise exception using errcode='22023',message='BPAY_NEXT_QUERY_PRODUCER_INPUT_INVALID';end if;
 select t.phase into current_phase from private.bpay_next_input_transaction t
   where t.preparing_transaction_id=pg_catalog.pg_current_xact_id();
 select p.* into owned from private.bpay_next_query_producer p
   where p.preparing_transaction_id=pg_catalog.pg_current_xact_id();
 if found then
   if owned.phase<>'BUSINESS_OPEN' or current_phase is distinct from 'OPEN' or pg_catalog.cardinality(owned.owner_stack)>=16 then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_NOT_OPEN';end if;
   parent:=owned.owner_stack[pg_catalog.cardinality(owned.owner_stack)];
   if not ((parent='CANDIDATE_APP_SUBMIT' and p_owner_kind in ('CANDIDATE_RESPONSE','CANDIDATE_MATERIALISE'))
     or (parent='CANDIDATE_SELF_SUBMIT' and p_owner_kind='CANDIDATE_MATERIALISE')
     or (parent='CANDIDATE_MATERIALISE' and p_owner_kind='QUERY_SYNC')
     or (parent in ('SOURCE_UPLOAD','SOURCE_ROWS_APPLY') and p_owner_kind='QUERY_SYNC')
     or (parent in ('SOURCE_FINAL','SOURCE_CORRECT_FINAL') and p_owner_kind='MANUAL_RESOLVE')
     or (parent='SOURCE_LOCAL_COMPLETE' and p_owner_kind in ('SOURCE_PUBLISH','MANUAL_RESOLVE'))
     or (parent in ('SOURCE_LATER_CHANGE','SOURCE_PENDING_RELEASE') and p_owner_kind='SOURCE_PUBLISH')
     or (parent='SOURCE_PUBLISH' and p_owner_kind='SOURCE_PUBLISH')) then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_EDGE_INVALID';end if;
   update private.bpay_next_query_producer p set owner_stack=pg_catalog.array_append(p.owner_stack,p_owner_kind)
     where p.id=owned.id and p.preparing_transaction_id=pg_catalog.pg_current_xact_id() and p.phase='BUSINESS_OPEN';
   if not found then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_CHANGED';end if;
   producer_id:=owned.id;is_outer:=false;return next;return;
 end if;
 if current_phase is not null and current_phase<>'OPEN' then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_TRANSACTION_NOT_FRESH';end if;
 -- True root is responsible for pure auth/shape/replay before this call.
 -- This stable module decision precedes its first business/session/FK lock.
 perform private.weekly_source_pay_query_admit_v2();
 select m.owner_epoch into epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
 if epoch is null then
   -- Preserve shared Source under the stable other-owner decision too.
   perform 1 from private.bpay_next_module_control m where m.id=1 for share;
   if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MODULE_DECISION_CHANGED';end if;
   producer_id:=null;is_outer:=false;return next;return;
 end if;
 if exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=pg_catalog.pg_current_xact_id())
   or exists(select 1 from public.banking_pay_scope_change_transactions x where x.state='PENDING'
     and x.xmin::text::numeric=mod(pg_catalog.pg_current_xact_id()::text::numeric,4294967296)) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MIXED_TRANSACTION_UNSUPPORTED';end if;
 insert into private.bpay_next_input_transaction(preparing_transaction_id,module_epoch,phase)
   values(pg_catalog.pg_current_xact_id(),epoch,'OPEN') on conflict(preparing_transaction_id) do nothing;
 if not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=pg_catalog.pg_current_xact_id()
   and t.module_epoch=epoch and t.phase='OPEN' and t.next_event_ordinal=0) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_TRANSACTION_OWNER_INVALID';end if;
 insert into private.bpay_next_query_producer(preparing_transaction_id,module_epoch,owner_kind,cause_kind,cause_key,
   request_sha256,mode,phase,owner_stack) values(pg_catalog.pg_current_xact_id(),epoch,p_owner_kind,p_cause_kind,p_cause_key,
   p_request_sha256,p_mode,'BUSINESS_OPEN',array[p_owner_kind]) returning id into producer_id;
 is_outer:=true;return next;
end
$function$;

create or replace function private.bpay_next_query_producer_leave_v1(p_producer_id uuid,p_owner_kind text)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;n integer;
begin
 if p_producer_id is null then return;end if;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id
   and p.preparing_transaction_id=pg_catalog.pg_current_xact_id() for update;
 n:=pg_catalog.cardinality(owned.owner_stack);
 if owned.phase<>'BUSINESS_OPEN' or n<2 or owned.owner_stack[n] is distinct from p_owner_kind then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_LEAVE_INVALID';end if;
 update private.bpay_next_query_producer p set owner_stack=p.owner_stack[1:n-1] where p.id=owned.id;
end
$function$;

create or replace function private.bpay_next_query_version_stage_v1(
 p_owner_relation text,p_owner_row_id uuid,p_candidate_id uuid,p_root_timesheet_id uuid,
 p_old_present boolean,p_new_present boolean,p_old_tuple jsonb,p_new_tuple jsonb
) returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;tx_phase text;ordinal bigint;event_id uuid;
 old_digest bytea;new_digest bytea;scope_digest bytea;operation text;
begin
 if p_owner_relation is null or p_owner_relation not in (
 'private.weekly_source_manual_reviews','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions',
 'public.weekly_exceptional_pay_target_families','public.weekly_exceptional_pay_family_events','public.weekly_exceptional_pay_target_events',
 'public.weekly_exceptional_payment_approvals','public.weekly_exceptional_c1_publication_requests','private.weekly_source_local_protected_decision_receipts',
 'private.bpay_next_protected_source_receipt','public.weekly_exceptional_pay_generations','public.weekly_exceptional_orchestration_runs',
 'public.weekly_exceptional_orchestration_steps','public.weekly_exceptional_c1_publication_checkpoints','public.weekly_source_pending_entitlement_bundles',
 'private.weekly_source_entitlement_publication_receipts','public.weekly_source_entitlement_heads','public.weekly_source_entitlement_head_components',
 'public.weekly_source_root_authorisations') or p_owner_row_id is null
   or p_old_present is null or p_new_present is null or not(p_old_present or p_new_present)
   or pg_catalog.jsonb_typeof(p_old_tuple) is distinct from 'object' or pg_catalog.jsonb_typeof(p_new_tuple) is distinct from 'object'
   or pg_catalog.octet_length(p_old_tuple::text)>16384 or pg_catalog.octet_length(p_new_tuple::text)>16384
   or (not p_old_present and p_old_tuple<>'{}'::jsonb) or (not p_new_present and p_new_tuple<>'{}'::jsonb) then
   raise exception using errcode='22023',message='BPAY_NEXT_QUERY_STAGE_INPUT_INVALID';end if;
 if p_old_present=p_new_present and p_old_tuple=p_new_tuple then return null;end if;
 select p.* into owned from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id() for update;
 if not found or owned.phase not in ('BUSINESS_OPEN','TAIL_PREPARED') then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MATERIAL_WRITER_UNOWNED';end if;
 select t.phase into strict tx_phase from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
   and t.module_epoch=owned.module_epoch;
 if exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
   and e.input_kind<>'PAY_QUERY') then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MIXED_TRANSACTION_UNSUPPORTED';end if;
 old_digest:=pg_catalog.sha256(pg_catalog.convert_to(p_old_tuple::text,'UTF8'));
 new_digest:=pg_catalog.sha256(pg_catalog.convert_to(p_new_tuple::text,'UTF8'));
 if tx_phase='OPEN' and owned.phase='BUSINESS_OPEN' then
   update private.bpay_next_input_transaction t set next_event_ordinal=t.next_event_ordinal+1 where t.preparing_transaction_id=owned.preparing_transaction_id
     and t.module_epoch=owned.module_epoch and t.phase='OPEN' and t.next_event_ordinal<1000 returning t.next_event_ordinal into ordinal;
   if not found then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_STAGE_BOUND_OR_OWNER_INVALID';end if;
 elsif tx_phase='CLOCK_SEALED' and owned.phase='TAIL_PREPARED' and owned.mode='SOURCE_PUBLICATION' then
   operation:=case when not p_old_present then 'INSERT' when not p_new_present then 'DELETE' else 'UPDATE' end;
   scope_digest:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_TAIL_SCOPE_V1',p_owner_relation,p_owner_row_id,
     p_candidate_id,p_root_timesheet_id,operation)::text,'UTF8'));
   if not exists(select 1 from private.bpay_next_query_tail_target target where target.producer_id=owned.id
     and target.owner_relation=p_owner_relation and target.owner_row_id=p_owner_row_id and target.operation=operation
     and target.candidate_id is not distinct from p_candidate_id and target.root_timesheet_id is not distinct from p_root_timesheet_id
     and target.scope_sha256=scope_digest) or owned.next_reserved_ordinal is null or owned.next_reserved_ordinal>owned.reserved_range_end then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_POSTCLOCK_TARGET_NOT_PREHELD';end if;
   ordinal:=owned.next_reserved_ordinal;
   update private.bpay_next_query_producer p set next_reserved_ordinal=p.next_reserved_ordinal+1 where p.id=owned.id;
 else raise exception using errcode='55000',message='BPAY_NEXT_QUERY_STAGE_AFTER_FINISH';end if;
 event_id:=pg_catalog.gen_random_uuid();
 insert into private.bpay_next_input_version_event(id,preparing_transaction_id,module_epoch,input_kind,owner_relation,owner_row_id,
   old_present,new_present,old_sha256,new_sha256,transaction_ordinal) values(event_id,owned.preparing_transaction_id,owned.module_epoch,'PAY_QUERY',
   p_owner_relation,p_owner_row_id,p_old_present,p_new_present,old_digest,new_digest,ordinal);
 insert into private.bpay_next_query_input_version(event_id,owner_relation,owner_row_id,candidate_id,root_timesheet_id,old_gate_tuple,new_gate_tuple)
   values(event_id,p_owner_relation,p_owner_row_id,p_candidate_id,p_root_timesheet_id,p_old_tuple,p_new_tuple);
 insert into private.bpay_next_query_event_owner(event_id,producer_id) values(event_id,owned.id);
 return event_id;
end
$function$;

alter function private.bpay_next_query_producer_enter_v1(text,text,text,bytea,text) owner to postgres;
revoke all on function private.bpay_next_query_producer_enter_v1(text,text,text,bytea,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_query_producer_leave_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_query_producer_leave_v1(uuid,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_query_version_stage_v1(text,uuid,uuid,uuid,boolean,boolean,jsonb,jsonb) owner to postgres;
revoke all on function private.bpay_next_query_version_stage_v1(text,uuid,uuid,uuid,boolean,boolean,jsonb,jsonb) from public,anon,authenticated,service_role;

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

commit;
