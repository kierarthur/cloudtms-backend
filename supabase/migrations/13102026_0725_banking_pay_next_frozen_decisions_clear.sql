-- WB28/BD175: empty private frozen future-clear evidence and nullable immutable defaults.
-- No backfill, real data, public trigger, captured-offer rewrite or money owner.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_selection_mode_memory add column remembered_selected boolean;
alter table private.bpay_next_selection_mode_memory add constraint bpay_next_selection_memory_header_default_null check(row_kind<>'HEADER' or remembered_selected is null);
create table private.bpay_next_decisions_clear_operation(
 request_id uuid primary key,agency_id uuid not null,actor_user_id uuid not null references public.tms_users(id),
 owner_epoch bigint not null check(owner_epoch>0),official_pay_date date not null,
 scope jsonb not null check(jsonb_typeof(scope)='object' and octet_length(scope::text)<=512),
 expected_workbench_revision bigint not null check(expected_workbench_revision>=0),initial_sequence bigint not null check(initial_sequence>=0),
 initial_args jsonb not null check(jsonb_typeof(initial_args)='object' and octet_length(initial_args::text)<=4096),
 scope_summary jsonb not null check(jsonb_typeof(scope_summary)='object' and octet_length(scope_summary::text)<=4096),
 state text not null check(state in('DISCOVERING','READY','APPLYING','COMPLETE','PARTIAL')),
 freeze_step bigint not null default 0 check(freeze_step>=0),apply_step bigint not null default 0 check(apply_step>=0),member_count bigint not null default 0 check(member_count>=0),
 after_work_id uuid,after_family text check(after_family in('GROSS','WORK_BUCKET')),
 expected_sequence bigint not null check(expected_sequence>=0),reset_sequence bigint,
 owner_count bigint not null default 0 check(owner_count>=0),excluded_count bigint not null default 0 check(excluded_count>=0),
 candidate_count bigint not null default 0 check(candidate_count>=0),work_count bigint not null default 0 check(work_count>=0),
 cleared_count bigint not null default 0 check(cleared_count>=0),refused_count bigint not null default 0 check(refused_count>=0),
 affected_amount numeric(38,2) not null default 0 check(affected_amount>=0),cleared_amount numeric(38,2) not null default 0 check(cleared_amount>=0),
 created_at_utc timestamptz not null default transaction_timestamp(),completed_at_utc timestamptz,
 check((after_work_id is null)=(after_family is null)),check(reset_sequence is null or reset_sequence>0 and state in('COMPLETE','PARTIAL') and completed_at_utc is not null),
 foreign key(request_id,reset_sequence) references private.bpay_next_command(id,agency_sequence)
);
create index bpay_next_decisions_clear_reset_actor_idx on private.bpay_next_decisions_clear_operation
 (agency_id,owner_epoch,actor_user_id,official_pay_date,reset_sequence desc) where reset_sequence is not null;
create table private.bpay_next_decisions_clear_member(
 request_id uuid not null references private.bpay_next_decisions_clear_operation(request_id),member_no bigint not null check(member_no>0),
 candidate_id uuid not null,work_id uuid not null,expected_revision_id uuid not null,owner_family text not null check(owner_family in('GROSS','WORK_BUCKET')),
 native_context jsonb not null check(jsonb_typeof(native_context)='object' and octet_length(native_context::text)<=4096),
 subject jsonb not null check(jsonb_typeof(subject)='object' and octet_length(subject::text)<=40000),
 owners jsonb not null check(jsonb_typeof(owners)='array' and jsonb_array_length(owners)<=100 and octet_length(owners::text)<=100000),
 exclusion_code text check(length(exclusion_code) between 1 and 128),
 primary key(request_id,member_no),unique(request_id,work_id,owner_family),
 foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
 foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id),
 check(jsonb_array_length(owners)>0 or exclusion_code is not null)
);
create index bpay_next_decisions_clear_member_candidate_idx on private.bpay_next_decisions_clear_member(request_id,candidate_id,member_no);
create table private.bpay_next_decisions_clear_page(
 request_id uuid not null references private.bpay_next_decisions_clear_operation(request_id),phase text not null check(phase in('FREEZE','APPLY')),
 step_no bigint not null check(step_no>=0),work_id uuid,subrequest_id uuid unique,
 request_body jsonb not null check(jsonb_typeof(request_body)='object' and octet_length(request_body::text)<=4096),
 native_body jsonb check(jsonb_typeof(native_body)='object' and octet_length(native_body::text)<=32768),
 native_receipt jsonb check(jsonb_typeof(native_receipt)='object' and octet_length(native_receipt::text)<=65536),
 owned_source_after jsonb check(jsonb_typeof(owned_source_after)='array' and jsonb_array_length(owned_source_after)<=200 and octet_length(owned_source_after::text)<=32768),
 outcome_code text check(length(outcome_code) between 1 and 128),
 result_body jsonb not null check(jsonb_typeof(result_body)='object' and octet_length(result_body::text)<=120000),
 captured_at_utc timestamptz not null default transaction_timestamp(),primary key(request_id,phase,step_no),
 check((native_body is null)=(subrequest_id is null)),check(native_receipt is null or native_body is not null),check(owned_source_after is null or native_receipt is not null)
);
create index bpay_next_decisions_clear_page_work_idx on private.bpay_next_decisions_clear_page(request_id,work_id,step_no desc) where phase='APPLY';
-- Cursor discovery by native current saved owner; no public-table index or trigger.
create index bpay_next_component_resolution_clear_scope_idx on private.bpay_next_component_resolution(work_id,(case when rate_family='STANDARD' then 'WORK_BUCKET' else 'GROSS' end),id) where state='ACTIVE';
create or replace function private.bpay_next_decisions_clear_evidence_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare o private.bpay_next_decisions_clear_operation%rowtype;c private.bpay_next_command%rowtype;
begin
 if tg_op in('DELETE','TRUNCATE') then raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
 if tg_op='UPDATE' then
  if tg_table_name<>'bpay_next_decisions_clear_operation' then
   raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  if old.reset_sequence is not null
   or to_jsonb(new)-array['state','freeze_step','apply_step','member_count','after_work_id','after_family','expected_sequence','reset_sequence','completed_at_utc','owner_count','excluded_count','candidate_count','work_count','cleared_count','refused_count','affected_amount','cleared_amount']
    is distinct from to_jsonb(old)-array['state','freeze_step','apply_step','member_count','after_work_id','after_family','expected_sequence','reset_sequence','completed_at_utc','owner_count','excluded_count','candidate_count','work_count','cleared_count','refused_count','affected_amount','cleared_amount']
   or new.freeze_step not between old.freeze_step and old.freeze_step+1 or new.apply_step not between old.apply_step and old.apply_step+1
   or new.member_count not between old.member_count and old.member_count+1 or new.expected_sequence not between old.expected_sequence and old.expected_sequence+1
   or new.owner_count<old.owner_count or new.excluded_count<old.excluded_count or new.candidate_count<old.candidate_count or new.work_count<old.work_count
   or new.cleared_count<old.cleared_count or new.refused_count<old.refused_count or new.affected_amount<old.affected_amount or new.cleared_amount<old.cleared_amount
   or new.cleared_count+new.refused_count>new.owner_count or new.cleared_amount>new.affected_amount
   or not((old.state='DISCOVERING' and new.state in('DISCOVERING','READY')) or(old.state in('READY','APPLYING') and new.state in('APPLYING','COMPLETE','PARTIAL')))
   or(old.state<>'DISCOVERING' and(new.freeze_step,new.member_count,new.owner_count,new.excluded_count,new.candidate_count,new.work_count,new.affected_amount,new.after_work_id,new.after_family)
     is distinct from(old.freeze_step,old.member_count,old.owner_count,old.excluded_count,old.candidate_count,old.work_count,old.affected_amount,old.after_work_id,old.after_family)) then
    raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  if new.state in('COMPLETE','PARTIAL') then
   select x.* into c from private.bpay_next_command x where x.id=new.request_id and x.agency_sequence=new.reset_sequence;
   if old.apply_step<>old.member_count or new.apply_step<>old.apply_step+1 or c.id is null or c.command_kind<>'DECISIONS_CLEAR'
    or c.module_epoch<>new.owner_epoch or c.status<>'COMPLETE' or c.sealed_at_utc is null or c.expected_member_count is distinct from 0 or c.enrolled_member_count<>0
    or exists(select 1 from private.bpay_next_command_member x where x.command_id=c.id)
    or exists(select 1 from private.bpay_next_job x where x.command_id=c.id)
    or new.completed_at_utc is null or(new.state='PARTIAL')<>(new.refused_count>0) then
     raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  elsif new.reset_sequence is not null or new.completed_at_utc is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  return new;
 end if;
 if tg_table_name='bpay_next_decisions_clear_operation' then
  if not(new.initial_args?&array['request_id','scope','owner_epoch','official_pay_date','expected_workbench_revision','phase','step','cursor','confirmed'])
   or new.state<>'DISCOVERING' or new.freeze_step<>0 or new.apply_step<>0 or new.member_count<>0 or new.reset_sequence is not null
   or new.expected_sequence<>new.initial_sequence or new.completed_at_utc is not null
   or new.initial_args->>'request_id' is distinct from new.request_id::text or new.initial_args->'scope' is distinct from new.scope
   or new.initial_args->>'owner_epoch' is distinct from new.owner_epoch::text or new.initial_args->>'official_pay_date' is distinct from new.official_pay_date::text
   or new.initial_args->>'expected_workbench_revision' is distinct from new.expected_workbench_revision::text
   or new.initial_args->>'phase' is distinct from 'FREEZE' or new.initial_args->>'step' is distinct from '0' or new.initial_args->'confirmed' is distinct from 'false'::jsonb
   or new.initial_sequence is distinct from(select last_sequence from private.bpay_next_command_clock where id=1)
   or new.expected_workbench_revision is distinct from(select workbench_revision from private.bpay_next_workbench_revision where id=1)
   or new.owner_epoch is distinct from(select owner_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT')
   or(new.owner_count,new.excluded_count,new.candidate_count,new.work_count,new.cleared_count,new.refused_count,new.affected_amount,new.cleared_amount)
    is distinct from(0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::numeric,0::numeric) then
    raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  return new;
 end if;
 select x.* into strict o from private.bpay_next_decisions_clear_operation x where x.request_id=new.request_id;
 if tg_table_name='bpay_next_decisions_clear_member' then
  if o.state<>'DISCOVERING' or new.member_no<>o.member_count+1 then
   raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
 elsif tg_table_name='bpay_next_decisions_clear_page' then
  if not(new.request_body?&array['request_id','scope','owner_epoch','official_pay_date','expected_workbench_revision','phase','step','cursor','confirmed'])
   or not(new.result_body?&array['request_id','phase','step','received','replay','processing','result'])
   or new.request_body->>'request_id' is distinct from new.request_id::text or new.request_body->'scope' is distinct from o.scope
   or new.request_body->>'owner_epoch' is distinct from o.owner_epoch::text or new.request_body->>'official_pay_date' is distinct from o.official_pay_date::text
   or new.request_body->>'expected_workbench_revision' is distinct from o.expected_workbench_revision::text
   or new.request_body->>'phase' is distinct from new.phase or new.request_body->>'step' is distinct from new.step_no::text
   or new.request_body->'confirmed' is distinct from to_jsonb(new.phase='APPLY')
   or new.result_body->>'request_id' is distinct from new.request_id::text or new.result_body->>'phase' is distinct from new.phase or new.result_body->>'step' is distinct from new.step_no::text
   or new.result_body->'received' is distinct from 'true'::jsonb or new.result_body->'replay' is distinct from 'false'::jsonb or new.result_body->'processing' is distinct from 'false'::jsonb
   or new.result_body#>>'{result,state}' is distinct from o.state or new.result_body#>>'{result,actor_user_id}' is distinct from o.actor_user_id::text
   or new.result_body#>>'{result,owner_epoch}' is distinct from o.owner_epoch::text or new.result_body#>>'{result,official_pay_date}' is distinct from o.official_pay_date::text
   or new.result_body#>'{result,scope}' is distinct from o.scope or(new.result_body#>>'{result,reset_sequence}') is distinct from o.reset_sequence::text
   or(new.phase='FREEZE' and(new.step_no+1<>o.freeze_step or new.native_receipt is not null or o.state not in('DISCOVERING','READY')))
   or(new.phase='APPLY' and(new.step_no+1<>o.apply_step or o.state not in('APPLYING','COMPLETE','PARTIAL'))) then
    raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  if new.native_receipt is not null then
   if new.phase<>'APPLY' or new.outcome_code is not null or new.native_body->>'request_id' is distinct from new.subrequest_id::text
    or new.native_receipt->>'request_id' is distinct from new.subrequest_id::text or new.native_receipt->'received' is distinct from 'true'::jsonb
    or new.native_receipt#>>'{result,action}' is distinct from 'RESOLUTION_CLEAR' or jsonb_array_length(new.native_receipt#>'{result,works}')<>1
    or new.native_receipt#>>'{result,works,0,work_id}' is distinct from new.work_id::text
    or new.native_receipt#>>'{result,works,0,agency_sequence}' is distinct from o.expected_sequence::text
    or not exists(select 1 from private.bpay_next_workbench_command_receipt nr where nr.request_id=new.subrequest_id
     and nr.actor_user_id=o.actor_user_id and nr.action='RESOLUTION_CLEAR' and nr.module_epoch=o.owner_epoch
     and nr.work_id=new.work_id and nr.candidate_id=(new.native_body->>'candidate_id')::uuid
     and nr.request_body=new.native_body and nr.result=new.native_receipt->'result')
    or not exists(select 1 from private.bpay_next_decisions_clear_member m where m.request_id=new.request_id and m.member_no=new.step_no+1 and m.work_id=new.work_id) then
     raise exception using errcode='23514',message='BPAY_NEXT_DECISIONS_CLEAR_EVIDENCE_IMMUTABLE';end if;
  end if;
 end if;
 return new;
end
$function$;
alter function private.bpay_next_decisions_clear_evidence_guard_v1() owner to postgres;
revoke all on function private.bpay_next_decisions_clear_evidence_guard_v1() from public,anon,authenticated,service_role;
create trigger bpay_next_decisions_clear_operation_guard before insert or update or delete on private.bpay_next_decisions_clear_operation for each row execute function private.bpay_next_decisions_clear_evidence_guard_v1();
create trigger bpay_next_decisions_clear_member_guard before insert or update or delete on private.bpay_next_decisions_clear_member for each row execute function private.bpay_next_decisions_clear_evidence_guard_v1();
create trigger bpay_next_decisions_clear_page_guard before insert or update or delete on private.bpay_next_decisions_clear_page for each row execute function private.bpay_next_decisions_clear_evidence_guard_v1();
create trigger bpay_next_decisions_clear_operation_truncate_guard before truncate on private.bpay_next_decisions_clear_operation for each statement execute function private.bpay_next_decisions_clear_evidence_guard_v1();
create trigger bpay_next_decisions_clear_member_truncate_guard before truncate on private.bpay_next_decisions_clear_member for each statement execute function private.bpay_next_decisions_clear_evidence_guard_v1();
create trigger bpay_next_decisions_clear_page_truncate_guard before truncate on private.bpay_next_decisions_clear_page for each statement execute function private.bpay_next_decisions_clear_evidence_guard_v1();
alter table private.bpay_next_decisions_clear_operation owner to postgres;
alter table private.bpay_next_decisions_clear_member owner to postgres;
alter table private.bpay_next_decisions_clear_page owner to postgres;
alter table private.bpay_next_decisions_clear_operation enable row level security;
alter table private.bpay_next_decisions_clear_operation force row level security;
alter table private.bpay_next_decisions_clear_member enable row level security;
alter table private.bpay_next_decisions_clear_member force row level security;
alter table private.bpay_next_decisions_clear_page enable row level security;
alter table private.bpay_next_decisions_clear_page force row level security;
create policy bpay_next_decisions_clear_operation_owner on private.bpay_next_decisions_clear_operation to postgres using(true) with check(true);
create policy bpay_next_decisions_clear_member_owner on private.bpay_next_decisions_clear_member to postgres using(true) with check(true);
create policy bpay_next_decisions_clear_page_owner on private.bpay_next_decisions_clear_page to postgres using(true) with check(true);
revoke all on private.bpay_next_decisions_clear_operation,private.bpay_next_decisions_clear_member,private.bpay_next_decisions_clear_page from public,anon,authenticated,service_role;
commit;
