-- One automatic ordinary WORK debt; W is noncash forgiveness, never R inverse.
-- Policy X: no run/hold/instruction mutation, legacy call or history chooser.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_write_off_receipt_v1(p_command_id uuid,p_replay boolean)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request private.bpay_next_write_off_request%rowtype;v_sequence bigint;
begin
  select r.* into strict v_request from private.bpay_next_write_off_request r where r.command_id=p_command_id;
  select c.agency_sequence into strict v_sequence from private.bpay_next_command c
    where c.id=p_command_id and c.command_kind='CASE_WRITE_OFF';
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'case_id',v_request.case_id,
    'case_component_id',v_request.case_component_id,'sequence',v_sequence::text,'phase',v_request.status,
    'requested_scope',v_request.requested_scope,'applied_amount',v_request.applied_amount::text,
    'remaining_amount',v_request.remaining_amount::text,'protected_amount',v_request.protected_amount::text,
    'event_id',v_request.event_id,'issue_code',v_request.issue_code,'replay',p_replay);
end
$function$;

create or replace function private.bpay_next_accept_write_off_v1(
  p_command_id uuid,p_case_component_id uuid,p_actor_user_id uuid,p_expected_component_revision bigint,
  p_scope text,p_input_amount text,p_reason text
) returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_epoch bigint;v_candidate uuid;v_prior private.bpay_next_write_off_request%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_amount numeric;v_sequence bigint;
begin
  if p_command_id is null or p_case_component_id is null or p_actor_user_id is null
     or p_expected_component_revision is null or p_expected_component_revision<1
     or p_scope is null or p_scope not in ('AMOUNT','ALL') or p_reason is null
     or pg_catalog.octet_length(p_reason) not between 1 and 1024 or pg_catalog.btrim(p_reason)=''
     or (p_scope='ALL' and p_input_amount is not null)
     or (p_scope='AMOUNT' and (p_input_amount is null or pg_catalog.octet_length(p_input_amount)>1024
       or p_input_amount!~'^(0|[1-9][0-9]*)([.][0-9]{1,2})?$')) then
    raise exception using errcode='22023',message='BPAY_NEXT_WRITE_OFF_INPUT_INVALID';
  end if;
  if p_scope='AMOUNT' then
    v_amount:=p_input_amount::numeric;
    if v_amount<=0 or v_amount<>pg_catalog.trunc(v_amount,2) then
      raise exception using errcode='22023',message='BPAY_NEXT_WRITE_OFF_INPUT_INVALID';end if;
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_WRITE_OFF_ACTOR_INVALID';end if;
  -- Same bare UUID mutex as case TRANSFER/CANCEL, BEFORE any Candidate lock.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_command_id::text,0));
  select r.* into v_prior from private.bpay_next_write_off_request r where r.command_id=p_command_id;
  if found then
    if (v_prior.case_component_id,v_prior.actor_user_id,v_prior.expected_component_revision,
        v_prior.requested_scope,v_prior.input_amount,v_prior.reason)
       is distinct from (p_case_component_id,p_actor_user_id,p_expected_component_revision,p_scope,p_input_amount,p_reason)
       or not exists(select 1 from private.bpay_next_command c where c.id=p_command_id
         and c.command_kind='CASE_WRITE_OFF' and c.module_epoch=v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REPLAY_CONFLICT';end if;
    return private.bpay_next_write_off_receipt_v1(p_command_id,true);
  end if;
  if exists(select 1 from private.bpay_next_command c where c.id=p_command_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMMAND_REPLAY_KIND_CONFLICT';end if;
  select x.candidate_id into strict v_candidate from private.bpay_next_case_component x where x.id=p_case_component_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_CONTROL_MISSING';end if;
  select c.* into strict v_case from private.bpay_next_finance_case c
    join private.bpay_next_case_component x on x.case_id=c.id and x.candidate_id=c.candidate_id
    where x.id=p_case_component_id for update of c;
  select x.* into strict v_component from private.bpay_next_case_component x where x.id=p_case_component_id for update;
  v_basis:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
  if v_basis.id is null or v_component.component_revision<>p_expected_component_revision
     or v_case.status not in ('OPEN','PAUSED') then
    raise exception using errcode='55000',message='BPAY_NEXT_WRITE_OFF_NOT_ELIGIBLE';end if;
  -- No financial change here; earlier ordered jobs can finish first. A changed
  -- basis is a retained REVIEW result at application, never a repriced request.
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASE_WRITE_OFF');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,1);
  insert into private.bpay_next_write_off_request(command_id,candidate_id,case_id,case_component_id,work_collection_id,
    actor_user_id,expected_component_revision,basis_case_revision,basis_principal,basis_recovered,basis_written_off,basis_protected,
    requested_scope,input_amount,requested_amount,reason)
    values(p_command_id,v_candidate,v_case.id,v_component.id,v_basis.id,p_actor_user_id,p_expected_component_revision,
      v_case.case_revision,v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount,
      p_scope,p_input_amount,v_amount,p_reason);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id and status='RECEIVED';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COMMAND_INVALID';end if;
  return private.bpay_next_write_off_receipt_v1(p_command_id,false);
end
$function$;

create or replace function private.bpay_next_claim_write_off_job_v1(p_job_id uuid,p_lease_seconds integer default 120)
returns table(lease_nonce uuid,owner_epoch bigint) language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_candidate uuid;v_job private.bpay_next_job%rowtype;v_nonce uuid;v_owner bigint;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_WRITE_OFF_LEASE_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'CASE_WRITE_OFF' or v_job.phase<>'NEW' or v_job.cursor_key is not null
     or v_job.status not in ('READY','LEASED') or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE';end if;
  if not exists(select 1 from private.bpay_next_write_off_request r where r.command_id=v_job.command_id
      and r.candidate_id=v_candidate and r.status='REQUESTED') then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,lease_nonce=v_nonce,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1 where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner;return next;
end
$function$;

-- Owner identity is relational, never a mutable GUC or caller-supplied clock.
create or replace function private.bpay_next_write_off_owner_v1(p_command_id uuid,p_candidate_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select exists(select 1 from private.bpay_next_job j
   join private.bpay_next_command cmd on cmd.id=j.command_id and cmd.command_kind='CASE_WRITE_OFF'
   join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
   join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
   where j.command_id=p_command_id and j.candidate_id=p_candidate_id and j.job_kind='CASE_WRITE_OFF'
     and j.status='LEASED' and j.phase='NEW' and j.cursor_key is null and j.lease_until_utc>pg_catalog.clock_timestamp()
     and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
       and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE'))
$function$;

create or replace function private.bpay_next_write_off_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private,public
as $function$
declare v_request private.bpay_next_write_off_request%rowtype;v_event private.bpay_next_case_event%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_delta numeric;v_basis private.bpay_next_work_collection%rowtype;v_case_id uuid;
begin
  if tg_table_name='bpay_next_write_off_request' then
    if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_IMMUTABLE';end if;
    if tg_op='INSERT' then
      v_basis:=private.bpay_next_work_collection_origin_v1(new.case_id,new.case_component_id,new.candidate_id);
      select * into strict v_case from private.bpay_next_finance_case where id=new.case_id;
      select * into strict v_component from private.bpay_next_case_component where id=new.case_component_id;
      if new.status<>'REQUESTED' or v_basis.id is distinct from new.work_collection_id
         or new.accepted_at_utc is distinct from pg_catalog.transaction_timestamp()
         or (new.expected_component_revision,new.basis_case_revision,new.basis_principal,new.basis_recovered,new.basis_written_off,new.basis_protected)
           is distinct from (v_component.component_revision,v_case.case_revision,v_case.principal_approved,
             v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         or (new.requested_scope='AMOUNT' and new.requested_amount is distinct from new.input_amount::numeric)
         or not exists(select 1 from private.bpay_next_command c join private.bpay_next_command_member cm on cm.command_id=c.id
           join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch
           where c.id=new.command_id and c.command_kind='CASE_WRITE_OFF' and c.status='RECEIVED'
             and cm.candidate_id=new.candidate_id and cm.member_no=1)
         or not exists(select 1 from public.tms_users u where u.id=new.actor_user_id and u.is_active is true and u.role::text='admin') then
        raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_SCOPE_INVALID';end if;
      return new;
    end if;
    if old.status<>'REQUESTED' or new.status not in ('WRITTEN_OFF','BLOCKED','REVIEW')
       or new.completed_at_utc is distinct from pg_catalog.transaction_timestamp()
       or (pg_catalog.to_jsonb(new)-array['status','applied_amount','remaining_amount','protected_amount','event_id','issue_code','completed_at_utc'])
         is distinct from (pg_catalog.to_jsonb(old)-array['status','applied_amount','remaining_amount','protected_amount','event_id','issue_code','completed_at_utc'])
       or not private.bpay_next_write_off_owner_v1(old.command_id,old.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_IMMUTABLE';end if;
    select * into strict v_case from private.bpay_next_finance_case where id=new.case_id;
    select * into strict v_component from private.bpay_next_case_component where id=new.case_component_id;
    if new.remaining_amount is distinct from v_case.principal_approved-v_case.principal_recovered-v_case.principal_written_off
       or new.protected_amount is distinct from v_case.active_recovery_hold_amount
       or (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_RESULT_BALANCE_INVALID';end if;
    if new.status='WRITTEN_OFF' then
      select * into strict v_event from private.bpay_next_case_event where id=new.event_id;
      if v_event.case_id<>new.case_id or v_event.operation_id<>new.command_id or v_event.operation_item_id<>new.work_collection_id
         or v_event.event_kind<>'WRITTEN_OFF' or v_event.written_off_delta is distinct from new.applied_amount
         or v_case.principal_written_off is distinct from new.basis_written_off+new.applied_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_RESULT_EVENT_INVALID';end if;
    elsif exists(select 1 from private.bpay_next_case_event where operation_id=new.command_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_BLOCKED_EVENT_FORBIDDEN';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_case_event' then
    if new.event_kind<>'WRITTEN_OFF' then return new;end if;
    select * into v_request from private.bpay_next_write_off_request where command_id=new.operation_id;
    if v_request.command_id is null or v_request.status<>'REQUESTED' or new.id<>v_request.command_id
       or new.case_id<>v_request.case_id or new.operation_item_id<>v_request.work_collection_id
       or new.written_off_delta<=0 or new.approved_delta<>0 or new.funded_delta<>0 or new.recovered_delta<>0
       or new.paid_credit_delta<>0 or new.original_transfer_id is not null or new.original_effect_id is not null
       or new.instruction_id is not null or new.allocation_result_id is not null
       or new.occurred_at_utc is distinct from pg_catalog.transaction_timestamp()
       or not private.bpay_next_write_off_owner_v1(v_request.command_id,v_request.candidate_id)
       or (v_request.requested_scope='AMOUNT' and new.written_off_delta is distinct from v_request.requested_amount)
       or (v_request.requested_scope='ALL' and new.written_off_delta is distinct from
         v_request.basis_principal-v_request.basis_recovered-v_request.basis_written_off)
       or new.written_off_delta>v_request.basis_principal-v_request.basis_recovered-v_request.basis_written_off-v_request.basis_protected then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_EVENT_SCOPE_INVALID';end if;
    return new;
  end if;
  -- A second use of the same event fails its exact BEFORE W basis. This also
  -- prevents forging a counter without a real ordered request/positive event.
  if tg_table_name='bpay_next_finance_case' then
    v_delta:=new.principal_written_off-old.principal_written_off;v_case_id:=old.id;
  else
    v_delta:=new.written_off_source_ex_vat-old.written_off_source_ex_vat;v_case_id:=old.case_id;
  end if;
  if v_delta=0 then return new;end if;
  select r.* into v_request from private.bpay_next_job j
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_write_off_request r on r.command_id=j.command_id
    join private.bpay_next_case_event e on e.id=j.command_id
    where j.candidate_id=old.candidate_id and j.job_kind='CASE_WRITE_OFF' and j.status='LEASED'
      and r.case_id=v_case_id and r.status='REQUESTED' and e.event_kind='WRITTEN_OFF' and e.written_off_delta=v_delta
      and private.bpay_next_write_off_owner_v1(r.command_id,r.candidate_id);
  if not found or v_delta<=0 or not private.bpay_next_write_off_owner_v1(v_request.command_id,v_request.candidate_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COUNTER_FORGERY';end if;
  if tg_table_name='bpay_next_finance_case' then
    if old.principal_written_off<>v_request.basis_written_off or old.case_revision<>v_request.basis_case_revision
       or (old.principal_approved,old.principal_recovered,old.principal_written_off,old.active_recovery_hold_amount)
         is distinct from (v_request.basis_principal,v_request.basis_recovered,v_request.basis_written_off,v_request.basis_protected)
       or new.case_revision<>old.case_revision+1
       or (pg_catalog.to_jsonb(new)-array['principal_written_off','case_revision','status','updated_at_utc'])
         is distinct from (pg_catalog.to_jsonb(old)-array['principal_written_off','case_revision','status','updated_at_utc'])
       or new.status is distinct from (case when new.principal_approved-new.principal_recovered-new.principal_written_off=0
         then 'WRITTEN_OFF' when old.status='PAUSED' then 'PAUSED' else 'OPEN' end) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COUNTER_FORGERY';end if;
  elsif old.id<>v_request.case_component_id or old.written_off_source_ex_vat<>v_request.basis_written_off
     or (old.approved_source_ex_vat,old.recovered_source_ex_vat,old.written_off_source_ex_vat,old.active_recovery_source_ex_vat)
       is distinct from (v_request.basis_principal,v_request.basis_recovered,v_request.basis_written_off,v_request.basis_protected)
     or old.component_revision<>v_request.expected_component_revision or new.component_revision<>old.component_revision+1
     or (pg_catalog.to_jsonb(new)-array['written_off_source_ex_vat','component_revision','updated_at_utc'])
       is distinct from (pg_catalog.to_jsonb(old)-array['written_off_source_ex_vat','component_revision','updated_at_utc']) then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COUNTER_FORGERY';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_write_off_commit_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
begin
  if tg_table_name='bpay_next_write_off_request' then
    if not exists(select 1 from private.bpay_next_write_off_request r
      join private.bpay_next_job j on j.command_id=r.command_id and j.candidate_id=r.candidate_id and j.job_kind='CASE_WRITE_OFF'
      join private.bpay_next_command c on c.id=r.command_id and c.command_kind='CASE_WRITE_OFF'
      where r.command_id=new.command_id and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW')
        and c.status=(case when r.status='WRITTEN_OFF' then 'COMPLETE' else 'FAILED' end)
        and j.status='DONE' and j.phase='DONE' and j.cursor_key is null) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_INCOMPLETE_UNIT';end if;
    return null;
  end if;
  if new.event_kind='WRITTEN_OFF' and not exists(select 1 from private.bpay_next_write_off_request r
      join private.bpay_next_job j on j.command_id=r.command_id and j.candidate_id=r.candidate_id and j.job_kind='CASE_WRITE_OFF'
      join private.bpay_next_command c on c.id=r.command_id and c.command_kind='CASE_WRITE_OFF' and c.status='COMPLETE'
      where r.command_id=new.id and r.case_id=new.case_id and r.status='WRITTEN_OFF' and r.event_id=new.id
        and r.applied_amount=new.written_off_delta and j.status='DONE' and j.phase='DONE' and j.cursor_key is null) then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_INCOMPLETE_UNIT';end if;
  return null;
end
$function$;

create or replace function private.bpay_next_apply_write_off_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_write_off_request%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_basis private.bpay_next_work_collection%rowtype;
  v_remaining numeric;v_amount numeric;v_phase text;v_issue text;v_receipt jsonb;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_WRITE_OFF_APPLY_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select r.* into strict v_request from private.bpay_next_write_off_request r where r.command_id=v_job.command_id for update;
  if v_job.job_kind<>'CASE_WRITE_OFF' or v_job.module_epoch<>v_epoch or v_request.candidate_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_job.cursor_key is null then
    if v_request.status='REQUESTED' or not exists(select 1 from private.bpay_next_command c where c.id=v_job.command_id
      and c.command_kind='CASE_WRITE_OFF' and c.module_epoch=v_epoch
      and c.status=case when v_request.status='WRITTEN_OFF' then 'COMPLETE' else 'FAILED' end)
      or (v_request.status='WRITTEN_OFF' and not exists(select 1 from private.bpay_next_case_event e
        where e.id=v_request.event_id and e.case_id=v_request.case_id and e.operation_id=v_request.command_id
          and e.operation_item_id=v_request.work_collection_id and e.event_kind='WRITTEN_OFF' and e.written_off_delta=v_request.applied_amount)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_TERMINAL_EVIDENCE_INVALID';end if;
    v_receipt:=private.bpay_next_write_off_receipt_v1(v_job.command_id,true);
    return v_receipt-array['command_id','sequence','requested_scope'];
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.cursor_key is not null
     or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or not private.bpay_next_write_off_owner_v1(v_job.command_id,v_candidate)
     or v_request.status<>'REQUESTED' then
    raise exception using errcode='55000',message='BPAY_NEXT_WRITE_OFF_LEASE_STALE';end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_request.work_collection_id;
  perform 1 from private.bpay_next_position where work_id=v_basis.work_id and component_key=v_basis.component_key for update;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_request.case_id for update;
  select x.* into strict v_component from private.bpay_next_case_component x where x.id=v_request.case_component_id for update;
  v_basis:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
  v_remaining:=v_case.principal_approved-v_case.principal_recovered-v_case.principal_written_off;
  if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
     is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
       v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat)
     or v_remaining<v_case.active_recovery_hold_amount then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_BALANCE_INVALID';end if;
  v_phase:='WRITTEN_OFF';
  if v_component.component_revision<>v_request.expected_component_revision or v_case.case_revision<>v_request.basis_case_revision then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED';
  elsif v_basis.id is null or v_case.status not in ('OPEN','PAUSED') then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND';
  elsif v_remaining=0 then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_NO_BALANCE';
  else
    v_amount:=case when v_request.requested_scope='ALL' then v_remaining else v_request.requested_amount end;
    if v_amount>v_remaining then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE';
    elsif v_amount>v_remaining-v_case.active_recovery_hold_amount then
      v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY';end if;
  end if;
  if v_phase='WRITTEN_OFF' then
    insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,written_off_delta)
      values(v_request.command_id,v_case.id,v_request.command_id,v_request.work_collection_id,'WRITTEN_OFF',v_amount);
    update private.bpay_next_case_component set written_off_source_ex_vat=written_off_source_ex_vat+v_amount,
      component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
    update private.bpay_next_finance_case set principal_written_off=principal_written_off+v_amount,case_revision=case_revision+1,
      status=case when v_remaining-v_amount=0 then 'WRITTEN_OFF' when status='PAUSED' then 'PAUSED' else 'OPEN' end,
      updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
    v_remaining:=v_remaining-v_amount;
    update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
      updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  else v_amount:=0;end if;
  update private.bpay_next_write_off_request set status=v_phase,applied_amount=v_amount,remaining_amount=v_remaining,
    protected_amount=v_case.active_recovery_hold_amount,event_id=case when v_phase='WRITTEN_OFF' then command_id else null end,
    issue_code=v_issue,completed_at_utc=pg_catalog.transaction_timestamp() where command_id=v_request.command_id;
  update private.bpay_next_job set status='DONE',phase='DONE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status=case when v_phase='WRITTEN_OFF' then 'COMPLETE' else 'FAILED' end where id=v_job.command_id;
  v_receipt:=private.bpay_next_write_off_receipt_v1(v_job.command_id,false);
  return v_receipt-array['command_id','sequence','requested_scope'];
end
$function$;

drop trigger if exists bpay_next_write_off_request_guard_v1 on private.bpay_next_write_off_request;
create trigger bpay_next_write_off_request_guard_v1 before insert or update or delete on private.bpay_next_write_off_request
  for each row execute function private.bpay_next_write_off_guard_v1();
drop trigger if exists bpay_next_write_off_request_truncate_v1 on private.bpay_next_write_off_request;
create trigger bpay_next_write_off_request_truncate_v1 before truncate on private.bpay_next_write_off_request
  for each statement execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_write_off_event_guard_v1 on private.bpay_next_case_event;
create trigger bpay_next_write_off_event_guard_v1 before insert on private.bpay_next_case_event
  for each row execute function private.bpay_next_write_off_guard_v1();
drop trigger if exists bpay_next_write_off_case_counter_guard_v1 on private.bpay_next_finance_case;
create trigger bpay_next_write_off_case_counter_guard_v1 before update of principal_written_off on private.bpay_next_finance_case
  for each row execute function private.bpay_next_write_off_guard_v1();
drop trigger if exists bpay_next_write_off_component_counter_guard_v1 on private.bpay_next_case_component;
create trigger bpay_next_write_off_component_counter_guard_v1 before update of written_off_source_ex_vat on private.bpay_next_case_component
  for each row execute function private.bpay_next_write_off_guard_v1();
drop trigger if exists bpay_next_write_off_commit_guard_v1 on private.bpay_next_case_event;
create constraint trigger bpay_next_write_off_commit_guard_v1 after insert on private.bpay_next_case_event
  deferrable initially deferred for each row execute function private.bpay_next_write_off_commit_guard_v1();
drop trigger if exists bpay_next_write_off_request_commit_guard_v1 on private.bpay_next_write_off_request;
create constraint trigger bpay_next_write_off_request_commit_guard_v1 after update on private.bpay_next_write_off_request
  deferrable initially deferred for each row execute function private.bpay_next_write_off_commit_guard_v1();

alter function private.bpay_next_write_off_receipt_v1(uuid,boolean) owner to postgres;
alter function private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text) owner to postgres;
alter function private.bpay_next_claim_write_off_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_apply_write_off_v1(uuid,uuid,bigint) owner to postgres;
alter function private.bpay_next_write_off_owner_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_write_off_guard_v1() owner to postgres;
alter function private.bpay_next_write_off_commit_guard_v1() owner to postgres;
revoke all on function private.bpay_next_write_off_receipt_v1(uuid,boolean),
  private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text),private.bpay_next_claim_write_off_job_v1(uuid,integer),
  private.bpay_next_apply_write_off_v1(uuid,uuid,bigint),private.bpay_next_write_off_owner_v1(uuid,uuid),
  private.bpay_next_write_off_guard_v1(),private.bpay_next_write_off_commit_guard_v1() from public,anon,authenticated,service_role;

commit;
