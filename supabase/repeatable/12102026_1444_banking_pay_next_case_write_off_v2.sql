-- FC11 / BD146: typed no-origin RECOVERY write-off; original Work forgiveness retained.
\set ON_ERROR_STOP on
begin;

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
  if (v_basis.id is null and (v_component.instruction_kind='RECOVERY' and v_component.direction='DEDUCTION' and v_case.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and (v_component.case_id,v_component.candidate_id,v_component.case_kind,v_component.rule_id,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,v_component.recovered_source_ex_vat,v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) is not distinct from (v_case.id,v_case.candidate_id,v_case.case_kind,v_case.current_rule_id,v_case.principal_approved,v_case.principal_funded,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)) is not true) or v_component.component_revision<>p_expected_component_revision
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
      v_case.case_revision,(case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end),v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount,
      p_scope,p_input_amount,v_amount,p_reason);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id and status='RECEIVED';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COMMAND_INVALID';end if;
  return private.bpay_next_write_off_receipt_v1(p_command_id,false);
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
          and e.operation_item_id=coalesce(v_request.work_collection_id,v_request.case_component_id) and e.event_kind='WRITTEN_OFF' and e.written_off_delta=v_request.applied_amount)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_TERMINAL_EVIDENCE_INVALID';end if;
    v_receipt:=private.bpay_next_write_off_receipt_v1(v_job.command_id,true);
    return v_receipt-array['command_id','sequence','requested_scope'];
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.cursor_key is not null
     or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or not private.bpay_next_write_off_owner_v1(v_job.command_id,v_candidate)
     or v_request.status<>'REQUESTED' then
    raise exception using errcode='55000',message='BPAY_NEXT_WRITE_OFF_LEASE_STALE';end if;
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
  if v_request.work_collection_id is not null then
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_request.work_collection_id;
    perform 1 from private.bpay_next_position where work_id=v_basis.work_id and component_key=v_basis.component_key for update;
  end if;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_request.case_id for update;
  select x.* into strict v_component from private.bpay_next_case_component x where x.id=v_request.case_component_id for update;
  v_basis:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
  v_remaining:=(case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end)-v_case.principal_recovered-v_case.principal_written_off;
  if ((case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end),v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
     is distinct from ((case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end),v_component.recovered_source_ex_vat,
       v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat)
     or v_remaining<v_case.active_recovery_hold_amount then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_BALANCE_INVALID';end if;
  v_phase:='WRITTEN_OFF';
  if v_component.component_revision<>v_request.expected_component_revision or v_case.case_revision<>v_request.basis_case_revision then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED';
  elsif v_basis.id is distinct from v_request.work_collection_id
     or (v_request.work_collection_id is null and (v_component.instruction_kind='RECOVERY' and v_component.direction='DEDUCTION' and v_case.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and (v_component.case_id,v_component.candidate_id,v_component.case_kind,v_component.rule_id,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,v_component.recovered_source_ex_vat,v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) is not distinct from (v_case.id,v_case.candidate_id,v_case.case_kind,v_case.current_rule_id,v_case.principal_approved,v_case.principal_funded,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)) is not true)
     or v_case.status not in ('OPEN','PAUSED') then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND';
  elsif v_remaining=0 then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_NO_BALANCE';
  else
    v_amount:=case when v_request.requested_scope='ALL' then v_remaining else v_request.requested_amount end;
    if v_amount>v_remaining then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE';
    elsif v_amount>v_remaining-v_case.active_recovery_hold_amount then
      v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY';end if;
  end if;
  if v_phase='WRITTEN_OFF' then
    -- FC-16: fresh post-component-lock reproof; never acquire a run header
    -- here or release a Candidate recursively from this money owner.
    begin
      perform private.bpay_next_case_change_assert_v1(v_case.id,v_case.case_revision,
        case when v_request.requested_scope='ALL' then 'WHOLE_CASE' else 'UNRESERVED_AMOUNT' end,
        case when v_request.requested_scope='ALL' then null else v_amount end,'RECOVERY');
    exception when check_violation then
      if sqlerrm='BPAY_NEXT_CASE_PROTECTION_PENDING' then
        v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_INPUT_PENDING';
      elsif sqlerrm='BPAY_NEXT_CASE_PROTECTED_SLICE' then
        v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY';
      else raise;end if;
    end;
  end if;
  if v_phase='WRITTEN_OFF' then
    insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,written_off_delta)
      values(v_request.command_id,v_case.id,v_request.command_id,coalesce(v_request.work_collection_id,v_request.case_component_id),'WRITTEN_OFF',v_amount);
    update private.bpay_next_case_component set written_off_source_ex_vat=written_off_source_ex_vat+v_amount,
      component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
    update private.bpay_next_finance_case set principal_written_off=principal_written_off+v_amount,case_revision=case_revision+1,
      status=case when v_remaining-v_amount=0 then 'WRITTEN_OFF' when status='PAUSED' then 'PAUSED' else 'OPEN' end,
      updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
    v_remaining:=v_remaining-v_amount;
    update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
      updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
    perform private.bpay_next_workbench_refresh_case_v1(v_case.id);
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
      select fc11_point_1.* into strict v_case from private.bpay_next_finance_case fc11_point_1 where id=new.case_id;
      select fc11_point_2.* into strict v_component from private.bpay_next_case_component fc11_point_2 where id=new.case_component_id;
      if new.status<>'REQUESTED' or v_basis.id is distinct from new.work_collection_id
         or (new.work_collection_id is null and (v_component.instruction_kind='RECOVERY' and v_component.direction='DEDUCTION' and v_case.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and (v_component.case_id,v_component.candidate_id,v_component.case_kind,v_component.rule_id,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,v_component.recovered_source_ex_vat,v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) is not distinct from (v_case.id,v_case.candidate_id,v_case.case_kind,v_case.current_rule_id,v_case.principal_approved,v_case.principal_funded,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)) is not true)
         or new.accepted_at_utc is distinct from pg_catalog.transaction_timestamp()
         or (new.expected_component_revision,new.basis_case_revision,new.basis_principal,new.basis_recovered,new.basis_written_off,new.basis_protected)
           is distinct from (v_component.component_revision,v_case.case_revision,(case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end),
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
    select fc11_point_3.* into strict v_case from private.bpay_next_finance_case fc11_point_3 where id=new.case_id;
    select fc11_point_4.* into strict v_component from private.bpay_next_case_component fc11_point_4 where id=new.case_component_id;
    if new.remaining_amount is distinct from (case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end)-v_case.principal_recovered-v_case.principal_written_off
       or new.protected_amount is distinct from v_case.active_recovery_hold_amount
       or ((case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end),v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from ((case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end),v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_RESULT_BALANCE_INVALID';end if;
    if new.status='WRITTEN_OFF' then
      select fc11_point_5.* into strict v_event from private.bpay_next_case_event fc11_point_5 where id=new.event_id;
      if v_event.case_id<>new.case_id or v_event.operation_id<>new.command_id or v_event.operation_item_id is distinct from coalesce(new.work_collection_id,new.case_component_id)
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
    select fc11_point_6.* into v_request from private.bpay_next_write_off_request fc11_point_6 where command_id=new.operation_id;
    if v_request.command_id is null or v_request.status<>'REQUESTED' or new.id<>v_request.command_id
       or new.case_id<>v_request.case_id or new.operation_item_id is distinct from coalesce(v_request.work_collection_id,v_request.case_component_id)
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
       or ((case when old.case_kind in ('LOAN','ADVANCE') then old.principal_funded else old.principal_approved end),old.principal_recovered,old.principal_written_off,old.active_recovery_hold_amount)
         is distinct from (v_request.basis_principal,v_request.basis_recovered,v_request.basis_written_off,v_request.basis_protected)
       or new.case_revision<>old.case_revision+1
       or (pg_catalog.to_jsonb(new)-array['principal_written_off','case_revision','status','updated_at_utc'])
         is distinct from (pg_catalog.to_jsonb(old)-array['principal_written_off','case_revision','status','updated_at_utc'])
       or new.status is distinct from (case when (case when new.case_kind in ('LOAN','ADVANCE') then new.principal_funded else new.principal_approved end)-new.principal_recovered-new.principal_written_off=0
         then 'WRITTEN_OFF' when old.status='PAUSED' then 'PAUSED' else 'OPEN' end) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COUNTER_FORGERY';end if;
  elsif old.id<>v_request.case_component_id or old.written_off_source_ex_vat<>v_request.basis_written_off
     or ((case when old.case_kind in ('LOAN','ADVANCE') then old.funded_source_ex_vat else old.approved_source_ex_vat end),old.recovered_source_ex_vat,old.written_off_source_ex_vat,old.active_recovery_source_ex_vat)
       is distinct from (v_request.basis_principal,v_request.basis_recovered,v_request.basis_written_off,v_request.basis_protected)
     or old.component_revision<>v_request.expected_component_revision or new.component_revision<>old.component_revision+1
     or (pg_catalog.to_jsonb(new)-array['written_off_source_ex_vat','component_revision','updated_at_utc'])
       is distinct from (pg_catalog.to_jsonb(old)-array['written_off_source_ex_vat','component_revision','updated_at_utc']) then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_COUNTER_FORGERY';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_reauth_scope_v1(
  p_actor uuid,p_action text,p_args jsonb,p_lock boolean default false
) returns jsonb language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_key text;v_intent jsonb:=p_args;v_id uuid;v_epoch bigint;
  v_w private.bpay_next_run_worker%rowtype;v_t private.bpay_next_transfer%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_collection private.bpay_next_work_collection%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;v_prior record;v_facts jsonb;v_amount numeric;
  v_run private.bpay_next_pay_run%rowtype;v_context jsonb;v_guard jsonb;
  v_candidate_id uuid;v_scope text;v_bank_hash text;v_bank_version timestamptz;
  v_replay boolean:=false;v_kind text;v_label text;v_basis text:='CASH';v_name text;
begin
  -- DR-10's policy exception has no pre-Draft monetary amount to certify.
  if p_action='PAYE_SAME_WEEK_OVERRIDE' then
   if p_actor is null or p_lock is null or jsonb_typeof(p_args) is distinct from 'object'
    or octet_length(p_args::text)>2048 or not(p_args ?& array['run_id','command_id','decision','reason','context'])
    or p_args-array['run_id','command_id','decision','reason','context']<>'{}'::jsonb
    or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or jsonb_typeof(p_args->'command_id') is distinct from 'string' or p_args->>'command_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or jsonb_typeof(p_args->'decision') is distinct from 'string' or p_args->>'decision' not in ('VERIFY_BOTH','VERIFY_PAYE')
    or jsonb_typeof(p_args->'reason') is distinct from 'string' or btrim(p_args->>'reason')=''
    or octet_length(p_args->>'reason') not between 1 and 500 then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
   if not exists(select 1 from public.tms_users where id=p_actor and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
   select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
   if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_id:=(p_args->>'command_id')::uuid;
   select r.* into v_run from private.bpay_next_pay_run r where id=(p_args->>'run_id')::uuid;
   if found and v_run.accepted_choice_descriptor is not null then
    if (v_run.actor_user_id,v_run.accepted_choice_command_id,v_run.paye_guard_decision,v_run.paye_override_reason)
      is distinct from (p_actor,v_id,p_args->>'decision',p_args->>'reason')
      or p_args->'context' is distinct from v_run.accepted_choice_descriptor-array['intent','revision','memory_baseline_run_id']
      or v_run.paye_guard_scope_sha256 is null
      or v_run.accepted_choice_descriptor->'namespace'->>'owner_epoch' is distinct from v_epoch::text then
     raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    v_scope:=v_run.paye_guard_scope_sha256;v_replay:=true;
   else
    if exists(select 1 from private.bpay_next_command where id=v_id) then
     raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    v_context:=private.bpay_next_paye_guard_context_v1(p_actor,p_args->'context');
    v_facts:=private.bpay_next_paye_same_week_facts_v1((v_context->'namespace'->>'official_pay_date')::date,(p_args->>'run_id')::uuid);
    if jsonb_array_length(v_facts)=0 or exists(select 1 from jsonb_array_elements(v_facts) f where f->'unexecuted'='true'::jsonb) then
     raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNEXECUTED';end if;
    v_scope:=encode(extensions.digest(jsonb_build_array(p_actor,p_action,v_epoch,p_args,v_facts)::text,'sha256'),'hex');
   end if;
   return jsonb_build_object('action_kind',p_action,'action_label','Verify and create','candidate_count',null,
    'candidate_name','Selected payments','total_amount',null,'amount_basis','PAYE_POLICY','scope_sha256',v_scope,'accepted',v_replay);
  end if;
  if p_actor is null or p_action is null or p_action not in
    ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN')
    or p_args is null or jsonb_typeof(p_args)<>'object' or octet_length(p_args::text)>2048
    or p_lock is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      ||case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      ||case when p_args ? 'run_id' or p_args ? 'pay_channel' then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  if (select count(*) from jsonb_object_keys(p_args))<>array_length(v_keys,1)
    or exists(select 1 from jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
  foreach v_key in array v_keys loop
    if jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
    if v_key in ('command_id','instruction_id','run_worker_id','transfer_id','return_cash_id','run_id','case_component_id') then
      if (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
      v_intent:=jsonb_set(v_intent,array[v_key],to_jsonb((p_args->>v_key)::uuid::text));
    end if;
  end loop;
  v_id:=coalesce(p_args->>'command_id',p_args->>'instruction_id')::uuid;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor and u.is_active is true and u.role::text='admin'
    and (p_action in ('WRITE_OFF','CANCEL_REQUEST') or u.payment_authoriser is true or u.payment_golden_key is true)) then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  v_kind:=case p_action when 'WRITE_OFF' then 'CASE_WRITE_OFF' when 'CANCEL_REQUEST' then 'SIMPLE_CANCEL' else p_action end;
  v_label:=case p_action when 'WRITE_OFF' then 'Write off debt' when 'CANCEL_REQUEST' then 'Cancel payment'
    when 'CASH_REISSUE' then 'Pay returned payment' when 'REISSUE_CANCEL' then 'Cancel returned payment'
    when 'CSV_ISSUE' then 'Create Bank CSV' else 'Record bank return' end;

  if p_action='WRITE_OFF' then
    v_basis:='SOURCE_DEBT';
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
      or (p_args->>'expected_component_revision')::numeric>9223372036854775807
      or p_args->>'scope' not in ('ALL','AMOUNT') or octet_length(p_args->>'reason') not between 1 and 1024
      or btrim(p_args->>'reason')='' or (p_args->>'scope'='AMOUNT' and
        ((p_args->>'amount')!~'^(0|[1-9][0-9]*)([.][0-9]{1,2})?$'
          or octet_length(p_args->>'amount')>1024 or (p_args->>'amount')::numeric<=0)) then
      raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
    v_intent:=jsonb_set(v_intent,'{expected_component_revision}',to_jsonb((p_args->>'expected_component_revision')::bigint::text));
    if p_lock then perform pg_advisory_xact_lock(hashtextextended(v_id::text,0));end if;
    select r.* into strict v_component from private.bpay_next_case_component r where id=(p_args->>'case_component_id')::uuid;
    v_candidate_id:=v_component.candidate_id;
    if p_lock then
      perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
      perform 1 from private.bpay_next_finance_case where id=v_component.case_id for update;
      perform 1 from private.bpay_next_case_component where id=v_component.id for update;
      select r.* into strict v_component from private.bpay_next_case_component r where id=v_component.id;
    end if;
    select r.* into strict v_case from private.bpay_next_finance_case r where id=v_component.case_id and candidate_id=v_candidate_id;
    select r.* into v_prior from private.bpay_next_write_off_request r where command_id=v_id;
    if found then
      if (v_prior.case_component_id,v_prior.actor_user_id,v_prior.expected_component_revision,
        v_prior.requested_scope,v_prior.input_amount,v_prior.reason) is distinct from
        (v_component.id,p_actor,(p_args->>'expected_component_revision')::bigint,p_args->>'scope',p_args->>'amount',p_args->>'reason') then
        raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
      v_replay:=true;
      v_amount:=case when v_prior.requested_scope='ALL' then v_prior.basis_principal-v_prior.basis_recovered-v_prior.basis_written_off
        else v_prior.requested_amount end;
    else
      v_collection:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate_id);
      if (v_collection.id is null and (v_component.instruction_kind='RECOVERY' and v_component.direction='DEDUCTION' and v_case.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and (v_component.case_id,v_component.candidate_id,v_component.case_kind,v_component.rule_id,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,v_component.recovered_source_ex_vat,v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) is not distinct from (v_case.id,v_case.candidate_id,v_case.case_kind,v_case.current_rule_id,v_case.principal_approved,v_case.principal_funded,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)) is not true) or v_case.status not in ('OPEN','PAUSED')
        or v_component.component_revision<>(p_args->>'expected_component_revision')::bigint then
        raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      v_amount:=case when p_args->>'scope'='ALL' then (case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end)-v_case.principal_recovered-v_case.principal_written_off
        else (p_args->>'amount')::numeric end;
      v_facts:=jsonb_build_array(v_candidate_id,v_case.id,v_component.id,v_collection.id,v_case.case_revision,
        v_component.component_revision,(case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded else v_case.principal_approved end),v_case.principal_recovered,
        v_case.principal_written_off,v_case.active_recovery_hold_amount);
    end if;
  else
    if p_action='CANCEL_REQUEST' then
      select r.* into strict v_w from private.bpay_next_run_worker r where id=(p_args->>'run_worker_id')::uuid;
      if p_args ? 'run_id' and (v_w.run_id is distinct from (p_args->>'run_id')::uuid
        or v_w.target_pay_channel is distinct from p_args->>'pay_channel') then
        raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
      if p_lock and v_w.case_selection_revision>0 then perform pg_advisory_xact_lock(hashtextextended(v_id::text,0));end if;
    elsif p_action='CASH_REISSUE' then
      select r.* into strict v_cash from private.bpay_next_return_cash r where id=(p_args->>'return_cash_id')::uuid;
      select r.* into strict v_t from private.bpay_next_transfer r where id=v_cash.original_transfer_id;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_t.run_worker_id;
    else
      select r.* into strict v_t from private.bpay_next_transfer r where id=(p_args->>'transfer_id')::uuid;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_t.run_worker_id;
    end if;
    v_candidate_id:=v_w.candidate_id;
    if p_lock then
      perform 1 from private.bpay_next_pay_run where id=v_w.run_id for update;
      if p_action in ('CANCEL_REQUEST','REISSUE_CANCEL','CSV_RETURN') then
        -- The original simple cancel creates this control if absent, under the same run lock.
        if p_action='CANCEL_REQUEST' and v_w.case_selection_revision=0 then
          insert into private.bpay_next_worker_control(candidate_id) values(v_candidate_id) on conflict(candidate_id) do nothing;
        end if;
        perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
      end if;
      perform 1 from private.bpay_next_run_worker where id=v_w.id for update;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_w.id;
      if p_action in ('CSV_ISSUE','REISSUE_CANCEL','CSV_RETURN') then
        perform 1 from private.bpay_next_transfer where id=v_t.id for update;
        select r.* into strict v_t from private.bpay_next_transfer r where id=v_t.id;
      end if;
      perform 1 from public.tms_users where id=p_actor for share;
      if p_action='CASH_REISSUE' then
        select r.* into strict v_cash from private.bpay_next_return_cash r where id=v_cash.id for update;
      end if;
    end if;
    if p_action='CANCEL_REQUEST' then
      select r.* into v_projection from private.bpay_next_net_projection r where run_worker_id=v_w.id
        and projection_no=v_w.net_projection_revision and retired_at_utc is null;
      v_amount:=coalesce(v_projection.cash_amount,v_w.gross_inc_vat);
      v_basis:=case when v_projection.id is null then 'DRAFT_GROSS' else 'CASH' end;
      select r.* into v_prior from private.bpay_next_cancel_request r where command_id=v_id;
      if found then
        if (v_prior.run_worker_id,v_prior.candidate_id) is distinct from (v_w.id,v_candidate_id)
          or (v_w.case_selection_revision>0 and not exists(select 1 from private.bpay_next_case_cancel_binding
            where command_id=v_id and run_worker_id=v_w.id)) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
        -- Receipt recovery does not claim a live projection as the original
        -- authorised cash. This immutable Draft gross is explicitly labelled.
        v_amount:=v_w.gross_inc_vat;v_basis:='DRAFT_GROSS';
      end if;
      v_facts:=jsonb_build_array(v_w.id,v_w.run_id,v_candidate_id,v_w.target_pay_channel,v_w.target_umbrella_id,
        v_w.captured_preparation_revision,v_w.case_selection_revision,v_w.captured_line_count,
        v_w.gross_ex_vat,v_w.gross_vat,v_w.gross_inc_vat,v_w.net_request_revision,
        v_w.net_projection_revision,v_projection.id,v_projection.cash_amount,v_projection.accepted_recoveries);
    elsif p_action='CASH_REISSUE' then
      select r.* into v_prior from private.bpay_next_reissue_request r where command_id=v_id;
      if found then
        if (v_prior.return_cash_id,v_prior.run_worker_id,v_prior.candidate_id,v_prior.actor_user_id)
          is distinct from (v_cash.id,v_w.id,v_candidate_id,p_actor) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if v_cash.amount_held<>0 or v_cash.amount_reissued_paid<>0 or v_t.status<>'RETURNED'
          or not exists(select 1 from private.bpay_next_return_request where transfer_id=v_t.id and posting_complete)
          or exists(select 1 from private.bpay_next_reissue_request where return_cash_id=v_cash.id and transfer_id is null) then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_amount:=case when v_replay then v_cash.amount_owed
        else v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid end;
      v_facts:=jsonb_build_array(v_cash.id,v_t.id,v_w.id,v_w.run_id,v_candidate_id,
        v_t.beneficiary_kind,v_t.beneficiary_id,v_t.currency,v_cash.amount_owed,v_cash.amount_held,v_cash.amount_reissued_paid);
    elsif p_action='REISSUE_CANCEL' then
      v_amount:=v_t.cash_amount;
      select r.* into v_prior from private.bpay_next_reissue_cancel_request r where command_id=v_id;
      if found then
        if (v_prior.transfer_id,v_prior.return_cash_id,v_prior.run_worker_id,v_prior.candidate_id,v_prior.actor_user_id,v_prior.amount)
          is distinct from (v_t.id,v_t.return_cash_id,v_w.id,v_candidate_id,p_actor,v_t.cash_amount) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if private.bpay_next_reissue_cancel_block_v1(v_t.id) is not null then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.return_cash_id,v_t.original_transfer_id,
        v_t.cash_amount,v_t.currency,v_t.beneficiary_kind,v_t.beneficiary_id,v_t.member_count,v_t.member_cash_sum);
    elsif p_action='CSV_RETURN' then
      if (p_args->>'amount')!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$'
        or (p_args->>'amount')::numeric<=0 or octet_length(p_args->>'receipt_id') not between 1 and 256
        or (p_args->>'occurred_at')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$'
        or not isfinite((p_args->>'occurred_at')::timestamptz) then
        raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
      v_intent:=jsonb_set(v_intent,'{amount}',to_jsonb(round((p_args->>'amount')::numeric,2)::text));
      v_intent:=jsonb_set(v_intent,'{occurred_at}',to_jsonb(to_char((p_args->>'occurred_at')::timestamptz at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
      v_amount:=v_t.cash_amount;
      select r.command_id,r.actor_user_id,r.run_worker_id,r.candidate_id,o.* into v_prior
        from private.bpay_next_transfer_outcome o join private.bpay_next_return_request r on r.outcome_id=o.id
        where o.receipt_id=p_args->>'receipt_id';
      if found then
        if (v_prior.command_id,v_prior.actor_user_id,v_prior.transfer_id,v_prior.run_worker_id,v_prior.candidate_id,
          v_prior.outcome_kind,v_prior.whole_transfer_amount,v_prior.occurred_at_utc) is distinct from
          (v_id,p_actor,v_t.id,v_w.id,v_candidate_id,'RETURNED'::text,(p_args->>'amount')::numeric,(p_args->>'occurred_at')::timestamptz) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if v_t.status<>'SETTLED' or v_t.destination_rail<>'CSV' or v_t.cash_amount<>(p_args->>'amount')::numeric
          or not exists(select 1 from private.bpay_next_outcome_request r join private.bpay_next_transfer_outcome o on o.id=r.outcome_id
            where r.transfer_id=v_t.id and o.outcome_kind='SETTLED' and o.whole_transfer_amount=v_t.cash_amount
              and o.occurred_at_utc<=(p_args->>'occurred_at')::timestamptz) then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.cash_amount,v_t.currency,
        v_t.beneficiary_kind,v_t.beneficiary_id,v_t.original_transfer_id,v_t.return_cash_id);
    else
      v_amount:=v_t.cash_amount;
      select r.* into v_prior from private.bpay_next_csv_instruction r where id=v_id;
      if found then
        if (v_prior.transfer_id,v_prior.run_worker_id,v_prior.actor_user_id,v_prior.cash_amount)
          is distinct from (v_t.id,v_w.id,p_actor,v_t.cash_amount) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_t.id) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        if p_lock then select r.* into strict v_candidate from public.candidates r where id=v_candidate_id for share;
        else select r.* into strict v_candidate from public.candidates r where id=v_candidate_id;end if;
        v_bank_hash:=public._bank_hash(v_candidate.sort_code,v_candidate.account_number,v_candidate.account_holder);
        select r.* into v_leg from private.bpay_next_destination_group_leg r where transfer_id=coalesce(v_t.original_transfer_id,v_t.id);
        if v_w.target_pay_channel='UMBRELLA' then
          if p_lock then select r.* into strict v_umbrella from public.umbrellas r where id=v_w.target_umbrella_id for share;
          else select r.* into strict v_umbrella from public.umbrellas r where id=v_w.target_umbrella_id;end if;
          v_bank_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
          if v_umbrella.enabled is not true or v_bank_hash is distinct from v_umbrella.bank_details_hash then
            raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
        elsif v_leg.leg_kind='ONEOFF' then
          select o.* into strict v_origin from private.bpay_next_net_destination_amount d
            join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id where d.destination_id=v_leg.destination_id;
          if p_lock then perform 1 from public.pay_finance_case_oneoff_payout_bank_details
            where finance_case_id=v_origin.legacy_case_id and candidate_id=v_origin.candidate_id for share;end if;
          if not exists(select 1 from public.pay_finance_case_oneoff_payout_bank_details b
            where b.finance_case_id=v_origin.legacy_case_id and b.candidate_id=v_origin.candidate_id
              and (b.updated_at_utc,b.bank_details_hash,b.beneficiary_name,replace(b.sort_code,'-',''),b.account_number)
                is not distinct from (v_origin.bank_version_at_utc,v_origin.bank_details_hash,v_origin.beneficiary_name,v_origin.sort_code,v_origin.account_number)) then
            raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
          v_bank_hash:=v_origin.bank_details_hash;v_bank_version:=v_origin.bank_version_at_utc;
        elsif v_bank_hash is distinct from v_candidate.bank_details_hash then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
        if v_t.status<>'MEMBERS_READY' or v_t.cash_amount<=0 or v_bank_hash is null then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.projection_id,v_t.return_cash_id,
        v_t.cash_amount,v_t.currency,v_t.beneficiary_kind,v_t.beneficiary_id,v_bank_hash,v_bank_version,
        v_t.member_count,v_t.member_cash_sum);
    end if;
  end if;

  if v_replay then
    if p_action<>'CSV_ISSUE' and not exists(select 1 from private.bpay_next_command
      where id=v_id and command_kind=v_kind and module_epoch=v_epoch) then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    select scope_sha256 into v_scope from private.bpay_next_reauth_proof
      where consumed_command_id=v_id and actor_user_id=p_actor and action_kind=p_action;
    -- A same-ID native CSV re-download is expressly exempt; no new issue.
    if v_scope is null and p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  else
    if p_action<>'CSV_ISSUE' and exists(select 1 from private.bpay_next_command where id=v_id) then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    v_scope:=encode(extensions.digest(jsonb_build_array(p_actor,p_action,v_epoch,v_intent,v_facts)::text,'sha256'),'hex');
  end if;
  select coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),'Candidate')
    into strict v_name from public.candidates c where c.id=v_candidate_id;
  return jsonb_build_object('action_kind',p_action,'action_label',v_label,'candidate_count',1,
    'candidate_name',v_name,'total_amount',round(v_amount,2)::text,'amount_basis',v_basis,
    'scope_sha256',v_scope,'accepted',v_replay);
end
$function$;

create or replace function private.bpay_next_segment_write_off_membership_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request private.bpay_next_write_off_request%rowtype;v_basis private.bpay_next_work_collection%rowtype;v_total numeric;
begin
  if new.event_kind<>'WRITTEN_OFF' then return new;end if;
  select r.* into v_request from private.bpay_next_write_off_request r where r.command_id=new.operation_id;
  if not found or v_request.work_collection_id is null then return new;end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_request.work_collection_id;
  if not v_basis.segment_attribution_complete then return new;end if;
  if (new.id,new.case_id,new.operation_item_id) is distinct from (v_request.command_id,v_request.case_id,v_basis.id)
    or new.written_off_delta<=0 or v_request.status<>'REQUESTED' or not exists(
      select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
      join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
      join private.bpay_next_finance_case fc on fc.id=v_basis.case_id and fc.case_revision=v_request.basis_case_revision
      join private.bpay_next_case_component x on x.id=v_basis.case_component_id and x.component_revision=v_request.expected_component_revision
      where j.command_id=new.operation_id and j.candidate_id=v_basis.candidate_id and j.job_kind='CASE_WRITE_OFF'
        and j.status='LEASED' and j.lease_until_utc>clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_OWNER_INVALID';
  end if;
  insert into private.bpay_next_work_segment_write_off_share(case_event_id,origin_share_id,work_collection_id,work_id,
    component_key,segment_key,written_off_source_ex_vat)
  with origins as (
    select o.*,coalesce(sum(o.available_source_ex_vat) over(order by o.segment_key,o.origin_share_id
      rows between unbounded preceding and 1 preceding),0) earlier
    from private.bpay_next_available_segment_recovery_origins_v1(v_basis.work_id,v_basis.component_key,v_basis.observed_revision_id) o
  ) select new.id,o.origin_share_id,v_basis.id,v_basis.work_id,v_basis.component_key,o.segment_key,
    least(o.available_source_ex_vat,greatest(new.written_off_delta-o.earlier,0)) from origins o
    where least(o.available_source_ex_vat,greatest(new.written_off_delta-o.earlier,0))>0;
  select sum(s.written_off_source_ex_vat) into v_total from private.bpay_next_work_segment_write_off_share s where s.case_event_id=new.id;
  if v_total is distinct from new.written_off_delta then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_WRITE_OFF_ORIGINS_INCOMPLETE';
  end if;
  return new;
end
$function$;

alter function private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text) owner to postgres;
revoke all on function private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_write_off_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_apply_write_off_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_write_off_guard_v1() owner to postgres;
revoke all on function private.bpay_next_write_off_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_segment_write_off_membership_v1() owner to postgres;
revoke all on function private.bpay_next_segment_write_off_membership_v1() from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
