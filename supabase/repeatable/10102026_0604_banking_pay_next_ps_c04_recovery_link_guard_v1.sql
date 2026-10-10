-- PS-04: qualify the existing native event link. No writer/counter change.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_recovery_effect_link_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_event record;v_basis record;v_source record;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_IMMUTABLE';
  end if;
  if new.effect_kind<>'WORK_COLLECTION_RECOVERED' then return new;end if;
  select operation_id,operation_item_id,event_kind,case_id,case_component_id,candidate_id,
    instruction_id,allocation_result_id,original_transfer_id,recovered_delta,source_amount_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc
    into v_event from private.bpay_next_case_event where id=new.operation_item_id;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_EVENT_MISSING';end if;
  select b.work_id,b.component_key,b.candidate_id,b.case_id,b.case_component_id,b.original_run_line_id,
    b.original_valuation_policy_id,i.run_worker_id,i.valuation_policy_id,w.original_timesheet_id
    into v_basis from private.bpay_next_run_case_instruction i
    join private.bpay_next_work_collection b on b.id=i.work_collection_id
    join private.bpay_next_work w on w.id=b.work_id
    where i.id=v_event.instruction_id;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_COLLECTION_MISSING';end if;
  if v_event.event_kind<>'RECOVERED' or v_event.source_amount_ex_vat<=0
     or v_event.source_amount_ex_vat is distinct from v_event.recovered_delta
     or v_basis.valuation_policy_id is distinct from v_basis.original_valuation_policy_id
     or (new.operation_id,new.candidate_id,new.case_id,new.original_transfer_id,new.occurred_at_utc)
       is distinct from (v_event.operation_id,v_event.candidate_id,v_event.case_id,v_event.original_transfer_id,v_event.occurred_at_utc)
     or (new.work_id,new.component_key,new.candidate_id,new.case_id,new.original_timesheet_id)
       is distinct from (v_basis.work_id,v_basis.component_key,v_basis.candidate_id,v_basis.case_id,v_basis.original_timesheet_id)
     or (v_event.case_id,v_event.case_component_id,v_event.candidate_id)
       is distinct from (v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id)
     or (new.source_disposition_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
       is distinct from (-v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_SCOPE_AMOUNT_MISMATCH';
  end if;
  if not exists(select 1 from private.bpay_next_case_hold h
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED'
      and u.realisation_event_id=new.operation_item_id
    join private.bpay_next_outcome_request r on r.command_id=new.operation_id
      and r.transfer_id=v_event.original_transfer_id and r.candidate_id=new.candidate_id
    join private.bpay_next_transfer_member m on m.transfer_id=r.transfer_id and m.subject_kind='CASE'
      and m.case_hold_id=h.id and m.case_instruction_id=v_event.instruction_id
      and m.case_allocation_result_id=v_event.allocation_result_id
    where h.id=v_event.operation_item_id and h.status='REALISED' and h.instruction_id=v_event.instruction_id
      and h.allocation_result_id=v_event.allocation_result_id and h.run_worker_id=v_basis.run_worker_id
      and h.run_worker_id=r.run_worker_id and h.source_reserved_ex_vat=v_event.source_amount_ex_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_REALISATION_MISSING';
  end if;
  if not exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control module on module.id=1 and module.active_owner='NEXT'
    join private.bpay_next_worker_control control on control.candidate_id=j.candidate_id
    where j.command_id=new.operation_id and j.candidate_id=new.candidate_id
      and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS' and j.status='LEASED'
      and j.module_epoch=module.owner_epoch and j.owner_epoch=control.active_owner_epoch
      and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_OWNER_STALE';
  end if;
  select id,source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_pay_channel
    into strict v_source from private.bpay_next_run_line where id=v_basis.original_run_line_id;
  if (new.source_run_line_id,new.source_binding_version_id,new.canonical_source_work_id,
      new.canonical_source_component_key,new.native_source_channel,new.source_case_event_id)
    is distinct from (case when v_source.source_binding_version_id is null then null else v_source.id end,
      v_source.source_binding_version_id,v_source.canonical_source_work_id,v_source.canonical_source_component_key,
      case when v_source.source_binding_version_id is null then null else v_source.source_pay_channel end,
      case when v_source.source_binding_version_id is null then null else new.operation_item_id end) then
    raise exception using errcode='23514',message='BPAY_NEXT_RECOVERY_LINK_SOURCE_MISMATCH';
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_recovery_effect_link_guard_v1 on private.bpay_next_financial_effect;
create trigger bpay_next_recovery_effect_link_guard_v1 before insert on private.bpay_next_financial_effect
  for each row execute function private.bpay_next_recovery_effect_link_guard_v1();
alter function private.bpay_next_recovery_effect_link_guard_v1() owner to postgres;
revoke all on function private.bpay_next_recovery_effect_link_guard_v1() from public,anon,authenticated,service_role;
commit;
