-- DR-23 finite version of private Case capacity; no business backfill or new fence.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_worker_control
 add column net_capacity_revision bigint not null default 0 check(net_capacity_revision>=0);
alter table private.bpay_next_paye_net_request
 add column preview_capacity_revision bigint check(preview_capacity_revision>=0);
-- Correct nullable audit shape through a successor, never its applied migration.
alter table private.bpay_next_paye_net_request drop constraint bpay_next_net_request_prior_revision,
 add constraint bpay_next_net_request_prior_revision
 check(prior_projection_id is null or (expected_projection_revision is not null and expected_projection_revision>0));

create or replace function private.bpay_next_net_capacity_revision_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_candidate uuid;v_changed boolean:=true;
begin
 if tg_table_schema<>'private' or tg_table_name not in ('bpay_next_finance_case','bpay_next_case_component','bpay_next_case_period',
  'bpay_next_case_hold','bpay_next_case_capacity_use','bpay_next_worker_week_contribution') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_REVISION_SCOPE_INVALID';end if;
 if tg_op='UPDATE' then
  if new.candidate_id is distinct from old.candidate_id then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_REVISION_SCOPE_INVALID';end if;
  case tg_table_name
   when 'bpay_next_finance_case' then
    v_changed:=(new.principal_funded,new.principal_approved,new.principal_recovered,new.principal_written_off,new.active_recovery_hold_amount,new.active_payout_hold_amount)
     is distinct from (old.principal_funded,old.principal_approved,old.principal_recovered,old.principal_written_off,old.active_recovery_hold_amount,old.active_payout_hold_amount);
   when 'bpay_next_case_component' then
    v_changed:=(new.funded_source_ex_vat,new.approved_source_ex_vat,new.recovered_source_ex_vat,new.written_off_source_ex_vat,new.active_recovery_source_ex_vat,new.active_payout_source_ex_vat)
     is distinct from (old.funded_source_ex_vat,old.approved_source_ex_vat,old.recovered_source_ex_vat,old.written_off_source_ex_vat,old.active_recovery_source_ex_vat,old.active_payout_source_ex_vat);
   when 'bpay_next_case_period' then
    v_changed:=(new.opening_due_source_ex_vat,new.realised_recovery_source_ex_vat,new.active_unrealised_recovery_source_ex_vat,new.active_payout_source_ex_vat)
     is distinct from (old.opening_due_source_ex_vat,old.realised_recovery_source_ex_vat,old.active_unrealised_recovery_source_ex_vat,old.active_payout_source_ex_vat);
   when 'bpay_next_case_hold' then
    v_changed:=(new.status,new.source_reserved_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.run_worker_id,new.instruction_id,new.allocation_result_id)
     is distinct from (old.status,old.source_reserved_ex_vat,old.target_amount_ex_vat,old.target_amount_vat,old.run_worker_id,old.instruction_id,old.allocation_result_id);
   when 'bpay_next_case_capacity_use' then
    v_changed:=(new.status,new.source_amount_ex_vat,new.case_hold_id)
     is distinct from (old.status,old.source_amount_ex_vat,old.case_hold_id);
   when 'bpay_next_worker_week_contribution' then
    v_changed:=(new.eligibility_state,new.eligible_arranged_amount,new.payment_state,new.basis_kind,
     new.original_gross_amount,new.original_payroll_net,new.accepted_projection_id,new.accepted_bank_cash,
     new.original_pay_date,new.original_created_at_utc,new.original_run_worker_id,new.pay_week_start,new.primary_binding_revision)
     is distinct from (old.eligibility_state,old.eligible_arranged_amount,old.payment_state,old.basis_kind,
     old.original_gross_amount,old.original_payroll_net,old.accepted_projection_id,old.accepted_bank_cash,
     old.original_pay_date,old.original_created_at_utc,old.original_run_worker_id,old.pay_week_start,old.primary_binding_revision);
  end case;
 end if;
 if v_changed then
  v_candidate:=case when tg_op='DELETE' then old.candidate_id else new.candidate_id end;
  -- Native financial owners already hold Candidate control before Case rows.
  -- No public table, money value, financial_view_revision or owner lease changes.
  update private.bpay_next_worker_control set net_capacity_revision=net_capacity_revision+1 where candidate_id=v_candidate;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CAPACITY_CONTROL_MISSING';end if;
 end if;
 if tg_op='DELETE' then return old;else return new;end if;
end
$function$;
alter function private.bpay_next_net_capacity_revision_v1() owner to postgres;
revoke all on function private.bpay_next_net_capacity_revision_v1() from public,anon,authenticated,service_role;

create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_finance_case
 for each row execute function private.bpay_next_net_capacity_revision_v1();
create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_case_component
 for each row execute function private.bpay_next_net_capacity_revision_v1();
create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_case_period
 for each row execute function private.bpay_next_net_capacity_revision_v1();
create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_case_hold
 for each row execute function private.bpay_next_net_capacity_revision_v1();
create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_case_capacity_use
 for each row execute function private.bpay_next_net_capacity_revision_v1();
create trigger bpay_next_net_capacity_revision after insert or update or delete on private.bpay_next_worker_week_contribution
 for each row execute function private.bpay_next_net_capacity_revision_v1();
commit;
