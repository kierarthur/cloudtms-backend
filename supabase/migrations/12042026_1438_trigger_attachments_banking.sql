begin;

do $$
begin
  if to_regprocedure('public.pay_workbench_mark_candidate_dirty()') is null then
    raise exception 'Required trigger function is missing: public.pay_workbench_mark_candidate_dirty()';
  end if;

  if to_regprocedure('public.pay_workbench_mark_finance_case_dirty()') is null then
    raise exception 'Required trigger function is missing: public.pay_workbench_mark_finance_case_dirty()';
  end if;

  if to_regprocedure('public.pay_workbench_mark_contract_client_dirty()') is null then
    raise exception 'Required trigger function is missing: public.pay_workbench_mark_contract_client_dirty()';
  end if;
end
$$;

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__timesheets
on public.timesheets;

create trigger trg_pay_workbench_mark_candidate_dirty__timesheets
after insert or update or delete
on public.timesheets
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__timesheets_financials
on public.timesheets_financials;

create trigger trg_pay_workbench_mark_candidate_dirty__timesheets_financials
after insert or update or delete
on public.timesheets_financials
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state
on public.timesheet_pay_state;

create trigger trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state
after insert or update or delete
on public.timesheet_pay_state
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__timesheet_payment_overrides
on public.timesheet_payment_overrides;

create trigger trg_pay_workbench_mark_candidate_dirty__timesheet_payment_overrides
after insert or update or delete
on public.timesheet_payment_overrides
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__pay_item_snoozes
on public.pay_item_snoozes;

create trigger trg_pay_workbench_mark_candidate_dirty__pay_item_snoozes
after insert or update or delete
on public.pay_item_snoozes
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__ts_pay_adjustments
on public.ts_pay_adjustments;

create trigger trg_pay_workbench_mark_candidate_dirty__ts_pay_adjustments
after insert or update or delete
on public.ts_pay_adjustments
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__pay_advances
on public.pay_advances;

create trigger trg_pay_workbench_mark_candidate_dirty__pay_advances
after insert or update or delete
on public.pay_advances
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__bank_name_checks
on public.bank_name_checks;

create trigger trg_pay_workbench_mark_candidate_dirty__bank_name_checks
after insert or update or delete
on public.bank_name_checks
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__bank_payee_map
on public.bank_payee_map;

create trigger trg_pay_workbench_mark_candidate_dirty__bank_payee_map
after insert or update or delete
on public.bank_payee_map
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__candidates
on public.candidates;

create trigger trg_pay_workbench_mark_candidate_dirty__candidates
after insert or update or delete
on public.candidates
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_candidate_dirty__umbrellas
on public.umbrellas;

create trigger trg_pay_workbench_mark_candidate_dirty__umbrellas
after insert or update or delete
on public.umbrellas
for each row
execute function public.pay_workbench_mark_candidate_dirty();

drop trigger if exists trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_components
on public.pay_finance_case_components;

create trigger trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_components
after insert or update or delete
on public.pay_finance_case_components
for each row
execute function public.pay_workbench_mark_finance_case_dirty();

drop trigger if exists trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_events
on public.pay_finance_case_events;

create trigger trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_events
after insert or update or delete
on public.pay_finance_case_events
for each row
execute function public.pay_workbench_mark_finance_case_dirty();

drop trigger if exists trg_pay_workbench_mark_contract_client_dirty__contracts
on public.contracts;

create trigger trg_pay_workbench_mark_contract_client_dirty__contracts
after insert or update or delete
on public.contracts
for each row
execute function public.pay_workbench_mark_contract_client_dirty();

drop trigger if exists trg_pay_workbench_mark_contract_client_dirty__client_settings
on public.client_settings;

create trigger trg_pay_workbench_mark_contract_client_dirty__client_settings
after insert or update or delete
on public.client_settings
for each row
execute function public.pay_workbench_mark_contract_client_dirty();

commit;
