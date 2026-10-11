-- One-time CloudTMS schema/data migration: banking_pay_next_held_choice_skip_check_v1
-- Owner 11 October A1: current Create/append write ON_HOLD. Private CHECK only.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_run_choice_member
  drop constraint bpay_next_run_choice_member_skip_code_check;

-- Preserve an already-stored old spelling only where it actually exists.
-- No stored row is rewritten; all current writers use ON_HOLD.
do $skip_check$
begin
  if exists(select 1 from private.bpay_next_run_choice_member
    where skip_code='BPAY_NEXT_SELECTION_PAY_ON_HOLD') then
    alter table private.bpay_next_run_choice_member
      add constraint bpay_next_run_choice_member_skip_code_check
      check(skip_code in ('BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE',
        'BPAY_NEXT_SELECTION_SNOOZED','BPAY_NEXT_SELECTION_ON_HOLD',
        'BPAY_NEXT_SELECTION_PAY_ON_HOLD',
        'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED','BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT',
        'BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE',
        'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE'));
  else
    alter table private.bpay_next_run_choice_member
      add constraint bpay_next_run_choice_member_skip_code_check
      check(skip_code in ('BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE',
        'BPAY_NEXT_SELECTION_SNOOZED','BPAY_NEXT_SELECTION_ON_HOLD',
        'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED','BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT',
        'BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE',
        'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE'));
  end if;
end
$skip_check$;

commit;
