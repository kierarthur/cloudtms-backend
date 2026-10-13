-- One-time CloudTMS schema/data migration: banking_pay_next_paye_same_week_guard
-- DR-10 / BD111: private original-Create policy audit and member skips only.
-- Additive empty columns; no legacy carry-over, business row rewrite or fixture.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_pay_run
  add column paye_override_reason text,
  add column paye_override_actor uuid references public.tms_users(id),
  add column reauth_proof_id uuid references private.bpay_next_reauth_proof(id),
  add column channel_scope text not null default 'ALL',
  add column paye_guard_decision text not null default 'NONE',
  add column paye_guard_scope_sha256 text,
  add constraint bpay_next_run_paye_guard_decision_check check (
    (paye_guard_decision='NONE' and channel_scope='ALL'
      and paye_override_reason is null and paye_override_actor is null
      and reauth_proof_id is null and paye_guard_scope_sha256 is null)
    or (paye_guard_decision='UMBRELLA_ONLY' and channel_scope='UMBRELLA'
      and paye_override_actor is not null and paye_override_reason is null
      and reauth_proof_id is null and paye_guard_scope_sha256 is null)
    or (paye_guard_decision in ('VERIFY_BOTH','VERIFY_PAYE')
      and channel_scope=case paye_guard_decision when 'VERIFY_BOTH' then 'ALL' else 'PAYE' end
      and paye_override_actor is not null and reauth_proof_id is not null
      and paye_override_reason is not null and btrim(paye_override_reason)<>''
      and octet_length(paye_override_reason) between 1 and 500
      and paye_guard_scope_sha256 is not null
      and paye_guard_scope_sha256 ~ '^[0-9a-f]{64}$'));

alter table private.bpay_next_run_choice_member
  drop constraint bpay_next_run_choice_member_skip_code_check;
-- Keep an old stored spelling only where actually retained. Never rewrite rows.
do $skip_check$
begin
  if exists(select 1 from private.bpay_next_run_choice_member
    where skip_code='BPAY_NEXT_SELECTION_PAY_ON_HOLD') then
    alter table private.bpay_next_run_choice_member
      add constraint bpay_next_run_choice_member_skip_code_check check(skip_code in (
        'BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE','BPAY_NEXT_SELECTION_SNOOZED',
        'BPAY_NEXT_SELECTION_ON_HOLD','BPAY_NEXT_SELECTION_PAY_ON_HOLD',
        'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED','BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT',
        'BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE','SOURCE_PAY_QUERY_OPEN',
        'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE','PAYE_EXCLUDED_BY_SAME_WEEK_GUARD'));
  else
    alter table private.bpay_next_run_choice_member
      add constraint bpay_next_run_choice_member_skip_code_check check(skip_code in (
        'BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE','BPAY_NEXT_SELECTION_SNOOZED',
        'BPAY_NEXT_SELECTION_ON_HOLD','BPAY_NEXT_SELECTION_WORK_NOT_APPROVED',
        'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT','BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE',
        'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE',
        'PAYE_EXCLUDED_BY_SAME_WEEK_GUARD'));
  end if;
end
$skip_check$;

commit;
