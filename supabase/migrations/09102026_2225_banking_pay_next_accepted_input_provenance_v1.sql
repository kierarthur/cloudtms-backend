-- F-25 successor of immutable locked2153. Complete reconstructed weekly
-- economic/eligibility origin and close the finite input-owner inventory.
-- No backfill, rewrite, counter copy, browser grant or external action.

\set ON_ERROR_STOP on

begin;

-- This is a first-install additive release. Unexpected populated receipts
-- cannot acquire invented historic origin JSON/sequence via a DEFAULT.
do $accepted_week_empty$
begin
  if exists(select 1 from private.bpay_next_accepted_week_input) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_WEEK_PROVENANCE_INSTALL_REQUIRES_EMPTY';
  end if;
end
$accepted_week_empty$;
alter table private.bpay_next_accepted_week_input
  add column economic_origin_proof jsonb not null check(pg_catalog.jsonb_typeof(economic_origin_proof)='object'
    and pg_catalog.octet_length(economic_origin_proof::text)<=16384),
  add column eligibility_command_id uuid not null,
  add column eligibility_sequence bigint not null check(eligibility_sequence>0),
  add constraint bpay_next_accepted_week_eligibility_command_fk foreign key(eligibility_command_id,eligibility_sequence)
    references private.bpay_next_command(id,agency_sequence) on delete restrict;
create index bpay_next_accepted_week_basis_command_idx
  on private.bpay_next_accepted_week_input(economic_basis_command_id,accepted_input_id);
create index bpay_next_accepted_week_eligibility_command_idx
  on private.bpay_next_accepted_week_input(eligibility_command_id,accepted_input_id);

alter table private.bpay_next_input_version_event
  add constraint bpay_next_input_version_owner_relation_shape check(
    (input_kind='CANDIDATE' and owner_relation='public.candidates')
    or (input_kind='UMBRELLA' and owner_relation='public.umbrellas')
    or (input_kind='POLICY' and owner_relation='private.bpay_next_valuation_policy_control')
    or (input_kind='PAY_QUERY' and owner_relation in (
      'private.weekly_source_manual_reviews','public.weekly_discrepancy_incidents',
      'public.weekly_issue_comparison_revisions','public.weekly_exceptional_pay_target_families',
      'public.weekly_exceptional_pay_family_events','public.weekly_exceptional_pay_target_events',
      'public.weekly_exceptional_payment_approvals','public.weekly_exceptional_c1_publication_requests',
      'private.weekly_source_local_protected_decision_receipts','private.bpay_next_protected_source_receipt',
      'public.weekly_exceptional_pay_generations','public.weekly_exceptional_orchestration_runs',
      'public.weekly_exceptional_orchestration_steps','public.weekly_exceptional_c1_publication_checkpoints',
      'public.weekly_source_pending_entitlement_bundles','private.weekly_source_entitlement_publication_receipts',
      'public.weekly_source_entitlement_heads','public.weekly_source_entitlement_head_components',
      'public.weekly_source_root_authorisations')));

commit;
