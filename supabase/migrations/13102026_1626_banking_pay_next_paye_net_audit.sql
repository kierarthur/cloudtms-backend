-- DR-23 append-only PAYE request audit; no historical backfill or business rows.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_paye_net_request
 add column prior_projection_id uuid,
 add column prior_entered_paye_net private.bpay_next_penny_amount check(prior_entered_paye_net>=0),
 add constraint bpay_next_net_request_prior_projection_fk
  foreign key(prior_projection_id,run_worker_id)
  references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
 add constraint bpay_next_net_request_prior_net_pair
  check((prior_projection_id is null)=(prior_entered_paye_net is null)),
 add constraint bpay_next_net_request_prior_revision
  check(prior_projection_id is null or expected_projection_revision>0);

-- Preserve historical NULL actors. Current authenticated no-case intake
-- records its Office actor; the existing Case and monotonic-number arms stay.
alter table private.bpay_next_paye_net_request drop constraint bpay_next_net_request_case_shape,
 add constraint bpay_next_net_request_case_shape check(
 (case_draft_state_id is null and preparation_revision is null
  and selection_revision is null and expected_projection_revision is null and input_kind='PAYE_MANUAL')
 or (case_draft_state_id is not null and actor_user_id is not null and preparation_revision is not null
  and selection_revision is not null and expected_projection_revision is not null and request_no>expected_projection_revision));

commit;
