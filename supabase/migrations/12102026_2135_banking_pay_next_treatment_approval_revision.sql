-- WB-29 / BD155: additive same-Work original approval identity; empty metadata only.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_work_revision add column payment_treatment_approval_revision_id uuid;
alter table private.bpay_next_work_revision add constraint bpay_next_treatment_approval_revision_fk
 foreign key(work_id,payment_treatment_approval_revision_id)
 references private.bpay_next_work_revision(work_id,id) on delete restrict;
alter table private.bpay_next_work_revision add constraint bpay_next_treatment_approval_distinct_check
 check(payment_treatment_approval_revision_id is null or
  (payment_treatment_approval_revision_id<>id and source_event_id is null));
comment on column private.bpay_next_work_revision.payment_treatment_approval_revision_id
 is 'WB-29 complete payment-treatment publication: exact original sealed approval, never another treatment. Original Source event and book remain there.';
commit;
