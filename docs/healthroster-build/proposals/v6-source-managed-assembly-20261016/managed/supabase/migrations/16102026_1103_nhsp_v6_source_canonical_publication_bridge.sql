-- One-time CloudTMS schema/data migration: nhsp_v6_source_canonical_publication_bridge
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Exact d863577918ce7d439206296e91ad78de298e98b2:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/04_complete_canonical_publication_bridge.sqlinc
-- After actual Banking publication/member/subject-binding schemas.
create table private.weekly_source_pay_decision_publication_adoptions (
 command_id uuid not null,publication_member_no integer not null check(publication_member_no>0),
 application_id uuid not null,member_key text not null,
 primary key(command_id,publication_member_no,application_id,member_key),
 foreign key(command_id,publication_member_no) references private.bpay_next_source_pay_decision_publication_member(command_id,member_no) on delete restrict,
 foreign key(application_id,member_key) references private.weekly_source_pay_decision_native_adoptions(application_id,member_key) on delete restrict
);
alter table private.weekly_source_pay_decisions add constraint source_pay_current_cut_same_decision_fk
 foreign key(id,current_financial_cut_id) references private.bpay_next_source_pay_decision_cut(decision_id,id) deferrable initially deferred;
alter table private.weekly_source_pay_decision_bindings add constraint source_pay_actual_disposition_binding_fk
 foreign key(banking_disposition_id,banking_disposition_binding_no) references private.bpay_next_worker_pay_disposition_subject_binding(disposition_id,binding_no) on delete restrict;
alter table private.weekly_source_pay_decision_members add constraint source_pay_actual_disposition_fk
 foreign key(pay_disposition_id) references private.bpay_next_worker_pay_disposition(id) on delete restrict;

commit;
