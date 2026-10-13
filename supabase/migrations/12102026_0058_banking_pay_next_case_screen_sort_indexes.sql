-- FC-02 / owner A3e: native Case list sort keys only; existing deferred refresh owns currentness.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_case_list_key add column list_status text;
alter table private.bpay_next_case_list_key add column list_original numeric;
alter table private.bpay_next_case_list_key add column list_outstanding numeric;
alter table private.bpay_next_case_list_key add column list_weekly_due numeric;
update private.bpay_next_case_list_key k set list_status=c.status,list_original=c.principal_approved,
 list_outstanding=(case when c.case_kind in ('LOAN','ADVANCE') then c.principal_funded else c.principal_approved end)-c.principal_recovered-c.principal_written_off-c.principal_paid_credit,
 list_weekly_due=r.weekly_due_source_ex_vat
 from private.bpay_next_finance_case c left join private.bpay_next_case_rule r on r.id=c.current_rule_id and r.case_id=c.id and r.candidate_id=c.candidate_id and r.rule_revision=c.current_rule_revision where k.case_id=c.id;
create index bpay_next_case_list_status_asc_idx on private.bpay_next_case_list_key ((list_status is null),list_status asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_status_desc_idx on private.bpay_next_case_list_key ((list_status is null),list_status desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_original_asc_idx on private.bpay_next_case_list_key ((list_original is null),list_original asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_original_desc_idx on private.bpay_next_case_list_key ((list_original is null),list_original desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_outstanding_asc_idx on private.bpay_next_case_list_key ((list_outstanding is null),list_outstanding asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_outstanding_desc_idx on private.bpay_next_case_list_key ((list_outstanding is null),list_outstanding desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_weekly_asc_idx on private.bpay_next_case_list_key ((list_weekly_due is null),list_weekly_due asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_weekly_desc_idx on private.bpay_next_case_list_key ((list_weekly_due is null),list_weekly_due desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
-- Native control ordering and scope probes. Joined-label sorts keep full residual ordering.
create index bpay_next_snooze_history_created_idx on private.bpay_next_snooze(active,created_at_utc desc,id);
create index bpay_next_snooze_until_asc_idx on private.bpay_next_snooze(active,(until_date is null),until_date asc,created_at_utc desc,id);
create index bpay_next_snooze_until_desc_idx on private.bpay_next_snooze(active,(until_date is null),until_date desc,created_at_utc desc,id);
create index bpay_next_snooze_note_asc_idx on private.bpay_next_snooze(active,note collate "C" asc,created_at_utc desc,id);
create index bpay_next_snooze_note_desc_idx on private.bpay_next_snooze(active,note collate "C" desc,created_at_utc desc,id);
create index bpay_next_snooze_work_labels_idx on private.bpay_next_snooze(work_id,scope_kind,id);
create index bpay_next_snooze_revision_candidate_idx on private.bpay_next_work_revision((left(lower(candidate_display_name),512)) collate "C",work_id,id);
create index bpay_next_snooze_revision_week_idx on private.bpay_next_work_revision(week_ending_date,work_id,id);
create index bpay_next_snooze_scope_label_idx on private.bpay_next_snooze(scope_kind,case_kind,created_at_utc desc,id);
create index bpay_next_snooze_history_created_asc_idx on private.bpay_next_snooze(active,created_at_utc asc,id);
commit;
