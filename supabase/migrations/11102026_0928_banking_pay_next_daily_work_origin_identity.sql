-- DY-03 additive native Work identity; existing rows remain CONTRACT.
-- No business rows are reclassified and restrictive physical/Contract FKs stay intact.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_work
  add column origin_kind text not null default 'CONTRACT',
  add column origin_work_date date;
alter table private.bpay_next_work
  alter column contract_id drop not null,
  add constraint bpay_next_work_origin_identity_check check (
    (origin_kind='CONTRACT' and contract_id is not null and origin_work_date is null)
    or (origin_kind='DAILY' and contract_id is null and work_kind='ORDINARY'
        and origin_work_date is not null)
  );
alter table private.bpay_next_work_revision
  add column daily_approval_evidence_json jsonb,
  add constraint bpay_next_revision_daily_evidence_shape_check check (
    daily_approval_evidence_json is null
    or jsonb_typeof(daily_approval_evidence_json)='object'
  );
commit;
