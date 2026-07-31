-- Bounded source-history lookup for NHSP/HealthRoster Weekly authoritative
-- reconciliation.  The index changes no import, invoice or payment behaviour.
create index if not exists idx_audit_import_correction_source_v1
on public.audit_events (
  (after_json ->> 'shift_id'),
  (after_json ->> 'external_row_key'),
  (after_json ->> 'correction_id'),
  action,
  ts_utc desc,
  id
)
where object_type in ('timesheets', 'invoices', 'contract_weeks')
  and action in (
    'NHSP_IMPORT_CORRECTION_APPLIED',
    'HR_IMPORT_CORRECTION_APPLIED'
  );
