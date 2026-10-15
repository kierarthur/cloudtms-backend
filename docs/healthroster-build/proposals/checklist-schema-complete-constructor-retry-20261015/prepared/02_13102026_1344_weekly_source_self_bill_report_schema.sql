-- One-time CloudTMS schema/data migration: weekly_source_self_bill_report_schema
-- WP-A1/A2 reuse existing upload rows/report scopes; no report pay-hours store.
-- Exact existing constraint names were verified on agency TEST before writing.





create table public.weekly_source_self_bill_reports (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  report_number text not null check (report_number ~ '^XYOXLR[0-9]{6}$'),
  invoice_date date not null,
  content_sha256 bytea not null check (pg_catalog.octet_length(content_sha256)=32),
  line_count integer not null check (line_count between 1 and 100000),
  net_pence bigint not null check (net_pence between -999999999999 and 999999999999),
  vat_pence bigint not null check (vat_pence between -999999999999 and 999999999999),
  gross_pence bigint not null check (gross_pence between -999999999999 and 999999999999),
  row_manifest_hash bytea not null check (pg_catalog.octet_length(row_manifest_hash)=32),
  layout text not null check (layout in ('A','B')),
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (source_group_id,client_id,report_number),
  unique (content_sha256),
  check (pg_catalog.substring(report_number,7)=pg_catalog.to_char(invoice_date,'DDMMYY'))
);
alter table public.weekly_source_self_bill_reports owner to postgres;

create table public.weekly_source_self_bill_report_line_bindings (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  report_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  export_upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  bound_row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  export_projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  self_bill_report_id uuid not null references public.weekly_source_self_bill_reports(id) on delete restrict,
  state text not null check (state in ('CURRENT','SUPERSEDED')),
  bound_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  superseded_at_utc timestamptz,
  check ((state='SUPERSEDED')=(superseded_at_utc is not null))
);
alter table public.weekly_source_self_bill_report_line_bindings owner to postgres;
create unique index weekly_source_report_line_current_binding_uq
  on public.weekly_source_self_bill_report_line_bindings(report_row_id) where state='CURRENT';
create index weekly_source_report_bindings_event_idx
  on public.weekly_source_self_bill_report_line_bindings(work_event_id,self_bill_report_id);

-- Both tables remain API-inaccessible; service-owned definers use the matching
-- provider-neutral policy. Immutable guards are reasserted after shared helpers
-- exist in repeatable order, just as in F1's clean NEW correction.
do $report_rls$
declare v_name text;
begin
  foreach v_name in array array['weekly_source_self_bill_reports','weekly_source_self_bill_report_line_bindings'] loop
    execute pg_catalog.format('alter table public.%I enable row level security',v_name);
    execute pg_catalog.format('alter table public.%I force row level security',v_name);
    execute pg_catalog.format('revoke all on table public.%I from public,anon,authenticated,service_role',v_name);
    execute pg_catalog.format('create policy cloudtms_miget_service_owner_all on public.%I for all to %I,service_role using (true) with check (true)',v_name,current_user);
  end loop;
end;
$report_rls$;

alter table public.weekly_source_report_scopes
  add column self_bill_report_id uuid references public.weekly_source_self_bill_reports(id) on delete restrict,
  drop constraint weekly_source_report_scopes_environment_agency_id_source_gr_key;
create unique index weekly_source_report_scopes_cutoff_uq
  on public.weekly_source_report_scopes(environment,agency_id,source_group_id,client_id,cutoff_at_utc)
  where self_bill_report_id is null;
create unique index weekly_source_report_scopes_self_bill_uq
  on public.weekly_source_report_scopes(source_group_id,client_id,self_bill_report_id)
  where self_bill_report_id is not null;

alter table public.weekly_source_format_profiles
  drop constraint weekly_source_format_profiles_final_authority_kind_check,
  add constraint weekly_source_format_profiles_final_authority_kind_check
    check (final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS','SELF_BILL_REPORT')),
  drop constraint weekly_source_format_profiles_check,
  add constraint weekly_source_format_profiles_check
    check ((final_authority_kind in ('NHSP_TRUST_BACKING_REPORT','SELF_BILL_REPORT'))=(omission_meaning='NO_INFERENCE'));

alter table public.weekly_final_source_correction_sessions
  drop constraint weekly_final_source_correction_sessi_authority_scope_kind_check,
  add constraint weekly_final_source_correction_sessi_authority_scope_kind_check check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')),
  drop constraint weekly_final_source_correction_sessions_check,
  add constraint weekly_final_source_correction_sessions_check check ((authority_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE'))=(report_scope_id is not null));
alter table public.weekly_source_upload_supersessions
  drop constraint weekly_source_upload_supersessions_authority_scope_kind_check,
  add constraint weekly_source_upload_supersessions_authority_scope_kind_check check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')),
  drop constraint weekly_source_upload_supersessions_check1,
  add constraint weekly_source_upload_supersessions_check1 check ((authority_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE'))=(report_scope_id is not null));
alter table public.weekly_source_projection_publications
  drop constraint weekly_source_projection_publication_authority_scope_kind_check,
  add constraint weekly_source_projection_publication_authority_scope_kind_check check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')),
  drop constraint weekly_source_projection_publications_check,
  add constraint weekly_source_projection_publications_check check ((authority_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE'))=(report_scope_id is not null));
alter table public.weekly_source_final_revisions
  drop constraint weekly_source_final_revisions_authority_scope_kind_check,
  add constraint weekly_source_final_revisions_authority_scope_kind_check check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')),
  drop constraint weekly_source_final_revisions_check,
  add constraint weekly_source_final_revisions_check check ((authority_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE'))=(report_scope_id is not null));

alter table public.weekly_source_upload_rows
  drop constraint weekly_source_upload_rows_row_finalisation_state_check,
  add constraint weekly_source_upload_rows_row_finalisation_state_check check (row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE','SOURCE_REPORT_LINE'));
alter table public.weekly_source_uploads
  drop constraint weekly_source_uploads_coverage_proof_kind_check,
  add constraint weekly_source_uploads_coverage_proof_kind_check check (coverage_proof_kind in ('FORMAT_MANIFEST','OFFICE_COMPLETE_EXPORT_ATTESTATION','EXPLICIT_EMPTY_CONFIRMATION','NHSP_TRUST_REPORT_SCOPE','HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION','SELF_BILL_REPORT_SCOPE'));
alter table public.weekly_source_client_manifests
  drop constraint weekly_source_client_manifests_invoice_state_check,
  add constraint weekly_source_client_manifests_invoice_state_check check (invoice_state in ('READY','PARTLY_ADMITTED','ADMITTED','PAY_ONLY'));


